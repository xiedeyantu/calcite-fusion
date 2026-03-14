# calcite-fusion Pipeline 执行引擎设计文档

## 1. 背景与目标

Calcite 默认的 `EnumerableConvention` 执行层采用 **Volcano Pull 模型**——每次 `next()` 只返回一行，且每行是 `Object[]`，存在大量装箱开销和逐行虚方法调用。

本引擎将执行层替换为 **Push-based Pipeline**，同时保持 Calcite 的解析、校验、优化层不变。

---

## 2. 整体架构

```
SQL 语句
   │
   ▼  Calcite Parser / Validator (calcite-core 原生，不修改)
   │
   ▼  Hep Planner 逻辑优化
   │    规则: FILTER_INTO_JOIN, FILTER_PROJECT_TRANSPOSE, PROJECT_MERGE
   │
   ▼  Volcano Planner 物理转换
   │    物理规则集: FusionRules.FUSION_PHYSICAL_RULES
   │    产出: FusionConvention 物理计划树
   │
   ▼  FusionImplementor.buildPipeline()
   │    自顶向下遍历物理树，自底向上装配算子链
   │
   ▼  Source.execute()   ← 唯一触发执行的调用点
   │
   ▼  ResultCollector.getResults()
```

整个过程只有一个执行入口：`PipelineExecutor.execute(String sql)` 方法。

---

## 3. 核心数据结构

### 3.1 ColumnBatch — 列式批数据

**文件**：`src/main/java/github/xiedeyantu/pipeline/ColumnBatch.java`

```
ColumnBatch
  Object[][] columns    // columns[列索引][行索引]，每列是独立 Object[]
  int        rowCount   // 本批分配的行容量
  int[]      selection  // 选择向量：null = 全行有效（dense）
  int        selSize    // 选择向量中的有效条目数
```

**选择向量机制**是 `FilterOperator` 零拷贝的关键：

```
Scan 产出（dense）:                    Filter 后（sparse）:
  columns[0] = [7499, 7521, 7654]        columns[0] = [7499, 7521, 7654]  ← 数组不变
  columns[2] = [SALESMAN, SALESMAN, ...]  columns[2] = [SALESMAN, ...]     ← 数组不变
  selection  = null                      selection  = [0, 1, 2]
  rowCount   = 3                         selSize    = 3
```

Filter 只写 `selection[]`，不移动、不复制列数组。

**`activeCount()`** 统一了 dense/sparse 两种状态下的行数：
```java
public int activeCount() {
    return isDense() ? rowCount : selSize;
}
```

---

## 4. 两个核心接口

### 4.1 Source — 驱动者

**文件**：`src/main/java/github/xiedeyantu/pipeline/Source.java`

```java
public interface Source {
    void execute();  // 读完所有数据，逐批 push 给 downstream，最后调用 done()
}
```

只有 `ScanOperator` 实现此接口。**整个流水线由 `rootSource.execute()` 这一次调用触发**，数据从 Scan 出发，经过每个 Consumer 算子，最终进入 `ResultCollector`。

### 4.2 Consumer — 被动接收者

**文件**：`src/main/java/github/xiedeyantu/pipeline/Consumer.java`

```java
public interface Consumer {
    void consume(ColumnBatch batch);  // 接收一批数据并处理
    void done();                      // 上游数据耗尽，pipeline-breaker 在此触发输出
}
```

`FilterOperator`、`ProjectOperator`、`HashJoinOperator`、`SortOperator`、`ResultCollector` 均实现此接口。

---

## 5. 算子实现

### 5.1 ScanOperator（Source）

**文件**：`src/main/java/github/xiedeyantu/pipeline/operators/ScanOperator.java`

- 读取 `BaseTable.getData()` 返回的 `List<Object[]>`（行存）
- 每批最多 `BATCH_SIZE = 4096` 行，转置为列存 `ColumnBatch`
- 逐批调用 `downstream.consume(batch)`，全部发完后调用 `downstream.done()`

```java
// 核心循环
while (cursor < total) {
    int end = Math.min(cursor + BATCH_SIZE, total);
    ColumnBatch batch = new ColumnBatch(colCount, end - cursor);
    for (int r = 0; r < batchRows; r++) {
        Object[] row = rowData.get(cursor + r);
        for (int c = 0; c < colCount; c++) {
            batch.columns[c][r] = row[c];  // 行→列转置
        }
    }
    downstream.consume(batch);
    cursor = end;
}
downstream.done();
```

### 5.2 FilterOperator（Consumer，非 Breaker）

**文件**：`src/main/java/github/xiedeyantu/pipeline/operators/FilterOperator.java`

- 调用 `Predicate.eval(batch, outSel)` 填充选择向量
- 把 `batch.selection` 和 `batch.selSize` 原地更新（**不创建新 batch，不拷贝列数据**）
- 如果整批被过滤（`selSize == 0`），直接 return，不 push 给下游

### 5.3 ProjectOperator（Consumer，非 Breaker）

**文件**：`src/main/java/github/xiedeyantu/pipeline/operators/ProjectOperator.java`

- 对每个输出表达式 `Expr.eval(batch, rowIdx)` 逐行求值
- 产出一个新的 **dense `ColumnBatch`**（选择向量在此处被"解开"，输出永远是 dense）
- 列数 = 投影表达式个数

### 5.4 HashJoinOperator（Consumer，Build 侧是 Pipeline Breaker）

**文件**：`src/main/java/github/xiedeyantu/pipeline/operators/HashJoinOperator.java`

内部有两个阶段，由 `FusionHashJoin.implement()` 驱动：

**Build 阶段（Breaker）**——在 `FusionHashJoin.implement()` 内同步完成：
```java
// FusionHashJoin.implement()
Source buildSource = implementor.buildPipeline(getRight(), joinOp.buildConsumer());
buildSource.execute();   // ← 整个右侧管道在这里同步跑完，哈希表建好
```
`buildConsumer()` 是一个匿名 `Consumer`，把每批的每行物化为 `Object[]` 存入 `buildTable`（`HashMap<Object, List<Object[]>>`）。

**Probe 阶段（流式）**——返回左侧 `Source`，由外部 `rootSource.execute()` 驱动：
```java
return implementor.buildPipeline(getLeft(), joinOp);  // joinOp 作为 probe 侧 Consumer
```
每次 `HashJoinOperator.consume(probeBatch)` 用 `probeKey` 查哈希表，把匹配行拼接后 push 给下游。

### 5.5 SortOperator（Consumer，Pipeline Breaker）

**文件**：`src/main/java/github/xiedeyantu/pipeline/operators/SortOperator.java`

- `consume()`：把所有活跃行物化到 `List<Object[]> buffer`（等待期，不输出）
- `done()`：触发排序（`buffer.sort(buildComparator())`），然后分批 push 给下游，最后调用 `downstream.done()`

`done()` 是 pipeline 接力的核心机制——`ScanOperator.done()` → `FilterOperator.done()` → `HashJoinOperator.done()` → `SortOperator.done()`，**`done()` 的传播链就是 pipeline 从流式变为"阻塞触发输出"的节点**。

### 5.6 ResultCollector（Consumer，终端）

**文件**：`src/main/java/github/xiedeyantu/pipeline/operators/ResultCollector.java`

- `consume()`：把每批活跃行还原为 `Object[]` 追加到 `results` 列表
- `done()`：空实现
- 调用 `getResults()` 取回最终结果

---

## 6. Pipeline 完整执行时序

以当前 Demo 查询为例：

```sql
SELECT e.empno, e.ename, e.job, e.sal, d.dname, d.loc
FROM emp e JOIN dept d ON e.deptno = d.deptno
WHERE e.job = 'SALESMAN'
ORDER BY e.sal DESC
```

**Hep 优化后的逻辑计划：**
```
LogicalSort
  LogicalProject
    LogicalJoin
      LogicalFilter(job='SALESMAN')   ← filter 下推到 emp 侧
        EnumerableTableScan(emp)
      EnumerableTableScan(dept)
```

**FusionConvention 物理计划（Volcano 转换后）：**
```
FusionSort
  FusionProject
    FusionHashJoin
      FusionFilter               ← 左侧 probe
        FusionTableScan(emp)
      FusionTableScan(dept)      ← 右侧 build
```

**`FusionImplementor.buildPipeline()` 自顶向下装配算子链的顺序：**

```
buildPipeline(FusionSort,    downstream=ResultCollector)
  → new SortOperator(downstream=ResultCollector)
  → buildPipeline(FusionProject, downstream=SortOperator)
      → new ProjectOperator(downstream=SortOperator)
      → buildPipeline(FusionHashJoin, downstream=ProjectOperator)
          → new HashJoinOperator(downstream=ProjectOperator)
          → [Build 阶段立即同步执行]
            buildPipeline(FusionTableScan(dept), downstream=buildConsumer)
              → new ScanOperator(dept, downstream=buildConsumer)
              → ScanOperator.execute()   ← dept 全表推入 hashTable
          → [Build 完成，hashTable 就绪]
          → buildPipeline(FusionFilter, downstream=HashJoinOperator)
              → new FilterOperator(downstream=HashJoinOperator)
              → buildPipeline(FusionTableScan(emp), downstream=FilterOperator)
                  → new ScanOperator(emp, downstream=FilterOperator)
                  → return ScanOperator   ← 这是最终返回的 rootSource
```

**`rootSource.execute()` 触发后的数据流（推流方向 →）：**

```
时间线 ──────────────────────────────────────────────────────────────────►

[Build 阶段，同步完成，probe 尚未开始]
ScanOperator(dept).execute()
  → batch{10,ACCOUNTING,NY}  → buildConsumer.consume()  → hashTable[10]={...}
  → batch{20,RESEARCH,...}   → buildConsumer.consume()  → hashTable[20]={...}
  → batch{30,SALES,...}      → buildConsumer.consume()  → hashTable[30]={...}
  → batch{40,OPERATIONS,...} → buildConsumer.consume()  → hashTable[40]={...}
  → buildConsumer.done()     // 哈希表构建完毕

[Probe 阶段，由 rootSource.execute() 驱动]
ScanOperator(emp).execute()
  ──push──► FilterOperator.consume(batch)       // job='SALESMAN' → 更新 selection[]
  ──push──► HashJoinOperator.consume(batch)     // deptno 查哈希表，拼接 dept 列
  ──push──► ProjectOperator.consume(batch)      // 取 empno,ename,job,sal,dname,loc
  ──push──► SortOperator.consume(batch)         // 追加到 buffer，暂不输出

ScanOperator(emp).done()
  ──done──► FilterOperator.done()
  ──done──► HashJoinOperator.done()
  ──done──► ProjectOperator.done()
  ──done──► SortOperator.done()
               buffer.sort(SAL DESC)             // pipeline breaker 触发排序
               ──push──► ResultCollector.consume()  // 输出有序结果
               ──done──► ResultCollector.done()
```

**pipeline 与 Volcano Pull 的核心对比：**

| 行为 | 传统 Volcano Pull | 本引擎 Push Pipeline |
|---|---|---|
| 控制流方向 | 上层拉（`next()`） | 下层推（`consume()`） |
| 每批行数 | 1 行 | 最多 4096 行 |
| filter 数据操作 | 跳过行，新分配 Object[] | 更新 `selection[]`，不复制列数组 |
| project 后数据 | 每行 new Object[] | 新 dense ColumnBatch，无中间行对象 |
| join build | Janino 代码生成 | `buildSource.execute()` 同步运行 |
| sort 触发时机 | 上层调用 `next()` 时 | `done()` 传播到 `SortOperator` 时 |
| 算子间虚调用 | 每行触发一次调用链 | 每 4096 行触发一次 `consume()` |

---

## 7. 表达式层：RexToExpr

**文件**：`src/main/java/github/xiedeyantu/pipeline/expr/RexToExpr.java`

把 Calcite 的 `RexNode` 树翻译成两类可执行对象：

- **`Expr`**：`Object eval(ColumnBatch batch, int rowIdx)` — 单行标量求值，用于 `ProjectOperator`
- **`Predicate`**：`int eval(ColumnBatch batch, int[] outSel)` — 批量过滤，写选择向量，用于 `FilterOperator`

支持的 `RexNode` 类型：

| 类型 | 覆盖内容 |
|---|---|
| `RexInputRef` | 列引用 |
| `RexLiteral` | INTEGER / BIGINT / DOUBLE / VARCHAR / BOOLEAN 常量 |
| `RexCall` | `=` `<>` `>` `>=` `<` `<=` `AND` `OR` `NOT` `IS NULL` `IS NOT NULL` `+` `-` `*` `CAST` |

---

## 8. Calcite Convention 集成

### 8.1 FusionConvention

**文件**：`src/main/java/github/xiedeyantu/pipeline/FusionConvention.java`

枚举单例，标识"本引擎可以执行此节点"。关键设置：
```java
useAbstractConvertersForConversion() → true
// 允许 Volcano 插入 AbstractConverter 过渡节点，再由 FusionXxxRule 替换
```

### 8.2 FusionRel

**文件**：`src/main/java/github/xiedeyantu/pipeline/FusionRel.java`

所有物理 `RelNode` 的接口：
```java
Source implement(FusionImplementor implementor, Consumer downstream);
```
`downstream` 作为参数传入，在递归装配时自然形成算子链，无需额外图遍历。

### 8.3 FusionRules（5 条转换规则）

**文件**：`src/main/java/github/xiedeyantu/pipeline/rules/FusionRules.java`

| 规则 | 输入 Convention | 产出节点 | 备注 |
|---|---|---|---|
| `FusionTableScanRule` | `ENUMERABLE` | `FusionTableScan` | BaseTable.toRel() 产出 EnumerableTableScan |
| `FusionFilterRule` | `NONE` | `FusionFilter` | |
| `FusionProjectRule` | `NONE` | `FusionProject` | |
| `FusionHashJoinRule` | `NONE` | `FusionHashJoin` | 仅 inner equi-join |
| `FusionSortRule` | `NONE` | `FusionSort` | |

---

## 9. 文件结构

```
src/main/java/github/xiedeyantu/
├── PipelineDemo.java
└── pipeline/
    ├── ColumnBatch.java          列式批数据 + 选择向量
    ├── Consumer.java             push 接口（consume + done）
    ├── Source.java               驱动接口（execute）
    ├── FusionConvention.java     Convention 枚举
    ├── FusionRel.java            物理算子接口
    ├── FusionImplementor.java    递归装配算子链
    ├── PipelineExecutor.java     Calcite Planner 集成 + 执行入口
    ├── operators/
    │   ├── ScanOperator.java     Source  — 行存转列批，驱动数据流
    │   ├── FilterOperator.java   Consumer — 选择向量（零拷贝）
    │   ├── ProjectOperator.java  Consumer — 列式投影（dense 输出）
    │   ├── HashJoinOperator.java Consumer — Build/Probe 两阶段
    │   ├── SortOperator.java     Consumer — Pipeline Breaker（缓冲→排序→输出）
    │   └── ResultCollector.java  Consumer — 终端收集器
    ├── expr/
    │   ├── Expr.java             标量表达式接口
    │   ├── Predicate.java        批量过滤接口
    │   └── RexToExpr.java        RexNode → Expr/Predicate 解释器
    ├── rel/
    │   ├── FusionTableScan.java
    │   ├── FusionFilter.java
    │   ├── FusionProject.java
    │   ├── FusionHashJoin.java   implement() 内同步执行 build 侧
    │   └── FusionSort.java
    └── rules/
        └── FusionRules.java      5 条 ConverterRule
```
