package github.xiedeyantu;

import github.xiedeyantu.rules.EnumRules;
import github.xiedeyantu.schema.MySchema;
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.stream.Collectors;

public class CalciteDemo {
    public static void main(String[] args) throws Exception {
        // 定义第一个规则集，主要包含Calcite核心规则
        RuleSet ruleSet0 = RuleSets.ofList(
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.CALC_MERGE);

        // 定义第二个规则集，主要包含Enumerable相关规则
        RuleSet physicalRuleSet = RuleSets.ofList(EnumRules.ENUMERABLE_RULES);

        // 创建Calcite连接
        Connection conn = DriverManager.getConnection("jdbc:calcite:", new Properties());
        CalciteConnection calciteConnection = conn.unwrap(CalciteConnection.class);
        // 获取根Schema对象
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        // 向Schema注册表
        rootSchema.add("testSchema", new MySchema());
        // 构建Calcite的配置
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema("testSchema")) // 设置默认Schema
                .parserConfig(SqlParser.config().withCaseSensitive(false)) // SQL解析不区分大小写
                // 配置两个优化程序：hep和volcano
                .programs(
                        Programs.hep(ruleSet0, true, DefaultRelMetadataProvider.INSTANCE),
                        Programs.of(physicalRuleSet))
                .build();
        Planner planner = Frameworks.getPlanner(config);

        // SQL查询语句
        String sql = "SELECT e.empid, e.name, e.salary, d.name as dept_name " +
                "FROM emps e " +
                "JOIN depts d ON e.deptno = d.deptno " +
                "WHERE d.deptno = 10 " +
                "ORDER BY e.salary DESC";
        System.out.println("=== SQL ===\n" + sql);
        // 解析SQL
        SqlNode parse = planner.parse(sql);
        // 校验SQL
        SqlNode validate = planner.validate(parse);
        // SQL转关系代数树
        RelNode convert = planner.rel(validate).rel;
        // 为关系树设置Enumerable物理属性
        RelTraitSet traitSet = convert.getTraitSet()
                .replace(EnumerableConvention.INSTANCE);

        // 使用hep优化器进行第一次优化（应用ruleSet0）
        RelNode transform = planner.transform(0, traitSet, convert);
        System.out.println("=== Hep Plan ===\n" + RelOptUtil.toString(transform));

        // 使用volcano优化器进行第二次优化（应用ruleSet1）
        RelNode transform2 = planner.transform(1, traitSet, transform);
        System.out.println("=== Volcano Plan ===\n" + RelOptUtil.toString(transform2));

        // 将优化后的关系树转为可执行的Bindable对象
        Bindable bindable = EnumerableInterpretable.toBindable(
                Collections.emptyMap(),
                CalcitePrepare.Dummy.getSparkHandler(false),
                (EnumerableRel) transform2,
                EnumerableRel.Prefer.ARRAY
        );

        // 绑定数据并获取结果集枚举器
        Enumerator<Object[]> enumerator =
                bindable.bind(DataContexts.of(calciteConnection, rootSchema))
                        .enumerator();

        // 打印查询结果的表头
        RelDataType rowType = transform2.getRowType();
        String header = rowType.getFieldList().stream()
                .map(RelDataTypeField::getName)
                .collect(Collectors.joining("\t"));
        System.out.println("=== Query Result ===\n" + header);

        // 遍历并打印每一行查询结果
        while (enumerator.moveNext()) {
            Object[] row = enumerator.current();
            for (Object col : row) {
                System.out.print(col + "\t");
            }
            System.out.println();
        }

        // 关闭枚举器
        enumerator.close();
    }

    // 定义内存Schema，包含一个mytable表
    public static class DataSchema {
        // mytable表，包含三行数据
        public final MyTable[] mytable = {
                new MyTable(1, "Alice"),
                new MyTable(2, "Bob"),
                new MyTable(3, "Carol"),
        };
    }
    // mytable表的结构定义
    public static class MyTable {
        public final int id;
        public final String name;
        public MyTable(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
