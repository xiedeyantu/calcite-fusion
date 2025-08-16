package github.xiedeyantu;

import com.google.common.collect.ImmutableList;
import github.xiedeyantu.rules.EnumRules;
import github.xiedeyantu.schema.MySchema;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
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
import java.util.Properties;

public class CalcitePlanner {
  public static void main(String[] args) throws Exception {
    // 定义第一个规则集，主要包含Calcite核心规则
    RuleSet ruleSet0 = RuleSets.ofList();

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
        .parserConfig(SqlParser.config()
            .withCaseSensitive(false)
            .withUnquotedCasing(Casing.UNCHANGED)
            .withQuotedCasing(Casing.UNCHANGED))
        .traitDefs(ImmutableList.of(
            ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE,
            RelDistributionTraitDef.INSTANCE))
        // 配置两个优化程序：hep和volcano
        .programs(
            Programs.hep(ruleSet0, true, DefaultRelMetadataProvider.INSTANCE),
            Programs.of(physicalRuleSet))
        .build();
    Planner planner = Frameworks.getPlanner(config);

    // SQL查询语句
    String sql = "SELECT e.empid, e.NAME, e.salary, d.name as DEPT_name, Array['a', 'b'] " +
        "FROM emps e " +
        "JOIN depts d ON e.deptno = d.deptno " +
        "WHERE d.deptno = 10 " +
        "ORDER BY e.salary DESC";
    System.out.println("=== SQL ===\n" + sql);
    // 解析SQL
    SqlNode parse = planner.parse(sql);

    // SqlDialect clickhouseDialect = SqlDialect.DatabaseProduct.CLICKHOUSE.getDialect();
    //
    // // 将 SqlNode 转换为 ClickHouse SQL
    // String clickhouseSql = parse.toSqlString(clickhouseDialect).getSql();
    // System.out.println(clickhouseSql);

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
  }
}
