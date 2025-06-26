package github.xiedeyantu;

import github.xiedeyantu.rules.EnumRules;
import github.xiedeyantu.schema.MySchema;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.tools.ValidationException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Executer {
    private static Planner planner;

    private static void initPlanner(RuleSet logicalRuleSet, RuleSet physicalRuleSet) throws SQLException {
        // 创建Calcite连接
        Connection conn = DriverManager.getConnection("jdbc:calcite:", new Properties());
        CalciteConnection calciteConnection = conn.unwrap(CalciteConnection.class);
        // 获取根Schema对象
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        // 向Schema注册表
        rootSchema.add("test", new MySchema());
        // 构建Calcite的配置
        List<Program> programs = new ArrayList<>();
        if (logicalRuleSet != null) {
            programs.add(Programs.hep(logicalRuleSet, true, DefaultRelMetadataProvider.INSTANCE));
        }
        if (physicalRuleSet != null) {
            programs.add(Programs.of(physicalRuleSet));
        }
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema("test")) // 设置默认Schema
                .parserConfig(SqlParser.config().withCaseSensitive(false)) // SQL解析不区分大小写
                .programs(programs)
                .build();
        planner = Frameworks.getPlanner(config);
    }

    public static RelNode sqlToRel(String sql, Planner planner) throws Exception {
        // 解析SQL
        SqlNode parse = planner.parse(sql);
        // 校验SQL
        SqlNode validate = planner.validate(parse);
        // SQL转关系代数树
        return planner.rel(validate).rel;
    }

    public static void printLogicalPlan(String sql, RuleSet rules) throws Exception {
        System.out.println("=== SQL ===\n" + sql + "\n");
        initPlanner(rules, null);
        RelNode initialRel = sqlToRel(sql, planner);
        System.out.println("=== Init Plan ===\n" + RelOptUtil.toString(initialRel));

        RelNode transform = planner.transform(0, null, initialRel);
        System.out.println("=== Hep Plan ===\n" + RelOptUtil.toString(transform));
    }

    public static void printPhysicalPlan(String sql, RuleSet logicalRules, RuleSet physicalRules) throws Exception {
        System.out.println("=== SQL ===\n" + sql + "\n");
        initPlanner(logicalRules, physicalRules);
        RelNode initialRel = sqlToRel(sql, planner);
        System.out.println("=== Init Plan ===\n" + RelOptUtil.toString(initialRel));

        RelNode transform = planner.transform(0, null, initialRel);
        System.out.println("=== Hep Plan ===\n" + RelOptUtil.toString(transform));

        RelTraitSet traitSet = initialRel.getTraitSet().replace(EnumerableConvention.INSTANCE);
        transform = planner.transform(1, traitSet, initialRel);
        System.out.println("=== Volcano Plan ===\n" + RelOptUtil.toString(transform));
    }

    public static void main(String[] args) throws Exception {
        // 示例SQL查询
        String sql = "SELECT e.empid, e.name, e.salary, d.name as dept_name " +
                "FROM emps e " +
                "JOIN depts d ON e.deptno = d.deptno " +
                "WHERE d.deptno = 10 " +
                "ORDER BY e.salary DESC";

        RuleSet ruleSet0 = RuleSets.ofList(
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.CALC_MERGE);

        printLogicalPlan(sql, ruleSet0);
        printPhysicalPlan(sql, ruleSet0, EnumRules.ENUMERABLE_RULES);
    }
}
