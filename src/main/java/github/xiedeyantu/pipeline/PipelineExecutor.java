package github.xiedeyantu.pipeline;

import github.xiedeyantu.pipeline.operators.ResultCollector;
import github.xiedeyantu.pipeline.rules.FusionRules;
import github.xiedeyantu.schema.MySchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
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
import java.util.List;
import java.util.Properties;

/**
 * Entry point for the push-based pipeline execution engine.
 *
 * <p>Execution flow:
 * <ol>
 *   <li>Parse + validate SQL with Calcite.</li>
 *   <li>Apply logical optimisation rules with the Hep planner (e.g.
 *       {@link CoreRules#FILTER_INTO_JOIN} to push filters below joins).</li>
 *   <li>Convert the logical plan to a {@link FusionConvention} physical plan
 *       using the Volcano planner and {@link FusionRules}.</li>
 *   <li>Walk the physical plan with {@link FusionImplementor} to build the
 *       push-pipeline operator tree.</li>
 *   <li>Call {@link Source#execute()} on the root source to run the query.</li>
 * </ol>
 */
public class PipelineExecutor {

    /**
     * Execute {@code sql} against the demo schema and return all result rows.
     *
     * @param sql SQL statement to execute
     * @return list of result rows; each row is an {@code Object[]}
     */
    public List<Object[]> execute(String sql) throws Exception {

        // Logical optimisation: push filter into join, merge consecutive projections.
        // CALC_SPLIT must run first to decompose any LogicalCalc (produced by
        // FILTER_PROJECT_TRANSPOSE or the SQL-to-rel converter) back into a
        // LogicalProject + LogicalFilter before the Fusion physical rules see the plan.
        RuleSet logicalRules = RuleSets.ofList(
                CoreRules.CALC_SPLIT,
                CoreRules.FILTER_INTO_JOIN,
                CoreRules.FILTER_PROJECT_TRANSPOSE,
                CoreRules.PROJECT_MERGE
        );

        // Set up Calcite connection + schema
        Connection conn = DriverManager.getConnection("jdbc:calcite:", new Properties());
        CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConn.getRootSchema();
        rootSchema.add("test", new MySchema());

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema("test"))
                .parserConfig(SqlParser.config().withCaseSensitive(false))
                .programs(
                        // Program 0: Hep — logical optimisation
                        Programs.hep(logicalRules, true, DefaultRelMetadataProvider.INSTANCE),
                        // Program 1: Volcano — convert to FusionConvention
                        Programs.of(FusionRules.FUSION_PHYSICAL_RULES)
                )
                .build();

        Planner planner = Frameworks.getPlanner(config);

        // Parse → validate → logical RelNode
        SqlNode parsed   = planner.parse(sql);
        SqlNode validated = planner.validate(parsed);
        RelNode logical  = planner.rel(validated).rel;
        System.out.println("=== Initial Plan ===\n" + RelOptUtil.toString(logical));

        // Hep: logical optimisation (program 0)
        RelNode hepResult = planner.transform(
                0,
                logical.getTraitSet().replace(FusionConvention.INSTANCE),
                logical);
        System.out.println("=== After Hep Optimisation ===\n" + RelOptUtil.toString(hepResult));

        // Volcano: physical conversion to FusionConvention (program 1)
        RelNode physical = planner.transform(
                1,
                hepResult.getTraitSet().replace(FusionConvention.INSTANCE),
                hepResult);
        System.out.println("=== Fusion Physical Plan ===\n" + RelOptUtil.toString(physical));

        conn.close();

        // Build and execute the push-pipeline operator tree
        ResultCollector collector = new ResultCollector();
        FusionImplementor implementor = new FusionImplementor();
        Source rootSource = implementor.buildPipeline(physical, collector);
        rootSource.execute();   // drives data through the entire pipeline

        return collector.getResults();
    }
}
