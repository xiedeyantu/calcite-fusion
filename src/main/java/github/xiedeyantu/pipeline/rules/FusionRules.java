package github.xiedeyantu.pipeline.rules;

import github.xiedeyantu.pipeline.FusionConvention;
import github.xiedeyantu.pipeline.rel.FusionFilter;
import github.xiedeyantu.pipeline.rel.FusionHashJoin;
import github.xiedeyantu.pipeline.rel.FusionProject;
import github.xiedeyantu.pipeline.rel.FusionSort;
import github.xiedeyantu.pipeline.rel.FusionTableScan;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

/**
 * Conversion rules that translate logical {@link RelNode} trees
 * ({@link Convention#NONE}) into physical {@link FusionConvention} trees.
 *
 * <p>Register {@link #FUSION_PHYSICAL_RULES} as the physical rule set in the
 * Volcano planner program (program index 1).
 */
public final class FusionRules {

    private FusionRules() {}

    /** Rule set to pass to {@code Programs.of(…)} for the Volcano phase. */
    public static final RuleSet FUSION_PHYSICAL_RULES = RuleSets.ofList(
            FusionTableScanRule.INSTANCE,
            FusionFilterRule.INSTANCE,
            FusionProjectRule.INSTANCE,
            FusionHashJoinRule.INSTANCE,
            FusionSortRule.INSTANCE
    );

    // -----------------------------------------------------------------------
    // TableScan (EnumerableTableScan) → FusionTableScan
    //
    // BaseTable.toRel() produces an EnumerableTableScan (ENUMERABLE convention),
    // not a LogicalTableScan, so the input convention must be ENUMERABLE.
    // -----------------------------------------------------------------------

    public static class FusionTableScanRule extends ConverterRule {

        static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(TableScan.class,
                        EnumerableConvention.INSTANCE,   // input: ENUMERABLE
                        FusionConvention.INSTANCE,        // output: FUSION
                        "FusionTableScanRule")
                .withRuleFactory(FusionTableScanRule::new);

        public static final FusionTableScanRule INSTANCE =
                (FusionTableScanRule) DEFAULT_CONFIG.toRule();

        protected FusionTableScanRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            TableScan scan = (TableScan) rel;
            return new FusionTableScan(
                    scan.getCluster(), scan.getTraitSet(), scan.getTable());
        }
    }

    // -----------------------------------------------------------------------
    // LogicalFilter → FusionFilter
    // -----------------------------------------------------------------------

    public static class FusionFilterRule extends ConverterRule {

        static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalFilter.class, Convention.NONE,
                        FusionConvention.INSTANCE, "FusionFilterRule")
                .withRuleFactory(FusionFilterRule::new);

        public static final FusionFilterRule INSTANCE =
                (FusionFilterRule) DEFAULT_CONFIG.toRule();

        protected FusionFilterRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalFilter filter = (LogicalFilter) rel;
            RelTraitSet traits = filter.getTraitSet().replace(FusionConvention.INSTANCE);
            RelNode input = convert(filter.getInput(), FusionConvention.INSTANCE);
            if (input == null) return null;
            return new FusionFilter(
                    filter.getCluster(), traits, input, filter.getCondition());
        }
    }

    // -----------------------------------------------------------------------
    // LogicalProject → FusionProject
    // -----------------------------------------------------------------------

    public static class FusionProjectRule extends ConverterRule {

        static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalProject.class, Convention.NONE,
                        FusionConvention.INSTANCE, "FusionProjectRule")
                .withRuleFactory(FusionProjectRule::new);

        public static final FusionProjectRule INSTANCE =
                (FusionProjectRule) DEFAULT_CONFIG.toRule();

        protected FusionProjectRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalProject project = (LogicalProject) rel;
            RelTraitSet traits = project.getTraitSet().replace(FusionConvention.INSTANCE);
            RelNode input = convert(project.getInput(), FusionConvention.INSTANCE);
            if (input == null) return null;
            return new FusionProject(
                    project.getCluster(), traits, input,
                    project.getProjects(), project.getRowType());
        }
    }

    // -----------------------------------------------------------------------
    // LogicalJoin → FusionHashJoin  (inner equi-join only)
    // -----------------------------------------------------------------------

    public static class FusionHashJoinRule extends ConverterRule {

        static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalJoin.class, Convention.NONE,
                        FusionConvention.INSTANCE, "FusionHashJoinRule")
                .withRuleFactory(FusionHashJoinRule::new);

        public static final FusionHashJoinRule INSTANCE =
                (FusionHashJoinRule) DEFAULT_CONFIG.toRule();

        protected FusionHashJoinRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalJoin join = (LogicalJoin) rel;

            // Only support inner equi-joins for now
            if (join.getJoinType() != JoinRelType.INNER) return null;
            JoinInfo info = join.analyzeCondition();
            if (!info.isEqui()) return null;

            RelTraitSet traits = join.getTraitSet().replace(FusionConvention.INSTANCE);
            RelNode left  = convert(join.getLeft(),  FusionConvention.INSTANCE);
            RelNode right = convert(join.getRight(), FusionConvention.INSTANCE);
            if (left == null || right == null) return null;

            return new FusionHashJoin(
                    join.getCluster(), traits, left, right,
                    join.getCondition(), join.getVariablesSet(), join.getJoinType());
        }
    }

    // -----------------------------------------------------------------------
    // LogicalSort → FusionSort
    // -----------------------------------------------------------------------

    public static class FusionSortRule extends ConverterRule {

        static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalSort.class, Convention.NONE,
                        FusionConvention.INSTANCE, "FusionSortRule")
                .withRuleFactory(FusionSortRule::new);

        public static final FusionSortRule INSTANCE =
                (FusionSortRule) DEFAULT_CONFIG.toRule();

        protected FusionSortRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalSort sort = (LogicalSort) rel;
            RelTraitSet traits = sort.getTraitSet().replace(FusionConvention.INSTANCE);
            RelNode input = convert(sort.getInput(), FusionConvention.INSTANCE);
            if (input == null) return null;
            return new FusionSort(
                    sort.getCluster(), traits, input,
                    sort.getCollation(), sort.offset, sort.fetch);
        }
    }
}
