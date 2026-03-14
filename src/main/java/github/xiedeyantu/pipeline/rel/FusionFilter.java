package github.xiedeyantu.pipeline.rel;

import github.xiedeyantu.pipeline.Consumer;
import github.xiedeyantu.pipeline.FusionConvention;
import github.xiedeyantu.pipeline.FusionImplementor;
import github.xiedeyantu.pipeline.FusionRel;
import github.xiedeyantu.pipeline.Source;
import github.xiedeyantu.pipeline.expr.Predicate;
import github.xiedeyantu.pipeline.expr.RexToExpr;
import github.xiedeyantu.pipeline.operators.FilterOperator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

/**
 * Physical filter node in {@link FusionConvention}.
 *
 * <p>Translates the Calcite {@link RexNode} predicate into a compiled
 * {@link Predicate} and inserts a {@link FilterOperator} into the push pipeline.
 */
public class FusionFilter extends Filter implements FusionRel {

    public FusionFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RexNode condition) {
        super(cluster, traits.replace(FusionConvention.INSTANCE), input, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new FusionFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public Source implement(FusionImplementor implementor, Consumer downstream) {
        Predicate pred = RexToExpr.toPredicate(getCondition());
        FilterOperator filterOp = new FilterOperator(pred, downstream);
        return implementor.buildPipeline(getInput(), filterOp);
    }
}
