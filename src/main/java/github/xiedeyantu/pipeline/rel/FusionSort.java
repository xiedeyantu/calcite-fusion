package github.xiedeyantu.pipeline.rel;

import github.xiedeyantu.pipeline.Consumer;
import github.xiedeyantu.pipeline.FusionConvention;
import github.xiedeyantu.pipeline.FusionImplementor;
import github.xiedeyantu.pipeline.FusionRel;
import github.xiedeyantu.pipeline.Source;
import github.xiedeyantu.pipeline.operators.SortOperator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.math.BigDecimal;

/**
 * Physical sort node in {@link FusionConvention}.
 *
 * <p>This node is a <strong>pipeline breaker</strong>: the entire input stream
 * is buffered, sorted, and only then pushed downstream (inside
 * {@link SortOperator#done()}).
 */
public class FusionSort extends Sort implements FusionRel {

    public FusionSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RelCollation collation,
            RexNode offset,
            RexNode fetch) {
        super(cluster, traits.replace(FusionConvention.INSTANCE),
                input, collation, offset, fetch);
    }

    @Override
    public Sort copy(
            RelTraitSet traitSet,
            RelNode newInput,
            RelCollation newCollation,
            RexNode offset,
            RexNode fetch) {
        return new FusionSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }

    @Override
    public Source implement(FusionImplementor implementor, Consumer downstream) {
        int fetchCount = extractFetch();
        SortOperator sortOp = new SortOperator(
                getCollation().getFieldCollations(), fetchCount, downstream);
        return implementor.buildPipeline(getInput(), sortOp);
    }

    /** Returns the FETCH limit, or {@code -1} if there is none. */
    private int extractFetch() {
        if (fetch instanceof RexLiteral) {
            Object val = ((RexLiteral) fetch).getValue();
            if (val instanceof BigDecimal) {
                return ((BigDecimal) val).intValue();
            }
        }
        return -1;
    }
}
