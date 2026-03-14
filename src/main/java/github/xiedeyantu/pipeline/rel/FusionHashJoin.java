package github.xiedeyantu.pipeline.rel;

import com.google.common.collect.ImmutableList;
import github.xiedeyantu.pipeline.Consumer;
import github.xiedeyantu.pipeline.FusionConvention;
import github.xiedeyantu.pipeline.FusionImplementor;
import github.xiedeyantu.pipeline.FusionRel;
import github.xiedeyantu.pipeline.Source;
import github.xiedeyantu.pipeline.operators.HashJoinOperator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.Set;

/**
 * Physical inner hash-join node in {@link FusionConvention}.
 *
 * <p>This node is a <strong>pipeline breaker</strong> on the build (right) side:
 * during {@link #implement} the entire right-side pipeline is executed eagerly
 * to populate the hash table, <em>before</em> the probe (left) side source is
 * returned to the caller.
 *
 * <p>Output schema: {@code [left-cols..., right-cols...]}
 */
public class FusionHashJoin extends Join implements FusionRel {

    public FusionHashJoin(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
        super(cluster, traits.replace(FusionConvention.INSTANCE),
                ImmutableList.of(), left, right, condition, variablesSet, joinType);
    }

    @Override
    public Join copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new FusionHashJoin(
                getCluster(), traitSet, left, right, conditionExpr,
                getVariablesSet(), joinType);
    }

    @Override
    public Source implement(FusionImplementor implementor, Consumer downstream) {
        JoinInfo joinInfo = analyzeCondition();
        if (joinInfo.leftKeys.isEmpty()) {
            throw new UnsupportedOperationException(
                    "FusionHashJoin only supports equi-joins, got: " + getCondition());
        }

        // Take only the first key pair (multi-key could be added similarly)
        int leftKeyIdx  = joinInfo.leftKeys.get(0);
        int rightKeyIdx = joinInfo.rightKeys.get(0);
        int leftColCount  = getLeft().getRowType().getFieldCount();
        int rightColCount = getRight().getRowType().getFieldCount();

        HashJoinOperator joinOp = new HashJoinOperator(
                leftKeyIdx, rightKeyIdx, leftColCount, rightColCount, downstream);

        // ── Build phase (pipeline breaker) ──────────────────────────────────
        // Execute the right side fully so the hash table is ready before probing.
        Source buildSource = implementor.buildPipeline(getRight(), joinOp.buildConsumer());
        buildSource.execute();

        // ── Probe phase ──────────────────────────────────────────────────────
        // Return the left-side source; its execution will probe the hash table.
        return implementor.buildPipeline(getLeft(), joinOp);
    }
}
