package github.xiedeyantu.pipeline.rel;

import com.google.common.collect.ImmutableList;
import github.xiedeyantu.pipeline.Consumer;
import github.xiedeyantu.pipeline.FusionConvention;
import github.xiedeyantu.pipeline.FusionImplementor;
import github.xiedeyantu.pipeline.FusionRel;
import github.xiedeyantu.pipeline.Source;
import github.xiedeyantu.pipeline.expr.Expr;
import github.xiedeyantu.pipeline.expr.RexToExpr;
import github.xiedeyantu.pipeline.operators.ProjectOperator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Physical projection node in {@link FusionConvention}.
 *
 * <p>Evaluates the output expressions column-by-column, producing a dense
 * batch with only the projected columns.
 */
public class FusionProject extends Project implements FusionRel {

    public FusionProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits.replace(FusionConvention.INSTANCE),
                ImmutableList.of(), input, projects, rowType);
    }

    @Override
    public Project copy(
            RelTraitSet traitSet,
            RelNode input,
            List<RexNode> projects,
            RelDataType rowType) {
        return new FusionProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public Source implement(FusionImplementor implementor, Consumer downstream) {
        List<Expr> exprs = getProjects().stream()
                .map(RexToExpr::toExpr)
                .collect(Collectors.toList());
        ProjectOperator projectOp = new ProjectOperator(exprs, downstream);
        return implementor.buildPipeline(getInput(), projectOp);
    }
}
