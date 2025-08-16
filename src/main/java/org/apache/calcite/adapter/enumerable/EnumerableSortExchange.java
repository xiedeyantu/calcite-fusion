package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.SortExchange;

public class EnumerableSortExchange
    extends SortExchange
    implements EnumerableRel {

  public EnumerableSortExchange(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RelDistribution distribution,
      RelCollation collation) {
    super(cluster, traitSet, child, distribution, collation);
    this.rowType = child.getRowType();
  }

  /** Creates an EnumerableSortExchange. */
  public static EnumerableSortExchange create(final RelNode input,
      RelDistribution distribution, RelCollation collation, Convention convention) {
    final RelOptCluster cluster = input.getCluster();
    distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution);
    RelTraitSet traitSet = cluster.traitSetOf(convention).replace(collation).replace(distribution);
    return new EnumerableSortExchange(cluster, traitSet, input, distribution, collation);
  }

  @Override
  public EnumerableSortExchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution, RelCollation newCollation) {
    return new EnumerableSortExchange(newInput.getCluster(), traitSet, newInput, newDistribution, newCollation);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw);
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    return null;
  }

  @Override
  public boolean isEnforcer() {
    return true;
  }
}
