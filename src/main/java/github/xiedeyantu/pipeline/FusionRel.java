package github.xiedeyantu.pipeline;

import org.apache.calcite.rel.RelNode;

/**
 * Marker interface for physical {@link RelNode} nodes in the
 * {@link FusionConvention} calling convention.
 *
 * <p>Each implementing class covers one logical operator
 * (Scan, Filter, Project, HashJoin, Sort) and knows how to produce the
 * corresponding {@link Source} when asked by the {@link FusionImplementor}.
 */
public interface FusionRel extends RelNode {

    /**
     * Build the push-pipeline operator for this node and wire it to
     * {@code downstream}.
     *
     * <ul>
     *   <li>For leaf nodes (TableScan) this returns a new {@link Source}.</li>
     *   <li>For inner nodes, the implementation creates the operator, sets
     *       {@code downstream} as its consumer, then recurses into its input(s)
     *       returning the {@link Source} produced by the deepest scan.</li>
     *   <li>Pipeline-breakers (HashJoin build side, Sort) are handled eagerly:
     *       the build/blocking phase runs immediately inside this call before
     *       the probe source is returned.</li>
     * </ul>
     *
     * @param implementor carries shared context (table registry, etc.)
     * @param downstream  the consumer that will receive batches from this node
     * @return the {@link Source} that drives the whole sub-pipeline
     */
    Source implement(FusionImplementor implementor, Consumer downstream);
}
