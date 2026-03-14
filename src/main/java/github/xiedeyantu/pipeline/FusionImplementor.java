package github.xiedeyantu.pipeline;

import org.apache.calcite.rel.RelNode;

/**
 * Walks a physical {@link FusionRel} tree top-down and builds the corresponding
 * push-pipeline operator tree bottom-up.
 *
 * <p>The implementor is stateless by design — all node-specific logic lives in
 * each {@link FusionRel} implementation. {@link #buildPipeline} is just the
 * single recursive dispatch point.
 */
public class FusionImplementor {

    /**
     * Build the push-pipeline operator rooted at {@code root}, wired to
     * {@code downstream}.
     *
     * @param root       a physical {@link FusionRel} node
     * @param downstream consumer that receives output batches from {@code root}
     * @return the {@link Source} that drives execution for this sub-tree
     */
    public Source buildPipeline(RelNode root, Consumer downstream) {
        if (!(root instanceof FusionRel)) {
            throw new IllegalStateException(
                    "Expected a FusionRel node but got: "
                            + root.getClass().getName()
                            + "\nNode: " + root);
        }
        return ((FusionRel) root).implement(this, downstream);
    }
}
