package github.xiedeyantu.pipeline;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/**
 * Calling convention for the push-based pipeline execution engine.
 *
 * <p>Any physical {@link RelNode} in this convention must implement
 * {@link FusionRel}. The Volcano planner converts the logical plan to this
 * convention via the rules in
 * {@link github.xiedeyantu.pipeline.rules.FusionRules}.
 */
public enum FusionConvention implements Convention {
    INSTANCE;

    @Override
    public Class<? extends RelNode> getInterface() {
        return FusionRel.class;
    }

    @Override
    public String getName() {
        return "FUSION";
    }

    @Override
    public RelTraitDef getTraitDef() {
        return ConventionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait trait) {
        return this == trait;
    }

    @Override
    public void register(RelOptPlanner planner) {
    }

    @Override
    public String toString() {
        return getName();
    }

    /** No enforcement (no sort/exchange insertion needed for this engine). */
    @Override
    public RelNode enforce(RelNode input, RelTraitSet required) {
        return null;
    }

    @Override
    public boolean canConvertConvention(Convention toConvention) {
        return false;
    }

    /**
     * Return true so the Volcano planner can insert AbstractConverter nodes
     * that are later replaced by the FusionXxx conversion rules.
     */
    @Override
    public boolean useAbstractConvertersForConversion(
            RelTraitSet fromTraits, RelTraitSet toTraits) {
        return true;
    }
}
