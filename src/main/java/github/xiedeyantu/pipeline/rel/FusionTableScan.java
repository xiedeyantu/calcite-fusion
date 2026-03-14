package github.xiedeyantu.pipeline.rel;

import com.google.common.collect.ImmutableList;
import github.xiedeyantu.pipeline.Consumer;
import github.xiedeyantu.pipeline.FusionConvention;
import github.xiedeyantu.pipeline.FusionImplementor;
import github.xiedeyantu.pipeline.FusionRel;
import github.xiedeyantu.pipeline.Source;
import github.xiedeyantu.pipeline.operators.ScanOperator;
import github.xiedeyantu.tables.BaseTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.Table;

import java.util.List;

/**
 * Physical {@link TableScan} node in {@link FusionConvention}.
 *
 * <p>Creates a {@link ScanOperator} that reads table data and pushes columnar
 * batches to the downstream consumer.
 */
public class FusionTableScan extends TableScan implements FusionRel {

    public FusionTableScan(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelOptTable table) {
        super(cluster, traits.replace(FusionConvention.INSTANCE), ImmutableList.of(), table);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new FusionTableScan(getCluster(), traitSet, getTable());
    }

    @Override
    public Source implement(FusionImplementor implementor, Consumer downstream) {
        Table table = getTable().unwrap(Table.class);
        if (!(table instanceof BaseTable)) {
            throw new IllegalStateException(
                    "FusionTableScan requires a BaseTable, got: "
                            + (table == null ? "null" : table.getClass().getName()));
        }
        BaseTable baseTable = (BaseTable) table;
        int colCount = getRowType().getFieldCount();
        return new ScanOperator(baseTable.getData(), colCount, downstream);
    }
}
