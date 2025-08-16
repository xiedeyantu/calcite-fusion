package github.xiedeyantu.tables;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;

public class BaseTable extends AbstractTable implements ScannableTable, TranslatableTable {
  protected List<Object[]> data;

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return null;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    return Linq4j.asEnumerable(data);
  }

  @Override
  public Statistic getStatistic() {
    return null;
  }

  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    return EnumerableTableScan.create(context.getCluster(), relOptTable);
  }
}
