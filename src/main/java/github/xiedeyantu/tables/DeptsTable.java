package github.xiedeyantu.tables;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DeptsTable extends AbstractTable implements ScannableTable {
    // 表数据
    private final List<Object[]> data = Arrays.asList(
            new Object[]{10, "Sales"},
            new Object[]{30, "Marketing"},
            new Object[]{40, "HR"}
    );

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
                .add("DEPTNO", SqlTypeName.INTEGER)
                .add("NAME", SqlTypeName.VARCHAR, 50)
                .build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(data);
    }

    @Override
    public Statistic getStatistic() {
      return new Statistic() {
        @Override
        public @Nullable List<ImmutableBitSet> getKeys() {
            return ImmutableList.of(ImmutableBitSet.of(0));
        }

        @Override
        public boolean isKey(ImmutableBitSet columns) {
            return getKeys().contains(columns);
        }

        @Override
        public @Nullable Double getRowCount() {
            return (double) data.size();
        }
      };
    }
}