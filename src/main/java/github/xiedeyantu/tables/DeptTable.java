package github.xiedeyantu.tables;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;

public class DeptTable extends BaseTable {
    public DeptTable() {
        super.data = Arrays.asList(
            new Object[]{10, "ACCOUNTING", "NEW YORK"},
            new Object[]{20, "RESEARCH",   "DALLAS"},
            new Object[]{30, "SALES",      "CHICAGO"},
            new Object[]{40, "OPERATIONS", "BOSTON"}
        );
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("DEPTNO", SqlTypeName.INTEGER)
            .add("DNAME",  SqlTypeName.VARCHAR, 20)
            .add("LOC",    SqlTypeName.VARCHAR, 20)
            .build();
    }

    @Override
    public Statistic getStatistic() {
        return new Statistic() {
            @Override public @Nullable List<ImmutableBitSet> getKeys() {
                return ImmutableList.of(ImmutableBitSet.of(0));
            }
            @Override public boolean isKey(ImmutableBitSet columns) {
                return getKeys().contains(columns);
            }
            @Override public @Nullable Double getRowCount() {
                return (double) data.size();
            }
            @Override public @Nullable RelDistribution getDistribution() {
                return RelDistributions.hash(ImmutableList.of(0));
            }
            @Override public @Nullable List<RelCollation> getCollations() {
                return ImmutableList.of();
            }
        };
    }
}
