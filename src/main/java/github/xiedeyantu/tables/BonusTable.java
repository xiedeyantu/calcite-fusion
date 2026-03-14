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

import java.util.ArrayList;
import java.util.List;

public class BonusTable extends BaseTable {
    public BonusTable() {
        // ENAME, JOB, SAL, COMM — initially empty (bonus is populated by application logic)
        super.data = new ArrayList<>();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataType varcharEname = typeFactory.createSqlType(SqlTypeName.VARCHAR, 10);
        RelDataType varcharJob   = typeFactory.createSqlType(SqlTypeName.VARCHAR, 9);
        RelDataType decimalType  = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        RelDataType nullableDec  = typeFactory.createTypeWithNullability(decimalType, true);

        return typeFactory.builder()
            .add("ENAME", varcharEname)
            .add("JOB",   varcharJob)
            .add("SAL",   nullableDec)
            .add("COMM",  nullableDec)
            .build();
    }

    @Override
    public Statistic getStatistic() {
        return new Statistic() {
            @Override public @Nullable List<ImmutableBitSet> getKeys() {
                return ImmutableList.of();
            }
            @Override public boolean isKey(ImmutableBitSet columns) {
                return false;
            }
            @Override public @Nullable Double getRowCount() {
                return (double) data.size();
            }
            @Override public @Nullable RelDistribution getDistribution() {
                return RelDistributions.BROADCAST_DISTRIBUTED;
            }
            @Override public @Nullable List<RelCollation> getCollations() {
                return ImmutableList.of();
            }
        };
    }
}
