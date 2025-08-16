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

public class EmpsTable extends BaseTable {
  public EmpsTable() {
    super.data = Arrays.asList(
        new Object[]{100, 10, "Bill", 10000, 1000},
        new Object[]{110, 10, "Theodore", 11500, 250},
        new Object[]{150, 10, "Sebastian", 7000, null},
        new Object[]{200, 20, "Eric", 8000, 500});
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    // 创建基本类型
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 50);

    // 创建可为空的commission类型
    RelDataType nullableIntType = typeFactory.createTypeWithNullability(intType, true);

    return typeFactory.builder()
        .add("EMPID", intType)
        .add("DEPTNO", intType)
        .add("NAME", varcharType)
        .add("SALARY", intType)
        .add("COMMISSION", nullableIntType)  // 使用可为空的类型
        .build();
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

      @Override
      public @Nullable RelDistribution getDistribution()  {
        return RelDistributions.hash(ImmutableList.of(0));
      }

      @Override
      public @Nullable List<RelCollation> getCollations()  {
        return ImmutableList.of();
      }
    };
  }
}
