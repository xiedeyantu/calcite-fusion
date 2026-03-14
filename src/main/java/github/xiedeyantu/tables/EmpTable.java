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

public class EmpTable extends BaseTable {
    public EmpTable() {
        // EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO
        super.data = Arrays.asList(
            new Object[]{7369, "SMITH",  "CLERK",     7902, "1980-12-17",  800.00, null,    20},
            new Object[]{7499, "ALLEN",  "SALESMAN",  7698, "1981-02-20", 1600.00,  300.00, 30},
            new Object[]{7521, "WARD",   "SALESMAN",  7698, "1981-02-22", 1250.00,  500.00, 30},
            new Object[]{7566, "JONES",  "MANAGER",   7839, "1981-02-04", 2975.00, null,    20},
            new Object[]{7654, "MARTIN", "SALESMAN",  7698, "1981-09-28", 1250.00, 1400.00, 30},
            new Object[]{7698, "BLAKE",  "MANAGER",   7839, "1981-01-05", 2850.00, null,    30},
            new Object[]{7782, "CLARK",  "MANAGER",   7839, "1981-06-09", 2450.00, null,    10},
            new Object[]{7788, "SCOTT",  "ANALYST",   7566, "1987-04-19", 3000.00, null,    20},
            new Object[]{7839, "KING",   "PRESIDENT", null, "1981-11-17", 5000.00, null,    10},
            new Object[]{7844, "TURNER", "SALESMAN",  7698, "1981-09-08", 1500.00,    0.00, 30},
            new Object[]{7876, "ADAMS",  "CLERK",     7788, "1987-05-23", 1100.00, null,    20},
            new Object[]{7900, "JAMES",  "CLERK",     7698, "1981-12-03",  950.00, null,    30},
            new Object[]{7902, "FORD",   "ANALYST",   7566, "1981-12-03", 3000.00, null,    20},
            new Object[]{7934, "MILLER", "CLERK",     7782, "1982-01-23", 1300.00, null,    10}
        );
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataType intType      = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType varcharType  = typeFactory.createSqlType(SqlTypeName.VARCHAR, 10);
        RelDataType jobType      = typeFactory.createSqlType(SqlTypeName.VARCHAR, 10);
        RelDataType dateType     = typeFactory.createSqlType(SqlTypeName.VARCHAR, 10);
        RelDataType decimalType  = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        RelDataType nullableInt  = typeFactory.createTypeWithNullability(intType, true);
        RelDataType nullableDec  = typeFactory.createTypeWithNullability(decimalType, true);

        return typeFactory.builder()
            .add("EMPNO",    intType)
            .add("ENAME",    varcharType)
            .add("JOB",      jobType)
            .add("MGR",      nullableInt)
            .add("HIREDATE", dateType)
            .add("SAL",      decimalType)
            .add("COMM",     nullableDec)
            .add("DEPTNO",   nullableInt)
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
