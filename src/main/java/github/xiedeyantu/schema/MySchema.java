package github.xiedeyantu.schema;

import github.xiedeyantu.tables.BonusTable;
import github.xiedeyantu.tables.DeptTable;
import github.xiedeyantu.tables.DeptsTable;
import github.xiedeyantu.tables.EmpTable;
import github.xiedeyantu.tables.EmpsTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

public class MySchema extends AbstractSchema {
    public MySchema() {}

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = new HashMap<>();
        // original demo tables
        tables.put("depts", new DeptsTable());
        tables.put("emps",  new EmpsTable());
        // Scott schema tables
        tables.put("dept",  new DeptTable());
        tables.put("emp",   new EmpTable());
        tables.put("bonus", new BonusTable());
        return tables;
    }
}
