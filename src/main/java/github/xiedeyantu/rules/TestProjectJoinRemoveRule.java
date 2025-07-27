package github.xiedeyantu.rules;

import github.xiedeyantu.Executer;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class TestProjectJoinRemoveRule {
    /**
     * === SQL ===
     * SELECT e.deptno FROM emps e LEFT JOIN depts d ON e.deptno = d.deptno
     *
     * === Init Plan ===
     * LogicalProject(DEPTNO=[$1])
     *   LogicalJoin(condition=[=($1, $5)], joinType=[left])
     *     LogicalTableScan(table=[[test, emps]])
     *     LogicalTableScan(table=[[test, depts]])
     *
     * === Hep Plan ===
     * LogicalProject(DEPTNO=[$1])
     *   LogicalTableScan(table=[[test, emps]])
     */
    public static void main(String[] args) throws Exception {
        String sql = "SELECT e.deptno " +
                "FROM emps e " +
                "LEFT JOIN depts d ON e.deptno = d.deptno";

        RuleSet ruleSet = RuleSets.ofList(
                CoreRules.PROJECT_JOIN_REMOVE);

        Executer.printLogicalPlan(sql, ruleSet);
    }
}
