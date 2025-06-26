package github.xiedeyantu.rules;

import github.xiedeyantu.Executer;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class TestFilterIntoJoinRule {
    /**
     * === SQL ===
     * SELECT e.empid, e.name, e.salary, d.name as dept_name FROM emps e JOIN depts d ON e.deptno = d.deptno WHERE d.deptno = 10 ORDER BY e.salary DESC
     *
     * === Init Plan ===
     * LogicalSort(sort0=[$2], dir0=[DESC])
     *   LogicalProject(EMPID=[$0], NAME=[$2], SALARY=[$3], DEPT_NAME=[$6])
     *     LogicalFilter(condition=[=($5, 10)])
     *       LogicalJoin(condition=[=($1, $5)], joinType=[inner])
     *         LogicalTableScan(table=[[test, emps]])
     *         LogicalTableScan(table=[[test, depts]])
     *
     * === Hep Plan ===
     * LogicalSort(sort0=[$2], dir0=[DESC])
     *   LogicalProject(EMPID=[$0], NAME=[$2], SALARY=[$3], DEPT_NAME=[$6])
     *     LogicalJoin(condition=[=($1, $5)], joinType=[inner])
     *       LogicalTableScan(table=[[test, emps]])
     *       LogicalFilter(condition=[=($0, 10)])
     *         LogicalTableScan(table=[[test, depts]])
     */
    public static void main(String[] args) throws Exception {
        String sql = "SELECT e.empid, e.name, e.salary, d.name as dept_name " +
                "FROM emps e " +
                "JOIN depts d ON e.deptno = d.deptno " +
                "WHERE d.deptno = 10 " +
                "ORDER BY e.salary DESC";

        RuleSet ruleSet = RuleSets.ofList(
                CoreRules.FILTER_INTO_JOIN);

        Executer.printLogicalPlan(sql, ruleSet);
    }
}
