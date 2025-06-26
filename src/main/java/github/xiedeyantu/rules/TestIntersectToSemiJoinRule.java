package github.xiedeyantu.rules;

import github.xiedeyantu.Executer;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class TestIntersectToSemiJoinRule {
    /**
     * === SQL ===
     * SELECT e.deptno FROM emps e INTERSECT SELECT d.deptno FROM depts d
     *
     * === Init Plan ===
     * LogicalIntersect(all=[false])
     *   LogicalProject(DEPTNO=[$1])
     *     LogicalTableScan(table=[[test, emps]])
     *   LogicalProject(DEPTNO=[$0])
     *     LogicalTableScan(table=[[test, depts]])
     *
     * === Hep Plan ===
     * LogicalAggregate(group=[{0}])
     *   LogicalJoin(condition=[=($0, $1)], joinType=[semi])
     *     LogicalProject(DEPTNO=[$1])
     *       LogicalTableScan(table=[[test, emps]])
     *     LogicalProject(DEPTNO=[$0])
     *       LogicalTableScan(table=[[test, depts]])
     */
    public static void main(String[] args) throws Exception {
        String sql = "SELECT e.deptno FROM emps e " +
                "INTERSECT " +
                "SELECT d.deptno FROM depts d";

        RuleSet ruleSet = RuleSets.ofList(
                CoreRules.INTERSECT_TO_SEMI_JOIN);

        Executer.printLogicalPlan(sql, ruleSet);
    }
}
