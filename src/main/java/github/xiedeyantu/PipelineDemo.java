package github.xiedeyantu;

import github.xiedeyantu.pipeline.PipelineExecutor;

import java.util.Arrays;
import java.util.List;

/**
 * Demonstrates the push-based pipeline execution engine.
 *
 * <p>Execution pipeline for the demo query:
 * <pre>
 *   ScanOperator(dept) ──► HashJoin.buildConsumer
 *                          [hash table built]
 *   ScanOperator(emp)  ──► FilterOperator(job=SALESMAN) ──► HashJoinOperator (probe)
 *                                                        ──► ProjectOperator
 *                                                        ──► SortOperator (buffer)
 *                                                        ──► ResultCollector
 * </pre>
 *
 * <p>Pipeline-breaker points:
 * <ul>
 *   <li>{@code HashJoin build side} — dept pipeline runs eagerly to populate
 *       the hash table before the emp probe side starts.</li>
 *   <li>{@code Sort} — all matching rows are buffered, sorted by SAL DESC, then
 *       emitted when {@code done()} propagates up from the scan.</li>
 * </ul>
 */
public class PipelineDemo {

    public static void main(String[] args) throws Exception {

        String sql =
                "SELECT e.empno, e.ename, e.job, e.sal, d.dname, d.loc "
                + "FROM emp e "
                + "JOIN dept d ON e.deptno = d.deptno "
                + "WHERE e.job = 'SALESMAN' "
                + "ORDER BY e.sal DESC";

        System.out.println("=== SQL ===\n" + sql + "\n");

        PipelineExecutor executor = new PipelineExecutor();
        List<Object[]> results = executor.execute(sql);

        System.out.println("=== Results ===");
        System.out.printf("%-8s %-10s %-12s %-10s %-12s %-10s%n",
                "EMPNO", "ENAME", "JOB", "SAL", "DNAME", "LOC");
        System.out.println(new String(new char[64]).replace('\0', '-'));
        for (Object[] row : results) {
            System.out.printf("%-8s %-10s %-12s %-10s %-12s %-10s%n",
                    row[0], row[1], row[2], row[3], row[4], row[5]);
        }
        System.out.println("\nTotal rows: " + results.size());
    }
}
