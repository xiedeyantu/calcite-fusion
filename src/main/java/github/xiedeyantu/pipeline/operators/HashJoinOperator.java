package github.xiedeyantu.pipeline.operators;

import github.xiedeyantu.pipeline.ColumnBatch;
import github.xiedeyantu.pipeline.Consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pipeline hash-join operator (inner equi-join only).
 *
 * <p>This operator is a <strong>pipeline breaker</strong> on the build side:
 * before any probe batches can be processed the entire build (right / inner)
 * side must be read and indexed into a hash table.
 *
 * <p>Usage inside {@code FusionHashJoin.implement()}:
 * <ol>
 *   <li>Create the operator pointing to the downstream consumer.</li>
 *   <li>Call {@link #buildConsumer()} and use it as the downstream for the
 *       right-side pipeline; execute that pipeline fully — the hash table is
 *       populated.</li>
 *   <li>Connect the left-side (probe) pipeline to this operator directly, then
 *       execute the left-side pipeline. Each probe batch is matched against the
 *       hash table and matching combined rows are pushed downstream.</li>
 * </ol>
 *
 * <p>Output schema: {@code [probe_cols..., build_cols...]}
 */
public class HashJoinOperator implements Consumer {

    // ------- build side -------
    /*
     * key   = value of the build-side join key column
     * value = list of materialised build rows (one Object[] per row)
     */
    private final Map<Object, List<Object[]>> buildTable = new HashMap<>();
    private final int buildKeyIdx;    // join key column index within the build side
    private final int buildColCount;  // number of columns in the build side

    // ------- probe side -------
    private final int probeKeyIdx;    // join key column index within the probe side
    private final int probeColCount;  // number of columns in the probe side
    private final Consumer downstream;

    public HashJoinOperator(
            int probeKeyIdx,
            int buildKeyIdx,
            int probeColCount,
            int buildColCount,
            Consumer downstream) {
        this.probeKeyIdx = probeKeyIdx;
        this.buildKeyIdx = buildKeyIdx;
        this.probeColCount = probeColCount;
        this.buildColCount = buildColCount;
        this.downstream = downstream;
    }

    /**
     * Returns a {@link Consumer} to be used as the downstream for the build
     * (right) side pipeline. Each batch consumed here materalises rows into
     * the hash table.
     */
    public Consumer buildConsumer() {
        return new Consumer() {
            @Override
            public void consume(ColumnBatch batch) {
                int count = batch.activeCount();
                for (int i = 0; i < count; i++) {
                    int rowIdx = batch.isDense() ? i : batch.selection[i];
                    Object key = batch.columns[buildKeyIdx][rowIdx];
                    Object[] row = new Object[buildColCount];
                    for (int c = 0; c < buildColCount; c++) {
                        row[c] = batch.columns[c][rowIdx];
                    }
                    buildTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                }
            }

            @Override
            public void done() { /* build phase complete — hash table is ready */ }
        };
    }

    // ------- probe phase driven by FusionHashJoin.implement() -------

    @Override
    public void consume(ColumnBatch probeBatch) {
        int count = probeBatch.activeCount();
        List<Object[]> resultRows = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            int rowIdx = probeBatch.isDense() ? i : probeBatch.selection[i];
            Object probeKey = probeBatch.columns[probeKeyIdx][rowIdx];
            List<Object[]> buildRows = buildTable.get(probeKey);
            if (buildRows == null) {
                continue; // no match — inner join drops the row
            }
            for (Object[] buildRow : buildRows) {
                // Combine probe columns then build columns
                Object[] combined = new Object[probeColCount + buildColCount];
                for (int c = 0; c < probeColCount; c++) {
                    combined[c] = probeBatch.columns[c][rowIdx];
                }
                System.arraycopy(buildRow, 0, combined, probeColCount, buildColCount);
                resultRows.add(combined);
            }
        }

        if (!resultRows.isEmpty()) {
            downstream.consume(rowsToBatch(resultRows));
        }
    }

    @Override
    public void done() {
        downstream.done();
    }

    // -------

    private ColumnBatch rowsToBatch(List<Object[]> rows) {
        int totalCols = probeColCount + buildColCount;
        ColumnBatch batch = new ColumnBatch(totalCols, rows.size());
        for (int r = 0; r < rows.size(); r++) {
            for (int c = 0; c < totalCols; c++) {
                batch.columns[c][r] = rows.get(r)[c];
            }
        }
        return batch;
    }
}
