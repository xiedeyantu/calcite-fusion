package github.xiedeyantu.pipeline.operators;

import github.xiedeyantu.pipeline.ColumnBatch;
import github.xiedeyantu.pipeline.Consumer;
import github.xiedeyantu.pipeline.Source;

import java.util.List;

/**
 * Pipeline source operator for an in-memory table.
 *
 * <p>Converts row-oriented data ({@code List<Object[]>}) into column-oriented
 * {@link ColumnBatch} batches and pushes them to the downstream consumer.
 * This is the only operator that drives execution — all other operators are
 * passive {@link Consumer} implementations.
 */
public class ScanOperator implements Source {

    private final List<Object[]> rowData;
    private final int colCount;
    private final Consumer downstream;

    public ScanOperator(List<Object[]> rowData, int colCount, Consumer downstream) {
        this.rowData = rowData;
        this.colCount = colCount;
        this.downstream = downstream;
    }

    @Override
    public void execute() {
        int total = rowData.size();
        int cursor = 0;
        while (cursor < total) {
            int end = Math.min(cursor + ColumnBatch.BATCH_SIZE, total);
            int batchRows = end - cursor;

            // Transpose: row-store → column-store
            ColumnBatch batch = new ColumnBatch(colCount, batchRows);
            for (int r = 0; r < batchRows; r++) {
                Object[] row = rowData.get(cursor + r);
                for (int c = 0; c < colCount; c++) {
                    batch.columns[c][r] = (c < row.length) ? row[c] : null;
                }
            }
            downstream.consume(batch);
            cursor = end;
        }
        downstream.done();
    }
}
