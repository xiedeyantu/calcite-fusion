package github.xiedeyantu.pipeline.operators;

import github.xiedeyantu.pipeline.ColumnBatch;
import github.xiedeyantu.pipeline.Consumer;

import java.util.ArrayList;
import java.util.List;

/**
 * Terminal consumer that collects all result rows into a {@code List<Object[]>}.
 *
 * <p>Used as the root downstream of the pipeline. After
 * {@link github.xiedeyantu.pipeline.PipelineExecutor} calls
 * {@link github.xiedeyantu.pipeline.Source#execute()}, the caller retrieves
 * results via {@link #getResults()}.
 */
public class ResultCollector implements Consumer {

    private final List<Object[]> results = new ArrayList<>();
    private int colCount = -1;

    @Override
    public void consume(ColumnBatch batch) {
        int count = batch.activeCount();
        if (colCount == -1) {
            colCount = batch.columns.length;
        }
        for (int i = 0; i < count; i++) {
            int rowIdx = batch.isDense() ? i : batch.selection[i];
            Object[] row = new Object[colCount];
            for (int c = 0; c < colCount; c++) {
                row[c] = batch.columns[c][rowIdx];
            }
            results.add(row);
        }
    }

    @Override
    public void done() {
        // Nothing to do — all results already collected via consume().
    }

    public List<Object[]> getResults() {
        return results;
    }
}
