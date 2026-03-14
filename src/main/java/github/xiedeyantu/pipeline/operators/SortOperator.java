package github.xiedeyantu.pipeline.operators;

import github.xiedeyantu.pipeline.ColumnBatch;
import github.xiedeyantu.pipeline.Consumer;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Pipeline sort operator — a <strong>pipeline breaker</strong>.
 *
 * <p>All incoming batches are materialised into an in-memory row buffer.
 * When {@link #done()} is called the buffer is sorted according to the
 * specified collations and the result is pushed downstream in batches.
 */
public class SortOperator implements Consumer {

    private final List<RelFieldCollation> collations;
    /** -1 means no fetch limit. */
    private final int fetch;
    private final Consumer downstream;

    private final List<Object[]> buffer = new ArrayList<>();
    private int colCount = -1;

    public SortOperator(
            List<RelFieldCollation> collations,
            int fetch,
            Consumer downstream) {
        this.collations = collations;
        this.fetch = fetch;
        this.downstream = downstream;
    }

    @Override
    public void consume(ColumnBatch batch) {
        int count = batch.activeCount();
        if (count == 0) {
            return;
        }
        if (colCount == -1) {
            colCount = batch.columns.length;
        }
        // Materialise active rows into row-store buffer
        for (int i = 0; i < count; i++) {
            int rowIdx = batch.isDense() ? i : batch.selection[i];
            Object[] row = new Object[colCount];
            for (int c = 0; c < colCount; c++) {
                row[c] = batch.columns[c][rowIdx];
            }
            buffer.add(row);
        }
    }

    @Override
    public void done() {
        if (buffer.isEmpty()) {
            downstream.done();
            return;
        }

        buffer.sort(buildComparator());

        List<Object[]> sorted = (fetch > 0)
                ? buffer.subList(0, Math.min(fetch, buffer.size()))
                : buffer;

        // Push sorted output downstream in batches
        int cursor = 0;
        while (cursor < sorted.size()) {
            int end = Math.min(cursor + ColumnBatch.BATCH_SIZE, sorted.size());
            downstream.consume(rowsToBatch(sorted.subList(cursor, end)));
            cursor = end;
        }
        downstream.done();
    }

    // -------

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Comparator<Object[]> buildComparator() {
        return (a, b) -> {
            for (RelFieldCollation col : collations) {
                int idx = col.getFieldIndex();
                Object va = a[idx];
                Object vb = b[idx];
                int cmp;
                if (va == null && vb == null) {
                    cmp = 0;
                } else if (va == null) {
                    cmp = 1;   // nulls last
                } else if (vb == null) {
                    cmp = -1;
                } else {
                    cmp = ((Comparable) va).compareTo(vb);
                }
                if (col.direction == RelFieldCollation.Direction.DESCENDING
                        || col.direction == RelFieldCollation.Direction.STRICTLY_DESCENDING) {
                    cmp = -cmp;
                }
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        };
    }

    private ColumnBatch rowsToBatch(List<Object[]> rows) {
        ColumnBatch batch = new ColumnBatch(colCount, rows.size());
        for (int r = 0; r < rows.size(); r++) {
            for (int c = 0; c < colCount; c++) {
                batch.columns[c][r] = rows.get(r)[c];
            }
        }
        return batch;
    }
}
