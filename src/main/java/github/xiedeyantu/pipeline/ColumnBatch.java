package github.xiedeyantu.pipeline;

/**
 * Columnar batch: the fundamental unit of data in the pipeline execution engine.
 *
 * <p>Each column is stored as a contiguous Object array. A "selection vector"
 * avoids moving data when rows are filtered — the selection array holds the
 * absolute row indices that are still active after a filter. When selection is
 * null every row in [0, rowCount) is active (dense / unfiltered batch).
 *
 * <pre>
 * Dense batch (after scan):        Sparse batch (after filter):
 *  columns[0] = [10, 20, 30, 40]    columns[0] = [10, 20, 30, 40]
 *  selection  = null                selection  = [0, 2]   // only rows 0,2 pass
 *  rowCount   = 4                   selSize    = 2
 * </pre>
 */
public final class ColumnBatch {

    /** Default number of rows per batch — tuned to fit L1/L2 cache. */
    public static final int BATCH_SIZE = 4096;

    /** columns[colIdx][rowIdx] — each column is a plain Object[]. */
    public final Object[][] columns;

    /** Total rows allocated (capacity), including rows that may be filtered out. */
    public int rowCount;

    /**
     * Selection vector: absolute row indices (into {@code columns[c][?]}) that
     * are active. {@code null} means all rows [0, rowCount) are active (dense).
     */
    public int[] selection;

    /** Number of valid entries in {@link #selection}. Ignored when selection is null. */
    public int selSize;

    public ColumnBatch(int colCount, int rowCount) {
        this.columns = new Object[colCount][];
        for (int i = 0; i < colCount; i++) {
            this.columns[i] = new Object[rowCount];
        }
        this.rowCount = rowCount;
        this.selection = null;
        this.selSize = rowCount;
    }

    /** True when all rows are active (no filter has been applied). */
    public boolean isDense() {
        return selection == null;
    }

    /** Number of active rows (respects selection vector). */
    public int activeCount() {
        return isDense() ? rowCount : selSize;
    }
}
