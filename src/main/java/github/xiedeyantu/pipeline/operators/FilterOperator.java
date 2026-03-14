package github.xiedeyantu.pipeline.operators;

import github.xiedeyantu.pipeline.ColumnBatch;
import github.xiedeyantu.pipeline.Consumer;
import github.xiedeyantu.pipeline.expr.Predicate;

/**
 * Pipeline filter operator.
 *
 * <p>Instead of moving data, this operator builds a <em>selection vector</em>:
 * an array of absolute row indices that satisfy the predicate. Downstream
 * operators read only those indices, so no data is ever copied.
 *
 * <pre>
 * incoming:  columns[0] = [10, 30, 10, 40],  rowCount=4, selection=null (dense)
 * predicate: col[0] == 10
 * outgoing:  same columns array, selection=[0,2], selSize=2
 * </pre>
 */
public class FilterOperator implements Consumer {

    private final Predicate predicate;
    private final Consumer downstream;

    public FilterOperator(Predicate predicate, Consumer downstream) {
        this.predicate = predicate;
        this.downstream = downstream;
    }

    @Override
    public void consume(ColumnBatch batch) {
        int[] sel = new int[batch.rowCount];
        int selSize = predicate.eval(batch, sel);
        if (selSize == 0) {
            return; // whole batch filtered out — nothing to push
        }
        // Update batch selection vector (no data copy)
        batch.selection = sel;
        batch.selSize = selSize;
        downstream.consume(batch);
    }

    @Override
    public void done() {
        downstream.done();
    }
}
