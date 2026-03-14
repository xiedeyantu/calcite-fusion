package github.xiedeyantu.pipeline.operators;

import github.xiedeyantu.pipeline.ColumnBatch;
import github.xiedeyantu.pipeline.Consumer;
import github.xiedeyantu.pipeline.expr.Expr;

import java.util.List;

/**
 * Pipeline projection operator.
 *
 * <p>Evaluates a list of output expressions and produces a new dense
 * {@link ColumnBatch} with only the projected columns. The upstream batch's
 * selection vector is fully resolved here — the output batch is always dense.
 */
public class ProjectOperator implements Consumer {

    private final List<Expr> exprs;
    private final Consumer downstream;

    public ProjectOperator(List<Expr> exprs, Consumer downstream) {
        this.exprs = exprs;
        this.downstream = downstream;
    }

    @Override
    public void consume(ColumnBatch batch) {
        int count = batch.activeCount();
        if (count == 0) {
            return;
        }

        // Output is always a dense batch with only the projected columns
        ColumnBatch result = new ColumnBatch(exprs.size(), count);
        for (int c = 0; c < exprs.size(); c++) {
            for (int i = 0; i < count; i++) {
                int rowIdx = batch.isDense() ? i : batch.selection[i];
                result.columns[c][i] = exprs.get(c).eval(batch, rowIdx);
            }
        }
        result.rowCount = count;
        downstream.consume(result);
    }

    @Override
    public void done() {
        downstream.done();
    }
}
