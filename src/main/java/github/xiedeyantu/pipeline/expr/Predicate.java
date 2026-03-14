package github.xiedeyantu.pipeline.expr;

import github.xiedeyantu.pipeline.ColumnBatch;

/**
 * A boolean filter over a columnar batch.
 *
 * <p>Implementations iterate the active rows in a batch (respecting any
 * existing selection vector), test the predicate condition, and write the
 * indices of passing rows into {@code outSel}.
 *
 * @see RexToExpr#toPredicate
 */
@FunctionalInterface
public interface Predicate {

    /**
     * Evaluate this predicate over {@code batch}.
     *
     * @param batch  the current columnar batch (respects its selection vector)
     * @param outSel output array; indices of rows that satisfy this predicate
     *               are written here (absolute row indices)
     * @return number of passing rows written into {@code outSel}
     */
    int eval(ColumnBatch batch, int[] outSel);
}
