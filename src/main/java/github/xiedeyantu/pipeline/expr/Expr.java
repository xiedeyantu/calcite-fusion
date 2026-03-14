package github.xiedeyantu.pipeline.expr;

import github.xiedeyantu.pipeline.ColumnBatch;

/**
 * A scalar expression that evaluates to a single value for one row.
 *
 * <p>Implementations correspond to Calcite {@link org.apache.calcite.rex.RexNode}
 * tree nodes:
 * <ul>
 *   <li>{@link org.apache.calcite.rex.RexInputRef} → column reference</li>
 *   <li>{@link org.apache.calcite.rex.RexLiteral}  → constant</li>
 *   <li>{@link org.apache.calcite.rex.RexCall}     → binary/unary operation</li>
 * </ul>
 *
 * @see RexToExpr
 */
@FunctionalInterface
public interface Expr {

    /**
     * Evaluate this expression for the row at absolute position {@code rowIdx}
     * inside {@code batch}.
     *
     * @param batch  the current columnar batch
     * @param rowIdx absolute row index into {@code batch.columns[c][rowIdx]}
     * @return the resulting value (may be {@code null})
     */
    Object eval(ColumnBatch batch, int rowIdx);
}
