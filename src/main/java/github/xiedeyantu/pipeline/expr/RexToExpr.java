package github.xiedeyantu.pipeline.expr;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts Calcite {@link RexNode} trees into executable {@link Expr} /
 * {@link Predicate} objects used by the pipeline operators.
 *
 * <p>Supported RexNode types:
 * <ul>
 *   <li>{@link RexInputRef}  — column reference</li>
 *   <li>{@link RexLiteral}   — constant (integer, long, double, string, boolean)</li>
 *   <li>{@link RexCall}      — binary comparisons, arithmetic, AND/OR/NOT,
 *                              IS NULL, IS NOT NULL, CAST (pass-through)</li>
 * </ul>
 */
public final class RexToExpr {

    private RexToExpr() {}

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Convert a {@link RexNode} to a value expression ({@link Expr}).
     * The expression is evaluated row-by-row using interpreted execution.
     */
    public static Expr toExpr(RexNode rex) {
        if (rex instanceof RexInputRef) {
            int idx = ((RexInputRef) rex).getIndex();
            return (batch, rowIdx) -> batch.columns[idx][rowIdx];
        }
        if (rex instanceof RexLiteral) {
            Object val = literalValue((RexLiteral) rex);
            return (batch, rowIdx) -> val;
        }
        if (rex instanceof RexCall) {
            RexCall call = (RexCall) rex;
            List<Expr> ops = call.getOperands().stream()
                    .map(RexToExpr::toExpr)
                    .collect(Collectors.toList());
            return buildCallExpr(call.getKind(), ops, call);
        }
        throw new UnsupportedOperationException(
                "Unsupported RexNode: " + rex.getClass().getSimpleName() + " — " + rex);
    }

    /**
     * Convert a boolean {@link RexNode} into a batch-level {@link Predicate}
     * that writes passing row indices into the caller-supplied selection vector.
     */
    public static Predicate toPredicate(RexNode rex) {
        Expr expr = toExpr(rex);
        return (batch, outSel) -> {
            int selSize = 0;
            int count = batch.activeCount();
            for (int i = 0; i < count; i++) {
                int rowIdx = batch.isDense() ? i : batch.selection[i];
                if (Boolean.TRUE.equals(expr.eval(batch, rowIdx))) {
                    outSel[selSize++] = rowIdx;
                }
            }
            return selSize;
        };
    }

    // -----------------------------------------------------------------------
    // Literal value extraction
    // -----------------------------------------------------------------------

    private static Object literalValue(RexLiteral lit) {
        if (lit.isNull()) {
            return null;
        }
        switch (lit.getType().getSqlTypeName()) {
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return ((BigDecimal) lit.getValue()).intValue();
            case BIGINT:
                return ((BigDecimal) lit.getValue()).longValue();
            case DOUBLE:
            case FLOAT:
            case REAL:
            case DECIMAL:
                return ((BigDecimal) lit.getValue()).doubleValue();
            case VARCHAR:
            case CHAR:
                Object v = lit.getValue();
                return (v instanceof NlsString) ? ((NlsString) v).getValue() : v;
            case BOOLEAN:
                return lit.getValue();
            default:
                return lit.getValue();
        }
    }

    // -----------------------------------------------------------------------
    // Call expression construction
    // -----------------------------------------------------------------------

    private static Expr buildCallExpr(SqlKind kind, List<Expr> ops, RexCall call) {
        switch (kind) {
            // --- comparisons ---
            case EQUALS:
                return (b, r) -> eq(ops.get(0).eval(b, r), ops.get(1).eval(b, r));
            case NOT_EQUALS:
                return (b, r) -> !eq(ops.get(0).eval(b, r), ops.get(1).eval(b, r));
            case GREATER_THAN:
                return (b, r) -> cmp(ops.get(0).eval(b, r), ops.get(1).eval(b, r)) > 0;
            case GREATER_THAN_OR_EQUAL:
                return (b, r) -> cmp(ops.get(0).eval(b, r), ops.get(1).eval(b, r)) >= 0;
            case LESS_THAN:
                return (b, r) -> cmp(ops.get(0).eval(b, r), ops.get(1).eval(b, r)) < 0;
            case LESS_THAN_OR_EQUAL:
                return (b, r) -> cmp(ops.get(0).eval(b, r), ops.get(1).eval(b, r)) <= 0;

            // --- logical ---
            case AND:
                return (b, r) -> {
                    for (Expr op : ops) {
                        if (!Boolean.TRUE.equals(op.eval(b, r))) return false;
                    }
                    return true;
                };
            case OR:
                return (b, r) -> {
                    for (Expr op : ops) {
                        if (Boolean.TRUE.equals(op.eval(b, r))) return true;
                    }
                    return false;
                };
            case NOT:
                return (b, r) -> !Boolean.TRUE.equals(ops.get(0).eval(b, r));

            // --- null checks ---
            case IS_NULL:
                return (b, r) -> ops.get(0).eval(b, r) == null;
            case IS_NOT_NULL:
                return (b, r) -> ops.get(0).eval(b, r) != null;

            // --- arithmetic ---
            case PLUS:
                return (b, r) -> numAdd(ops.get(0).eval(b, r), ops.get(1).eval(b, r));
            case MINUS:
                return (b, r) -> numSub(ops.get(0).eval(b, r), ops.get(1).eval(b, r));
            case TIMES:
                return (b, r) -> numMul(ops.get(0).eval(b, r), ops.get(1).eval(b, r));

            // --- cast: pass-through (type coercion handled at scan time) ---
            case CAST:
                return ops.get(0);

            default:
                throw new UnsupportedOperationException(
                        "Unsupported SQL operator: " + kind + " — " + call);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static boolean eq(Object a, Object b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).longValue() == ((Number) b).longValue();
        }
        return a.equals(b);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int cmp(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }
        return ((Comparable) a).compareTo(b);
    }

    private static Object numAdd(Object a, Object b) {
        if (a == null || b == null) return null;
        if (a instanceof Integer && b instanceof Integer) return (Integer) a + (Integer) b;
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).longValue() + ((Number) b).longValue();
        }
        return null;
    }

    private static Object numSub(Object a, Object b) {
        if (a == null || b == null) return null;
        if (a instanceof Integer && b instanceof Integer) return (Integer) a - (Integer) b;
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).longValue() - ((Number) b).longValue();
        }
        return null;
    }

    private static Object numMul(Object a, Object b) {
        if (a == null || b == null) return null;
        if (a instanceof Integer && b instanceof Integer) return (Integer) a * (Integer) b;
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).longValue() * ((Number) b).longValue();
        }
        return null;
    }
}
