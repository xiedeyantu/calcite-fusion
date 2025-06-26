package github.xiedeyantu.rules;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class EnumRules {
    public static final RuleSet ENUMERABLE_RULES =
            RuleSets.ofList(EnumerableRules.ENUMERABLE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_ASOFJOIN_RULE,
                    EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_CORRELATE_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_RULE,
                    EnumerableRules.ENUMERABLE_CALC_RULE,
                    EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                    EnumerableRules.ENUMERABLE_SORT_RULE,
                    EnumerableRules.ENUMERABLE_LIMIT_RULE,
                    EnumerableRules.ENUMERABLE_COLLECT_RULE,
                    EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
                    EnumerableRules.ENUMERABLE_MERGE_UNION_RULE,
                    EnumerableRules.ENUMERABLE_UNION_RULE,
                    EnumerableRules.ENUMERABLE_REPEAT_UNION_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_SPOOL_RULE,
                    EnumerableRules.ENUMERABLE_INTERSECT_RULE,
                    EnumerableRules.ENUMERABLE_MINUS_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
                    EnumerableRules.ENUMERABLE_VALUES_RULE,
                    EnumerableRules.ENUMERABLE_WINDOW_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE,
                    EnumerableRules.ENUMERABLE_MATCH_RULE);

    private EnumRules() {}
}
