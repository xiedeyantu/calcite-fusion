/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.enumerable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Planner rule that converts a
 * {@link LogicalJoin} relational expression
 * {@link EnumerableConvention enumerable calling convention}.
 * You may provide a custom config to convert other nodes that extend {@link Join}.
 *
 * @see EnumerableRules#ENUMERABLE_JOIN_RULE */
class EnumerableJoinRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(LogicalJoin.class, Convention.NONE,
          EnumerableConvention.INSTANCE, "EnumerableJoinRule")
      .withRuleFactory(EnumerableJoinRule::new);

  /** Called from the Config. */
  protected EnumerableJoinRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    throw new RuntimeException(this.getClass().getName() + " convert() is not callable");
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    Set<Convention> joinConvention = Sets.newHashSet(EnumerableConvention.INSTANCE);

    List<Pair<JoinImplement, List<RelTraitSet>>> possibleImplement = deriveJoinTraitSet(join, joinConvention);
    for (Pair<JoinImplement, List<RelTraitSet>> implement : possibleImplement) {
      final List<RelNode> newInputs = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        RelNode input = join.getInput(i);
        newInputs.add(convert(input, implement.right.get(i)));
      }
      RelNode newJoin;
      switch (implement.left) {
        case HASH:
          newJoin = new EnumerableHashJoin(
              join.getCluster(),
              implement.right.get(0),
              newInputs.get(0),
              newInputs.get(1),
              join.getCondition(),
              join.getVariablesSet(),
              join.getJoinType());
          break;
        case NESTED_LOOP:
          newJoin = new EnumerableNestedLoopJoin(
              join.getCluster(),
              implement.right.get(0),
              newInputs.get(0),
              newInputs.get(1),
              join.getCondition(),
              join.getVariablesSet(),
              join.getJoinType());
          break;
        default:
          throw new IllegalStateException("Unsupported join implementation: " + implement.left);
      }
      call.transformTo(newJoin);
    }
  }

  private List<Pair<JoinImplement, List<RelTraitSet>>> deriveJoinTraitSet(Join join, Set<Convention> joinConvention) {
    final JoinInfo info = join.analyzeCondition();
    final boolean hasEquiKeys = hasEquiKeys(info);

    final List<Pair<JoinImplement, List<RelTraitSet>>> possibleTraitSet = new ArrayList<>();
    if (hasEquiKeys) {
      for (Convention convention : joinConvention) {
        possibleTraitSet.add(
            Pair.of(
                JoinImplement.HASH,
                ImmutableList.of(
                    join.getLeft().getTraitSet().replace(convention).replace(RelDistributions.hash(info.leftKeys)),
                    join.getRight().getTraitSet().replace(convention).replace(RelDistributions.hash(info.rightKeys)))
            ));
        if (couldBroadcastByType(join.getJoinType()) && couldBroadcastBySize(join)) {
          possibleTraitSet.add(
              Pair.of(
                  JoinImplement.HASH,
                  ImmutableList.of(
                      join.getLeft().getTraitSet().replace(convention).replace(RelDistributions.ANY),
                      join.getRight().getTraitSet().replace(convention).replace(RelDistributions.BROADCAST_DISTRIBUTED))
              ));
        }
      }
    } else {
      for (Convention convention : joinConvention) {
        if (couldBroadcastByType(join.getJoinType())) {
          possibleTraitSet.add(
              Pair.of(
                  JoinImplement.NESTED_LOOP,
                  ImmutableList.of(
                      join.getLeft().getTraitSet().replace(convention).replace(RelDistributions.ANY),
                      join.getRight().getTraitSet().replace(convention).replace(RelDistributions.BROADCAST_DISTRIBUTED))
              ));
        } else {
          throw new IllegalStateException("Unsupported nested loop join type: " + join.getJoinType());
        }
      }
    }
    return possibleTraitSet;
  }

  public static boolean couldBroadcastByType(JoinRelType joinType) {
    return !(joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL);
  }

  public static boolean couldBroadcastBySize(Join join) {
    RelMetadataQuery mq = join.getCluster().getMetadataQuery();
    double rowCount = join.getRight().estimateRowCount(mq);
    return rowCount <= 100;
  }

  public static boolean hasEquiKeys(JoinInfo joinInfo) {
    return !joinInfo.leftKeys.isEmpty() && !joinInfo.rightKeys.isEmpty();
  }

  public enum JoinImplement {
    HASH,
    SORT_MERGE,
    NESTED_LOOP;
  }
}
