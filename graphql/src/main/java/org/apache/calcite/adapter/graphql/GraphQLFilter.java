/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.graphql;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implementation of {@link Filter} in {@link GraphQLRel#CONVENTION} convention.
 */
public class GraphQLFilter extends Filter implements GraphQLRel {
  public GraphQLFilter(RelOptCluster cluster, RelTraitSet traits,
      RelNode input, RexNode condition) {
    super(cluster, traits, ImmutableList.of(), input, condition);
    assert getConvention() == GraphQLRel.CONVENTION;
  }

  @Override public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new GraphQLFilter(getCluster(), traitSet, input, condition);
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    RelOptCost cost = super.computeSelfCost(planner, mq);
    if (cost == null) {
      return null;
    }
    // GraphQL filtering is typically pushed to the server
    return cost.multiplyBy(0.1);
  }

  @Override public void explain(RelWriter pw) {
    super.explain(pw);
    pw.item("graphqlFilter", getCondition().toString());
  }

  @Override public RelNode accept(RexShuttle shuttle) {
    RexNode condition = shuttle.apply(getCondition());
    if (condition == getCondition()) {
      return this;
    }
    return new GraphQLFilter(getCluster(), getTraitSet(), getInput(), condition);
  }

  @Override public boolean isValid(Litmus litmus, @Nullable Context context) {
    return super.isValid(litmus, context);
  }

  @Override public void implement(GraphQLRel.Implementor implementor) {
    implementor.visitInput(0, getInput());
    implementor.addFilter(getCondition());
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    // Default filter selectivity of 0.5
    return mq.getRowCount(getInput()) * 0.5;
  }
}
