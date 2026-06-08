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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implementation of Sort relational operator for GraphQL queries.
 */
public class GraphQLSort extends Sort implements GraphQLRel {
  private static final Logger LOGGER = LogManager.getLogger(GraphQLSort.class);

  public GraphQLSort(RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    super(cluster, traits, input, collation, offset, fetch);
    LOGGER.debug("Created GraphQLSort with collation: {}, offset: {}, fetch: {}",
        collation, offset, fetch);
  }

  @Override public Sort copy(RelTraitSet traitSet,
      RelNode newInput,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    return new GraphQLSort(getCluster(), traitSet, newInput,
        collation, offset, fetch);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitInput(0, getInput());

    // Add ordering information
    if (!collation.getFieldCollations().isEmpty()) {
      implementor.addOrder(collation);
    }

    // Add offset if present
    if (offset != null) {
      implementor.addOffset(offset);
    }

    // Add fetch/limit if present
    if (fetch != null) {
      implementor.addFetch(fetch);
    }
  }
}
