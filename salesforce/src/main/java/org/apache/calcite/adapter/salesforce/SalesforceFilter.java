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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/**
 * Implementation of {@link Filter} relational expression in Salesforce.
 */
public class SalesforceFilter extends Filter implements SalesforceRel {

  public SalesforceFilter(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RexNode condition) {
    super(cluster, traitSet, input, condition);
    assert getConvention() == SalesforceRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override public SalesforceFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    return new SalesforceFilter(getCluster(), traitSet, input, condition);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Salesforce can push down filters effectively
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    // Convert filter condition to SOQL WHERE clause
    String whereClause = SOQLBuilder.buildWhereClause(condition);

    if (implementor.whereClause == null) {
      implementor.whereClause = whereClause;
    } else {
      // Combine with existing WHERE clause
      implementor.whereClause = "(" + implementor.whereClause + ") AND (" + whereClause + ")";
    }
  }
}
