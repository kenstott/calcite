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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link Sort} relational expression in Salesforce.
 */
public class SalesforceSort extends Sort implements SalesforceRel {

  public SalesforceSort(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, input, collation, offset, fetch);
    assert getConvention() == SalesforceRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override public SalesforceSort copy(RelTraitSet traitSet, RelNode input,
      RelCollation collation, RexNode offset, RexNode fetch) {
    return new SalesforceSort(getCluster(), traitSet, input, collation,
        offset, fetch);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Salesforce can push down ORDER BY and LIMIT
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    // Build ORDER BY clause
    if (!collation.getFieldCollations().isEmpty()) {
      List<String> orderByItems = new ArrayList<>();
      RelDataType rowType = getInput().getRowType();

      for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
        String fieldName = rowType.getFieldList()
            .get(fieldCollation.getFieldIndex()).getName();
        String direction = fieldCollation.getDirection().isDescending() ? "DESC" : "ASC";

        // Handle nulls direction if specified
        String nullsDirection = "";
        switch (fieldCollation.nullDirection) {
        case FIRST:
          nullsDirection = " NULLS FIRST";
          break;
        case LAST:
          nullsDirection = " NULLS LAST";
          break;
        }

        orderByItems.add(fieldName + " " + direction + nullsDirection);
      }

      implementor.orderByClause = String.join(", ", orderByItems);
    }

    // Handle LIMIT
    if (fetch != null && fetch instanceof RexLiteral) {
      RexLiteral fetchLiteral = (RexLiteral) fetch;
      implementor.limitValue = fetchLiteral.getValueAs(Integer.class);
    }

    // Handle OFFSET
    if (offset != null && offset instanceof RexLiteral) {
      RexLiteral offsetLiteral = (RexLiteral) offset;
      implementor.offsetValue = offsetLiteral.getValueAs(Integer.class);
    }
  }
}
