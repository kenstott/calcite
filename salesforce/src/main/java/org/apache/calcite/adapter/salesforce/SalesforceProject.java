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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link Project} relational expression in Salesforce.
 */
public class SalesforceProject extends Project implements SalesforceRel {

  public SalesforceProject(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
    assert getConvention() == SalesforceRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override public SalesforceProject copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> projects, RelDataType rowType) {
    return new SalesforceProject(getCluster(), traitSet, input, projects, rowType);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Salesforce can efficiently project fields
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    // Build SELECT clause from projected fields
    List<String> fieldNames = extractFieldNames(this);
    implementor.selectClause = String.join(", ", fieldNames);
  }

  /**
   * Extract field names from a project.
   */
  public static List<String> extractFieldNames(Project project) {
    List<String> fieldNames = new ArrayList<>();
    RelDataType inputRowType = project.getInput().getRowType();

    for (RexNode proj : project.getProjects()) {
      if (proj instanceof RexInputRef) {
        RexInputRef inputRef = (RexInputRef) proj;
        String fieldName = inputRowType.getFieldList()
            .get(inputRef.getIndex()).getName();
        fieldNames.add(fieldName);
      } else {
        // Complex expressions not supported in SOQL projection
        throw new UnsupportedOperationException(
            "Complex expressions in projection not supported: " + proj);
      }
    }

    return fieldNames;
  }
}
