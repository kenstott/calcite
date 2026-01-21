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
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Relational expression representing a scan of a Salesforce table.
 */
public class SalesforceTableScan extends TableScan implements SalesforceRel {

  private final SalesforceTable salesforceTable;
  private final String sObjectType;
  private final ImmutableList<String> projectedFields;

  public SalesforceTableScan(RelOptCluster cluster, RelOptTable table,
      SalesforceTable salesforceTable, String sObjectType) {
    this(cluster, cluster.traitSetOf(SalesforceRel.CONVENTION), table,
        salesforceTable, sObjectType, null);
  }

  public SalesforceTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, SalesforceTable salesforceTable, String sObjectType,
      List<String> projectedFields) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.salesforceTable = salesforceTable;
    this.sObjectType = sObjectType;
    this.projectedFields = projectedFields == null ? null : ImmutableList.copyOf(projectedFields);

    assert getConvention() == SalesforceRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new SalesforceTableScan(getCluster(), traitSet, table,
        salesforceTable, sObjectType, projectedFields);
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(SalesforceRules.TO_ENUMERABLE);
    for (RelOptRule rule : SalesforceRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override public void implement(Implementor implementor) {
    implementor.salesforceTable = salesforceTable;
    implementor.table = table;
    implementor.sObjectType = sObjectType;
  }

  /**
   * Get the projected fields, or null if all fields should be selected.
   */
  public List<String> getProjectedFields() {
    return projectedFields;
  }

  /**
   * Get the sObject type.
   */
  public String getSObjectType() {
    return sObjectType;
  }
}
