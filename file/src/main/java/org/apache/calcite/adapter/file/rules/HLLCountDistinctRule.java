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
package org.apache.calcite.adapter.file.rules;

import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that replaces COUNT(DISTINCT) operations with pre-computed HLL sketch lookups.
 * This provides instant responses for cardinality queries without scanning data.
 * Only handles simple aggregates without GROUP BY.
 */
@Value.Enclosing
public class HLLCountDistinctRule extends RelRule<HLLCountDistinctRule.Config> {

  public static final HLLCountDistinctRule INSTANCE =
      Config.DEFAULT.toRule();

  private HLLCountDistinctRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode input = aggregate.getInput();

    // Only handle simple aggregates without GROUP BY
    if (!aggregate.getGroupSet().isEmpty()) {
      return;
    }

    boolean hasOptimizableAgg = false;
    List<Long> hllEstimates = new ArrayList<>();

    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (aggCall.getAggregation().getKind() == SqlKind.COUNT && aggCall.isDistinct()) {
        Long estimate = getHLLEstimate(input, aggCall);
        if (estimate != null) {
          hasOptimizableAgg = true;
          hllEstimates.add(estimate);
        } else {
          hllEstimates.add(null);
        }
      } else {
        hllEstimates.add(null);
      }
    }

    if (!hasOptimizableAgg) {
      return;
    }

    RelNode valuesNode = createHLLValues(aggregate, hllEstimates);
    if (valuesNode != null) {
      call.transformTo(valuesNode, com.google.common.collect.ImmutableMap.of());
    }
  }

  private Long getHLLEstimate(RelNode input, AggregateCall aggCall) {
    TableScan tableScan = findTableScan(input);
    if (tableScan == null || aggCall.getArgList().isEmpty()) {
      return null;
    }

    int columnIndex = aggCall.getArgList().get(0);
    String columnName = input.getRowType().getFieldNames().get(columnIndex);

    List<String> qualifiedName = tableScan.getTable().getQualifiedName();
    String schemaName = qualifiedName.size() >= 2 ? qualifiedName.get(qualifiedName.size() - 2) : "";
    String tableName = qualifiedName.get(qualifiedName.size() - 1);

    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = cache.getSketch(schemaName, tableName, columnName);
    return sketch != null ? sketch.getEstimate() : null;
  }

  private static TableScan findTableScan(RelNode node) {
    if (node instanceof TableScan) {
      return (TableScan) node;
    }
    for (RelNode input : node.getInputs()) {
      TableScan scan = findTableScan(input);
      if (scan != null) {
        return scan;
      }
    }
    return null;
  }

  private static RelNode createHLLValues(Aggregate aggregate, List<Long> hllEstimates) {
    RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();

    RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();
    List<RexLiteral> values = new ArrayList<>();

    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      Long estimate = hllEstimates.get(i);
      if (estimate != null) {
        RelDataType bigIntType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        AggregateCall aggCall = aggregate.getAggCallList().get(i);
        String fieldName = aggCall.getName() != null ? aggCall.getName() : "EXPR$" + i;
        typeBuilder.add(fieldName, bigIntType);
        values.add((RexLiteral) rexBuilder.makeLiteral(estimate, bigIntType, true));
      } else {
        return null;
      }
    }

    RelDataType rowType = typeBuilder.build();
    return LogicalValues.create(
        aggregate.getCluster(),
        rowType,
        com.google.common.collect.ImmutableList.of(
            com.google.common.collect.ImmutableList.copyOf(values)));
  }

  /** Configuration for HLLCountDistinctRule. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableHLLCountDistinctRule.Config.builder()
        .withOperandSupplier(b0 ->
            b0.operand(Aggregate.class)
                .oneInput(b1 ->
                    b1.operand(RelNode.class)
                        .anyInputs()))
        .build();

    @Override default HLLCountDistinctRule toRule() {
      return new HLLCountDistinctRule(this);
    }
  }
}
