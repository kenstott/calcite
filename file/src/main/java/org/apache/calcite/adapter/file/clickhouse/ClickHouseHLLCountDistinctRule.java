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
package org.apache.calcite.adapter.file.clickhouse;

import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Rule that replaces COUNT(DISTINCT) operations on JDBC/ClickHouse tables
 * with pre-computed HLL sketch lookups.
 *
 * <p>This is the ClickHouse-specific version of the HLL rule that matches
 * JDBC table scan patterns and rewrites them to VALUES nodes containing
 * the HLL estimate, avoiding expensive DISTINCT computation in ClickHouse.
 *
 * <p>ClickHouse natively supports approximate count distinct via {@code uniq()},
 * {@code uniqExact()}, and {@code uniqHLL12()} functions. This rule provides
 * an additional optimization layer by using pre-computed HLL sketches from
 * the statistics cache, returning results instantly without any ClickHouse query.
 */
public class ClickHouseHLLCountDistinctRule extends RelOptRule {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ClickHouseHLLCountDistinctRule.class);

  public static final ClickHouseHLLCountDistinctRule INSTANCE =
      new ClickHouseHLLCountDistinctRule();

  @SuppressWarnings("deprecation")
  private ClickHouseHLLCountDistinctRule() {
    super(
        operand(Aggregate.class, any()),
        "ClickHouseHLLCountDistinctRule");
  }

  @Override public boolean matches(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);

    // Only handle simple aggregates without GROUP BY
    if (!aggregate.getGroupSet().isEmpty()) {
      return false;
    }

    // Check if this has COUNT(DISTINCT)
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (aggCall.getAggregation().getKind() == SqlKind.COUNT && aggCall.isDistinct()) {
        LOGGER.debug("[CH-HLL] Found COUNT(DISTINCT) in ClickHouse query");
        return true;
      }
    }

    return false;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode input = aggregate.getInput();

    LOGGER.debug("[CH-HLL] Rule matched on aggregate: {}", aggregate);

    // Find the table scan in the input tree
    TableInfo tableInfo = findTableInfo(input);
    if (tableInfo == null) {
      LOGGER.debug("[CH-HLL] Could not find table information in query tree");
      return;
    }

    LOGGER.debug("[CH-HLL] Found table: schema='{}', table='{}'",
                tableInfo.schemaName, tableInfo.tableName);

    // Try to get HLL estimates for each COUNT(DISTINCT)
    List<Long> hllEstimates = new ArrayList<>();
    boolean hasOptimizableCountDistinct = false;

    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (aggCall.getAggregation().getKind() == SqlKind.COUNT && aggCall.isDistinct()) {
        Long estimate = getHLLEstimate(tableInfo, input, aggCall);
        if (estimate != null) {
          hasOptimizableCountDistinct = true;
          hllEstimates.add(estimate);
          LOGGER.info("[CH-HLL] Found HLL estimate for COUNT(DISTINCT): {}", estimate);
        } else {
          hllEstimates.add(null);
        }
      } else {
        hllEstimates.add(null);
      }
    }

    if (!hasOptimizableCountDistinct) {
      LOGGER.debug("[CH-HLL] No HLL estimates available - query will be pushed to ClickHouse");
      return;
    }

    // Create VALUES node with HLL estimates
    RelNode valuesNode = createHLLValues(aggregate, hllEstimates);
    if (valuesNode != null) {
      LOGGER.info("[CH-HLL] Replaced COUNT(DISTINCT) with HLL estimates: {}", hllEstimates);
      call.transformTo(valuesNode);
    }
  }

  /**
   * Helper class to hold table information extracted from scan nodes.
   */
  private static class TableInfo {
    final String schemaName;
    final String tableName;

    TableInfo(String schemaName, String tableName) {
      this.schemaName = schemaName;
      this.tableName = tableName;
    }
  }

  /**
   * Finds table information from the input tree, handling both JDBC and file adapter patterns.
   */
  private TableInfo findTableInfo(RelNode node) {
    if (node == null) {
      return null;
    }

    // Handle RelSubset nodes from Volcano planner
    if (node.getClass().getName().contains("RelSubset")) {
      try {
        java.lang.reflect.Method getBest = node.getClass().getMethod("getBest");
        RelNode best = (RelNode) getBest.invoke(node);
        if (best != null && best != node) {
          return findTableInfo(best);
        }

        java.lang.reflect.Method getOriginal = node.getClass().getMethod("getOriginal");
        RelNode original = (RelNode) getOriginal.invoke(node);
        if (original != null && original != node) {
          return findTableInfo(original);
        }
      } catch (Exception e) {
        // Silently continue
      }
    }

    // Handle JDBC table scan
    if (node instanceof JdbcTableScan) {
      JdbcTableScan scan = (JdbcTableScan) node;
      List<String> qualifiedName = scan.getTable().getQualifiedName();
      String schemaName = qualifiedName.size() >= 2
          ? qualifiedName.get(qualifiedName.size() - 2) : "";
      String tableName = qualifiedName.get(qualifiedName.size() - 1);
      return new TableInfo(schemaName, tableName);
    }

    // Handle file adapter table scan
    if (node instanceof org.apache.calcite.rel.core.TableScan) {
      org.apache.calcite.rel.core.TableScan scan =
          (org.apache.calcite.rel.core.TableScan) node;
      List<String> qualifiedName = scan.getTable().getQualifiedName();
      String schemaName = qualifiedName.size() >= 2
          ? qualifiedName.get(qualifiedName.size() - 2) : "";
      String tableName = qualifiedName.get(qualifiedName.size() - 1);
      return new TableInfo(schemaName, tableName);
    }

    // Recursively search through all inputs
    for (RelNode input : node.getInputs()) {
      TableInfo info = findTableInfo(input);
      if (info != null) {
        return info;
      }
    }

    return null;
  }

  private Long getHLLEstimate(TableInfo tableInfo, RelNode input, AggregateCall aggCall) {
    try {
      if (aggCall.getArgList().isEmpty()) {
        return null;
      }

      int fieldIndex = aggCall.getArgList().get(0);
      String columnName = input.getRowType().getFieldNames().get(fieldIndex);

      // Remove quotes from table name if present
      String cleanTableName = tableInfo.tableName.replace("\"", "");

      LOGGER.debug("[CH-HLL] Looking for HLL sketch: schema='{}', table='{}', column='{}'",
                   tableInfo.schemaName, cleanTableName, columnName);

      HLLSketchCache cache = HLLSketchCache.getInstance();
      HyperLogLogSketch sketch =
          cache.getSketch(tableInfo.schemaName, cleanTableName, columnName);

      if (sketch != null) {
        long estimate = sketch.getEstimate();
        LOGGER.info("[CH-HLL] Found HLL sketch for {}.{}.{}: estimate={}",
                   tableInfo.schemaName, cleanTableName, columnName, estimate);
        return estimate;
      } else {
        LOGGER.debug("[CH-HLL] No HLL sketch found for {}.{}.{}",
                   tableInfo.schemaName, cleanTableName, columnName);
      }

    } catch (Exception e) {
      LOGGER.debug("[CH-HLL] Failed to get HLL estimate: {}", e.getMessage());
    }

    return null;
  }

  private RelNode createHLLValues(Aggregate aggregate, List<Long> hllEstimates) {
    try {
      RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
      RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();

      List<RexLiteral> values = new ArrayList<>();
      List<AggregateCall> aggCalls = aggregate.getAggCallList();

      for (int i = 0; i < aggCalls.size(); i++) {
        Long estimate = hllEstimates.get(i);
        if (estimate != null) {
          values.add(rexBuilder.makeBigintLiteral(BigDecimal.valueOf(estimate)));
        } else {
          values.add(rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.BIGINT)));
        }
      }

      RelDataType rowType = aggregate.getRowType();

      return EnumerableValues.create(
          aggregate.getCluster(),
          rowType,
          ImmutableList.of(ImmutableList.copyOf(values)));

    } catch (Exception e) {
      LOGGER.error("[CH-HLL] Failed to create HLL VALUES node", e);
      return null;
    }
  }
}
