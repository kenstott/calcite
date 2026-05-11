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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link DuckDBIcebergCountStarRule} targeting 63 missed lines.
 * Focuses on matches() branches (GROUP BY, multiple agg calls, non-COUNT,
 * DISTINCT, arguments), onMatch() path when no table scan found,
 * getDuckDBSchema branches (non-JdbcTableScan, non-DuckDBJdbcSchema),
 * findTableScan recursive and RelSubset handling, createCountStarValues,
 * and the INSTANCE singleton.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class DuckDBIcebergCountStarRuleDeepCoverageTest {

  // ========== INSTANCE ==========

  @Test
  void testInstanceNotNull() {
    assertNotNull(DuckDBIcebergCountStarRule.INSTANCE);
    assertEquals("DuckDBIcebergCountStarRule",
        DuckDBIcebergCountStarRule.INSTANCE.toString());
  }

  // ========== matches: GROUP BY ==========

  @Test
  void testMatchesWithGroupBy() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate agg = mock(Aggregate.class);
    when(call.rel(0)).thenReturn(agg);
    when(agg.getGroupSet()).thenReturn(ImmutableBitSet.of(0)); // has GROUP BY

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  // ========== matches: multiple aggregate calls ==========

  @Test
  void testMatchesMultipleAggCalls() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate agg = mock(Aggregate.class);
    when(call.rel(0)).thenReturn(agg);
    when(agg.getGroupSet()).thenReturn(ImmutableBitSet.of());

    AggregateCall call1 = mock(AggregateCall.class);
    AggregateCall call2 = mock(AggregateCall.class);
    when(agg.getAggCallList()).thenReturn(ImmutableList.of(call1, call2));

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  // ========== matches: non-COUNT ==========

  @Test
  void testMatchesNonCountAgg() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate agg = mock(Aggregate.class);
    when(call.rel(0)).thenReturn(agg);
    when(agg.getGroupSet()).thenReturn(ImmutableBitSet.of());

    AggregateCall aggCall = mock(AggregateCall.class);
    org.apache.calcite.sql.SqlAggFunction sumFn = mock(org.apache.calcite.sql.SqlAggFunction.class);
    when(sumFn.getKind()).thenReturn(SqlKind.SUM);
    when(aggCall.getAggregation()).thenReturn(sumFn);
    when(agg.getAggCallList()).thenReturn(ImmutableList.of(aggCall));

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  // ========== matches: COUNT DISTINCT ==========

  @Test
  void testMatchesCountDistinct() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate agg = mock(Aggregate.class);
    when(call.rel(0)).thenReturn(agg);
    when(agg.getGroupSet()).thenReturn(ImmutableBitSet.of());

    AggregateCall aggCall = mock(AggregateCall.class);
    org.apache.calcite.sql.SqlAggFunction countFn =
        mock(org.apache.calcite.sql.SqlAggFunction.class);
    when(countFn.getKind()).thenReturn(SqlKind.COUNT);
    when(aggCall.getAggregation()).thenReturn(countFn);
    when(aggCall.isDistinct()).thenReturn(true);
    when(agg.getAggCallList()).thenReturn(ImmutableList.of(aggCall));

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  // ========== matches: COUNT with args ==========

  @Test
  void testMatchesCountWithArgs() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate agg = mock(Aggregate.class);
    when(call.rel(0)).thenReturn(agg);
    when(agg.getGroupSet()).thenReturn(ImmutableBitSet.of());

    AggregateCall aggCall = mock(AggregateCall.class);
    org.apache.calcite.sql.SqlAggFunction countFn =
        mock(org.apache.calcite.sql.SqlAggFunction.class);
    when(countFn.getKind()).thenReturn(SqlKind.COUNT);
    when(aggCall.getAggregation()).thenReturn(countFn);
    when(aggCall.isDistinct()).thenReturn(false);
    when(aggCall.getArgList()).thenReturn(ImmutableList.of(0)); // has args
    when(agg.getAggCallList()).thenReturn(ImmutableList.of(aggCall));

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  // ========== matches: valid COUNT(*) ==========

  @Test
  void testMatchesValidCountStar() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate agg = mock(Aggregate.class);
    when(call.rel(0)).thenReturn(agg);
    when(agg.getGroupSet()).thenReturn(ImmutableBitSet.of());

    AggregateCall aggCall = mock(AggregateCall.class);
    org.apache.calcite.sql.SqlAggFunction countFn =
        mock(org.apache.calcite.sql.SqlAggFunction.class);
    when(countFn.getKind()).thenReturn(SqlKind.COUNT);
    when(aggCall.getAggregation()).thenReturn(countFn);
    when(aggCall.isDistinct()).thenReturn(false);
    when(aggCall.getArgList()).thenReturn(ImmutableList.<Integer>of());
    when(agg.getAggCallList()).thenReturn(ImmutableList.of(aggCall));

    assertTrue(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  // ========== matches: zero aggregate calls ==========

  @Test
  void testMatchesZeroAggCalls() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate agg = mock(Aggregate.class);
    when(call.rel(0)).thenReturn(agg);
    when(agg.getGroupSet()).thenReturn(ImmutableBitSet.of());
    when(agg.getAggCallList()).thenReturn(ImmutableList.<AggregateCall>of());

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  // ========== findTableScan ==========

  @Test
  void testFindTableScanNull() throws Exception {
    Method m = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "findTableScan", RelNode.class);
    m.setAccessible(true);

    assertNull(m.invoke(DuckDBIcebergCountStarRule.INSTANCE, (RelNode) null));
  }

  @Test
  void testFindTableScanDirectTableScan() throws Exception {
    Method m = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "findTableScan", RelNode.class);
    m.setAccessible(true);

    TableScan scan = mock(TableScan.class);
    when(scan.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    RelNode result = (RelNode) m.invoke(DuckDBIcebergCountStarRule.INSTANCE, scan);
    assertSame(scan, result);
  }

  @Test
  void testFindTableScanDeepInTree() throws Exception {
    Method m = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "findTableScan", RelNode.class);
    m.setAccessible(true);

    TableScan scan = mock(TableScan.class);
    when(scan.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    // Intermediate node
    RelNode intermediate = mock(RelNode.class);
    when(intermediate.getInputs()).thenReturn(Collections.singletonList((RelNode) scan));

    // Root node
    RelNode root = mock(RelNode.class);
    when(root.getInputs()).thenReturn(Collections.singletonList(intermediate));

    RelNode result = (RelNode) m.invoke(DuckDBIcebergCountStarRule.INSTANCE, root);
    assertSame(scan, result);
  }

  @Test
  void testFindTableScanNoScanInTree() throws Exception {
    Method m = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "findTableScan", RelNode.class);
    m.setAccessible(true);

    RelNode leaf = mock(RelNode.class);
    when(leaf.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    RelNode result = (RelNode) m.invoke(DuckDBIcebergCountStarRule.INSTANCE, leaf);
    assertNull(result);
  }

  // ========== getDuckDBSchema ==========

  @Test
  void testGetDuckDBSchemaNonJdbcScan() throws Exception {
    Method m = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "getDuckDBSchema", TableScan.class);
    m.setAccessible(true);

    // Plain TableScan (not JdbcTableScan) => null
    TableScan scan = mock(TableScan.class);
    assertNull(m.invoke(DuckDBIcebergCountStarRule.INSTANCE, scan));
  }

  @Test
  void testGetDuckDBSchemaJdbcScanNonDuckDB() throws Exception {
    Method m = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "getDuckDBSchema", TableScan.class);
    m.setAccessible(true);

    // JdbcTableScan with a regular JdbcSchema (not DuckDBJdbcSchema) => null
    JdbcTableScan scan = mock(JdbcTableScan.class);
    JdbcTable table = mock(JdbcTable.class);
    JdbcSchema schema = mock(JdbcSchema.class);

    // Access the public field directly
    java.lang.reflect.Field tableField = JdbcTableScan.class.getDeclaredField("jdbcTable");
    tableField.setAccessible(true);
    tableField.set(scan, table);

    java.lang.reflect.Field schemaField = JdbcTable.class.getDeclaredField("jdbcSchema");
    schemaField.setAccessible(true);
    schemaField.set(table, schema);

    Object result = m.invoke(DuckDBIcebergCountStarRule.INSTANCE, scan);
    assertNull(result);
  }

  // ========== onMatch: input has no table scan ==========

  @Test
  void testOnMatchNoTableScan() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate agg = mock(Aggregate.class);
    when(call.rel(0)).thenReturn(agg);

    // Input with no table scan
    RelNode input = mock(RelNode.class);
    when(input.getInputs()).thenReturn(Collections.<RelNode>emptyList());
    when(agg.getInput()).thenReturn(input);

    // Should not throw, should return without calling transformTo
    DuckDBIcebergCountStarRule.INSTANCE.onMatch(call);
    verify(call, never()).transformTo(any(RelNode.class));
  }

  // ========== createCountStarValues via reflection ==========

  @Test
  void testCreateCountStarValuesNullOnException() throws Exception {
    Method m = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "createCountStarValues", Aggregate.class, long.class);
    m.setAccessible(true);

    // If cluster/typeFactory is null, should return null gracefully
    Aggregate agg = mock(Aggregate.class);
    RelOptCluster cluster = mock(RelOptCluster.class);
    when(agg.getCluster()).thenReturn(cluster);
    when(cluster.getRexBuilder()).thenThrow(new NullPointerException("test"));

    RelNode result = (RelNode) m.invoke(
        DuckDBIcebergCountStarRule.INSTANCE, agg, 42L);
    assertNull(result, "Should return null on exception");
  }
}
