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

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Coverage unit tests for {@link ClickHouseIcebergCountStarRule}
 * and {@link ClickHouseHLLCountDistinctRule}.
 *
 * <p>Tests rule matching logic, onMatch behavior, and helper methods
 * using mocked RelNode trees without requiring a running ClickHouse server.
 */
@Tag("unit")
class ClickHouseRulesCoverageTest {

  // ============================================================
  // ClickHouseIcebergCountStarRule tests
  // ============================================================

  @Test void testIcebergCountStarRuleInstance() {
    assertNotNull(ClickHouseIcebergCountStarRule.INSTANCE);
    assertEquals("ClickHouseIcebergCountStarRule",
        ClickHouseIcebergCountStarRule.INSTANCE.toString());
  }

  @Test void testIcebergCountStarRuleOperandType() {
    assertEquals(Aggregate.class,
        ClickHouseIcebergCountStarRule.INSTANCE.getOperand().getMatchedClass());
  }

  @Test void testIcebergCountStarMatchesCountStarAggregate() {
    // Create mocked aggregate with COUNT(*) - no group by, single call
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());

    AggregateCall countStar = mock(AggregateCall.class);
    when(countStar.getAggregation()).thenReturn(SqlStdOperatorTable.COUNT);
    when(countStar.isDistinct()).thenReturn(false);
    when(countStar.getArgList()).thenReturn(ImmutableList.<Integer>of());

    when(aggregate.getAggCallList())
        .thenReturn(ImmutableList.of(countStar));

    boolean matches = ClickHouseIcebergCountStarRule.INSTANCE.matches(call);
    assertTrue(matches, "Should match COUNT(*) aggregate");
  }

  @Test void testIcebergCountStarDoesNotMatchWithGroupBy() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of(0));

    boolean matches = ClickHouseIcebergCountStarRule.INSTANCE.matches(call);
    assertFalse(matches, "Should not match aggregate with GROUP BY");
  }

  @Test void testIcebergCountStarDoesNotMatchMultipleAggCalls() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());

    AggregateCall call1 = mock(AggregateCall.class);
    AggregateCall call2 = mock(AggregateCall.class);
    when(aggregate.getAggCallList())
        .thenReturn(ImmutableList.of(call1, call2));

    boolean matches = ClickHouseIcebergCountStarRule.INSTANCE.matches(call);
    assertFalse(matches, "Should not match with multiple aggregate calls");
  }

  @Test void testIcebergCountStarDoesNotMatchSum() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());

    AggregateCall sumCall = mock(AggregateCall.class);
    when(sumCall.getAggregation()).thenReturn(SqlStdOperatorTable.SUM);
    when(aggregate.getAggCallList())
        .thenReturn(ImmutableList.of(sumCall));

    boolean matches = ClickHouseIcebergCountStarRule.INSTANCE.matches(call);
    assertFalse(matches, "Should not match SUM aggregate");
  }

  @Test void testIcebergCountStarDoesNotMatchCountDistinct() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());

    AggregateCall countDistinct = mock(AggregateCall.class);
    when(countDistinct.getAggregation()).thenReturn(SqlStdOperatorTable.COUNT);
    when(countDistinct.isDistinct()).thenReturn(true);
    when(aggregate.getAggCallList())
        .thenReturn(ImmutableList.of(countDistinct));

    boolean matches = ClickHouseIcebergCountStarRule.INSTANCE.matches(call);
    assertFalse(matches, "Should not match COUNT(DISTINCT ...)");
  }

  @Test void testIcebergCountStarDoesNotMatchCountWithArgs() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());

    AggregateCall countWithArg = mock(AggregateCall.class);
    when(countWithArg.getAggregation()).thenReturn(SqlStdOperatorTable.COUNT);
    when(countWithArg.isDistinct()).thenReturn(false);
    when(countWithArg.getArgList()).thenReturn(ImmutableList.of(0));
    when(aggregate.getAggCallList())
        .thenReturn(ImmutableList.of(countWithArg));

    boolean matches = ClickHouseIcebergCountStarRule.INSTANCE.matches(call);
    assertFalse(matches, "Should not match COUNT(column)");
  }

  @Test void testIcebergCountStarOnMatchNoTableScan() {
    // onMatch with an aggregate whose input has no table scan
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);
    RelNode input = mock(RelNode.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getInput()).thenReturn(input);
    when(input.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    // Should not throw - just return without transformation
    ClickHouseIcebergCountStarRule.INSTANCE.onMatch(call);
  }

  @Test void testIcebergCountStarOnMatchNonJdbcTableScan() {
    // onMatch with a table scan that is not JdbcTableScan
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);
    TableScan tableScan = mock(TableScan.class);
    RelOptTable relOptTable = mock(RelOptTable.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getInput()).thenReturn(tableScan);
    when(tableScan.getInputs()).thenReturn(Collections.<RelNode>emptyList());
    when(tableScan.getTable()).thenReturn(relOptTable);
    when(relOptTable.getQualifiedName())
        .thenReturn(Arrays.asList("root", "myschema", "mytable"));

    // Should not throw - just return without transformation
    ClickHouseIcebergCountStarRule.INSTANCE.onMatch(call);
  }

  @Test void testIcebergCountStarFindTableScanNull() throws Exception {
    java.lang.reflect.Method method =
        ClickHouseIcebergCountStarRule.class.getDeclaredMethod("findTableScan", RelNode.class);
    method.setAccessible(true);

    Object result = method.invoke(ClickHouseIcebergCountStarRule.INSTANCE, (RelNode) null);
    assertNull(result, "findTableScan(null) should return null");
  }

  @Test void testIcebergCountStarFindTableScanDirectScan() throws Exception {
    java.lang.reflect.Method method =
        ClickHouseIcebergCountStarRule.class.getDeclaredMethod("findTableScan", RelNode.class);
    method.setAccessible(true);

    TableScan scan = mock(TableScan.class);
    Object result = method.invoke(ClickHouseIcebergCountStarRule.INSTANCE, scan);
    assertEquals(scan, result, "Should return TableScan directly");
  }

  @Test void testIcebergCountStarFindTableScanInChildren() throws Exception {
    java.lang.reflect.Method method =
        ClickHouseIcebergCountStarRule.class.getDeclaredMethod("findTableScan", RelNode.class);
    method.setAccessible(true);

    TableScan scan = mock(TableScan.class);
    RelNode parent = mock(RelNode.class);
    when(parent.getInputs()).thenReturn(Collections.singletonList((RelNode) scan));

    Object result = method.invoke(ClickHouseIcebergCountStarRule.INSTANCE, parent);
    assertEquals(scan, result, "Should find TableScan in children");
  }

  @Test void testIcebergCountStarGetClickHouseSchemaReturnsNull() throws Exception {
    java.lang.reflect.Method method =
        ClickHouseIcebergCountStarRule.class.getDeclaredMethod("getClickHouseSchema", TableScan.class);
    method.setAccessible(true);

    // Non-JdbcTableScan should return null
    TableScan scan = mock(TableScan.class);
    Object result = method.invoke(ClickHouseIcebergCountStarRule.INSTANCE, scan);
    assertNull(result, "Non-JdbcTableScan should return null");
  }

  // ============================================================
  // ClickHouseHLLCountDistinctRule tests
  // ============================================================

  @Test void testHLLRuleInstance() {
    assertNotNull(ClickHouseHLLCountDistinctRule.INSTANCE);
    assertEquals("ClickHouseHLLCountDistinctRule",
        ClickHouseHLLCountDistinctRule.INSTANCE.toString());
  }

  @Test void testHLLRuleOperandType() {
    assertEquals(Aggregate.class,
        ClickHouseHLLCountDistinctRule.INSTANCE.getOperand().getMatchedClass());
  }

  @Test void testHLLMatchesCountDistinct() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());

    AggregateCall countDistinct = mock(AggregateCall.class);
    when(countDistinct.getAggregation()).thenReturn(SqlStdOperatorTable.COUNT);
    when(countDistinct.isDistinct()).thenReturn(true);
    when(aggregate.getAggCallList())
        .thenReturn(ImmutableList.of(countDistinct));

    boolean matches = ClickHouseHLLCountDistinctRule.INSTANCE.matches(call);
    assertTrue(matches, "Should match COUNT(DISTINCT ...)");
  }

  @Test void testHLLDoesNotMatchWithGroupBy() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of(0, 1));

    boolean matches = ClickHouseHLLCountDistinctRule.INSTANCE.matches(call);
    assertFalse(matches, "Should not match with GROUP BY");
  }

  @Test void testHLLDoesNotMatchPlainCount() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());

    AggregateCall plainCount = mock(AggregateCall.class);
    when(plainCount.getAggregation()).thenReturn(SqlStdOperatorTable.COUNT);
    when(plainCount.isDistinct()).thenReturn(false);
    when(aggregate.getAggCallList())
        .thenReturn(ImmutableList.of(plainCount));

    boolean matches = ClickHouseHLLCountDistinctRule.INSTANCE.matches(call);
    assertFalse(matches, "Should not match plain COUNT (non-distinct)");
  }

  @Test void testHLLDoesNotMatchSumAggregate() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());

    AggregateCall sumCall = mock(AggregateCall.class);
    when(sumCall.getAggregation()).thenReturn(SqlStdOperatorTable.SUM);
    when(sumCall.isDistinct()).thenReturn(false);
    when(aggregate.getAggCallList())
        .thenReturn(ImmutableList.of(sumCall));

    boolean matches = ClickHouseHLLCountDistinctRule.INSTANCE.matches(call);
    assertFalse(matches, "Should not match SUM aggregate");
  }

  @Test void testHLLOnMatchNoTableInfo() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);
    RelNode input = mock(RelNode.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getInput()).thenReturn(input);
    when(input.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    // The findTableInfo should return null, so onMatch should return without transformation
    ClickHouseHLLCountDistinctRule.INSTANCE.onMatch(call);
  }

  @Test void testHLLFindTableInfoNull() throws Exception {
    java.lang.reflect.Method method =
        ClickHouseHLLCountDistinctRule.class.getDeclaredMethod("findTableInfo", RelNode.class);
    method.setAccessible(true);

    Object result = method.invoke(ClickHouseHLLCountDistinctRule.INSTANCE, (RelNode) null);
    assertNull(result, "findTableInfo(null) should return null");
  }

  @Test void testHLLFindTableInfoFromTableScan() throws Exception {
    java.lang.reflect.Method method =
        ClickHouseHLLCountDistinctRule.class.getDeclaredMethod("findTableInfo", RelNode.class);
    method.setAccessible(true);

    TableScan scan = mock(TableScan.class);
    RelOptTable table = mock(RelOptTable.class);
    when(scan.getTable()).thenReturn(table);
    when(table.getQualifiedName()).thenReturn(Arrays.asList("root", "myschema", "mytable"));

    Object result = method.invoke(ClickHouseHLLCountDistinctRule.INSTANCE, (RelNode) scan);
    assertNotNull(result, "Should find table info from TableScan");
  }

  @Test void testHLLFindTableInfoRecursive() throws Exception {
    java.lang.reflect.Method method =
        ClickHouseHLLCountDistinctRule.class.getDeclaredMethod("findTableInfo", RelNode.class);
    method.setAccessible(true);

    TableScan scan = mock(TableScan.class);
    RelOptTable table = mock(RelOptTable.class);
    when(scan.getTable()).thenReturn(table);
    when(table.getQualifiedName()).thenReturn(Arrays.asList("myschema", "mytable"));

    RelNode parent = mock(RelNode.class);
    when(parent.getInputs()).thenReturn(Collections.singletonList((RelNode) scan));

    Object result = method.invoke(ClickHouseHLLCountDistinctRule.INSTANCE, (RelNode) parent);
    assertNotNull(result, "Should find table info recursively through children");
  }

  @Test void testHLLGetHLLEstimateNoArgs() throws Exception {
    java.lang.reflect.Method getEstimate =
        ClickHouseHLLCountDistinctRule.class.getDeclaredMethod(
            "getHLLEstimate",
            getTableInfoClass(),
            RelNode.class,
            AggregateCall.class);
    getEstimate.setAccessible(true);

    Object tableInfo = createTableInfo("myschema", "mytable");
    RelNode input = mock(RelNode.class);
    AggregateCall aggCall = mock(AggregateCall.class);
    when(aggCall.getArgList()).thenReturn(ImmutableList.<Integer>of());

    Object result = getEstimate.invoke(
        ClickHouseHLLCountDistinctRule.INSTANCE, tableInfo, input, aggCall);
    assertNull(result, "Empty arg list should return null");
  }

  @Test void testHLLOnMatchWithTableScanNoHLLSketch() {
    // Test onMatch when table scan is found but no HLL sketch is cached
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate aggregate = mock(Aggregate.class);
    TableScan scan = mock(TableScan.class);
    RelOptTable table = mock(RelOptTable.class);
    RelDataType rowType = mock(RelDataType.class);

    when(call.rel(0)).thenReturn(aggregate);
    when(aggregate.getInput()).thenReturn(scan);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());
    when(scan.getTable()).thenReturn(table);
    when(scan.getInputs()).thenReturn(Collections.<RelNode>emptyList());
    when(table.getQualifiedName()).thenReturn(Arrays.asList("schema1", "table1"));

    AggregateCall countDistinct = mock(AggregateCall.class);
    when(countDistinct.getAggregation()).thenReturn(SqlStdOperatorTable.COUNT);
    when(countDistinct.isDistinct()).thenReturn(true);
    when(countDistinct.getArgList()).thenReturn(ImmutableList.of(0));
    when(aggregate.getAggCallList())
        .thenReturn(ImmutableList.of(countDistinct));

    when(aggregate.getInput()).thenReturn(scan);
    when(scan.getRowType()).thenReturn(rowType);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1", "col2"));

    // onMatch should complete without transformation (no HLL sketch available)
    ClickHouseHLLCountDistinctRule.INSTANCE.onMatch(call);
  }

  // ---- Helper methods ----

  private Class<?> getTableInfoClass() throws Exception {
    Class<?>[] innerClasses = ClickHouseHLLCountDistinctRule.class.getDeclaredClasses();
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("TableInfo")) {
        return c;
      }
    }
    throw new RuntimeException("TableInfo inner class not found");
  }

  private Object createTableInfo(String schemaName, String tableName) throws Exception {
    Class<?> tableInfoClass = getTableInfoClass();
    java.lang.reflect.Constructor<?> ctor =
        tableInfoClass.getDeclaredConstructor(String.class, String.class);
    ctor.setAccessible(true);
    return ctor.newInstance(schemaName, tableName);
  }

  private static void assertNull(Object obj, String msg) {
    assertEquals(null, obj, msg);
  }
}
