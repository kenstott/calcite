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

import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptCluster;
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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Coverage tests for {@link DuckDBIcebergCountStarRule} and {@link DuckDBHLLCountDistinctRule}.
 * Tests rule matching logic, helper methods, and RelNode tree traversal using mock objects.
 */
@Tag("unit")
public class DuckDBRulesCoverageTest {

  @TempDir
  Path tempDir;

  // ====================================================================
  // DuckDBIcebergCountStarRule - INSTANCE
  // ====================================================================

  @Test
  void testIcebergCountStarRuleInstance() {
    assertNotNull(DuckDBIcebergCountStarRule.INSTANCE);
  }

  // ====================================================================
  // DuckDBIcebergCountStarRule - matches() tests
  // ====================================================================

  @Test
  void testIcebergMatchesSimpleCountStar() {
    RelOptRuleCall call = mockCallWithAggregate(
        createMockAggregate(
            ImmutableBitSet.of(),
            Collections.singletonList(createCountStarAggCall())));

    assertTrue(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  @Test
  void testIcebergMatchesRejectsGroupBy() {
    RelOptRuleCall call = mockCallWithAggregate(
        createMockAggregate(
            ImmutableBitSet.of(0),
            Collections.singletonList(createCountStarAggCall())));

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  @Test
  void testIcebergMatchesRejectsMultipleAggCalls() {
    List<AggregateCall> aggCalls = new ArrayList<AggregateCall>();
    aggCalls.add(createCountStarAggCall());
    aggCalls.add(createCountStarAggCall());

    RelOptRuleCall call = mockCallWithAggregate(
        createMockAggregate(ImmutableBitSet.of(), aggCalls));

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  @Test
  void testIcebergMatchesRejectsNoAggCalls() {
    RelOptRuleCall call = mockCallWithAggregate(
        createMockAggregate(
            ImmutableBitSet.of(),
            Collections.<AggregateCall>emptyList()));

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  @Test
  void testIcebergMatchesRejectsDistinctCount() {
    RelOptRuleCall call = mockCallWithAggregate(
        createMockAggregate(
            ImmutableBitSet.of(),
            Collections.singletonList(createDistinctCountAggCall())));

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  @Test
  void testIcebergMatchesRejectsCountWithArgs() {
    AggregateCall countWithArgs = createCountWithArgsAggCall();
    RelOptRuleCall call = mockCallWithAggregate(
        createMockAggregate(
            ImmutableBitSet.of(),
            Collections.singletonList(countWithArgs)));

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  @Test
  void testIcebergMatchesRejectsNonCountFunction() {
    AggregateCall sumCall = createSumAggCall();
    RelOptRuleCall call = mockCallWithAggregate(
        createMockAggregate(
            ImmutableBitSet.of(),
            Collections.singletonList(sumCall)));

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  // ====================================================================
  // DuckDBIcebergCountStarRule - findTableScan (via reflection)
  // ====================================================================

  @Test
  void testIcebergFindTableScanNull() throws Exception {
    Method method = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "findTableScan", RelNode.class);
    method.setAccessible(true);

    Object result = method.invoke(DuckDBIcebergCountStarRule.INSTANCE, (RelNode) null);
    assertNull(result);
  }

  @Test
  void testIcebergFindTableScanDirectMatch() throws Exception {
    Method method = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "findTableScan", RelNode.class);
    method.setAccessible(true);

    TableScan mockScan = mock(TableScan.class);
    Object result = method.invoke(DuckDBIcebergCountStarRule.INSTANCE, mockScan);
    assertEquals(mockScan, result);
  }

  @Test
  void testIcebergFindTableScanInInputTree() throws Exception {
    Method method = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "findTableScan", RelNode.class);
    method.setAccessible(true);

    // Create a relay node that has a TableScan as input
    TableScan mockScan = mock(TableScan.class);
    RelNode parent = mock(RelNode.class);
    when(parent.getInputs()).thenReturn(Collections.singletonList((RelNode) mockScan));

    Object result = method.invoke(DuckDBIcebergCountStarRule.INSTANCE, parent);
    assertEquals(mockScan, result);
  }

  @Test
  void testIcebergFindTableScanNoMatch() throws Exception {
    Method method = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "findTableScan", RelNode.class);
    method.setAccessible(true);

    RelNode mockNode = mock(RelNode.class);
    when(mockNode.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    Object result = method.invoke(DuckDBIcebergCountStarRule.INSTANCE, mockNode);
    assertNull(result);
  }

  // ====================================================================
  // DuckDBIcebergCountStarRule - getDuckDBSchema (via reflection)
  // ====================================================================

  @Test
  void testIcebergGetDuckDBSchemaNonJdbcScan() throws Exception {
    Method method = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "getDuckDBSchema", TableScan.class);
    method.setAccessible(true);

    TableScan mockScan = mock(TableScan.class);
    Object result = method.invoke(DuckDBIcebergCountStarRule.INSTANCE, mockScan);
    assertNull(result);
  }

  @Test
  void testIcebergGetDuckDBSchemaJdbcScanNonDuckDB() throws Exception {
    Method method = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "getDuckDBSchema", TableScan.class);
    method.setAccessible(true);

    JdbcTableScan mockJdbcScan = mock(JdbcTableScan.class);
    JdbcTable mockJdbcTable = mock(JdbcTable.class);
    JdbcSchema mockJdbcSchema = mock(JdbcSchema.class);

    // Set public fields via reflection
    java.lang.reflect.Field tableField = JdbcTableScan.class.getDeclaredField("jdbcTable");
    tableField.setAccessible(true);
    tableField.set(mockJdbcScan, mockJdbcTable);

    java.lang.reflect.Field schemaField = JdbcTable.class.getDeclaredField("jdbcSchema");
    schemaField.setAccessible(true);
    schemaField.set(mockJdbcTable, mockJdbcSchema);

    Object result = method.invoke(DuckDBIcebergCountStarRule.INSTANCE, mockJdbcScan);
    assertNull(result);
  }

  // ====================================================================
  // DuckDBIcebergCountStarRule - createCountStarValues (via reflection)
  // ====================================================================

  @Test
  void testIcebergCreateCountStarValuesWithPartialMock() throws Exception {
    Method method = DuckDBIcebergCountStarRule.class.getDeclaredMethod(
        "createCountStarValues", Aggregate.class, long.class);
    method.setAccessible(true);

    // With a partial mock cluster that cannot provide traitSetOf, the method
    // catches the exception internally and returns null
    Aggregate mockAggregate = createMockAggregateWithCluster(100L);

    Object result = method.invoke(DuckDBIcebergCountStarRule.INSTANCE, mockAggregate, 100L);
    // The method catches NPE from EnumerableValues.create due to mock cluster
    // and returns null (logged as error)
    assertNull(result);
  }

  // ====================================================================
  // DuckDBHLLCountDistinctRule - INSTANCE
  // ====================================================================

  @Test
  void testHLLRuleInstance() {
    assertNotNull(DuckDBHLLCountDistinctRule.INSTANCE);
  }

  // ====================================================================
  // DuckDBHLLCountDistinctRule - matches() tests
  // ====================================================================

  @Test
  void testHLLMatchesDistinctCount() {
    RelOptRuleCall call = mockCallWithAggregate(
        createMockAggregate(
            ImmutableBitSet.of(),
            Collections.singletonList(createDistinctCountAggCall())));

    assertTrue(DuckDBHLLCountDistinctRule.INSTANCE.matches(call));
  }

  @Test
  void testHLLMatchesRejectsGroupBy() {
    RelOptRuleCall call = mockCallWithAggregate(
        createMockAggregate(
            ImmutableBitSet.of(0),
            Collections.singletonList(createDistinctCountAggCall())));

    assertFalse(DuckDBHLLCountDistinctRule.INSTANCE.matches(call));
  }

  @Test
  void testHLLMatchesRejectsNonDistinctCount() {
    RelOptRuleCall call = mockCallWithAggregate(
        createMockAggregate(
            ImmutableBitSet.of(),
            Collections.singletonList(createCountStarAggCall())));

    assertFalse(DuckDBHLLCountDistinctRule.INSTANCE.matches(call));
  }

  @Test
  void testHLLMatchesRejectsSumFunction() {
    RelOptRuleCall call = mockCallWithAggregate(
        createMockAggregate(
            ImmutableBitSet.of(),
            Collections.singletonList(createSumAggCall())));

    assertFalse(DuckDBHLLCountDistinctRule.INSTANCE.matches(call));
  }

  @Test
  void testHLLMatchesNoAggCallsRejects() {
    RelOptRuleCall call = mockCallWithAggregate(
        createMockAggregate(
            ImmutableBitSet.of(),
            Collections.<AggregateCall>emptyList()));

    assertFalse(DuckDBHLLCountDistinctRule.INSTANCE.matches(call));
  }

  @Test
  void testHLLMatchesMultipleCallsOneDistinct() {
    List<AggregateCall> aggCalls = new ArrayList<AggregateCall>();
    aggCalls.add(createCountStarAggCall());
    aggCalls.add(createDistinctCountAggCall());

    RelOptRuleCall call = mockCallWithAggregate(
        createMockAggregate(ImmutableBitSet.of(), aggCalls));

    assertTrue(DuckDBHLLCountDistinctRule.INSTANCE.matches(call));
  }

  // ====================================================================
  // DuckDBHLLCountDistinctRule - findTableInfo (via reflection)
  // ====================================================================

  @Test
  void testHLLFindTableInfoNull() throws Exception {
    Method method = DuckDBHLLCountDistinctRule.class.getDeclaredMethod(
        "findTableInfo", RelNode.class);
    method.setAccessible(true);

    Object result = method.invoke(DuckDBHLLCountDistinctRule.INSTANCE, (RelNode) null);
    assertNull(result);
  }

  @Test
  void testHLLFindTableInfoDirectTableScan() throws Exception {
    Method method = DuckDBHLLCountDistinctRule.class.getDeclaredMethod(
        "findTableInfo", RelNode.class);
    method.setAccessible(true);

    TableScan mockScan = mock(TableScan.class);
    RelOptTable mockTable = mock(RelOptTable.class);
    when(mockScan.getTable()).thenReturn(mockTable);
    when(mockTable.getQualifiedName()).thenReturn(Arrays.asList("catalog", "schema", "table"));

    Object result = method.invoke(DuckDBHLLCountDistinctRule.INSTANCE, mockScan);
    assertNotNull(result);
  }

  @Test
  void testHLLFindTableInfoJdbcTableScan() throws Exception {
    Method method = DuckDBHLLCountDistinctRule.class.getDeclaredMethod(
        "findTableInfo", RelNode.class);
    method.setAccessible(true);

    JdbcTableScan mockJdbcScan = mock(JdbcTableScan.class);
    RelOptTable mockTable = mock(RelOptTable.class);
    when(mockJdbcScan.getTable()).thenReturn(mockTable);
    when(mockTable.getQualifiedName()).thenReturn(Arrays.asList("schema", "table"));

    Object result = method.invoke(DuckDBHLLCountDistinctRule.INSTANCE, mockJdbcScan);
    assertNotNull(result);
  }

  @Test
  void testHLLFindTableInfoInInputTree() throws Exception {
    Method method = DuckDBHLLCountDistinctRule.class.getDeclaredMethod(
        "findTableInfo", RelNode.class);
    method.setAccessible(true);

    TableScan mockScan = mock(TableScan.class);
    RelOptTable mockTable = mock(RelOptTable.class);
    when(mockScan.getTable()).thenReturn(mockTable);
    when(mockTable.getQualifiedName()).thenReturn(Arrays.asList("schema", "my_table"));

    RelNode parent = mock(RelNode.class);
    when(parent.getInputs()).thenReturn(Collections.singletonList((RelNode) mockScan));

    Object result = method.invoke(DuckDBHLLCountDistinctRule.INSTANCE, parent);
    assertNotNull(result);
  }

  @Test
  void testHLLFindTableInfoNoMatch() throws Exception {
    Method method = DuckDBHLLCountDistinctRule.class.getDeclaredMethod(
        "findTableInfo", RelNode.class);
    method.setAccessible(true);

    RelNode mockNode = mock(RelNode.class);
    when(mockNode.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    Object result = method.invoke(DuckDBHLLCountDistinctRule.INSTANCE, mockNode);
    assertNull(result);
  }

  // ====================================================================
  // DuckDBHLLCountDistinctRule - getHLLEstimate (via reflection)
  // ====================================================================

  @Test
  void testHLLGetEstimateNoArgs() throws Exception {
    Method method = DuckDBHLLCountDistinctRule.class.getDeclaredMethod(
        "getHLLEstimate",
        getDuckDBTableInfoClass(),
        RelNode.class,
        AggregateCall.class);
    method.setAccessible(true);

    Object tableInfo = createTableInfo("schema1", "table1");
    RelNode mockInput = mock(RelNode.class);
    AggregateCall emptyArgs = createCountStarAggCall(); // no args

    Object result = method.invoke(DuckDBHLLCountDistinctRule.INSTANCE,
        tableInfo, mockInput, emptyArgs);
    assertNull(result);
  }

  @Test
  void testHLLGetEstimateWithSketchInCache() throws Exception {
    // Put a sketch in the cache
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(42L);
    cache.putSketch("test_schema", "test_table", "test_col", sketch);

    Method method = DuckDBHLLCountDistinctRule.class.getDeclaredMethod(
        "getHLLEstimate",
        getDuckDBTableInfoClass(),
        RelNode.class,
        AggregateCall.class);
    method.setAccessible(true);

    Object tableInfo = createTableInfo("test_schema", "test_table");

    // Create mock input with row type
    RelNode mockInput = mock(RelNode.class);
    RelDataType mockRowType = mock(RelDataType.class);
    when(mockInput.getRowType()).thenReturn(mockRowType);
    when(mockRowType.getFieldNames()).thenReturn(Arrays.asList("test_col"));

    AggregateCall distinctCount = createDistinctCountAggCall();

    Object result = method.invoke(DuckDBHLLCountDistinctRule.INSTANCE,
        tableInfo, mockInput, distinctCount);

    assertNotNull(result);
    assertEquals(42L, result);
  }

  @Test
  void testHLLGetEstimateNoSketchInCache() throws Exception {
    Method method = DuckDBHLLCountDistinctRule.class.getDeclaredMethod(
        "getHLLEstimate",
        getDuckDBTableInfoClass(),
        RelNode.class,
        AggregateCall.class);
    method.setAccessible(true);

    Object tableInfo = createTableInfo("nonexistent_schema", "nonexistent_table");

    RelNode mockInput = mock(RelNode.class);
    RelDataType mockRowType = mock(RelDataType.class);
    when(mockInput.getRowType()).thenReturn(mockRowType);
    when(mockRowType.getFieldNames()).thenReturn(Arrays.asList("some_column"));

    AggregateCall distinctCount = createDistinctCountAggCall();

    Object result = method.invoke(DuckDBHLLCountDistinctRule.INSTANCE,
        tableInfo, mockInput, distinctCount);

    assertNull(result);
  }

  // ====================================================================
  // DuckDBHLLCountDistinctRule - createHLLValues (via reflection)
  // ====================================================================

  @Test
  void testHLLCreateValuesWithEstimatesPartialMock() throws Exception {
    Method method = DuckDBHLLCountDistinctRule.class.getDeclaredMethod(
        "createHLLValues", Aggregate.class, List.class);
    method.setAccessible(true);

    Aggregate mockAggregate = createMockAggregateWithCluster(42L);
    List<Long> estimates = new ArrayList<Long>();
    estimates.add(42L);

    // With a partial mock cluster that cannot provide traitSetOf,
    // EnumerableValues.create fails and the method catches the exception
    Object result = method.invoke(DuckDBHLLCountDistinctRule.INSTANCE,
        mockAggregate, estimates);
    assertNull(result);
  }

  @Test
  void testHLLCreateValuesWithNullEstimate() throws Exception {
    Method method = DuckDBHLLCountDistinctRule.class.getDeclaredMethod(
        "createHLLValues", Aggregate.class, List.class);
    method.setAccessible(true);

    Aggregate mockAggregate = createMockAggregateWithCluster(42L);
    List<Long> estimates = new ArrayList<Long>();
    estimates.add(null);

    // Same as above - mock cluster causes EnumerableValues.create to fail
    Object result = method.invoke(DuckDBHLLCountDistinctRule.INSTANCE,
        mockAggregate, estimates);
    assertNull(result);
  }

  // ====================================================================
  // DuckDBHLLCountDistinctRule - TableInfo inner class
  // ====================================================================

  @Test
  void testTableInfoCreation() throws Exception {
    Object tableInfo = createTableInfo("my_schema", "my_table");
    assertNotNull(tableInfo);

    Class<?> tableInfoClass = getDuckDBTableInfoClass();
    java.lang.reflect.Field schemaField = tableInfoClass.getDeclaredField("schemaName");
    schemaField.setAccessible(true);
    assertEquals("my_schema", schemaField.get(tableInfo));

    java.lang.reflect.Field tableField = tableInfoClass.getDeclaredField("tableName");
    tableField.setAccessible(true);
    assertEquals("my_table", tableField.get(tableInfo));
  }

  // ====================================================================
  // DuckDBHLLCountDistinctRule - onMatch without table scan
  // ====================================================================

  @Test
  void testHLLOnMatchNoTableScan() {
    // Create a mock aggregate with no table scan in its input
    Aggregate mockAggregate = createMockAggregate(
        ImmutableBitSet.of(),
        Collections.singletonList(createDistinctCountAggCall()));

    RelNode mockInput = mock(RelNode.class);
    when(mockInput.getInputs()).thenReturn(Collections.<RelNode>emptyList());
    when(mockAggregate.getInput()).thenReturn(mockInput);

    RelOptRuleCall call = mock(RelOptRuleCall.class);
    when(call.rel(0)).thenReturn(mockAggregate);

    // Should not throw
    DuckDBHLLCountDistinctRule.INSTANCE.onMatch(call);

    // Verify transformTo was NOT called (no optimization possible)
    verify(call, never()).transformTo(any(RelNode.class));
  }

  // ====================================================================
  // DuckDBIcebergCountStarRule - onMatch without table scan
  // ====================================================================

  @Test
  void testIcebergOnMatchNoTableScan() {
    Aggregate mockAggregate = createMockAggregate(
        ImmutableBitSet.of(),
        Collections.singletonList(createCountStarAggCall()));

    RelNode mockInput = mock(RelNode.class);
    when(mockInput.getInputs()).thenReturn(Collections.<RelNode>emptyList());
    when(mockAggregate.getInput()).thenReturn(mockInput);

    RelOptRuleCall call = mock(RelOptRuleCall.class);
    when(call.rel(0)).thenReturn(mockAggregate);

    // Should not throw
    DuckDBIcebergCountStarRule.INSTANCE.onMatch(call);

    // Verify transformTo was NOT called
    verify(call, never()).transformTo(any(RelNode.class));
  }

  // ====================================================================
  // Helper methods
  // ====================================================================

  private RelOptRuleCall mockCallWithAggregate(Aggregate aggregate) {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    when(call.rel(0)).thenReturn(aggregate);
    return call;
  }

  private Aggregate createMockAggregate(ImmutableBitSet groupSet,
      List<AggregateCall> aggCalls) {
    Aggregate aggregate = mock(Aggregate.class);
    when(aggregate.getGroupSet()).thenReturn(groupSet);
    when(aggregate.getAggCallList()).thenReturn(aggCalls);
    return aggregate;
  }

  private Aggregate createMockAggregateWithCluster(long dummyCount) {
    // Create a real enough aggregate for createCountStarValues / createHLLValues
    org.apache.calcite.jdbc.JavaTypeFactoryImpl typeFactory =
        new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

    RelDataType bigIntType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    RelDataType rowType = typeFactory.builder()
        .add("EXPR$0", bigIntType)
        .build();

    RexBuilder rexBuilder = new RexBuilder(typeFactory);

    RelOptCluster cluster = mock(RelOptCluster.class);
    when(cluster.getRexBuilder()).thenReturn(rexBuilder);
    when(cluster.getTypeFactory()).thenReturn(typeFactory);

    // Mock planner for EnumerableValues
    org.apache.calcite.plan.RelOptPlanner mockPlanner =
        mock(org.apache.calcite.plan.RelOptPlanner.class);
    when(cluster.getPlanner()).thenReturn(mockPlanner);
    when(mockPlanner.emptyTraitSet()).thenReturn(RelTraitSet.createEmpty());

    AggregateCall aggCall = createCountStarAggCall();
    Aggregate aggregate = mock(Aggregate.class);
    when(aggregate.getCluster()).thenReturn(cluster);
    when(aggregate.getAggCallList()).thenReturn(Collections.singletonList(aggCall));
    when(aggregate.getRowType()).thenReturn(rowType);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());
    when(aggregate.getTraitSet()).thenReturn(RelTraitSet.createEmpty());

    return aggregate;
  }

  private AggregateCall createCountStarAggCall() {
    SqlAggFunction countFn = mock(SqlAggFunction.class);
    when(countFn.getKind()).thenReturn(SqlKind.COUNT);

    AggregateCall aggCall = mock(AggregateCall.class);
    when(aggCall.getAggregation()).thenReturn(countFn);
    when(aggCall.isDistinct()).thenReturn(false);
    when(aggCall.getArgList()).thenReturn(Collections.<Integer>emptyList());
    when(aggCall.getName()).thenReturn("EXPR$0");
    return aggCall;
  }

  private AggregateCall createDistinctCountAggCall() {
    SqlAggFunction countFn = mock(SqlAggFunction.class);
    when(countFn.getKind()).thenReturn(SqlKind.COUNT);

    AggregateCall aggCall = mock(AggregateCall.class);
    when(aggCall.getAggregation()).thenReturn(countFn);
    when(aggCall.isDistinct()).thenReturn(true);
    when(aggCall.getArgList()).thenReturn(Collections.singletonList(0));
    when(aggCall.getName()).thenReturn("EXPR$0");
    return aggCall;
  }

  private AggregateCall createCountWithArgsAggCall() {
    SqlAggFunction countFn = mock(SqlAggFunction.class);
    when(countFn.getKind()).thenReturn(SqlKind.COUNT);

    AggregateCall aggCall = mock(AggregateCall.class);
    when(aggCall.getAggregation()).thenReturn(countFn);
    when(aggCall.isDistinct()).thenReturn(false);
    when(aggCall.getArgList()).thenReturn(Collections.singletonList(0));
    when(aggCall.getName()).thenReturn("EXPR$0");
    return aggCall;
  }

  private AggregateCall createSumAggCall() {
    SqlAggFunction sumFn = mock(SqlAggFunction.class);
    when(sumFn.getKind()).thenReturn(SqlKind.SUM);

    AggregateCall aggCall = mock(AggregateCall.class);
    when(aggCall.getAggregation()).thenReturn(sumFn);
    when(aggCall.isDistinct()).thenReturn(false);
    when(aggCall.getArgList()).thenReturn(Collections.singletonList(0));
    when(aggCall.getName()).thenReturn("SUM$0");
    return aggCall;
  }

  /**
   * Gets the private TableInfo inner class from DuckDBHLLCountDistinctRule.
   */
  private Class<?> getDuckDBTableInfoClass() {
    for (Class<?> inner : DuckDBHLLCountDistinctRule.class.getDeclaredClasses()) {
      if (inner.getSimpleName().equals("TableInfo")) {
        return inner;
      }
    }
    throw new RuntimeException("TableInfo inner class not found");
  }

  /**
   * Creates a TableInfo instance via reflection.
   */
  private Object createTableInfo(String schemaName, String tableName) {
    try {
      Class<?> tableInfoClass = getDuckDBTableInfoClass();
      java.lang.reflect.Constructor<?> ctor =
          tableInfoClass.getDeclaredConstructor(String.class, String.class);
      ctor.setAccessible(true);
      return ctor.newInstance(schemaName, tableName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create TableInfo", e);
    }
  }
}
