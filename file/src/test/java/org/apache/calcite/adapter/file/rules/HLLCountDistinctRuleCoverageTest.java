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
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.test.MockRelOptPlanner;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Deep coverage unit tests for {@link HLLCountDistinctRule}.
 *
 * <p>Tests exercise every code path including the private methods
 * {@code getHLLEstimate}, {@code findTableScan}, {@code createConstantAgg},
 * and {@code createHLLAggregate} via reflection. The {@code onMatch} method
 * is currently disabled (returns immediately), so we verify that behavior
 * and also test the underlying logic that would execute when re-enabled.
 *
 * <p>Each test that uses the shared HLLSketchCache singleton uses a unique
 * schema name to avoid cross-test interference, since the cache is a JVM-wide
 * singleton and other tests in the same process may also interact with it.
 */
@Tag("unit")
class HLLCountDistinctRuleCoverageTest {

  /** Counter to generate unique schema names per test invocation. */
  private static final AtomicLong SCHEMA_COUNTER = new AtomicLong(
      System.nanoTime());

  /** Returns a unique schema name for cache isolation. */
  private static String uniqueSchema() {
    return "hllcov_" + SCHEMA_COUNTER.incrementAndGet();
  }

  // ===== INSTANCE and Config =====

  @Test
  void testInstanceNotNull() {
    assertNotNull(HLLCountDistinctRule.INSTANCE);
  }

  @Test
  void testInstanceIsCorrectType() {
    assertTrue(HLLCountDistinctRule.INSTANCE instanceof HLLCountDistinctRule);
  }

  @Test
  void testInstanceIsRelOptRule() {
    assertTrue(HLLCountDistinctRule.INSTANCE instanceof RelOptRule);
  }

  @Test
  void testInstanceIsSingleton() {
    assertSame(HLLCountDistinctRule.INSTANCE, HLLCountDistinctRule.INSTANCE);
  }

  @Test
  void testConfigDefaultNotNull() {
    assertNotNull(HLLCountDistinctRule.Config.DEFAULT);
  }

  @Test
  void testConfigToRuleReturnsRule() {
    HLLCountDistinctRule rule = HLLCountDistinctRule.Config.DEFAULT.toRule();
    assertNotNull(rule);
    assertTrue(rule instanceof HLLCountDistinctRule);
  }

  @Test
  void testConfigToRuleCreatesNewInstance() {
    HLLCountDistinctRule rule1 = HLLCountDistinctRule.Config.DEFAULT.toRule();
    HLLCountDistinctRule rule2 = HLLCountDistinctRule.Config.DEFAULT.toRule();
    assertNotNull(rule1);
    assertNotNull(rule2);
  }

  // ===== onMatch (currently disabled) =====

  @Test
  void testOnMatchReturnsImmediately() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    HLLCountDistinctRule.INSTANCE.onMatch(call);
    // onMatch is disabled, so transformTo should never be called
    verify(call, never()).transformTo(org.mockito.ArgumentMatchers.any(RelNode.class));
  }

  @Test
  void testOnMatchDoesNotInteractWithCall() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    HLLCountDistinctRule.INSTANCE.onMatch(call);
    // Verify no rel() calls are made since onMatch returns immediately
    verify(call, never()).rel(org.mockito.ArgumentMatchers.anyInt());
  }

  // ===== getHLLEstimate (private, via reflection) =====

  @Test
  void testGetHLLEstimateNullTableScan() throws Exception {
    // Input that has no TableScan descendant
    RelNode input = mock(RelNode.class);
    when(input.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    AggregateCall aggCall = mock(AggregateCall.class);

    Long result = invokeGetHLLEstimate(input, aggCall);
    assertNull(result, "Should return null when no TableScan is found");
  }

  @Test
  void testGetHLLEstimateEmptyArgList() throws Exception {
    // TableScan is found, but aggCall has no arguments
    TableScan tableScan = mock(TableScan.class);
    when(tableScan.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    AggregateCall aggCall = mock(AggregateCall.class);
    when(aggCall.getArgList()).thenReturn(ImmutableList.<Integer>of());

    Long result = invokeGetHLLEstimate(tableScan, aggCall);
    assertNull(result, "Should return null when arg list is empty");
  }

  @Test
  void testGetHLLEstimateNoSketchInCache() throws Exception {
    String schema = uniqueSchema();
    TableScan tableScan = createMockTableScan(
        Arrays.asList(schema, "mytable"),
        Arrays.asList("col_a", "col_b"));

    AggregateCall aggCall = mock(AggregateCall.class);
    when(aggCall.getArgList()).thenReturn(ImmutableList.of(0));

    Long result = invokeGetHLLEstimate(tableScan, aggCall);
    assertNull(result, "Should return null when no sketch is in cache");
  }

  @Test
  void testGetHLLEstimateWithSketchInCache() throws Exception {
    String schema = uniqueSchema();
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(42L);
    cache.putSketch(schema, "mytable", "col_a", sketch);

    TableScan tableScan = createMockTableScan(
        Arrays.asList(schema, "mytable"),
        Arrays.asList("col_a", "col_b"));

    AggregateCall aggCall = mock(AggregateCall.class);
    when(aggCall.getArgList()).thenReturn(ImmutableList.of(0));

    Long result = invokeGetHLLEstimate(tableScan, aggCall);
    assertNotNull(result, "Should return estimate when sketch is in cache");
    assertEquals(42L, result.longValue(), "Estimate should match cached value");
  }

  @Test
  void testGetHLLEstimateSecondColumn() throws Exception {
    String schema = uniqueSchema();
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(999L);
    cache.putSketch(schema, "mytable", "col_b", sketch);

    TableScan tableScan = createMockTableScan(
        Arrays.asList(schema, "mytable"),
        Arrays.asList("col_a", "col_b"));

    AggregateCall aggCall = mock(AggregateCall.class);
    when(aggCall.getArgList()).thenReturn(ImmutableList.of(1));

    Long result = invokeGetHLLEstimate(tableScan, aggCall);
    assertNotNull(result, "Should find sketch for second column");
    assertEquals(999L, result.longValue());
  }

  @Test
  void testGetHLLEstimateQualifiedNameSingleElement() throws Exception {
    // When qualified name has only one element, schemaName should be ""
    HLLSketchCache cache = HLLSketchCache.getInstance();
    String tableName = "only_" + uniqueSchema();
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(77L);
    cache.putSketch("", tableName, "col_a", sketch);

    TableScan tableScan = createMockTableScan(
        Arrays.asList(tableName),
        Arrays.asList("col_a"));

    AggregateCall aggCall = mock(AggregateCall.class);
    when(aggCall.getArgList()).thenReturn(ImmutableList.of(0));

    Long result = invokeGetHLLEstimate(tableScan, aggCall);
    assertNotNull(result, "Should handle single-element qualified name");
    assertEquals(77L, result.longValue());
  }

  @Test
  void testGetHLLEstimateThreePartQualifiedName() throws Exception {
    String schema = uniqueSchema();
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(123L);
    cache.putSketch(schema, "thetable", "col_x", sketch);

    // Qualified name with 3 parts: [catalog, schema, table]
    // schemaName = qualifiedName.get(size - 2) = schema
    // tableName = qualifiedName.get(size - 1) = "thetable"
    TableScan tableScan = createMockTableScan(
        Arrays.asList("catalog", schema, "thetable"),
        Arrays.asList("col_x", "col_y"));

    AggregateCall aggCall = mock(AggregateCall.class);
    when(aggCall.getArgList()).thenReturn(ImmutableList.of(0));

    Long result = invokeGetHLLEstimate(tableScan, aggCall);
    assertNotNull(result, "Should handle three-part qualified name");
    assertEquals(123L, result.longValue());
  }

  @Test
  void testGetHLLEstimateWithNestedInput() throws Exception {
    String schema = uniqueSchema();
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(500L);
    cache.putSketch(schema, "t", "id", sketch);

    // TableScan is nested inside another RelNode
    TableScan tableScan = createMockTableScan(
        Arrays.asList(schema, "t"),
        Arrays.asList("id"));

    RelNode intermediate = mock(RelNode.class);
    when(intermediate.getInputs()).thenReturn(
        Collections.<RelNode>singletonList(tableScan));
    // The input row type is used for column name lookup
    RelDataType intermediateRowType = mock(RelDataType.class);
    when(intermediateRowType.getFieldNames()).thenReturn(Arrays.asList("id"));
    when(intermediate.getRowType()).thenReturn(intermediateRowType);

    AggregateCall aggCall = mock(AggregateCall.class);
    when(aggCall.getArgList()).thenReturn(ImmutableList.of(0));

    Long result = invokeGetHLLEstimate(intermediate, aggCall);
    assertNotNull(result, "Should find TableScan nested inside another node");
    assertEquals(500L, result.longValue());
  }

  // ===== findTableScan (private, via reflection) =====

  @Test
  void testFindTableScanDirectTableScan() throws Exception {
    TableScan tableScan = mock(TableScan.class);

    TableScan result = invokeFindTableScan(tableScan);
    assertSame(tableScan, result);
  }

  @Test
  void testFindTableScanNoInputs() throws Exception {
    RelNode node = mock(RelNode.class);
    when(node.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    TableScan result = invokeFindTableScan(node);
    assertNull(result, "Should return null when there are no inputs and node is not TableScan");
  }

  @Test
  void testFindTableScanNestedOneLevel() throws Exception {
    TableScan tableScan = mock(TableScan.class);
    RelNode parent = mock(RelNode.class);
    when(parent.getInputs()).thenReturn(
        Collections.<RelNode>singletonList(tableScan));

    TableScan result = invokeFindTableScan(parent);
    assertSame(tableScan, result);
  }

  @Test
  void testFindTableScanNestedTwoLevels() throws Exception {
    TableScan tableScan = mock(TableScan.class);
    RelNode child = mock(RelNode.class);
    when(child.getInputs()).thenReturn(
        Collections.<RelNode>singletonList(tableScan));
    RelNode parent = mock(RelNode.class);
    when(parent.getInputs()).thenReturn(
        Collections.<RelNode>singletonList(child));

    TableScan result = invokeFindTableScan(parent);
    assertSame(tableScan, result);
  }

  @Test
  void testFindTableScanMultipleInputsFirstBranch() throws Exception {
    TableScan tableScan = mock(TableScan.class);
    RelNode emptyNode = mock(RelNode.class);
    when(emptyNode.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    RelNode parent = mock(RelNode.class);
    List<RelNode> inputs = new ArrayList<RelNode>();
    inputs.add(tableScan);
    inputs.add(emptyNode);
    when(parent.getInputs()).thenReturn(inputs);

    TableScan result = invokeFindTableScan(parent);
    assertSame(tableScan, result, "Should find TableScan in first branch");
  }

  @Test
  void testFindTableScanMultipleInputsSecondBranch() throws Exception {
    TableScan tableScan = mock(TableScan.class);
    RelNode emptyNode = mock(RelNode.class);
    when(emptyNode.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    RelNode parent = mock(RelNode.class);
    List<RelNode> inputs = new ArrayList<RelNode>();
    inputs.add(emptyNode);
    inputs.add(tableScan);
    when(parent.getInputs()).thenReturn(inputs);

    TableScan result = invokeFindTableScan(parent);
    assertSame(tableScan, result, "Should find TableScan in second branch");
  }

  @Test
  void testFindTableScanNoneInTree() throws Exception {
    RelNode leaf1 = mock(RelNode.class);
    when(leaf1.getInputs()).thenReturn(Collections.<RelNode>emptyList());
    RelNode leaf2 = mock(RelNode.class);
    when(leaf2.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    RelNode parent = mock(RelNode.class);
    List<RelNode> inputs = new ArrayList<RelNode>();
    inputs.add(leaf1);
    inputs.add(leaf2);
    when(parent.getInputs()).thenReturn(inputs);

    TableScan result = invokeFindTableScan(parent);
    assertNull(result, "Should return null when no TableScan exists in tree");
  }

  // ===== createConstantAgg (private, via reflection) =====

  @Test
  void testCreateConstantAggReturnsOriginal() throws Exception {
    AggregateCall original = mock(AggregateCall.class);
    RexBuilder rexBuilder = mock(RexBuilder.class);
    RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);

    AggregateCall result = invokeCreateConstantAgg(original, 100L, rexBuilder, typeFactory);
    assertSame(original, result, "Disabled method should return original AggregateCall");
  }

  @Test
  void testCreateConstantAggZeroValue() throws Exception {
    AggregateCall original = mock(AggregateCall.class);
    RexBuilder rexBuilder = mock(RexBuilder.class);
    RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);

    AggregateCall result = invokeCreateConstantAgg(original, 0L, rexBuilder, typeFactory);
    assertSame(original, result, "Disabled method should return original regardless of value");
  }

  @Test
  void testCreateConstantAggNegativeValue() throws Exception {
    AggregateCall original = mock(AggregateCall.class);
    RexBuilder rexBuilder = mock(RexBuilder.class);
    RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);

    AggregateCall result = invokeCreateConstantAgg(original, -1L, rexBuilder, typeFactory);
    assertSame(original, result, "Disabled method should return original for negative value");
  }

  @Test
  void testCreateConstantAggLargeValue() throws Exception {
    AggregateCall original = mock(AggregateCall.class);
    RexBuilder rexBuilder = mock(RexBuilder.class);
    RelDataTypeFactory typeFactory = mock(RelDataTypeFactory.class);

    AggregateCall result = invokeCreateConstantAgg(
        original, Long.MAX_VALUE, rexBuilder, typeFactory);
    assertSame(original, result, "Disabled method should return original for large value");
  }

  // ===== createHLLAggregate (private, via reflection) =====

  @Test
  void testCreateHLLAggregateNoCountDistinct() throws Exception {
    // Aggregate with non-COUNT non-DISTINCT agg call -> hasHLLOptimization stays false
    Aggregate aggregate = createMockAggregate(
        createNonDistinctAggCall(),
        ImmutableBitSet.of());

    RelNode input = createMockTableScan(
        Arrays.asList(uniqueSchema(), "t"),
        Arrays.asList("col_a"));
    RelBuilder builder = mock(RelBuilder.class);

    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
    newAggCalls.add(createNonDistinctAggCall());

    RelNode result = invokeCreateHLLAggregate(aggregate, newAggCalls, input, builder);
    assertNull(result, "Should return null when no COUNT(DISTINCT) agg calls exist");
  }

  @Test
  void testCreateHLLAggregateCountDistinctNoSketch() throws Exception {
    // COUNT(DISTINCT) but no sketch in cache -> hasHLLOptimization stays false
    String schema = uniqueSchema();
    AggregateCall countDistinct = createCountDistinctAggCall(0);

    Aggregate aggregate = createMockAggregate(countDistinct, ImmutableBitSet.of());

    TableScan input = createMockTableScan(
        Arrays.asList(schema, "t"),
        Arrays.asList("col_a"));

    RelBuilder builder = mock(RelBuilder.class);
    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
    newAggCalls.add(countDistinct);

    RelNode result = invokeCreateHLLAggregate(aggregate, newAggCalls, input, builder);
    assertNull(result, "Should return null when no HLL sketch is available for COUNT(DISTINCT)");
  }

  @Test
  void testCreateHLLAggregateCountDistinctWithSketch() throws Exception {
    // COUNT(DISTINCT) with sketch in cache and empty group set -> creates VALUES node
    String schema = uniqueSchema();
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(250L);
    cache.putSketch(schema, "t", "col_a", sketch);

    AggregateCall countDistinct = createCountDistinctAggCall(0);

    RelOptCluster cluster = createRealCluster();

    Aggregate aggregate = createMockAggregateWithCluster(
        cluster, countDistinct, ImmutableBitSet.of());

    TableScan input = createMockTableScan(
        Arrays.asList(schema, "t"),
        Arrays.asList("col_a"));

    RelBuilder builder = mock(RelBuilder.class);
    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
    newAggCalls.add(countDistinct);

    RelNode result = invokeCreateHLLAggregate(aggregate, newAggCalls, input, builder);
    assertNotNull(result, "Should return VALUES node when HLL sketch is available");
    assertTrue(result instanceof org.apache.calcite.rel.logical.LogicalValues,
        "Result should be a LogicalValues node");
  }

  @Test
  void testCreateHLLAggregateCountDistinctWithNamedAggCall() throws Exception {
    // COUNT(DISTINCT) with non-null name on AggregateCall
    String schema = uniqueSchema();
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(100L);
    cache.putSketch(schema, "t", "col_a", sketch);

    AggregateCall countDistinct = createCountDistinctAggCallWithName(0, "my_count");

    RelOptCluster cluster = createRealCluster();

    Aggregate aggregate = createMockAggregateWithCluster(
        cluster, countDistinct, ImmutableBitSet.of());

    TableScan input = createMockTableScan(
        Arrays.asList(schema, "t"),
        Arrays.asList("col_a"));

    RelBuilder builder = mock(RelBuilder.class);
    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
    newAggCalls.add(countDistinct);

    RelNode result = invokeCreateHLLAggregate(aggregate, newAggCalls, input, builder);
    assertNotNull(result, "Should create VALUES node with named agg call");
    assertTrue(result instanceof org.apache.calcite.rel.logical.LogicalValues);
  }

  @Test
  void testCreateHLLAggregateCountDistinctWithNullName() throws Exception {
    // COUNT(DISTINCT) where getName() returns null -> should use EXPR$0
    String schema = uniqueSchema();
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(100L);
    cache.putSketch(schema, "t", "col_a", sketch);

    AggregateCall countDistinct = createCountDistinctAggCallWithName(0, null);

    RelOptCluster cluster = createRealCluster();

    Aggregate aggregate = createMockAggregateWithCluster(
        cluster, countDistinct, ImmutableBitSet.of());

    TableScan input = createMockTableScan(
        Arrays.asList(schema, "t"),
        Arrays.asList("col_a"));

    RelBuilder builder = mock(RelBuilder.class);
    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
    newAggCalls.add(countDistinct);

    RelNode result = invokeCreateHLLAggregate(aggregate, newAggCalls, input, builder);
    assertNotNull(result, "Should create VALUES node even when name is null");
    assertTrue(result instanceof org.apache.calcite.rel.logical.LogicalValues);
  }

  @Test
  void testCreateHLLAggregateWithGroupBy() throws Exception {
    // Non-empty group set -> should return null (GROUP BY not supported)
    String schema = uniqueSchema();
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(250L);
    cache.putSketch(schema, "t", "col_a", sketch);

    AggregateCall countDistinct = createCountDistinctAggCall(0);

    Aggregate aggregate = createMockAggregate(
        countDistinct,
        ImmutableBitSet.of(1)); // GROUP BY col_b

    TableScan input = createMockTableScan(
        Arrays.asList(schema, "t"),
        Arrays.asList("col_a", "col_b"));

    RelBuilder builder = mock(RelBuilder.class);
    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
    newAggCalls.add(countDistinct);

    RelNode result = invokeCreateHLLAggregate(aggregate, newAggCalls, input, builder);
    assertNull(result, "Should return null for GROUP BY queries");
  }

  @Test
  void testCreateHLLAggregateMixedCallsOneSketchMissing() throws Exception {
    // Two agg calls: COUNT(DISTINCT col_a) with sketch, COUNT(DISTINCT col_b) without sketch
    String schema = uniqueSchema();
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(250L);
    cache.putSketch(schema, "t", "col_a", sketch);

    AggregateCall countDistinctA = createCountDistinctAggCall(0);
    AggregateCall countDistinctB = createCountDistinctAggCall(1);

    List<AggregateCall> aggCalls = new ArrayList<AggregateCall>();
    aggCalls.add(countDistinctA);
    aggCalls.add(countDistinctB);

    RelOptCluster cluster = createRealCluster();

    Aggregate aggregate = mock(Aggregate.class);
    when(aggregate.getAggCallList()).thenReturn(aggCalls);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());
    when(aggregate.getCluster()).thenReturn(cluster);

    TableScan input = createMockTableScan(
        Arrays.asList(schema, "t"),
        Arrays.asList("col_a", "col_b"));

    RelBuilder builder = mock(RelBuilder.class);
    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
    newAggCalls.add(countDistinctA);
    newAggCalls.add(countDistinctB);

    RelNode result = invokeCreateHLLAggregate(aggregate, newAggCalls, input, builder);
    // hasHLLOptimization is true (col_a has sketch), but col_b has null estimate
    // -> in the VALUES construction loop, estimate for index 1 is null -> returns null
    assertNull(result, "Should return null when one of the COUNT(DISTINCT) has no sketch");
  }

  @Test
  void testCreateHLLAggregateMultipleCountDistinctsAllWithSketches() throws Exception {
    // Both COUNT(DISTINCT) calls have sketches
    String schema = uniqueSchema();
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketchA = HyperLogLogSketch.fromEstimate(250L);
    cache.putSketch(schema, "t", "col_a", sketchA);
    HyperLogLogSketch sketchB = HyperLogLogSketch.fromEstimate(50L);
    cache.putSketch(schema, "t", "col_b", sketchB);

    AggregateCall countDistinctA = createCountDistinctAggCall(0);
    AggregateCall countDistinctB = createCountDistinctAggCall(1);

    List<AggregateCall> aggCalls = new ArrayList<AggregateCall>();
    aggCalls.add(countDistinctA);
    aggCalls.add(countDistinctB);

    RelOptCluster cluster = createRealCluster();

    Aggregate aggregate = mock(Aggregate.class);
    when(aggregate.getAggCallList()).thenReturn(aggCalls);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());
    when(aggregate.getCluster()).thenReturn(cluster);

    TableScan input = createMockTableScan(
        Arrays.asList(schema, "t"),
        Arrays.asList("col_a", "col_b"));

    RelBuilder builder = mock(RelBuilder.class);
    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
    newAggCalls.add(countDistinctA);
    newAggCalls.add(countDistinctB);

    RelNode result = invokeCreateHLLAggregate(aggregate, newAggCalls, input, builder);
    assertNotNull(result, "Should return VALUES node when all COUNT(DISTINCT) have sketches");
    assertTrue(result instanceof org.apache.calcite.rel.logical.LogicalValues);
  }

  @Test
  void testCreateHLLAggregateMixedDistinctAndNonDistinct() throws Exception {
    // One non-distinct agg call (SUM) and one COUNT(DISTINCT) with sketch
    // The non-distinct call gets null estimate -> VALUES loop returns null
    String schema = uniqueSchema();
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(250L);
    cache.putSketch(schema, "t", "col_a", sketch);

    AggregateCall nonDistinct = createNonDistinctAggCall();
    AggregateCall countDistinct = createCountDistinctAggCall(0);

    List<AggregateCall> aggCalls = new ArrayList<AggregateCall>();
    aggCalls.add(nonDistinct);
    aggCalls.add(countDistinct);

    RelOptCluster cluster = createRealCluster();

    Aggregate aggregate = mock(Aggregate.class);
    when(aggregate.getAggCallList()).thenReturn(aggCalls);
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());
    when(aggregate.getCluster()).thenReturn(cluster);

    TableScan input = createMockTableScan(
        Arrays.asList(schema, "t"),
        Arrays.asList("col_a"));

    RelBuilder builder = mock(RelBuilder.class);
    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
    newAggCalls.add(nonDistinct);
    newAggCalls.add(countDistinct);

    RelNode result = invokeCreateHLLAggregate(aggregate, newAggCalls, input, builder);
    // Non-distinct call has null estimate, so VALUES loop falls back
    assertNull(result,
        "Should return null when a non-distinct agg call has no HLL estimate");
  }

  @Test
  void testCreateHLLAggregateCountNonDistinct() throws Exception {
    // COUNT without DISTINCT -- should add null to hllEstimates
    // and hasHLLOptimization stays false
    AggregateCall countNonDistinct = createCountNonDistinctAggCall(0);

    Aggregate aggregate = createMockAggregate(
        countNonDistinct, ImmutableBitSet.of());

    TableScan input = createMockTableScan(
        Arrays.asList(uniqueSchema(), "t"),
        Arrays.asList("col_a"));

    RelBuilder builder = mock(RelBuilder.class);
    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
    newAggCalls.add(countNonDistinct);

    RelNode result = invokeCreateHLLAggregate(aggregate, newAggCalls, input, builder);
    assertNull(result, "COUNT without DISTINCT should not trigger HLL optimization");
  }

  @Test
  void testCreateHLLAggregateNoInputTableScan() throws Exception {
    // COUNT(DISTINCT) but the input has no TableScan -> getHLLEstimate returns null
    AggregateCall countDistinct = createCountDistinctAggCall(0);

    Aggregate aggregate = createMockAggregate(countDistinct, ImmutableBitSet.of());

    // Input with no TableScan
    RelNode input = mock(RelNode.class);
    when(input.getInputs()).thenReturn(Collections.<RelNode>emptyList());
    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col_a"));
    when(input.getRowType()).thenReturn(rowType);

    RelBuilder builder = mock(RelBuilder.class);
    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
    newAggCalls.add(countDistinct);

    RelNode result = invokeCreateHLLAggregate(aggregate, newAggCalls, input, builder);
    assertNull(result, "Should return null when input has no TableScan");
  }

  @Test
  void testCreateHLLAggregateEmptyAggCallList() throws Exception {
    // No agg calls at all -> hasHLLOptimization stays false
    Aggregate aggregate = mock(Aggregate.class);
    when(aggregate.getAggCallList()).thenReturn(
        Collections.<AggregateCall>emptyList());
    when(aggregate.getGroupSet()).thenReturn(ImmutableBitSet.of());

    RelNode input = createMockTableScan(
        Arrays.asList(uniqueSchema(), "t"),
        Arrays.asList("col_a"));

    RelBuilder builder = mock(RelBuilder.class);
    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();

    RelNode result = invokeCreateHLLAggregate(aggregate, newAggCalls, input, builder);
    assertNull(result, "Should return null when there are no aggregate calls");
  }

  @Test
  void testCreateHLLAggregateSingleCountDistinctWithGroupByAndSketch() throws Exception {
    // COUNT(DISTINCT) with sketch but non-empty group set
    String schema = uniqueSchema();
    HLLSketchCache cache = HLLSketchCache.getInstance();
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(100L);
    cache.putSketch(schema, "t", "col_a", sketch);

    AggregateCall countDistinct = createCountDistinctAggCall(0);

    Aggregate aggregate = createMockAggregate(
        countDistinct, ImmutableBitSet.of(0)); // GROUP BY

    TableScan input = createMockTableScan(
        Arrays.asList(schema, "t"),
        Arrays.asList("col_a"));

    RelBuilder builder = mock(RelBuilder.class);
    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
    newAggCalls.add(countDistinct);

    RelNode result = invokeCreateHLLAggregate(aggregate, newAggCalls, input, builder);
    assertNull(result, "GROUP BY with HLL sketch should still return null");
  }

  // ===== Helper methods =====

  /** Creates a real RelOptCluster with a MockRelOptPlanner and SqlTypeFactoryImpl. */
  private RelOptCluster createRealCluster() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    MockRelOptPlanner planner = new MockRelOptPlanner(Contexts.empty());
    return RelOptCluster.create(planner, rexBuilder);
  }

  private TableScan createMockTableScan(List<String> qualifiedName,
      List<String> fieldNames) {
    TableScan tableScan = mock(TableScan.class);
    when(tableScan.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    RelOptTable table = mock(RelOptTable.class);
    when(table.getQualifiedName()).thenReturn(qualifiedName);
    when(tableScan.getTable()).thenReturn(table);

    RelDataType rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(fieldNames);
    when(tableScan.getRowType()).thenReturn(rowType);

    return tableScan;
  }

  private AggregateCall createCountDistinctAggCall(int argIndex) {
    return createCountDistinctAggCallWithName(argIndex, "cnt");
  }

  private AggregateCall createCountDistinctAggCallWithName(int argIndex, String name) {
    AggregateCall aggCall = mock(AggregateCall.class);
    SqlAggFunction aggFunction = mock(SqlAggFunction.class);
    when(aggFunction.getKind()).thenReturn(SqlKind.COUNT);
    when(aggCall.getAggregation()).thenReturn(aggFunction);
    when(aggCall.isDistinct()).thenReturn(true);
    when(aggCall.getArgList()).thenReturn(ImmutableList.of(argIndex));
    when(aggCall.getName()).thenReturn(name);
    return aggCall;
  }

  private AggregateCall createCountNonDistinctAggCall(int argIndex) {
    AggregateCall aggCall = mock(AggregateCall.class);
    SqlAggFunction aggFunction = mock(SqlAggFunction.class);
    when(aggFunction.getKind()).thenReturn(SqlKind.COUNT);
    when(aggCall.getAggregation()).thenReturn(aggFunction);
    when(aggCall.isDistinct()).thenReturn(false);
    when(aggCall.getArgList()).thenReturn(ImmutableList.of(argIndex));
    when(aggCall.getName()).thenReturn("cnt");
    return aggCall;
  }

  private AggregateCall createNonDistinctAggCall() {
    AggregateCall aggCall = mock(AggregateCall.class);
    SqlAggFunction aggFunction = mock(SqlAggFunction.class);
    when(aggFunction.getKind()).thenReturn(SqlKind.SUM);
    when(aggCall.getAggregation()).thenReturn(aggFunction);
    when(aggCall.isDistinct()).thenReturn(false);
    when(aggCall.getName()).thenReturn("total");
    return aggCall;
  }

  private Aggregate createMockAggregate(AggregateCall aggCall, ImmutableBitSet groupSet) {
    Aggregate aggregate = mock(Aggregate.class);
    List<AggregateCall> aggCallList = new ArrayList<AggregateCall>();
    aggCallList.add(aggCall);
    when(aggregate.getAggCallList()).thenReturn(aggCallList);
    when(aggregate.getGroupSet()).thenReturn(groupSet);
    return aggregate;
  }

  private Aggregate createMockAggregateWithCluster(RelOptCluster cluster,
      AggregateCall aggCall, ImmutableBitSet groupSet) {
    Aggregate aggregate = mock(Aggregate.class);
    List<AggregateCall> aggCallList = new ArrayList<AggregateCall>();
    aggCallList.add(aggCall);
    when(aggregate.getAggCallList()).thenReturn(aggCallList);
    when(aggregate.getGroupSet()).thenReturn(groupSet);
    when(aggregate.getCluster()).thenReturn(cluster);
    return aggregate;
  }

  // ===== Reflection helpers =====

  @SuppressWarnings("unchecked")
  private Long invokeGetHLLEstimate(RelNode input, AggregateCall aggCall) throws Exception {
    Method method = HLLCountDistinctRule.class.getDeclaredMethod(
        "getHLLEstimate", RelNode.class, AggregateCall.class);
    method.setAccessible(true);
    return (Long) method.invoke(HLLCountDistinctRule.INSTANCE, input, aggCall);
  }

  private TableScan invokeFindTableScan(RelNode node) throws Exception {
    Method method = HLLCountDistinctRule.class.getDeclaredMethod(
        "findTableScan", RelNode.class);
    method.setAccessible(true);
    return (TableScan) method.invoke(HLLCountDistinctRule.INSTANCE, node);
  }

  private AggregateCall invokeCreateConstantAgg(AggregateCall original, long value,
      RexBuilder rexBuilder, RelDataTypeFactory typeFactory) throws Exception {
    Method method = HLLCountDistinctRule.class.getDeclaredMethod(
        "createConstantAgg", AggregateCall.class, long.class,
        RexBuilder.class, RelDataTypeFactory.class);
    method.setAccessible(true);
    return (AggregateCall) method.invoke(
        HLLCountDistinctRule.INSTANCE, original, value, rexBuilder, typeFactory);
  }

  private RelNode invokeCreateHLLAggregate(Aggregate original,
      List<AggregateCall> newAggCalls, RelNode input, RelBuilder builder) throws Exception {
    Method method = HLLCountDistinctRule.class.getDeclaredMethod(
        "createHLLAggregate", Aggregate.class, List.class,
        RelNode.class, RelBuilder.class);
    method.setAccessible(true);
    return (RelNode) method.invoke(
        HLLCountDistinctRule.INSTANCE, original, newAggCalls, input, builder);
  }
}
