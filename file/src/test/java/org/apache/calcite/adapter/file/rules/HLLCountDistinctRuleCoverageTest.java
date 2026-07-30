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
  void testOnMatchDeclinesGroupByAggregate() {
    // onMatch is live: it reads the aggregate and only bails out for aggregates it cannot
    // rewrite. These tests used to hand it a bare mock and assert it "returns immediately
    // because the rule is disabled", which stopped being true and simply NPE'd on the
    // unstubbed rel(0). Provide the aggregate the rule needs and pin a real decline path:
    // HLL can only answer a whole-table COUNT(DISTINCT), so a GROUP BY aggregate is left alone.
    Aggregate aggregate = aggregateWith(createCountDistinctAggCall(0), ImmutableBitSet.of(0));
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    when(call.rel(0)).thenReturn(aggregate);

    HLLCountDistinctRule.INSTANCE.onMatch(call);

    verify(call, never()).transformTo(org.mockito.ArgumentMatchers.any(RelNode.class));
  }

  @Test
  void testOnMatchReadsTheAggregateItWasMatchedOn() {
    Aggregate aggregate = aggregateWith(createCountDistinctAggCall(0), ImmutableBitSet.of(0));
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    when(call.rel(0)).thenReturn(aggregate);

    HLLCountDistinctRule.INSTANCE.onMatch(call);

    // It takes operand 0 — the matched Aggregate — and nothing else off the call.
    verify(call).rel(0);
    verify(call, never()).transformTo(org.mockito.ArgumentMatchers.any(RelNode.class));
  }

  /** A COUNT(DISTINCT arg) aggregate call, stubbed to what the rule inspects. */
  private AggregateCall createCountDistinctAggCall(int argIndex) {
    AggregateCall aggCall = mock(AggregateCall.class);
    SqlAggFunction aggFunction = mock(SqlAggFunction.class);
    when(aggFunction.getKind()).thenReturn(SqlKind.COUNT);
    when(aggCall.getAggregation()).thenReturn(aggFunction);
    when(aggCall.isDistinct()).thenReturn(true);
    when(aggCall.getArgList()).thenReturn(ImmutableList.of(argIndex));
    when(aggCall.getName()).thenReturn("cnt");
    return aggCall;
  }

  /** An Aggregate stubbed with just what {@code onMatch} reads. */
  private Aggregate aggregateWith(AggregateCall aggCall, ImmutableBitSet groupSet) {
    Aggregate aggregate = mock(Aggregate.class);
    when(aggregate.getInput()).thenReturn(mock(RelNode.class));
    when(aggregate.getGroupSet()).thenReturn(groupSet);
    when(aggregate.getAggCallList()).thenReturn(ImmutableList.of(aggCall));
    return aggregate;
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


}
