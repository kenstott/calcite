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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Method;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link DuckDBIcebergCountStarRule} targeting 63 missed lines.
 * Focuses on matches() branches (GROUP BY, multiple agg calls, non-COUNT,
 * DISTINCT, arguments), onMatch() path when no table scan found,
 * the scan-shape whitelist that decides which plan trees a stored count may answer
 * (single scan, at most one Filter sitting directly on it), onMatch declining a scan that is
 * not Iceberg-backed, and the INSTANCE singleton.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class DuckDBIcebergCountStarRuleDeepCoverageTest {

  // ========== INSTANCE ==========

  @Test void testInstanceNotNull() {
    assertNotNull(DuckDBIcebergCountStarRule.INSTANCE);
    assertEquals("DuckDBIcebergCountStarRule",
        DuckDBIcebergCountStarRule.INSTANCE.toString());
  }

  // ========== matches: GROUP BY ==========

  @Test void testMatchesWithGroupBy() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate agg = mock(Aggregate.class);
    when(call.rel(0)).thenReturn(agg);
    when(agg.getGroupSet()).thenReturn(ImmutableBitSet.of(0)); // has GROUP BY

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  // ========== matches: multiple aggregate calls ==========

  @Test void testMatchesMultipleAggCalls() {
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

  @Test void testMatchesNonCountAgg() {
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

  @Test void testMatchesCountDistinct() {
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

  @Test void testMatchesCountWithArgs() {
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

  @Test void testMatchesValidCountStar() {
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
    // The rule now requires the input to reduce to exactly one TableScan: a cached per-table
    // count cannot answer COUNT(*) over a join, which previously returned one side's row count
    // (a self cross join of 33,791 rows answered 33,791 instead of ~1.1e9). An unstubbed
    // getInput() is null, which the whitelist treats as "not simple", so the input has to be
    // provided for this to reach the match it is testing.
    when(agg.getInput()).thenReturn(mock(TableScan.class));


    assertTrue(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  // ========== matches: zero aggregate calls ==========

  @Test void testMatchesZeroAggCalls() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate agg = mock(Aggregate.class);
    when(call.rel(0)).thenReturn(agg);
    when(agg.getGroupSet()).thenReturn(ImmutableBitSet.of());
    when(agg.getAggCallList()).thenReturn(ImmutableList.<AggregateCall>of());

    assertFalse(DuckDBIcebergCountStarRule.INSTANCE.matches(call));
  }

  // ========== resolveScanShape ==========

  @Test void testScanShapeNullInput() throws Exception {
    assertNull(resolveScanShape(null));
  }

  @Test void testScanShapeDirectTableScan() throws Exception {
    TableScan scan = mock(TableScan.class);
    Object shape = resolveScanShape(scan);
    assertNotNull(shape);
    assertSame(scan, field(shape, "scan"));
    assertNull(field(shape, "filter"), "an unfiltered scan carries no filter");
  }

  @Test void testScanShapeFilterDirectlyOnScan() throws Exception {
    TableScan scan = mock(TableScan.class);
    Filter filter = mock(Filter.class);
    when(filter.getInput()).thenReturn(scan);

    Object shape = resolveScanShape(filter);
    assertNotNull(shape);
    assertSame(scan, field(shape, "scan"));
    assertSame(filter, field(shape, "filter"));
  }

  @Test void testScanShapeProjectAboveFilter() throws Exception {
    // A Project above the Filter does not renumber the Filter's own input refs, so it is walked
    // through: this is the shape `SELECT COUNT(*) FROM t WHERE part = 'x'` arrives in.
    TableScan scan = mock(TableScan.class);
    Filter filter = mock(Filter.class);
    when(filter.getInput()).thenReturn(scan);
    Project project = mock(Project.class);
    when(project.getInputs()).thenReturn(Collections.<RelNode>singletonList(filter));
    when(project.getInput(0)).thenReturn(filter);

    Object shape = resolveScanShape(project);
    assertNotNull(shape);
    assertSame(scan, field(shape, "scan"));
    assertSame(filter, field(shape, "filter"));
  }

  @Test void testScanShapeRefusesProjectBetweenFilterAndScan() throws Exception {
    // Here the Project DOES renumber: the Filter's input refs index the Project's output, not
    // the scan's columns, so reading them as column names would name the wrong columns.
    TableScan scan = mock(TableScan.class);
    Project project = mock(Project.class);
    when(project.getInputs()).thenReturn(Collections.<RelNode>singletonList(scan));
    when(project.getInput(0)).thenReturn(scan);
    Filter filter = mock(Filter.class);
    when(filter.getInput()).thenReturn(project);

    assertNull(resolveScanShape(filter));
  }

  @Test void testScanShapeRefusesStackedFilters() throws Exception {
    TableScan scan = mock(TableScan.class);
    Filter inner = mock(Filter.class);
    when(inner.getInput()).thenReturn(scan);
    Filter outer = mock(Filter.class);
    when(outer.getInput()).thenReturn(inner);

    assertNull(resolveScanShape(outer));
  }

  @Test void testScanShapeRefusesJoin() throws Exception {
    // A join predicate becomes the join condition rather than a Filter, so only the whitelist
    // stops a per-table row count from answering COUNT(*) over a join.
    TableScan left = mock(TableScan.class);
    TableScan right = mock(TableScan.class);
    Join join = mock(Join.class);
    when(join.getInputs()).thenReturn(ImmutableList.<RelNode>of(left, right));

    assertNull(resolveScanShape(join));
  }

  @Test void testScanShapeRefusesSort() throws Exception {
    // A Sort with a fetch/offset truncates the row count.
    TableScan scan = mock(TableScan.class);
    Sort sort = mock(Sort.class);
    when(sort.getInputs()).thenReturn(Collections.<RelNode>singletonList(scan));
    when(sort.getInput()).thenReturn(scan);

    assertNull(resolveScanShape(sort));
  }

  @Test void testScanShapeRefusesUnknownNode() throws Exception {
    // An operator the whitelist has never heard of must stop the rule, not default to safe.
    RelNode unknown = mock(RelNode.class);
    when(unknown.getInputs()).thenReturn(Collections.<RelNode>emptyList());

    assertNull(resolveScanShape(unknown));
  }

  // ========== onMatch: input has no table scan ==========

  @Test void testOnMatchNoTableScan() {
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

  @Test void testOnMatchNonDuckDbScan() {
    // A scan that is not DuckDB-backed is the ordinary case for this rule, which is registered
    // on every Aggregate: it must decline quietly, not throw.
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    Aggregate agg = mock(Aggregate.class);
    when(call.rel(0)).thenReturn(agg);

    TableScan scan = mock(TableScan.class);
    org.apache.calcite.plan.RelOptTable relOptTable =
        mock(org.apache.calcite.plan.RelOptTable.class);
    when(relOptTable.getQualifiedName()).thenReturn(ImmutableList.of("cftc", "cftc_trades"));
    when(scan.getTable()).thenReturn(relOptTable);
    when(agg.getInput()).thenReturn(scan);

    DuckDBIcebergCountStarRule.INSTANCE.onMatch(call);
    verify(call, never()).transformTo(any(RelNode.class));
  }

  // ========== helpers ==========

  private static Object resolveScanShape(RelNode node) throws Exception {
    Method m =
        DuckDBIcebergCountStarRule.class.getDeclaredMethod("resolveScanShape", RelNode.class);
    m.setAccessible(true);
    return m.invoke(DuckDBIcebergCountStarRule.INSTANCE, node);
  }

  private static Object field(Object shape, String name) throws Exception {
    java.lang.reflect.Field f = shape.getClass().getDeclaredField(name);
    f.setAccessible(true);
    return f.get(shape);
  }
}
