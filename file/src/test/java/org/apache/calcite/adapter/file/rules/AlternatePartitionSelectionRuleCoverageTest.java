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

import org.apache.calcite.adapter.file.table.PartitionedParquetTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Coverage unit tests for {@link AlternatePartitionSelectionRule}.
 *
 * <p>Tests the rule's filter column extraction, alternate partition selection,
 * and onMatch behavior using mocked RelNode trees and PartitionedParquetTable.
 */
@Tag("unit")
class AlternatePartitionSelectionRuleCoverageTest {

  @Test void testRuleInstanceNotNull() {
    assertNotNull(AlternatePartitionSelectionRule.INSTANCE,
        "INSTANCE should not be null");
  }

  @Test void testRuleIsRelRule() {
    assertTrue(AlternatePartitionSelectionRule.INSTANCE
        instanceof org.apache.calcite.plan.RelRule);
  }

  @Test void testConfigDefault() {
    AlternatePartitionSelectionRule.Config config = AlternatePartitionSelectionRule.Config.DEFAULT;
    assertNotNull(config, "Config.DEFAULT should not be null");

    AlternatePartitionSelectionRule rule = config.toRule();
    assertNotNull(rule, "Config.toRule() should return a rule");
  }

  // ---- extractFilterColumns tests (via reflection) ----

  @Test void testExtractFilterColumnsFromRexInputRef() throws Exception {
    TableScan scan = mock(TableScan.class);
    RelDataType rowType = mock(RelDataType.class);
    when(scan.getRowType()).thenReturn(rowType);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("state", "year", "population"));

    // Create a RexInputRef pointing to column 0 ("state")
    RexInputRef ref = mock(RexInputRef.class);
    when(ref.getIndex()).thenReturn(0);

    Set<String> result = invokeExtractFilterColumns(ref, scan);
    assertTrue(result.contains("state"),
        "Should extract 'state' from RexInputRef at index 0");
  }

  @Test void testExtractFilterColumnsFromEqualsCall() throws Exception {
    TableScan scan = mock(TableScan.class);
    RelDataType rowType = mock(RelDataType.class);
    when(scan.getRowType()).thenReturn(rowType);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("state", "year", "population"));

    // EQUALS(col0, literal)
    RexInputRef ref = mock(RexInputRef.class);
    when(ref.getIndex()).thenReturn(1);
    RexLiteral literal = mock(RexLiteral.class);

    RexCall equalsCall = mock(RexCall.class);
    when(equalsCall.getKind()).thenReturn(SqlKind.EQUALS);
    when(equalsCall.getOperands()).thenReturn(ImmutableList.<RexNode>of(ref, literal));

    Set<String> result = invokeExtractFilterColumns(equalsCall, scan);
    assertTrue(result.contains("year"),
        "Should extract 'year' from EQUALS condition");
  }

  @Test void testExtractFilterColumnsFromAndCondition() throws Exception {
    TableScan scan = mock(TableScan.class);
    RelDataType rowType = mock(RelDataType.class);
    when(scan.getRowType()).thenReturn(rowType);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("state", "year", "population"));

    RexInputRef ref0 = mock(RexInputRef.class);
    when(ref0.getIndex()).thenReturn(0);
    RexInputRef ref1 = mock(RexInputRef.class);
    when(ref1.getIndex()).thenReturn(1);
    RexLiteral lit = mock(RexLiteral.class);

    RexCall eq1 = mock(RexCall.class);
    when(eq1.getKind()).thenReturn(SqlKind.EQUALS);
    when(eq1.getOperands()).thenReturn(ImmutableList.<RexNode>of(ref0, lit));

    RexCall gt1 = mock(RexCall.class);
    when(gt1.getKind()).thenReturn(SqlKind.GREATER_THAN);
    when(gt1.getOperands()).thenReturn(ImmutableList.<RexNode>of(ref1, lit));

    RexCall andCall = mock(RexCall.class);
    when(andCall.getKind()).thenReturn(SqlKind.AND);
    when(andCall.getOperands()).thenReturn(ImmutableList.<RexNode>of(eq1, gt1));

    Set<String> result = invokeExtractFilterColumns(andCall, scan);
    assertTrue(result.contains("state"), "Should extract 'state' from AND condition");
    assertTrue(result.contains("year"), "Should extract 'year' from AND condition");
  }

  @Test void testExtractFilterColumnsFromOrCondition() throws Exception {
    TableScan scan = mock(TableScan.class);
    RelDataType rowType = mock(RelDataType.class);
    when(scan.getRowType()).thenReturn(rowType);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col_a", "col_b"));

    RexInputRef ref0 = mock(RexInputRef.class);
    when(ref0.getIndex()).thenReturn(0);
    RexInputRef ref1 = mock(RexInputRef.class);
    when(ref1.getIndex()).thenReturn(1);
    RexLiteral lit = mock(RexLiteral.class);

    RexCall lt = mock(RexCall.class);
    when(lt.getKind()).thenReturn(SqlKind.LESS_THAN);
    when(lt.getOperands()).thenReturn(ImmutableList.<RexNode>of(ref0, lit));

    RexCall gte = mock(RexCall.class);
    when(gte.getKind()).thenReturn(SqlKind.GREATER_THAN_OR_EQUAL);
    when(gte.getOperands()).thenReturn(ImmutableList.<RexNode>of(ref1, lit));

    RexCall orCall = mock(RexCall.class);
    when(orCall.getKind()).thenReturn(SqlKind.OR);
    when(orCall.getOperands()).thenReturn(ImmutableList.<RexNode>of(lt, gte));

    Set<String> result = invokeExtractFilterColumns(orCall, scan);
    assertTrue(result.contains("col_a"));
    assertTrue(result.contains("col_b"));
  }

  @Test void testExtractFilterColumnsFromLessThanOrEqual() throws Exception {
    TableScan scan = mock(TableScan.class);
    RelDataType rowType = mock(RelDataType.class);
    when(scan.getRowType()).thenReturn(rowType);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("amount"));

    RexInputRef ref = mock(RexInputRef.class);
    when(ref.getIndex()).thenReturn(0);
    RexLiteral lit = mock(RexLiteral.class);

    RexCall lteCall = mock(RexCall.class);
    when(lteCall.getKind()).thenReturn(SqlKind.LESS_THAN_OR_EQUAL);
    when(lteCall.getOperands()).thenReturn(ImmutableList.<RexNode>of(ref, lit));

    Set<String> result = invokeExtractFilterColumns(lteCall, scan);
    assertTrue(result.contains("amount"));
  }

  @Test void testExtractFilterColumnsFromInCondition() throws Exception {
    TableScan scan = mock(TableScan.class);
    RelDataType rowType = mock(RelDataType.class);
    when(scan.getRowType()).thenReturn(rowType);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("category"));

    RexInputRef ref = mock(RexInputRef.class);
    when(ref.getIndex()).thenReturn(0);
    RexLiteral lit = mock(RexLiteral.class);

    RexCall inCall = mock(RexCall.class);
    when(inCall.getKind()).thenReturn(SqlKind.IN);
    when(inCall.getOperands()).thenReturn(ImmutableList.<RexNode>of(ref, lit));

    Set<String> result = invokeExtractFilterColumns(inCall, scan);
    assertTrue(result.contains("category"));
  }

  @Test void testExtractFilterColumnsIgnoresUnsupportedKind() throws Exception {
    TableScan scan = mock(TableScan.class);
    RelDataType rowType = mock(RelDataType.class);
    when(scan.getRowType()).thenReturn(rowType);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("col1"));

    // A LIKE call should not extract columns
    RexCall likeCall = mock(RexCall.class);
    when(likeCall.getKind()).thenReturn(SqlKind.LIKE);

    Set<String> result = invokeExtractFilterColumns(likeCall, scan);
    assertTrue(result.isEmpty(),
        "LIKE condition should not contribute to extracted filter columns");
  }

  @Test void testExtractFilterColumnsLowercases() throws Exception {
    TableScan scan = mock(TableScan.class);
    RelDataType rowType = mock(RelDataType.class);
    when(scan.getRowType()).thenReturn(rowType);
    when(rowType.getFieldNames()).thenReturn(Arrays.asList("MixedCase"));

    RexInputRef ref = mock(RexInputRef.class);
    when(ref.getIndex()).thenReturn(0);

    Set<String> result = invokeExtractFilterColumns(ref, scan);
    assertTrue(result.contains("mixedcase"),
        "Column names should be lowercased");
    assertFalse(result.contains("MixedCase"),
        "Original case should not be present");
  }

  // ---- selectBestAlternate tests (via reflection) ----

  @Test void testSelectBestAlternateSingleMatch() throws Exception {
    List<Object> alternates = new ArrayList<Object>();
    alternates.add(createAlternatePartitionInfo("alt1", "alt1/*.parquet",
        Arrays.asList("state")));

    Set<String> filterColumns = new HashSet<String>();
    filterColumns.add("state");

    Object best = invokeSelectBestAlternate(alternates, filterColumns);
    assertNotNull(best, "Should select the alternate that covers all filter columns");
  }

  @Test void testSelectBestAlternatePreferFewerKeys() throws Exception {
    List<Object> alternates = new ArrayList<Object>();
    alternates.add(createAlternatePartitionInfo("alt_many", "alt_many/*.parquet",
        Arrays.asList("state", "year", "county")));
    alternates.add(createAlternatePartitionInfo("alt_few", "alt_few/*.parquet",
        Arrays.asList("state")));

    Set<String> filterColumns = new HashSet<String>();
    filterColumns.add("state");

    Object best = invokeSelectBestAlternate(alternates, filterColumns);
    assertNotNull(best, "Should select alternate");

    // Verify the best is alt_few (fewer partition keys)
    java.lang.reflect.Field nameField = best.getClass().getDeclaredField("tableName");
    nameField.setAccessible(true);
    assertEquals("alt_few", nameField.get(best),
        "Should prefer alternate with fewer partition keys");
  }

  @Test void testSelectBestAlternateNoMatchingKeys() throws Exception {
    List<Object> alternates = new ArrayList<Object>();
    alternates.add(createAlternatePartitionInfo("alt1", "alt1/*.parquet",
        Arrays.asList("year")));

    Set<String> filterColumns = new HashSet<String>();
    filterColumns.add("state");

    Object best = invokeSelectBestAlternate(alternates, filterColumns);
    assertNull(best, "Should return null when no alternate covers filter columns");
  }

  @Test void testSelectBestAlternatePartialCoverage() throws Exception {
    // Alternate has "state" but filter requires "state" AND "county"
    List<Object> alternates = new ArrayList<Object>();
    alternates.add(createAlternatePartitionInfo("alt1", "alt1/*.parquet",
        Arrays.asList("state")));

    Set<String> filterColumns = new HashSet<String>();
    filterColumns.add("state");
    filterColumns.add("county");

    Object best = invokeSelectBestAlternate(alternates, filterColumns);
    assertNull(best,
        "Should return null when alternate does not cover all filter columns");
  }

  @Test void testSelectBestAlternateCaseInsensitive() throws Exception {
    // Partition keys in different case
    List<Object> alternates = new ArrayList<Object>();
    alternates.add(createAlternatePartitionInfo("alt1", "alt1/*.parquet",
        Arrays.asList("State", "Year")));

    Set<String> filterColumns = new HashSet<String>();
    filterColumns.add("state");
    filterColumns.add("year");

    Object best = invokeSelectBestAlternate(alternates, filterColumns);
    assertNotNull(best, "Case-insensitive matching should work");
  }

  @Test void testSelectBestAlternateEmptyAlternates() throws Exception {
    List<Object> alternates = new ArrayList<Object>();

    Set<String> filterColumns = new HashSet<String>();
    filterColumns.add("state");

    Object best = invokeSelectBestAlternate(alternates, filterColumns);
    assertNull(best, "Empty alternates list should return null");
  }

  // ---- onMatch tests ----

  @Test void testOnMatchWithNonPartitionedTable() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    LogicalFilter filter = mock(LogicalFilter.class);
    TableScan scan = mock(TableScan.class);
    RelOptTable relOptTable = mock(RelOptTable.class);
    Table plainTable = mock(Table.class);

    when(call.rel(0)).thenReturn(filter);
    when(call.rel(1)).thenReturn(scan);
    when(scan.getTable()).thenReturn(relOptTable);
    when(relOptTable.unwrap(Table.class)).thenReturn(plainTable);

    AlternatePartitionSelectionRule.INSTANCE.onMatch(call);
    // Should not transform - not a PartitionedParquetTable
    verify(call, never()).transformTo(org.mockito.ArgumentMatchers.any(RelNode.class));
  }

  @Test void testOnMatchWithPartitionedTableNoAlternates() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    LogicalFilter filter = mock(LogicalFilter.class);
    TableScan scan = mock(TableScan.class);
    RelOptTable relOptTable = mock(RelOptTable.class);
    PartitionedParquetTable partTable = mock(PartitionedParquetTable.class);

    when(call.rel(0)).thenReturn(filter);
    when(call.rel(1)).thenReturn(scan);
    when(scan.getTable()).thenReturn(relOptTable);
    when(relOptTable.unwrap(Table.class)).thenReturn(partTable);

    // No registry, so no alternates
    when(relOptTable.getQualifiedName()).thenReturn(Arrays.asList("root", "schema", "table1"));
    RelOptSchema schema = mock(RelOptSchema.class);
    when(relOptTable.getRelOptSchema()).thenReturn(schema);
    when(schema.getTableForMember(org.mockito.ArgumentMatchers.anyList())).thenReturn(null);

    AlternatePartitionSelectionRule.INSTANCE.onMatch(call);
    // Should not transform - no alternates found
    verify(call, never()).transformTo(org.mockito.ArgumentMatchers.any(RelNode.class));
  }

  @Test void testOnMatchWithEmptyFilterColumns() {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    LogicalFilter filter = mock(LogicalFilter.class);
    TableScan scan = mock(TableScan.class);
    RelOptTable relOptTable = mock(RelOptTable.class);
    PartitionedParquetTable partTable = mock(PartitionedParquetTable.class);

    when(call.rel(0)).thenReturn(filter);
    when(call.rel(1)).thenReturn(scan);
    when(scan.getTable()).thenReturn(relOptTable);
    when(relOptTable.unwrap(Table.class)).thenReturn(partTable);
    when(relOptTable.getQualifiedName()).thenReturn(Arrays.asList("root", "schema", "table1"));

    // Return no schema/registry
    RelOptSchema schema = mock(RelOptSchema.class);
    when(relOptTable.getRelOptSchema()).thenReturn(schema);
    when(schema.getTableForMember(org.mockito.ArgumentMatchers.anyList())).thenReturn(null);

    // Filter with non-extractable condition (e.g., literal-only)
    RexLiteral condition = mock(RexLiteral.class);
    when(filter.getCondition()).thenReturn(condition);

    AlternatePartitionSelectionRule.INSTANCE.onMatch(call);
    verify(call, never()).transformTo(org.mockito.ArgumentMatchers.any(RelNode.class));
  }

  // ---- getRegistryFromSchema tests (via reflection) ----

  @Test void testGetRegistryFromSchemaNullRelOptSchema() throws Exception {
    java.lang.reflect.Method method =
        AlternatePartitionSelectionRule.class.getDeclaredMethod(
            "getRegistryFromSchema", RelOptTable.class);
    method.setAccessible(true);

    RelOptTable table = mock(RelOptTable.class);
    when(table.getRelOptSchema()).thenReturn(null);

    Object result = method.invoke(AlternatePartitionSelectionRule.INSTANCE, table);
    assertNull(result, "Should return null when RelOptSchema is null");
  }

  @Test void testGetRegistryFromSchemaShortQualifiedName() throws Exception {
    java.lang.reflect.Method method =
        AlternatePartitionSelectionRule.class.getDeclaredMethod(
            "getRegistryFromSchema", RelOptTable.class);
    method.setAccessible(true);

    RelOptTable table = mock(RelOptTable.class);
    RelOptSchema schema = mock(RelOptSchema.class);
    when(table.getRelOptSchema()).thenReturn(schema);
    when(table.getQualifiedName()).thenReturn(Collections.singletonList("table_only"));

    Object result = method.invoke(AlternatePartitionSelectionRule.INSTANCE, table);
    assertNull(result,
        "Should return null when qualified name has fewer than 2 parts");
  }

  // ---- getAlternatePartitions tests (via reflection) ----

  @Test void testGetAlternatePartitionsNonPartitioned() throws Exception {
    java.lang.reflect.Method method =
        AlternatePartitionSelectionRule.class.getDeclaredMethod(
            "getAlternatePartitions", RelOptTable.class);
    method.setAccessible(true);

    RelOptTable table = mock(RelOptTable.class);
    Table plainTable = mock(Table.class);
    when(table.unwrap(Table.class)).thenReturn(plainTable);

    @SuppressWarnings("unchecked")
    List<Object> result = (List<Object>) method.invoke(
        AlternatePartitionSelectionRule.INSTANCE, table);
    assertTrue(result.isEmpty(),
        "Non-PartitionedParquetTable should return empty list");
  }

  @Test void testGetAlternatePartitionsShortName() throws Exception {
    java.lang.reflect.Method method =
        AlternatePartitionSelectionRule.class.getDeclaredMethod(
            "getAlternatePartitions", RelOptTable.class);
    method.setAccessible(true);

    RelOptTable table = mock(RelOptTable.class);
    PartitionedParquetTable partTable = mock(PartitionedParquetTable.class);
    when(table.unwrap(Table.class)).thenReturn(partTable);
    when(table.getQualifiedName()).thenReturn(Collections.singletonList("tablename"));

    @SuppressWarnings("unchecked")
    List<Object> result = (List<Object>) method.invoke(
        AlternatePartitionSelectionRule.INSTANCE, table);
    assertTrue(result.isEmpty(),
        "Single-element qualified name should return empty list");
  }

  // ---- findAlternateTable tests (via reflection) ----

  @Test void testFindAlternateTableShortName() throws Exception {
    java.lang.reflect.Method method =
        AlternatePartitionSelectionRule.class.getDeclaredMethod(
            "findAlternateTable", TableScan.class, String.class, RelOptRuleCall.class);
    method.setAccessible(true);

    TableScan scan = mock(TableScan.class);
    RelOptTable table = mock(RelOptTable.class);
    when(scan.getTable()).thenReturn(table);
    when(table.getQualifiedName()).thenReturn(Collections.singletonList("table_only"));

    RelOptRuleCall call = mock(RelOptRuleCall.class);

    Object result = method.invoke(
        AlternatePartitionSelectionRule.INSTANCE, scan, "alt_table", call);
    assertNull(result,
        "Short qualified name should return null");
  }

  @Test void testFindAlternateTableNullSchema() throws Exception {
    java.lang.reflect.Method method =
        AlternatePartitionSelectionRule.class.getDeclaredMethod(
            "findAlternateTable", TableScan.class, String.class, RelOptRuleCall.class);
    method.setAccessible(true);

    TableScan scan = mock(TableScan.class);
    RelOptTable table = mock(RelOptTable.class);
    when(scan.getTable()).thenReturn(table);
    when(table.getQualifiedName()).thenReturn(Arrays.asList("root", "schema", "table"));
    when(table.getRelOptSchema()).thenReturn(null);

    RelOptRuleCall call = mock(RelOptRuleCall.class);

    Object result = method.invoke(
        AlternatePartitionSelectionRule.INSTANCE, scan, "alt_table", call);
    assertNull(result,
        "Null RelOptSchema should return null");
  }

  // ---- Helpers ----

  @SuppressWarnings("unchecked")
  private Set<String> invokeExtractFilterColumns(RexNode condition, TableScan scan)
      throws Exception {
    java.lang.reflect.Method method =
        AlternatePartitionSelectionRule.class.getDeclaredMethod(
            "extractFilterColumns", RexNode.class, TableScan.class);
    method.setAccessible(true);
    return (Set<String>) method.invoke(AlternatePartitionSelectionRule.INSTANCE, condition, scan);
  }

  @SuppressWarnings("unchecked")
  private Object invokeSelectBestAlternate(List<Object> alternates, Set<String> filterColumns)
      throws Exception {
    java.lang.reflect.Method method =
        AlternatePartitionSelectionRule.class.getDeclaredMethod(
            "selectBestAlternate", List.class, Set.class);
    method.setAccessible(true);
    return method.invoke(AlternatePartitionSelectionRule.INSTANCE, alternates, filterColumns);
  }

  private Object createAlternatePartitionInfo(String tableName, String pattern,
      List<String> partitionKeys) throws Exception {
    Class<?>[] innerClasses = AlternatePartitionSelectionRule.class.getDeclaredClasses();
    Class<?> altInfoClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("AlternatePartitionInfo")) {
        altInfoClass = c;
        break;
      }
    }
    assertNotNull(altInfoClass, "AlternatePartitionInfo inner class should exist");

    java.lang.reflect.Constructor<?> ctor = altInfoClass.getDeclaredConstructor(
        String.class, String.class, List.class);
    ctor.setAccessible(true);
    return ctor.newInstance(tableName, pattern, partitionKeys);
  }
}
