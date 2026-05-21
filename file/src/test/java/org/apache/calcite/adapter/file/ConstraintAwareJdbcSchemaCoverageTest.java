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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.schema.CommentableSchema;
import org.apache.calcite.schema.CommentableTable;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Coverage tests for {@link ConstraintAwareJdbcSchema} and its inner class
 * ConstraintAwareJdbcTable.
 */
@Tag("unit")
public class ConstraintAwareJdbcSchemaCoverageTest {

  private JdbcSchema mockDelegate;
  private Map<String, Map<String, Object>> constraintMetadata;

  @BeforeEach
  public void setUp() {
    mockDelegate = mock(JdbcSchema.class);
    constraintMetadata = new LinkedHashMap<String, Map<String, Object>>();
  }

  // =========================================================================
  // ConstraintAwareJdbcSchema constructor and delegation
  // =========================================================================

  @Test public void testConstructorWithNullMetadata() {
    ConstraintAwareJdbcSchema schema = new ConstraintAwareJdbcSchema(mockDelegate, null);
    assertNotNull(schema);
    assertEquals(mockDelegate, schema.getDelegate());
  }

  @Test public void testConstructorWithEmptyMetadata() {
    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertNotNull(schema);
  }

  @Test public void testConstructorWithMetadata() {
    Map<String, Object> tableConstraints = new LinkedHashMap<String, Object>();
    tableConstraints.put("primaryKey", Collections.singletonList("id"));
    constraintMetadata.put("users", tableConstraints);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertNotNull(schema);
  }

  @Test public void testGetDelegate() {
    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertEquals(mockDelegate, schema.getDelegate());
  }

  // =========================================================================
  // Delegation methods
  // =========================================================================

  @SuppressWarnings("deprecation")
  @Test public void testGetTableNames() {
    Set<String> names = new HashSet<String>();
    names.add("users");
    names.add("orders");
    when(mockDelegate.getTableNames()).thenReturn(names);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertEquals(names, schema.getTableNames());
  }

  @Test public void testGetType() {
    when(mockDelegate.getType("mytype")).thenReturn(null);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertNull(schema.getType("mytype"));
  }

  @Test public void testGetTypeNames() {
    Set<String> names = new HashSet<String>();
    when(mockDelegate.getTypeNames()).thenReturn(names);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertEquals(names, schema.getTypeNames());
  }

  @Test public void testGetFunctions() {
    Collection<Function> fns = Collections.emptyList();
    when(mockDelegate.getFunctions("myfunc")).thenReturn(fns);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertEquals(fns, schema.getFunctions("myfunc"));
  }

  @Test public void testGetFunctionNames() {
    Set<String> names = new HashSet<String>();
    when(mockDelegate.getFunctionNames()).thenReturn(names);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertEquals(names, schema.getFunctionNames());
  }

  @SuppressWarnings("deprecation")
  @Test public void testGetSubSchema() {
    when(mockDelegate.getSubSchema("sub")).thenReturn(null);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertNull(schema.getSubSchema("sub"));
  }

  @SuppressWarnings("deprecation")
  @Test public void testGetSubSchemaNames() {
    Set<String> names = new HashSet<String>();
    when(mockDelegate.getSubSchemaNames()).thenReturn(names);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertEquals(names, schema.getSubSchemaNames());
  }

  @Test public void testGetExpression() {
    Expression mockExpr = mock(Expression.class);
    when(mockDelegate.getExpression(null, "name")).thenReturn(mockExpr);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertEquals(mockExpr, schema.getExpression(null, "name"));
  }

  @Test public void testIsMutable() {
    when(mockDelegate.isMutable()).thenReturn(true);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertTrue(schema.isMutable());
  }

  @Test public void testSnapshot() {
    SchemaVersion version = mock(SchemaVersion.class);
    Schema mockSnapshot = mock(Schema.class);
    when(mockDelegate.snapshot(version)).thenReturn(mockSnapshot);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertEquals(mockSnapshot, schema.snapshot(version));
  }

  // =========================================================================
  // unwrap
  // =========================================================================

  @Test public void testUnwrapSelf() {
    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);

    ConstraintAwareJdbcSchema unwrapped = schema.unwrap(ConstraintAwareJdbcSchema.class);
    assertEquals(schema, unwrapped);
  }

  @Test public void testUnwrapSchema() {
    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);

    Schema unwrapped = schema.unwrap(Schema.class);
    assertEquals(schema, unwrapped);
  }

  @Test public void testUnwrapDelegatesToJdbcSchema() {
    when(mockDelegate.unwrap(JdbcSchema.class)).thenReturn(mockDelegate);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);

    JdbcSchema unwrapped = schema.unwrap(JdbcSchema.class);
    assertEquals(mockDelegate, unwrapped);
  }

  // =========================================================================
  // getComment
  // =========================================================================

  @Test public void testGetCommentDelegateIsCommentable() {
    // Create a mock that implements both JdbcSchema and CommentableSchema
    JdbcSchema commentableDelegate =
        mock(JdbcSchema.class, Mockito.withSettings().extraInterfaces(CommentableSchema.class));
    when(((CommentableSchema) commentableDelegate).getComment()).thenReturn("My schema comment");

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(commentableDelegate, constraintMetadata);
    assertEquals("My schema comment", schema.getComment());
  }

  @Test public void testGetCommentDelegateNotCommentable() {
    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertNull(schema.getComment());
  }

  // =========================================================================
  // getTable - no constraints for table
  // =========================================================================

  @SuppressWarnings("deprecation")
  @Test public void testGetTableNoConstraints() {
    Table mockTable = mock(Table.class);
    when(mockDelegate.getTable("users")).thenReturn(mockTable);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    Table result = schema.getTable("users");
    assertEquals(mockTable, result);
  }

  @SuppressWarnings("deprecation")
  @Test public void testGetTableReturnsNull() {
    when(mockDelegate.getTable("nonexistent")).thenReturn(null);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    assertNull(schema.getTable("nonexistent"));
  }

  // =========================================================================
  // getTable - with constraints, JdbcTable directly
  // =========================================================================

  @SuppressWarnings("deprecation")
  @Test public void testGetTableWithConstraintsDirectJdbcTable() {
    Map<String, Object> tableConstraints = new LinkedHashMap<String, Object>();
    tableConstraints.put("primaryKey", Collections.singletonList("id"));
    constraintMetadata.put("users", tableConstraints);

    JdbcTable mockJdbcTable = mock(JdbcTable.class);
    when(mockDelegate.getTable("users")).thenReturn(mockJdbcTable);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    Table result = schema.getTable("users");

    // Should be wrapped in ConstraintAwareJdbcTable
    assertNotNull(result);
    assertTrue(result instanceof CommentableTable,
        "Expected CommentableTable but got " + result.getClass().getName());
  }

  // =========================================================================
  // getTable - with constraints, non-JdbcTable (CommentableJdbcTableWrapper)
  // =========================================================================

  @SuppressWarnings("deprecation")
  @Test public void testGetTableWithConstraintsNonJdbcTable() {
    Map<String, Object> tableConstraints = new LinkedHashMap<String, Object>();
    tableConstraints.put("primaryKey", Collections.singletonList("id"));
    constraintMetadata.put("users", tableConstraints);

    // Return a non-JdbcTable from getTable
    Table mockWrappedTable = mock(Table.class);
    when(mockDelegate.getTable("users")).thenReturn(mockWrappedTable);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    Table result = schema.getTable("users");

    // When reflection fails to get JdbcTable, it returns the unwrapped table
    assertNotNull(result);
  }

  // =========================================================================
  // ConstraintAwareJdbcTable inner class - via getTable
  // =========================================================================

  @SuppressWarnings("deprecation")
  @Test public void testConstraintAwareJdbcTableGetJdbcTableType() {
    Map<String, Object> tableConstraints = new LinkedHashMap<String, Object>();
    tableConstraints.put("primaryKey", Collections.singletonList("id"));
    constraintMetadata.put("users", tableConstraints);

    JdbcTable mockJdbcTable = mock(JdbcTable.class);
    when(mockJdbcTable.getJdbcTableType()).thenReturn(Schema.TableType.TABLE);
    when(mockDelegate.getTable("users")).thenReturn(mockJdbcTable);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    Table result = schema.getTable("users");

    assertNotNull(result);
    assertEquals(Schema.TableType.TABLE, result.getJdbcTableType());
  }

  @SuppressWarnings("deprecation")
  @Test public void testConstraintAwareJdbcTableGetRowType() {
    Map<String, Object> tableConstraints = new LinkedHashMap<String, Object>();
    tableConstraints.put("primaryKey", Collections.singletonList("id"));
    constraintMetadata.put("users", tableConstraints);

    JdbcTable mockJdbcTable = mock(JdbcTable.class);
    RelDataTypeFactory mockFactory = mock(RelDataTypeFactory.class);
    RelDataType mockRowType = mock(RelDataType.class);
    when(mockJdbcTable.getRowType(mockFactory)).thenReturn(mockRowType);
    when(mockDelegate.getTable("users")).thenReturn(mockJdbcTable);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    Table result = schema.getTable("users");

    assertNotNull(result);
    assertEquals(mockRowType, result.getRowType(mockFactory));
  }

  @SuppressWarnings("deprecation")
  @Test public void testConstraintAwareJdbcTableIsRolledUp() {
    Map<String, Object> tableConstraints = new LinkedHashMap<String, Object>();
    tableConstraints.put("primaryKey", Collections.singletonList("id"));
    constraintMetadata.put("users", tableConstraints);

    JdbcTable mockJdbcTable = mock(JdbcTable.class);
    when(mockJdbcTable.isRolledUp("col")).thenReturn(false);
    when(mockDelegate.getTable("users")).thenReturn(mockJdbcTable);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    Table result = schema.getTable("users");

    assertNotNull(result);
    assertEquals(false, result.isRolledUp("col"));
  }

  @SuppressWarnings("deprecation")
  @Test public void testConstraintAwareJdbcTableRolledUpColumnValidInsideAgg() {
    Map<String, Object> tableConstraints = new LinkedHashMap<String, Object>();
    tableConstraints.put("primaryKey", Collections.singletonList("id"));
    constraintMetadata.put("users", tableConstraints);

    JdbcTable mockJdbcTable = mock(JdbcTable.class);
    when(mockJdbcTable.rolledUpColumnValidInsideAgg("col", null, null, null)).thenReturn(true);
    when(mockDelegate.getTable("users")).thenReturn(mockJdbcTable);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    Table result = schema.getTable("users");

    assertNotNull(result);
    assertTrue(result.rolledUpColumnValidInsideAgg("col", null, null, null));
  }

  // =========================================================================
  // ConstraintAwareJdbcTable - getTableComment / getColumnComment
  // =========================================================================

  @SuppressWarnings("deprecation")
  @Test public void testConstraintAwareJdbcTableGetTableCommentWithCommentable() {
    Map<String, Object> tableConstraints = new LinkedHashMap<String, Object>();
    tableConstraints.put("primaryKey", Collections.singletonList("id"));
    constraintMetadata.put("users", tableConstraints);

    // Create a mock that is both JdbcTable and CommentableTable
    JdbcTable mockJdbcTable =
        mock(JdbcTable.class, Mockito.withSettings().extraInterfaces(CommentableTable.class));
    when(((CommentableTable) mockJdbcTable).getTableComment()).thenReturn("User table");
    when(((CommentableTable) mockJdbcTable).getColumnComment("id")).thenReturn("Primary key");
    when(mockDelegate.getTable("users")).thenReturn(mockJdbcTable);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    Table result = schema.getTable("users");

    assertNotNull(result);
    assertTrue(result instanceof CommentableTable);
    assertEquals("User table", ((CommentableTable) result).getTableComment());
    assertEquals("Primary key", ((CommentableTable) result).getColumnComment("id"));
  }

  @SuppressWarnings("deprecation")
  @Test public void testConstraintAwareJdbcTableGetTableCommentNotCommentable() {
    Map<String, Object> tableConstraints = new LinkedHashMap<String, Object>();
    tableConstraints.put("primaryKey", Collections.singletonList("id"));
    constraintMetadata.put("users", tableConstraints);

    JdbcTable mockJdbcTable = mock(JdbcTable.class);
    when(mockDelegate.getTable("users")).thenReturn(mockJdbcTable);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    Table result = schema.getTable("users");

    assertNotNull(result);
    assertTrue(result instanceof CommentableTable);
    // wrappedDelegate is JdbcTable (not CommentableTable), so returns null
    assertNull(((CommentableTable) result).getTableComment());
    assertNull(((CommentableTable) result).getColumnComment("id"));
  }

  // =========================================================================
  // ConstraintAwareJdbcTable - getStatistic with constraint metadata
  // =========================================================================

  @SuppressWarnings("deprecation")
  @Test public void testConstraintAwareJdbcTableGetStatistic() {
    Map<String, Object> tableConstraints = new LinkedHashMap<String, Object>();
    tableConstraints.put("primaryKey", Collections.singletonList("id"));
    constraintMetadata.put("users", tableConstraints);

    JdbcTable mockJdbcTable = mock(JdbcTable.class);

    // Setup row type mock
    RelDataType mockRowType = mock(RelDataType.class);
    List<RelDataTypeField> fields = new ArrayList<RelDataTypeField>();
    RelDataType varcharType = mock(RelDataType.class);
    fields.add(new RelDataTypeFieldImpl("id", 0, varcharType));
    fields.add(new RelDataTypeFieldImpl("name", 1, varcharType));
    when(mockRowType.getFieldList()).thenReturn(fields);
    when(mockJdbcTable.getRowType(Mockito.any(RelDataTypeFactory.class))).thenReturn(mockRowType);

    // Setup base statistic
    Statistic baseStat = Statistics.UNKNOWN;
    when(mockJdbcTable.getStatistic()).thenReturn(baseStat);

    when(mockDelegate.getTable("users")).thenReturn(mockJdbcTable);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    Table result = schema.getTable("users");

    assertNotNull(result);
    Statistic stat = result.getStatistic();
    assertNotNull(stat);
  }

  @SuppressWarnings("deprecation")
  @Test public void testConstraintAwareJdbcTableGetStatisticCaching() {
    Map<String, Object> tableConstraints = new LinkedHashMap<String, Object>();
    tableConstraints.put("primaryKey", Collections.singletonList("id"));
    constraintMetadata.put("users", tableConstraints);

    JdbcTable mockJdbcTable = mock(JdbcTable.class);

    RelDataType mockRowType = mock(RelDataType.class);
    List<RelDataTypeField> fields = new ArrayList<RelDataTypeField>();
    RelDataType varcharType = mock(RelDataType.class);
    fields.add(new RelDataTypeFieldImpl("id", 0, varcharType));
    when(mockRowType.getFieldList()).thenReturn(fields);
    when(mockJdbcTable.getRowType(Mockito.any(RelDataTypeFactory.class))).thenReturn(mockRowType);

    Statistic baseStat = Statistics.UNKNOWN;
    when(mockJdbcTable.getStatistic()).thenReturn(baseStat);

    when(mockDelegate.getTable("users")).thenReturn(mockJdbcTable);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    Table result = schema.getTable("users");

    // Call getStatistic twice - second call should return cached
    Statistic stat1 = result.getStatistic();
    Statistic stat2 = result.getStatistic();
    assertNotNull(stat1);
    assertNotNull(stat2);
    // Both should be the same object (cached)
    assertTrue(stat1 == stat2, "Expected cached statistic to be same object");
  }

  @SuppressWarnings("deprecation")
  @Test public void testConstraintAwareJdbcTableGetStatisticErrorFallback() {
    Map<String, Object> tableConstraints = new LinkedHashMap<String, Object>();
    tableConstraints.put("primaryKey", Collections.singletonList("id"));
    constraintMetadata.put("users", tableConstraints);

    JdbcTable mockJdbcTable = mock(JdbcTable.class);

    // Make getRowType throw to trigger the error fallback
    when(mockJdbcTable.getRowType(Mockito.any(RelDataTypeFactory.class)))
        .thenThrow(new RuntimeException("Test error"));

    Statistic baseStat = Statistics.UNKNOWN;
    when(mockJdbcTable.getStatistic()).thenReturn(baseStat);

    when(mockDelegate.getTable("users")).thenReturn(mockJdbcTable);

    ConstraintAwareJdbcSchema schema =
        new ConstraintAwareJdbcSchema(mockDelegate, constraintMetadata);
    Table result = schema.getTable("users");

    // Should fall back to delegate's statistic
    Statistic stat = result.getStatistic();
    assertNotNull(stat);
  }
}
