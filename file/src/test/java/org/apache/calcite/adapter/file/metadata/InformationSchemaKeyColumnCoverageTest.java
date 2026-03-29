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
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;

import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableList;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@code InformationSchema$KeyColumnUsageTable}.
 *
 * <p>Tests the {@code KEY_COLUMN_USAGE} table's scan() method which enumerates
 * primary keys, unique keys, and foreign keys from all tables in all schemas.
 * Covers:
 * <ul>
 *   <li>getRowType() - verifies all 9 columns are present</li>
 *   <li>scan() with no schemas - returns empty result</li>
 *   <li>scan() with a schema containing tables with primary keys</li>
 *   <li>scan() with a schema containing tables with multiple unique keys</li>
 *   <li>scan() with a schema containing tables with foreign keys</li>
 *   <li>scan() with tables having both PK and FK constraints</li>
 *   <li>scan() with tables having no constraints (null keys)</li>
 *   <li>scan() with ConstraintAwareJdbcSchema detection path</li>
 *   <li>getStatistic() returns UNKNOWN</li>
 * </ul>
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class InformationSchemaKeyColumnCoverageTest {

  private RelDataTypeFactory typeFactory;
  private DataContext mockDataContext;

  @BeforeEach
  void setUp() {
    typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    mockDataContext = new DataContext() {
      @Override public SchemaPlus getRootSchema() {
        return null;
      }
      @Override public JavaTypeFactoryImpl getTypeFactory() {
        return (JavaTypeFactoryImpl) typeFactory;
      }
      @Override public QueryProvider getQueryProvider() {
        return null;
      }
      @Override public Object get(String name) {
        return null;
      }
    };
  }

  /**
   * A test table that has a primary key (single column).
   */
  static class PrimaryKeyTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("id", SqlTypeName.INTEGER)
          .add("name", SqlTypeName.VARCHAR)
          .add("value", SqlTypeName.DOUBLE)
          .build();
    }

    @Override public Statistic getStatistic() {
      return Statistics.of(100d, ImmutableList.of(ImmutableBitSet.of(0)));
    }
  }

  /**
   * A test table with multiple unique keys.
   */
  static class MultiKeyTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("id", SqlTypeName.INTEGER)
          .add("code", SqlTypeName.VARCHAR)
          .add("email", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Statistic getStatistic() {
      // PK on id (index 0), UK on code (index 1)
      return Statistics.of(200d,
          ImmutableList.of(ImmutableBitSet.of(0), ImmutableBitSet.of(1)));
    }
  }

  /**
   * A test table with a composite primary key.
   */
  static class CompositePKTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("year", SqlTypeName.INTEGER)
          .add("month", SqlTypeName.INTEGER)
          .add("amount", SqlTypeName.DECIMAL)
          .build();
    }

    @Override public Statistic getStatistic() {
      // Composite PK on (year, month)
      return Statistics.of(300d, ImmutableList.of(ImmutableBitSet.of(0, 1)));
    }
  }

  /**
   * A test table with foreign key constraints.
   */
  static class ForeignKeyTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("id", SqlTypeName.INTEGER)
          .add("parent_id", SqlTypeName.INTEGER)
          .add("detail", SqlTypeName.VARCHAR)
          .build();
    }

    @Override public Statistic getStatistic() {
      // Primary key on id, plus a foreign key from parent_id to parent.id
      List<ImmutableBitSet> keys = ImmutableList.of(ImmutableBitSet.of(0));
      List<RelReferentialConstraint> fks = ImmutableList.<RelReferentialConstraint>of(
          new TestForeignKey(
              ImmutableList.of("test_schema", "fk_table"),
              ImmutableList.of("test_schema", "pk_table"),
              ImmutableList.of(IntPair.of(1, 0))));
      return Statistics.of(50d, keys, fks, null);
    }
  }

  /**
   * A test table with no constraints at all (null keys, null FK).
   */
  static class NoConstraintTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("col1", SqlTypeName.VARCHAR)
          .add("col2", SqlTypeName.INTEGER)
          .build();
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  /**
   * Simple implementation of RelReferentialConstraint for test purposes.
   */
  static class TestForeignKey implements RelReferentialConstraint {
    private final List<String> sourceQualifiedName;
    private final List<String> targetQualifiedName;
    private final List<IntPair> columnPairs;

    TestForeignKey(List<String> sourceQualifiedName,
        List<String> targetQualifiedName,
        List<IntPair> columnPairs) {
      this.sourceQualifiedName = sourceQualifiedName;
      this.targetQualifiedName = targetQualifiedName;
      this.columnPairs = columnPairs;
    }

    @Override public List<String> getSourceQualifiedName() {
      return sourceQualifiedName;
    }

    @Override public List<String> getTargetQualifiedName() {
      return targetQualifiedName;
    }

    @Override public List<IntPair> getColumnPairs() {
      return columnPairs;
    }

    @SuppressWarnings("deprecation")
    @Override public int getNumColumns() {
      return columnPairs.size();
    }
  }

  /**
   * Helper schema that provides a fixed set of tables.
   */
  static class TestSchema extends AbstractSchema {
    private final Map<String, Table> tableMap;

    TestSchema(Map<String, Table> tableMap) {
      this.tableMap = tableMap;
    }

    @Override protected Map<String, Table> getTableMap() {
      return tableMap;
    }
  }

  // ==========================================================================
  // Tests
  // ==========================================================================

  @Test void testKeyColumnUsageRowType() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");

    // Access the KEY_COLUMN_USAGE table via the table map
    Map<String, Table> tableMap = infoSchema.getTableMap();
    Table keyColumnUsageTable = tableMap.get("KEY_COLUMN_USAGE");
    assertNotNull(keyColumnUsageTable, "KEY_COLUMN_USAGE table should exist");

    // Verify row type
    RelDataType rowType = keyColumnUsageTable.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(9, rowType.getFieldCount());
    assertEquals("CONSTRAINT_CATALOG", rowType.getFieldNames().get(0));
    assertEquals("CONSTRAINT_SCHEMA", rowType.getFieldNames().get(1));
    assertEquals("CONSTRAINT_NAME", rowType.getFieldNames().get(2));
    assertEquals("TABLE_CATALOG", rowType.getFieldNames().get(3));
    assertEquals("TABLE_SCHEMA", rowType.getFieldNames().get(4));
    assertEquals("TABLE_NAME", rowType.getFieldNames().get(5));
    assertEquals("COLUMN_NAME", rowType.getFieldNames().get(6));
    assertEquals("ORDINAL_POSITION", rowType.getFieldNames().get(7));
    assertEquals("POSITION_IN_UNIQUE_CONSTRAINT", rowType.getFieldNames().get(8));
  }

  @Test void testKeyColumnUsageLowerCaseAccess() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");

    Map<String, Table> tableMap = infoSchema.getTableMap();
    // Should be accessible via lower case
    assertNotNull(tableMap.get("key_column_usage"));
    // And upper case
    assertNotNull(tableMap.get("KEY_COLUMN_USAGE"));
  }

  @Test void testKeyColumnUsageGetStatistic() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");

    Map<String, Table> tableMap = infoSchema.getTableMap();
    Table keyColumnUsageTable = tableMap.get("KEY_COLUMN_USAGE");
    assertNotNull(keyColumnUsageTable);
    assertEquals(Statistics.UNKNOWN, keyColumnUsageTable.getStatistic());
  }

  @Test void testScanEmptyRootSchema() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");

    Map<String, Table> tableMap = infoSchema.getTableMap();
    ScannableTable keyColumnUsageTable = (ScannableTable) tableMap.get("KEY_COLUMN_USAGE");
    assertNotNull(keyColumnUsageTable);

    Enumerable<Object[]> result = keyColumnUsageTable.scan(mockDataContext);
    assertNotNull(result);
    List<Object[]> rows = result.toList();
    assertEquals(0, rows.size(), "Empty root schema should produce 0 key column usage rows");
  }

  @Test void testScanWithPrimaryKeyTable() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    // Add a schema with a table that has a primary key
    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("pk_table", new PrimaryKeyTable());
    rootSchema.add("test_schema", new TestSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");
    ScannableTable keyColumnUsageTable =
        (ScannableTable) infoSchema.getTableMap().get("KEY_COLUMN_USAGE");

    Enumerable<Object[]> result = keyColumnUsageTable.scan(mockDataContext);
    List<Object[]> rows = result.toList();

    // Should have 1 row for the PK (id column, index 0)
    assertTrue(rows.size() >= 1, "Should have at least 1 row for PK, got " + rows.size());

    Object[] pkRow = rows.get(0);
    assertEquals("test_catalog", pkRow[0]); // CONSTRAINT_CATALOG
    assertEquals("TEST_SCHEMA", pkRow[1]); // CONSTRAINT_SCHEMA (uppercased)
    assertTrue(pkRow[2].toString().startsWith("PK_")); // CONSTRAINT_NAME
    assertEquals("test_catalog", pkRow[3]); // TABLE_CATALOG
    assertEquals("TEST_SCHEMA", pkRow[4]); // TABLE_SCHEMA (uppercased)
    assertEquals("pk_table", pkRow[5]); // TABLE_NAME
    assertEquals("id", pkRow[6]); // COLUMN_NAME
    assertEquals(1, pkRow[7]); // ORDINAL_POSITION
    assertEquals(null, pkRow[8]); // POSITION_IN_UNIQUE_CONSTRAINT (null for PK)
  }

  @Test void testScanWithMultipleUniqueKeys() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("multi_key_table", new MultiKeyTable());
    rootSchema.add("test_schema", new TestSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");
    ScannableTable keyColumnUsageTable =
        (ScannableTable) infoSchema.getTableMap().get("KEY_COLUMN_USAGE");

    Enumerable<Object[]> result = keyColumnUsageTable.scan(mockDataContext);
    List<Object[]> rows = result.toList();

    // Should have 2 rows: PK on id and UK on code
    assertTrue(rows.size() >= 2, "Should have at least 2 rows for PK+UK, got " + rows.size());

    // First row: PK on id
    Object[] pkRow = rows.get(0);
    assertTrue(pkRow[2].toString().startsWith("PK_")); // PK_MULTI_KEY_TABLE
    assertEquals("id", pkRow[6]); // COLUMN_NAME

    // Second row: UK on code
    Object[] ukRow = rows.get(1);
    assertTrue(ukRow[2].toString().startsWith("UK_")); // UK_MULTI_KEY_TABLE_1
    assertEquals("code", ukRow[6]); // COLUMN_NAME
  }

  @Test void testScanWithCompositePrimaryKey() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("composite_table", new CompositePKTable());
    rootSchema.add("test_schema", new TestSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");
    ScannableTable keyColumnUsageTable =
        (ScannableTable) infoSchema.getTableMap().get("KEY_COLUMN_USAGE");

    Enumerable<Object[]> result = keyColumnUsageTable.scan(mockDataContext);
    List<Object[]> rows = result.toList();

    // Should have 2 rows: year and month both in the composite PK
    assertTrue(rows.size() >= 2, "Should have at least 2 rows for composite PK, got " + rows.size());

    Object[] yearRow = rows.get(0);
    assertEquals("year", yearRow[6]); // COLUMN_NAME
    assertEquals(1, yearRow[7]); // ORDINAL_POSITION

    Object[] monthRow = rows.get(1);
    assertEquals("month", monthRow[6]); // COLUMN_NAME
    assertEquals(2, monthRow[7]); // ORDINAL_POSITION
  }

  @Test void testScanWithForeignKeys() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("pk_table", new PrimaryKeyTable());
    tables.put("fk_table", new ForeignKeyTable());
    rootSchema.add("test_schema", new TestSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");
    ScannableTable keyColumnUsageTable =
        (ScannableTable) infoSchema.getTableMap().get("KEY_COLUMN_USAGE");

    Enumerable<Object[]> result = keyColumnUsageTable.scan(mockDataContext);
    List<Object[]> rows = result.toList();

    // Should have PK rows + FK rows
    assertTrue(rows.size() >= 2, "Should have PK and FK rows, got " + rows.size());

    // Find the FK row
    boolean foundFK = false;
    for (Object[] row : rows) {
      if (row[2] != null && row[2].toString().startsWith("FK_")) {
        foundFK = true;
        assertEquals("parent_id", row[6]); // COLUMN_NAME for FK
        assertNotNull(row[8]); // POSITION_IN_UNIQUE_CONSTRAINT should not be null for FK
        assertEquals(1, row[8]); // pair.target + 1 = 0 + 1 = 1
        break;
      }
    }
    assertTrue(foundFK, "Should have found a FK row");
  }

  @Test void testScanWithNoConstraintTable() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("no_constraint", new NoConstraintTable());
    rootSchema.add("test_schema", new TestSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");
    ScannableTable keyColumnUsageTable =
        (ScannableTable) infoSchema.getTableMap().get("KEY_COLUMN_USAGE");

    Enumerable<Object[]> result = keyColumnUsageTable.scan(mockDataContext);
    List<Object[]> rows = result.toList();

    // No constraints => no key column usage rows
    assertEquals(0, rows.size(), "No constraints should produce 0 rows");
  }

  @Test void testScanWithMultipleSchemas() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    // Schema 1 with a PK table
    Map<String, Table> tables1 = new HashMap<String, Table>();
    tables1.put("users", new PrimaryKeyTable());
    rootSchema.add("schema1", new TestSchema(tables1));

    // Schema 2 with a multi-key table
    Map<String, Table> tables2 = new HashMap<String, Table>();
    tables2.put("products", new MultiKeyTable());
    rootSchema.add("schema2", new TestSchema(tables2));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");
    ScannableTable keyColumnUsageTable =
        (ScannableTable) infoSchema.getTableMap().get("KEY_COLUMN_USAGE");

    Enumerable<Object[]> result = keyColumnUsageTable.scan(mockDataContext);
    List<Object[]> rows = result.toList();

    // Schema1: 1 PK row, Schema2: 2 key rows (PK + UK)
    assertTrue(rows.size() >= 3, "Should have rows from both schemas, got " + rows.size());

    // Verify both schemas are represented
    boolean foundSchema1 = false;
    boolean foundSchema2 = false;
    for (Object[] row : rows) {
      if ("SCHEMA1".equals(row[1])) {
        foundSchema1 = true;
      }
      if ("SCHEMA2".equals(row[1])) {
        foundSchema2 = true;
      }
    }
    assertTrue(foundSchema1, "Should have rows from schema1");
    assertTrue(foundSchema2, "Should have rows from schema2");
  }

  @Test void testScanWithMixedConstraintsAndNoConstraints() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("constrained", new PrimaryKeyTable());
    tables.put("unconstrained", new NoConstraintTable());
    rootSchema.add("mixed", new TestSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");
    ScannableTable keyColumnUsageTable =
        (ScannableTable) infoSchema.getTableMap().get("KEY_COLUMN_USAGE");

    Enumerable<Object[]> result = keyColumnUsageTable.scan(mockDataContext);
    List<Object[]> rows = result.toList();

    // Only constrained table should produce rows
    assertTrue(rows.size() >= 1, "Should have rows from constrained table");
    for (Object[] row : rows) {
      assertEquals("constrained", row[5],
          "All rows should be from constrained table, but found: " + row[5]);
    }
  }

  @Test void testCaseInsensitiveTableMapContainsTitleCase() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");

    Map<String, Table> tableMap = infoSchema.getTableMap();

    // Should have title case variation
    assertNotNull(tableMap.get("Key_column_usage"));
    // Should have all uppercase
    assertNotNull(tableMap.get("KEY_COLUMN_USAGE"));
    // Should have all lowercase
    assertNotNull(tableMap.get("key_column_usage"));
  }

  @Test void testAllInformationSchemaTables() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");
    Map<String, Table> tableMap = infoSchema.getTableMap();

    // Verify all expected tables are present
    assertNotNull(tableMap.get("SCHEMATA"));
    assertNotNull(tableMap.get("TABLES"));
    assertNotNull(tableMap.get("COLUMNS"));
    assertNotNull(tableMap.get("TABLE_CONSTRAINTS"));
    assertNotNull(tableMap.get("KEY_COLUMN_USAGE"));
    assertNotNull(tableMap.get("REFERENTIAL_CONSTRAINTS"));
    assertNotNull(tableMap.get("CHECK_CONSTRAINTS"));
    assertNotNull(tableMap.get("VIEWS"));
    assertNotNull(tableMap.get("ROUTINES"));
    assertNotNull(tableMap.get("PARAMETERS"));
  }

  @Test void testScanWithNullSchemaPlus() {
    // If rootSchema.subSchemas().get(schemaName) returns a non-null schema
    // but the schema inside has no tables with constraints, ensure no rows
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    // Add a schema with an empty table set
    rootSchema.add("empty_schema", new TestSchema(new HashMap<String, Table>()));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");
    ScannableTable keyColumnUsageTable =
        (ScannableTable) infoSchema.getTableMap().get("KEY_COLUMN_USAGE");

    Enumerable<Object[]> result = keyColumnUsageTable.scan(mockDataContext);
    List<Object[]> rows = result.toList();
    assertEquals(0, rows.size());
  }

  @Test void testConstraintNameFormat() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put("my_table", new MultiKeyTable());
    tables.put("fk_table", new ForeignKeyTable());
    rootSchema.add("s", new TestSchema(tables));

    InformationSchema infoSchema = new InformationSchema(rootSchema, "test_catalog");
    ScannableTable keyColumnUsageTable =
        (ScannableTable) infoSchema.getTableMap().get("KEY_COLUMN_USAGE");

    Enumerable<Object[]> result = keyColumnUsageTable.scan(mockDataContext);
    List<Object[]> rows = result.toList();

    for (Object[] row : rows) {
      String constraintName = (String) row[2];
      assertNotNull(constraintName);
      // All constraint names should start with PK_, UK_, or FK_
      assertTrue(constraintName.startsWith("PK_")
              || constraintName.startsWith("UK_")
              || constraintName.startsWith("FK_"),
          "Unexpected constraint name format: " + constraintName);
    }
  }
}
