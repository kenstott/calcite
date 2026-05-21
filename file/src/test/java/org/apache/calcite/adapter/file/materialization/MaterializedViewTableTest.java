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
package org.apache.calcite.adapter.file.materialization;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.materialized.MaterializedViewTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link MaterializedViewTable}.
 *
 * <p>Tests the lazy materialization behavior: SQL is executed and results
 * are written to Parquet on the first call to {@code getRowType()}.
 */
@Tag("integration")
public class MaterializedViewTableTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MaterializedViewTableTest.class);

  @TempDir
  Path tempDir;

  /** In-memory ScannableTable for testing. */
  private static class MemoryTable extends AbstractTable
      implements ScannableTable {
    private final String[] columnNames;
    private final SqlTypeName[] columnTypes;
    private final Object[][] rows;

    MemoryTable(String[] columnNames, SqlTypeName[] columnTypes,
        Object[][] rows) {
      this.columnNames = columnNames;
      this.columnTypes = columnTypes;
      this.rows = rows;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (int i = 0; i < columnNames.length; i++) {
        if (columnTypes[i] == SqlTypeName.VARCHAR) {
          builder.add(columnNames[i],
              typeFactory.createSqlType(columnTypes[i], 255));
        } else {
          builder.add(columnNames[i],
              typeFactory.createSqlType(columnTypes[i]));
        }
      }
      return builder.build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(Arrays.asList(rows));
    }
  }

  /**
   * Creates a ScannableTable backed by in-memory data.
   */
  private ScannableTable createScannableTable(
      String[] columnNames, SqlTypeName[] columnTypes, Object[][] rows) {
    return new MemoryTable(columnNames, columnTypes, rows);
  }

  /**
   * Creates a table map with a single "items" table having
   * string, int, and double columns with 3 rows.
   */
  private Map<String, Table> createItemsTableMap() {
    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put(
        "items", createScannableTable(
        new String[]{"name", "quantity", "price"},
        new SqlTypeName[]{
            SqlTypeName.VARCHAR, SqlTypeName.INTEGER, SqlTypeName.DOUBLE},
        new Object[][]{
            {"Widget", 10, 25.50},
            {"Gadget", 5, 50.00},
            {"Gizmo", 8, 75.00}
        }));
    return tables;
  }

  // ---------------------------------------------------------------
  // Test 1: getRowType() triggers materialization and returns columns
  // ---------------------------------------------------------------
  @Test public void materializeOnFirstRowTypeAccess() throws Exception {
    Map<String, Table> tables = createItemsTableMap();
    File parquetFile = new File(tempDir.toFile(), "test_mv.parquet");

    MaterializedViewTable mvTable =
        new MaterializedViewTable(null, "test_schema", "test_mv",
        "SELECT \"name\", SUM(\"quantity\") as total_qty"
            + " FROM \"items\" GROUP BY \"name\"",
        parquetFile, tables);

    JavaTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = mvTable.getRowType(typeFactory);

    assertNotNull(rowType, "Row type should not be null after materialization");
    assertEquals(2, rowType.getFieldCount(),
        "Should have 2 columns: name, total_qty");

    assertTrue(parquetFile.exists(),
        "Parquet file should exist after materialization");
    assertTrue(parquetFile.length() > 0,
        "Parquet file should not be empty");

    LOGGER.debug("Row type fields: {}", rowType.getFieldList());
    LOGGER.debug("Parquet file size: {} bytes", parquetFile.length());
  }

  // ---------------------------------------------------------------
  // Test 2: Second getRowType() call does not re-write the file
  // ---------------------------------------------------------------
  @Test public void materializeOnlyOnce() throws Exception {
    Map<String, Table> tables = createItemsTableMap();
    File parquetFile = new File(tempDir.toFile(), "once_mv.parquet");

    MaterializedViewTable mvTable =
        new MaterializedViewTable(null, "test_schema", "once_mv",
        "SELECT \"name\", SUM(\"quantity\") as total_qty"
            + " FROM \"items\" GROUP BY \"name\"",
        parquetFile, tables);

    JavaTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    // First call triggers materialization
    RelDataType rowType1 = mvTable.getRowType(typeFactory);
    assertNotNull(rowType1, "First getRowType should succeed");
    assertTrue(parquetFile.exists(), "Parquet file should exist");

    long firstModified = parquetFile.lastModified();
    long firstSize = parquetFile.length();
    LOGGER.debug("After first call: modified={}, size={}",
        firstModified, firstSize);

    // Small delay to ensure timestamp difference would be visible
    Thread.sleep(100);

    // Second call should NOT re-materialize
    RelDataType rowType2 = mvTable.getRowType(typeFactory);
    assertNotNull(rowType2, "Second getRowType should succeed");

    assertEquals(firstModified, parquetFile.lastModified(),
        "Parquet file should not be re-written on second access");
    assertEquals(firstSize, parquetFile.length(),
        "Parquet file size should remain the same");
  }

  // ---------------------------------------------------------------
  // Test 3: Materialize with existing tables, SQL filters correctly
  // ---------------------------------------------------------------
  @Test public void materializeWithExistingTables() throws Exception {
    Map<String, Table> tables = createItemsTableMap();
    File parquetFile = new File(tempDir.toFile(), "filtered_mv.parquet");

    // SQL that filters - only items with quantity > 5
    MaterializedViewTable mvTable =
        new MaterializedViewTable(null, "test_schema", "filtered_mv",
        "SELECT \"name\", \"quantity\", \"price\""
            + " FROM \"items\" WHERE \"quantity\" > 5",
        parquetFile, tables);

    JavaTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = mvTable.getRowType(typeFactory);

    assertNotNull(rowType, "Row type should not be null");
    assertEquals(3, rowType.getFieldCount(),
        "Should have 3 columns: name, quantity, price");

    assertTrue(parquetFile.exists(),
        "Parquet file should exist after materialization");

    LOGGER.debug("Filtered MV row type: {}", rowType.getFieldList());
  }

  // ---------------------------------------------------------------
  // Test 4: Supplier constructor invoked during materialization
  // ---------------------------------------------------------------
  @Test public void materializeWithTableSupplier() throws Exception {
    final AtomicInteger supplierCallCount = new AtomicInteger(0);

    Supplier<Map<String, Table>> supplier =
        new Supplier<Map<String, Table>>() {
          @Override public Map<String, Table> get() {
            supplierCallCount.incrementAndGet();
            return createItemsTableMap();
          }
        };

    File parquetFile = new File(tempDir.toFile(), "supplier_mv.parquet");

    MaterializedViewTable mvTable =
        new MaterializedViewTable(null, "test_schema", "supplier_mv",
        "SELECT \"name\", SUM(\"quantity\") as total_qty"
            + " FROM \"items\" GROUP BY \"name\"",
        parquetFile, supplier);

    JavaTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    // Supplier should not be called yet
    assertEquals(0, supplierCallCount.get(),
        "Supplier should not be called before materialization");

    // Trigger materialization
    RelDataType rowType = mvTable.getRowType(typeFactory);
    assertNotNull(rowType, "Row type should not be null");

    assertEquals(1, supplierCallCount.get(),
        "Supplier should be called exactly once during materialization");

    // Second call should not trigger supplier again
    mvTable.getRowType(typeFactory);
    assertEquals(1, supplierCallCount.get(),
        "Supplier should still be called exactly once after second access");

    assertTrue(parquetFile.exists(), "Parquet file should exist");
    LOGGER.debug("Supplier call count: {}", supplierCallCount.get());
  }

  // ---------------------------------------------------------------
  // Test 5: INT/BIGINT mapped correctly to Parquet long types
  // ---------------------------------------------------------------
  @Test public void materializeIntegerTypes() throws Exception {
    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put(
        "numbers", createScannableTable(
        new String[]{"id", "small_val", "big_val"},
        new SqlTypeName[]{
            SqlTypeName.INTEGER, SqlTypeName.INTEGER, SqlTypeName.BIGINT},
        new Object[][]{{1, 100, 999999999L}}));

    File parquetFile = new File(tempDir.toFile(), "int_mv.parquet");

    MaterializedViewTable mvTable =
        new MaterializedViewTable(null, "test_schema", "int_mv",
        "SELECT \"id\", \"small_val\", \"big_val\" FROM \"numbers\"",
        parquetFile, tables);

    JavaTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = mvTable.getRowType(typeFactory);

    assertNotNull(rowType, "Row type should not be null");
    assertEquals(3, rowType.getFieldCount(), "Should have 3 columns");

    assertTrue(parquetFile.exists(),
        "Parquet file should be created for integer types");
    assertTrue(parquetFile.length() > 0,
        "Parquet file should not be empty");

    assertNotNull(rowType.getField("id", true, false),
        "Should have 'id' column");
    assertNotNull(rowType.getField("small_val", true, false),
        "Should have 'small_val' column");
    assertNotNull(rowType.getField("big_val", true, false),
        "Should have 'big_val' column");

    LOGGER.debug("Integer types row type: {}", rowType.getFieldList());
  }

  // ---------------------------------------------------------------
  // Test 6: FLOAT/DOUBLE mapped correctly to Parquet double types
  // ---------------------------------------------------------------
  @Test public void materializeDoubleTypes() throws Exception {
    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put(
        "decimals", createScannableTable(
        new String[]{"label", "float_val", "double_val"},
        new SqlTypeName[]{
            SqlTypeName.VARCHAR, SqlTypeName.FLOAT, SqlTypeName.DOUBLE},
        new Object[][]{{"test", 1.5d, 3.14159d}}));

    File parquetFile = new File(tempDir.toFile(), "double_mv.parquet");

    MaterializedViewTable mvTable =
        new MaterializedViewTable(null, "test_schema", "double_mv",
        "SELECT \"label\", \"float_val\", \"double_val\""
            + " FROM \"decimals\"",
        parquetFile, tables);

    JavaTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = mvTable.getRowType(typeFactory);

    assertNotNull(rowType, "Row type should not be null");
    assertEquals(3, rowType.getFieldCount(), "Should have 3 columns");

    assertTrue(parquetFile.exists(),
        "Parquet file should be created for double types");
    assertTrue(parquetFile.length() > 0,
        "Parquet file should not be empty");

    assertNotNull(rowType.getField("label", true, false),
        "Should have 'label' column");
    assertNotNull(rowType.getField("float_val", true, false),
        "Should have 'float_val' column");
    assertNotNull(rowType.getField("double_val", true, false),
        "Should have 'double_val' column");

    LOGGER.debug("Double types row type: {}", rowType.getFieldList());
  }

  // ---------------------------------------------------------------
  // Test 7: Invalid SQL throws RuntimeException
  // ---------------------------------------------------------------
  @Test public void materializationFailureThrows() {
    Map<String, Table> tables = createItemsTableMap();
    File parquetFile = new File(tempDir.toFile(), "fail_mv.parquet");

    MaterializedViewTable mvTable =
        new MaterializedViewTable(null, "test_schema", "fail_mv",
        "SELECT * FROM \"nonexistent_table\"",
        parquetFile, tables);

    JavaTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    RuntimeException ex =
        assertThrows(RuntimeException.class, new org.junit.jupiter.api.function.Executable() {
          @Override public void execute() {
            mvTable.getRowType(typeFactory);
          }
        },
        "Should throw RuntimeException for invalid SQL");

    LOGGER.debug("Expected exception: {}", ex.getMessage());
    assertTrue(ex.getMessage().contains("Failed to materialize view"),
        "Exception message should mention materialization failure");
  }

  // ---------------------------------------------------------------
  // Test 8: BOOLEAN type mapped correctly to Parquet boolean
  // ---------------------------------------------------------------
  @Test public void materializeBooleanTypes() throws Exception {
    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put(
        "flags", createScannableTable(
        new String[]{"id", "active", "verified"},
        new SqlTypeName[]{
            SqlTypeName.INTEGER, SqlTypeName.BOOLEAN, SqlTypeName.BOOLEAN},
        new Object[][]{
            {1, true, false},
            {2, false, true},
            {3, true, true}
        }));

    File parquetFile = new File(tempDir.toFile(), "bool_mv.parquet");

    MaterializedViewTable mvTable =
        new MaterializedViewTable(null, "test_schema", "bool_mv",
        "SELECT \"id\", \"active\", \"verified\" FROM \"flags\"",
        parquetFile, tables);

    JavaTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = mvTable.getRowType(typeFactory);

    assertNotNull(rowType, "Row type should not be null");
    assertEquals(3, rowType.getFieldCount(), "Should have 3 columns");
    assertTrue(parquetFile.exists(),
        "Parquet file should be created for boolean types");
    assertTrue(parquetFile.length() > 0,
        "Parquet file should not be empty");

    assertNotNull(rowType.getField("id", true, false),
        "Should have 'id' column");
    assertNotNull(rowType.getField("active", true, false),
        "Should have 'active' column");
    assertNotNull(rowType.getField("verified", true, false),
        "Should have 'verified' column");
  }

  // ---------------------------------------------------------------
  // Test 9: Mixed types including DECIMAL mapped correctly
  // ---------------------------------------------------------------
  @Test public void materializeMixedTypes() throws Exception {
    Map<String, Table> tables = new HashMap<String, Table>();
    tables.put(
        "mixed", createScannableTable(
        new String[]{"name", "count", "ratio", "active"},
        new SqlTypeName[]{
            SqlTypeName.VARCHAR, SqlTypeName.BIGINT,
            SqlTypeName.DECIMAL, SqlTypeName.BOOLEAN},
        new Object[][]{
            {"Alpha", 100L, 0.75d, true},
            {"Beta", 200L, 0.50d, false}
        }));

    File parquetFile = new File(tempDir.toFile(), "mixed_mv.parquet");

    MaterializedViewTable mvTable =
        new MaterializedViewTable(null, "test_schema", "mixed_mv",
        "SELECT \"name\", \"count\", \"ratio\", \"active\" FROM \"mixed\"",
        parquetFile, tables);

    JavaTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = mvTable.getRowType(typeFactory);

    assertNotNull(rowType, "Row type should not be null");
    assertEquals(4, rowType.getFieldCount(), "Should have 4 columns");
    assertTrue(parquetFile.exists(),
        "Parquet file should be created for mixed types");
    assertTrue(parquetFile.length() > 0,
        "Parquet file should not be empty");
  }
}
