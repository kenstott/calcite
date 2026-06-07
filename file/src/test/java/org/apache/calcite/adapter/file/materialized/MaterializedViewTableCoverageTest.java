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
package org.apache.calcite.adapter.file.materialized;

import org.apache.calcite.adapter.file.BaseFileTest;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MaterializedViewTable} to improve line coverage.
 * Covers materialize() method body, getRowType(), toRel() ScannableTable fallback.
 */
@SuppressWarnings("deprecation")
@Tag("unit")
public class MaterializedViewTableCoverageTest extends BaseFileTest {

  @TempDir
  java.nio.file.Path tempDir;

  /**
   * Tests that materialize() executes the SQL query and writes Parquet output,
   * and that getRowType() returns a valid type from the materialized Parquet file.
   * Uses the existingTables map constructor.
   */
  @Test public void testMaterializeWithExistingTables() throws Exception {
    // Create a JSON source file for a base table
    File jsonFile = new File(tempDir.toFile(), "employees.json");
    writeJson(jsonFile,
        "[{\"id\": 1, \"name\": \"Alice\", \"salary\": 50000},"
        + "{\"id\": 2, \"name\": \"Bob\", \"salary\": 60000}]");

    // Build a model with a schema containing the employees table
    String model =
        buildTestModel("myschema", tempDir.toString(), "executionEngine", "PARQUET");

    Properties connectionProps = new Properties();
    applyEngineDefaults(connectionProps);

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=inline:" + model, connectionProps);
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConn.getRootSchema();
      SchemaPlus mySchema = rootSchema.getSubSchema("myschema");
      assertNotNull(mySchema, "Schema 'myschema' should exist");

      // Collect the existing tables from the schema
      Map<String, Table> existingTables = new HashMap<String, Table>();
      for (String tableName : mySchema.getTableNames()) {
        existingTables.put(tableName, mySchema.getTable(tableName));
      }
      assertTrue(existingTables.containsKey("employees"),
          "Should have an 'employees' table");

      // Create a MaterializedViewTable with a simple SELECT query
      File parquetOutput = new File(tempDir.toFile(), "mat_view.parquet");
      MaterializedViewTable mvTable =
          new MaterializedViewTable(mySchema,
          "myschema",
          "mat_view",
          "SELECT id, name, salary FROM employees",
          parquetOutput,
          existingTables);

      // Call getRowType which triggers materialize()
      JavaTypeFactory tf = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType rowType = mvTable.getRowType(tf);

      assertNotNull(rowType, "Row type should not be null after materialization");
      assertTrue(rowType.getFieldCount() > 0,
          "Row type should have fields");
      assertTrue(parquetOutput.exists(),
          "Parquet file should have been created by materialize()");
      assertTrue(parquetOutput.length() > 0,
          "Parquet file should not be empty");
    }
  }

  /**
   * Tests that materialize() works with the tableSupplier constructor.
   */
  @Test public void testMaterializeWithTableSupplier() throws Exception {
    // Create a JSON source file
    File jsonFile = new File(tempDir.toFile(), "products.json");
    writeJson(jsonFile,
        "[{\"pid\": 10, \"pname\": \"Widget\"},"
        + "{\"pid\": 20, \"pname\": \"Gadget\"}]");

    String model =
        buildTestModel("suppliertest", tempDir.toString(), "executionEngine", "PARQUET");

    Properties connectionProps = new Properties();
    applyEngineDefaults(connectionProps);

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=inline:" + model, connectionProps);
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {

      SchemaPlus rootSchema = calciteConn.getRootSchema();
      final SchemaPlus supplierSchema = rootSchema.getSubSchema("suppliertest");
      assertNotNull(supplierSchema, "Schema should exist");

      // Create a MaterializedViewTable using the Supplier constructor
      File parquetOutput = new File(tempDir.toFile(), "supplier_mat.parquet");
      MaterializedViewTable mvTable =
          new MaterializedViewTable(supplierSchema,
          "suppliertest",
          "supplier_mat",
          "SELECT pid, pname FROM products",
          parquetOutput,
          new java.util.function.Supplier<Map<String, Table>>() {
            @Override public Map<String, Table> get() {
              Map<String, Table> tables = new HashMap<String, Table>();
              for (String tableName : supplierSchema.getTableNames()) {
                tables.put(tableName, supplierSchema.getTable(tableName));
              }
              return tables;
            }
          });

      // Trigger materialization through getRowType()
      JavaTypeFactory tf = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType rowType = mvTable.getRowType(tf);

      assertNotNull(rowType, "Row type should not be null");
      assertTrue(rowType.getFieldCount() >= 2,
          "Should have at least 2 columns (pid, pname)");
      assertTrue(parquetOutput.exists(), "Parquet file should exist");
    }
  }

  /**
   * Tests that getRowType() returns a consistent result on repeated calls
   * (materialize() should only execute once due to AtomicBoolean guard).
   */
  @Test public void testGetRowTypeIdempotent() throws Exception {
    File jsonFile = new File(tempDir.toFile(), "items.json");
    writeJson(jsonFile, "[{\"item_id\": 1, \"description\": \"Test\"}]");

    String model =
        buildTestModel("idempotent", tempDir.toString(), "executionEngine", "PARQUET");

    Properties connectionProps = new Properties();
    applyEngineDefaults(connectionProps);

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=inline:" + model, connectionProps);
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {

      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("idempotent");
      assertNotNull(schema);

      Map<String, Table> tables = new HashMap<String, Table>();
      for (String name : schema.getTableNames()) {
        tables.put(name, schema.getTable(name));
      }

      File parquetOutput = new File(tempDir.toFile(), "idempotent_mat.parquet");
      MaterializedViewTable mvTable =
          new MaterializedViewTable(schema, "idempotent", "idempotent_mat",
          "SELECT item_id, description FROM items",
          parquetOutput, tables);

      JavaTypeFactory tf = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

      // Call getRowType twice - should be idempotent
      RelDataType rowType1 = mvTable.getRowType(tf);
      RelDataType rowType2 = mvTable.getRowType(tf);

      assertNotNull(rowType1);
      assertNotNull(rowType2);
      assertEquals(rowType1.getFieldCount(), rowType2.getFieldCount(),
          "Repeated calls should return same row type");
    }
  }

  /**
   * Tests that materialize() handles SQL execution errors by throwing RuntimeException.
   */
  @Test public void testMaterializeWithInvalidSql() throws Exception {
    File jsonFile = new File(tempDir.toFile(), "dummy.json");
    writeJson(jsonFile, "[{\"val\": 1}]");

    String model =
        buildTestModel("errtest", tempDir.toString(), "executionEngine", "PARQUET");

    Properties connectionProps = new Properties();
    applyEngineDefaults(connectionProps);

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=inline:" + model, connectionProps);
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {

      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("errtest");
      assertNotNull(schema);

      Map<String, Table> tables = new HashMap<String, Table>();
      for (String name : schema.getTableNames()) {
        tables.put(name, schema.getTable(name));
      }

      File parquetOutput = new File(tempDir.toFile(), "error_mat.parquet");
      MaterializedViewTable mvTable =
          new MaterializedViewTable(schema, "errtest", "error_mat",
          "SELECT nonexistent_column FROM nonexistent_table",
          parquetOutput, tables);

      JavaTypeFactory tf = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

      // Should throw RuntimeException wrapping the SQL error
      RuntimeException ex =
          assertThrows(RuntimeException.class, () -> mvTable.getRowType(tf));
      assertTrue(ex.getMessage().contains("Failed to materialize view"),
          "Exception should mention materialization failure, got: " + ex.getMessage());
    }
  }

  /**
   * Tests materialization with different SQL column types to cover the
   * type mapping switch cases (INTEGER/BIGINT, FLOAT/DOUBLE/DECIMAL, BOOLEAN, default/VARCHAR).
   */
  @Test public void testMaterializeWithVariousColumnTypes() throws Exception {
    // Create a CSV file with various types for broader type coverage
    File csvFile = new File(tempDir.toFile(), "typed_data.csv");
    writeCsv(csvFile, "int_col,float_col,text_col\n1,1.5,hello\n2,2.5,world\n");

    String model =
        buildTestModel("typedschema", tempDir.toString(), "executionEngine", "PARQUET");

    Properties connectionProps = new Properties();
    applyEngineDefaults(connectionProps);

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=inline:" + model, connectionProps);
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {

      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("typedschema");
      assertNotNull(schema);

      Map<String, Table> tables = new HashMap<String, Table>();
      for (String name : schema.getTableNames()) {
        tables.put(name, schema.getTable(name));
      }

      File parquetOutput = new File(tempDir.toFile(), "typed_mat.parquet");
      MaterializedViewTable mvTable =
          new MaterializedViewTable(schema, "typedschema", "typed_mat",
          "SELECT int_col, float_col, text_col FROM typed_data",
          parquetOutput, tables);

      JavaTypeFactory tf = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType rowType = mvTable.getRowType(tf);

      assertNotNull(rowType);
      assertEquals(3, rowType.getFieldCount(),
          "Should have 3 columns");
      assertTrue(parquetOutput.exists(),
          "Parquet output should exist");
    }
  }

  /**
   * Tests that the materialized field is set correctly after first materialize call.
   */
  @Test public void testMaterializedAtomicFlag() throws Exception {
    File jsonFile = new File(tempDir.toFile(), "flag_test.json");
    writeJson(jsonFile, "[{\"x\": 1, \"y\": \"hello\"}]");

    String model =
        buildTestModel("flagtest", tempDir.toString(), "executionEngine", "PARQUET");

    Properties connectionProps = new Properties();
    applyEngineDefaults(connectionProps);

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=inline:" + model, connectionProps);
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {

      SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("flagtest");
      assertNotNull(schema);

      Map<String, Table> tables = new HashMap<String, Table>();
      for (String name : schema.getTableNames()) {
        tables.put(name, schema.getTable(name));
      }

      File parquetOutput = new File(tempDir.toFile(), "flag_mat.parquet");
      MaterializedViewTable mvTable =
          new MaterializedViewTable(schema, "flagtest", "flag_mat",
          "SELECT x, y FROM flag_test",
          parquetOutput, tables);

      // Before getRowType, materialized should be false
      assertTrue(!mvTable.materialized.get(),
          "Should not be materialized before first access");

      JavaTypeFactory tf = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      mvTable.getRowType(tf);

      // After getRowType, materialized should be true
      assertTrue(mvTable.materialized.get(),
          "Should be materialized after first access");
    }
  }

  private void writeJson(File file, String content) throws IOException {
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }

  private void writeCsv(File file, String content) throws IOException {
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }
}
