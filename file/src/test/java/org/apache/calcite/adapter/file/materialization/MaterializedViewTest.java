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

import org.apache.calcite.adapter.file.BaseFileTest;
import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Consolidated test for materialized view functionality with Parquet engine.
 *
 * <p>Tests creation, metadata, querying, and multiple materializations
 * using both model.json-based and programmatic schema approaches.
 */
@Tag("integration")
public class MaterializedViewTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(MaterializedViewTest.class);

  @TempDir
  Path tempDir;

  @BeforeEach
  public void setUp() {
    // Skip for engines that do not support materialized views
    String engineStr = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    assumeFalse(
        engineStr != null
            && ("LINQ4J".equalsIgnoreCase(engineStr)
            || "ARROW".equalsIgnoreCase(engineStr)),
        "Skipping materialized view test for " + engineStr
            + " engine (not supported)");
  }

  @AfterEach
  public void tearDown() {
    Sources.clearFileCache();
  }

  // ---------------------------------------------------------------
  // Helper: write sales.csv (6 rows)
  // ---------------------------------------------------------------
  private File writeSalesCsv() throws Exception {
    File salesCsv = new File(tempDir.toFile(), "sales.csv");
    try (FileWriter writer = new FileWriter(salesCsv, StandardCharsets.UTF_8)) {
      writer.write("date:string,product:string,quantity:int,price:double\n");
      writer.write("2024-01-01,Widget,10,25.50\n");
      writer.write("2024-01-01,Gadget,5,50.00\n");
      writer.write("2024-01-02,Widget,15,25.50\n");
      writer.write("2024-01-02,Gizmo,8,75.00\n");
      writer.write("2024-01-03,Gadget,12,50.00\n");
      writer.write("2024-01-03,Widget,20,25.50\n");
    }
    return salesCsv;
  }

  // ---------------------------------------------------------------
  // Helper: write products.csv (3 rows)
  // ---------------------------------------------------------------
  private File writeProductsCsv() throws Exception {
    File productsCsv = new File(tempDir.toFile(), "products.csv");
    try (FileWriter writer =
             new FileWriter(productsCsv, StandardCharsets.UTF_8)) {
      writer.write("name:string,category:string,base_price:double\n");
      writer.write("Widget,Electronics,25.50\n");
      writer.write("Gadget,Tools,50.00\n");
      writer.write("Gizmo,Hardware,75.00\n");
    }
    return productsCsv;
  }

  // ---------------------------------------------------------------
  // Helper: create .materialized_views directory
  // ---------------------------------------------------------------
  private File createMvDirectory() {
    File mvDir = new File(tempDir.toFile(), ".materialized_views");
    mvDir.mkdirs();
    LOGGER.debug("Created .materialized_views directory: {}",
        mvDir.getAbsolutePath());
    return mvDir;
  }

  // ---------------------------------------------------------------
  // Helper: write a pre-materialized CSV into .materialized_views
  // ---------------------------------------------------------------
  private void writeMaterializedCsv(File mvDir, String fileName,
      String header, String... rows) throws Exception {
    File csvFile = new File(mvDir, fileName);
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write(header + "\n");
      for (String row : rows) {
        writer.write(row + "\n");
      }
    }
    LOGGER.debug("Created pre-materialized CSV: {}", csvFile.getAbsolutePath());
  }

  // ---------------------------------------------------------------
  // Helper: build model.json content
  // ---------------------------------------------------------------
  private String buildModelJson(String schemaName, String directory,
      String materializationsJson) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("  \"version\": \"1.0\",\n");
    sb.append("  \"defaultSchema\": \"").append(schemaName).append("\",\n");
    sb.append("  \"schemas\": [{\n");
    sb.append("    \"name\": \"").append(schemaName).append("\",\n");
    sb.append("    \"type\": \"custom\",\n");
    sb.append("    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n");
    sb.append("    \"operand\": {\n");
    sb.append("      \"directory\": \"")
        .append(directory.replace("\\", "\\\\")).append("\",\n");
    sb.append("      \"executionEngine\": \"parquet\",\n");
    sb.append("      \"ephemeralCache\": true,\n");
    sb.append("      \"materializations\": ").append(materializationsJson)
        .append("\n");
    sb.append("    }\n");
    sb.append("  }]\n");
    sb.append("}\n");
    return sb.toString();
  }

  // ---------------------------------------------------------------
  // Test 1: model.json-based creation and metadata verification
  // ---------------------------------------------------------------

  /**
   * Creates a model.json with Parquet engine and materializations config.
   * Asserts that the {@code .materialized_views} directory exists, that
   * the "sales" table appears in schema metadata, and sales has 6 rows.
   */
  @Test public void testMaterializedViewCreationAndMetadata() throws Exception {
    writeSalesCsv();
    File mvDir = createMvDirectory();
    assertTrue(mvDir.exists(),
        ".materialized_views directory should exist");

    String materializationsJson = "[{"
        + "\"view\": \"daily_summary\","
        + "\"table\": \"daily_summary_mv\","
        + "\"sql\": \"SELECT \\\"date\\\","
        + " COUNT(*) as transaction_count,"
        + " SUM(\\\"quantity\\\") as total_quantity,"
        + " SUM(\\\"quantity\\\" * \\\"price\\\") as total_revenue"
        + " FROM sales GROUP BY \\\"date\\\"\""
        + "}]";

    String modelJson = buildModelJson("PARQUET_MV_TEST",
        tempDir.toString(), materializationsJson);

    File modelFile = new File(tempDir.toFile(), "model.json");
    try (FileWriter writer =
             new FileWriter(modelFile, StandardCharsets.UTF_8)) {
      writer.write(modelJson);
    }

    Properties info = new Properties();
    BaseFileTest.applyEngineDefaults(info);
    info.put("model", modelFile.getAbsolutePath());

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = connection.createStatement()) {

      // Verify table listing contains "sales"
      ResultSet tables = connection.getMetaData()
          .getTables(null, "PARQUET_MV_TEST", "%", null);
      List<String> tableNames = new ArrayList<>();
      while (tables.next()) {
        tableNames.add(tables.getString("TABLE_NAME"));
      }
      LOGGER.debug("Tables found in schema: {}", tableNames);
      assertTrue(tableNames.contains("sales"),
          "Schema should contain 'sales' table, found: " + tableNames);

      // Verify row count from the sales base table
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) FROM \"PARQUET_MV_TEST\".sales")) {
        assertTrue(rs.next(), "Should have a count result");
        long rowCount = rs.getLong(1);
        assertEquals(6L, rowCount,
            "Sales table should have 6 rows");
      }
    }
  }

  // ---------------------------------------------------------------
  // Test 2: programmatic schema, query pre-materialized CSV
  // ---------------------------------------------------------------

  /**
   * Uses programmatic schema via {@code CalciteConnection.getRootSchema()}
   * and {@code FileSchemaFactory.INSTANCE.create()}. Defines MV
   * {@code daily_summary -> daily_summary_mv}. Creates a pre-materialized
   * CSV in {@code .materialized_views} sub-directory. Queries the MV and
   * asserts the row count equals 3.
   */
  @Test public void testMaterializedViewQuery() throws Exception {
    writeSalesCsv();
    File mvDir = createMvDirectory();

    // Write pre-materialized CSV that the engine will read as the MV table
    writeMaterializedCsv(mvDir, "daily_summary_mv.csv",
        "date:string,transaction_count:long,total_quantity:long,total_revenue:double",
        "2024-01-01,2,15,505.00",
        "2024-01-02,2,23,982.50",
        "2024-01-03,2,32,1110.00");

    Properties info = new Properties();
    BaseFileTest.applyEngineDefaults(info);

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Materialization definition
      List<Map<String, Object>> materializations = new ArrayList<>();
      Map<String, Object> dailySalesMV = new HashMap<>();
      dailySalesMV.put("view", "daily_summary");
      dailySalesMV.put("table", "daily_summary_mv");
      dailySalesMV.put("sql",
          "SELECT \"date\", "
              + "COUNT(*) as transaction_count, "
              + "SUM(\"quantity\") as total_quantity, "
              + "SUM(\"quantity\" * \"price\") as total_revenue "
              + "FROM \"sales\" "
              + "GROUP BY \"date\"");
      materializations.add(dailySalesMV);

      // Schema operand
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");
      operand.put("materializations", materializations);
      operand.put("primeCache", false);

      rootSchema.add("parquet_mv_test",
          FileSchemaFactory.INSTANCE.create(
              rootSchema, "parquet_mv_test", operand));

      try (Statement stmt = connection.createStatement()) {
        // Verify the MV view is registered
        ResultSet tables = connection.getMetaData()
            .getTables(null, "parquet_mv_test", "%", null);
        boolean foundMV = false;
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          LOGGER.debug("Table in schema: {}", tableName);
          if ("daily_summary".equals(tableName)) {
            foundMV = true;
          }
        }

        assertTrue(foundMV,
            "Materialized view 'daily_summary' should be registered");

        // Query the materialized view
        try (ResultSet rs = stmt.executeQuery(
            "SELECT * FROM parquet_mv_test.daily_summary "
                + "ORDER BY \"date\"")) {
          int rowCount = 0;
          while (rs.next()) {
            LOGGER.debug("MV row: date={}, count={}, qty={}, revenue={}",
                rs.getString("date"),
                rs.getLong("transaction_count"),
                rs.getLong("total_quantity"),
                rs.getDouble("total_revenue"));
            rowCount++;
          }
          assertEquals(3, rowCount,
              "Materialized view should have 3 rows");
        }
      }
    }
  }

  // ---------------------------------------------------------------
  // Test 3: multiple materializations with two base tables
  // ---------------------------------------------------------------

  /**
   * Creates sales.csv and products.csv. Defines two MVs:
   * {@code daily_sales_summary} and {@code product_summary}. Uses
   * programmatic schema. Asserts base table row count and aggregation
   * row counts.
   */
  @Test public void testMultipleMaterializations() throws Exception {
    writeSalesCsv();
    writeProductsCsv();

    Properties info = new Properties();
    BaseFileTest.applyEngineDefaults(info);

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // Materialization definitions
      List<Map<String, Object>> materializations = new ArrayList<>();

      Map<String, Object> dailySalesMV = new HashMap<>();
      dailySalesMV.put("view", "daily_sales_summary");
      dailySalesMV.put("table", "daily_sales_mv");
      dailySalesMV.put("sql",
          "SELECT \"date\", "
              + "COUNT(*) as transaction_count, "
              + "SUM(\"quantity\") as total_quantity, "
              + "SUM(\"quantity\" * \"price\") as total_revenue "
              + "FROM \"sales\" "
              + "GROUP BY \"date\"");
      materializations.add(dailySalesMV);

      Map<String, Object> productSummaryMV = new HashMap<>();
      productSummaryMV.put("view", "product_summary");
      productSummaryMV.put("table", "product_summary_mv");
      productSummaryMV.put("sql",
          "SELECT \"product\", "
              + "COUNT(*) as sales_count, "
              + "SUM(\"quantity\") as total_quantity, "
              + "SUM(\"quantity\" * \"price\") as total_revenue "
              + "FROM \"sales\" "
              + "GROUP BY \"product\"");
      materializations.add(productSummaryMV);

      // Schema operand
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("executionEngine", "parquet");
      operand.put("ephemeralCache", true);
      operand.put("materializations", materializations);

      rootSchema.add("MV_MULTI",
          FileSchemaFactory.INSTANCE.create(
              rootSchema, "MV_MULTI", operand));

      try (Statement stmt = connection.createStatement()) {
        // Verify base sales table has 6 rows
        try (ResultSet rs = stmt.executeQuery(
            "SELECT COUNT(*) as cnt FROM \"MV_MULTI\".sales")) {
          assertTrue(rs.next(), "Should have a count result");
          assertEquals(6, rs.getInt("cnt"),
              "Sales base table should have 6 rows");
        }

        // Verify base products table has 3 rows
        try (ResultSet rs = stmt.executeQuery(
            "SELECT COUNT(*) as cnt FROM \"MV_MULTI\".products")) {
          assertTrue(rs.next(), "Should have a count result");
          assertEquals(3, rs.getInt("cnt"),
              "Products base table should have 3 rows");
        }

        // Daily sales aggregation: 3 distinct dates
        try (ResultSet rs = stmt.executeQuery(
            "SELECT \"date\", "
                + "COUNT(*) as transaction_count, "
                + "SUM(\"quantity\") as total_quantity, "
                + "SUM(\"quantity\" * \"price\") as total_revenue "
                + "FROM \"MV_MULTI\".sales "
                + "GROUP BY \"date\" "
                + "ORDER BY \"date\"")) {
          int rowCount = 0;
          while (rs.next()) {
            LOGGER.debug("Daily row: date={}, count={}, qty={}, revenue={}",
                rs.getString("date"),
                rs.getInt("transaction_count"),
                rs.getInt("total_quantity"),
                rs.getDouble("total_revenue"));
            rowCount++;
          }
          assertEquals(3, rowCount,
              "Daily sales aggregation should yield 3 rows (3 dates)");
        }

        // Product aggregation: 3 distinct products
        try (ResultSet rs = stmt.executeQuery(
            "SELECT \"product\", "
                + "COUNT(*) as sales_count, "
                + "SUM(\"quantity\") as total_quantity, "
                + "SUM(\"quantity\" * \"price\") as total_revenue "
                + "FROM \"MV_MULTI\".sales "
                + "GROUP BY \"product\" "
                + "ORDER BY \"product\"")) {
          int rowCount = 0;
          while (rs.next()) {
            LOGGER.debug("Product row: product={}, count={}, qty={}, revenue={}",
                rs.getString("product"),
                rs.getInt("sales_count"),
                rs.getInt("total_quantity"),
                rs.getDouble("total_revenue"));
            rowCount++;
          }
          assertEquals(3, rowCount,
              "Product aggregation should yield 3 rows (3 products)");
        }
      }
    }
  }
}
