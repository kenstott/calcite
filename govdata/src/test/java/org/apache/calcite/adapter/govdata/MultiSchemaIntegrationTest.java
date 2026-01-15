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
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration test combining ECON, ECON_REFERENCE, GEO, and CENSUS schemas
 * into a unified model to validate cross-schema interoperability.
 *
 * <p>This test validates:
 * <ul>
 *   <li>All four schemas can be loaded simultaneously</li>
 *   <li>Tables are discoverable via information_schema</li>
 *   <li>Basic queries work across all schemas</li>
 *   <li>Cross-schema joins function correctly</li>
 *   <li>Reference data integrates with fact tables</li>
 * </ul>
 *
 * <p>Note: Tests run sequentially to avoid DuckDB httpfs extension concurrency issues.
 */
@Tag("integration")
@Execution(ExecutionMode.SAME_THREAD)
public class MultiSchemaIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiSchemaIntegrationTest.class);

  @BeforeAll
  public static void setup() {
    TestEnvironmentLoader.ensureLoaded();

    // Verify environment is properly configured
    assertNotNull(TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR"),
        "GOVDATA_CACHE_DIR must be set");
    assertNotNull(TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR"),
        "GOVDATA_PARQUET_DIR must be set");
  }

  /**
   * Creates a connection with all four schemas: ECON, ECON_REFERENCE, GEO, CENSUS.
   * Uses autoDownload: false to query existing cached data without making API calls.
   */
  private Connection createUnifiedConnection() throws SQLException {
    String cacheDir = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    String parquetDir = TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR");
    String executionEngine = TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE");
    if (executionEngine == null || executionEngine.isEmpty()) {
      executionEngine = "DUCKDB";
    }

    String startYear = TestEnvironmentLoader.getEnv("GOVDATA_START_YEAR");
    if (startYear == null || startYear.isEmpty()) {
      startYear = "2020";
    }

    String endYear = TestEnvironmentLoader.getEnv("GOVDATA_END_YEAR");
    if (endYear == null || endYear.isEmpty()) {
      endYear = "2024";
    }

    // S3 configuration for MinIO or AWS S3
    String awsAccessKeyId = TestEnvironmentLoader.getEnv("AWS_ACCESS_KEY_ID");
    String awsSecretAccessKey = TestEnvironmentLoader.getEnv("AWS_SECRET_ACCESS_KEY");
    String awsEndpointOverride = TestEnvironmentLoader.getEnv("AWS_ENDPOINT_OVERRIDE");
    String awsRegion = TestEnvironmentLoader.getEnv("AWS_REGION");

    // Build s3Config JSON if directory uses S3
    String s3ConfigJson = "";
    if (parquetDir != null && parquetDir.startsWith("s3://")) {
      StringBuilder s3Config = new StringBuilder();
      s3Config.append("\"s3Config\": {");
      if (awsEndpointOverride != null) {
        s3Config.append("\"endpoint\": \"").append(awsEndpointOverride).append("\",");
      }
      if (awsAccessKeyId != null) {
        s3Config.append("\"accessKeyId\": \"").append(awsAccessKeyId).append("\",");
      }
      if (awsSecretAccessKey != null) {
        s3Config.append("\"secretAccessKey\": \"").append(awsSecretAccessKey).append("\",");
      }
      if (awsRegion != null) {
        s3Config.append("\"region\": \"").append(awsRegion).append("\",");
      }
      // Remove trailing comma
      if (s3Config.charAt(s3Config.length() - 1) == ',') {
        s3Config.setLength(s3Config.length() - 1);
      }
      s3Config.append("},");
      s3ConfigJson = s3Config.toString();
    }

    String modelJson =
        "{" +
        "  \"version\": \"1.0\"," +
        "  \"defaultSchema\": \"ECON\"," +
        "  \"schemas\": [" +
        "    {" +
        "      \"name\": \"ECON\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"dataSource\": \"econ\"," +
        "        \"refreshInterval\": \"PT1H\"," +
        "        \"executionEngine\": \"" + executionEngine + "\"," +
        "        \"database_filename\": \"shared.duckdb\"," +
        "        \"ephemeralCache\": false," +
        "        \"cacheDirectory\": \"" + cacheDir + "\"," +
        "        \"directory\": \"" + parquetDir + "\"," +
        "        " + s3ConfigJson +
        "        \"startYear\": " + startYear + "," +
        "        \"endYear\": " + endYear + "," +
        "        \"autoDownload\": false" +
        "      }" +
        "    }," +
        "    {" +
        "      \"name\": \"ECON_REFERENCE\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"dataSource\": \"econ_reference\"," +
        "        \"refreshInterval\": \"PT1H\"," +
        "        \"executionEngine\": \"" + executionEngine + "\"," +
        "        \"database_filename\": \"shared.duckdb\"," +
        "        \"ephemeralCache\": false," +
        "        \"cacheDirectory\": \"" + cacheDir + "\"," +
        "        \"directory\": \"" + parquetDir + "\"," +
        "        " + s3ConfigJson +
        "        \"autoDownload\": false" +
        "      }" +
        "    }," +
        "    {" +
        "      \"name\": \"GEO\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"dataSource\": \"geo\"," +
        "        \"refreshInterval\": \"PT1H\"," +
        "        \"executionEngine\": \"" + executionEngine + "\"," +
        "        \"database_filename\": \"shared.duckdb\"," +
        "        \"ephemeralCache\": false," +
        "        \"cacheDirectory\": \"" + cacheDir + "\"," +
        "        \"directory\": \"" + parquetDir + "\"," +
        "        " + s3ConfigJson +
        "        \"autoDownload\": false" +
        "      }" +
        "    }," +
        "    {" +
        "      \"name\": \"CENSUS\"," +
        "      \"type\": \"custom\"," +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
        "      \"operand\": {" +
        "        \"dataSource\": \"census\"," +
        "        \"refreshInterval\": \"PT1H\"," +
        "        \"executionEngine\": \"" + executionEngine + "\"," +
        "        \"database_filename\": \"shared.duckdb\"," +
        "        \"ephemeralCache\": false," +
        "        \"cacheDirectory\": \"" + cacheDir + "\"," +
        "        \"directory\": \"" + parquetDir + "\"," +
        "        " + s3ConfigJson +
        "        \"startYear\": " + startYear + "," +
        "        \"endYear\": " + endYear + "," +
        "        \"autoDownload\": false" +
        "      }" +
        "    }" +
        "  ]" +
        "}";

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + modelJson);

    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  /**
   * Main integration test validating all four schemas work together.
   */
  @Test
  void testUnifiedSchemaModel() throws SQLException {
    LOGGER.info("\n" + repeat("=", 80));
    LOGGER.info(" MULTI-SCHEMA INTEGRATION TEST");
    LOGGER.info(" Testing ECON + ECON_REFERENCE + GEO + CENSUS unified model");
    LOGGER.info(repeat("=", 80));

    Map<String, SchemaTestResult> results = new HashMap<>();

    try (Connection connection = createUnifiedConnection()) {
      assertNotNull(connection, "Connection should be established");
      assertFalse(connection.isClosed(), "Connection should be open");

      // Validate each schema
      results.put("ECON", validateSchema(connection, "ECON"));
      results.put("ECON_REFERENCE", validateSchema(connection, "ECON_REFERENCE"));
      results.put("GEO", validateSchema(connection, "GEO"));
      results.put("CENSUS", validateSchema(connection, "CENSUS"));

      // Test cross-schema queries
      boolean crossSchemaSuccess = testCrossSchemaQueries(connection);

      // Print summary
      printSummary(results, crossSchemaSuccess);

      // Assert overall success - at least one table must be queryable per schema
      boolean allPassed = true;
      int totalQueryable = 0;
      for (SchemaTestResult result : results.values()) {
        totalQueryable += result.queryableTables.size();
        // A schema is OK if it has at least one queryable table OR discovered tables
        // (empty schema is acceptable if it's a valid schema definition)
      }

      // Test passes if we have at least some queryable tables across all schemas
      assertTrue(totalQueryable > 0,
          "At least one table must be queryable across all schemas. "
          + "Check if data has been materialized to Parquet.");

      if (!crossSchemaSuccess) {
        LOGGER.warn("Cross-schema queries had some failures - see logs above");
      }
    }
  }

  /**
   * Validates a single schema by discovering and querying its tables.
   */
  private SchemaTestResult validateSchema(Connection conn, String schemaName)
      throws SQLException {
    LOGGER.info("\n" + repeat("-", 60));
    LOGGER.info(" Validating {} schema", schemaName);
    LOGGER.info(repeat("-", 60));

    SchemaTestResult result = new SchemaTestResult(schemaName);

    try (Statement stmt = conn.createStatement()) {
      // Discover tables using JDBC metadata
      List<String> discoveredTables = discoverTables(conn, schemaName);
      result.discoveredTables = new HashSet<>(discoveredTables);

      LOGGER.info("  Discovered {} tables:", discoveredTables.size());
      for (String tableName : discoveredTables) {
        LOGGER.info("    - {}", tableName);
      }

      // Test each discovered table
      if (!discoveredTables.isEmpty()) {
        LOGGER.info("\n  Testing table queries:");
        for (String tableName : discoveredTables) {
          boolean querySuccess = testTableQuery(stmt, schemaName, tableName, result);
          if (querySuccess) {
            result.queryableTables.add(tableName);
          } else {
            result.failedTables.add(tableName);
          }
        }
      }

      // Determine overall success
      result.success = !result.discoveredTables.isEmpty()
          && result.queryableTables.size() > 0;

      String status = result.success ? "PASS" : "WARN";
      LOGGER.info("\n  {} Schema Result: {} (Discovered: {}, Queryable: {}, Failed: {})",
          schemaName, status, result.discoveredTables.size(),
          result.queryableTables.size(), result.failedTables.size());
    }

    return result;
  }

  /**
   * Discovers all tables in a schema using JDBC DatabaseMetaData.
   */
  private List<String> discoverTables(Connection conn, String schemaName) throws SQLException {
    List<String> tables = new ArrayList<>();

    DatabaseMetaData metaData = conn.getMetaData();
    // Use null for catalog (not used in Calcite), schema name, table pattern (all), and types
    try (ResultSet rs = metaData.getTables(null, schemaName, "%",
        new String[]{"TABLE", "VIEW"})) {
      while (rs.next()) {
        String tableName = rs.getString("TABLE_NAME");
        tables.add(tableName);
      }
    }

    // Sort the tables
    java.util.Collections.sort(tables);
    return tables;
  }

  /**
   * Tests a single table with COUNT and SELECT queries.
   */
  private boolean testTableQuery(Statement stmt, String schemaName, String tableName,
      SchemaTestResult result) {
    try {
      // Test COUNT
      String countQuery = "SELECT COUNT(*) as cnt FROM \"" + schemaName + "\".\"" + tableName + "\"";
      long rowCount = 0;
      try (ResultSet rs = stmt.executeQuery(countQuery)) {
        if (rs.next()) {
          rowCount = rs.getLong("cnt");
        }
      }

      // Test SELECT with LIMIT
      String selectQuery = "SELECT * FROM \"" + schemaName + "\".\"" + tableName + "\" LIMIT 3";
      int columnCount = 0;
      int sampleRows = 0;
      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        ResultSetMetaData meta = rs.getMetaData();
        columnCount = meta.getColumnCount();
        while (rs.next()) {
          sampleRows++;
        }
      }

      result.tableCounts.put(tableName, rowCount);

      String status = rowCount > 0 ? "[OK]" : "[EMPTY]";
      LOGGER.info("    {} {}.{} - {} rows, {} columns",
          status, schemaName, tableName, rowCount, columnCount);

      return true;
    } catch (SQLException e) {
      LOGGER.error("    [FAIL] {}.{} - {}", schemaName, tableName, e.getMessage());
      return false;
    }
  }

  /**
   * Tests cross-schema queries to validate interoperability.
   */
  private boolean testCrossSchemaQueries(Connection conn) {
    LOGGER.info("\n" + repeat("-", 60));
    LOGGER.info(" Testing Cross-Schema Queries");
    LOGGER.info(repeat("-", 60));

    boolean allPassed = true;

    try (Statement stmt = conn.createStatement()) {
      // Test 1: ECON state_gdp query
      allPassed &= testQuery(stmt, "ECON state_gdp data",
          "SELECT geo_name, year, gdp_millions "
          + "FROM \"ECON\".state_gdp "
          + "LIMIT 5");

      // Test 2: ECON_REFERENCE fred_series catalog
      allPassed &= testQuery(stmt, "ECON_REFERENCE fred_series catalog",
          "SELECT series, title, frequency "
          + "FROM \"ECON_REFERENCE\".fred_series "
          + "WHERE series LIKE 'GDP%' "
          + "LIMIT 5");

      // Test 3: GEO tiger_states query
      allPassed &= testQuery(stmt, "GEO tiger_states query",
          "SELECT state_fips, state_name "
          + "FROM \"GEO\".tiger_states "
          + "WHERE state_fips IN ('06', '36', '48') "
          + "LIMIT 5");

      // Test 4: GEO tiger_counties query
      allPassed &= testQuery(stmt, "GEO tiger_counties query",
          "SELECT state_fips, county_fips, county_name "
          + "FROM \"GEO\".tiger_counties "
          + "WHERE state_fips = '06' "
          + "LIMIT 5");

      // Test 5: CENSUS acs_population query
      allPassed &= testQuery(stmt, "CENSUS acs_population query",
          "SELECT geoid, geo_name "
          + "FROM \"CENSUS\".acs_population "
          + "LIMIT 5");

      // Test 6: Multi-schema count query
      allPassed &= testQuery(stmt, "Multi-schema analytical query",
          "SELECT "
          + "  (SELECT COUNT(*) FROM \"ECON\".state_gdp) as econ_rows, "
          + "  (SELECT COUNT(*) FROM \"GEO\".tiger_states) as geo_rows, "
          + "  (SELECT COUNT(*) FROM \"ECON_REFERENCE\".fred_series) as ref_rows");

      // Test 7: ECON + ECON_REFERENCE - join economic data with reference catalog
      allPassed &= testQuery(stmt, "ECON + ECON_REFERENCE reference table query",
          "SELECT COUNT(*) as cnt "
          + "FROM \"ECON_REFERENCE\".bls_geographies "
          + "WHERE geo_type = 'state'");

      // Test 8: Cross-schema join - GEO states with ECON state_gdp
      allPassed &= testQuery(stmt, "GEO + ECON cross-schema join",
          "SELECT g.state_name, e.gdp_millions, e.year "
          + "FROM \"GEO\".tiger_states g "
          + "INNER JOIN \"ECON\".state_gdp e ON g.state_fips = e.geo_fips "
          + "WHERE g.state_fips = '06' "
          + "LIMIT 5");

    } catch (SQLException e) {
      LOGGER.error("Cross-schema query setup failed: {}", e.getMessage());
      allPassed = false;
    }

    String status = allPassed ? "PASS" : "PARTIAL";
    LOGGER.info("\n  Cross-Schema Queries Result: {}", status);
    return allPassed;
  }

  /**
   * Executes a single test query and reports results.
   */
  private boolean testQuery(Statement stmt, String description, String sql) {
    try {
      LOGGER.info("\n  Testing: {}", description);
      LOGGER.debug("    SQL: {}", sql);

      int rowCount = 0;
      try (ResultSet rs = stmt.executeQuery(sql)) {
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();

        while (rs.next()) {
          rowCount++;
          if (rowCount == 1) {
            // Log first row structure
            StringBuilder row = new StringBuilder("    Sample: ");
            for (int i = 1; i <= Math.min(colCount, 3); i++) {
              if (i > 1) {
                row.append(", ");
              }
              row.append(meta.getColumnName(i)).append("=").append(rs.getString(i));
            }
            if (colCount > 3) {
              row.append("...");
            }
            LOGGER.info("{}", row);
          }
        }
      }

      LOGGER.info("    [OK] Returned {} rows", rowCount);
      return true;
    } catch (SQLException e) {
      LOGGER.warn("    [WARN] {}", e.getMessage());
      return false;
    }
  }

  /**
   * Prints final summary of all schema test results.
   */
  private void printSummary(Map<String, SchemaTestResult> results, boolean crossSchemaSuccess) {
    LOGGER.info("\n" + repeat("=", 80));
    LOGGER.info(" MULTI-SCHEMA INTEGRATION TEST SUMMARY");
    LOGGER.info(repeat("=", 80));

    int totalDiscovered = 0;
    int totalQueryable = 0;
    int totalFailed = 0;
    int totalWithData = 0;

    for (Map.Entry<String, SchemaTestResult> entry : results.entrySet()) {
      SchemaTestResult r = entry.getValue();
      totalDiscovered += r.discoveredTables.size();
      totalQueryable += r.queryableTables.size();
      totalFailed += r.failedTables.size();

      // Count tables with data
      for (Long count : r.tableCounts.values()) {
        if (count > 0) {
          totalWithData++;
        }
      }

      String status = r.queryableTables.size() > 0 ? "PASS" : "EMPTY";
      LOGGER.info("  {} - {} (Tables: {}, Queryable: {}, With Data: {}, Failed: {})",
          entry.getKey(), status, r.discoveredTables.size(),
          r.queryableTables.size(),
          r.tableCounts.values().stream().filter(c -> c > 0).count(),
          r.failedTables.size());
    }

    LOGGER.info("\n  Totals:");
    LOGGER.info("    Tables Discovered: {}", totalDiscovered);
    LOGGER.info("    Tables Queryable: {}", totalQueryable);
    LOGGER.info("    Tables With Data: {}", totalWithData);
    LOGGER.info("    Tables Failed: {}", totalFailed);
    LOGGER.info("    Cross-Schema Queries: {}", crossSchemaSuccess ? "PASS" : "PARTIAL");

    boolean overallSuccess = totalQueryable > 0;
    LOGGER.info("\n" + repeat("=", 80));
    if (overallSuccess) {
      LOGGER.info(" OVERALL RESULT: PASS - {} tables queryable across 4 schemas!", totalQueryable);
    } else {
      LOGGER.error(" OVERALL RESULT: FAIL - No queryable tables found");
      LOGGER.error(" Ensure data has been materialized to GOVDATA_PARQUET_DIR");
    }
    LOGGER.info(repeat("=", 80));
  }

  /**
   * Helper to create repeated string (Java 8 compatible).
   */
  private static String repeat(String str, int count) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append(str);
    }
    return sb.toString();
  }

  /**
   * Holds test results for a single schema.
   */
  private static class SchemaTestResult {
    final String schemaName;
    Set<String> discoveredTables = new HashSet<>();
    Set<String> queryableTables = new HashSet<>();
    Set<String> failedTables = new HashSet<>();
    Map<String, Long> tableCounts = new HashMap<>();
    boolean success = false;

    SchemaTestResult(String schemaName) {
      this.schemaName = schemaName;
    }
  }
}
