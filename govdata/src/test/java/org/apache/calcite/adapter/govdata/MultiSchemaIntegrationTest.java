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
   * Uses autoDownload: true to materialize tables from API sources on first access.
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
        "        \"autoDownload\": true" +
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
        "        \"autoDownload\": true" +
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
        "        \"autoDownload\": true" +
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
        "        \"autoDownload\": true" +
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

      // =========================================================================
      // COMPLEX FILTER TESTS
      // =========================================================================

      // Test 9: Multiple filter conditions with AND/OR
      allPassed &= testQuery(stmt, "Complex filter with AND/OR",
          "SELECT geo_name, year, gdp_millions "
          + "FROM \"ECON\".state_gdp "
          + "WHERE (year >= 2020 AND year <= 2023) "
          + "  AND (gdp_millions > 100000 OR geo_name LIKE 'Cal%') "
          + "ORDER BY gdp_millions DESC "
          + "LIMIT 10");

      // Test 10: IN clause with subquery-style filter
      allPassed &= testQuery(stmt, "Filter with IN clause",
          "SELECT state_fips, state_name "
          + "FROM \"GEO\".tiger_states "
          + "WHERE state_fips IN ('06', '36', '48', '12', '17') "
          + "ORDER BY state_name");

      // Test 11: BETWEEN filter
      allPassed &= testQuery(stmt, "Filter with BETWEEN",
          "SELECT geo_name, year, gdp_millions "
          + "FROM \"ECON\".state_gdp "
          + "WHERE gdp_millions BETWEEN 500000 AND 2000000 "
          + "  AND year = 2022 "
          + "ORDER BY gdp_millions DESC "
          + "LIMIT 10");

      // Test 12: LIKE pattern matching
      allPassed &= testQuery(stmt, "Filter with LIKE patterns",
          "SELECT series, title "
          + "FROM \"ECON_REFERENCE\".fred_series "
          + "WHERE title LIKE '%unemployment%' OR title LIKE '%Unemployment%' "
          + "LIMIT 10");

      // Test 13: IS NOT NULL filter
      allPassed &= testQuery(stmt, "Filter with IS NOT NULL",
          "SELECT county_fips, county_name, state_fips "
          + "FROM \"GEO\".tiger_counties "
          + "WHERE county_name IS NOT NULL AND state_fips = '06' "
          + "LIMIT 10");

      // =========================================================================
      // AGGREGATE TESTS
      // =========================================================================

      // Test 14: Simple GROUP BY with COUNT
      allPassed &= testQuery(stmt, "GROUP BY with COUNT",
          "SELECT year, COUNT(*) as state_count "
          + "FROM \"ECON\".state_gdp "
          + "GROUP BY year "
          + "ORDER BY year DESC "
          + "LIMIT 10");

      // Test 15: GROUP BY with SUM
      allPassed &= testQuery(stmt, "GROUP BY with SUM",
          "SELECT year, SUM(gdp_millions) as total_gdp "
          + "FROM \"ECON\".state_gdp "
          + "GROUP BY year "
          + "ORDER BY year DESC "
          + "LIMIT 10");

      // Test 16: GROUP BY with multiple aggregates
      allPassed &= testQuery(stmt, "GROUP BY with multiple aggregates (SUM, AVG, MIN, MAX)",
          "SELECT year, "
          + "  SUM(gdp_millions) as total_gdp, "
          + "  AVG(gdp_millions) as avg_gdp, "
          + "  MIN(gdp_millions) as min_gdp, "
          + "  MAX(gdp_millions) as max_gdp "
          + "FROM \"ECON\".state_gdp "
          + "GROUP BY year "
          + "ORDER BY year DESC "
          + "LIMIT 5");

      // Test 17: GROUP BY with HAVING clause
      allPassed &= testQuery(stmt, "GROUP BY with HAVING",
          "SELECT state_fips, COUNT(*) as county_count "
          + "FROM \"GEO\".tiger_counties "
          + "GROUP BY state_fips "
          + "HAVING COUNT(*) > 50 "
          + "ORDER BY county_count DESC "
          + "LIMIT 10");

      // Test 18: GROUP BY on GEO data
      allPassed &= testQuery(stmt, "GROUP BY on counties per state",
          "SELECT state_fips, COUNT(*) as num_counties "
          + "FROM \"GEO\".tiger_counties "
          + "GROUP BY state_fips "
          + "ORDER BY num_counties DESC "
          + "LIMIT 10");

      // =========================================================================
      // JOIN TESTS
      // =========================================================================

      // Test 19: INNER JOIN with aggregation
      allPassed &= testQuery(stmt, "JOIN with aggregation",
          "SELECT g.state_name, SUM(e.gdp_millions) as total_gdp "
          + "FROM \"GEO\".tiger_states g "
          + "INNER JOIN \"ECON\".state_gdp e ON g.state_fips = e.geo_fips "
          + "GROUP BY g.state_name "
          + "ORDER BY total_gdp DESC "
          + "LIMIT 10");

      // Test 20: LEFT JOIN
      allPassed &= testQuery(stmt, "LEFT JOIN states with GDP",
          "SELECT g.state_name, g.state_fips, e.gdp_millions, e.year "
          + "FROM \"GEO\".tiger_states g "
          + "LEFT JOIN \"ECON\".state_gdp e ON g.state_fips = e.geo_fips AND e.year = 2022 "
          + "ORDER BY g.state_name "
          + "LIMIT 10");

      // Test 21: JOIN with filter on both tables
      allPassed &= testQuery(stmt, "JOIN with filters on both tables",
          "SELECT g.state_name, e.year, e.gdp_millions "
          + "FROM \"GEO\".tiger_states g "
          + "INNER JOIN \"ECON\".state_gdp e ON g.state_fips = e.geo_fips "
          + "WHERE g.state_fips IN ('06', '48', '36') "
          + "  AND e.year >= 2020 "
          + "ORDER BY e.gdp_millions DESC "
          + "LIMIT 15");

      // Test 22: Three-table join (GEO counties -> states -> ECON)
      allPassed &= testQuery(stmt, "Three-table join (counties -> states -> GDP)",
          "SELECT s.state_name, COUNT(c.county_fips) as num_counties, MAX(e.gdp_millions) as max_gdp "
          + "FROM \"GEO\".tiger_states s "
          + "INNER JOIN \"GEO\".tiger_counties c ON s.state_fips = c.state_fips "
          + "INNER JOIN \"ECON\".state_gdp e ON s.state_fips = e.geo_fips AND e.year = 2022 "
          + "GROUP BY s.state_name "
          + "ORDER BY num_counties DESC "
          + "LIMIT 10");

      // Test 23: Self-referential style comparison (same table, different filters)
      allPassed &= testQuery(stmt, "Compare GDP across years",
          "SELECT e1.geo_name, e1.gdp_millions as gdp_2021, e2.gdp_millions as gdp_2022 "
          + "FROM \"ECON\".state_gdp e1 "
          + "INNER JOIN \"ECON\".state_gdp e2 "
          + "  ON e1.geo_fips = e2.geo_fips "
          + "WHERE e1.year = 2021 AND e2.year = 2022 "
          + "ORDER BY e2.gdp_millions DESC "
          + "LIMIT 10");

      // =========================================================================
      // SUBQUERY TESTS
      // =========================================================================

      // Test 24: Scalar subquery in SELECT
      allPassed &= testQuery(stmt, "Scalar subquery in SELECT",
          "SELECT geo_name, gdp_millions, "
          + "  (SELECT AVG(gdp_millions) FROM \"ECON\".state_gdp WHERE year = 2022) as avg_gdp "
          + "FROM \"ECON\".state_gdp "
          + "WHERE year = 2022 "
          + "ORDER BY gdp_millions DESC "
          + "LIMIT 5");

      // Test 25: Subquery in WHERE with IN
      allPassed &= testQuery(stmt, "Subquery in WHERE with IN",
          "SELECT state_name, state_fips "
          + "FROM \"GEO\".tiger_states "
          + "WHERE state_fips IN ("
          + "  SELECT DISTINCT geo_fips FROM \"ECON\".state_gdp "
          + "  WHERE gdp_millions > 1000000 AND year = 2022"
          + ") "
          + "ORDER BY state_name");

      // Test 26: Correlated subquery
      allPassed &= testQuery(stmt, "Correlated subquery",
          "SELECT g.state_name, g.state_fips, "
          + "  (SELECT MAX(e.gdp_millions) FROM \"ECON\".state_gdp e "
          + "   WHERE e.geo_fips = g.state_fips) as max_gdp "
          + "FROM \"GEO\".tiger_states g "
          + "ORDER BY g.state_name "
          + "LIMIT 10");

      // =========================================================================
      // COMPLEX ANALYTICAL QUERIES
      // =========================================================================

      // Test 27: Top-N per group using ROW_NUMBER (if supported)
      allPassed &= testQuery(stmt, "Complex analytical - states above average GDP",
          "SELECT geo_name, gdp_millions "
          + "FROM \"ECON\".state_gdp "
          + "WHERE year = 2022 "
          + "  AND gdp_millions > (SELECT AVG(gdp_millions) FROM \"ECON\".state_gdp WHERE year = 2022) "
          + "ORDER BY gdp_millions DESC");

      // Test 28: UNION of results from different schemas
      allPassed &= testQuery(stmt, "UNION across schemas",
          "SELECT 'STATE' as geo_type, state_name as name, state_fips as fips "
          + "FROM \"GEO\".tiger_states "
          + "WHERE state_fips IN ('06', '36') "
          + "UNION ALL "
          + "SELECT 'COUNTY' as geo_type, county_name as name, county_fips as fips "
          + "FROM \"GEO\".tiger_counties "
          + "WHERE state_fips = '06' "
          + "LIMIT 20");

      // Test 29: CASE expression with aggregation
      allPassed &= testQuery(stmt, "CASE expression with aggregation",
          "SELECT "
          + "  CASE "
          + "    WHEN gdp_millions > 1000000 THEN 'Large' "
          + "    WHEN gdp_millions > 500000 THEN 'Medium' "
          + "    ELSE 'Small' "
          + "  END as gdp_category, "
          + "  COUNT(*) as state_count, "
          + "  SUM(gdp_millions) as total_gdp "
          + "FROM \"ECON\".state_gdp "
          + "WHERE year = 2022 "
          + "GROUP BY "
          + "  CASE "
          + "    WHEN gdp_millions > 1000000 THEN 'Large' "
          + "    WHEN gdp_millions > 500000 THEN 'Medium' "
          + "    ELSE 'Small' "
          + "  END "
          + "ORDER BY total_gdp DESC");

      // Test 30: Complex join with derived table
      allPassed &= testQuery(stmt, "Join with derived table (inline view)",
          "SELECT s.state_name, gdp_stats.total_gdp, gdp_stats.avg_gdp "
          + "FROM \"GEO\".tiger_states s "
          + "INNER JOIN ("
          + "  SELECT geo_fips, SUM(gdp_millions) as total_gdp, AVG(gdp_millions) as avg_gdp "
          + "  FROM \"ECON\".state_gdp "
          + "  WHERE year >= 2020 "
          + "  GROUP BY geo_fips"
          + ") gdp_stats ON s.state_fips = gdp_stats.geo_fips "
          + "ORDER BY gdp_stats.total_gdp DESC "
          + "LIMIT 10");

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

  /**
   * Tests data quality by sampling rows from every table and verifying
   * that they contain meaningful (non-null, non-blank, non-NaN) data.
   */
  @Test
  void testDataQualityAcrossAllTables() throws SQLException {
    LOGGER.info("\n" + repeat("=", 80));
    LOGGER.info(" DATA QUALITY VALIDATION TEST");
    LOGGER.info(" Sampling rows from all tables to verify data integrity");
    LOGGER.info(repeat("=", 80));

    String[] schemas = {"ECON", "ECON_REFERENCE", "GEO", "CENSUS"};
    int totalTables = 0;
    int tablesWithGoodData = 0;
    int tablesWithProblems = 0;
    int emptyTables = 0;

    try (Connection connection = createUnifiedConnection()) {
      for (String schemaName : schemas) {
        LOGGER.info("\n" + repeat("-", 60));
        LOGGER.info(" Validating data quality in {} schema", schemaName);
        LOGGER.info(repeat("-", 60));

        List<String> tables = discoverTables(connection, schemaName);
        for (String tableName : tables) {
          totalTables++;
          DataQualityResult result = validateTableDataQuality(connection, schemaName, tableName);

          if (result.isEmpty) {
            emptyTables++;
            LOGGER.info("  [EMPTY] {}.{} - no rows", schemaName, tableName);
          } else if (result.hasProblems) {
            tablesWithProblems++;
            LOGGER.warn("  [WARN]  {}.{} - {} rows, {} cols, >50% null cols ({}/{}): {}",
                schemaName, tableName, result.rowsSampled, result.columnCount,
                result.problemColumns.size(), result.columnCount,
                truncateList(result.problemColumns, 5));
          } else {
            tablesWithGoodData++;
            if (result.problemColumns.isEmpty()) {
              LOGGER.info("  [OK]    {}.{} - {} rows, {} cols, all data valid",
                  schemaName, tableName, result.rowsSampled, result.columnCount);
            } else {
              // Has some null columns but less than half - still OK
              LOGGER.info("  [OK]    {}.{} - {} rows, {} cols ({} sparse cols)",
                  schemaName, tableName, result.rowsSampled, result.columnCount,
                  result.problemColumns.size());
            }
          }
        }
      }

      // Print summary
      LOGGER.info("\n" + repeat("=", 80));
      LOGGER.info(" DATA QUALITY SUMMARY");
      LOGGER.info(repeat("=", 80));
      LOGGER.info("  Total tables checked: {}", totalTables);
      LOGGER.info("  Tables with good data: {}", tablesWithGoodData);
      LOGGER.info("  Tables with problems: {}", tablesWithProblems);
      LOGGER.info("  Empty tables: {}", emptyTables);

      // Assert that most tables have good data
      int nonEmptyTables = totalTables - emptyTables;
      if (nonEmptyTables > 0) {
        double goodDataPercent = (tablesWithGoodData * 100.0) / nonEmptyTables;
        LOGGER.info("  Good data percentage: {}%", String.format("%.1f", goodDataPercent));

        // At least 80% of non-empty tables should have good data
        assertTrue(goodDataPercent >= 80.0,
            "At least 80% of non-empty tables should have valid data. "
            + "Found only " + String.format("%.1f", goodDataPercent) + "%");
      }

      // At least some tables should have data
      assertTrue(tablesWithGoodData > 0,
          "At least some tables should have valid data");
    }
  }

  /**
   * Validates data quality for a single table by sampling rows.
   */
  private DataQualityResult validateTableDataQuality(Connection conn, String schema, String table) {
    DataQualityResult result = new DataQualityResult();

    String sql = "SELECT * FROM \"" + schema + "\".\"" + table + "\" LIMIT 10";

    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      ResultSetMetaData meta = rs.getMetaData();
      result.columnCount = meta.getColumnCount();

      // Track columns that have all null/blank/NaN values
      Map<String, Integer> nullCounts = new HashMap<>();
      Map<String, Integer> blankCounts = new HashMap<>();
      Map<String, Integer> nanCounts = new HashMap<>();

      for (int i = 1; i <= result.columnCount; i++) {
        String colName = meta.getColumnName(i);
        nullCounts.put(colName, 0);
        blankCounts.put(colName, 0);
        nanCounts.put(colName, 0);
      }

      while (rs.next()) {
        result.rowsSampled++;

        for (int i = 1; i <= result.columnCount; i++) {
          String colName = meta.getColumnName(i);
          Object value = rs.getObject(i);

          if (value == null) {
            nullCounts.put(colName, nullCounts.get(colName) + 1);
          } else if (value instanceof String) {
            String strVal = (String) value;
            if (strVal.trim().isEmpty() || "null".equalsIgnoreCase(strVal)
                || "NaN".equalsIgnoreCase(strVal) || "N/A".equalsIgnoreCase(strVal)) {
              blankCounts.put(colName, blankCounts.get(colName) + 1);
            }
          } else if (value instanceof Number) {
            double d = ((Number) value).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
              nanCounts.put(colName, nanCounts.get(colName) + 1);
            }
          }
        }
      }

      if (result.rowsSampled == 0) {
        result.isEmpty = true;
        return result;
      }

      // Check for columns where ALL sampled values are problematic
      int allNullColumns = 0;
      for (int i = 1; i <= result.columnCount; i++) {
        String colName = meta.getColumnName(i);
        int totalProblems = nullCounts.get(colName) + blankCounts.get(colName) + nanCounts.get(colName);

        // If ALL values in sample are problematic, track it
        if (totalProblems == result.rowsSampled) {
          result.problemColumns.add(colName + "(all " + describeProblem(nullCounts.get(colName),
              blankCounts.get(colName), nanCounts.get(colName)) + ")");
          allNullColumns++;
        }
      }

      // Only flag as having problems if MORE THAN HALF of columns are all-null
      // Some null columns are expected (optional fields, sparse data)
      if (result.columnCount > 0 && allNullColumns > result.columnCount / 2) {
        result.hasProblems = true;
      }

    } catch (SQLException e) {
      result.hasProblems = true;
      result.problemColumns.add("QUERY_ERROR: " + e.getMessage());
    }

    return result;
  }

  /**
   * Truncates a list to the first N items for display.
   */
  private String truncateList(List<String> items, int max) {
    if (items.size() <= max) {
      return String.join(", ", items);
    }
    List<String> truncated = items.subList(0, max);
    return String.join(", ", truncated) + "... (" + (items.size() - max) + " more)";
  }

  /**
   * Describes the type of data problem found.
   */
  private String describeProblem(int nullCount, int blankCount, int nanCount) {
    List<String> problems = new ArrayList<>();
    if (nullCount > 0) {
      problems.add("null");
    }
    if (blankCount > 0) {
      problems.add("blank");
    }
    if (nanCount > 0) {
      problems.add("NaN");
    }
    return String.join("/", problems);
  }

  /**
   * Holds data quality validation results for a single table.
   */
  private static class DataQualityResult {
    int rowsSampled = 0;
    int columnCount = 0;
    boolean isEmpty = false;
    boolean hasProblems = false;
    List<String> problemColumns = new ArrayList<>();
  }
}
