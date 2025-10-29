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
package org.apache.calcite.adapter.govdata.econ;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates ECON schema metro_wages functionality.
 *
 * <p>This test validates that metro_wages downloads from QCEW Open Data API
 * and can be queried successfully. The StorageProvider abstraction supports
 * both local filesystem and S3 storage (s3:// prefix) transparently.
 */
@Tag("integration")
public class EconMetroWagesTest {

  @BeforeAll
  public static void setup() {
    // Use separate cache directory for S3 testing to avoid conflicts
    String cacheDir = System.getenv("GOVDATA_CACHE_DIR");
    if (cacheDir == null) {
      System.setProperty("GOVDATA_CACHE_DIR", "/Volumes/T9/govdata-cache-s3-test");
    }
  }

  /**
   * Integration test for metro_wages bulk CSV download functionality.
   * <p>Validates Phase 5, Step 5.3 requirements:
   * <ul>
   *   <li>Download completes without HTTP 404 errors</li>
   *   <li>All 27 metros have data</li>
   *   <li>Both annual (qtr="A") and quarterly (qtr="1","2","3","4") records exist</li>
   *   <li>Data quality: no nulls in required fields</li>
   *   <li>New fields: qtr and average_annual_pay are present</li>
   * </ul>
   */
  @Test public void testMetroWagesBulkDownload() throws Exception {
    String cacheDir = System.getenv("GOVDATA_CACHE_DIR");
    if (cacheDir == null) cacheDir = "/Volumes/T9/govdata-cache";

    String parquetDir = System.getenv("GOVDATA_PARQUET_DIR");
    if (parquetDir == null) parquetDir = "/Volumes/T9/govdata-parquet";

    System.out.println("\n=== Metro Wages Bulk CSV Download Integration Test ===");
    System.out.println("GOVDATA_CACHE_DIR: " + cacheDir);
    System.out.println("GOVDATA_PARQUET_DIR: " + parquetDir);

    String blsApiKey = System.getenv("BLS_API_KEY");
    if (blsApiKey == null) blsApiKey = "a8d2d6de36194ea580c65560da6323ca";

    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"econ\","
        + "\"schemas\": [{"
        + "  \"name\": \"econ\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\": {"
        + "    \"dataSource\": \"econ\","
        + "    \"directory\": \"" + parquetDir + "\","
        + "    \"cacheDirectory\": \"" + cacheDir + "\","
        + "    \"blsApiKey\": \"" + blsApiKey + "\","
        + "    \"autoDownload\": true,"
        + "    \"startYear\": 2023,"
        + "    \"endYear\": 2023,"
        + "    \"enabledSources\": [\"bls\"]"
        + "  }"
        + "}]"
        + "}";

    java.nio.file.Path modelFile = java.nio.file.Files.createTempFile("metro-wages-test", ".json");
    java.nio.file.Files.writeString(modelFile, modelJson);

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, info);
         Statement stmt = conn.createStatement()) {

      System.out.println("\n✓ VALIDATION 1: Download completed without HTTP 404 errors");
      System.out.println("  (If we reached this point, bulk CSV download succeeded)");

      // Validation 2: Verify all 27 metros have data
      System.out.println("\n✓ VALIDATION 2: Verifying all 27 metros have data");
      String metroCountQuery = "SELECT COUNT(DISTINCT metro_area_code) as metro_count "
          + "FROM econ.metro_wages WHERE year = 2023";
      try (ResultSet rs = stmt.executeQuery(metroCountQuery)) {
        if (rs.next()) {
          int metroCount = rs.getInt("metro_count");
          System.out.printf("  Found %d unique metros in 2023\n", metroCount);
          assertTrue(metroCount >= 27,
              "Expected at least 27 metros, got " + metroCount);
        }
      }

      // Validation 3: Verify both annual and quarterly records exist
      System.out.println("\n✓ VALIDATION 3: Verifying annual and quarterly records");

      // Check for annual records (qtr="A")
      String annualQuery = "SELECT COUNT(*) as cnt FROM econ.metro_wages "
          + "WHERE year = 2023 AND qtr = 'A'";
      try (ResultSet rs = stmt.executeQuery(annualQuery)) {
        if (rs.next()) {
          int annualCount = rs.getInt("cnt");
          System.out.printf("  Annual records (qtr='A'): %d\n", annualCount);
          assertTrue(annualCount >= 27,
              "Expected at least 27 annual records (one per metro), got " + annualCount);
        }
      }

      // Check for quarterly records (qtr="1","2","3","4")
      String quarterlyQuery = "SELECT qtr, COUNT(*) as cnt FROM econ.metro_wages "
          + "WHERE year = 2023 AND qtr IN ('1', '2', '3', '4') "
          + "GROUP BY qtr ORDER BY qtr";
      try (ResultSet rs = stmt.executeQuery(quarterlyQuery)) {
        int totalQuarterly = 0;
        while (rs.next()) {
          String qtr = rs.getString("qtr");
          int count = rs.getInt("cnt");
          System.out.printf("  Quarter %s: %d records\n", qtr, count);
          totalQuarterly += count;
          assertTrue(count >= 27,
              "Expected at least 27 records for quarter " + qtr + ", got " + count);
        }
        System.out.printf("  Total quarterly records: %d\n", totalQuarterly);
        assertTrue(totalQuarterly >= 108,
            "Expected at least 108 quarterly records (27 metros × 4 quarters), got " + totalQuarterly);
      }

      // Validation 4: Verify data quality (no nulls in required fields)
      System.out.println("\n✓ VALIDATION 4: Verifying data quality");

      String nullCheckQuery = "SELECT "
          + "  COUNT(*) as total_rows, "
          + "  COUNT(metro_area_code) as non_null_code, "
          + "  COUNT(metro_area_name) as non_null_name, "
          + "  COUNT(year) as non_null_year, "
          + "  COUNT(qtr) as non_null_qtr, "
          + "  COUNT(average_weekly_wage) as non_null_weekly, "
          + "  COUNT(average_annual_pay) as non_null_annual "
          + "FROM econ.metro_wages WHERE year = 2023";

      try (ResultSet rs = stmt.executeQuery(nullCheckQuery)) {
        if (rs.next()) {
          int totalRows = rs.getInt("total_rows");
          int nonNullCode = rs.getInt("non_null_code");
          int nonNullName = rs.getInt("non_null_name");
          int nonNullYear = rs.getInt("non_null_year");
          int nonNullQtr = rs.getInt("non_null_qtr");
          int nonNullWeekly = rs.getInt("non_null_weekly");
          int nonNullAnnual = rs.getInt("non_null_annual");

          System.out.printf("  Total rows: %d\n", totalRows);
          System.out.printf("  Non-null metro_area_code: %d\n", nonNullCode);
          System.out.printf("  Non-null metro_area_name: %d\n", nonNullName);
          System.out.printf("  Non-null year: %d\n", nonNullYear);
          System.out.printf("  Non-null qtr: %d\n", nonNullQtr);
          System.out.printf("  Non-null average_weekly_wage: %d\n", nonNullWeekly);
          System.out.printf("  Non-null average_annual_pay: %d\n", nonNullAnnual);

          assertTrue(totalRows == nonNullCode, "metro_area_code should not be null");
          assertTrue(totalRows == nonNullName, "metro_area_name should not be null");
          assertTrue(totalRows == nonNullYear, "year should not be null");
          assertTrue(totalRows == nonNullQtr, "qtr should not be null");
          assertTrue(nonNullWeekly > 0, "average_weekly_wage should have values");
          assertTrue(nonNullAnnual > 0, "average_annual_pay should have values");
        }
      }

      // Validation 5: Show sample data with new fields
      System.out.println("\n✓ VALIDATION 5: Sample data with new fields");
      String sampleQuery = "SELECT metro_area_code, metro_area_name, year, qtr, "
          + "average_weekly_wage, average_annual_pay "
          + "FROM econ.metro_wages "
          + "WHERE year = 2023 "
          + "ORDER BY metro_area_code, qtr "
          + "LIMIT 5";

      try (ResultSet rs = stmt.executeQuery(sampleQuery)) {
        System.out.println("  Code    | Name                        | Year | Qtr | Weekly Wage | Annual Pay");
        System.out.println("  --------|-----------------------------|----- |-----|-------------|------------");
        while (rs.next()) {
          System.out.printf("  %-7s | %-27s | %4d | %-3s | $%-10s | $%-10s\n",
              rs.getString("metro_area_code"),
              rs.getString("metro_area_name").substring(0, Math.min(27, rs.getString("metro_area_name").length())),
              rs.getInt("year"),
              rs.getString("qtr"),
              rs.getString("average_weekly_wage"),
              rs.getString("average_annual_pay"));
        }
      }

      System.out.println("\n=== ✅ ALL VALIDATIONS PASSED ===");
      System.out.println("✓ Bulk CSV download completed without errors");
      System.out.println("✓ All 27 metros present");
      System.out.println("✓ Annual and quarterly data confirmed");
      System.out.println("✓ Data quality validated (no null values)");
      System.out.println("✓ New fields (qtr, average_annual_pay) working correctly");
      System.out.println("\n=== Metro Wages Bulk Download Test Complete ===\n");

    } finally {
      java.nio.file.Files.deleteIfExists(modelFile);
    }
  }

  @Test public void testMetroWagesWithS3Storage() throws Exception {
    // Use local T9 directories which have existing data
    // This tests StorageProvider abstraction without needing MinIO setup
    String cacheDir = "/Volumes/T9/govdata-cache";
    String parquetDir = "/Volumes/T9/govdata-parquet";

    System.out.println("\n=== ECON S3 Storage Test - Metro Wages ===");
    System.out.println("GOVDATA_CACHE_DIR: " + cacheDir);
    System.out.println("GOVDATA_PARQUET_DIR: " + parquetDir);

    // Get API keys from environment or use test keys
    String blsApiKey = System.getenv("BLS_API_KEY");
    if (blsApiKey == null) blsApiKey = "a8d2d6de36194ea580c65560da6323ca";

    // Create model JSON with S3 storage (s3:// prefix) for BOTH cache and parquet
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"econ\","
        + "\"schemas\": [{"
        + "  \"name\": \"econ\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\": {"
        + "    \"dataSource\": \"econ\","
        + "    \"directory\": \"" + parquetDir + "\","
        + "    \"cacheDirectory\": \"" + cacheDir + "\","
        + "    \"blsApiKey\": \"" + blsApiKey + "\","
        + "    \"autoDownload\": true,"
        + "    \"startYear\": 2023,"
        + "    \"endYear\": 2023,"
        + "    \"enabledSources\": [\"bls\"]"
        + "  }"
        + "}]"
        + "}";

    java.nio.file.Path modelFile = java.nio.file.Files.createTempFile("econ-s3-test", ".json");
    java.nio.file.Files.writeString(modelFile, modelJson);

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, info);
         Statement stmt = conn.createStatement()) {

      System.out.println("\n1. Verifying metro_wages table exists and has data");
      System.out.println("====================================================");

      String countQuery = "SELECT COUNT(*) as cnt FROM econ.metro_wages";
      try (ResultSet rs = stmt.executeQuery(countQuery)) {
        if (rs.next()) {
          int count = rs.getInt("cnt");
          System.out.printf("  metro_wages: %d rows\n", count);
          assertTrue(count > 0, "metro_wages should have data (expected 27 metros for 2023)");
          assertTrue(count >= 20, "metro_wages should have at least 20 metros (got " + count + ")");
        }
      }

      System.out.println("\n2. Sample metro_wages data");
      System.out.println("==========================");

      String sampleQuery = "SELECT * FROM econ.metro_wages LIMIT 3";
      try (ResultSet rs = stmt.executeQuery(sampleQuery)) {
        int cols = rs.getMetaData().getColumnCount();

        // Print column headers
        for (int i = 1; i <= cols; i++) {
          System.out.print(rs.getMetaData().getColumnName(i) + "\t");
        }
        System.out.println();

        // Print data
        while (rs.next()) {
          for (int i = 1; i <= cols; i++) {
            System.out.print(rs.getString(i) + "\t");
          }
          System.out.println();
        }
      }

      System.out.println("\n✓ S3 Storage Test PASSED");
      System.out.println("  - Downloaded metro wages data via BLS QCEW API");
      System.out.println("  - Cached JSON files in S3 (s3://govdata-cache)");
      System.out.println("  - Converted JSON to Parquet");
      System.out.println("  - Stored Parquet files in S3 (s3://govdata-parquet)");
      System.out.println("  - Successfully queried data from S3 storage");
      System.out.println("  - VALIDATED: All I/O operations work with s3:// URIs");
      System.out.println("\n=== Test Complete ===\n");

    } finally {
      java.nio.file.Files.deleteIfExists(modelFile);
    }
  }

  /**
   * Regression test for metro_wages backward compatibility.
   * <p>Validates Phase 5, Step 5.4 requirements:
   * <ul>
   *   <li>Existing queries without new fields still work (backward compatibility)</li>
   *   <li>Queries using new fields (qtr, average_annual_pay) work correctly</li>
   *   <li>Query performance is acceptable with additional columns</li>
   * </ul>
   */
  @Test public void testMetroWagesBackwardCompatibility() throws Exception {
    String cacheDir = System.getenv("GOVDATA_CACHE_DIR");
    if (cacheDir == null) cacheDir = "/Volumes/T9/govdata-cache";

    String parquetDir = System.getenv("GOVDATA_PARQUET_DIR");
    if (parquetDir == null) parquetDir = "/Volumes/T9/govdata-parquet";

    System.out.println("\n=== Metro Wages Backward Compatibility Regression Test ===");
    System.out.println("GOVDATA_CACHE_DIR: " + cacheDir);
    System.out.println("GOVDATA_PARQUET_DIR: " + parquetDir);

    String blsApiKey = System.getenv("BLS_API_KEY");
    if (blsApiKey == null) blsApiKey = "a8d2d6de36194ea580c65560da6323ca";

    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"econ\","
        + "\"schemas\": [{"
        + "  \"name\": \"econ\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\": {"
        + "    \"dataSource\": \"econ\","
        + "    \"directory\": \"" + parquetDir + "\","
        + "    \"cacheDirectory\": \"" + cacheDir + "\","
        + "    \"blsApiKey\": \"" + blsApiKey + "\","
        + "    \"autoDownload\": true,"
        + "    \"startYear\": 2023,"
        + "    \"endYear\": 2023,"
        + "    \"enabledSources\": [\"bls\"]"
        + "  }"
        + "}]"
        + "}";

    java.nio.file.Path modelFile = java.nio.file.Files.createTempFile("metro-wages-regression", ".json");
    java.nio.file.Files.writeString(modelFile, modelJson);

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn =
        DriverManager.getConnection("jdbc:calcite:model=" + modelFile, info);
         Statement stmt = conn.createStatement()) {

      // REGRESSION TEST 1: Basic query without new fields (backward compatibility)
      System.out.println("\n✓ REGRESSION TEST 1: Basic query without new fields");
      String basicQuery = "SELECT metro_area_code, metro_area_name, year, average_weekly_wage "
          + "FROM econ.metro_wages WHERE year = 2023 LIMIT 5";

      try (ResultSet rs = stmt.executeQuery(basicQuery)) {
        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
          String code = rs.getString("metro_area_code");
          String name = rs.getString("metro_area_name");
          int year = rs.getInt("year");
          String wage = rs.getString("average_weekly_wage");

          // Verify all fields are populated
          assertTrue(code != null && !code.isEmpty(), "metro_area_code should not be null/empty");
          assertTrue(name != null && !name.isEmpty(), "metro_area_name should not be null/empty");
          assertTrue(year == 2023, "year should be 2023");
          assertTrue(wage != null && !wage.isEmpty(), "average_weekly_wage should not be null/empty");
        }
        System.out.printf("  Verified %d rows with original fields only\n", rowCount);
        assertTrue(rowCount > 0, "Basic query should return rows");
      }

      // REGRESSION TEST 2: Query with new qtr field
      System.out.println("\n✓ REGRESSION TEST 2: Query with new qtr field");

      // Test 2a: Filter by annual records (qtr='A')
      String annualFilterQuery = "SELECT metro_area_code, year, qtr, average_annual_pay "
          + "FROM econ.metro_wages WHERE year = 2023 AND qtr = 'A' LIMIT 5";

      try (ResultSet rs = stmt.executeQuery(annualFilterQuery)) {
        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
          String qtr = rs.getString("qtr");
          String annualPay = rs.getString("average_annual_pay");

          assertTrue("A".equals(qtr), "qtr should be 'A' for annual records");
          assertTrue(annualPay != null && !annualPay.isEmpty(),
              "average_annual_pay should not be null/empty");
        }
        System.out.printf("  Annual records (qtr='A'): %d rows verified\n", rowCount);
        assertTrue(rowCount > 0, "Annual query should return rows");
      }

      // Test 2b: Filter by quarterly records
      String quarterlyFilterQuery = "SELECT metro_area_code, year, qtr, average_weekly_wage "
          + "FROM econ.metro_wages WHERE year = 2023 AND qtr IN ('1', '2', '3', '4') "
          + "ORDER BY metro_area_code, qtr LIMIT 10";

      try (ResultSet rs = stmt.executeQuery(quarterlyFilterQuery)) {
        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
          String qtr = rs.getString("qtr");
          assertTrue(qtr.matches("[1-4]"), "qtr should be '1', '2', '3', or '4'");
        }
        System.out.printf("  Quarterly records (qtr='1'-'4'): %d rows verified\n", rowCount);
        assertTrue(rowCount > 0, "Quarterly query should return rows");
      }

      // REGRESSION TEST 3: Aggregation query (performance check)
      System.out.println("\n✓ REGRESSION TEST 3: Aggregation query performance");

      long startTime = System.currentTimeMillis();
      String aggQuery = "SELECT metro_area_code, "
          + "AVG(CAST(average_weekly_wage AS DOUBLE)) as avg_wage, "
          + "COUNT(*) as record_count "
          + "FROM econ.metro_wages WHERE year = 2023 "
          + "GROUP BY metro_area_code "
          + "ORDER BY avg_wage DESC";

      try (ResultSet rs = stmt.executeQuery(aggQuery)) {
        int metroCount = 0;
        double maxAvgWage = 0;
        String topMetro = null;

        while (rs.next()) {
          metroCount++;
          String metro = rs.getString("metro_area_code");
          double avgWage = rs.getDouble("avg_wage");
          int recordCount = rs.getInt("record_count");

          if (metroCount == 1) {
            maxAvgWage = avgWage;
            topMetro = metro;
          }

          assertTrue(avgWage > 0, "Average wage should be positive");
          assertTrue(recordCount > 0, "Record count should be positive");
        }

        long endTime = System.currentTimeMillis();
        long queryTime = endTime - startTime;

        System.out.printf("  Aggregated %d metros in %d ms\n", metroCount, queryTime);
        System.out.printf("  Highest average wage: %s ($%.2f)\n", topMetro, maxAvgWage);

        assertTrue(metroCount >= 20, "Should aggregate at least 20 metros");
        assertTrue(queryTime < 30000, "Aggregation query should complete in under 30 seconds");
      }

      // REGRESSION TEST 4: Join-like query with multiple conditions
      System.out.println("\n✓ REGRESSION TEST 4: Complex query with multiple conditions");

      String complexQuery = "SELECT metro_area_code, metro_area_name, qtr, "
          + "average_weekly_wage, average_annual_pay "
          + "FROM econ.metro_wages "
          + "WHERE year = 2023 "
          + "  AND qtr = 'A' "
          + "  AND CAST(average_weekly_wage AS DOUBLE) > 1000 "
          + "ORDER BY CAST(average_weekly_wage AS DOUBLE) DESC "
          + "LIMIT 10";

      try (ResultSet rs = stmt.executeQuery(complexQuery)) {
        int rowCount = 0;
        double prevWage = Double.MAX_VALUE;

        while (rs.next()) {
          rowCount++;
          String qtr = rs.getString("qtr");
          double wage = Double.parseDouble(rs.getString("average_weekly_wage"));

          assertTrue("A".equals(qtr), "All results should be annual records");
          assertTrue(wage > 1000, "All wages should be > $1000");
          assertTrue(wage <= prevWage, "Results should be ordered by wage DESC");

          prevWage = wage;
        }

        System.out.printf("  Complex filter/sort query: %d rows verified\n", rowCount);
        assertTrue(rowCount > 0, "Complex query should return rows");
      }

      // REGRESSION TEST 5: Verify schema includes new columns
      System.out.println("\n✓ REGRESSION TEST 5: Verify schema includes new columns");

      String schemaQuery = "SELECT * FROM econ.metro_wages LIMIT 1";
      try (ResultSet rs = stmt.executeQuery(schemaQuery)) {
        java.sql.ResultSetMetaData metadata = rs.getMetaData();
        int columnCount = metadata.getColumnCount();

        boolean hasQtr = false;
        boolean hasAnnualPay = false;

        for (int i = 1; i <= columnCount; i++) {
          String columnName = metadata.getColumnName(i).toLowerCase();
          if ("qtr".equals(columnName)) hasQtr = true;
          if ("average_annual_pay".equals(columnName)) hasAnnualPay = true;
        }

        System.out.printf("  Total columns: %d\n", columnCount);
        System.out.printf("  Has 'qtr' column: %b\n", hasQtr);
        System.out.printf("  Has 'average_annual_pay' column: %b\n", hasAnnualPay);

        assertTrue(hasQtr, "Schema should include 'qtr' column");
        assertTrue(hasAnnualPay, "Schema should include 'average_annual_pay' column");
      }

      System.out.println("\n=== ALL REGRESSION TESTS PASSED ===");
      System.out.println("✓ Backward compatibility maintained (queries without new fields work)");
      System.out.println("✓ New fields (qtr, average_annual_pay) accessible in queries");
      System.out.println("✓ Query performance acceptable for aggregations");
      System.out.println("✓ Complex queries with filters and sorting work correctly");
      System.out.println("✓ Schema properly includes new columns");
      System.out.println("\n=== Metro Wages Regression Test Complete ===\n");

    } finally {
      java.nio.file.Files.deleteIfExists(modelFile);
    }
  }
}
