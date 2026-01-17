package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that cached tables are discovered quickly when connecting with autoDownload=true.
 * Uses all 4 schemas (GEO, CENSUS, ECON, ECON_REFERENCE) for comprehensive testing.
 */
@Tag("integration")
public class CacheDiscoveryTimingTest {

  @Test
  void testCacheDiscoveryUnderOneMinute() throws Exception {
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

    String awsAccessKeyId = TestEnvironmentLoader.getEnv("AWS_ACCESS_KEY_ID");
    String awsSecretAccessKey = TestEnvironmentLoader.getEnv("AWS_SECRET_ACCESS_KEY");
    String awsEndpointOverride = TestEnvironmentLoader.getEnv("AWS_ENDPOINT_OVERRIDE");
    String awsRegion = TestEnvironmentLoader.getEnv("AWS_REGION");

    assertNotNull(parquetDir, "GOVDATA_PARQUET_DIR must be set");

    System.out.println("=== Cache Discovery Timing Test ===");
    System.out.println("Parquet Dir: " + parquetDir);
    System.out.println();

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
      if (s3Config.charAt(s3Config.length() - 1) == ',') {
        s3Config.setLength(s3Config.length() - 1);
      }
      s3Config.append("},");
      s3ConfigJson = s3Config.toString();
    }

    // Build model with all 4 schemas using shared.duckdb (persistent database)
    String model =
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
    props.setProperty("model", "inline:" + model);

    long startTime = System.currentTimeMillis();

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      long connectTime = System.currentTimeMillis() - startTime;
      System.out.println("Connection established in: " + connectTime + "ms");

      DatabaseMetaData meta = conn.getMetaData();

      // Discover tables in all schemas
      int totalTables = 0;

      long tableStartTime = System.currentTimeMillis();
      List<String> econTables = new ArrayList<>();
      try (ResultSet rs = meta.getTables(null, "ECON", "%", null)) {
        while (rs.next()) {
          econTables.add(rs.getString("TABLE_NAME"));
        }
      }
      long tableTime = System.currentTimeMillis() - tableStartTime;
      System.out.println("ECON tables discovered (" + econTables.size() + "): " + tableTime + "ms");
      totalTables += econTables.size();

      tableStartTime = System.currentTimeMillis();
      List<String> refTables = new ArrayList<>();
      try (ResultSet rs = meta.getTables(null, "ECON_REFERENCE", "%", null)) {
        while (rs.next()) {
          refTables.add(rs.getString("TABLE_NAME"));
        }
      }
      tableTime = System.currentTimeMillis() - tableStartTime;
      System.out.println("ECON_REFERENCE tables discovered (" + refTables.size() + "): " + tableTime + "ms");
      totalTables += refTables.size();

      tableStartTime = System.currentTimeMillis();
      List<String> geoTables = new ArrayList<>();
      try (ResultSet rs = meta.getTables(null, "GEO", "%", null)) {
        while (rs.next()) {
          geoTables.add(rs.getString("TABLE_NAME"));
        }
      }
      tableTime = System.currentTimeMillis() - tableStartTime;
      System.out.println("GEO tables discovered (" + geoTables.size() + "): " + tableTime + "ms");
      totalTables += geoTables.size();

      tableStartTime = System.currentTimeMillis();
      List<String> censusTables = new ArrayList<>();
      try (ResultSet rs = meta.getTables(null, "CENSUS", "%", null)) {
        while (rs.next()) {
          censusTables.add(rs.getString("TABLE_NAME"));
        }
      }
      tableTime = System.currentTimeMillis() - tableStartTime;
      System.out.println("CENSUS tables discovered (" + censusTables.size() + "): " + tableTime + "ms");
      totalTables += censusTables.size();

      System.out.println();
      System.out.println("Total tables discovered: " + totalTables);
      System.out.println();
      System.out.println("Testing queries...");

      try (Statement stmt = conn.createStatement()) {
        long queryStart = System.currentTimeMillis();
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"ECON_REFERENCE\".\"jolts_industries\"")) {
          rs.next();
          long count = rs.getLong(1);
          System.out.println("  ECON_REFERENCE.jolts_industries: " + count + " rows (" + (System.currentTimeMillis() - queryStart) + "ms)");
        }

        queryStart = System.currentTimeMillis();
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"ECON\".\"employment_statistics\"")) {
          rs.next();
          long count = rs.getLong(1);
          System.out.println("  ECON.employment_statistics: " + count + " rows (" + (System.currentTimeMillis() - queryStart) + "ms)");
        }

        queryStart = System.currentTimeMillis();
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"GEO\".\"states\"")) {
          rs.next();
          long count = rs.getLong(1);
          System.out.println("  GEO.states: " + count + " rows (" + (System.currentTimeMillis() - queryStart) + "ms)");
        }

        queryStart = System.currentTimeMillis();
        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"CENSUS\".\"acs_population\"")) {
          rs.next();
          long count = rs.getLong(1);
          System.out.println("  CENSUS.acs_population: " + count + " rows (" + (System.currentTimeMillis() - queryStart) + "ms)");
        }
      }

      long totalTime = System.currentTimeMillis() - startTime;
      System.out.println();
      System.out.println("=== TOTAL TIME: " + totalTime + "ms ===");

      // Assertions - expect 106 tables across all schemas
      assertTrue(econTables.size() >= 20, "Expected at least 20 ECON tables, found: " + econTables.size());
      assertTrue(refTables.size() >= 5, "Expected at least 5 ECON_REFERENCE tables, found: " + refTables.size());
      assertTrue(geoTables.size() >= 25, "Expected at least 25 GEO tables, found: " + geoTables.size());
      assertTrue(censusTables.size() >= 30, "Expected at least 30 CENSUS tables, found: " + censusTables.size());
      assertTrue(totalTables >= 100, "Expected at least 100 total tables, found: " + totalTables);
      assertTrue(totalTime < 60000, "Total time should be under 60 seconds, was: " + totalTime + "ms");

      System.out.println("Result: PASS (under 1 minute with " + totalTables + " tables)");
    }
  }
}
