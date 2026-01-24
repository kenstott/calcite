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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for SEC schema with real EDGAR data.
 * Downloads filings and counts rows in all tables.
 */
@Tag("integration")
public class SecIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SecIntegrationTest.class);

  // Test companies
  private static final String APPLE_CIK = "0000320193";
  private static final String BANK_OF_AMERICA_CIK = "0000070858";
  private static final String CATERPILLAR_CIK = "0000018230";

  // Year range
  private static final int START_YEAR = 2021;
  private static final int END_YEAR = 2024;

  @BeforeAll
  public static void setup() {
    TestEnvironmentLoader.ensureLoaded();
    assertNotNull(TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR"),
        "GOVDATA_CACHE_DIR must be set");
    assertNotNull(TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR"),
        "GOVDATA_PARQUET_DIR must be set");
  }

  private Connection createConnection() throws SQLException {
    String cacheDir = TestEnvironmentLoader.getEnv("GOVDATA_CACHE_DIR");
    String parquetDir = TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR");
    String executionEngine = TestEnvironmentLoader.getEnv("CALCITE_EXECUTION_ENGINE");
    if (executionEngine == null || executionEngine.isEmpty()) {
      executionEngine = "DUCKDB";
    }

    // S3 configuration
    String awsAccessKeyId = TestEnvironmentLoader.getEnv("AWS_ACCESS_KEY_ID");
    String awsSecretAccessKey = TestEnvironmentLoader.getEnv("AWS_SECRET_ACCESS_KEY");
    String awsEndpointOverride = TestEnvironmentLoader.getEnv("AWS_ENDPOINT_OVERRIDE");
    String awsRegion = TestEnvironmentLoader.getEnv("AWS_REGION");

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

    String ciksJson = "[\"" + APPLE_CIK + "\", \"" + BANK_OF_AMERICA_CIK + "\", \""
        + CATERPILLAR_CIK + "\"]";

    String modelJson =
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"SEC\","
        + "  \"schemas\": [{"
        + "    \"name\": \"SEC\","
        + "    \"type\": \"custom\","
        + "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "    \"operand\": {"
        + "      \"dataSource\": \"sec\","
        + "      \"executionEngine\": \"" + executionEngine + "\","
        + "      \"database_filename\": \"shared.duckdb\","
        + "      \"ephemeralCache\": false,"
        + "      \"cacheDirectory\": \"" + cacheDir + "\","
        + "      \"directory\": \"" + parquetDir + "\","
        + "      " + s3ConfigJson
        + "      \"startYear\": " + START_YEAR + ","
        + "      \"endYear\": " + END_YEAR + ","
        + "      \"ciks\": " + ciksJson + ","
        + "      \"filingTypes\": [\"10-K\", \"10-Q\", \"8-K\", \"4\"],"
        + "      \"autoDownload\": true,"
        + "      \"textSimilarity\": {\"enabled\": true}"
        + "    }"
        + "  }]"
        + "}";

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + modelJson);

    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  @Test
  void testAllTablesRowCounts() throws SQLException {
    LOGGER.info("=== SEC Integration Test: Table Row Counts ===");
    LOGGER.info("Companies: AAPL, BAC, CAT");
    LOGGER.info("Years: {} - {}", START_YEAR, END_YEAR);
    LOGGER.info("autoDownload: true");
    LOGGER.info("");

    try (Connection conn = createConnection()) {
      // Get all tables in SEC schema
      List<String> tables = new ArrayList<>();
      DatabaseMetaData meta = conn.getMetaData();
      // Use exact schema name as specified in model ("SEC" uppercase)
      // Use null for types to include both TABLEs and VIEWs (DuckDB registers Iceberg as views)
      try (ResultSet rs = meta.getTables(null, "SEC", "%", null)) {
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }
      }

      if (tables.isEmpty()) {
        LOGGER.warn("No tables found in SEC schema");
        return;
      }

      LOGGER.info("Found {} tables in SEC schema:", tables.size());
      LOGGER.info("");
      LOGGER.info(String.format("%-35s %15s", "Table", "Row Count"));
      LOGGER.info(String.format("%-35s %15s", "-".repeat(35), "-".repeat(15)));

      long totalRows = 0;
      int tablesWithData = 0;

      try (Statement stmt = conn.createStatement()) {
        for (String table : tables) {
          try {
            String sql = "SELECT COUNT(*) as cnt FROM \"SEC\".\"" + table + "\"";
            try (ResultSet rs = stmt.executeQuery(sql)) {
              if (rs.next()) {
                long count = rs.getLong("cnt");
                LOGGER.info(String.format("%-35s %,15d", table, count));
                totalRows += count;
                if (count > 0) {
                  tablesWithData++;
                }
              }
            }
          } catch (SQLException e) {
            LOGGER.info(String.format("%-35s %15s", table, "ERROR: " + e.getMessage()));
          }
        }
      }

      LOGGER.info(String.format("%-35s %15s", "-".repeat(35), "-".repeat(15)));
      LOGGER.info(String.format("%-35s %,15d", "TOTAL", totalRows));
      LOGGER.info("");
      LOGGER.info("Tables with data: {}/{}", tablesWithData, tables.size());

      assertTrue(totalRows > 0, "Should have downloaded and processed some data");
    }
  }

  @Test
  void testRecreateVectorizedChunks() throws SQLException {
    LOGGER.info("=== Recreating vectorized_chunks Iceberg table ===");

    String parquetDir = TestEnvironmentLoader.getEnv("GOVDATA_PARQUET_DIR");
    assertNotNull(parquetDir, "GOVDATA_PARQUET_DIR must be set");

    // Build Iceberg catalog config
    Map<String, Object> icebergConfig = new HashMap<>();
    icebergConfig.put("catalog", "hadoop");
    icebergConfig.put("warehouse", parquetDir + "/source=sec/SEC");

    // S3 configuration - both Iceberg S3FileIO and Hadoop S3A settings
    String awsAccessKeyId = TestEnvironmentLoader.getEnv("AWS_ACCESS_KEY_ID");
    String awsSecretAccessKey = TestEnvironmentLoader.getEnv("AWS_SECRET_ACCESS_KEY");
    String awsEndpointOverride = TestEnvironmentLoader.getEnv("AWS_ENDPOINT_OVERRIDE");
    String awsRegion = TestEnvironmentLoader.getEnv("AWS_REGION");

    // Iceberg S3FileIO settings
    if (awsAccessKeyId != null) {
      icebergConfig.put("s3.access-key-id", awsAccessKeyId);
    }
    if (awsSecretAccessKey != null) {
      icebergConfig.put("s3.secret-access-key", awsSecretAccessKey);
    }
    if (awsEndpointOverride != null) {
      icebergConfig.put("s3.endpoint", awsEndpointOverride);
    }
    if (awsRegion != null) {
      icebergConfig.put("s3.region", awsRegion);
    }

    // Hadoop S3A filesystem settings (required for HadoopCatalog with S3)
    // These must be in a nested "hadoopConfig" map as per IcebergCatalogManager
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    hadoopConfig.put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    if (awsAccessKeyId != null) {
      hadoopConfig.put("fs.s3a.access.key", awsAccessKeyId);
    }
    if (awsSecretAccessKey != null) {
      hadoopConfig.put("fs.s3a.secret.key", awsSecretAccessKey);
    }
    if (awsEndpointOverride != null) {
      hadoopConfig.put("fs.s3a.endpoint", awsEndpointOverride);
    }
    hadoopConfig.put("fs.s3a.path.style.access", "true");
    icebergConfig.put("hadoopConfig", hadoopConfig);

    // Drop existing table with purge (deletes data files)
    String tableId = "vectorized_chunks";
    boolean dropped = IcebergCatalogManager.dropTable(icebergConfig, tableId, true);
    LOGGER.info("Dropped vectorized_chunks table: {}", dropped);

    // Now run the main test which will trigger materialization
    LOGGER.info("Running table row counts (will trigger re-materialization)...");
    testAllTablesRowCounts();

    // Verify vectorized_chunks has MDA content
    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {
      String sql = "SELECT source_type, COUNT(*) as cnt FROM \"SEC\".\"vectorized_chunks\" "
          + "GROUP BY source_type ORDER BY source_type";
      LOGGER.info("Verifying vectorized_chunks content:");
      try (ResultSet rs = stmt.executeQuery(sql)) {
        int mdaCount = 0;
        while (rs.next()) {
          String sourceType = rs.getString(1);
          long count = rs.getLong(2);
          LOGGER.info("  {}: {}", sourceType, count);
          if ("mda_paragraph".equals(sourceType)) {
            mdaCount = (int) count;
          }
        }
        assertTrue(mdaCount > 0, "vectorized_chunks should have mda_paragraph entries");
      }
    }
  }
}
