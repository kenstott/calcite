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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Queries Apple (CIK 0000320193) filing data from Cloudflare R2 materialized S3 data
 * via the govdata Calcite adapter with autoDownload=false.
 *
 * <p>Bootstraps DuckDB with iceberg_scan views for existing S3 Iceberg tables
 * (currently filing_metadata) before creating the Calcite connection.
 * financial_line_items will be available once ETL workers finish materializing it.
 */
@Tag("integration")
public class AaplRevenueR2IntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(AaplRevenueR2IntegrationTest.class);

  private static final String R2_ACCESS_KEY = "d4dc1320014b34fef4be78f0b126e63b";
  private static final String R2_SECRET_KEY =
      "7c2a89d9f0f976116a69654026c9c47a33e8c3af2293f83d8204b1cf92a2e577";
  private static final String R2_ENDPOINT =
      "https://21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com";
  private static final String R2_ENDPOINT_HOST =
      "21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com";
  private static final String R2_PARQUET_DIR = "s3://govdata-parquet-v1";
  private static final String R2_CACHE_DIR = "s3://govdata-raw-v1";
  private static final String APPLE_CIK = "0000320193";

  private static final String FILING_METADATA_BASE =
      "s3://govdata-parquet-v1/source=sec/SEC/filing_metadata";

  /**
   * Pre-populate the shared DuckDB file with iceberg_scan views for tables that
   * exist in S3. This runs before Calcite connections are opened so that the
   * DuckDB JDBC schema discovers these views on startup.
   *
   * <p>DuckDB's iceberg_scan requires the path to include the metadata version file
   * (e.g., metadata/v3.metadata.json) because version-hint.text is in the metadata/
   * subdirectory, not at the table root. We read the version hint dynamically.
   */
  @BeforeAll
  static void bootstrapDuckDBViews() throws Exception {
    File workingDir = new File(System.getProperty("user.dir"));
    File duckdbDir = new File(workingDir, ".aperio/.duckdb");
    duckdbDir.mkdirs();
    File dbFile = new File(duckdbDir, "shared.duckdb");

    LOGGER.info("Bootstrapping DuckDB at: {}", dbFile.getAbsolutePath());

    Class.forName("org.duckdb.DuckDBDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + dbFile.getAbsolutePath())) {
      // Install extensions
      try { conn.createStatement().execute("INSTALL httpfs"); } catch (Exception ignored) { }
      conn.createStatement().execute("LOAD httpfs");
      try { conn.createStatement().execute("INSTALL iceberg"); } catch (Exception ignored) { }
      conn.createStatement().execute("LOAD iceberg");

      // Configure S3 for R2 (PERSISTENT so it survives connection reuse)
      conn.createStatement().execute(
          "CREATE OR REPLACE PERSISTENT SECRET duckdb_s3_secret ("
          + "TYPE s3, PROVIDER config, "
          + "KEY_ID '" + R2_ACCESS_KEY + "', "
          + "SECRET '" + R2_SECRET_KEY + "', "
          + "ENDPOINT '" + R2_ENDPOINT_HOST + "', "
          + "URL_STYLE 'path', "
          + "USE_SSL true"
          + ")");

      // Read version hint to find the latest Iceberg metadata version
      String versionHintPath = FILING_METADATA_BASE + "/metadata/version-hint.text";
      String version;
      try (ResultSet rs = conn.createStatement().executeQuery(
          "SELECT trim(content) FROM read_text('" + versionHintPath + "')")) {
        assertTrue(rs.next(), "version-hint.text must exist at " + versionHintPath);
        version = rs.getString(1);
      }
      LOGGER.info("filing_metadata Iceberg version: {}", version);

      String metadataPath = FILING_METADATA_BASE + "/metadata/v" + version + ".metadata.json";

      // Create DuckDB schema to match Calcite schema name
      conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS sec");

      // Bootstrap filing_metadata Iceberg view
      conn.createStatement().execute(
          "CREATE OR REPLACE VIEW sec.filing_metadata AS "
          + "SELECT * FROM iceberg_scan('" + metadataPath + "')");

      // Verify
      try (ResultSet rs = conn.createStatement().executeQuery(
          "SELECT COUNT(*) FROM sec.filing_metadata WHERE cik = '" + APPLE_CIK + "'")) {
        rs.next();
        LOGGER.info("Bootstrap complete: {} Apple rows in filing_metadata", rs.getLong(1));
      }
    }
  }

  private Connection createConnection() throws Exception {
    String modelJson =
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"sec\","
        + "  \"schemas\": [{"
        + "    \"name\": \"sec\","
        + "    \"type\": \"custom\","
        + "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "    \"operand\": {"
        + "      \"dataSource\": \"sec\","
        + "      \"executionEngine\": \"DUCKDB\","
        + "      \"database_filename\": \"shared.duckdb\","
        + "      \"ephemeralCache\": false,"
        + "      \"ciks\": [\"" + APPLE_CIK + "\"],"
        + "      \"filingTypes\": [\"10-K\", \"10-Q\", \"8-K\"],"
        + "      \"startYear\": 2020,"
        + "      \"endYear\": 2025,"
        + "      \"autoDownload\": false,"
        + "      \"directory\": \"" + R2_PARQUET_DIR + "\","
        + "      \"cacheDirectory\": \"" + R2_CACHE_DIR + "\","
        + "      \"s3Config\": {"
        + "        \"accessKeyId\": \"" + R2_ACCESS_KEY + "\","
        + "        \"secretAccessKey\": \"" + R2_SECRET_KEY + "\","
        + "        \"endpoint\": \"" + R2_ENDPOINT + "\""
        + "      }"
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
  void testAppleFilings() throws Exception {
    LOGGER.info("=== Apple Filings via govdata Calcite adapter (R2 S3) ===");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      String sql =
          "SELECT cik, company_name, ticker, filing_type, filing_date, period_of_report"
          + " FROM \"sec\".\"filing_metadata\""
          + " WHERE cik = '" + APPLE_CIK + "'"
          + " ORDER BY filing_date";

      LOGGER.info("Executing: {}", sql);

      int rowCount = 0;
      try (ResultSet rs = stmt.executeQuery(sql)) {
        java.sql.ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= cols; i++) {
          header.append(meta.getColumnName(i)).append("\t");
        }
        LOGGER.info("{}", header);
        LOGGER.info("------------------------------------------------------");
        while (rs.next()) {
          StringBuilder row = new StringBuilder();
          for (int i = 1; i <= cols; i++) {
            row.append(rs.getString(i)).append("\t");
          }
          LOGGER.info("{}", row);
          rowCount++;
        }
      }

      LOGGER.info("Total Apple filings in filing_metadata: {}", rowCount);
      assertTrue(rowCount > 0, "Expected filing_metadata rows for AAPL from R2");
    }
  }

  @Test
  void testAvailableTables() throws Exception {
    LOGGER.info("=== Available tables in SEC schema (R2 S3) ===");

    try (Connection conn = createConnection()) {
      java.sql.DatabaseMetaData meta = conn.getMetaData();
      try (ResultSet tables = meta.getTables(null, "sec", "%", new String[]{"TABLE"})) {
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          LOGGER.info("Table: {}", tableName);
        }
      }
    }
  }
}
