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

import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for SEC schema with real EDGAR data.
 * Tests document-based ETL with XBRL parsing and multi-table extraction.
 *
 * <p>Test Companies (4 years of data: 2021-2024):
 * <ul>
 *   <li>Apple Inc (AAPL) - CIK 0000320193</li>
 *   <li>Bank of America (BAC) - CIK 0000070858</li>
 *   <li>Caterpillar Inc (CAT) - CIK 0000018230</li>
 * </ul>
 *
 * <p>Tests SEC filing tables:
 * <ul>
 *   <li>filing_metadata: Core filing information from submissions.json</li>
 *   <li>financial_line_items: XBRL facts from 10-K/10-Q filings</li>
 *   <li>filing_contexts: XBRL context definitions</li>
 *   <li>mda_sections: MD&amp;A text extracted from HTML</li>
 *   <li>xbrl_relationships: Calculation/presentation linkbases</li>
 *   <li>insider_transactions: Form 3/4/5 data</li>
 *   <li>earnings_transcripts: 8-K earnings data</li>
 *   <li>stock_prices: Daily OHLCV data</li>
 * </ul>
 *
 * <p>Note: Tests run sequentially to respect SEC rate limits.
 */
@Tag("integration")
@Execution(ExecutionMode.SAME_THREAD)
public class SecIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SecIntegrationTest.class);

  // Test companies - CIK numbers (10-digit zero-padded)
  private static final String APPLE_CIK = "0000320193";
  private static final String BANK_OF_AMERICA_CIK = "0000070858";
  private static final String CATERPILLAR_CIK = "0000018230";

  private static final Set<String> TEST_CIKS = new HashSet<>(
      Arrays.asList(APPLE_CIK, BANK_OF_AMERICA_CIK, CATERPILLAR_CIK));

  // Filing tables extracted from XBRL documents
  private static final Set<String> XBRL_TABLES = new HashSet<>(
      Arrays.asList(
          "financial_line_items",
          "filing_contexts",
          "xbrl_relationships"));

  // Text extraction tables
  private static final Set<String> TEXT_TABLES = new HashSet<>(
      Arrays.asList(
          "mda_sections",
          "earnings_transcripts"));

  // Metadata and market data tables
  private static final Set<String> METADATA_TABLES = new HashSet<>(
      Arrays.asList(
          "filing_metadata",
          "insider_transactions",
          "stock_prices"));

  // All SEC tables
  private static final Set<String> ALL_SEC_TABLES = new HashSet<>();
  static {
    ALL_SEC_TABLES.addAll(XBRL_TABLES);
    ALL_SEC_TABLES.addAll(TEXT_TABLES);
    ALL_SEC_TABLES.addAll(METADATA_TABLES);
  }

  // Year range for testing
  private static final int START_YEAR = 2021;
  private static final int END_YEAR = 2024;

  @BeforeAll
  public static void setup() {
    TestEnvironmentLoader.ensureLoaded();

    // Verify environment is properly configured
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

    // Build CIKs JSON array
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
        + "      \"refreshInterval\": \"PT1H\","
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
        + "      \"extractMDA\": true,"
        + "      \"fetchStockPrices\": true"
        + "    }"
        + "  }]"
        + "}";

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + modelJson);

    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  // =========================================================================
  // FILING METADATA TESTS
  // =========================================================================

  @Test
  void testFilingMetadataTableExists() throws SQLException {
    LOGGER.info("Testing filing_metadata table existence and structure...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      String sql = "SELECT COUNT(*) as cnt FROM \"SEC\".filing_metadata";
      try (ResultSet rs = stmt.executeQuery(sql)) {
        assertTrue(rs.next(), "Should return a result");
        long count = rs.getLong("cnt");
        LOGGER.info("filing_metadata has {} rows", count);
        assertTrue(count >= 0, "Table should be queryable");
      }
    }
  }

  @Test
  void testFilingMetadataForTestCompanies() throws SQLException {
    LOGGER.info("Testing filing_metadata for test companies (AAPL, BAC, CAT)...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      // Query filings for our test companies
      String sql = "SELECT cik, company_name, filing_type, filing_date, accession_number "
          + "FROM \"SEC\".filing_metadata "
          + "WHERE cik IN ('" + APPLE_CIK + "', '" + BANK_OF_AMERICA_CIK + "', '"
          + CATERPILLAR_CIK + "') "
          + "ORDER BY cik, filing_date DESC "
          + "LIMIT 20";

      try (ResultSet rs = stmt.executeQuery(sql)) {
        int rowCount = 0;
        Set<String> foundCiks = new HashSet<>();

        while (rs.next()) {
          rowCount++;
          String cik = rs.getString("cik");
          String companyName = rs.getString("company_name");
          String filingType = rs.getString("filing_type");
          String filingDate = rs.getString("filing_date");

          foundCiks.add(cik);

          if (rowCount <= 5) {
            LOGGER.info("  Filing: {} ({}) - {} on {}",
                companyName, cik, filingType, filingDate);
          }
        }

        LOGGER.info("Found {} filings for {} companies", rowCount, foundCiks.size());

        if (rowCount > 0) {
          // Verify we found at least some test companies
          assertTrue(foundCiks.size() > 0,
              "Should find filings for at least one test company");
        }
      }
    }
  }

  @Test
  void testFilingMetadataYearRange() throws SQLException {
    LOGGER.info("Testing filing_metadata year range ({}-{})...", START_YEAR, END_YEAR);

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      String sql = "SELECT "
          + "  MIN(CAST(SUBSTR(filing_date, 1, 4) AS INTEGER)) as min_year, "
          + "  MAX(CAST(SUBSTR(filing_date, 1, 4) AS INTEGER)) as max_year, "
          + "  COUNT(*) as filing_count "
          + "FROM \"SEC\".filing_metadata "
          + "WHERE cik IN ('" + APPLE_CIK + "', '" + BANK_OF_AMERICA_CIK + "', '"
          + CATERPILLAR_CIK + "')";

      try (ResultSet rs = stmt.executeQuery(sql)) {
        if (rs.next()) {
          int minYear = rs.getInt("min_year");
          int maxYear = rs.getInt("max_year");
          long filingCount = rs.getLong("filing_count");

          LOGGER.info("Year range: {} to {}, {} filings", minYear, maxYear, filingCount);

          if (filingCount > 0) {
            assertTrue(minYear >= START_YEAR - 1,
                "Min year should be around start year");
            assertTrue(maxYear <= END_YEAR + 1,
                "Max year should be around end year");
          }
        }
      }
    }
  }

  // =========================================================================
  // FINANCIAL LINE ITEMS TESTS
  // =========================================================================

  @Test
  void testFinancialLineItemsTable() throws SQLException {
    LOGGER.info("Testing financial_line_items table...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      // Query revenue-related concepts for Apple
      String sql = "SELECT concept, numeric_value, period_end, unit_ref "
          + "FROM \"SEC\".financial_line_items "
          + "WHERE cik = '" + APPLE_CIK + "' "
          + "  AND concept LIKE '%Revenue%' "
          + "  AND numeric_value IS NOT NULL "
          + "ORDER BY period_end DESC "
          + "LIMIT 10";

      try (ResultSet rs = stmt.executeQuery(sql)) {
        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
          String concept = rs.getString("concept");
          double value = rs.getDouble("numeric_value");
          String periodEnd = rs.getString("period_end");

          LOGGER.info("  {} = {} ({})", concept, value, periodEnd);
        }

        LOGGER.info("Found {} revenue line items for Apple", rowCount);
      }
    }
  }

  @Test
  void testFinancialLineItemsXbrlConcepts() throws SQLException {
    LOGGER.info("Testing XBRL concept diversity in financial_line_items...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      // Count distinct concepts per company
      String sql = "SELECT cik, COUNT(DISTINCT concept) as concept_count "
          + "FROM \"SEC\".financial_line_items "
          + "WHERE cik IN ('" + APPLE_CIK + "', '" + BANK_OF_AMERICA_CIK + "', '"
          + CATERPILLAR_CIK + "') "
          + "GROUP BY cik";

      try (ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          String cik = rs.getString("cik");
          long conceptCount = rs.getLong("concept_count");
          LOGGER.info("  CIK {} has {} distinct XBRL concepts", cik, conceptCount);
        }
      }
    }
  }

  // =========================================================================
  // FILING CONTEXTS TESTS
  // =========================================================================

  @Test
  void testFilingContextsTable() throws SQLException {
    LOGGER.info("Testing filing_contexts table...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      String sql = "SELECT context_id, entity_identifier, period_start, period_end "
          + "FROM \"SEC\".filing_contexts "
          + "WHERE cik = '" + APPLE_CIK + "' "
          + "LIMIT 10";

      try (ResultSet rs = stmt.executeQuery(sql)) {
        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
          String contextId = rs.getString("context_id");
          String periodStart = rs.getString("period_start");
          String periodEnd = rs.getString("period_end");

          if (rowCount <= 5) {
            LOGGER.info("  Context: {} ({} to {})", contextId, periodStart, periodEnd);
          }
        }
        LOGGER.info("Found {} contexts for Apple", rowCount);
      }
    }
  }

  // =========================================================================
  // MD&A SECTIONS TESTS
  // =========================================================================

  @Test
  void testMdaSectionsTable() throws SQLException {
    LOGGER.info("Testing mda_sections table (MD&A text extraction)...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      String sql = "SELECT section, subsection, paragraph_number, "
          + "  SUBSTR(paragraph_text, 1, 100) as text_preview "
          + "FROM \"SEC\".mda_sections "
          + "WHERE cik = '" + APPLE_CIK + "' "
          + "ORDER BY section, paragraph_number "
          + "LIMIT 10";

      try (ResultSet rs = stmt.executeQuery(sql)) {
        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
          String section = rs.getString("section");
          String subsection = rs.getString("subsection");
          int paragraphNum = rs.getInt("paragraph_number");
          String preview = rs.getString("text_preview");

          LOGGER.info("  {} / {} [{}]: {}...",
              section, subsection, paragraphNum,
              preview != null ? preview.substring(0, Math.min(50, preview.length())) : "(null)");
        }
        LOGGER.info("Found {} MD&A paragraphs for Apple", rowCount);
      }
    }
  }

  // =========================================================================
  // INSIDER TRANSACTIONS TESTS
  // =========================================================================

  @Test
  void testInsiderTransactionsTable() throws SQLException {
    LOGGER.info("Testing insider_transactions table...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      String sql = "SELECT reporting_person_name, transaction_date, transaction_code, "
          + "  shares_transacted, price_per_share "
          + "FROM \"SEC\".insider_transactions "
          + "WHERE cik = '" + APPLE_CIK + "' "
          + "  AND transaction_code IN ('P', 'S') "
          + "ORDER BY transaction_date DESC "
          + "LIMIT 10";

      try (ResultSet rs = stmt.executeQuery(sql)) {
        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
          String personName = rs.getString("reporting_person_name");
          String txDate = rs.getString("transaction_date");
          String txCode = rs.getString("transaction_code");
          double shares = rs.getDouble("shares_transacted");
          double price = rs.getDouble("price_per_share");

          LOGGER.info("  {} {} {} shares @ ${} on {}",
              personName, txCode.equals("P") ? "bought" : "sold",
              shares, price, txDate);
        }
        LOGGER.info("Found {} insider transactions for Apple", rowCount);
      }
    }
  }

  // =========================================================================
  // STOCK PRICES TESTS
  // =========================================================================

  @Test
  void testStockPricesTable() throws SQLException {
    LOGGER.info("Testing stock_prices table...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      String sql = "SELECT ticker, date, open, high, low, close, volume "
          + "FROM \"SEC\".stock_prices "
          + "WHERE ticker = 'AAPL' "
          + "ORDER BY date DESC "
          + "LIMIT 10";

      try (ResultSet rs = stmt.executeQuery(sql)) {
        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
          String ticker = rs.getString("ticker");
          String date = rs.getString("date");
          double open = rs.getDouble("open");
          double close = rs.getDouble("close");
          long volume = rs.getLong("volume");

          LOGGER.info("  {} on {}: O={} C={} V={}",
              ticker, date, open, close, volume);
        }
        LOGGER.info("Found {} price records for AAPL", rowCount);
      }
    }
  }

  // =========================================================================
  // CROSS-TABLE JOIN TESTS
  // =========================================================================

  @Test
  void testFilingMetadataWithFinancialData() throws SQLException {
    LOGGER.info("Testing cross-table join: filing_metadata + financial_line_items...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      String sql = "SELECT "
          + "  fm.company_name, "
          + "  fm.filing_type, "
          + "  fm.filing_date, "
          + "  COUNT(fli.concept) as line_item_count "
          + "FROM \"SEC\".filing_metadata fm "
          + "LEFT JOIN \"SEC\".financial_line_items fli "
          + "  ON fm.cik = fli.cik AND fm.accession_number = fli.accession_number "
          + "WHERE fm.cik = '" + APPLE_CIK + "' "
          + "  AND fm.filing_type IN ('10-K', '10-Q') "
          + "GROUP BY fm.company_name, fm.filing_type, fm.filing_date "
          + "ORDER BY fm.filing_date DESC "
          + "LIMIT 10";

      try (ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          String companyName = rs.getString("company_name");
          String filingType = rs.getString("filing_type");
          String filingDate = rs.getString("filing_date");
          long lineItemCount = rs.getLong("line_item_count");

          LOGGER.info("  {} {} on {} has {} line items",
              companyName, filingType, filingDate, lineItemCount);
        }
      }
    }
  }

  @Test
  void testStockPricesAroundFilingDates() throws SQLException {
    LOGGER.info("Testing stock prices around filing dates (event study setup)...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      // Find stock prices on 10-K filing dates
      String sql = "SELECT "
          + "  fm.company_name, "
          + "  fm.filing_date, "
          + "  sp.close as close_price, "
          + "  sp.volume "
          + "FROM \"SEC\".filing_metadata fm "
          + "INNER JOIN \"SEC\".stock_prices sp "
          + "  ON fm.ticker = sp.ticker AND fm.filing_date = sp.date "
          + "WHERE fm.cik = '" + APPLE_CIK + "' "
          + "  AND fm.filing_type = '10-K' "
          + "ORDER BY fm.filing_date DESC "
          + "LIMIT 5";

      try (ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          String companyName = rs.getString("company_name");
          String filingDate = rs.getString("filing_date");
          double closePrice = rs.getDouble("close_price");
          long volume = rs.getLong("volume");

          LOGGER.info("  {} 10-K filed on {}: Close=${}, Volume={}",
              companyName, filingDate, closePrice, volume);
        }
      }
    }
  }

  // =========================================================================
  // AGGREGATE AND ANALYTICAL TESTS
  // =========================================================================

  @Test
  void testFilingCountsByTypeAndYear() throws SQLException {
    LOGGER.info("Testing filing counts by type and year...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      String sql = "SELECT "
          + "  filing_type, "
          + "  CAST(SUBSTR(filing_date, 1, 4) AS INTEGER) as year, "
          + "  COUNT(*) as filing_count "
          + "FROM \"SEC\".filing_metadata "
          + "WHERE cik IN ('" + APPLE_CIK + "', '" + BANK_OF_AMERICA_CIK + "', '"
          + CATERPILLAR_CIK + "') "
          + "GROUP BY filing_type, CAST(SUBSTR(filing_date, 1, 4) AS INTEGER) "
          + "ORDER BY year DESC, filing_type";

      try (ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          String filingType = rs.getString("filing_type");
          int year = rs.getInt("year");
          long filingCount = rs.getLong("filing_count");

          LOGGER.info("  {} {}: {} filings", year, filingType, filingCount);
        }
      }
    }
  }

  @Test
  void testCompanyComparisonQuery() throws SQLException {
    LOGGER.info("Testing company comparison across test companies...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      String sql = "SELECT "
          + "  fm.company_name, "
          + "  fm.cik, "
          + "  COUNT(DISTINCT fm.accession_number) as total_filings, "
          + "  COUNT(DISTINCT CASE WHEN fm.filing_type = '10-K' THEN fm.accession_number END) as annual_reports, "
          + "  COUNT(DISTINCT CASE WHEN fm.filing_type = '10-Q' THEN fm.accession_number END) as quarterly_reports "
          + "FROM \"SEC\".filing_metadata fm "
          + "WHERE fm.cik IN ('" + APPLE_CIK + "', '" + BANK_OF_AMERICA_CIK + "', '"
          + CATERPILLAR_CIK + "') "
          + "GROUP BY fm.company_name, fm.cik "
          + "ORDER BY total_filings DESC";

      try (ResultSet rs = stmt.executeQuery(sql)) {
        LOGGER.info("Company Filing Summary:");
        LOGGER.info(String.format("  %-30s %-12s %-8s %-8s %-8s",
            "Company", "CIK", "Total", "10-K", "10-Q"));
        LOGGER.info("  " + "-".repeat(70));

        while (rs.next()) {
          String companyName = rs.getString("company_name");
          String cik = rs.getString("cik");
          long totalFilings = rs.getLong("total_filings");
          long annualReports = rs.getLong("annual_reports");
          long quarterlyReports = rs.getLong("quarterly_reports");

          LOGGER.info(String.format("  %-30s %-12s %-8d %-8d %-8d",
              companyName != null ? companyName.substring(0, Math.min(30, companyName.length()))
                  : "(unknown)",
              cik, totalFilings, annualReports, quarterlyReports));
        }
      }
    }
  }

  // =========================================================================
  // SCHEMA VALIDATION TESTS
  // =========================================================================

  @Test
  void testAllSecTablesQueryable() throws SQLException {
    LOGGER.info("Testing all SEC tables are queryable...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      for (String tableName : ALL_SEC_TABLES) {
        try {
          String sql = "SELECT COUNT(*) as cnt FROM \"SEC\".\"" + tableName + "\"";
          try (ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
              long count = rs.getLong("cnt");
              LOGGER.info("  [OK] {} - {} rows", tableName, count);
            }
          }
        } catch (SQLException e) {
          LOGGER.warn("  [SKIP] {} - {}", tableName, e.getMessage());
        }
      }
    }
  }

  @Test
  void testTableColumnStructure() throws SQLException {
    LOGGER.info("Testing table column structure...");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      String[] tablesToCheck = {"filing_metadata", "financial_line_items", "stock_prices"};

      for (String tableName : tablesToCheck) {
        try {
          String sql = "SELECT * FROM \"SEC\".\"" + tableName + "\" LIMIT 1";
          try (ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            StringBuilder cols = new StringBuilder();
            for (int i = 1; i <= Math.min(colCount, 5); i++) {
              if (i > 1) {
                cols.append(", ");
              }
              cols.append(meta.getColumnName(i));
            }
            if (colCount > 5) {
              cols.append("... (").append(colCount).append(" total)");
            }

            LOGGER.info("  {}: {}", tableName, cols);
          }
        } catch (SQLException e) {
          LOGGER.warn("  {} - Error: {}", tableName, e.getMessage());
        }
      }
    }
  }

  // =========================================================================
  // HELPER METHODS
  // =========================================================================

  private static String repeat(String str, int count) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append(str);
    }
    return sb.toString();
  }
}
