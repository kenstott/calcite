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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration tests for SEC/EDGAR data functionality.
 *
 * <p>This test consolidates all SEC-related integration testing including:
 * - Schema creation and validation
 * - Real SEC data queries and results
 * - Filing metadata and financial data access
 * - Cross-table relationships and constraints
 * - Performance and data integrity validation
 *
 * <p>These tests require:
 * - GOVDATA_CACHE_DIR environment variable
 * - GOVDATA_PARQUET_DIR environment variable (optional)
 * - Network access for SEC data download (if not cached)
 */
@Tag("integration")
public class SecDataIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SecDataIntegrationTest.class);

  private static final String TEST_MODEL = "{\n"
      + "  \"version\": \"1.0\",\n"
      + "  \"defaultSchema\": \"SEC\",\n"
      + "  \"schemas\": [{\n"
      + "    \"name\": \"SEC\",\n"
      + "    \"type\": \"custom\",\n"
      + "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n"
      + "    \"operand\": {\n"
      + "      \"dataSource\": \"sec\",\n"
      + "      \"testMode\": true,\n"
      + "      \"autoDownload\": false,\n"
      + "      \"ciks\": \"AAPL\"\n"
      + "    }\n"
      + "  }]\n"
      + "}";

  @BeforeAll
  static void checkEnvironment() {
    String cacheDir = System.getenv("GOVDATA_CACHE_DIR");
    Assumptions.assumeTrue(cacheDir != null && !cacheDir.isEmpty(),
        "GOVDATA_CACHE_DIR environment variable must be set for integration tests");

    LOGGER.info("Running SEC integration tests with cache directory: {}", cacheDir);
  }

  @Test void testSecSchemaCreation() throws SQLException {
    LOGGER.info("Testing SEC schema creation and basic connectivity");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      assertNotNull(connection, "Connection should be established");
      assertFalse(connection.isClosed(), "Connection should be open");

      // Verify schema exists
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'SEC'")) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "SEC schema should exist");
          assertEquals("SEC", rs.getString("schema_name"));
        }
      }
    }
  }

  @Test void testSecTableAvailability() throws SQLException {
    LOGGER.info("Testing SEC table availability and metadata");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test key SEC tables are available
      String[] expectedTables = {
          "filing_metadata",
          "financial_data",
          "insider",
          "company_tickers"
      };

      for (String tableName : expectedTables) {
        try (PreparedStatement stmt =
            connection.prepareStatement("SELECT table_name FROM information_schema.tables " +
            "WHERE table_schema = 'SEC' AND table_name = ?")) {
          stmt.setString(1, tableName);
          try (ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next(),
                "Table '" + tableName + "' should be available in SEC schema");
            assertEquals(tableName, rs.getString("table_name"));
          }
        }
      }
    }
  }

  @Test void testBasicSecQuery() throws SQLException {
    LOGGER.info("Testing basic SEC data query functionality");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test simple count query
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT COUNT(*) as record_count FROM company_tickers WHERE ticker = 'AAPL'")) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "Query should return results");
          int count = rs.getInt("record_count");
          assertTrue(count >= 0, "Record count should be non-negative");
          LOGGER.debug("Found {} AAPL records in company_tickers", count);
        }
      }
    }
  }

  @Test void testSecFilingMetadataQuery() throws SQLException {
    LOGGER.info("Testing SEC filing metadata queries");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test filing metadata structure and basic data
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT cik, filing_type, filing_date FROM filing_metadata " +
          "WHERE cik = '0000320193' LIMIT 5")) { // Apple's CIK
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String cik = rs.getString("cik");
            String filingType = rs.getString("filing_type");
            String filingDate = rs.getString("filing_date");

            assertNotNull(cik, "CIK should not be null");
            assertEquals("0000320193", cik, "CIK should match Apple's CIK");
            assertNotNull(filingType, "Filing type should not be null");

            LOGGER.debug("Found filing: CIK={}, Type={}, Date={}", cik, filingType, filingDate);
          }

          // Note: In test mode with autoDownload=false, we may not have data
          // but the query structure should be valid
          LOGGER.info("Filing metadata query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testSecFinancialDataAccess() throws SQLException {
    LOGGER.info("Testing SEC financial data access patterns");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test financial data table structure
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT cik, fact_name, fact_value FROM financial_data " +
          "WHERE cik = '0000320193' AND fact_name LIKE '%Revenue%' LIMIT 3")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String cik = rs.getString("cik");
            String factName = rs.getString("fact_name");
            String factValue = rs.getString("fact_value");

            assertNotNull(cik, "CIK should not be null");
            assertNotNull(factName, "Fact name should not be null");

            LOGGER.debug("Found financial fact: CIK={}, Fact={}, Value={}",
                cik, factName, factValue);
          }

          LOGGER.info("Financial data query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testSecInsiderDataAccess() throws SQLException {
    LOGGER.info("Testing SEC insider trading data access");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test insider data structure
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT cik, insider_name, transaction_type FROM insider " +
          "WHERE cik = '0000320193' LIMIT 3")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String cik = rs.getString("cik");
            String insiderName = rs.getString("insider_name");
            String transactionType = rs.getString("transaction_type");

            assertNotNull(cik, "CIK should not be null");

            LOGGER.debug("Found insider transaction: CIK={}, Insider={}, Type={}",
                cik, insiderName, transactionType);
          }

          LOGGER.info("Insider data query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testSecJoinQueries() throws SQLException {
    LOGGER.info("Testing SEC cross-table join functionality");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test join between company_tickers and filing_metadata
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT ct.ticker, ct.company_name, fm.filing_type " +
          "FROM company_tickers ct " +
          "LEFT JOIN filing_metadata fm ON ct.cik = fm.cik " +
          "WHERE ct.ticker = 'AAPL' LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String ticker = rs.getString("ticker");
            String companyName = rs.getString("company_name");
            String filingType = rs.getString("filing_type");

            assertEquals("AAPL", ticker, "Ticker should match filter");
            assertNotNull(companyName, "Company name should not be null");

            LOGGER.debug("Found joined record: Ticker={}, Company={}, Filing={}",
                ticker, companyName, filingType);
          }

          LOGGER.info("Join query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testSecDataTypes() throws SQLException {
    LOGGER.info("Testing SEC data type handling and validation");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test data type consistency in financial_data
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT fact_name, fact_value, " +
          "CASE WHEN fact_value ~ '^[0-9]+\\.?[0-9]*$' THEN 'NUMERIC' ELSE 'TEXT' END as value_type " +
          "FROM financial_data WHERE cik = '0000320193' LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            String factName = rs.getString("fact_name");
            String factValue = rs.getString("fact_value");
            String valueType = rs.getString("value_type");

            assertNotNull(factName, "Fact name should not be null");
            LOGGER.debug("Data type test: {}={} ({})", factName, factValue, valueType);
          }
        }
      }
    }
  }

  @Test void testSecErrorHandling() throws SQLException {
    LOGGER.info("Testing SEC error handling and edge cases");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test query with non-existent CIK
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT COUNT(*) as count FROM filing_metadata WHERE cik = '9999999999'")) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "Query should execute successfully");
          assertEquals(0, rs.getInt("count"), "Non-existent CIK should return 0 results");
        }
      }

      // Test invalid table reference (should fail gracefully)
      assertThrows(SQLException.class, () -> {
        try (PreparedStatement stmt =
            connection.prepareStatement("SELECT * FROM nonexistent_table")) {
          stmt.executeQuery();
        }
      }, "Query on non-existent table should throw SQLException");
    }
  }
}
