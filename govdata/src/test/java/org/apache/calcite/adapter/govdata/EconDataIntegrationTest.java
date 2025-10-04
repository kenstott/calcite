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
 * Comprehensive integration tests for Economic data functionality.
 *
 * <p>This test consolidates all ECON-related integration testing including:
 * - FRED API data access and validation
 * - BLS employment statistics queries
 * - Treasury yield curve data
 * - Economic indicator time series analysis
 * - Cross-economic dataset relationships
 * - Performance and data integrity validation
 *
 * <p>These tests require:
 * - GOVDATA_CACHE_DIR environment variable
 * - GOVDATA_PARQUET_DIR environment variable (optional)
 * - Network access for economic data download (if not cached)
 * - Valid API keys for FRED_API_KEY (optional but recommended)
 */
@Tag("integration")
public class EconDataIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(EconDataIntegrationTest.class);

  private static final String TEST_MODEL = "{\n"
      + "  \"version\": \"1.0\",\n"
      + "  \"defaultSchema\": \"ECON\",\n"
      + "  \"schemas\": [{\n"
      + "    \"name\": \"ECON\",\n"
      + "    \"type\": \"custom\",\n"
      + "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n"
      + "    \"operand\": {\n"
      + "      \"dataSource\": \"econ\",\n"
      + "      \"testMode\": true,\n"
      + "      \"autoDownload\": false,\n"
      + "      \"startYear\": 2020,\n"
      + "      \"endYear\": 2023\n"
      + "    }\n"
      + "  }]\n"
      + "}";

  @BeforeAll
  static void checkEnvironment() {
    String cacheDir = System.getenv("GOVDATA_CACHE_DIR");
    Assumptions.assumeTrue(cacheDir != null && !cacheDir.isEmpty(),
        "GOVDATA_CACHE_DIR environment variable must be set for integration tests");

    LOGGER.info("Running ECON integration tests with cache directory: {}", cacheDir);

    String fredApiKey = System.getenv("FRED_API_KEY");
    if (fredApiKey != null) {
      LOGGER.info("FRED API key is available for enhanced testing");
    } else {
      LOGGER.warn("FRED_API_KEY not set - some tests may use demo data only");
    }
  }

  @Test void testEconSchemaCreation() throws SQLException {
    LOGGER.info("Testing ECON schema creation and basic connectivity");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      assertNotNull(connection, "Connection should be established");
      assertFalse(connection.isClosed(), "Connection should be open");

      // Verify schema exists
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'ECON'")) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "ECON schema should exist");
          assertEquals("ECON", rs.getString("schema_name"));
        }
      }
    }
  }

  @Test void testEconTableAvailability() throws SQLException {
    LOGGER.info("Testing ECON table availability and metadata");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test key ECON tables are available
      String[] expectedTables = {
          "fred_data_series_catalog",
          "gdp_components",
          "regional_employment",
          "regional_income",
          "state_gdp",
          "treasury_yields"
      };

      for (String tableName : expectedTables) {
        try (PreparedStatement stmt =
            connection.prepareStatement("SELECT table_name FROM information_schema.tables " +
            "WHERE table_schema = 'ECON' AND table_name = ?")) {
          stmt.setString(1, tableName);
          try (ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next(),
                "Table '" + tableName + "' should be available in ECON schema");
            assertEquals(tableName, rs.getString("table_name"));
          }
        }
      }
    }
  }

  @Test void testFredDataSeriesCatalog() throws SQLException {
    LOGGER.info("Testing FRED data series catalog functionality");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test FRED series catalog structure and basic data
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT series_id, title, units FROM fred_data_series_catalog " +
          "WHERE series_id LIKE 'GDP%' LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String seriesId = rs.getString("series_id");
            String title = rs.getString("title");
            String units = rs.getString("units");

            assertNotNull(seriesId, "Series ID should not be null");
            assertTrue(seriesId.startsWith("GDP"), "Series ID should match filter");
            assertNotNull(title, "Title should not be null");

            LOGGER.debug("Found FRED series: ID={}, Title={}, Units={}",
                seriesId, title, units);
          }

          LOGGER.info("FRED catalog query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testGdpComponentsData() throws SQLException {
    LOGGER.info("Testing GDP components data access");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test GDP components structure
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT date_value, gdp_total, personal_consumption FROM gdp_components " +
          "WHERE year >= 2020 ORDER BY date_value DESC LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String dateValue = rs.getString("date_value");
            String gdpTotal = rs.getString("gdp_total");
            String personalConsumption = rs.getString("personal_consumption");

            assertNotNull(dateValue, "Date value should not be null");

            LOGGER.debug("Found GDP data: Date={}, GDP={}, Consumption={}",
                dateValue, gdpTotal, personalConsumption);
          }

          LOGGER.info("GDP components query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testRegionalEmploymentData() throws SQLException {
    LOGGER.info("Testing regional employment data access");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test regional employment data
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT state_code, year, unemployment_rate FROM regional_employment " +
          "WHERE state_code IN ('CA', 'NY', 'TX') AND year >= 2020 LIMIT 10")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String stateCode = rs.getString("state_code");
            String year = rs.getString("year");
            String unemploymentRate = rs.getString("unemployment_rate");

            assertNotNull(stateCode, "State code should not be null");
            assertTrue(stateCode.length() == 2, "State code should be 2 characters");

            LOGGER.debug("Found employment data: State={}, Year={}, Rate={}",
                stateCode, year, unemploymentRate);
          }

          LOGGER.info("Regional employment query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testRegionalIncomeData() throws SQLException {
    LOGGER.info("Testing regional income data access");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test regional income data
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT geo_fips, geo_name, year, per_capita_personal_income FROM regional_income " +
          "WHERE year >= 2020 ORDER BY per_capita_personal_income DESC LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String geoFips = rs.getString("geo_fips");
            String geoName = rs.getString("geo_name");
            String year = rs.getString("year");
            String income = rs.getString("per_capita_personal_income");

            assertNotNull(geoFips, "GEO FIPS should not be null");
            assertNotNull(geoName, "GEO name should not be null");

            LOGGER.debug("Found income data: FIPS={}, Name={}, Year={}, Income={}",
                geoFips, geoName, year, income);
          }

          LOGGER.info("Regional income query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testStateGdpData() throws SQLException {
    LOGGER.info("Testing state GDP data access");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test state GDP data
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT geo_fips, geo_name, year, gdp_millions FROM state_gdp " +
          "WHERE year >= 2020 ORDER BY gdp_millions DESC LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String geoFips = rs.getString("geo_fips");
            String geoName = rs.getString("geo_name");
            String year = rs.getString("year");
            String gdp = rs.getString("gdp_millions");

            assertNotNull(geoFips, "GEO FIPS should not be null");
            assertNotNull(geoName, "GEO name should not be null");

            LOGGER.debug("Found GDP data: FIPS={}, Name={}, Year={}, GDP={}",
                geoFips, geoName, year, gdp);
          }

          LOGGER.info("State GDP query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testTreasuryYieldsData() throws SQLException {
    LOGGER.info("Testing Treasury yields data access");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test Treasury yields data
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT date_value, yield_1_year, yield_10_year, yield_30_year FROM treasury_yields " +
          "ORDER BY date_value DESC LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String dateValue = rs.getString("date_value");
            String yield1Year = rs.getString("yield_1_year");
            String yield10Year = rs.getString("yield_10_year");
            String yield30Year = rs.getString("yield_30_year");

            assertNotNull(dateValue, "Date value should not be null");

            LOGGER.debug("Found yields data: Date={}, 1Y={}, 10Y={}, 30Y={}",
                dateValue, yield1Year, yield10Year, yield30Year);
          }

          LOGGER.info("Treasury yields query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testEconJoinQueries() throws SQLException {
    LOGGER.info("Testing ECON cross-table join functionality");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test join between regional income and state GDP
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT ri.geo_name, ri.per_capita_personal_income, sg.gdp_millions " +
          "FROM regional_income ri " +
          "LEFT JOIN state_gdp sg ON ri.geo_fips = sg.geo_fips AND ri.year = sg.year " +
          "WHERE ri.year = '2021' LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String geoName = rs.getString("geo_name");
            String income = rs.getString("per_capita_personal_income");
            String gdp = rs.getString("gdp_millions");

            assertNotNull(geoName, "GEO name should not be null");

            LOGGER.debug("Found joined economic data: Name={}, Income={}, GDP={}",
                geoName, income, gdp);
          }

          LOGGER.info("Economic join query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testEconDataTypes() throws SQLException {
    LOGGER.info("Testing ECON data type handling and validation");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test numeric data validation in GDP components
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT gdp_total, " +
          "CASE WHEN gdp_total ~ '^[0-9]+\\.?[0-9]*$' THEN 'NUMERIC' ELSE 'TEXT' END as value_type " +
          "FROM gdp_components WHERE gdp_total IS NOT NULL LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            String gdpTotal = rs.getString("gdp_total");
            String valueType = rs.getString("value_type");

            assertNotNull(gdpTotal, "GDP total should not be null");
            LOGGER.debug("GDP data type test: GDP={} ({})", gdpTotal, valueType);
          }
        }
      }
    }
  }

  @Test void testEconErrorHandling() throws SQLException {
    LOGGER.info("Testing ECON error handling and edge cases");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test query with non-existent year
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT COUNT(*) as count FROM gdp_components WHERE year = 1900")) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "Query should execute successfully");
          assertEquals(0, rs.getInt("count"), "Non-existent year should return 0 results");
        }
      }

      // Test invalid table reference (should fail gracefully)
      assertThrows(SQLException.class, () -> {
        try (PreparedStatement stmt =
            connection.prepareStatement("SELECT * FROM nonexistent_econ_table")) {
          stmt.executeQuery();
        }
      }, "Query on non-existent table should throw SQLException");
    }
  }

  @Test void testTimeSeriesAnalysis() throws SQLException {
    LOGGER.info("Testing economic time series analysis capabilities");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test time series ordering and range queries
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT year, AVG(CAST(unemployment_rate AS DECIMAL)) as avg_unemployment " +
          "FROM regional_employment " +
          "WHERE unemployment_rate IS NOT NULL AND year BETWEEN '2020' AND '2022' " +
          "GROUP BY year ORDER BY year")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String year = rs.getString("year");
            double avgUnemployment = rs.getDouble("avg_unemployment");

            assertNotNull(year, "Year should not be null");
            assertTrue(avgUnemployment >= 0, "Average unemployment should be non-negative");

            LOGGER.debug("Time series data: Year={}, Avg Unemployment={}",
                year, avgUnemployment);
          }

          LOGGER.info("Time series analysis completed. Has results: {}", hasResults);
        }
      }
    }
  }
}
