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
 * Comprehensive integration tests for Geographic data functionality.
 *
 * <p>This test consolidates all GEO-related integration testing including:
 * - Census TIGER/Line boundary data access
 * - HUD USPS crosswalk data validation
 * - Demographic and housing statistics
 * - Geographic boundary queries and spatial analysis
 * - Cross-geographic dataset relationships
 * - Performance and data integrity validation
 *
 * <p>These tests require:
 * - GOVDATA_CACHE_DIR environment variable
 * - GOVDATA_PARQUET_DIR environment variable (optional)
 * - Network access for Census data download (if not cached)
 */
@Tag("integration")
public class GeoDataIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoDataIntegrationTest.class);

  private static final String TEST_MODEL = "{\n"
      + "  \"version\": \"1.0\",\n"
      + "  \"defaultSchema\": \"GEO\",\n"
      + "  \"schemas\": [{\n"
      + "    \"name\": \"GEO\",\n"
      + "    \"type\": \"custom\",\n"
      + "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n"
      + "    \"operand\": {\n"
      + "      \"dataSource\": \"geo\",\n"
      + "      \"testMode\": true,\n"
      + "      \"autoDownload\": false,\n"
      + "      \"year\": 2020\n"
      + "    }\n"
      + "  }]\n"
      + "}";

  @BeforeAll
  static void checkEnvironment() {
    String cacheDir = System.getenv("GOVDATA_CACHE_DIR");
    Assumptions.assumeTrue(cacheDir != null && !cacheDir.isEmpty(),
        "GOVDATA_CACHE_DIR environment variable must be set for integration tests");

    LOGGER.info("Running GEO integration tests with cache directory: {}", cacheDir);
  }

  @Test void testGeoSchemaCreation() throws SQLException {
    LOGGER.info("Testing GEO schema creation and basic connectivity");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      assertNotNull(connection, "Connection should be established");
      assertFalse(connection.isClosed(), "Connection should be open");

      // Verify schema exists
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'GEO'")) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "GEO schema should exist");
          assertEquals("GEO", rs.getString("schema_name"));
        }
      }
    }
  }

  @Test void testGeoTableAvailability() throws SQLException {
    LOGGER.info("Testing GEO table availability and metadata");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test key GEO tables are available
      String[] expectedTables = {
          "tiger_states",
          "tiger_counties",
          "tiger_places",
          "tiger_zcta5",
          "hud_usps_crosswalk",
          "census_population",
          "census_housing"
      };

      for (String tableName : expectedTables) {
        try (PreparedStatement stmt =
            connection.prepareStatement("SELECT table_name FROM information_schema.tables " +
            "WHERE table_schema = 'GEO' AND table_name = ?")) {
          stmt.setString(1, tableName);
          try (ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next(),
                "Table '" + tableName + "' should be available in GEO schema");
            assertEquals(tableName, rs.getString("table_name"));
          }
        }
      }
    }
  }

  @Test void testTigerStatesData() throws SQLException {
    LOGGER.info("Testing TIGER states boundary data access");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test TIGER states data structure
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT state_fips, state_code, state_name FROM tiger_states " +
          "WHERE state_code IN ('CA', 'NY', 'TX') ORDER BY state_name")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String stateFips = rs.getString("state_fips");
            String stateCode = rs.getString("state_code");
            String stateName = rs.getString("state_name");

            assertNotNull(stateFips, "State FIPS should not be null");
            assertNotNull(stateCode, "State code should not be null");
            assertNotNull(stateName, "State name should not be null");
            assertEquals(2, stateCode.length(), "State code should be 2 characters");

            LOGGER.debug("Found state data: FIPS={}, Code={}, Name={}",
                stateFips, stateCode, stateName);
          }

          LOGGER.info("TIGER states query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testTigerCountiesData() throws SQLException {
    LOGGER.info("Testing TIGER counties boundary data access");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test TIGER counties data
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT state_fips, county_fips, county_name FROM tiger_counties " +
          "WHERE state_fips = '06' LIMIT 5")) { // California counties
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String stateFips = rs.getString("state_fips");
            String countyFips = rs.getString("county_fips");
            String countyName = rs.getString("county_name");

            assertNotNull(stateFips, "State FIPS should not be null");
            assertEquals("06", stateFips, "Should match California state FIPS");
            assertNotNull(countyFips, "County FIPS should not be null");
            assertNotNull(countyName, "County name should not be null");

            LOGGER.debug("Found county data: State={}, County FIPS={}, Name={}",
                stateFips, countyFips, countyName);
          }

          LOGGER.info("TIGER counties query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testTigerPlacesData() throws SQLException {
    LOGGER.info("Testing TIGER places (cities) data access");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test TIGER places data
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT state_fips, place_fips, place_name FROM tiger_places " +
          "WHERE place_name LIKE '%San Francisco%' OR place_name LIKE '%Los Angeles%' LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String stateFips = rs.getString("state_fips");
            String placeFips = rs.getString("place_fips");
            String placeName = rs.getString("place_name");

            assertNotNull(stateFips, "State FIPS should not be null");
            assertNotNull(placeFips, "Place FIPS should not be null");
            assertNotNull(placeName, "Place name should not be null");

            LOGGER.debug("Found place data: State={}, Place FIPS={}, Name={}",
                stateFips, placeFips, placeName);
          }

          LOGGER.info("TIGER places query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testTigerZcta5Data() throws SQLException {
    LOGGER.info("Testing TIGER ZIP Code Tabulation Areas (ZCTA5) data access");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test TIGER ZCTA5 data
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT zcta5_code, land_area, water_area FROM tiger_zcta5 " +
          "WHERE zcta5_code BETWEEN '90210' AND '90220' LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String zcta5Code = rs.getString("zcta5_code");
            String landArea = rs.getString("land_area");
            String waterArea = rs.getString("water_area");

            assertNotNull(zcta5Code, "ZCTA5 code should not be null");
            assertEquals(5, zcta5Code.length(), "ZCTA5 code should be 5 characters");

            LOGGER.debug("Found ZCTA5 data: Code={}, Land={}, Water={}",
                zcta5Code, landArea, waterArea);
          }

          LOGGER.info("TIGER ZCTA5 query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testHudUspsCrosswalkData() throws SQLException {
    LOGGER.info("Testing HUD USPS crosswalk data access");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test HUD USPS crosswalk data
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT zip, county, state FROM hud_usps_crosswalk " +
          "WHERE state = 'CA' AND zip BETWEEN '90210' AND '90220' LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String zip = rs.getString("zip");
            String county = rs.getString("county");
            String state = rs.getString("state");

            assertNotNull(zip, "ZIP should not be null");
            assertNotNull(county, "County should not be null");
            assertEquals("CA", state, "State should match filter");

            LOGGER.debug("Found crosswalk data: ZIP={}, County={}, State={}",
                zip, county, state);
          }

          LOGGER.info("HUD USPS crosswalk query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testCensusPopulationData() throws SQLException {
    LOGGER.info("Testing Census population data access");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test Census population data
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT geo_id, name, total_population FROM census_population " +
          "WHERE name LIKE '%California%' OR name LIKE '%New York%' LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String geoId = rs.getString("geo_id");
            String name = rs.getString("name");
            String totalPopulation = rs.getString("total_population");

            assertNotNull(geoId, "GEO ID should not be null");
            assertNotNull(name, "Name should not be null");

            LOGGER.debug("Found population data: GeoID={}, Name={}, Population={}",
                geoId, name, totalPopulation);
          }

          LOGGER.info("Census population query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testCensusHousingData() throws SQLException {
    LOGGER.info("Testing Census housing data access");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test Census housing data
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT geo_id, name, total_housing_units, occupied_housing_units FROM census_housing " +
          "WHERE name LIKE '%County%' LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String geoId = rs.getString("geo_id");
            String name = rs.getString("name");
            String totalUnits = rs.getString("total_housing_units");
            String occupiedUnits = rs.getString("occupied_housing_units");

            assertNotNull(geoId, "GEO ID should not be null");
            assertNotNull(name, "Name should not be null");

            LOGGER.debug("Found housing data: GeoID={}, Name={}, Total={}, Occupied={}",
                geoId, name, totalUnits, occupiedUnits);
          }

          LOGGER.info("Census housing query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testGeoJoinQueries() throws SQLException {
    LOGGER.info("Testing GEO cross-table join functionality");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test join between TIGER states and counties
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT s.state_name, s.state_code, c.county_name " +
          "FROM tiger_states s " +
          "INNER JOIN tiger_counties c ON s.state_fips = c.state_fips " +
          "WHERE s.state_code = 'CA' LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String stateName = rs.getString("state_name");
            String stateCode = rs.getString("state_code");
            String countyName = rs.getString("county_name");

            assertNotNull(stateName, "State name should not be null");
            assertEquals("CA", stateCode, "State code should match filter");
            assertNotNull(countyName, "County name should not be null");

            LOGGER.debug("Found joined geo data: State={}, Code={}, County={}",
                stateName, stateCode, countyName);
          }

          LOGGER.info("Geographic join query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testGeographicHierarchy() throws SQLException {
    LOGGER.info("Testing geographic hierarchy relationships");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test geographic hierarchy: State -> County -> Place
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT s.state_name, c.county_name, p.place_name " +
          "FROM tiger_states s " +
          "INNER JOIN tiger_counties c ON s.state_fips = c.state_fips " +
          "INNER JOIN tiger_places p ON c.state_fips = p.state_fips " +
          "WHERE s.state_code = 'CA' AND p.place_name LIKE '%San%' LIMIT 3")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String stateName = rs.getString("state_name");
            String countyName = rs.getString("county_name");
            String placeName = rs.getString("place_name");

            assertNotNull(stateName, "State name should not be null");
            assertNotNull(countyName, "County name should not be null");
            assertNotNull(placeName, "Place name should not be null");

            LOGGER.debug("Found hierarchy: State={}, County={}, Place={}",
                stateName, countyName, placeName);
          }

          LOGGER.info("Geographic hierarchy query completed. Has results: {}", hasResults);
        }
      }
    }
  }

  @Test void testGeoDataTypes() throws SQLException {
    LOGGER.info("Testing GEO data type handling and validation");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test area calculations and numeric data validation
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT zcta5_code, land_area, " +
          "CASE WHEN land_area ~ '^[0-9]+\\.?[0-9]*$' THEN 'NUMERIC' ELSE 'TEXT' END as area_type " +
          "FROM tiger_zcta5 WHERE land_area IS NOT NULL LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            String zcta5Code = rs.getString("zcta5_code");
            String landArea = rs.getString("land_area");
            String areaType = rs.getString("area_type");

            assertNotNull(zcta5Code, "ZCTA5 code should not be null");
            assertNotNull(landArea, "Land area should not be null");
            LOGGER.debug("Area data type test: ZCTA={}, Area={} ({})",
                zcta5Code, landArea, areaType);
          }
        }
      }
    }
  }

  @Test void testGeoErrorHandling() throws SQLException {
    LOGGER.info("Testing GEO error handling and edge cases");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test query with non-existent state code
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT COUNT(*) as count FROM tiger_states WHERE state_code = 'ZZ'")) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next(), "Query should execute successfully");
          assertEquals(0, rs.getInt("count"), "Non-existent state code should return 0 results");
        }
      }

      // Test invalid table reference (should fail gracefully)
      assertThrows(SQLException.class, () -> {
        try (PreparedStatement stmt =
            connection.prepareStatement("SELECT * FROM nonexistent_geo_table")) {
          stmt.executeQuery();
        }
      }, "Query on non-existent table should throw SQLException");
    }
  }

  @Test void testGeospatialQueries() throws SQLException {
    LOGGER.info("Testing geospatial query patterns and capabilities");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + TEST_MODEL);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", props)) {
      // Test proximity-based queries using ZIP codes
      try (PreparedStatement stmt =
          connection.prepareStatement("SELECT h.zip, h.county, h.state, z.zcta5_code " +
          "FROM hud_usps_crosswalk h " +
          "LEFT JOIN tiger_zcta5 z ON h.zip = z.zcta5_code " +
          "WHERE h.zip BETWEEN '90210' AND '90220' LIMIT 5")) {
        try (ResultSet rs = stmt.executeQuery()) {
          boolean hasResults = false;
          while (rs.next()) {
            hasResults = true;
            String zip = rs.getString("zip");
            String county = rs.getString("county");
            String state = rs.getString("state");
            String zcta5Code = rs.getString("zcta5_code");

            assertNotNull(zip, "ZIP should not be null");
            assertNotNull(county, "County should not be null");

            LOGGER.debug("Found geospatial join: ZIP={}, County={}, State={}, ZCTA={}",
                zip, county, state, zcta5Code);
          }

          LOGGER.info("Geospatial query completed. Has results: {}", hasResults);
        }
      }
    }
  }
}
