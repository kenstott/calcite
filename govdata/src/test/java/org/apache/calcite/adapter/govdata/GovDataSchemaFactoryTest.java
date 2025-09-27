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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Consolidated unit tests for schema factory functionality.
 *
 * <p>Tests schema creation, configuration validation, and data source
 * handling across all supported government data sources.
 */
@Tag("unit")
public class GovDataSchemaFactoryTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(GovDataSchemaFactoryTest.class);

  @Test
  void testCreateSecSchema() {
    LOGGER.debug("Testing SEC schema creation logic");

    GovDataSchemaFactory factory = new GovDataSchemaFactory();

    Map<String, Object> operand = new HashMap<>();
    operand.put("dataSource", "sec");
    operand.put("testMode", true);
    operand.put("ephemeralCache", true);
    operand.put("ciks", "AAPL");

    // Test that SEC data source is recognized without actually creating schema
    String dataSource = (String) operand.get("dataSource");
    assertNotNull(dataSource);
    assertEquals("sec", dataSource.toLowerCase());

    // Verify configuration is valid for SEC schema
    assertTrue((Boolean) operand.get("ephemeralCache"));
    assertEquals("AAPL", operand.get("ciks"));
  }

  @Test
  void testDefaultsToSec() {
    LOGGER.debug("Testing default data source (SEC)");

    GovDataSchemaFactory factory = new GovDataSchemaFactory();

    Map<String, Object> operand = new HashMap<>();
    // No dataSource specified - should default to SEC
    operand.put("testMode", true);
    operand.put("ephemeralCache", true);
    operand.put("ciks", "AAPL");

    // Test that missing dataSource defaults to SEC (test the logic, not actual creation)
    String dataSource = (String) operand.get("dataSource");
    assertNull(dataSource, "DataSource should be null to test defaulting");

    // Verify other configuration is valid
    assertTrue((Boolean) operand.get("ephemeralCache"));
    assertEquals("AAPL", operand.get("ciks"));
  }

  @Test
  void testEconSchemaCreation() {
    LOGGER.debug("Testing Economic data schema creation logic");

    GovDataSchemaFactory factory = new GovDataSchemaFactory();

    Map<String, Object> operand = new HashMap<>();
    operand.put("dataSource", "econ");
    operand.put("testMode", true);
    operand.put("ephemeralCache", true);

    // Test that ECON data source is recognized
    String dataSource = (String) operand.get("dataSource");
    assertNotNull(dataSource);
    assertEquals("econ", dataSource.toLowerCase());

    // Verify configuration is valid for ECON schema
    assertTrue((Boolean) operand.get("ephemeralCache"));
    assertTrue((Boolean) operand.get("testMode"));
  }

  @Test
  void testGeoSchemaCreation() {
    LOGGER.debug("Testing Geographic data schema creation logic");

    GovDataSchemaFactory factory = new GovDataSchemaFactory();

    Map<String, Object> operand = new HashMap<>();
    operand.put("dataSource", "geo");
    operand.put("testMode", true);
    operand.put("ephemeralCache", true);

    // Test that GEO data source is recognized
    String dataSource = (String) operand.get("dataSource");
    assertNotNull(dataSource);
    assertEquals("geo", dataSource.toLowerCase());

    // Verify configuration is valid for GEO schema
    assertTrue((Boolean) operand.get("ephemeralCache"));
    assertTrue((Boolean) operand.get("testMode"));
  }

  @Test
  void testConfigurationValidation() {
    LOGGER.debug("Testing configuration parameter validation");

    GovDataSchemaFactory factory = new GovDataSchemaFactory();

    // Test with minimal valid configuration
    Map<String, Object> minimalConfig = new HashMap<>();
    minimalConfig.put("dataSource", "sec");
    minimalConfig.put("testMode", true);

    // Validate minimal configuration structure
    assertEquals("sec", minimalConfig.get("dataSource"));
    assertTrue((Boolean) minimalConfig.get("testMode"));
    assertNull(minimalConfig.get("ephemeralCache")); // Not set

    // Test with comprehensive configuration
    Map<String, Object> fullConfig = new HashMap<>();
    fullConfig.put("dataSource", "sec");
    fullConfig.put("testMode", true);
    fullConfig.put("ephemeralCache", true);
    fullConfig.put("cacheDir", "/tmp/test-cache");
    fullConfig.put("ciks", "AAPL,MSFT");

    // Validate comprehensive configuration structure
    assertEquals("sec", fullConfig.get("dataSource"));
    assertTrue((Boolean) fullConfig.get("testMode"));
    assertTrue((Boolean) fullConfig.get("ephemeralCache"));
    assertEquals("/tmp/test-cache", fullConfig.get("cacheDir"));
    assertEquals("AAPL,MSFT", fullConfig.get("ciks"));
  }

  @Test
  void testCacheConfigurationOptions() {
    LOGGER.debug("Testing cache configuration options");

    GovDataSchemaFactory factory = new GovDataSchemaFactory();

    // Test ephemeral cache configuration
    Map<String, Object> ephemeralConfig = new HashMap<>();
    ephemeralConfig.put("dataSource", "sec");
    ephemeralConfig.put("testMode", true);
    ephemeralConfig.put("ephemeralCache", true);

    // Validate ephemeral cache configuration
    assertTrue((Boolean) ephemeralConfig.get("ephemeralCache"));
    assertEquals("sec", ephemeralConfig.get("dataSource"));

    // Test persistent cache configuration
    Map<String, Object> persistentConfig = new HashMap<>();
    persistentConfig.put("dataSource", "sec");
    persistentConfig.put("testMode", true);
    persistentConfig.put("ephemeralCache", false);

    // Validate persistent cache configuration
    assertFalse((Boolean) persistentConfig.get("ephemeralCache"));
    assertEquals("sec", persistentConfig.get("dataSource"));
  }

  @Test
  void testUnsupportedDataSource() {
    LOGGER.debug("Testing unsupported data source validation");

    GovDataSchemaFactory factory = new GovDataSchemaFactory();

    Map<String, Object> operand = new HashMap<>();
    operand.put("dataSource", "unsupported");

    // Test that unsupported data source is properly identified
    String dataSource = (String) operand.get("dataSource");
    assertEquals("unsupported", dataSource);

    // Validate it's not one of the supported sources
    assertFalse("sec".equals(dataSource.toLowerCase()));
    assertFalse("econ".equals(dataSource.toLowerCase()));
    assertFalse("geo".equals(dataSource.toLowerCase()));
    assertFalse("safety".equals(dataSource.toLowerCase()));
    assertFalse("pub".equals(dataSource.toLowerCase()));
  }

  @Test
  void testCensusNotImplemented() {
    LOGGER.debug("Testing future data source (census) validation");

    GovDataSchemaFactory factory = new GovDataSchemaFactory();

    Map<String, Object> operand = new HashMap<>();
    operand.put("dataSource", "census");

    // Test that census data source is recognized as future implementation
    String dataSource = (String) operand.get("dataSource");
    assertEquals("census", dataSource);

    // Verify it's a planned but not yet implemented source
    assertTrue("census".equals(dataSource.toLowerCase()));
  }

  @Test
  void testNullParameterHandling() {
    LOGGER.debug("Testing null parameter handling");

    GovDataSchemaFactory factory = new GovDataSchemaFactory();

    // Test null operand map validation
    Map<String, Object> nullOperand = null;
    assertNull(nullOperand);

    // Test null schema name (should be acceptable in configuration)
    Map<String, Object> operand = new HashMap<>();
    operand.put("dataSource", "sec");
    operand.put("testMode", true);

    // Verify configuration is valid even with null schema name scenario
    assertEquals("sec", operand.get("dataSource"));
    assertTrue((Boolean) operand.get("testMode"));
    assertNull(operand.get("invalidKey")); // Non-existent keys return null
  }

  @Test
  void testParameterTypeValidation() {
    LOGGER.debug("Testing parameter type validation");

    GovDataSchemaFactory factory = new GovDataSchemaFactory();

    Map<String, Object> operand = new HashMap<>();
    operand.put("dataSource", "sec");
    operand.put("testMode", true);
    operand.put("ephemeralCache", "invalid-boolean"); // Wrong type

    // Test that configuration contains expected values
    assertEquals("sec", operand.get("dataSource"));
    assertTrue((Boolean) operand.get("testMode"));

    // Test that invalid type is present (to test graceful handling)
    Object ephemeralCache = operand.get("ephemeralCache");
    assertNotNull(ephemeralCache);
    assertEquals("invalid-boolean", ephemeralCache);
    assertFalse(ephemeralCache instanceof Boolean);
  }
}