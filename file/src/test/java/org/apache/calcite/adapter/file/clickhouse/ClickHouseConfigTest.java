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
package org.apache.calcite.adapter.file.clickhouse;

import org.apache.calcite.adapter.file.execution.clickhouse.ClickHouseConfig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ClickHouseConfig}.
 */
@Tag("unit")
public class ClickHouseConfigTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ClickHouseConfigTest.class);

  @Test public void testDefaultConstructorUsesDefaults() {
    ClickHouseConfig config = new ClickHouseConfig();
    assertEquals(ClickHouseConfig.DEFAULT_MODE, config.getMode());
    assertEquals(ClickHouseConfig.DEFAULT_HOST, config.getHost());
    assertEquals(ClickHouseConfig.DEFAULT_PORT, config.getPort());
    assertEquals(ClickHouseConfig.DEFAULT_DATABASE, config.getDatabase());
    assertNull(config.getLocalBinaryPath());
    assertNull(config.getDataDir());
    assertEquals(ClickHouseConfig.DEFAULT_MAX_MEMORY, config.getMaxMemory());
    assertEquals(ClickHouseConfig.DEFAULT_MAX_THREADS, config.getMaxThreads());
    assertNotNull(config.getAdditionalSettings());
    assertTrue(config.getAdditionalSettings().isEmpty());
    LOGGER.debug("Default ClickHouseConfig: {}", config);
  }

  @Test public void testDefaultConstructorIsServerMode() {
    ClickHouseConfig config = new ClickHouseConfig();
    assertFalse(config.isLocalMode());
    assertEquals("server", config.getMode());
  }

  @Test public void testMapConstructorWithAllSettings() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("mode", "local");
    configMap.put("host", "192.168.1.1");
    configMap.put("port", "9000");
    configMap.put("database", "analytics");
    configMap.put("localBinaryPath", "/usr/bin/clickhouse-local");
    configMap.put("dataDir", "/tmp/ch-data");
    configMap.put("maxMemory", "8GB");
    configMap.put("maxThreads", Integer.valueOf(16));

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertEquals("local", config.getMode());
    assertEquals("192.168.1.1", config.getHost());
    assertEquals("9000", config.getPort());
    assertEquals("analytics", config.getDatabase());
    assertEquals("/usr/bin/clickhouse-local", config.getLocalBinaryPath());
    assertEquals("/tmp/ch-data", config.getDataDir());
    assertEquals("8GB", config.getMaxMemory());
    assertEquals(16, config.getMaxThreads());
    assertTrue(config.isLocalMode());
  }

  @Test public void testMapConstructorWithStringPort() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("port", "9000");

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertEquals("9000", config.getPort());
  }

  @Test public void testMapConstructorWithIntegerPort() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("port", Integer.valueOf(9000));

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertEquals("9000", config.getPort());
  }

  @Test public void testMapConstructorWithMissingSettingsUsesDefaults() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertEquals(ClickHouseConfig.DEFAULT_MODE, config.getMode());
    assertEquals(ClickHouseConfig.DEFAULT_HOST, config.getHost());
    assertEquals(ClickHouseConfig.DEFAULT_PORT, config.getPort());
    assertEquals(ClickHouseConfig.DEFAULT_DATABASE, config.getDatabase());
    assertNull(config.getLocalBinaryPath());
    assertNull(config.getDataDir());
    assertEquals(ClickHouseConfig.DEFAULT_MAX_MEMORY, config.getMaxMemory());
    assertEquals(ClickHouseConfig.DEFAULT_MAX_THREADS, config.getMaxThreads());
  }

  @Test public void testFullConstructorWithAllParams() {
    Properties additionalSettings = new Properties();
    additionalSettings.setProperty("max_partitions_per_insert_block", "100");

    ClickHouseConfig config =
        new ClickHouseConfig("local", "10.0.0.1", "9000", "mydb",
        "/opt/clickhouse-local", "/data/ch",
        "16GB", 32, additionalSettings);

    assertEquals("local", config.getMode());
    assertEquals("10.0.0.1", config.getHost());
    assertEquals("9000", config.getPort());
    assertEquals("mydb", config.getDatabase());
    assertEquals("/opt/clickhouse-local", config.getLocalBinaryPath());
    assertEquals("/data/ch", config.getDataDir());
    assertEquals("16GB", config.getMaxMemory());
    assertEquals(32, config.getMaxThreads());
    assertEquals("100",
        config.getAdditionalSettings().getProperty("max_partitions_per_insert_block"));
  }

  @Test public void testFullConstructorWithNullsUsesDefaults() {
    ClickHouseConfig config =
        new ClickHouseConfig(null, null, null, null, null, null, null, -1, null);

    assertEquals(ClickHouseConfig.DEFAULT_MODE, config.getMode());
    assertEquals(ClickHouseConfig.DEFAULT_HOST, config.getHost());
    assertEquals(ClickHouseConfig.DEFAULT_PORT, config.getPort());
    assertEquals(ClickHouseConfig.DEFAULT_DATABASE, config.getDatabase());
    assertNull(config.getLocalBinaryPath());
    assertNull(config.getDataDir());
    assertEquals(ClickHouseConfig.DEFAULT_MAX_MEMORY, config.getMaxMemory());
    assertEquals(ClickHouseConfig.DEFAULT_MAX_THREADS, config.getMaxThreads());
    assertNotNull(config.getAdditionalSettings());
    assertTrue(config.getAdditionalSettings().isEmpty());
  }

  @Test public void testIsLocalModeWithLocalMode() {
    ClickHouseConfig config =
        new ClickHouseConfig("local", null, null, null, null, null, null, 0, null);
    assertTrue(config.isLocalMode());
  }

  @Test public void testIsLocalModeIsCaseInsensitive() {
    ClickHouseConfig config =
        new ClickHouseConfig("LOCAL", null, null, null, null, null, null, 0, null);
    assertTrue(config.isLocalMode());

    ClickHouseConfig config2 =
        new ClickHouseConfig("Local", null, null, null, null, null, null, 0, null);
    assertTrue(config2.isLocalMode());
  }

  @Test public void testIsLocalModeReturnsFalseForServerMode() {
    ClickHouseConfig config =
        new ClickHouseConfig("server", null, null, null, null, null, null, 0, null);
    assertFalse(config.isLocalMode());
  }

  @Test public void testToClickHouseSettingsContainsCoreSettings() {
    ClickHouseConfig config = new ClickHouseConfig();
    String[] settings = config.toClickHouseSettings();
    assertNotNull(settings);
    assertTrue(settings.length >= 2,
        "Should have at least 2 settings (max_memory_usage and max_threads)");

    boolean hasMaxMemoryUsage = false;
    boolean hasMaxThreads = false;

    for (String setting : settings) {
      if (setting.contains("max_memory_usage")) {
        hasMaxMemoryUsage = true;
      }
      if (setting.contains("max_threads")) {
        hasMaxThreads = true;
      }
      LOGGER.debug("ClickHouse setting: {}", setting);
    }

    assertTrue(hasMaxMemoryUsage, "Should include max_memory_usage setting");
    assertTrue(hasMaxThreads, "Should include max_threads setting");
  }

  @Test public void testToClickHouseSettingsParseMemoryGigabytes() {
    ClickHouseConfig config =
        new ClickHouseConfig("server", null, null, null, null, null, "4GB", 4, null);
    String[] settings = config.toClickHouseSettings();

    long expectedBytes = 4L * 1024 * 1024 * 1024;
    boolean found = false;
    for (String setting : settings) {
      if (setting.contains("max_memory_usage")) {
        assertTrue(setting.contains(String.valueOf(expectedBytes)),
            "4GB should be parsed to " + expectedBytes + " bytes, got: " + setting);
        found = true;
      }
    }
    assertTrue(found, "Should have max_memory_usage setting");
  }

  @Test public void testToClickHouseSettingsParseMemoryMegabytes() {
    ClickHouseConfig config =
        new ClickHouseConfig("server", null, null, null, null, null, "512MB", 4, null);
    String[] settings = config.toClickHouseSettings();

    long expectedBytes = 512L * 1024 * 1024;
    boolean found = false;
    for (String setting : settings) {
      if (setting.contains("max_memory_usage")) {
        assertTrue(setting.contains(String.valueOf(expectedBytes)),
            "512MB should be parsed to " + expectedBytes + " bytes, got: " + setting);
        found = true;
      }
    }
    assertTrue(found, "Should have max_memory_usage setting");
  }

  @Test public void testToClickHouseSettingsParseMemoryKilobytes() {
    ClickHouseConfig config =
        new ClickHouseConfig("server", null, null, null, null, null, "1024KB", 2, null);
    String[] settings = config.toClickHouseSettings();

    long expectedBytes = 1024L * 1024;
    boolean found = false;
    for (String setting : settings) {
      if (setting.contains("max_memory_usage")) {
        assertTrue(setting.contains(String.valueOf(expectedBytes)),
            "1024KB should be parsed to " + expectedBytes + " bytes, got: " + setting);
        found = true;
      }
    }
    assertTrue(found, "Should have max_memory_usage setting");
  }

  @Test public void testToClickHouseSettingsIncludesAdditionalSettings() {
    Properties additional = new Properties();
    additional.setProperty("max_insert_block_size", "1048576");

    ClickHouseConfig config =
        new ClickHouseConfig("server", null, null, null, null, null, "4GB", 4, additional);
    String[] settings = config.toClickHouseSettings();

    boolean hasCustom = false;
    for (String setting : settings) {
      if (setting.contains("max_insert_block_size")) {
        hasCustom = true;
        // Numeric value should not be quoted
        assertTrue(setting.contains("= 1048576"),
            "Numeric value should not be quoted");
      }
    }
    assertTrue(hasCustom, "Should include additional settings");
  }

  @Test public void testToClickHouseSettingsQuotesStringValues() {
    Properties additional = new Properties();
    additional.setProperty("log_level", "warning");

    ClickHouseConfig config =
        new ClickHouseConfig("server", null, null, null, null, null, "4GB", 4, additional);
    String[] settings = config.toClickHouseSettings();

    boolean hasLogLevel = false;
    for (String setting : settings) {
      if (setting.contains("log_level")) {
        hasLogLevel = true;
        assertTrue(setting.contains("'warning'"),
            "String values should be quoted");
      }
    }
    assertTrue(hasLogLevel, "Should include log_level setting");
  }

  @Test public void testMapConstructorAdditionalSettingsExcludeKnownKeys() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("mode", "local");
    configMap.put("host", "myhost");
    configMap.put("port", "9000");
    configMap.put("database", "mydb");
    configMap.put("localBinaryPath", "/usr/bin/ch");
    configMap.put("dataDir", "/data");
    configMap.put("maxMemory", "8GB");
    configMap.put("maxThreads", Integer.valueOf(8));
    configMap.put("custom_extra", "extra_value");

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    Properties additional = config.getAdditionalSettings();

    // Known keys should NOT appear in additional settings
    assertNull(additional.getProperty("mode"));
    assertNull(additional.getProperty("host"));
    assertNull(additional.getProperty("port"));
    assertNull(additional.getProperty("database"));
    assertNull(additional.getProperty("localBinaryPath"));
    assertNull(additional.getProperty("dataDir"));
    assertNull(additional.getProperty("maxMemory"));
    assertNull(additional.getProperty("maxThreads"));

    // Custom key should appear
    assertEquals("extra_value", additional.getProperty("custom_extra"));
  }

  @Test public void testToStringContainsAllFields() {
    ClickHouseConfig config =
        new ClickHouseConfig("local", "10.0.0.1", "9000", "analytics",
        "/opt/ch", "/data", "8GB", 16, null);

    String str = config.toString();
    assertTrue(str.contains("local"), "toString should include mode");
    assertTrue(str.contains("10.0.0.1"), "toString should include host");
    assertTrue(str.contains("9000"), "toString should include port");
    assertTrue(str.contains("analytics"), "toString should include database");
    assertTrue(str.contains("/opt/ch"), "toString should include localBinaryPath");
    assertTrue(str.contains("/data"), "toString should include dataDir");
    assertTrue(str.contains("8GB"), "toString should include maxMemory");
    assertTrue(str.contains("16"), "toString should include maxThreads");
    LOGGER.debug("ClickHouseConfig toString: {}", str);
  }

  @Test public void testDefaultConstants() {
    assertEquals("server", ClickHouseConfig.DEFAULT_MODE);
    assertEquals("localhost", ClickHouseConfig.DEFAULT_HOST);
    assertEquals("8123", ClickHouseConfig.DEFAULT_PORT);
    assertEquals("default", ClickHouseConfig.DEFAULT_DATABASE);
    assertEquals("4GB", ClickHouseConfig.DEFAULT_MAX_MEMORY);
    assertTrue(ClickHouseConfig.DEFAULT_MAX_THREADS > 0,
        "DEFAULT_MAX_THREADS should be positive");
    assertEquals("server", ClickHouseConfig.MODE_SERVER);
    assertEquals("local", ClickHouseConfig.MODE_LOCAL);
  }

  @Test public void testParseMemoryBytesWithInvalidInput() {
    // Invalid memory string should fall back to 4GB default
    ClickHouseConfig config =
        new ClickHouseConfig("server", null, null, null, null, null, "invalid", 4, null);
    String[] settings = config.toClickHouseSettings();

    long defaultBytes = 4L * 1024 * 1024 * 1024;
    boolean found = false;
    for (String setting : settings) {
      if (setting.contains("max_memory_usage")) {
        assertTrue(setting.contains(String.valueOf(defaultBytes)),
            "Invalid memory should default to 4GB, got: " + setting);
        found = true;
      }
    }
    assertTrue(found);
  }

  @Test public void testParseMemoryBytesWithNullMemory() {
    ClickHouseConfig config =
        new ClickHouseConfig("server", null, null, null, null, null, null, 4, null);
    String[] settings = config.toClickHouseSettings();

    long defaultBytes = 4L * 1024 * 1024 * 1024;
    boolean found = false;
    for (String setting : settings) {
      if (setting.contains("max_memory_usage")) {
        assertTrue(setting.contains(String.valueOf(defaultBytes)),
            "Null memory should default to 4GB, got: " + setting);
        found = true;
      }
    }
    assertTrue(found);
  }

  @Test public void testParseMemoryBytesWithRawNumber() {
    // Raw number without suffix should be treated as bytes
    ClickHouseConfig config =
        new ClickHouseConfig("server", null, null, null, null, null, "1073741824", 4, null);
    String[] settings = config.toClickHouseSettings();

    boolean found = false;
    for (String setting : settings) {
      if (setting.contains("max_memory_usage")) {
        assertTrue(setting.contains("1073741824"),
            "Raw number should be kept as-is, got: " + setting);
        found = true;
      }
    }
    assertTrue(found);
  }

  @Test public void testToClickHouseSettingsBooleanValuesNotQuoted() {
    Properties additional = new Properties();
    additional.setProperty("enable_optimize_predicate_expression", "true");

    ClickHouseConfig config =
        new ClickHouseConfig("server", null, null, null, null, null, "4GB", 4, additional);
    String[] settings = config.toClickHouseSettings();

    boolean found = false;
    for (String setting : settings) {
      if (setting.contains("enable_optimize_predicate_expression")) {
        found = true;
        assertFalse(setting.contains("'true'"),
            "Boolean values should not be quoted: " + setting);
        assertTrue(setting.contains("= true"),
            "Boolean value should be unquoted: " + setting);
      }
    }
    assertTrue(found, "Should include boolean additional setting");
  }

  @Test public void testMapConstructorWithNullValueInMap() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("mode", "server");
    configMap.put("custom_key", null);

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    // Null values in map should not appear in additional settings
    assertNull(config.getAdditionalSettings().getProperty("custom_key"));
  }
}
