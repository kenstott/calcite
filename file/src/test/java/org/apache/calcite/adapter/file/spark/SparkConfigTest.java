/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file.spark;

import org.apache.calcite.adapter.file.execution.spark.SparkConfig;

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
 * Tests for {@link SparkConfig}.
 */
@Tag("unit")
public class SparkConfigTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SparkConfigTest.class);

  @Test
  public void testDefaultConstructorUsesDefaults() {
    SparkConfig config = new SparkConfig();
    assertEquals(SparkConfig.DEFAULT_HOST, config.getHost());
    assertEquals(SparkConfig.DEFAULT_PORT, config.getPort());
    assertEquals(SparkConfig.DEFAULT_DATABASE, config.getDatabase());
    assertNull(config.getUser());
    assertNull(config.getPassword());
    assertEquals(SparkConfig.DEFAULT_ICEBERG_CATALOG_TYPE, config.getIcebergCatalogType());
    assertNull(config.getIcebergWarehouse());
    assertNull(config.getMaxMemory());
    assertEquals(0, config.getMaxThreads());
    assertNotNull(config.getAdditionalSettings());
    assertEquals(0, config.getAdditionalSettings().size());
    LOGGER.debug("Default config: {}", config);
  }

  @Test
  public void testMapConstructorSetsAllFields() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("host", "spark-master.example.com");
    configMap.put("port", "10001");
    configMap.put("database", "analytics");
    configMap.put("user", "sparkuser");
    configMap.put("password", "secret123");
    configMap.put("icebergCatalogType", "hive");
    configMap.put("icebergWarehouse", "s3://bucket/warehouse");
    configMap.put("maxMemory", "8g");
    configMap.put("maxThreads", 4);

    SparkConfig config = new SparkConfig(configMap);
    assertEquals("spark-master.example.com", config.getHost());
    assertEquals("10001", config.getPort());
    assertEquals("analytics", config.getDatabase());
    assertEquals("sparkuser", config.getUser());
    assertEquals("secret123", config.getPassword());
    assertEquals("hive", config.getIcebergCatalogType());
    assertEquals("s3://bucket/warehouse", config.getIcebergWarehouse());
    assertEquals("8g", config.getMaxMemory());
    assertEquals(4, config.getMaxThreads());
  }

  @Test
  public void testFullConstructorSetsAllFields() {
    Properties additionalSettings = new Properties();
    additionalSettings.setProperty("spark.sql.shuffle.partitions", "200");

    SparkConfig config = new SparkConfig(
        "myhost", "10002", "mydb",
        "admin", "pass",
        "hive", "/data/iceberg",
        "16g", 8,
        additionalSettings);

    assertEquals("myhost", config.getHost());
    assertEquals("10002", config.getPort());
    assertEquals("mydb", config.getDatabase());
    assertEquals("admin", config.getUser());
    assertEquals("pass", config.getPassword());
    assertEquals("hive", config.getIcebergCatalogType());
    assertEquals("/data/iceberg", config.getIcebergWarehouse());
    assertEquals("16g", config.getMaxMemory());
    assertEquals(8, config.getMaxThreads());
    assertEquals("200",
        config.getAdditionalSettings().getProperty("spark.sql.shuffle.partitions"));
  }

  @Test
  public void testFullConstructorNullDefaultsToDefaults() {
    SparkConfig config = new SparkConfig(
        null, null, null,
        null, null,
        null, null,
        null, 0,
        null);

    assertEquals(SparkConfig.DEFAULT_HOST, config.getHost());
    assertEquals(SparkConfig.DEFAULT_PORT, config.getPort());
    assertEquals(SparkConfig.DEFAULT_DATABASE, config.getDatabase());
    assertEquals(SparkConfig.DEFAULT_ICEBERG_CATALOG_TYPE, config.getIcebergCatalogType());
    assertNull(config.getUser());
    assertNull(config.getPassword());
    assertNull(config.getIcebergWarehouse());
    assertNull(config.getMaxMemory());
    assertEquals(0, config.getMaxThreads());
    assertNotNull(config.getAdditionalSettings());
    assertEquals(0, config.getAdditionalSettings().size());
  }

  @Test
  public void testMapConstructorPortAsNumber() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("port", 10001);

    SparkConfig config = new SparkConfig(configMap);
    assertEquals("10001", config.getPort());
  }

  @Test
  public void testToSparkSettingsWithMemoryAndThreads() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("maxMemory", "4g");
    configMap.put("maxThreads", 2);

    SparkConfig config = new SparkConfig(configMap);
    String[] settings = config.toSparkSettings();

    assertNotNull(settings);
    assertTrue(settings.length >= 2,
        "Should have at least memory and threads settings");

    boolean hasMemory = false;
    boolean hasCores = false;
    for (String setting : settings) {
      if (setting.contains("spark.executor.memory") && setting.contains("4g")) {
        hasMemory = true;
      }
      if (setting.contains("spark.executor.cores") && setting.contains("2")) {
        hasCores = true;
      }
    }
    assertTrue(hasMemory, "Should contain spark.executor.memory = 4g");
    assertTrue(hasCores, "Should contain spark.executor.cores = 2");
  }

  @Test
  public void testToSparkSettingsEmptyForDefaults() {
    SparkConfig config = new SparkConfig();
    String[] settings = config.toSparkSettings();

    assertNotNull(settings);
    assertEquals(0, settings.length,
        "Default config should produce no SET statements");
  }

  @Test
  public void testToSparkSettingsIncludesAdditionalSettings() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("spark.sql.shuffle.partitions", "200");

    SparkConfig config = new SparkConfig(configMap);
    String[] settings = config.toSparkSettings();

    boolean found = false;
    for (String s : settings) {
      if (s.contains("spark.sql.shuffle.partitions") && s.contains("200")) {
        found = true;
      }
    }
    assertTrue(found, "Should include additional settings in toSparkSettings output");
  }

  @Test
  public void testToIcebergCatalogSettingsWithWarehouseOverride() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("icebergWarehouse", "s3://bucket/iceberg");

    SparkConfig config = new SparkConfig(configMap);
    String[] settings = config.toIcebergCatalogSettings("/default/path");

    assertNotNull(settings);
    assertTrue(settings.length >= 2,
        "Should have at least catalog class and type settings");

    boolean hasCatalogClass = false;
    boolean hasCatalogType = false;
    boolean hasWarehouse = false;
    for (String setting : settings) {
      if (setting.contains(SparkConfig.ICEBERG_CATALOG_NAME)
          && setting.contains("SparkCatalog")) {
        hasCatalogClass = true;
      }
      if (setting.contains(SparkConfig.ICEBERG_CATALOG_NAME)
          && setting.contains(".type = ")) {
        hasCatalogType = true;
      }
      if (setting.contains("s3://bucket/iceberg")) {
        hasWarehouse = true;
      }
    }
    assertTrue(hasCatalogClass, "Should set Iceberg SparkCatalog class");
    assertTrue(hasCatalogType, "Should set Iceberg catalog type");
    assertTrue(hasWarehouse, "Should use configured warehouse over default");
  }

  @Test
  public void testToIcebergCatalogSettingsWithoutWarehouseOverride() {
    SparkConfig config = new SparkConfig();
    String[] settings = config.toIcebergCatalogSettings("/data/warehouse");

    boolean hasDefaultWarehouse = false;
    for (String setting : settings) {
      if (setting.contains("/data/warehouse")) {
        hasDefaultWarehouse = true;
      }
    }
    assertTrue(hasDefaultWarehouse,
        "Should use provided warehousePath when icebergWarehouse is null");
  }

  @Test
  public void testIcebergCatalogNameConstant() {
    assertEquals("aperio_iceberg", SparkConfig.ICEBERG_CATALOG_NAME);
  }

  @Test
  public void testToStringContainsKey() {
    SparkConfig config = new SparkConfig();
    String str = config.toString();
    assertNotNull(str);
    assertTrue(str.contains("SparkConfig"));
    assertTrue(str.contains("localhost"));
    assertTrue(str.contains("10000"));
    LOGGER.debug("toString: {}", str);
  }

  @Test
  public void testToStringMasksPassword() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("user", "admin");
    configMap.put("password", "supersecretpassword");

    SparkConfig config = new SparkConfig(configMap);
    String str = config.toString();

    assertFalse(str.contains("supersecretpassword"),
        "Password should be masked in toString output");
    assertTrue(str.contains("***"),
        "Should show masked password indicator");
  }

  @Test
  public void testMapConstructorWithNullValueInMap() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("host", "myhost");
    configMap.put("custom_key", null);

    SparkConfig config = new SparkConfig(configMap);
    assertNull(config.getAdditionalSettings().getProperty("custom_key"),
        "Null values in map should not appear in additional settings");
  }

  @Test
  public void testToSparkSettingsWithOnlyMaxMemory() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("maxMemory", "8g");

    SparkConfig config = new SparkConfig(configMap);
    String[] settings = config.toSparkSettings();

    assertEquals(1, settings.length,
        "Should have exactly one setting for memory only");
    assertTrue(settings[0].contains("spark.executor.memory"),
        "Setting should be for memory");
  }

  @Test
  public void testToSparkSettingsWithOnlyMaxThreads() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("maxThreads", 4);

    SparkConfig config = new SparkConfig(configMap);
    String[] settings = config.toSparkSettings();

    assertEquals(1, settings.length,
        "Should have exactly one setting for threads only");
    assertTrue(settings[0].contains("spark.executor.cores"),
        "Setting should be for cores");
  }

  @Test
  public void testToIcebergCatalogSettingsWithBothWarehousesNull() {
    SparkConfig config = new SparkConfig(
        "host", "10000", "db",
        null, null,
        "hadoop", null,
        null, 0, null);
    String[] settings = config.toIcebergCatalogSettings(null);

    // Should have catalog class and type, but no warehouse
    assertEquals(2, settings.length,
        "Should have only catalog class and type when both warehouses are null");
    for (String s : settings) {
      assertFalse(s.contains(".warehouse"),
          "Should not have warehouse setting when both are null");
    }
  }

  @Test
  public void testMapConstructorMaxThreadsAsStringIgnored() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("maxThreads", "not_a_number");

    SparkConfig config = new SparkConfig(configMap);
    assertEquals(0, config.getMaxThreads(),
        "Non-numeric maxThreads string should default to 0");
  }

  @Test
  public void testToStringShowsAdditionalSettingsCount() {
    Properties additional = new Properties();
    additional.setProperty("key1", "val1");
    additional.setProperty("key2", "val2");

    SparkConfig config = new SparkConfig(
        "host", "10000", "db", null, null,
        "hadoop", null, null, 0, additional);
    String str = config.toString();
    assertTrue(str.contains("2 items"),
        "toString should show count of additional settings");
  }
}
