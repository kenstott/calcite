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
package org.apache.calcite.adapter.file.execution;

import org.apache.calcite.adapter.file.execution.duckdb.DuckDBConfig;

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
 * Tests for {@link DuckDBConfig}.
 */
@Tag("unit")
public class DuckDBConfigTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DuckDBConfigTest.class);

  @Test
  public void testDefaultConstructorUsesDefaults() {
    DuckDBConfig config = new DuckDBConfig();
    assertEquals(DuckDBConfig.DEFAULT_MEMORY_LIMIT, config.getMemoryLimit());
    assertEquals(DuckDBConfig.DEFAULT_THREADS, config.getThreads());
    assertEquals(DuckDBConfig.DEFAULT_MAX_MEMORY, config.getMaxMemory());
    assertNull(config.getTempDirectory());
    assertFalse(config.isEnableProgressBar());
    assertTrue(config.isPreserveInsertionOrder());
    assertTrue(config.isUseArrowOptimization());
    assertEquals(DuckDBConfig.DEFAULT_ARROW_BATCH_SIZE, config.getArrowBatchSize());
    assertNotNull(config.getAdditionalSettings());
    assertTrue(config.getAdditionalSettings().isEmpty());
    LOGGER.debug("Default DuckDBConfig: {}", config);
  }

  @Test
  public void testMapConstructorWithAllSettings() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("memory_limit", "4GB");
    configMap.put("threads", 8);
    configMap.put("max_memory", "90%");
    configMap.put("temp_directory", "/tmp/duckdb");
    configMap.put("enable_progress_bar", true);
    configMap.put("preserve_insertion_order", true);
    configMap.put("use_arrow_optimization", true);
    configMap.put("arrow_batch_size", 2048);

    DuckDBConfig config = new DuckDBConfig(configMap);
    assertEquals("4GB", config.getMemoryLimit());
    assertEquals(8, config.getThreads());
    assertEquals("90%", config.getMaxMemory());
    assertEquals("/tmp/duckdb", config.getTempDirectory());
    assertTrue(config.isEnableProgressBar());
    assertTrue(config.isPreserveInsertionOrder());
    assertTrue(config.isUseArrowOptimization());
    assertEquals(2048, config.getArrowBatchSize());
  }

  @Test
  public void testMapConstructorWithMissingSettingsUsesDefaults() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    DuckDBConfig config = new DuckDBConfig(configMap);
    assertEquals(DuckDBConfig.DEFAULT_MEMORY_LIMIT, config.getMemoryLimit());
    assertEquals(DuckDBConfig.DEFAULT_THREADS, config.getThreads());
    assertEquals(DuckDBConfig.DEFAULT_MAX_MEMORY, config.getMaxMemory());
    assertNull(config.getTempDirectory());
    assertFalse(config.isEnableProgressBar());
  }

  @Test
  public void testMapConstructorWithAdditionalSettings() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("custom_setting", "custom_value");
    configMap.put("another_setting", "42");

    DuckDBConfig config = new DuckDBConfig(configMap);
    Properties additional = config.getAdditionalSettings();
    assertEquals("custom_value", additional.getProperty("custom_setting"));
    assertEquals("42", additional.getProperty("another_setting"));
  }

  @Test
  public void testMapConstructorIgnoresNullAdditionalValues() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("custom_setting", null);

    DuckDBConfig config = new DuckDBConfig(configMap);
    Properties additional = config.getAdditionalSettings();
    assertNull(additional.getProperty("custom_setting"));
  }

  @Test
  public void testMapConstructorWithNumberThreads() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("threads", Integer.valueOf(16));

    DuckDBConfig config = new DuckDBConfig(configMap);
    assertEquals(16, config.getThreads());
  }

  @Test
  public void testMapConstructorWithNonNumberThreadsUsesDefault() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("threads", "not_a_number");

    DuckDBConfig config = new DuckDBConfig(configMap);
    assertEquals(DuckDBConfig.DEFAULT_THREADS, config.getThreads());
  }

  @Test
  public void testMapConstructorWithNumberBatchSize() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("arrow_batch_size", Integer.valueOf(4096));

    DuckDBConfig config = new DuckDBConfig(configMap);
    assertEquals(4096, config.getArrowBatchSize());
  }

  @Test
  public void testFullConstructorWithAllParams() {
    Properties additionalSettings = new Properties();
    additionalSettings.setProperty("custom", "value");

    DuckDBConfig config = new DuckDBConfig(
        "8GB", 16, "95%", "/tmp/duck", true, false,
        true, 4096, additionalSettings);

    assertEquals("8GB", config.getMemoryLimit());
    assertEquals(16, config.getThreads());
    assertEquals("95%", config.getMaxMemory());
    assertEquals("/tmp/duck", config.getTempDirectory());
    assertTrue(config.isEnableProgressBar());
    assertFalse(config.isPreserveInsertionOrder());
    assertTrue(config.isUseArrowOptimization());
    assertEquals(4096, config.getArrowBatchSize());
    assertEquals("value", config.getAdditionalSettings().getProperty("custom"));
  }

  @Test
  public void testFullConstructorWithNullsUsesDefaults() {
    DuckDBConfig config = new DuckDBConfig(
        null, -1, null, null, false, true,
        false, -1, null);

    assertEquals(DuckDBConfig.DEFAULT_MEMORY_LIMIT, config.getMemoryLimit());
    assertEquals(DuckDBConfig.DEFAULT_THREADS, config.getThreads());
    assertEquals(DuckDBConfig.DEFAULT_MAX_MEMORY, config.getMaxMemory());
    assertNull(config.getTempDirectory());
    assertEquals(DuckDBConfig.DEFAULT_ARROW_BATCH_SIZE, config.getArrowBatchSize());
    assertNotNull(config.getAdditionalSettings());
    assertTrue(config.getAdditionalSettings().isEmpty());
  }

  @Test
  public void testToDuckDBSettingsContainsCoreSettings() {
    DuckDBConfig config = new DuckDBConfig();
    String[] settings = config.toDuckDBSettings();
    assertNotNull(settings);
    assertTrue(settings.length >= 5,
        "Should have at least 5 settings");

    boolean hasMemoryLimit = false;
    boolean hasThreads = false;
    boolean hasMaxMemory = false;
    boolean hasProgressBar = false;
    boolean hasInsertionOrder = false;

    for (String setting : settings) {
      if (setting.contains("memory_limit")) {
        hasMemoryLimit = true;
      }
      if (setting.contains("SET threads")) {
        hasThreads = true;
      }
      if (setting.contains("max_memory")) {
        hasMaxMemory = true;
      }
      if (setting.contains("enable_progress_bar")) {
        hasProgressBar = true;
      }
      if (setting.contains("preserve_insertion_order")) {
        hasInsertionOrder = true;
      }
      LOGGER.debug("DuckDB setting: {}", setting);
    }

    assertTrue(hasMemoryLimit, "Should include memory_limit setting");
    assertTrue(hasThreads, "Should include threads setting");
    assertTrue(hasMaxMemory, "Should include max_memory setting");
    assertTrue(hasProgressBar, "Should include enable_progress_bar setting");
    assertTrue(hasInsertionOrder, "Should include preserve_insertion_order setting");
  }

  @Test
  public void testToDuckDBSettingsIncludesTempDirectory() {
    DuckDBConfig config = new DuckDBConfig(
        "1GB", 4, "80%", "/tmp/duck", false, true,
        true, 1024, null);

    String[] settings = config.toDuckDBSettings();
    boolean hasTempDir = false;
    for (String setting : settings) {
      if (setting.contains("temp_directory")) {
        hasTempDir = true;
        assertTrue(setting.contains("/tmp/duck"));
      }
    }
    assertTrue(hasTempDir, "Should include temp_directory when set");
  }

  @Test
  public void testToDuckDBSettingsExcludesTempDirectoryWhenNull() {
    DuckDBConfig config = new DuckDBConfig();
    String[] settings = config.toDuckDBSettings();
    for (String setting : settings) {
      assertFalse(setting.contains("temp_directory"),
          "Should not include temp_directory when null");
    }
  }

  @Test
  public void testToDuckDBSettingsIncludesAdditionalNumericSetting() {
    Properties additional = new Properties();
    additional.setProperty("parallel_csv_reader", "true");

    DuckDBConfig config = new DuckDBConfig(
        "1GB", 4, "80%", null, false, true,
        true, 1024, additional);

    String[] settings = config.toDuckDBSettings();
    boolean hasCustom = false;
    for (String setting : settings) {
      if (setting.contains("parallel_csv_reader")) {
        hasCustom = true;
        // Boolean value should not be quoted
        assertTrue(setting.contains("= true"),
            "Boolean value should not be quoted");
      }
    }
    assertTrue(hasCustom, "Should include additional settings");
  }

  @Test
  public void testToDuckDBSettingsQuotesStringValues() {
    Properties additional = new Properties();
    additional.setProperty("access_mode", "read_only");

    DuckDBConfig config = new DuckDBConfig(
        "1GB", 4, "80%", null, false, true,
        true, 1024, additional);

    String[] settings = config.toDuckDBSettings();
    boolean hasAccessMode = false;
    for (String setting : settings) {
      if (setting.contains("access_mode")) {
        hasAccessMode = true;
        assertTrue(setting.contains("'read_only'"),
            "String values should be quoted");
      }
    }
    assertTrue(hasAccessMode, "Should include access_mode setting");
  }

  @Test
  public void testToStringContainsAllFields() {
    DuckDBConfig config = new DuckDBConfig(
        "4GB", 8, "90%", "/tmp", true, false,
        true, 2048, null);

    String str = config.toString();
    assertTrue(str.contains("4GB"), "toString should include memoryLimit");
    assertTrue(str.contains("8"), "toString should include threads");
    assertTrue(str.contains("90%"), "toString should include maxMemory");
    assertTrue(str.contains("/tmp"), "toString should include tempDirectory");
    assertTrue(str.contains("true"), "toString should include enableProgressBar");
    LOGGER.debug("DuckDBConfig toString: {}", str);
  }

  @Test
  public void testDefaultConstants() {
    assertEquals("1GB", DuckDBConfig.DEFAULT_MEMORY_LIMIT);
    assertEquals("80%", DuckDBConfig.DEFAULT_MAX_MEMORY);
    assertFalse(DuckDBConfig.DEFAULT_ENABLE_PROGRESS_BAR);
    assertTrue(DuckDBConfig.DEFAULT_PRESERVE_INSERTION_ORDER);
    assertTrue(DuckDBConfig.DEFAULT_USE_ARROW_OPTIMIZATION);
    assertEquals(1024, DuckDBConfig.DEFAULT_ARROW_BATCH_SIZE);
    assertTrue(DuckDBConfig.DEFAULT_THREADS > 0);
  }
}
