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
package org.apache.calcite.adapter.file.execution;

import org.apache.calcite.adapter.file.execution.clickhouse.ClickHouseConfig;
import org.apache.calcite.adapter.file.execution.duckdb.DuckDBConfig;
import org.apache.calcite.adapter.file.execution.parquet.ParquetConfig;
import org.apache.calcite.adapter.file.execution.spark.SparkConfig;
import org.apache.calcite.adapter.file.execution.trino.TrinoConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive coverage tests for execution engine configuration classes:
 *
 * <ul>
 *   <li>{@link ExecutionEngineConfig} - Core engine selection and configuration</li>
 *   <li>{@link DuckDBConfig} - DuckDB-specific configuration and SET statements</li>
 *   <li>{@link ParquetConfig} - Parquet batch sizing and vectorized reader config</li>
 *   <li>{@link ClickHouseConfig} - ClickHouse mode, memory parsing, SET statements</li>
 *   <li>{@link SparkConfig} - Spark SQL and Iceberg catalog configuration</li>
 *   <li>{@link TrinoConfig} - Trino catalogs, Hive/Iceberg properties, S3 config</li>
 * </ul>
 *
 * <p>All tests are pure unit tests with no external dependencies.
 */
@Tag("unit")
public class ExecutionEngineCoverageTest {

  // =========================================================================
  // ExecutionEngineConfig Tests
  // =========================================================================

  @Nested
  class ExecutionEngineConfigTests {

    @Test void testDefaultConstructorEngineType() {
      ExecutionEngineConfig config = new ExecutionEngineConfig();
      assertEquals(ExecutionEngineConfig.ExecutionEngineType.PARQUET, config.getEngineType());
    }

    @Test void testDefaultConstructorBatchSize() {
      ExecutionEngineConfig config = new ExecutionEngineConfig();
      assertEquals(ExecutionEngineConfig.DEFAULT_BATCH_SIZE, config.getBatchSize());
    }

    @Test void testDefaultConstructorMemoryThreshold() {
      ExecutionEngineConfig config = new ExecutionEngineConfig();
      assertEquals(ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD, config.getMemoryThreshold());
    }

    @Test void testDefaultConstructorNullMvPath() {
      ExecutionEngineConfig config = new ExecutionEngineConfig();
      assertNull(config.getMaterializedViewStoragePath());
      assertFalse(config.hasCustomStoragePath());
    }

    @Test void testDefaultConstructorCreatesDefaultDuckDBConfig() {
      ExecutionEngineConfig config = new ExecutionEngineConfig();
      assertNotNull(config.getDuckDBConfig());
    }

    @Test void testDefaultConstructorNullParquetCacheDir() {
      ExecutionEngineConfig config = new ExecutionEngineConfig();
      assertNull(config.getParquetCacheDirectory());
    }

    @Test void testTwoArgDuckDB() {
      ExecutionEngineConfig config = new ExecutionEngineConfig("duckdb", 4096);
      assertEquals(ExecutionEngineConfig.ExecutionEngineType.DUCKDB, config.getEngineType());
      assertEquals(4096, config.getBatchSize());
    }

    @Test void testTwoArgArrow() {
      ExecutionEngineConfig config = new ExecutionEngineConfig("arrow", 512);
      assertEquals(ExecutionEngineConfig.ExecutionEngineType.ARROW, config.getEngineType());
      assertEquals(512, config.getBatchSize());
    }

    @Test void testTwoArgVectorized() {
      ExecutionEngineConfig config = new ExecutionEngineConfig("vectorized", 1024);
      assertEquals(ExecutionEngineConfig.ExecutionEngineType.VECTORIZED, config.getEngineType());
    }

    @Test void testTwoArgLinq4j() {
      ExecutionEngineConfig config = new ExecutionEngineConfig("linq4j", 256);
      assertEquals(ExecutionEngineConfig.ExecutionEngineType.LINQ4J, config.getEngineType());
    }

    @Test void testTwoArgTrino() {
      ExecutionEngineConfig config = new ExecutionEngineConfig("trino", 8192);
      assertEquals(ExecutionEngineConfig.ExecutionEngineType.TRINO, config.getEngineType());
    }

    @Test void testTwoArgSpark() {
      ExecutionEngineConfig config = new ExecutionEngineConfig("spark", 2048);
      assertEquals(ExecutionEngineConfig.ExecutionEngineType.SPARK, config.getEngineType());
    }

    @Test void testTwoArgClickhouse() {
      ExecutionEngineConfig config = new ExecutionEngineConfig("clickhouse", 2048);
      assertEquals(ExecutionEngineConfig.ExecutionEngineType.CLICKHOUSE, config.getEngineType());
    }

    @Test void testThreeArgWithMvPath() {
      ExecutionEngineConfig config = new ExecutionEngineConfig("parquet", 2048, "/tmp/views");
      assertEquals("/tmp/views", config.getMaterializedViewStoragePath());
      assertTrue(config.hasCustomStoragePath());
    }

    @Test void testThreeArgWithNullMvPath() {
      ExecutionEngineConfig config = new ExecutionEngineConfig("parquet", 2048, (String) null);
      assertNull(config.getMaterializedViewStoragePath());
      assertFalse(config.hasCustomStoragePath());
    }

    @Test void testFourArgSetsCustomMemoryThreshold() {
      long customMemory = 256L * 1024 * 1024;
      ExecutionEngineConfig config =
          new ExecutionEngineConfig("arrow", 1024, customMemory, "/tmp/mv");
      assertEquals(customMemory, config.getMemoryThreshold());
      assertEquals("/tmp/mv", config.getMaterializedViewStoragePath());
      assertTrue(config.hasCustomStoragePath());
    }

    @Test void testFiveArgWithExplicitDuckDBConfig() {
      DuckDBConfig duckDBCfg =
          new DuckDBConfig("8GB", 16, "95%", "/tmp", true, false, true, 4096, null);
      ExecutionEngineConfig config =
          new ExecutionEngineConfig("duckdb", 2048,
              ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD, null, duckDBCfg);
      assertEquals("8GB", config.getDuckDBConfig().getMemoryLimit());
      assertEquals(16, config.getDuckDBConfig().getThreads());
    }

    @Test void testFiveArgNullDuckDBConfigCreatesDefault() {
      ExecutionEngineConfig config =
          new ExecutionEngineConfig("duckdb", 2048,
              ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD, null, null);
      assertNotNull(config.getDuckDBConfig());
      assertEquals(DuckDBConfig.DEFAULT_MEMORY_LIMIT, config.getDuckDBConfig().getMemoryLimit());
    }

    @Test void testSixArgSetsParquetCacheDirectory() {
      ExecutionEngineConfig config =
          new ExecutionEngineConfig("parquet", 2048,
              ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD,
              "/tmp/mv", null, "/tmp/pcache");
      assertEquals("/tmp/pcache", config.getParquetCacheDirectory());
    }

    @Test void testCaseInsensitiveParsing() {
      ExecutionEngineConfig lower = new ExecutionEngineConfig("duckdb", 2048);
      ExecutionEngineConfig upper = new ExecutionEngineConfig("DUCKDB", 2048);
      ExecutionEngineConfig mixed = new ExecutionEngineConfig("DuCkDb", 2048);
      assertEquals(lower.getEngineType(), upper.getEngineType());
      assertEquals(lower.getEngineType(), mixed.getEngineType());
    }

    @Test void testInvalidEngineTypeThrows() {
      IllegalArgumentException ex =
          assertThrows(IllegalArgumentException.class, () -> new ExecutionEngineConfig("bogus", 2048));
      assertTrue(ex.getMessage().contains("Invalid execution engine"));
      assertTrue(ex.getMessage().contains("bogus"));
    }

    @Test void testInvalidEngineTypeMessageContainsAllOptions() {
      try {
        new ExecutionEngineConfig("invalid", 2048);
      } catch (IllegalArgumentException e) {
        String msg = e.getMessage();
        for (String expected : ExecutionEngineConfig.getAvailableEngineTypes()) {
          assertTrue(msg.contains(expected),
              "Error message should contain '" + expected + "', was: " + msg);
        }
      }
    }

    @Test void testGetAvailableEngineTypesContainsAllExpected() {
      String[] types = ExecutionEngineConfig.getAvailableEngineTypes();
      Set<String> typeSet = new HashSet<String>(Arrays.asList(types));
      assertTrue(typeSet.contains("linq4j"));
      assertTrue(typeSet.contains("arrow"));
      assertTrue(typeSet.contains("vectorized"));
      assertTrue(typeSet.contains("parquet"));
      assertTrue(typeSet.contains("duckdb"));
      assertTrue(typeSet.contains("trino"));
      assertTrue(typeSet.contains("spark"));
      assertTrue(typeSet.contains("clickhouse"));
      assertEquals(8, types.length);
    }

    @Test void testAllAvailableEngineTypesAreParseable() {
      for (String engine : ExecutionEngineConfig.getAvailableEngineTypes()) {
        ExecutionEngineConfig config = new ExecutionEngineConfig(engine, 2048);
        assertNotNull(config.getEngineType(), "Should parse: " + engine);
      }
    }

    @Test void testEnumValuesMatchAvailableTypes() {
      ExecutionEngineConfig.ExecutionEngineType[] values =
          ExecutionEngineConfig.ExecutionEngineType.values();
      assertEquals(8, values.length);
    }

    @Test void testEnumValueOf() {
      assertEquals(ExecutionEngineConfig.ExecutionEngineType.DUCKDB,
          ExecutionEngineConfig.ExecutionEngineType.valueOf("DUCKDB"));
      assertEquals(ExecutionEngineConfig.ExecutionEngineType.PARQUET,
          ExecutionEngineConfig.ExecutionEngineType.valueOf("PARQUET"));
    }

    @Test void testDefaultConstants() {
      assertEquals(2048, ExecutionEngineConfig.DEFAULT_BATCH_SIZE);
      assertEquals(64L * 1024 * 1024, ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD);
      assertEquals("parquet", ExecutionEngineConfig.DEFAULT_EXECUTION_ENGINE);
    }
  }

  // =========================================================================
  // DuckDBConfig Tests
  // =========================================================================

  @Nested
  class DuckDBConfigTests {

    @Test void testDefaultConstructorMemoryLimit() {
      DuckDBConfig config = new DuckDBConfig();
      assertEquals("1GB", config.getMemoryLimit());
    }

    @Test void testDefaultConstructorThreads() {
      DuckDBConfig config = new DuckDBConfig();
      assertTrue(config.getThreads() > 0, "Thread count should be positive");
      assertEquals(Runtime.getRuntime().availableProcessors(), config.getThreads());
    }

    @Test void testDefaultConstructorMaxMemory() {
      DuckDBConfig config = new DuckDBConfig();
      assertEquals("80%", config.getMaxMemory());
    }

    @Test void testDefaultConstructorNullTempDir() {
      DuckDBConfig config = new DuckDBConfig();
      assertNull(config.getTempDirectory());
    }

    @Test void testDefaultConstructorProgressBarDisabled() {
      DuckDBConfig config = new DuckDBConfig();
      assertFalse(config.isEnableProgressBar());
    }

    @Test void testDefaultConstructorPreserveInsertionOrderEnabled() {
      DuckDBConfig config = new DuckDBConfig();
      assertTrue(config.isPreserveInsertionOrder());
    }

    @Test void testDefaultConstructorArrowOptimizationEnabled() {
      DuckDBConfig config = new DuckDBConfig();
      assertTrue(config.isUseArrowOptimization());
    }

    @Test void testDefaultConstructorArrowBatchSize() {
      DuckDBConfig config = new DuckDBConfig();
      assertEquals(1024, config.getArrowBatchSize());
    }

    @Test void testDefaultConstructorEmptyAdditionalSettings() {
      DuckDBConfig config = new DuckDBConfig();
      assertNotNull(config.getAdditionalSettings());
      assertTrue(config.getAdditionalSettings().isEmpty());
    }

    @Test void testMapConstructorAllKnownSettings() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("memory_limit", "8GB");
      map.put("threads", 16);
      map.put("max_memory", "95%");
      map.put("temp_directory", "/tmp/duck");
      map.put("enable_progress_bar", true);
      map.put("preserve_insertion_order", true);
      map.put("use_arrow_optimization", true);
      map.put("arrow_batch_size", 4096);

      DuckDBConfig config = new DuckDBConfig(map);
      assertEquals("8GB", config.getMemoryLimit());
      assertEquals(16, config.getThreads());
      assertEquals("95%", config.getMaxMemory());
      assertEquals("/tmp/duck", config.getTempDirectory());
      assertTrue(config.isEnableProgressBar());
      assertTrue(config.isPreserveInsertionOrder());
      assertTrue(config.isUseArrowOptimization());
      assertEquals(4096, config.getArrowBatchSize());
    }

    @Test void testMapConstructorEmptyMapUsesDefaults() {
      Map<String, Object> map = new HashMap<String, Object>();
      DuckDBConfig config = new DuckDBConfig(map);
      assertEquals(DuckDBConfig.DEFAULT_MEMORY_LIMIT, config.getMemoryLimit());
      assertEquals(DuckDBConfig.DEFAULT_THREADS, config.getThreads());
      assertEquals(DuckDBConfig.DEFAULT_MAX_MEMORY, config.getMaxMemory());
    }

    @Test void testMapConstructorNonNumberThreadsFallsToDefault() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("threads", "not_a_number");
      DuckDBConfig config = new DuckDBConfig(map);
      assertEquals(DuckDBConfig.DEFAULT_THREADS, config.getThreads());
    }

    @Test void testMapConstructorNonNumberBatchSizeFallsToDefault() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("arrow_batch_size", "not_a_number");
      DuckDBConfig config = new DuckDBConfig(map);
      assertEquals(DuckDBConfig.DEFAULT_ARROW_BATCH_SIZE, config.getArrowBatchSize());
    }

    @Test void testMapConstructorAdditionalSettingsCollected() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("custom_key", "custom_value");
      map.put("another_key", "42");
      DuckDBConfig config = new DuckDBConfig(map);
      Properties additional = config.getAdditionalSettings();
      assertEquals("custom_value", additional.getProperty("custom_key"));
      assertEquals("42", additional.getProperty("another_key"));
    }

    @Test void testMapConstructorNullValueNotInAdditionalSettings() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("custom_key", null);
      DuckDBConfig config = new DuckDBConfig(map);
      assertNull(config.getAdditionalSettings().getProperty("custom_key"));
    }

    @Test void testMapConstructorKnownSettingsNotInAdditionalSettings() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("memory_limit", "2GB");
      map.put("threads", 4);
      DuckDBConfig config = new DuckDBConfig(map);
      assertNull(config.getAdditionalSettings().getProperty("memory_limit"));
      assertNull(config.getAdditionalSettings().getProperty("threads"));
    }

    @Test void testFullConstructorNullMemoryLimitUsesDefault() {
      DuckDBConfig config =
          new DuckDBConfig(null, 4, "80%", null, false, true, true, 1024, null);
      assertEquals(DuckDBConfig.DEFAULT_MEMORY_LIMIT, config.getMemoryLimit());
    }

    @Test void testFullConstructorNegativeThreadsUsesDefault() {
      DuckDBConfig config =
          new DuckDBConfig("1GB", -1, "80%", null, false, true, true, 1024, null);
      assertEquals(DuckDBConfig.DEFAULT_THREADS, config.getThreads());
    }

    @Test void testFullConstructorZeroThreadsUsesDefault() {
      DuckDBConfig config =
          new DuckDBConfig("1GB", 0, "80%", null, false, true, true, 1024, null);
      assertEquals(DuckDBConfig.DEFAULT_THREADS, config.getThreads());
    }

    @Test void testFullConstructorNullMaxMemoryUsesDefault() {
      DuckDBConfig config =
          new DuckDBConfig("1GB", 4, null, null, false, true, true, 1024, null);
      assertEquals(DuckDBConfig.DEFAULT_MAX_MEMORY, config.getMaxMemory());
    }

    @Test void testFullConstructorNegativeBatchSizeUsesDefault() {
      DuckDBConfig config =
          new DuckDBConfig("1GB", 4, "80%", null, false, true, true, -1, null);
      assertEquals(DuckDBConfig.DEFAULT_ARROW_BATCH_SIZE, config.getArrowBatchSize());
    }

    @Test void testFullConstructorNullAdditionalSettingsCreatesEmpty() {
      DuckDBConfig config =
          new DuckDBConfig("1GB", 4, "80%", null, false, true, true, 1024, null);
      assertNotNull(config.getAdditionalSettings());
      assertTrue(config.getAdditionalSettings().isEmpty());
    }

    @Test void testFullConstructorWithAllFields() {
      Properties additional = new Properties();
      additional.setProperty("custom", "val");
      DuckDBConfig config =
          new DuckDBConfig("4GB", 8, "90%", "/data/tmp", true, false, false, 2048, additional);
      assertEquals("4GB", config.getMemoryLimit());
      assertEquals(8, config.getThreads());
      assertEquals("90%", config.getMaxMemory());
      assertEquals("/data/tmp", config.getTempDirectory());
      assertTrue(config.isEnableProgressBar());
      assertFalse(config.isPreserveInsertionOrder());
      assertFalse(config.isUseArrowOptimization());
      assertEquals(2048, config.getArrowBatchSize());
      assertEquals("val", config.getAdditionalSettings().getProperty("custom"));
    }

    @Test void testToDuckDBSettingsCoreSetting() {
      DuckDBConfig config =
          new DuckDBConfig("2GB", 8, "90%", null, false, true, true, 1024, null);
      String[] settings = config.toDuckDBSettings();

      boolean hasMemory = false;
      boolean hasThreads = false;
      boolean hasMaxMem = false;
      boolean hasProgress = false;
      boolean hasInsertion = false;
      for (String s : settings) {
        if (s.equals("SET memory_limit = '2GB'")) {
          hasMemory = true;
        }
        if (s.equals("SET threads = 8")) {
          hasThreads = true;
        }
        if (s.equals("SET max_memory = '90%'")) {
          hasMaxMem = true;
        }
        if (s.contains("enable_progress_bar")) {
          hasProgress = true;
        }
        if (s.contains("preserve_insertion_order")) {
          hasInsertion = true;
        }
      }
      assertTrue(hasMemory, "Should include memory_limit");
      assertTrue(hasThreads, "Should include threads");
      assertTrue(hasMaxMem, "Should include max_memory");
      assertTrue(hasProgress, "Should include enable_progress_bar");
      assertTrue(hasInsertion, "Should include preserve_insertion_order");
    }

    @Test void testToDuckDBSettingsWithTempDirectory() {
      DuckDBConfig config =
          new DuckDBConfig("1GB", 4, "80%", "/tmp/ddb", false, true, true, 1024, null);
      String[] settings = config.toDuckDBSettings();
      boolean hasTempDir = false;
      for (String s : settings) {
        if (s.contains("temp_directory") && s.contains("/tmp/ddb")) {
          hasTempDir = true;
        }
      }
      assertTrue(hasTempDir, "Should include temp_directory");
    }

    @Test void testToDuckDBSettingsWithoutTempDirectory() {
      DuckDBConfig config = new DuckDBConfig();
      String[] settings = config.toDuckDBSettings();
      for (String s : settings) {
        assertFalse(s.contains("temp_directory"),
            "Should not include temp_directory when null");
      }
    }

    @Test void testToDuckDBSettingsAdditionalBooleanValueNotQuoted() {
      Properties additional = new Properties();
      additional.setProperty("external_access", "true");
      DuckDBConfig config =
          new DuckDBConfig("1GB", 4, "80%", null, false, true, true, 1024, additional);
      String[] settings = config.toDuckDBSettings();
      boolean found = false;
      for (String s : settings) {
        if (s.contains("external_access")) {
          found = true;
          assertTrue(s.contains("= true"), "Boolean should not be quoted: " + s);
          assertFalse(s.contains("'true'"), "Boolean should not be quoted: " + s);
        }
      }
      assertTrue(found);
    }

    @Test void testToDuckDBSettingsAdditionalNumericValueNotQuoted() {
      Properties additional = new Properties();
      additional.setProperty("checkpoint_threshold", "1000");
      DuckDBConfig config =
          new DuckDBConfig("1GB", 4, "80%", null, false, true, true, 1024, additional);
      String[] settings = config.toDuckDBSettings();
      boolean found = false;
      for (String s : settings) {
        if (s.contains("checkpoint_threshold")) {
          found = true;
          assertTrue(s.contains("= 1000"), "Numeric should not be quoted: " + s);
        }
      }
      assertTrue(found);
    }

    @Test void testToDuckDBSettingsAdditionalStringValueQuoted() {
      Properties additional = new Properties();
      additional.setProperty("access_mode", "read_only");
      DuckDBConfig config =
          new DuckDBConfig("1GB", 4, "80%", null, false, true, true, 1024, additional);
      String[] settings = config.toDuckDBSettings();
      boolean found = false;
      for (String s : settings) {
        if (s.contains("access_mode")) {
          found = true;
          assertTrue(s.contains("'read_only'"), "String should be quoted: " + s);
        }
      }
      assertTrue(found);
    }

    @Test void testToStringContainsFields() {
      DuckDBConfig config =
          new DuckDBConfig("4GB", 8, "90%", "/tmp", true, false, true, 2048, null);
      String str = config.toString();
      assertTrue(str.contains("4GB"));
      assertTrue(str.contains("8"));
      assertTrue(str.contains("90%"));
      assertTrue(str.contains("/tmp"));
      assertTrue(str.contains("DuckDBConfig"));
    }

    @Test void testDefaultConstants() {
      assertEquals("1GB", DuckDBConfig.DEFAULT_MEMORY_LIMIT);
      assertEquals("80%", DuckDBConfig.DEFAULT_MAX_MEMORY);
      assertFalse(DuckDBConfig.DEFAULT_ENABLE_PROGRESS_BAR);
      assertTrue(DuckDBConfig.DEFAULT_PRESERVE_INSERTION_ORDER);
      assertTrue(DuckDBConfig.DEFAULT_USE_ARROW_OPTIMIZATION);
      assertEquals(1024, DuckDBConfig.DEFAULT_ARROW_BATCH_SIZE);
    }
  }

  // =========================================================================
  // ParquetConfig Tests
  // =========================================================================

  @Nested
  class ParquetConfigTests {

    private String origBatchSize;
    private String origVectorized;
    private String origDebug;

    @BeforeEach
    void saveProperties() {
      origBatchSize = System.getProperty("calcite.file.parquet.batch.size");
      origVectorized = System.getProperty("parquet.enable.vectorized.reader");
      origDebug = System.getProperty("calcite.debug.batch.computation");
    }

    @AfterEach
    void restoreProperties() {
      restoreProperty("calcite.file.parquet.batch.size", origBatchSize);
      restoreProperty("parquet.enable.vectorized.reader", origVectorized);
      restoreProperty("calcite.debug.batch.computation", origDebug);
    }

    private void restoreProperty(String key, String value) {
      if (value != null) {
        System.setProperty(key, value);
      } else {
        System.clearProperty(key);
      }
    }

    @Test void testConstructorSetsFields() {
      ParquetConfig config = new ParquetConfig(4096, true);
      assertEquals(4096, config.getBatchSize());
      assertTrue(config.isVectorizedReaderEnabled());
    }

    @Test void testConstructorVectorizedDisabled() {
      ParquetConfig config = new ParquetConfig(2048, false);
      assertFalse(config.isVectorizedReaderEnabled());
    }

    @Test void testComputeOptimalBatchSizeWithinBounds() {
      int batchSize = ParquetConfig.computeOptimalBatchSize();
      assertTrue(batchSize >= 1024, "Min bound: " + batchSize);
      assertTrue(batchSize <= 16384, "Max bound: " + batchSize);
    }

    @Test void testComputeOptimalBatchSizeConsistency() {
      int first = ParquetConfig.computeOptimalBatchSize();
      int second = ParquetConfig.computeOptimalBatchSize();
      assertTrue(Math.abs(first - second) < 1024,
          "Should be consistent: " + first + " vs " + second);
    }

    @Test void testDefaultStaticInstance() {
      ParquetConfig def = ParquetConfig.DEFAULT;
      assertNotNull(def);
      assertTrue(def.getBatchSize() >= 1024);
      assertFalse(def.isVectorizedReaderEnabled());
    }

    @Test void testFromSystemPropertiesDefaults() {
      System.clearProperty("calcite.file.parquet.batch.size");
      System.clearProperty("parquet.enable.vectorized.reader");

      ParquetConfig config = ParquetConfig.fromSystemProperties();
      assertNotNull(config);
      assertTrue(config.getBatchSize() >= 1024);
      assertFalse(config.isVectorizedReaderEnabled());
    }

    @Test void testFromSystemPropertiesCustomBatchSize() {
      System.setProperty("calcite.file.parquet.batch.size", "6000");
      ParquetConfig config = ParquetConfig.fromSystemProperties();
      assertEquals(6000, config.getBatchSize());
    }

    @Test void testFromSystemPropertiesVectorizedEnabled() {
      System.setProperty("parquet.enable.vectorized.reader", "true");
      ParquetConfig config = ParquetConfig.fromSystemProperties();
      assertTrue(config.isVectorizedReaderEnabled());
    }

    @Test void testFromSystemPropertiesVectorizedExplicitlyDisabled() {
      System.setProperty("parquet.enable.vectorized.reader", "false");
      ParquetConfig config = ParquetConfig.fromSystemProperties();
      assertFalse(config.isVectorizedReaderEnabled());
    }

    @Test void testToString() {
      ParquetConfig config = new ParquetConfig(8192, true);
      String str = config.toString();
      assertTrue(str.contains("8192"));
      assertTrue(str.contains("true"));
      assertTrue(str.contains("ParquetConfig"));
    }

    @Test void testSmallBatchSizeAccepted() {
      ParquetConfig config = new ParquetConfig(1, false);
      assertEquals(1, config.getBatchSize());
    }

    @Test void testLargeBatchSizeAccepted() {
      ParquetConfig config = new ParquetConfig(500000, false);
      assertEquals(500000, config.getBatchSize());
    }
  }

  // =========================================================================
  // ClickHouseConfig Tests
  // =========================================================================

  @Nested
  class ClickHouseConfigTests {

    @Test void testDefaultConstructorMode() {
      ClickHouseConfig config = new ClickHouseConfig();
      assertEquals("server", config.getMode());
      assertFalse(config.isLocalMode());
    }

    @Test void testDefaultConstructorHost() {
      ClickHouseConfig config = new ClickHouseConfig();
      assertEquals("localhost", config.getHost());
    }

    @Test void testDefaultConstructorPort() {
      ClickHouseConfig config = new ClickHouseConfig();
      assertEquals("8123", config.getPort());
    }

    @Test void testDefaultConstructorDatabase() {
      ClickHouseConfig config = new ClickHouseConfig();
      assertEquals("default", config.getDatabase());
    }

    @Test void testDefaultConstructorNullLocalBinaryPath() {
      ClickHouseConfig config = new ClickHouseConfig();
      assertNull(config.getLocalBinaryPath());
    }

    @Test void testDefaultConstructorNullDataDir() {
      ClickHouseConfig config = new ClickHouseConfig();
      assertNull(config.getDataDir());
    }

    @Test void testDefaultConstructorMaxMemory() {
      ClickHouseConfig config = new ClickHouseConfig();
      assertEquals("4GB", config.getMaxMemory());
    }

    @Test void testDefaultConstructorMaxThreads() {
      ClickHouseConfig config = new ClickHouseConfig();
      assertTrue(config.getMaxThreads() > 0);
    }

    @Test void testMapConstructorAllSettings() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("mode", "local");
      map.put("host", "ch-server");
      map.put("port", "9000");
      map.put("database", "analytics");
      map.put("localBinaryPath", "/usr/bin/clickhouse-local");
      map.put("dataDir", "/data/ch");
      map.put("maxMemory", "16GB");
      map.put("maxThreads", 32);

      ClickHouseConfig config = new ClickHouseConfig(map);
      assertEquals("local", config.getMode());
      assertTrue(config.isLocalMode());
      assertEquals("ch-server", config.getHost());
      assertEquals("9000", config.getPort());
      assertEquals("analytics", config.getDatabase());
      assertEquals("/usr/bin/clickhouse-local", config.getLocalBinaryPath());
      assertEquals("/data/ch", config.getDataDir());
      assertEquals("16GB", config.getMaxMemory());
      assertEquals(32, config.getMaxThreads());
    }

    @Test void testMapConstructorPortAsNumber() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("port", Integer.valueOf(9000));
      ClickHouseConfig config = new ClickHouseConfig(map);
      assertEquals("9000", config.getPort());
    }

    @Test void testMapConstructorPortAsString() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("port", "9001");
      ClickHouseConfig config = new ClickHouseConfig(map);
      assertEquals("9001", config.getPort());
    }

    @Test void testMapConstructorPortUnknownTypeUsesDefault() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("port", Boolean.TRUE);
      ClickHouseConfig config = new ClickHouseConfig(map);
      assertEquals(ClickHouseConfig.DEFAULT_PORT, config.getPort());
    }

    @Test void testMapConstructorNonNumberThreadsUsesDefault() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("maxThreads", "not_a_number");
      ClickHouseConfig config = new ClickHouseConfig(map);
      assertEquals(ClickHouseConfig.DEFAULT_MAX_THREADS, config.getMaxThreads());
    }

    @Test void testMapConstructorAdditionalSettings() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("custom_param", "custom_val");
      ClickHouseConfig config = new ClickHouseConfig(map);
      assertEquals("custom_val", config.getAdditionalSettings().getProperty("custom_param"));
    }

    @Test void testMapConstructorNullValueExcluded() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("custom_param", null);
      ClickHouseConfig config = new ClickHouseConfig(map);
      assertNull(config.getAdditionalSettings().getProperty("custom_param"));
    }

    @Test void testMapConstructorKnownSettingsNotInAdditional() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("mode", "server");
      map.put("host", "myhost");
      ClickHouseConfig config = new ClickHouseConfig(map);
      assertNull(config.getAdditionalSettings().getProperty("mode"));
      assertNull(config.getAdditionalSettings().getProperty("host"));
    }

    @Test void testFullConstructorNullsUseDefaults() {
      ClickHouseConfig config =
          new ClickHouseConfig(null, null, null, null, null, null, null, -1, null);
      assertEquals(ClickHouseConfig.DEFAULT_MODE, config.getMode());
      assertEquals(ClickHouseConfig.DEFAULT_HOST, config.getHost());
      assertEquals(ClickHouseConfig.DEFAULT_PORT, config.getPort());
      assertEquals(ClickHouseConfig.DEFAULT_DATABASE, config.getDatabase());
      assertEquals(ClickHouseConfig.DEFAULT_MAX_MEMORY, config.getMaxMemory());
      assertEquals(ClickHouseConfig.DEFAULT_MAX_THREADS, config.getMaxThreads());
      assertNotNull(config.getAdditionalSettings());
    }

    @Test void testIsLocalModeServerIsFalse() {
      ClickHouseConfig config =
          new ClickHouseConfig("server", "localhost", "8123", "default",
          null, null, "4GB", 4, null);
      assertFalse(config.isLocalMode());
    }

    @Test void testIsLocalModeCaseInsensitive() {
      ClickHouseConfig config =
          new ClickHouseConfig("LOCAL", "localhost", "8123", "default",
          null, null, "4GB", 4, null);
      assertTrue(config.isLocalMode());
    }

    @Test void testToClickHouseSettingsContainsMemory() {
      ClickHouseConfig config =
          new ClickHouseConfig("server", "localhost", "8123", "default",
          null, null, "4GB", 4, null);
      String[] settings = config.toClickHouseSettings();
      boolean hasMemory = false;
      for (String s : settings) {
        if (s.contains("max_memory_usage")) {
          hasMemory = true;
          // 4GB = 4 * 1024^3
          assertTrue(s.contains(String.valueOf(4L * 1024 * 1024 * 1024)));
        }
      }
      assertTrue(hasMemory, "Should include max_memory_usage");
    }

    @Test void testToClickHouseSettingsContainsThreads() {
      ClickHouseConfig config =
          new ClickHouseConfig("server", "localhost", "8123", "default",
          null, null, "4GB", 8, null);
      String[] settings = config.toClickHouseSettings();
      boolean hasThreads = false;
      for (String s : settings) {
        if (s.equals("SET max_threads = 8")) {
          hasThreads = true;
        }
      }
      assertTrue(hasThreads, "Should include max_threads");
    }

    @Test void testToClickHouseSettingsMemoryParsingGB() {
      ClickHouseConfig config =
          new ClickHouseConfig("server", "localhost", "8123", "default",
          null, null, "2GB", 4, null);
      String[] settings = config.toClickHouseSettings();
      String memSetting = settings[0]; // first setting is max_memory_usage
      assertTrue(memSetting.contains(String.valueOf(2L * 1024 * 1024 * 1024)));
    }

    @Test void testToClickHouseSettingsMemoryParsingMB() {
      ClickHouseConfig config =
          new ClickHouseConfig("server", "localhost", "8123", "default",
          null, null, "512MB", 4, null);
      String[] settings = config.toClickHouseSettings();
      String memSetting = settings[0];
      assertTrue(memSetting.contains(String.valueOf(512L * 1024 * 1024)));
    }

    @Test void testToClickHouseSettingsMemoryParsingKB() {
      ClickHouseConfig config =
          new ClickHouseConfig("server", "localhost", "8123", "default",
          null, null, "1024KB", 4, null);
      String[] settings = config.toClickHouseSettings();
      String memSetting = settings[0];
      assertTrue(memSetting.contains(String.valueOf(1024L * 1024)));
    }

    @Test void testToClickHouseSettingsMemoryParsingPlainNumber() {
      ClickHouseConfig config =
          new ClickHouseConfig("server", "localhost", "8123", "default",
          null, null, "1000000", 4, null);
      String[] settings = config.toClickHouseSettings();
      String memSetting = settings[0];
      assertTrue(memSetting.contains("1000000"));
    }

    @Test void testToClickHouseSettingsAdditionalBooleanNotQuoted() {
      Properties additional = new Properties();
      additional.setProperty("use_uncompressed_cache", "true");
      ClickHouseConfig config =
          new ClickHouseConfig("server", "localhost", "8123", "default",
          null, null, "4GB", 4, additional);
      String[] settings = config.toClickHouseSettings();
      boolean found = false;
      for (String s : settings) {
        if (s.contains("use_uncompressed_cache")) {
          found = true;
          assertTrue(s.contains("= true"));
          assertFalse(s.contains("'true'"));
        }
      }
      assertTrue(found);
    }

    @Test void testToClickHouseSettingsAdditionalStringQuoted() {
      Properties additional = new Properties();
      additional.setProperty("log_level", "debug");
      ClickHouseConfig config =
          new ClickHouseConfig("server", "localhost", "8123", "default",
          null, null, "4GB", 4, additional);
      String[] settings = config.toClickHouseSettings();
      boolean found = false;
      for (String s : settings) {
        if (s.contains("log_level")) {
          found = true;
          assertTrue(s.contains("'debug'"));
        }
      }
      assertTrue(found);
    }

    @Test void testToClickHouseSettingsAdditional0And1AreBooleanOrNumeric() {
      Properties additional = new Properties();
      additional.setProperty("flag", "0");
      ClickHouseConfig config =
          new ClickHouseConfig("server", "localhost", "8123", "default",
          null, null, "4GB", 4, additional);
      String[] settings = config.toClickHouseSettings();
      boolean found = false;
      for (String s : settings) {
        if (s.contains("flag")) {
          found = true;
          // "0" is numeric, so should not be quoted
          assertTrue(s.contains("= 0"));
        }
      }
      assertTrue(found);
    }

    @Test void testToStringContainsFields() {
      ClickHouseConfig config =
          new ClickHouseConfig("local", "myhost", "9000", "mydb",
          "/usr/bin/ch-local", "/data", "8GB", 16, null);
      String str = config.toString();
      assertTrue(str.contains("local"));
      assertTrue(str.contains("myhost"));
      assertTrue(str.contains("9000"));
      assertTrue(str.contains("mydb"));
      assertTrue(str.contains("8GB"));
      assertTrue(str.contains("ClickHouseConfig"));
    }

    @Test void testDefaultConstants() {
      assertEquals("server", ClickHouseConfig.DEFAULT_MODE);
      assertEquals("localhost", ClickHouseConfig.DEFAULT_HOST);
      assertEquals("8123", ClickHouseConfig.DEFAULT_PORT);
      assertEquals("default", ClickHouseConfig.DEFAULT_DATABASE);
      assertEquals("4GB", ClickHouseConfig.DEFAULT_MAX_MEMORY);
      assertEquals("server", ClickHouseConfig.MODE_SERVER);
      assertEquals("local", ClickHouseConfig.MODE_LOCAL);
    }
  }

  // =========================================================================
  // SparkConfig Tests
  // =========================================================================

  @Nested
  class SparkConfigTests {

    @Test void testDefaultConstructorHost() {
      SparkConfig config = new SparkConfig();
      assertEquals("localhost", config.getHost());
    }

    @Test void testDefaultConstructorPort() {
      SparkConfig config = new SparkConfig();
      assertEquals("10000", config.getPort());
    }

    @Test void testDefaultConstructorDatabase() {
      SparkConfig config = new SparkConfig();
      assertEquals("default", config.getDatabase());
    }

    @Test void testDefaultConstructorNullUser() {
      SparkConfig config = new SparkConfig();
      assertNull(config.getUser());
    }

    @Test void testDefaultConstructorNullPassword() {
      SparkConfig config = new SparkConfig();
      assertNull(config.getPassword());
    }

    @Test void testDefaultConstructorIcebergCatalogType() {
      SparkConfig config = new SparkConfig();
      assertEquals("hadoop", config.getIcebergCatalogType());
    }

    @Test void testDefaultConstructorNullIcebergWarehouse() {
      SparkConfig config = new SparkConfig();
      assertNull(config.getIcebergWarehouse());
    }

    @Test void testDefaultConstructorNullMaxMemory() {
      SparkConfig config = new SparkConfig();
      assertNull(config.getMaxMemory());
    }

    @Test void testDefaultConstructorZeroMaxThreads() {
      SparkConfig config = new SparkConfig();
      assertEquals(0, config.getMaxThreads());
    }

    @Test void testDefaultConstructorEmptyAdditionalSettings() {
      SparkConfig config = new SparkConfig();
      assertTrue(config.getAdditionalSettings().isEmpty());
    }

    @Test void testMapConstructorAllSettings() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("host", "spark-master");
      map.put("port", "10001");
      map.put("database", "mydb");
      map.put("user", "admin");
      map.put("password", "secret");
      map.put("icebergCatalogType", "hive");
      map.put("icebergWarehouse", "/data/warehouse");
      map.put("maxMemory", "8g");
      map.put("maxThreads", 12);

      SparkConfig config = new SparkConfig(map);
      assertEquals("spark-master", config.getHost());
      assertEquals("10001", config.getPort());
      assertEquals("mydb", config.getDatabase());
      assertEquals("admin", config.getUser());
      assertEquals("secret", config.getPassword());
      assertEquals("hive", config.getIcebergCatalogType());
      assertEquals("/data/warehouse", config.getIcebergWarehouse());
      assertEquals("8g", config.getMaxMemory());
      assertEquals(12, config.getMaxThreads());
    }

    @Test void testMapConstructorPortAsNumber() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("port", Integer.valueOf(10002));
      SparkConfig config = new SparkConfig(map);
      assertEquals("10002", config.getPort());
    }

    @Test void testMapConstructorPortUnknownTypeUsesDefault() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("port", Boolean.FALSE);
      SparkConfig config = new SparkConfig(map);
      assertEquals(SparkConfig.DEFAULT_PORT, config.getPort());
    }

    @Test void testMapConstructorNonNumberThreadsUsesZero() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("maxThreads", "not_a_number");
      SparkConfig config = new SparkConfig(map);
      assertEquals(0, config.getMaxThreads());
    }

    @Test void testMapConstructorAdditionalSettings() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("spark.custom.param", "val");
      SparkConfig config = new SparkConfig(map);
      assertEquals("val", config.getAdditionalSettings().getProperty("spark.custom.param"));
    }

    @Test void testMapConstructorKnownSettingsNotInAdditional() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("host", "spark-master");
      map.put("database", "mydb");
      SparkConfig config = new SparkConfig(map);
      assertNull(config.getAdditionalSettings().getProperty("host"));
      assertNull(config.getAdditionalSettings().getProperty("database"));
    }

    @Test void testFullConstructorNullsUseDefaults() {
      SparkConfig config =
          new SparkConfig(null, null, null, null, null, null, null, null, 0, null);
      assertEquals(SparkConfig.DEFAULT_HOST, config.getHost());
      assertEquals(SparkConfig.DEFAULT_PORT, config.getPort());
      assertEquals(SparkConfig.DEFAULT_DATABASE, config.getDatabase());
      assertEquals(SparkConfig.DEFAULT_ICEBERG_CATALOG_TYPE, config.getIcebergCatalogType());
    }

    @Test void testFullConstructorAllFields() {
      Properties additional = new Properties();
      additional.setProperty("spark.sql.shuffle.partitions", "200");
      SparkConfig config =
          new SparkConfig("spark-host", "10003", "testdb", "user1", "pass1",
          "hive", "/wh", "16g", 24, additional);
      assertEquals("spark-host", config.getHost());
      assertEquals("10003", config.getPort());
      assertEquals("testdb", config.getDatabase());
      assertEquals("user1", config.getUser());
      assertEquals("pass1", config.getPassword());
      assertEquals("hive", config.getIcebergCatalogType());
      assertEquals("/wh", config.getIcebergWarehouse());
      assertEquals("16g", config.getMaxMemory());
      assertEquals(24, config.getMaxThreads());
      assertEquals("200",
          config.getAdditionalSettings().getProperty("spark.sql.shuffle.partitions"));
    }

    @Test void testToSparkSettingsWithMemoryAndThreads() {
      SparkConfig config =
          new SparkConfig("localhost", "10000", "default", null, null,
          "hadoop", null, "4g", 8, null);
      String[] settings = config.toSparkSettings();
      boolean hasMem = false;
      boolean hasCores = false;
      for (String s : settings) {
        if (s.equals("SET spark.executor.memory = 4g")) {
          hasMem = true;
        }
        if (s.equals("SET spark.executor.cores = 8")) {
          hasCores = true;
        }
      }
      assertTrue(hasMem, "Should include executor.memory");
      assertTrue(hasCores, "Should include executor.cores");
    }

    @Test void testToSparkSettingsNullMemoryExcluded() {
      SparkConfig config = new SparkConfig();
      String[] settings = config.toSparkSettings();
      for (String s : settings) {
        assertFalse(s.contains("spark.executor.memory"),
            "Should not include memory when null");
      }
    }

    @Test void testToSparkSettingsEmptyMemoryExcluded() {
      SparkConfig config =
          new SparkConfig("localhost", "10000", "default", null, null,
          "hadoop", null, "", 0, null);
      String[] settings = config.toSparkSettings();
      for (String s : settings) {
        assertFalse(s.contains("spark.executor.memory"),
            "Should not include memory when empty");
      }
    }

    @Test void testToSparkSettingsZeroThreadsExcluded() {
      SparkConfig config = new SparkConfig();
      String[] settings = config.toSparkSettings();
      for (String s : settings) {
        assertFalse(s.contains("spark.executor.cores"),
            "Should not include cores when 0");
      }
    }

    @Test void testToSparkSettingsIncludesAdditional() {
      Properties additional = new Properties();
      additional.setProperty("spark.sql.adaptive.enabled", "true");
      SparkConfig config =
          new SparkConfig("localhost", "10000", "default", null, null,
          "hadoop", null, null, 0, additional);
      String[] settings = config.toSparkSettings();
      boolean found = false;
      for (String s : settings) {
        if (s.contains("spark.sql.adaptive.enabled")) {
          found = true;
        }
      }
      assertTrue(found, "Should include additional settings");
    }

    @Test void testToIcebergCatalogSettingsWithExplicitWarehouse() {
      SparkConfig config =
          new SparkConfig("localhost", "10000", "default", null, null,
          "hadoop", "/data/iceberg", null, 0, null);
      String[] settings = config.toIcebergCatalogSettings("/fallback");
      assertTrue(settings.length >= 3, "Should have at least 3 settings");
      boolean hasType = false;
      boolean hasWarehouse = false;
      for (String s : settings) {
        if (s.contains("type = hadoop")) {
          hasType = true;
        }
        if (s.contains("warehouse = /data/iceberg")) {
          hasWarehouse = true;
        }
      }
      assertTrue(hasType, "Should include catalog type");
      assertTrue(hasWarehouse, "Should use explicit warehouse, not fallback");
    }

    @Test void testToIcebergCatalogSettingsFallbackWarehouse() {
      SparkConfig config =
          new SparkConfig("localhost", "10000", "default", null, null,
          "hadoop", null, null, 0, null);
      String[] settings = config.toIcebergCatalogSettings("/fallback/path");
      boolean hasWarehouse = false;
      for (String s : settings) {
        if (s.contains("warehouse = /fallback/path")) {
          hasWarehouse = true;
        }
      }
      assertTrue(hasWarehouse, "Should use fallback warehouse");
    }

    @Test void testToIcebergCatalogSettingsNullWarehouseExcluded() {
      SparkConfig config =
          new SparkConfig("localhost", "10000", "default", null, null,
          "hadoop", null, null, 0, null);
      String[] settings = config.toIcebergCatalogSettings(null);
      for (String s : settings) {
        assertFalse(s.contains("warehouse"),
            "Should not include warehouse when both null");
      }
    }

    @Test void testIcebergCatalogName() {
      assertEquals("aperio_iceberg", SparkConfig.ICEBERG_CATALOG_NAME);
    }

    @Test void testToStringMasksUserCredentials() {
      SparkConfig config =
          new SparkConfig("localhost", "10000", "default", "admin", "secret",
          "hadoop", null, null, 0, null);
      String str = config.toString();
      assertTrue(str.contains("***"), "User should be masked in toString");
      assertFalse(str.contains("secret"), "Password should not appear in toString");
    }

    @Test void testToStringNullUserShowsNull() {
      SparkConfig config = new SparkConfig();
      String str = config.toString();
      assertTrue(str.contains("user='null'"));
    }

    @Test void testDefaultConstants() {
      assertEquals("localhost", SparkConfig.DEFAULT_HOST);
      assertEquals("10000", SparkConfig.DEFAULT_PORT);
      assertEquals("default", SparkConfig.DEFAULT_DATABASE);
      assertEquals("hadoop", SparkConfig.DEFAULT_ICEBERG_CATALOG_TYPE);
    }
  }

  // =========================================================================
  // TrinoConfig Tests
  // =========================================================================

  @Nested
  class TrinoConfigTests {

    @Test void testDefaultConstructorHost() {
      TrinoConfig config = new TrinoConfig();
      assertEquals("localhost", config.getHost());
    }

    @Test void testDefaultConstructorPort() {
      TrinoConfig config = new TrinoConfig();
      assertEquals("8080", config.getPort());
    }

    @Test void testDefaultConstructorCatalog() {
      TrinoConfig config = new TrinoConfig();
      assertEquals("hive", config.getCatalog());
    }

    @Test void testDefaultConstructorSchema() {
      TrinoConfig config = new TrinoConfig();
      assertEquals("default", config.getSchema());
    }

    @Test void testDefaultConstructorIcebergCatalog() {
      TrinoConfig config = new TrinoConfig();
      assertEquals("iceberg", config.getIcebergCatalog());
    }

    @Test void testDefaultConstructorWarehouseDir() {
      TrinoConfig config = new TrinoConfig();
      assertEquals("/data/warehouse", config.getWarehouseDir());
    }

    @Test void testDefaultConstructorNullCredentials() {
      TrinoConfig config = new TrinoConfig();
      assertNull(config.getUser());
      assertNull(config.getPassword());
      assertNull(config.getS3AccessKey());
      assertNull(config.getS3SecretKey());
      assertNull(config.getS3Endpoint());
    }

    @Test void testDefaultConstructorEmptyAdditionalSettings() {
      TrinoConfig config = new TrinoConfig();
      assertTrue(config.getAdditionalSettings().isEmpty());
    }

    @Test void testMapConstructorAllSettings() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("host", "trino-coordinator");
      map.put("port", "8443");
      map.put("catalog", "myhive");
      map.put("schema", "myschema");
      map.put("user", "admin");
      map.put("password", "secret");
      map.put("icebergCatalog", "myiceberg");
      map.put("warehouseDir", "/opt/warehouse");
      map.put("s3AccessKey", "AKID");
      map.put("s3SecretKey", "SKEY");
      map.put("s3Endpoint", "http://minio:9000");

      TrinoConfig config = new TrinoConfig(map);
      assertEquals("trino-coordinator", config.getHost());
      assertEquals("8443", config.getPort());
      assertEquals("myhive", config.getCatalog());
      assertEquals("myschema", config.getSchema());
      assertEquals("admin", config.getUser());
      assertEquals("secret", config.getPassword());
      assertEquals("myiceberg", config.getIcebergCatalog());
      assertEquals("/opt/warehouse", config.getWarehouseDir());
      assertEquals("AKID", config.getS3AccessKey());
      assertEquals("SKEY", config.getS3SecretKey());
      assertEquals("http://minio:9000", config.getS3Endpoint());
    }

    @Test void testMapConstructorPortAsNumber() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("port", Integer.valueOf(8443));
      TrinoConfig config = new TrinoConfig(map);
      assertEquals("8443", config.getPort());
    }

    @Test void testMapConstructorPortUnknownTypeUsesDefault() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("port", Boolean.TRUE);
      TrinoConfig config = new TrinoConfig(map);
      assertEquals(TrinoConfig.DEFAULT_PORT, config.getPort());
    }

    @Test void testMapConstructorAdditionalSettings() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("query_max_memory", "10GB");
      TrinoConfig config = new TrinoConfig(map);
      assertEquals("10GB", config.getAdditionalSettings().getProperty("query_max_memory"));
    }

    @Test void testMapConstructorKnownSettingsNotInAdditional() {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put("host", "h");
      map.put("port", "p");
      map.put("catalog", "c");
      map.put("schema", "s");
      map.put("user", "u");
      map.put("password", "pw");
      map.put("icebergCatalog", "ic");
      map.put("warehouseDir", "wd");
      map.put("s3AccessKey", "ak");
      map.put("s3SecretKey", "sk");
      map.put("s3Endpoint", "se");
      TrinoConfig config = new TrinoConfig(map);
      Properties additional = config.getAdditionalSettings();
      assertTrue(additional.isEmpty(),
          "All known settings should be excluded: " + additional);
    }

    @Test void testFullConstructorNullsUseDefaults() {
      TrinoConfig config =
          new TrinoConfig(null, null, null, null, null, null, null, null,
          null, null, null, null);
      assertEquals(TrinoConfig.DEFAULT_HOST, config.getHost());
      assertEquals(TrinoConfig.DEFAULT_PORT, config.getPort());
      assertEquals(TrinoConfig.DEFAULT_CATALOG, config.getCatalog());
      assertEquals(TrinoConfig.DEFAULT_SCHEMA, config.getSchema());
      assertEquals(TrinoConfig.DEFAULT_ICEBERG_CATALOG, config.getIcebergCatalog());
      assertEquals(TrinoConfig.DEFAULT_WAREHOUSE_DIR, config.getWarehouseDir());
      assertNull(config.getUser());
      assertNull(config.getS3AccessKey());
    }

    @Test void testFullConstructorAllFields() {
      Properties additional = new Properties();
      additional.setProperty("query_max_run_time", "600s");
      TrinoConfig config =
          new TrinoConfig("host1", "443", "cat1", "sch1", "user1", "pass1",
          "ice1", "/wh1", "ak1", "sk1", "ep1", additional);
      assertEquals("host1", config.getHost());
      assertEquals("443", config.getPort());
      assertEquals("cat1", config.getCatalog());
      assertEquals("sch1", config.getSchema());
      assertEquals("user1", config.getUser());
      assertEquals("pass1", config.getPassword());
      assertEquals("ice1", config.getIcebergCatalog());
      assertEquals("/wh1", config.getWarehouseDir());
      assertEquals("ak1", config.getS3AccessKey());
      assertEquals("sk1", config.getS3SecretKey());
      assertEquals("ep1", config.getS3Endpoint());
      assertEquals("600s",
          config.getAdditionalSettings().getProperty("query_max_run_time"));
    }

    @Test void testToSessionSettingsEmpty() {
      TrinoConfig config = new TrinoConfig();
      String[] settings = config.toSessionSettings();
      assertEquals(0, settings.length);
    }

    @Test void testToSessionSettingsWithAdditional() {
      Properties additional = new Properties();
      additional.setProperty("query_max_memory", "10GB");
      TrinoConfig config =
          new TrinoConfig("localhost", "8080", "hive", "default", null, null,
          "iceberg", "/data/warehouse", null, null, null, additional);
      String[] settings = config.toSessionSettings();
      assertEquals(1, settings.length);
      assertTrue(settings[0].startsWith("SET SESSION "));
      assertTrue(settings[0].contains("query_max_memory"));
      assertTrue(settings[0].contains("'10GB'"));
    }

    @Test void testGetHiveCatalogPropertiesBasic() {
      TrinoConfig config = new TrinoConfig();
      Properties props = config.getHiveCatalogProperties();
      assertEquals("hive", props.getProperty("connector.name"));
      assertEquals("file", props.getProperty("hive.metastore"));
      assertEquals("/data/warehouse", props.getProperty("hive.metastore.catalog.dir"));
      assertEquals("allow-all", props.getProperty("hive.security"));
      assertEquals("true", props.getProperty("hive.non-managed-table-writes-enabled"));
      assertNull(props.getProperty("hive.s3.aws-access-key"));
    }

    @Test void testGetHiveCatalogPropertiesWithS3() {
      TrinoConfig config =
          new TrinoConfig("localhost", "8080", "hive", "default", null, null,
          "iceberg", "/wh", "AKID", "SKEY", "http://minio:9000", null);
      Properties props = config.getHiveCatalogProperties();
      assertEquals("AKID", props.getProperty("hive.s3.aws-access-key"));
      assertEquals("SKEY", props.getProperty("hive.s3.aws-secret-key"));
      assertEquals("http://minio:9000", props.getProperty("hive.s3.endpoint"));
      assertEquals("true", props.getProperty("hive.s3.path-style-access"));
    }

    @Test void testGetHiveCatalogPropertiesWithS3NoEndpoint() {
      TrinoConfig config =
          new TrinoConfig("localhost", "8080", "hive", "default", null, null,
          "iceberg", "/wh", "AKID", "SKEY", null, null);
      Properties props = config.getHiveCatalogProperties();
      assertEquals("AKID", props.getProperty("hive.s3.aws-access-key"));
      assertEquals("SKEY", props.getProperty("hive.s3.aws-secret-key"));
      assertNull(props.getProperty("hive.s3.endpoint"));
      assertNull(props.getProperty("hive.s3.path-style-access"));
    }

    @Test void testGetIcebergCatalogPropertiesBasic() {
      TrinoConfig config = new TrinoConfig();
      Properties props = config.getIcebergCatalogProperties();
      assertEquals("iceberg", props.getProperty("connector.name"));
      assertEquals("TESTING_FILE_METASTORE", props.getProperty("iceberg.catalog.type"));
      assertEquals("/data/warehouse", props.getProperty("hive.metastore.catalog.dir"));
      assertNull(props.getProperty("hive.s3.aws-access-key"));
    }

    @Test void testGetIcebergCatalogPropertiesWithS3() {
      TrinoConfig config =
          new TrinoConfig("localhost", "8080", "hive", "default", null, null,
          "iceberg", "/wh", "AKID", "SKEY", "http://minio:9000", null);
      Properties props = config.getIcebergCatalogProperties();
      assertEquals("AKID", props.getProperty("hive.s3.aws-access-key"));
      assertEquals("SKEY", props.getProperty("hive.s3.aws-secret-key"));
      assertEquals("http://minio:9000", props.getProperty("hive.s3.endpoint"));
      assertEquals("true", props.getProperty("hive.s3.path-style-access"));
    }

    @Test void testGenerateCatalogFilesCreatesFiles(@TempDir Path tempDir) throws IOException {
      TrinoConfig config =
          new TrinoConfig("localhost", "8080", "hive", "default", null, null,
          "iceberg", "/data/warehouse", null, null, null, null);
      String outputDir = tempDir.resolve("catalog").toString();
      config.generateCatalogFiles(outputDir);

      File hiveFile = new File(outputDir, "hive.properties");
      File icebergFile = new File(outputDir, "iceberg.properties");
      assertTrue(hiveFile.exists(), "Hive catalog file should exist");
      assertTrue(icebergFile.exists(), "Iceberg catalog file should exist");
      assertTrue(hiveFile.length() > 0, "Hive catalog file should have content");
      assertTrue(icebergFile.length() > 0, "Iceberg catalog file should have content");
    }

    @Test void testGenerateCatalogFilesUsesCustomCatalogNames(@TempDir Path tempDir)
        throws IOException {
      TrinoConfig config =
          new TrinoConfig("localhost", "8080", "mycatalog", "default", null, null,
          "myiceberg", "/data/warehouse", null, null, null, null);
      String outputDir = tempDir.resolve("catalog").toString();
      config.generateCatalogFiles(outputDir);

      File hiveFile = new File(outputDir, "mycatalog.properties");
      File icebergFile = new File(outputDir, "myiceberg.properties");
      assertTrue(hiveFile.exists(), "Custom hive catalog file should exist");
      assertTrue(icebergFile.exists(), "Custom iceberg catalog file should exist");
    }

    @Test void testGenerateCatalogFilesCreatesDirectoryIfNeeded(@TempDir Path tempDir)
        throws IOException {
      String outputDir = tempDir.resolve("nested/dir/catalog").toString();
      TrinoConfig config = new TrinoConfig();
      config.generateCatalogFiles(outputDir);

      File dir = new File(outputDir);
      assertTrue(dir.exists() && dir.isDirectory());
    }

    @Test void testToStringMasksCredentials() {
      TrinoConfig config =
          new TrinoConfig("localhost", "8080", "hive", "default", "admin", "secret",
          "iceberg", "/wh", null, null, null, null);
      String str = config.toString();
      assertTrue(str.contains("***"), "User should be masked");
      assertFalse(str.contains("secret"), "Password should not appear");
    }

    @Test void testToStringNullUserShowsNull() {
      TrinoConfig config = new TrinoConfig();
      String str = config.toString();
      assertTrue(str.contains("user='null'"));
    }

    @Test void testDefaultConstants() {
      assertEquals("localhost", TrinoConfig.DEFAULT_HOST);
      assertEquals("8080", TrinoConfig.DEFAULT_PORT);
      assertEquals("hive", TrinoConfig.DEFAULT_CATALOG);
      assertEquals("default", TrinoConfig.DEFAULT_SCHEMA);
      assertEquals("iceberg", TrinoConfig.DEFAULT_ICEBERG_CATALOG);
      assertEquals("/data/warehouse", TrinoConfig.DEFAULT_WAREHOUSE_DIR);
    }
  }

  // =========================================================================
  // Cross-Engine Configuration Interaction Tests
  // =========================================================================

  @Nested
  class CrossEngineTests {

    @Test void testDuckDBConfigInExecutionEngineConfig() {
      DuckDBConfig duckDBConfig =
          new DuckDBConfig("4GB", 8, "90%", "/tmp", true, false, true, 2048, null);
      ExecutionEngineConfig config =
          new ExecutionEngineConfig("duckdb", 2048,
              ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD, null, duckDBConfig);
      assertEquals(ExecutionEngineConfig.ExecutionEngineType.DUCKDB, config.getEngineType());
      assertEquals("4GB", config.getDuckDBConfig().getMemoryLimit());
      assertEquals(8, config.getDuckDBConfig().getThreads());
    }

    @Test void testParquetEngineWithParquetCacheDir() {
      ExecutionEngineConfig config =
          new ExecutionEngineConfig("parquet", 4096,
              128L * 1024 * 1024, "/tmp/mv", null, "/data/parquet-cache");
      assertEquals(ExecutionEngineConfig.ExecutionEngineType.PARQUET, config.getEngineType());
      assertEquals("/data/parquet-cache", config.getParquetCacheDirectory());
      assertEquals(4096, config.getBatchSize());
    }

    @Test void testAllEngineConfigsCanBeInstantiatedWithDefaults() {
      // Verify no exceptions when creating default configs for all engines
      assertNotNull(new DuckDBConfig());
      assertNotNull(new ParquetConfig(2048, false));
      assertNotNull(new ClickHouseConfig());
      assertNotNull(new SparkConfig());
      assertNotNull(new TrinoConfig());
    }

    @Test void testAllEngineTypesCanBeUsedInExecutionEngineConfig() {
      for (String engine : ExecutionEngineConfig.getAvailableEngineTypes()) {
        ExecutionEngineConfig config = new ExecutionEngineConfig(engine, 2048);
        assertNotNull(config.getEngineType());
        assertNotNull(config.getDuckDBConfig());
      }
    }
  }
}
