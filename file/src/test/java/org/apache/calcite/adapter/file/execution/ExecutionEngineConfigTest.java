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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ExecutionEngineConfig}.
 */
@Tag("unit")
public class ExecutionEngineConfigTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ExecutionEngineConfigTest.class);

  @Test
  public void testDefaultConstructorUsesDefaults() {
    ExecutionEngineConfig config = new ExecutionEngineConfig();
    assertNotNull(config.getEngineType());
    assertEquals(ExecutionEngineConfig.DEFAULT_BATCH_SIZE, config.getBatchSize());
    assertEquals(ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD, config.getMemoryThreshold());
    assertNull(config.getMaterializedViewStoragePath());
    assertFalse(config.hasCustomStoragePath());
    assertNotNull(config.getDuckDBConfig());
    assertNull(config.getParquetCacheDirectory());
    LOGGER.debug("Default config engine type: {}", config.getEngineType());
  }

  @Test
  public void testTwoArgConstructorSetsEngineAndBatchSize() {
    ExecutionEngineConfig config = new ExecutionEngineConfig("linq4j", 4096);
    assertEquals(ExecutionEngineConfig.ExecutionEngineType.LINQ4J, config.getEngineType());
    assertEquals(4096, config.getBatchSize());
    assertEquals(ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD, config.getMemoryThreshold());
    assertNull(config.getMaterializedViewStoragePath());
    assertFalse(config.hasCustomStoragePath());
  }

  @Test
  public void testThreeArgConstructorSetsMaterializedViewPath() {
    ExecutionEngineConfig config =
        new ExecutionEngineConfig("parquet", 2048, "/tmp/views");
    assertEquals(ExecutionEngineConfig.ExecutionEngineType.PARQUET, config.getEngineType());
    assertEquals(2048, config.getBatchSize());
    assertEquals("/tmp/views", config.getMaterializedViewStoragePath());
    assertTrue(config.hasCustomStoragePath());
  }

  @Test
  public void testFourArgConstructorSetsMemoryThreshold() {
    long customMemory = 128L * 1024 * 1024;
    ExecutionEngineConfig config =
        new ExecutionEngineConfig("arrow", 1024, customMemory, "/tmp/views");
    assertEquals(ExecutionEngineConfig.ExecutionEngineType.ARROW, config.getEngineType());
    assertEquals(1024, config.getBatchSize());
    assertEquals(customMemory, config.getMemoryThreshold());
    assertEquals("/tmp/views", config.getMaterializedViewStoragePath());
    assertTrue(config.hasCustomStoragePath());
  }

  @Test
  public void testFiveArgConstructorSetsDuckDBConfig() {
    DuckDBConfig duckdbConfig = new DuckDBConfig();
    ExecutionEngineConfig config =
        new ExecutionEngineConfig("duckdb", 2048,
            ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD, null, duckdbConfig);
    assertEquals(ExecutionEngineConfig.ExecutionEngineType.DUCKDB, config.getEngineType());
    assertNotNull(config.getDuckDBConfig());
    assertFalse(config.hasCustomStoragePath());
  }

  @Test
  public void testSixArgConstructorSetsParquetCacheDirectory() {
    ExecutionEngineConfig config =
        new ExecutionEngineConfig("parquet", 2048,
            ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD,
            "/tmp/views", null, "/tmp/cache");
    assertEquals(ExecutionEngineConfig.ExecutionEngineType.PARQUET, config.getEngineType());
    assertEquals("/tmp/cache", config.getParquetCacheDirectory());
    assertTrue(config.hasCustomStoragePath());
  }

  @Test
  public void testNullMaterializedViewPathSetsUseCustomStoragePathFalse() {
    ExecutionEngineConfig config =
        new ExecutionEngineConfig("parquet", 2048,
            ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD, null);
    assertNull(config.getMaterializedViewStoragePath());
    assertFalse(config.hasCustomStoragePath());
  }

  @Test
  public void testNullDuckDBConfigCreatesDefault() {
    ExecutionEngineConfig config =
        new ExecutionEngineConfig("duckdb", 2048,
            ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD, null, null);
    assertNotNull(config.getDuckDBConfig());
  }

  @Test
  public void testParseAllValidEngineTypes() {
    String[] engines = ExecutionEngineConfig.getAvailableEngineTypes();
    for (String engine : engines) {
      ExecutionEngineConfig config = new ExecutionEngineConfig(engine, 2048);
      assertNotNull(config.getEngineType(),
          "Engine type should be parsed for: " + engine);
      LOGGER.debug("Parsed engine type: {} -> {}", engine, config.getEngineType());
    }
  }

  @Test
  public void testParseEngineTypeIsCaseInsensitive() {
    ExecutionEngineConfig lower = new ExecutionEngineConfig("duckdb", 2048);
    ExecutionEngineConfig upper = new ExecutionEngineConfig("DUCKDB", 2048);
    ExecutionEngineConfig mixed = new ExecutionEngineConfig("DuckDB", 2048);
    assertEquals(lower.getEngineType(), upper.getEngineType());
    assertEquals(lower.getEngineType(), mixed.getEngineType());
  }

  @Test
  public void testInvalidEngineTypeThrowsIllegalArgument() {
    assertThrows(IllegalArgumentException.class,
        () -> new ExecutionEngineConfig("invalid_engine", 2048));
  }

  @Test
  public void testInvalidEngineTypeExceptionContainsValidOptions() {
    try {
      new ExecutionEngineConfig("nonexistent", 2048);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("linq4j"),
          "Exception should list valid engine types");
      assertTrue(e.getMessage().contains("duckdb"),
          "Exception should list valid engine types");
      LOGGER.debug("Exception message: {}", e.getMessage());
    }
  }

  @Test
  public void testGetAvailableEngineTypes() {
    String[] types = ExecutionEngineConfig.getAvailableEngineTypes();
    assertNotNull(types);
    assertTrue(types.length >= 4,
        "Should have at least 4 engine types");
    boolean hasDuckdb = false;
    boolean hasParquet = false;
    boolean hasLinq4j = false;
    for (String type : types) {
      if ("duckdb".equals(type)) {
        hasDuckdb = true;
      }
      if ("parquet".equals(type)) {
        hasParquet = true;
      }
      if ("linq4j".equals(type)) {
        hasLinq4j = true;
      }
    }
    assertTrue(hasDuckdb, "Should include duckdb");
    assertTrue(hasParquet, "Should include parquet");
    assertTrue(hasLinq4j, "Should include linq4j");
  }

  @Test
  public void testDefaultBatchSize() {
    assertEquals(2048, ExecutionEngineConfig.DEFAULT_BATCH_SIZE);
  }

  @Test
  public void testDefaultMemoryThreshold() {
    assertEquals(64L * 1024 * 1024, ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD);
  }

  @Test
  public void testDefaultExecutionEngine() {
    assertNotNull(ExecutionEngineConfig.DEFAULT_EXECUTION_ENGINE);
    assertEquals("parquet", ExecutionEngineConfig.DEFAULT_EXECUTION_ENGINE);
  }

  @Test
  public void testAllEngineTypeEnumValues() {
    ExecutionEngineConfig.ExecutionEngineType[] values =
        ExecutionEngineConfig.ExecutionEngineType.values();
    assertTrue(values.length >= 8,
        "Should have at least 8 engine types in enum");

    // Verify specific engine types exist
    assertNotNull(ExecutionEngineConfig.ExecutionEngineType.valueOf("LINQ4J"));
    assertNotNull(ExecutionEngineConfig.ExecutionEngineType.valueOf("ARROW"));
    assertNotNull(ExecutionEngineConfig.ExecutionEngineType.valueOf("VECTORIZED"));
    assertNotNull(ExecutionEngineConfig.ExecutionEngineType.valueOf("PARQUET"));
    assertNotNull(ExecutionEngineConfig.ExecutionEngineType.valueOf("DUCKDB"));
    assertNotNull(ExecutionEngineConfig.ExecutionEngineType.valueOf("TRINO"));
    assertNotNull(ExecutionEngineConfig.ExecutionEngineType.valueOf("SPARK"));
    assertNotNull(ExecutionEngineConfig.ExecutionEngineType.valueOf("CLICKHOUSE"));
  }

  @Test
  public void testVectorizedEngineConfig() {
    ExecutionEngineConfig config = new ExecutionEngineConfig("vectorized", 512);
    assertEquals(ExecutionEngineConfig.ExecutionEngineType.VECTORIZED, config.getEngineType());
    assertEquals(512, config.getBatchSize());
  }

  @Test
  public void testTrinoEngineConfig() {
    ExecutionEngineConfig config = new ExecutionEngineConfig("trino", 8192);
    assertEquals(ExecutionEngineConfig.ExecutionEngineType.TRINO, config.getEngineType());
  }

  @Test
  public void testSparkEngineConfig() {
    ExecutionEngineConfig config = new ExecutionEngineConfig("spark", 4096);
    assertEquals(ExecutionEngineConfig.ExecutionEngineType.SPARK, config.getEngineType());
  }

  @Test
  public void testClickhouseEngineConfig() {
    ExecutionEngineConfig config = new ExecutionEngineConfig("clickhouse", 4096);
    assertEquals(ExecutionEngineConfig.ExecutionEngineType.CLICKHOUSE, config.getEngineType());
  }
}
