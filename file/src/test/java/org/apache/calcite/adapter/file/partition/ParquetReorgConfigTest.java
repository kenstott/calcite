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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParquetReorganizer.ReorgConfig} and its builder.
 */
@Tag("unit")
public class ParquetReorgConfigTest {

  // ===== Builder validation =====

  @Test void testBuilderRequiresSourcePattern() {
    assertThrows(IllegalArgumentException.class, () -> {
      ParquetReorganizer.ReorgConfig.builder()
          .targetBase("target")
          .build();
    });
  }

  @Test void testBuilderRequiresTargetBase() {
    assertThrows(IllegalArgumentException.class, () -> {
      ParquetReorganizer.ReorgConfig.builder()
          .sourcePattern("source/*.parquet")
          .build();
    });
  }

  @Test void testBuilderRejectsEmptySourcePattern() {
    assertThrows(IllegalArgumentException.class, () -> {
      ParquetReorganizer.ReorgConfig.builder()
          .sourcePattern("")
          .targetBase("target")
          .build();
    });
  }

  @Test void testBuilderRejectsEmptyTargetBase() {
    assertThrows(IllegalArgumentException.class, () -> {
      ParquetReorganizer.ReorgConfig.builder()
          .sourcePattern("source/*.parquet")
          .targetBase("")
          .build();
    });
  }

  // ===== Minimal config =====

  @Test void testMinimalConfig() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("year=*/*.parquet")
        .targetBase("output")
        .build();

    assertEquals("year=*/*.parquet", config.getSourcePattern());
    assertEquals("output", config.getTargetBase());
    assertNotNull(config.getPartitionColumns());
    assertTrue(config.getPartitionColumns().isEmpty());
    assertNotNull(config.getColumnMappings());
    assertTrue(config.getColumnMappings().isEmpty());
    assertNotNull(config.getBatchPartitionColumns());
    assertTrue(config.getBatchPartitionColumns().isEmpty());
    assertEquals(0, config.getStartYear());
    assertEquals(0, config.getEndYear());
    assertNull(config.getName());
    assertEquals(2, config.getThreads()); // default
    assertNotNull(config.getIncrementalKeys());
    assertTrue(config.getIncrementalKeys().isEmpty());
    assertSame(IncrementalTracker.NOOP, config.getIncrementalTracker());
    assertNull(config.getSourceTable());
    assertFalse(config.isSourceIsIceberg());
    assertNull(config.getIcebergWarehousePath());
    assertEquals(1, config.getCurrentYearTtlDays()); // default
    assertFalse(config.supportsIncremental());
  }

  // ===== Full config =====

  @Test void testFullConfig() {
    List<String> partCols = Arrays.asList("geo", "year");
    Map<String, String> colMappings = new LinkedHashMap<String, String>();
    colMappings.put("geo", "GeoFips");
    List<String> batchCols = Arrays.asList("year", "geo_fips_set");
    List<String> incKeys = Collections.singletonList("year");

    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("year=*/geo=*/*.parquet")
        .targetBase("type=income_by_geo")
        .partitionColumns(partCols)
        .columnMappings(colMappings)
        .batchPartitionColumns(batchCols)
        .yearRange(2020, 2024)
        .name("income_reorg")
        .threads(4)
        .incrementalKeys(incKeys)
        .incrementalTracker(IncrementalTracker.NOOP)
        .sourceTable("regional_income")
        .sourceIsIceberg(true)
        .icebergWarehousePath("/warehouse")
        .currentYearTtlDays(7)
        .build();

    assertEquals("year=*/geo=*/*.parquet", config.getSourcePattern());
    assertEquals("type=income_by_geo", config.getTargetBase());
    assertEquals(partCols, config.getPartitionColumns());
    assertEquals(colMappings, config.getColumnMappings());
    assertEquals(batchCols, config.getBatchPartitionColumns());
    assertEquals(2020, config.getStartYear());
    assertEquals(2024, config.getEndYear());
    assertEquals("income_reorg", config.getName());
    assertEquals(4, config.getThreads());
    assertEquals(incKeys, config.getIncrementalKeys());
    assertEquals("regional_income", config.getSourceTable());
    assertTrue(config.isSourceIsIceberg());
    assertEquals("/warehouse", config.getIcebergWarehousePath());
    assertEquals(7, config.getCurrentYearTtlDays());
  }

  // ===== Defaults for specific fields =====

  @Test void testDefaultThreads() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("out")
        .threads(0) // 0 should fall back to default
        .build();

    assertEquals(2, config.getThreads());
  }

  @Test void testNegativeThreadsUsesDefault() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("out")
        .threads(-1)
        .build();

    assertEquals(2, config.getThreads());
  }

  @Test void testDefaultCurrentYearTtlDays() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("out")
        .currentYearTtlDays(0) // 0 should fall back to default
        .build();

    assertEquals(1, config.getCurrentYearTtlDays());
  }

  @Test void testCurrentYearTtlMillis() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("out")
        .currentYearTtlDays(7)
        .build();

    assertEquals(7L * 24 * 60 * 60 * 1000, config.getCurrentYearTtlMillis());
  }

  @Test void testDefaultCurrentYearTtlMillis() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("out")
        .build();

    // Default 1 day = 86400000 ms
    assertEquals(86400000L, config.getCurrentYearTtlMillis());
  }

  // ===== supportsIncremental =====

  @Test void testSupportsIncrementalTrue() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("out")
        .incrementalKeys(Collections.singletonList("year"))
        .incrementalTracker(
            DuckDBPartitionStatusStore.getInstance(
            System.getProperty("java.io.tmpdir") + "/test_reorg_" + System.nanoTime()))
        .build();

    assertTrue(config.supportsIncremental());
  }

  @Test void testSupportsIncrementalFalseNoKeys() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("out")
        .build();

    assertFalse(config.supportsIncremental());
  }

  @Test void testSupportsIncrementalFalseNoopTracker() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("out")
        .incrementalKeys(Collections.singletonList("year"))
        // Default tracker is NOOP
        .build();

    assertFalse(config.supportsIncremental());
  }

  // ===== Null collection defaults =====

  @Test void testNullPartitionColumnsDefaultsToEmpty() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("out")
        .partitionColumns(null)
        .build();

    assertNotNull(config.getPartitionColumns());
    assertTrue(config.getPartitionColumns().isEmpty());
  }

  @Test void testNullColumnMappingsDefaultsToEmpty() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("out")
        .columnMappings(null)
        .build();

    assertNotNull(config.getColumnMappings());
    assertTrue(config.getColumnMappings().isEmpty());
  }

  @Test void testNullBatchPartitionColumnsDefaultsToEmpty() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("out")
        .batchPartitionColumns(null)
        .build();

    assertNotNull(config.getBatchPartitionColumns());
    assertTrue(config.getBatchPartitionColumns().isEmpty());
  }

  @Test void testNullIncrementalKeysDefaultsToEmpty() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("out")
        .incrementalKeys(null)
        .build();

    assertNotNull(config.getIncrementalKeys());
    assertTrue(config.getIncrementalKeys().isEmpty());
  }

  @Test void testNullTrackerDefaultsToNoop() {
    ParquetReorganizer.ReorgConfig config = ParquetReorganizer.ReorgConfig.builder()
        .sourcePattern("*.parquet")
        .targetBase("out")
        .incrementalTracker(null)
        .build();

    assertSame(IncrementalTracker.NOOP, config.getIncrementalTracker());
  }
}
