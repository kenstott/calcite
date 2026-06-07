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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig.ColumnDefinition;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for IcebergMaterializer.
 */
@Tag("unit")
public class IcebergMaterializerTest {

  @TempDir
  Path tempDir;

  private IcebergMaterializer materializer;

  @BeforeEach
  void setUp() {
    String warehousePath = tempDir.resolve("warehouse").toString();
    materializer =
        new IcebergMaterializer(warehousePath,
        new LocalFileStorageProvider(),
        IncrementalTracker.NOOP);
  }

  @Test void testMaterializationConfigBuilder() {
    List<ColumnDefinition> partitionCols = new ArrayList<ColumnDefinition>();
    partitionCols.add(new ColumnDefinition("geo", "VARCHAR"));

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("type=income/year=*/*.parquet")
            .sourceFormat(IcebergMaterializer.SourceFormat.PARQUET)
            .targetTableId("income_by_geo")
            .sourceTableName("regional_income")
            .partitionColumns(partitionCols)
            .batchPartitionColumns(Arrays.asList("year"))
            .incrementalKeys(Arrays.asList("year"))
            .yearRange(2020, 2024)
            .threads(4)
            .description("Income by geography")
            .build();

    assertNotNull(config);
    assertEquals("type=income/year=*/*.parquet", config.getSourcePattern());
    assertEquals("income_by_geo", config.getTargetTableId());
    assertEquals("regional_income", config.getSourceTableName());
    assertEquals(IcebergMaterializer.SourceFormat.PARQUET, config.getSourceFormat());
    assertEquals(Arrays.asList("year"), config.getBatchPartitionColumns());
    assertEquals(Arrays.asList("year"), config.getIncrementalKeys());
    assertEquals(2020, config.getStartYear());
    assertEquals(2024, config.getEndYear());
    assertEquals(4, config.getThreads());
    assertEquals("Income by geography", config.getDescription());
  }

  @Test void testMaterializationConfigSupportsIncremental() {
    // With incremental keys
    IcebergMaterializer.MaterializationConfig withKeys =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .incrementalKeys(Arrays.asList("year"))
            .build();
    assertEquals(true, withKeys.supportsIncremental());

    // Without incremental keys
    IcebergMaterializer.MaterializationConfig withoutKeys =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();
    assertEquals(false, withoutKeys.supportsIncremental());
  }

  @Test void testMaterializationResult() {
    IcebergMaterializer.MaterializationResult result =
        new IcebergMaterializer.MaterializationResult(
            "test_table", 10, 0, 5, 1500);

    assertEquals("test_table", result.getTableId());
    assertEquals(10, result.getSuccessCount());
    assertEquals(0, result.getFailedCount());
    assertEquals(5, result.getSkippedCount());
    assertEquals(1500, result.getDurationMs());
    assertEquals(true, result.isFullySuccessful());

    // With failures
    IcebergMaterializer.MaterializationResult withFailures =
        new IcebergMaterializer.MaterializationResult(
            "test_table", 8, 2, 5, 1500);
    assertEquals(false, withFailures.isFullySuccessful());
  }

  @Test void testMaterializerCreation() {
    assertNotNull(materializer);
  }

  @Test void testMaterializerWithCustomRetrySettings() {
    IcebergMaterializer customMaterializer =
        new IcebergMaterializer(tempDir.resolve("warehouse").toString(),
        new LocalFileStorageProvider(),
        IncrementalTracker.NOOP,
        5,    // maxRetries
        2000);  // retryDelayMs
    assertNotNull(customMaterializer);
  }
}
