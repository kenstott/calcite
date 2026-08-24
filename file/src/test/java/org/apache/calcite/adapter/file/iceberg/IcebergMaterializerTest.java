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
import java.util.Collections;
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

  @Test void testMaterializationConfigCompactionDefaultsToOn() {
    // A config that never calls the compaction/maintenance builder methods (e.g. a table whose
    // schema declares no materialize.iceberg block) must still compact and expire snapshots --
    // matching this class's historical unconditional behavior -- rather than silently going dark
    // because the caller did not know to wire the knobs up.
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .build();

    assertEquals(true, config.isRunCompaction());
    assertEquals(128L * 1024 * 1024, config.getCompactionTargetFileSizeBytes());
    assertEquals(10, config.getCompactionMinFiles());
    assertEquals(10L * 1024 * 1024, config.getCompactionSmallFileSizeBytes());
    assertEquals(true, config.isRunMaintenance());
    assertEquals(7, config.getSnapshotRetentionDays());
    assertEquals(Collections.emptyList(), config.getSortOrder());
  }

  @Test void testMaterializationConfigCompactionHonorsSchemaOverrides() {
    // Values as SecSchemaFactory#buildMaterializationConfig reads them out of a table's
    // materialize.iceberg YAML block (e.g. sec-schema.yaml's iceberg_defaults anchor).
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .runCompaction(true)
            .compactionTargetFileSizeBytes(134217728L)
            .compactionMinFiles(5)
            .compactionSmallFileSizeBytes(5L * 1024 * 1024)
            .runMaintenance(true)
            .snapshotRetentionDays(14)
            .sortOrder(Arrays.asList("cik", "accession_number"))
            .build();

    assertEquals(true, config.isRunCompaction());
    assertEquals(134217728L, config.getCompactionTargetFileSizeBytes());
    assertEquals(5, config.getCompactionMinFiles());
    assertEquals(5L * 1024 * 1024, config.getCompactionSmallFileSizeBytes());
    assertEquals(true, config.isRunMaintenance());
    assertEquals(14, config.getSnapshotRetentionDays());
    assertEquals(Arrays.asList("cik", "accession_number"), config.getSortOrder());

    // An explicit false must stick -- distinguishing "never called" (defaults to true) from
    // "called with false" is the whole point of the runCompactionSet/runMaintenanceSet flags.
    IcebergMaterializer.MaterializationConfig disabled =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern("data/*.parquet")
            .targetTableId("test")
            .runCompaction(false)
            .runMaintenance(false)
            .build();
    assertEquals(false, disabled.isRunCompaction());
    assertEquals(false, disabled.isRunMaintenance());
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
