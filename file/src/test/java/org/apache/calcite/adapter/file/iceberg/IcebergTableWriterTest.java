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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for IcebergTableWriter.
 */
@Tag("unit")
public class IcebergTableWriterTest {

  @TempDir
  Path tempDir;

  private Map<String, Object> catalogConfig;
  private Table table;
  private StorageProvider storageProvider;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
    String warehousePath = tempDir.resolve("warehouse").toString();
    catalogConfig = new HashMap<>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", warehousePath);

    // Create a test table
    Schema schema =
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "year", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("year")
        .build();

    table = IcebergCatalogManager.createTable(catalogConfig, "test_table", schema, spec);
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  @Test void testWriterCreation() {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    assertNotNull(writer);
    assertNotNull(writer.getTable());
  }

  @Test void testMaintenanceDoesNotThrow() {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    // Should not throw even on empty table
    writer.runMaintenance(7, 1);
  }

  @Test void testCommitFromStagingEmptyDirectory() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    // Create an empty staging directory
    Path stagingPath = tempDir.resolve("staging");
    Files.createDirectories(stagingPath);

    // Should not throw, just log warning
    writer.commitFromStaging(stagingPath.toString(), null);
  }

  private static final long TARGET_128MB = 128L * 1024 * 1024;

  /**
   * A highly-compressible table (FAOSTAT: ~0.25 on-disk bytes/record) must NOT drive the
   * records-per-file target into the hundreds of millions — that is the sizing bug that funneled
   * 64M records through one Parquet writer and OOM'd the worker. It is capped at 2M/file.
   */
  @Test void testComputeRecordsPerFileCapsHighlyCompressibleTable() {
    // 64M records in ~16MB on-disk == ~0.26 bytes/record. Unbounded, this was ~539M records/file.
    int recordsPerFile = IcebergTableWriter.computeRecordsPerFile(
        16L * 1024 * 1024, 64_000_000L, TARGET_128MB);
    assertEquals(2_000_000, recordsPerFile,
        "highly-compressible table must roll at the 2M-record ceiling, not stream to one file");
  }

  /** A normally-sized table (~256 bytes/record) sizes from the byte estimate, under the ceiling. */
  @Test void testComputeRecordsPerFileNormalTableUsesByteEstimate() {
    // 128MB target / 256 bytes/record == 512K records/file.
    int recordsPerFile = IcebergTableWriter.computeRecordsPerFile(
        256L * 1_000_000L, 1_000_000L, TARGET_128MB);
    assertEquals(524288, recordsPerFile);
    assertTrue(recordsPerFile < 2_000_000);
  }

  /** A wide/sparse table (huge bytes/record) still batches at the 1000-record floor. */
  @Test void testComputeRecordsPerFileFloorsWideTable() {
    // 10MB/record would compute to 12 records/file — floored to 1000.
    int recordsPerFile = IcebergTableWriter.computeRecordsPerFile(
        10L * 1024 * 1024 * 100L, 100L, TARGET_128MB);
    assertEquals(1000, recordsPerFile);
  }

  /** Zero records (degenerate partition) returns the floor without dividing by zero. */
  @Test void testComputeRecordsPerFileHandlesZeroRecords() {
    assertEquals(1000, IcebergTableWriter.computeRecordsPerFile(0L, 0L, TARGET_128MB));
  }
}
