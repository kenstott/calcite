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
    writer.runMaintenance(7);
  }

  @Test void testCommitFromStagingEmptyDirectory() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    // Create an empty staging directory
    Path stagingPath = tempDir.resolve("staging");
    Files.createDirectories(stagingPath);

    // Should not throw, just log warning
    writer.commitFromStaging(stagingPath.toString(), null);
  }

  // ---- deleteRows tests — the force-reprocess correction primitive: a targeted, row-level
  // delete keyed on an arbitrary column (e.g. accession_number), not the whole-partition scope
  // deletePartition has. Verified against real committed rows (countRows), not just "doesn't
  // throw" — that distinction is exactly what this method exists to get right.

  @Test void testDeleteRowsNullColumn() {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    assertTrue(org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
        () -> writer.deleteRows(null, java.util.Collections.singleton("x")))
        .getMessage().contains("Column"));
  }

  @Test void testDeleteRowsEmptyColumn() {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
        () -> writer.deleteRows("", java.util.Collections.singleton("x")));
  }

  @Test void testDeleteRowsNullOrEmptyValuesIsNoop() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeAndCommitRow(writer, 1, "keep-me", 2024);
    assertEquals(1, countRows());

    // Neither call should touch the table — no values means nothing to delete.
    writer.deleteRows("data", null);
    writer.deleteRows("data", java.util.Collections.<String>emptySet());
    assertEquals(1, countRows(), "no-op deleteRows must not remove any row");
  }

  @Test void testDeleteRowsRemovesOnlyMatchingRows() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeAndCommitRow(writer, 1, "wrong-accession-a", 2024);
    writeAndCommitRow(writer, 2, "wrong-accession-b", 2024);
    writeAndCommitRow(writer, 3, "unrelated-accession", 2024);
    assertEquals(3, countRows());

    // The force-reprocess scenario: two accessions are being corrected, one is untouched.
    writer.deleteRows("data",
        new java.util.HashSet<>(java.util.Arrays.asList("wrong-accession-a", "wrong-accession-b")));

    assertEquals(1, countRows(), "only the two targeted rows should be deleted");
    assertEquals("unrelated-accession", onlyRemainingDataValue());
  }

  @Test void testDeleteRowsThenRewriteLandsTheCorrection() throws Exception {
    // The exact scenario this method exists for: an accession's row is wrong, force-reprocess
    // re-derives it correctly, but a plain write would be silently excluded by the
    // accession-already-present dedup elsewhere in the pipeline. This test operates one level
    // down — proving deleteRows itself makes room for the replacement to land without duplicating.
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeAndCommitRow(writer, 1, "acc-123", 2024);
    assertEquals(1, countRows());

    writer.deleteRows("data", java.util.Collections.singleton("acc-123"));
    assertEquals(0, countRows(), "old wrong row must be gone before the correction writes");

    writeAndCommitRow(writer, 1, "acc-123", 2024);
    assertEquals(1, countRows(), "corrected row lands as exactly one row, not a duplicate");
  }

  // ---- deleteRows(Map) tests — the composite-key generalization of deleteRows(column, values),
  // for tables whose reprocess/rewrite unit isn't a single column (e.g. ref.vectorized_chunks'
  // generalized (source_schema, source_table, stringified_fk) identity). Reuses the same
  // (data, year) columns as an AND-of-two-equalities stand-in for a real composite key, so these
  // tests prove the AND semantics (same `data` but a different `year` must survive) without
  // needing a schema change.

  @Test void testDeleteRowsMapNullOrEmptyFilterIsNoop() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeAndCommitRow(writer, 1, "keep-me", 2024);
    assertEquals(1, countRows());

    writer.deleteRows((Map<String, String>) null);
    writer.deleteRows(java.util.Collections.<String, String>emptyMap());
    assertEquals(1, countRows(), "no-op deleteRows(Map) must not remove any row");
  }

  @Test void testDeleteRowsMapRequiresAllColumnsToMatch() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeAndCommitRow(writer, 1, "shared-key", 2023);   // same `data`, different `year`: must survive
    writeAndCommitRow(writer, 2, "shared-key", 2024);   // matches both columns: must be deleted
    writeAndCommitRow(writer, 3, "other-key", 2024);    // matches only `year`: must survive
    assertEquals(3, countRows());

    Map<String, String> filter = new HashMap<>();
    filter.put("data", "shared-key");
    filter.put("year", "2024");
    writer.deleteRows(filter);

    assertEquals(2, countRows(), "only the row matching every column should be deleted");
  }

  @Test void testDeleteRowsMapThenRewriteLandsTheCorrection() throws Exception {
    // Same scenario as the single-column version, keyed on a composite instead: a source row's
    // existing chunks must be gone before its corrected replacement writes, or the replacement
    // duplicates rather than replaces.
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeAndCommitRow(writer, 1, "acc-123", 2024);
    assertEquals(1, countRows());

    Map<String, String> filter = new HashMap<>();
    filter.put("data", "acc-123");
    filter.put("year", "2024");
    writer.deleteRows(filter);
    assertEquals(0, countRows(), "old wrong row must be gone before the correction writes");

    writeAndCommitRow(writer, 1, "acc-123", 2024);
    assertEquals(1, countRows(), "corrected row lands as exactly one row, not a duplicate");
  }

  private void writeAndCommitRow(IcebergTableWriter writer, int id, String data, int year)
      throws Exception {
    Map<String, Object> row = new HashMap<>();
    row.put("id", id);
    row.put("data", data);
    row.put("year", year);
    java.util.List<Map<String, Object>> records = java.util.Collections.singletonList(row);
    Map<String, String> partVals = new HashMap<>();
    partVals.put("year", String.valueOf(year));
    org.apache.iceberg.DataFile df = writer.writeRecords(records, partVals);
    writer.commitDataFiles(java.util.Collections.singletonList(df), null);
  }

  private long countRows() throws Exception {
    long total = 0;
    try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
             table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        total += task.file().recordCount();
      }
    }
    return total;
  }

  private String onlyRemainingDataValue() throws Exception {
    java.util.List<String> values = new java.util.ArrayList<>();
    try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.data.Record> records =
             org.apache.iceberg.data.IcebergGenerics.read(table).build()) {
      for (org.apache.iceberg.data.Record r : records) {
        values.add(String.valueOf(r.getField("data")));
      }
    }
    assertEquals(1, values.size(), "expected exactly one remaining row");
    return values.get(0);
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
