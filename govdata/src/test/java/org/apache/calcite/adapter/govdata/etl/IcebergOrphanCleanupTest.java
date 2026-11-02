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
package org.apache.calcite.adapter.govdata.etl;

import org.apache.calcite.adapter.file.iceberg.CrossProcessCommitLock;
import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.iceberg.IcebergTableWriter;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Orphan cleanup deletes only files that no retained snapshot references, that are older than the
 * safety window, and only when asked to.
 */
@Tag("unit")
public class IcebergOrphanCleanupTest {

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "name", Types.StringType.get()),
      Types.NestedField.optional(2, "year", Types.IntegerType.get()));

  private static final int WINDOW_DAYS = 3;

  @TempDir
  Path tempDir;

  private Map<String, Object> catalogConfig;
  private Table table;
  private Table sibling;
  private final StorageProvider storage = new LocalFileStorageProvider();

  @BeforeEach void setUp() throws Exception {
    catalogConfig = new HashMap<>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", tempDir.resolve("warehouse").toString());
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("year").build();
    table = IcebergCatalogManager.createTable(catalogConfig, "trade", SCHEMA, spec);
    sibling = IcebergCatalogManager.createTable(catalogConfig, "trade_extra", SCHEMA, spec);
    append(table);
    append(sibling);
  }

  @AfterEach void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  private void append(Table t) throws IOException {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int r = 0; r < 5; r++) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("name", "n" + r);
      row.put("year", 2024);
      rows.add(row);
    }
    Map<String, String> partition = new HashMap<>();
    partition.put("year", "2024");
    DataFile df = new IcebergTableWriter(t, storage).writeRecords(rows, partition);
    assertNotNull(df);
    t.newAppend().appendFile(df).commit();
    t.refresh();
  }

  private Path liveFile(Table t) {
    try (CloseableIterable<FileScanTask> tasks = t.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        return Path.of(task.file().path().toString().replaceFirst("^file:", ""));
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    throw new IllegalStateException("table has no live file");
  }

  private Path orphanNextTo(Table t, String name, long ageDays) throws IOException {
    Path orphan = liveFile(t).resolveSibling(name);
    Files.write(orphan, new byte[] {1, 2, 3});
    long modified = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(ageDays);
    Files.setLastModifiedTime(orphan, FileTime.fromMillis(modified));
    return orphan;
  }

  @Test void dryRunReportsOldOrphanAndDeletesNothing() throws Exception {
    Path old = orphanNextTo(table, "old-orphan.parquet", 10);

    IcebergOrphanCleanup.Outcome out = IcebergOrphanCleanup.cleanTable("trade", table, storage,
        WINDOW_DAYS, false, System.currentTimeMillis());

    assertEquals(1, out.trueOrphans);
    assertEquals(1, out.deletable);
    assertEquals(0, out.deletedCount);
    assertTrue(Files.exists(old), "a dry run deletes nothing");
  }

  @Test void oldOrphanIsDeletedOnlyWithTheFlag() throws Exception {
    Path old = orphanNextTo(table, "old-orphan.parquet", 10);
    Path live = liveFile(table);

    IcebergOrphanCleanup.Outcome out = IcebergOrphanCleanup.cleanTable("trade", table, storage,
        WINDOW_DAYS, true, System.currentTimeMillis());

    assertEquals(1, out.deletedCount);
    assertFalse(Files.exists(old));
    assertTrue(Files.exists(live), "a file the current snapshot references is never deleted");
  }

  @Test void freshUnreferencedFileInsideTheWindowSurvives() throws Exception {
    Path fresh = orphanNextTo(table, "fresh-orphan.parquet", 0);

    IcebergOrphanCleanup.Outcome out = IcebergOrphanCleanup.cleanTable("trade", table, storage,
        WINDOW_DAYS, true, System.currentTimeMillis());

    assertEquals(1, out.trueOrphans);
    assertEquals(1, out.withinSafetyWindow);
    assertEquals(0, out.deletedCount);
    assertTrue(Files.exists(fresh));
  }

  @Test void fileHeldOnlyByARetainedOlderSnapshotIsNotAnOrphan() throws Exception {
    Path first = liveFile(table);
    table.newDelete().deleteFromRowFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue())
        .commit();
    table.refresh();
    long old = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(10);
    Files.setLastModifiedTime(first, FileTime.fromMillis(old));

    IcebergOrphanCleanup.Outcome out = IcebergOrphanCleanup.cleanTable("trade", table, storage,
        WINDOW_DAYS, true, System.currentTimeMillis());

    assertEquals(1, out.heldByRetention);
    assertEquals(0, out.trueOrphans);
    assertTrue(Files.exists(first), "retention, not orphan cleanup, releases this file");
  }

  @Test void siblingTableWithPrefixNameIsNeverTouched() throws Exception {
    Path siblingFile = liveFile(sibling);
    Files.setLastModifiedTime(siblingFile,
        FileTime.fromMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30)));

    IcebergOrphanCleanup.Outcome out = IcebergOrphanCleanup.cleanTable("trade", table, storage,
        WINDOW_DAYS, true, System.currentTimeMillis());

    assertEquals(1, out.onDisk, "only this table's own data file is counted");
    assertEquals(0, out.trueOrphans);
    assertTrue(Files.exists(siblingFile), "trade_extra's files are not trade's orphans");
  }

  @Test void tableMidCommitIsSkipped() throws Exception {
    Path old = orphanNextTo(table, "old-orphan.parquet", 10);
    try (CrossProcessCommitLock.Handle held = CrossProcessCommitLock.acquire(table.location())) {
      IcebergOrphanCleanup.Outcome[] outcome = new IcebergOrphanCleanup.Outcome[1];
      Thread other = new Thread(() -> {
        try {
          outcome[0] = IcebergOrphanCleanup.cleanTable("trade", table, storage, WINDOW_DAYS, true,
              System.currentTimeMillis());
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      });
      other.start();
      other.join();
      assertTrue(outcome[0].skippedBusy);
    }
    assertTrue(Files.exists(old));
  }

  @Test void windowsAndUriPathsNormalizeToTheSameKey() {
    assertEquals("C:/w/trade/data/f.parquet",
        IcebergOrphanCleanup.normalize("C:\\w\\trade\\data\\f.parquet"));
    assertEquals("C:/w/trade/data/f.parquet",
        IcebergOrphanCleanup.normalize("file:/C:/w/trade/data/f.parquet"));
    assertEquals("/w/trade/f.parquet", IcebergOrphanCleanup.normalize("file:///w/trade/f.parquet"));
    assertEquals(IcebergOrphanCleanup.normalize("s3://b/k"),
        IcebergOrphanCleanup.normalize("s3a://b/k"));
  }

  @Test void directoryPlaceholderKeysAreNotDataFiles() {
    assertTrue(IcebergOrphanCleanup.isDataFileEntry(new StorageProvider.FileEntry(
        "s3://b/t/data/f.parquet", "f.parquet", false, 10L, 0L)));
    assertFalse(IcebergOrphanCleanup.isDataFileEntry(new StorageProvider.FileEntry(
        "s3://b/t/data/", "data", false, 0L, 0L)), "zero-byte data/ marker");
    assertFalse(IcebergOrphanCleanup.isDataFileEntry(new StorageProvider.FileEntry(
        "s3://b/t/data", "data", true, 0L, 0L)));
    assertFalse(IcebergOrphanCleanup.isDataFileEntry(new StorageProvider.FileEntry(
        "s3://b/t/metadata/v1.metadata.json", "v1.metadata.json", false, 10L, 0L)));
    assertFalse(IcebergOrphanCleanup.isDataFileEntry(new StorageProvider.FileEntry(
        "s3://b/t/data/.f.parquet.crc", ".f.parquet.crc", false, 10L, 0L)));
  }

  @Test void safetyWindowBelowOneDayIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> IcebergOrphanCleanup.cleanTable("trade",
        table, storage, 0, true, System.currentTimeMillis()));
  }
}
