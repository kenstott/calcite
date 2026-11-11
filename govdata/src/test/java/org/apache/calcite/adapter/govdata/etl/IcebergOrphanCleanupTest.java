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

  @Test void tableOutsideTheWarehouseIsRefused() {
    IcebergOrphanCleanup.requireUnderWarehouse("t", "s3a://dq/patents/t", "s3://dq/patents");
    IcebergOrphanCleanup.requireUnderWarehouse("t", "s3a://dq/patents/t", "s3://dq/patents/");
    assertThrows(IllegalStateException.class, () -> IcebergOrphanCleanup.requireUnderWarehouse(
        "t", "s3a://prod/patents/t", "s3://dq/patents"));
    assertThrows(IllegalStateException.class, () -> IcebergOrphanCleanup.requireUnderWarehouse(
        "t", "s3a://dq/patents2/t", "s3://dq/patents"));
  }

  @Test void safetyWindowBelowOneDayIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> IcebergOrphanCleanup.cleanTable("trade",
        table, storage, 0, true, System.currentTimeMillis()));
  }

  @Test void summaryNamesFailedAndUnreachedTables() {
    IcebergOrphanCleanup.RunSummary summary = new IcebergOrphanCleanup.RunSummary();
    summary.pending.add("a");
    summary.pending.add("b");
    summary.pending.add("c");
    summary.completed("a");
    summary.failed("b", new IOException("S3 LIST failed"));

    assertEquals(1, summary.failedCount());
    String report = summary.render();
    assertTrue(report.contains("completed 1, failed 1, not reached 1"), report);
    assertTrue(report.contains("FAILED b: IOException: S3 LIST failed"), report);
    assertTrue(report.contains("NOT REACHED c"), report);
  }

  @Test void tablesScopeKeepsOnlyNamedTablesAndRecordsMatches() {
    List<IcebergIdleMaintenance.Target> targets = new ArrayList<>();
    targets.add(new IcebergIdleMaintenance.Target("cftc_trades", "s3://w/cftc", 7));
    targets.add(new IcebergIdleMaintenance.Target("other", "s3://w/cftc", 7));
    java.util.Set<String> scope = new java.util.HashSet<>();
    scope.add("cftc/cftc_trades");
    scope.add("sec/filing_metadata");
    java.util.Set<String> matched = new java.util.HashSet<>();

    List<IcebergIdleMaintenance.Target> kept = IcebergOrphanCleanup.scopeTargets(
        "cftc/cftc-schema.yaml", targets, scope, matched);

    assertEquals(1, kept.size());
    assertEquals("cftc_trades", kept.get(0).name);
    assertEquals(java.util.Collections.singleton("cftc/cftc_trades"), matched);
  }

  @Test void emptyTablesScopeKeepsEveryTarget() {
    List<IcebergIdleMaintenance.Target> targets = new ArrayList<>();
    targets.add(new IcebergIdleMaintenance.Target("a", "s3://w/x", 7));
    targets.add(new IcebergIdleMaintenance.Target("b", "s3://w/x", 7));

    assertEquals(2, IcebergOrphanCleanup.scopeTargets("x/x-schema.yaml", targets,
        new java.util.HashSet<String>(), new java.util.HashSet<String>()).size());
  }

  @Test void scopeKeyRejectsResourceWithoutSchemaDirectory() {
    assertThrows(IllegalArgumentException.class,
        () -> IcebergOrphanCleanup.scopeKey("schema.yaml", "t"));
  }

  private Path metadataDir(Table t) {
    return Path.of(t.location().replaceFirst("^file:", ""), "metadata");
  }

  private Path orphanManifest(Table t, String name, long ageDays) throws IOException {
    Path orphan = metadataDir(t).resolve(name);
    Files.write(orphan, new byte[] {1, 2, 3});
    Files.setLastModifiedTime(orphan,
        FileTime.fromMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(ageDays)));
    return orphan;
  }

  private List<Path> metadataFiles(Table t) throws IOException {
    try (java.util.stream.Stream<Path> files = Files.list(metadataDir(t))) {
      return files.sorted().collect(java.util.stream.Collectors.toList());
    }
  }

  @Test void metadataDryRunListsOldOrphanManifestAndDeletesNothing() throws Exception {
    Path old = orphanManifest(table, "orphan-m0.avro", 30);

    IcebergOrphanCleanup.MetadataOutcome out = IcebergOrphanCleanup.cleanMetadata("trade", table,
        storage, 14, false, System.currentTimeMillis());

    assertEquals(1, out.orphans);
    assertEquals(1, out.deletable);
    assertEquals(3L, out.deletableBytes);
    assertEquals(0, out.deletedCount);
    assertTrue(Files.exists(old), "a dry run deletes nothing");
  }

  @Test void metadataOrphanIsDeletedOnlyWithTheFlagAndLiveFilesSurvive() throws Exception {
    Path old = orphanManifest(table, "orphan-m1.avro", 30);
    // Age every live metadata file past the window so only reachability protects it.
    List<Path> before = metadataFiles(table);
    for (Path f : before) {
      Files.setLastModifiedTime(f,
          FileTime.fromMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(60)));
    }

    IcebergOrphanCleanup.MetadataOutcome out = IcebergOrphanCleanup.cleanMetadata("trade", table,
        storage, 14, true, System.currentTimeMillis());

    assertEquals(1, out.deletedCount);
    assertFalse(Files.exists(old));
    assertEquals(1, out.orphans, "no live manifest, manifest list or metadata.json is an orphan");
    for (Path f : before) {
      if (!f.equals(old)) {
        assertTrue(Files.exists(f), "reachable metadata file was deleted: " + f);
      }
    }
    table.refresh();
    assertNotNull(table.currentSnapshot());
  }

  @Test void youngOrphanManifestInsideTheWindowSurvives() throws Exception {
    Path young = orphanManifest(table, "young-m0.avro", 1);

    IcebergOrphanCleanup.MetadataOutcome out = IcebergOrphanCleanup.cleanMetadata("trade", table,
        storage, 14, true, System.currentTimeMillis());

    assertEquals(1, out.orphans);
    assertEquals(1, out.withinSafetyWindow);
    assertEquals(0, out.deletedCount);
    assertTrue(Files.exists(young));
  }

  @Test void manifestHeldByARetainedOlderSnapshotIsNotAMetadataOrphan() throws Exception {
    table.newDelete().deleteFromRowFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue())
        .commit();
    table.refresh();
    long old = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(60);
    for (Path f : metadataFiles(table)) {
      Files.setLastModifiedTime(f, FileTime.fromMillis(old));
    }

    IcebergOrphanCleanup.MetadataOutcome out = IcebergOrphanCleanup.cleanMetadata("trade", table,
        storage, 14, true, System.currentTimeMillis());

    assertEquals(0, out.orphans, "manifests of retained snapshots are reachable");
    assertEquals(0, out.deletedCount);
  }

  @Test void metadataPassIgnoresVersionHintAndForeignFilesAndSiblingTables() throws Exception {
    Path foreign = orphanManifest(table, "notes.txt", 60);
    Path siblingManifest = orphanManifest(sibling, "sibling-orphan.avro", 60);

    IcebergOrphanCleanup.MetadataOutcome out = IcebergOrphanCleanup.cleanMetadata("trade", table,
        storage, 14, true, System.currentTimeMillis());

    assertEquals(0, out.orphans);
    assertTrue(Files.exists(foreign), "only names Iceberg writes are candidates");
    assertTrue(Files.exists(siblingManifest), "another table's metadata/ is never listed");
    assertTrue(Files.exists(metadataDir(table).resolve("version-hint.text")));
  }

  @Test void metadataWindowBelowOneDayIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> IcebergOrphanCleanup.cleanMetadata("trade",
        table, storage, 0, true, System.currentTimeMillis()));
  }

  @Test void failedCommitLeavesNoNewManifestInMetadata() throws Exception {
    DataFile df = new IcebergTableWriter(table, storage).writeRecords(
        java.util.Collections.singletonList(java.util.Collections.<String, Object>singletonMap(
            "name", "late")), java.util.Collections.singletonMap("year", "2024"));
    long staleSnapshot = table.currentSnapshot().snapshotId();
    append(table);
    List<Path> before = metadataFiles(table);

    assertThrows(org.apache.iceberg.exceptions.ValidationException.class,
        () -> table.newOverwrite().addFile(df).validateFromSnapshot(staleSnapshot)
            .conflictDetectionFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue())
            .validateNoConflictingData().validateNoConflictingDeletes().commit());

    table.refresh();
    assertEquals(before, metadataFiles(table), "a commit that fails must not leave a manifest");
  }

  private List<String[]> listingRows(StringBuilder listing) {
    List<String[]> rows = new ArrayList<>();
    for (String line : listing.toString().split("\n")) {
      if (!line.isEmpty()) {
        rows.add(line.split("\t"));
      }
    }
    return rows;
  }

  @Test void listingNamesEveryOrphanAndAccountsForEveryReachableFile() throws Exception {
    append(table);
    Path old = orphanManifest(table, "orphan-m0.avro", 30);
    Path young = orphanManifest(table, "young-m0.avro", 1);
    StringBuilder listing = new StringBuilder();

    IcebergOrphanCleanup.MetadataOutcome out = IcebergOrphanCleanup.cleanMetadata("trade", table,
        storage, 14, false, System.currentTimeMillis(), listing);

    List<String[]> rows = listingRows(listing);
    assertEquals(out.onDisk, rows.size(), "one line per classified file");
    java.util.Set<String> expectedReachable = new java.util.HashSet<>();
    for (String p : org.apache.iceberg.ReachableFileUtil.metadataFileLocations(table, true)) {
      expectedReachable.add(IcebergOrphanCleanup.normalize(p));
    }
    for (String p : org.apache.iceberg.ReachableFileUtil.manifestListLocations(table)) {
      expectedReachable.add(IcebergOrphanCleanup.normalize(p));
    }
    for (org.apache.iceberg.Snapshot snap : table.snapshots()) {
      for (org.apache.iceberg.ManifestFile m : snap.allManifests(table.io())) {
        expectedReachable.add(IcebergOrphanCleanup.normalize(m.path()));
      }
    }
    int reachableRows = 0;
    for (String[] row : rows) {
      assertEquals(5, row.length);
      assertEquals("trade", row[0]);
      String path = IcebergOrphanCleanup.normalize(row[4]);
      if (row[1].startsWith("REACHABLE:")) {
        reachableRows++;
        assertTrue(expectedReachable.contains(path), "unexpected reachable file " + path);
      } else {
        assertFalse(expectedReachable.contains(path), "a reachable file is listed as orphan");
      }
    }
    assertEquals(expectedReachable.size(), reachableRows,
        "every reachable file on disk is accounted for");
    assertEquals(1, rows.stream().filter(r -> r[1].equals("ORPHAN_DELETABLE")
        && r[4].endsWith("orphan-m0.avro") && r[2].equals("3")).count());
    assertEquals(1, rows.stream().filter(r -> r[1].equals("ORPHAN_YOUNG")
        && r[4].endsWith("young-m0.avro")).count());
    assertTrue(Files.exists(old) && Files.exists(young), "listing deletes nothing");
  }

  @Test void listingOfAReachableMetadataFileIsNeverMarkedDeletableEvenWhenOld() throws Exception {
    append(table);
    for (Path f : metadataFiles(table)) {
      Files.setLastModifiedTime(f,
          FileTime.fromMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(90)));
    }
    StringBuilder listing = new StringBuilder();

    IcebergOrphanCleanup.cleanMetadata("trade", table, storage, 14, false,
        System.currentTimeMillis(), listing);

    for (String[] row : listingRows(listing)) {
      assertFalse(row[1].startsWith("ORPHAN"), "no orphan expected, got " + row[1] + " " + row[4]);
    }
  }

  @Test void listOrphansRefusedWithDeleteAndWithoutMetadata() {
    String[] withDelete = {"--schema-resource", "x/x-schema.yaml", "--metadata", "--delete",
        "--list-orphans", tempDir.resolve("l.tsv").toString()};
    String[] withoutMetadata = {"--schema-resource", "x/x-schema.yaml",
        "--list-orphans", tempDir.resolve("l.tsv").toString()};
    assertThrows(IllegalArgumentException.class, () -> IcebergOrphanCleanup.main(withDelete));
    assertThrows(IllegalArgumentException.class,
        () -> IcebergOrphanCleanup.main(withoutMetadata));
  }
}
