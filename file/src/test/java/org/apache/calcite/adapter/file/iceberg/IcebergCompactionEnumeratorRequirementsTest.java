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
import org.apache.calcite.util.Sources;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exact-assertion recoding of the file-adapter Iceberg compaction and enumerator-precedence
 * requirements (FILE-036, FILE-075, FILE-145). Fully hermetic: every test builds a local
 * Hadoop-catalog Iceberg table under {@code @TempDir} (no S3, no network), writes through
 * {@link IcebergTableWriter}, compacts through {@link CompactionRunner}'s underlying
 * {@link IcebergTableWriter#compactSmallFiles}, and reads back through {@link IcebergEnumerator}.
 *
 * <p>Catalog setup, writer invocation, and enumerator usage mirror the existing exemplar
 * {@code IcebergRequirementsTest} exactly.
 */
@Tag("integration")
public class IcebergCompactionEnumeratorRequirementsTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;
  private Schema schema;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
    // lowercase Oracle-lex identifiers; "year" is the identity partition column.
    schema =
        new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "year", Types.IntegerType.get()));
  }

  /** Creates a fresh local Hadoop-catalog table partitioned by {@code year}. */
  private Table createTable(String tableName) {
    Configuration conf = new Configuration();
    HadoopCatalog catalog =
        new HadoopCatalog(conf, tempDir.resolve("warehouse").toString());
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("year").build();
    return catalog.createTable(TableIdentifier.of(tableName), schema, spec);
  }

  private Map<String, Object> row(int id, String data, int year) {
    Map<String, Object> m = new HashMap<String, Object>();
    m.put("id", id);
    m.put("data", data);
    m.put("year", year);
    return m;
  }

  /** Reads every (id,data,year) tuple from the current (HEAD) snapshot. */
  private Set<String> readAll(Table table) {
    return readSnapshot(table, null, null);
  }

  /**
   * Reads every (id,data,year) tuple through {@link IcebergEnumerator}, honoring the same
   * snapshot-selector precedence the enumerator applies (snapshotId, then asOfTimestamp,
   * then current).
   */
  private Set<String> readSnapshot(Table table, Long snapshotId, String asOfTimestamp) {
    Set<String> rows = new LinkedHashSet<String>();
    AtomicBoolean cancel = new AtomicBoolean(false);
    IcebergEnumerator en =
        new IcebergEnumerator(table, snapshotId, asOfTimestamp, cancel);
    try {
      while (en.moveNext()) {
        Object[] r = en.current();
        rows.add(r[0] + "|" + r[1] + "|" + r[2]);
      }
    } finally {
      en.close();
    }
    return rows;
  }

  /** Counts the data files referenced by the table's current snapshot. */
  private int dataFileCount(Table table) throws Exception {
    int count = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        count++;
      }
    }
    return count;
  }

  /**
   * Appends {@code n} single-row data files to the {@code 2024} partition, one file (and one
   * snapshot) per row. Each Iceberg-written Parquet file is tiny (far below any realistic
   * small-file threshold), so this manufactures the many-small-files condition compaction targets.
   *
   * @return the set of (id,data,year) tuples written
   */
  private Set<String> appendSmallFiles(IcebergTableWriter writer, Table table, int n)
      throws Exception {
    Map<String, String> p2024 = new HashMap<String, String>();
    p2024.put("year", "2024");
    Set<String> expected = new LinkedHashSet<String>();
    for (int i = 1; i <= n; i++) {
      DataFile df = writer.writeRecords(
          Collections.singletonList(row(i, "v" + i, 2024)), p2024);
      assertNotNull(df, "writeRecords should return a data file");
      writer.bulkCommitDataFiles(Collections.singletonList(df));
      expected.add(i + "|v" + i + "|2024");
    }
    table.refresh();
    return expected;
  }

  // ----------------------------------------------------------------------------------------
  // FILE-036: compaction merges small files WITHOUT changing query results. The table reads
  // back value-identical (same rows, same values) before and after compaction.
  // ----------------------------------------------------------------------------------------
  @Test
  @Tag("FILE-036")
  public void compactionPreservesQueryResultsExactly() throws Exception {
    Table table = createTable("file036");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    // Write 5 small files into one partition.
    Set<String> expected = appendSmallFiles(writer, table, 5);

    // Pre-compaction read-back is the reference value set.
    Set<String> before = readAll(table);
    assertEquals(expected, before, "pre-compaction read-back must be value-identical");

    // Compact: target large, smallFileSize huge so every file counts as "small",
    // minFiles=3 so the 5-file partition qualifies.
    int compactedPartitions =
        writer.compactSmallFiles(128L * 1024 * 1024, 3, 1024L * 1024 * 1024);
    assertEquals(1, compactedPartitions, "the single 5-small-file partition must compact");
    table.refresh();

    // Post-compaction read-back must equal the pre-compaction value set exactly.
    Set<String> after = readAll(table);
    assertEquals(before, after, "compaction must not change the query result set");
  }

  // ----------------------------------------------------------------------------------------
  // FILE-075: inline compaction thresholds. A partition is compacted when its small-file count
  // reaches compactionMinFiles (standalone CompactionRunner default 3); files below
  // compactionSmallFileSizeBytes are merged toward compactionTargetFileSizeBytes; afterward
  // non-current snapshots are expired and orphan files removed. Assert the underlying data-file
  // count DROPS while the row set is unchanged.
  // NOTE: the literal default constants live as local variables in CompactionRunner.main
  //   (targetFileSize 128MB, minFiles 3, smallFileSize 10MB) and are not exposed as named public
  //   fields; this test pins the documented default min-files value (3) by passing it explicitly,
  //   which is exactly how CompactionRunner.main feeds compactSmallFiles.
  // ----------------------------------------------------------------------------------------
  @Test
  @Tag("FILE-075")
  public void compactionReducesFileCountAtMinFilesThreshold() throws Exception {
    Table table = createTable("file075");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    // CompactionRunner standalone default min-files.
    final int compactionMinFiles = 3;

    // Write exactly the threshold number of small files into one partition.
    Set<String> expected = appendSmallFiles(writer, table, compactionMinFiles);

    int filesBefore = dataFileCount(table);
    assertEquals(compactionMinFiles, filesBefore,
        "each append must contribute one small data file");
    Set<String> rowsBefore = readAll(table);
    assertEquals(expected, rowsBefore, "pre-compaction rows must match what was written");

    // Run compaction with the same thresholds CompactionRunner.main uses (target 128MB,
    // min-files 3, small-file 10MB). With tiny single-row files all qualify as small.
    int compactedPartitions =
        writer.compactSmallFiles(128L * 1024 * 1024, compactionMinFiles, 10L * 1024 * 1024);
    assertEquals(1, compactedPartitions, "partition at the min-files threshold must compact");
    table.refresh();

    // Fewer files after compaction (the 3 small files merge toward one target-size file).
    int filesAfter = dataFileCount(table);
    assertTrue(filesAfter < filesBefore,
        "compaction must reduce the data-file count (" + filesBefore + " -> " + filesAfter + ")");

    // After compaction only the current snapshot is retained (non-current snapshots expired).
    table.refresh();
    int snapshotCount = 0;
    for (Snapshot s : table.snapshots()) {
      snapshotCount++;
    }
    assertEquals(1, snapshotCount,
        "non-current snapshots must be expired after compaction");

    // Row set is unchanged.
    Set<String> rowsAfter = readAll(table);
    assertEquals(rowsBefore, rowsAfter, "compaction must preserve the exact row set");
  }

  // ----------------------------------------------------------------------------------------
  // FILE-145: IcebergEnumerator snapshot precedence is snapshotId > asOfTimestamp > current,
  // and IcebergTable.withSnapshot/asOf return NEW immutable instances (the original unchanged).
  // NOTE: the precedence branch itself lives in IcebergEnumerator's constructor (snapshotId is
  //   checked before asOfTimestamp); it is asserted directly here by passing BOTH selectors to a
  //   single enumerator and observing snapshotId win. The withSnapshot/asOf immutability is
  //   asserted on IcebergTable, the public carrier of those selectors.
  // ----------------------------------------------------------------------------------------
  @Test
  @Tag("FILE-145")
  public void enumeratorSnapshotIdWinsAndIcebergTableSelectorsAreImmutable() throws Exception {
    Table table = createTable("file145");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Map<String, String> p2024 = new HashMap<String, String>();
    p2024.put("year", "2024");

    // Snapshot A: one row.
    DataFile dfA = writer.writeRecords(
        Collections.singletonList(row(1, "alpha", 2024)), p2024);
    writer.bulkCommitDataFiles(Collections.singletonList(dfA));
    table.refresh();
    long earlierSnapshotId = table.currentSnapshot().snapshotId();

    // Snapshot B (HEAD): add a second row.
    DataFile dfB = writer.writeRecords(
        Collections.singletonList(row(2, "beta", 2024)), p2024);
    writer.bulkCommitDataFiles(Collections.singletonList(dfB));
    table.refresh();

    Set<String> earlierExpected = new LinkedHashSet<String>(
        Arrays.asList("1|alpha|2024"));
    Set<String> headExpected = new LinkedHashSet<String>(
        Arrays.asList("1|alpha|2024", "2|beta|2024"));

    // Precedence: snapshotId set to the EARLIER snapshot, asOfTimestamp set to "now" (which
    // would otherwise resolve to HEAD). snapshotId must win => only the earlier row is read.
    String nowIso = java.time.Instant.now().toString();
    assertEquals(earlierExpected, readSnapshot(table, earlierSnapshotId, nowIso),
        "snapshotId must take precedence over asOfTimestamp");

    // Control: with no snapshotId, asOfTimestamp=now resolves to HEAD (both rows).
    assertEquals(headExpected, readSnapshot(table, null, nowIso),
        "with snapshotId unset, asOfTimestamp=now must resolve to HEAD");

    // Immutability: withSnapshot / asOf return NEW instances; the original is unchanged.
    IcebergTable base = new IcebergTable(table, Sources.of(new File(table.location())));
    IcebergTable withSnap = base.withSnapshot(earlierSnapshotId);
    IcebergTable withAsOf = base.asOf(nowIso);

    assertNotSame(base, withSnap, "withSnapshot must return a new instance");
    assertNotSame(base, withAsOf, "asOf must return a new instance");
    assertNotSame(withSnap, withAsOf, "withSnapshot and asOf must be distinct instances");

    // The original instance still reads HEAD (its selectors were not mutated in place).
    AtomicBoolean cancel = new AtomicBoolean(false);
    IcebergEnumerator baseEn =
        new IcebergEnumerator(base.getIcebergTable(), null, null, cancel);
    Set<String> baseRows = new LinkedHashSet<String>();
    try {
      while (baseEn.moveNext()) {
        Object[] r = baseEn.current();
        baseRows.add(r[0] + "|" + r[1] + "|" + r[2]);
      }
    } finally {
      baseEn.close();
    }
    assertEquals(headExpected, baseRows,
        "the original IcebergTable must be unchanged after withSnapshot/asOf");
  }

  // ----------------------------------------------------------------------------------------
  // FILE-145 / C-22 (RESOLVED: code-wrong). INTENDED behavior: IcebergTable.getStatistic() must
  // honor the selected snapshotId/asOfTimestamp, so a withSnapshot(earlier) table's row-count
  // statistic equals the EARLIER snapshot's row count (1), not HEAD's (2). The current code always
  // sums currentSnapshot row counts, ignoring the selector, so this is DISABLED pending the fix.
  // This target documents the intended behavior only; it does NOT assert current behavior passes.
  // ----------------------------------------------------------------------------------------
  @Test
  @Tag("FILE-145")
  @Disabled("C-22: getStatistic must honor the selected snapshotId/asOfTimestamp — pending code fix")
  public void getStatisticHonorsSelectedSnapshot() throws Exception {
    Table table = createTable("file145c22");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Map<String, String> p2024 = new HashMap<String, String>();
    p2024.put("year", "2024");

    // Snapshot A: one row.
    DataFile dfA = writer.writeRecords(
        Collections.singletonList(row(1, "alpha", 2024)), p2024);
    writer.bulkCommitDataFiles(Collections.singletonList(dfA));
    table.refresh();
    long earlierSnapshotId = table.currentSnapshot().snapshotId();

    // Snapshot B (HEAD): add a second row.
    DataFile dfB = writer.writeRecords(
        Collections.singletonList(row(2, "beta", 2024)), p2024);
    writer.bulkCommitDataFiles(Collections.singletonList(dfB));
    table.refresh();

    // INTENDED: the snapshot-pinned table reports the earlier snapshot's row count (1.0).
    IcebergTable pinned =
        new IcebergTable(table, Sources.of(new File(table.location())))
            .withSnapshot(earlierSnapshotId);
    Double rowCount = pinned.getStatistic().getRowCount();
    assertNotNull(rowCount, "pinned statistic should expose a row count");
    assertEquals(1.0, rowCount.doubleValue(), 0.0,
        "getStatistic must reflect the pinned (earlier) snapshot's row count, not HEAD's");
  }
}
