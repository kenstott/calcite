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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exact-assertion recoding of the file-adapter Iceberg requirements (FILE-031, FILE-071,
 * FILE-144). Fully hermetic: every test builds a local Hadoop-catalog Iceberg table under
 * {@code @TempDir} (no S3, no network) and writes through {@link IcebergTableWriter},
 * reading back through {@link IcebergEnumerator}.
 *
 * <p>Catalog setup and writer invocation mirror the existing exemplars
 * {@code IcebergEnumeratorTest} and {@code IcebergTableWriterTest}.
 */
@Tag("integration")
public class IcebergRequirementsTest {

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
    return readSnapshot(table, null);
  }

  /** Reads every (id,data,year) tuple as of the given snapshot id (null = HEAD). */
  private Set<String> readSnapshot(Table table, Long snapshotId) {
    Set<String> rows = new LinkedHashSet<String>();
    AtomicBoolean cancel = new AtomicBoolean(false);
    IcebergEnumerator en =
        new IcebergEnumerator(table, snapshotId, null, cancel);
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

  // ----------------------------------------------------------------------------------------
  // FILE-031: writer materializes a value-identical table; a second append advances the
  // current snapshot to a new id with a higher sequence number.
  // ----------------------------------------------------------------------------------------
  @Test
  @Tag("FILE-031")
  public void writeReadBackIdenticalAndSnapshotsAdvance() throws Exception {
    Table table = createTable("file031");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    // First append.
    List<Map<String, Object>> batch1 = new ArrayList<Map<String, Object>>();
    batch1.add(row(1, "alpha", 2024));
    batch1.add(row(2, "beta", 2024));
    Map<String, String> p2024 = new HashMap<String, String>();
    p2024.put("year", "2024");
    DataFile df1 = writer.writeRecords(batch1, p2024);
    assertNotNull(df1, "writeRecords should return a data file");
    writer.bulkCommitDataFiles(Collections.singletonList(df1));

    table.refresh();
    Snapshot snap1 = table.currentSnapshot();
    assertNotNull(snap1, "first append must create a snapshot");

    // Value-identical read-back of the first snapshot.
    Set<String> expected1 = new LinkedHashSet<String>(
        Arrays.asList("1|alpha|2024", "2|beta|2024"));
    assertEquals(expected1, readAll(table), "read-back must be value-identical");

    // Second append -> new current snapshot, higher id and higher sequence number.
    List<Map<String, Object>> batch2 = new ArrayList<Map<String, Object>>();
    batch2.add(row(3, "gamma", 2025));
    Map<String, String> p2025 = new HashMap<String, String>();
    p2025.put("year", "2025");
    DataFile df2 = writer.writeRecords(batch2, p2025);
    writer.bulkCommitDataFiles(Collections.singletonList(df2));

    table.refresh();
    Snapshot snap2 = table.currentSnapshot();
    assertNotNull(snap2);
    assertTrue(snap2.snapshotId() != snap1.snapshotId(),
        "second append must produce a new current snapshot id");
    assertTrue(snap2.sequenceNumber() > snap1.sequenceNumber(),
        "second append must advance the sequence number");

    // HEAD now contains all three rows.
    Set<String> expectedAll = new LinkedHashSet<String>(
        Arrays.asList("1|alpha|2024", "2|beta|2024", "3|gamma|2025"));
    assertEquals(expectedAll, readAll(table), "HEAD must contain both appends");
  }

  // ----------------------------------------------------------------------------------------
  // FILE-071: time travel. With no snapshot selector the read returns HEAD (latest);
  // a snapshotId selects an exact earlier snapshot's data.
  // NOTE: asOfTimestamp and timeRange {start,end}+snapshotColumn sub-clauses are OMITTED —
  //   they require driving the full IcebergTable/IcebergTimeRangeTable wiring (model config,
  //   ExecutionEngineConfig, snapshotColumn injection) rather than the hermetic enumerator
  //   path used here; the resolver wiring is exercised by IcebergTimeRangeTest. This test
  //   asserts the two required cases: HEAD-latest and snapshotId-selects-earlier.
  // ----------------------------------------------------------------------------------------
  @Test
  @Tag("FILE-071")
  public void timeTravelHeadLatestAndSnapshotIdSelectsEarlier() throws Exception {
    Table table = createTable("file071");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Map<String, String> p2024 = new HashMap<String, String>();
    p2024.put("year", "2024");

    // Snapshot A: one row.
    DataFile dfA = writer.writeRecords(
        Collections.singletonList(row(1, "alpha", 2024)), p2024);
    writer.bulkCommitDataFiles(Collections.singletonList(dfA));
    table.refresh();
    long earlierSnapshotId = table.currentSnapshot().snapshotId();
    Set<String> earlierExpected = new LinkedHashSet<String>(
        Arrays.asList("1|alpha|2024"));

    // Snapshot B (HEAD): add a second row.
    DataFile dfB = writer.writeRecords(
        Collections.singletonList(row(2, "beta", 2024)), p2024);
    writer.bulkCommitDataFiles(Collections.singletonList(dfB));
    table.refresh();

    // No selector -> HEAD/latest = both rows.
    Set<String> headExpected = new LinkedHashSet<String>(
        Arrays.asList("1|alpha|2024", "2|beta|2024"));
    assertEquals(headExpected, readSnapshot(table, null),
        "no snapshot selector must return the latest (HEAD) snapshot");

    // Exact earlier snapshot id -> only the first row.
    assertEquals(earlierExpected, readSnapshot(table, earlierSnapshotId),
        "snapshotId must select the exact earlier snapshot's data");
  }

  // ----------------------------------------------------------------------------------------
  // FILE-144: AppendFiles append-only commit. An EMPTY data-file list is a no-op (zero-row
  // run never advances the snapshot). After commits, ensureVersionHint repairs
  // version-hint.text to the highest v<N>.metadata.json.
  // ----------------------------------------------------------------------------------------
  @Test
  @Tag("FILE-144")
  public void emptyCommitIsNoOpAndVersionHintTracksLatestMetadata() throws Exception {
    Table table = createTable("file144");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Map<String, String> p2024 = new HashMap<String, String>();
    p2024.put("year", "2024");

    // One real append to establish a baseline snapshot.
    DataFile df1 = writer.writeRecords(
        Collections.singletonList(row(1, "alpha", 2024)), p2024);
    writer.bulkCommitDataFiles(Collections.singletonList(df1));
    table.refresh();
    long snapAfterFirst = table.currentSnapshot().snapshotId();

    // Empty bulk commit -> no-op: snapshot id must be unchanged.
    writer.bulkCommitDataFiles(new ArrayList<DataFile>());
    table.refresh();
    assertEquals(snapAfterFirst, table.currentSnapshot().snapshotId(),
        "an empty data-file list must not advance the snapshot");

    // Empty commitDataFiles -> also a no-op.
    writer.commitDataFiles(new ArrayList<DataFile>(), null);
    table.refresh();
    assertEquals(snapAfterFirst, table.currentSnapshot().snapshotId(),
        "empty commitDataFiles must not advance the snapshot");

    // A second real append to push the metadata version higher.
    DataFile df2 = writer.writeRecords(
        Collections.singletonList(row(2, "beta", 2024)), p2024);
    writer.bulkCommitDataFiles(Collections.singletonList(df2));
    table.refresh();

    // version-hint.text must point at the highest v<N>.metadata.json on disk.
    Path metadataDir = Path.of(table.location()).resolve("metadata");
    int maxVersion = highestMetadataVersion(metadataDir);
    assertTrue(maxVersion > 0, "expected at least one v<N>.metadata.json file");

    Path versionHint = metadataDir.resolve("version-hint.text");
    assertTrue(Files.exists(versionHint), "version-hint.text must exist after commits");
    int hinted = Integer.parseInt(readFirstLine(versionHint).trim());
    assertEquals(maxVersion, hinted,
        "version-hint.text must point to the latest metadata version");
  }

  /** Highest N among {@code v<N>.metadata.json} files in the metadata directory. */
  private int highestMetadataVersion(Path metadataDir) throws Exception {
    int max = 0;
    java.util.regex.Pattern p =
        java.util.regex.Pattern.compile("v(\\d+)\\.metadata\\.json$");
    List<Path> files = new ArrayList<Path>();
    Files.list(metadataDir).forEach(files::add);
    for (Path f : files) {
      java.util.regex.Matcher m = p.matcher(f.getFileName().toString());
      if (m.find()) {
        int v = Integer.parseInt(m.group(1));
        if (v > max) {
          max = v;
        }
      }
    }
    return max;
  }

  private String readFirstLine(Path file) throws Exception {
    BufferedReader r = new BufferedReader(
        new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8));
    try {
      String line = r.readLine();
      return line == null ? "" : line;
    } finally {
      r.close();
    }
  }
}
