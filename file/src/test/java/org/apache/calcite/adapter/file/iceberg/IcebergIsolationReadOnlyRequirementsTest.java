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
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.Sources;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exact-assertion recoding of the file-adapter Iceberg READ-ONLY and SNAPSHOT-ISOLATION
 * requirements (FILE-072, FILE-074). Fully hermetic: every test builds a local Hadoop-catalog
 * Iceberg table under {@code @TempDir} (no S3, no network), writes through
 * {@link IcebergTableWriter}, and reads back through {@link IcebergEnumerator}.
 *
 * <p>Catalog setup, writer invocation, and enumerator usage mirror the existing exemplars
 * {@code IcebergRequirementsTest} and {@code IcebergCompactionEnumeratorRequirementsTest} exactly.
 */
@Tag("integration")
public class IcebergIsolationReadOnlyRequirementsTest {

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
  // FILE-072: snapshot isolation + atomic append. Each query reads a single CONSISTENT
  // snapshot. A reader pinned to snapshot S keeps seeing S's data even after a later append
  // creates S+1. Each committed append is all-or-nothing (every row of the batch is visible,
  // never a partial subset).
  // ----------------------------------------------------------------------------------------
  @Test
  @Tag("FILE-072")
  public void pinnedSnapshotIsIsolatedFromLaterAppendAndAppendsAreAtomic() throws Exception {
    Table table = createTable("file072");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Map<String, String> p2024 = new HashMap<String, String>();
    p2024.put("year", "2024");

    // --- Snapshot S: a single multi-row append. Atomicity is observable as all-or-nothing:
    // the committed snapshot exposes EVERY row of the batch, never a partial subset. ---
    List<Map<String, Object>> batchS = Arrays.asList(
        row(1, "alpha", 2024), row(2, "beta", 2024));
    DataFile dfS = writer.writeRecords(batchS, p2024);
    assertNotNull(dfS, "writeRecords should return a data file");
    writer.bulkCommitDataFiles(Collections.singletonList(dfS));
    table.refresh();

    long snapshotS = table.currentSnapshot().snapshotId();
    Set<String> rowsAtS = new LinkedHashSet<String>(
        Arrays.asList("1|alpha|2024", "2|beta|2024"));

    // The whole batch is atomically visible at S (all-or-nothing committed append).
    assertEquals(rowsAtS, readSnapshot(table, snapshotS),
        "a committed append must expose all of its rows atomically at snapshot S");

    // --- Append creates snapshot S+1, leaving S immutable on disk. ---
    DataFile dfNext = writer.writeRecords(
        Collections.singletonList(row(3, "gamma", 2024)), p2024);
    writer.bulkCommitDataFiles(Collections.singletonList(dfNext));
    table.refresh();

    long snapshotSPlus1 = table.currentSnapshot().snapshotId();
    assertTrue(snapshotSPlus1 != snapshotS,
        "the second append must create a new snapshot (S+1)");

    // --- Snapshot isolation: a read pinned to S still returns exactly S's rows, even though
    // S+1 now exists. A fresh HEAD read returns the new (S+1) rows. ---
    assertEquals(rowsAtS, readSnapshot(table, snapshotS),
        "a reader pinned to snapshot S must keep seeing S's data after a later append");

    Set<String> rowsAtHead = new LinkedHashSet<String>(
        Arrays.asList("1|alpha|2024", "2|beta|2024", "3|gamma|2024"));
    assertEquals(rowsAtHead, readSnapshot(table, null),
        "a fresh HEAD read must return the new snapshot's data");
    assertEquals(rowsAtHead, readSnapshot(table, snapshotSPlus1),
        "an explicit read of S+1 must return the new snapshot's data");

    // The pinned-to-S read set is strictly the earlier, smaller set (consistency: it never
    // picked up the concurrently-appended row).
    assertFalse(readSnapshot(table, snapshotS).contains("3|gamma|2024"),
        "snapshot S must never expose a row that was appended in S+1");
  }

  // ----------------------------------------------------------------------------------------
  // FILE-074: Iceberg READ integration is READ-ONLY BY CONSTRUCTION. There is no SQL
  // INSERT/UPDATE/DELETE surface on the read table — only copy-on-write reads are supported
  // (position/equality deletes are not exercised by this scan path). The read table is a pure
  // ScannableTable with NO write API exposed.
  //
  // NOTE: read-only here is STRUCTURAL, not enforced by a thrown "unsupported" at a SQL DML
  //   entry point. IcebergTable implements ScannableTable + CommentableTable only; it is NOT a
  //   ModifiableTable and NOT a TranslatableTable, so Calcite has no path to plan an INSERT/
  //   UPDATE/DELETE against it (a DML attempt fails to validate/convert — there is no
  //   getModifiableCollection/toModificationRel surface to target). We assert that absence
  //   structurally on the read table, and assert that the read enumerator exposes only a
  //   forward scan (moveNext/current/close) with no mutation entry point — reset() itself is
  //   unsupported. There is genuinely no SQL write path to drive, so we do NOT fabricate one.
  // ----------------------------------------------------------------------------------------
  @Test
  @Tag("FILE-074")
  public void readTableExposesNoMutationSurfaceAndReadsAreScanOnly() throws Exception {
    Table table = createTable("file074");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    Map<String, String> p2024 = new HashMap<String, String>();
    p2024.put("year", "2024");

    // Seed one row so the read path is exercised, not just inspected.
    DataFile df = writer.writeRecords(
        Collections.singletonList(row(1, "alpha", 2024)), p2024);
    writer.bulkCommitDataFiles(Collections.singletonList(df));
    table.refresh();

    IcebergTable readTable = new IcebergTable(table, Sources.of(new File(table.location())));

    // --- Structural read-only: the read table is a ScannableTable but exposes NO write API.
    // ModifiableTable (SQL INSERT/UPDATE/DELETE target) and TranslatableTable (custom DML
    // planning) are both ABSENT, so Calcite cannot plan any mutation against this table. ---
    assertTrue(readTable instanceof ScannableTable,
        "the Iceberg read table must be scan-based (ScannableTable)");
    assertFalse(readTable instanceof ModifiableTable,
        "the Iceberg read table must NOT be a ModifiableTable (no SQL INSERT/UPDATE/DELETE target)");
    assertFalse(readTable instanceof TranslatableTable,
        "the Iceberg read table must NOT be a TranslatableTable (no custom DML planning surface)");

    // No mutation methods exist on the read table's public surface: it carries only read /
    // snapshot-selection methods (scan, getStatistic, withSnapshot, asOf, getIcebergTable,
    // comments). Assert there is no insert/update/delete/modify/write method to call.
    for (Method m : IcebergTable.class.getMethods()) {
      String name = m.getName().toLowerCase(java.util.Locale.ROOT);
      assertFalse(name.startsWith("insert") || name.startsWith("update")
              || name.startsWith("delete") || name.startsWith("modify")
              || name.equals("getmodifiablecollection"),
          "read table must expose no mutation method, found: " + m.getName());
    }

    // --- The read path is purely a forward scan: the enumerator yields rows via
    // moveNext()/current()/close() and has NO mutation entry point. reset() is itself
    // unsupported, confirming this is a one-shot read cursor, not a writable surface. ---
    AtomicBoolean cancel = new AtomicBoolean(false);
    IcebergEnumerator en = new IcebergEnumerator(table, null, null, cancel);
    try {
      assertTrue(en.moveNext(), "the seeded row must be readable through the scan path");
      Object[] r = en.current();
      assertEquals("1|alpha|2024", r[0] + "|" + r[1] + "|" + r[2],
          "the read enumerator must return the committed row verbatim");
      assertFalse(en.moveNext(), "the scan must end after the single seeded row");

      // The enumerator exposes no write/insert method — only the read cursor + close.
      for (Method m : IcebergEnumerator.class.getDeclaredMethods()) {
        if (!m.isSynthetic()) {
          String name = m.getName().toLowerCase(java.util.Locale.ROOT);
          assertFalse(name.startsWith("insert") || name.startsWith("update")
                  || name.startsWith("delete") || name.startsWith("write")
                  || name.startsWith("commit"),
              "read enumerator must expose no mutation method, found: " + m.getName());
        }
      }
    } finally {
      en.close();
    }

    // reset() is unsupported: the read enumerator is a forward-only, non-rewindable cursor.
    AtomicBoolean cancel2 = new AtomicBoolean(false);
    IcebergEnumerator en2 = new IcebergEnumerator(table, null, null, cancel2);
    try {
      boolean unsupported = false;
      try {
        en2.reset();
      } catch (UnsupportedOperationException expected) {
        unsupported = true;
      }
      assertTrue(unsupported,
          "the read enumerator must not support reset() (forward-only read cursor)");
    } finally {
      en2.close();
    }
  }
}
