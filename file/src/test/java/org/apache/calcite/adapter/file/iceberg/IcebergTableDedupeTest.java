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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link IcebergTableWriter#dedupeCopies}: removes the surplus copies of whole re-ingested groups
 * and nothing else.
 */
@Tag("unit")
class IcebergTableDedupeTest {
  private static final List<String> KEY = Arrays.asList("acc", "seq");

  @TempDir
  Path tempDir;

  private Table table;
  private IcebergTableWriter writer;

  @BeforeEach
  void setUp() {
    Map<String, Object> catalog = new HashMap<String, Object>();
    catalog.put("catalogType", "hadoop");
    catalog.put("warehousePath", tempDir.resolve("warehouse").toString());
    Schema schema = new Schema(
        Types.NestedField.optional(1, "acc", Types.StringType.get()),
        Types.NestedField.optional(2, "seq", Types.IntegerType.get()),
        Types.NestedField.optional(3, "txt", Types.StringType.get()),
        Types.NestedField.optional(4, "year", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("year").build();
    table = IcebergCatalogManager.createTable(catalog, "dedupe_table", schema, spec);
    writer = new IcebergTableWriter(table, new LocalFileStorageProvider());
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  private static Map<String, Object> row(String acc, int seq, String txt, int year) {
    Map<String, Object> r = new LinkedHashMap<String, Object>();
    r.put("acc", acc);
    r.put("seq", Integer.valueOf(seq));
    r.put("txt", txt);
    r.put("year", Integer.valueOf(year));
    return r;
  }

  /** One data file holding {@code rows}, all in {@code year}. */
  private void file(int year, List<Map<String, Object>> rows) throws IOException {
    DataFile f = writer.writeRecords(rows,
        Collections.singletonMap("year", String.valueOf(year)));
    writer.commitDataFiles(Collections.singletonList(f), null);
  }

  private List<Map<String, Object>> filing(String acc, int rows, int year) {
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    for (int i = 1; i <= rows; i++) {
      out.add(row(acc, i, "paragraph " + i, year));
    }
    return out;
  }

  /** acc -> number of rows currently in the table for that acc. */
  private Map<String, Integer> countsByAcc() throws IOException {
    table.refresh();
    Map<String, Integer> counts = new TreeMap<String, Integer>();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record r : records) {
        String acc = String.valueOf(r.getField("acc"));
        Integer n = counts.get(acc);
        counts.put(acc, n == null ? 1 : n + 1);
      }
    }
    return counts;
  }

  private DedupeReport run(int year, boolean execute) throws IOException {
    return writer.dedupeCopies("acc", KEY, Expressions.equal("year", year), execute);
  }

  @Test void aFilingIngestedTwiceLosesItsSecondCopy() throws IOException {
    file(2024, filing("A", 3, 2024));
    file(2024, filing("A", 3, 2024));
    file(2024, filing("B", 3, 2024));

    DedupeReport dry = run(2024, false);
    assertEquals(9, dry.rows);
    assertEquals(6, dry.distinctRows);
    assertEquals(3, dry.rowsToRemove);
    assertEquals(0, dry.rowsRemoved);
    assertEquals(1, dry.groupsWithCopies());

    DedupeReport done = run(2024, true);
    assertEquals(3, done.rowsRemoved);
    assertEquals("OK", done.verification);
    Map<String, Integer> expected = new TreeMap<String, Integer>();
    expected.put("A", 3);
    expected.put("B", 3);
    assertEquals(expected, countsByAcc());
  }

  @Test void aFilingIngestedThreeTimesKeepsOne() throws IOException {
    for (int i = 0; i < 3; i++) {
      file(2024, filing("A", 2, 2024));
    }
    DedupeReport done = run(2024, true);
    assertEquals(4, done.rowsRemoved);
    assertEquals(Collections.singletonMap("A", 2), countsByAcc());
    assertEquals(Collections.singletonMap(3, 1), done.groupsByCopies);
  }

  @Test void aRowThatLegitimatelyRepeatsInOneIngestionIsKept() throws IOException {
    // Once ingested: row seq 1 appears twice, seq 2 once. Multiplicities 2 and 1 share no factor.
    List<Map<String, Object>> c = new ArrayList<Map<String, Object>>();
    c.add(row("C", 1, "same", 2024));
    c.add(row("C", 1, "same", 2024));
    c.add(row("C", 2, "other", 2024));
    file(2024, c);
    DedupeReport done = run(2024, true);
    assertEquals(0, done.rowsToRemove);
    assertEquals(Collections.singletonMap("C", 3), countsByAcc());
  }

  @Test void aLegitimateRepeatInsideATwiceIngestedFilingKeepsItsOwnTwin() throws IOException {
    // Ingested twice: seq 1 (legitimately twice) is there 4 times, seq 2 twice. gcd 2 -> 2 and 1.
    List<Map<String, Object>> c = new ArrayList<Map<String, Object>>();
    c.add(row("D", 1, "same", 2024));
    c.add(row("D", 1, "same", 2024));
    c.add(row("D", 2, "other", 2024));
    file(2024, c);
    file(2024, c);
    DedupeReport done = run(2024, true);
    assertEquals(3, done.rowsRemoved);
    assertEquals(Collections.singletonMap("D", 3), countsByAcc());
    assertEquals("OK", done.verification);
  }

  @Test void copiesAcrossDifferentFilesAreFound() throws IOException {
    List<Map<String, Object>> both = filing("E", 2, 2024);
    both.addAll(filing("E", 2, 2024));
    file(2024, both);            // both copies inside one file
    file(2024, filing("E", 2, 2024));  // and a third in another
    DedupeReport done = run(2024, true);
    assertEquals(4, done.rowsRemoved);
    assertEquals(Collections.singletonMap("E", 2), countsByAcc());
  }

  @Test void onlyTheNamedPartitionIsTouched() throws IOException {
    file(2024, filing("A", 2, 2024));
    file(2024, filing("A", 2, 2024));
    file(2025, filing("Z", 2, 2025));
    file(2025, filing("Z", 2, 2025));
    run(2024, true);
    Map<String, Integer> expected = new TreeMap<String, Integer>();
    expected.put("A", 2);
    expected.put("Z", 4);
    assertEquals(expected, countsByAcc());
  }

  @Test void aDryRunChangesNothing() throws IOException {
    file(2024, filing("A", 2, 2024));
    file(2024, filing("A", 2, 2024));
    long before = table.currentSnapshot().snapshotId();
    DedupeReport dry = run(2024, false);
    table.refresh();
    assertEquals(before, table.currentSnapshot().snapshotId());
    assertEquals(before, dry.snapshotBefore);
    assertEquals(Collections.singletonMap("A", 4), countsByAcc());
  }

  @Test void runningTwiceRemovesNothingTheSecondTime() throws IOException {
    file(2024, filing("A", 2, 2024));
    file(2024, filing("A", 2, 2024));
    run(2024, true);
    DedupeReport again = run(2024, true);
    assertEquals(0, again.rowsToRemove);
    assertEquals(0, again.rowsRemoved);
  }

  @Test void theKeyAndDistinctRowSetsSurvive() throws IOException {
    file(2024, filing("A", 4, 2024));
    file(2024, filing("A", 4, 2024));
    file(2024, filing("B", 4, 2024));
    DedupeReport before = run(2024, false);
    DedupeReport done = run(2024, true);
    assertEquals(before.distinctKeys, done.distinctKeys);
    assertEquals(before.distinctRows, done.distinctRows);
    assertEquals(8, done.distinctKeys);
  }

  @Test void aRolledBackSnapshotRestoresTheCopies() throws IOException {
    file(2024, filing("A", 2, 2024));
    file(2024, filing("A", 2, 2024));
    DedupeReport done = run(2024, true);
    assertEquals(Collections.singletonMap("A", 2), countsByAcc());
    table.manageSnapshots().rollbackTo(done.snapshotBefore).commit();
    assertEquals(Collections.singletonMap("A", 4), countsByAcc());
  }

  @Test void anUnknownColumnIsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> writer.dedupeCopies("nope", KEY, Expressions.equal("year", 2024), false));
    assertThrows(IllegalArgumentException.class,
        () -> writer.dedupeCopies("acc", Collections.singletonList("nope"),
            Expressions.equal("year", 2024), false));
  }

  @Test void digestCountsGrowAndKeepTheirCounts() {
    RowDigestCounts counts = new RowDigestCounts();
    int n = 200000;
    for (int i = 0; i < n; i++) {
      counts.add(i, i * 31L, i % 7);
      if (i % 3 == 0) {
        counts.add(i, i * 31L, i % 7);
      }
    }
    assertEquals(n, counts.size());
    assertTrue(counts.capacity() > n);
    for (int i = 0; i < n; i += 997) {
      int slot = counts.find(i, i * 31L);
      assertEquals(i % 3 == 0 ? 2 : 1, counts.count(slot));
      assertEquals(i % 7, counts.group(slot));
    }
    assertEquals(-1, counts.find(-5, -5));
  }
}
