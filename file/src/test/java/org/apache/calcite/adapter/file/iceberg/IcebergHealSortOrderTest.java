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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers {@link IcebergTableWriter#healSortOrder}, the full-partition rewrite.
 *
 * <p>Ordinary sorted compaction only inspects files below the small-file threshold, so a table
 * already packed into large files — or rebuilt in full each run, as the entity bridges are —
 * never gets sorted by it. Heal exists for exactly that case, and these tests pin the parts that
 * would otherwise fail silently: that it rewrites files a size-based compaction would skip, that
 * it is idempotent via the recorded property, and that it refuses rather than doing an expensive
 * rewrite it cannot honour.
 */
@Tag("unit")
public class IcebergHealSortOrderTest {

  @TempDir
  Path tempDir;

  private Table table;
  private StorageProvider storageProvider;

  private static final String BUDGET_PROPERTY = "calcite.iceberg.sort.memory.budget.bytes";

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "name", Types.StringType.get()),
      Types.NestedField.optional(2, "id", Types.IntegerType.get()),
      Types.NestedField.optional(3, "year", Types.IntegerType.get()));

  @BeforeEach void setUp() {
    storageProvider = new LocalFileStorageProvider();
    Map<String, Object> catalogConfig = new HashMap<>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", tempDir.resolve("warehouse").toString());
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("year").build();
    table = IcebergCatalogManager.createTable(catalogConfig, "heal_test", SCHEMA, spec);
  }

  @AfterEach void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  private static Map<String, Object> row(String name, int id, int year) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("name", name);
    m.put("id", id);
    m.put("year", year);
    return m;
  }

  private void writeFile(IcebergTableWriter writer, int year, List<Map<String, Object>> rows)
      throws Exception {
    Map<String, String> partition = new HashMap<>();
    partition.put("year", String.valueOf(year));
    DataFile file = writer.writeRecords(rows, partition);
    assertNotNull(file);
    table.newAppend().appendFile(file).commit();
  }

  private List<String> namesIn(int year) throws Exception {
    List<String> names = new ArrayList<>();
    table.refresh();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        if (!task.file().partition().toString().contains(String.valueOf(year))) {
          continue;
        }
        InputFile in = table.io().newInputFile(task.file().path().toString());
        try (CloseableIterable<Record> records = Parquet.read(in)
            .project(SCHEMA)
            .createReaderFunc(fs -> GenericParquetReaders.buildReader(SCHEMA, fs))
            .build()) {
          for (Record r : records) {
            names.add((String) r.getField("name"));
          }
        }
      }
    }
    return names;
  }

  @Test void healSortsFilesThatSizeBasedCompactionWouldSkip() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeFile(writer, 2024, Arrays.asList(row("zebra", 1, 2024), row("alpha", 2, 2024)));
    writeFile(writer, 2024, Arrays.asList(row("mango", 3, 2024), row("beta", 4, 2024)));
    writeFile(writer, 2024, Arrays.asList(row("yak", 5, 2024), row("cherry", 6, 2024)));

    // Precondition: with a 1-byte small-file threshold nothing qualifies as small, so ordinary
    // compaction is a no-op. This is the state every already-compacted table is in.
    assertEquals(0, writer.compactSmallFiles(128L * 1024 * 1024, 2, 1L, 7,
        Arrays.asList("name")),
        "size-based compaction must find nothing to do — that is why heal exists");

    List<String> before = namesIn(2024);
    List<String> expected = new ArrayList<>(before);
    Collections.sort(expected);
    assertTrue(!before.equals(expected), "precondition: partition starts unsorted");

    int healed = writer.healSortOrder(Arrays.asList("name"), 128L * 1024 * 1024, 7);

    assertEquals(1, healed, "the one partition should be rewritten");
    assertEquals(expected, namesIn(2024), "heal must leave the partition sorted on disk");
  }

  @Test void healIsIdempotentViaTheRecordedProperty() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeFile(writer, 2024, Arrays.asList(row("zebra", 1, 2024), row("alpha", 2, 2024)));

    assertEquals(1, writer.healSortOrder(Arrays.asList("name"), 128L * 1024 * 1024, 7));
    table.refresh();
    assertEquals("name", table.properties().get(IcebergTableWriter.SORTED_BY_PROPERTY));

    // Second call must cost nothing: this runs every ETL cycle and a full rewrite each time
    // would be ruinous.
    assertEquals(0, writer.healSortOrder(Arrays.asList("name"), 128L * 1024 * 1024, 7),
        "a table already recording this order must not be rewritten again");
  }

  @Test void changingTheDeclaredOrderTriggersAnotherHeal() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeFile(writer, 2024, Arrays.asList(row("zebra", 1, 2024), row("alpha", 2, 2024)));

    assertEquals(1, writer.healSortOrder(Arrays.asList("name"), 128L * 1024 * 1024, 7));
    // A different declared order is exactly the case the property gate must NOT swallow.
    assertEquals(1, writer.healSortOrder(Arrays.asList("id"), 128L * 1024 * 1024, 7),
        "a changed sortOrder must re-heal rather than being treated as already done");
    table.refresh();
    assertEquals("id", table.properties().get(IcebergTableWriter.SORTED_BY_PROPERTY));
  }

  @Test void healsEachPartitionIndependently() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeFile(writer, 2023, Arrays.asList(row("delta", 1, 2023), row("apple", 2, 2023)));
    writeFile(writer, 2024, Arrays.asList(row("zebra", 3, 2024), row("beta", 4, 2024)));

    assertEquals(2, writer.healSortOrder(Arrays.asList("name"), 128L * 1024 * 1024, 7));

    assertEquals(Arrays.asList("apple", "delta"), namesIn(2023));
    assertEquals(Arrays.asList("beta", "zebra"), namesIn(2024));
  }

  @Test void refusesToHealOnAnUnresolvableSortOrder() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeFile(writer, 2024, Arrays.asList(row("zebra", 1, 2024), row("alpha", 2, 2024)));

    // Rewriting the whole table and THEN recording a sort order it was not written in would be
    // worse than doing nothing: the claim would be false and the cost already paid.
    assertEquals(0, writer.healSortOrder(Arrays.asList("no_such_column"), 128L * 1024 * 1024, 7));
    table.refresh();
    assertEquals(null, table.properties().get(IcebergTableWriter.SORTED_BY_PROPERTY),
        "a refused heal must not record a sort order");
    assertEquals(Arrays.asList("zebra", "alpha"), namesIn(2024), "data left untouched");
  }

  @Test void healUsesTheExternalMergeWhenThePartitionExceedsTheBudget() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    // Six files whose rows interleave across the whole alphabet, so a correct result cannot
    // come from any single run being emitted in order.
    writeFile(writer, 2024, Arrays.asList(row("zebra", 1, 2024), row("alpha", 2, 2024)));
    writeFile(writer, 2024, Arrays.asList(row("mango", 3, 2024), row("beta", 4, 2024)));
    writeFile(writer, 2024, Arrays.asList(row("yak", 5, 2024), row("cherry", 6, 2024)));
    writeFile(writer, 2024, Arrays.asList(row("delta", 7, 2024), row("walrus", 8, 2024)));
    writeFile(writer, 2024, Arrays.asList(row("echo", 9, 2024), row("violet", 10, 2024)));
    writeFile(writer, 2024, Arrays.asList(row("foxtrot", 11, 2024), row("umbra", 12, 2024)));

    List<String> expected = new ArrayList<>(namesIn(2024));
    Collections.sort(expected);

    // A 1-byte budget forces every partition down the spill-and-merge path. Without this the
    // external merge is unreachable in a unit test, and it is the half of heal that carries the
    // real risk: run spilling, the k-way merge, and record reuse across readers.
    String previous = System.getProperty(BUDGET_PROPERTY);
    System.setProperty(BUDGET_PROPERTY, "1");
    try {
      assertEquals(1, writer.healSortOrder(Arrays.asList("name"), 128L * 1024 * 1024, 7));
    } finally {
      if (previous == null) {
        System.clearProperty(BUDGET_PROPERTY);
      } else {
        System.setProperty(BUDGET_PROPERTY, previous);
      }
    }

    assertEquals(expected, namesIn(2024),
        "the external merge must produce the same total order as an in-memory sort");
  }

  @Test void emptySortOrderIsANoOp() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeFile(writer, 2024, Arrays.asList(row("zebra", 1, 2024)));
    assertEquals(0, writer.healSortOrder(Collections.<String>emptyList(),
        128L * 1024 * 1024, 7));
  }
}
