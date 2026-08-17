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
 * End-to-end smoke test for sorted compaction: writes several small files with deliberately
 * out-of-order rows, compacts them with a sortOrder, and reads the rewritten files back.
 *
 * <p>Distinct from {@code IcebergCompactionSortOrderTest}, which exercises the comparator and
 * the config parser in isolation. This one proves the whole path — that the sortOrder actually
 * reaches the rewrite, that buffered rows survive the reader's record reuse, and that what
 * lands on disk is ordered. Sorting is only worth anything if the ON-DISK order changes, since
 * the entire point is narrowing each file's min/max so the reader can prune.
 */
@Tag("unit")
public class IcebergCompactionSortSmokeTest {

  @TempDir
  Path tempDir;

  private Table table;
  private StorageProvider storageProvider;

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
    table = IcebergCatalogManager.createTable(catalogConfig, "sort_smoke", SCHEMA, spec);
  }

  @AfterEach void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  /** One row as the writer's map form. */
  private static Map<String, Object> row(String name, int id) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("name", name);
    m.put("id", id);
    m.put("year", 2024);
    return m;
  }

  /** Writes one small file per call so compaction has several to merge. */
  private void writeFile(IcebergTableWriter writer, List<Map<String, Object>> rows)
      throws Exception {
    Map<String, String> partition = new HashMap<>();
    partition.put("year", "2024");
    DataFile file = writer.writeRecords(rows, partition);
    assertNotNull(file, "each write should produce a data file");
    table.newAppend().appendFile(file).commit();
  }

  /** Every row currently in the table, in the order the files and rows are laid out. */
  private List<String> readNamesInFileOrder() throws Exception {
    List<String> names = new ArrayList<>();
    table.refresh();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        InputFile in = table.io().newInputFile(task.file().path().toString());
        try (CloseableIterable<Record> records = Parquet.read(in)
            .project(SCHEMA)
            .createReaderFunc(fileSchema ->
                GenericParquetReaders.buildReader(SCHEMA, fileSchema))
            .build()) {
          for (Record r : records) {
            names.add((String) r.getField("name"));
          }
        }
      }
    }
    return names;
  }

  @Test void compactionWritesRowsInSortOrder() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    // Five files, each internally unsorted and interleaved with the others, so no single
    // file's order could produce a sorted result by accident.
    writeFile(writer, Arrays.asList(row("zebra", 1), row("alpha", 2)));
    writeFile(writer, Arrays.asList(row("mango", 3), row("beta", 4)));
    writeFile(writer, Arrays.asList(row("yak", 5), row("cherry", 6)));
    writeFile(writer, Arrays.asList(row("delta", 7), row("walrus", 8)));
    writeFile(writer, Arrays.asList(row("echo", 9), row("violet", 10)));

    List<String> before = readNamesInFileOrder();
    assertEquals(10, before.size(), "precondition: all rows written");
    List<String> sortedExpectation = new ArrayList<>(before);
    Collections.sort(sortedExpectation);
    assertTrue(!before.equals(sortedExpectation),
        "precondition: the table must start out of order, or this test proves nothing");

    // smallFileSizeBytes is generous so these tiny test files all qualify as "small".
    int compacted = writer.compactSmallFiles(
        128L * 1024 * 1024, 2, 100L * 1024 * 1024, 7, Arrays.asList("name"));
    assertEquals(1, compacted, "the single year=2024 partition should be compacted");

    List<String> after = readNamesInFileOrder();
    assertEquals(sortedExpectation, after,
        "after sorted compaction the rows on disk must be in sortOrder");
  }

  @Test void compactionWithoutSortOrderLeavesOrderAlone() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeFile(writer, Arrays.asList(row("zebra", 1), row("alpha", 2)));
    writeFile(writer, Arrays.asList(row("mango", 3), row("beta", 4)));
    writeFile(writer, Arrays.asList(row("yak", 5), row("cherry", 6)));

    // No sortOrder: compaction must still merge the files, just without reordering. This is
    // the pre-existing behaviour and the fallback path when a sort budget is exceeded.
    int compacted = writer.compactSmallFiles(
        128L * 1024 * 1024, 2, 100L * 1024 * 1024, 7, Collections.<String>emptyList());
    assertEquals(1, compacted);

    // Every row survives, but the SEQUENCE is not asserted: planFiles() returns files in an
    // arbitrary order, so the concatenation order varies run to run even though no reordering
    // happens within a file. The contract without a sortOrder is "same rows", not "same order".
    List<String> after = readNamesInFileOrder();
    List<String> expected =
        new ArrayList<>(Arrays.asList("zebra", "alpha", "mango", "beta", "yak", "cherry"));
    Collections.sort(expected);
    List<String> actual = new ArrayList<>(after);
    Collections.sort(actual);
    assertEquals(expected, actual, "an unsorted rewrite must preserve every row");
  }

  @Test void unknownSortColumnFallsBackToUnsortedRatherThanFailing() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    writeFile(writer, Arrays.asList(row("zebra", 1), row("alpha", 2)));
    writeFile(writer, Arrays.asList(row("mango", 3), row("beta", 4)));

    // A sortOrder naming a column that is not in the schema must not fail the compaction —
    // merging the files is still correct and valuable; only the ordering is forgone.
    int compacted = writer.compactSmallFiles(
        128L * 1024 * 1024, 2, 100L * 1024 * 1024, 7, Arrays.asList("no_such_column"));
    assertEquals(1, compacted);

    List<String> after = readNamesInFileOrder();
    List<String> expected = new ArrayList<>(Arrays.asList("zebra", "alpha", "mango", "beta"));
    Collections.sort(expected);
    List<String> actual = new ArrayList<>(after);
    Collections.sort(actual);
    assertEquals(expected, actual, "an unsortable sortOrder must still preserve every row");
  }
}
