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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A compaction replace commit keeps the record count and every referenced file exists. */
@Tag("unit")
public class IcebergCompactionRecordInvariantTest {

  @TempDir
  Path tempDir;

  private Table table;
  private IcebergTableWriter writer;

  @BeforeEach
  void setUp() throws Exception {
    Map<String, Object> catalogConfig = new HashMap<>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", tempDir.resolve("warehouse").toString());
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "year", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("year").build();
    table = IcebergCatalogManager.createTable(catalogConfig, "t", schema, spec);
    writer = new IcebergTableWriter(table, new LocalFileStorageProvider());
    for (int i = 0; i < 12; i++) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", i);
      row.put("year", 2020);
      Map<String, String> part = new HashMap<>();
      part.put("year", "2020");
      DataFile df = writer.writeRecords(Collections.singletonList(row), part);
      writer.commitDataFiles(Collections.singletonList(df), null);
    }
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  private List<DataFile> liveFiles() throws Exception {
    table.refresh();
    List<DataFile> out = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask t : tasks) {
        out.add(t.file());
      }
    }
    return out;
  }

  private long liveRecords() throws Exception {
    long n = 0;
    for (DataFile f : liveFiles()) {
      n += f.recordCount();
    }
    return n;
  }

  @Test void unreadableSourceFileFailsCompactionAndCommitsNothing() throws Exception {
    long before = liveRecords();
    long snapshotBefore = table.currentSnapshot().snapshotId();
    String victim = liveFiles().get(0).path().toString();
    Files.delete(Paths.get(victim.replaceFirst("^file:", "")));

    assertThrows(RuntimeException.class,
        () -> writer.compactSmallFiles(128L * 1024 * 1024, 2, 128L * 1024 * 1024, 7));

    table.refresh();
    assertEquals(snapshotBefore, table.currentSnapshot().snapshotId(),
        "a failed compaction must not commit a replace snapshot");
    assertEquals(before, liveRecords());
  }

  @Test void concurrentCompactionsKeepRecordTotalAndReferencedFiles() throws Exception {
    long before = liveRecords();
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch go = new CountDownLatch(1);
    List<Future<?>> results = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      results.add(pool.submit(() -> {
        go.await();
        new IcebergTableWriter(table, new LocalFileStorageProvider())
            .compactSmallFiles(128L * 1024 * 1024, 2, 128L * 1024 * 1024, 7);
        return null;
      }));
    }
    go.countDown();
    for (Future<?> f : results) {
      try {
        f.get();
      } catch (java.util.concurrent.ExecutionException e) {
        // a loud conflict is acceptable; a silent record loss is not
      }
    }
    pool.shutdown();

    assertEquals(before, liveRecords(), "compaction must not change the record total");
    for (DataFile f : liveFiles()) {
      assertTrue(Files.exists(Paths.get(f.path().toString().replaceFirst("^file:", ""))),
          "referenced file missing: " + f.path());
    }
  }
}
