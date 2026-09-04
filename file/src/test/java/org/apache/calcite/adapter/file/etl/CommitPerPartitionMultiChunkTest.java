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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code commitPerPartition} when one fetch unit arrives as several chunks.
 *
 * <p>{@code writeBatch} is a CHUNK boundary, not a fetch-unit boundary: a fetch unit larger
 * than the flush threshold is delivered as several calls carrying the same partition variables.
 * Because the commit path is {@code replacePartitions}, which swaps in exactly the files it is
 * given, the second chunk's commit must carry the first chunk's files too — otherwise it
 * replaces the partition with only its own rows and the first chunk is silently discarded.
 *
 * <p>This is the case that broke cftc in production: chunk 2 of every large day either lost
 * chunk 1's rows or failed the batch outright.
 */
@Tag("unit")
public class CommitPerPartitionMultiChunkTest {

  @TempDir
  File tempDir;

  private StorageProvider storageProvider;
  private IcebergMaterializationWriter writer;

  @BeforeEach public void setUp() {
    storageProvider = new LocalFileStorageProvider();
  }

  @AfterEach public void tearDown() throws Exception {
    if (writer != null) {
      writer.close();
    }
  }

  @Test public void secondChunkOfAPartitionKeepsTheFirstChunksRows() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_multichunk");
    warehouseDir.mkdirs();
    writer = new IcebergMaterializationWriter(storageProvider,
        warehouseDir.getAbsolutePath(), null);
    writer.initialize(config(warehouseDir, "multi_chunk_table", true));

    // Two chunks of ONE fetch unit — same partition variables, as the pipeline delivers them.
    Map<String, String> partition = partition("east");
    writer.writeBatch(rows(1, 2), partition);
    writer.writeBatch(rows(3, 4), partition);
    writer.commit();

    assertEquals(4L, committedRecords(),
        "the second chunk's commit must carry the first chunk's files, or replacePartitions "
            + "swaps the partition down to just the second chunk");
  }

  /** Several chunks, as a large day produces — every one of them must survive. */
  @Test public void manyChunksOfOnePartitionAllSurvive() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_manychunks");
    warehouseDir.mkdirs();
    writer = new IcebergMaterializationWriter(storageProvider,
        warehouseDir.getAbsolutePath(), null);
    writer.initialize(config(warehouseDir, "many_chunk_table", true));

    Map<String, String> partition = partition("east");
    for (int i = 0; i < 5; i++) {
      writer.writeBatch(rows(i * 2 + 1, i * 2 + 2), partition);
    }
    writer.commit();

    assertEquals(10L, committedRecords());
  }

  /** Chunking one partition must not disturb another partition committed alongside it. */
  @Test public void chunksOfOnePartitionDoNotDisturbAnother() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_twoparts");
    warehouseDir.mkdirs();
    writer = new IcebergMaterializationWriter(storageProvider,
        warehouseDir.getAbsolutePath(), null);
    writer.initialize(config(warehouseDir, "two_part_table", true));

    writer.writeBatch(rows(1, 2), partition("east"));
    writer.writeBatch(rows(3, 4), partition("west"));
    writer.writeBatch(rows(5, 6), partition("east"));
    writer.commit();

    assertEquals(6L, committedRecords());
  }

  /** With the mode off, the end-of-run commit must still publish every row exactly once. */
  @Test public void commitPerPartitionOffStillWritesEveryRowOnce() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_off");
    warehouseDir.mkdirs();
    writer = new IcebergMaterializationWriter(storageProvider,
        warehouseDir.getAbsolutePath(), null);
    writer.initialize(config(warehouseDir, "off_table", false));

    Map<String, String> partition = partition("east");
    writer.writeBatch(rows(1, 2), partition);
    writer.writeBatch(rows(3, 4), partition);
    writer.commit();

    assertEquals(4L, committedRecords());
  }

  /** Rows must land during the run, not only at the end — that is the whole point of the mode. */
  @Test public void partitionIsVisibleBeforeTheRunEnds() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_incremental");
    warehouseDir.mkdirs();
    writer = new IcebergMaterializationWriter(storageProvider,
        warehouseDir.getAbsolutePath(), null);
    writer.initialize(config(warehouseDir, "incremental_table", true));

    writer.writeBatch(rows(1, 2), partition("east"));

    assertTrue(committedRecords() >= 2L,
        "an interrupted run must keep partitions it finished, so they have to be committed "
            + "before commit() is ever called");
  }

  /**
   * A partition is displaced once and added to thereafter: the first commit carrying files for it
   * is a replace, every later one an append. Re-replacing with the partition's whole cumulative
   * file list would also read correctly here, but it re-registers DataFile handles for objects
   * this run committed earlier — and compaction reclaims those once it rewrites a partition, so
   * the live snapshot ends up naming deleted objects. Asserting the operation sequence is what
   * keeps that from creeping back.
   */
  @Test public void replacesAPartitionOnceThenAppends() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_replace_once");
    warehouseDir.mkdirs();
    writer = new IcebergMaterializationWriter(storageProvider,
        warehouseDir.getAbsolutePath(), null);
    writer.initialize(config(warehouseDir, "replace_once_table", true));

    Map<String, String> partition = partition("east");
    writer.writeBatch(rows(1, 2), partition);
    writer.writeBatch(rows(3, 4), partition);
    writer.writeBatch(rows(5, 6), partition);
    writer.commit();

    Table table = new HadoopTables(new Configuration()).load(writer.getTableLocation());
    List<String> operations = new ArrayList<>();
    for (Snapshot snapshot : table.snapshots()) {
      operations.add(snapshot.operation());
    }
    assertFalse(operations.isEmpty(), "expected at least one snapshot");
    // On a fresh table the opening replace-partitions deletes nothing, so Iceberg records it as
    // an append; what matters is that no LATER commit removes files, which is what re-committing
    // a partition's cumulative list would do.
    int deletingSnapshots = 0;
    for (Snapshot snapshot : table.snapshots()) {
      String deleted = snapshot.summary().get("deleted-data-files");
      if (deleted != null && Integer.parseInt(deleted) > 0) {
        deletingSnapshots++;
      }
    }
    assertEquals(0, deletingSnapshots,
        "no commit may delete a file this run already committed; operations=" + operations);
    assertEquals(6, committedRecords(), "every row survives the replace-then-append sequence");
  }

  private static Map<String, String> partition(String region) {
    Map<String, String> vars = new LinkedHashMap<>();
    vars.put("region", region);
    return vars;
  }

  private static Iterator<Map<String, Object>> rows(int... ids) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int id : ids) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", id);
      row.put("value", "v" + id);
      rows.add(row);
    }
    return rows.iterator();
  }

  /** Reads the committed record count straight from the table the writer created. */
  private long committedRecords() {
    Table table = new HadoopTables(new Configuration()).load(writer.getTableLocation());
    table.refresh();
    if (table.currentSnapshot() == null) {
      return 0L;
    }
    String total = table.currentSnapshot().summary().get("total-records");
    return total == null ? 0L : Long.parseLong(total);
  }

  private static MaterializeConfig config(File warehouseDir, String tableName,
      boolean commitPerPartition) {
    List<ColumnConfig> columns = Arrays.asList(
        column("id", "INTEGER"), column("value", "VARCHAR"), column("region", "VARCHAR"));
    return MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .name(tableName)
        .targetTableId(tableName)
        .output(MaterializeOutputConfig.builder().build())
        .columns(columns)
        .partition(MaterializePartitionConfig.builder()
            .columns(Collections.singletonList("region"))
            .build())
        .options(MaterializeOptionsConfig.builder()
            .commitPerPartition(commitPerPartition)
            .build())
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
            .warehousePath(warehouseDir.getAbsolutePath())
            .namespace("default")
            .overwritePartitions(true)
            .build())
        .build();
  }

  private static ColumnConfig column(String name, String type) {
    return ColumnConfig.builder().name(name).type(type).build();
  }
}
