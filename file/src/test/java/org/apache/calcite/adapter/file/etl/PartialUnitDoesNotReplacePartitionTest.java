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
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import org.junit.jupiter.api.AfterEach;
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
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A fetch unit that fails part-way (a refused or cut-short source response) must leave the
 * partition it was replacing untouched. The unit is large enough to flush data files before it
 * fails, which is the point at which a partition could otherwise be committed as a prefix.
 */
@Tag("unit")
public class PartialUnitDoesNotReplacePartitionTest {

  /** Above the writer's per-partition flush threshold, so files are written mid-unit. */
  private static final int ROWS_BEFORE_FAILURE = 120000;

  @TempDir
  File tempDir;

  private IcebergMaterializationWriter writer;

  @AfterEach public void tearDown() throws Exception {
    if (writer != null) {
      writer.close();
    }
  }

  @Test public void failedUnitKeepsThePreviousPartition() throws Exception {
    assertOldRowsSurvive(false);
  }

  @Test public void failedUnitKeepsThePreviousPartitionWithCommitPerPartition() throws Exception {
    assertOldRowsSurvive(true);
  }

  @Test public void completedUnitReplacesThePreviousPartition() throws Exception {
    File warehouse = new File(tempDir, "warehouse_complete");
    warehouse.mkdirs();
    seed(warehouse, "t_complete", false);

    writer = open(warehouse, "t_complete", false);
    writer.writeBatch(rows(ROWS_BEFORE_FAILURE, false), partition());
    writer.commit();

    assertEquals((long) ROWS_BEFORE_FAILURE, committedRecords());
  }

  private void assertOldRowsSurvive(boolean commitPerPartition) throws Exception {
    File warehouse = new File(tempDir, "warehouse_fail_" + commitPerPartition);
    warehouse.mkdirs();
    seed(warehouse, "t_fail", commitPerPartition);

    writer = open(warehouse, "t_fail", commitPerPartition);
    assertThrows(RuntimeException.class,
        () -> writer.writeBatch(rows(ROWS_BEFORE_FAILURE, true), partition()));
    writer.commit();

    assertEquals(5L, committedRecords(),
        "a unit that failed part-way must not replace the partition with its partial rows");
  }

  /** Commits the partition's existing 5 rows with a separate writer, as a previous run would. */
  private void seed(File warehouse, String table, boolean commitPerPartition) throws Exception {
    IcebergMaterializationWriter seeder = open(warehouse, table, commitPerPartition);
    try {
      seeder.writeBatch(rows(5, false), partition());
      seeder.commit();
    } finally {
      seeder.close();
    }
    writer = null;
    tableLocation = seeder.getTableLocation();
  }

  private String tableLocation;

  private IcebergMaterializationWriter open(File warehouse, String table,
      boolean commitPerPartition) throws Exception {
    StorageProvider storage = new LocalFileStorageProvider();
    IcebergMaterializationWriter w = new IcebergMaterializationWriter(storage,
        warehouse.getAbsolutePath(), null, tempDir);
    w.initialize(config(warehouse, table, commitPerPartition));
    return w;
  }

  private static Map<String, String> partition() {
    Map<String, String> vars = new LinkedHashMap<>();
    vars.put("region", "east");
    return vars;
  }

  /** {@code n} rows; when {@code failAfter}, throws once they are delivered, as a refusal would. */
  private static Iterator<Map<String, Object>> rows(final int n, final boolean failAfter) {
    return new Iterator<Map<String, Object>>() {
      private int next = 0;

      @Override public boolean hasNext() {
        if (next >= n && failAfter) {
          throw new IllegalStateException("source refused the request");
        }
        return next < n;
      }

      @Override public Map<String, Object> next() {
        if (next >= n) {
          throw new NoSuchElementException();
        }
        Map<String, Object> row = new HashMap<>();
        row.put("id", next);
        row.put("value", "v" + next);
        next++;
        return row;
      }
    };
  }

  private long committedRecords() {
    Table table = new HadoopTables(new Configuration()).load(tableLocation);
    table.refresh();
    if (table.currentSnapshot() == null) {
      return 0L;
    }
    return Long.parseLong(table.currentSnapshot().summary().get("total-records"));
  }

  private static MaterializeConfig config(File warehouse, String table,
      boolean commitPerPartition) {
    List<ColumnConfig> columns = Arrays.asList(
        column("id", "INTEGER"), column("value", "VARCHAR"), column("region", "VARCHAR"));
    return MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .name(table)
        .targetTableId(table)
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
            .warehousePath(warehouse.getAbsolutePath())
            .namespace("default")
            .overwritePartitions(true)
            .build())
        .build();
  }

  private static ColumnConfig column(String name, String type) {
    return ColumnConfig.builder().name(name).type(type).build();
  }
}
