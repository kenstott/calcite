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

import org.apache.calcite.adapter.file.etl.MaterializeConfig;
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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the {@code failOnPartitionShrink} guard and its tolerance. */
@Tag("unit")
public class IcebergPartitionShrinkGuardTest {

  @TempDir
  Path tempDir;

  private Table table;
  private IcebergTableWriter writer;

  @BeforeEach void setUp() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "year", Types.IntegerType.get()));
    Map<String, Object> catalogConfig = new HashMap<>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", tempDir.resolve("warehouse").toString());
    table = IcebergCatalogManager.createTable(catalogConfig, "shrink_guard", schema,
        PartitionSpec.builderFor(schema).identity("year").build());
    writer = new IcebergTableWriter(table, new LocalFileStorageProvider());
    writer.replacePartitionsDataFiles(file(1000));
  }

  @AfterEach void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  private List<DataFile> file(int rowCount) throws Exception {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("id", i);
      row.put("year", 2024);
      rows.add(row);
    }
    return Collections.singletonList(
        writer.writeRecords(rows, Collections.singletonMap("year", "2024")));
  }

  private long rowCount() throws Exception {
    table.refresh();
    long count = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        count += task.file().recordCount();
      }
    }
    return count;
  }

  private void replace(int rowCount, double maxShrinkFraction) throws Exception {
    writer.replacePartitionsDataFiles(file(rowCount), false, true, maxShrinkFraction);
  }

  @Test void shortOverwriteKeepsTheCommittedRows() throws Exception {
    assertThrows(IllegalStateException.class, () -> replace(127, 0.001));
    assertEquals(1000, rowCount());
  }

  @Test void dropWithinToleranceCommits() throws Exception {
    replace(999, 0.001);
    assertEquals(999, rowCount());
  }

  @Test void dropBeyondToleranceIsRejected() throws Exception {
    assertThrows(IllegalStateException.class, () -> replace(998, 0.001));
    assertEquals(1000, rowCount());
  }

  @Test void zeroToleranceRejectsAnyDrop() throws Exception {
    assertThrows(IllegalStateException.class, () -> replace(999, 0));
    replace(1000, 0);
    assertEquals(1000, rowCount());
  }

  @Test void toleranceIsReadFromConfig() {
    Map<String, Object> map = new HashMap<>();
    map.put("failOnPartitionShrink", true);
    map.put("maxPartitionShrinkFraction", 0.001);
    assertEquals(0.001,
        MaterializeConfig.IcebergConfig.fromMap(map).getMaxPartitionShrinkFraction());
  }

  @Test void toleranceWithoutGuardOrOutOfRangeIsRejected() {
    Map<String, Object> noGuard = new HashMap<>();
    noGuard.put("maxPartitionShrinkFraction", 0.001);
    assertThrows(IllegalArgumentException.class,
        () -> MaterializeConfig.IcebergConfig.fromMap(noGuard));
    Map<String, Object> tooLarge = new HashMap<>();
    tooLarge.put("failOnPartitionShrink", true);
    tooLarge.put("maxPartitionShrinkFraction", 1);
    assertThrows(IllegalArgumentException.class,
        () -> MaterializeConfig.IcebergConfig.fromMap(tooLarge));
  }
}
