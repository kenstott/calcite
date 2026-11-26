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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A table identity-partitioned by a {@code date} column. */
@Tag("unit")
class IcebergTableWriterDatePartitionTest {

  @TempDir
  Path tempDir;

  private Table table;

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  private IcebergTableWriter writer() {
    Map<String, Object> catalogConfig = new HashMap<String, Object>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", tempDir.resolve("warehouse").toString());
    Schema schema =
        new Schema(Types.NestedField.optional(1, "data", Types.StringType.get()),
            Types.NestedField.optional(2, "as_of", Types.DateType.get()));
    table = IcebergCatalogManager.createTable(catalogConfig, "date_partitioned", schema,
        PartitionSpec.builderFor(schema).identity("as_of").build());
    return new IcebergTableWriter(table, new LocalFileStorageProvider());
  }

  private static void replace(IcebergTableWriter writer, String data, String asOf)
      throws Exception {
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("data", data);
    row.put("as_of", asOf);
    DataFile file = writer.writeRecords(Collections.singletonList(row),
        Collections.singletonMap("as_of", asOf));
    writer.replacePartitionsDataFiles(Collections.singletonList(file));
  }

  private List<String> contents() throws Exception {
    table.refresh();
    List<String> out = new ArrayList<String>();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record r : records) {
        out.add(r.getField("as_of") + ":" + r.getField("data"));
      }
    }
    Collections.sort(out);
    return out;
  }

  @Test void replacingOneDatePartitionKeepsTheOthers() throws Exception {
    IcebergTableWriter writer = writer();
    replace(writer, "first", "2026-11-25");
    replace(writer, "second", "2026-11-26");
    replace(writer, "second-rerun", "2026-11-26");

    assertEquals(Arrays.asList("2026-11-25:first", "2026-11-26:second-rerun"), contents());
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = table.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        int days = task.file().partition().get(0, Integer.class).intValue();
        assertTrue(task.file().path().toString()
            .contains("as_of=" + LocalDate.ofEpochDay(days)));
      }
    }
  }

  @Test void rowDeleteRewritesTheFileInItsDatePartition() throws Exception {
    IcebergTableWriter writer = writer();
    Map<String, Object> keep = new HashMap<String, Object>();
    keep.put("data", "keep");
    keep.put("as_of", "2026-11-26");
    Map<String, Object> drop = new HashMap<String, Object>(keep);
    drop.put("data", "drop");
    DataFile file = writer.writeRecords(Arrays.asList(keep, drop),
        Collections.singletonMap("as_of", "2026-11-26"));
    writer.replacePartitionsDataFiles(Collections.singletonList(file));

    writer.deleteRows("data", Collections.singleton("drop"));

    assertEquals(Collections.singletonList("2026-11-26:keep"), contents());
  }

  @Test void unreadableDatePartitionValueFails() {
    IcebergTableWriter writer = writer();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("data", "x");
    assertThrows(IllegalStateException.class, () ->
        writer.writeRecords(Collections.singletonList(row),
            Collections.singletonMap("as_of", "yesterday")));
  }
}
