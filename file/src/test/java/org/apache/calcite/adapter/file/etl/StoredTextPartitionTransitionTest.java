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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * GOV-075: a table already storing a declared-integer partition as text keeps loading as text;
 * a new table takes the declared type.
 */
@Tag("integration")
public class StoredTextPartitionTransitionTest {

  private static final String TABLE = "spending";

  @TempDir File tempDir;

  private static MaterializeConfig config(File warehouse, String yearType) {
    return MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .name(TABLE)
        .targetTableId(TABLE)
        .output(MaterializeOutputConfig.builder().build())
        .columns(
            Arrays.asList(ColumnConfig.builder().name("county").type("VARCHAR").build(),
            ColumnConfig.builder().name("amount").type("DOUBLE").build()))
        .partition(MaterializePartitionConfig.builder()
            .columns(Collections.singletonList("year"))
            .columnDefinitions(Collections.singletonList(
                new MaterializePartitionConfig.ColumnDefinition("year", yearType)))
            .build())
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
            .warehousePath(warehouse.getAbsolutePath())
            .namespace("default")
            .build())
        .build();
  }

  /** Loads one row under {@code year}, declared as {@code yearType}; returns the table's location. */
  private String load(File warehouse, String yearType, String year) throws Exception {
    Map<String, Object> row = new HashMap<>();
    row.put("county", "39001");
    row.put("amount", 1.5);
    List<Map<String, Object>> rows = Collections.singletonList(row);
    try (IcebergMaterializationWriter writer =
             new IcebergMaterializationWriter(new LocalFileStorageProvider(),
                 warehouse.getAbsolutePath(), null, new File(tempDir, "work-" + year))) {
      writer.initialize(config(warehouse, yearType));
      writer.writeBatch(rows.iterator(), Collections.singletonMap("year", year));
      writer.commit();
      return writer.getTableLocation();
    }
  }

  private static Table table(String location) {
    return new HadoopTables(new Configuration()).load(location);
  }

  @Test void aTableStoringTheYearAsTextKeepsLoadingItAsText() throws Exception {
    File warehouse = new File(tempDir, "existing");
    // As the 211 govdata tables were written before the writer took the declared types.
    load(warehouse, "VARCHAR", "2023");
    Table table = table(load(warehouse, "INTEGER", "2024"));
    assertEquals(Types.StringType.get(), table.schema().findField("year").type());
    assertEquals("2", table.currentSnapshot().summary().get("total-records"),
        "both loads committed");
  }

  @Test void aNewTableTakesTheDeclaredType() throws Exception {
    File warehouse = new File(tempDir, "new");
    Table table = table(load(warehouse, "INTEGER", "2024"));
    assertEquals(Types.IntegerType.get(), table.schema().findField("year").type());
  }
}
