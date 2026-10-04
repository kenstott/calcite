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

import org.apache.iceberg.Table;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * The tracker's "already materialized" record must say what it was recorded for: one year range
 * of one table instance. A record that outlives its range or its table lets a run skip work it
 * never did.
 */
@Tag("unit")
class IcebergMaterializerScopedCompletionTest {

  @TempDir
  Path tempDir;

  private Map<String, Object> catalogConfig;

  @BeforeEach
  void setUp() {
    catalogConfig = new HashMap<String, Object>();
    catalogConfig.put("catalog", "hadoop");
    catalogConfig.put("warehouse", tempDir.resolve("warehouse").toString());
    catalogConfig.put("namespace", "test_ns");
    IcebergCatalogManager.clearCache();
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  private static IcebergMaterializer.MaterializationConfig configFor(int startYear, int endYear) {
    return IcebergMaterializer.MaterializationConfig.builder()
        .sourcePattern("year=*/*facts*.parquet")
        .sourceFormat(IcebergMaterializer.SourceFormat.PARQUET)
        .targetTableId("financial_line_items")
        .sourceTableName("facts")
        .batchPartitionColumns(Collections.singletonList("year"))
        .yearRange(startYear, endYear)
        .description("facts")
        .build();
  }

  @Test void testEachYearRangeHasItsOwnCompletionKey() {
    String y2015 = IcebergMaterializer.scopedCompletionKey(configFor(2015, 2015));
    String y2016 = IcebergMaterializer.scopedCompletionKey(configFor(2016, 2016));
    String wide = IcebergMaterializer.scopedCompletionKey(configFor(2015, 2016));

    assertNotEquals(y2015, y2016, "a completion recorded for 2015 must not be read back for 2016");
    assertNotEquals(y2015, wide, "a one-year record must not stand in for a wider range");
    assertEquals(y2015, IcebergMaterializer.scopedCompletionKey(configFor(2015, 2015)),
        "the same range must find its own record");
  }

  @Test void testRecreatedTableIsADifferentInstance() {
    List<IcebergCatalogManager.ColumnDef> columns = new ArrayList<IcebergCatalogManager.ColumnDef>();
    columns.add(new IcebergCatalogManager.ColumnDef("accession_number", "VARCHAR"));
    columns.add(new IcebergCatalogManager.ColumnDef("year", "VARCHAR"));
    List<String> partitions = Collections.singletonList("year");

    Table first = IcebergCatalogManager.createTableFromColumns(
        catalogConfig, "test_ns.facts", columns, partitions);
    String firstInstance = IcebergMaterializer.tableInstanceId(first);
    assertEquals(firstInstance, IcebergMaterializer.tableInstanceId(
        IcebergCatalogManager.loadTable(catalogConfig, "test_ns.facts")),
        "reloading the same table must report the same instance");

    IcebergCatalogManager.dropTable(catalogConfig, "test_ns.facts", true);
    Table second = IcebergCatalogManager.createTableFromColumns(
        catalogConfig, "test_ns.facts", columns, partitions);

    assertNotEquals(firstInstance, IcebergMaterializer.tableInstanceId(second),
        "tracker state recorded against the dropped table must not match the recreated one");
  }
}
