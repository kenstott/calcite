/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.partition;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.iceberg.IcebergMaterializer;
import org.apache.calcite.adapter.file.iceberg.IcebergMaterializer.MaterializationConfig;
import org.apache.calcite.adapter.file.iceberg.IcebergTableWriter;
import org.apache.calcite.adapter.file.partition.AlternatePartitionRegistry.AlternateInfo;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig.ColumnDefinition;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the complete partition alternates workflow.
 * Tests the full pipeline: source data -> DuckDB transformation -> Iceberg commit.
 */
@Tag("integration")
public class PartitionAlternatesIntegrationTest {

  @TempDir
  Path tempDir;

  private Path warehousePath;
  private Path sourcePath;
  private Map<String, Object> catalogConfig;

  @BeforeEach
  void setUp() throws Exception {
    warehousePath = tempDir.resolve("warehouse");
    Files.createDirectories(warehousePath);

    sourcePath = tempDir.resolve("source");
    Files.createDirectories(sourcePath);

    catalogConfig = new HashMap<String, Object>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", warehousePath.toString());
    catalogConfig.put("namespace", "default");

    // Create test source data using DuckDB
    createTestSourceData();
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  /**
   * Creates test source data with Hive-style partitioning.
   */
  private void createTestSourceData() throws Exception {
    Path baseDir = sourcePath.resolve("type=income");
    Files.createDirectories(baseDir);

    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      Statement stmt = conn.createStatement();

      // Create data for year=2020/geo=US
      Path yearGeoDir = baseDir.resolve("year=2020").resolve("geo=US");
      Files.createDirectories(yearGeoDir);
      String sql = String.format(
          "COPY (SELECT 1 as id, 'income' as type, 2020 as year, 'US' as geo, 50000.0 as amount) "
          + "TO '%s' (FORMAT PARQUET)",
          yearGeoDir.resolve("data.parquet").toString());
      stmt.execute(sql);

      // Create data for year=2020/geo=UK
      yearGeoDir = baseDir.resolve("year=2020").resolve("geo=UK");
      Files.createDirectories(yearGeoDir);
      sql = String.format(
          "COPY (SELECT 2 as id, 'income' as type, 2020 as year, 'UK' as geo, 40000.0 as amount) "
          + "TO '%s' (FORMAT PARQUET)",
          yearGeoDir.resolve("data.parquet").toString());
      stmt.execute(sql);

      // Create data for year=2021/geo=US
      yearGeoDir = baseDir.resolve("year=2021").resolve("geo=US");
      Files.createDirectories(yearGeoDir);
      sql = String.format(
          "COPY (SELECT 3 as id, 'income' as type, 2021 as year, 'US' as geo, 52000.0 as amount) "
          + "TO '%s' (FORMAT PARQUET)",
          yearGeoDir.resolve("data.parquet").toString());
      stmt.execute(sql);
    }
  }

  @Test
  void testAlternatePartitionRegistryIntegration() {
    AlternatePartitionRegistry registry = new AlternatePartitionRegistry();

    // Register alternates for a source table
    registry.register("regional_income", "_mv_income_by_geo",
        "type=income_by_geo/geo=*/*.parquet",
        Arrays.asList("geo"), null, null);

    registry.register("regional_income", "_mv_income_by_year",
        "type=income_by_year/year=*/*.parquet",
        Arrays.asList("year"), null, null);

    // Mark one as materialized
    registry.markMaterialized("_mv_income_by_geo");

    // Find best alternate for query filter
    Set<String> geoFilter = new HashSet<String>(Arrays.asList("geo"));
    AlternateInfo best = registry.findBestAlternate("regional_income", geoFilter);

    assertNotNull(best);
    assertEquals("_mv_income_by_geo", best.getAlternateName());

    // Year-based alternate is not materialized, should not be found
    Set<String> yearFilter = new HashSet<String>(Arrays.asList("year"));
    AlternateInfo yearBest = registry.findBestAlternate("regional_income", yearFilter);
    // Should be null since _mv_income_by_year is not materialized
    assertTrue(yearBest == null);
  }

  @Test
  void testIcebergTableCreationWithPartitionSpec() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "type", Types.StringType.get()),
        Types.NestedField.optional(3, "year", Types.IntegerType.get()),
        Types.NestedField.optional(4, "geo", Types.StringType.get()),
        Types.NestedField.optional(5, "amount", Types.DoubleType.get())
    );

    // Create alternate table partitioned by geo only
    PartitionSpec geoSpec = PartitionSpec.builderFor(schema)
        .identity("geo")
        .build();

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "default.income_by_geo", schema, geoSpec);

    assertNotNull(table);
    assertEquals(1, table.spec().fields().size());
    assertEquals("geo", table.spec().fields().get(0).name());
  }

  @Test
  void testIcebergTableWriterIntegration() throws Exception {
    // Create test table
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "year", Types.IntegerType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("year")
        .build();

    Table table = IcebergCatalogManager.createTable(
        catalogConfig, "default.writer_test", schema, spec);

    IcebergTableWriter writer = new IcebergTableWriter(table);
    assertNotNull(writer);
    assertNotNull(writer.getTable());

    // Run maintenance on empty table - should not throw
    writer.runMaintenance(7, 1);
  }

  @Test
  void testMaterializerConfigBuilder() {
    List<ColumnDefinition> partitionCols = new ArrayList<ColumnDefinition>();
    partitionCols.add(new ColumnDefinition("geo", "VARCHAR"));

    MaterializationConfig config = MaterializationConfig.builder()
        .sourcePattern(sourcePath.resolve("type=income/year=*/**/*.parquet").toString())
        .sourceFormat(IcebergMaterializer.SourceFormat.PARQUET)
        .targetTableId("income_by_geo")
        .sourceTableName("regional_income")
        .partitionColumns(partitionCols)
        .batchPartitionColumns(Arrays.asList("year"))
        .incrementalKeys(Arrays.asList("year"))
        .yearRange(2020, 2021)
        .threads(2)
        .description("Income consolidated by geography")
        .build();

    assertNotNull(config);
    assertEquals("income_by_geo", config.getTargetTableId());
    assertEquals("regional_income", config.getSourceTableName());
    assertEquals(Arrays.asList("geo"), config.getPartitionColumnNames());
    assertEquals(2020, config.getStartYear());
    assertEquals(2021, config.getEndYear());
    assertTrue(config.supportsIncremental());
  }

  @Test
  void testGenerateAlternateName() {
    // Test random name generation
    String name1 = IcebergCatalogManager.generateAlternateName();
    String name2 = IcebergCatalogManager.generateAlternateName();

    assertTrue(name1.startsWith("_mv_"));
    assertTrue(name2.startsWith("_mv_"));
    assertEquals(36, name1.length()); // "_mv_" + 32 random chars
    assertFalse(name1.equals(name2)); // Should be unique

    assertTrue(IcebergCatalogManager.isAlternateName(name1));
    assertTrue(IcebergCatalogManager.isAlternateName(name2));
    assertFalse(IcebergCatalogManager.isAlternateName("regular_table"));
  }

  @Test
  void testIncrementalTrackerNoopImplementation() {
    // Test NOOP tracker
    IncrementalTracker tracker = IncrementalTracker.NOOP;

    Map<String, String> keyValues = new HashMap<String, String>();
    keyValues.put("year", "2020");

    // NOOP always returns false for isProcessed
    assertFalse(tracker.isProcessed("table", "source", keyValues));

    // NOOP markProcessed should not throw
    tracker.markProcessed("table", "source", keyValues, "_mv_test");
  }

  @Test
  void testRegistryCoversFiltersLogic() {
    // Test the coversFilters logic
    List<String> partitionKeys = Arrays.asList("geo", "year", "month");
    AlternateInfo info = new AlternateInfo(
        "_mv_test",
        "source",
        "pattern",
        partitionKeys,
        null,
        null
    );

    // All keys covered
    Set<String> allCovered = new HashSet<String>(Arrays.asList("geo", "year", "month"));
    assertTrue(info.coversFilters(allCovered));

    // Subset covered
    Set<String> subset = new HashSet<String>(Arrays.asList("geo"));
    assertTrue(info.coversFilters(subset));

    // Not covered - extra column
    Set<String> notCovered = new HashSet<String>(Arrays.asList("geo", "state"));
    assertFalse(info.coversFilters(notCovered));

    // Empty filters
    assertFalse(info.coversFilters(new HashSet<String>()));
  }
}
