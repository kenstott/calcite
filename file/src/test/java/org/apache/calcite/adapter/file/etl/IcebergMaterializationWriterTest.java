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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link IcebergMaterializationWriter}.
 *
 * <p>Tests verify the Iceberg materialization write path including:
 * table creation, partition spec application, schema validation,
 * and commit lifecycle.
 *
 * <p>Uses a local Hadoop catalog for test isolation. Requires
 * Iceberg and DuckDB dependencies on the classpath.
 */
@Tag("integration")
public class IcebergMaterializationWriterTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IcebergMaterializationWriterTest.class);

  @TempDir
  File tempDir;

  private StorageProvider storageProvider;
  private IcebergMaterializationWriter writer;

  @BeforeEach
  public void setUp() {
    storageProvider = new LocalFileStorageProvider();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (writer != null) {
      writer.close();
    }
  }

  @Test public void testIcebergInitializeCreatesTable() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse");
    warehouseDir.mkdirs();
    writer =
        new IcebergMaterializationWriter(storageProvider, warehouseDir.getAbsolutePath(), null);

    MaterializeConfig config =
        buildIcebergConfig(
            warehouseDir, "test_init_table", Arrays.asList(
            createColumnConfig("id", "INTEGER"),
            createColumnConfig("name", "VARCHAR"),
            createColumnConfig("amount", "DOUBLE")),
        Collections.<String>emptyList());

    writer.initialize(config);

    assertTrue(writer.getTotalRowsWritten() == 0,
        "No rows should be written after initialization");
    assertNotNull(writer.getTableLocation(),
        "Table location should be set after initialization");
    LOGGER.debug("Iceberg table location: {}", writer.getTableLocation());
  }

  @Test public void testIcebergWriteBatchAndCommit() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_write");
    warehouseDir.mkdirs();
    writer =
        new IcebergMaterializationWriter(storageProvider, warehouseDir.getAbsolutePath(), null);

    MaterializeConfig config =
        buildIcebergConfig(
            warehouseDir, "test_write_table", Arrays.asList(
            createColumnConfig("id", "INTEGER"),
            createColumnConfig("value", "VARCHAR")),
        Collections.<String>emptyList());

    writer.initialize(config);

    // Write a batch
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 5; i++) {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", i);
      row.put("value", "item_" + i);
      rows.add(row);
    }

    long written =
        writer.writeBatch(rows.iterator(), Collections.<String, String>emptyMap());
    writer.commit();

    assertTrue(written > 0, "Should report rows written");
    LOGGER.debug("Iceberg write: {} rows written, {} total files",
        writer.getTotalRowsWritten(), writer.getTotalFilesWritten());
  }

  @Test public void testIcebergPartitionSpecApplied() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_partition");
    warehouseDir.mkdirs();
    writer =
        new IcebergMaterializationWriter(storageProvider, warehouseDir.getAbsolutePath(), null);

    List<String> partitionColumns = Arrays.asList("region");
    MaterializeConfig config =
        buildIcebergConfig(
            warehouseDir, "test_partition_table", Arrays.asList(
            createColumnConfig("id", "INTEGER"),
            createColumnConfig("value", "VARCHAR"),
            createColumnConfig("region", "VARCHAR")),
        partitionColumns);

    writer.initialize(config);

    // Write rows with different partition values
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row1 = new HashMap<String, Object>();
    row1.put("id", 1);
    row1.put("value", "east_item");
    row1.put("region", "EAST");
    rows.add(row1);

    Map<String, Object> row2 = new HashMap<String, Object>();
    row2.put("id", 2);
    row2.put("value", "west_item");
    row2.put("region", "WEST");
    rows.add(row2);

    Map<String, String> partitionVars = new HashMap<String, String>();
    partitionVars.put("region", "ALL");
    writer.writeBatch(rows.iterator(), partitionVars);
    writer.commit();

    assertEquals(2, writer.getTotalRowsWritten(),
        "Should have written 2 rows across partitions");
  }

  @Test public void testIcebergInitializeRequiresIcebergFormat() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_format_check");
    warehouseDir.mkdirs();
    writer =
        new IcebergMaterializationWriter(storageProvider, warehouseDir.getAbsolutePath(), null);

    // Build config with PARQUET format instead of ICEBERG
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .name("wrong_format_table")
        .output(MaterializeOutputConfig.builder().build())
        .build();

    assertThrows(IllegalArgumentException.class,
        () -> writer.initialize(config),
        "Should throw IllegalArgumentException for non-ICEBERG format");
  }

  @Test public void testIcebergInitializeWithNullConfigThrows() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_null");
    warehouseDir.mkdirs();
    writer =
        new IcebergMaterializationWriter(storageProvider, warehouseDir.getAbsolutePath(), null);

    assertThrows(IllegalArgumentException.class,
        () -> writer.initialize(null),
        "Should throw IllegalArgumentException for null config");
  }

  @Test public void testIcebergInitializeWithDisabledConfigThrows() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_disabled");
    warehouseDir.mkdirs();
    writer =
        new IcebergMaterializationWriter(storageProvider, warehouseDir.getAbsolutePath(), null);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(false)
        .format(MaterializeConfig.Format.ICEBERG)
        .name("disabled_table")
        .output(MaterializeOutputConfig.builder().build())
        .build();

    assertThrows(IOException.class,
        () -> writer.initialize(config),
        "Should throw IOException when materialization is disabled");
  }

  @Test public void testIcebergGetFormatReturnsIceberg() {
    File warehouseDir = new File(tempDir, "warehouse_format");
    warehouseDir.mkdirs();
    writer =
        new IcebergMaterializationWriter(storageProvider, warehouseDir.getAbsolutePath(), null);

    assertEquals(MaterializeConfig.Format.ICEBERG, writer.getFormat(),
        "Format should be ICEBERG");
  }

  @Test public void testIcebergWriteWithoutInitializeThrows() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_uninit");
    warehouseDir.mkdirs();
    writer =
        new IcebergMaterializationWriter(storageProvider, warehouseDir.getAbsolutePath(), null);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    rows.add(row);

    assertThrows(IllegalStateException.class,
        () -> writer.writeBatch(rows.iterator(),
            Collections.<String, String>emptyMap()),
        "Should throw IllegalStateException when writing without initialization");
  }

  @Test public void testIcebergWriteRequiresTargetTableId() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_no_table");
    warehouseDir.mkdirs();
    writer =
        new IcebergMaterializationWriter(storageProvider, warehouseDir.getAbsolutePath(), null);

    // Config without targetTableId or name
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
            .warehousePath(warehouseDir.getAbsolutePath())
            .build())
        .build();

    assertThrows(IllegalArgumentException.class,
        () -> writer.initialize(config),
        "Should throw when no target table ID is provided");
  }

  @Test public void testIcebergSchemaEvolutionWithNewColumns() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_evolution");
    warehouseDir.mkdirs();

    // First write with 2 columns
    writer =
        new IcebergMaterializationWriter(storageProvider, warehouseDir.getAbsolutePath(), null);

    MaterializeConfig config1 =
        buildIcebergConfig(
            warehouseDir, "evolution_table", Arrays.asList(
            createColumnConfig("id", "INTEGER"),
            createColumnConfig("name", "VARCHAR")),
        Collections.<String>emptyList());

    writer.initialize(config1);

    List<Map<String, Object>> rows1 = new ArrayList<Map<String, Object>>();
    Map<String, Object> row1 = new HashMap<String, Object>();
    row1.put("id", 1);
    row1.put("name", "first");
    rows1.add(row1);
    writer.writeBatch(rows1.iterator(), Collections.<String, String>emptyMap());
    writer.commit();
    writer.close();

    // Second write with 3 columns (new column "amount")
    writer =
        new IcebergMaterializationWriter(storageProvider, warehouseDir.getAbsolutePath(), null);

    MaterializeConfig config2 =
        buildIcebergConfig(
            warehouseDir, "evolution_table", Arrays.asList(
            createColumnConfig("id", "INTEGER"),
            createColumnConfig("name", "VARCHAR"),
            createColumnConfig("amount", "DOUBLE")),
        Collections.<String>emptyList());

    writer.initialize(config2);

    List<Map<String, Object>> rows2 = new ArrayList<Map<String, Object>>();
    Map<String, Object> row2 = new HashMap<String, Object>();
    row2.put("id", 2);
    row2.put("name", "second");
    row2.put("amount", 100.0);
    rows2.add(row2);
    writer.writeBatch(rows2.iterator(), Collections.<String, String>emptyMap());
    writer.commit();

    // Table should have processed both writes successfully
    long totalRows = writer.getTotalRowsWritten();
    assertTrue(totalRows > 0,
        "Should have written rows in the evolved schema write");
  }

  @Test public void testIcebergEmptyBatchReturnsZero() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_empty");
    warehouseDir.mkdirs();
    writer =
        new IcebergMaterializationWriter(storageProvider, warehouseDir.getAbsolutePath(), null);

    MaterializeConfig config =
        buildIcebergConfig(
            warehouseDir, "empty_table", Arrays.asList(
            createColumnConfig("id", "INTEGER")),
        Collections.<String>emptyList());

    writer.initialize(config);

    List<Map<String, Object>> emptyRows = Collections.emptyList();
    long written =
        writer.writeBatch(emptyRows.iterator(), Collections.<String, String>emptyMap());

    assertEquals(0, written, "Empty batch should return 0");
  }

  @Test public void testReplaceColumnExcludesRawSourceAndUsesExpression() throws Exception {
    File warehouseDir = new File(tempDir, "warehouse_replace");
    warehouseDir.mkdirs();
    writer =
        new IcebergMaterializationWriter(storageProvider, warehouseDir.getAbsolutePath(), null);

    // county_code is raw integer in source; replace: true wraps it with printf
    ColumnConfig idCol = ColumnConfig.builder().name("id").type("INTEGER").build();
    ColumnConfig countyCol = ColumnConfig.builder()
        .name("county_code")
        .type("VARCHAR")
        .expression("printf('%05d', county_code)")
        .replace(true)
        .build();

    MaterializeConfig config =
        buildIcebergConfig(warehouseDir, "replace_test", Arrays.asList(idCol, countyCol),
        Collections.<String>emptyList());

    writer.initialize(config);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("county_code", 73);

    writer.writeBatch(Collections.singletonList(row).iterator(),
        Collections.<String, String>emptyMap());
    writer.commit();

    String tableLocation = writer.getTableLocation();
    assertNotNull(tableLocation, "Table location must be set after commit");

    // Read back via DuckDB and verify replace semantics
    String parquetGlob = tableLocation.replaceAll("^file:", "") + "/data/**/*.parquet";
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {

        // Column county_code should be the padded string, not the raw integer
        String valueQuery = "SELECT county_code FROM read_parquet('"
            + parquetGlob.replace("'", "''") + "', union_by_name=true)";
        try (ResultSet rs = stmt.executeQuery(valueQuery)) {
          assertTrue(rs.next(), "Should have at least one row");
          assertEquals("00073", rs.getString("county_code"),
              "replace: true should produce padded string via printf expression");
        }

        // Schema should contain county_code exactly once (not raw int + derived varchar)
        String describeQuery = "DESCRIBE SELECT * FROM read_parquet('"
            + parquetGlob.replace("'", "''") + "', union_by_name=true)";
        Set<String> columnNames = new HashSet<String>();
        try (ResultSet rs = stmt.executeQuery(describeQuery)) {
          while (rs.next()) {
            columnNames.add(rs.getString("column_name").toLowerCase());
          }
        }
        assertEquals(2, columnNames.size(), "Should have exactly id and county_code");
        assertTrue(columnNames.contains("county_code"),
            "county_code column must be present");
        assertFalse(columnNames.contains("raw_county_code"),
            "No raw duplicate column should exist");
      }
    }
  }

  // --- Helper methods ---

  private ColumnConfig createColumnConfig(String name, String type) {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", name);
    map.put("type", type);
    return ColumnConfig.fromMap(map);
  }

  private MaterializeConfig buildIcebergConfig(File warehouseDir, String tableName,
      List<ColumnConfig> columns, List<String> partitionColumns) {

    MaterializeConfig.Builder builder = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .name(tableName)
        .targetTableId(tableName)
        .output(MaterializeOutputConfig.builder().build())
        .columns(columns)
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
            .warehousePath(warehouseDir.getAbsolutePath())
            .namespace("default")
            .build());

    if (!partitionColumns.isEmpty()) {
      builder.partition(MaterializePartitionConfig.builder()
          .columns(partitionColumns)
          .build());
    }

    return builder.build();
  }
}
