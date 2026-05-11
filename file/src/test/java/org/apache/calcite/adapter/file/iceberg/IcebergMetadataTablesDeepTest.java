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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link IcebergMetadataTables}.
 * Tests creation of metadata tables, row types, and scanning.
 */
@Tag("unit")
public class IcebergMetadataTablesDeepTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IcebergMetadataTablesDeepTest.class);

  @TempDir
  Path tempDir;

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(
      org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

  private IcebergTable icebergTable;
  private Map<String, Object> config;

  @BeforeEach
  public void setUp() {
    IcebergCatalogManager.clearCache();
    config = new HashMap<>();
    config.put("catalog", "hadoop");
    config.put("warehouse", tempDir.toString());
    config.put("namespace", "default");

    // Create a real Iceberg table
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()));

    org.apache.iceberg.Table table = IcebergCatalogManager.createTable(
        config, "metadata_test", schema, PartitionSpec.unpartitioned());

    icebergTable = new IcebergTable(table, null);
  }

  @AfterEach
  public void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  @Test
  public void testCreateMetadataTablesReturnsAllTables() {
    Map<String, Table> metadataTables =
        IcebergMetadataTables.createMetadataTables(icebergTable);

    assertNotNull(metadataTables);
    assertEquals(5, metadataTables.size());
    assertTrue(metadataTables.containsKey("history"));
    assertTrue(metadataTables.containsKey("snapshots"));
    assertTrue(metadataTables.containsKey("files"));
    assertTrue(metadataTables.containsKey("manifests"));
    assertTrue(metadataTables.containsKey("partitions"));
  }

  @Test
  public void testHistoryTableRowType() {
    Map<String, Table> metadataTables =
        IcebergMetadataTables.createMetadataTables(icebergTable);

    Table historyTable = metadataTables.get("history");
    assertNotNull(historyTable);

    RelDataType rowType = historyTable.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(4, rowType.getFieldCount());
    assertEquals("made_current_at", rowType.getFieldList().get(0).getName());
    assertEquals("snapshot_id", rowType.getFieldList().get(1).getName());
    assertEquals("parent_id", rowType.getFieldList().get(2).getName());
    assertEquals("is_current_ancestor", rowType.getFieldList().get(3).getName());
  }

  @Test
  public void testSnapshotsTableRowType() {
    Map<String, Table> metadataTables =
        IcebergMetadataTables.createMetadataTables(icebergTable);

    Table snapshotsTable = metadataTables.get("snapshots");
    assertNotNull(snapshotsTable);

    RelDataType rowType = snapshotsTable.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(6, rowType.getFieldCount());
    assertEquals("committed_at", rowType.getFieldList().get(0).getName());
    assertEquals("snapshot_id", rowType.getFieldList().get(1).getName());
    assertEquals("parent_id", rowType.getFieldList().get(2).getName());
    assertEquals("operation", rowType.getFieldList().get(3).getName());
    assertEquals("manifest_list", rowType.getFieldList().get(4).getName());
    assertEquals("summary", rowType.getFieldList().get(5).getName());
  }

  @Test
  public void testFilesTableRowType() {
    Map<String, Table> metadataTables =
        IcebergMetadataTables.createMetadataTables(icebergTable);

    Table filesTable = metadataTables.get("files");
    assertNotNull(filesTable);

    RelDataType rowType = filesTable.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(6, rowType.getFieldCount());
    assertEquals("content", rowType.getFieldList().get(0).getName());
    assertEquals("file_path", rowType.getFieldList().get(1).getName());
    assertEquals("file_format", rowType.getFieldList().get(2).getName());
    assertEquals("spec_id", rowType.getFieldList().get(3).getName());
    assertEquals("record_count", rowType.getFieldList().get(4).getName());
    assertEquals("file_size_in_bytes", rowType.getFieldList().get(5).getName());
  }

  @Test
  public void testManifestsTableRowType() {
    Map<String, Table> metadataTables =
        IcebergMetadataTables.createMetadataTables(icebergTable);

    Table manifestsTable = metadataTables.get("manifests");
    assertNotNull(manifestsTable);

    RelDataType rowType = manifestsTable.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(7, rowType.getFieldCount());
    assertEquals("path", rowType.getFieldList().get(0).getName());
    assertEquals("length", rowType.getFieldList().get(1).getName());
    assertEquals("partition_spec_id", rowType.getFieldList().get(2).getName());
    assertEquals("added_snapshot_id", rowType.getFieldList().get(3).getName());
    assertEquals("added_data_files_count", rowType.getFieldList().get(4).getName());
    assertEquals("existing_data_files_count", rowType.getFieldList().get(5).getName());
    assertEquals("deleted_data_files_count", rowType.getFieldList().get(6).getName());
  }

  @Test
  public void testPartitionsTableRowType() {
    Map<String, Table> metadataTables =
        IcebergMetadataTables.createMetadataTables(icebergTable);

    Table partitionsTable = metadataTables.get("partitions");
    assertNotNull(partitionsTable);

    RelDataType rowType = partitionsTable.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(3, rowType.getFieldCount());
    assertEquals("partition", rowType.getFieldList().get(0).getName());
    assertEquals("record_count", rowType.getFieldList().get(1).getName());
    assertEquals("file_count", rowType.getFieldList().get(2).getName());
  }
}
