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

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sources;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link IcebergMetadataTables}.
 */
@Tag("unit")
public class IcebergMetadataTablesTest {

  @TempDir
  Path tempDir;

  private IcebergTable emptyIcebergTable;
  private IcebergTable populatedIcebergTable;
  private RelDataTypeFactory typeFactory;

  @BeforeEach
  void setUp() throws Exception {
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    String warehousePath = tempDir.resolve("warehouse").toString();
    Configuration conf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    // Create empty table
    Schema emptySchema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()));

    org.apache.iceberg.Table emptyTable =
        catalog.createTable(TableIdentifier.of("empty_table"), emptySchema);
    emptyIcebergTable = new IcebergTable(emptyTable, Sources.of(tempDir.toFile()));

    // Create table with data
    Schema dataSchema =
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "value", Types.StringType.get()));

    org.apache.iceberg.Table dataTable =
        catalog.createTable(TableIdentifier.of("data_table"), dataSchema,
            PartitionSpec.unpartitioned());

    // Add data to table
    OutputFile outputFile =
        dataTable.io().newOutputFile(dataTable.location() + "/data/file-" + UUID.randomUUID() + ".parquet");

    DataWriter<Record> writer = Parquet.writeData(outputFile)
        .schema(dataSchema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .build();

    GenericRecord record1 = GenericRecord.create(dataSchema);
    record1.setField("id", 1);
    record1.setField("value", "hello");
    writer.write(record1);

    GenericRecord record2 = GenericRecord.create(dataSchema);
    record2.setField("id", 2);
    record2.setField("value", "world");
    writer.write(record2);

    writer.close();
    dataTable.newAppend().appendFile(writer.toDataFile()).commit();

    populatedIcebergTable = new IcebergTable(dataTable, Sources.of(tempDir.toFile()));
  }

  @Test public void testCreateMetadataTablesReturnsAllExpectedTables() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);

    assertNotNull(tables);
    assertEquals(5, tables.size());
    assertTrue(tables.containsKey("history"));
    assertTrue(tables.containsKey("snapshots"));
    assertTrue(tables.containsKey("files"));
    assertTrue(tables.containsKey("manifests"));
    assertTrue(tables.containsKey("partitions"));
  }

  @Test public void testHistoryTableRowType() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);
    Table historyTable = tables.get("history");

    RelDataType rowType = historyTable.getRowType(typeFactory);
    assertNotNull(rowType);

    List<String> fieldNames = rowType.getFieldNames();
    assertEquals(4, fieldNames.size());
    assertTrue(fieldNames.contains("made_current_at"));
    assertTrue(fieldNames.contains("snapshot_id"));
    assertTrue(fieldNames.contains("parent_id"));
    assertTrue(fieldNames.contains("is_current_ancestor"));
  }

  @Test public void testSnapshotsTableRowType() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);
    Table snapshotsTable = tables.get("snapshots");

    RelDataType rowType = snapshotsTable.getRowType(typeFactory);
    assertNotNull(rowType);

    List<String> fieldNames = rowType.getFieldNames();
    assertEquals(6, fieldNames.size());
    assertTrue(fieldNames.contains("committed_at"));
    assertTrue(fieldNames.contains("snapshot_id"));
    assertTrue(fieldNames.contains("parent_id"));
    assertTrue(fieldNames.contains("operation"));
    assertTrue(fieldNames.contains("manifest_list"));
    assertTrue(fieldNames.contains("summary"));
  }

  @Test public void testFilesTableRowType() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);
    Table filesTable = tables.get("files");

    RelDataType rowType = filesTable.getRowType(typeFactory);
    assertNotNull(rowType);

    List<String> fieldNames = rowType.getFieldNames();
    assertEquals(6, fieldNames.size());
    assertTrue(fieldNames.contains("content"));
    assertTrue(fieldNames.contains("file_path"));
    assertTrue(fieldNames.contains("file_format"));
    assertTrue(fieldNames.contains("spec_id"));
    assertTrue(fieldNames.contains("record_count"));
    assertTrue(fieldNames.contains("file_size_in_bytes"));
  }

  @Test public void testManifestsTableRowType() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);
    Table manifestsTable = tables.get("manifests");

    RelDataType rowType = manifestsTable.getRowType(typeFactory);
    assertNotNull(rowType);

    List<String> fieldNames = rowType.getFieldNames();
    assertEquals(7, fieldNames.size());
    assertTrue(fieldNames.contains("path"));
    assertTrue(fieldNames.contains("length"));
    assertTrue(fieldNames.contains("partition_spec_id"));
    assertTrue(fieldNames.contains("added_snapshot_id"));
    assertTrue(fieldNames.contains("added_data_files_count"));
    assertTrue(fieldNames.contains("existing_data_files_count"));
    assertTrue(fieldNames.contains("deleted_data_files_count"));
  }

  @Test public void testPartitionsTableRowType() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);
    Table partitionsTable = tables.get("partitions");

    RelDataType rowType = partitionsTable.getRowType(typeFactory);
    assertNotNull(rowType);

    List<String> fieldNames = rowType.getFieldNames();
    assertEquals(3, fieldNames.size());
    assertTrue(fieldNames.contains("partition"));
    assertTrue(fieldNames.contains("record_count"));
    assertTrue(fieldNames.contains("file_count"));
  }

  @Test public void testAllTablesAreScannableTable() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);
    for (Map.Entry<String, Table> entry : tables.entrySet()) {
      assertTrue(entry.getValue() instanceof ScannableTable,
          entry.getKey() + " should implement ScannableTable");
    }
  }

  @Test public void testHistoryTableScanEmptyTable() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);
    ScannableTable historyTable = (ScannableTable) tables.get("history");

    Enumerable<Object[]> result = historyTable.scan(null);
    assertNotNull(result);

    Enumerator<Object[]> enumerator = result.enumerator();
    // Empty table has no snapshots
    assertFalse(enumerator.moveNext());
    enumerator.close();
  }

  @Test public void testHistoryTableScanPopulatedTable() {
    Map<String, Table> tables =
        IcebergMetadataTables.createMetadataTables(populatedIcebergTable);
    ScannableTable historyTable = (ScannableTable) tables.get("history");

    Enumerable<Object[]> result = historyTable.scan(null);
    Enumerator<Object[]> enumerator = result.enumerator();

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);
    assertEquals(4, row.length);
    // snapshot_id should be non-null
    assertNotNull(row[1]);
    enumerator.close();
  }

  @Test public void testSnapshotsTableScanPopulatedTable() {
    Map<String, Table> tables =
        IcebergMetadataTables.createMetadataTables(populatedIcebergTable);
    ScannableTable snapshotsTable = (ScannableTable) tables.get("snapshots");

    Enumerable<Object[]> result = snapshotsTable.scan(null);
    Enumerator<Object[]> enumerator = result.enumerator();

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);
    assertEquals(6, row.length);
    // committed_at
    assertNotNull(row[0]);
    // snapshot_id
    assertNotNull(row[1]);
    // operation
    assertNotNull(row[3]);
    enumerator.close();
  }

  @Test public void testFilesTableScanPopulatedTable() {
    Map<String, Table> tables =
        IcebergMetadataTables.createMetadataTables(populatedIcebergTable);
    ScannableTable filesTable = (ScannableTable) tables.get("files");

    Enumerable<Object[]> result = filesTable.scan(null);
    Enumerator<Object[]> enumerator = result.enumerator();

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);
    assertEquals(6, row.length);
    // file_path should be a string
    assertNotNull(row[1]);
    assertTrue(row[1].toString().endsWith(".parquet"));
    // record_count should be 2
    assertEquals(2L, row[4]);
    enumerator.close();
  }

  @Test public void testManifestsTableScanPopulatedTable() {
    Map<String, Table> tables =
        IcebergMetadataTables.createMetadataTables(populatedIcebergTable);
    ScannableTable manifestsTable = (ScannableTable) tables.get("manifests");

    Enumerable<Object[]> result = manifestsTable.scan(null);
    Enumerator<Object[]> enumerator = result.enumerator();

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);
    assertEquals(7, row.length);
    // path should be non-null
    assertNotNull(row[0]);
    // length should be > 0
    assertTrue((Long) row[1] > 0);
    enumerator.close();
  }

  @Test public void testPartitionsTableScanPopulatedTable() {
    Map<String, Table> tables =
        IcebergMetadataTables.createMetadataTables(populatedIcebergTable);
    ScannableTable partitionsTable = (ScannableTable) tables.get("partitions");

    Enumerable<Object[]> result = partitionsTable.scan(null);
    Enumerator<Object[]> enumerator = result.enumerator();

    // Unpartitioned table should have one partition (empty partition key)
    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);
    assertEquals(3, row.length);
    // record_count should be 2
    assertEquals(2L, row[1]);
    // file_count should be 1
    assertEquals(1, row[2]);
    enumerator.close();
  }

  @Test public void testFilesTableScanEmptyTable() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);
    ScannableTable filesTable = (ScannableTable) tables.get("files");

    Enumerable<Object[]> result = filesTable.scan(null);
    Enumerator<Object[]> enumerator = result.enumerator();
    // Empty table has no files
    assertFalse(enumerator.moveNext());
    enumerator.close();
  }

  @Test public void testManifestsTableScanEmptyTable() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);
    ScannableTable manifestsTable = (ScannableTable) tables.get("manifests");

    Enumerable<Object[]> result = manifestsTable.scan(null);
    Enumerator<Object[]> enumerator = result.enumerator();
    // Empty table has no manifests
    assertFalse(enumerator.moveNext());
    enumerator.close();
  }

  @Test public void testPartitionsTableScanEmptyTable() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);
    ScannableTable partitionsTable = (ScannableTable) tables.get("partitions");

    Enumerable<Object[]> result = partitionsTable.scan(null);
    Enumerator<Object[]> enumerator = result.enumerator();
    // Empty table has no partitions
    assertFalse(enumerator.moveNext());
    enumerator.close();
  }

  @Test public void testHistoryTableFieldTypes() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);
    Table historyTable = tables.get("history");
    RelDataType rowType = historyTable.getRowType(typeFactory);

    assertEquals(SqlTypeName.TIMESTAMP, rowType.getFieldList().get(0).getType().getSqlTypeName());
    assertEquals(SqlTypeName.BIGINT, rowType.getFieldList().get(1).getType().getSqlTypeName());
    assertEquals(SqlTypeName.BIGINT, rowType.getFieldList().get(2).getType().getSqlTypeName());
    assertEquals(SqlTypeName.BOOLEAN, rowType.getFieldList().get(3).getType().getSqlTypeName());
  }

  @Test public void testSnapshotsTableFieldTypes() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);
    Table snapshotsTable = tables.get("snapshots");
    RelDataType rowType = snapshotsTable.getRowType(typeFactory);

    assertEquals(SqlTypeName.TIMESTAMP, rowType.getFieldList().get(0).getType().getSqlTypeName());
    assertEquals(SqlTypeName.BIGINT, rowType.getFieldList().get(1).getType().getSqlTypeName());
    assertEquals(SqlTypeName.BIGINT, rowType.getFieldList().get(2).getType().getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, rowType.getFieldList().get(3).getType().getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, rowType.getFieldList().get(4).getType().getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, rowType.getFieldList().get(5).getType().getSqlTypeName());
  }
}
