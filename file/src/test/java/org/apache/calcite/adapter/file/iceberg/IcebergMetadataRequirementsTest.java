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

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-164 — exact-assertion golden for {@code IcebergMetadataTables.createMetadataTables} (recode of
 * the weak {@code IcebergMetadataTablesTest#testCreateMetadataTablesReturnsAllExpectedTables}, which
 * used containsKey/&gt;0/notNull). Pins the EXACT 5-table name set, each table's EXACT ordered column
 * list, that all are ScannableTable, that an empty table's metadata scans yield zero rows, and the
 * populated unpartitioned facts (partitions row record_count=2/file_count=1; files row record_count=2
 * with a .parquet path).
 *
 * <p>Hermetic and unit-grade: a local HadoopCatalog under {@code @TempDir} (no network/S3) — kept in
 * its own {@code @Tag("unit")} class rather than the {@code @Tag("integration")} IcebergRequirementsTest
 * so the unit suite runs it (an integration class-tag would exclude it).
 */
@Tag("unit")
public class IcebergMetadataRequirementsTest {

  @TempDir Path tempDir;

  private IcebergTable emptyIcebergTable;
  private IcebergTable populatedIcebergTable;
  private RelDataTypeFactory typeFactory;

  @BeforeEach void setUp() throws Exception {
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    Configuration conf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(conf, tempDir.resolve("warehouse").toString());

    Schema emptySchema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()));
    org.apache.iceberg.Table emptyTable =
        catalog.createTable(TableIdentifier.of("empty_table"), emptySchema);
    emptyIcebergTable = new IcebergTable(emptyTable, Sources.of(tempDir.toFile()));

    Schema dataSchema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "value", Types.StringType.get()));
    org.apache.iceberg.Table dataTable = catalog.createTable(
        TableIdentifier.of("data_table"), dataSchema, PartitionSpec.unpartitioned());

    OutputFile outputFile = dataTable.io()
        .newOutputFile(dataTable.location() + "/data/file-" + UUID.randomUUID() + ".parquet");
    DataWriter<Record> writer = Parquet.writeData(outputFile)
        .schema(dataSchema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .build();
    GenericRecord r1 = GenericRecord.create(dataSchema);
    r1.setField("id", 1);
    r1.setField("value", "hello");
    writer.write(r1);
    GenericRecord r2 = GenericRecord.create(dataSchema);
    r2.setField("id", 2);
    r2.setField("value", "world");
    writer.write(r2);
    writer.close();
    dataTable.newAppend().appendFile(writer.toDataFile()).commit();
    populatedIcebergTable = new IcebergTable(dataTable, Sources.of(tempDir.toFile()));
  }

  @Test @Tag("FILE-164") void metadataTablesHaveExactNamesAndColumnLists() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);

    assertEquals(new HashSet<String>(Arrays.asList(
            "history", "snapshots", "files", "manifests", "partitions")),
        tables.keySet(), "exactly these five metadata tables");

    for (Map.Entry<String, Table> e : tables.entrySet()) {
      assertTrue(e.getValue() instanceof ScannableTable, e.getKey() + " is ScannableTable");
    }

    assertEquals(Arrays.asList("made_current_at", "snapshot_id", "parent_id", "is_current_ancestor"),
        fieldNames(tables, "history"));
    assertEquals(Arrays.asList(
            "committed_at", "snapshot_id", "parent_id", "operation", "manifest_list", "summary"),
        fieldNames(tables, "snapshots"));
    assertEquals(Arrays.asList(
            "content", "file_path", "file_format", "spec_id", "record_count", "file_size_in_bytes"),
        fieldNames(tables, "files"));
    assertEquals(Arrays.asList(
            "path", "length", "partition_spec_id", "added_snapshot_id",
            "added_data_files_count", "existing_data_files_count", "deleted_data_files_count"),
        fieldNames(tables, "manifests"));
    assertEquals(Arrays.asList("partition", "record_count", "file_count"),
        fieldNames(tables, "partitions"));
  }

  @Test @Tag("FILE-164") void emptyTableMetadataScansYieldZeroRows() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(emptyIcebergTable);
    for (String name : new String[] {"history", "files", "manifests", "partitions"}) {
      Enumerator<Object[]> en = ((ScannableTable) tables.get(name)).scan(null).enumerator();
      assertFalse(en.moveNext(), name + " of an empty table scans to zero rows");
      en.close();
    }
  }

  @Test @Tag("FILE-164") void populatedUnpartitionedTableYieldsExactCounts() {
    Map<String, Table> tables = IcebergMetadataTables.createMetadataTables(populatedIcebergTable);

    // partitions: exactly one row for the single (empty) partition; record_count=2, file_count=1.
    Enumerator<Object[]> parts = ((ScannableTable) tables.get("partitions")).scan(null).enumerator();
    assertTrue(parts.moveNext(), "one partition row");
    Object[] prow = parts.current();
    assertEquals(2L, prow[1], "partitions.record_count");
    assertEquals(1, prow[2], "partitions.file_count");
    assertFalse(parts.moveNext(), "exactly one partition row");
    parts.close();

    // files: exactly one data file with record_count=2 and a .parquet path.
    Enumerator<Object[]> files = ((ScannableTable) tables.get("files")).scan(null).enumerator();
    assertTrue(files.moveNext(), "one file row");
    Object[] frow = files.current();
    assertEquals(2L, frow[4], "files.record_count");
    assertTrue(frow[1].toString().endsWith(".parquet"), "files.file_path is a .parquet path");
    assertFalse(files.moveNext(), "exactly one file row");
    files.close();
  }

  private java.util.List<String> fieldNames(Map<String, Table> tables, String name) {
    RelDataType rowType = tables.get(name).getRowType(typeFactory);
    return rowType.getFieldNames();
  }
}
