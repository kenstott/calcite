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

import org.apache.calcite.adapter.file.BaseFileTest;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the partition package:
 * PartitionDetector, PartitionedTableConfig, PartitionedParquetTable,
 * and Hive-style partitioned table queries through JDBC.
 */
@Tag("integration")
@SuppressWarnings("deprecation")
public class PartitionIntegrationTest extends BaseFileTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PartitionIntegrationTest.class);

  @TempDir
  Path tempDir;

  private void writeParquet(File file, Schema schema, List<GenericRecord> records)
      throws IOException {
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }
  }

  private void writeCsv(File dir, String name, String content) throws IOException {
    try (FileWriter w = new FileWriter(new File(dir, name))) {
      w.write(content);
    }
  }

  private Connection connect(String modelJson) throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + addEphemeralCacheToModel(modelJson));
    applyEngineDefaults(info);
    return DriverManager.getConnection("jdbc:calcite:", info);
  }

  // --- PartitionDetector unit tests ---

  @Test void testDetectHivePartitionScheme() {
    List<String> paths = Arrays.asList(
        "/data/year=2024/month=01/data.parquet",
        "/data/year=2024/month=02/data.parquet",
        "/data/year=2023/month=12/data.parquet"
    );

    PartitionDetector.PartitionInfo info =
        PartitionDetector.detectPartitionScheme(paths);
    assertNotNull(info, "Should detect Hive-style partitions");
    assertTrue(info.isHiveStyle());
    assertTrue(info.getPartitionColumns().contains("year"));
    assertTrue(info.getPartitionColumns().contains("month"));
  }

  @Test void testDetectNoPartitionScheme() {
    List<String> paths = Arrays.asList(
        "/data/file1.parquet",
        "/data/file2.parquet",
        "/data/file3.parquet"
    );

    PartitionDetector.PartitionInfo info =
        PartitionDetector.detectPartitionScheme(paths);
    assertNull(info, "Should not detect partitions for flat files");
  }

  @Test void testDetectNullInput() {
    assertNull(PartitionDetector.detectPartitionScheme(null));
    assertNull(PartitionDetector.detectPartitionScheme(
        new ArrayList<String>()));
  }

  @Test void testExtractHivePartitions() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions(
            "/data/region=us-east/year=2024/data.parquet");
    assertNotNull(info);
    assertTrue(info.isHiveStyle());
    assertEquals("us-east", info.getPartitionValues().get("region"));
    assertEquals("2024", info.getPartitionValues().get("year"));
    assertEquals(Arrays.asList("region", "year"), info.getPartitionColumns());
  }

  @Test void testExtractHivePartitionsSingleLevel() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions(
            "/data/category=electronics/file.parquet");
    assertNotNull(info);
    assertEquals(1, info.getPartitionColumns().size());
    assertEquals("category", info.getPartitionColumns().get(0));
    assertEquals("electronics", info.getPartitionValues().get("category"));
  }

  @Test void testExtractHivePartitionsNone() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions("/data/plain/file.parquet");
    assertNull(info, "Non-partitioned path should return null");
  }

  @Test void testExtractDirectoryPartitions() {
    List<String> columns = Arrays.asList("year", "month");
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractDirectoryPartitions(
            "/data/2024/01/file.parquet", columns);
    assertNotNull(info);
    assertFalse(info.isHiveStyle());
    assertEquals("2024", info.getPartitionValues().get("year"));
    assertEquals("01", info.getPartitionValues().get("month"));
  }

  @Test void testExtractDirectoryPartitionsNull() {
    assertNull(PartitionDetector.extractDirectoryPartitions(
        "/data/file.parquet", null));
    assertNull(PartitionDetector.extractDirectoryPartitions(
        "/data/file.parquet", new ArrayList<String>()));
  }

  @Test void testExtractCustomPartitions() {
    List<PartitionedTableConfig.ColumnMapping> mappings =
        new ArrayList<PartitionedTableConfig.ColumnMapping>();
    mappings.add(new PartitionedTableConfig.ColumnMapping("year", 1, "VARCHAR"));
    mappings.add(new PartitionedTableConfig.ColumnMapping("month", 2, "VARCHAR"));

    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractCustomPartitions(
            "/data/sales_2024_03.parquet",
            "sales_(\\d{4})_(\\d{2})", mappings);
    assertNotNull(info);
    assertEquals("2024", info.getPartitionValues().get("year"));
    assertEquals("03", info.getPartitionValues().get("month"));
  }

  @Test void testExtractCustomPartitionsNoMatch() {
    List<PartitionedTableConfig.ColumnMapping> mappings =
        new ArrayList<PartitionedTableConfig.ColumnMapping>();
    mappings.add(new PartitionedTableConfig.ColumnMapping("year", 1, "VARCHAR"));

    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractCustomPartitions(
            "/data/other_file.parquet",
            "sales_(\\d{4})", mappings);
    assertNull(info, "Non-matching path should return null");
  }

  @Test void testExtractCustomPartitionsNullArgs() {
    assertNull(PartitionDetector.extractCustomPartitions("/data/f.parquet", null, null));
  }

  @Test void testInconsistentHivePartitions() {
    List<String> paths = Arrays.asList(
        "/data/year=2024/month=01/data.parquet",
        "/data/region=us/data.parquet"
    );

    PartitionDetector.PartitionInfo info =
        PartitionDetector.detectPartitionScheme(paths);
    assertNull(info, "Inconsistent partition columns should return null");
  }

  // --- PartitionedTableConfig unit tests ---

  @Test void testPartitionedTableConfigBasic() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig(
            "hive", Arrays.asList("year", "month"), null, null, null);

    PartitionedTableConfig config =
        new PartitionedTableConfig("sales", "sales_*.parquet", "partitioned", pc);

    assertEquals("sales", config.getName());
    assertEquals("sales_*.parquet", config.getPattern());
    assertEquals("partitioned", config.getType());
    assertNotNull(config.getPartitions());
    assertEquals("hive", config.getPartitions().getStyle());
  }

  @Test void testPartitionedTableConfigWithComment() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig(
            "auto", null, null, null, null);

    PartitionedTableConfig config =
        new PartitionedTableConfig("data", "**/*.parquet", null, pc,
            "Test table comment", null);

    assertEquals("Test table comment", config.getComment());
    assertEquals("partitioned", config.getType()); // default type
  }

  @Test void testPartitionConfigDefaults() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig(
            null, null, null, null, null);
    assertEquals("auto", pc.getStyle()); // default style
    assertNull(pc.getColumns());
    assertNull(pc.getRegex());
    assertNull(pc.getColumnMappings());
  }

  @Test void testColumnMappingGetters() {
    PartitionedTableConfig.ColumnMapping mapping =
        new PartitionedTableConfig.ColumnMapping("year", 1, "VARCHAR");
    assertEquals("year", mapping.getName());
    assertEquals(1, mapping.getGroup());
    assertEquals("VARCHAR", mapping.getType());
  }

  // --- Hive-partitioned Parquet JDBC tests ---

  @Test void testHivePartitionedParquetQuery() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("Record")
        .fields()
        .requiredInt("id")
        .requiredString("name")
        .requiredDouble("amount")
        .endRecord();

    // Create Hive-style partitioned directory structure
    String[] years = {"2023", "2024"};
    String[] quarters = {"Q1", "Q2"};

    int id = 1;
    for (String year : years) {
      for (String quarter : quarters) {
        File partDir = new File(dir, "year=" + year + "/quarter=" + quarter);
        partDir.mkdirs();

        List<GenericRecord> records = new ArrayList<GenericRecord>();
        for (int i = 0; i < 5; i++) {
          GenericRecord r = new GenericData.Record(schema);
          r.put("id", id++);
          r.put("name", "Item_" + id);
          r.put("amount", 100.0 * id);
          records.add(r);
        }
        writeParquet(new File(partDir, "data.parquet"), schema, records);
      }
    }

    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"part\","
        + "\"schemas\":[{"
        + "\"name\":\"part\","
        + "\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{"
        + "\"directory\":\"" + dir.getAbsolutePath().replace("\\", "\\\\") + "\","
        + "\"ephemeralCache\":true,"
        + "\"partitionedTables\":[{"
        + "\"name\":\"events\","
        + "\"pattern\":\"**/data.parquet\","
        + "\"partitions\":{\"style\":\"hive\"}"
        + "}]"
        + "}}]}";

    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement()) {
      // Count total records
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM part.events")) {
        assertTrue(rs.next());
        assertEquals(20, rs.getInt("cnt"));
      }
    }
  }

  @Test void testPartitionedParquetWithDirectoryStyle() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("LogRecord")
        .fields()
        .requiredInt("log_id")
        .requiredString("message")
        .endRecord();

    // Create directory-partitioned structure
    String[] regions = {"us_east", "us_west", "eu_west"};
    int logId = 1;
    for (String region : regions) {
      File regionDir = new File(dir, region);
      regionDir.mkdirs();
      List<GenericRecord> records = new ArrayList<GenericRecord>();
      for (int i = 0; i < 3; i++) {
        GenericRecord r = new GenericData.Record(schema);
        r.put("log_id", logId++);
        r.put("message", "Log from " + region);
        records.add(r);
      }
      writeParquet(new File(regionDir, "logs.parquet"), schema, records);
    }

    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"logs\","
        + "\"schemas\":[{"
        + "\"name\":\"logs\","
        + "\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{"
        + "\"directory\":\"" + dir.getAbsolutePath().replace("\\", "\\\\") + "\","
        + "\"ephemeralCache\":true,"
        + "\"partitionedTables\":[{"
        + "\"name\":\"log_data\","
        + "\"pattern\":\"*/logs.parquet\","
        + "\"partitions\":{"
        + "\"style\":\"directory\","
        + "\"columns\":[\"region\"]"
        + "}"
        + "}]"
        + "}}]}";

    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM logs.log_data")) {
        assertTrue(rs.next());
        assertEquals(9, rs.getInt("cnt"));
      }
    }
  }

  @Test void testPartitionedTableWithRegex() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("SalesRecord")
        .fields()
        .requiredInt("sale_id")
        .requiredDouble("amount")
        .endRecord();

    // Create files with date-encoded names
    String[] dates = {"2024_01", "2024_02", "2024_03"};
    int saleId = 1;
    for (String date : dates) {
      List<GenericRecord> records = new ArrayList<GenericRecord>();
      for (int i = 0; i < 4; i++) {
        GenericRecord r = new GenericData.Record(schema);
        r.put("sale_id", saleId++);
        r.put("amount", 100.0 + saleId);
        records.add(r);
      }
      writeParquet(new File(dir, "sales_" + date + ".parquet"), schema, records);
    }

    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"rx\","
        + "\"schemas\":[{"
        + "\"name\":\"rx\","
        + "\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{"
        + "\"directory\":\"" + dir.getAbsolutePath().replace("\\", "\\\\") + "\","
        + "\"ephemeralCache\":true,"
        + "\"partitionedTables\":[{"
        + "\"name\":\"monthly_sales\","
        + "\"pattern\":\"sales_*.parquet\","
        + "\"partitions\":{"
        + "\"style\":\"regex\","
        + "\"regex\":\"sales_(\\\\d{4})_(\\\\d{2})\\\\.parquet\","
        + "\"columnMappings\":["
        + "{\"name\":\"year\",\"group\":1},"
        + "{\"name\":\"month\",\"group\":2}"
        + "]"
        + "}"
        + "}]"
        + "}}]}";

    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM rx.monthly_sales")) {
        assertTrue(rs.next());
        assertEquals(12, rs.getInt("cnt"));
      }
    }
  }

  // --- PartitionInfoRegistry tests ---

  @Test void testPartitionInfoRegistrySingleton() {
    PartitionInfoRegistry registry = PartitionInfoRegistry.getInstance();
    assertNotNull(registry, "Singleton instance should be available");

    // Lookup for non-existent should return null
    assertNull(registry.lookup("nonexistent_schema", "nonexistent_table"));
    assertNull(registry.getPartitionColumns("nonexistent_schema", "nonexistent_table"));
    assertFalse(registry.isPartitionColumn("nonexistent_schema", "nonexistent_table", "col"));
  }

  // --- Multiple partitioned tables in same schema ---

  @Test void testMultiplePartitionedTables() throws Exception {
    File dir = tempDir.toFile();
    Schema ordersSchema = SchemaBuilder.record("OrderRecord")
        .fields()
        .requiredInt("order_id")
        .requiredDouble("total")
        .endRecord();

    Schema shipmentsSchema = SchemaBuilder.record("ShipmentRecord")
        .fields()
        .requiredInt("shipment_id")
        .requiredString("status")
        .endRecord();

    // Orders partitioned by year
    for (String year : new String[]{"2023", "2024"}) {
      File yearDir = new File(dir, "orders/year=" + year);
      yearDir.mkdirs();
      List<GenericRecord> records = new ArrayList<GenericRecord>();
      GenericRecord r = new GenericData.Record(ordersSchema);
      r.put("order_id", Integer.parseInt(year));
      r.put("total", 999.99);
      records.add(r);
      writeParquet(new File(yearDir, "data.parquet"), ordersSchema, records);
    }

    // Shipments partitioned by status
    for (String status : new String[]{"pending", "shipped"}) {
      File statusDir = new File(dir, "shipments/status=" + status);
      statusDir.mkdirs();
      List<GenericRecord> records = new ArrayList<GenericRecord>();
      GenericRecord r = new GenericData.Record(shipmentsSchema);
      r.put("shipment_id", status.hashCode());
      r.put("status", status);
      records.add(r);
      writeParquet(new File(statusDir, "data.parquet"), shipmentsSchema, records);
    }

    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"multi\","
        + "\"schemas\":[{"
        + "\"name\":\"multi\","
        + "\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{"
        + "\"directory\":\"" + dir.getAbsolutePath().replace("\\", "\\\\") + "\","
        + "\"ephemeralCache\":true,"
        + "\"partitionedTables\":["
        + "{\"name\":\"orders\",\"pattern\":\"orders/**/data.parquet\","
        + "\"partitions\":{\"style\":\"hive\"}},"
        + "{\"name\":\"shipments\",\"pattern\":\"shipments/**/data.parquet\","
        + "\"partitions\":{\"style\":\"hive\"}}"
        + "]"
        + "}}]}";

    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM multi.orders")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("cnt"));
      }
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM multi.shipments")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("cnt"));
      }
    }
  }

  // --- PartitionedTableConfig with column definitions ---

  @Test void testPartitionConfigColumnDefinitions() {
    List<PartitionedTableConfig.ColumnDefinition> colDefs =
        new ArrayList<PartitionedTableConfig.ColumnDefinition>();
    colDefs.add(new PartitionedTableConfig.ColumnDefinition("year", "INTEGER"));
    colDefs.add(new PartitionedTableConfig.ColumnDefinition("month", "VARCHAR"));

    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig(
            "hive", Arrays.asList("year", "month"), colDefs, null, null);

    assertNotNull(pc.getColumnDefinitions());
    assertEquals(2, pc.getColumnDefinitions().size());
    assertEquals("year", pc.getColumnDefinitions().get(0).getName());
    assertEquals("INTEGER", pc.getColumnDefinitions().get(0).getType());
    assertEquals("month", pc.getColumnDefinitions().get(1).getName());
  }

  @Test void testPartitionedTableConfigWithAllFields() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig(
            "auto", null, null, null, null);

    List<PartitionedTableConfig.TableColumn> columns =
        new ArrayList<PartitionedTableConfig.TableColumn>();
    columns.add(new PartitionedTableConfig.TableColumn("id", "INTEGER", true, null));
    columns.add(new PartitionedTableConfig.TableColumn("name", "VARCHAR", true, "The name"));

    java.util.Map<String, String> colComments =
        new java.util.LinkedHashMap<String, String>();
    colComments.put("id", "Primary key");

    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "*.parquet", "partitioned",
            pc, "Table comment", colComments, columns);

    assertEquals("test_table", config.getName());
    assertEquals("Table comment", config.getComment());
    assertNotNull(config.getColumns());
    assertEquals(2, config.getColumns().size());
    assertEquals("id", config.getColumns().get(0).getName());
    assertEquals("INTEGER", config.getColumns().get(0).getType());
    assertNotNull(config.getColumnComments());
    assertEquals("Primary key", config.getColumnComments().get("id"));
  }

  // --- AlternatePartitionRegistry tests ---

  @Test void testAlternatePartitionRegistryBasics() {
    AlternatePartitionRegistry registry = new AlternatePartitionRegistry();

    registry.register("sales", "monthly_sales", "monthly_*.parquet",
        Arrays.asList("year", "month"), null, null);
    registry.register("sales", "quarterly_sales", "quarterly_*.parquet",
        Arrays.asList("year", "quarter"), null, null);

    List<AlternatePartitionRegistry.AlternateInfo> alternates = registry.getAlternates("sales");
    assertNotNull(alternates);
    assertEquals(2, alternates.size());
    assertEquals("monthly_sales", alternates.get(0).getAlternateName());
    assertEquals("quarterly_sales", alternates.get(1).getAlternateName());
  }

  @Test void testAlternatePartitionRegistryMissing() {
    AlternatePartitionRegistry registry = new AlternatePartitionRegistry();
    List<AlternatePartitionRegistry.AlternateInfo> alternates = registry.getAlternates("nonexistent");
    assertTrue(alternates.isEmpty());
  }

  @Test void testAlternatePartitionRegistryLookup() {
    AlternatePartitionRegistry registry = new AlternatePartitionRegistry();

    registry.register("orders", "alt_orders", "alt_*.parquet",
        Arrays.asList("region"), null, null);

    AlternatePartitionRegistry.AlternateInfo info = registry.getByName("alt_orders");
    assertNotNull(info);
    assertEquals("alt_orders", info.getAlternateName());
    assertEquals("orders", info.getSourceTableName());
    assertEquals(Arrays.asList("region"), info.getPartitionKeys());
    assertFalse(info.isMaterialized());
  }

  @Test void testAlternatePartitionRegistryMaterialize() {
    AlternatePartitionRegistry registry = new AlternatePartitionRegistry();

    registry.register("data", "mat_data", "*.parquet",
        Arrays.asList("year"), null, null);
    assertFalse(registry.isMaterialized("mat_data"));

    registry.markMaterialized("mat_data");
    assertTrue(registry.isMaterialized("mat_data"));
  }

  @Test void testAlternatePartitionRegistryFindBest() {
    AlternatePartitionRegistry registry = new AlternatePartitionRegistry();

    registry.register("sales", "by_region", "region_*.parquet",
        Arrays.asList("region"), null, null);
    registry.register("sales", "by_region_year", "region_year_*.parquet",
        Arrays.asList("region", "year"), null, null);

    // Mark both as materialized
    registry.markMaterialized("by_region");
    registry.markMaterialized("by_region_year");

    // For filter on region only, should pick by_region (fewer keys)
    java.util.Set<String> regionFilter = new java.util.LinkedHashSet<String>();
    regionFilter.add("region");
    AlternatePartitionRegistry.AlternateInfo best = registry.findBestAlternate("sales", regionFilter);
    assertNotNull(best);
    assertEquals("by_region", best.getAlternateName());
  }

  @Test void testAlternatePartitionRegistryClear() {
    AlternatePartitionRegistry registry = new AlternatePartitionRegistry();
    registry.register("t", "alt_t", "*.parquet",
        Arrays.asList("col"), null, null);
    assertEquals(1, registry.size());

    registry.clear();
    assertEquals(0, registry.size());
  }

  @Test void testAlternatePartitionRegistryUnregister() {
    AlternatePartitionRegistry registry = new AlternatePartitionRegistry();
    registry.register("t", "alt_t", "*.parquet",
        Arrays.asList("col"), null, null);
    assertEquals(1, registry.size());

    registry.unregister("alt_t");
    assertEquals(0, registry.size());
    assertNull(registry.getByName("alt_t"));
  }

  @Test void testAlternateInfoCoversFilters() {
    AlternatePartitionRegistry.AlternateInfo info =
        new AlternatePartitionRegistry.AlternateInfo(
            "alt", "source", "*.parquet",
            Arrays.asList("year", "month"), null, null);
    info.setMaterialized(true);

    java.util.Set<String> yearFilter = new java.util.LinkedHashSet<String>();
    yearFilter.add("year");
    assertTrue(info.coversFilters(yearFilter));

    java.util.Set<String> regionFilter = new java.util.LinkedHashSet<String>();
    regionFilter.add("region");
    assertFalse(info.coversFilters(regionFilter));

    assertFalse(info.coversFilters(null));
    assertFalse(info.coversFilters(new java.util.LinkedHashSet<String>()));
  }

  @Test void testAlternateInfoToString() {
    AlternatePartitionRegistry.AlternateInfo info =
        new AlternatePartitionRegistry.AlternateInfo(
            "alt", "source", "*.parquet",
            Arrays.asList("year"), null, null);
    String str = info.toString();
    assertTrue(str.contains("alt"));
    assertTrue(str.contains("source"));
  }

  // --- Deep Hive partition structure ---

  @Test void testDeepHivePartition() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("DeepRecord")
        .fields()
        .requiredInt("id")
        .requiredString("value")
        .endRecord();

    // 3-level deep partitioning
    File partDir = new File(dir, "country=US/state=CA/city=LA");
    partDir.mkdirs();
    List<GenericRecord> records = new ArrayList<GenericRecord>();
    GenericRecord r = new GenericData.Record(schema);
    r.put("id", 1);
    r.put("value", "test");
    records.add(r);
    writeParquet(new File(partDir, "data.parquet"), schema, records);

    File partDir2 = new File(dir, "country=US/state=NY/city=NYC");
    partDir2.mkdirs();
    GenericRecord r2 = new GenericData.Record(schema);
    r2.put("id", 2);
    r2.put("value", "test2");
    List<GenericRecord> records2 = new ArrayList<GenericRecord>();
    records2.add(r2);
    writeParquet(new File(partDir2, "data.parquet"), schema, records2);

    // Verify PartitionDetector can handle 3-level deep partitions
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions(
            new File(partDir, "data.parquet").getAbsolutePath());
    assertNotNull(info);
    assertEquals(3, info.getPartitionColumns().size());
    assertTrue(info.getPartitionColumns().contains("country"));
    assertTrue(info.getPartitionColumns().contains("state"));
    assertTrue(info.getPartitionColumns().contains("city"));
  }

  @Test void testPartitionedTableAutoDetect() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("AutoRecord")
        .fields()
        .requiredInt("id")
        .requiredString("data")
        .endRecord();

    // Create auto-detectable Hive partitions
    for (String cat : new String[]{"A", "B"}) {
      File catDir = new File(dir, "category=" + cat);
      catDir.mkdirs();
      List<GenericRecord> records = new ArrayList<GenericRecord>();
      GenericRecord r = new GenericData.Record(schema);
      r.put("id", cat.charAt(0));
      r.put("data", "Data_" + cat);
      records.add(r);
      writeParquet(new File(catDir, "part.parquet"), schema, records);
    }

    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"auto\","
        + "\"schemas\":[{"
        + "\"name\":\"auto\","
        + "\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{"
        + "\"directory\":\"" + dir.getAbsolutePath().replace("\\", "\\\\") + "\","
        + "\"ephemeralCache\":true,"
        + "\"partitionedTables\":[{"
        + "\"name\":\"auto_table\","
        + "\"pattern\":\"**/part.parquet\","
        + "\"partitions\":{\"style\":\"auto\"}"
        + "}]"
        + "}}]}";

    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM auto.auto_table")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("cnt"));
      }
    }
  }

  @Test void testColumnDefinitionGetters() {
    PartitionedTableConfig.ColumnDefinition def =
        new PartitionedTableConfig.ColumnDefinition("year", "INTEGER");
    assertEquals("year", def.getName());
    assertEquals("INTEGER", def.getType());
  }

  @Test void testTableColumnGetters() {
    PartitionedTableConfig.TableColumn col =
        new PartitionedTableConfig.TableColumn("amount", "DOUBLE", true, "The amount value");
    assertEquals("amount", col.getName());
    assertEquals("DOUBLE", col.getType());
    assertEquals("The amount value", col.getComment());
    assertTrue(col.isNullable());
  }

  @Test void testTableColumnWithoutComment() {
    PartitionedTableConfig.TableColumn col =
        new PartitionedTableConfig.TableColumn("id", "INTEGER", false, null);
    assertEquals("id", col.getName());
    assertEquals("INTEGER", col.getType());
    assertNull(col.getComment());
    assertFalse(col.isNullable());
  }

  @Test void testTableColumnWithExpression() {
    PartitionedTableConfig.TableColumn col =
        new PartitionedTableConfig.TableColumn("computed", "INTEGER", true, null,
            "col1 + col2");
    assertEquals("computed", col.getName());
    assertTrue(col.hasExpression());
    assertTrue(col.isComputed());
    assertEquals("col1 + col2", col.getExpression());
  }

  @Test void testTableColumnVectorType() {
    PartitionedTableConfig.TableColumn col =
        new PartitionedTableConfig.TableColumn("embedding", "array<float>", true, null);
    assertTrue(col.isVectorType());

    PartitionedTableConfig.TableColumn normalCol =
        new PartitionedTableConfig.TableColumn("name", "VARCHAR", true, null);
    assertFalse(normalCol.isVectorType());
  }
}
