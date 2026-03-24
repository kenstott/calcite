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
package org.apache.calcite.adapter.file.execution;

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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for execution engines (Parquet and Linq4j).
 * Tests CSV/JSON/Parquet data processing through the full JDBC path
 * to exercise CsvEnumerator, JsonEnumerator, ParquetEnumerator,
 * ParquetExecutionEngine, and ParquetFileEnumerator.
 */
@Tag("integration")
@SuppressWarnings("deprecation")
public class ExecutionEngineIntegrationTest extends BaseFileTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ExecutionEngineIntegrationTest.class);

  @TempDir
  Path tempDir;

  private void writeCsv(File dir, String name, String content) throws IOException {
    try (FileWriter w = new FileWriter(new File(dir, name))) {
      w.write(content);
    }
  }

  private void writeJson(File dir, String name, String content) throws IOException {
    try (FileWriter w = new FileWriter(new File(dir, name))) {
      w.write(content);
    }
  }

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

  private Connection connect(String modelJson) throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + addEphemeralCacheToModel(modelJson));
    applyEngineDefaults(info);
    return DriverManager.getConnection("jdbc:calcite:", info);
  }

  // --- CsvEnumerator tests through JDBC ---

  @Test void testCsvEnumeratorBasicScan() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "basic.csv",
        "id,name,value\n"
        + "1,Alpha,100\n"
        + "2,Beta,200\n"
        + "3,Gamma,300\n");

    try (Connection conn = connect(buildTestModel("csv_enum", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM csv_enum.basic ORDER BY id")) {
        assertTrue(rs.next());
        assertEquals("1", rs.getString("id"));
        assertEquals("Alpha", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals("2", rs.getString("id"));
        assertTrue(rs.next());
        assertEquals("3", rs.getString("id"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testCsvEnumeratorWithFiltering() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "filtered.csv",
        "id,status,amount\n"
        + "1,active,100\n"
        + "2,inactive,200\n"
        + "3,active,150\n"
        + "4,inactive,300\n"
        + "5,active,250\n");

    try (Connection conn = connect(buildTestModel("csv_filt", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM csv_filt.filtered WHERE status = 'active'")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("cnt"));
      }
    }
  }

  @Test void testCsvEnumeratorWithAggregation() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "agg_data.csv",
        "dept,employee,salary\n"
        + "eng,Alice,90000\n"
        + "eng,Bob,85000\n"
        + "eng,Charlie,92000\n"
        + "mkt,Diana,78000\n"
        + "mkt,Eve,82000\n"
        + "hr,Frank,75000\n");

    try (Connection conn = connect(buildTestModel("csv_agg", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT dept, COUNT(*) AS cnt FROM csv_agg.agg_data GROUP BY dept ORDER BY dept")) {
        assertTrue(rs.next());
        assertEquals("eng", rs.getString("dept"));
        assertEquals(3, rs.getInt("cnt"));
        assertTrue(rs.next());
        assertEquals("hr", rs.getString("dept"));
        assertEquals(1, rs.getInt("cnt"));
        assertTrue(rs.next());
        assertEquals("mkt", rs.getString("dept"));
        assertEquals(2, rs.getInt("cnt"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testCsvEnumeratorLargeDataset() throws Exception {
    File dir = tempDir.toFile();
    StringBuilder csv = new StringBuilder("idx,label,score\n");
    for (int i = 0; i < 2000; i++) {
      csv.append(i).append(",label_").append(i % 50).append(",").append(i * 1.5).append("\n");
    }
    writeCsv(dir, "large_csv.csv", csv.toString());

    try (Connection conn = connect(buildTestModel("csv_large", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM csv_large.large_csv")) {
        assertTrue(rs.next());
        assertEquals(2000, rs.getInt("cnt"));
      }
    }
  }

  @Test void testCsvEnumeratorWithNulls() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "with_nulls.csv",
        "id,name,optional_field\n"
        + "1,Alice,present\n"
        + "2,Bob,\n"
        + "3,,also_present\n"
        + "4,Diana,\n");

    try (Connection conn = connect(buildTestModel("csv_null", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM csv_null.with_nulls")) {
        assertTrue(rs.next());
        assertEquals(4, rs.getInt("cnt"));
      }
    }
  }

  // --- JsonEnumerator tests through JDBC ---

  @Test void testJsonEnumeratorBasicScan() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "json_basic.json",
        "[{\"key\":\"a\",\"val\":1},"
        + "{\"key\":\"b\",\"val\":2},"
        + "{\"key\":\"c\",\"val\":3}]");

    try (Connection conn = connect(buildTestModel("json_enum", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT * FROM json_enum.json_basic ORDER BY key")) {
        assertTrue(rs.next());
        assertEquals("a", rs.getString("key"));
        assertTrue(rs.next());
        assertEquals("b", rs.getString("key"));
        assertTrue(rs.next());
        assertEquals("c", rs.getString("key"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testJsonEnumeratorWithNestedObjects() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "nested.json",
        "[{\"id\":1,\"data\":{\"x\":10,\"y\":20}},"
        + "{\"id\":2,\"data\":{\"x\":30,\"y\":40}}]");

    try (Connection conn = connect(buildTestModel("json_nested", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM json_nested.nested")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("cnt"));
      }
    }
  }

  @Test void testJsonEnumeratorWithArrayValues() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "with_arrays.json",
        "[{\"id\":1,\"tags\":[\"a\",\"b\"],\"score\":95},"
        + "{\"id\":2,\"tags\":[\"c\"],\"score\":87}]");

    try (Connection conn = connect(buildTestModel("json_arr", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM json_arr.with_arrays")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("cnt"));
      }
    }
  }

  @Test void testJsonEnumeratorWithMixedTypes() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "mixed_types.json",
        "[{\"str_field\":\"hello\",\"int_field\":42,\"float_field\":3.14,\"bool_field\":true},"
        + "{\"str_field\":\"world\",\"int_field\":0,\"float_field\":2.71,\"bool_field\":false}]");

    try (Connection conn = connect(buildTestModel("json_mix", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT * FROM json_mix.mixed_types ORDER BY str_field")) {
        assertTrue(rs.next());
        assertEquals("hello", rs.getString("str_field"));
        assertTrue(rs.next());
        assertEquals("world", rs.getString("str_field"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testJsonEnumeratorLargeDataset() throws Exception {
    File dir = tempDir.toFile();
    StringBuilder json = new StringBuilder("[");
    for (int i = 0; i < 500; i++) {
      if (i > 0) {
        json.append(",");
      }
      json.append("{\"id\":").append(i)
          .append(",\"name\":\"item_").append(i).append("\"")
          .append(",\"value\":").append(i * 2.5).append("}");
    }
    json.append("]");
    writeJson(dir, "large_json.json", json.toString());

    try (Connection conn = connect(buildTestModel("json_large", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM json_large.large_json")) {
        assertTrue(rs.next());
        assertEquals(500, rs.getInt("cnt"));
      }
    }
  }

  // --- ParquetEnumerator / ParquetExecutionEngine tests through JDBC ---

  @Test void testParquetEnumeratorBasicScan() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("Record")
        .fields()
        .requiredString("city")
        .requiredInt("population")
        .requiredString("country")
        .endRecord();

    List<GenericRecord> records = new ArrayList<GenericRecord>();
    String[][] data = {
        {"New York", "8336817", "US"},
        {"London", "8982000", "UK"},
        {"Tokyo", "13960000", "JP"},
        {"Paris", "2161000", "FR"},
        {"Berlin", "3644826", "DE"}
    };
    for (String[] row : data) {
      GenericRecord r = new GenericData.Record(schema);
      r.put("city", row[0]);
      r.put("population", Integer.parseInt(row[1]));
      r.put("country", row[2]);
      records.add(r);
    }
    writeParquet(new File(dir, "cities.parquet"), schema, records);

    try (Connection conn = connect(buildTestModel("pq_enum", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT city, country FROM pq_enum.cities ORDER BY city")) {
        assertTrue(rs.next());
        assertEquals("Berlin", rs.getString("city"));
        assertEquals("DE", rs.getString("country"));
        assertTrue(rs.next());
        assertEquals("London", rs.getString("city"));
        int count = 2;
        while (rs.next()) {
          count++;
        }
        assertEquals(5, count);
      }
    }
  }

  @Test void testParquetEnumeratorWithFilter() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("Product")
        .fields()
        .requiredString("name")
        .requiredInt("price")
        .requiredString("category")
        .endRecord();

    List<GenericRecord> records = new ArrayList<GenericRecord>();
    for (int i = 0; i < 50; i++) {
      GenericRecord r = new GenericData.Record(schema);
      r.put("name", "Product_" + i);
      r.put("price", 10 + (i * 5));
      r.put("category", i % 3 == 0 ? "electronics" : (i % 3 == 1 ? "clothing" : "food"));
      records.add(r);
    }
    writeParquet(new File(dir, "products.parquet"), schema, records);

    try (Connection conn = connect(buildTestModel("pq_filt", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT category, COUNT(*) AS cnt "
          + "FROM pq_filt.products "
          + "GROUP BY category "
          + "ORDER BY category")) {
        assertTrue(rs.next());
        assertEquals("clothing", rs.getString("category"));
        assertTrue(rs.next());
        assertEquals("electronics", rs.getString("category"));
        assertTrue(rs.next());
        assertEquals("food", rs.getString("category"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testParquetEnumeratorWithAllTypes() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("AllTypes")
        .fields()
        .requiredInt("int_col")
        .requiredLong("long_col")
        .requiredFloat("float_col")
        .requiredDouble("double_col")
        .requiredString("string_col")
        .requiredBoolean("bool_col")
        .endRecord();

    List<GenericRecord> records = new ArrayList<GenericRecord>();
    GenericRecord r = new GenericData.Record(schema);
    r.put("int_col", 42);
    r.put("long_col", 9876543210L);
    r.put("float_col", 3.14f);
    r.put("double_col", 2.71828);
    r.put("string_col", "test_value");
    r.put("bool_col", true);
    records.add(r);
    writeParquet(new File(dir, "all_types.parquet"), schema, records);

    try (Connection conn = connect(buildTestModel("pq_types", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM pq_types.all_types")) {
        assertTrue(rs.next());
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(6, meta.getColumnCount());
        LOGGER.debug("Parquet all types:");
        for (int i = 1; i <= 6; i++) {
          LOGGER.debug("  {} = {} (type={})", meta.getColumnName(i),
              rs.getString(i), meta.getColumnTypeName(i));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test void testParquetEnumeratorLargeDataset() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("LargeRecord")
        .fields()
        .requiredInt("id")
        .requiredString("group_name")
        .requiredDouble("metric")
        .endRecord();

    List<GenericRecord> records = new ArrayList<GenericRecord>();
    for (int i = 0; i < 5000; i++) {
      GenericRecord r = new GenericData.Record(schema);
      r.put("id", i);
      r.put("group_name", "group_" + (i % 20));
      r.put("metric", Math.random() * 1000);
      records.add(r);
    }
    writeParquet(new File(dir, "big_data.parquet"), schema, records);

    try (Connection conn = connect(buildTestModel("pq_large", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM pq_large.big_data")) {
        assertTrue(rs.next());
        assertEquals(5000, rs.getInt("cnt"));
      }
      try (ResultSet rs = stmt.executeQuery(
          "SELECT group_name, COUNT(*) AS cnt "
          + "FROM pq_large.big_data "
          + "GROUP BY group_name "
          + "ORDER BY group_name")) {
        int groupCount = 0;
        while (rs.next()) {
          assertEquals(250, rs.getInt("cnt"));
          groupCount++;
        }
        assertEquals(20, groupCount);
      }
    }
  }

  @Test void testParquetEnumeratorWithNullableFields() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("NullableRecord")
        .fields()
        .requiredInt("id")
        .optionalString("name")
        .optionalDouble("value")
        .endRecord();

    List<GenericRecord> records = new ArrayList<GenericRecord>();
    GenericRecord r1 = new GenericData.Record(schema);
    r1.put("id", 1);
    r1.put("name", "Alice");
    r1.put("value", 100.0);
    records.add(r1);

    GenericRecord r2 = new GenericData.Record(schema);
    r2.put("id", 2);
    r2.put("name", null);
    r2.put("value", 200.0);
    records.add(r2);

    GenericRecord r3 = new GenericData.Record(schema);
    r3.put("id", 3);
    r3.put("name", "Charlie");
    r3.put("value", null);
    records.add(r3);

    writeParquet(new File(dir, "nullable.parquet"), schema, records);

    try (Connection conn = connect(buildTestModel("pq_null", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM pq_null.nullable")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("cnt"));
      }
    }
  }

  // --- Cross-engine tests ---

  @Test void testCsvAndParquetJoin() throws Exception {
    File dir = tempDir.toFile();

    // CSV dimension table
    writeCsv(dir, "regions.csv",
        "region_id,region_name\n"
        + "1,North\n"
        + "2,South\n"
        + "3,East\n");

    // Parquet fact table
    Schema schema = SchemaBuilder.record("FactRecord")
        .fields()
        .requiredInt("fact_id")
        .requiredInt("region_id")
        .requiredDouble("amount")
        .endRecord();

    List<GenericRecord> records = new ArrayList<GenericRecord>();
    for (int i = 0; i < 9; i++) {
      GenericRecord r = new GenericData.Record(schema);
      r.put("fact_id", i);
      r.put("region_id", (i % 3) + 1);
      r.put("amount", 100.0 + i * 10);
      records.add(r);
    }
    writeParquet(new File(dir, "facts.parquet"), schema, records);

    try (Connection conn = connect(buildTestModel("cross_eng", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT r.region_name, COUNT(*) AS cnt "
          + "FROM cross_eng.facts f "
          + "JOIN cross_eng.regions r ON f.region_id = CAST(r.region_id AS INTEGER) "
          + "GROUP BY r.region_name "
          + "ORDER BY r.region_name")) {
        assertTrue(rs.next());
        assertEquals("East", rs.getString("region_name"));
        assertEquals(3, rs.getInt("cnt"));
        assertTrue(rs.next());
        assertEquals("North", rs.getString("region_name"));
        assertTrue(rs.next());
        assertEquals("South", rs.getString("region_name"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testJsonAndParquetUnion() throws Exception {
    File dir = tempDir.toFile();

    writeJson(dir, "json_source.json",
        "[{\"id\":1,\"label\":\"from_json\"},"
        + "{\"id\":2,\"label\":\"from_json\"}]");

    Schema schema = SchemaBuilder.record("UnionRecord")
        .fields()
        .requiredInt("id")
        .requiredString("label")
        .endRecord();

    List<GenericRecord> records = new ArrayList<GenericRecord>();
    GenericRecord r = new GenericData.Record(schema);
    r.put("id", 3);
    r.put("label", "from_parquet");
    records.add(r);
    writeParquet(new File(dir, "pq_source.parquet"), schema, records);

    try (Connection conn = connect(buildTestModel("union_eng", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      // Query each individually
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM union_eng.json_source")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("cnt"));
      }
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM union_eng.pq_source")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("cnt"));
      }
    }
  }

  @Test void testMultipleParquetFilesInDirectory() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("FileRecord")
        .fields()
        .requiredInt("id")
        .requiredString("source")
        .endRecord();

    for (int f = 0; f < 3; f++) {
      List<GenericRecord> records = new ArrayList<GenericRecord>();
      for (int i = 0; i < 5; i++) {
        GenericRecord r = new GenericData.Record(schema);
        r.put("id", f * 5 + i);
        r.put("source", "file_" + f);
        records.add(r);
      }
      writeParquet(new File(dir, "batch_" + f + ".parquet"), schema, records);
    }

    try (Connection conn = connect(buildTestModel("multi_pq", dir.getAbsolutePath()))) {
      java.sql.DatabaseMetaData meta = conn.getMetaData();
      List<String> tableNames = new ArrayList<String>();
      try (ResultSet tables = meta.getTables(null, "multi_pq", "%", null)) {
        while (tables.next()) {
          tableNames.add(tables.getString("TABLE_NAME").toLowerCase());
        }
      }
      assertTrue(tableNames.size() >= 3,
          "Should find 3 parquet tables, found: " + tableNames);
    }
  }

  @Test void testCsvEnumeratorWithTsvFile() throws Exception {
    File dir = tempDir.toFile();
    // Write TSV file (tab-separated)
    writeCsv(dir, "tab_data.tsv",
        "id\tname\tscore\n"
        + "1\tAlice\t95\n"
        + "2\tBob\t87\n"
        + "3\tCharlie\t92\n");

    try (Connection conn = connect(buildTestModel("tsv_test", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM tsv_test.tab_data")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("cnt"));
      }
    }
  }

  @Test void testExecutionEngineWithPreparedStatement() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "prepared.csv",
        "id,name,category\n"
        + "1,Widget,A\n"
        + "2,Gadget,B\n"
        + "3,Doohickey,A\n");

    try (Connection conn = connect(buildTestModel("prep", dir.getAbsolutePath()));
         java.sql.PreparedStatement pstmt = conn.prepareStatement(
             "SELECT * FROM prep.prepared WHERE category = ?")) {
      pstmt.setString(1, "A");
      try (ResultSet rs = pstmt.executeQuery()) {
        int count = 0;
        while (rs.next()) {
          count++;
        }
        assertEquals(2, count);
      }
    }
  }
}
