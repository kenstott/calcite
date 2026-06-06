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
package org.apache.calcite.adapter.file.table;

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
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the table package:
 * CsvScannableTable, JsonScannableTable, ParquetScannableTable,
 * CsvTranslatableTable, EnhancedCsvTranslatableTable, EnhancedJsonScannableTable,
 * GlobParquetTable, HLLAcceleratedTable, and PartitionedParquetTable.
 */
@Tag("integration")
@SuppressWarnings("deprecation")
public class TableIntegrationTest extends BaseFileTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TableIntegrationTest.class);

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

  private void writeParquet(File file, Schema schema, List<GenericRecord> records) throws IOException {
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

  // --- CsvScannableTable tests ---

  @Test void testCsvScannableTableScan() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "inventory.csv",
        "sku,product,quantity,warehouse\n"
        + "SKU001,Widget,100,WH-A\n"
        + "SKU002,Gadget,50,WH-B\n"
        + "SKU003,Doohickey,200,WH-A\n"
        + "SKU004,Thingamajig,75,WH-C\n"
        + "SKU005,Whatchamacallit,30,WH-B\n");

    try (Connection conn = connect(buildTestModel("inv", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT product, warehouse FROM inv.inventory ORDER BY product")) {
        assertTrue(rs.next());
        assertEquals("Doohickey", rs.getString("product"));
        assertEquals("WH-A", rs.getString("warehouse"));
        assertTrue(rs.next());
        assertEquals("Gadget", rs.getString("product"));
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
      }
    }
  }

  @Test void testCsvTableWithTypeInference() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "typed.csv",
        "id,name,score,active\n"
        + "1,Alice,95.5,true\n"
        + "2,Bob,87.3,false\n"
        + "3,Charlie,92.1,true\n");

    String model = buildTestModel("typed_schema", dir.getAbsolutePath(),
        "csvTypeInference",
        "{\"enabled\":true,\"samplingRate\":1.0,\"maxSampleRows\":100,"
        + "\"makeAllNullable\":true,\"inferDates\":true}");

    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT * FROM typed_schema.typed ORDER BY id")) {
        ResultSetMetaData meta = rs.getMetaData();
        assertTrue(meta.getColumnCount() >= 4);
        assertTrue(rs.next());
        assertNotNull(rs.getObject("id"));
        assertNotNull(rs.getObject("name"));
      }
    }
  }

  @Test void testCsvTableGroupByHaving() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "transactions.csv",
        "tx_id,merchant,amount,category\n"
        + "1,StoreA,50,food\n"
        + "2,StoreB,120,electronics\n"
        + "3,StoreA,30,food\n"
        + "4,StoreC,200,electronics\n"
        + "5,StoreA,45,food\n"
        + "6,StoreB,80,clothing\n");

    try (Connection conn = connect(buildTestModel("tx", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT merchant, COUNT(*) AS cnt FROM tx.transactions "
          + "GROUP BY merchant HAVING COUNT(*) >= 2 ORDER BY merchant")) {
        assertTrue(rs.next());
        assertEquals("StoreA", rs.getString("merchant"));
        assertEquals(3, rs.getInt("cnt"));
        assertTrue(rs.next());
        assertEquals("StoreB", rs.getString("merchant"));
        assertEquals(2, rs.getInt("cnt"));
        assertFalse(rs.next());
      }
    }
  }

  // --- JsonScannableTable tests ---

  @Test void testJsonScannableTableScan() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "books.json",
        "[{\"isbn\":\"978-0-13-468599-1\",\"title\":\"The Pragmatic Programmer\","
        + "\"author\":\"David Thomas\",\"year\":2019},"
        + "{\"isbn\":\"978-0-201-63361-0\",\"title\":\"Design Patterns\","
        + "\"author\":\"Gang of Four\",\"year\":1994},"
        + "{\"isbn\":\"978-0-596-00712-6\",\"title\":\"Head First Design Patterns\","
        + "\"author\":\"Eric Freeman\",\"year\":2004}]");

    try (Connection conn = connect(buildTestModel("lib", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT title, author FROM lib.books ORDER BY title")) {
        assertTrue(rs.next());
        assertEquals("Design Patterns", rs.getString("title"));
        assertTrue(rs.next());
        assertEquals("Head First Design Patterns", rs.getString("title"));
        assertTrue(rs.next());
        assertEquals("The Pragmatic Programmer", rs.getString("title"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testJsonTableWithNullFields() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "nullable.json",
        "[{\"id\":1,\"name\":\"Alice\",\"email\":\"alice@test.com\"},"
        + "{\"id\":2,\"name\":\"Bob\",\"email\":null},"
        + "{\"id\":3,\"name\":null,\"email\":\"charlie@test.com\"}]");

    try (Connection conn = connect(buildTestModel("ndb", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM ndb.nullable")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("cnt"));
      }
    }
  }

  @Test void testJsonTableFilter() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "sensors.json",
        "[{\"sensor_id\":\"S1\",\"temp\":22.5,\"humidity\":45},"
        + "{\"sensor_id\":\"S2\",\"temp\":35.0,\"humidity\":30},"
        + "{\"sensor_id\":\"S3\",\"temp\":18.2,\"humidity\":65},"
        + "{\"sensor_id\":\"S4\",\"temp\":28.7,\"humidity\":55}]");

    try (Connection conn = connect(buildTestModel("iot", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT sensor_id FROM iot.sensors ORDER BY sensor_id")) {
        int count = 0;
        while (rs.next()) {
          assertNotNull(rs.getString("sensor_id"));
          count++;
        }
        assertEquals(4, count);
      }
    }
  }

  @Test void testJsonTableJoin() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "authors.json",
        "[{\"author_id\":1,\"name\":\"Alice\"},"
        + "{\"author_id\":2,\"name\":\"Bob\"}]");
    writeJson(dir, "articles.json",
        "[{\"article_id\":101,\"author_id\":1,\"title\":\"Intro to SQL\"},"
        + "{\"article_id\":102,\"author_id\":2,\"title\":\"Advanced Joins\"},"
        + "{\"article_id\":103,\"author_id\":1,\"title\":\"Subqueries\"}]");

    try (Connection conn = connect(buildTestModel("blog", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT a.name, COUNT(*) AS article_count "
          + "FROM blog.articles art "
          + "JOIN blog.authors a ON art.author_id = a.author_id "
          + "GROUP BY a.name "
          + "ORDER BY a.name")) {
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals("Bob", rs.getString("name"));
        assertFalse(rs.next());
      }
    }
  }

  // --- ParquetTable tests ---

  @Test void testParquetTableScan() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .requiredString("name")
        .requiredInt("age")
        .requiredDouble("salary")
        .endRecord();

    List<GenericRecord> records = new ArrayList<GenericRecord>();
    for (int i = 0; i < 10; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("name", "Person_" + i);
      record.put("age", 25 + i);
      record.put("salary", 50000.0 + (i * 5000));
      records.add(record);
    }
    writeParquet(new File(dir, "people.parquet"), schema, records);

    try (Connection conn = connect(buildTestModel("pq", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM pq.people")) {
        assertTrue(rs.next());
        assertEquals(10, rs.getInt("cnt"));
      }
    }
  }

  @Test void testParquetTableFilter() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("SalesRecord")
        .fields()
        .requiredString("region")
        .requiredInt("amount")
        .requiredString("product")
        .endRecord();

    List<GenericRecord> records = new ArrayList<GenericRecord>();
    String[] regions = {"North", "South", "East", "West"};
    String[] products = {"Widget", "Gadget"};
    for (int i = 0; i < 20; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("region", regions[i % 4]);
      record.put("amount", 100 + (i * 50));
      record.put("product", products[i % 2]);
      records.add(record);
    }
    writeParquet(new File(dir, "sales.parquet"), schema, records);

    try (Connection conn = connect(buildTestModel("pqs", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT region, COUNT(*) AS cnt FROM pqs.sales GROUP BY region ORDER BY region")) {
        assertTrue(rs.next());
        assertEquals("East", rs.getString("region"));
        assertEquals(5, rs.getInt("cnt"));
        assertTrue(rs.next());
        assertEquals("North", rs.getString("region"));
        assertTrue(rs.next());
        assertEquals("South", rs.getString("region"));
        assertTrue(rs.next());
        assertEquals("West", rs.getString("region"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testParquetTableColumnTypes() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("TypesRecord")
        .fields()
        .requiredInt("int_col")
        .requiredLong("long_col")
        .requiredFloat("float_col")
        .requiredDouble("double_col")
        .requiredString("string_col")
        .requiredBoolean("bool_col")
        .endRecord();

    List<GenericRecord> records = new ArrayList<GenericRecord>();
    GenericRecord record = new GenericData.Record(schema);
    record.put("int_col", 42);
    record.put("long_col", 123456789L);
    record.put("float_col", 3.14f);
    record.put("double_col", 2.71828);
    record.put("string_col", "hello");
    record.put("bool_col", true);
    records.add(record);
    writeParquet(new File(dir, "types.parquet"), schema, records);

    try (Connection conn = connect(buildTestModel("pqt", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM pqt.types")) {
        ResultSetMetaData meta = rs.getMetaData();
        assertTrue(meta.getColumnCount() >= 6);
        assertTrue(rs.next());
        LOGGER.debug("Parquet column types:");
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          LOGGER.debug("  {}: {} ({})", meta.getColumnName(i),
              meta.getColumnTypeName(i), meta.getColumnType(i));
        }
      }
    }
  }

  @Test void testParquetTableWithNullableFields() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("NullableRecord")
        .fields()
        .requiredInt("id")
        .optionalString("name")
        .optionalInt("score")
        .endRecord();

    List<GenericRecord> records = new ArrayList<GenericRecord>();
    GenericRecord r1 = new GenericData.Record(schema);
    r1.put("id", 1);
    r1.put("name", "Alice");
    r1.put("score", 95);
    records.add(r1);

    GenericRecord r2 = new GenericData.Record(schema);
    r2.put("id", 2);
    r2.put("name", null);
    r2.put("score", 80);
    records.add(r2);

    GenericRecord r3 = new GenericData.Record(schema);
    r3.put("id", 3);
    r3.put("name", "Charlie");
    r3.put("score", null);
    records.add(r3);

    writeParquet(new File(dir, "nullable.parquet"), schema, records);

    try (Connection conn = connect(buildTestModel("pqn", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM pqn.nullable")) {
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("cnt"));
      }
    }
  }

  // --- Mixed format tests ---

  @Test void testMixedCsvJsonParquet() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "csv_data.csv", "id,name\n1,CsvAlice\n2,CsvBob\n");
    writeJson(dir, "json_data.json",
        "[{\"id\":3,\"name\":\"JsonCharlie\"},{\"id\":4,\"name\":\"JsonDiana\"}]");

    Schema schema = SchemaBuilder.record("PqRecord")
        .fields().requiredInt("id").requiredString("name").endRecord();
    List<GenericRecord> records = new ArrayList<GenericRecord>();
    GenericRecord r = new GenericData.Record(schema);
    r.put("id", 5);
    r.put("name", "ParquetEve");
    records.add(r);
    writeParquet(new File(dir, "pq_data.parquet"), schema, records);

    try (Connection conn = connect(buildTestModel("mixed", dir.getAbsolutePath()))) {
      DatabaseMetaData meta = conn.getMetaData();
      List<String> tableNames = new ArrayList<String>();
      try (ResultSet tables = meta.getTables(null, "mixed", "%", null)) {
        while (tables.next()) {
          tableNames.add(tables.getString("TABLE_NAME").toLowerCase());
        }
      }
      LOGGER.debug("Mixed format tables: {}", tableNames);
      assertTrue(tableNames.size() >= 3,
          "Should discover CSV, JSON, and Parquet tables, found: " + tableNames);
    }
  }

  // --- CsvTranslatableTable / EnhancedCsvTranslatableTable tests ---

  @Test void testCsvTableSelectProject() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "wide_table.csv",
        "a,b,c,d,e,f,g,h\n"
        + "1,2,3,4,5,6,7,8\n"
        + "10,20,30,40,50,60,70,80\n");

    try (Connection conn = connect(buildTestModel("proj", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT a, d, g FROM proj.wide_table ORDER BY a")) {
        assertTrue(rs.next());
        assertEquals("1", rs.getString("a"));
        assertEquals("4", rs.getString("d"));
        assertEquals("7", rs.getString("g"));
        assertTrue(rs.next());
        assertEquals("10", rs.getString("a"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testCsvTableCaseInsensitiveQuery() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "case_test.csv",
        "Name,Age,City\n"
        + "Alice,30,NYC\n"
        + "Bob,25,LA\n");

    try (Connection conn = connect(buildTestModel("ci", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT name, age, city FROM ci.case_test ORDER BY name")) {
        assertTrue(rs.next());
        assertNotNull(rs.getString(1));
      }
    }
  }

  // --- EnhancedJsonScannableTable / flatten tests ---

  @Test void testFlattenedJsonTable() throws Exception {
    File dir = tempDir.toFile();
    writeJson(dir, "nested_obj.json",
        "[{\"id\":1,\"details\":{\"color\":\"red\",\"size\":\"large\"}},"
        + "{\"id\":2,\"details\":{\"color\":\"blue\",\"size\":\"small\"}}]");

    // flatten requires explicit table definition with flatten:true per table
    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"flat_json\","
        + "\"schemas\":[{"
        + "\"name\":\"flat_json\","
        + "\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{"
        + "\"directory\":\"" + dir.getAbsolutePath().replace("\\", "\\\\") + "\","
        + "\"ephemeralCache\":true,"
        + "\"tables\":[{"
        + "\"name\":\"nested_obj\","
        + "\"url\":\"" + new File(dir, "nested_obj.json").getAbsolutePath().replace("\\", "\\\\") + "\","
        + "\"flatten\":true"
        + "}]"
        + "}}]}";

    try (Connection conn = connect(model);
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM flat_json.nested_obj")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("cnt"));
      }
    }
  }

  // --- Large parquet test ---

  @Test void testLargeParquetTable() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("LargeRecord")
        .fields()
        .requiredInt("id")
        .requiredString("category")
        .requiredDouble("value")
        .endRecord();

    List<GenericRecord> records = new ArrayList<GenericRecord>();
    for (int i = 0; i < 1000; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("id", i);
      record.put("category", "cat_" + (i % 10));
      record.put("value", Math.random() * 1000);
      records.add(record);
    }
    writeParquet(new File(dir, "big_data.parquet"), schema, records);

    try (Connection conn = connect(buildTestModel("lg", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM lg.big_data")) {
        assertTrue(rs.next());
        assertEquals(1000, rs.getInt("cnt"));
      }
      try (ResultSet rs = stmt.executeQuery(
          "SELECT category, COUNT(*) AS cnt FROM lg.big_data "
          + "GROUP BY category ORDER BY category")) {
        int groupCount = 0;
        while (rs.next()) {
          assertEquals(100, rs.getInt("cnt"));
          groupCount++;
        }
        assertEquals(10, groupCount);
      }
    }
  }

  // --- Parquet join tests ---

  @Test void testParquetCsvJoin() throws Exception {
    File dir = tempDir.toFile();
    Schema schema = SchemaBuilder.record("DeptRecord")
        .fields()
        .requiredInt("dept_id")
        .requiredString("dept_name")
        .endRecord();

    List<GenericRecord> records = new ArrayList<GenericRecord>();
    GenericRecord r1 = new GenericData.Record(schema);
    r1.put("dept_id", 1);
    r1.put("dept_name", "Engineering");
    records.add(r1);
    GenericRecord r2 = new GenericData.Record(schema);
    r2.put("dept_id", 2);
    r2.put("dept_name", "Marketing");
    records.add(r2);
    writeParquet(new File(dir, "depts.parquet"), schema, records);

    writeCsv(dir, "emps.csv",
        "emp_id,emp_name,dept_id\n"
        + "101,Alice,1\n"
        + "102,Bob,2\n"
        + "103,Charlie,1\n");

    try (Connection conn = connect(buildTestModel("cross_fmt", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT e.emp_name, d.dept_name "
          + "FROM cross_fmt.emps e "
          + "JOIN cross_fmt.depts d ON CAST(e.dept_id AS INTEGER) = d.dept_id "
          + "ORDER BY e.emp_name")) {
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString("emp_name"));
        assertEquals("Engineering", rs.getString("dept_name"));
        assertTrue(rs.next());
        assertEquals("Bob", rs.getString("emp_name"));
        assertTrue(rs.next());
        assertEquals("Charlie", rs.getString("emp_name"));
        assertFalse(rs.next());
      }
    }
  }

  @Test void testTableMetadata() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "meta_test.csv", "col1,col2,col3\n1,a,x\n2,b,y\n");

    try (Connection conn = connect(buildTestModel("mt", dir.getAbsolutePath()))) {
      DatabaseMetaData meta = conn.getMetaData();
      try (ResultSet cols = meta.getColumns(null, "mt", "meta_test", "%")) {
        int colCount = 0;
        while (cols.next()) {
          String colName = cols.getString("COLUMN_NAME");
          String typeName = cols.getString("TYPE_NAME");
          LOGGER.debug("Column metadata: {} ({})", colName, typeName);
          colCount++;
        }
        assertTrue(colCount >= 3, "Should have at least 3 columns in metadata");
      }
    }
  }

  @Test void testCsvWithWhitespace() throws Exception {
    File dir = tempDir.toFile();
    writeCsv(dir, "spaced.csv",
        "id, name , value \n"
        + "1, Alice , 100 \n"
        + "2, Bob , 200 \n");

    try (Connection conn = connect(buildTestModel("sp", dir.getAbsolutePath()));
         Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM sp.spaced")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("cnt"));
      }
    }
  }
}
