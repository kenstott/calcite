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
package org.apache.calcite.adapter.file;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.util.Sources;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * FILE-032 — hive-style partition discovery + pruning, exact-row golden (recode of the weak
 * {@code table/PartitionedTableTest#testHiveStylePartitionedTable}, which asserted only COUNT(*) and
 * regex-matched the partition values). Path-layout {@code year=YYYY/month=MM} yields VARCHAR
 * partition columns; a WHERE on a partition column prunes to exactly the matching files, and the
 * projected rows (partition + data columns) are asserted exactly. Hermetic: a small parquet fixture
 * is written in-test under a {@code @TempDir} (no live service).
 *
 * <p>Fixture: 6 single-row parquet files, sales/year=YYYY/month=MM/data.parquet for years 2022-2024
 * x months 01-02; per file order_id=year*100+month, product="Product-"+year+month, amount=100.0*month.
 */
@Tag("unit")
public class PartitionPruningRequirementsTest {

  private static final String AVRO_SCHEMA_STRING = "{"
      + "\"type\": \"record\","
      + "\"name\": \"Sales\","
      + "\"fields\": ["
      + "  {\"name\": \"order_id\", \"type\": \"int\"},"
      + "  {\"name\": \"product\", \"type\": \"string\"},"
      + "  {\"name\": \"amount\", \"type\": \"double\"}"
      + "]"
      + "}";

  @TempDir File tempDir;
  private Schema avroSchema;

  @BeforeEach void setUp() throws Exception {
    String engineStr = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    assumeFalse(engineStr != null
            && ("LINQ4J".equalsIgnoreCase(engineStr) || "ARROW".equalsIgnoreCase(engineStr)),
        "partitioned tables require the parquet engine");
    avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);
    Sources.clearFileCache();
    createHivePartitionedData();
  }

  @Test @Tag("FILE-032") void prunedFilterYieldsExactRowSet() throws Exception {
    // WHERE on the path-derived partition column prunes to only the year=2024 files.
    try (Connection c = openWithSalesSchema();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT \"order_id\", \"product\", \"amount\", \"year\", \"month\" "
                 + "FROM \"test\".\"sales\" WHERE \"year\" = '2024' ORDER BY \"month\"")) {
      assertTrue(rs.next(), "first 2024 row");
      assertEquals(202401, rs.getInt("order_id"));
      assertEquals("Product-20241", rs.getString("product"));
      assertEquals(100.0, rs.getDouble("amount"), 0.0);
      assertEquals("2024", rs.getString("year"));
      assertEquals("01", rs.getString("month"));

      assertTrue(rs.next(), "second 2024 row");
      assertEquals(202402, rs.getInt("order_id"));
      assertEquals("Product-20242", rs.getString("product"));
      assertEquals(200.0, rs.getDouble("amount"), 0.0);
      assertEquals("2024", rs.getString("year"));
      assertEquals("02", rs.getString("month"));

      assertFalse(rs.next(), "pruning must leave exactly the two 2024 rows");
    }
  }

  @Test @Tag("FILE-032") void unprunedFullScanReturnsExactRowSet() throws Exception {
    try (Connection c = openWithSalesSchema();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT \"order_id\", \"product\", \"amount\", \"year\", \"month\" "
                 + "FROM \"test\".\"sales\" ORDER BY \"year\", \"month\"")) {
      int[] orderIds = {202201, 202202, 202301, 202302, 202401, 202402};
      String[] products = {
          "Product-20221", "Product-20222", "Product-20231",
          "Product-20232", "Product-20241", "Product-20242"};
      double[] amounts = {100.0, 200.0, 100.0, 200.0, 100.0, 200.0};
      String[] years = {"2022", "2022", "2023", "2023", "2024", "2024"};
      String[] months = {"01", "02", "01", "02", "01", "02"};
      for (int i = 0; i < 6; i++) {
        assertTrue(rs.next(), "row " + i);
        assertEquals(orderIds[i], rs.getInt("order_id"), "order_id row " + i);
        assertEquals(products[i], rs.getString("product"), "product row " + i);
        assertEquals(amounts[i], rs.getDouble("amount"), 0.0, "amount row " + i);
        assertEquals(years[i], rs.getString("year"), "year row " + i);
        assertEquals(months[i], rs.getString("month"), "month row " + i);
      }
      assertFalse(rs.next(), "exactly six partitioned rows");
    }
  }

  @Test @Tag("FILE-032") void pruningSkipsAllPartitionsForNonexistentValue() throws Exception {
    try (Connection c = openWithSalesSchema();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT \"order_id\" FROM \"test\".\"sales\" WHERE \"year\" = '2099'")) {
      assertFalse(rs.next(), "no partition matches year=2099, so zero rows (no silent full scan)");
    }
  }

  private Connection openWithSalesSchema() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    operand.put("ephemeralCache", true);
    operand.put("partitionedTables",
        Arrays.asList(createPartitionedTableConfig("sales", "sales/**/*.parquet")));
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
    CalciteConnection cc = connection.unwrap(CalciteConnection.class);
    cc.getRootSchema().add("test",
        FileSchemaFactory.INSTANCE.create(cc.getRootSchema(), "test", operand));
    return connection;
  }

  private void createHivePartitionedData() throws IOException {
    for (int year = 2022; year <= 2024; year++) {
      for (int month = 1; month <= 2; month++) {
        File partDir =
            new File(tempDir, String.format(Locale.ROOT, "sales/year=%d/month=%02d", year, month));
        partDir.mkdirs();
        writeParquetFile(new File(partDir, "data.parquet"),
            createSalesRecord(year * 100 + month, "Product-" + year + month, 100.0 * month));
      }
    }
  }

  private Map<String, Object> createPartitionedTableConfig(String name, String pattern) {
    Map<String, Object> config = new HashMap<>();
    config.put("name", name);
    config.put("pattern", pattern);
    return config;
  }

  private GenericRecord createSalesRecord(int orderId, String product, double amount) {
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put("order_id", orderId);
    record.put("product", product);
    record.put("amount", amount);
    return record;
  }

  private void writeParquetFile(File file, GenericRecord record) throws IOException {
    Configuration conf = new Configuration();
    OutputFile outputFile = HadoopOutputFile.fromPath(new Path(file.getAbsolutePath()), conf);
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
        .withSchema(avroSchema)
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      writer.write(record);
    }
  }
}
