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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.file.BaseFileTest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test to verify DuckDB+Hive optimization with baseline and pattern-based refresh.
 * Tests that:
 * 1. Initial file scan is skipped for DuckDB+Hive tables
 * 2. Baseline is created and persisted
 * 3. File changes are detected using fast metadata comparison
 * 4. Pattern-based view refresh works correctly
 */
@Tag("integration")
@SuppressWarnings("deprecation")
public class DuckDBHiveOptimizationTest extends BaseFileTest {
  private File tempDir;
  private File dataDir;
  private File modelFile;

  @BeforeEach
  public void setUp() throws Exception {
    // Skip test if not running with parquet+duckdb engine
    String engine = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    assumeTrue("parquet+duckdb".equalsIgnoreCase(engine),
        "Skipping DuckDB+Hive optimization test for engine: " + engine);

    tempDir = new File(System.getProperty("java.io.tmpdir"), "duckdb-hive-test-" + System.currentTimeMillis());
    tempDir.mkdirs();
    tempDir.deleteOnExit();

    // Create Hive-partitioned directory structure: year=2023/month=01/
    dataDir = new File(tempDir, "sales");
    dataDir.mkdirs();

    File partition1 = new File(dataDir, "year=2023/month=01");
    partition1.mkdirs();
    File partition2 = new File(dataDir, "year=2023/month=02");
    partition2.mkdirs();

    // Create parquet files in each partition
    createParquetFile(new File(partition1, "data.parquet"), 2023, 1, 100);
    createParquetFile(new File(partition2, "data.parquet"), 2023, 2, 150);

    // Create model file with DuckDB+Hive configuration and short refresh interval
    modelFile = new File(tempDir, "model.json");
    try (FileWriter writer = new FileWriter(modelFile)) {
      writer.write("{\n");
      writer.write("  \"version\": \"1.0\",\n");
      writer.write("  \"defaultSchema\": \"TEST\",\n");
      writer.write("  \"schemas\": [\n");
      writer.write("    {\n");
      writer.write("      \"name\": \"TEST\",\n");
      writer.write("      \"type\": \"custom\",\n");
      writer.write("      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n");
      writer.write("      \"operand\": {\n");
      writer.write("        \"directory\": \"" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "\",\n");
      writer.write("        \"ephemeralCache\": true,\n");
      writer.write("        \"refreshInterval\": \"5s\",\n");
      writer.write("        \"engine\": {\n");
      writer.write("          \"type\": \"parquet+duckdb\"\n");
      writer.write("        },\n");
      writer.write("        \"tables\": [\n");
      writer.write("          {\n");
      writer.write("            \"name\": \"sales\",\n");
      writer.write("            \"type\": \"partitioned_parquet\",\n");
      writer.write("            \"directory\": \"" + dataDir.getAbsolutePath().replace("\\", "\\\\") + "\",\n");
      writer.write("            \"pattern\": \"**/*.parquet\",\n");
      writer.write("            \"partitions\": {\n");
      writer.write("              \"style\": \"hive\"\n");
      writer.write("            }\n");
      writer.write("          }\n");
      writer.write("        ]\n");
      writer.write("      }\n");
      writer.write("    }\n");
      writer.write("  ]\n");
      writer.write("}\n");
    }
  }

  /**
   * Creates a parquet file with sample sales data.
   */
  private void createParquetFile(File file, int year, int month, int amount) throws Exception {
    // Define Avro schema
    String schemaJson = "{\n"
  +
        "  \"type\": \"record\",\n"
  +
        "  \"name\": \"Sale\",\n"
  +
        "  \"fields\": [\n"
  +
        "    {\"name\": \"id\", \"type\": \"int\"},\n"
  +
        "    {\"name\": \"amount\", \"type\": \"int\"},\n"
  +
        "    {\"name\": \"product\", \"type\": \"string\"}\n"
  +
        "  ]\n"
  +
        "}";
    Schema schema = new Schema.Parser().parse(schemaJson);

    // Write parquet file
    Path path = new Path(file.toURI());
    Configuration conf = new Configuration();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(path)
        .withSchema(schema)
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {

      // Write a few records
      for (int i = 1; i <= 3; i++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", i);
        record.put("amount", amount + i);
        record.put("product", "Product" + i);
        writer.write(record);
      }
    }
  }

  @Test public void testInitialScanSkipped() throws Exception {
    System.setProperty("CALCITE_FILE_ENGINE_TYPE", "parquet+duckdb");

    long startTime = System.currentTimeMillis();

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + modelFile.getAbsolutePath())) {
      long connectionTime = System.currentTimeMillis() - startTime;

      // Connection should be fast (< 2 seconds) because initial scan is skipped
      assertTrue(connectionTime < 2000,
          "Connection should be fast with skipped initial scan, took: " + connectionTime + "ms");

      try (Statement stmt = connection.createStatement()) {
        // Query the table - this will trigger lazy initialization
        String sql = "SELECT COUNT(*) as cnt FROM \"TEST\".\"sales\"";
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next(), "Should have results");
          int count = rs.getInt("cnt");
          assertEquals(6, count, "Should have 6 total rows (3 per partition)");
        }

        // Verify partition columns are accessible
        sql = "SELECT \"year\", \"month\", SUM(\"amount\") as total FROM \"TEST\".\"sales\" GROUP BY \"year\", \"month\" ORDER BY \"year\", \"month\"";
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next(), "Should have first partition");
          assertEquals("2023", rs.getString("year"));
          assertEquals("01", rs.getString("month"));
          assertTrue(rs.getInt("total") > 0);

          assertTrue(rs.next(), "Should have second partition");
          assertEquals("2023", rs.getString("year"));
          assertEquals("02", rs.getString("month"));
          assertTrue(rs.getInt("total") > 0);
        }
      }
    } finally {
      System.clearProperty("CALCITE_FILE_ENGINE_TYPE");
    }
  }

  @Test public void testBaselineCreatedAndPersisted() throws Exception {
    System.setProperty("CALCITE_FILE_ENGINE_TYPE", "parquet+duckdb");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + modelFile.getAbsolutePath())) {
      try (Statement stmt = connection.createStatement()) {
        // Query to initialize table and create baseline
        String sql = "SELECT COUNT(*) FROM \"TEST\".\"sales\"";
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
        }

        // Wait a moment for baseline to be persisted
        Thread.sleep(500);

        // Check that conversion metadata file was created
        File cacheDir = new File(tempDir, ".aperio/TEST");
        assertTrue(cacheDir.exists(), "Cache directory should exist");

        File metadataFile = new File(cacheDir, ".conversions.json");
        assertTrue(metadataFile.exists(), "Conversion metadata file should exist");

        // Read the metadata file to verify baseline was stored
        String metadataContent = new String(java.nio.file.Files.readAllBytes(metadataFile.toPath()));
        assertTrue(metadataContent.contains("baseline"), "Metadata should contain baseline");
        assertTrue(metadataContent.contains("files"), "Baseline should contain files");
      }
    } finally {
      System.clearProperty("CALCITE_FILE_ENGINE_TYPE");
    }
  }

  @Test public void testChangeDetectionWithNewFile() throws Exception {
    System.setProperty("CALCITE_FILE_ENGINE_TYPE", "parquet+duckdb");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + modelFile.getAbsolutePath())) {
      try (Statement stmt = connection.createStatement()) {
        // Initial query
        String sql = "SELECT COUNT(*) as cnt FROM \"TEST\".\"sales\"";
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          assertEquals(6, rs.getInt("cnt"), "Should have 6 rows initially");
        }

        // Add a new file to an existing partition
        File partition1 = new File(dataDir, "year=2023/month=01");
        createParquetFile(new File(partition1, "data2.parquet"), 2023, 1, 200);

        // Wait for refresh interval (5 seconds + buffer)
        Thread.sleep(6000);

        // Query again - should detect new file and include new data
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
          int newCount = rs.getInt("cnt");
          assertEquals(9, newCount, "Should have 9 rows after adding new file (6 + 3)");
        }
      }
    } finally {
      System.clearProperty("CALCITE_FILE_ENGINE_TYPE");
    }
  }

  @Test public void testPatternBasedViewCreation() throws Exception {
    System.setProperty("CALCITE_FILE_ENGINE_TYPE", "parquet+duckdb");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + modelFile.getAbsolutePath())) {
      try (Statement stmt = connection.createStatement()) {
        // Query to verify Hive partitioning works
        String sql = "SELECT \"year\", \"month\", COUNT(*) as cnt FROM \"TEST\".\"sales\" GROUP BY \"year\", \"month\"";
        try (ResultSet rs = stmt.executeQuery(sql)) {
          int partitionCount = 0;
          while (rs.next()) {
            partitionCount++;
            assertNotNull(rs.getString("year"), "Year partition should not be null");
            assertNotNull(rs.getString("month"), "Month partition should not be null");
            assertTrue(rs.getInt("cnt") > 0, "Each partition should have rows");
          }
          assertEquals(2, partitionCount, "Should have 2 partitions");
        }
      }
    } finally {
      System.clearProperty("CALCITE_FILE_ENGINE_TYPE");
    }
  }
}
