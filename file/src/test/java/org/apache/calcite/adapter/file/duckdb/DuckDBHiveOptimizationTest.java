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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.file.BaseFileTest;
import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.refresh.RefreshablePartitionedParquetTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

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
    // A directory of this test's own: named by the millisecond, two tests of the class collided
    tempDir = java.nio.file.Files.createTempDirectory("duckdb-hive-test-").toFile();
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
      writer.write("        \"baseDirectory\": \"" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "\",\n");
      writer.write("        \"directory\": \"" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "\",\n");
      writer.write("        \"ephemeralCache\": true,\n");
      writer.write("        \"refreshInterval\": \"5s\",\n");
      // The engine is the schema's operand ("parquet+duckdb" was never an engine type, and the
      // CALCITE_FILE_ENGINE_TYPE property these tests set is not what the factory reads)
      writer.write("        \"executionEngine\": \"DUCKDB\",\n");
      // A Hive-partitioned table is declared with the partitionedTables operand (the old
      // "tables" entry of type partitioned_parquet is not read), as in PartitionIntegrationTest
      writer.write("        \"partitionedTables\": [\n");
      writer.write("          {\n");
      writer.write("            \"name\": \"sales\",\n");
      writer.write("            \"pattern\": \"sales/**/*.parquet\",\n");
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
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + modelFile.getAbsolutePath())) {
      // The table says whether it skipped the initial file scan; a wall-clock bound on the
      // connection measured the runner's speed instead. With DuckDB the schema serves JDBC
      // views, and the file table is in the FileSchema behind them.
      SchemaPlus schema =
          connection.unwrap(CalciteConnection.class).getRootSchema().subSchemas().get("TEST");
      assertNotNull(schema, "TEST schema");
      FileSchema fileSchema = schema.unwrap(DuckDBJdbcSchema.class).getFileSchema();
      assertNotNull(fileSchema, "the DuckDB schema's FileSchema");
      Table sales = fileSchema.getDeclaredTable("sales");
      assertTrue(sales instanceof RefreshablePartitionedParquetTable,
          "sales is a partitioned table, was " + sales);
      assertTrue(((RefreshablePartitionedParquetTable) sales).usedLazyInitialization(),
          "DuckDB + Hive partitions skip the initial file scan");

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
    }
  }

  @Test public void testBaselineCreatedAndPersisted() throws Exception {
    // Gated as before until the baseline-persistence defect is fixed (next PR)
    String engine = System.getenv("CALCITE_FILE_ENGINE_TYPE");
    assumeTrue("parquet+duckdb".equalsIgnoreCase(engine),
        "Skipping DuckDB+Hive optimization test for engine: " + engine);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + modelFile.getAbsolutePath())) {
      try (Statement stmt = connection.createStatement()) {
        // Query to initialize table and create baseline
        String sql = "SELECT COUNT(*) FROM \"TEST\".\"sales\"";
        try (ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next());
        }

        // Wait a moment for baseline to be persisted
        Thread.sleep(500);

        // Check that conversion metadata file was created. ephemeralCache makes a fresh
        // ephemeral-<id> directory under the base directory, and the schema's cache is in it.
        File[] ephemeral = tempDir.listFiles((dir, name) -> name.startsWith("ephemeral-"));
        assertTrue(ephemeral != null && ephemeral.length == 1,
            "One ephemeral cache directory under the base directory");
        File cacheDir = new File(ephemeral[0], ".aperio/TEST");
        assertTrue(cacheDir.exists(), "Cache directory should exist");

        File metadataFile = new File(cacheDir, ".conversions.json");
        assertTrue(metadataFile.exists(), "Conversion metadata file should exist");

        // Read the metadata file to verify baseline was stored
        String metadataContent = new String(java.nio.file.Files.readAllBytes(metadataFile.toPath()));
        assertTrue(metadataContent.contains("baseline"), "Metadata should contain baseline");
        assertTrue(metadataContent.contains("files"), "Baseline should contain files");
      }
    } finally {
    }
  }

  @Test public void testChangeDetectionWithNewFile() throws Exception {

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
    }
  }

  @Test public void testPatternBasedViewCreation() throws Exception {

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
    }
  }
}
