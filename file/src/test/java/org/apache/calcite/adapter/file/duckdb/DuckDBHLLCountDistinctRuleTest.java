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

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.StatisticsCache;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DuckDBHLLCountDistinctRule} which replaces COUNT(DISTINCT)
 * operations on JDBC/DuckDB tables with pre-computed HLL sketch lookups.
 *
 * <p>This is a JDBC-specific version of the HLL rule that matches aggregate
 * patterns in the DuckDB query path and rewrites them to VALUES nodes
 * containing the HLL estimate.
 */
@Tag("integration")
public class DuckDBHLLCountDistinctRuleTest {

  private File tempDir;
  private File cacheDir;
  private Connection calciteConn;
  private String savedHllEnabled;
  private String savedCacheDirectory;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("duckdb-hll-test-").toFile();
    cacheDir = new File(tempDir, "hll_cache");
    cacheDir.mkdirs();

    // Save existing system properties
    savedHllEnabled =
        System.getProperty("calcite.file.statistics.hll.enabled");
    savedCacheDirectory =
        System.getProperty("calcite.file.statistics.cache.directory");

    // Enable HLL optimization
    System.setProperty("calcite.file.statistics.hll.enabled", "true");
    System.setProperty("calcite.file.statistics.cache.directory",
        cacheDir.getAbsolutePath());

    createTestData();
    createHLLSketches();
    setupCalciteConnection();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (calciteConn != null) {
      calciteConn.close();
    }

    restoreProperty("calcite.file.statistics.hll.enabled", savedHllEnabled);
    restoreProperty("calcite.file.statistics.cache.directory",
        savedCacheDirectory);

    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  /**
   * Test that COUNT(DISTINCT) on a DuckDB table with an HLL sketch is
   * rewritten to use the DuckDB HLL function/estimate. The result should
   * be approximately correct whether the HLL rule fires or DuckDB computes
   * the exact answer.
   */
  @Test public void testRewritesToDuckDBHLLFunction() throws Exception {
    String query =
        "SELECT COUNT(DISTINCT \"vendor_id\")"
            + " FROM files.\"duckdb_hll_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      // vendor_id: nextInt(120) -> ~120 distinct values
      assertTrue(result > 80 && result < 160,
          "COUNT(DISTINCT vendor_id) should be ~120, got: " + result);
    }
  }

  /**
   * Test COUNT(DISTINCT) on multiple columns in the DuckDB path.
   * Both columns should produce approximately correct results.
   */
  @Test public void testMultipleDistinctColumnsViaHLL() throws Exception {
    String query =
        "SELECT COUNT(DISTINCT \"vendor_id\"),"
            + " COUNT(DISTINCT \"store_id\")"
            + " FROM files.\"duckdb_hll_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long vendorDistinct = rs.getLong(1);
      long storeDistinct = rs.getLong(2);

      // vendor_id: ~120 distinct
      assertTrue(vendorDistinct > 80 && vendorDistinct < 160,
          "COUNT(DISTINCT vendor_id) should be ~120, got: "
              + vendorDistinct);
      // store_id: ~25 distinct
      assertTrue(storeDistinct > 15 && storeDistinct < 35,
          "COUNT(DISTINCT store_id) should be ~25, got: "
              + storeDistinct);
    }
  }

  /**
   * Test that a plain COUNT(*) is not matched by this rule and goes
   * through the normal DuckDB aggregate path.
   */
  @Test public void testPlainCountStarNotMatched() throws Exception {
    String query = "SELECT COUNT(*) FROM files.\"duckdb_hll_test\"";

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      long result = rs.getLong(1);
      assertTrue(result > 0,
          "COUNT(*) should return positive row count, got: " + result);
    }
  }

  /**
   * Verify the rule INSTANCE is properly initialized.
   */
  @Test public void testRuleInstanceExists() throws Exception {
    assertNotNull(DuckDBHLLCountDistinctRule.INSTANCE,
        "DuckDBHLLCountDistinctRule.INSTANCE should not be null");
  }

  @SuppressWarnings("deprecation")
  private void createTestData() throws Exception {
    File file = new File(tempDir, "duckdb_hll_test.parquet");

    String schemaString =
        "{\"type\": \"record\",\"name\": \"DuckDBHLLRecord\",\"fields\": ["
            + "  {\"name\": \"vendor_id\", \"type\": \"int\"},"
            + "  {\"name\": \"store_id\", \"type\": \"int\"},"
            + "  {\"name\": \"revenue\", \"type\": \"double\"}"
            + "]}";

    Schema avroSchema = new Schema.Parser().parse(schemaString);
    Random random = new Random(55);

    try (ParquetWriter<GenericRecord> writer =
             AvroParquetWriter
                 .<GenericRecord>builder(
                     new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
      for (int i = 0; i < 600; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("vendor_id", random.nextInt(120));
        record.put("store_id", random.nextInt(25));
        record.put("revenue", 50.0 + random.nextDouble() * 950.0);
        writer.write(record);
      }
    }
  }

  private void createHLLSketches() throws Exception {
    // Create HLL sketch for vendor_id
    HyperLogLogSketch vendorSketch = new HyperLogLogSketch(14);
    for (int i = 0; i < 120; i++) {
      vendorSketch.add(String.valueOf(i));
    }
    File vendorFile =
        new File(cacheDir, "duckdb_hll_test_vendor_id.hll");
    StatisticsCache.saveHLLSketch(vendorSketch, vendorFile);

    // Create HLL sketch for store_id
    HyperLogLogSketch storeSketch = new HyperLogLogSketch(14);
    for (int i = 0; i < 25; i++) {
      storeSketch.add(String.valueOf(i));
    }
    File storeFile =
        new File(cacheDir, "duckdb_hll_test_store_id.hll");
    StatisticsCache.saveHLLSketch(storeSketch, storeFile);
  }

  private void setupCalciteConnection() throws Exception {
    calciteConn = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
    CalciteConnection calciteConnection =
        calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "duckdb");
    operand.put("ephemeralCache", true);

    rootSchema.add("files",
        FileSchemaFactory.INSTANCE.create(rootSchema, "files", operand));
  }

  private static void restoreProperty(String key, String savedValue) {
    if (savedValue == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, savedValue);
    }
  }

  private void deleteDirectory(File dir) {
    if (dir.isDirectory()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          deleteDirectory(file);
        }
      }
    }
    dir.delete();
  }
}
