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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Sources;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HLLAcceleratedTable}.
 *
 * <p>Verifies that HLL sketches are built correctly, that approximate
 * distinct counts are within tolerance of exact counts, and that
 * caching behavior works on repeated calls.
 */
@Tag("integration")
public class HLLAcceleratedTableTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(HLLAcceleratedTableTest.class);

  private File tempDir;
  private File parquetFile;
  private JavaTypeFactory typeFactory;
  private String savedCacheDir;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = java.nio.file.Files.createTempDirectory(
        "hll-table-test-").toFile();
    typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    // Save and set cache directory for HLL sketch persistence
    savedCacheDir = System.getProperty(
        "calcite.file.statistics.cache.directory");
    File cacheDir = new File(tempDir, "hll_cache");
    cacheDir.mkdirs();
    System.setProperty("calcite.file.statistics.cache.directory",
        cacheDir.getAbsolutePath());

    // Create test parquet file with known distinct values
    parquetFile = createTestParquetFile();
  }

  @AfterEach
  public void tearDown() {
    // Restore system property
    if (savedCacheDir != null) {
      System.setProperty("calcite.file.statistics.cache.directory",
          savedCacheDir);
    } else {
      System.clearProperty("calcite.file.statistics.cache.directory");
    }

    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
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

  @Test
  public void testBuildSketchesCreatesSketchesForAllColumns() {
    RelDataType rowType = createRowType();
    HLLAcceleratedTable table = new HLLAcceleratedTable(
        Sources.of(parquetFile), "test_table", rowType);

    // hasHLLSketch triggers lazy build of sketches
    boolean hasIdSketch = table.hasHLLSketch("id");
    boolean hasNameSketch = table.hasHLLSketch("name");
    boolean hasCategorySketch = table.hasHLLSketch("category");

    assertTrue(hasIdSketch, "Should have HLL sketch for 'id' column");
    assertTrue(hasNameSketch,
        "Should have HLL sketch for 'name' column");
    assertTrue(hasCategorySketch,
        "Should have HLL sketch for 'category' column");

    LOGGER.debug("Sketches built for all columns: id={}, name={}, category={}",
        hasIdSketch, hasNameSketch, hasCategorySketch);
  }

  @Test
  public void testApproximateCountVsExactCountWithinTolerance() {
    RelDataType rowType = createRowType();
    HLLAcceleratedTable table = new HLLAcceleratedTable(
        Sources.of(parquetFile), "test_table", rowType);

    // We wrote 100 rows with: id = 1..100 (100 distinct),
    // category = one of 5 values (5 distinct)
    long idDistinct = table.getDistinctCount("id");
    long categoryDistinct = table.getDistinctCount("category");

    // HLL with precision 14 has ~0.8% standard error, allow 20% tolerance
    // for small datasets where relative error can be higher
    double idTolerance = 100 * 0.20;
    double categoryTolerance = 5 * 0.50;  // More tolerance for very small counts

    assertTrue(Math.abs(idDistinct - 100) <= idTolerance,
        "Id distinct count " + idDistinct
            + " should be within 20% of 100");
    assertTrue(Math.abs(categoryDistinct - 5) <= categoryTolerance,
        "Category distinct count " + categoryDistinct
            + " should be within 50% of 5");

    LOGGER.debug("HLL estimates - id: {} (exact: 100), "
            + "category: {} (exact: 5)",
        idDistinct, categoryDistinct);
  }

  @Test
  public void testSecondCallUsesCache() {
    RelDataType rowType = createRowType();
    HLLAcceleratedTable table = new HLLAcceleratedTable(
        Sources.of(parquetFile), "test_table", rowType);

    // First call - triggers sketch building
    long startFirst = System.nanoTime();
    long firstCallResult = table.getDistinctCount("id");
    long firstDuration = System.nanoTime() - startFirst;

    // Second call - should use cached sketches (no rebuild)
    long startSecond = System.nanoTime();
    long secondCallResult = table.getDistinctCount("id");
    long secondDuration = System.nanoTime() - startSecond;

    // Results must be identical (same sketches)
    assertEquals(firstCallResult, secondCallResult,
        "Cached result should be identical to first result");

    // Second call should be significantly faster (no file scan)
    assertTrue(secondDuration < firstDuration,
        "Second call (" + secondDuration + " ns) should be faster "
            + "than first call (" + firstDuration + " ns)");

    LOGGER.debug("First call: {} ns, result={}. Second call: {} ns, result={}",
        firstDuration, firstCallResult, secondDuration, secondCallResult);
  }

  @Test
  public void testDistinctCountForNameColumn() {
    RelDataType rowType = createRowType();
    HLLAcceleratedTable table = new HLLAcceleratedTable(
        Sources.of(parquetFile), "test_table", rowType);

    // 100 rows with name = "name_0" through "name_99" -> 100 distinct
    long nameDistinct = table.getDistinctCount("name");

    // Allow 20% tolerance for HLL approximation
    double tolerance = 100 * 0.20;
    assertTrue(Math.abs(nameDistinct - 100) <= tolerance,
        "Name distinct count " + nameDistinct
            + " should be within 20% of 100");

    LOGGER.debug("HLL estimate for name column: {} (exact: 100)",
        nameDistinct);
  }

  @Test
  public void testNonExistentColumnReturnsNegativeOne() {
    RelDataType rowType = createRowType();
    HLLAcceleratedTable table = new HLLAcceleratedTable(
        Sources.of(parquetFile), "test_table", rowType);

    long result = table.getDistinctCount("nonexistent_column");
    assertEquals(-1, result,
        "Non-existent column should return -1");

    LOGGER.debug("Non-existent column distinct count: {}", result);
  }

  @SuppressWarnings("deprecation")
  private File createTestParquetFile() throws Exception {
    String schemaString = "{\"type\":\"record\","
        + "\"name\":\"TestRecord\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"},"
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"category\",\"type\":\"string\"}"
        + "]}";
    Schema avroSchema = new Schema.Parser().parse(schemaString);

    String[] categories = {"A", "B", "C", "D", "E"};

    File file = new File(tempDir, "hll_test.parquet");
    try (ParquetWriter<GenericRecord> writer =
             AvroParquetWriter
                 .<GenericRecord>builder(
                     new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
      for (int i = 0; i < 100; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", i);
        record.put("name", "name_" + i);
        record.put("category", categories[i % 5]);
        writer.write(record);
      }
    }

    return file;
  }

  private RelDataType createRowType() {
    List<String> names = new ArrayList<>();
    names.add("id");
    names.add("name");
    names.add("category");

    List<RelDataType> types = new ArrayList<>();
    types.add(typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.INTEGER), true));
    types.add(typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.VARCHAR), true));
    types.add(typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.VARCHAR), true));

    return typeFactory.createStructType(Pair.zip(names, types));
  }
}
