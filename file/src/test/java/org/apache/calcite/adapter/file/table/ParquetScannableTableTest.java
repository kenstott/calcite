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

import org.apache.calcite.adapter.file.FileSchemaFactory;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParquetScannableTable}.
 *
 * <p>Verifies that scan returns the correct number of rows,
 * FilterableTable filter pushdown works, and null values are handled.
 * Uses partitionedTables configuration for proper table discovery
 * through the file adapter.
 */
@Tag("integration")
public class ParquetScannableTableTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ParquetScannableTableTest.class);

  private File tempDir;
  private Connection calciteConn;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = java.nio.file.Files.createTempDirectory(
        "parquet-scannable-test-").toFile();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (calciteConn != null) {
      calciteConn.close();
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
  public void testScanReturnsCorrectNumberOfRows() throws Exception {
    int expectedRows = 15;
    createTestParquetData("scan_data", expectedRows, false);
    setupCalciteConnection("scan_data", "scan_data/**/*.parquet");

    String query = "SELECT count(*) AS cnt FROM \"files\".\"scan_data\"";
    int actualCount = 0;

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      actualCount = rs.getInt(1);
    }

    assertEquals(expectedRows, actualCount,
        "Scan should return " + expectedRows + " rows");
    LOGGER.debug("Scan returned {} rows as expected", actualCount);
  }

  @Test
  public void testScanReturnsCorrectColumnValues() throws Exception {
    createTestParquetData("values_data", 5, false);
    setupCalciteConnection("values_data", "values_data/**/*.parquet");

    String query = "SELECT \"id\", \"name\", \"amount\" "
        + "FROM \"files\".\"values_data\" ORDER BY \"id\"";

    List<Integer> ids = new ArrayList<>();
    List<String> names = new ArrayList<>();
    List<Double> amounts = new ArrayList<>();

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        ids.add(rs.getInt("id"));
        names.add(rs.getString("name"));
        amounts.add(rs.getDouble("amount"));
      }
    }

    assertEquals(5, ids.size(), "Should have 5 rows");
    assertEquals(1, ids.get(0).intValue(), "First id should be 1");
    assertEquals("name_1", names.get(0), "First name should be name_1");
    assertEquals(100.0, amounts.get(0), 0.01,
        "First amount should be 100.0");
    assertEquals(5, ids.get(4).intValue(), "Last id should be 5");

    LOGGER.debug("Verified column values for {} rows", ids.size());
  }

  @Test
  public void testFilterPushdownWithWhereClause() throws Exception {
    createTestParquetData("filter_data", 20, false);
    setupCalciteConnection("filter_data", "filter_data/**/*.parquet");

    // Use a WHERE clause that triggers filter pushdown (IS NOT NULL)
    String query = "SELECT count(*) AS cnt "
        + "FROM \"files\".\"filter_data\" "
        + "WHERE \"name\" IS NOT NULL";

    int filteredCount = 0;
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      filteredCount = rs.getInt(1);
    }

    // All rows have non-null names, so count should be 20
    assertEquals(20, filteredCount,
        "All rows have non-null names, so filtered count should be 20");
    LOGGER.debug("Filter pushdown returned {} rows", filteredCount);
  }

  @Test
  public void testNullValuesHandledCorrectly() throws Exception {
    createTestParquetData("null_data", 10, true);
    setupCalciteConnection("null_data", "null_data/**/*.parquet");

    // Query all rows - some will have null names
    String query = "SELECT \"id\", \"name\" "
        + "FROM \"files\".\"null_data\" ORDER BY \"id\"";

    int totalRows = 0;
    int nullNameCount = 0;
    int nonNullNameCount = 0;

    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        totalRows++;
        String name = rs.getString("name");
        if (rs.wasNull() || name == null) {
          nullNameCount++;
        } else {
          nonNullNameCount++;
        }
      }
    }

    assertEquals(10, totalRows, "Should have 10 total rows");
    assertTrue(nullNameCount > 0,
        "Should have at least one row with null name");
    assertTrue(nonNullNameCount > 0,
        "Should have at least one row with non-null name");
    assertEquals(totalRows, nullNameCount + nonNullNameCount,
        "Null + non-null counts should equal total rows");

    LOGGER.debug("Null handling: {} total, {} null, {} non-null",
        totalRows, nullNameCount, nonNullNameCount);
  }

  @Test
  public void testFilterPushdownExcludesNullRows() throws Exception {
    createTestParquetData("null_filter_data", 10, true);
    setupCalciteConnection("null_filter_data",
        "null_filter_data/**/*.parquet");

    // IS NOT NULL filter should exclude rows with null names
    String query = "SELECT count(*) AS cnt "
        + "FROM \"files\".\"null_filter_data\" "
        + "WHERE \"name\" IS NOT NULL";

    int filteredCount = 0;
    try (Statement stmt = calciteConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Should have a result row");
      filteredCount = rs.getInt(1);
    }

    // Even-indexed rows (0, 2, 4, 6, 8) have null names -> ids 1, 3, 5, 7, 9
    // Odd-indexed rows (1, 3, 5, 7, 9) have non-null names -> ids 2, 4, 6, 8, 10
    assertEquals(5, filteredCount,
        "Should have 5 rows with non-null names");
    LOGGER.debug("IS NOT NULL filter returned {} of 10 rows",
        filteredCount);
  }

  @SuppressWarnings("deprecation")
  private void createTestParquetData(String dirName, int numRows,
      boolean includeNulls) throws Exception {
    // Use a union type for nullable string field
    String nameType = includeNulls
        ? "[\"null\", \"string\"]"
        : "\"string\"";

    String schemaString = "{\"type\":\"record\","
        + "\"name\":\"TestRecord\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"},"
        + "{\"name\":\"name\",\"type\":" + nameType + "},"
        + "{\"name\":\"amount\",\"type\":\"double\"}"
        + "]}";
    Schema avroSchema = new Schema.Parser().parse(schemaString);

    // Create data inside a nested subdirectory for glob-based discovery
    // The pattern "dirName/**/*.parquet" needs at least one subdirectory level
    File dataDir = new File(tempDir, dirName + "/part_0");
    dataDir.mkdirs();

    File parquetFile = new File(dataDir, "data.parquet");
    try (ParquetWriter<GenericRecord> writer =
             AvroParquetWriter
                 .<GenericRecord>builder(
                     new org.apache.hadoop.fs.Path(
                         parquetFile.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
      for (int i = 0; i < numRows; i++) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", i + 1);
        if (includeNulls && i % 2 == 0) {
          record.put("name", null);
        } else {
          record.put("name", "name_" + (i + 1));
        }
        record.put("amount", 100.0 * (i + 1));
        writer.write(record);
      }
    }
  }

  private void setupCalciteConnection(String tableName, String pattern)
      throws Exception {
    calciteConn = DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
    CalciteConnection calciteConnection =
        calciteConn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Map<String, Object> tableConfig = new HashMap<>();
    tableConfig.put("name", tableName);
    tableConfig.put("pattern", pattern);

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "parquet");
    operand.put("ephemeralCache", true);
    operand.put("partitionedTables", Arrays.asList(tableConfig));

    rootSchema.add("files",
        FileSchemaFactory.INSTANCE.create(rootSchema, "files", operand));
  }
}
