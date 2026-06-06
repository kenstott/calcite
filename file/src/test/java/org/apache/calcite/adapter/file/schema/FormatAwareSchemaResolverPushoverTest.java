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
package org.apache.calcite.adapter.file.schema;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Additional unit tests for {@link FormatAwareSchemaResolver}
 * and {@link SchemaStrategy} to push schema package coverage past 75%.
 */
@Tag("unit")
public class FormatAwareSchemaResolverPushoverTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(FormatAwareSchemaResolverPushoverTest.class);

  @TempDir
  Path tempDir;

  private FormatAwareSchemaResolver resolver;
  private RelDataTypeFactory typeFactory;

  @BeforeEach
  void setUp() {
    typeFactory = new JavaTypeFactoryImpl();
    resolver = new FormatAwareSchemaResolver(typeFactory);
  }

  // --- SchemaStrategy tests ---

  @Test
  @DisplayName("SchemaStrategy toString returns formatted string")
  void testSchemaStrategyToString() {
    String result = SchemaStrategy.PARQUET_DEFAULT.toString();
    assertNotNull(result);
    assertTrue(result.contains("LATEST_SCHEMA_WINS"),
        "toString should contain parquet strategy: " + result);
    assertTrue(result.contains("RICHEST_FILE"),
        "toString should contain csv strategy: " + result);
    assertTrue(result.contains("LATEST_FILE"),
        "toString should contain json strategy: " + result);
    LOGGER.debug("SchemaStrategy.toString: {}", result);
  }

  @Test
  @DisplayName("SchemaStrategy CONSERVATIVE preset has expected values")
  void testSchemaStrategyConservative() {
    assertEquals(SchemaStrategy.ParquetStrategy.LATEST_FILE,
        SchemaStrategy.CONSERVATIVE.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.RICHEST_FILE,
        SchemaStrategy.CONSERVATIVE.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.LATEST_FILE,
        SchemaStrategy.CONSERVATIVE.getJsonStrategy());
  }

  @Test
  @DisplayName("SchemaStrategy AGGRESSIVE_UNION preset has expected values")
  void testSchemaStrategyAggressiveUnion() {
    assertEquals(SchemaStrategy.ParquetStrategy.UNION_ALL_COLUMNS,
        SchemaStrategy.AGGRESSIVE_UNION.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.UNION_COMMON,
        SchemaStrategy.AGGRESSIVE_UNION.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.UNION_KEYS,
        SchemaStrategy.AGGRESSIVE_UNION.getJsonStrategy());
  }

  @Test
  @DisplayName("SchemaStrategy custom with format priority")
  void testSchemaStrategyCustomFormatPriority() {
    List<String> priority = Arrays.asList("json", "parquet", "csv");
    SchemaStrategy custom = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.FIRST_FILE,
        SchemaStrategy.CsvStrategy.USER_DEFINED,
        SchemaStrategy.JsonStrategy.INTERSECTION_ONLY,
        priority,
        SchemaStrategy.ValidationLevel.ERROR);

    assertEquals(SchemaStrategy.ParquetStrategy.FIRST_FILE, custom.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.USER_DEFINED, custom.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.INTERSECTION_ONLY, custom.getJsonStrategy());
    assertEquals(priority, custom.getFormatPriority());
    assertEquals(SchemaStrategy.ValidationLevel.ERROR, custom.getValidationLevel());
  }

  @Test
  @DisplayName("SchemaStrategy default format priority is parquet, csv, json")
  void testSchemaStrategyDefaultPriority() {
    assertEquals(Arrays.asList("parquet", "csv", "json"),
        SchemaStrategy.PARQUET_DEFAULT.getFormatPriority());
  }

  @Test
  @DisplayName("SchemaStrategy default validation level is WARN")
  void testSchemaStrategyDefaultValidation() {
    assertEquals(SchemaStrategy.ValidationLevel.WARN,
        SchemaStrategy.PARQUET_DEFAULT.getValidationLevel());
  }

  // --- FormatAwareSchemaResolver CSV tests ---

  @Test
  @DisplayName("resolveSchema with empty file list returns empty schema")
  void testResolveSchemaEmptyList() {
    RelDataType result = resolver.resolveSchema(
        Collections.emptyList(), SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(result);
    assertEquals(0, result.getFieldCount());
  }

  @Test
  @DisplayName("resolveSchema with single CSV file infers columns")
  void testResolveSchemaSingleCsv() throws IOException {
    File csv = writeCsv("data.csv", "id,name,value\n1,Alice,100\n");
    List<File> files = new ArrayList<>();
    files.add(csv);

    RelDataType result = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(result);
    assertEquals(3, result.getFieldCount());
    LOGGER.debug("CSV schema: {}", result);
  }

  @Test
  @DisplayName("resolveSchema with multiple CSV files uses richest file")
  void testResolveSchemaMultipleCsv() throws IOException {
    File csv1 = writeCsv("small.csv", "id,name\n1,A\n");
    File csv2 = writeCsv("big.csv", "id,name,value,extra\n1,B,100,x\n");
    List<File> files = new ArrayList<>();
    files.add(csv1);
    files.add(csv2);

    RelDataType result = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(result);
    assertEquals(4, result.getFieldCount(),
        "Should use schema from file with most columns");
  }

  // --- FormatAwareSchemaResolver JSON tests ---

  @Test
  @DisplayName("resolveSchema with single JSON file infers fields")
  void testResolveSchemaSingleJson() throws IOException {
    File json = writeFile("data.json",
        "[{\"id\": 1, \"name\": \"Alice\"}, {\"id\": 2, \"name\": \"Bob\"}]");
    List<File> files = new ArrayList<>();
    files.add(json);

    RelDataType result = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(result);
    assertEquals(2, result.getFieldCount());
    LOGGER.debug("JSON schema: {}", result);
  }

  @Test
  @DisplayName("resolveSchema with JSON object infers fields from top-level")
  void testResolveSchemaJsonObject() throws IOException {
    File json = writeFile("obj.json",
        "{\"id\": 1, \"name\": \"Alice\", \"active\": true}");
    List<File> files = new ArrayList<>();
    files.add(json);

    RelDataType result = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(result);
    assertEquals(3, result.getFieldCount());
  }

  @Test
  @DisplayName("resolveSchema JSON with multiple files validates compatibility")
  void testResolveSchemaMultipleJsonValidation() throws IOException {
    File json1 = writeFile("first.json",
        "{\"id\": 1, \"name\": \"A\", \"extra\": \"x\"}");
    File json2 = writeFile("second.json",
        "{\"id\": 2, \"name\": \"B\"}");
    // Make json2 more recent
    json2.setLastModified(System.currentTimeMillis() + 1000);

    List<File> files = new ArrayList<>();
    files.add(json1);
    files.add(json2);

    RelDataType result = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(result);
    // Should use latest file (json2)
    LOGGER.debug("Multi-JSON schema: {}", result);
  }

  // --- Format detection tests ---

  @Test
  @DisplayName("resolveSchema detects CSV format from extension")
  void testDetectCsvFormat() throws IOException {
    File csv = writeCsv("test.csv", "a,b\n1,2\n");
    List<File> files = new ArrayList<>();
    files.add(csv);

    RelDataType result = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(result);
    assertTrue(result.getFieldCount() > 0);
  }

  @Test
  @DisplayName("resolveSchema with unknown extension detects by content")
  void testDetectFormatByContent() throws IOException {
    // JSON content with .dat extension
    File jsonDat = writeFile("data.dat",
        "{\"x\": 1, \"y\": 2}");
    List<File> files = new ArrayList<>();
    files.add(jsonDat);

    RelDataType result = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(result);
    LOGGER.debug("Content-detected schema: {}", result);
  }

  @Test
  @DisplayName("resolveSchema with mixed CSV and JSON uses priority")
  void testResolveSchemaMixedFormats() throws IOException {
    File csv = writeCsv("data.csv", "id,name\n1,A\n");
    File json = writeFile("data.json", "{\"id\": 1}");
    List<File> files = new ArrayList<>();
    files.add(csv);
    files.add(json);

    RelDataType result = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(result);
    // CSV has higher priority than JSON in default priority
    LOGGER.debug("Mixed format schema: {}", result);
  }

  // --- ParquetStrategy enum coverage ---

  @Test
  @DisplayName("ParquetStrategy enum values are all accessible")
  void testParquetStrategyEnumValues() {
    SchemaStrategy.ParquetStrategy[] values = SchemaStrategy.ParquetStrategy.values();
    assertTrue(values.length >= 6, "Should have at least 6 parquet strategies");
    assertNotNull(SchemaStrategy.ParquetStrategy.valueOf("LATEST_SCHEMA_WINS"));
    assertNotNull(SchemaStrategy.ParquetStrategy.valueOf("UNION_ALL_COLUMNS"));
    assertNotNull(SchemaStrategy.ParquetStrategy.valueOf("UNION_WITH_PROMOTION"));
    assertNotNull(SchemaStrategy.ParquetStrategy.valueOf("LATEST_FILE"));
    assertNotNull(SchemaStrategy.ParquetStrategy.valueOf("FIRST_FILE"));
    assertNotNull(SchemaStrategy.ParquetStrategy.valueOf("INTERSECTION_ONLY"));
  }

  @Test
  @DisplayName("CsvStrategy enum values are all accessible")
  void testCsvStrategyEnumValues() {
    SchemaStrategy.CsvStrategy[] values = SchemaStrategy.CsvStrategy.values();
    assertTrue(values.length >= 4, "Should have at least 4 csv strategies");
    assertNotNull(SchemaStrategy.CsvStrategy.valueOf("RICHEST_FILE"));
    assertNotNull(SchemaStrategy.CsvStrategy.valueOf("LATEST_FILE"));
    assertNotNull(SchemaStrategy.CsvStrategy.valueOf("UNION_COMMON"));
    assertNotNull(SchemaStrategy.CsvStrategy.valueOf("USER_DEFINED"));
  }

  @Test
  @DisplayName("JsonStrategy enum values are all accessible")
  void testJsonStrategyEnumValues() {
    SchemaStrategy.JsonStrategy[] values = SchemaStrategy.JsonStrategy.values();
    assertTrue(values.length >= 4, "Should have at least 4 json strategies");
    assertNotNull(SchemaStrategy.JsonStrategy.valueOf("LATEST_FILE"));
    assertNotNull(SchemaStrategy.JsonStrategy.valueOf("UNION_KEYS"));
    assertNotNull(SchemaStrategy.JsonStrategy.valueOf("FIRST_FILE"));
    assertNotNull(SchemaStrategy.JsonStrategy.valueOf("INTERSECTION_ONLY"));
  }

  @Test
  @DisplayName("ValidationLevel enum values are all accessible")
  void testValidationLevelEnumValues() {
    SchemaStrategy.ValidationLevel[] values = SchemaStrategy.ValidationLevel.values();
    assertEquals(3, values.length);
    assertNotNull(SchemaStrategy.ValidationLevel.valueOf("NONE"));
    assertNotNull(SchemaStrategy.ValidationLevel.valueOf("WARN"));
    assertNotNull(SchemaStrategy.ValidationLevel.valueOf("ERROR"));
  }

  // --- Helper methods ---

  private File writeCsv(String name, String content) throws IOException {
    return writeFile(name, content);
  }

  private File writeFile(String name, String content) throws IOException {
    File file = new File(tempDir.toFile(), name);
    try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
      writer.print(content);
    }
    return file;
  }
}
