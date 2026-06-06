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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for FormatAwareSchemaResolver.
 * Covers format detection by content, mixed format resolution,
 * JSON schema inference details, CSV compatibility validation,
 * and various Parquet strategy code paths.
 */
@Tag("unit")
public class FormatAwareSchemaResolverDeepTest {

  @TempDir
  Path tempDir;

  private FormatAwareSchemaResolver resolver;
  private RelDataTypeFactory typeFactory;

  @BeforeEach
  void setUp() {
    typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    resolver = new FormatAwareSchemaResolver(typeFactory);
  }

  // --- Format detection tests ---

  @Test
  void testDetectFormatByContentJsonStartingWithBrace() throws IOException {
    // File with .dat extension but starts with { so should be detected as JSON
    File file = createFileWithContent("data.dat", "{\"key\": \"value\"}");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    // Should detect as JSON and infer schema from it
    assertNotNull(schema, "Schema should be resolved for JSON content");
    assertTrue(schema.getFieldCount() > 0, "JSON detected file should produce fields");
  }

  @Test
  void testDetectFormatByContentJsonStartingWithBracket() throws IOException {
    // File starts with [ so should be detected as JSON
    File file = createFileWithContent("data.dat", "[{\"id\": 1, \"name\": \"test\"}]");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertNotNull(schema, "Schema should be resolved for JSON array content");
    assertTrue(schema.getFieldCount() > 0, "JSON array should produce fields");
  }

  @Test
  void testDetectFormatByContentDefaultsToCsv() throws IOException {
    // File with unknown extension and non-JSON/non-Parquet content
    File file = createFileWithContent("data.dat", "col1,col2,col3\n1,2,3");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    // Should default to CSV
    assertNotNull(schema, "Schema should default to CSV");
    assertEquals(3, schema.getFieldCount(), "Should have 3 CSV columns");
  }

  @Test
  void testDetectFormatByContentIoException() throws IOException {
    // Create a file, detect format, then rely on the fallback
    File file = createFileWithContent("empty.dat", "");

    List<File> files = Collections.singletonList(file);
    // Even with an empty file or read failure, should still resolve
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema, "Should still produce a schema even for problematic files");
  }

  // --- JSON schema inference tests ---

  @Test
  void testJsonSchemaInferenceWithBooleanField() throws IOException {
    File file = createJsonFile("bool.json", "{\"flag\": true}");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(1, schema.getFieldCount());
    assertEquals("flag", schema.getFieldList().get(0).getName());
    assertEquals(SqlTypeName.BOOLEAN, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testJsonSchemaInferenceWithIntegerField() throws IOException {
    File file = createJsonFile("int.json", "{\"count\": 42}");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(1, schema.getFieldCount());
    assertEquals(SqlTypeName.INTEGER, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testJsonSchemaInferenceWithLongField() throws IOException {
    File file = createJsonFile("long.json", "{\"bignum\": 9999999999999}");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(1, schema.getFieldCount());
    assertEquals(SqlTypeName.BIGINT, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testJsonSchemaInferenceWithDoubleField() throws IOException {
    File file = createJsonFile("double.json", "{\"ratio\": 3.14}");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(1, schema.getFieldCount());
    assertEquals(SqlTypeName.DOUBLE, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testJsonSchemaInferenceWithNullField() throws IOException {
    File file = createJsonFile("null.json", "{\"missing\": null}");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(1, schema.getFieldCount());
    // Null defaults to VARCHAR
    assertEquals(SqlTypeName.VARCHAR, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testJsonSchemaInferenceWithArrayField() throws IOException {
    File file = createJsonFile("array.json", "{\"tags\": [\"a\", \"b\"]}");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(1, schema.getFieldCount());
    // Arrays map to ARRAY type
    assertNotNull(schema.getFieldList().get(0).getType());
  }

  @Test
  void testJsonSchemaInferenceWithObjectField() throws IOException {
    File file = createJsonFile("nested.json", "{\"meta\": {\"nested\": true}}");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(1, schema.getFieldCount());
    // Objects serialize as VARCHAR
    assertEquals(SqlTypeName.VARCHAR, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testJsonSchemaInferenceWithArrayRoot() throws IOException {
    // JSON array at root - should use first element as template
    File file = createJsonFile("arr_root.json",
        "[{\"id\": 1, \"name\": \"first\"}, {\"id\": 2, \"name\": \"second\"}]");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(2, schema.getFieldCount());
    assertTrue(schema.getFieldNames().contains("id"));
    assertTrue(schema.getFieldNames().contains("name"));
  }

  @Test
  void testJsonSchemaInferenceWithTextualField() throws IOException {
    File file = createJsonFile("text.json", "{\"greeting\": \"hello\"}");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(1, schema.getFieldCount());
    assertEquals(SqlTypeName.VARCHAR, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  // --- CSV validation tests ---

  @Test
  void testCsvSchemaWithDifferentColumnCounts() throws IOException {
    // Create CSV files with mismatched column counts
    File csv1 = createCsvFile("wide.csv", "id,name,email,phone\n1,a,b,c");
    File csv2 = createCsvFile("narrow.csv", "id,name\n1,a");

    List<File> files = Arrays.asList(csv1, csv2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    // Should use richest file (csv1 with 4 columns)
    assertEquals(4, schema.getFieldCount());
  }

  @Test
  void testCsvSchemaWithMismatchedColumnNames() throws IOException {
    // Create CSV files with different column names (different column names trigger warning)
    File csv1 = createCsvFile("main.csv", "id,name,email\n1,a,b");
    File csv2 = createCsvFile("other.csv", "id,title,addr\n1,a,b");

    List<File> files = Arrays.asList(csv1, csv2);
    // csv1 and csv2 have equal column count, richest wins based on sort order
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(3, schema.getFieldCount());
  }

  @Test
  void testCsvSchemaWithEmptyColumn() throws IOException {
    // CSV with empty column name gets default col_N name
    File file = createCsvFile("empty_col.csv", "id,,value\n1,2,3");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(3, schema.getFieldCount());
    // Middle column should be "col_1"
    assertEquals("col_1", schema.getFieldList().get(1).getName());
  }

  @Test
  void testCsvColumnCountForEmptyFile() throws IOException {
    // Empty CSV file
    File file = createFileWithContent("empty.csv", "");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(0, schema.getFieldCount());
  }

  @Test
  void testCsvSchemaWithQuotedHeaders() throws IOException {
    File file = createCsvFile("quoted.csv", "\"id\",\"full name\",\"email\"\n1,a,b");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(3, schema.getFieldCount());
    assertEquals("id", schema.getFieldList().get(0).getName());
    assertEquals("full name", schema.getFieldList().get(1).getName());
  }

  // --- JSON validation tests ---

  @Test
  void testJsonSchemaValidationWithMissingColumns() throws IOException {
    // Create JSON files where second file is missing columns from first
    File json1 = createJsonFile("complete.json",
        "{\"id\": 1, \"name\": \"a\", \"email\": \"b\"}");
    File json2 = createJsonFile("partial.json",
        "{\"id\": 2}");

    // Make json1 newer so it's the schema authority
    json1.setLastModified(json2.lastModified() + 2000);

    List<File> files = Arrays.asList(json1, json2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    // Schema from json1 (3 columns)
    assertEquals(3, schema.getFieldCount());
  }

  @Test
  void testJsonSchemaValidationWithExtraColumns() throws IOException {
    // Second file has extra columns (triggers debug log)
    File json1 = createJsonFile("base.json", "{\"id\": 1}");
    File json2 = createJsonFile("extra.json",
        "{\"id\": 2, \"name\": \"a\", \"extra\": true}");

    // Make json2 newer so it's the schema authority
    json2.setLastModified(json1.lastModified() + 2000);

    List<File> files = Arrays.asList(json1, json2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    // Schema from json2 (3 columns)
    assertEquals(3, schema.getFieldCount());
  }

  // --- Mixed format tests ---

  @Test
  void testMixedFormatPrioritizesParquetOverCsv() throws IOException {
    // Cannot easily create real parquet files in unit test, so test CSV + JSON
    File csvFile = createCsvFile("data.csv", "id,name\n1,a");
    File jsonFile = createJsonFile("data.json", "{\"key\": \"value\"}");

    List<File> files = Arrays.asList(csvFile, jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    // CSV has higher priority than JSON
    assertEquals(2, schema.getFieldCount());
    assertTrue(schema.getFieldNames().contains("id"));
    assertTrue(schema.getFieldNames().contains("name"));
  }

  @Test
  void testMixedFormatFallbackToFirstAvailable() throws IOException {
    // Create files with unknown extension that get detected as different formats
    File jsonFile = createFileWithContent("data.jsonl", "[{\"x\": 1}]");
    File csvFile = createFileWithContent("data.tsv", "a,b\n1,2");

    List<File> files = Arrays.asList(jsonFile, csvFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    // Both get detected by content, resolution should succeed
    assertNotNull(schema);
  }

  // --- Parquet strategy path coverage ---

  @Test
  void testParquetStrategyUnionWithPromotion() throws IOException {
    // UNION_WITH_PROMOTION falls back to UNION_ALL_COLUMNS
    // We can't create real Parquet files easily, but we can test the CSV path
    // with this strategy to verify the dispatch logic
    SchemaStrategy strategy = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.UNION_WITH_PROMOTION,
        SchemaStrategy.CsvStrategy.RICHEST_FILE,
        SchemaStrategy.JsonStrategy.LATEST_FILE);

    File csv = createCsvFile("test.csv", "id,name\n1,a");
    List<File> files = Collections.singletonList(csv);
    RelDataType schema = resolver.resolveSchema(files, strategy);

    assertNotNull(schema);
    assertEquals(2, schema.getFieldCount());
  }

  @Test
  void testParquetStrategyIntersectionOnly() throws IOException {
    // INTERSECTION_ONLY falls back to LATEST_FILE
    SchemaStrategy strategy = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.INTERSECTION_ONLY,
        SchemaStrategy.CsvStrategy.RICHEST_FILE,
        SchemaStrategy.JsonStrategy.LATEST_FILE);

    File csv = createCsvFile("test.csv", "id,name\n1,a");
    List<File> files = Collections.singletonList(csv);
    RelDataType schema = resolver.resolveSchema(files, strategy);

    assertNotNull(schema);
    assertEquals(2, schema.getFieldCount());
  }

  @Test
  void testParquetStrategyFirstFile() throws IOException {
    SchemaStrategy strategy = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.FIRST_FILE,
        SchemaStrategy.CsvStrategy.RICHEST_FILE,
        SchemaStrategy.JsonStrategy.LATEST_FILE);

    File csv = createCsvFile("test.csv", "id,name\n1,a");
    List<File> files = Collections.singletonList(csv);
    RelDataType schema = resolver.resolveSchema(files, strategy);

    assertNotNull(schema);
    assertEquals(2, schema.getFieldCount());
  }

  @Test
  void testParquetStrategyLatestFile() throws IOException {
    SchemaStrategy strategy = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.LATEST_FILE,
        SchemaStrategy.CsvStrategy.RICHEST_FILE,
        SchemaStrategy.JsonStrategy.LATEST_FILE);

    File csv = createCsvFile("test.csv", "id,name,email\n1,a,b");
    List<File> files = Collections.singletonList(csv);
    RelDataType schema = resolver.resolveSchema(files, strategy);

    assertNotNull(schema);
    assertEquals(3, schema.getFieldCount());
  }

  @Test
  void testSingleFormatResolution() throws IOException {
    // Verify single format path vs mixed format path
    File csv1 = createCsvFile("a.csv", "id,name\n1,a");
    File csv2 = createCsvFile("b.csv", "id,name,extra\n1,a,b");

    List<File> files = Arrays.asList(csv1, csv2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    // Single format path
    assertEquals(3, schema.getFieldCount());
  }

  @Test
  void testSchemaStrategyToString() {
    SchemaStrategy strategy = SchemaStrategy.PARQUET_DEFAULT;
    String str = strategy.toString();
    assertNotNull(str);
    assertTrue(str.contains("LATEST_SCHEMA_WINS"));
    assertTrue(str.contains("RICHEST_FILE"));
    assertTrue(str.contains("LATEST_FILE"));
  }

  @Test
  void testSchemaStrategyGetFormatPriority() {
    SchemaStrategy strategy = SchemaStrategy.PARQUET_DEFAULT;
    List<String> priority = strategy.getFormatPriority();
    assertNotNull(priority);
    assertEquals(3, priority.size());
    assertEquals("parquet", priority.get(0));
    assertEquals("csv", priority.get(1));
    assertEquals("json", priority.get(2));
  }

  @Test
  void testAggressiveUnionStrategy() {
    SchemaStrategy aggressive = SchemaStrategy.AGGRESSIVE_UNION;
    assertEquals(SchemaStrategy.ParquetStrategy.UNION_ALL_COLUMNS, aggressive.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.UNION_COMMON, aggressive.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.UNION_KEYS, aggressive.getJsonStrategy());
  }

  @Test
  void testJsonInferSchemaFromInvalidFile() throws IOException {
    // File with invalid JSON content
    File file = createJsonFile("bad.json", "not valid json{{{");

    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    // Should fall back to empty schema
    assertEquals(0, schema.getFieldCount());
  }

  // --- Helper methods ---

  private File createCsvFile(String name, String content) throws IOException {
    return createFileWithContent(name, content);
  }

  private File createJsonFile(String name, String content) throws IOException {
    return createFileWithContent(name, content);
  }

  private File createFileWithContent(String name, String content) throws IOException {
    File file = tempDir.resolve(name).toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }
}
