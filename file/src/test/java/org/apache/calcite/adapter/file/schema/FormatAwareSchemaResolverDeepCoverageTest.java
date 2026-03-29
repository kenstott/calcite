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
package org.apache.calcite.adapter.file.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link FormatAwareSchemaResolver}.
 *
 * <p>Focuses on format detection (extension and content-based), CSV schema inference,
 * JSON schema inference, mixed format resolution, validation logic, and edge cases.
 */
@Tag("unit")
class FormatAwareSchemaResolverDeepCoverageTest {

  @TempDir
  Path tempDir;

  private RelDataTypeFactory typeFactory;
  private FormatAwareSchemaResolver resolver;

  @BeforeEach
  void setUp() {
    typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    resolver = new FormatAwareSchemaResolver(typeFactory);
  }

  // ====================================================================
  // resolveSchema: empty list
  // ====================================================================

  @Test
  void testResolveSchemaEmptyList() {
    RelDataType result = resolver.resolveSchema(
        Collections.<File>emptyList(), SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(result);
    assertEquals(0, result.getFieldCount());
  }

  // ====================================================================
  // detectFormat: extension-based detection
  // ====================================================================

  @Test
  void testDetectFormatParquetExtension() throws Exception {
    File parquetFile = tempDir.resolve("data.parquet").toFile();
    Files.write(parquetFile.toPath(), new byte[]{0, 0, 0, 0});
    String format = invokeDetectFormat(parquetFile);
    assertEquals("parquet", format);
  }

  @Test
  void testDetectFormatCsvExtension() throws Exception {
    File csvFile = tempDir.resolve("data.csv").toFile();
    Files.write(csvFile.toPath(), "a,b,c\n1,2,3\n".getBytes());
    String format = invokeDetectFormat(csvFile);
    assertEquals("csv", format);
  }

  @Test
  void testDetectFormatJsonExtension() throws Exception {
    File jsonFile = tempDir.resolve("data.json").toFile();
    Files.write(jsonFile.toPath(), "{\"a\":1}".getBytes());
    String format = invokeDetectFormat(jsonFile);
    assertEquals("json", format);
  }

  @Test
  void testDetectFormatCsvExtensionUpperCase() throws Exception {
    File csvFile = tempDir.resolve("DATA.CSV").toFile();
    Files.write(csvFile.toPath(), "a,b\n1,2\n".getBytes());
    String format = invokeDetectFormat(csvFile);
    assertEquals("csv", format);
  }

  @Test
  void testDetectFormatParquetExtensionUpperCase() throws Exception {
    File parquetFile = tempDir.resolve("DATA.PARQUET").toFile();
    Files.write(parquetFile.toPath(), new byte[]{0, 0, 0, 0});
    String format = invokeDetectFormat(parquetFile);
    assertEquals("parquet", format);
  }

  // ====================================================================
  // detectFormatByContent: content-based detection
  // ====================================================================

  @Test
  void testDetectFormatByContentParquetMagic() throws Exception {
    // PAR1 magic bytes
    File file = tempDir.resolve("unknown_parquet").toFile();
    byte[] magic = new byte[]{'P', 'A', 'R', '1'};
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(magic);
    }
    String format = invokeDetectFormatByContent(file);
    assertEquals("parquet", format);
  }

  @Test
  void testDetectFormatByContentJsonObject() throws Exception {
    File file = tempDir.resolve("unknown_json").toFile();
    Files.write(file.toPath(), "{\"key\": \"value\"}".getBytes());
    String format = invokeDetectFormatByContent(file);
    assertEquals("json", format);
  }

  @Test
  void testDetectFormatByContentJsonArray() throws Exception {
    File file = tempDir.resolve("unknown_json_array").toFile();
    Files.write(file.toPath(), "[{\"a\":1}]".getBytes());
    String format = invokeDetectFormatByContent(file);
    assertEquals("json", format);
  }

  @Test
  void testDetectFormatByContentDefaultsCsv() throws Exception {
    File file = tempDir.resolve("unknown_csv").toFile();
    Files.write(file.toPath(), "name,age\nJohn,30\n".getBytes());
    String format = invokeDetectFormatByContent(file);
    assertEquals("csv", format);
  }

  @Test
  void testDetectFormatByContentShortFile() throws Exception {
    // Less than 4 bytes
    File file = tempDir.resolve("tiny").toFile();
    Files.write(file.toPath(), "ab".getBytes());
    String format = invokeDetectFormatByContent(file);
    assertEquals("csv", format);
  }

  @Test
  void testDetectFormatByContentEmptyFile() throws Exception {
    File file = tempDir.resolve("empty").toFile();
    Files.write(file.toPath(), new byte[0]);
    String format = invokeDetectFormatByContent(file);
    assertEquals("csv", format);
  }

  @Test
  void testDetectFormatByContentNonExistentFile() throws Exception {
    File file = tempDir.resolve("does_not_exist").toFile();
    // Should not throw, returns "csv" on IOException
    String format = invokeDetectFormatByContent(file);
    assertEquals("csv", format);
  }

  // ====================================================================
  // resolveSchema: single CSV format
  // ====================================================================

  @Test
  void testResolveSchemaSingleCsvFile() throws Exception {
    File csvFile = createCsvFile("single.csv", "name,age,salary\nAlice,30,50000\n");
    List<File> files = Collections.singletonList(csvFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(3, schema.getFieldCount());
    assertEquals("name", schema.getFieldList().get(0).getName());
    assertEquals("age", schema.getFieldList().get(1).getName());
    assertEquals("salary", schema.getFieldList().get(2).getName());
  }

  @Test
  void testResolveSchemaCsvWithQuotedHeaders() throws Exception {
    File csvFile = createCsvFile("quoted.csv", "\"Name\",\"Age\",\"Salary\"\nAlice,30,50000\n");
    List<File> files = Collections.singletonList(csvFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(3, schema.getFieldCount());
    assertEquals("Name", schema.getFieldList().get(0).getName());
  }

  @Test
  void testResolveSchemaCsvMultipleFilesRichestWins() throws Exception {
    // File with 2 columns
    File csv1 = createCsvFile("small.csv", "a,b\n1,2\n");
    // File with 4 columns (richest)
    File csv2 = createCsvFile("rich.csv", "a,b,c,d\n1,2,3,4\n");
    // File with 3 columns
    File csv3 = createCsvFile("medium.csv", "a,b,c\n1,2,3\n");

    List<File> files = Arrays.asList(csv1, csv2, csv3);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(4, schema.getFieldCount());
  }

  @Test
  void testResolveSchemaCsvEmptyColumns() throws Exception {
    // File with empty column names should use fallback naming
    File csvFile = createCsvFile("empty_cols.csv", ",col2,\n1,2,3\n");
    List<File> files = Collections.singletonList(csvFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(3, schema.getFieldCount());
    // Empty column names should be replaced with col_0, col_2
    assertEquals("col_0", schema.getFieldList().get(0).getName());
    assertEquals("col2", schema.getFieldList().get(1).getName());
    assertEquals("col_2", schema.getFieldList().get(2).getName());
  }

  @Test
  void testResolveSchemaCsvEmptyFile() throws Exception {
    File csvFile = createCsvFile("empty.csv", "");
    List<File> files = Collections.singletonList(csvFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(0, schema.getFieldCount());
  }

  // ====================================================================
  // resolveSchema: single JSON format
  // ====================================================================

  @Test
  void testResolveSchemaJsonObject() throws Exception {
    File jsonFile = createJsonFile("obj.json",
        "{\"name\": \"Alice\", \"age\": 30, \"active\": true}");
    List<File> files = Collections.singletonList(jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(3, schema.getFieldCount());
  }

  @Test
  void testResolveSchemaJsonArray() throws Exception {
    File jsonFile = createJsonFile("arr.json",
        "[{\"name\": \"Alice\", \"age\": 30}, {\"name\": \"Bob\", \"age\": 25}]");
    List<File> files = Collections.singletonList(jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(2, schema.getFieldCount());
  }

  @Test
  void testResolveSchemaJsonMultipleFiles() throws Exception {
    File json1 = createJsonFile("data1.json",
        "{\"name\": \"Alice\", \"age\": 30}");
    File json2 = createJsonFile("data2.json",
        "{\"name\": \"Bob\", \"age\": 25, \"city\": \"NYC\"}");
    // Make json2 newer
    json2.setLastModified(json1.lastModified() + 5000);

    List<File> files = Arrays.asList(json1, json2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    // Latest file (json2) should be used as schema authority
    assertEquals(3, schema.getFieldCount());
  }

  @Test
  void testResolveSchemaJsonEmptyObject() throws Exception {
    File jsonFile = createJsonFile("empty_obj.json", "{}");
    List<File> files = Collections.singletonList(jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(0, schema.getFieldCount());
  }

  @Test
  void testResolveSchemaJsonEmptyArray() throws Exception {
    File jsonFile = createJsonFile("empty_arr.json", "[]");
    List<File> files = Collections.singletonList(jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(0, schema.getFieldCount());
  }

  @Test
  void testResolveSchemaJsonInvalidSyntax() throws Exception {
    File jsonFile = createJsonFile("invalid.json", "not json at all");
    List<File> files = Collections.singletonList(jsonFile);
    // Should not throw, returns empty schema on error
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(0, schema.getFieldCount());
  }

  // ====================================================================
  // inferJsonFieldType: comprehensive type detection
  // ====================================================================

  @Test
  void testJsonFieldTypeNull() throws Exception {
    File jsonFile = createJsonFile("null_val.json", "{\"field\": null}");
    List<File> files = Collections.singletonList(jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(SqlTypeName.VARCHAR, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testJsonFieldTypeBoolean() throws Exception {
    File jsonFile = createJsonFile("bool_val.json", "{\"field\": true}");
    List<File> files = Collections.singletonList(jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(SqlTypeName.BOOLEAN, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testJsonFieldTypeInteger() throws Exception {
    File jsonFile = createJsonFile("int_val.json", "{\"field\": 42}");
    List<File> files = Collections.singletonList(jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(SqlTypeName.INTEGER, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testJsonFieldTypeLong() throws Exception {
    // Long value exceeding int range
    File jsonFile = createJsonFile("long_val.json", "{\"field\": 9999999999}");
    List<File> files = Collections.singletonList(jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(SqlTypeName.BIGINT, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testJsonFieldTypeDouble() throws Exception {
    File jsonFile = createJsonFile("dbl_val.json", "{\"field\": 3.14}");
    List<File> files = Collections.singletonList(jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(SqlTypeName.DOUBLE, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testJsonFieldTypeString() throws Exception {
    File jsonFile = createJsonFile("str_val.json", "{\"field\": \"hello\"}");
    List<File> files = Collections.singletonList(jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(SqlTypeName.VARCHAR, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testJsonFieldTypeArray() throws Exception {
    File jsonFile = createJsonFile("arr_val.json", "{\"field\": [1, 2, 3]}");
    List<File> files = Collections.singletonList(jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    // Arrays should be mapped to ARRAY type
    RelDataType fieldType = schema.getFieldList().get(0).getType();
    assertNotNull(fieldType);
    assertTrue(fieldType.getSqlTypeName() == SqlTypeName.ARRAY
        || fieldType.toString().contains("ARRAY"));
  }

  @Test
  void testJsonFieldTypeNestedObject() throws Exception {
    File jsonFile = createJsonFile("nested_obj.json",
        "{\"field\": {\"inner\": \"value\"}}");
    List<File> files = Collections.singletonList(jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    // Nested objects should be serialized as VARCHAR
    assertEquals(SqlTypeName.VARCHAR, schema.getFieldList().get(0).getType().getSqlTypeName());
  }

  // ====================================================================
  // resolveSchema: mixed format (priority-based)
  // ====================================================================

  @Test
  void testResolveSchemaMixedFormatsParquetCsv() throws Exception {
    // Parquet has higher priority than CSV
    File csvFile = createCsvFile("data.csv", "a,b,c\n1,2,3\n");
    // Create a "parquet" file by extension (won't be valid for schema read)
    // The resolver will try parquet first due to priority, fail, and return empty
    // But the detection logic routes it to parquet format resolution
    File fakeParquet = tempDir.resolve("data.parquet").toFile();
    Files.write(fakeParquet.toPath(), "not real parquet".getBytes());

    List<File> files = Arrays.asList(csvFile, fakeParquet);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    // Parquet resolution will fail, so returns empty schema
  }

  @Test
  void testResolveSchemaMixedCsvAndJson() throws Exception {
    File csvFile = createCsvFile("data.csv", "a,b,c\n1,2,3\n");
    File jsonFile = createJsonFile("data.json", "{\"x\": 1, \"y\": 2}");

    List<File> files = Arrays.asList(csvFile, jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    // CSV has higher priority than JSON, so CSV schema used
    assertEquals(3, schema.getFieldCount());
  }

  // ====================================================================
  // getCsvColumnCount edge cases
  // ====================================================================

  @Test
  void testGetCsvColumnCountNormal() throws Exception {
    File csvFile = createCsvFile("count.csv", "a,b,c,d\n1,2,3,4\n");
    int count = invokeGetCsvColumnCount(csvFile);
    assertEquals(4, count);
  }

  @Test
  void testGetCsvColumnCountEmptyFile() throws Exception {
    File csvFile = createCsvFile("empty_count.csv", "");
    int count = invokeGetCsvColumnCount(csvFile);
    assertEquals(0, count);
  }

  @Test
  void testGetCsvColumnCountSingleColumn() throws Exception {
    File csvFile = createCsvFile("single_col.csv", "name\nAlice\nBob\n");
    int count = invokeGetCsvColumnCount(csvFile);
    assertEquals(1, count);
  }

  @Test
  void testGetCsvColumnCountNonExistent() throws Exception {
    File csvFile = tempDir.resolve("nonexistent.csv").toFile();
    int count = invokeGetCsvColumnCount(csvFile);
    assertEquals(0, count);
  }

  // ====================================================================
  // inferCsvSchema edge cases
  // ====================================================================

  @Test
  void testInferCsvSchemaAllVarchar() throws Exception {
    File csvFile = createCsvFile("types.csv", "name,city,country\n");
    RelDataType schema = invokeInferCsvSchema(csvFile);
    assertNotNull(schema);
    assertEquals(3, schema.getFieldCount());
    for (int i = 0; i < 3; i++) {
      assertEquals(SqlTypeName.VARCHAR, schema.getFieldList().get(i).getType().getSqlTypeName());
    }
  }

  @Test
  void testInferCsvSchemaNonExistentFile() throws Exception {
    File csvFile = tempDir.resolve("no_such.csv").toFile();
    RelDataType schema = invokeInferCsvSchema(csvFile);
    assertNotNull(schema);
    assertEquals(0, schema.getFieldCount());
  }

  // ====================================================================
  // validateCsvCompatibility
  // ====================================================================

  @Test
  void testValidateCsvCompatibilityMismatchedColumnCount() throws Exception {
    File csv1 = createCsvFile("base.csv", "a,b,c\n1,2,3\n");
    File csv2 = createCsvFile("different.csv", "a,b\n1,2\n");
    RelDataType baseSchema = invokeInferCsvSchema(csv1);
    // Should not throw - just logs warnings
    invokeValidateCsvCompatibility(csv2, baseSchema);
  }

  @Test
  void testValidateCsvCompatibilityColumnNameMismatch() throws Exception {
    File csv1 = createCsvFile("base_names.csv", "x,y,z\n1,2,3\n");
    File csv2 = createCsvFile("diff_names.csv", "a,b,c\n1,2,3\n");
    RelDataType baseSchema = invokeInferCsvSchema(csv1);
    // Should not throw - just logs warnings about name mismatches
    invokeValidateCsvCompatibility(csv2, baseSchema);
  }

  @Test
  void testValidateCsvCompatibilityNonExistentFile() throws Exception {
    File csv1 = createCsvFile("base2.csv", "a,b\n1,2\n");
    File csv2 = tempDir.resolve("nonexistent_compat.csv").toFile();
    RelDataType baseSchema = invokeInferCsvSchema(csv1);
    // Should not throw on error - handles exception gracefully
    invokeValidateCsvCompatibility(csv2, baseSchema);
  }

  // ====================================================================
  // validateJsonCompatibility
  // ====================================================================

  @Test
  void testValidateJsonCompatibilityMissingColumns() throws Exception {
    File json1 = createJsonFile("base.json", "{\"a\": 1, \"b\": 2, \"c\": 3}");
    File json2 = createJsonFile("partial.json", "{\"a\": 1}");
    RelDataType baseSchema = invokeInferJsonSchema(json1);
    // Should not throw - logs warnings about missing columns
    invokeValidateJsonCompatibility(json2, baseSchema);
  }

  @Test
  void testValidateJsonCompatibilityExtraColumns() throws Exception {
    File json1 = createJsonFile("base2.json", "{\"a\": 1}");
    File json2 = createJsonFile("extra.json", "{\"a\": 1, \"b\": 2, \"c\": 3}");
    RelDataType baseSchema = invokeInferJsonSchema(json1);
    // Should not throw - logs debug about extra columns
    invokeValidateJsonCompatibility(json2, baseSchema);
  }

  @Test
  void testValidateJsonCompatibilityInvalidFile() throws Exception {
    File json1 = createJsonFile("valid_base.json", "{\"a\": 1}");
    File json2 = createJsonFile("invalid_compat.json", "not json");
    RelDataType baseSchema = invokeInferJsonSchema(json1);
    // Should not throw - handles error gracefully
    invokeValidateJsonCompatibility(json2, baseSchema);
  }

  // ====================================================================
  // resolveSchemaForFormat: unknown format
  // ====================================================================

  @Test
  void testResolveSchemaForUnknownFormat() throws Exception {
    // Create a file with an unknown extension
    File file = createCsvFile("data.xyz", "a,b\n1,2\n");
    // Force it through by using reflection on the detectFormat
    // The file will be content-detected as CSV by detectFormatByContent
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    // Unknown extension, but content detection returns "csv"
    assertEquals(2, schema.getFieldCount());
  }

  // ====================================================================
  // resolveGenericSchema
  // ====================================================================

  @Test
  void testResolveGenericSchema() throws Exception {
    File csvFile = createCsvFile("generic.csv", "x,y\n1,2\n");
    Method method = FormatAwareSchemaResolver.class.getDeclaredMethod(
        "resolveGenericSchema", List.class);
    method.setAccessible(true);
    @SuppressWarnings("unchecked")
    RelDataType result = (RelDataType) method.invoke(resolver,
        Collections.singletonList(csvFile));
    assertNotNull(result);
    assertEquals(2, result.getFieldCount());
  }

  // ====================================================================
  // JSON schema with multiple types in one object
  // ====================================================================

  @Test
  void testResolveSchemaJsonMixedTypes() throws Exception {
    File jsonFile = createJsonFile("mixed_types.json",
        "{\"name\": \"Alice\", \"age\": 30, \"score\": 95.5, \"active\": true, \"data\": null}");
    List<File> files = Collections.singletonList(jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(5, schema.getFieldCount());
    assertEquals(SqlTypeName.VARCHAR, schema.getFieldList().get(0).getType().getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, schema.getFieldList().get(1).getType().getSqlTypeName());
    assertEquals(SqlTypeName.DOUBLE, schema.getFieldList().get(2).getType().getSqlTypeName());
    assertEquals(SqlTypeName.BOOLEAN, schema.getFieldList().get(3).getType().getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, schema.getFieldList().get(4).getType().getSqlTypeName());
  }

  // ====================================================================
  // CSV with multiple files compatibility validation
  // ====================================================================

  @Test
  void testResolveSchemaCsvValidatesCompatibility() throws Exception {
    // Base file with 3 columns (richest)
    File csv1 = createCsvFile("compat1.csv", "a,b,c\n1,2,3\n");
    // File with different column names
    File csv2 = createCsvFile("compat2.csv", "x,y\n1,2\n");
    List<File> files = Arrays.asList(csv1, csv2);
    // Should resolve without throwing, validating compatibility for csv2
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(3, schema.getFieldCount());
  }

  // ====================================================================
  // JSON with multiple files compatibility validation
  // ====================================================================

  @Test
  void testResolveSchemaJsonValidatesCompatibility() throws Exception {
    File json1 = createJsonFile("jsoncompat1.json", "{\"a\": 1, \"b\": 2}");
    File json2 = createJsonFile("jsoncompat2.json", "{\"a\": 1, \"c\": 3}");
    // Make json1 newer
    json1.setLastModified(json2.lastModified() + 5000);

    List<File> files = Arrays.asList(json1, json2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    // Latest file (json1) is the schema authority
    assertEquals(2, schema.getFieldCount());
  }

  // ====================================================================
  // SchemaStrategy: Parquet strategies fall through gracefully
  // ====================================================================

  @Test
  void testResolveSchemaParquetUnionWithPromotionFallback() throws Exception {
    // UNION_WITH_PROMOTION falls back to UNION_ALL_COLUMNS which requires real parquet files
    // Test with fake parquet file that will fail to read - returns empty schema
    File fakeParquet = tempDir.resolve("promo.parquet").toFile();
    Files.write(fakeParquet.toPath(), "not real".getBytes());
    List<File> files = Collections.singletonList(fakeParquet);

    SchemaStrategy promoStrategy = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.UNION_WITH_PROMOTION,
        SchemaStrategy.CsvStrategy.RICHEST_FILE,
        SchemaStrategy.JsonStrategy.LATEST_FILE);

    RelDataType schema = resolver.resolveSchema(files, promoStrategy);
    assertNotNull(schema);
  }

  @Test
  void testResolveSchemaParquetIntersectionOnlyFallback() throws Exception {
    File fakeParquet = tempDir.resolve("intersect.parquet").toFile();
    Files.write(fakeParquet.toPath(), "not real".getBytes());
    List<File> files = Collections.singletonList(fakeParquet);

    SchemaStrategy intersectStrategy = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.INTERSECTION_ONLY,
        SchemaStrategy.CsvStrategy.RICHEST_FILE,
        SchemaStrategy.JsonStrategy.LATEST_FILE);

    RelDataType schema = resolver.resolveSchema(files, intersectStrategy);
    assertNotNull(schema);
  }

  @Test
  void testResolveSchemaParquetFirstFileFallback() throws Exception {
    File fakeParquet = tempDir.resolve("first.parquet").toFile();
    Files.write(fakeParquet.toPath(), "not real".getBytes());
    List<File> files = Collections.singletonList(fakeParquet);

    SchemaStrategy firstFileStrategy = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.FIRST_FILE,
        SchemaStrategy.CsvStrategy.RICHEST_FILE,
        SchemaStrategy.JsonStrategy.LATEST_FILE);

    RelDataType schema = resolver.resolveSchema(files, firstFileStrategy);
    assertNotNull(schema);
  }

  @Test
  void testResolveSchemaParquetLatestFileFallback() throws Exception {
    File fakeParquet = tempDir.resolve("latest.parquet").toFile();
    Files.write(fakeParquet.toPath(), "not real".getBytes());
    List<File> files = Collections.singletonList(fakeParquet);

    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.CONSERVATIVE);
    assertNotNull(schema);
  }

  // ====================================================================
  // Mixed format fallback when none of priority formats present
  // ====================================================================

  @Test
  void testResolveMixedFormatFallback() throws Exception {
    // Only create files with unknown extension that will be detected as CSV
    File file1 = tempDir.resolve("data1.dat").toFile();
    Files.write(file1.toPath(), "a,b\n1,2\n".getBytes());
    File file2 = tempDir.resolve("data2.json").toFile();
    Files.write(file2.toPath(), "{\"x\": 1}".getBytes());

    List<File> files = Arrays.asList(file1, file2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  // ====================================================================
  // Helper methods
  // ====================================================================

  private File createCsvFile(String name, String content) throws IOException {
    File file = tempDir.resolve(name).toFile();
    try (FileWriter fw = new FileWriter(file)) {
      fw.write(content);
    }
    return file;
  }

  private File createJsonFile(String name, String content) throws IOException {
    File file = tempDir.resolve(name).toFile();
    try (FileWriter fw = new FileWriter(file)) {
      fw.write(content);
    }
    return file;
  }

  private String invokeDetectFormat(File file) throws Exception {
    Method method = FormatAwareSchemaResolver.class.getDeclaredMethod("detectFormat", File.class);
    method.setAccessible(true);
    return (String) method.invoke(resolver, file);
  }

  private String invokeDetectFormatByContent(File file) throws Exception {
    Method method = FormatAwareSchemaResolver.class.getDeclaredMethod(
        "detectFormatByContent", File.class);
    method.setAccessible(true);
    return (String) method.invoke(resolver, file);
  }

  private int invokeGetCsvColumnCount(File file) throws Exception {
    Method method = FormatAwareSchemaResolver.class.getDeclaredMethod(
        "getCsvColumnCount", File.class);
    method.setAccessible(true);
    return (Integer) method.invoke(resolver, file);
  }

  private RelDataType invokeInferCsvSchema(File file) throws Exception {
    Method method = FormatAwareSchemaResolver.class.getDeclaredMethod(
        "inferCsvSchema", File.class);
    method.setAccessible(true);
    return (RelDataType) method.invoke(resolver, file);
  }

  private RelDataType invokeInferJsonSchema(File file) throws Exception {
    Method method = FormatAwareSchemaResolver.class.getDeclaredMethod(
        "inferJsonSchema", File.class);
    method.setAccessible(true);
    return (RelDataType) method.invoke(resolver, file);
  }

  private void invokeValidateCsvCompatibility(File file, RelDataType schema) throws Exception {
    Method method = FormatAwareSchemaResolver.class.getDeclaredMethod(
        "validateCsvCompatibility", File.class, RelDataType.class);
    method.setAccessible(true);
    method.invoke(resolver, file, schema);
  }

  private void invokeValidateJsonCompatibility(File file, RelDataType schema) throws Exception {
    Method method = FormatAwareSchemaResolver.class.getDeclaredMethod(
        "validateJsonCompatibility", File.class, RelDataType.class);
    method.setAccessible(true);
    method.invoke(resolver, file, schema);
  }
}
