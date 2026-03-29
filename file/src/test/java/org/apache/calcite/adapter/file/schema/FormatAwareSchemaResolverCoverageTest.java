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
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link FormatAwareSchemaResolver}.
 *
 * <p>Tests all methods that can be unit tested without full Calcite infrastructure,
 * using reflection to access private methods for comprehensive coverage.
 */
@Tag("unit")
class FormatAwareSchemaResolverCoverageTest {

  @TempDir
  Path tempDir;

  private FormatAwareSchemaResolver resolver;
  private RelDataTypeFactory typeFactory;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeEach
  void setUp() {
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    resolver = new FormatAwareSchemaResolver(typeFactory);
  }

  // ========== detectFormat tests (via resolveSchema) ==========

  @Test void testDetectFormatParquetExtension() throws IOException {
    File file = tempDir.resolve("data.parquet").toFile();
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(new byte[]{'P', 'A', 'R', '1', 0, 0, 0, 0});
    }
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testDetectFormatCsvExtension() throws IOException {
    File file = createCsvFile("data.csv", "id,name,value");
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(3, schema.getFieldCount());
  }

  @Test void testDetectFormatJsonExtension() throws IOException {
    File file = createJsonFile("data.json", "{\"id\": 1, \"name\": \"test\"}");
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(2, schema.getFieldCount());
  }

  @Test void testDetectFormatParquetUpperCase() throws IOException {
    File file = createCsvFile("data.PARQUET", "a,b");
    // Parquet extension detection is case-insensitive (toLowerCase)
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testDetectFormatCsvUpperCase() throws IOException {
    File file = createCsvFile("data.CSV", "x,y,z");
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(3, schema.getFieldCount());
  }

  @Test void testDetectFormatJsonUpperCase() throws IOException {
    File file = createJsonFile("data.JSON", "{\"a\": 1}");
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testDetectFormatUnknownExtensionDefaultsCsv() throws IOException {
    File file = tempDir.resolve("data.txt").toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("col1,col2\nval1,val2\n");
    }
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(2, schema.getFieldCount());
  }

  @Test void testDetectFormatYamlExtensionFallsToContent() throws IOException {
    // .yaml is not a recognized extension, so content detection kicks in
    File file = tempDir.resolve("data.yaml").toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("key: value\nname: test\n");
    }
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testDetectFormatArrowExtensionFallsToContent() throws IOException {
    File file = tempDir.resolve("data.arrow").toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("some,arrow,data\n1,2,3\n");
    }
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  // ========== detectFormatByContent tests (via reflection) ==========

  @Test void testDetectFormatByContentParquetMagic() throws Exception {
    File file = tempDir.resolve("magic.bin").toFile();
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(new byte[]{'P', 'A', 'R', '1', 0, 0, 0, 0});
    }
    String result = invokeDetectFormatByContent(file);
    assertEquals("parquet", result);
  }

  @Test void testDetectFormatByContentJsonCurlyBrace() throws Exception {
    File file = tempDir.resolve("curly.dat").toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("{\"key\": \"value\"}");
    }
    String result = invokeDetectFormatByContent(file);
    assertEquals("json", result);
  }

  @Test void testDetectFormatByContentJsonSquareBracket() throws Exception {
    File file = tempDir.resolve("bracket.dat").toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("[{\"id\": 1}, {\"id\": 2}]");
    }
    String result = invokeDetectFormatByContent(file);
    assertEquals("json", result);
  }

  @Test void testDetectFormatByContentCsvDefault() throws Exception {
    File file = tempDir.resolve("plain.dat").toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("a,b,c\n1,2,3\n");
    }
    String result = invokeDetectFormatByContent(file);
    assertEquals("csv", result);
  }

  @Test void testDetectFormatByContentEmptyFile() throws Exception {
    File file = tempDir.resolve("empty.dat").toFile();
    file.createNewFile();
    String result = invokeDetectFormatByContent(file);
    // Empty file (fewer than 4 bytes read) defaults to csv
    assertEquals("csv", result);
  }

  @Test void testDetectFormatByContentShortFile() throws Exception {
    File file = tempDir.resolve("short.dat").toFile();
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(new byte[]{'a', 'b'});
    }
    String result = invokeDetectFormatByContent(file);
    assertEquals("csv", result);
  }

  @Test void testDetectFormatByContentNonexistentFile() throws Exception {
    File file = tempDir.resolve("nonexistent.dat").toFile();
    // File does not exist, IOException triggers csv default
    String result = invokeDetectFormatByContent(file);
    assertEquals("csv", result);
  }

  @Test void testDetectFormatByContentBinaryData() throws Exception {
    File file = tempDir.resolve("binary.dat").toFile();
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(new byte[]{0x00, 0x01, 0x02, 0x03, 0x04});
    }
    String result = invokeDetectFormatByContent(file);
    assertEquals("csv", result);
  }

  // ========== getCsvColumnCount tests ==========

  @Test void testGetCsvColumnCountSingleColumn() throws Exception {
    File file = createCsvFile("single.csv", "id");
    int count = invokeGetCsvColumnCount(file);
    assertEquals(1, count);
  }

  @Test void testGetCsvColumnCountMultipleColumns() throws Exception {
    File file = createCsvFile("multi.csv", "id,name,email,phone");
    int count = invokeGetCsvColumnCount(file);
    assertEquals(4, count);
  }

  @Test void testGetCsvColumnCountEmptyFile() throws Exception {
    File file = tempDir.resolve("empty.csv").toFile();
    file.createNewFile();
    int count = invokeGetCsvColumnCount(file);
    assertEquals(0, count);
  }

  @Test void testGetCsvColumnCountWithSpaces() throws Exception {
    File file = createCsvFile("spaces.csv", "id , name , email");
    int count = invokeGetCsvColumnCount(file);
    assertEquals(3, count);
  }

  @Test void testGetCsvColumnCountWithQuotedCommas() throws Exception {
    // Simple split won't handle quoted commas perfectly, but tests the basic behavior
    File file = createCsvFile("quoted.csv", "id,\"name,with,commas\",email");
    int count = invokeGetCsvColumnCount(file);
    // Simple comma split gives 5: id, "name, with, commas", email (does not understand CSV quoting)
    assertEquals(5, count);
  }

  @Test void testGetCsvColumnCountNonexistentFile() throws Exception {
    File file = tempDir.resolve("nonexistent.csv").toFile();
    int count = invokeGetCsvColumnCount(file);
    assertEquals(0, count);
  }

  // ========== inferCsvSchema tests ==========

  @Test void testInferCsvSchemaSimpleHeaders() throws Exception {
    File file = createCsvFile("simple.csv", "id,name,value");
    RelDataType schema = invokeInferCsvSchema(file);
    assertEquals(3, schema.getFieldCount());
    assertEquals("id", schema.getFieldList().get(0).getName());
    assertEquals("name", schema.getFieldList().get(1).getName());
    assertEquals("value", schema.getFieldList().get(2).getName());
    // All should be VARCHAR
    assertEquals(SqlTypeName.VARCHAR,
        schema.getFieldList().get(0).getType().getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR,
        schema.getFieldList().get(1).getType().getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR,
        schema.getFieldList().get(2).getType().getSqlTypeName());
  }

  @Test void testInferCsvSchemaEmptyColumnName() throws Exception {
    File file = createCsvFile("emptycol.csv", "id,,name");
    RelDataType schema = invokeInferCsvSchema(file);
    assertEquals(3, schema.getFieldCount());
    assertEquals("col_1", schema.getFieldList().get(1).getName());
  }

  @Test void testInferCsvSchemaQuotedHeaders() throws Exception {
    File file = createCsvFile("quotedh.csv", "\"id\",\"full name\",\"email\"");
    RelDataType schema = invokeInferCsvSchema(file);
    assertEquals(3, schema.getFieldCount());
    assertEquals("id", schema.getFieldList().get(0).getName());
    assertEquals("full name", schema.getFieldList().get(1).getName());
    assertEquals("email", schema.getFieldList().get(2).getName());
  }

  @Test void testInferCsvSchemaEmptyFile() throws Exception {
    File file = tempDir.resolve("empty_schema.csv").toFile();
    file.createNewFile();
    RelDataType schema = invokeInferCsvSchema(file);
    assertEquals(0, schema.getFieldCount());
  }

  @Test void testInferCsvSchemaAllEmptyColumnNames() throws Exception {
    // ",," splits into ["", "", ""] with split(",", -1), giving 3 empty-named columns
    File file = createCsvFile("allempty.csv", ",,");
    RelDataType schema = invokeInferCsvSchema(file);
    assertEquals(3, schema.getFieldCount());
    assertEquals("col_0", schema.getFieldList().get(0).getName());
    assertEquals("col_1", schema.getFieldList().get(1).getName());
    assertEquals("col_2", schema.getFieldList().get(2).getName());
  }

  @Test void testInferCsvSchemaLeadingEmptyColumnName() throws Exception {
    // ",b,c" splits into ["", "b", "c"] - first element is empty, gets default name
    File file = createCsvFile("leading_empty.csv", ",b,c");
    RelDataType schema = invokeInferCsvSchema(file);
    assertEquals(3, schema.getFieldCount());
    assertEquals("col_0", schema.getFieldList().get(0).getName());
    assertEquals("b", schema.getFieldList().get(1).getName());
    assertEquals("c", schema.getFieldList().get(2).getName());
  }

  @Test void testInferCsvSchemaSingleColumn() throws Exception {
    File file = createCsvFile("onecol.csv", "only_column");
    RelDataType schema = invokeInferCsvSchema(file);
    assertEquals(1, schema.getFieldCount());
    assertEquals("only_column", schema.getFieldList().get(0).getName());
  }

  @Test void testInferCsvSchemaWhitespaceHeaders() throws Exception {
    File file = createCsvFile("ws.csv", " id , name , email ");
    RelDataType schema = invokeInferCsvSchema(file);
    assertEquals(3, schema.getFieldCount());
    assertEquals("id", schema.getFieldList().get(0).getName());
    assertEquals("name", schema.getFieldList().get(1).getName());
    assertEquals("email", schema.getFieldList().get(2).getName());
  }

  // ========== inferJsonSchema tests ==========

  @Test void testInferJsonSchemaObjectWithFields() throws Exception {
    File file = createJsonFile("obj.json",
        "{\"id\": 1, \"name\": \"test\", \"active\": true}");
    RelDataType schema = invokeInferJsonSchema(file);
    assertEquals(3, schema.getFieldCount());
    assertTrue(schema.getFieldNames().contains("id"));
    assertTrue(schema.getFieldNames().contains("name"));
    assertTrue(schema.getFieldNames().contains("active"));
  }

  @Test void testInferJsonSchemaArrayOfObjects() throws Exception {
    File file = createJsonFile("arr.json",
        "[{\"id\": 1, \"name\": \"Alice\"}, {\"id\": 2, \"name\": \"Bob\"}]");
    RelDataType schema = invokeInferJsonSchema(file);
    // Uses first element as schema template
    assertEquals(2, schema.getFieldCount());
    assertTrue(schema.getFieldNames().contains("id"));
    assertTrue(schema.getFieldNames().contains("name"));
  }

  @Test void testInferJsonSchemaEmptyArray() throws Exception {
    File file = createJsonFile("emptyarr.json", "[]");
    RelDataType schema = invokeInferJsonSchema(file);
    // Empty array: no fields
    assertEquals(0, schema.getFieldCount());
  }

  @Test void testInferJsonSchemaScalarValue() throws Exception {
    File file = createJsonFile("scalar.json", "\"just a string\"");
    RelDataType schema = invokeInferJsonSchema(file);
    // Scalar is not an object; schema should be empty
    assertEquals(0, schema.getFieldCount());
  }

  @Test void testInferJsonSchemaInvalidJson() throws Exception {
    File file = tempDir.resolve("invalid.json").toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("not valid json at all {{{}}}");
    }
    RelDataType schema = invokeInferJsonSchema(file);
    // Error should return empty schema
    assertEquals(0, schema.getFieldCount());
  }

  @Test void testInferJsonSchemaNestedObject() throws Exception {
    File file = createJsonFile("nested.json",
        "{\"id\": 1, \"address\": {\"city\": \"NYC\", \"zip\": \"10001\"}}");
    RelDataType schema = invokeInferJsonSchema(file);
    assertEquals(2, schema.getFieldCount());
  }

  @Test void testInferJsonSchemaMixedTypes() throws Exception {
    String json = "{\"bool_field\": true, \"int_field\": 42, \"long_field\": 9999999999, "
        + "\"double_field\": 3.14, \"str_field\": \"hello\", \"null_field\": null, "
        + "\"arr_field\": [1,2,3], \"obj_field\": {\"nested\": true}}";
    File file = createJsonFile("mixed.json", json);
    RelDataType schema = invokeInferJsonSchema(file);
    assertEquals(8, schema.getFieldCount());
  }

  // ========== inferJsonFieldType tests ==========

  @Test void testInferJsonFieldTypeNull() throws Exception {
    RelDataType type = invokeInferJsonFieldType(NullNode.getInstance());
    assertEquals(SqlTypeName.VARCHAR, type.getSqlTypeName());
  }

  @Test void testInferJsonFieldTypeBoolean() throws Exception {
    RelDataType type = invokeInferJsonFieldType(BooleanNode.TRUE);
    assertEquals(SqlTypeName.BOOLEAN, type.getSqlTypeName());
  }

  @Test void testInferJsonFieldTypeBooleanFalse() throws Exception {
    RelDataType type = invokeInferJsonFieldType(BooleanNode.FALSE);
    assertEquals(SqlTypeName.BOOLEAN, type.getSqlTypeName());
  }

  @Test void testInferJsonFieldTypeInt() throws Exception {
    RelDataType type = invokeInferJsonFieldType(IntNode.valueOf(42));
    assertEquals(SqlTypeName.INTEGER, type.getSqlTypeName());
  }

  @Test void testInferJsonFieldTypeIntZero() throws Exception {
    RelDataType type = invokeInferJsonFieldType(IntNode.valueOf(0));
    assertEquals(SqlTypeName.INTEGER, type.getSqlTypeName());
  }

  @Test void testInferJsonFieldTypeIntNegative() throws Exception {
    RelDataType type = invokeInferJsonFieldType(IntNode.valueOf(-100));
    assertEquals(SqlTypeName.INTEGER, type.getSqlTypeName());
  }

  @Test void testInferJsonFieldTypeLong() throws Exception {
    RelDataType type = invokeInferJsonFieldType(LongNode.valueOf(9999999999L));
    assertEquals(SqlTypeName.BIGINT, type.getSqlTypeName());
  }

  @Test void testInferJsonFieldTypeLongNegative() throws Exception {
    RelDataType type = invokeInferJsonFieldType(LongNode.valueOf(-9999999999L));
    assertEquals(SqlTypeName.BIGINT, type.getSqlTypeName());
  }

  @Test void testInferJsonFieldTypeDouble() throws Exception {
    RelDataType type = invokeInferJsonFieldType(DoubleNode.valueOf(3.14));
    assertEquals(SqlTypeName.DOUBLE, type.getSqlTypeName());
  }

  @Test void testInferJsonFieldTypeFloat() throws Exception {
    RelDataType type = invokeInferJsonFieldType(FloatNode.valueOf(1.5f));
    assertEquals(SqlTypeName.DOUBLE, type.getSqlTypeName());
  }

  @Test void testInferJsonFieldTypeString() throws Exception {
    RelDataType type = invokeInferJsonFieldType(TextNode.valueOf("hello"));
    assertEquals(SqlTypeName.VARCHAR, type.getSqlTypeName());
  }

  @Test void testInferJsonFieldTypeEmptyString() throws Exception {
    RelDataType type = invokeInferJsonFieldType(TextNode.valueOf(""));
    assertEquals(SqlTypeName.VARCHAR, type.getSqlTypeName());
  }

  @Test void testInferJsonFieldTypeArray() throws Exception {
    ArrayNode arrayNode = MAPPER.createArrayNode();
    arrayNode.add(1);
    arrayNode.add(2);
    arrayNode.add(3);
    RelDataType type = invokeInferJsonFieldType(arrayNode);
    // Arrays become ARRAY types
    assertTrue(type.getSqlTypeName() == SqlTypeName.ARRAY
        || type.toString().contains("ARRAY"),
        "Expected ARRAY type but got: " + type);
  }

  @Test void testInferJsonFieldTypeEmptyArray() throws Exception {
    ArrayNode arrayNode = MAPPER.createArrayNode();
    RelDataType type = invokeInferJsonFieldType(arrayNode);
    assertTrue(type.getSqlTypeName() == SqlTypeName.ARRAY
        || type.toString().contains("ARRAY"),
        "Expected ARRAY type but got: " + type);
  }

  @Test void testInferJsonFieldTypeObject() throws Exception {
    ObjectNode objectNode = MAPPER.createObjectNode();
    objectNode.put("key", "value");
    RelDataType type = invokeInferJsonFieldType(objectNode);
    assertEquals(SqlTypeName.VARCHAR, type.getSqlTypeName());
  }

  @Test void testInferJsonFieldTypeEmptyObject() throws Exception {
    ObjectNode objectNode = MAPPER.createObjectNode();
    RelDataType type = invokeInferJsonFieldType(objectNode);
    assertEquals(SqlTypeName.VARCHAR, type.getSqlTypeName());
  }

  // ========== resolveSchema edge cases ==========

  @Test void testResolveSchemaEmptyFileList() {
    List<File> files = Collections.emptyList();
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(0, schema.getFieldCount());
  }

  @Test void testResolveSchemaConservativeStrategy() throws IOException {
    File csvFile = createCsvFile("test.csv", "id,name");
    List<File> files = Collections.singletonList(csvFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.CONSERVATIVE);
    assertNotNull(schema);
    assertEquals(2, schema.getFieldCount());
  }

  @Test void testResolveSchemaAggressiveUnionStrategy() throws IOException {
    File csvFile = createCsvFile("test.csv", "id,name");
    List<File> files = Collections.singletonList(csvFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.AGGRESSIVE_UNION);
    assertNotNull(schema);
    assertEquals(2, schema.getFieldCount());
  }

  // ========== CSV schema resolution via resolveSchema ==========

  @Test void testCsvSchemaRichestFile() throws IOException {
    File csv1 = createCsvFile("a.csv", "id,name");
    File csv2 = createCsvFile("b.csv", "id,name,email,phone");
    File csv3 = createCsvFile("c.csv", "id,name,email");

    List<File> files = Arrays.asList(csv1, csv2, csv3);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(4, schema.getFieldCount());
  }

  @Test void testCsvSchemaColumnNameValidation() throws IOException {
    File csv1 = createCsvFile("base.csv", "id,name,email");
    File csv2 = createCsvFile("different.csv", "id,fullname,mail");

    List<File> files = Arrays.asList(csv1, csv2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(3, schema.getFieldCount());
  }

  @Test void testCsvSchemaEmptyColumnNames() throws IOException {
    File file = createCsvFile("empty_col.csv", "id,,name");
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(3, schema.getFieldCount());
    assertTrue(schema.getFieldNames().contains("col_1"));
  }

  // ========== JSON schema resolution via resolveSchema ==========

  @Test void testJsonSchemaLatestFile() throws IOException {
    File json1 = createJsonFile("old.json", "{\"id\": 1, \"name\": \"old\"}");
    File json2 = createJsonFile("new.json",
        "{\"id\": 1, \"name\": \"new\", \"status\": \"active\"}");
    json2.setLastModified(json1.lastModified() + 2000);

    List<File> files = Arrays.asList(json1, json2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(3, schema.getFieldCount());
    assertTrue(schema.getFieldNames().contains("status"));
  }

  @Test void testJsonSchemaWithArrayFile() throws IOException {
    File file = createJsonFile("array.json", "[{\"id\": 1, \"name\": \"test\"}]");
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(2, schema.getFieldCount());
  }

  @Test void testJsonSchemaValidationMissingColumns() throws IOException {
    File json1 = createJsonFile("base.json",
        "{\"id\": 1, \"name\": \"test\", \"email\": \"a@b.com\"}");
    File json2 = createJsonFile("other.json", "{\"id\": 2}");
    json1.setLastModified(json2.lastModified() + 2000);

    List<File> files = Arrays.asList(json1, json2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    // json1 is latest, has 3 fields
    assertEquals(3, schema.getFieldCount());
  }

  @Test void testJsonSchemaValidationExtraColumns() throws IOException {
    File json1 = createJsonFile("base2.json", "{\"id\": 1}");
    File json2 = createJsonFile("extra2.json", "{\"id\": 2, \"bonus\": \"field\"}");
    json1.setLastModified(json2.lastModified() + 2000);

    List<File> files = Arrays.asList(json1, json2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    // json1 is latest, has 1 field
    assertEquals(1, schema.getFieldCount());
  }

  @Test void testJsonSchemaParseError() throws IOException {
    File badJson = tempDir.resolve("bad.json").toFile();
    try (FileWriter writer = new FileWriter(badJson)) {
      writer.write("this is not valid json!!!");
    }
    List<File> files = Collections.singletonList(badJson);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(0, schema.getFieldCount());
  }

  // ========== Mixed format resolution ==========

  @Test void testMixedFormatsCsvAndJson() throws IOException {
    File csvFile = createCsvFile("data.csv", "id,name,amount");
    File jsonFile = createJsonFile("data.json", "{\"id\": 1, \"title\": \"test\"}");

    List<File> files = Arrays.asList(csvFile, jsonFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    // CSV has higher priority than JSON
    assertEquals(3, schema.getFieldCount());
  }

  @Test void testMixedFormatsJsonAndUnknown() throws IOException {
    File jsonFile = createJsonFile("data.json", "{\"a\": 1, \"b\": 2}");
    File unknownFile = tempDir.resolve("data.xyz").toFile();
    try (FileWriter writer = new FileWriter(unknownFile)) {
      writer.write("x,y\n1,2\n");
    }

    List<File> files = Arrays.asList(jsonFile, unknownFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testMixedFormatsAllUnknown() throws IOException {
    File file1 = tempDir.resolve("data1.xyz").toFile();
    try (FileWriter writer = new FileWriter(file1)) {
      writer.write("a,b,c\n1,2,3\n");
    }
    File file2 = tempDir.resolve("data2.abc").toFile();
    try (FileWriter writer = new FileWriter(file2)) {
      writer.write("x,y\n4,5\n");
    }

    List<File> files = Arrays.asList(file1, file2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  // ========== SchemaStrategy tests ==========

  @Test void testSchemaStrategyToString() {
    String str = SchemaStrategy.PARQUET_DEFAULT.toString();
    assertNotNull(str);
    assertTrue(str.contains("SchemaStrategy"));
    assertTrue(str.contains("LATEST_SCHEMA_WINS"));
  }

  @Test void testSchemaStrategyFormatPriority() {
    List<String> priority = SchemaStrategy.PARQUET_DEFAULT.getFormatPriority();
    assertNotNull(priority);
    assertEquals(3, priority.size());
    assertEquals("parquet", priority.get(0));
    assertEquals("csv", priority.get(1));
    assertEquals("json", priority.get(2));
  }

  @Test void testSchemaStrategyGetters() {
    assertEquals(SchemaStrategy.ParquetStrategy.LATEST_SCHEMA_WINS,
        SchemaStrategy.PARQUET_DEFAULT.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.RICHEST_FILE,
        SchemaStrategy.PARQUET_DEFAULT.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.LATEST_FILE,
        SchemaStrategy.PARQUET_DEFAULT.getJsonStrategy());
    assertEquals(SchemaStrategy.ValidationLevel.WARN,
        SchemaStrategy.PARQUET_DEFAULT.getValidationLevel());
  }

  @Test void testSchemaStrategyCustom() {
    SchemaStrategy custom = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.UNION_WITH_PROMOTION,
        SchemaStrategy.CsvStrategy.LATEST_FILE,
        SchemaStrategy.JsonStrategy.UNION_KEYS,
        Arrays.asList("json", "csv"),
        SchemaStrategy.ValidationLevel.NONE);

    assertEquals(SchemaStrategy.ParquetStrategy.UNION_WITH_PROMOTION,
        custom.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.LATEST_FILE, custom.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.UNION_KEYS, custom.getJsonStrategy());
    assertEquals(SchemaStrategy.ValidationLevel.NONE, custom.getValidationLevel());
    assertEquals(2, custom.getFormatPriority().size());
  }

  @Test void testSchemaStrategyConservative() {
    assertEquals(SchemaStrategy.ParquetStrategy.LATEST_FILE,
        SchemaStrategy.CONSERVATIVE.getParquetStrategy());
  }

  @Test void testSchemaStrategyAggressiveUnion() {
    assertEquals(SchemaStrategy.ParquetStrategy.UNION_ALL_COLUMNS,
        SchemaStrategy.AGGRESSIVE_UNION.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.UNION_COMMON,
        SchemaStrategy.AGGRESSIVE_UNION.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.UNION_KEYS,
        SchemaStrategy.AGGRESSIVE_UNION.getJsonStrategy());
  }

  @Test void testSchemaStrategyValidationLevelError() {
    SchemaStrategy custom = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.FIRST_FILE,
        SchemaStrategy.CsvStrategy.USER_DEFINED,
        SchemaStrategy.JsonStrategy.FIRST_FILE,
        Arrays.asList("parquet"),
        SchemaStrategy.ValidationLevel.ERROR);
    assertEquals(SchemaStrategy.ValidationLevel.ERROR, custom.getValidationLevel());
  }

  @Test void testSchemaStrategyAllParquetEnumValues() {
    // Ensure all enum values exist and are accessible
    SchemaStrategy.ParquetStrategy[] values = SchemaStrategy.ParquetStrategy.values();
    assertTrue(values.length >= 6);
  }

  @Test void testSchemaStrategyAllCsvEnumValues() {
    SchemaStrategy.CsvStrategy[] values = SchemaStrategy.CsvStrategy.values();
    assertTrue(values.length >= 4);
  }

  @Test void testSchemaStrategyAllJsonEnumValues() {
    SchemaStrategy.JsonStrategy[] values = SchemaStrategy.JsonStrategy.values();
    assertTrue(values.length >= 4);
  }

  @Test void testSchemaStrategyAllValidationLevelValues() {
    SchemaStrategy.ValidationLevel[] values = SchemaStrategy.ValidationLevel.values();
    assertEquals(3, values.length);
  }

  // ========== Reflection helper methods ==========

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
    return (int) method.invoke(resolver, file);
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

  private RelDataType invokeInferJsonFieldType(JsonNode node) throws Exception {
    Method method = FormatAwareSchemaResolver.class.getDeclaredMethod(
        "inferJsonFieldType", JsonNode.class);
    method.setAccessible(true);
    return (RelDataType) method.invoke(resolver, node);
  }

  // ========== File creation helpers ==========

  private File createCsvFile(String name, String header) throws IOException {
    File file = tempDir.resolve(name).toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(header + "\n");
      writer.write("1,test,value\n");
    }
    return file;
  }

  private File createJsonFile(String name, String content) throws IOException {
    File file = tempDir.resolve(name).toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }
}
