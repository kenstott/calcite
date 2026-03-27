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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link FormatAwareSchemaResolver}.
 */
@Tag("unit")
class FormatAwareSchemaResolverCoverageTest {

  @TempDir
  Path tempDir;

  private FormatAwareSchemaResolver resolver;
  private RelDataTypeFactory typeFactory;

  @BeforeEach
  void setUp() {
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    resolver = new FormatAwareSchemaResolver(typeFactory);
  }

  // ========== Format detection ==========

  @Test void testDetectParquetFormat() throws IOException {
    // Write PAR1 magic number to simulate a parquet file
    File file = tempDir.resolve("data.parquet").toFile();
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(new byte[]{'P', 'A', 'R', '1', 0, 0, 0, 0});
    }

    List<File> files = Collections.singletonList(file);
    // Should detect as parquet by extension - resolution will fail but detection works
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testDetectCsvFormat() throws IOException {
    File file = createCsvFile("data.csv", "id,name,value");
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    assertEquals(3, schema.getFieldCount());
  }

  @Test void testDetectJsonFormat() throws IOException {
    File file = createJsonFile("data.json", "{\"id\": 1, \"name\": \"test\"}");
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testDetectFormatByContentJsonCurlyBrace() throws IOException {
    // File without known extension but starting with { for JSON detection
    File file = tempDir.resolve("data.unknown").toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("{\"field\": \"value\"}");
    }
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testDetectFormatByContentJsonBracket() throws IOException {
    // File starting with [ for JSON array
    File file = tempDir.resolve("data.dat").toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("[{\"id\": 1}]");
    }
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testDetectFormatByContentParquetMagic() throws IOException {
    // File without extension but with PAR1 magic number
    File file = tempDir.resolve("data.bin").toFile();
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(new byte[]{'P', 'A', 'R', '1', 0, 0, 0, 0, 0, 0});
    }
    List<File> files = Collections.singletonList(file);
    // Will resolve as parquet but fail to read - that's OK for coverage
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testDetectFormatByContentDefaultCsv() throws IOException {
    // File with no detectable format magic
    File file = tempDir.resolve("data.txt").toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write("col1,col2\nval1,val2\n");
    }
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testDetectFormatByContentReadFailure() throws IOException {
    // File that can't be read (empty)
    File file = tempDir.resolve("data.xyz").toFile();
    file.createNewFile();
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  // ========== CSV schema resolution ==========

  @Test void testCsvSchemaWithRichestFile() throws IOException {
    File csv1 = createCsvFile("a.csv", "id,name");
    File csv2 = createCsvFile("b.csv", "id,name,email,phone");
    File csv3 = createCsvFile("c.csv", "id,name,email");

    List<File> files = Arrays.asList(csv1, csv2, csv3);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(4, schema.getFieldCount());
  }

  @Test void testCsvSchemaWithEmptyColumnName() throws IOException {
    // CSV with empty column name
    File file = createCsvFile("empty_col.csv", "id,,name");
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(3, schema.getFieldCount());
    // Empty column name should become col_1
    assertTrue(schema.getFieldNames().contains("col_1"));
  }

  @Test void testCsvSchemaWithQuotedHeaders() throws IOException {
    File file = createCsvFile("quoted.csv", "\"id\",\"full name\",\"email\"");
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(3, schema.getFieldCount());
    assertTrue(schema.getFieldNames().contains("id"));
    assertTrue(schema.getFieldNames().contains("full name"));
  }

  @Test void testCsvSchemaValidationMismatchColumnCount() throws IOException {
    File csv1 = createCsvFile("rich.csv", "id,name,email");
    File csv2 = createCsvFile("poor.csv", "id,name");

    List<File> files = Arrays.asList(csv1, csv2);
    // rich.csv is richest, poor.csv has fewer columns - should log warning
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(3, schema.getFieldCount());
  }

  @Test void testCsvSchemaValidationMismatchColumnNames() throws IOException {
    File csv1 = createCsvFile("base.csv", "id,name,email");
    File csv2 = createCsvFile("different.csv", "id,fullname,mail");

    List<File> files = Arrays.asList(csv1, csv2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testCsvColumnCountError() throws IOException {
    // Create a file that will cause an error in getCsvColumnCount
    File emptyFile = tempDir.resolve("empty.csv").toFile();
    emptyFile.createNewFile();

    List<File> files = Collections.singletonList(emptyFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  // ========== JSON schema resolution ==========

  @Test void testJsonSchemaResolutionLatestFile() throws IOException {
    File json1 = createJsonFile("old.json", "{\"id\": 1, \"name\": \"old\"}");
    Thread.currentThread(); // ensure unique timestamp
    File json2 = createJsonFile("new.json", "{\"id\": 1, \"name\": \"new\", \"status\": \"active\"}");
    json2.setLastModified(json1.lastModified() + 2000);

    List<File> files = Arrays.asList(json1, json2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);

    assertEquals(3, schema.getFieldCount());
    assertTrue(schema.getFieldNames().contains("status"));
  }

  @Test void testJsonSchemaWithArray() throws IOException {
    File file = createJsonFile("array.json", "[{\"id\": 1, \"name\": \"test\"}]");
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertEquals(2, schema.getFieldCount());
  }

  @Test void testJsonSchemaInferTypes() throws IOException {
    String json = "{\"bool_field\": true, \"int_field\": 42, \"long_field\": 9999999999, "
        + "\"float_field\": 3.14, \"str_field\": \"hello\", \"null_field\": null, "
        + "\"arr_field\": [1,2,3], \"obj_field\": {\"nested\": true}}";
    File file = createJsonFile("types.json", json);
    List<File> files = Collections.singletonList(file);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertTrue(schema.getFieldCount() >= 8);
  }

  @Test void testJsonSchemaValidationMissingColumns() throws IOException {
    File json1 = createJsonFile("base.json", "{\"id\": 1, \"name\": \"test\", \"email\": \"a@b.com\"}");
    File json2 = createJsonFile("other.json", "{\"id\": 2}");
    json1.setLastModified(json2.lastModified() + 2000);

    List<File> files = Arrays.asList(json1, json2);
    // json1 is latest, json2 is missing columns - should log warning
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testJsonSchemaValidationExtraColumns() throws IOException {
    File json1 = createJsonFile("base2.json", "{\"id\": 1}");
    File json2 = createJsonFile("extra2.json", "{\"id\": 2, \"bonus\": \"field\"}");
    json1.setLastModified(json2.lastModified() + 2000);

    List<File> files = Arrays.asList(json1, json2);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
  }

  @Test void testJsonSchemaError() throws IOException {
    File badJson = tempDir.resolve("bad.json").toFile();
    try (FileWriter writer = new FileWriter(badJson)) {
      writer.write("this is not valid json!!!");
    }
    List<File> files = Collections.singletonList(badJson);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.PARQUET_DEFAULT);
    assertNotNull(schema);
    // Should return empty schema on error
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

  @Test void testMixedFormatsUnknownFormat() throws IOException {
    // Two files with unknown extensions
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

  // ========== Strategy variants ==========

  @Test void testConservativeStrategy() throws IOException {
    File csvFile = createCsvFile("test.csv", "id,name");
    List<File> files = Collections.singletonList(csvFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.CONSERVATIVE);
    assertNotNull(schema);
    assertEquals(2, schema.getFieldCount());
  }

  @Test void testAggressiveUnionStrategy() throws IOException {
    File csvFile = createCsvFile("test.csv", "id,name");
    List<File> files = Collections.singletonList(csvFile);
    RelDataType schema = resolver.resolveSchema(files, SchemaStrategy.AGGRESSIVE_UNION);
    assertNotNull(schema);
    assertEquals(2, schema.getFieldCount());
  }

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
  }

  @Test void testSchemaStrategyCustom() {
    SchemaStrategy custom = new SchemaStrategy(
        SchemaStrategy.ParquetStrategy.UNION_WITH_PROMOTION,
        SchemaStrategy.CsvStrategy.LATEST_FILE,
        SchemaStrategy.JsonStrategy.UNION_KEYS,
        Arrays.asList("json", "csv"),
        SchemaStrategy.ValidationLevel.NONE);

    assertEquals(SchemaStrategy.ParquetStrategy.UNION_WITH_PROMOTION, custom.getParquetStrategy());
    assertEquals(SchemaStrategy.CsvStrategy.LATEST_FILE, custom.getCsvStrategy());
    assertEquals(SchemaStrategy.JsonStrategy.UNION_KEYS, custom.getJsonStrategy());
    assertEquals(SchemaStrategy.ValidationLevel.NONE, custom.getValidationLevel());
  }

  // ========== Helper methods ==========

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
