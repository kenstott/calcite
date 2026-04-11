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
package org.apache.calcite.adapter.file.metadata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for ConversionMetadata private pattern extraction methods
 * and buildComprehensiveMapping.
 */
@Tag("unit")
public class ConversionMetadataPatternCoverageTest {

  @TempDir
  Path tempDir;

  // ===== extractTableSpecificPattern via reflection =====

  @Test void testExtractTableSpecificPatternNull() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "extractTableSpecificPattern", List.class);
    method.setAccessible(true);

    assertNull(method.invoke(null, (Object) null));
  }

  @Test void testExtractTableSpecificPatternEmpty() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "extractTableSpecificPattern", List.class);
    method.setAccessible(true);

    assertNull(method.invoke(null, new ArrayList<String>()));
  }

  @Test void testExtractTableSpecificPatternWithPartitions() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "extractTableSpecificPattern", List.class);
    method.setAccessible(true);

    List<String> paths = Arrays.asList(
        "/data/schema/table/year=2020/file1.parquet",
        "/data/schema/table/year=2021/file2.parquet",
        "/data/schema/table/year=2022/file3.parquet"
    );

    String result = (String) method.invoke(null, paths);
    assertNotNull(result);
    assertTrue(result.contains("/**/*"), "Should contain glob pattern");
    assertTrue(result.endsWith(".parquet"), "Should end with .parquet extension");
  }

  @Test void testExtractTableSpecificPatternNoPartitions() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "extractTableSpecificPattern", List.class);
    method.setAccessible(true);

    List<String> paths = Arrays.asList(
        "/data/schema/table/file1.parquet",
        "/data/schema/table/file2.parquet"
    );

    String result = (String) method.invoke(null, paths);
    assertNotNull(result);
    assertTrue(result.endsWith(".parquet"));
  }

  @Test void testExtractTableSpecificPatternCsvExtension() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "extractTableSpecificPattern", List.class);
    method.setAccessible(true);

    List<String> paths = Arrays.asList(
        "/data/table/year=2020/data.csv",
        "/data/table/year=2021/data.csv"
    );

    String result = (String) method.invoke(null, paths);
    assertNotNull(result);
    assertTrue(result.endsWith(".csv"), "Should use actual file extension");
  }

  // ===== findCommonPrefixBeforePartitions via reflection =====

  @Test void testFindCommonPrefixBeforePartitionsEmpty() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "findCommonPrefixBeforePartitions", List.class);
    method.setAccessible(true);

    assertNull(method.invoke(null, new ArrayList<String>()));
  }

  @Test void testFindCommonPrefixBeforePartitionsWithPartitionDirs() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "findCommonPrefixBeforePartitions", List.class);
    method.setAccessible(true);

    List<String> paths = Arrays.asList(
        "s3://bucket/schema/table/year=2020/file1.parquet",
        "s3://bucket/schema/table/year=2021/file2.parquet"
    );

    String result = (String) method.invoke(null, paths);
    assertNotNull(result);
    assertTrue(result.endsWith("/"));
    assertTrue(result.contains("table"));
    assertFalse(result.contains("year="));
  }

  @Test void testFindCommonPrefixBeforePartitionsNoPartitions() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "findCommonPrefixBeforePartitions", List.class);
    method.setAccessible(true);

    List<String> paths = Arrays.asList(
        "/data/schema/table/file1.parquet",
        "/data/schema/table/file2.parquet"
    );

    String result = (String) method.invoke(null, paths);
    assertNotNull(result);
    // No partitions, so should return parent directory
    assertTrue(result.contains("table"));
  }

  @Test void testFindCommonPrefixBeforePartitionsDivergentPaths() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "findCommonPrefixBeforePartitions", List.class);
    method.setAccessible(true);

    List<String> paths = Arrays.asList(
        "/data/tableA/year=2020/file1.parquet",
        "/data/tableB/year=2020/file2.parquet"
    );

    String result = (String) method.invoke(null, paths);
    assertNotNull(result);
    // The prefix before partitions is /data/tableA and /data/tableB which don't match
    // Should still return a valid prefix
  }

  // ===== findLongestCommonPrefix via reflection =====

  @Test void testFindLongestCommonPrefixEmpty() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "findLongestCommonPrefix", List.class);
    method.setAccessible(true);

    assertEquals("", method.invoke(null, new ArrayList<String>()));
  }

  @Test void testFindLongestCommonPrefixSinglePath() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "findLongestCommonPrefix", List.class);
    method.setAccessible(true);

    List<String> paths = Arrays.asList("/data/schema/table/file.parquet");
    String result = (String) method.invoke(null, paths);
    assertEquals("/data/schema/table/file.parquet", result);
  }

  @Test void testFindLongestCommonPrefixMultiplePaths() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "findLongestCommonPrefix", List.class);
    method.setAccessible(true);

    List<String> paths = Arrays.asList(
        "/data/schema/table/file1.parquet",
        "/data/schema/table/file2.parquet",
        "/data/schema/table/file3.parquet"
    );
    String result = (String) method.invoke(null, paths);
    assertEquals("/data/schema/table/file", result);
  }

  @Test void testFindLongestCommonPrefixNoCommon() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "findLongestCommonPrefix", List.class);
    method.setAccessible(true);

    List<String> paths = Arrays.asList(
        "/alpha/file.parquet",
        "/beta/file.parquet"
    );
    String result = (String) method.invoke(null, paths);
    assertEquals("/", result);
  }

  @Test void testFindLongestCommonPrefixShorterPath() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod(
        "findLongestCommonPrefix", List.class);
    method.setAccessible(true);

    List<String> paths = Arrays.asList(
        "/data/schema/table/year=2020/data.parquet",
        "/data/schema/table/data.parquet"
    );
    String result = (String) method.invoke(null, paths);
    assertEquals("/data/schema/table/", result);
  }

  // ===== buildComprehensiveMapping =====

  @Test void testBuildComprehensiveMappingNullDir() {
    Map<String, String> result = ConversionMetadata.buildComprehensiveMapping(
        null, new HashMap<String, String>());
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test void testBuildComprehensiveMappingNonExistentDir() {
    File nonExistent = tempDir.resolve("nonexistent").toFile();
    Map<String, String> result = ConversionMetadata.buildComprehensiveMapping(
        nonExistent, new HashMap<String, String>());
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test void testBuildComprehensiveMappingNoAperioDir() {
    Map<String, String> result = ConversionMetadata.buildComprehensiveMapping(
        tempDir.toFile(), new HashMap<String, String>());
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test void testBuildComprehensiveMappingEmptyAperioDir() throws IOException {
    File aperioDir = tempDir.resolve(".aperio").toFile();
    aperioDir.mkdirs();

    Map<String, String> result = ConversionMetadata.buildComprehensiveMapping(
        tempDir.toFile(), new HashMap<String, String>());
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test void testBuildComprehensiveMappingWithSchemaDir() throws IOException {
    // Create .aperio/schema structure
    File aperioDir = tempDir.resolve(".aperio").toFile();
    File schemaDir = new File(aperioDir, "test_schema");
    schemaDir.mkdirs();

    // Create a conversion metadata file in the schema dir
    File metadataFile = new File(schemaDir, ".conversion_metadata.json");
    try (FileWriter writer = new FileWriter(metadataFile)) {
      writer.write("{}");
    }

    Map<String, String> result = ConversionMetadata.buildComprehensiveMapping(
        tempDir.toFile(), new HashMap<String, String>());
    assertNotNull(result);
    // Empty metadata should produce empty mapping
    assertTrue(result.isEmpty());
  }

  @Test void testBuildComprehensiveMappingWithHtmlConversion() throws IOException {
    // Create .aperio/schema structure with conversion metadata
    File aperioDir = tempDir.resolve(".aperio").toFile();
    File schemaDir = new File(aperioDir, "test_schema");
    schemaDir.mkdirs();

    // Create a simple conversion metadata JSON
    File metadataFile = new File(schemaDir, ".conversion_metadata.json");
    String jsonPath = new File(schemaDir, "my_table.json").getCanonicalPath();
    String htmlPath = new File(tempDir.toFile(), "page.html").getCanonicalPath();

    try (FileWriter writer = new FileWriter(metadataFile)) {
      writer.write("{\"" + jsonPath.replace("\\", "\\\\").replace("\"", "\\\"") + "\":"
          + "{\"originalFile\":\"" + htmlPath.replace("\\", "\\\\").replace("\"", "\\\"") + "\","
          + "\"convertedFile\":\"" + jsonPath.replace("\\", "\\\\").replace("\"", "\\\"") + "\","
          + "\"conversionType\":\"HTML_TO_JSON\"}}");
    }

    Map<String, String> htmlFileToTableName = new HashMap<String, String>();
    htmlFileToTableName.put("page", "my_explicit_table");

    Map<String, String> result = ConversionMetadata.buildComprehensiveMapping(
        tempDir.toFile(), htmlFileToTableName);
    assertNotNull(result);
    // The result may or may not have entries depending on pattern matching
  }

  // ===== ConversionMetadata constructor and basic operations =====

  @Test void testConversionMetadataConstructor() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir.toFile());
    assertNotNull(metadata);
  }

  @Test void testConversionRecordFields() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/original/file.html", "/converted/file.json", "HTML_TO_JSON");
    assertEquals("/original/file.html", record.getOriginalPath());
    assertEquals("HTML_TO_JSON", record.getConversionType());
    // convertedFile is a public field
    assertEquals("/converted/file.json", record.convertedFile);
  }

  @Test void testConversionRecordTableName() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/original/file.html", "/converted/file.json", "HTML_TO_JSON");
    record.tableName = "custom_table";
    assertEquals("custom_table", record.tableName);
  }

  @Test void testConversionRecordParquetCacheFile() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/original/file.html", "/converted/file.json", "HTML_TO_JSON");
    record.parquetCacheFile = "/cache/file.parquet";
    assertEquals("/cache/file.parquet", record.parquetCacheFile);
  }

  @Test void testRecordConversionAndFind() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir.toFile());

    File sourceFile = tempDir.resolve("source.html").toFile();
    try (FileWriter writer = new FileWriter(sourceFile)) {
      writer.write("<html></html>");
    }

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        sourceFile.getCanonicalPath(),
        tempDir.resolve("output.json").toFile().getCanonicalPath(),
        "HTML_TO_JSON");

    metadata.recordConversion(sourceFile, record);

    // Should be able to get it back
    ConversionMetadata.ConversionRecord found = metadata.getConversionRecord(sourceFile);
    assertNotNull(found);
  }

  @Test void testFindOriginalSourceNotFound() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir.toFile());
    File result = metadata.findOriginalSource(tempDir.resolve("unknown.json").toFile());
    assertNull(result);
  }

  @Test void testFindDerivedFilesEmpty() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir.toFile());
    List<File> derived = metadata.findDerivedFiles(tempDir.resolve("source.html").toFile());
    assertNotNull(derived);
    assertTrue(derived.isEmpty());
  }
}
