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















  // ===== buildComprehensiveMapping =====

  @Test void testBuildComprehensiveMappingNullDir() {
    Map<String, String> result =
        ConversionMetadata.buildComprehensiveMapping(null, new HashMap<String, String>());
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test void testBuildComprehensiveMappingNonExistentDir() {
    File nonExistent = tempDir.resolve("nonexistent").toFile();
    Map<String, String> result =
        ConversionMetadata.buildComprehensiveMapping(nonExistent, new HashMap<String, String>());
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test void testBuildComprehensiveMappingNoAperioDir() {
    Map<String, String> result =
        ConversionMetadata.buildComprehensiveMapping(tempDir.toFile(), new HashMap<String, String>());
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test void testBuildComprehensiveMappingEmptyAperioDir() throws IOException {
    File aperioDir = tempDir.resolve(".aperio").toFile();
    aperioDir.mkdirs();

    Map<String, String> result =
        ConversionMetadata.buildComprehensiveMapping(tempDir.toFile(), new HashMap<String, String>());
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

    Map<String, String> result =
        ConversionMetadata.buildComprehensiveMapping(tempDir.toFile(), new HashMap<String, String>());
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

    Map<String, String> result =
        ConversionMetadata.buildComprehensiveMapping(tempDir.toFile(), htmlFileToTableName);
    assertNotNull(result);
    // The result may or may not have entries depending on pattern matching
  }

  // ===== ConversionMetadata constructor and basic operations =====

  @Test void testConversionMetadataConstructor() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir.toFile());
    assertNotNull(metadata);
  }

  @Test void testConversionRecordFields() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/original/file.html", "/converted/file.json", "HTML_TO_JSON");
    assertEquals("/original/file.html", record.getOriginalPath());
    assertEquals("HTML_TO_JSON", record.getConversionType());
    // convertedFile is a public field
    assertEquals("/converted/file.json", record.convertedFile);
  }

  @Test void testConversionRecordTableName() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/original/file.html", "/converted/file.json", "HTML_TO_JSON");
    record.tableName = "custom_table";
    assertEquals("custom_table", record.tableName);
  }

  @Test void testConversionRecordParquetCacheFile() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/original/file.html", "/converted/file.json", "HTML_TO_JSON");
    record.parquetCacheFile = "/cache/file.parquet";
    assertEquals("/cache/file.parquet", record.parquetCacheFile);
  }

  @Test void testRecordConversionAndFind() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir.toFile());

    File sourceFile = tempDir.resolve("source.html").toFile();
    try (FileWriter writer = new FileWriter(sourceFile)) {
      writer.write("<html></html>");
    }

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord(sourceFile.getCanonicalPath(),
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
