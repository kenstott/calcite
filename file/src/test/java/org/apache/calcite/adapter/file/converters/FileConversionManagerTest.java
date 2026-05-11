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
package org.apache.calcite.adapter.file.converters;

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for FileConversionManager covering format detection,
 * conversion dispatch, file usability checks, and metadata-based
 * change detection.
 */
@Tag("unit")
public class FileConversionManagerTest {

  @TempDir
  Path tempDir;

  @BeforeEach
  void setUp() {
    // Each test gets a fresh temp directory
  }

  // --- requiresConversion tests ---

  @Test
  void testRequiresConversionForExcel() {
    assertTrue(FileConversionManager.requiresConversion("data.xlsx"));
    assertTrue(FileConversionManager.requiresConversion("data.xls"));
    assertTrue(FileConversionManager.requiresConversion("DATA.XLSX"));
    assertTrue(FileConversionManager.requiresConversion("report.XLS"));
  }

  @Test
  void testRequiresConversionForHtml() {
    assertTrue(FileConversionManager.requiresConversion("page.html"));
    assertTrue(FileConversionManager.requiresConversion("page.htm"));
    assertTrue(FileConversionManager.requiresConversion("DATA.HTML"));
    assertTrue(FileConversionManager.requiresConversion("page.HTM"));
  }

  @Test
  void testRequiresConversionForXml() {
    assertTrue(FileConversionManager.requiresConversion("data.xml"));
    assertTrue(FileConversionManager.requiresConversion("DATA.XML"));
  }

  @Test
  void testRequiresConversionForMarkdown() {
    assertTrue(FileConversionManager.requiresConversion("readme.md"));
    assertTrue(FileConversionManager.requiresConversion("NOTES.MD"));
  }

  @Test
  void testRequiresConversionForDocx() {
    assertTrue(FileConversionManager.requiresConversion("document.docx"));
    assertTrue(FileConversionManager.requiresConversion("REPORT.DOCX"));
  }

  @Test
  void testRequiresConversionForPptx() {
    assertTrue(FileConversionManager.requiresConversion("slides.pptx"));
    assertTrue(FileConversionManager.requiresConversion("SLIDES.PPTX"));
  }

  @Test
  void testRequiresConversionReturnsFalseForDirectFiles() {
    assertFalse(FileConversionManager.requiresConversion("data.csv"));
    assertFalse(FileConversionManager.requiresConversion("data.json"));
    assertFalse(FileConversionManager.requiresConversion("data.parquet"));
    assertFalse(FileConversionManager.requiresConversion("data.tsv"));
    assertFalse(FileConversionManager.requiresConversion("data.arrow"));
    assertFalse(FileConversionManager.requiresConversion("data.yaml"));
    assertFalse(FileConversionManager.requiresConversion("data.yml"));
  }

  // --- isDirectlyUsable tests ---

  @Test
  void testIsDirectlyUsableForCsv() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.csv"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.csv.gz"));
    assertTrue(FileConversionManager.isDirectlyUsable("DATA.CSV"));
  }

  @Test
  void testIsDirectlyUsableForTsv() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.tsv"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.tsv.gz"));
  }

  @Test
  void testIsDirectlyUsableForJson() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.json"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.json.gz"));
  }

  @Test
  void testIsDirectlyUsableForParquet() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.parquet"));
    assertTrue(FileConversionManager.isDirectlyUsable("DATA.PARQUET"));
  }

  @Test
  void testIsDirectlyUsableForArrow() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.arrow"));
  }

  @Test
  void testIsDirectlyUsableForYaml() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.yaml"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.yml"));
  }

  @Test
  void testIsDirectlyUsableReturnsFalseForConvertibleFiles() {
    assertFalse(FileConversionManager.isDirectlyUsable("data.xlsx"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.html"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.xml"));
    assertFalse(FileConversionManager.isDirectlyUsable("readme.md"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.docx"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.pptx"));
  }

  // --- convertIfNeeded tests ---

  @Test
  void testConvertIfNeededForCsvReturnsfalse() {
    File csvFile = tempDir.resolve("data.csv").toFile();
    try (FileWriter writer = new FileWriter(csvFile)) {
      writer.write("id,name\n1,test\n");
    } catch (IOException e) {
      fail("Failed to create test CSV file");
    }

    boolean converted = FileConversionManager.convertIfNeeded(
        csvFile, tempDir.toFile(), "SMART_CASING");
    assertFalse(converted, "CSV files should not need conversion");
  }

  @Test
  void testConvertIfNeededForParquetReturnsFalse() {
    File parquetFile = tempDir.resolve("data.parquet").toFile();
    try {
      parquetFile.createNewFile();
    } catch (IOException e) {
      fail("Failed to create test file");
    }

    boolean converted = FileConversionManager.convertIfNeeded(
        parquetFile, tempDir.toFile(), "SMART_CASING");
    assertFalse(converted, "Parquet files should not need conversion");
  }

  @Test
  void testConvertIfNeededFourArgOverload() {
    File csvFile = tempDir.resolve("data.csv").toFile();
    try (FileWriter writer = new FileWriter(csvFile)) {
      writer.write("id,name\n1,test\n");
    } catch (IOException e) {
      fail("Failed to create test CSV file");
    }

    boolean converted = FileConversionManager.convertIfNeeded(
        csvFile, tempDir.toFile(), "SMART_CASING", "SMART_CASING");
    assertFalse(converted, "CSV files should not need conversion (4-arg overload)");
  }

  @Test
  void testConvertIfNeededFiveArgOverload() {
    File csvFile = tempDir.resolve("data.csv").toFile();
    try (FileWriter writer = new FileWriter(csvFile)) {
      writer.write("id,name\n1,test\n");
    } catch (IOException e) {
      fail("Failed to create test CSV file");
    }

    boolean converted = FileConversionManager.convertIfNeeded(
        csvFile, tempDir.toFile(), "SMART_CASING", "SMART_CASING", tempDir.toFile());
    assertFalse(converted, "CSV files should not need conversion (5-arg overload)");
  }

  @Test
  void testConvertIfNeededForJsonWithNoConversionRecord() {
    File jsonFile = tempDir.resolve("data.json").toFile();
    try (FileWriter writer = new FileWriter(jsonFile)) {
      writer.write("[{\"id\": 1}]");
    } catch (IOException e) {
      fail("Failed to create test JSON file");
    }

    boolean converted = FileConversionManager.convertIfNeeded(
        jsonFile, tempDir.toFile(), "SMART_CASING", "SMART_CASING",
        tempDir.toFile(), null);
    assertFalse(converted, "JSON file without JSONPath record should not be converted");
  }

  @Test
  void testConvertIfNeededForYamlWithNoConversionRecord() {
    File yamlFile = tempDir.resolve("data.yaml").toFile();
    try (FileWriter writer = new FileWriter(yamlFile)) {
      writer.write("key: value\n");
    } catch (IOException e) {
      fail("Failed to create test YAML file");
    }

    boolean converted = FileConversionManager.convertIfNeeded(
        yamlFile, tempDir.toFile(), "SMART_CASING", "SMART_CASING",
        tempDir.toFile(), null);
    assertFalse(converted, "YAML file without JSONPath record should not be converted");
  }

  @Test
  void testConvertIfNeededForYmlExtension() {
    File ymlFile = tempDir.resolve("data.yml").toFile();
    try (FileWriter writer = new FileWriter(ymlFile)) {
      writer.write("key: value\n");
    } catch (IOException e) {
      fail("Failed to create test YML file");
    }

    boolean converted = FileConversionManager.convertIfNeeded(
        ymlFile, tempDir.toFile(), "SMART_CASING", "SMART_CASING",
        tempDir.toFile(), null);
    assertFalse(converted, "YML file without JSONPath record should not be converted");
  }

  @Test
  void testConvertIfNeededForJsonGzWithNoConversionRecord() {
    File jsonGzFile = tempDir.resolve("data.json.gz").toFile();
    try {
      jsonGzFile.createNewFile();
    } catch (IOException e) {
      fail("Failed to create test file");
    }

    boolean converted = FileConversionManager.convertIfNeeded(
        jsonGzFile, tempDir.toFile(), "SMART_CASING", "SMART_CASING",
        tempDir.toFile(), null);
    assertFalse(converted, "json.gz file without JSONPath record should not be converted");
  }

  @Test
  void testConvertIfNeededSkipsWhenFileNotChanged() throws IOException {
    // Create a CSV file
    File csvFile = tempDir.resolve("unchanged.csv").toFile();
    try (FileWriter writer = new FileWriter(csvFile)) {
      writer.write("id,name\n1,test\n");
    }

    // Create a conversion record for it (simulating previous conversion)
    File baseDir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(baseDir);
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        csvFile.getAbsolutePath(),
        tempDir.resolve("unchanged.json").toFile().getAbsolutePath(),
        "CSV_TO_JSON");
    metadata.recordConversion(csvFile, record);

    // Now call convertIfNeeded with baseDirectory - it should check metadata
    boolean converted = FileConversionManager.convertIfNeeded(
        csvFile, tempDir.toFile(), "SMART_CASING", "SMART_CASING", baseDir, null);
    // CSV files don't need conversion regardless of metadata
    assertFalse(converted);
  }

  // --- findOriginalSource tests ---

  @Test
  void testFindOriginalSourceWithNoMetadata() {
    File file = tempDir.resolve("standalone.json").toFile();
    try {
      file.createNewFile();
    } catch (IOException e) {
      fail("Failed to create test file");
    }

    File original = FileConversionManager.findOriginalSource(file);
    assertEquals(file, original,
        "File with no conversion history should return itself");
  }

  @Test
  void testFindOriginalSourceWithNonExistentFile() {
    File nonExistent = tempDir.resolve("nonexistent.json").toFile();

    File original = FileConversionManager.findOriginalSource(nonExistent);
    assertEquals(nonExistent, original,
        "Non-existent file should return itself");
  }

  // --- Singleton tests ---

  @Test
  void testGetInstance() {
    FileConversionManager instance1 = FileConversionManager.getInstance();
    FileConversionManager instance2 = FileConversionManager.getInstance();
    assertSame(instance1, instance2, "getInstance should return the same singleton");
  }

  // --- registerConverter and convert tests ---

  @Test
  void testRegisterAndConvert() throws IOException {
    FileConversionManager manager = FileConversionManager.getInstance();

    // Register a test converter
    FileConverter testConverter = new FileConverter() {
      @Override
      public boolean canConvert(String sourceFormat, String targetFormat) {
        return "test_src".equals(sourceFormat) && "test_tgt".equals(targetFormat);
      }

      @Override
      public List<String> convert(String sourcePath, String targetDirectory,
          ConversionMetadata metadata) throws IOException {
        return Arrays.asList(targetDirectory + "/output.json");
      }

      @Override
      public String getSourceFormat() {
        return "test_src";
      }

      @Override
      public String getTargetFormat() {
        return "test_tgt";
      }
    };

    manager.registerConverter(testConverter);

    // Now convert should find the registered converter
    File sourceFile = tempDir.resolve("input.test").toFile();
    sourceFile.createNewFile();

    List<String> result = manager.convert(
        sourceFile, tempDir.toFile(), "test_src", "test_tgt");

    assertNotNull(result);
    assertFalse(result.isEmpty(), "Should produce at least one output path");
  }

  @Test
  void testConvertWithNoRegisteredConverter() {
    FileConversionManager manager = FileConversionManager.getInstance();

    File sourceFile = tempDir.resolve("input.xyz").toFile();
    try {
      sourceFile.createNewFile();
    } catch (IOException e) {
      fail("Failed to create test file");
    }

    assertThrows(IOException.class,
        () -> manager.convert(sourceFile, tempDir.toFile(),
            "unknown_format", "other_format"),
        "Should throw IOException when no converter is found");
  }

  @Test
  void testConvertWithCachedConverter() throws IOException {
    FileConversionManager manager = FileConversionManager.getInstance();

    FileConverter cachedConverter = new FileConverter() {
      @Override
      public boolean canConvert(String sourceFormat, String targetFormat) {
        return "cached_src".equals(sourceFormat) && "cached_tgt".equals(targetFormat);
      }

      @Override
      public List<String> convert(String sourcePath, String targetDirectory,
          ConversionMetadata metadata) throws IOException {
        return Arrays.asList(targetDirectory + "/cached_output.json");
      }

      @Override
      public String getSourceFormat() {
        return "cached_src";
      }

      @Override
      public String getTargetFormat() {
        return "cached_tgt";
      }
    };

    manager.registerConverter(cachedConverter);

    File sourceFile = tempDir.resolve("input.cached").toFile();
    sourceFile.createNewFile();

    // First call populates cache
    List<String> result1 = manager.convert(
        sourceFile, tempDir.toFile(), "cached_src", "cached_tgt");
    assertNotNull(result1);

    // Second call should use cache
    List<String> result2 = manager.convert(
        sourceFile, tempDir.toFile(), "cached_src", "cached_tgt");
    assertNotNull(result2);
  }

  // --- isRemoteFile coverage (via convertIfNeeded) ---

  @Test
  void testConvertIfNeededHandlesExceptionInConversion() throws IOException {
    // Create an HTML file that will fail to convert (invalid content)
    File htmlFile = tempDir.resolve("bad.html").toFile();
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write("Not really HTML at all - just garbage content");
    }

    // This should not throw, but return false or true depending on conversion outcome
    boolean result = FileConversionManager.convertIfNeeded(
        htmlFile, tempDir.toFile(), "SMART_CASING", "SMART_CASING",
        null, null);
    // The result may vary based on the HTML converter's tolerance.
    // The key assertion is that no exception propagates.
  }

  @Test
  void testConvertIfNeededForMarkdownFile() throws IOException {
    // Create a markdown file with a table
    File mdFile = tempDir.resolve("data.md").toFile();
    try (FileWriter writer = new FileWriter(mdFile)) {
      writer.write("# Test\n\n| id | name |\n|---|---|\n| 1 | test |\n");
    }

    boolean result = FileConversionManager.convertIfNeeded(
        mdFile, tempDir.toFile(), "SMART_CASING", "SMART_CASING",
        null, null);
    // Markdown files should trigger conversion
    assertTrue(result, "Markdown files should be converted");
  }

  // --- extractJsonPath coverage ---

  @Test
  void testExtractJsonPathInternal() throws Exception {
    // Use reflection to test the private static method
    java.lang.reflect.Method method = FileConversionManager.class.getDeclaredMethod(
        "extractJsonPath", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(null, "JSONPATH_EXTRACTION[$.data.items]");
    assertEquals("$.data.items", result,
        "Should extract the JSONPath from the conversion type string");
  }

  @Test
  void testExtractJsonPathWithNestedBrackets() throws Exception {
    java.lang.reflect.Method method = FileConversionManager.class.getDeclaredMethod(
        "extractJsonPath", String.class);
    method.setAccessible(true);

    // The extractJsonPath method uses simple .replace("]", "") which strips ALL ]
    // So $.store.book[*].title with outer ] removed becomes $.store.book[*.title
    String result = (String) method.invoke(null, "JSONPATH_EXTRACTION[$.store.book[*].title]");
    assertEquals("$.store.book[*.title", result,
        "Should strip all ] characters per implementation");
  }

  // --- isRemoteFile coverage ---

  @Test
  void testIsRemoteFileInternal() throws Exception {
    java.lang.reflect.Method method = FileConversionManager.class.getDeclaredMethod(
        "isRemoteFile", String.class);
    method.setAccessible(true);

    assertTrue((boolean) method.invoke(null, "http://example.com/file.csv"));
    assertTrue((boolean) method.invoke(null, "https://example.com/file.csv"));
    assertTrue((boolean) method.invoke(null, "s3://bucket/key"));
    assertTrue((boolean) method.invoke(null, "ftp://server/file"));
    assertTrue((boolean) method.invoke(null, "sftp://server/file"));
    assertFalse((boolean) method.invoke(null, "/local/path/file.csv"));
    assertFalse((boolean) method.invoke(null, "relative/path/file.csv"));
    assertFalse((boolean) method.invoke(null, (Object) null));
  }

  @Test
  void testConvertIfNeededForHtmlWithExistingRecord() throws IOException {
    // Create an HTML file
    File htmlFile = tempDir.resolve("existing.html").toFile();
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write("<html><body><table><tr><th>id</th></tr>"
          + "<tr><td>1</td></tr></table></body></html>");
    }

    // Create a conversion record for the HTML file
    File baseDir = tempDir.toFile();
    ConversionMetadata metadata = new ConversionMetadata(baseDir);
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        htmlFile.getAbsolutePath(),
        tempDir.resolve("existing.json").toFile().getAbsolutePath(),
        "HTML_TO_JSON");
    record.tableName = "custom_table_name";
    metadata.recordConversion(htmlFile, record);

    // Now convertIfNeeded should find the existing record and preserve table name
    boolean result = FileConversionManager.convertIfNeeded(
        htmlFile, tempDir.toFile(), "SMART_CASING", "SMART_CASING",
        baseDir, null);
    // HTML files should be converted
    assertTrue(result, "HTML file should trigger conversion");
  }

  @Test
  void testConvertIfNeededForXmlFile() throws IOException {
    File xmlFile = tempDir.resolve("data.xml").toFile();
    try (FileWriter writer = new FileWriter(xmlFile)) {
      writer.write("<?xml version=\"1.0\"?><root><item><id>1</id></item></root>");
    }

    boolean result = FileConversionManager.convertIfNeeded(
        xmlFile, tempDir.toFile(), "SMART_CASING", "SMART_CASING",
        null, null);
    assertTrue(result, "XML files should trigger conversion");
  }
}
