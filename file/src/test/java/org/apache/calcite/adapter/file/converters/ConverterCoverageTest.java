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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for HtmlToJsonConverter, ExcelToJsonConverter,
 * and FileConversionManager.
 *
 * <p>Creates test files programmatically and verifies conversion output.
 */
@Tag("unit")
class ConverterCoverageTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir
  Path tempDir;

  private File outputDir;
  private File baseDir;

  @BeforeEach
  void setUp() throws IOException {
    outputDir = Files.createDirectories(tempDir.resolve("output")).toFile();
    baseDir = Files.createDirectories(tempDir.resolve("base")).toFile();
  }

  // ---------------------------------------------------------------
  // HtmlToJsonConverter tests
  // ---------------------------------------------------------------

  @Test void testHtmlToJsonConverterSimpleTable() throws IOException {
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>Name</th><th>Age</th></tr>"
        + "<tr><td>Alice</td><td>30</td></tr>"
        + "<tr><td>Bob</td><td>25</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("simple_table.html", html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, baseDir);

    assertNotNull(jsonFiles);
    assertFalse(jsonFiles.isEmpty());

    // Read the JSON and verify content
    File jsonFile = jsonFiles.get(0);
    assertTrue(jsonFile.exists());
    ArrayNode array = (ArrayNode) MAPPER.readTree(jsonFile);
    assertEquals(2, array.size());

    JsonNode row1 = array.get(0);
    assertNotNull(row1);
  }

  @Test void testHtmlToJsonConverterWithSmartCasing() throws IOException {
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>First Name</th><th>Last Name</th><th>Phone Number</th></tr>"
        + "<tr><td>Alice</td><td>Smith</td><td>555-1234</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("casing_test.html", html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        htmlFile, outputDir, "SMART_CASING", baseDir);

    assertNotNull(jsonFiles);
    assertFalse(jsonFiles.isEmpty());

    File jsonFile = jsonFiles.get(0);
    ArrayNode array = (ArrayNode) MAPPER.readTree(jsonFile);
    assertEquals(1, array.size());
  }

  @Test void testHtmlToJsonConverterWithTableNameCasing() throws IOException {
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>Col A</th></tr>"
        + "<tr><td>Value1</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("table_casing.html", html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        htmlFile, outputDir, "SMART_CASING", "SMART_CASING", baseDir);

    assertNotNull(jsonFiles);
    assertFalse(jsonFiles.isEmpty());
  }

  @Test void testHtmlToJsonConverterMultipleTables() throws IOException {
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>Product</th><th>Price</th></tr>"
        + "<tr><td>Widget</td><td>9.99</td></tr>"
        + "</table>"
        + "<table>"
        + "<tr><th>Region</th><th>Sales</th></tr>"
        + "<tr><td>North</td><td>1000</td></tr>"
        + "<tr><td>South</td><td>2000</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("multi_tables.html", html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        htmlFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);

    assertNotNull(jsonFiles);
    assertEquals(2, jsonFiles.size());
  }

  @Test void testHtmlToJsonConverterWithExplicitTableName() throws IOException {
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>Item</th><th>Count</th></tr>"
        + "<tr><td>A</td><td>10</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("explicit_name.html", html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        htmlFile, outputDir, "UNCHANGED", "UNCHANGED",
        "custom_table_name", baseDir, null);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());

    File jsonFile = jsonFiles.get(0);
    assertTrue(jsonFile.getName().startsWith("custom_table_name"));
  }

  @Test void testHtmlToJsonConverterWithSelector() throws IOException {
    String html = "<html><body>"
        + "<table class=\"data sortable\">"
        + "<tr><th>Name</th><th>Score</th></tr>"
        + "<tr><td>Alice</td><td>95</td></tr>"
        + "</table>"
        + "<table class=\"data sortable\">"
        + "<tr><th>City</th><th>Pop</th></tr>"
        + "<tr><td>NYC</td><td>8000000</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("selector_test.html", html);

    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        htmlFile, outputDir, "UNCHANGED", "UNCHANGED",
        "table.data", 1, "city_data", baseDir);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());

    ArrayNode array = (ArrayNode) MAPPER.readTree(jsonFiles.get(0));
    assertEquals(1, array.size());
  }

  @Test void testHtmlToJsonConverterSelectorOutOfBounds() throws IOException {
    String html = "<html><body>"
        + "<table class=\"data\">"
        + "<tr><th>A</th></tr><tr><td>1</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("oob_selector.html", html);

    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        htmlFile, outputDir, "UNCHANGED", "UNCHANGED",
        "table.data", 5, "test_table", baseDir);

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.isEmpty());
  }

  @Test void testHtmlToJsonConverterNoTablesForSelector() throws IOException {
    String html = "<html><body>"
        + "<p>No tables here</p>"
        + "</body></html>";

    File htmlFile = createTempFile("no_tables.html", html);

    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        htmlFile, outputDir, "UNCHANGED", "UNCHANGED",
        "table.nonexistent", 0, "test_table", baseDir);

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.isEmpty());
  }

  @Test void testHtmlToJsonConverterNullSelector() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File htmlFile = createTempFile("null_selector.html", html);

    // When selector is null, falls back to regular conversion
    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        htmlFile, outputDir, "UNCHANGED", "UNCHANGED",
        null, null, "test_table", baseDir);

    assertNotNull(jsonFiles);
    // Falls back to convert() with explicit table name
    assertFalse(jsonFiles.isEmpty());
  }

  @Test void testHtmlToJsonConverterNoTh() throws IOException {
    // Table without th elements should use default header names
    String html = "<html><body>"
        + "<table>"
        + "<tr><td>1</td><td>Alice</td></tr>"
        + "<tr><td>2</td><td>Bob</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("no_th.html", html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        htmlFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);

    assertNotNull(jsonFiles);
    assertFalse(jsonFiles.isEmpty());

    ArrayNode array = (ArrayNode) MAPPER.readTree(jsonFiles.get(0));
    // Without th headers, default col0/col1 names should be used
    // All rows should be included since there are no th rows to skip
    assertEquals(2, array.size());
  }

  @Test void testHtmlToJsonHasExtractedFiles() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>X</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File htmlFile = createTempFile("check_extracted.html", html);

    // Before conversion - no files
    assertFalse(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));

    // Convert
    HtmlToJsonConverter.convert(htmlFile, outputDir, baseDir);

    // After conversion - files should exist
    assertTrue(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));
  }

  @Test void testHtmlToJsonConverterWithRelativePath() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>Name</th></tr><tr><td>Test</td></tr></table>"
        + "</body></html>";

    File htmlFile = createTempFile("relpath.html", html);

    List<File> jsonFiles = HtmlToJsonConverter.convert(
        htmlFile, outputDir, "UNCHANGED", "UNCHANGED",
        baseDir, "subdir" + File.separator + "relpath.html");

    assertNotNull(jsonFiles);
    assertFalse(jsonFiles.isEmpty());
    // Filename should contain directory prefix
    assertTrue(jsonFiles.get(0).getName().contains("subdir_"));
  }

  @Test void testHtmlToJsonConverterBrHandling() throws IOException {
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>Description</th></tr>"
        + "<tr><td>Line one<br>Line two</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("br_test.html", html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, baseDir);

    assertNotNull(jsonFiles);
    assertFalse(jsonFiles.isEmpty());

    ArrayNode array = (ArrayNode) MAPPER.readTree(jsonFiles.get(0));
    assertEquals(1, array.size());
    // br should be converted to spaces
    String description = array.get(0).fieldNames().next();
    assertNotNull(description);
  }

  // ---------------------------------------------------------------
  // ExcelToJsonConverter tests
  // ---------------------------------------------------------------

  @Test void testExcelToJsonConverterSimpleSheet() throws IOException {
    File excelFile = createSimpleExcelFile("simple.xlsx",
        new String[]{"employee_name", "department", "salary"},
        new Object[][]{
            {"Alice", "Engineering", 100000},
            {"Bob", "Marketing", 80000}
        });

    ExcelToJsonConverter.convertFileToJson(
        excelFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);

    // Check output files were created
    File[] jsonFiles = outputDir.listFiles(
        (dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length > 0);

    // Verify content
    ArrayNode array = (ArrayNode) MAPPER.readTree(jsonFiles[0]);
    assertEquals(2, array.size());
    assertNotNull(array.get(0).get("employee_name"));
  }

  @Test void testExcelToJsonConverterMultipleSheets() throws IOException {
    File excelFile = createMultiSheetExcelFile("multi.xlsx");

    ExcelToJsonConverter.convertFileToJson(
        excelFile, outputDir, "SMART_CASING", "SMART_CASING", baseDir);

    File[] jsonFiles = outputDir.listFiles(
        (dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    // Should have one JSON file per sheet
    assertTrue(jsonFiles.length >= 2);
  }

  @Test void testExcelToJsonConverterWithRelativePath() throws IOException {
    File excelFile = createSimpleExcelFile("relpath.xlsx",
        new String[]{"item", "price"},
        new Object[][]{{"Widget", 9.99}});

    ExcelToJsonConverter.convertFileToJson(
        excelFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir,
        "subdir" + File.separator + "relpath.xlsx");

    File[] jsonFiles = outputDir.listFiles(
        (dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length > 0);
    // Filename should contain directory prefix
    assertTrue(jsonFiles[0].getName().contains("subdir_"));
  }

  @Test void testExcelToJsonConverterEmptySheet() throws IOException {
    File excelFile = tempDir.resolve("empty.xlsx").toFile();
    try (Workbook wb = new XSSFWorkbook();
         FileOutputStream out = new FileOutputStream(excelFile)) {
      Sheet sheet = wb.createSheet("EmptySheet");
      // Only add a blank row
      Row row = sheet.createRow(0);
      row.createCell(0);
      wb.write(out);
    }

    ExcelToJsonConverter.convertFileToJson(
        excelFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);

    // Empty sheets may be skipped or produce empty JSON
    // Should not throw
  }

  @Test void testExcelToJsonConverterNumericHeaders() throws IOException {
    File excelFile = createSimpleExcelFile("numeric.xlsx",
        new String[]{"2020", "2021", "2022"},
        new Object[][]{{100, 200, 300}});

    ExcelToJsonConverter.convertFileToJson(
        excelFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);

    File[] jsonFiles = outputDir.listFiles(
        (dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length > 0);
  }

  @Test void testExcelToJsonConverterBooleanCell() throws IOException {
    File excelFile = tempDir.resolve("booleans.xlsx").toFile();
    try (Workbook wb = new XSSFWorkbook();
         FileOutputStream out = new FileOutputStream(excelFile)) {
      Sheet sheet = wb.createSheet("Flags");
      Row header = sheet.createRow(0);
      header.createCell(0).setCellValue("flag_name");
      header.createCell(1).setCellValue("is_active");

      Row row1 = sheet.createRow(1);
      row1.createCell(0).setCellValue("feature_a");
      row1.createCell(1).setCellValue(true);

      Row row2 = sheet.createRow(2);
      row2.createCell(0).setCellValue("feature_b");
      row2.createCell(1).setCellValue(false);

      wb.write(out);
    }

    ExcelToJsonConverter.convertFileToJson(
        excelFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);

    File[] jsonFiles = outputDir.listFiles(
        (dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length > 0);

    ArrayNode array = (ArrayNode) MAPPER.readTree(jsonFiles[0]);
    assertEquals(2, array.size());
    assertTrue(array.get(0).get("is_active").asBoolean());
    assertFalse(array.get(1).get("is_active").asBoolean());
  }

  // ---------------------------------------------------------------
  // FileConversionManager tests
  // ---------------------------------------------------------------

  @Test void testFileConversionManagerGetInstance() {
    FileConversionManager instance = FileConversionManager.getInstance();
    assertNotNull(instance);

    // Singleton - same instance
    FileConversionManager instance2 = FileConversionManager.getInstance();
    assertTrue(instance == instance2);
  }

  @Test void testFileConversionManagerRequiresConversion() {
    assertTrue(FileConversionManager.requiresConversion("test.xlsx"));
    assertTrue(FileConversionManager.requiresConversion("test.xls"));
    assertTrue(FileConversionManager.requiresConversion("page.html"));
    assertTrue(FileConversionManager.requiresConversion("page.htm"));
    assertTrue(FileConversionManager.requiresConversion("data.xml"));
    assertTrue(FileConversionManager.requiresConversion("readme.md"));
    assertTrue(FileConversionManager.requiresConversion("report.docx"));
    assertTrue(FileConversionManager.requiresConversion("slides.pptx"));

    assertFalse(FileConversionManager.requiresConversion("data.csv"));
    assertFalse(FileConversionManager.requiresConversion("data.json"));
    assertFalse(FileConversionManager.requiresConversion("data.parquet"));
    assertFalse(FileConversionManager.requiresConversion("data.txt"));
  }

  @Test void testFileConversionManagerIsDirectlyUsable() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.csv"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.csv.gz"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.tsv"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.tsv.gz"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.json"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.json.gz"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.parquet"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.arrow"));
    assertTrue(FileConversionManager.isDirectlyUsable("config.yaml"));
    assertTrue(FileConversionManager.isDirectlyUsable("config.yml"));

    assertFalse(FileConversionManager.isDirectlyUsable("test.xlsx"));
    assertFalse(FileConversionManager.isDirectlyUsable("test.html"));
    assertFalse(FileConversionManager.isDirectlyUsable("test.xml"));
  }

  @Test void testConvertIfNeededWithCsvFile() {
    File csvFile = new File(tempDir.toFile(), "data.csv");
    // CSV files should not need conversion
    boolean converted = FileConversionManager.convertIfNeeded(
        csvFile, outputDir, "UNCHANGED");
    assertFalse(converted);
  }

  @Test void testConvertIfNeededWithHtmlFile() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File htmlFile = createTempFile("convert_test.html", html);

    boolean converted = FileConversionManager.convertIfNeeded(
        htmlFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);

    assertTrue(converted);

    // Verify output JSON files exist
    File[] jsonFiles = outputDir.listFiles(
        (dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length > 0);
  }

  @Test void testConvertIfNeededWithExcelFile() throws IOException {
    File excelFile = createSimpleExcelFile("convert.xlsx",
        new String[]{"label", "count"},
        new Object[][]{{"A", 1}});

    boolean converted = FileConversionManager.convertIfNeeded(
        excelFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);

    assertTrue(converted);
  }

  @Test void testConvertIfNeededSkipsUnchangedFile() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>X</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File htmlFile = createTempFile("unchanged.html", html);

    // First conversion should happen
    boolean first = FileConversionManager.convertIfNeeded(
        htmlFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);
    assertTrue(first);

    // Second conversion should be skipped (file unchanged)
    boolean second = FileConversionManager.convertIfNeeded(
        htmlFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);
    assertFalse(second);
  }

  @Test void testConvertIfNeededWithNullBaseDirectory() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>Y</th></tr><tr><td>2</td></tr></table>"
        + "</body></html>";

    File htmlFile = createTempFile("null_base.html", html);

    // Should still work with null base directory
    boolean converted = FileConversionManager.convertIfNeeded(
        htmlFile, outputDir, "UNCHANGED", "UNCHANGED", null);

    assertTrue(converted);
  }

  @Test void testFindOriginalSourceNoHistory() {
    File csvFile = new File(tempDir.toFile(), "data.csv");
    File original = FileConversionManager.findOriginalSource(csvFile);
    // Without conversion history, returns the same file
    assertEquals(csvFile, original);
  }

  @Test void testConvertIfNeededWithJsonFile() throws IOException {
    // JSON files that are not JSONPath extractions should not be converted
    File jsonFile = new File(tempDir.toFile(), "plain.json");
    Files.write(jsonFile.toPath(),
        "[{\"a\": 1}]".getBytes(StandardCharsets.UTF_8));

    boolean converted = FileConversionManager.convertIfNeeded(
        jsonFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);
    assertFalse(converted);
  }

  @Test void testConvertIfNeededWithYamlFile() throws IOException {
    File yamlFile = new File(tempDir.toFile(), "config.yaml");
    Files.write(yamlFile.toPath(),
        "key: value".getBytes(StandardCharsets.UTF_8));

    boolean converted = FileConversionManager.convertIfNeeded(
        yamlFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);
    assertFalse(converted);
  }

  @Test void testFileConversionManagerRegisterConverter() {
    FileConversionManager manager = FileConversionManager.getInstance();

    // Register a test converter
    manager.registerConverter(new FileConverter() {
      @Override
      public boolean canConvert(String sourceFormat, String targetFormat) {
        return "test".equals(sourceFormat) && "json".equals(targetFormat);
      }

      @Override
      public String getSourceFormat() {
        return "test";
      }

      @Override
      public String getTargetFormat() {
        return "json";
      }

      @Override
      public List<String> convert(String sourcePath, String targetDirectory,
          org.apache.calcite.adapter.file.metadata.ConversionMetadata metadata) {
        return java.util.Collections.emptyList();
      }
    });

    // Should not throw
  }

  @Test void testFileConversionManagerConvertUnknownFormat() {
    FileConversionManager manager = FileConversionManager.getInstance();
    File sourceFile = new File(tempDir.toFile(), "data.unknown");

    assertThrows(IOException.class, () -> {
      manager.convert(sourceFile, outputDir, "unknown_format", "json");
    });
  }

  // ---------------------------------------------------------------
  // Utility methods
  // ---------------------------------------------------------------

  private File createTempFile(String name, String content) throws IOException {
    File file = new File(tempDir.toFile(), name);
    Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
    return file;
  }

  private File createSimpleExcelFile(String name, String[] headers,
      Object[][] data) throws IOException {
    File file = tempDir.resolve(name).toFile();
    try (Workbook wb = new XSSFWorkbook();
         FileOutputStream out = new FileOutputStream(file)) {
      Sheet sheet = wb.createSheet("data_sheet");

      Row headerRow = sheet.createRow(0);
      for (int i = 0; i < headers.length; i++) {
        headerRow.createCell(i).setCellValue(headers[i]);
      }

      for (int r = 0; r < data.length; r++) {
        Row row = sheet.createRow(r + 1);
        for (int c = 0; c < data[r].length; c++) {
          Object val = data[r][c];
          if (val instanceof String) {
            row.createCell(c).setCellValue((String) val);
          } else if (val instanceof Number) {
            row.createCell(c).setCellValue(((Number) val).doubleValue());
          } else if (val instanceof Boolean) {
            row.createCell(c).setCellValue((Boolean) val);
          }
        }
      }

      wb.write(out);
    }
    return file;
  }

  private File createMultiSheetExcelFile(String name) throws IOException {
    File file = tempDir.resolve(name).toFile();
    try (Workbook wb = new XSSFWorkbook();
         FileOutputStream out = new FileOutputStream(file)) {
      // Sheet 1: products
      Sheet sheet1 = wb.createSheet("products");
      Row h1 = sheet1.createRow(0);
      h1.createCell(0).setCellValue("product_id");
      h1.createCell(1).setCellValue("product_name");
      Row d1 = sheet1.createRow(1);
      d1.createCell(0).setCellValue(1);
      d1.createCell(1).setCellValue("Widget");

      // Sheet 2: regions
      Sheet sheet2 = wb.createSheet("regions");
      Row h2 = sheet2.createRow(0);
      h2.createCell(0).setCellValue("region_code");
      h2.createCell(1).setCellValue("region_name");
      Row d2 = sheet2.createRow(1);
      d2.createCell(0).setCellValue("US");
      d2.createCell(1).setCellValue("United States");

      wb.write(out);
    }
    return file;
  }
}
