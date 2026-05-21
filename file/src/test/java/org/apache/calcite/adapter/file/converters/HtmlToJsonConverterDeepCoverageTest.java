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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link HtmlToJsonConverter} focusing on missed code paths:
 * - extractTextFromElement with br tags and footnotes
 * - extractRawHeaders with no th elements (default headers)
 * - extractHeaders with column name casing
 * - shouldSkipFirstRow logic
 * - hasExtractedFiles checking
 * - convertWithSelector with null/invalid selector and index
 * - convertWithSelector with field configs including skip and mapping
 * - convert with relativePath directory prefix
 * - convert with explicit table name and multiple tables (largest selection)
 * - sanitizeUrlForFileName edge cases
 * - processDataFile for various file types
 * - writeTableAsJson with field configs and skip headers
 */
@Tag("unit")
public class HtmlToJsonConverterDeepCoverageTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir
  Path tempDir;

  // ========== extractTextFromElement via writeTableAsJson ==========

  @Test void testTableWithBrTagsInCells() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Name</th><th>Info</th></tr>"
        + "<tr><td>Alice<br>Smith</td><td>Line1<br>Line2<br>Line3</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("br_tags.html", html);
    File outputDir = createOutputDir("out_br");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertTrue(json.isArray());
    assertTrue(json.size() > 0);
    // br tags should become spaces
    String nameValue = json.get(0).get("Name").asText();
    assertTrue(nameValue.contains("Alice") && nameValue.contains("Smith"),
        "Expected name to contain 'Alice' and 'Smith', got: " + nameValue);
  }

  @Test void testTableWithFootnotes() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Product</th><th>Price</th></tr>"
        + "<tr><td>Widget<sup>[1]</sup></td><td>$9.99</td></tr>"
        + "<tr><td>Gadget<sup>[2]</sup></td><td>$19.99</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("footnotes.html", html);
    File outputDir = createOutputDir("out_footnotes");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertTrue(json.isArray());
    // Footnotes should be handled naturally by JSoup
    String product = json.get(0).get("Product").asText();
    assertTrue(product.contains("Widget"), "Expected product to contain 'Widget'");
  }

  @Test void testTableWithMultipleSpaces() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Label</th></tr>"
        + "<tr><td>word1   word2     word3</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("spaces.html", html);
    File outputDir = createOutputDir("out_spaces");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    String label = json.get(0).get("Label").asText();
    // Multiple spaces should be collapsed to single space
    assertFalse(label.contains("  "), "Multiple spaces should be collapsed");
  }

  // ========== extractRawHeaders with no th elements (default headers) ==========

  @Test void testTableWithNoThElements() throws IOException {
    // Table with only td elements - should generate col0, col1, etc.
    String html = "<html><body><table>"
        + "<tr><td>Alice</td><td>30</td></tr>"
        + "<tr><td>Bob</td><td>25</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("no_th.html", html);
    File outputDir = createOutputDir("out_no_th");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertTrue(json.isArray());
    // Should use default headers col0, col1
    JsonNode firstRow = json.get(0);
    assertTrue(firstRow.has("col0") || firstRow.has("Col0"),
        "Expected default header col0, got: " + firstRow);
  }

  @Test void testTableWithEmptyRows() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Name</th><th>Value</th></tr>"
        + "<tr><td></td><td></td></tr>"
        + "<tr><td>Alice</td><td>100</td></tr>"
        + "<tr></tr>"
        + "<tr><td>Bob</td><td>200</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("empty_rows.html", html);
    File outputDir = createOutputDir("out_empty");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    // Empty rows should be skipped; rows with empty cells result in nulls
    assertTrue(json.isArray());
  }

  // ========== shouldSkipFirstRow logic ==========

  @Test void testShouldSkipFirstRowWithTdFirstRow() throws IOException {
    // When first row has td elements only, it should NOT be skipped
    String html = "<html><body><table>"
        + "<tr><td>Data1</td><td>Data2</td></tr>"
        + "<tr><td>Data3</td><td>Data4</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("td_first.html", html);
    File outputDir = createOutputDir("out_td_first");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    // All rows should be included since first row is data, not headers
    assertTrue(json.size() >= 2, "Expected at least 2 rows, first row should not be skipped");
  }

  @Test void testShouldSkipFirstRowWithTheadTh() throws IOException {
    String html = "<html><body><table>"
        + "<thead><tr><th>Col A</th><th>Col B</th></tr></thead>"
        + "<tbody>"
        + "<tr><td>val1</td><td>val2</td></tr>"
        + "<tr><td>val3</td><td>val4</td></tr>"
        + "</tbody>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("thead.html", html);
    File outputDir = createOutputDir("out_thead");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    // First row (th in thead) should be skipped; 2 data rows remain
    assertEquals(2, json.size(), "Expected 2 data rows with header skipped");
  }

  // ========== hasExtractedFiles ==========

  @Test void testHasExtractedFilesWhenFilesExist() throws IOException {
    File outputDir = createOutputDir("out_exists");
    // Create a file matching the pattern: basename__tablename.json
    File matching = new File(outputDir, "myfile__table1.json");
    try (FileWriter fw = new FileWriter(matching, StandardCharsets.UTF_8)) {
      fw.write("[]");
    }

    File htmlFile = new File(tempDir.toFile(), "myfile.html");
    assertTrue(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));
  }

  @Test void testHasExtractedFilesWhenNoFilesExist() throws IOException {
    File outputDir = createOutputDir("out_none");
    File htmlFile = new File(tempDir.toFile(), "nonexistent.html");
    assertFalse(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));
  }

  @Test void testHasExtractedFilesWithHtmExtension() throws IOException {
    File outputDir = createOutputDir("out_htm");
    File matching = new File(outputDir, "mypage__data.json");
    try (FileWriter fw = new FileWriter(matching, StandardCharsets.UTF_8)) {
      fw.write("[]");
    }

    File htmlFile = new File(tempDir.toFile(), "mypage.htm");
    assertTrue(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));
  }

  @Test void testHasExtractedFilesOutputDirDoesNotExist() {
    File outputDir = new File(tempDir.toFile(), "nonexistent_dir");
    File htmlFile = new File(tempDir.toFile(), "test.html");
    assertFalse(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));
  }

  // ========== convertWithSelector ==========

  @Test void testConvertWithSelectorNullSelector() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>A</th></tr>"
        + "<tr><td>1</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("nullsel.html", html);
    File outputDir = createOutputDir("out_nullsel");

    // null selector should fall back to regular conversion
    List<File> result =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        null, 0, "fallback_table", tempDir.toFile());

    assertNotNull(result);
    // Falls back to regular convert with explicitTableName
    assertTrue(result.size() >= 1);
  }

  @Test void testConvertWithSelectorNullIndex() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>A</th></tr>"
        + "<tr><td>1</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("nullidx.html", html);
    File outputDir = createOutputDir("out_nullidx");

    // null index should fall back to regular conversion
    List<File> result =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table", null, "fallback_table", tempDir.toFile());

    assertNotNull(result);
    assertTrue(result.size() >= 1);
  }

  @Test void testConvertWithSelectorNoMatchingTables() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>A</th></tr>"
        + "<tr><td>1</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("nomatch.html", html);
    File outputDir = createOutputDir("out_nomatch");

    // Selector that matches nothing
    List<File> result =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.nonexistent-class", 0, "test_table", tempDir.toFile());

    assertNotNull(result);
    assertEquals(0, result.size(), "Expected no files when selector matches nothing");
  }

  @Test void testConvertWithSelectorIndexOutOfBounds() throws IOException {
    String html = "<html><body><table class='target'>"
        + "<tr><th>A</th></tr>"
        + "<tr><td>1</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("oob.html", html);
    File outputDir = createOutputDir("out_oob");

    // Index larger than number of matching tables
    List<File> result =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.target", 5, "test_table", tempDir.toFile());

    assertNotNull(result);
    assertEquals(0, result.size(), "Expected no files when index is out of bounds");
  }

  @Test void testConvertWithSelectorValidSelection() throws IOException {
    String html = "<html><body>"
        + "<table class='data'><tr><th>X</th></tr><tr><td>1</td></tr></table>"
        + "<table class='data'><tr><th>Y</th></tr><tr><td>2</td></tr></table>"
        + "</body></html>";

    File htmlFile = createHtmlFile("valid_sel.html", html);
    File outputDir = createOutputDir("out_valid_sel");

    // Select second table
    List<File> result =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.data", 1, "second_table", tempDir.toFile());

    assertNotNull(result);
    assertEquals(1, result.size());
    JsonNode json = MAPPER.readTree(Files.readString(result.get(0).toPath()));
    assertTrue(json.isArray());
    assertEquals(1, json.size());
    assertEquals("2", json.get(0).get("Y").asText());
  }

  @Test void testConvertWithSelectorNullBaseDirectory() throws IOException {
    String html = "<html><body>"
        + "<table class='tgt'><tr><th>Z</th></tr><tr><td>99</td></tr></table>"
        + "</body></html>";

    File htmlFile = createHtmlFile("nullbase.html", html);
    File outputDir = createOutputDir("out_nullbase");

    // baseDirectory is null - should use ConversionRecorder path
    List<File> result =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.tgt", 0, "null_base_table", null);

    assertNotNull(result);
    assertEquals(1, result.size());
  }

  // ========== convertWithSelector with fieldConfigs ==========

  @Test void testConvertWithSelectorFieldConfigsMapping() throws IOException {
    String html = "<html><body>"
        + "<table class='mapped'>"
        + "<tr><th>Full Name</th><th>Annual Salary</th></tr>"
        + "<tr><td>Alice</td><td>50000</td></tr>"
        + "<tr><td>Bob</td><td>60000</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("fieldmap.html", html);
    File outputDir = createOutputDir("out_fieldmap");

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> field1 = new HashMap<>();
    field1.put("th", "Full Name");
    field1.put("name", "employee_name");
    fieldConfigs.add(field1);

    Map<String, Object> field2 = new HashMap<>();
    field2.put("th", "Annual Salary");
    field2.put("name", "salary");
    fieldConfigs.add(field2);

    List<File> result =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.mapped", 0, "mapped_table", tempDir.toFile(), fieldConfigs);

    assertNotNull(result);
    assertEquals(1, result.size());
    JsonNode json = MAPPER.readTree(Files.readString(result.get(0).toPath()));
    assertTrue(json.isArray());
    assertEquals(2, json.size());
    assertTrue(json.get(0).has("employee_name"), "Expected mapped column name 'employee_name'");
    assertTrue(json.get(0).has("salary"), "Expected mapped column name 'salary'");
  }

  @Test void testConvertWithSelectorFieldConfigsSkip() throws IOException {
    String html = "<html><body>"
        + "<table class='skip'>"
        + "<tr><th>Name</th><th>Internal ID</th><th>Score</th></tr>"
        + "<tr><td>Alice</td><td>X123</td><td>95</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("skipfield.html", html);
    File outputDir = createOutputDir("out_skipfield");

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    // Skip the Internal ID column
    Map<String, Object> skipField = new HashMap<>();
    skipField.put("th", "Internal ID");
    skipField.put("skip", "true");
    fieldConfigs.add(skipField);

    List<File> result =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.skip", 0, "skip_table", tempDir.toFile(), fieldConfigs);

    assertNotNull(result);
    assertEquals(1, result.size());
    JsonNode json = MAPPER.readTree(Files.readString(result.get(0).toPath()));
    JsonNode row = json.get(0);
    assertTrue(row.has("Name"), "Expected 'Name' column");
    assertFalse(row.has("Internal ID"), "Should not have skipped 'Internal ID' column");
    assertTrue(row.has("Score"), "Expected 'Score' column");
  }

  @Test void testConvertWithSelectorFieldConfigsWithThButNoName() throws IOException {
    // When a field config has th but no name, it should apply default casing
    String html = "<html><body>"
        + "<table class='noname'>"
        + "<tr><th>First Name</th><th>Last Name</th></tr>"
        + "<tr><td>Alice</td><td>Smith</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("noname.html", html);
    File outputDir = createOutputDir("out_noname");

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> field1 = new HashMap<>();
    field1.put("th", "First Name");
    // No "name" key - should use casing-applied th value
    fieldConfigs.add(field1);

    List<File> result =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.noname", 0, "noname_table", tempDir.toFile(), fieldConfigs);

    assertNotNull(result);
    assertEquals(1, result.size());
  }

  // ========== convert with relativePath ==========

  @Test void testConvertWithRelativePath() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Key</th><th>Val</th></tr>"
        + "<tr><td>k1</td><td>v1</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("relpath.html", html);
    File outputDir = createOutputDir("out_relpath");

    // Use a relative path with directory separator
    String relativePath = "subdir" + File.separator + "relpath.html";

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING", tempDir.toFile(), relativePath);

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    // The JSON filename should include the directory prefix
    String fileName = jsonFiles.get(0).getName();
    assertTrue(fileName.contains("subdir"),
        "Expected filename to contain directory prefix 'subdir', got: " + fileName);
  }

  @Test void testConvertWithRelativePathNoSeparator() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Key</th></tr>"
        + "<tr><td>k1</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("nodir.html", html);
    File outputDir = createOutputDir("out_nodir");

    // Relative path without directory separator
    String relativePath = "nodir.html";

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING", tempDir.toFile(), relativePath);

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
  }

  // ========== convert with explicit table name and multiple tables ==========

  @Test void testConvertWithExplicitTableNameSingleTable() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>A</th></tr>"
        + "<tr><td>1</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("single.html", html);
    File outputDir = createOutputDir("out_single");

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "my_explicit_table", tempDir.toFile(), null);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    assertEquals("my_explicit_table.json", jsonFiles.get(0).getName());
  }

  @Test void testConvertWithExplicitTableNameMultipleTables() throws IOException {
    // Multiple tables - should select the one with most columns
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>Small</th></tr>"
        + "<tr><td>s1</td></tr>"
        + "</table>"
        + "<table>"
        + "<tr><th>Big1</th><th>Big2</th><th>Big3</th></tr>"
        + "<tr><td>b1</td><td>b2</td><td>b3</td></tr>"
        + "</table>"
        + "<table>"
        + "<tr><th>Med1</th><th>Med2</th></tr>"
        + "<tr><td>m1</td><td>m2</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createHtmlFile("multi.html", html);
    File outputDir = createOutputDir("out_multi");

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "largest_table", tempDir.toFile(), null);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    assertEquals("largest_table.json", jsonFiles.get(0).getName());

    // Verify it selected the largest table (3 columns)
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertTrue(json.isArray());
    JsonNode row = json.get(0);
    assertTrue(row.has("Big1") || row.has("big1"),
        "Expected to select the table with most columns");
  }

  @Test void testConvertWithExplicitTableNameNullBaseDirectory() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Col</th></tr>"
        + "<tr><td>val</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("nullbase2.html", html);
    File outputDir = createOutputDir("out_nullbase2");

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "nullbase_table", null, null);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
  }

  // ========== convert with no explicit table name (null) ==========

  @Test void testConvertMultipleTablesNoExplicitName() throws IOException {
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>T1Col</th></tr>"
        + "<tr><td>t1v</td></tr>"
        + "</table>"
        + "<table>"
        + "<tr><th>T2Col</th></tr>"
        + "<tr><td>t2v</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createHtmlFile("multino.html", html);
    File outputDir = createOutputDir("out_multino");

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING", tempDir.toFile());

    assertNotNull(jsonFiles);
    // Should generate separate JSON files for each table
    assertTrue(jsonFiles.size() >= 1);
  }

  // ========== convert overloads chain ==========

  @Test void testConvert3ArgOverload() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>H</th></tr>"
        + "<tr><td>V</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("overload3.html", html);
    File outputDir = createOutputDir("out_overload3");

    // 3-arg convert(File, File, File) delegates to 4-arg with UNCHANGED
    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
  }

  @Test void testConvert4ArgOverload() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>H</th></tr>"
        + "<tr><td>V</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("overload4.html", html);
    File outputDir = createOutputDir("out_overload4");

    // 4-arg convert with columnNameCasing
    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
  }

  // ========== sanitizeUrlForFileName via reflection ==========

  @Test void testSanitizeUrlForFileName() throws Exception {
    Method method =
        HtmlToJsonConverter.class.getDeclaredMethod("sanitizeUrlForFileName", String.class);
    method.setAccessible(true);

    // Basic URL
    String result = (String) method.invoke(null, "https://example.com/page.html");
    assertNotNull(result);
    assertFalse(result.contains("https://"));
    assertFalse(result.contains("://"));

    // URL with special characters
    String result2 = (String) method.invoke(null, "http://ex.com/path?q=1&a=2#frag");
    assertNotNull(result2);
    assertFalse(result2.contains("?"));
    assertFalse(result2.contains("&"));

    // Very long URL should be truncated to 100 chars
    StringBuilder longUrl = new StringBuilder("https://");
    for (int i = 0; i < 200; i++) {
      longUrl.append("a");
    }
    String result3 = (String) method.invoke(null, longUrl.toString());
    assertTrue(result3.length() <= 100, "Expected truncated to 100 chars, got: " + result3.length());

    // URL ending with special chars -> trailing underscores removed
    String result4 = (String) method.invoke(null, "https://example.com/path/");
    assertFalse(result4.endsWith("_"), "Trailing underscores should be removed");
  }

  // ========== Table with row headers (th in data rows) ==========

  @Test void testTableWithThInDataRows() throws IOException {
    // Some tables use th for row headers in the first column
    String html = "<html><body><table>"
        + "<tr><th>Category</th><th>Count</th></tr>"
        + "<tr><th>Fruits</th><td>10</td></tr>"
        + "<tr><th>Vegetables</th><td>20</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("row_headers.html", html);
    File outputDir = createOutputDir("out_row_headers");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertTrue(json.isArray());
    assertTrue(json.size() > 0, "Expected data rows to be present");
  }

  // ========== Table with cells exceeding headers ==========

  @Test void testTableWithMoreCellsThanHeaders() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>A</th><th>B</th></tr>"
        + "<tr><td>1</td><td>2</td><td>3</td><td>4</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("extra_cells.html", html);
    File outputDir = createOutputDir("out_extra");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    // Extra cells beyond header count should be ignored
    JsonNode row = json.get(0);
    assertNotNull(row);
  }

  // ========== Output directory creation ==========

  @Test void testConvertCreatesOutputDirIfNotExists() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>X</th></tr>"
        + "<tr><td>1</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("mkdir.html", html);
    File outputDir = new File(tempDir.toFile(), "new_output_dir");
    assertFalse(outputDir.exists());

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertTrue(outputDir.exists(), "Output directory should be created");
    assertNotNull(jsonFiles);
  }

  // ========== Convert with 7-arg overload for existing table name ==========

  @Test void testConvert7ArgWithExistingTableName() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Col</th></tr>"
        + "<tr><td>val</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("existing_name.html", html);
    File outputDir = createOutputDir("out_existing");

    // This overload is used for preserving existing table names
    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        tempDir.toFile(), null, "preserved_table_name");

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    assertEquals("preserved_table_name.json", jsonFiles.get(0).getName());
  }

  // ========== Empty HTML file ==========

  @Test void testConvertEmptyHtmlFile() throws IOException {
    String html = "<html><body></body></html>";
    File htmlFile = createHtmlFile("empty.html", html);
    File outputDir = createOutputDir("out_empty_html");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertEquals(0, jsonFiles.size(), "Expected no JSON files for empty HTML");
  }

  @Test void testConvertHtmlWithNoTables() throws IOException {
    String html = "<html><body><p>Hello, no tables here!</p></body></html>";
    File htmlFile = createHtmlFile("notables.html", html);
    File outputDir = createOutputDir("out_notables");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertEquals(0, jsonFiles.size(), "Expected no JSON files when HTML has no tables");
  }

  // ========== Helpers ==========

  private File createHtmlFile(String name, String content) throws IOException {
    File file = new File(tempDir.toFile(), name);
    try (FileWriter fw = new FileWriter(file, StandardCharsets.UTF_8)) {
      fw.write(content);
    }
    return file;
  }

  private File createOutputDir(String name) {
    File dir = new File(tempDir.toFile(), name);
    dir.mkdirs();
    return dir;
  }
}
