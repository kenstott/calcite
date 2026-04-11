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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HtmlToJsonConverter}.
 * Tests conversion of HTML tables to JSON files, including:
 * - Single table conversion
 * - Multiple table conversion
 * - Column name casing
 * - Explicit table names
 * - CSS selector-based table selection
 * - Field mappings
 * - Default headers (no th elements)
 * - hasExtractedFiles check
 * - sanitizeUrlForFileName
 */
@Tag("unit")
public class HtmlToJsonConverterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir
  File tempDir;

  private File outputDir;
  private File htmlFile;

  @BeforeEach
  public void setUp() throws IOException {
    outputDir = new File(tempDir, "output");
    outputDir.mkdirs();
  }

  // ========== Helper Methods ==========

  private File createHtmlFile(String content) throws IOException {
    File file = new File(tempDir, "test.html");
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
    return file;
  }

  private File createHtmlFileWithName(String name, String content) throws IOException {
    File file = new File(tempDir, name);
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
    return file;
  }

  // ========== Basic Conversion Tests ==========

  @Test
  public void testConvertSingleTableWithHeaders() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>Name</th><th>Age</th></tr>"
        + "<tr><td>Alice</td><td>30</td></tr>"
        + "<tr><td>Bob</td><td>25</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(file, outputDir, tempDir);

    assertNotNull(jsonFiles);
    assertFalse(jsonFiles.isEmpty());

    // Verify JSON content
    File jsonFile = jsonFiles.get(0);
    assertTrue(jsonFile.exists());
    JsonNode array = MAPPER.readTree(jsonFile);
    assertTrue(array.isArray());
    assertEquals(2, array.size());
  }

  @Test
  public void testConvertWithColumnNameCasing() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>First Name</th><th>Last Name</th></tr>"
        + "<tr><td>Alice</td><td>Smith</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(file, outputDir, "TO_LOWER", tempDir);

    assertNotNull(jsonFiles);
    assertFalse(jsonFiles.isEmpty());

    File jsonFile = jsonFiles.get(0);
    JsonNode array = MAPPER.readTree(jsonFile);
    assertTrue(array.isArray());
    assertTrue(array.size() > 0);
  }

  @Test
  public void testConvertWithTableNameCasing() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>Col1</th><th>Col2</th></tr>"
        + "<tr><td>A</td><td>B</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        file, outputDir, "UNCHANGED", "SMART_CASING", tempDir);

    assertNotNull(jsonFiles);
  }

  @Test
  public void testConvertWithExplicitTableName() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>Name</th><th>Value</th></tr>"
        + "<tr><td>test</td><td>123</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        file, outputDir, "UNCHANGED", "SMART_CASING",
        "my_table", tempDir, null);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    assertEquals("my_table.json", jsonFiles.get(0).getName());
  }

  @Test
  public void testConvertWithExplicitTableNameAndBaseDirectory() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>Name</th><th>Value</th></tr>"
        + "<tr><td>test</td><td>123</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    File baseDir = new File(tempDir, "basedir");
    baseDir.mkdirs();

    List<File> jsonFiles = HtmlToJsonConverter.convert(
        file, outputDir, "UNCHANGED", "SMART_CASING",
        "explicit_name", baseDir, null);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    assertEquals("explicit_name.json", jsonFiles.get(0).getName());
  }

  @Test
  public void testConvertWithExplicitTableNameNullBaseDirectory() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>Name</th><th>Value</th></tr>"
        + "<tr><td>test</td><td>123</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        file, outputDir, "UNCHANGED", "SMART_CASING",
        "my_table2", null, null);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
  }

  // ========== Multiple Tables Tests ==========

  @Test
  public void testConvertMultipleTables() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "<table><tr><th>B</th></tr><tr><td>2</td></tr></table>"
        + "</body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        file, outputDir, "UNCHANGED", "SMART_CASING", tempDir);

    assertNotNull(jsonFiles);
    assertEquals(2, jsonFiles.size());
  }

  @Test
  public void testConvertMultipleTablesWithExplicitName() throws IOException {
    // Multiple tables in HTML - explicit name selects the largest
    String html = "<html><body>"
        + "<table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "<table><tr><th>B</th><th>C</th><th>D</th></tr><tr><td>2</td><td>3</td><td>4</td></tr></table>"
        + "</body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        file, outputDir, "UNCHANGED", "SMART_CASING",
        "biggest_table", tempDir, null);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    assertEquals("biggest_table.json", jsonFiles.get(0).getName());
  }

  // ========== convertWithSelector Tests ==========

  @Test
  public void testConvertWithSelectorNullSelector() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        file, outputDir, "UNCHANGED", "SMART_CASING",
        null, Integer.valueOf(0), "test_table", tempDir);

    // Falls back to regular conversion since selector is null
    assertNotNull(jsonFiles);
  }

  @Test
  public void testConvertWithSelectorNullIndex() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        file, outputDir, "UNCHANGED", "SMART_CASING",
        "table", null, "test_table", tempDir);

    // Falls back to regular conversion since index is null
    assertNotNull(jsonFiles);
  }

  @Test
  public void testConvertWithSelectorValidSelection() throws IOException {
    String html = "<html><body>"
        + "<table class=\"data\"><tr><th>X</th></tr><tr><td>10</td></tr></table>"
        + "<table class=\"data\"><tr><th>Y</th></tr><tr><td>20</td></tr></table>"
        + "</body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        file, outputDir, "UNCHANGED", "SMART_CASING",
        "table.data", Integer.valueOf(1), "second_table", tempDir);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    assertEquals("second_table.json", jsonFiles.get(0).getName());
  }

  @Test
  public void testConvertWithSelectorNoMatch() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        file, outputDir, "UNCHANGED", "SMART_CASING",
        "table.nonexistent", Integer.valueOf(0), "test_table", tempDir);

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.isEmpty());
  }

  @Test
  public void testConvertWithSelectorIndexOutOfBounds() throws IOException {
    String html = "<html><body>"
        + "<table class=\"wiki\"><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        file, outputDir, "UNCHANGED", "SMART_CASING",
        "table.wiki", Integer.valueOf(5), "test_table", tempDir);

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.isEmpty());
  }

  @Test
  public void testConvertWithSelectorWithFieldConfigs() throws IOException {
    String html = "<html><body>"
        + "<table class=\"data\">"
        + "<tr><th>Original Name</th><th>Skip This</th><th>Keep This</th></tr>"
        + "<tr><td>Alice</td><td>hidden</td><td>visible</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);

    List<Map<String, Object>> fieldConfigs = new ArrayList<Map<String, Object>>();

    Map<String, Object> field1 = new HashMap<String, Object>();
    field1.put("th", "Original Name");
    field1.put("name", "renamed_col");
    fieldConfigs.add(field1);

    Map<String, Object> field2 = new HashMap<String, Object>();
    field2.put("th", "Skip This");
    field2.put("skip", "true");
    fieldConfigs.add(field2);

    Map<String, Object> field3 = new HashMap<String, Object>();
    field3.put("th", "Keep This");
    field3.put("name", "kept_col");
    fieldConfigs.add(field3);

    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        file, outputDir, "UNCHANGED", "SMART_CASING",
        "table.data", Integer.valueOf(0), "mapped_table", tempDir,
        fieldConfigs);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());

    JsonNode array = MAPPER.readTree(jsonFiles.get(0));
    assertTrue(array.isArray());
    assertEquals(1, array.size());

    JsonNode row = array.get(0);
    assertTrue(row.has("renamed_col"));
    assertTrue(row.has("kept_col"));
    assertFalse(row.has("Skip This"));
    assertFalse(row.has("Original Name"));
  }

  @Test
  public void testConvertWithSelectorBaseDirectoryNull() throws IOException {
    String html = "<html><body>"
        + "<table class=\"data\"><tr><th>Col</th></tr><tr><td>val</td></tr></table>"
        + "</body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        file, outputDir, "UNCHANGED", "SMART_CASING",
        "table.data", Integer.valueOf(0), "test_table", null);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
  }

  // ========== Table Without th Elements (Default Headers) Tests ==========

  @Test
  public void testConvertTableWithoutThElements() throws IOException {
    // Table with no th elements - should generate default col0, col1, etc.
    String html = "<html><body>"
        + "<table>"
        + "<tr><td>Alice</td><td>30</td></tr>"
        + "<tr><td>Bob</td><td>25</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        file, outputDir, "UNCHANGED", "SMART_CASING", tempDir);

    assertNotNull(jsonFiles);
    assertFalse(jsonFiles.isEmpty());

    File jsonFile = jsonFiles.get(0);
    JsonNode array = MAPPER.readTree(jsonFile);
    assertTrue(array.isArray());
    // Both rows should be data rows (no header skipping)
    assertEquals(2, array.size());

    // Check that default headers are used
    JsonNode firstRow = array.get(0);
    assertTrue(firstRow.has("col0") || firstRow.has("Col0"));
  }

  // ========== Empty Rows and Cells Tests ==========

  @Test
  public void testConvertTableWithEmptyRows() throws IOException {
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>Name</th><th>Value</th></tr>"
        + "<tr><td></td><td></td></tr>"
        + "<tr><td>Alice</td><td>100</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        file, outputDir, "UNCHANGED", "SMART_CASING", tempDir);

    assertNotNull(jsonFiles);
    assertFalse(jsonFiles.isEmpty());
  }

  // ========== hasExtractedFiles Tests ==========

  @Test
  public void testHasExtractedFilesWhenEmpty() throws IOException {
    File file = createHtmlFile("<html></html>");
    assertFalse(HtmlToJsonConverter.hasExtractedFiles(file, outputDir));
  }

  @Test
  public void testHasExtractedFilesWhenExists() throws IOException {
    File htmlFile = createHtmlFileWithName("mypage.html", "<html></html>");

    // Create matching file
    File jsonFile = new File(outputDir, "mypage__table1.json");
    try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
      writer.write("[]");
    }

    assertTrue(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));
  }

  @Test
  public void testHasExtractedFilesWithNonMatchingFiles() throws IOException {
    File htmlFile = createHtmlFileWithName("mypage.html", "<html></html>");

    // Create non-matching file
    File jsonFile = new File(outputDir, "other__table1.json");
    try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
      writer.write("[]");
    }

    assertFalse(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));
  }

  // ========== sanitizeUrlForFileName Tests ==========

  @Test
  public void testSanitizeUrlForFileName() throws Exception {
    Method sanitize = HtmlToJsonConverter.class.getDeclaredMethod(
        "sanitizeUrlForFileName", String.class);
    sanitize.setAccessible(true);

    String result = (String) sanitize.invoke(null, "https://example.com/path/to/page");
    assertNotNull(result);
    assertFalse(result.contains("://"));
    // Should only contain safe characters
    assertTrue(result.matches("[a-zA-Z0-9._-]+"));
  }

  @Test
  public void testSanitizeUrlForFileNameLongUrl() throws Exception {
    Method sanitize = HtmlToJsonConverter.class.getDeclaredMethod(
        "sanitizeUrlForFileName", String.class);
    sanitize.setAccessible(true);

    // Build a very long URL
    StringBuilder longUrl = new StringBuilder("https://example.com/");
    for (int i = 0; i < 200; i++) {
      longUrl.append("path/");
    }

    String result = (String) sanitize.invoke(null, longUrl.toString());
    assertNotNull(result);
    assertTrue(result.length() <= 100);
  }

  @Test
  public void testSanitizeUrlForFileNameWithTrailingSpecialChars() throws Exception {
    Method sanitize = HtmlToJsonConverter.class.getDeclaredMethod(
        "sanitizeUrlForFileName", String.class);
    sanitize.setAccessible(true);

    String result = (String) sanitize.invoke(null, "https://example.com/page?q=1&r=2");
    assertNotNull(result);
    assertFalse(result.endsWith("_"));
  }

  // ========== extractTextFromElement Tests ==========

  @Test
  public void testExtractTextFromElementWithBrTags() throws Exception {
    Method extractText = HtmlToJsonConverter.class.getDeclaredMethod(
        "extractTextFromElement", org.jsoup.nodes.Element.class);
    extractText.setAccessible(true);

    org.jsoup.nodes.Document doc = org.jsoup.Jsoup.parse(
        "<html><body><table><tr><td>Line1<br>Line2<br>Line3</td></tr></table></body></html>");
    org.jsoup.nodes.Element td = doc.selectFirst("td");
    assertNotNull(td, "td element should be found in parsed HTML");

    String result = (String) extractText.invoke(null, td);
    assertNotNull(result);
    // br tags should be replaced with spaces
    assertTrue(result.contains("Line1"));
    assertTrue(result.contains("Line2"));
    assertTrue(result.contains("Line3"));
  }

  @Test
  public void testExtractTextFromElementCleanupMultipleSpaces() throws Exception {
    Method extractText = HtmlToJsonConverter.class.getDeclaredMethod(
        "extractTextFromElement", org.jsoup.nodes.Element.class);
    extractText.setAccessible(true);

    org.jsoup.nodes.Document doc = org.jsoup.Jsoup.parse(
        "<html><body><table><tr><td>  Hello    World  </td></tr></table></body></html>");
    org.jsoup.nodes.Element td = doc.selectFirst("td");
    assertNotNull(td, "td element should be found in parsed HTML");

    String result = (String) extractText.invoke(null, td);
    assertNotNull(result);
    // Multiple spaces should be collapsed
    assertEquals("Hello World", result);
  }

  // ========== Relative Path Tests ==========

  @Test
  public void testConvertWithRelativePath() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>Name</th></tr><tr><td>Alice</td></tr></table>"
        + "</body></html>";

    File file = createHtmlFile(html);
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        file, outputDir, "UNCHANGED", "SMART_CASING", tempDir,
        "subdir" + File.separator + "test.html");

    assertNotNull(jsonFiles);
    assertFalse(jsonFiles.isEmpty());

    // Verify directory prefix is included in filename
    String fileName = jsonFiles.get(0).getName();
    assertTrue(fileName.contains("subdir_"));
  }

  // ========== shouldSkipFirstRow Tests ==========

  @Test
  public void testShouldSkipFirstRowWithThElements() throws Exception {
    Method shouldSkip = HtmlToJsonConverter.class.getDeclaredMethod(
        "shouldSkipFirstRow", org.jsoup.nodes.Element.class, List.class);
    shouldSkip.setAccessible(true);

    org.jsoup.nodes.Document doc = org.jsoup.Jsoup.parse(
        "<table><tr><th>Name</th></tr><tr><td>Alice</td></tr></table>");
    org.jsoup.nodes.Element table = doc.selectFirst("table");

    List<String> headers = new ArrayList<String>();
    headers.add("Name");

    boolean result = (Boolean) shouldSkip.invoke(null, table, headers);
    assertTrue(result);
  }

  @Test
  public void testShouldSkipFirstRowWithoutThElements() throws Exception {
    Method shouldSkip = HtmlToJsonConverter.class.getDeclaredMethod(
        "shouldSkipFirstRow", org.jsoup.nodes.Element.class, List.class);
    shouldSkip.setAccessible(true);

    org.jsoup.nodes.Document doc = org.jsoup.Jsoup.parse(
        "<table><tr><td>Alice</td></tr><tr><td>Bob</td></tr></table>");
    org.jsoup.nodes.Element table = doc.selectFirst("table");

    List<String> headers = new ArrayList<String>();
    headers.add("col0");

    boolean result = (Boolean) shouldSkip.invoke(null, table, headers);
    assertFalse(result);
  }

  // ========== extractHeaders Tests ==========

  @Test
  public void testExtractHeadersWithThElements() throws Exception {
    Method extractHeaders = HtmlToJsonConverter.class.getDeclaredMethod(
        "extractHeaders", org.jsoup.nodes.Element.class, String.class);
    extractHeaders.setAccessible(true);

    org.jsoup.nodes.Document doc = org.jsoup.Jsoup.parse(
        "<table><tr><th>First Name</th><th>Last Name</th></tr>"
        + "<tr><td>Alice</td><td>Smith</td></tr></table>");
    org.jsoup.nodes.Element table = doc.selectFirst("table");

    @SuppressWarnings("unchecked")
    List<String> headers = (List<String>) extractHeaders.invoke(null, table, "UNCHANGED");
    assertNotNull(headers);
    assertEquals(2, headers.size());
  }

  @Test
  public void testExtractHeadersWithoutThGeneratesDefaults() throws Exception {
    Method extractHeaders = HtmlToJsonConverter.class.getDeclaredMethod(
        "extractHeaders", org.jsoup.nodes.Element.class, String.class);
    extractHeaders.setAccessible(true);

    org.jsoup.nodes.Document doc = org.jsoup.Jsoup.parse(
        "<table><tr><td>A</td><td>B</td><td>C</td></tr></table>");
    org.jsoup.nodes.Element table = doc.selectFirst("table");

    @SuppressWarnings("unchecked")
    List<String> headers = (List<String>) extractHeaders.invoke(null, table, "UNCHANGED");
    assertNotNull(headers);
    assertEquals(3, headers.size());
    // Default headers should be col0, col1, col2
    assertTrue(headers.get(0).contains("col0"));
    assertTrue(headers.get(1).contains("col1"));
    assertTrue(headers.get(2).contains("col2"));
  }

  // ========== extractRawHeaders Tests ==========

  @Test
  public void testExtractRawHeadersWithTheadTh() throws Exception {
    Method extractRawHeaders = HtmlToJsonConverter.class.getDeclaredMethod(
        "extractRawHeaders", org.jsoup.nodes.Element.class);
    extractRawHeaders.setAccessible(true);

    org.jsoup.nodes.Document doc = org.jsoup.Jsoup.parse(
        "<table><thead><tr><th>Name</th><th>Age</th></tr></thead>"
        + "<tbody><tr><td>Alice</td><td>30</td></tr></tbody></table>");
    org.jsoup.nodes.Element table = doc.selectFirst("table");

    @SuppressWarnings("unchecked")
    List<String> headers = (List<String>) extractRawHeaders.invoke(null, table);
    assertNotNull(headers);
    assertEquals(2, headers.size());
    assertEquals("Name", headers.get(0));
    assertEquals("Age", headers.get(1));
  }

  // ========== Field Config Without th Key ==========

  @Test
  public void testConvertWithFieldConfigWithoutThKey() throws IOException {
    String html = "<html><body>"
        + "<table class=\"data\">"
        + "<tr><th>Name</th><th>Value</th></tr>"
        + "<tr><td>Alice</td><td>100</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);

    List<Map<String, Object>> fieldConfigs = new ArrayList<Map<String, Object>>();
    Map<String, Object> field1 = new HashMap<String, Object>();
    // No "th" key - this field config should be skipped in mapping
    field1.put("name", "some_col");
    fieldConfigs.add(field1);

    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        file, outputDir, "UNCHANGED", "SMART_CASING",
        "table.data", Integer.valueOf(0), "test_table", tempDir,
        fieldConfigs);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
  }

  // ========== overload delegation Tests ==========

  @Test
  public void testConvert7ArgOverload() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File file = createHtmlFile(html);
    // This calls convert(file, outputDir, casing, tableCasing, baseDir, relativePath, existingTableName)
    List<File> jsonFiles = HtmlToJsonConverter.convert(
        file, outputDir, "UNCHANGED", "SMART_CASING", tempDir, null, "existing_name");

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    assertEquals("existing_name.json", jsonFiles.get(0).getName());
  }

  @Test
  public void testConvertWithSelectorWithoutFieldConfigs() throws IOException {
    String html = "<html><body>"
        + "<table class=\"wiki\"><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File file = createHtmlFile(html);
    // Uses the overload without fieldConfigs
    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        file, outputDir, "UNCHANGED", "SMART_CASING",
        "table.wiki", Integer.valueOf(0), "wiki_table", tempDir);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
  }

  // ========== Field Config th with no name (apply casing) ==========

  @Test
  public void testConvertWithFieldConfigThNoName() throws IOException {
    String html = "<html><body>"
        + "<table class=\"data\">"
        + "<tr><th>My Header</th></tr>"
        + "<tr><td>value</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);

    List<Map<String, Object>> fieldConfigs = new ArrayList<Map<String, Object>>();
    Map<String, Object> field1 = new HashMap<String, Object>();
    field1.put("th", "My Header");
    // No "name" key - should apply casing to th value
    fieldConfigs.add(field1);

    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        file, outputDir, "TO_LOWER", "SMART_CASING",
        "table.data", Integer.valueOf(0), "test_table", tempDir,
        fieldConfigs);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
  }
}
