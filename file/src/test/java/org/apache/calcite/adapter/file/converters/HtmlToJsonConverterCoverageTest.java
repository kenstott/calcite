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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive tests for {@link HtmlToJsonConverter} covering table extraction,
 * column name casing, selector-based conversion, header extraction, text extraction,
 * and skip-row logic.
 */
@Tag("unit")
public class HtmlToJsonConverterCoverageTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir
  Path tempDir;

  // ========== convert(File, File, File) - basic 3-arg version ==========

  @Test void testConvertSimpleTableWithHeaders() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Name</th><th>Age</th></tr>"
        + "<tr><td>Alice</td><td>30</td></tr>"
        + "<tr><td>Bob</td><td>25</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("simple.html", html);
    File outputDir = createOutputDir("out_simple");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    File jsonFile = jsonFiles.get(0);
    assertTrue(jsonFile.exists());
    JsonNode json = MAPPER.readTree(Files.readString(jsonFile.toPath()));
    assertTrue(json.isArray());
    assertEquals(2, json.size());
  }

  @Test void testConvertTableWithNumericValues() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>id</th><th>score</th></tr>"
        + "<tr><td>1</td><td>95.5</td></tr>"
        + "<tr><td>2</td><td>87.3</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("numeric.html", html);
    File outputDir = createOutputDir("out_numeric");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertEquals(2, json.size());
  }

  @Test void testConvertTableSingleRow() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Key</th><th>Value</th></tr>"
        + "<tr><td>config</td><td>enabled</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("single_row.html", html);
    File outputDir = createOutputDir("out_single_row");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertEquals(1, json.size());
  }

  @Test void testConvertTableHeaderOnly() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>A</th><th>B</th></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("header_only.html", html);
    File outputDir = createOutputDir("out_header_only");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    // Only header row, no data rows
    assertEquals(0, json.size());
  }

  @Test void testConvertCreatesOutputDirectory() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>X</th></tr>"
        + "<tr><td>1</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("autocreate.html", html);
    File outputDir = new File(tempDir.toFile(), "auto_created_dir");
    // outputDir does not exist yet

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertTrue(outputDir.exists());
    assertNotNull(jsonFiles);
  }

  // ========== convert with column name casing ==========

  @Test void testConvertWithSmartCasing() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Full Name</th><th>Age Score</th></tr>"
        + "<tr><td>Alice</td><td>30</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("casing.html", html);
    File outputDir = createOutputDir("out_casing");

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "SMART_CASING", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertTrue(json.isArray());
    assertEquals(1, json.size());
  }

  @Test void testConvertWithUnchangedCasing() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>MyColumn</th><th>AnotherColumn</th></tr>"
        + "<tr><td>val1</td><td>val2</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("unchanged.html", html);
    File outputDir = createOutputDir("out_unchanged");

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertTrue(json.isArray());
    JsonNode row = json.get(0);
    assertTrue(row.has("MyColumn"), "Expected 'MyColumn' in: " + row);
    assertTrue(row.has("AnotherColumn"), "Expected 'AnotherColumn' in: " + row);
  }

  // ========== convert with table name casing ==========

  @Test void testConvertWithTableNameCasing() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Col</th></tr>"
        + "<tr><td>val</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("tablecasing.html", html);
    File outputDir = createOutputDir("out_tablecasing");

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
  }

  // ========== convert with explicit table name ==========

  @Test void testConvertWithExplicitTableName() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Name</th></tr>"
        + "<tr><td>Alice</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("explicit.html", html);
    File outputDir = createOutputDir("out_explicit");

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "my_custom_table", tempDir.toFile(), null);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    assertEquals("my_custom_table.json", jsonFiles.get(0).getName());
  }

  @Test void testConvertWithExplicitTableNameMultipleTables() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "<table><tr><th>B</th><th>C</th><th>D</th></tr>"
        + "<tr><td>2</td><td>3</td><td>4</td></tr></table>"
        + "</body></html>";

    File htmlFile = createHtmlFile("multi_explicit.html", html);
    File outputDir = createOutputDir("out_multi_explicit");

    // With explicit name and multiple tables, should pick the largest table
    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "selected", tempDir.toFile(), null);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    assertEquals("selected.json", jsonFiles.get(0).getName());
  }

  // ========== convert with relative path ==========

  @Test void testConvertWithRelativePath() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Col</th></tr>"
        + "<tr><td>val</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("relpath.html", html);
    File outputDir = createOutputDir("out_relpath");

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        tempDir.toFile(), "subdir" + File.separator + "relpath.html");

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    // JSON filename should include the directory prefix
    String jsonName = jsonFiles.get(0).getName();
    assertTrue(jsonName.contains("subdir"), "Expected directory prefix in: " + jsonName);
  }

  @Test void testConvertWithSimpleRelativePath() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Col</th></tr>"
        + "<tr><td>val</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("simple_rel.html", html);
    File outputDir = createOutputDir("out_simple_rel");

    // Relative path without separator should not add prefix
    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        tempDir.toFile(), "simple_rel.html");

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
  }

  // ========== Multiple tables ==========

  @Test void testConvertMultipleTables() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>T1</th></tr><tr><td>a</td></tr></table>"
        + "<table><tr><th>T2</th></tr><tr><td>b</td></tr></table>"
        + "</body></html>";

    File htmlFile = createHtmlFile("multi.html", html);
    File outputDir = createOutputDir("out_multi");

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
  }

  @Test void testConvertThreeTables() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "<table><tr><th>B</th></tr><tr><td>2</td></tr></table>"
        + "<table><tr><th>C</th></tr><tr><td>3</td></tr></table>"
        + "</body></html>";

    File htmlFile = createHtmlFile("three.html", html);
    File outputDir = createOutputDir("out_three");

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
  }

  // ========== hasExtractedFiles tests ==========

  @Test void testHasExtractedFilesNoFiles() throws IOException {
    File htmlFile = createHtmlFile("check.html", "<html></html>");
    File outputDir = createOutputDir("out_check");

    assertFalse(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));
  }

  @Test void testHasExtractedFilesWithMatchingFile() throws IOException {
    File htmlFile = createHtmlFile("check2.html", "<html></html>");
    File outputDir = createOutputDir("out_check2");

    // Create a matching file
    File matching = new File(outputDir, "check2__table1.json");
    try (FileWriter writer = new FileWriter(matching)) {
      writer.write("[]");
    }

    assertTrue(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));
  }

  @Test void testHasExtractedFilesWithNonMatchingFile() throws IOException {
    File htmlFile = createHtmlFile("check3.html", "<html></html>");
    File outputDir = createOutputDir("out_check3");

    // Create a non-matching file
    File nonMatching = new File(outputDir, "other_file.json");
    try (FileWriter writer = new FileWriter(nonMatching)) {
      writer.write("[]");
    }

    assertFalse(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));
  }

  @Test void testHasExtractedFilesWithHtmExtension() throws IOException {
    File htmlFile = new File(tempDir.toFile(), "page.htm");
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write("<html></html>");
    }
    File outputDir = createOutputDir("out_htm");

    // Create a matching file for .htm
    File matching = new File(outputDir, "page__table1.json");
    try (FileWriter writer = new FileWriter(matching)) {
      writer.write("[]");
    }

    assertTrue(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));
  }

  @Test void testHasExtractedFilesNonexistentDirectory() throws IOException {
    File htmlFile = createHtmlFile("nodir.html", "<html></html>");
    File outputDir = new File(tempDir.toFile(), "nonexistent_dir");

    // outputDir does not exist, listFiles returns null
    assertFalse(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));
  }

  // ========== extractHeaders tests (via reflection) ==========

  @Test void testExtractHeadersWithThElements() throws Exception {
    Document doc = Jsoup.parse("<table>"
        + "<tr><th>Name</th><th>Age</th><th>City</th></tr>"
        + "<tr><td>Alice</td><td>30</td><td>NYC</td></tr>"
        + "</table>");
    Element table = doc.select("table").first();

    List<String> headers = invokeExtractHeaders(table, "UNCHANGED");
    assertEquals(3, headers.size());
    assertEquals("Name", headers.get(0));
    assertEquals("Age", headers.get(1));
    assertEquals("City", headers.get(2));
  }

  @Test void testExtractHeadersWithThead() throws Exception {
    Document doc = Jsoup.parse("<table>"
        + "<thead><tr><th>Col1</th><th>Col2</th></tr></thead>"
        + "<tbody><tr><td>a</td><td>b</td></tr></tbody>"
        + "</table>");
    Element table = doc.select("table").first();

    List<String> headers = invokeExtractHeaders(table, "UNCHANGED");
    assertEquals(2, headers.size());
    assertEquals("Col1", headers.get(0));
    assertEquals("Col2", headers.get(1));
  }

  @Test void testExtractHeadersNoThGeneratesDefaults() throws Exception {
    Document doc = Jsoup.parse("<table>"
        + "<tr><td>a</td><td>b</td><td>c</td></tr>"
        + "<tr><td>d</td><td>e</td><td>f</td></tr>"
        + "</table>");
    Element table = doc.select("table").first();

    List<String> headers = invokeExtractHeaders(table, "UNCHANGED");
    assertEquals(3, headers.size());
    // Default headers are col0, col1, col2
    assertEquals("col0", headers.get(0));
    assertEquals("col1", headers.get(1));
    assertEquals("col2", headers.get(2));
  }

  @Test void testExtractHeadersSmartCasing() throws Exception {
    Document doc = Jsoup.parse("<table>"
        + "<tr><th>Full Name</th><th>Date Of Birth</th></tr>"
        + "<tr><td>Alice</td><td>2000-01-01</td></tr>"
        + "</table>");
    Element table = doc.select("table").first();

    List<String> headers = invokeExtractHeaders(table, "SMART_CASING");
    assertEquals(2, headers.size());
    // SmartCasing should transform the header text
    assertNotNull(headers.get(0));
    assertNotNull(headers.get(1));
  }

  @Test void testExtractHeadersEmptyTable() throws Exception {
    Document doc = Jsoup.parse("<table></table>");
    Element table = doc.select("table").first();

    List<String> headers = invokeExtractHeaders(table, "UNCHANGED");
    assertEquals(0, headers.size());
  }

  @Test void testExtractHeadersSingleColumn() throws Exception {
    Document doc = Jsoup.parse("<table>"
        + "<tr><th>OnlyColumn</th></tr>"
        + "<tr><td>value</td></tr>"
        + "</table>");
    Element table = doc.select("table").first();

    List<String> headers = invokeExtractHeaders(table, "UNCHANGED");
    assertEquals(1, headers.size());
    assertEquals("OnlyColumn", headers.get(0));
  }

  // ========== shouldSkipFirstRow tests (via reflection) ==========

  @Test void testShouldSkipFirstRowWithThElements() throws Exception {
    Document doc = Jsoup.parse("<table>"
        + "<tr><th>A</th><th>B</th></tr>"
        + "<tr><td>1</td><td>2</td></tr>"
        + "</table>");
    Element table = doc.select("table").first();

    boolean result = invokeShouldSkipFirstRow(table, Arrays.asList("A", "B"));
    assertTrue(result);
  }

  @Test void testShouldSkipFirstRowWithTdOnly() throws Exception {
    Document doc = Jsoup.parse("<table>"
        + "<tr><td>a</td><td>b</td></tr>"
        + "<tr><td>c</td><td>d</td></tr>"
        + "</table>");
    Element table = doc.select("table").first();

    boolean result = invokeShouldSkipFirstRow(table, Arrays.asList("col0", "col1"));
    assertFalse(result);
  }

  @Test void testShouldSkipFirstRowWithThead() throws Exception {
    Document doc = Jsoup.parse("<table>"
        + "<thead><tr><th>X</th></tr></thead>"
        + "<tbody><tr><td>1</td></tr></tbody>"
        + "</table>");
    Element table = doc.select("table").first();

    boolean result = invokeShouldSkipFirstRow(table, Collections.singletonList("X"));
    assertTrue(result);
  }

  @Test void testShouldSkipFirstRowEmptyTable() throws Exception {
    Document doc = Jsoup.parse("<table></table>");
    Element table = doc.select("table").first();

    boolean result = invokeShouldSkipFirstRow(table, Collections.emptyList());
    assertFalse(result);
  }

  // ========== extractTextFromElement tests (via reflection) ==========

  @Test void testExtractTextSimple() throws Exception {
    Element td = parseTd("<td>Hello World</td>");

    String result = invokeExtractTextFromElement(td);
    assertEquals("Hello World", result);
  }

  @Test void testExtractTextWithBrTag() throws Exception {
    Element td = parseTd("<td>First<br>Second</td>");

    String result = invokeExtractTextFromElement(td);
    assertTrue(result.contains("First"));
    assertTrue(result.contains("Second"));
    // br should be converted to space
    assertFalse(result.contains("<br>"));
  }

  @Test void testExtractTextWithMultipleBrTags() throws Exception {
    Element td = parseTd("<td>A<br>B<br>C</td>");

    String result = invokeExtractTextFromElement(td);
    assertTrue(result.contains("A"));
    assertTrue(result.contains("B"));
    assertTrue(result.contains("C"));
  }

  @Test void testExtractTextWithNestedElements() throws Exception {
    Element td = parseTd("<td><span>Inner</span> text</td>");

    String result = invokeExtractTextFromElement(td);
    assertEquals("Inner text", result);
  }

  @Test void testExtractTextWithLink() throws Exception {
    Element td = parseTd("<td><a href='http://example.com'>Link Text</a></td>");

    String result = invokeExtractTextFromElement(td);
    assertEquals("Link Text", result);
  }

  @Test void testExtractTextWithWhitespace() throws Exception {
    Element td = parseTd("<td>  lots   of   spaces  </td>");

    String result = invokeExtractTextFromElement(td);
    assertEquals("lots of spaces", result);
  }

  @Test void testExtractTextEmpty() throws Exception {
    Element td = parseTd("<td></td>");

    String result = invokeExtractTextFromElement(td);
    assertEquals("", result);
  }

  @Test void testExtractTextWithBoldAndItalic() throws Exception {
    Element td = parseTd("<td><b>Bold</b> and <i>Italic</i></td>");

    String result = invokeExtractTextFromElement(td);
    assertEquals("Bold and Italic", result);
  }

  @Test void testExtractTextWithSupTag() throws Exception {
    Element td = parseTd("<td>Text<sup>[1]</sup></td>");

    String result = invokeExtractTextFromElement(td);
    assertTrue(result.contains("Text"));
    assertTrue(result.contains("[1]"));
  }

  // ========== convertWithSelector tests ==========

  @Test void testConvertWithSelectorAndIndex() throws IOException {
    String html = "<html><body>"
        + "<table class='data'><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "<table class='data'><tr><th>B</th></tr><tr><td>2</td></tr></table>"
        + "</body></html>";

    File htmlFile = createHtmlFile("selector.html", html);
    File outputDir = createOutputDir("out_selector");

    List<File> jsonFiles =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.data", 1, "selected_table", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    assertEquals("selected_table.json", jsonFiles.get(0).getName());

    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertTrue(json.isArray());
    assertEquals(1, json.size());
    assertEquals("2", json.get(0).get("B").asText());
  }

  @Test void testConvertWithSelectorOutOfBounds() throws IOException {
    String html = "<html><body>"
        + "<table class='data'><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File htmlFile = createHtmlFile("oob.html", html);
    File outputDir = createOutputDir("out_oob");

    List<File> jsonFiles =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.data", 5, "selected_table", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.isEmpty());
  }

  @Test void testConvertWithSelectorNoMatch() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File htmlFile = createHtmlFile("nomatch.html", html);
    File outputDir = createOutputDir("out_nomatch");

    List<File> jsonFiles =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.nonexistent", 0, "selected_table", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.isEmpty());
  }

  @Test void testConvertWithNullSelectorFallback() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Name</th></tr><tr><td>Alice</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("nullsel.html", html);
    File outputDir = createOutputDir("out_nullsel");

    List<File> jsonFiles =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        null, null, "table_name", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
  }

  @Test void testConvertWithSelectorFirstTable() throws IOException {
    String html = "<html><body>"
        + "<table class='target'><tr><th>First</th></tr><tr><td>value1</td></tr></table>"
        + "<table class='target'><tr><th>Second</th></tr><tr><td>value2</td></tr></table>"
        + "</body></html>";

    File htmlFile = createHtmlFile("firstsel.html", html);
    File outputDir = createOutputDir("out_firstsel");

    List<File> jsonFiles =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.target", 0, "first_table", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertEquals("value1", json.get(0).get("First").asText());
  }

  // ========== Field mapping tests ==========

  @Test void testConvertWithFieldMappingsRename() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Original Header</th><th>Keep This</th></tr>"
        + "<tr><td>value1</td><td>value2</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("rename.html", html);
    File outputDir = createOutputDir("out_rename");

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> field1 = new HashMap<>();
    field1.put("th", "Original Header");
    field1.put("name", "renamed_field");
    fieldConfigs.add(field1);

    List<File> jsonFiles =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table", 0, "mapped_table", tempDir.toFile(), fieldConfigs);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    JsonNode row = json.get(0);
    assertTrue(row.has("renamed_field"), "Expected 'renamed_field' in: " + row);
  }

  @Test void testConvertWithFieldMappingsSkip() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Keep</th><th>Skip Me</th><th>Also Keep</th></tr>"
        + "<tr><td>a</td><td>b</td><td>c</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("skip.html", html);
    File outputDir = createOutputDir("out_skip");

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> skipField = new HashMap<>();
    skipField.put("th", "Skip Me");
    skipField.put("skip", "true");
    fieldConfigs.add(skipField);

    List<File> jsonFiles =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table", 0, "skip_table", tempDir.toFile(), fieldConfigs);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    JsonNode row = json.get(0);
    assertFalse(row.has("Skip Me"), "Should not have 'Skip Me' in: " + row);
    assertEquals(2, row.size());
  }

  @Test void testConvertWithFieldMappingsRenameAndSkip() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Original</th><th>Remove</th><th>Keep</th></tr>"
        + "<tr><td>v1</td><td>v2</td><td>v3</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("combo.html", html);
    File outputDir = createOutputDir("out_combo");

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();

    Map<String, Object> renameField = new HashMap<>();
    renameField.put("th", "Original");
    renameField.put("name", "new_name");
    fieldConfigs.add(renameField);

    Map<String, Object> skipField = new HashMap<>();
    skipField.put("th", "Remove");
    skipField.put("skip", "true");
    fieldConfigs.add(skipField);

    Map<String, Object> keepField = new HashMap<>();
    keepField.put("th", "Keep");
    fieldConfigs.add(keepField);

    List<File> jsonFiles =
        HtmlToJsonConverter.convertWithSelector(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table", 0, "combo_table", tempDir.toFile(), fieldConfigs);

    assertNotNull(jsonFiles);
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    JsonNode row = json.get(0);
    assertTrue(row.has("new_name"));
    assertFalse(row.has("Remove"));
    assertTrue(row.has("Keep"));
    assertEquals(2, row.size());
  }

  // ========== Table without headers (td only) ==========

  @Test void testConvertTableWithoutHeaders() throws IOException {
    String html = "<html><body><table>"
        + "<tr><td>a</td><td>b</td></tr>"
        + "<tr><td>c</td><td>d</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("noheaders.html", html);
    File outputDir = createOutputDir("out_noheaders");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertTrue(json.isArray());
    // Both rows should be data rows (no header consumed)
    assertTrue(json.size() >= 2);
  }

  @Test void testConvertTableWithoutHeadersDefaultColumnNames() throws IOException {
    String html = "<html><body><table>"
        + "<tr><td>val1</td><td>val2</td><td>val3</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("defaults.html", html);
    File outputDir = createOutputDir("out_defaults");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    // Auto-generated headers should be col0, col1, col2
    JsonNode row = json.get(0);
    assertTrue(row.has("col0") || row.has("col_0"),
        "Expected default column name in: " + row);
  }

  // ========== br tag and HTML content handling ==========

  @Test void testConvertWithBreakTags() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Description</th></tr>"
        + "<tr><td>Line One<br>Line Two<br>Line Three</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("br.html", html);
    File outputDir = createOutputDir("out_br");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertTrue(json.isArray());
    String value = json.get(0).get("Description").asText();
    assertTrue(value.contains("Line One"), "Expected 'Line One' in: " + value);
    assertTrue(value.contains("Line Two"), "Expected 'Line Two' in: " + value);
    assertTrue(value.contains("Line Three"), "Expected 'Line Three' in: " + value);
  }

  @Test void testConvertWithNestedHtml() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Content</th></tr>"
        + "<tr><td><b>Bold</b> <i>Italic</i> <a href='#'>Link</a></td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("nested.html", html);
    File outputDir = createOutputDir("out_nested");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    String content = json.get(0).get("Content").asText();
    assertTrue(content.contains("Bold"));
    assertTrue(content.contains("Italic"));
    assertTrue(content.contains("Link"));
  }

  // ========== sanitizeUrlForFileName tests (via reflection) ==========

  @Test void testSanitizeUrlForFileName() throws Exception {
    String result = invokeSanitizeUrlForFileName("https://example.com/path/to/page?key=value");
    assertNotNull(result);
    assertFalse(result.contains("https"));
    assertFalse(result.contains("?"));
    assertFalse(result.contains("="));
  }

  @Test void testSanitizeUrlForFileNameLongUrl() throws Exception {
    StringBuilder url = new StringBuilder("https://example.com/");
    for (int i = 0; i < 200; i++) {
      url.append("segment/");
    }
    String result = invokeSanitizeUrlForFileName(url.toString());
    assertTrue(result.length() <= 100);
  }

  @Test void testSanitizeUrlForFileNameSimple() throws Exception {
    String result = invokeSanitizeUrlForFileName("https://example.com");
    assertEquals("example.com", result);
  }

  @Test void testSanitizeUrlForFileNameHttpUrl() throws Exception {
    String result = invokeSanitizeUrlForFileName("http://test.org/data");
    assertNotNull(result);
    assertFalse(result.startsWith("http"));
    assertTrue(result.contains("test.org"));
  }

  @Test void testSanitizeUrlForFileNameSpecialChars() throws Exception {
    String result = invokeSanitizeUrlForFileName("https://example.com/a&b=c#d");
    assertNotNull(result);
    assertFalse(result.contains("&"));
    assertFalse(result.contains("#"));
  }

  // ========== convert with null baseDirectory ==========

  @Test void testConvertWithNullBaseDirectory() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Col</th></tr>"
        + "<tr><td>val</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("nullbase.html", html);
    File outputDir = createOutputDir("out_nullbase");

    // null baseDirectory should still work (uses ConversionRecorder instead)
    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "explicit_name", null, null);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
  }

  // ========== existingTableName overload ==========

  @Test void testConvertWithExistingTableName() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Col</th></tr>"
        + "<tr><td>val</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("existing.html", html);
    File outputDir = createOutputDir("out_existing");

    List<File> jsonFiles =
        HtmlToJsonConverter.convert(htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        tempDir.toFile(), null, "previous_name");

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
  }

  // ========== Table with empty rows ==========

  @Test void testConvertTableWithEmptyRows() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>A</th><th>B</th></tr>"
        + "<tr></tr>"
        + "<tr><td>1</td><td>2</td></tr>"
        + "<tr></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("emptyrows.html", html);
    File outputDir = createOutputDir("out_emptyrows");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    // Empty rows should be skipped
    assertEquals(1, json.size());
  }

  // ========== Table with th in body rows (row headers) ==========

  @Test void testConvertTableWithThInDataRows() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Category</th><th>Value</th></tr>"
        + "<tr><th>Row1</th><td>100</td></tr>"
        + "<tr><th>Row2</th><td>200</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile("rowheaders.html", html);
    File outputDir = createOutputDir("out_rowheaders");

    List<File> jsonFiles = HtmlToJsonConverter.convert(htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles.get(0).toPath()));
    assertTrue(json.size() >= 2);
  }

  // ========== Reflection helper methods ==========

  @SuppressWarnings("unchecked")
  private List<String> invokeExtractHeaders(Element table, String casing) throws Exception {
    Method method =
        HtmlToJsonConverter.class.getDeclaredMethod("extractHeaders", Element.class, String.class);
    method.setAccessible(true);
    return (List<String>) method.invoke(null, table, casing);
  }

  private boolean invokeShouldSkipFirstRow(Element table, List<String> headers) throws Exception {
    Method method =
        HtmlToJsonConverter.class.getDeclaredMethod("shouldSkipFirstRow", Element.class, List.class);
    method.setAccessible(true);
    return (boolean) method.invoke(null, table, headers);
  }

  private String invokeExtractTextFromElement(Element element) throws Exception {
    Method method =
        HtmlToJsonConverter.class.getDeclaredMethod("extractTextFromElement", Element.class);
    method.setAccessible(true);
    return (String) method.invoke(null, element);
  }

  private String invokeSanitizeUrlForFileName(String url) throws Exception {
    Method method =
        HtmlToJsonConverter.class.getDeclaredMethod("sanitizeUrlForFileName", String.class);
    method.setAccessible(true);
    return (String) method.invoke(null, url);
  }

  /**
   * Parses an HTML td fragment by wrapping it in a proper table structure,
   * since Jsoup requires td to be inside a table context for correct parsing.
   */
  private Element parseTd(String tdHtml) {
    Document doc = Jsoup.parse("<html><body><table><tr>" + tdHtml + "</tr></table></body></html>");
    return doc.select("td").first();
  }

  // ========== File creation helpers ==========

  private File createHtmlFile(String name, String content) throws IOException {
    File file = new File(tempDir.toFile(), name);
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }

  private File createOutputDir(String name) {
    File dir = new File(tempDir.toFile(), name);
    dir.mkdirs();
    return dir;
  }
}
