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
 * Tests for HtmlToJsonConverter covering table extraction,
 * column name casing, selector-based conversion, and header handling.
 */
@Tag("unit")
public class HtmlToJsonConverterCoverageTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir
  java.nio.file.Path tempDir;

  private File createHtmlFile(String content) throws IOException {
    File file = new File(tempDir.toFile(), "test.html");
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }

  @Test void testConvertSimpleTable() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Name</th><th>Age</th></tr>"
        + "<tr><td>Alice</td><td>30</td></tr>"
        + "<tr><td>Bob</td><td>25</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile(html);
    File outputDir = new File(tempDir.toFile(), "output");
    outputDir.mkdirs();

    List<File> jsonFiles = HtmlToJsonConverter.convert(
        htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);

    // Verify JSON content
    File jsonFile = jsonFiles.get(0);
    assertTrue(jsonFile.exists());
    String content = Files.readString(jsonFile.toPath());
    JsonNode json = MAPPER.readTree(content);
    assertTrue(json.isArray());
    assertEquals(2, json.size());
  }

  @Test void testConvertWithColumnNameCasing() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Full Name</th><th>Age Score</th></tr>"
        + "<tr><td>Alice</td><td>30</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile(html);
    File outputDir = new File(tempDir.toFile(), "output_casing");
    outputDir.mkdirs();

    List<File> jsonFiles = HtmlToJsonConverter.convert(
        htmlFile, outputDir, "SMART_CASING", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);

    // Read and check the content
    File jsonFile = jsonFiles.get(0);
    String content = Files.readString(jsonFile.toPath());
    JsonNode json = MAPPER.readTree(content);
    assertTrue(json.isArray());
    assertEquals(1, json.size());
  }

  @Test void testConvertWithSelectorAndIndex() throws IOException {
    String html = "<html><body>"
        + "<table class='data'>"
        + "<tr><th>A</th></tr>"
        + "<tr><td>1</td></tr>"
        + "</table>"
        + "<table class='data'>"
        + "<tr><th>B</th></tr>"
        + "<tr><td>2</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createHtmlFile(html);
    File outputDir = new File(tempDir.toFile(), "output_selector");
    outputDir.mkdirs();

    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.data", 1, "selected_table", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());

    File jsonFile = jsonFiles.get(0);
    assertEquals("selected_table.json", jsonFile.getName());

    String content = Files.readString(jsonFile.toPath());
    JsonNode json = MAPPER.readTree(content);
    assertTrue(json.isArray());
    assertEquals(1, json.size());
    // Check the value from the second table
    assertEquals("2", json.get(0).get("B").asText());
  }

  @Test void testConvertWithSelectorOutOfBounds() throws IOException {
    String html = "<html><body>"
        + "<table class='data'>"
        + "<tr><th>A</th></tr>"
        + "<tr><td>1</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createHtmlFile(html);
    File outputDir = new File(tempDir.toFile(), "output_oob");
    outputDir.mkdirs();

    // Index 5 is out of bounds (only 1 table)
    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.data", 5, "selected_table", tempDir.toFile());

    // Should return empty list
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.isEmpty());
  }

  @Test void testConvertWithSelectorNoMatch() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File htmlFile = createHtmlFile(html);
    File outputDir = new File(tempDir.toFile(), "output_nomatch");
    outputDir.mkdirs();

    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table.nonexistent", 0, "selected_table", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.isEmpty());
  }

  @Test void testConvertWithExplicitTableName() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Name</th></tr>"
        + "<tr><td>Alice</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile(html);
    File outputDir = new File(tempDir.toFile(), "output_explicit");
    outputDir.mkdirs();

    List<File> jsonFiles = HtmlToJsonConverter.convert(
        htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "my_custom_table", tempDir.toFile(), null);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());
    assertEquals("my_custom_table.json", jsonFiles.get(0).getName());
  }

  @Test void testConvertWithFieldMappings() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Original Header</th><th>Skip Me</th><th>Keep This</th></tr>"
        + "<tr><td>value1</td><td>skip</td><td>value2</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile(html);
    File outputDir = new File(tempDir.toFile(), "output_fields");
    outputDir.mkdirs();

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();

    Map<String, Object> field1 = new HashMap<>();
    field1.put("th", "Original Header");
    field1.put("name", "renamed_field");
    fieldConfigs.add(field1);

    Map<String, Object> field2 = new HashMap<>();
    field2.put("th", "Skip Me");
    field2.put("skip", "true");
    fieldConfigs.add(field2);

    Map<String, Object> field3 = new HashMap<>();
    field3.put("th", "Keep This");
    fieldConfigs.add(field3);

    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        "table", 0, "mapped_table", tempDir.toFile(), fieldConfigs);

    assertNotNull(jsonFiles);
    assertEquals(1, jsonFiles.size());

    String content = Files.readString(jsonFiles.get(0).toPath());
    JsonNode json = MAPPER.readTree(content);
    assertTrue(json.isArray());
    assertEquals(1, json.size());

    JsonNode row = json.get(0);
    assertTrue(row.has("renamed_field"),
        "Expected 'renamed_field' in JSON: " + row);
    assertFalse(row.has("Skip Me"),
        "Should not have 'Skip Me' in JSON: " + row);
    // "Keep This" may have casing applied
    // Check that we have 2 fields (renamed_field + Keep This with UNCHANGED casing)
    assertEquals(2, row.size(),
        "Should have 2 fields after skipping: " + row);
  }

  @Test void testHasExtractedFiles() throws IOException {
    File htmlFile = new File(tempDir.toFile(), "test.html");
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write("<html><body></body></html>");
    }

    File outputDir = new File(tempDir.toFile(), "check_output");
    outputDir.mkdirs();

    // No files yet
    assertFalse(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));

    // Create a matching file
    File matching = new File(outputDir, "test__table1.json");
    try (FileWriter writer = new FileWriter(matching)) {
      writer.write("[]");
    }

    assertTrue(HtmlToJsonConverter.hasExtractedFiles(htmlFile, outputDir));
  }

  @Test void testTableWithoutHeaders() throws IOException {
    // Table with only td elements, no th
    String html = "<html><body><table>"
        + "<tr><td>a</td><td>b</td></tr>"
        + "<tr><td>c</td><td>d</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile(html);
    File outputDir = new File(tempDir.toFile(), "output_noheaders");
    outputDir.mkdirs();

    List<File> jsonFiles = HtmlToJsonConverter.convert(
        htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);

    // Should have auto-generated headers
    File jsonFile = jsonFiles.get(0);
    String content = Files.readString(jsonFile.toPath());
    JsonNode json = MAPPER.readTree(content);
    assertTrue(json.isArray());
    // All rows should be data (no header row consumed)
    assertTrue(json.size() >= 2);
  }

  @Test void testConvertMultipleTables() throws IOException {
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>T1</th></tr>"
        + "<tr><td>a</td></tr>"
        + "</table>"
        + "<table>"
        + "<tr><th>T2</th></tr>"
        + "<tr><td>b</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createHtmlFile(html);
    File outputDir = new File(tempDir.toFile(), "output_multi");
    outputDir.mkdirs();

    List<File> jsonFiles = HtmlToJsonConverter.convert(
        htmlFile, outputDir, "UNCHANGED", "SMART_CASING", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
  }

  @Test void testConvertWithBreakTags() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Name</th></tr>"
        + "<tr><td>First<br>Second</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile(html);
    File outputDir = new File(tempDir.toFile(), "output_br");
    outputDir.mkdirs();

    List<File> jsonFiles = HtmlToJsonConverter.convert(
        htmlFile, outputDir, tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);

    // The br tag should be converted to a space
    File jsonFile = jsonFiles.get(0);
    String content = Files.readString(jsonFile.toPath());
    JsonNode json = MAPPER.readTree(content);
    // Check that the text is properly extracted
    assertTrue(json.isArray());
    assertTrue(json.size() > 0);
  }

  @Test void testSanitizeUrlForFileName() throws Exception {
    // Test the private sanitizeUrlForFileName method via reflection
    java.lang.reflect.Method method = HtmlToJsonConverter.class.getDeclaredMethod(
        "sanitizeUrlForFileName", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(null, "https://example.com/path/to/page?key=value");
    assertNotNull(result);
    // Should not contain protocol
    assertFalse(result.contains("https"));
    // Should have replaced special chars
    assertFalse(result.contains("?"));
    assertFalse(result.contains("="));
  }

  @Test void testSanitizeUrlForFileNameLongUrl() throws Exception {
    java.lang.reflect.Method method = HtmlToJsonConverter.class.getDeclaredMethod(
        "sanitizeUrlForFileName", String.class);
    method.setAccessible(true);

    // Create a very long URL
    StringBuilder url = new StringBuilder("https://example.com/");
    for (int i = 0; i < 200; i++) {
      url.append("segment/");
    }

    String result = (String) method.invoke(null, url.toString());
    // Should be limited to 100 characters
    assertTrue(result.length() <= 100);
  }

  @Test void testConvertWithNullSelectorFallsBack() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Name</th></tr>"
        + "<tr><td>Alice</td></tr>"
        + "</table></body></html>";

    File htmlFile = createHtmlFile(html);
    File outputDir = new File(tempDir.toFile(), "output_null_sel");
    outputDir.mkdirs();

    // Null selector and index should fall back to regular conversion
    List<File> jsonFiles = HtmlToJsonConverter.convertWithSelector(
        htmlFile, outputDir, "UNCHANGED", "SMART_CASING",
        null, null, "table_name", tempDir.toFile());

    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.size() >= 1);
  }
}
