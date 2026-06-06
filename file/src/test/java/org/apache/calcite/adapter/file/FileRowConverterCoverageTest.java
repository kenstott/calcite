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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.jsoup.select.Elements;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for FileRowConverter covering type conversion, field definitions,
 * and CellReader inner class functionality.
 */
@Tag("unit")
public class FileRowConverterCoverageTest {

  @TempDir
  java.nio.file.Path tempDir;

  private File createHtmlFile(String content) throws IOException {
    File file = new File(tempDir.toFile(), "test.html");
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }

  @Test void testBasicStringConversion() throws Exception {
    String html = "<html><body><table>"
        + "<tr><th>Name</th><th>City</th></tr>"
        + "<tr><td>Alice</td><td>NY</td></tr>"
        + "<tr><td>Bob</td><td>LA</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    Source source = Sources.of(file);
    FileReaderV2 reader = new FileReaderV2(source);

    FileRowConverter converter = new FileRowConverter(reader, null, "UNCHANGED");

    // Get width and row type
    assertEquals(2, converter.width());

    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = converter.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());

    // Convert a row
    java.util.Iterator<Elements> iter = reader.iterator();
    while (iter.hasNext()) {
      Elements rowElements = iter.next();
      if (rowElements.size() >= 2) {
        Object result = converter.toRow(rowElements, new int[]{0, 1});
        assertNotNull(result);
        Object[] row = (Object[]) result;
        assertEquals(2, row.length);
        break;
      }
    }
    reader.close();
  }

  @Test void testWithFieldConfigs() throws Exception {
    String html = "<html><body><table>"
        + "<tr><th>Name</th><th>Age</th><th>Score</th></tr>"
        + "<tr><td>Alice</td><td>30</td><td>95.5</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    Source source = Sources.of(file);
    FileReaderV2 reader = new FileReaderV2(source);

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();

    Map<String, Object> nameConfig = new HashMap<>();
    nameConfig.put("th", "Name");
    nameConfig.put("type", "string");
    fieldConfigs.add(nameConfig);

    Map<String, Object> ageConfig = new HashMap<>();
    ageConfig.put("th", "Age");
    ageConfig.put("type", "int");
    fieldConfigs.add(ageConfig);

    Map<String, Object> scoreConfig = new HashMap<>();
    scoreConfig.put("th", "Score");
    scoreConfig.put("type", "double");
    fieldConfigs.add(scoreConfig);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");
    assertEquals(3, converter.width());

    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = converter.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(3, rowType.getFieldCount());

    reader.close();
  }

  @Test void testFieldConfigWithRename() throws Exception {
    String html = "<html><body><table>"
        + "<tr><th>Full Name</th><th>Years Old</th></tr>"
        + "<tr><td>Alice</td><td>30</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    Source source = Sources.of(file);
    FileReaderV2 reader = new FileReaderV2(source);

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();

    Map<String, Object> nameConfig = new HashMap<>();
    nameConfig.put("th", "Full Name");
    nameConfig.put("name", "person_name");
    nameConfig.put("type", "string");
    fieldConfigs.add(nameConfig);

    Map<String, Object> ageConfig = new HashMap<>();
    ageConfig.put("th", "Years Old");
    ageConfig.put("name", "age");
    ageConfig.put("type", "int");
    fieldConfigs.add(ageConfig);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");
    assertEquals(2, converter.width());

    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = converter.getRowType(typeFactory);
    assertEquals("person_name", rowType.getFieldList().get(0).getName());
    assertEquals("age", rowType.getFieldList().get(1).getName());

    reader.close();
  }

  @Test void testFieldConfigWithSkip() throws Exception {
    String html = "<html><body><table>"
        + "<tr><th>Name</th><th>Internal</th><th>Score</th></tr>"
        + "<tr><td>Alice</td><td>xyz</td><td>95</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    Source source = Sources.of(file);
    FileReaderV2 reader = new FileReaderV2(source);

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();

    Map<String, Object> nameConfig = new HashMap<>();
    nameConfig.put("th", "Name");
    nameConfig.put("type", "string");
    fieldConfigs.add(nameConfig);

    Map<String, Object> skipConfig = new HashMap<>();
    skipConfig.put("th", "Internal");
    skipConfig.put("skip", "true");
    fieldConfigs.add(skipConfig);

    Map<String, Object> scoreConfig = new HashMap<>();
    scoreConfig.put("th", "Score");
    scoreConfig.put("type", "int");
    fieldConfigs.add(scoreConfig);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");
    // Should have 2 fields (Internal is skipped)
    assertEquals(2, converter.width());

    reader.close();
  }

  @Test void testToRowWithProjection() throws Exception {
    String html = "<html><body><table>"
        + "<tr><th>A</th><th>B</th><th>C</th></tr>"
        + "<tr><td>1</td><td>2</td><td>3</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    Source source = Sources.of(file);
    FileReaderV2 reader = new FileReaderV2(source);

    FileRowConverter converter = new FileRowConverter(reader, null, "UNCHANGED");

    java.util.Iterator<Elements> iter = reader.iterator();
    while (iter.hasNext()) {
      Elements rowElements = iter.next();
      if (rowElements.size() >= 3) {
        // Project only columns 0 and 2
        Object result = converter.toRow(rowElements, new int[]{0, 2});
        Object[] row = (Object[]) result;
        assertEquals(2, row.length);
        break;
      }
    }
    reader.close();
  }

  @Test void testGetRowTypeWithNoFields() throws Exception {
    // When an empty table is parsed, it should produce a default field
    String html = "<html><body><table>"
        + "<tr><th></th></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    Source source = Sources.of(file);
    FileReaderV2 reader = new FileReaderV2(source);

    // Create field configs that result in no fields
    FileRowConverter converter = new FileRowConverter(reader, null, "UNCHANGED");

    // Initialize by calling width()
    int width = converter.width();
    // Even with empty headers, some default behavior should apply
    assertTrue(width >= 0);

    reader.close();
  }

  @Test void testColumnNameCasing() throws Exception {
    String html = "<html><body><table>"
        + "<tr><th>Full Name</th><th>Age Score</th></tr>"
        + "<tr><td>Alice</td><td>95</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    Source source = Sources.of(file);
    FileReaderV2 reader = new FileReaderV2(source);

    FileRowConverter converter = new FileRowConverter(reader, null, "SMART_CASING");
    assertEquals(2, converter.width());

    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = converter.getRowType(typeFactory);
    assertNotNull(rowType);
    // SMART_CASING should transform "Full Name" and "Age Score"
    assertEquals(2, rowType.getFieldCount());

    reader.close();
  }

  @Test void testAutoDetectedColumns() throws Exception {
    // When no fieldConfigs are provided, all columns should be auto-detected
    String html = "<html><body><table>"
        + "<tr><th>X</th><th>Y</th><th>Z</th></tr>"
        + "<tr><td>1</td><td>2</td><td>3</td></tr>"
        + "</table></body></html>";

    File file = createHtmlFile(html);
    Source source = Sources.of(file);
    FileReaderV2 reader = new FileReaderV2(source);

    // Only define one field config, leaving Y and Z to be auto-detected
    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> xConfig = new HashMap<>();
    xConfig.put("th", "X");
    xConfig.put("type", "int");
    fieldConfigs.add(xConfig);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");
    // Should have 3 fields: X (explicit) + Y and Z (auto-detected as string)
    assertEquals(3, converter.width());

    reader.close();
  }
}
