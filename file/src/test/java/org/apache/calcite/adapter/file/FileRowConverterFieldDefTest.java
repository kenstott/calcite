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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.util.Sources;

import org.jsoup.select.Elements;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for FileRowConverter and its inner class FieldDef.
 * Covers type conversions, cell parsing, and row type generation.
 */
@Tag("unit")
public class FileRowConverterFieldDefTest {

  @TempDir
  Path tempDir;

  private JavaTypeFactory typeFactory;

  @BeforeEach
  void setUp() {
    typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  }

  /**
   * Creates a temp HTML file with a single table and returns a FileReaderV2.
   */
  private FileReaderV2 createReader(String[] headers, String[]... dataRows)
      throws IOException {
    StringBuilder html = new StringBuilder();
    html.append("<html><body><table>\n");
    html.append("<tr>");
    for (String h : headers) {
      html.append("<th>").append(h).append("</th>");
    }
    html.append("</tr>\n");
    for (String[] row : dataRows) {
      html.append("<tr>");
      for (String cell : row) {
        html.append("<td>").append(cell).append("</td>");
      }
      html.append("</tr>\n");
    }
    html.append("</table></body></html>");

    File htmlFile = tempDir.resolve("test_" + System.nanoTime() + ".html").toFile();
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write(html.toString());
    }

    return new FileReaderV2(Sources.of(htmlFile));
  }

  @Test
  void testWidthWithNoFieldConfigs() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Name", "Age", "City"},
        new String[]{"Alice", "30", "Boston"});
    FileRowConverter converter = new FileRowConverter(reader, null, "UNCHANGED");
    int width = converter.width();
    assertEquals(3, width, "Width should match number of header columns");
  }

  @Test
  void testWidthWithFieldConfigs() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Name", "Age", "City"},
        new String[]{"Alice", "30", "Boston"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> nameConfig = new HashMap<>();
    nameConfig.put("th", "Name");
    nameConfig.put("type", "String");
    fieldConfigs.add(nameConfig);

    Map<String, Object> ageConfig = new HashMap<>();
    ageConfig.put("th", "Age");
    ageConfig.put("type", "int");
    fieldConfigs.add(ageConfig);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");
    int width = converter.width();
    // Name and Age are configured; City is picked up automatically
    assertEquals(3, width, "Width should include configured fields plus non-configured headers");
  }

  @Test
  void testWidthWithSkippedField() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Name", "Age", "City"},
        new String[]{"Alice", "30", "Boston"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> nameConfig = new HashMap<>();
    nameConfig.put("th", "Name");
    fieldConfigs.add(nameConfig);

    Map<String, Object> ageConfig = new HashMap<>();
    ageConfig.put("th", "Age");
    ageConfig.put("skip", "true");
    fieldConfigs.add(ageConfig);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");
    int width = converter.width();
    // Name + City (Age is skipped)
    assertEquals(2, width, "Skipped fields should not be counted");
  }

  @Test
  void testGetRowTypeWithStringFields() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Name", "City"},
        new String[]{"Alice", "Boston"});
    FileRowConverter converter = new FileRowConverter(reader, null, "UNCHANGED");
    RelDataType rowType = converter.getRowType(typeFactory);

    assertNotNull(rowType, "Row type should not be null");
    assertEquals(2, rowType.getFieldCount(), "Should have 2 fields");
  }

  @Test
  void testGetRowTypeWithTypedFields() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Name", "Age", "Active"},
        new String[]{"Alice", "30", "true"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();

    Map<String, Object> nameConfig = new HashMap<>();
    nameConfig.put("th", "Name");
    nameConfig.put("type", "String");
    fieldConfigs.add(nameConfig);

    Map<String, Object> ageConfig = new HashMap<>();
    ageConfig.put("th", "Age");
    ageConfig.put("type", "int");
    fieldConfigs.add(ageConfig);

    Map<String, Object> activeConfig = new HashMap<>();
    activeConfig.put("th", "Active");
    activeConfig.put("type", "boolean");
    fieldConfigs.add(activeConfig);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");
    RelDataType rowType = converter.getRowType(typeFactory);

    assertEquals(3, rowType.getFieldCount(), "Should have 3 typed fields");
  }

  @Test
  void testGetRowTypeWithFieldNameOverride() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Name", "Age"},
        new String[]{"Alice", "30"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Name");
    config.put("name", "full_name");
    config.put("type", "String");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");
    RelDataType rowType = converter.getRowType(typeFactory);

    boolean hasFullName = false;
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      if ("full_name".equals(rowType.getFieldList().get(i).getName())) {
        hasFullName = true;
        break;
      }
    }
    assertTrue(hasFullName, "Should have field named 'full_name'");
  }

  @Test
  void testToRowWithStringConversion() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Name", "City"},
        new String[]{"Alice", "Boston"});
    FileRowConverter converter = new FileRowConverter(reader, null, "UNCHANGED");

    // Get the first data row from the reader
    FileReaderV2 reader2 = createReader(
        new String[]{"Name", "City"},
        new String[]{"Alice", "Boston"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements row = it.next(); // first data row

    int[] projection = {0, 1};
    Object result = converter.toRow(row, projection);

    assertNotNull(result, "Row result should not be null");
    assertTrue(result instanceof Object[], "Result should be an Object array");
    Object[] rowArr = (Object[]) result;
    assertEquals(2, rowArr.length, "Row should have 2 elements");
    assertEquals("Alice", rowArr[0], "First cell should be 'Alice'");
    assertEquals("Boston", rowArr[1], "Second cell should be 'Boston'");
  }

  @Test
  void testToRowWithBooleanType() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Active"},
        new String[]{"true"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Active");
    config.put("type", "boolean");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Active"},
        new String[]{"true"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertEquals(Boolean.TRUE, rowArr[0], "Should parse 'true' as Boolean.TRUE");
  }

  @Test
  void testToRowWithByteType() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Value"},
        new String[]{"42"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Value");
    config.put("type", "byte");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Value"},
        new String[]{"42"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertEquals((byte) 42, rowArr[0], "Should parse '42' as byte 42");
  }

  @Test
  void testToRowWithShortType() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Value"},
        new String[]{"1234"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Value");
    config.put("type", "short");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Value"},
        new String[]{"1234"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertEquals((short) 1234, rowArr[0], "Should parse '1234' as short");
  }

  @Test
  void testToRowWithIntType() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Count"},
        new String[]{"12345"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Count");
    config.put("type", "int");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Count"},
        new String[]{"12345"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertEquals(12345, rowArr[0], "Should parse '12345' as int");
  }

  @Test
  void testToRowWithLongType() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"BigNum"},
        new String[]{"9876543210"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "BigNum");
    config.put("type", "long");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"BigNum"},
        new String[]{"9876543210"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertEquals(9876543210L, rowArr[0], "Should parse large number as long");
  }

  @Test
  void testToRowWithFloatType() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Ratio"},
        new String[]{"3.14"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Ratio");
    config.put("type", "float");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Ratio"},
        new String[]{"3.14"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNotNull(rowArr[0], "Float value should not be null");
    assertEquals(3.14f, ((Number) rowArr[0]).floatValue(), 0.01f);
  }

  @Test
  void testToRowWithDoubleType() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Amount"},
        new String[]{"123.456"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Amount");
    config.put("type", "double");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Amount"},
        new String[]{"123.456"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNotNull(rowArr[0], "Double value should not be null");
    assertEquals(123.456, ((Number) rowArr[0]).doubleValue(), 0.001);
  }

  @Test
  void testToRowWithDoubleParseFailure() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Value"},
        new String[]{"not_a_number"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Value");
    config.put("type", "double");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Value"},
        new String[]{"not_a_number"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNull(rowArr[0], "Unparseable double should return null");
  }

  @Test
  void testToRowWithDateIsoFormat() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Date"},
        new String[]{"2024-01-15"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Date");
    config.put("type", "Date");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Date"},
        new String[]{"2024-01-15"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNotNull(rowArr[0], "ISO date should be parsed");
    assertEquals(java.sql.Date.valueOf("2024-01-15"), rowArr[0]);
  }

  @Test
  void testToRowWithDateNattyFormat() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Date"},
        new String[]{"January 15, 2024"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Date");
    config.put("type", "Date");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Date"},
        new String[]{"January 15, 2024"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNotNull(rowArr[0], "Natty-parsed date should not be null");
  }

  @Test
  void testToRowWithTimeHhMmSsFormat() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Time"},
        new String[]{"14:30:00"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Time");
    config.put("type", "Time");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Time"},
        new String[]{"14:30:00"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNotNull(rowArr[0], "Time should be parsed");
    assertTrue(rowArr[0] instanceof Integer, "Time should be Integer (millis since midnight)");
    assertEquals(52200000, rowArr[0], "14:30:00 should be 52200000 millis since midnight");
  }

  @Test
  void testToRowWithTimestampIsoFormat() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Timestamp"},
        new String[]{"2024-01-15 14:30:00"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Timestamp");
    config.put("type", "Timestamp");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Timestamp"},
        new String[]{"2024-01-15 14:30:00"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNotNull(rowArr[0], "Timestamp should be parsed");
    assertTrue(rowArr[0] instanceof Long, "Timestamp should be Long (millis since epoch)");
  }

  @Test
  void testToRowWithTimestampWithLocalTimeZone() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Timestamp"},
        new String[]{"January 15, 2024"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Timestamp");
    config.put("type", "TimestampWithLocalTimeZone");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Timestamp"},
        new String[]{"January 15, 2024"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNotNull(rowArr[0], "Timestamp with local timezone should not be null");
    assertTrue(rowArr[0] instanceof Long, "Should be millis since epoch");
  }

  @Test
  void testToRowWithNullValue() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Value"},
        new String[]{""});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Value");
    config.put("type", "int");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Value"},
        new String[]{""});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNull(rowArr[0], "Empty string should convert to null for typed fields");
  }

  @Test
  void testToRowWithNullTypeReturnsString() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Name"},
        new String[]{"hello"});
    FileRowConverter converter = new FileRowConverter(reader, null, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Name"},
        new String[]{"hello"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertEquals("hello", rowArr[0], "Null type should return string as-is");
  }

  @Test
  void testToRowWithProjection() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"A", "B", "C"},
        new String[]{"alpha", "beta", "gamma"});
    FileRowConverter converter = new FileRowConverter(reader, null, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"A", "B", "C"},
        new String[]{"alpha", "beta", "gamma"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0, 2};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertEquals(2, rowArr.length, "Projected row should have 2 elements");
    assertEquals("alpha", rowArr[0], "First projected value should be 'alpha'");
    assertEquals("gamma", rowArr[1], "Second projected value should be 'gamma'");
  }

  @Test
  void testInitializationIsIdempotent() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"A", "B"},
        new String[]{"x", "y"});
    FileRowConverter converter = new FileRowConverter(reader, null, "UNCHANGED");

    int width1 = converter.width();
    int width2 = converter.width();
    assertEquals(width1, width2, "Multiple width() calls should return same value");
    assertEquals(2, width1, "Width should be 2");
  }

  @Test
  void testDuplicateHeadingThrowsException() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Name", "Name"},
        new String[]{"A", "B"});

    FileRowConverter converter = new FileRowConverter(reader, null, "UNCHANGED");
    assertThrows(RuntimeException.class, () -> converter.width(),
        "Duplicate headings should throw RuntimeException");
  }

  @Test
  void testBadSourceColumnInFieldConfig() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Name", "Age"},
        new String[]{"Alice", "30"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "NonExistent");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");
    assertThrows(RuntimeException.class, () -> converter.width(),
        "Non-existent column reference should throw RuntimeException");
  }

  @Test
  void testDuplicateColumnNameInFieldConfig() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Name", "Age"},
        new String[]{"Alice", "30"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config1 = new HashMap<>();
    config1.put("th", "Name");
    config1.put("name", "col");
    fieldConfigs.add(config1);

    Map<String, Object> config2 = new HashMap<>();
    config2.put("th", "Age");
    config2.put("name", "col");
    fieldConfigs.add(config2);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");
    assertThrows(RuntimeException.class, () -> converter.width(),
        "Duplicate column names should throw RuntimeException");
  }

  @Test
  void testCellReaderWithReplace() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Price"},
        new String[]{"$100"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Price");
    config.put("type", "String");
    config.put("replace", "\\$");
    config.put("replaceWith", "");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Price"},
        new String[]{"$100"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertEquals("100", rowArr[0], "Should strip dollar sign via replace pattern");
  }

  @Test
  void testCellReaderWithMatch() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Data"},
        new String[]{"abc 123 def 456"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Data");
    config.put("type", "String");
    config.put("match", "\\d+");
    config.put("matchSeq", 0);
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Data"},
        new String[]{"abc 123 def 456"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertEquals("123", rowArr[0], "Should extract first matching number");
  }

  @Test
  void testCellReaderWithMatchNoMatch() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Data"},
        new String[]{"no numbers here"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Data");
    config.put("type", "String");
    config.put("match", "\\d+");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Data"},
        new String[]{"no numbers here"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNull(rowArr[0], "No match should return null");
  }

  @Test
  void testShortParseException() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Val"},
        new String[]{"not_a_number"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Val");
    config.put("type", "short");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Val"},
        new String[]{"not_a_number"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNull(rowArr[0], "Unparseable short should return null");
  }

  @Test
  void testIntParseException() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Val"},
        new String[]{"not_a_number"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Val");
    config.put("type", "int");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Val"},
        new String[]{"not_a_number"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNull(rowArr[0], "Unparseable int should return null");
  }

  @Test
  void testLongParseException() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Val"},
        new String[]{"not_a_number"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Val");
    config.put("type", "long");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Val"},
        new String[]{"not_a_number"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNull(rowArr[0], "Unparseable long should return null");
  }

  @Test
  void testFloatParseException() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Val"},
        new String[]{"not_a_number"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Val");
    config.put("type", "float");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Val"},
        new String[]{"not_a_number"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNull(rowArr[0], "Unparseable float should return null");
  }

  @Test
  void testGetRowTypeWithEmptyHeaders() throws IOException {
    // Edge case: table with no explicit th, only td - FileReaderV2 generates default col names
    StringBuilder html = new StringBuilder();
    html.append("<html><body><table>");
    html.append("<tr><td>v1</td><td>v2</td></tr>");
    html.append("</table></body></html>");

    File htmlFile = tempDir.resolve("empty_header.html").toFile();
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write(html.toString());
    }

    FileReaderV2 reader = new FileReaderV2(Sources.of(htmlFile));
    FileRowConverter converter = new FileRowConverter(reader, null, "UNCHANGED");
    RelDataType rowType = converter.getRowType(typeFactory);

    // FileReaderV2 generates default col names (col0, col1) when no th elements found
    assertTrue(rowType.getFieldCount() >= 1, "Should have at least 1 field");
  }

  @Test
  void testTimestampNattyFallback() throws IOException {
    FileReaderV2 reader = createReader(
        new String[]{"Timestamp"},
        new String[]{"March 15 2024"});

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Timestamp");
    config.put("type", "Timestamp");
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader2 = createReader(
        new String[]{"Timestamp"},
        new String[]{"March 15 2024"});
    java.util.Iterator<Elements> it = reader2.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertNotNull(rowArr[0], "Natty-parsed timestamp should not be null");
  }

  @Test
  void testCellReaderWithSelectedElement() throws IOException {
    // Create HTML with inner span elements inside a td
    StringBuilder html = new StringBuilder();
    html.append("<html><body><table>");
    html.append("<tr><th>Items</th></tr>");
    html.append("<tr><td><span>first</span><span>second</span><span>third</span></td></tr>");
    html.append("</table></body></html>");

    File htmlFile = tempDir.resolve("selected_elem.html").toFile();
    try (FileWriter writer = new FileWriter(htmlFile)) {
      writer.write(html.toString());
    }

    FileReaderV2 reader = new FileReaderV2(Sources.of(htmlFile));

    List<Map<String, Object>> fieldConfigs = new ArrayList<>();
    Map<String, Object> config = new HashMap<>();
    config.put("th", "Items");
    config.put("type", "String");
    config.put("selector", "span");
    config.put("selectedElement", 1);
    fieldConfigs.add(config);

    FileRowConverter converter = new FileRowConverter(reader, fieldConfigs, "UNCHANGED");

    FileReaderV2 reader3 = new FileReaderV2(Sources.of(htmlFile));
    java.util.Iterator<Elements> it = reader3.iterator();
    Elements dataRow = it.next();

    int[] projection = {0};
    Object result = converter.toRow(dataRow, projection);
    Object[] rowArr = (Object[]) result;
    assertEquals("second", rowArr[0], "Should select second span element");
  }
}
