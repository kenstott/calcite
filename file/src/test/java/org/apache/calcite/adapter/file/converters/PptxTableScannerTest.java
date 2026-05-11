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

import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTable;
import org.apache.poi.xslf.usermodel.XSLFTableCell;
import org.apache.poi.xslf.usermodel.XSLFTableRow;
import org.apache.poi.xslf.usermodel.XSLFTextShape;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link PptxTableScanner}.
 * Tests table extraction from PPTX files, file naming logic,
 * header detection, and various table structures.
 */
@Tag("unit")
public class PptxTableScannerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir
  File tempDir;

  // ========== Helper Methods ==========

  /**
   * Creates a PPTX file with a single table on a single slide.
   */
  private File createPptxWithTable(String[][] data) throws IOException {
    return createPptxWithTable(data, null, null);
  }

  /**
   * Creates a PPTX file with a single table, optional title, and optional slide title.
   */
  private File createPptxWithTable(String[][] data, String slideTitle,
      String tableTitle) throws IOException {
    File pptxFile = new File(tempDir, "test.pptx");

    try (XMLSlideShow ppt = new XMLSlideShow()) {
      XSLFSlide slide = ppt.createSlide();

      // Add slide title if provided
      if (slideTitle != null) {
        XSLFTextShape titleShape = slide.createTextBox();
        titleShape.setText(slideTitle);
        titleShape.setAnchor(new Rectangle2D.Double(50, 10, 600, 40));
      }

      // Add table title if provided
      if (tableTitle != null) {
        XSLFTextShape tableTitleShape = slide.createTextBox();
        tableTitleShape.setText(tableTitle);
        tableTitleShape.setAnchor(new Rectangle2D.Double(50, 60, 600, 30));
      }

      // Create table
      if (data.length > 0) {
        XSLFTable table = slide.createTable(data.length, data[0].length);
        table.setAnchor(new Rectangle2D.Double(50, 100, 600, 300));

        for (int i = 0; i < data.length; i++) {
          XSLFTableRow row = table.getRows().get(i);
          for (int j = 0; j < data[i].length; j++) {
            row.getCells().get(j).setText(data[i][j]);
          }
        }
      }

      try (FileOutputStream fos = new FileOutputStream(pptxFile)) {
        ppt.write(fos);
      }
    }

    return pptxFile;
  }

  /**
   * Creates a PPTX file with multiple tables on the same slide.
   */
  private File createPptxWithMultipleTables(String[][] data1, String[][] data2)
      throws IOException {
    File pptxFile = new File(tempDir, "multi_table.pptx");

    try (XMLSlideShow ppt = new XMLSlideShow()) {
      XSLFSlide slide = ppt.createSlide();

      // First table
      if (data1.length > 0) {
        XSLFTable table1 = slide.createTable(data1.length, data1[0].length);
        table1.setAnchor(new Rectangle2D.Double(50, 50, 300, 200));
        for (int i = 0; i < data1.length; i++) {
          XSLFTableRow row = table1.getRows().get(i);
          for (int j = 0; j < data1[i].length; j++) {
            row.getCells().get(j).setText(data1[i][j]);
          }
        }
      }

      // Second table
      if (data2.length > 0) {
        XSLFTable table2 = slide.createTable(data2.length, data2[0].length);
        table2.setAnchor(new Rectangle2D.Double(400, 50, 300, 200));
        for (int i = 0; i < data2.length; i++) {
          XSLFTableRow row = table2.getRows().get(i);
          for (int j = 0; j < data2[i].length; j++) {
            row.getCells().get(j).setText(data2[i][j]);
          }
        }
      }

      try (FileOutputStream fos = new FileOutputStream(pptxFile)) {
        ppt.write(fos);
      }
    }

    return pptxFile;
  }

  /**
   * Creates a PPTX file with multiple slides, each with a table.
   */
  private File createPptxWithMultipleSlides(String[][] data1, String[][] data2)
      throws IOException {
    File pptxFile = new File(tempDir, "multi_slide.pptx");

    try (XMLSlideShow ppt = new XMLSlideShow()) {
      // Slide 1
      XSLFSlide slide1 = ppt.createSlide();
      if (data1.length > 0) {
        XSLFTable table1 = slide1.createTable(data1.length, data1[0].length);
        table1.setAnchor(new Rectangle2D.Double(50, 100, 600, 300));
        for (int i = 0; i < data1.length; i++) {
          XSLFTableRow row = table1.getRows().get(i);
          for (int j = 0; j < data1[i].length; j++) {
            row.getCells().get(j).setText(data1[i][j]);
          }
        }
      }

      // Slide 2
      XSLFSlide slide2 = ppt.createSlide();
      if (data2.length > 0) {
        XSLFTable table2 = slide2.createTable(data2.length, data2[0].length);
        table2.setAnchor(new Rectangle2D.Double(50, 100, 600, 300));
        for (int i = 0; i < data2.length; i++) {
          XSLFTableRow row = table2.getRows().get(i);
          for (int j = 0; j < data2[i].length; j++) {
            row.getCells().get(j).setText(data2[i][j]);
          }
        }
      }

      try (FileOutputStream fos = new FileOutputStream(pptxFile)) {
        ppt.write(fos);
      }
    }

    return pptxFile;
  }

  // ========== scanAndConvertTables Tests ==========

  @Test
  public void testScanAndConvertSingleTable() throws IOException {
    String[][] data = {
        {"Name", "Age", "City"},
        {"Alice", "30", "NYC"},
        {"Bob", "25", "LA"}
    };

    File pptxFile = createPptxWithTable(data);
    File outputDir = new File(tempDir, "output");
    outputDir.mkdirs();

    PptxTableScanner.scanAndConvertTables(pptxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length > 0);

    // Verify JSON content
    JsonNode array = MAPPER.readTree(jsonFiles[0]);
    assertTrue(array.isArray());
    assertEquals(2, array.size()); // 2 data rows
  }

  @Test
  public void testScanAndConvertWithSlideTitle() throws IOException {
    String[][] data = {
        {"Product", "Sales"},
        {"Widget", "1000"},
        {"Gadget", "2000"}
    };

    File pptxFile = createPptxWithTable(data, "Sales Report", null);
    File outputDir = new File(tempDir, "output2");
    outputDir.mkdirs();

    PptxTableScanner.scanAndConvertTables(pptxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length > 0);

    // Verify filename includes slide title
    String fileName = jsonFiles[0].getName();
    assertTrue(fileName.contains("sales"));
  }

  @Test
  public void testScanAndConvertWithTableTitle() throws IOException {
    String[][] data = {
        {"Metric", "Value"},
        {"Revenue", "50000"}
    };

    File pptxFile = createPptxWithTable(data, null, "Key Metrics");
    File outputDir = new File(tempDir, "output3");
    outputDir.mkdirs();

    PptxTableScanner.scanAndConvertTables(pptxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length > 0);
  }

  @Test
  public void testScanAndConvertMultipleTablesOnSlide() throws IOException {
    String[][] data1 = {
        {"A", "B"},
        {"1", "2"}
    };
    String[][] data2 = {
        {"X", "Y"},
        {"3", "4"}
    };

    File pptxFile = createPptxWithMultipleTables(data1, data2);
    File outputDir = new File(tempDir, "output4");
    outputDir.mkdirs();

    PptxTableScanner.scanAndConvertTables(pptxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 2);
  }

  @Test
  public void testScanAndConvertMultipleSlides() throws IOException {
    String[][] data1 = {
        {"Name", "Score"},
        {"Alice", "95"}
    };
    String[][] data2 = {
        {"Product", "Price"},
        {"Widget", "9.99"}
    };

    File pptxFile = createPptxWithMultipleSlides(data1, data2);
    File outputDir = new File(tempDir, "output5");
    outputDir.mkdirs();

    PptxTableScanner.scanAndConvertTables(pptxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 2);
  }

  @Test
  public void testScanAndConvertEmptyPresentation() throws IOException {
    File pptxFile = new File(tempDir, "empty.pptx");
    try (XMLSlideShow ppt = new XMLSlideShow()) {
      ppt.createSlide(); // Empty slide
      try (FileOutputStream fos = new FileOutputStream(pptxFile)) {
        ppt.write(fos);
      }
    }

    File outputDir = new File(tempDir, "output6");
    outputDir.mkdirs();

    PptxTableScanner.scanAndConvertTables(pptxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((dir, name) -> name.endsWith(".json"));
    // No tables, so no JSON files or empty
    assertTrue(jsonFiles == null || jsonFiles.length == 0);
  }

  @Test
  public void testScanAndConvertWithRelativePath() throws IOException {
    String[][] data = {
        {"Key", "Value"},
        {"a", "1"}
    };

    File pptxFile = createPptxWithTable(data);
    File outputDir = new File(tempDir, "output7");
    outputDir.mkdirs();

    PptxTableScanner.scanAndConvertTables(pptxFile, outputDir,
        "subdir" + File.separator + "test.pptx");

    File[] jsonFiles = outputDir.listFiles((dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length > 0);

    // Filename should include directory prefix
    String fileName = jsonFiles[0].getName();
    assertTrue(fileName.contains("subdir_"));
  }

  // ========== generateBaseFileName Tests ==========

  @Test
  public void testGenerateBaseFileNameWithSlideTitle() throws Exception {
    Method genFileName = PptxTableScanner.class.getDeclaredMethod(
        "generateBaseFileName", String.class, String.class, String.class, int.class);
    genFileName.setAccessible(true);

    String result = (String) genFileName.invoke(null,
        "presentation", "Table Title", "Slide Title", 1);
    assertNotNull(result);
    assertTrue(result.contains("presentation"));
    assertTrue(result.contains("slide_title"));
    assertTrue(result.contains("table_title"));
  }

  @Test
  public void testGenerateBaseFileNameWithNoSlideTitle() throws Exception {
    Method genFileName = PptxTableScanner.class.getDeclaredMethod(
        "generateBaseFileName", String.class, String.class, String.class, int.class);
    genFileName.setAccessible(true);

    String result = (String) genFileName.invoke(null,
        "presentation", "Table Title", null, 3);
    assertNotNull(result);
    assertTrue(result.contains("slide3"));
  }

  @Test
  public void testGenerateBaseFileNameWithEmptySlideTitle() throws Exception {
    Method genFileName = PptxTableScanner.class.getDeclaredMethod(
        "generateBaseFileName", String.class, String.class, String.class, int.class);
    genFileName.setAccessible(true);

    String result = (String) genFileName.invoke(null,
        "presentation", null, "", 2);
    assertNotNull(result);
    assertTrue(result.contains("slide2"));
    assertTrue(result.contains("__table"));
  }

  @Test
  public void testGenerateBaseFileNameWithNoTableTitle() throws Exception {
    Method genFileName = PptxTableScanner.class.getDeclaredMethod(
        "generateBaseFileName", String.class, String.class, String.class, int.class);
    genFileName.setAccessible(true);

    String result = (String) genFileName.invoke(null,
        "presentation", null, "My Slide", 1);
    assertNotNull(result);
    assertTrue(result.contains("__table"));
    assertFalse(result.contains("null"));
  }

  // ========== toPascalCase Tests ==========

  @Test
  public void testToPascalCase() throws Exception {
    Method toPascalCase = PptxTableScanner.class.getDeclaredMethod(
        "toPascalCase", String.class);
    toPascalCase.setAccessible(true);

    assertEquals("HelloWorld", toPascalCase.invoke(null, "hello world"));
    assertEquals("TestCase", toPascalCase.invoke(null, "test_case"));
    assertEquals("MyHyphenated", toPascalCase.invoke(null, "my-hyphenated"));
    assertEquals("Single", toPascalCase.invoke(null, "single"));
    assertEquals("", toPascalCase.invoke(null, ""));
  }

  // ========== looksLikeHeader Tests ==========

  @Test
  public void testLooksLikeHeaderWithTextCells() throws Exception {
    Method looksLikeHeader = PptxTableScanner.class.getDeclaredMethod(
        "looksLikeHeader", List.class);
    looksLikeHeader.setAccessible(true);

    List<String> textCells = Arrays.asList("Name", "Age", "City");
    assertTrue((Boolean) looksLikeHeader.invoke(null, textCells));
  }

  @Test
  public void testLooksLikeHeaderWithNumericCells() throws Exception {
    Method looksLikeHeader = PptxTableScanner.class.getDeclaredMethod(
        "looksLikeHeader", List.class);
    looksLikeHeader.setAccessible(true);

    List<String> numericCells = Arrays.asList("100", "200", "300");
    assertFalse((Boolean) looksLikeHeader.invoke(null, numericCells));
  }

  @Test
  public void testLooksLikeHeaderWithEmptyCells() throws Exception {
    Method looksLikeHeader = PptxTableScanner.class.getDeclaredMethod(
        "looksLikeHeader", List.class);
    looksLikeHeader.setAccessible(true);

    List<String> emptyCells = Collections.emptyList();
    assertFalse((Boolean) looksLikeHeader.invoke(null, emptyCells));
  }

  @Test
  public void testLooksLikeHeaderWithMixedCells() throws Exception {
    Method looksLikeHeader = PptxTableScanner.class.getDeclaredMethod(
        "looksLikeHeader", List.class);
    looksLikeHeader.setAccessible(true);

    // More text than numeric -> header
    List<String> mixedCells = Arrays.asList("Name", "Age", "100");
    assertTrue((Boolean) looksLikeHeader.invoke(null, mixedCells));
  }

  @Test
  public void testLooksLikeHeaderWithAllEmpty() throws Exception {
    Method looksLikeHeader = PptxTableScanner.class.getDeclaredMethod(
        "looksLikeHeader", List.class);
    looksLikeHeader.setAccessible(true);

    List<String> allEmpty = Arrays.asList("", "", "");
    // No text, no numeric -> text == numeric (both 0), not header
    assertFalse((Boolean) looksLikeHeader.invoke(null, allEmpty));
  }

  @Test
  public void testLooksLikeHeaderWithDecimalNumbers() throws Exception {
    Method looksLikeHeader = PptxTableScanner.class.getDeclaredMethod(
        "looksLikeHeader", List.class);
    looksLikeHeader.setAccessible(true);

    List<String> decimalCells = Arrays.asList("1.5", "-2.3", "0.0");
    assertFalse((Boolean) looksLikeHeader.invoke(null, decimalCells));
  }

  // ========== isEmptyRow Tests ==========

  @Test
  public void testIsEmptyRowWithEmptyCells() throws Exception {
    Method isEmptyRow = PptxTableScanner.class.getDeclaredMethod(
        "isEmptyRow", List.class);
    isEmptyRow.setAccessible(true);

    List<String> emptyCells = Arrays.asList("", "  ", "");
    assertTrue((Boolean) isEmptyRow.invoke(null, emptyCells));
  }

  @Test
  public void testIsEmptyRowWithNonEmptyCells() throws Exception {
    Method isEmptyRow = PptxTableScanner.class.getDeclaredMethod(
        "isEmptyRow", List.class);
    isEmptyRow.setAccessible(true);

    List<String> cells = Arrays.asList("", "data", "");
    assertFalse((Boolean) isEmptyRow.invoke(null, cells));
  }

  // ========== buildColumnHeaders Tests ==========

  @Test
  public void testBuildColumnHeadersEmptyList() throws Exception {
    Method buildHeaders = PptxTableScanner.class.getDeclaredMethod(
        "buildColumnHeaders", List.class);
    buildHeaders.setAccessible(true);

    List<List<String>> emptyHeaders = Collections.emptyList();
    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) buildHeaders.invoke(null, emptyHeaders);
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testBuildColumnHeadersSingleRow() throws Exception {
    Method buildHeaders = PptxTableScanner.class.getDeclaredMethod(
        "buildColumnHeaders", List.class);
    buildHeaders.setAccessible(true);

    List<List<String>> headerRows = new ArrayList<List<String>>();
    headerRows.add(Arrays.asList("Name", "Age", "City"));

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) buildHeaders.invoke(null, headerRows);
    assertNotNull(result);
    assertEquals(3, result.size());
    // Should be lowercased
    assertEquals("name", result.get(0));
    assertEquals("age", result.get(1));
    assertEquals("city", result.get(2));
  }

  @Test
  public void testBuildColumnHeadersMultipleRows() throws Exception {
    Method buildHeaders = PptxTableScanner.class.getDeclaredMethod(
        "buildColumnHeaders", List.class);
    buildHeaders.setAccessible(true);

    // Group header: "Sales" spans two columns, then empty
    // Detail header: "Q1", "Q2", "Total"
    List<List<String>> headerRows = new ArrayList<List<String>>();
    headerRows.add(Arrays.asList("Sales", "", ""));
    headerRows.add(Arrays.asList("Q1", "Q2", "Total"));

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) buildHeaders.invoke(null, headerRows);
    assertNotNull(result);
    assertEquals(3, result.size());
    // Group header should prefix detail headers
    assertTrue(result.get(0).contains("sales"));
    assertTrue(result.get(0).contains("q1"));
  }

  // ========== Table with empty rows (data) ==========

  @Test
  public void testScanTableWithEmptyDataRows() throws IOException {
    String[][] data = {
        {"Header1", "Header2"},
        {"", ""},  // Empty data row - should be skipped
        {"value1", "value2"}
    };

    File pptxFile = createPptxWithTable(data);
    File outputDir = new File(tempDir, "output_empty");
    outputDir.mkdirs();

    PptxTableScanner.scanAndConvertTables(pptxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length > 0);

    JsonNode array = MAPPER.readTree(jsonFiles[0]);
    assertTrue(array.isArray());
    // Empty row should be skipped, only value1/value2 row
    assertEquals(1, array.size());
  }

  // ========== Duplicate File Names ==========

  @Test
  public void testDuplicateFileNamesGetIndices() throws IOException {
    // Two tables on same slide with same generated name (no title, no slide title)
    String[][] data1 = {{"A", "B"}, {"1", "2"}};
    String[][] data2 = {{"C", "D"}, {"3", "4"}};

    File pptxFile = createPptxWithMultipleTables(data1, data2);
    File outputDir = new File(tempDir, "output_dup");
    outputDir.mkdirs();

    PptxTableScanner.scanAndConvertTables(pptxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    // Should have two files with different names (indices added for duplicates)
    assertTrue(jsonFiles.length >= 2);

    // Verify filenames are unique
    java.util.Set<String> fileNames = new java.util.HashSet<String>();
    for (File f : jsonFiles) {
      assertTrue(fileNames.add(f.getName()), "Duplicate filename: " + f.getName());
    }
  }

  // ========== determineHeaderRowCount Tests ==========

  @Test
  public void testScanTableWithGroupHeaders() throws IOException {
    // First row has fewer cells than second (group header pattern)
    String[][] data = {
        {"Group", "", ""},
        {"Col1", "Col2", "Col3"},
        {"1", "2", "3"}
    };

    File pptxFile = createPptxWithTable(data);
    File outputDir = new File(tempDir, "output_group");
    outputDir.mkdirs();

    PptxTableScanner.scanAndConvertTables(pptxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((dir, name) -> name.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length > 0);
  }
}
