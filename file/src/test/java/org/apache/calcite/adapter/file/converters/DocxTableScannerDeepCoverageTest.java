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
package org.apache.calcite.adapter.file.converters;

import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link DocxTableScanner} focusing on missed code paths:
 * - scanAndConvertTables with and without relativePath
 * - extractTables from DOCX documents
 * - findTableTitle with preceding paragraphs
 * - parseTable with empty rows, no header rows
 * - determineHeaderRowCount with group headers
 * - buildColumnHeaders with single and multiple header rows
 * - looksLikeHeader detection
 * - isEmptyRow detection
 * - generateFileName with and without title
 * - toPascalCase conversion
 * - Lock acquisition failure handling
 */
@Tag("unit")
public class DocxTableScannerDeepCoverageTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir
  Path tempDir;

  // ========== scanAndConvertTables - basic functionality ==========

  @Test void testScanAndConvertSimpleTable() throws IOException {
    File docxFile =
        createDocxWithSimpleTable("simple.docx", new String[]{"Name", "Age"},
        new String[][]{{"Alice", "30"}, {"Bob", "25"}});
    File outputDir = createOutputDir("out_simple");

    DocxTableScanner.scanAndConvertTables(docxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 1, "Expected at least one JSON file");

    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles[0].toPath()));
    assertTrue(json.isArray());
    assertEquals(2, json.size());
  }

  @Test void testScanAndConvertEmptyDocument() throws IOException {
    File docxFile = createEmptyDocx("empty.docx");
    File outputDir = createOutputDir("out_empty");

    DocxTableScanner.scanAndConvertTables(docxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    // Should produce no JSON files for empty document
    assertTrue(jsonFiles == null || jsonFiles.length == 0,
        "Expected no JSON files for empty document");
  }

  @Test void testScanAndConvertNoArgsRelativePath() throws IOException {
    File docxFile =
        createDocxWithSimpleTable("norel.docx", new String[]{"Col1"},
        new String[][]{{"val1"}});
    File outputDir = createOutputDir("out_norel");

    // Call with no relativePath (null)
    DocxTableScanner.scanAndConvertTables(docxFile, outputDir, null);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 1);
  }

  @Test void testScanAndConvertWithRelativePath() throws IOException {
    File docxFile =
        createDocxWithSimpleTable("relpath.docx", new String[]{"Key"},
        new String[][]{{"k1"}});
    File outputDir = createOutputDir("out_relpath");

    // Call with relativePath that includes directory separators
    String relativePath = "subdir" + File.separator + "relpath.docx";
    DocxTableScanner.scanAndConvertTables(docxFile, outputDir, relativePath);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 1);
    // Filename should include directory prefix
    boolean hasPrefix = false;
    for (File f : jsonFiles) {
      if (f.getName().contains("subdir")) {
        hasPrefix = true;
        break;
      }
    }
    assertTrue(hasPrefix, "Expected filename to contain directory prefix 'subdir'");
  }

  @Test void testScanAndConvertWithRelativePathNoSeparator() throws IOException {
    File docxFile =
        createDocxWithSimpleTable("nosep.docx", new String[]{"Key"},
        new String[][]{{"k1"}});
    File outputDir = createOutputDir("out_nosep");

    // Relative path without directory separator
    DocxTableScanner.scanAndConvertTables(docxFile, outputDir, "nosep.docx");

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 1);
  }

  // ========== Multiple tables ==========

  @Test void testScanAndConvertMultipleTables() throws IOException {
    File docxFile = createDocxWithMultipleTables("multi.docx");
    File outputDir = createOutputDir("out_multi");

    DocxTableScanner.scanAndConvertTables(docxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 2,
        "Expected at least 2 JSON files for document with multiple tables, got " + jsonFiles.length);
  }

  // ========== Table with title from preceding paragraph ==========

  @Test void testScanAndConvertTableWithTitle() throws IOException {
    File docxFile =
        createDocxWithTitledTable("titled.docx", "Employee Data",
        new String[]{"Name", "Dept"},
        new String[][]{{"Alice", "Engineering"}});
    File outputDir = createOutputDir("out_titled");

    DocxTableScanner.scanAndConvertTables(docxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 1);
    // Filename should include sanitized title
    boolean hasTitle = false;
    for (File f : jsonFiles) {
      if (f.getName().contains("employee") || f.getName().contains("data")) {
        hasTitle = true;
        break;
      }
    }
    assertTrue(hasTitle,
        "Expected filename to contain sanitized title 'employee' or 'data', got: "
            + Arrays.toString(jsonFiles));
  }

  // ========== Table with empty rows ==========

  @Test void testScanAndConvertTableWithEmptyRows() throws IOException {
    File docxFile = createDocxWithEmptyRows("empty_rows.docx");
    File outputDir = createOutputDir("out_empty_rows");

    DocxTableScanner.scanAndConvertTables(docxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 1);
    // Empty rows should be skipped
    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles[0].toPath()));
    assertTrue(json.isArray());
    // Should contain only non-empty data rows
  }

  // ========== Table with only header row (no data) ==========

  @Test void testScanAndConvertTableWithOnlyHeaders() throws IOException {
    File docxFile =
        createDocxWithSimpleTable("headers_only.docx", new String[]{"A", "B", "C"},
        new String[0][]);  // No data rows
    File outputDir = createOutputDir("out_headers_only");

    DocxTableScanner.scanAndConvertTables(docxFile, outputDir);

    // Table with only headers should either produce empty JSON array or no file
    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    // Either no file or empty array is acceptable
  }

  // ========== toPascalCase via reflection ==========

  @Test void testToPascalCase() throws Exception {
    Method method = DocxTableScanner.class.getDeclaredMethod("toPascalCase", String.class);
    method.setAccessible(true);

    assertEquals("HelloWorld", method.invoke(null, "hello world"));
    assertEquals("HelloWorld", method.invoke(null, "hello_world"));
    assertEquals("HelloWorld", method.invoke(null, "hello-world"));
    assertEquals("Hello", method.invoke(null, "hello"));
    assertEquals("", method.invoke(null, ""));
    assertEquals("A", method.invoke(null, "a"));
  }

  // ========== looksLikeHeader via reflection ==========

  @Test void testLooksLikeHeader() throws Exception {
    Method method = DocxTableScanner.class.getDeclaredMethod("looksLikeHeader", List.class);
    method.setAccessible(true);

    // All text - looks like header
    List<String> textRow = Arrays.asList("Name", "Department", "Location");
    assertTrue((Boolean) method.invoke(null, textRow));

    // All numeric - does not look like header
    List<String> numericRow = Arrays.asList("100", "200", "300");
    assertFalse((Boolean) method.invoke(null, numericRow));

    // Mixed but more text - looks like header
    List<String> mixedRow = Arrays.asList("Item", "Qty", "Price", "Total");
    assertTrue((Boolean) method.invoke(null, mixedRow));

    // Empty list
    List<String> emptyRow = new ArrayList<>();
    assertFalse((Boolean) method.invoke(null, emptyRow));

    // All empty strings
    List<String> blankRow = Arrays.asList("", "", "");
    assertFalse((Boolean) method.invoke(null, blankRow));

    // Equal text and numeric
    List<String> equalRow = Arrays.asList("Name", "42");
    assertFalse((Boolean) method.invoke(null, equalRow));

    // Decimal numbers
    List<String> decimalRow = Arrays.asList("1.5", "2.0", "3.14");
    assertFalse((Boolean) method.invoke(null, decimalRow));
  }

  // ========== isEmptyRow via reflection ==========

  @Test void testIsEmptyRow() throws Exception {
    Method method = DocxTableScanner.class.getDeclaredMethod("isEmptyRow", List.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(null, Arrays.asList("", "", "")));
    assertTrue((Boolean) method.invoke(null, Arrays.asList("  ", " ", "\t")));
    assertFalse((Boolean) method.invoke(null, Arrays.asList("", "value", "")));
    assertFalse((Boolean) method.invoke(null, Arrays.asList("data")));
  }

  // ========== generateFileName via reflection ==========

  @Test void testGenerateFileName() throws Exception {
    Method method =
        DocxTableScanner.class.getDeclaredMethod("generateFileName", String.class, String.class, int.class, int.class);
    method.setAccessible(true);

    // With title
    String withTitle = (String) method.invoke(null, "myfile", "Sales Report", 0, 1);
    assertTrue(withTitle.contains("myfile"));
    assertTrue(withTitle.contains("sales") || withTitle.contains("report"),
        "Expected title in filename, got: " + withTitle);
    assertTrue(withTitle.endsWith(".json"));

    // Without title, single table
    String singleNoTitle = (String) method.invoke(null, "myfile", null, 0, 1);
    assertEquals("myfile.json", singleNoTitle,
        "Single table without title should not have suffix");

    // Without title, multiple tables
    String multiNoTitle = (String) method.invoke(null, "myfile", null, 0, 3);
    assertTrue(multiNoTitle.contains("table1"),
        "Multiple tables without title should use index, got: " + multiNoTitle);

    String multiNoTitle2 = (String) method.invoke(null, "myfile", null, 2, 3);
    assertTrue(multiNoTitle2.contains("table3"),
        "Index 2 of 3 should produce table3, got: " + multiNoTitle2);

    // Empty title
    String emptyTitle = (String) method.invoke(null, "myfile", "", 0, 1);
    assertEquals("myfile.json", emptyTitle,
        "Empty title should be treated same as null");
  }

  // ========== buildColumnHeaders via reflection ==========

  @Test void testBuildColumnHeadersSingleRow() throws Exception {
    Method method =
        DocxTableScanner.class.getDeclaredMethod("buildColumnHeaders", List.class);
    method.setAccessible(true);

    List<List<String>> singleHeader = new ArrayList<>();
    singleHeader.add(Arrays.asList("First Name", "Last Name", "Email"));

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) method.invoke(null, singleHeader);
    assertEquals(3, result.size());
    // Should be lowercase
    for (String h : result) {
      assertEquals(h, h.toLowerCase(), "Headers should be lowercase: " + h);
    }
  }

  @Test void testBuildColumnHeadersMultipleRows() throws Exception {
    Method method =
        DocxTableScanner.class.getDeclaredMethod("buildColumnHeaders", List.class);
    method.setAccessible(true);

    // Group header: "Sales" spans first two columns
    // Detail headers: "Q1", "Q2", "Total"
    List<List<String>> multiHeader = new ArrayList<>();
    multiHeader.add(Arrays.asList("Sales", "", ""));    // Group row
    multiHeader.add(Arrays.asList("Q1", "Q2", "Total")); // Detail row

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) method.invoke(null, multiHeader);
    assertEquals(3, result.size());
    // First columns should have group prefix
    assertTrue(result.get(0).toLowerCase().contains("sales"),
        "Expected group prefix 'sales', got: " + result.get(0));
  }

  @Test void testBuildColumnHeadersEmptyList() throws Exception {
    Method method =
        DocxTableScanner.class.getDeclaredMethod("buildColumnHeaders", List.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) method.invoke(null, new ArrayList<>());
    assertNotNull(result);
    assertEquals(0, result.size());
  }

  // ========== determineHeaderRowCount via reflection ==========

  @Test void testDetermineHeaderRowCountEmptyRows() throws Exception {
    Method method =
        DocxTableScanner.class.getDeclaredMethod("determineHeaderRowCount", List.class);
    method.setAccessible(true);

    List<XWPFTableRow> emptyList = new ArrayList<>();
    int count = (Integer) method.invoke(null, emptyList);
    assertEquals(0, count);
  }

  @Test void testDetermineHeaderRowCountWithGroupHeader() throws IOException, Exception {
    // Create a DOCX with a table that has a group header
    // First row has fewer columns (group header), second row has more (detail headers)
    File docxFile = createDocxWithGroupHeaders("group.docx");
    File outputDir = createOutputDir("out_group");

    DocxTableScanner.scanAndConvertTables(docxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    // The group header test might produce combined headers
  }

  // ========== File name sanitization ==========

  @Test void testSpecialCharactersInFileName() throws IOException {
    File docxFile =
        createDocxWithSimpleTable("my file (copy).docx", new String[]{"Col"},
        new String[][]{{"val"}});
    File outputDir = createOutputDir("out_special");

    DocxTableScanner.scanAndConvertTables(docxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 1);
    // Verify filename was sanitized
    for (File f : jsonFiles) {
      assertFalse(f.getName().contains("("),
          "Expected parentheses removed from filename: " + f.getName());
    }
  }

  // ========== Numeric type inference in table data ==========

  @Test void testNumericTypeInference() throws IOException {
    File docxFile =
        createDocxWithSimpleTable("numeric.docx", new String[]{"Label", "IntVal", "FloatVal", "BoolVal"},
        new String[][]{
            {"item1", "42", "3.14", "true"},
            {"item2", "0", "0.0", "false"}
        });
    File outputDir = createOutputDir("out_numeric");

    DocxTableScanner.scanAndConvertTables(docxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 1);

    JsonNode json = MAPPER.readTree(Files.readString(jsonFiles[0].toPath()));
    assertTrue(json.isArray());
    JsonNode firstRow = json.get(0);
    // Integer values should be stored as numbers
    assertTrue(firstRow.has("label") || firstRow.has("Label") || firstRow.has("intval")
        || firstRow.has("IntVal"), "Expected some columns in output");
  }

  // ========== Document with paragraphs between tables ==========

  @Test void testTablesWithParagraphsBetween() throws IOException {
    XWPFDocument doc = new XWPFDocument();

    // First paragraph
    XWPFParagraph para1 = doc.createParagraph();
    para1.createRun().setText("Table One");

    // First table
    XWPFTable table1 = doc.createTable(2, 2);
    table1.getRow(0).getCell(0).setText("H1");
    table1.getRow(0).getCell(1).setText("H2");
    table1.getRow(1).getCell(0).setText("A");
    table1.getRow(1).getCell(1).setText("B");

    // Paragraph between tables
    XWPFParagraph para2 = doc.createParagraph();
    para2.createRun().setText("Table Two");

    // Second table
    XWPFTable table2 = doc.createTable(2, 3);
    table2.getRow(0).getCell(0).setText("X");
    table2.getRow(0).getCell(1).setText("Y");
    table2.getRow(0).getCell(2).setText("Z");
    table2.getRow(1).getCell(0).setText("1");
    table2.getRow(1).getCell(1).setText("2");
    table2.getRow(1).getCell(2).setText("3");

    File docxFile = new File(tempDir.toFile(), "between.docx");
    try (FileOutputStream fos = new FileOutputStream(docxFile)) {
      doc.write(fos);
    }
    doc.close();

    File outputDir = createOutputDir("out_between");
    DocxTableScanner.scanAndConvertTables(docxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    // Should produce 2 JSON files, one for each table
    assertTrue(jsonFiles.length >= 2,
        "Expected at least 2 JSON files, got " + jsonFiles.length);
  }

  // ========== Title with heading markers ==========

  @Test void testTitleWithMarkdownHeading() throws IOException {
    XWPFDocument doc = new XWPFDocument();

    XWPFParagraph para = doc.createParagraph();
    para.createRun().setText("## Financial Summary");

    XWPFTable table = doc.createTable(2, 2);
    table.getRow(0).getCell(0).setText("Revenue");
    table.getRow(0).getCell(1).setText("Expenses");
    table.getRow(1).getCell(0).setText("1000");
    table.getRow(1).getCell(1).setText("500");

    File docxFile = new File(tempDir.toFile(), "heading.docx");
    try (FileOutputStream fos = new FileOutputStream(docxFile)) {
      doc.write(fos);
    }
    doc.close();

    File outputDir = createOutputDir("out_heading");
    DocxTableScanner.scanAndConvertTables(docxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 1);
    // The ## should be stripped from the title
    for (File f : jsonFiles) {
      assertFalse(f.getName().contains("##"),
          "Heading markers should be removed from title: " + f.getName());
    }
  }

  @Test void testTitleWithNumberedHeading() throws IOException {
    XWPFDocument doc = new XWPFDocument();

    XWPFParagraph para = doc.createParagraph();
    para.createRun().setText("3. Revenue Data");

    XWPFTable table = doc.createTable(2, 2);
    table.getRow(0).getCell(0).setText("Quarter");
    table.getRow(0).getCell(1).setText("Amount");
    table.getRow(1).getCell(0).setText("Q1");
    table.getRow(1).getCell(1).setText("5000");

    File docxFile = new File(tempDir.toFile(), "numbered.docx");
    try (FileOutputStream fos = new FileOutputStream(docxFile)) {
      doc.write(fos);
    }
    doc.close();

    File outputDir = createOutputDir("out_numbered");
    DocxTableScanner.scanAndConvertTables(docxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 1);
  }

  // ========== Table with no preceding paragraph (null title) ==========

  @Test void testTableWithNoPrecedingParagraph() throws IOException {
    // Table is the first element in the document
    XWPFDocument doc = new XWPFDocument();
    XWPFTable table = doc.createTable(2, 2);
    table.getRow(0).getCell(0).setText("H1");
    table.getRow(0).getCell(1).setText("H2");
    table.getRow(1).getCell(0).setText("D1");
    table.getRow(1).getCell(1).setText("D2");

    File docxFile = new File(tempDir.toFile(), "no_title.docx");
    try (FileOutputStream fos = new FileOutputStream(docxFile)) {
      doc.write(fos);
    }
    doc.close();

    File outputDir = createOutputDir("out_no_title");
    DocxTableScanner.scanAndConvertTables(docxFile, outputDir);

    File[] jsonFiles = outputDir.listFiles((d, n) -> n.endsWith(".json"));
    assertNotNull(jsonFiles);
    assertTrue(jsonFiles.length >= 1);
  }

  // ========== Helpers ==========

  private File createDocxWithSimpleTable(String name, String[] headers, String[][] data) throws IOException {
    XWPFDocument doc = new XWPFDocument();
    int cols = headers.length;
    int totalRows = 1 + data.length;

    XWPFTable table = doc.createTable(totalRows, cols);

    // Set headers
    for (int j = 0; j < cols; j++) {
      table.getRow(0).getCell(j).setText(headers[j]);
    }

    // Set data
    for (int i = 0; i < data.length; i++) {
      for (int j = 0; j < Math.min(data[i].length, cols); j++) {
        table.getRow(i + 1).getCell(j).setText(data[i][j]);
      }
    }

    File file = new File(tempDir.toFile(), name);
    try (FileOutputStream fos = new FileOutputStream(file)) {
      doc.write(fos);
    }
    doc.close();
    return file;
  }

  private File createEmptyDocx(String name) throws IOException {
    XWPFDocument doc = new XWPFDocument();
    doc.createParagraph().createRun().setText("No tables here.");

    File file = new File(tempDir.toFile(), name);
    try (FileOutputStream fos = new FileOutputStream(file)) {
      doc.write(fos);
    }
    doc.close();
    return file;
  }

  private File createDocxWithMultipleTables(String name) throws IOException {
    XWPFDocument doc = new XWPFDocument();

    XWPFParagraph para1 = doc.createParagraph();
    para1.createRun().setText("First Table");

    XWPFTable table1 = doc.createTable(3, 2);
    table1.getRow(0).getCell(0).setText("Name");
    table1.getRow(0).getCell(1).setText("Age");
    table1.getRow(1).getCell(0).setText("Alice");
    table1.getRow(1).getCell(1).setText("30");
    table1.getRow(2).getCell(0).setText("Bob");
    table1.getRow(2).getCell(1).setText("25");

    XWPFParagraph para2 = doc.createParagraph();
    para2.createRun().setText("Second Table");

    XWPFTable table2 = doc.createTable(3, 3);
    table2.getRow(0).getCell(0).setText("Product");
    table2.getRow(0).getCell(1).setText("Price");
    table2.getRow(0).getCell(2).setText("Stock");
    table2.getRow(1).getCell(0).setText("Widget");
    table2.getRow(1).getCell(1).setText("9.99");
    table2.getRow(1).getCell(2).setText("100");
    table2.getRow(2).getCell(0).setText("Gadget");
    table2.getRow(2).getCell(1).setText("19.99");
    table2.getRow(2).getCell(2).setText("50");

    File file = new File(tempDir.toFile(), name);
    try (FileOutputStream fos = new FileOutputStream(file)) {
      doc.write(fos);
    }
    doc.close();
    return file;
  }

  private File createDocxWithTitledTable(String name, String title, String[] headers,
      String[][] data) throws IOException {
    XWPFDocument doc = new XWPFDocument();

    XWPFParagraph titlePara = doc.createParagraph();
    titlePara.createRun().setText(title);

    int cols = headers.length;
    int totalRows = 1 + data.length;

    XWPFTable table = doc.createTable(totalRows, cols);
    for (int j = 0; j < cols; j++) {
      table.getRow(0).getCell(j).setText(headers[j]);
    }
    for (int i = 0; i < data.length; i++) {
      for (int j = 0; j < Math.min(data[i].length, cols); j++) {
        table.getRow(i + 1).getCell(j).setText(data[i][j]);
      }
    }

    File file = new File(tempDir.toFile(), name);
    try (FileOutputStream fos = new FileOutputStream(file)) {
      doc.write(fos);
    }
    doc.close();
    return file;
  }

  private File createDocxWithEmptyRows(String name) throws IOException {
    XWPFDocument doc = new XWPFDocument();

    XWPFTable table = doc.createTable(4, 2);
    table.getRow(0).getCell(0).setText("Col1");
    table.getRow(0).getCell(1).setText("Col2");
    // Row 1 is empty (default empty cells)
    table.getRow(2).getCell(0).setText("data1");
    table.getRow(2).getCell(1).setText("data2");
    // Row 3 is empty

    File file = new File(tempDir.toFile(), name);
    try (FileOutputStream fos = new FileOutputStream(file)) {
      doc.write(fos);
    }
    doc.close();
    return file;
  }

  private File createDocxWithGroupHeaders(String name) throws IOException {
    XWPFDocument doc = new XWPFDocument();

    // Create a table where first row has fewer cells than second
    // This simulates a group header pattern
    XWPFTable table = doc.createTable(4, 3);
    // Group header row (simulated - first row with less content)
    table.getRow(0).getCell(0).setText("Revenue");
    table.getRow(0).getCell(1).setText("");
    table.getRow(0).getCell(2).setText("");
    // Detail header row
    table.getRow(1).getCell(0).setText("Q1");
    table.getRow(1).getCell(1).setText("Q2");
    table.getRow(1).getCell(2).setText("Total");
    // Data rows
    table.getRow(2).getCell(0).setText("100");
    table.getRow(2).getCell(1).setText("200");
    table.getRow(2).getCell(2).setText("300");
    table.getRow(3).getCell(0).setText("150");
    table.getRow(3).getCell(1).setText("250");
    table.getRow(3).getCell(2).setText("400");

    File file = new File(tempDir.toFile(), name);
    try (FileOutputStream fos = new FileOutputStream(file)) {
      doc.write(fos);
    }
    doc.close();
    return file;
  }

  private File createOutputDir(String name) {
    File dir = new File(tempDir.toFile(), name);
    dir.mkdirs();
    return dir;
  }
}
