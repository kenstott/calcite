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

import org.apache.calcite.adapter.file.converters.DocxTableScanner;
import org.apache.calcite.adapter.file.converters.MultiTableExcelToJsonConverter;
import org.apache.calcite.adapter.file.converters.PptxTableScanner;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFTable;
import org.apache.poi.xslf.usermodel.XSLFTableCell;
import org.apache.poi.xslf.usermodel.XSLFTableRow;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-014 / FILE-016 / FILE-106 / FILE-019 — golden assertions for the complex-format decomposition
 * law: an xlsx/docx/pptx workbook decomposes into exactly one JSON array file per discovered table.
 *
 * <p>Each fixture is generated in-test with Apache POI, then the corresponding converter entry point
 * is invoked directly (the exact public {@code static} signatures read from the converter sources).
 * The emitted JSON array files are then read back and asserted exactly — both the emitted file-SET
 * (names) and the table content.
 *
 * <p>Behavioural contrast pinned here:
 * <ul>
 *   <li>{@link DocxTableScanner} runs per-cell type inference via
 *       {@code ConverterUtils.setJsonValueWithTypeInference}, so a numeric cell becomes a JSON
 *       number; header-less / dataless tables emit no JSON.</li>
 *   <li>{@link PptxTableScanner} keeps RAW trimmed strings (no inference), so a numeric-looking
 *       cell stays a JSON string.</li>
 * </ul>
 */
@Tag("unit")
public class ConverterDecompositionTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** The set of emitted JSON-array (table-data) file names directly under {@code dir}. */
  private static Set<String> tableJsonNames(File dir) throws Exception {
    Set<String> names = new TreeSet<>();
    File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isFile() && f.getName().endsWith(".json")) {
          String content = new String(Files.readAllBytes(f.toPath())).trim();
          if (content.startsWith("[")) {
            names.add(f.getName());
          }
        }
      }
    }
    return names;
  }

  private static JsonNode readArray(File dir, String name) throws Exception {
    JsonNode node = MAPPER.readTree(new File(dir, name));
    assertTrue(node.isArray(), "emitted JSON must be an array: " + name);
    return node;
  }

  // ============================================================ FILE-014 (xlsx) ==================

  @Test @Tag("FILE-014")
  void excelEmitsOneJsonPerSheetTable(@TempDir Path root) throws Exception {
    File xlsx = root.resolve("people.xlsx").toFile();
    try (XSSFWorkbook wb = new XSSFWorkbook(); OutputStream out = Files.newOutputStream(xlsx.toPath())) {
      Sheet employees = wb.createSheet("employees");
      Row eh = employees.createRow(0);
      eh.createCell(0).setCellValue("id");
      eh.createCell(1).setCellValue("name");
      Row e1 = employees.createRow(1);
      e1.createCell(0).setCellValue(1);
      e1.createCell(1).setCellValue("alice");
      Row e2 = employees.createRow(2);
      e2.createCell(0).setCellValue(2);
      e2.createCell(1).setCellValue("bob");

      Sheet products = wb.createSheet("products");
      Row ph = products.createRow(0);
      ph.createCell(0).setCellValue("sku");
      ph.createCell(1).setCellValue("label");
      Row p1 = products.createRow(1);
      p1.createCell(0).setCellValue(100);
      p1.createCell(1).setCellValue("widget");
      wb.write(out);
    }

    File outDir = Files.createDirectories(root.resolve("out")).toFile();
    // Signature: convertFileToJson(input, outputDir, detectMultipleTables, tableNameCasing,
    //            columnNameCasing, baseDirectory)
    MultiTableExcelToJsonConverter.convertFileToJson(xlsx, outDir, true, "LOWER", "LOWER", outDir);

    // One JSON array file per sheet/table. Single table per sheet with no title row =>
    // file name is baseName + "__" + sheetName.
    Set<String> expected = new TreeSet<>();
    expected.add("people__employees.json");
    expected.add("people__products.json");
    assertEquals(expected, tableJsonNames(outDir),
        "xlsx must decompose into exactly one JSON file per discovered sheet/table");

    // Content of the employees table: exact column names + first row's values.
    JsonNode employees = readArray(outDir, "people__employees.json");
    assertEquals(2, employees.size(), "two data rows in employees");
    JsonNode r0 = employees.get(0);
    Set<String> cols = new TreeSet<>();
    r0.fieldNames().forEachRemaining(cols::add);
    Set<String> expectedCols = new TreeSet<>();
    expectedCols.add("id");
    expectedCols.add("name");
    assertEquals(expectedCols, cols, "employees columns");
    // Whole-number cell is emitted as a JSON integer (Long), text as a JSON string.
    assertTrue(r0.get("id").isNumber(), "id must be a JSON number");
    assertEquals(1L, r0.get("id").asLong(), "first id value");
    assertTrue(r0.get("name").isTextual(), "name must be a JSON string");
    assertEquals("alice", r0.get("name").asText(), "first name value");
  }

  // ============================================================ FILE-016 (docx) ==================

  @Test @Tag("FILE-016")
  void docxEmitsOneJsonPerInlineTable(@TempDir Path root) throws Exception {
    File docx = root.resolve("report.docx").toFile();
    try (XWPFDocument doc = new XWPFDocument(); OutputStream out = Files.newOutputStream(docx.toPath())) {
      // Table 1: 2 columns x (1 header + 2 data)
      XWPFTable t1 = doc.createTable(3, 2);
      setDocxCell(t1, 0, 0, "city");
      setDocxCell(t1, 0, 1, "country");
      setDocxCell(t1, 1, 0, "paris");
      setDocxCell(t1, 1, 1, "france");
      setDocxCell(t1, 2, 0, "tokyo");
      setDocxCell(t1, 2, 1, "japan");

      // A blank separating paragraph so the two tables stay distinct in the body.
      doc.createParagraph();

      // Table 2: 2 columns x (1 header + 1 data)
      XWPFTable t2 = doc.createTable(2, 2);
      setDocxCell(t2, 0, 0, "team");
      setDocxCell(t2, 0, 1, "wins");
      setDocxCell(t2, 1, 0, "reds");
      setDocxCell(t2, 1, 1, "12");
      doc.write(out);
    }

    File outDir = Files.createDirectories(root.resolve("out")).toFile();
    // Signature: scanAndConvertTables(input, outputDir)
    DocxTableScanner.scanAndConvertTables(docx, outDir);

    // Two inline tables, no titles => baseName + "__table" + (index+1).
    Set<String> expected = new TreeSet<>();
    expected.add("report__table1.json");
    expected.add("report__table2.json");
    assertEquals(expected, tableJsonNames(outDir),
        "docx must decompose into exactly one JSON file per detected table");

    // Headers + a row's names for the first table.
    JsonNode t1 = readArray(outDir, "report__table1.json");
    assertEquals(2, t1.size(), "two data rows in first docx table");
    JsonNode r0 = t1.get(0);
    Set<String> cols = new TreeSet<>();
    r0.fieldNames().forEachRemaining(cols::add);
    Set<String> expectedCols = new TreeSet<>();
    expectedCols.add("city");
    expectedCols.add("country");
    assertEquals(expectedCols, cols, "first docx table columns (row 0 treated as header)");
    assertEquals("paris", r0.get("city").asText(), "first row city");
    assertEquals("france", r0.get("country").asText(), "first row country");
  }

  // ============================================================ FILE-106 (docx typing) ===========

  @Test @Tag("FILE-106")
  void docxInfersCellTypesAndSkipsEmptyTables(@TempDir Path root) throws Exception {
    File docx = root.resolve("typed.docx").toFile();
    try (XWPFDocument doc = new XWPFDocument(); OutputStream out = Files.newOutputStream(docx.toPath())) {
      // Header row + one data row carrying a numeric cell.
      XWPFTable typed = doc.createTable(2, 2);
      setDocxCell(typed, 0, 0, "name");
      setDocxCell(typed, 0, 1, "score");
      setDocxCell(typed, 1, 0, "alice");
      setDocxCell(typed, 1, 1, "42");

      doc.createParagraph();

      // A header-only table (no data rows) => no JSON should be emitted for it.
      XWPFTable headerOnly = doc.createTable(1, 2);
      setDocxCell(headerOnly, 0, 0, "h1");
      setDocxCell(headerOnly, 0, 1, "h2");
      doc.write(out);
    }

    File outDir = Files.createDirectories(root.resolve("out")).toFile();
    DocxTableScanner.scanAndConvertTables(docx, outDir);

    // Only the table with data rows survives => a single JSON file. The header-only table is
    // dropped (no data rows), so with one surviving table there is no "__tableN" suffix.
    Set<String> emitted = tableJsonNames(outDir);
    assertEquals(1, emitted.size(),
        "header-only/empty table must produce no JSON; only the data-bearing table remains");
    assertTrue(emitted.contains("typed.json"),
        "single surviving table => unsuffixed base name, got " + emitted);

    JsonNode rows = readArray(outDir, "typed.json");
    assertEquals(1, rows.size(), "one data row");
    JsonNode r0 = rows.get(0);
    // Type inference: the "42" cell is emitted as a JSON number, not a quoted string.
    assertTrue(r0.get("score").isNumber(), "numeric cell must be a JSON number (type inference)");
    assertFalse(r0.get("score").isTextual(), "numeric cell must NOT be a quoted string");
    assertEquals(42L, r0.get("score").asLong(), "inferred numeric value");
    assertEquals("alice", r0.get("name").asText(), "text cell stays a string");
  }

  // ============================================================ FILE-019 (pptx) ==================

  @Test @Tag("FILE-019")
  void pptxEmitsOneJsonPerSlideTableAndKeepsRawStrings(@TempDir Path root) throws Exception {
    File pptx = root.resolve("deck.pptx").toFile();
    try (XMLSlideShow ppt = new XMLSlideShow(); OutputStream out = Files.newOutputStream(pptx.toPath())) {
      // Slide 1: one table (header + 1 data row, with a numeric-looking cell).
      XSLFTable s1t = ppt.createSlide().createTable();
      s1t.setAnchor(new Rectangle2D.Double(50, 50, 400, 100));
      addPptxRow(s1t, "metric", "value");
      addPptxRow(s1t, "latency", "99");

      // Slide 2: one table (header + 1 data row).
      XSLFTable s2t = ppt.createSlide().createTable();
      s2t.setAnchor(new Rectangle2D.Double(50, 50, 400, 100));
      addPptxRow(s2t, "region", "code");
      addPptxRow(s2t, "east", "us-east");
      ppt.write(out);
    }

    File outDir = Files.createDirectories(root.resolve("out")).toFile();
    // Signature: scanAndConvertTables(input, outputDir)
    PptxTableScanner.scanAndConvertTables(pptx, outDir);

    // No slide titles, no table titles => baseName + "__slide" + N + "__table".
    Set<String> expected = new TreeSet<>();
    expected.add("deck__slide1__table.json");
    expected.add("deck__slide2__table.json");
    assertEquals(expected, tableJsonNames(outDir),
        "pptx must decompose into exactly one JSON file per detected slide table");

    // RAW trimmed strings: the numeric-looking "99" stays a JSON string (no type inference),
    // contrasting with the docx behaviour above.
    JsonNode s1 = readArray(outDir, "deck__slide1__table.json");
    assertEquals(1, s1.size(), "one data row on slide 1");
    JsonNode r0 = s1.get(0);
    assertTrue(r0.get("value").isTextual(), "pptx keeps numeric-looking cell as a JSON string");
    assertFalse(r0.get("value").isNumber(), "pptx must NOT infer a numeric type");
    assertEquals("99", r0.get("value").asText(), "raw string value preserved");
    assertEquals("latency", r0.get("metric").asText(), "raw header-keyed value");
  }

  // ============================================================ helpers ==========================

  private static void setDocxCell(XWPFTable table, int rowIdx, int colIdx, String text) {
    XWPFTableRow row = table.getRow(rowIdx);
    row.getCell(colIdx).setText(text);
  }

  private static void addPptxRow(XSLFTable table, String c0, String c1) {
    XSLFTableRow row = table.addRow();
    XSLFTableCell cell0 = row.addCell();
    cell0.setText(c0);
    XSLFTableCell cell1 = row.addCell();
    cell1.setText(c1);
  }
}
