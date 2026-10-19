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
package org.apache.calcite.adapter.file.excel;

import org.apache.calcite.adapter.file.converters.ExcelReadHints;
import org.apache.calcite.adapter.file.converters.MultiTableExcelToJsonConverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.poi.ss.SpreadsheetVersion;
import org.apache.poi.ss.usermodel.Name;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.AreaReference;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFTable;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How the Excel reader finds a sheet's tables, rule by rule, and each hint an operator gives
 * it ({@link ExcelReadHints}).
 */
@Tag("unit")
public class ExcelReadHintsTest {

  @TempDir
  public File tempDir;

  /** Write the cells (null for none) of each row from row {@code top}, column {@code left}. */
  private static void cells(Sheet sheet, int top, int left, Object[]... rows) {
    for (int r = 0; r < rows.length; r++) {
      Row row = sheet.getRow(top + r) != null ? sheet.getRow(top + r) : sheet.createRow(top + r);
      for (int c = 0; c < rows[r].length; c++) {
        Object value = rows[r][c];
        if (value instanceof String) {
          row.createCell(left + c).setCellValue((String) value);
        } else if (value instanceof Number) {
          row.createCell(left + c).setCellValue(((Number) value).doubleValue());
        } else if (value instanceof Formula) {
          row.createCell(left + c).setCellFormula(((Formula) value).text);
        }
      }
    }
  }

  /** A formula cell. */
  private static final class Formula {
    final String text;

    Formula(String text) {
      this.text = text;
    }
  }

  private static Map<String, Object> map(Object... keysAndValues) {
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }

  /** Convert the workbook under the hints; each table written, by name. */
  private Map<String, JsonNode> convert(XSSFWorkbook workbook, Map<String, Object> hints)
      throws Exception {
    File out = new File(tempDir, "out-" + System.nanoTime());
    out.mkdirs();
    File file = new File(out, "book.xlsx");
    try (FileOutputStream fos = new FileOutputStream(file)) {
      workbook.write(fos);
    }
    workbook.close();
    MultiTableExcelToJsonConverter.convertFileToJson(file, out, true, "SMART_CASING",
        "SMART_CASING", out, null, ExcelReadHints.fromOperand(hints.isEmpty() ? null : hints));
    Map<String, JsonNode> tables = new TreeMap<>();
    File[] json = out.listFiles((dir, name) -> name.endsWith(".json") && !name.startsWith("."));
    for (File f : json) {
      tables.put(f.getName().replace(".json", ""), new ObjectMapper().readTree(f));
    }
    return tables;
  }

  private static List<String> columns(JsonNode rows) {
    List<String> names = new ArrayList<>();
    rows.get(0).fieldNames().forEachRemaining(names::add);
    return names;
  }

  // --- Blocks of cells -----------------------------------------------------------------------

  @Test void sideBySideTablesAreReadAsTwo() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {"name", "team"}, new Object[] {"ann", "red"});
    cells(sheet, 0, 4, new Object[] {"city", "zone"}, new Object[] {"oslo", "n"},
        new Object[] {"rome", "s"});
    Map<String, JsonNode> tables = convert(workbook, Collections.emptyMap());
    assertEquals(Arrays.asList("book__s_t1", "book__s_t2"), new ArrayList<>(tables.keySet()));
    assertEquals(Arrays.asList("name", "team"), columns(tables.get("book__s_t1")));
    assertEquals(2, tables.get("book__s_t2").size());
  }

  @Test void aSingleBlankColumnInsideATableIsKept() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {"name", null, "team"}, new Object[] {"ann", null, "red"});
    Map<String, JsonNode> tables = convert(workbook, Collections.emptyMap());
    assertEquals(Collections.singletonList("book__s"), new ArrayList<>(tables.keySet()));
    assertEquals(Arrays.asList("name", "team"), columns(tables.get("book__s")));
  }

  @Test void columnGapSetsHowManyBlankColumnsSeparateTables() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {"name", "team", null, "city", "zone"},
        new Object[] {"ann", "red", null, "oslo", "n"});
    assertEquals(Collections.singletonList("book__s"),
        new ArrayList<>(convert(sheetCopy(workbook), Collections.emptyMap()).keySet()));
    Map<String, JsonNode> tables = convert(workbook, map("columnGap", 1));
    assertEquals(Arrays.asList("book__s_t1", "book__s_t2"), new ArrayList<>(tables.keySet()));
  }

  @Test void twoBlankRowsSeparateTablesAndOneDoesNot() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {"name", "team"}, new Object[] {"ann", "red"});
    cells(sheet, 3, 0, new Object[] {"bob", "blue"});
    cells(sheet, 6, 0, new Object[] {"city", "zone"}, new Object[] {"oslo", "n"});
    Map<String, JsonNode> tables = convert(workbook, Collections.emptyMap());
    assertEquals(Arrays.asList("book__s_t1", "book__s_t2"), new ArrayList<>(tables.keySet()));
    assertEquals(2, tables.get("book__s_t1").size(), "one blank row is inside the table");
  }

  @Test void tableGapRowsSetsHowManyBlankRowsEndATable() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {"name", "team"}, new Object[] {"ann", "red"});
    cells(sheet, 3, 0, new Object[] {"city", "zone"}, new Object[] {"oslo", "n"});
    assertEquals(1, convert(sheetCopy(workbook), Collections.emptyMap()).size());
    Map<String, JsonNode> tables = convert(workbook, map("tableGapRows", 1));
    assertEquals(Arrays.asList("book__s_t1", "book__s_t2"), new ArrayList<>(tables.keySet()));
  }

  /** A copy of a one-sheet workbook, so that it can be converted twice. */
  private static XSSFWorkbook sheetCopy(XSSFWorkbook workbook) throws Exception {
    java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
    workbook.write(bytes);
    return new XSSFWorkbook(new java.io.ByteArrayInputStream(bytes.toByteArray()));
  }

  @Test void aRowOfEmptyStringsIsBlank() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {"name", "team"}, new Object[] {"ann", "red"},
        new Object[] {"", ""}, new Object[] {"", ""},
        new Object[] {"city", "zone"}, new Object[] {"oslo", "n"});
    Map<String, JsonNode> tables = convert(workbook, Collections.emptyMap());
    assertEquals(2, tables.size(), "two rows of empty strings separate the tables");
  }

  // --- Titles and header rows -----------------------------------------------------------------

  @Test void aSingleCellAboveATableIsItsTitle() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {"Staff"}, new Object[] {"name", "team"},
        new Object[] {"ann", "red"});
    Map<String, JsonNode> tables = convert(workbook, Collections.emptyMap());
    assertEquals(Collections.singletonList("book__s_staff"), new ArrayList<>(tables.keySet()));
    assertEquals(1, tables.get("book__s_staff").size());
  }

  @Test void headerRowsReadsASheetTheHeuristicMisreads() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    // Years over 1000 read as data: the reader finds no header row.
    cells(sheet, 0, 0, new Object[] {"region", 2023, 2024}, new Object[] {"north", 5, 6},
        new Object[] {"south", 7, 8});
    assertTrue(convert(sheetCopy(workbook), Collections.emptyMap()).isEmpty());
    Map<String, JsonNode> tables =
        convert(workbook, map("tables", map("book__s", map("headerRows", 1))));
    JsonNode rows = tables.get("book__s");
    assertEquals(2, rows.size());
    assertEquals("north", rows.get(0).get("region").asText());
    assertEquals(3, columns(rows).size());
  }

  @Test void headerRowsTakesThatManyRows() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {"q", null, "r"}, new Object[] {"a", "b", "c"},
        new Object[] {"1", "2", "3"});
    Map<String, JsonNode> tables = convert(workbook, map("headerRows", 1));
    assertEquals(2, tables.get("book__s").size(), "only the first row is a header row");
  }

  @Test void maxHeaderRowsCapsDetection() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {null, "q1", null, "q2", null},
        new Object[] {"region", "sales", "costs", "sales", "costs"},
        new Object[] {"north", 1, 2, 3, 4});
    sheet.addMergedRegion(new CellRangeAddress(0, 0, 1, 2));
    sheet.addMergedRegion(new CellRangeAddress(0, 0, 3, 4));
    assertEquals(Arrays.asList("region", "q1_sales", "q1_costs", "q2_sales", "q2_costs"),
        columns(convert(sheetCopy(workbook), Collections.emptyMap()).get("book__s")));
    Map<String, JsonNode> tables =
        convert(workbook, map("workbooks", map("book.xlsx", map("maxHeaderRows", 1))));
    assertEquals(2, tables.get("book__s").size(), "the second row is data");
  }

  @Test void aFormulaIsJudgedByItsValue() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {new Formula("\"na\"&\"me\""), new Formula("\"te\"&\"am\"")},
        new Object[] {"ann", "red"});
    Map<String, JsonNode> tables = convert(workbook, Collections.emptyMap());
    assertEquals(Arrays.asList("name", "team"), columns(tables.get("book__s")));
  }

  // --- Sizes -----------------------------------------------------------------------------------

  @Test void minColumnsReadsAOneColumnTable() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {"name"}, new Object[] {"ann"}, new Object[] {"bob"});
    assertTrue(convert(sheetCopy(workbook), Collections.emptyMap()).isEmpty());
    Map<String, JsonNode> tables = convert(workbook, map("minColumns", 1));
    assertEquals(2, tables.get("book__s").size());
    assertEquals(Collections.singletonList("name"), columns(tables.get("book__s")));
  }

  @Test void minDataRowsDropsSmallerTables() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {"name", "team"}, new Object[] {"ann", "red"});
    assertEquals(1, convert(sheetCopy(workbook), Collections.emptyMap()).size());
    assertTrue(convert(workbook, map("minDataRows", 2)).isEmpty());
  }

  // --- Tables the workbook or the operator declares ------------------------------------------

  @Test void anExcelTableIsReadAsItDeclaresItself() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    XSSFSheet sheet = workbook.createSheet("s");
    // Two blank rows inside: the search would end the table there; the Excel table holds them.
    cells(sheet, 0, 0, new Object[] {"name", "team"}, new Object[] {"ann", "red"});
    cells(sheet, 4, 0, new Object[] {"bob", "blue"});
    XSSFTable table = sheet.createTable(
        new AreaReference("A1:B5", SpreadsheetVersion.EXCEL2007));
    table.setName("Staff");
    Map<String, JsonNode> tables = convert(workbook, Collections.emptyMap());
    assertEquals(Collections.singletonList("book__s"), new ArrayList<>(tables.keySet()));
    assertEquals(2, tables.get("book__s").size());
  }

  @Test void aNamedRangeIsReadAsOneTable() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    // Two blank rows inside: the search would end the table there; the named range holds them.
    cells(sheet, 0, 0, new Object[] {"name", "team"}, new Object[] {"ann", "red"});
    cells(sheet, 4, 0, new Object[] {"bob", "blue"});
    Name name = workbook.createName();
    name.setNameName("staff");
    name.setRefersToFormula("s!$A$1:$B$5");
    Map<String, JsonNode> tables = convert(workbook, Collections.emptyMap());
    assertEquals(Collections.singletonList("book__s"), new ArrayList<>(tables.keySet()));
    assertEquals(2, tables.get("book__s").size());
  }

  @Test void aRangeReadsItsCellsAsTheTableItNames() throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {"Notes on the figures below"}, new Object[] {},
        new Object[] {"code", "units"}, new Object[] {"A1", "3"}, new Object[] {"B2", "4"},
        new Object[] {"C3", "5"});
    Map<String, JsonNode> tables = convert(workbook,
        map("tables", map("book__codes", map("range", "s!A3:B5"))));
    assertEquals(Collections.singletonList("book__codes"), new ArrayList<>(tables.keySet()));
    assertEquals(2, tables.get("book__codes").size(), "the range ends where it says");
  }

  // --- The operand ---------------------------------------------------------------------------

  @Test void hintsAreRefusedByWhatIsWrong() {
    IllegalArgumentException unknown = assertThrows(IllegalArgumentException.class,
        () -> ExcelReadHints.fromOperand(map("headerRow", 1)));
    assertTrue(unknown.getMessage().contains("unknown key 'headerRow'"));
    IllegalArgumentException zero = assertThrows(IllegalArgumentException.class,
        () -> ExcelReadHints.fromOperand(map("maxHeaderRows", 0)));
    assertTrue(zero.getMessage().contains("excelHints.maxHeaderRows must be a whole number"));
    IllegalArgumentException gap = assertThrows(IllegalArgumentException.class,
        () -> ExcelReadHints.fromOperand(map("tables", map("t", map("columnGap", 1)))));
    assertTrue(gap.getMessage().contains("excelHints.tables.t: unknown key 'columnGap'"));
    IllegalArgumentException range = assertThrows(IllegalArgumentException.class,
        () -> ExcelReadHints.fromOperand(map("tables", map("t", map("range", "A1:B2")))));
    assertTrue(range.getMessage().contains("names no sheet"));
  }

  @Test void theNarrowestHintWins() {
    ExcelReadHints hints = ExcelReadHints.fromOperand(map(
        "maxHeaderRows", 2,
        "workbooks", map("book.xlsx", map("maxHeaderRows", 1, "minDataRows", 3)),
        "tables", map("book__s", map("minDataRows", 5))));
    ExcelReadHints.ForTable table = hints.forTable("book.xlsx", "book__s");
    assertEquals(1, table.maxHeaderRows);
    assertEquals(5, table.minDataRows);
    assertEquals(ExcelReadHints.DEFAULT_MIN_COLUMNS, table.minColumns);
    assertEquals(2, hints.forTable("other.xlsx", "other__s").maxHeaderRows);
    assertEquals(null, hints.forTable("other.xlsx", "other__s").headerRows);
  }

  @Test void aSchemaReadsItsWorkbooksUnderItsHintsAndAgainWhenTheyChange(@TempDir Path root)
      throws Exception {
    Path src = Files.createDirectories(root.resolve("src"));
    Path cache = Files.createDirectories(root.resolve("cache"));
    XSSFWorkbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("s");
    cells(sheet, 0, 0, new Object[] {"region", 2023}, new Object[] {"north", 5});
    try (FileOutputStream fos = new FileOutputStream(src.resolve("book.xlsx").toFile())) {
      workbook.write(fos);
    }
    workbook.close();
    assertEquals(Collections.singletonList("book__s"),
        tables(src, cache, "{\"headerRows\": 1}"));
    // Without the hint, the years read as data: the table found under it is gone.
    assertEquals(Collections.emptyList(), tables(src, cache, null), () -> listing(cache));
  }

  private static String listing(Path dir) {
    try (java.util.stream.Stream<Path> files = Files.walk(dir)) {
      StringBuilder out = new StringBuilder();
      files.forEach(f -> {
        out.append(dir.relativize(f)).append('\n');
        if (f.toString().endsWith("conversions.json") || f.getFileName().toString().startsWith(".")) {
          try {
            if (Files.isRegularFile(f)) {
              out.append(new String(Files.readAllBytes(f), java.nio.charset.StandardCharsets.UTF_8)).append('\n');
            }
          } catch (java.io.IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      return out.toString();
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<String> tables(Path src, Path cache, String hints) throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:{\"version\": \"1.0\", \"defaultSchema\": \"s\", \"schemas\": [{"
        + "\"name\": \"s\", \"type\": \"custom\","
        + " \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\", \"operand\": {"
        + "\"directory\": \"" + src.toString().replace("\\", "\\\\") + "\","
        + " \"baseDirectory\": \"" + cache.toString().replace("\\", "\\\\") + "\","
        + " \"ephemeralCache\": false, \"executionEngine\": \"linq4j\""
        + (hints == null ? "" : ", \"excelHints\": " + hints) + "}}]}");
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    List<String> names = new ArrayList<>();
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         ResultSet rs = conn.getMetaData().getTables(null, "s", "%", new String[] {"TABLE"})) {
      while (rs.next()) {
        names.add(rs.getString("TABLE_NAME"));
      }
    }
    Collections.sort(names);
    return names;
  }
}
