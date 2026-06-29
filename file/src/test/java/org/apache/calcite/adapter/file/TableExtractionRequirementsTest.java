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

import org.apache.calcite.adapter.file.converters.HtmlToJsonConverter;
import org.apache.calcite.adapter.file.converters.MarkdownTableScanner;
import org.apache.calcite.adapter.file.converters.MultiTableExcelToJsonConverter;
import org.apache.calcite.adapter.file.format.json.JsonMultiTableFactory;
import org.apache.calcite.adapter.file.format.json.JsonSearchConfig;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Sources;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-017 / FILE-021 / FILE-102 / FILE-056 — golden assertions for the simple-format
 * table-extraction law, extending the decomposition law already proven in
 * {@link ConverterDecompositionTest}: an HTML / markdown / multi-table-Excel / JSONPath source
 * decomposes into exactly one logical table per discovered structure.
 *
 * <p>Each fixture is generated in-test (HTML and markdown as plain strings, xlsx via
 * {@link XSSFWorkbook}), then the exact public {@code static} converter entry point read from the
 * converter sources is invoked directly. Emitted JSON array files are read back and asserted
 * exactly — both the emitted file-SET (names) and the table content.
 *
 * <p>Behavioural contrasts pinned here (mirroring the docx/pptx contrast in
 * {@link ConverterDecompositionTest}):
 * <ul>
 *   <li>{@link HtmlToJsonConverter} runs per-cell type inference via
 *       {@code ConverterUtils.setJsonValueWithTypeInference}, so a numeric-looking cell becomes a
 *       JSON number.</li>
 *   <li>{@link MarkdownTableScanner} keeps RAW trimmed strings (no inference), so a numeric-looking
 *       cell stays a JSON string; empty cells are dropped (key absent), never written.</li>
 *   <li>{@link MultiTableExcelToJsonConverter} writes an empty cell as JSON {@code null}
 *       (key present, {@code putNull}).</li>
 * </ul>
 */
@Tag("unit")
public class TableExtractionRequirementsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** The set of emitted JSON-array (table-data) file names directly under {@code dir}. */
  private static Set<String> tableJsonNames(File dir) throws Exception {
    Set<String> names = new TreeSet<>();
    File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isFile() && f.getName().endsWith(".json")) {
          String content = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8).trim();
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

  private static Set<String> fieldNames(JsonNode obj) {
    Set<String> cols = new TreeSet<>();
    obj.fieldNames().forEachRemaining(cols::add);
    return cols;
  }

  // ============================================================ FILE-017 (HTML) ==================

  /**
   * FILE-017: {@link HtmlToJsonConverter} emits one JSON array file per detected HTML
   * {@code <table>}. Casing is set to {@code UNCHANGED} so the default table names are exactly
   * {@code T1}/{@code T2} and the header text is used verbatim as the JSON keys — keeping the
   * golden assertions deterministic without depending on the SMART_CASING transform.
   */
  @Test @Tag("FILE-017")
  void htmlEmitsOneJsonPerHtmlTable(@TempDir Path root) throws Exception {
    String html =
        "<html><body>"
        + "<table>"
        + "<tr><th>city</th><th>country</th></tr>"
        + "<tr><td>paris</td><td>france</td></tr>"
        + "<tr><td>tokyo</td><td>japan</td></tr>"
        + "</table>"
        + "<table>"
        + "<tr><th>team</th><th>wins</th></tr>"
        + "<tr><td>reds</td><td>12</td></tr>"
        + "</table>"
        + "</body></html>";
    File htmlFile = root.resolve("doc.html").toFile();
    Files.write(htmlFile.toPath(), html.getBytes(StandardCharsets.UTF_8));

    File outDir = Files.createDirectories(root.resolve("out")).toFile();
    // Signature: convert(htmlFile, outputDir, columnNameCasing, tableNameCasing, baseDirectory)
    HtmlToJsonConverter.convert(htmlFile, outDir, "UNCHANGED", "UNCHANGED", outDir);

    // Two HTML tables, no ids/captions/headings => default names T1, T2 =>
    // file name is baseName + "__" + tableName.
    Set<String> expected = new TreeSet<>();
    expected.add("doc__T1.json");
    expected.add("doc__T2.json");
    assertEquals(expected, tableJsonNames(outDir),
        "HTML must decompose into exactly one JSON file per detected <table>");

    // Headers + a row of the first table.
    JsonNode t1 = readArray(outDir, "doc__T1.json");
    assertEquals(2, t1.size(), "two data rows in first HTML table");
    JsonNode r0 = t1.get(0);
    Set<String> expectedCols = new TreeSet<>();
    expectedCols.add("city");
    expectedCols.add("country");
    assertEquals(expectedCols, fieldNames(r0), "first HTML table columns (th row used as headers)");
    assertEquals("paris", r0.get("city").asText(), "first row city");
    assertEquals("france", r0.get("country").asText(), "first row country");

    // Type inference: the "12" cell in the second table is emitted as a JSON number.
    JsonNode t2 = readArray(outDir, "doc__T2.json");
    assertEquals(1, t2.size(), "one data row in second HTML table");
    JsonNode w0 = t2.get(0);
    assertTrue(w0.get("wins").isNumber(), "HTML infers a numeric type for a numeric-looking cell");
    assertEquals(12L, w0.get("wins").asLong(), "inferred numeric value");
    assertEquals("reds", w0.get("team").asText(), "text cell stays a string");
  }

  // ============================================================ FILE-021 (markdown) =============

  /**
   * FILE-021: {@link MarkdownTableScanner} emits one JSON array file per detected pipe table.
   * Markdown keeps RAW trimmed strings (no type inference). Two tables in one {@code .md} =>
   * {@code base__Table1.json}/{@code base__Table2.json} (the {@code totalTables > 1} suffix rule).
   * The base name is PascalCase of the file name, so {@code notes.md} => {@code Notes}.
   */
  @Test @Tag("FILE-021")
  void markdownEmitsOneJsonPerPipeTableAndKeepsRawStrings(@TempDir Path root) throws Exception {
    String md =
        "| city | country |\n"
        + "| --- | --- |\n"
        + "| paris | france |\n"
        + "| tokyo | japan |\n"
        + "\n"
        + "| team | wins |\n"
        + "| --- | --- |\n"
        + "| reds | 12 |\n";
    File mdFile = root.resolve("notes.md").toFile();
    Files.write(mdFile.toPath(), md.getBytes(StandardCharsets.UTF_8));

    File outDir = Files.createDirectories(root.resolve("out")).toFile();
    // Signature: scanAndConvertTables(inputFile, outputDir)
    MarkdownTableScanner.scanAndConvertTables(mdFile, outDir);

    // Two tables, no titles => baseName ("Notes") + "__Table" + (index + 1).
    Set<String> expected = new TreeSet<>();
    expected.add("Notes__Table1.json");
    expected.add("Notes__Table2.json");
    assertEquals(expected, tableJsonNames(outDir),
        "markdown must decompose into exactly one JSON file per detected pipe table");

    // Headers + a row of the first table.
    JsonNode t1 = readArray(outDir, "Notes__Table1.json");
    assertEquals(2, t1.size(), "two data rows in first markdown table");
    JsonNode r0 = t1.get(0);
    Set<String> expectedCols = new TreeSet<>();
    expectedCols.add("city");
    expectedCols.add("country");
    assertEquals(expectedCols, fieldNames(r0), "first markdown table columns");
    assertEquals("paris", r0.get("city").asText(), "first row city");
    assertEquals("france", r0.get("country").asText(), "first row country");

    // RAW trimmed strings: the numeric-looking "12" stays a JSON string (no type inference),
    // contrasting with the HTML behaviour above.
    JsonNode t2 = readArray(outDir, "Notes__Table2.json");
    assertEquals(1, t2.size(), "one data row in second markdown table");
    JsonNode w0 = t2.get(0);
    assertTrue(w0.get("wins").isTextual(), "markdown keeps numeric-looking cell as a JSON string");
    assertFalse(w0.get("wins").isNumber(), "markdown must NOT infer a numeric type");
    assertEquals("12", w0.get("wins").asText(), "raw string value preserved");
  }

  // ============================================================ FILE-102 (Excel multi-table) ====

  /**
   * FILE-102: {@link MultiTableExcelToJsonConverter} splits ONE sheet into multiple tables when
   * {@code >= MIN_EMPTY_ROWS_BETWEEN_TABLES} (2) blank rows separate two data blocks. A
   * single-non-empty-cell row is treated as a TITLE row. An empty cell within a row is written as
   * JSON {@code null} (key present, {@code putNull}). The sheet here lays out:
   *
   * <pre>
   *   row 0:  id | name              (block 1 header)
   *   row 1:  1  | alice             (block 1 data; "name" cell deliberately left empty)
   *   row 2:  (blank)
   *   row 3:  (blank)                 >= 2 blank rows => table boundary
   *   row 4:  second                  single-cell TITLE row for block 2
   *   row 5:  sku | label             (block 2 header)
   *   row 6:  100 | widget            (block 2 data)
   * </pre>
   */
  @Test @Tag("FILE-102")
  void excelSplitsOneSheetOnBlankRowsAndWritesEmptyCellAsNull(@TempDir Path root) throws Exception {
    File xlsx = root.resolve("blocks.xlsx").toFile();
    try (XSSFWorkbook wb = new XSSFWorkbook(); OutputStream out = Files.newOutputStream(xlsx.toPath())) {
      Sheet sheet = wb.createSheet("data");

      // Block 1: header + one data row whose second cell is intentionally empty.
      Row h1 = sheet.createRow(0);
      h1.createCell(0).setCellValue("id");
      h1.createCell(1).setCellValue("name");
      Row d1 = sheet.createRow(1);
      d1.createCell(0).setCellValue(1);
      // Leave column 1 (name) empty so the converter exercises the putNull path.

      // rows 2 and 3 left entirely empty => 2 blank rows => table boundary.

      // Block 2: a single-cell TITLE row, then header, then a data row.
      Row title = sheet.createRow(4);
      title.createCell(0).setCellValue("second");
      Row h2 = sheet.createRow(5);
      h2.createCell(0).setCellValue("sku");
      h2.createCell(1).setCellValue("label");
      Row d2 = sheet.createRow(6);
      d2.createCell(0).setCellValue(100);
      d2.createCell(1).setCellValue("widget");

      wb.write(out);
    }

    File outDir = Files.createDirectories(root.resolve("out")).toFile();
    // Signature: convertFileToJson(input, outputDir, detectMultipleTables, tableNameCasing,
    //            columnNameCasing, baseDirectory)
    MultiTableExcelToJsonConverter.convertFileToJson(xlsx, outDir, true, "UNCHANGED", "UNCHANGED", outDir);

    // TWO tables emerge from the single sheet.
    Set<String> emitted = tableJsonNames(outDir);
    assertEquals(2, emitted.size(),
        "one sheet with two blank-row-separated blocks must yield two JSON tables, got " + emitted);

    // Block 2 carried a single-cell TITLE row ("second"); that title becomes part of the file name.
    // Block 1 had no title. Locate each table by its columns rather than hard-coding the full
    // (casing/identifier-sanitised) file names.
    JsonNode block1 = null;
    JsonNode block2 = null;
    for (String name : emitted) {
      JsonNode arr = readArray(outDir, name);
      assertTrue(arr.size() > 0, "no zero-data-row table should be emitted: " + name);
      Set<String> cols = fieldNames(arr.get(0));
      if (cols.contains("id")) {
        block1 = arr;
      } else if (cols.contains("sku")) {
        block2 = arr;
      }
    }
    assertTrue(block1 != null, "block 1 (id/name) table must be emitted");
    assertTrue(block2 != null, "block 2 (sku/label) table must be emitted");

    // Empty cell is present-as-null: the "name" key exists on the row and is JSON null.
    assertEquals(1, block1.size(), "one data row in block 1");
    JsonNode r0 = block1.get(0);
    assertTrue(r0.has("name"), "empty cell still yields the key (putNull keeps structure)");
    assertTrue(r0.get("name").isNull(), "empty cell must be written as JSON null, not omitted");
    assertEquals(1L, r0.get("id").asLong(), "non-empty numeric cell preserved");

    // Block 2 content.
    assertEquals(1, block2.size(), "one data row in block 2");
    JsonNode s0 = block2.get(0);
    assertEquals("widget", s0.get("label").asText(), "block 2 text cell");
    assertEquals(100L, s0.get("sku").asLong(), "block 2 numeric cell");
  }

  // ============================================================ FILE-056 (JSONPath multi-table) =

  /**
   * FILE-056: configuring {@code jsonSearchPaths} (JSONPath like {@code $.data.users}) yields one
   * table per path. Invoked through the real production entry point
   * {@link JsonMultiTableFactory#createTables(org.apache.calcite.util.Source, JsonSearchConfig)} —
   * the same call {@code FileSchema} makes when a tableDef carries {@code jsonSearchPaths}.
   *
   * <p>NOTE: the spec's "a nested array under a row becomes a CHILD table whose rows carry the
   * parent's id as an FK column" describes a feature that does not exist in this codebase. The
   * {@code jsonSearchPaths} path resolves each configured JSONPath to exactly one
   * {@code JsonScannableTable} via {@code JsonMultiTableFactory.createTables}; there is no
   * automatic nested-array-to-child-table promotion and no synthesised parent-id FK column
   * (verified: no such logic in {@code format/json/*} or {@code FileSchema.registerDuckDBNativeTable}
   * / the {@code jsonSearchPaths} branch). That half of the requirement is therefore OMITTED.
   * To still cover the parent/child shape we configure BOTH the parent path and the nested-array
   * path explicitly and assert each becomes its own table — which is the actual mechanism by which
   * a nested array is surfaced as a separate table here.
   */
  @Test @Tag("FILE-056")
  void jsonSearchPathsYieldOneTablePerPath(@TempDir Path root) throws Exception {
    // $.data.users is an array of user objects; each user has a nested "orders" array.
    String json =
        "{\"data\": {"
        + "  \"users\": ["
        + "    {\"id\": 1, \"name\": \"alice\", \"orders\": [{\"oid\": 100}, {\"oid\": 101}]},"
        + "    {\"id\": 2, \"name\": \"bob\", \"orders\": [{\"oid\": 200}]}"
        + "  ],"
        + "  \"products\": [{\"sku\": \"w1\"}, {\"sku\": \"w2\"}]"
        + "}}";
    File jsonFile = root.resolve("api.json").toFile();
    Files.write(jsonFile.toPath(), json.getBytes(StandardCharsets.UTF_8));

    // Two top-level paths => two tables, named by their last path segment.
    List<String> paths = new ArrayList<>();
    paths.add("$.data.users");
    paths.add("$.data.products");
    JsonSearchConfig config = new JsonSearchConfig().withJsonSearchPaths(paths);

    Map<String, Table> tables =
        JsonMultiTableFactory.createTables(Sources.of(jsonFile), config);

    assertEquals(2, tables.size(), "two configured JSONPaths must yield exactly two tables");
    assertTrue(tables.containsKey("users"), "table named after last path segment of $.data.users");
    assertTrue(tables.containsKey("products"),
        "table named after last path segment of $.data.products");

    // The "nested array becomes its own table" mechanism: point a path AT the nested array.
    // (This is the real way a nested array is surfaced as a separate table; there is no automatic
    //  parent-id FK column — see the class/method NOTE above.)
    List<String> nestedPaths = new ArrayList<>();
    nestedPaths.add("$.data.users");
    nestedPaths.add("$.data.users[0].orders");
    JsonSearchConfig nestedConfig = new JsonSearchConfig().withJsonSearchPaths(nestedPaths);

    Map<String, Table> nestedTables =
        JsonMultiTableFactory.createTables(Sources.of(jsonFile), nestedConfig);

    assertEquals(2, nestedTables.size(),
        "a parent path plus an explicit nested-array path yield two tables");
    assertTrue(nestedTables.containsKey("users"), "parent table from $.data.users");
    assertTrue(nestedTables.containsKey("orders"),
        "child table from the nested $.data.users[0].orders path");
  }
}
