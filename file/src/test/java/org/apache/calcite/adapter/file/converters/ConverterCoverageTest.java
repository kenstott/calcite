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

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive coverage tests for converter classes:
 * {@link FileConversionManager}, {@link HtmlTableScanner}, and {@link YamlPathConverter}.
 */
@Tag("unit")
class ConverterCoverageTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir
  Path tempDir;

  private File outputDir;
  private File baseDir;

  @BeforeEach
  void setUp() throws IOException {
    outputDir = Files.createDirectories(tempDir.resolve("output")).toFile();
    baseDir = Files.createDirectories(tempDir.resolve("base")).toFile();
  }

  // ===================================================================
  // FileConversionManager - singleton
  // ===================================================================

  @Test void testGetInstanceReturnsNonNull() {
    FileConversionManager instance = FileConversionManager.getInstance();
    assertNotNull(instance);
  }

  @Test void testGetInstanceReturnsSameSingleton() {
    FileConversionManager first = FileConversionManager.getInstance();
    FileConversionManager second = FileConversionManager.getInstance();
    assertSame(first, second, "getInstance() should return the same singleton instance");
  }

  // ===================================================================
  // FileConversionManager - requiresConversion
  // ===================================================================

  @Test void testRequiresConversionExcelFormats() {
    assertTrue(FileConversionManager.requiresConversion("report.xlsx"));
    assertTrue(FileConversionManager.requiresConversion("report.xls"));
    assertTrue(FileConversionManager.requiresConversion("REPORT.XLSX"));
    assertTrue(FileConversionManager.requiresConversion("REPORT.XLS"));
  }

  @Test void testRequiresConversionHtmlFormats() {
    assertTrue(FileConversionManager.requiresConversion("page.html"));
    assertTrue(FileConversionManager.requiresConversion("page.htm"));
    assertTrue(FileConversionManager.requiresConversion("PAGE.HTML"));
    assertTrue(FileConversionManager.requiresConversion("PAGE.HTM"));
  }

  @Test void testRequiresConversionXml() {
    assertTrue(FileConversionManager.requiresConversion("data.xml"));
    assertTrue(FileConversionManager.requiresConversion("DATA.XML"));
  }

  @Test void testRequiresConversionMarkdown() {
    assertTrue(FileConversionManager.requiresConversion("readme.md"));
    assertTrue(FileConversionManager.requiresConversion("README.MD"));
  }

  @Test void testRequiresConversionDocx() {
    assertTrue(FileConversionManager.requiresConversion("report.docx"));
    assertTrue(FileConversionManager.requiresConversion("REPORT.DOCX"));
  }

  @Test void testRequiresConversionPptx() {
    assertTrue(FileConversionManager.requiresConversion("slides.pptx"));
    assertTrue(FileConversionManager.requiresConversion("SLIDES.PPTX"));
  }

  @Test void testRequiresConversionReturnsFalseForDirectFormats() {
    assertFalse(FileConversionManager.requiresConversion("data.csv"));
    assertFalse(FileConversionManager.requiresConversion("data.json"));
    assertFalse(FileConversionManager.requiresConversion("data.parquet"));
    assertFalse(FileConversionManager.requiresConversion("data.arrow"));
    assertFalse(FileConversionManager.requiresConversion("config.yaml"));
    assertFalse(FileConversionManager.requiresConversion("config.yml"));
    assertFalse(FileConversionManager.requiresConversion("data.tsv"));
    assertFalse(FileConversionManager.requiresConversion("data.txt"));
    assertFalse(FileConversionManager.requiresConversion("unknown.foo"));
  }

  // ===================================================================
  // FileConversionManager - isDirectlyUsable
  // ===================================================================

  @Test void testIsDirectlyUsableCsvFormats() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.csv"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.csv.gz"));
    assertTrue(FileConversionManager.isDirectlyUsable("DATA.CSV"));
    assertTrue(FileConversionManager.isDirectlyUsable("DATA.CSV.GZ"));
  }

  @Test void testIsDirectlyUsableTsvFormats() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.tsv"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.tsv.gz"));
    assertTrue(FileConversionManager.isDirectlyUsable("DATA.TSV"));
    assertTrue(FileConversionManager.isDirectlyUsable("DATA.TSV.GZ"));
  }

  @Test void testIsDirectlyUsableJsonFormats() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.json"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.json.gz"));
    assertTrue(FileConversionManager.isDirectlyUsable("DATA.JSON"));
    assertTrue(FileConversionManager.isDirectlyUsable("DATA.JSON.GZ"));
  }

  @Test void testIsDirectlyUsableParquet() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.parquet"));
    assertTrue(FileConversionManager.isDirectlyUsable("DATA.PARQUET"));
  }

  @Test void testIsDirectlyUsableArrow() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.arrow"));
    assertTrue(FileConversionManager.isDirectlyUsable("DATA.ARROW"));
  }

  @Test void testIsDirectlyUsableYamlFormats() {
    assertTrue(FileConversionManager.isDirectlyUsable("config.yaml"));
    assertTrue(FileConversionManager.isDirectlyUsable("config.yml"));
    assertTrue(FileConversionManager.isDirectlyUsable("CONFIG.YAML"));
    assertTrue(FileConversionManager.isDirectlyUsable("CONFIG.YML"));
  }

  @Test void testIsDirectlyUsableReturnsFalseForConvertibleFormats() {
    assertFalse(FileConversionManager.isDirectlyUsable("test.xlsx"));
    assertFalse(FileConversionManager.isDirectlyUsable("test.xls"));
    assertFalse(FileConversionManager.isDirectlyUsable("test.html"));
    assertFalse(FileConversionManager.isDirectlyUsable("test.htm"));
    assertFalse(FileConversionManager.isDirectlyUsable("test.xml"));
    assertFalse(FileConversionManager.isDirectlyUsable("test.md"));
    assertFalse(FileConversionManager.isDirectlyUsable("test.docx"));
    assertFalse(FileConversionManager.isDirectlyUsable("test.pptx"));
    assertFalse(FileConversionManager.isDirectlyUsable("test.txt"));
    assertFalse(FileConversionManager.isDirectlyUsable("test.unknown"));
  }

  // ===================================================================
  // FileConversionManager - registerConverter
  // ===================================================================

  @Test void testRegisterConverterAddsConverter() {
    FileConversionManager manager = FileConversionManager.getInstance();

    FileConverter testConverter = new FileConverter() {
      @Override public boolean canConvert(String sourceFormat, String targetFormat) {
        return "custom".equals(sourceFormat) && "json".equals(targetFormat);
      }

      @Override public String getSourceFormat() {
        return "custom";
      }

      @Override public String getTargetFormat() {
        return "json";
      }

      @Override public List<String> convert(String sourcePath, String targetDirectory,
          ConversionMetadata metadata) {
        return Collections.emptyList();
      }
    };

    // Should not throw
    manager.registerConverter(testConverter);
  }

  @Test void testRegisterMultipleConverters() {
    FileConversionManager manager = FileConversionManager.getInstance();

    for (int i = 0; i < 3; i++) {
      final String fmt = "fmt" + i;
      manager.registerConverter(new FileConverter() {
        @Override public boolean canConvert(String sourceFormat, String targetFormat) {
          return fmt.equals(sourceFormat) && "json".equals(targetFormat);
        }

        @Override public String getSourceFormat() {
          return fmt;
        }

        @Override public String getTargetFormat() {
          return "json";
        }

        @Override public List<String> convert(String sourcePath, String targetDirectory,
            ConversionMetadata metadata) {
          return Collections.emptyList();
        }
      });
    }
    // Should not throw
  }

  // ===================================================================
  // FileConversionManager - convert with unknown format
  // ===================================================================

  @Test void testConvertUnknownFormatThrowsIOException() {
    FileConversionManager manager = FileConversionManager.getInstance();
    File source = new File(tempDir.toFile(), "data.unknown_format");

    assertThrows(IOException.class, () ->
        manager.convert(source, outputDir, "totally_unknown", "json"));
  }

  // ===================================================================
  // FileConversionManager - findOriginalSource
  // ===================================================================

  @Test void testFindOriginalSourceNoHistory() {
    File file = new File(tempDir.toFile(), "plain.csv");
    File result = FileConversionManager.findOriginalSource(file);
    // Without conversion history, returns the same file
    assertEquals(file, result);
  }

  @Test void testFindOriginalSourceNonExistentFile() {
    File file = new File(tempDir.toFile(), "does_not_exist.json");
    File result = FileConversionManager.findOriginalSource(file);
    assertEquals(file, result);
  }

  // ===================================================================
  // FileConversionManager - convertIfNeeded
  // ===================================================================

  @Test void testConvertIfNeededCsvReturnsFalse() {
    File csv = new File(tempDir.toFile(), "data.csv");
    boolean converted = FileConversionManager.convertIfNeeded(csv, outputDir, "UNCHANGED");
    assertFalse(converted);
  }

  @Test void testConvertIfNeededParquetReturnsFalse() {
    File parquet = new File(tempDir.toFile(), "data.parquet");
    boolean converted = FileConversionManager.convertIfNeeded(parquet, outputDir, "UNCHANGED");
    assertFalse(converted);
  }

  @Test void testConvertIfNeededTsvReturnsFalse() {
    File tsv = new File(tempDir.toFile(), "data.tsv");
    boolean converted = FileConversionManager.convertIfNeeded(tsv, outputDir, "UNCHANGED");
    assertFalse(converted);
  }

  @Test void testConvertIfNeededJsonWithoutPathReturnsFalse() throws IOException {
    File jsonFile = new File(tempDir.toFile(), "plain.json");
    Files.write(jsonFile.toPath(),
        "[{\"a\": 1}]".getBytes(StandardCharsets.UTF_8));
    boolean converted =
        FileConversionManager.convertIfNeeded(jsonFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);
    assertFalse(converted);
  }

  @Test void testConvertIfNeededYamlWithoutPathReturnsFalse() throws IOException {
    File yamlFile = new File(tempDir.toFile(), "config.yaml");
    Files.write(yamlFile.toPath(),
        "key: value\n".getBytes(StandardCharsets.UTF_8));
    boolean converted =
        FileConversionManager.convertIfNeeded(yamlFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);
    assertFalse(converted);
  }

  @Test void testConvertIfNeededYmlExtensionReturnsFalse() throws IOException {
    File ymlFile = new File(tempDir.toFile(), "config.yml");
    Files.write(ymlFile.toPath(),
        "key: value\n".getBytes(StandardCharsets.UTF_8));
    boolean converted =
        FileConversionManager.convertIfNeeded(ymlFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);
    assertFalse(converted);
  }

  @Test void testConvertIfNeededHtmlFileConverts() throws IOException {
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>Name</th><th>Age</th></tr>"
        + "<tr><td>Alice</td><td>30</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("test.html", html);
    boolean converted =
        FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);
    assertTrue(converted);
  }

  @Test void testConvertIfNeededTwoArgCasing() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>Col</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File htmlFile = createTempFile("two_arg.html", html);
    boolean converted =
        FileConversionManager.convertIfNeeded(htmlFile, outputDir, "SMART_CASING");
    assertTrue(converted);
  }

  @Test void testConvertIfNeededThreeArgCasing() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>Col</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File htmlFile = createTempFile("three_arg.html", html);
    boolean converted =
        FileConversionManager.convertIfNeeded(htmlFile, outputDir, "SMART_CASING", "SMART_CASING");
    assertTrue(converted);
  }

  @Test void testConvertIfNeededWithNullBaseDirectory() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>X</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File htmlFile = createTempFile("null_base.html", html);
    boolean converted =
        FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED", "UNCHANGED", null);
    assertTrue(converted);
  }

  @Test void testConvertIfNeededSkipsUnchangedFile() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>A</th></tr><tr><td>1</td></tr></table>"
        + "</body></html>";

    File htmlFile = createTempFile("skip_unchanged.html", html);

    boolean first =
        FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);
    assertTrue(first);

    boolean second =
        FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir);
    assertFalse(second);
  }

  @Test void testConvertIfNeededNonExistentFileFails() {
    File nonExistent = new File(tempDir.toFile(), "does_not_exist.html");
    boolean converted =
        FileConversionManager.convertIfNeeded(nonExistent, outputDir, "UNCHANGED", "UNCHANGED", baseDir);
    // Non-existent file should fail gracefully
    assertFalse(converted);
  }

  @Test void testConvertIfNeededWithRelativePath() throws IOException {
    String html = "<html><body>"
        + "<table><tr><th>Z</th></tr><tr><td>99</td></tr></table>"
        + "</body></html>";

    File htmlFile = createTempFile("rp.html", html);
    boolean converted =
        FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED", "UNCHANGED", baseDir,
        "subdir" + File.separator + "rp.html");
    assertTrue(converted);
  }

  // ===================================================================
  // HtmlTableScanner - TableInfo inner class
  // ===================================================================

  @Test void testTableInfoConstructorAndGetters() {
    HtmlTableScanner.TableInfo info =
        new HtmlTableScanner.TableInfo("employees", "#emp-table", 0, 5);

    assertEquals("employees", info.name);
    assertEquals("#emp-table", info.selector);
    assertEquals(0, info.index);
    assertEquals(5, info.rowCount);
  }

  @Test void testTableInfoWithDifferentValues() {
    HtmlTableScanner.TableInfo info =
        new HtmlTableScanner.TableInfo("sales_data", "table[index=2]", 2, 100);

    assertEquals("sales_data", info.name);
    assertEquals("table[index=2]", info.selector);
    assertEquals(2, info.index);
    assertEquals(100, info.rowCount);
  }

  @Test void testTableInfoZeroRowCount() {
    HtmlTableScanner.TableInfo info =
        new HtmlTableScanner.TableInfo("empty_table", "#empty", 0, 0);

    assertEquals("empty_table", info.name);
    assertEquals(0, info.rowCount);
  }

  // ===================================================================
  // HtmlTableScanner - scanTables
  // ===================================================================

  @Test void testScanTablesSingleTable() throws IOException {
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>Name</th><th>Age</th></tr>"
        + "<tr><td>Alice</td><td>30</td></tr>"
        + "<tr><td>Bob</td><td>25</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("single_table.html", html);
    Source source = Sources.of(htmlFile);

    List<HtmlTableScanner.TableInfo> tables = HtmlTableScanner.scanTables(source);

    assertNotNull(tables);
    assertEquals(1, tables.size());

    HtmlTableScanner.TableInfo table = tables.get(0);
    assertNotNull(table.name);
    assertEquals(0, table.index);
    assertEquals(3, table.rowCount); // header row + 2 data rows
  }

  @Test void testScanTablesMultipleTables() throws IOException {
    String html = "<html><body>"
        + "<table id=\"t1\">"
        + "<tr><th>A</th></tr>"
        + "<tr><td>1</td></tr>"
        + "</table>"
        + "<table id=\"t2\">"
        + "<tr><th>B</th></tr>"
        + "<tr><td>2</td></tr>"
        + "<tr><td>3</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("multi_table.html", html);
    Source source = Sources.of(htmlFile);

    List<HtmlTableScanner.TableInfo> tables = HtmlTableScanner.scanTables(source);

    assertNotNull(tables);
    assertEquals(2, tables.size());
    assertEquals(0, tables.get(0).index);
    assertEquals(1, tables.get(1).index);
    assertEquals(2, tables.get(0).rowCount);
    assertEquals(3, tables.get(1).rowCount);
  }

  @Test void testScanTablesNoTables() throws IOException {
    String html = "<html><body><p>No tables here</p></body></html>";

    File htmlFile = createTempFile("no_tables.html", html);
    Source source = Sources.of(htmlFile);

    List<HtmlTableScanner.TableInfo> tables = HtmlTableScanner.scanTables(source);

    assertNotNull(tables);
    assertTrue(tables.isEmpty());
  }

  @Test void testScanTablesWithTableId() throws IOException {
    String html = "<html><body>"
        + "<table id=\"employees\">"
        + "<tr><th>Name</th></tr>"
        + "<tr><td>Alice</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("id_table.html", html);
    Source source = Sources.of(htmlFile);

    List<HtmlTableScanner.TableInfo> tables = HtmlTableScanner.scanTables(source);

    assertEquals(1, tables.size());
    // Table with valid CSS id should use # selector
    assertTrue(tables.get(0).selector.startsWith("#"),
        "Selector should use # prefix for table with valid id");
  }

  @Test void testScanTablesWithCaption() throws IOException {
    String html = "<html><body>"
        + "<table>"
        + "<caption>Sales Data</caption>"
        + "<tr><th>Region</th><th>Revenue</th></tr>"
        + "<tr><td>North</td><td>1000</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("caption_table.html", html);
    Source source = Sources.of(htmlFile);

    List<HtmlTableScanner.TableInfo> tables = HtmlTableScanner.scanTables(source);

    assertEquals(1, tables.size());
    assertNotNull(tables.get(0).name);
    // The name should be derived from the caption
    assertFalse(tables.get(0).name.isEmpty());
  }

  @Test void testScanTablesWithPrecedingHeading() throws IOException {
    String html = "<html><body>"
        + "<h2>Employee List</h2>"
        + "<table>"
        + "<tr><th>Name</th></tr>"
        + "<tr><td>Alice</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("heading_table.html", html);
    Source source = Sources.of(htmlFile);

    List<HtmlTableScanner.TableInfo> tables = HtmlTableScanner.scanTables(source);

    assertEquals(1, tables.size());
    assertNotNull(tables.get(0).name);
    assertFalse(tables.get(0).name.isEmpty());
  }

  @Test void testScanTablesWithColumnNameCasing() throws IOException {
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>First Name</th></tr>"
        + "<tr><td>Alice</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("casing_table.html", html);
    Source source = Sources.of(htmlFile);

    List<HtmlTableScanner.TableInfo> tables =
        HtmlTableScanner.scanTables(source, "SMART_CASING");

    assertEquals(1, tables.size());
    assertNotNull(tables.get(0).name);
  }

  @Test void testScanTablesDuplicateNames() throws IOException {
    // Two tables with no identifying attributes will get default names
    // The scanner should produce unique names
    String html = "<html><body>"
        + "<table>"
        + "<tr><th>A</th></tr><tr><td>1</td></tr>"
        + "</table>"
        + "<table>"
        + "<tr><th>B</th></tr><tr><td>2</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("dup_names.html", html);
    Source source = Sources.of(htmlFile);

    List<HtmlTableScanner.TableInfo> tables = HtmlTableScanner.scanTables(source);

    assertEquals(2, tables.size());
    // Names should be different
    assertFalse(tables.get(0).name.equals(tables.get(1).name),
        "Duplicate table names should be de-duplicated");
  }

  @Test void testScanTablesWithSpecialCharsInId() throws IOException {
    // Table with CSS-incompatible id should fall back to index-based selector
    String html = "<html><body>"
        + "<table id=\"data.table#1\">"
        + "<tr><th>X</th></tr><tr><td>1</td></tr>"
        + "</table>"
        + "</body></html>";

    File htmlFile = createTempFile("special_id.html", html);
    Source source = Sources.of(htmlFile);

    List<HtmlTableScanner.TableInfo> tables = HtmlTableScanner.scanTables(source);

    assertEquals(1, tables.size());
    // Since the id contains special chars (. and #), selector should NOT use #
    assertTrue(tables.get(0).selector.contains("index="),
        "Special character IDs should fall back to index-based selector");
  }

  @Test void testScanTablesEmptyTable() throws IOException {
    String html = "<html><body>"
        + "<table></table>"
        + "</body></html>";

    File htmlFile = createTempFile("empty_table.html", html);
    Source source = Sources.of(htmlFile);

    List<HtmlTableScanner.TableInfo> tables = HtmlTableScanner.scanTables(source);

    assertEquals(1, tables.size());
    assertEquals(0, tables.get(0).rowCount);
  }

  // ===================================================================
  // YamlPathConverter - extract
  // ===================================================================

  @Test void testYamlPathConverterExtractToYaml() throws IOException {
    String yamlContent = "data:\n"
        + "  users:\n"
        + "    - name: Alice\n"
        + "      age: 30\n"
        + "    - name: Bob\n"
        + "      age: 25\n";

    File yamlFile = createTempFile("source.yaml", yamlContent);
    File outputFile = new File(outputDir, "users.yaml");

    YamlPathConverter.extract(yamlFile, outputFile, "$.data.users", baseDir);

    assertTrue(outputFile.exists());
    String content = new String(Files.readAllBytes(outputFile.toPath()), StandardCharsets.UTF_8);
    assertFalse(content.isEmpty());
    assertTrue(content.contains("Alice"));
    assertTrue(content.contains("Bob"));
  }

  @Test void testYamlPathConverterExtractToJson() throws IOException {
    String yamlContent = "data:\n"
        + "  settings:\n"
        + "    theme: dark\n"
        + "    language: en\n";

    File yamlFile = createTempFile("source2.yaml", yamlContent);
    File outputFile = new File(outputDir, "settings.json");

    YamlPathConverter.extract(yamlFile, outputFile, "$.data.settings", baseDir);

    assertTrue(outputFile.exists());
    String content = new String(Files.readAllBytes(outputFile.toPath()), StandardCharsets.UTF_8);
    JsonNode node = MAPPER.readTree(content);
    assertNotNull(node);
    assertEquals("dark", node.get("theme").asText());
    assertEquals("en", node.get("language").asText());
  }

  @Test void testYamlPathConverterExtractNestedPath() throws IOException {
    String yamlContent = "root:\n"
        + "  level1:\n"
        + "    level2:\n"
        + "      value: deep_value\n";

    File yamlFile = createTempFile("deep.yaml", yamlContent);
    File outputFile = new File(outputDir, "deep_result.json");

    YamlPathConverter.extract(yamlFile, outputFile, "$.root.level1.level2", baseDir);

    assertTrue(outputFile.exists());
    JsonNode node = MAPPER.readTree(outputFile);
    assertEquals("deep_value", node.get("value").asText());
  }

  @Test void testYamlPathConverterNonExistentPath() throws IOException {
    String yamlContent = "data:\n  key: value\n";

    File yamlFile = createTempFile("nonexist.yaml", yamlContent);
    File outputFile = new File(outputDir, "nonexist_result.json");

    YamlPathConverter.extract(yamlFile, outputFile, "$.missing.path", baseDir);

    // Should write empty object for missing path
    assertTrue(outputFile.exists());
    JsonNode node = MAPPER.readTree(outputFile);
    assertNotNull(node);
  }

  @Test void testYamlPathConverterExtractArray() throws IOException {
    String yamlContent = "items:\n"
        + "  - id: 1\n"
        + "    name: first\n"
        + "  - id: 2\n"
        + "    name: second\n";

    File yamlFile = createTempFile("array.yaml", yamlContent);
    File outputFile = new File(outputDir, "items.json");

    YamlPathConverter.extract(yamlFile, outputFile, "$.items", baseDir);

    assertTrue(outputFile.exists());
    JsonNode node = MAPPER.readTree(outputFile);
    assertTrue(node.isArray());
    assertEquals(2, node.size());
  }

  @Test void testYamlPathConverterUnsupportedOutputFormat() throws IOException {
    String yamlContent = "data:\n  key: value\n";

    File yamlFile = createTempFile("unsup.yaml", yamlContent);
    File outputFile = new File(outputDir, "result.txt");

    assertThrows(IllegalArgumentException.class, () ->
        YamlPathConverter.extract(yamlFile, outputFile, "$.data", baseDir));
  }

  @Test void testYamlPathConverterExtractToJsonConvenience() throws IOException {
    String yamlContent = "items:\n"
        + "  - name: first\n"
        + "  - name: second\n";

    // Create the yaml file inside a subdirectory so parent directory is our tmpDir
    File sourceDir = Files.createDirectories(tempDir.resolve("yaml_src")).toFile();
    File yamlFile = new File(sourceDir, "data.yaml");
    Files.write(yamlFile.toPath(), yamlContent.getBytes(StandardCharsets.UTF_8));

    File result = YamlPathConverter.extractToJson(yamlFile, "$.items", "extracted_items");

    assertNotNull(result);
    assertTrue(result.exists());
    assertTrue(result.getName().endsWith(".json"));
    JsonNode node = MAPPER.readTree(result);
    assertTrue(node.isArray());
    assertEquals(2, node.size());
  }

  @Test void testYamlPathConverterExtractToYamlConvenience() throws IOException {
    String yamlContent = "config:\n"
        + "  db:\n"
        + "    host: localhost\n"
        + "    port: 5432\n";

    File sourceDir = Files.createDirectories(tempDir.resolve("yaml_src2")).toFile();
    File yamlFile = new File(sourceDir, "config.yaml");
    Files.write(yamlFile.toPath(), yamlContent.getBytes(StandardCharsets.UTF_8));

    File result = YamlPathConverter.extractToYaml(yamlFile, "$.config.db", "db_config");

    assertNotNull(result);
    assertTrue(result.exists());
    assertTrue(result.getName().endsWith(".yaml"));
    String content = new String(Files.readAllBytes(result.toPath()), StandardCharsets.UTF_8);
    assertTrue(content.contains("localhost"));
  }

  @Test void testYamlPathConverterYmlExtension() throws IOException {
    String yamlContent = "data:\n  value: 42\n";

    File yamlFile = createTempFile("source.yml", yamlContent);
    File outputFile = new File(outputDir, "result.yml");

    YamlPathConverter.extract(yamlFile, outputFile, "$.data", baseDir);

    assertTrue(outputFile.exists());
    String content = new String(Files.readAllBytes(outputFile.toPath()), StandardCharsets.UTF_8);
    assertTrue(content.contains("42"));
  }

  // ===================================================================
  // JsonPathConverter - extractPath (shared logic used by YamlPathConverter)
  // ===================================================================

  @Test void testJsonPathConverterExtractSimpleField() throws IOException {
    String json = "{\"name\": \"Alice\", \"age\": 30}";
    JsonNode root = MAPPER.readTree(json);

    JsonNode result = JsonPathConverter.extractPath(root, "name");
    assertNotNull(result);
    assertEquals("Alice", result.asText());
  }

  @Test void testJsonPathConverterExtractNestedField() throws IOException {
    String json = "{\"data\": {\"user\": {\"name\": \"Bob\"}}}";
    JsonNode root = MAPPER.readTree(json);

    JsonNode result = JsonPathConverter.extractPath(root, "data.user.name");
    assertNotNull(result);
    assertEquals("Bob", result.asText());
  }

  @Test void testJsonPathConverterExtractWithDollarPrefix() throws IOException {
    String json = "{\"data\": {\"value\": 42}}";
    JsonNode root = MAPPER.readTree(json);

    JsonNode result = JsonPathConverter.extractPath(root, "$.data.value");
    assertNotNull(result);
    assertEquals(42, result.asInt());
  }

  @Test void testJsonPathConverterExtractArrayElement() throws IOException {
    String json = "{\"items\": [\"a\", \"b\", \"c\"]}";
    JsonNode root = MAPPER.readTree(json);

    JsonNode result = JsonPathConverter.extractPath(root, "items[1]");
    assertNotNull(result);
    assertEquals("b", result.asText());
  }

  @Test void testJsonPathConverterExtractEmptyPath() throws IOException {
    String json = "{\"key\": \"value\"}";
    JsonNode root = MAPPER.readTree(json);

    // Empty path after $ should return entire node
    JsonNode result = JsonPathConverter.extractPath(root, "$");
    assertNotNull(result);
    assertEquals(root, result);
  }

  @Test void testJsonPathConverterExtractMissingField() throws IOException {
    String json = "{\"name\": \"Alice\"}";
    JsonNode root = MAPPER.readTree(json);

    JsonNode result = JsonPathConverter.extractPath(root, "nonexistent");
    // Missing field should return null
    assertTrue(result == null || result.isNull());
  }

  // ===================================================================
  // Utility methods
  // ===================================================================

  private File createTempFile(String name, String content) throws IOException {
    File file = new File(tempDir.toFile(), name);
    Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
    return file;
  }
}
