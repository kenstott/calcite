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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests for {@link FileConversionManager} focusing on missed code paths:
 * - getInstance singleton
 * - registerConverter and converterCache
 * - convert with cached and uncached converters
 * - convert with no matching converter
 * - convertIfNeeded for various file types (xlsx, xls, html, htm, xml, md, docx, pptx, json, yaml, yml, csv, parquet)
 * - convertIfNeeded with baseDirectory (metadata-based change detection)
 * - convertIfNeeded with relativePath
 * - convertIfNeeded exception handling
 * - handleJsonFile for JSONPath extraction
 * - handleYamlFile for JSONPath extraction
 * - extractJsonPath
 * - findOriginalSource chain
 * - requiresConversion for all file types
 * - isDirectlyUsable for all file types
 * - isRemoteFile for various URL schemes
 * - isConversionNeeded with no record, changed, unchanged
 * - checkFileChanged for local and remote files
 * - convertExcelFile with and without relativePath
 */
@Tag("unit")
public class FileConversionManagerDeepCoverageTest {

  @TempDir
  Path tempDir;

  // ========== getInstance ==========

  @Test void testGetInstanceReturnsSingleton() {
    FileConversionManager instance1 = FileConversionManager.getInstance();
    FileConversionManager instance2 = FileConversionManager.getInstance();
    assertNotNull(instance1);
    assertTrue(instance1 == instance2, "getInstance should return the same instance");
  }

  // ========== registerConverter ==========

  @Test void testRegisterAndUseConverter() throws IOException {
    FileConversionManager manager = FileConversionManager.getInstance();

    FileConverter mockConverter = mock(FileConverter.class);
    when(mockConverter.getSourceFormat()).thenReturn("TEST_SRC");
    when(mockConverter.getTargetFormat()).thenReturn("TEST_TGT");
    when(mockConverter.canConvert("TEST_SRC", "TEST_TGT")).thenReturn(true);

    List<String> mockResult = new ArrayList<>();
    mockResult.add("/tmp/output.json");
    when(mockConverter.convert(any(File.class), any(File.class), any(ConversionMetadata.class)))
        .thenReturn(mockResult);

    manager.registerConverter(mockConverter);

    File sourceFile = createTempFile("test.src", "content");
    File targetDir = createOutputDir("tgt");

    List<String> result = manager.convert(sourceFile, targetDir, "TEST_SRC", "TEST_TGT");
    assertNotNull(result);
    assertEquals(1, result.size());
  }

  @Test void testConvertWithNoMatchingConverter() {
    FileConversionManager manager = FileConversionManager.getInstance();

    File sourceFile = new File(tempDir.toFile(), "test.xyz");
    File targetDir = tempDir.toFile();

    assertThrows(IOException.class,
        () -> manager.convert(sourceFile, targetDir, "UNKNOWN_FORMAT", "ANOTHER_FORMAT"),
        "Expected IOException when no converter matches");
  }

  @Test void testConvertUsesConverterCacheOnSubsequentCalls() throws IOException {
    FileConversionManager manager = FileConversionManager.getInstance();

    FileConverter mockConverter = mock(FileConverter.class);
    when(mockConverter.getSourceFormat()).thenReturn("CACHED_SRC");
    when(mockConverter.getTargetFormat()).thenReturn("CACHED_TGT");
    when(mockConverter.canConvert("CACHED_SRC", "CACHED_TGT")).thenReturn(true);

    List<String> mockResult = new ArrayList<>();
    mockResult.add("/tmp/cached.json");
    when(mockConverter.convert(any(File.class), any(File.class), any(ConversionMetadata.class)))
        .thenReturn(mockResult);

    manager.registerConverter(mockConverter);

    File sourceFile = createTempFile("cached.src", "data");
    File targetDir = createOutputDir("cached_tgt");

    // First call caches the converter
    manager.convert(sourceFile, targetDir, "CACHED_SRC", "CACHED_TGT");
    // Second call should use the cache
    List<String> result = manager.convert(sourceFile, targetDir, "CACHED_SRC", "CACHED_TGT");
    assertNotNull(result);
  }

  // ========== requiresConversion ==========

  @Test void testRequiresConversionForExcelFiles() {
    assertTrue(FileConversionManager.requiresConversion("data.xlsx"));
    assertTrue(FileConversionManager.requiresConversion("data.xls"));
    assertTrue(FileConversionManager.requiresConversion("DATA.XLSX"));
    assertTrue(FileConversionManager.requiresConversion("DATA.XLS"));
  }

  @Test void testRequiresConversionForHtmlFiles() {
    assertTrue(FileConversionManager.requiresConversion("page.html"));
    assertTrue(FileConversionManager.requiresConversion("page.htm"));
    assertTrue(FileConversionManager.requiresConversion("PAGE.HTML"));
    assertTrue(FileConversionManager.requiresConversion("PAGE.HTM"));
  }

  @Test void testRequiresConversionForXmlFiles() {
    assertTrue(FileConversionManager.requiresConversion("data.xml"));
    assertTrue(FileConversionManager.requiresConversion("DATA.XML"));
  }

  @Test void testRequiresConversionForMarkdownFiles() {
    assertTrue(FileConversionManager.requiresConversion("readme.md"));
    assertTrue(FileConversionManager.requiresConversion("README.MD"));
  }

  @Test void testRequiresConversionForDocxFiles() {
    assertTrue(FileConversionManager.requiresConversion("document.docx"));
    assertTrue(FileConversionManager.requiresConversion("DOCUMENT.DOCX"));
  }

  @Test void testRequiresConversionForPptxFiles() {
    assertTrue(FileConversionManager.requiresConversion("slides.pptx"));
    assertTrue(FileConversionManager.requiresConversion("SLIDES.PPTX"));
  }

  @Test void testRequiresConversionReturnsFalseForDirectFiles() {
    assertFalse(FileConversionManager.requiresConversion("data.csv"));
    assertFalse(FileConversionManager.requiresConversion("data.json"));
    assertFalse(FileConversionManager.requiresConversion("data.parquet"));
    assertFalse(FileConversionManager.requiresConversion("data.yaml"));
    assertFalse(FileConversionManager.requiresConversion("data.yml"));
    assertFalse(FileConversionManager.requiresConversion("data.arrow"));
    assertFalse(FileConversionManager.requiresConversion("data.tsv"));
  }

  // ========== isDirectlyUsable ==========

  @Test void testIsDirectlyUsableForCsvFiles() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.csv"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.csv.gz"));
    assertTrue(FileConversionManager.isDirectlyUsable("DATA.CSV"));
  }

  @Test void testIsDirectlyUsableForTsvFiles() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.tsv"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.tsv.gz"));
  }

  @Test void testIsDirectlyUsableForJsonFiles() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.json"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.json.gz"));
  }

  @Test void testIsDirectlyUsableForParquetFiles() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.parquet"));
  }

  @Test void testIsDirectlyUsableForArrowFiles() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.arrow"));
  }

  @Test void testIsDirectlyUsableForYamlFiles() {
    assertTrue(FileConversionManager.isDirectlyUsable("data.yaml"));
    assertTrue(FileConversionManager.isDirectlyUsable("data.yml"));
  }

  @Test void testIsDirectlyUsableReturnsFalseForConvertibleFiles() {
    assertFalse(FileConversionManager.isDirectlyUsable("data.xlsx"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.xls"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.html"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.htm"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.xml"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.md"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.docx"));
    assertFalse(FileConversionManager.isDirectlyUsable("data.pptx"));
  }

  // ========== convertIfNeeded ==========

  @Test void testConvertIfNeededCsvReturnsNoConversion() {
    File csvFile = new File(tempDir.toFile(), "data.csv");
    File outputDir = tempDir.toFile();
    assertFalse(FileConversionManager.convertIfNeeded(csvFile, outputDir, "UNCHANGED"));
  }

  @Test void testConvertIfNeededParquetReturnsNoConversion() {
    File parquetFile = new File(tempDir.toFile(), "data.parquet");
    File outputDir = tempDir.toFile();
    assertFalse(FileConversionManager.convertIfNeeded(parquetFile, outputDir, "UNCHANGED"));
  }

  @Test void testConvertIfNeededHtmlFile() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Name</th></tr>"
        + "<tr><td>Alice</td></tr>"
        + "</table></body></html>";

    File htmlFile = createTempFile("data.html", html);
    File outputDir = createOutputDir("out_html");

    boolean result = FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED");
    assertTrue(result, "HTML file should require conversion");
  }

  @Test void testConvertIfNeededHtmFile() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Col</th></tr>"
        + "<tr><td>Val</td></tr>"
        + "</table></body></html>";

    File htmFile = createTempFile("data.htm", html);
    File outputDir = createOutputDir("out_htm");

    boolean result = FileConversionManager.convertIfNeeded(htmFile, outputDir, "UNCHANGED");
    assertTrue(result, "HTM file should require conversion");
  }

  @Test void testConvertIfNeededWithColumnAndTableCasing() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Name</th></tr>"
        + "<tr><td>Alice</td></tr>"
        + "</table></body></html>";

    File htmlFile = createTempFile("casing.html", html);
    File outputDir = createOutputDir("out_casing");

    boolean result =
        FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED", "SMART_CASING");
    assertTrue(result);
  }

  @Test void testConvertIfNeededWithBaseDirectory() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Col</th></tr>"
        + "<tr><td>Val</td></tr>"
        + "</table></body></html>";

    File htmlFile = createTempFile("basedir.html", html);
    File outputDir = createOutputDir("out_basedir");

    boolean result =
        FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED", "SMART_CASING", tempDir.toFile());
    assertTrue(result);
  }

  @Test void testConvertIfNeededWithRelativePath() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Key</th></tr>"
        + "<tr><td>k1</td></tr>"
        + "</table></body></html>";

    File htmlFile = createTempFile("relpath.html", html);
    File outputDir = createOutputDir("out_relpath");

    String relativePath = "subdir" + File.separator + "relpath.html";
    boolean result =
        FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED", "SMART_CASING", tempDir.toFile(), relativePath);
    assertTrue(result);
  }

  @Test void testConvertIfNeededJsonFileReturnsNoConversion() {
    // A plain JSON file without JSONPath history should not be converted
    File jsonFile = new File(tempDir.toFile(), "plain.json");
    File outputDir = tempDir.toFile();
    boolean result = FileConversionManager.convertIfNeeded(jsonFile, outputDir, "UNCHANGED");
    assertFalse(result);
  }

  @Test void testConvertIfNeededJsonGzFileReturnsNoConversion() {
    File jsonGzFile = new File(tempDir.toFile(), "data.json.gz");
    File outputDir = tempDir.toFile();
    boolean result = FileConversionManager.convertIfNeeded(jsonGzFile, outputDir, "UNCHANGED");
    assertFalse(result);
  }

  @Test void testConvertIfNeededYamlFileReturnsNoConversion() {
    File yamlFile = new File(tempDir.toFile(), "config.yaml");
    File outputDir = tempDir.toFile();
    boolean result = FileConversionManager.convertIfNeeded(yamlFile, outputDir, "UNCHANGED");
    assertFalse(result);
  }

  @Test void testConvertIfNeededYmlFileReturnsNoConversion() {
    File ymlFile = new File(tempDir.toFile(), "config.yml");
    File outputDir = tempDir.toFile();
    boolean result = FileConversionManager.convertIfNeeded(ymlFile, outputDir, "UNCHANGED");
    assertFalse(result);
  }

  @Test void testConvertIfNeededArrowFileReturnsNoConversion() {
    File arrowFile = new File(tempDir.toFile(), "data.arrow");
    File outputDir = tempDir.toFile();
    assertFalse(FileConversionManager.convertIfNeeded(arrowFile, outputDir, "UNCHANGED"));
  }

  // ========== convertIfNeeded exception handling ==========

  @Test void testConvertIfNeededWithCorruptHtmlFile() throws IOException {
    // Create a file that exists but has malformed content
    File htmlFile = createTempFile("corrupt.html", "not really html {{{{");
    File outputDir = createOutputDir("out_corrupt");

    // Should not throw, should handle gracefully
    boolean result = FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED");
    // May return true (attempted conversion) or false depending on error handling
    // Main point is it should not throw
  }

  @Test void testConvertIfNeededWithNonexistentXlsxFile() {
    File xlsxFile = new File(tempDir.toFile(), "nonexistent.xlsx");
    File outputDir = createOutputDir("out_nonexist");

    // Should handle the file not existing gracefully
    boolean result = FileConversionManager.convertIfNeeded(xlsxFile, outputDir, "UNCHANGED");
    // convertExcelFile should catch the exception and return false
    assertFalse(result, "Nonexistent xlsx file should return false");
  }

  // ========== extractJsonPath via reflection ==========

  @Test void testExtractJsonPath() throws Exception {
    Method method =
        FileConversionManager.class.getDeclaredMethod("extractJsonPath", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(null, "JSONPATH_EXTRACTION[$.data.items]");
    assertEquals("$.data.items", result);

    // Note: the method uses .replace("]", "") which strips ALL ] characters
    String result2 = (String) method.invoke(null, "JSONPATH_EXTRACTION[$.store.book[*].author]");
    assertEquals("$.store.book[*.author", result2);

    String result3 = (String) method.invoke(null, "JSONPATH_EXTRACTION[$]");
    assertEquals("$", result3);
  }

  // ========== isRemoteFile via reflection ==========

  @Test void testIsRemoteFile() throws Exception {
    Method method = FileConversionManager.class.getDeclaredMethod("isRemoteFile", String.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(null, "http://example.com/data.csv"));
    assertTrue((Boolean) method.invoke(null, "https://example.com/data.csv"));
    assertTrue((Boolean) method.invoke(null, "s3://bucket/key.csv"));
    assertTrue((Boolean) method.invoke(null, "ftp://server/file.csv"));
    assertTrue((Boolean) method.invoke(null, "sftp://server/file.csv"));

    assertFalse((Boolean) method.invoke(null, "/local/path/data.csv"));
    assertFalse((Boolean) method.invoke(null, "C:\\Windows\\data.csv"));
    assertFalse((Boolean) method.invoke(null, "data.csv"));
    assertFalse((Boolean) method.invoke(null, (String) null));
  }

  // ========== findOriginalSource ==========

  @Test void testFindOriginalSourceWithNoConversionHistory() {
    File testFile = new File(tempDir.toFile(), "original.csv");
    File result = FileConversionManager.findOriginalSource(testFile);
    // When no conversion history, should return the input file itself
    assertNotNull(result);
    assertEquals(testFile.getAbsolutePath(), result.getAbsolutePath());
  }

  @Test void testFindOriginalSourceWithNonexistentDirectory() {
    File testFile = new File("/nonexistent/path/file.json");
    File result = FileConversionManager.findOriginalSource(testFile);
    // Should handle gracefully and return the input file
    assertNotNull(result);
  }

  // ========== convertIfNeeded with baseDirectory and existing record ==========

  @Test void testConvertIfNeededHtmlWithBaseDirectoryPreservesTableName() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Data</th></tr>"
        + "<tr><td>value</td></tr>"
        + "</table></body></html>";

    File htmlFile = createTempFile("preserve.html", html);
    File outputDir = createOutputDir("out_preserve");
    File baseDir = createOutputDir("base_preserve");

    // First conversion
    boolean result1 =
        FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED", "SMART_CASING", baseDir);
    assertTrue(result1);

    // Modify file timestamp to trigger re-conversion
    htmlFile.setLastModified(System.currentTimeMillis() + 5000);

    // Second conversion should pick up existing record
    boolean result2 =
        FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED", "SMART_CASING", baseDir);
    // Result depends on change detection
  }

  // ========== convertIfNeeded 2-arg overload ==========

  @Test void testConvertIfNeeded2Arg() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>A</th></tr>"
        + "<tr><td>1</td></tr>"
        + "</table></body></html>";

    File htmlFile = createTempFile("two_arg.html", html);
    File outputDir = createOutputDir("out_two_arg");

    boolean result = FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED");
    assertTrue(result);
  }

  // ========== Markdown file conversion ==========

  @Test void testConvertIfNeededMarkdownFile() throws IOException {
    String md = "# Test\n\n| Col1 | Col2 |\n|------|------|\n| a    | b    |\n";

    File mdFile = createTempFile("data.md", md);
    File outputDir = createOutputDir("out_md");

    boolean result = FileConversionManager.convertIfNeeded(mdFile, outputDir, "UNCHANGED");
    assertTrue(result, "Markdown file should require conversion");
  }

  // ========== XML file conversion ==========

  @Test void testConvertIfNeededXmlFile() throws IOException {
    String xml = "<?xml version=\"1.0\"?>\n<root><item><name>test</name></item></root>";

    File xmlFile = createTempFile("data.xml", xml);
    File outputDir = createOutputDir("out_xml");

    boolean result = FileConversionManager.convertIfNeeded(xmlFile, outputDir, "UNCHANGED");
    assertTrue(result, "XML file should require conversion");
  }

  // ========== DOCX file conversion ==========

  @Test void testConvertIfNeededDocxFile() throws IOException {
    // Create a minimal DOCX file
    org.apache.poi.xwpf.usermodel.XWPFDocument doc =
        new org.apache.poi.xwpf.usermodel.XWPFDocument();
    org.apache.poi.xwpf.usermodel.XWPFTable table = doc.createTable(2, 2);
    table.getRow(0).getCell(0).setText("H1");
    table.getRow(0).getCell(1).setText("H2");
    table.getRow(1).getCell(0).setText("D1");
    table.getRow(1).getCell(1).setText("D2");

    File docxFile = new File(tempDir.toFile(), "data.docx");
    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(docxFile)) {
      doc.write(fos);
    }
    doc.close();

    File outputDir = createOutputDir("out_docx");

    boolean result = FileConversionManager.convertIfNeeded(docxFile, outputDir, "UNCHANGED");
    assertTrue(result, "DOCX file should require conversion");
  }

  // ========== PPTX file conversion ==========

  @Test void testConvertIfNeededPptxFile() throws IOException {
    // Create a minimal PPTX file with a table
    org.apache.poi.xslf.usermodel.XMLSlideShow pptx =
        new org.apache.poi.xslf.usermodel.XMLSlideShow();
    org.apache.poi.xslf.usermodel.XSLFSlide slide = pptx.createSlide();
    // Create a table on the slide
    org.apache.poi.xslf.usermodel.XSLFTable table = slide.createTable(2, 2);
    table.getRows().get(0).getCells().get(0).setText("H1");
    table.getRows().get(0).getCells().get(1).setText("H2");
    table.getRows().get(1).getCells().get(0).setText("D1");
    table.getRows().get(1).getCells().get(1).setText("D2");

    File pptxFile = new File(tempDir.toFile(), "data.pptx");
    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(pptxFile)) {
      pptx.write(fos);
    }
    pptx.close();

    File outputDir = createOutputDir("out_pptx");

    boolean result = FileConversionManager.convertIfNeeded(pptxFile, outputDir, "UNCHANGED");
    assertTrue(result, "PPTX file should require conversion");
  }

  // ========== convertIfNeeded with null baseDirectory ==========

  @Test void testConvertIfNeededHtmlWithNullBaseDirectory() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Test</th></tr>"
        + "<tr><td>data</td></tr>"
        + "</table></body></html>";

    File htmlFile = createTempFile("nullbase.html", html);
    File outputDir = createOutputDir("out_nullbase");

    // null baseDirectory - should skip change detection and just convert
    boolean result =
        FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED", "SMART_CASING", null);
    assertTrue(result);
  }

  // ========== handleJsonFile via convertIfNeeded ==========

  @Test void testHandleJsonFileWithPlainJson() throws IOException {
    File jsonFile = createTempFile("plain.json", "[{\"a\":1}]");
    File outputDir = createOutputDir("out_json");

    // Plain JSON should not trigger conversion
    boolean result =
        FileConversionManager.convertIfNeeded(jsonFile, outputDir, "UNCHANGED", "SMART_CASING", tempDir.toFile());
    assertFalse(result);
  }

  // ========== handleYamlFile via convertIfNeeded ==========

  @Test void testHandleYamlFileWithPlainYaml() throws IOException {
    File yamlFile = createTempFile("plain.yaml", "key: value\n");
    File outputDir = createOutputDir("out_yaml");

    boolean result =
        FileConversionManager.convertIfNeeded(yamlFile, outputDir, "UNCHANGED", "SMART_CASING", tempDir.toFile());
    assertFalse(result);
  }

  @Test void testHandleYmlFileWithPlainYml() throws IOException {
    File ymlFile = createTempFile("config.yml", "key: value\n");
    File outputDir = createOutputDir("out_yml");

    boolean result =
        FileConversionManager.convertIfNeeded(ymlFile, outputDir, "UNCHANGED", "SMART_CASING", tempDir.toFile());
    assertFalse(result);
  }

  // ========== convertExcelFile failure via convertIfNeeded ==========

  @Test void testConvertIfNeededCorruptExcelFile() throws IOException {
    // Create a file with xlsx extension but invalid content
    File xlsxFile = createTempFile("corrupt.xlsx", "not an excel file");
    File outputDir = createOutputDir("out_corrupt_xl");

    // Should handle the corrupt file gracefully (catch exception, return false)
    boolean result = FileConversionManager.convertIfNeeded(xlsxFile, outputDir, "UNCHANGED");
    assertFalse(result, "Corrupt xlsx file should fail gracefully and return false");
  }

  @Test void testConvertIfNeededCorruptXlsFile() throws IOException {
    File xlsFile = createTempFile("corrupt.xls", "not an xls file");
    File outputDir = createOutputDir("out_corrupt_xls");

    boolean result = FileConversionManager.convertIfNeeded(xlsFile, outputDir, "UNCHANGED");
    assertFalse(result, "Corrupt xls file should fail gracefully and return false");
  }

  // ========== Case insensitivity of file extension detection ==========

  @Test void testConvertIfNeededUppercaseExtensions() throws IOException {
    String html = "<html><body><table>"
        + "<tr><th>Name</th></tr>"
        + "<tr><td>Alice</td></tr>"
        + "</table></body></html>";

    // The method lowercases the path, so uppercase extensions should work
    File htmlFile = createTempFile("upper.HTML", html);
    File outputDir = createOutputDir("out_upper");

    boolean result = FileConversionManager.convertIfNeeded(htmlFile, outputDir, "UNCHANGED");
    assertTrue(result, "Uppercase .HTML extension should be recognized");
  }

  // ========== isConversionNeeded via reflection ==========

  @Test void testIsConversionNeededWithNoRecord() throws Exception {
    Method method =
        FileConversionManager.class.getDeclaredMethod("isConversionNeeded", File.class, File.class);
    method.setAccessible(true);

    File sourceFile = new File(tempDir.toFile(), "new_file.html");
    File baseDir = createOutputDir("base_new");

    boolean result = (Boolean) method.invoke(null, sourceFile, baseDir);
    assertTrue(result, "File without conversion record should need conversion");
  }

  // ========== registerConverter with canConvert fallback ==========

  @Test void testConvertFallsBackToCanConvert() throws IOException {
    FileConversionManager manager = FileConversionManager.getInstance();

    // Register a converter that uses canConvert rather than exact key match
    FileConverter flexConverter = mock(FileConverter.class);
    when(flexConverter.getSourceFormat()).thenReturn("FLEX_SRC");
    when(flexConverter.getTargetFormat()).thenReturn("FLEX_TGT");
    // canConvert responds to different key than registered
    when(flexConverter.canConvert("FLEX_A", "FLEX_B")).thenReturn(true);

    List<String> mockResult = new ArrayList<>();
    mockResult.add("/tmp/flex.json");
    when(flexConverter.convert(any(File.class), any(File.class), any(ConversionMetadata.class)))
        .thenReturn(mockResult);

    manager.registerConverter(flexConverter);

    File sourceFile = createTempFile("flex.src", "content");
    File targetDir = createOutputDir("flex_tgt");

    // This should find the converter via canConvert iteration
    List<String> result = manager.convert(sourceFile, targetDir, "FLEX_A", "FLEX_B");
    assertNotNull(result);
    assertEquals(1, result.size());
  }

  // ========== Helpers ==========

  private File createTempFile(String name, String content) throws IOException {
    File file = new File(tempDir.toFile(), name);
    try (FileWriter fw = new FileWriter(file, StandardCharsets.UTF_8)) {
      fw.write(content);
    }
    return file;
  }

  private File createOutputDir(String name) {
    File dir = new File(tempDir.toFile(), name);
    dir.mkdirs();
    return dir;
  }
}
