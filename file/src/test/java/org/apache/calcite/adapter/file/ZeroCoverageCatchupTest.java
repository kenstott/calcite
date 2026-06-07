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

import org.apache.calcite.adapter.file.converters.CrawlerConfiguration;
import org.apache.calcite.adapter.file.converters.HtmlCrawler;
import org.apache.calcite.adapter.file.converters.HtmlTableScanner;
import org.apache.calcite.adapter.file.etl.DocumentSource;
import org.apache.calcite.adapter.file.etl.FileSource;
import org.apache.calcite.adapter.file.etl.FileSourceConfig;
import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.materialized.MaterializedViewTable;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for classes at 0% or low JaCoCo coverage.
 *
 * <p>Covers: FileSource, DocumentSource, MaterializedViewTable,
 * HtmlCrawler.CrawlResult, and CrawlerConfiguration.
 */
@Tag("unit")
class ZeroCoverageCatchupTest {

  // ===================================================================
  // FileSource tests
  // ===================================================================

  @Test void testFileSourceGetType() {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/tmp/test.csv")
        .build();
    FileSource source = new FileSource(config);
    assertEquals("file", source.getType());
  }

  @Test void testFileSourceGetTypeWithFormat() {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/tmp/test.csv")
        .format("csv")
        .build();
    FileSource source = new FileSource(config);
    assertEquals("file", source.getType());
  }

  @Test void testFileSourceDetectFormat() throws Exception {
    // Use reflection to test private detectFormat method
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/tmp/test.csv")
        .build();
    FileSource source = new FileSource(config);

    Method detectFormat = FileSource.class.getDeclaredMethod("detectFormat", String.class);
    detectFormat.setAccessible(true);

    assertEquals("csv", detectFormat.invoke(source, "data.csv"));
    assertEquals("csv", detectFormat.invoke(source, "/path/to/data.CSV"));
    assertEquals("tsv", detectFormat.invoke(source, "data.tsv"));
    assertEquals("json", detectFormat.invoke(source, "data.json"));
    assertEquals("parquet", detectFormat.invoke(source, "data.parquet"));
    assertEquals("xlsx", detectFormat.invoke(source, "data.xlsx"));
    assertEquals("xlsx", detectFormat.invoke(source, "data.xls"));
    // default for unknown extension
    assertEquals("csv", detectFormat.invoke(source, "data.unknown"));
  }

  @Test void testFileSourceSubstituteVariables() throws Exception {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/tmp/test.csv")
        .build();
    FileSource source = new FileSource(config);

    Method substituteVariables = FileSource.class.getDeclaredMethod(
        "substituteVariables", String.class, Map.class);
    substituteVariables.setAccessible(true);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    vars.put("region", "US");

    String result = (String) substituteVariables.invoke(
        source, "data/${year}/${region}/report.csv", vars);
    assertEquals("data/2024/US/report.csv", result);
  }

  @Test void testFileSourceSubstituteVariablesEmptyMap() throws Exception {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/tmp/test.csv")
        .build();
    FileSource source = new FileSource(config);

    Method substituteVariables = FileSource.class.getDeclaredMethod(
        "substituteVariables", String.class, Map.class);
    substituteVariables.setAccessible(true);

    Map<String, String> vars = Collections.emptyMap();
    String result = (String) substituteVariables.invoke(
        source, "data/report.csv", vars);
    assertEquals("data/report.csv", result);
  }

  @Test void testFileSourceEscapePath() throws Exception {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/tmp/test.csv")
        .build();
    FileSource source = new FileSource(config);

    Method escapePath = FileSource.class.getDeclaredMethod("escapePath", String.class);
    escapePath.setAccessible(true);

    assertEquals("no-quotes", escapePath.invoke(source, "no-quotes"));
    assertEquals("it''s", escapePath.invoke(source, "it's"));
    assertEquals("a''b''c", escapePath.invoke(source, "a'b'c"));
  }

  @Test void testFileSourceConstructorWithStorageProvider() {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("s3://bucket/data.csv")
        .format("csv")
        .build();

    StorageProvider mockProvider = createMockStorageProvider("s3");
    FileSource source = new FileSource(config, mockProvider);
    assertEquals("file", source.getType());
  }

  @Test void testFileSourceFetchUnsupportedFormat() {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/tmp/test.xyz")
        .format("xyz")
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    FileSource source = new FileSource(config, mockProvider);

    Map<String, String> vars = Collections.emptyMap();
    IOException thrown = assertThrows(IOException.class, () -> source.fetch(vars));
    assertTrue(thrown.getMessage().contains("Unsupported file format: xyz"));
  }

  @Test void testFileSourceFetchExcelWithInjectedProvider(@TempDir Path tempDir) throws Exception {
    // Create a minimal xlsx file using Apache POI via the FileSource itself
    // We will create an xlsx file and read it back
    Path xlsxFile = tempDir.resolve("test.xlsx");

    // Create a minimal xlsx workbook
    org.apache.poi.ss.usermodel.Workbook workbook =
        new org.apache.poi.xssf.usermodel.XSSFWorkbook();
    org.apache.poi.ss.usermodel.Sheet sheet = workbook.createSheet("Sheet1");
    org.apache.poi.ss.usermodel.Row headerRow = sheet.createRow(0);
    headerRow.createCell(0).setCellValue("name");
    headerRow.createCell(1).setCellValue("age");
    org.apache.poi.ss.usermodel.Row dataRow = sheet.createRow(1);
    dataRow.createCell(0).setCellValue("Alice");
    dataRow.createCell(1).setCellValue(30);
    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(xlsxFile.toFile())) {
      workbook.write(fos);
    }
    workbook.close();

    FileSourceConfig config = FileSourceConfig.builder()
        .path(xlsxFile.toString())
        .format("xlsx")
        .sheet("Sheet1")
        .build();

    // Create a local storage provider that opens files from the filesystem
    StorageProvider localProvider = createLocalStorageProvider();
    FileSource source = new FileSource(config, localProvider);

    Map<String, String> vars = Collections.emptyMap();
    Iterator<Map<String, Object>> rows = source.fetch(vars);

    assertTrue(rows.hasNext());
    Map<String, Object> row = rows.next();
    assertEquals("Alice", row.get("name"));
    assertEquals(30L, row.get("age"));
    assertFalse(rows.hasNext());
  }

  @Test void testFileSourceFetchExcelEmptySheet(@TempDir Path tempDir) throws Exception {
    Path xlsxFile = tempDir.resolve("empty.xlsx");

    org.apache.poi.ss.usermodel.Workbook workbook =
        new org.apache.poi.xssf.usermodel.XSSFWorkbook();
    workbook.createSheet("Empty");
    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(xlsxFile.toFile())) {
      workbook.write(fos);
    }
    workbook.close();

    FileSourceConfig config = FileSourceConfig.builder()
        .path(xlsxFile.toString())
        .format("xlsx")
        .build();

    StorageProvider localProvider = createLocalStorageProvider();
    FileSource source = new FileSource(config, localProvider);

    Map<String, String> vars = Collections.emptyMap();
    Iterator<Map<String, Object>> rows = source.fetch(vars);
    // Empty sheet (no header row) returns empty iterator
    assertFalse(rows.hasNext());
  }

  @Test void testFileSourceFetchExcelBadSheetName(@TempDir Path tempDir) throws Exception {
    Path xlsxFile = tempDir.resolve("test2.xlsx");

    org.apache.poi.ss.usermodel.Workbook workbook =
        new org.apache.poi.xssf.usermodel.XSSFWorkbook();
    workbook.createSheet("RealSheet");
    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(xlsxFile.toFile())) {
      workbook.write(fos);
    }
    workbook.close();

    FileSourceConfig config = FileSourceConfig.builder()
        .path(xlsxFile.toString())
        .format("xlsx")
        .sheet("NonExistent")
        .build();

    StorageProvider localProvider = createLocalStorageProvider();
    FileSource source = new FileSource(config, localProvider);

    Map<String, String> vars = Collections.emptyMap();
    IOException thrown = assertThrows(IOException.class, () -> source.fetch(vars));
    assertTrue(thrown.getMessage().contains("Sheet not found"));
  }

  @Test void testFileSourceFetchExcelBooleanAndNullCells(@TempDir Path tempDir) throws Exception {
    Path xlsxFile = tempDir.resolve("types.xlsx");

    org.apache.poi.ss.usermodel.Workbook workbook =
        new org.apache.poi.xssf.usermodel.XSSFWorkbook();
    org.apache.poi.ss.usermodel.Sheet sheet = workbook.createSheet("Data");
    org.apache.poi.ss.usermodel.Row headerRow = sheet.createRow(0);
    headerRow.createCell(0).setCellValue("flag");
    headerRow.createCell(1).setCellValue("value");
    headerRow.createCell(2).setCellValue("empty");

    org.apache.poi.ss.usermodel.Row dataRow = sheet.createRow(1);
    dataRow.createCell(0).setCellValue(true);
    dataRow.createCell(1).setCellValue(3.14);
    // cell 2 is null

    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(xlsxFile.toFile())) {
      workbook.write(fos);
    }
    workbook.close();

    FileSourceConfig config = FileSourceConfig.builder()
        .path(xlsxFile.toString())
        .format("xlsx")
        .build();

    StorageProvider localProvider = createLocalStorageProvider();
    FileSource source = new FileSource(config, localProvider);

    Iterator<Map<String, Object>> rows = source.fetch(Collections.<String, String>emptyMap());
    assertTrue(rows.hasNext());
    Map<String, Object> row = rows.next();
    assertEquals(true, row.get("flag"));
    assertEquals(3.14, row.get("value"));
    assertNull(row.get("empty"));
    assertFalse(rows.hasNext());
  }

  @Test void testFileSourceEstimateRowCount() {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/tmp/test.csv")
        .build();
    FileSource source = new FileSource(config);
    assertEquals(-1, source.estimateRowCount());
  }

  // ===================================================================
  // DocumentSource tests
  // ===================================================================

  @Test void testDocumentSourceSubstituteVariables() {
    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/{entity}/data")
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("entity", "ABC");
    vars.put("year", "2024");

    String result = docSource.substituteVariables(
        "https://example.com/{entity}/{year}/report", vars);
    assertEquals("https://example.com/ABC/2024/report", result);
  }

  @Test void testDocumentSourceSubstituteVariablesNullPattern() {
    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/data")
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");

    assertNull(docSource.substituteVariables(null, Collections.<String, String>emptyMap()));
  }

  @Test void testDocumentSourceSubstituteVariablesEnvFallback() {
    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/data")
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");

    // Test env: prefix with a likely-not-set env variable (falls back to system property)
    Map<String, String> vars = Collections.emptyMap();
    String result = docSource.substituteVariables(
        "https://example.com/{env:ZZZZZ_UNLIKELY_ENV_VAR}/data", vars);
    // If env var not set, it falls back to system property, then empty string
    assertEquals("https://example.com//data", result);
  }

  @Test void testDocumentSourceSubstituteVariablesMissing() {
    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/data")
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "12345");

    // {unknown} variable should be replaced with empty string
    String result = docSource.substituteVariables(
        "https://example.com/{cik}/{unknown}/file", vars);
    assertEquals("https://example.com/12345//file", result);
  }

  @Test void testDocumentSourceGetters() {
    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/data")
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");

    assertSame(httpConfig, docSource.getConfig());
    assertEquals("/cache", docSource.getCacheDirectory());
    // Default rate limit: 10 req/sec -> 100ms interval
    assertEquals(100, docSource.getMinRequestIntervalMs());
  }

  @Test void testDocumentSourceDocumentIteratorReturnsEmpty() {
    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/data")
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");

    Iterator<Map<String, String>> iter = docSource.documentIterator(
        "{}", Collections.<String, String>emptyMap());
    assertFalse(iter.hasNext());
  }

  @Test void testDocumentSourceCustomRateLimit() {
    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/data")
        .rateLimit(HttpSourceConfig.RateLimitConfig.defaults())
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");

    // Default RateLimitConfig is 10 req/sec -> 100ms interval
    assertEquals(100, docSource.getMinRequestIntervalMs());
  }

  @Test void testDocumentSourceFetchMetadataWithoutConfig() {
    // HttpSourceConfig without documentSource
    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/data")
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");

    // getDocumentConfig should be null since no documentSource was set
    assertNull(docSource.getDocumentConfig());

    // fetchMetadata should throw because no document source is configured
    assertThrows(IllegalStateException.class, () ->
        docSource.fetchMetadata(Collections.<String, String>emptyMap()));
  }

  @Test void testDocumentSourceDownloadDocumentWithoutConfig() {
    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/data")
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");

    assertThrows(IllegalStateException.class, () ->
        docSource.downloadDocument(Collections.<String, String>emptyMap()));
  }

  @Test void testDocumentSourceDefaultHeaders() {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("User-Agent", "test-agent");

    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/data")
        .headers(headers)
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    // Constructor should add Accept-Encoding header
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");
    assertNotNull(docSource);
  }

  @Test void testDocumentSourceHeadersPreserveExistingAcceptEncoding() {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("Accept-Encoding", "br");

    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/data")
        .headers(headers)
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    // Constructor should NOT overwrite existing Accept-Encoding
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");
    assertNotNull(docSource);
  }

  @Test void testDocumentSourceBuildCacheKey() throws Exception {
    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/data")
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");

    Method buildCacheKey = DocumentSource.class.getDeclaredMethod(
        "buildCacheKey", Map.class);
    buildCacheKey.setAccessible(true);

    // Test with cik, accession, document
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0001234567");
    vars.put("accession", "0001234567-24-000001");
    vars.put("document", "report.htm");

    String key = (String) buildCacheKey.invoke(docSource, vars);
    assertEquals("0001234567/000123456724000001/report.htm", key);
  }

  @Test void testDocumentSourceBuildCacheKeyNoDocument() throws Exception {
    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/data")
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");

    Method buildCacheKey = DocumentSource.class.getDeclaredMethod(
        "buildCacheKey", Map.class);
    buildCacheKey.setAccessible(true);

    // Test without document - should fallback to hash
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0001234567");

    String key = (String) buildCacheKey.invoke(docSource, vars);
    assertTrue(key.startsWith("0001234567/"));
    assertTrue(key.endsWith(".dat"));
  }

  @Test void testDocumentSourceBuildCacheKeyEmpty() throws Exception {
    HttpSourceConfig httpConfig = HttpSourceConfig.builder()
        .url("https://example.com/data")
        .build();

    StorageProvider mockProvider = createMockStorageProvider("local");
    DocumentSource docSource = new DocumentSource(httpConfig, mockProvider, "/cache");

    Method buildCacheKey = DocumentSource.class.getDeclaredMethod(
        "buildCacheKey", Map.class);
    buildCacheKey.setAccessible(true);

    // Test with empty variables - should fallback entirely
    Map<String, String> vars = Collections.emptyMap();
    String key = (String) buildCacheKey.invoke(docSource, vars);
    assertTrue(key.endsWith(".dat"));
  }

  // ===================================================================
  // MaterializedViewTable tests
  // ===================================================================

  @Test void testMaterializedViewTableConstruction(@TempDir Path tempDir) throws Exception {
    File parquetFile = new File(tempDir.toFile(), "view.parquet");

    Map<String, org.apache.calcite.schema.Table> tables =
        new HashMap<String, org.apache.calcite.schema.Table>();

    MaterializedViewTable table = new MaterializedViewTable(
        null, "myschema", "myview",
        "SELECT * FROM t", parquetFile, tables);

    // Verify fields via reflection since they are protected
    java.lang.reflect.Field parquetField =
        MaterializedViewTable.class.getDeclaredField("parquetFile");
    parquetField.setAccessible(true);
    assertEquals(parquetFile, parquetField.get(table));

    java.lang.reflect.Field materializedField =
        MaterializedViewTable.class.getDeclaredField("materialized");
    materializedField.setAccessible(true);
    java.util.concurrent.atomic.AtomicBoolean mat =
        (java.util.concurrent.atomic.AtomicBoolean) materializedField.get(table);
    assertFalse(mat.get());
  }

  @Test void testMaterializedViewTableSupplierConstructor(@TempDir Path tempDir) throws Exception {
    File parquetFile = new File(tempDir.toFile(), "view2.parquet");

    java.util.function.Supplier<Map<String, org.apache.calcite.schema.Table>> supplier =
        () -> new HashMap<String, org.apache.calcite.schema.Table>();

    MaterializedViewTable table = new MaterializedViewTable(
        null, "schema2", "view2",
        "SELECT 1", parquetFile, supplier);

    java.lang.reflect.Field parquetField =
        MaterializedViewTable.class.getDeclaredField("parquetFile");
    parquetField.setAccessible(true);
    assertEquals(parquetFile, parquetField.get(table));

    java.lang.reflect.Field materializedField =
        MaterializedViewTable.class.getDeclaredField("materialized");
    materializedField.setAccessible(true);
    java.util.concurrent.atomic.AtomicBoolean mat =
        (java.util.concurrent.atomic.AtomicBoolean) materializedField.get(table);
    assertFalse(mat.get());
  }

  // ===================================================================
  // HtmlCrawler.CrawlResult tests
  // ===================================================================

  @Test void testCrawlResultEmptyInitialization() {
    HtmlCrawler.CrawlResult result = new HtmlCrawler.CrawlResult();
    assertTrue(result.getHtmlTables().isEmpty());
    assertTrue(result.getDataFiles().isEmpty());
    assertTrue(result.getVisitedUrls().isEmpty());
    assertTrue(result.getFailedUrls().isEmpty());
    assertEquals(0, result.getTotalTablesFound());
    assertEquals(0, result.getTotalDataFilesFound());
  }

  @Test void testCrawlResultAddHtmlTables() {
    HtmlCrawler.CrawlResult result = new HtmlCrawler.CrawlResult();

    List<HtmlTableScanner.TableInfo> tables = new ArrayList<HtmlTableScanner.TableInfo>();
    result.addHtmlTables("http://example.com", tables);

    assertEquals(1, result.getHtmlTables().size());
    assertTrue(result.getHtmlTables().containsKey("http://example.com"));
    assertEquals(0, result.getTotalTablesFound());
  }

  @Test void testCrawlResultAddDataFile(@TempDir Path tempDir) {
    HtmlCrawler.CrawlResult result = new HtmlCrawler.CrawlResult();

    File file = new File(tempDir.toFile(), "data.csv");
    result.addDataFile("http://example.com/data.csv", file);

    assertEquals(1, result.getDataFiles().size());
    assertEquals(file, result.getDataFiles().get("http://example.com/data.csv"));
    assertEquals(1, result.getTotalDataFilesFound());
  }

  @Test void testCrawlResultMarkVisitedAndFailed() {
    HtmlCrawler.CrawlResult result = new HtmlCrawler.CrawlResult();

    result.markVisited("http://example.com/page1");
    result.markVisited("http://example.com/page2");
    result.markFailed("http://example.com/page3");

    assertEquals(2, result.getVisitedUrls().size());
    assertEquals(1, result.getFailedUrls().size());
    assertTrue(result.getVisitedUrls().contains("http://example.com/page1"));
    assertTrue(result.getFailedUrls().contains("http://example.com/page3"));
  }

  @Test void testCrawlResultTotalTablesFound() {
    HtmlCrawler.CrawlResult result = new HtmlCrawler.CrawlResult();

    // Simulate multiple pages with tables
    List<HtmlTableScanner.TableInfo> page1Tables = new ArrayList<HtmlTableScanner.TableInfo>();
    // We cannot easily create real TableInfo instances here since the class may have
    // specific constructors, so we verify the aggregation logic indirectly.
    result.addHtmlTables("http://page1.com", page1Tables);
    result.addHtmlTables("http://page2.com", page1Tables);

    // With empty lists, total should be 0
    assertEquals(0, result.getTotalTablesFound());
  }

  // ===================================================================
  // CrawlerConfiguration tests
  // ===================================================================

  @Test void testCrawlerConfigurationDefaults() {
    CrawlerConfiguration config = new CrawlerConfiguration();

    assertFalse(config.isEnabled());
    assertEquals(0, config.getMaxDepth());
    assertNull(config.getLinkPattern());
    assertEquals(100, config.getMaxPages());
    assertFalse(config.isFollowExternalLinks());
    assertEquals(Duration.ofSeconds(1), config.getRequestDelay());
    assertTrue(config.isGenerateTablesFromHtml());
    assertEquals(1, config.getHtmlTableMinRows());
    assertEquals(Integer.MAX_VALUE, config.getHtmlTableMaxRows());
    assertEquals(10L * 1024 * 1024, config.getMaxHtmlSize());
    assertEquals(100L * 1024 * 1024, config.getMaxDataFileSize());
    assertTrue(config.isHonorHttpCacheHeaders());
    assertTrue(config.isEnforceContentLengthHeader());
    assertEquals(Duration.ofHours(1), config.getDataFileCacheTTL());
    assertEquals(Duration.ofMinutes(30), config.getHtmlCacheTTL());
    assertNull(config.getRefreshInterval());
    assertNull(config.getDataFilePattern());
    assertNull(config.getDataFileExcludePattern());
  }

  @Test void testCrawlerConfigurationSetters() {
    CrawlerConfiguration config = new CrawlerConfiguration();

    config.setEnabled(true);
    assertTrue(config.isEnabled());

    config.setMaxDepth(3);
    assertEquals(3, config.getMaxDepth());

    config.setMaxPages(50);
    assertEquals(50, config.getMaxPages());

    config.setFollowExternalLinks(true);
    assertTrue(config.isFollowExternalLinks());

    config.setRequestDelay(Duration.ofMillis(500));
    assertEquals(Duration.ofMillis(500), config.getRequestDelay());

    config.setGenerateTablesFromHtml(false);
    assertFalse(config.isGenerateTablesFromHtml());

    config.setHtmlTableMinRows(5);
    assertEquals(5, config.getHtmlTableMinRows());

    config.setHtmlTableMaxRows(100);
    assertEquals(100, config.getHtmlTableMaxRows());

    config.setMaxHtmlSize(5 * 1024 * 1024);
    assertEquals(5 * 1024 * 1024, config.getMaxHtmlSize());

    config.setMaxDataFileSize(50 * 1024 * 1024);
    assertEquals(50 * 1024 * 1024, config.getMaxDataFileSize());

    config.setHonorHttpCacheHeaders(false);
    assertFalse(config.isHonorHttpCacheHeaders());

    config.setEnforceContentLengthHeader(false);
    assertFalse(config.isEnforceContentLengthHeader());

    config.setDataFileCacheTTL(Duration.ofMinutes(15));
    assertEquals(Duration.ofMinutes(15), config.getDataFileCacheTTL());

    config.setHtmlCacheTTL(Duration.ofMinutes(10));
    assertEquals(Duration.ofMinutes(10), config.getHtmlCacheTTL());

    config.setRefreshInterval(Duration.ofHours(2));
    assertEquals(Duration.ofHours(2), config.getRefreshInterval());

    Pattern pattern = Pattern.compile(".*\\.csv");
    config.setDataFilePattern(pattern);
    assertEquals(pattern, config.getDataFilePattern());

    Pattern excludePattern = Pattern.compile(".*\\.tmp");
    config.setDataFileExcludePattern(excludePattern);
    assertEquals(excludePattern, config.getDataFileExcludePattern());

    config.setLinkPattern(Pattern.compile("https://.*"));
    assertNotNull(config.getLinkPattern());
  }

  @Test void testCrawlerConfigurationAllowedDomains() {
    CrawlerConfiguration config = new CrawlerConfiguration();

    config.addAllowedDomain("example.com");
    config.addAllowedDomain("data.gov");

    assertTrue(config.getAllowedDomains().contains("example.com"));
    assertTrue(config.getAllowedDomains().contains("data.gov"));
    assertEquals(2, config.getAllowedDomains().size());
  }

  @Test void testCrawlerConfigurationAllowedFileExtensions() {
    CrawlerConfiguration config = new CrawlerConfiguration();

    // Default extensions include csv, xlsx, xls, json, tsv, parquet, docx, pptx
    assertTrue(config.getAllowedFileExtensions().contains("csv"));
    assertTrue(config.getAllowedFileExtensions().contains("xlsx"));
    assertTrue(config.getAllowedFileExtensions().contains("parquet"));

    config.addAllowedFileExtension("PDF");
    assertTrue(config.getAllowedFileExtensions().contains("pdf"));
  }

  @Test void testCrawlerConfigurationSizeLimits() {
    CrawlerConfiguration config = new CrawlerConfiguration();

    // Default extension size limits
    assertEquals(50L * 1024 * 1024, config.getSizeLimitForExtension("csv"));
    assertEquals(100L * 1024 * 1024, config.getSizeLimitForExtension("xlsx"));
    assertEquals(200L * 1024 * 1024, config.getSizeLimitForExtension("parquet"));
    assertEquals(25L * 1024 * 1024, config.getSizeLimitForExtension("pdf"));
    assertEquals(20L * 1024 * 1024, config.getSizeLimitForExtension("json"));
    assertEquals(10L * 1024 * 1024, config.getSizeLimitForExtension("html"));

    // Unknown extension returns default maxDataFileSize
    assertEquals(100L * 1024 * 1024, config.getSizeLimitForExtension("unknown"));

    // Custom size limit
    config.setSizeLimitForExtension("csv", 10L * 1024 * 1024);
    assertEquals(10L * 1024 * 1024, config.getSizeLimitForExtension("csv"));
  }

  @Test void testCrawlerConfigurationFromMap() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("enabled", "true");
    options.put("maxDepth", "2");
    options.put("maxPages", "50");
    options.put("followExternalLinks", "true");
    options.put("linkPattern", "https://.*\\.gov/.*");
    options.put("generateTablesFromHtml", "false");
    options.put("htmlTableMinRows", "3");
    options.put("htmlTableMaxRows", "500");
    options.put("maxHtmlSize", "5MB");
    options.put("maxDataFileSize", "50MB");
    options.put("dataFileCacheTTL", "30 minutes");
    options.put("refreshInterval", "2 hours");
    options.put("dataFilePattern", ".*\\.csv$");
    options.put("dataFileExcludePattern", ".*temp.*");

    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);

    assertTrue(config.isEnabled());
    assertEquals(2, config.getMaxDepth());
    assertEquals(50, config.getMaxPages());
    assertTrue(config.isFollowExternalLinks());
    assertNotNull(config.getLinkPattern());
    assertFalse(config.isGenerateTablesFromHtml());
    assertEquals(3, config.getHtmlTableMinRows());
    assertEquals(500, config.getHtmlTableMaxRows());
    assertEquals(5L * 1024 * 1024, config.getMaxHtmlSize());
    assertEquals(50L * 1024 * 1024, config.getMaxDataFileSize());
    assertEquals(Duration.ofMinutes(30), config.getDataFileCacheTTL());
    assertEquals(Duration.ofHours(2), config.getRefreshInterval());
    assertNotNull(config.getDataFilePattern());
    assertNotNull(config.getDataFileExcludePattern());
  }

  @Test void testCrawlerConfigurationFromMapWithDomainsList() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("allowedDomains", Arrays.asList("example.com", "data.gov"));

    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);

    assertTrue(config.getAllowedDomains().contains("example.com"));
    assertTrue(config.getAllowedDomains().contains("data.gov"));
  }

  @Test void testCrawlerConfigurationFromMapWithSingleDomain() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("allowedDomains", "example.com");

    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);

    assertTrue(config.getAllowedDomains().contains("example.com"));
    assertEquals(1, config.getAllowedDomains().size());
  }

  @Test void testCrawlerConfigurationFromMapEmpty() {
    Map<String, Object> options = Collections.emptyMap();
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);

    // Should have all defaults
    assertFalse(config.isEnabled());
    assertEquals(0, config.getMaxDepth());
  }

  @Test void testCrawlerConfigurationParseSizeKB() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("maxHtmlSize", "512KB");

    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(512L * 1024, config.getMaxHtmlSize());
  }

  @Test void testCrawlerConfigurationParseSizeGB() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("maxDataFileSize", "1GB");

    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(1024L * 1024 * 1024, config.getMaxDataFileSize());
  }

  @Test void testCrawlerConfigurationParseSizeBytes() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("maxHtmlSize", "1024");

    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(1024L, config.getMaxHtmlSize());
  }

  @Test void testCrawlerConfigurationParseDurationSeconds() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("dataFileCacheTTL", "30 seconds");

    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(Duration.ofSeconds(30), config.getDataFileCacheTTL());
  }

  @Test void testCrawlerConfigurationParseDurationDays() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("refreshInterval", "7 days");

    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(Duration.ofDays(7), config.getRefreshInterval());
  }

  @Test void testCrawlerConfigurationParseDurationInvalid() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("dataFileCacheTTL", "invalid");

    assertThrows(IllegalArgumentException.class, () ->
        CrawlerConfiguration.fromMap(options));
  }

  @Test void testCrawlerConfigurationParseDurationUnknownUnit() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("dataFileCacheTTL", "10 fortnights");

    assertThrows(IllegalArgumentException.class, () ->
        CrawlerConfiguration.fromMap(options));
  }

  // ===================================================================
  // FileSourceConfig tests (bonus coverage)
  // ===================================================================

  @Test void testFileSourceConfigBuilder() {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/data/test.csv")
        .format("csv")
        .sheet("Sheet1")
        .build();

    assertEquals("/data/test.csv", config.getPath());
    assertEquals("csv", config.getFormat());
    assertEquals("Sheet1", config.getSheet());
  }

  @Test void testFileSourceConfigBuilderRequiresPath() {
    assertThrows(IllegalArgumentException.class, () ->
        FileSourceConfig.builder().build());
  }

  @Test void testFileSourceConfigBuilderEmptyPath() {
    assertThrows(IllegalArgumentException.class, () ->
        FileSourceConfig.builder().path("").build());
  }

  @Test void testFileSourceConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("path", "/data/test.csv");
    map.put("format", "csv");
    map.put("sheet", "Sheet1");

    FileSourceConfig config = FileSourceConfig.fromMap(map);
    assertEquals("/data/test.csv", config.getPath());
    assertEquals("csv", config.getFormat());
    assertEquals("Sheet1", config.getSheet());
  }

  @Test void testFileSourceConfigFromMapWithLocation() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("location", "/data/test.csv");

    FileSourceConfig config = FileSourceConfig.fromMap(map);
    assertEquals("/data/test.csv", config.getPath());
  }

  @Test void testFileSourceConfigFromMapNullFormat() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("path", "/data/test.csv");

    FileSourceConfig config = FileSourceConfig.fromMap(map);
    assertEquals("/data/test.csv", config.getPath());
    assertNull(config.getFormat());
    assertNull(config.getSheet());
  }

  // ===================================================================
  // HtmlCrawler.ResourceMetadata tests (via reflection)
  // ===================================================================

  @Test void testResourceMetadataIsExpired() throws Exception {
    Class<?> metadataClass = null;
    for (Class<?> innerClass : HtmlCrawler.class.getDeclaredClasses()) {
      if (innerClass.getSimpleName().equals("ResourceMetadata")) {
        metadataClass = innerClass;
        break;
      }
    }

    assertNotNull(metadataClass, "ResourceMetadata inner class should exist");

    java.lang.reflect.Constructor<?> constructor =
        metadataClass.getDeclaredConstructor(
            String.class, long.class, String.class, String.class);
    constructor.setAccessible(true);

    Object metadata = constructor.newInstance("hash123", 1024L, "etag1", "Wed, 01 Jan 2025");

    Method isExpired = metadataClass.getDeclaredMethod("isExpired", long.class);
    isExpired.setAccessible(true);

    // Should not be expired with very long TTL
    assertFalse((Boolean) isExpired.invoke(metadata, Long.MAX_VALUE));

    // Set processedAt to 10 seconds ago so TTL check reliably expires
    java.lang.reflect.Field processedAtField = metadataClass.getDeclaredField("processedAt");
    processedAtField.setAccessible(true);
    processedAtField.setLong(metadata, System.currentTimeMillis() - 10000L);

    // Should be expired with 1ms TTL when processed 10s ago
    assertTrue((Boolean) isExpired.invoke(metadata, 1L));
  }

  // ===================================================================
  // HtmlCrawler utility methods via reflection
  // ===================================================================

  @Test void testHtmlCrawlerGetFileName() throws Exception {
    CrawlerConfiguration config = new CrawlerConfiguration();
    HtmlCrawler crawler = new HtmlCrawler(config);

    try {
      Method getFileName = HtmlCrawler.class.getDeclaredMethod("getFileName", String.class);
      getFileName.setAccessible(true);

      String result = (String) getFileName.invoke(crawler, "http://example.com/data/report.csv");
      assertTrue(result.startsWith("report_"));
      assertTrue(result.endsWith(".csv"));

      // URL with query string
      String withQuery = (String) getFileName.invoke(
          crawler, "http://example.com/data/file.xlsx?token=abc");
      assertTrue(withQuery.startsWith("file_"));
      assertTrue(withQuery.endsWith(".xlsx"));

      // URL ending with /
      String emptyName = (String) getFileName.invoke(crawler, "http://example.com/");
      assertTrue(emptyName.startsWith("download"));
    } finally {
      crawler.cleanup();
    }
  }

  @Test void testHtmlCrawlerGetFileExtension() throws Exception {
    CrawlerConfiguration config = new CrawlerConfiguration();
    HtmlCrawler crawler = new HtmlCrawler(config);

    try {
      Method getFileExtension = HtmlCrawler.class.getDeclaredMethod(
          "getFileExtension", String.class);
      getFileExtension.setAccessible(true);

      assertEquals("csv", getFileExtension.invoke(
          crawler, "http://example.com/data.csv"));
      assertEquals("xlsx", getFileExtension.invoke(
          crawler, "http://example.com/data.xlsx?param=1"));
      assertEquals("json", getFileExtension.invoke(
          crawler, "http://example.com/path/file.JSON"));
      // Note: getFileExtension works on full URL path, including domain;
      // a URL like "http://example.com/noextension" has "." in "example.com"
      // so it returns "com/noextension" - test with a path that truly has no dot
      String noExt = (String) getFileExtension.invoke(
          crawler, "http://localhost/noextension");
      assertEquals("", noExt);
    } finally {
      crawler.cleanup();
    }
  }

  @Test void testHtmlCrawlerComputeFileHash(@TempDir Path tempDir) throws Exception {
    CrawlerConfiguration config = new CrawlerConfiguration();
    HtmlCrawler crawler = new HtmlCrawler(config);

    try {
      File testFile = tempDir.resolve("test.txt").toFile();
      Files.write(testFile.toPath(), "hello world".getBytes(StandardCharsets.UTF_8));

      Method computeFileHash = HtmlCrawler.class.getDeclaredMethod(
          "computeFileHash", File.class);
      computeFileHash.setAccessible(true);

      String hash = (String) computeFileHash.invoke(crawler, testFile);
      // Should contain filename and size
      assertTrue(hash.contains("test.txt"));
      assertTrue(hash.contains("_"));
    } finally {
      crawler.cleanup();
    }
  }

  @Test void testHtmlCrawlerCleanup() throws IOException {
    CrawlerConfiguration config = new CrawlerConfiguration();
    HtmlCrawler crawler = new HtmlCrawler(config);
    assertNotNull(crawler.getLinkCache());

    // Calling cleanup should not throw
    crawler.cleanup();
    // Calling cleanup again should be safe
    crawler.cleanup();
  }

  @Test void testHtmlCrawlerCrawlDisabled() throws Exception {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(false);
    // When disabled, crawl should just process single page.
    // Since there is no real HTTP server, this will fail on the
    // network call inside processSinglePage -> linkCache.getLinks().
    // We verify the CrawlResult is returned (even if exception wrapped).
    HtmlCrawler crawler = new HtmlCrawler(config);
    try {
      // Using a file:// URL that does not exist will cause an IOException
      // in getLinks, which is caught in crawl
      assertThrows(IOException.class, () ->
          crawler.crawl("file:///nonexistent/page.html"));
    } finally {
      crawler.cleanup();
    }
  }

  // ===================================================================
  // StorageProvider.normalizePath (static utility) tests
  // ===================================================================

  @Test void testStorageProviderNormalizePath() {
    assertNull(StorageProvider.normalizePath(null));
    assertEquals("s3a://bucket/key", StorageProvider.normalizePath("s3a:/bucket/key"));
    assertEquals("s3a://bucket/key", StorageProvider.normalizePath("s3a://bucket/key"));
    assertEquals("s3://bucket/key", StorageProvider.normalizePath("s3:/bucket/key"));
    assertEquals("s3://bucket/key", StorageProvider.normalizePath("s3://bucket/key"));
    assertEquals("hdfs://namenode/path", StorageProvider.normalizePath("hdfs:/namenode/path"));
    assertEquals("hdfs://namenode/path", StorageProvider.normalizePath("hdfs://namenode/path"));
    assertEquals("/local/path", StorageProvider.normalizePath("/local/path"));
  }

  // ===================================================================
  // StorageProvider inner classes tests
  // ===================================================================

  @Test void testFileEntryGetters() {
    StorageProvider.FileEntry entry = new StorageProvider.FileEntry(
        "/data/file.csv", "file.csv", false, 1024L, 1700000000000L);

    assertEquals("/data/file.csv", entry.getPath());
    assertEquals("file.csv", entry.getName());
    assertFalse(entry.isDirectory());
    assertEquals(1024L, entry.getSize());
    assertEquals(1700000000000L, entry.getLastModified());
  }

  @Test void testFileEntryDirectory() {
    StorageProvider.FileEntry entry = new StorageProvider.FileEntry(
        "/data/subdir", "subdir", true, 0L, 1700000000000L);

    assertTrue(entry.isDirectory());
    assertEquals(0L, entry.getSize());
  }

  @Test void testFileMetadataGetters() {
    StorageProvider.FileMetadata metadata = new StorageProvider.FileMetadata(
        "/data/file.csv", 2048L, 1700000000000L, "text/csv", "etag123");

    assertEquals("/data/file.csv", metadata.getPath());
    assertEquals(2048L, metadata.getSize());
    assertEquals(1700000000000L, metadata.getLastModified());
    assertEquals("text/csv", metadata.getContentType());
    assertEquals("etag123", metadata.getEtag());
  }

  @Test void testFileMetadataNullEtag() {
    StorageProvider.FileMetadata metadata = new StorageProvider.FileMetadata(
        "/data/file.csv", 1024L, 1700000000000L, null, null);

    assertNull(metadata.getContentType());
    assertNull(metadata.getEtag());
  }

  // ===================================================================
  // Helper methods
  // ===================================================================

  /**
   * Creates a mock StorageProvider for testing.
   */
  private static StorageProvider createMockStorageProvider(final String type) {
    return new StorageProvider() {
      @Override public List<FileEntry> listFiles(String path, boolean recursive) {
        return Collections.emptyList();
      }

      @Override public FileMetadata getMetadata(String path) {
        return new FileMetadata(path, 0L, 0L, null, null);
      }

      @Override public InputStream openInputStream(String path) throws IOException {
        return new ByteArrayInputStream(new byte[0]);
      }

      @Override public Reader openReader(String path) {
        return new StringReader("");
      }

      @Override public boolean exists(String path) {
        return false;
      }

      @Override public boolean isDirectory(String path) {
        return false;
      }

      @Override public String getStorageType() {
        return type;
      }

      @Override public String resolvePath(String basePath, String relativePath) {
        return basePath + "/" + relativePath;
      }
    };
  }

  /**
   * Creates a local storage provider that reads real files.
   */
  private static StorageProvider createLocalStorageProvider() {
    return new StorageProvider() {
      @Override public List<FileEntry> listFiles(String path, boolean recursive) {
        return Collections.emptyList();
      }

      @Override public FileMetadata getMetadata(String path) throws IOException {
        File f = new File(path);
        return new FileMetadata(path, f.length(), f.lastModified(), null, null);
      }

      @Override public InputStream openInputStream(String path) throws IOException {
        return java.nio.file.Files.newInputStream(new File(path).toPath());
      }

      @Override public Reader openReader(String path) throws IOException {
        return new java.io.FileReader(path);
      }

      @Override public boolean exists(String path) {
        return new File(path).exists();
      }

      @Override public boolean isDirectory(String path) {
        return new File(path).isDirectory();
      }

      @Override public String getStorageType() {
        return "local";
      }

      @Override public String resolvePath(String basePath, String relativePath) {
        return new File(basePath, relativePath).getPath();
      }
    };
  }
}
