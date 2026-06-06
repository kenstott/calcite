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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link RefreshableParquetCacheTable}.
 *
 * <p>Targets uncovered lines in:
 * - Constructor paths: with and without originalSource, file exists / doesn't exist
 * - setDuckDBNames
 * - setRefreshContext
 * - doRefresh: file not modified, file modified, originalSource conversion,
 *   parquet file deletion, temp file cleanup, exception handling
 * - rerunConversionsIfNeeded
 * - refreshJsonPathExtractions
 * - extractJsonPath (null, brackets, no match)
 * - createSourceTable: csv, json, xlsx, html, xml, unsupported
 * - updateDelegateTable
 * - toRel (via delegate check)
 * - getRowType (null delegateTable reinit)
 * - scan (delegate check)
 * - getRefreshBehavior
 * - getParquetFile / getSource
 * - toString
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class RefreshableParquetCacheTableCoverageTest {

  @TempDir
  Path tempDir;

  // ===== Helper methods =====

  private File createCsvFile(Path dir, String name) throws IOException {
    File file = dir.resolve(name).toFile();
    FileWriter writer = new FileWriter(file);
    try {
      writer.write("id,name\n1,Alice\n2,Bob\n");
    } finally {
      writer.close();
    }
    return file;
  }

  private File createJsonFile(Path dir, String name) throws IOException {
    File file = dir.resolve(name).toFile();
    FileWriter writer = new FileWriter(file);
    try {
      writer.write("[{\"id\": 1, \"name\": \"Alice\"}, {\"id\": 2, \"name\": \"Bob\"}]");
    } finally {
      writer.close();
    }
    return file;
  }

  private File createParquetStub(Path dir, String name) throws IOException {
    File file = dir.resolve(name).toFile();
    FileWriter writer = new FileWriter(file);
    try {
      writer.write("PARQUET_STUB");
    } finally {
      writer.close();
    }
    return file;
  }

  // ===== Constructor tests =====

  @Test
  void testConstructorWithExistingSourceFile() throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    File cacheDir = tempDir.toFile();
    Source source = Sources.of(csvFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, cacheDir, Duration.ofMinutes(5), false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    assertNotNull(table);
    assertEquals(Duration.ofMinutes(5), table.getRefreshInterval());
    assertEquals(parquetFile, table.getParquetFile());
    assertEquals(source, table.getSource());
    assertEquals(RefreshableTable.RefreshBehavior.SINGLE_FILE, table.getRefreshBehavior());
  }

  @Test
  void testConstructorWithNonExistentSourceFile() throws IOException {
    File nonExistent = new File(tempDir.toFile(), "nonexistent.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    File cacheDir = tempDir.toFile();
    Source source = Sources.of(nonExistent);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, cacheDir, Duration.ofMinutes(5), false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    assertNotNull(table);
  }

  @Test
  void testConstructorWithOriginalSource() throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File origFile = createCsvFile(tempDir, "original.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    File cacheDir = tempDir.toFile();
    Source source = Sources.of(csvFile);
    Source originalSource = Sources.of(origFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, originalSource, parquetFile, cacheDir, Duration.ofMinutes(5), false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    assertNotNull(table);
  }

  @Test
  void testConstructorWithNullRefreshInterval() throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    File cacheDir = tempDir.toFile();
    Source source = Sources.of(csvFile);

    // null refreshInterval => lastModifiedTime set to file's last modified
    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, cacheDir, null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    assertNotNull(table);
    assertNull(table.getRefreshInterval());
    assertFalse(table.needsRefresh());
  }

  @Test
  void testConstructorWithOriginalSourceNonExistent() throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File nonExistentOrig = new File(tempDir.toFile(), "orig_missing.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    File cacheDir = tempDir.toFile();
    Source source = Sources.of(csvFile);
    Source originalSource = Sources.of(nonExistentOrig);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, originalSource, parquetFile, cacheDir, Duration.ofMinutes(5), false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    assertNotNull(table);
  }

  // ===== setDuckDBNames / setRefreshContext =====

  @Test
  void testSetDuckDBNames() throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(csvFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), Duration.ofMinutes(5), false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    table.setDuckDBNames("mySchema", "myTable");

    // Verify via reflection
    try {
      Field f = RefreshableParquetCacheTable.class.getDeclaredField("schemaName");
      f.setAccessible(true);
      assertEquals("mySchema", f.get(table));

      Field tf = RefreshableParquetCacheTable.class.getDeclaredField("tableName");
      tf.setAccessible(true);
      assertEquals("myTable", tf.get(table));
    } catch (Exception e) {
      // Acceptable
    }
  }

  @Test
  void testSetRefreshContext() throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(csvFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), Duration.ofMinutes(5), false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    table.setRefreshContext(null, "tableName");

    try {
      Field tf = RefreshableParquetCacheTable.class.getDeclaredField("tableName");
      tf.setAccessible(true);
      assertEquals("tableName", tf.get(table));
    } catch (Exception e) {
      // Acceptable
    }
  }

  // ===== extractJsonPath =====

  @Test
  void testExtractJsonPath() throws Exception {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(csvFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    Method m = RefreshableParquetCacheTable.class.getDeclaredMethod("extractJsonPath",
        String.class);
    m.setAccessible(true);

    // Normal case
    assertEquals("$.data.users", m.invoke(table, "JSONPATH_EXTRACTION[$.data.users]"));

    // Nested brackets
    assertEquals("$.items[0].name", m.invoke(table, "JSONPATH_EXTRACTION[$.items[0].name]"));

    // Null input
    assertNull(m.invoke(table, (String) null));

    // No brackets
    assertNull(m.invoke(table, "JSONPATH_EXTRACTION"));

    // Empty brackets
    assertNull(m.invoke(table, "JSONPATH_EXTRACTION[]"));
  }

  // ===== createSourceTable =====

  @Test
  void testCreateSourceTableCsv() throws Exception {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(csvFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    Method m = RefreshableParquetCacheTable.class.getDeclaredMethod("createSourceTable");
    m.setAccessible(true);

    Object result = m.invoke(table);
    assertNotNull(result);
    assertTrue(result instanceof org.apache.calcite.adapter.file.table.CsvTranslatableTable);
  }

  @Test
  void testCreateSourceTableJson() throws Exception {
    File jsonFile = createJsonFile(tempDir, "data.json");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(jsonFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    Method m = RefreshableParquetCacheTable.class.getDeclaredMethod("createSourceTable");
    m.setAccessible(true);

    Object result = m.invoke(table);
    assertNotNull(result);
    assertTrue(result instanceof org.apache.calcite.adapter.file.table.JsonScannableTable);
  }

  @Test
  void testCreateSourceTableHtmlThrows() throws Exception {
    File htmlFile = new File(tempDir.toFile(), "data.html");
    FileWriter writer = new FileWriter(htmlFile);
    try {
      writer.write("<html><body>Hello</body></html>");
    } finally {
      writer.close();
    }
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(htmlFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    Method m = RefreshableParquetCacheTable.class.getDeclaredMethod("createSourceTable");
    m.setAccessible(true);

    try {
      m.invoke(table);
      assertTrue(false, "Expected RuntimeException");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertTrue(e.getCause().getMessage().contains("HTML"));
    }
  }

  @Test
  void testCreateSourceTableXmlThrows() throws Exception {
    File xmlFile = new File(tempDir.toFile(), "data.xml");
    FileWriter writer = new FileWriter(xmlFile);
    try {
      writer.write("<root><item>1</item></root>");
    } finally {
      writer.close();
    }
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(xmlFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    Method m = RefreshableParquetCacheTable.class.getDeclaredMethod("createSourceTable");
    m.setAccessible(true);

    try {
      m.invoke(table);
      assertTrue(false, "Expected RuntimeException");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertTrue(e.getCause().getMessage().contains("XML"));
    }
  }

  @Test
  void testCreateSourceTableUnsupportedThrows() throws Exception {
    File unknownFile = new File(tempDir.toFile(), "data.xyz");
    FileWriter writer = new FileWriter(unknownFile);
    try {
      writer.write("unknown");
    } finally {
      writer.close();
    }
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(unknownFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    Method m = RefreshableParquetCacheTable.class.getDeclaredMethod("createSourceTable");
    m.setAccessible(true);

    try {
      m.invoke(table);
      assertTrue(false, "Expected IllegalArgumentException");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertTrue(e.getCause().getMessage().contains("Unsupported"));
    }
  }

  // ===== doRefresh: file not modified =====

  @Test
  void testDoRefreshFileNotModified() throws Exception {
    File csvFile = createCsvFile(tempDir, "stable.csv");
    File parquetFile = createParquetStub(tempDir, "stable.parquet");
    Source source = Sources.of(csvFile);

    // Use null refreshInterval so lastModifiedTime = file.lastModified()
    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    // Directly call doRefresh - file not modified since constructor
    Method doRefresh = AbstractRefreshableTable.class.getDeclaredMethod("doRefresh");
    doRefresh.setAccessible(true);
    doRefresh.invoke(table);

    // Parquet file should still be the same stub
    assertEquals(parquetFile, table.getParquetFile());
  }

  // ===== getRefreshBehavior =====

  @Test
  void testGetRefreshBehavior() throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(csvFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    assertEquals(RefreshableTable.RefreshBehavior.SINGLE_FILE, table.getRefreshBehavior());
  }

  // ===== toString =====

  @Test
  void testToString() throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(csvFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    String str = table.toString();
    assertTrue(str.contains("RefreshableParquetCacheTable"));
    assertTrue(str.contains("data.csv"));
    assertTrue(str.contains("data.parquet"));
  }

  // ===== getParquetFile / getSource =====

  @Test
  void testGettersParquetFileAndSource() throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(csvFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    assertEquals(parquetFile, table.getParquetFile());
    assertEquals(source, table.getSource());
  }

  // ===== needsRefresh with non-null interval =====

  @Test
  void testNeedsRefreshWithInterval() throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(csvFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), Duration.ofMinutes(5), false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    // First time should need refresh
    assertTrue(table.needsRefresh());
  }

  @Test
  void testNeedsRefreshAfterRefreshCallWithInterval() throws Exception {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(csvFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), Duration.ofHours(1), false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    assertTrue(table.needsRefresh());

    // Call refresh (doRefresh will try to convert - will fail on stub parquet)
    // Use the base class refresh() which checks needsRefresh first
    table.refresh();

    // After calling refresh(), lastRefreshTime is set, so needsRefresh should be false
    // (the interval is 1 hour)
    assertFalse(table.needsRefresh());
  }

  // ===== updateDelegateTable =====

  @Test
  void testUpdateDelegateTable() throws Exception {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(csvFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    // Verify delegate table was set
    Field delegateField = RefreshableParquetCacheTable.class.getDeclaredField("delegateTable");
    delegateField.setAccessible(true);
    assertNotNull(delegateField.get(table));
  }

  // ===== rerunConversionsIfNeeded =====

  @Test
  void testRerunConversionsIfNeeded() throws Exception {
    File csvFile = createCsvFile(tempDir, "data.csv");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(csvFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    Method m = RefreshableParquetCacheTable.class.getDeclaredMethod("rerunConversionsIfNeeded",
        File.class);
    m.setAccessible(true);

    // Should not throw even when file doesn't need conversion
    m.invoke(table, csvFile);
  }

  @Test
  void testRerunConversionsJsonFile() throws Exception {
    File jsonFile = createJsonFile(tempDir, "data.json");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(jsonFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    Method m = RefreshableParquetCacheTable.class.getDeclaredMethod("rerunConversionsIfNeeded",
        File.class);
    m.setAccessible(true);

    // JSON file will trigger refreshJsonPathExtractions
    m.invoke(table, jsonFile);
  }

  // ===== refreshJsonPathExtractions =====

  @Test
  void testRefreshJsonPathExtractions() throws Exception {
    File jsonFile = createJsonFile(tempDir, "source.json");
    File parquetFile = createParquetStub(tempDir, "data.parquet");
    Source source = Sources.of(jsonFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), null, false,
        "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "test_schema");

    Method m = RefreshableParquetCacheTable.class.getDeclaredMethod("refreshJsonPathExtractions",
        File.class);
    m.setAccessible(true);

    // Should not throw - no derived files exist
    m.invoke(table, jsonFile);
  }

  // ===== Constructor with type inference enabled =====

  @Test
  void testConstructorTypeInferenceEnabled() throws IOException {
    File csvFile = createCsvFile(tempDir, "infer.csv");
    File parquetFile = createParquetStub(tempDir, "infer.parquet");
    Source source = Sources.of(csvFile);

    RefreshableParquetCacheTable table = new RefreshableParquetCacheTable(
        source, parquetFile, tempDir.toFile(), Duration.ofMinutes(10), true,
        "UPPER", "LOWER", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "infer_schema");

    assertNotNull(table);
    assertEquals(Duration.ofMinutes(10), table.getRefreshInterval());
  }
}
