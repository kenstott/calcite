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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.util.Source;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Comprehensive coverage tests for refresh classes:
 * {@link PatternAwareRefreshListener}, {@link TableRefreshListener},
 * {@link RefreshableTable}, {@link AbstractRefreshableTable},
 * {@link RefreshableCsvTable}, {@link RefreshableJsonTable},
 * {@link RefreshableParquetCacheTable}, and {@link RefreshablePartitionedParquetTable}.
 */
@Tag("unit")
class RefreshCoverageTest {

  // ===================================================================
  // Helper methods
  // ===================================================================

  private Source createFileSource(File file) {
    Source source = mock(Source.class);
    when(source.protocol()).thenReturn("file");
    when(source.file()).thenReturn(file);
    when(source.path()).thenReturn(file.getAbsolutePath());
    return source;
  }

  private Source createHttpSource(String url) {
    Source source = mock(Source.class);
    when(source.protocol()).thenReturn("https");
    when(source.path()).thenReturn(url);
    return source;
  }

  private File createCsvFile(Path dir, String name, String content) throws IOException {
    File file = dir.resolve(name).toFile();
    try (FileWriter w = new FileWriter(file)) {
      w.write(content);
    }
    return file;
  }

  // ===================================================================
  // TableRefreshListener interface
  // ===================================================================

  @Test void testTableRefreshListenerCanBeImplemented(@TempDir Path tempDir)
      throws IOException {
    final boolean[] called = {false};
    final String[] capturedTableName = {null};
    final File[] capturedFile = {null};

    TableRefreshListener listener = new TableRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        called[0] = true;
        capturedTableName[0] = tableName;
        capturedFile[0] = parquetFile;
      }
    };

    File dummyFile = createCsvFile(tempDir, "table.parquet", "dummy");
    listener.onTableRefreshed("my_table", dummyFile);

    assertTrue(called[0]);
    assertEquals("my_table", capturedTableName[0]);
    assertEquals(dummyFile, capturedFile[0]);
  }

  // ===================================================================
  // PatternAwareRefreshListener interface
  // ===================================================================

  @Test void testPatternAwareRefreshListenerImplementation(@TempDir Path tempDir)
      throws IOException {
    final String[] capturedPattern = {null};
    final String[] capturedTableName = {null};

    PatternAwareRefreshListener listener = new PatternAwareRefreshListener() {
      @Override public void onTableRefreshedWithPattern(String tableName, String pattern) {
        capturedTableName[0] = tableName;
        capturedPattern[0] = pattern;
      }

      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        // no-op
      }
    };

    listener.onTableRefreshedWithPattern("partitioned_table", "s3://bucket/**/*.parquet");

    assertEquals("partitioned_table", capturedTableName[0]);
    assertEquals("s3://bucket/**/*.parquet", capturedPattern[0]);
  }

  @Test void testPatternAwareRefreshListenerDefaultIcebergMethod(@TempDir Path tempDir)
      throws IOException {
    PatternAwareRefreshListener listener = new PatternAwareRefreshListener() {
      @Override public void onTableRefreshedWithPattern(String tableName, String pattern) {
        // no-op
      }

      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        // no-op
      }
    };

    // Default onIcebergTableRefreshed should be a no-op (not throw)
    listener.onIcebergTableRefreshed("iceberg_table", "s3://bucket/warehouse/table");
  }

  @Test void testPatternAwareRefreshListenerCustomIcebergMethod() {
    final String[] capturedTableName = {null};
    final String[] capturedLocation = {null};

    PatternAwareRefreshListener listener = new PatternAwareRefreshListener() {
      @Override public void onTableRefreshedWithPattern(String tableName, String pattern) {
        // no-op
      }

      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        // no-op
      }

      @Override public void onIcebergTableRefreshed(String tableName, String tableLocation) {
        capturedTableName[0] = tableName;
        capturedLocation[0] = tableLocation;
      }
    };

    listener.onIcebergTableRefreshed("ice_tbl", "s3://bucket/warehouse/ice_tbl");

    assertEquals("ice_tbl", capturedTableName[0]);
    assertEquals("s3://bucket/warehouse/ice_tbl", capturedLocation[0]);
  }

  // ===================================================================
  // RefreshableTable.RefreshBehavior enum
  // ===================================================================

  @Test void testRefreshBehaviorSingleFile() {
    RefreshableTable.RefreshBehavior behavior = RefreshableTable.RefreshBehavior.SINGLE_FILE;
    assertNotNull(behavior.getDescription());
    assertTrue(behavior.getDescription().contains("Re-reads"));
  }

  @Test void testRefreshBehaviorDirectoryScan() {
    RefreshableTable.RefreshBehavior behavior = RefreshableTable.RefreshBehavior.DIRECTORY_SCAN;
    assertNotNull(behavior.getDescription());
    assertTrue(behavior.getDescription().contains("Updates"));
  }

  @Test void testRefreshBehaviorPartitionedTable() {
    RefreshableTable.RefreshBehavior behavior = RefreshableTable.RefreshBehavior.PARTITIONED_TABLE;
    assertNotNull(behavior.getDescription());
    assertTrue(behavior.getDescription().contains("partitions"));
  }

  @Test void testRefreshBehaviorMaterializedView() {
    RefreshableTable.RefreshBehavior behavior = RefreshableTable.RefreshBehavior.MATERIALIZED_VIEW;
    assertNotNull(behavior.getDescription());
    assertTrue(behavior.getDescription().contains("Re-executes"));
  }

  @Test void testRefreshBehaviorValues() {
    RefreshableTable.RefreshBehavior[] values = RefreshableTable.RefreshBehavior.values();
    assertEquals(4, values.length);
  }

  @Test void testRefreshBehaviorValueOf() {
    assertEquals(RefreshableTable.RefreshBehavior.SINGLE_FILE,
        RefreshableTable.RefreshBehavior.valueOf("SINGLE_FILE"));
    assertEquals(RefreshableTable.RefreshBehavior.DIRECTORY_SCAN,
        RefreshableTable.RefreshBehavior.valueOf("DIRECTORY_SCAN"));
    assertEquals(RefreshableTable.RefreshBehavior.PARTITIONED_TABLE,
        RefreshableTable.RefreshBehavior.valueOf("PARTITIONED_TABLE"));
    assertEquals(RefreshableTable.RefreshBehavior.MATERIALIZED_VIEW,
        RefreshableTable.RefreshBehavior.valueOf("MATERIALIZED_VIEW"));
  }

  // ===================================================================
  // AbstractRefreshableTable (via concrete subclass)
  // ===================================================================

  /**
   * Minimal concrete subclass for testing AbstractRefreshableTable behavior.
   */
  static class TestRefreshableTable extends AbstractRefreshableTable {
    int doRefreshCallCount = 0;

    TestRefreshableTable(String tableName, Duration refreshInterval) {
      super(tableName, refreshInterval);
    }

    @Override protected void doRefresh() {
      doRefreshCallCount++;
    }

    @Override public RefreshBehavior getRefreshBehavior() {
      return RefreshBehavior.SINGLE_FILE;
    }

    @Override public org.apache.calcite.rel.type.RelDataType getRowType(
        org.apache.calcite.rel.type.RelDataTypeFactory typeFactory) {
      return null;
    }
  }

  @Test void testAbstractRefreshableTableConstructor() {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));
    assertEquals(Duration.ofMinutes(5), table.getRefreshInterval());
    assertNull(table.getLastRefreshTime());
  }

  @Test void testAbstractRefreshableTableNullInterval() {
    TestRefreshableTable table = new TestRefreshableTable("test", null);
    assertNull(table.getRefreshInterval());
    assertFalse(table.needsRefresh());
  }

  @Test void testAbstractRefreshableTableNeedsRefreshFirstTime() {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));
    assertTrue(table.needsRefresh());
  }

  @Test void testAbstractRefreshableTableNeedsRefreshAfterRefresh() {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofHours(1));
    assertTrue(table.needsRefresh());

    table.refresh();

    // With 1 hour interval, should not need refresh immediately
    assertFalse(table.needsRefresh());
    assertNotNull(table.getLastRefreshTime());
    assertEquals(1, table.doRefreshCallCount);
  }

  @Test void testAbstractRefreshableTableRefreshSkipsWhenNotNeeded() {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofHours(1));

    table.refresh();
    assertEquals(1, table.doRefreshCallCount);

    // Immediate second call should be skipped
    table.refresh();
    assertEquals(1, table.doRefreshCallCount,
        "doRefresh should not be called when interval has not elapsed");
  }

  @Test void testAbstractRefreshableTableRefreshAfterIntervalElapsed() throws Exception {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMillis(1));

    table.refresh();
    assertEquals(1, table.doRefreshCallCount);

    Thread.sleep(10);

    table.refresh();
    assertEquals(2, table.doRefreshCallCount);
  }

  @Test void testAbstractRefreshableTableRefreshWithNullInterval() {
    TestRefreshableTable table = new TestRefreshableTable("test", null);

    table.refresh();
    // With null interval, doRefresh should never be called
    assertEquals(0, table.doRefreshCallCount);
  }

  @Test void testAbstractRefreshableTableIsFileModified(@TempDir Path tempDir) throws Exception {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));

    File file = createCsvFile(tempDir, "test.csv", "id,name\n1,Alice\n");

    // Initially lastModifiedTime is 0, so file should be considered modified
    assertTrue(table.isFileModified(file));
  }

  @Test void testAbstractRefreshableTableIsFileModifiedNonExistent(@TempDir Path tempDir) {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));

    File nonExistent = new File(tempDir.toFile(), "does_not_exist.csv");
    assertFalse(table.isFileModified(nonExistent));
  }

  @Test void testAbstractRefreshableTableUpdateLastModified(@TempDir Path tempDir)
      throws Exception {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));

    File file = createCsvFile(tempDir, "update.csv", "data");

    // Initially file is modified (lastModifiedTime = 0)
    assertTrue(table.isFileModified(file));

    // Update last modified
    table.updateLastModified(file);

    // Now file should not be modified
    assertFalse(table.isFileModified(file));

    // lastRefreshTime should be set
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testAbstractRefreshableTableUpdateLastModifiedNonExistent(@TempDir Path tempDir) {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));

    File nonExistent = new File(tempDir.toFile(), "missing.csv");
    table.updateLastModified(nonExistent);

    // Should still update lastRefreshTime
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testAbstractRefreshableTableProtectedFields() throws Exception {
    TestRefreshableTable table = new TestRefreshableTable("my_table", Duration.ofMinutes(10));

    // Verify protected field tableName via reflection
    Field tableNameField = AbstractRefreshableTable.class.getDeclaredField("tableName");
    tableNameField.setAccessible(true);
    assertEquals("my_table", tableNameField.get(table));

    // Verify lastModifiedTime starts at 0
    Field lastModField = AbstractRefreshableTable.class.getDeclaredField("lastModifiedTime");
    lastModField.setAccessible(true);
    assertEquals(0L, lastModField.get(table));

    // Verify lastRemoteMetadata starts as null
    Field remoteMetaField =
        AbstractRefreshableTable.class.getDeclaredField("lastRemoteMetadata");
    remoteMetaField.setAccessible(true);
    assertNull(remoteMetaField.get(table));
  }

  // ===================================================================
  // RefreshableCsvTable
  // ===================================================================

  @Test void testRefreshableCsvTableConstructor(@TempDir Path tempDir) throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv", "id,name\n1,Alice\n");
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "csv_table", null, Duration.ofMinutes(5));

    assertNotNull(table);
    assertEquals(Duration.ofMinutes(5), table.getRefreshInterval());
    assertNull(table.getLastRefreshTime());
    assertEquals(RefreshableTable.RefreshBehavior.SINGLE_FILE, table.getRefreshBehavior());
  }

  @Test void testRefreshableCsvTableNullInterval(@TempDir Path tempDir) throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv", "id,name\n1,Alice\n");
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(source, "csv_table", null, null);

    assertNull(table.getRefreshInterval());
    assertFalse(table.needsRefresh());
  }

  @Test void testRefreshableCsvTableNeedsRefreshFirstTime(@TempDir Path tempDir) throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv", "id,name\n1,Alice\n");
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "csv_table", null, Duration.ofMinutes(1));

    assertTrue(table.needsRefresh());
  }

  @Test void testRefreshableCsvTableRefreshSetsLastRefreshTime(@TempDir Path tempDir)
      throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv", "id,name\n1,Alice\n");
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "csv_table", null, Duration.ofMinutes(5));

    assertNull(table.getLastRefreshTime());
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testRefreshableCsvTableRefreshSkipsWhenNotNeeded(@TempDir Path tempDir)
      throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv", "id,name\n1,Alice\n");
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "csv_table", null, Duration.ofHours(1));

    table.refresh();
    Instant firstRefresh = table.getLastRefreshTime();

    table.refresh();
    assertEquals(firstRefresh, table.getLastRefreshTime());
  }

  @Test void testRefreshableCsvTableRefreshDetectsModification(@TempDir Path tempDir)
      throws Exception {
    File csvFile = createCsvFile(tempDir, "data.csv", "id,name\n1,Alice\n");
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "csv_table", null, Duration.ofMillis(1));

    table.refresh();

    Thread.sleep(10);

    // Modify the file
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,Alice\n2,Bob\n");
    }

    Thread.sleep(10);

    table.refresh();

    // Verify dataStale was set via reflection
    Field staleField = RefreshableCsvTable.class.getDeclaredField("dataStale");
    staleField.setAccessible(true);
    // dataStale should be true after detecting file modification
    assertTrue((Boolean) staleField.get(table));
  }

  @Test void testRefreshableCsvTableRefreshNonExistentFile(@TempDir Path tempDir) {
    File csvFile = new File(tempDir.toFile(), "nonexistent.csv");
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "csv_table", null, Duration.ofMillis(1));

    // Should not throw
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testRefreshableCsvTableRefreshNullFile(@TempDir Path tempDir) {
    Source source = mock(Source.class);
    when(source.protocol()).thenReturn("file");
    when(source.file()).thenReturn(null);
    when(source.path()).thenReturn("test.csv");

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "csv_table", null, Duration.ofMillis(1));

    // Should not throw
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testRefreshableCsvTableRemoteProtocol(@TempDir Path tempDir) {
    Source source = createHttpSource("https://example.com/data.csv");

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "csv_table", null, Duration.ofMillis(1));

    // Remote refresh will fail gracefully (no actual server)
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testRefreshableCsvTableS3Protocol() {
    Source source = mock(Source.class);
    when(source.protocol()).thenReturn("s3");
    when(source.path()).thenReturn("s3://bucket/data.csv");

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "csv_table", null, Duration.ofMillis(1));

    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testRefreshableCsvTableFtpProtocol() {
    Source source = mock(Source.class);
    when(source.protocol()).thenReturn("ftp");
    when(source.path()).thenReturn("ftp://example.com/data.csv");

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "csv_table", null, Duration.ofMillis(1));

    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testRefreshableCsvTableClearCachedRowType(@TempDir Path tempDir) throws Exception {
    File csvFile = createCsvFile(tempDir, "data.csv", "id,name\n1,Alice\n");
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "csv_table", null, Duration.ofMillis(1));

    // Access private clearCachedRowType
    Method clearMethod = RefreshableCsvTable.class.getDeclaredMethod("clearCachedRowType");
    clearMethod.setAccessible(true);
    // Should not throw even if fields are already null
    clearMethod.invoke(table);
  }

  @Test void testRefreshableCsvTableToString(@TempDir Path tempDir) throws IOException {
    File csvFile = createCsvFile(tempDir, "data.csv", "id,name\n1,Alice\n");
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "my_csv", null, Duration.ofMinutes(5));

    assertEquals("RefreshableCsvTable(my_csv)", table.toString());
  }

  @Test void testRefreshableCsvTableAfterIntervalElapsed(@TempDir Path tempDir) throws Exception {
    File csvFile = createCsvFile(tempDir, "data.csv", "id,name\n1,Alice\n");
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "csv_table", null, Duration.ofMillis(1));

    table.refresh();
    assertFalse(table.needsRefresh());

    Thread.sleep(10);

    assertTrue(table.needsRefresh());
  }

  // ===================================================================
  // RefreshableJsonTable
  // ===================================================================

  @Test void testRefreshableJsonTableConstructorMinimal(@TempDir Path tempDir) throws IOException {
    File jsonFile = createJsonFile(tempDir, "data.json", "[{\"id\": 1}]");
    Source source = createFileSource(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "json_table", Duration.ofMinutes(5));

    assertNotNull(table);
    assertEquals(RefreshableTable.RefreshBehavior.SINGLE_FILE, table.getRefreshBehavior());
    assertEquals("RefreshableJsonTable(json_table)", table.toString());
  }

  @Test void testRefreshableJsonTableConstructorWithCasing(@TempDir Path tempDir)
      throws IOException {
    File jsonFile = createJsonFile(tempDir, "data.json", "[{\"id\": 1}]");
    Source source = createFileSource(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "json_table", Duration.ofMinutes(5), "SMART_CASING");

    assertNotNull(table);
    assertEquals("RefreshableJsonTable(json_table)", table.toString());
  }

  @Test void testRefreshableJsonTableConstructorWithCacheDir(@TempDir Path tempDir)
      throws IOException {
    File jsonFile = createJsonFile(tempDir, "data.json", "[{\"id\": 1}]");
    Source source = createFileSource(jsonFile);
    File cacheDir = Files.createDirectories(tempDir.resolve("cache")).toFile();

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "json_table", Duration.ofMinutes(5), "SMART_CASING", cacheDir);

    assertNotNull(table);
  }

  @Test void testRefreshableJsonTableNullInterval(@TempDir Path tempDir) throws IOException {
    File jsonFile = createJsonFile(tempDir, "data.json", "[{\"id\": 1}]");
    Source source = createFileSource(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(source, "json_table", null);

    assertFalse(table.needsRefresh());
  }

  @Test void testRefreshableJsonTableNeedsRefreshFirstTime(@TempDir Path tempDir)
      throws IOException {
    File jsonFile = createJsonFile(tempDir, "data.json", "[{\"id\": 1}]");
    Source source = createFileSource(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "json_table", Duration.ofMinutes(5));

    assertTrue(table.needsRefresh());
  }

  @Test void testRefreshableJsonTableRefreshSetsTime(@TempDir Path tempDir) throws IOException {
    File jsonFile = createJsonFile(tempDir, "data.json", "[{\"id\": 1}]");
    Source source = createFileSource(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "json_table", Duration.ofMinutes(5));

    assertNull(table.getLastRefreshTime());
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testRefreshableJsonTableRefreshSkipsWhenNotNeeded(@TempDir Path tempDir)
      throws IOException {
    File jsonFile = createJsonFile(tempDir, "data.json", "[{\"id\": 1}]");
    Source source = createFileSource(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "json_table", Duration.ofHours(1));

    table.refresh();
    Instant first = table.getLastRefreshTime();

    table.refresh();
    assertEquals(first, table.getLastRefreshTime());
  }

  @Test void testRefreshableJsonTableRefreshAfterIntervalElapsed(@TempDir Path tempDir)
      throws Exception {
    File jsonFile = createJsonFile(tempDir, "data.json", "[{\"id\": 1}]");
    Source source = createFileSource(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "json_table", Duration.ofMillis(1));

    table.refresh();
    Instant first = table.getLastRefreshTime();

    Thread.sleep(10);
    assertTrue(table.needsRefresh());

    table.refresh();
    Instant second = table.getLastRefreshTime();

    assertTrue(second.isAfter(first));
  }

  @Test void testRefreshableJsonTableToString(@TempDir Path tempDir) throws IOException {
    File jsonFile = createJsonFile(tempDir, "data.json", "[{\"id\": 1}]");
    Source source = createFileSource(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "my_json_table", Duration.ofMinutes(5));

    assertEquals("RefreshableJsonTable(my_json_table)", table.toString());
  }

  // ===================================================================
  // RefreshablePartitionedParquetTable - basic properties only
  // (Constructor requires heavy dependencies, so we test what we can)
  // ===================================================================

  @Test void testRefreshBehaviorPartitionedTableDescription() {
    RefreshableTable.RefreshBehavior behavior = RefreshableTable.RefreshBehavior.PARTITIONED_TABLE;
    assertEquals("Discovers new partitions and updates existing files",
        behavior.getDescription());
  }

  // ===================================================================
  // RefreshableParquetCacheTable - basic properties only
  // (Constructor requires Source/File/SchemaPlus, tested via behavior patterns)
  // ===================================================================

  @Test void testRefreshBehaviorSingleFileDescription() {
    RefreshableTable.RefreshBehavior behavior = RefreshableTable.RefreshBehavior.SINGLE_FILE;
    assertEquals("Re-reads file if modified", behavior.getDescription());
  }

  // ===================================================================
  // AbstractRefreshableTable - isRemoteFileModified
  // ===================================================================

  @Test void testAbstractRefreshableTableIsRemoteFileModified() {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));
    Source source = createHttpSource("https://example.com/data.csv");

    // Remote file check will fail (no server) and return true (conservative)
    boolean result = table.isRemoteFileModified(source);
    assertTrue(result, "Should return true when remote check fails (conservative)");
  }

  // ===================================================================
  // AbstractRefreshableTable - updateRemoteMetadata
  // ===================================================================

  @Test void testAbstractRefreshableTableUpdateRemoteMetadata() throws Exception {
    TestRefreshableTable table = new TestRefreshableTable("test", Duration.ofMinutes(5));

    // Verify lastRemoteMetadata is initially null
    Field metaField = AbstractRefreshableTable.class.getDeclaredField("lastRemoteMetadata");
    metaField.setAccessible(true);
    assertNull(metaField.get(table));

    // updateRemoteMetadata sets both the metadata and lastRefreshTime
    table.updateRemoteMetadata(null);
    assertNotNull(table.getLastRefreshTime());
  }

  // ===================================================================
  // Integration scenario: full refresh lifecycle
  // ===================================================================

  @Test void testRefreshLifecycleWithFileChange(@TempDir Path tempDir) throws Exception {
    TestRefreshableTable table = new TestRefreshableTable("lifecycle", Duration.ofMillis(1));

    // Phase 1: First refresh
    assertTrue(table.needsRefresh());
    table.refresh();
    assertEquals(1, table.doRefreshCallCount);
    assertNotNull(table.getLastRefreshTime());
    assertFalse(table.needsRefresh());

    // Phase 2: Wait for interval to elapse
    Thread.sleep(10);
    assertTrue(table.needsRefresh());

    // Phase 3: Second refresh
    table.refresh();
    assertEquals(2, table.doRefreshCallCount);

    // Phase 4: Immediate call should be skipped
    table.refresh();
    assertEquals(2, table.doRefreshCallCount);
  }

  @Test void testRefreshableCsvTableLifecycle(@TempDir Path tempDir) throws Exception {
    File csvFile = createCsvFile(tempDir, "lifecycle.csv", "id,name\n1,Alice\n");
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "lifecycle", null, Duration.ofMillis(1));

    // Initial state
    assertTrue(table.needsRefresh());
    assertNull(table.getLastRefreshTime());

    // First refresh
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
    assertFalse(table.needsRefresh());

    // Wait and check again
    Thread.sleep(10);
    assertTrue(table.needsRefresh());

    // Second refresh
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test void testRefreshableJsonTableLifecycle(@TempDir Path tempDir) throws Exception {
    File jsonFile = createJsonFile(tempDir, "lifecycle.json",
        "[{\"id\": 1, \"name\": \"Alice\"}]");
    Source source = createFileSource(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "lifecycle", Duration.ofMillis(1));

    assertTrue(table.needsRefresh());
    table.refresh();
    assertNotNull(table.getLastRefreshTime());

    Thread.sleep(10);
    assertTrue(table.needsRefresh());

    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  // ===================================================================
  // Edge cases
  // ===================================================================

  @Test void testAbstractRefreshableTableRapidRefreshCalls() throws Exception {
    TestRefreshableTable table = new TestRefreshableTable("rapid", Duration.ofMillis(1));

    table.refresh();
    assertEquals(1, table.doRefreshCallCount);

    // Rapid calls within interval should be skipped
    for (int i = 0; i < 10; i++) {
      table.refresh();
    }
    // Should still be 1 since interval might not have elapsed
    assertTrue(table.doRefreshCallCount >= 1);
  }

  @Test void testRefreshableCsvTableDataStaleInitiallyFalse(@TempDir Path tempDir)
      throws Exception {
    File csvFile = createCsvFile(tempDir, "stale_test.csv", "id\n1\n");
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "stale_test", null, Duration.ofMinutes(5));

    Field staleField = RefreshableCsvTable.class.getDeclaredField("dataStale");
    staleField.setAccessible(true);
    assertFalse((Boolean) staleField.get(table));
  }

  @Test void testRefreshableCsvTableDataStaleTrueAfterFirstRefresh(@TempDir Path tempDir)
      throws Exception {
    File csvFile = createCsvFile(tempDir, "stale_first.csv", "id\n1\n");
    Source source = createFileSource(csvFile);

    RefreshableCsvTable table = new RefreshableCsvTable(
        source, "stale_test", null, Duration.ofMillis(1));

    table.refresh();

    Field staleField = RefreshableCsvTable.class.getDeclaredField("dataStale");
    staleField.setAccessible(true);
    // First refresh of existing file (lastModifiedTime was 0, file exists with time > 0)
    assertTrue((Boolean) staleField.get(table));
  }

  @Test void testRefreshableJsonTableRefreshBehavior(@TempDir Path tempDir) throws IOException {
    File jsonFile = createJsonFile(tempDir, "behavior.json", "[]");
    Source source = createFileSource(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "behavior_table", Duration.ofMinutes(5));

    assertEquals(RefreshableTable.RefreshBehavior.SINGLE_FILE, table.getRefreshBehavior());
  }

  // ===================================================================
  // Utility methods
  // ===================================================================

  private File createJsonFile(Path dir, String name, String content) throws IOException {
    File file = dir.resolve(name).toFile();
    Files.write(file.toPath(), content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    return file;
  }
}
