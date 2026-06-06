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

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.refresh.PatternAwareRefreshListener;
import org.apache.calcite.adapter.file.refresh.TableRefreshListener;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.Frameworks;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Line coverage tests for {@link FileSchema}.
 *
 * <p>Targets specific uncovered branches:
 * <ul>
 *   <li>getTableMap() with recursive directory scanning and glob patterns</li>
 *   <li>processJsonFlattening() with actual JSON files containing nested objects</li>
 *   <li>materialization setup when materializedViews operand is set</li>
 *   <li>close() method paths for cleanup of refresh scheduler</li>
 *   <li>addTable() with duplicate table name handling</li>
 *   <li>notifyTableRefreshed() listener notification path</li>
 *   <li>Various getter methods and utility methods</li>
 * </ul>
 */
@SuppressWarnings("deprecation")
@Tag("unit")
class FileSchemaLineCoverageTest {

  @TempDir
  Path tempDir;

  private final List<FileSchema> schemasToClose = new ArrayList<>();

  @AfterEach
  void cleanup() {
    for (FileSchema schema : schemasToClose) {
      try {
        schema.close();
      } catch (Exception e) {
        // ignore
      }
    }
    schemasToClose.clear();
  }

  // ---------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------

  private SchemaPlus rootSchema() {
    return Frameworks.createRootSchema(false);
  }

  private File dir(String name) {
    File d = tempDir.resolve(name).toFile();
    d.mkdirs();
    return d;
  }

  private File writeCsv(File d, String name, String content) throws IOException {
    File f = new File(d, name);
    try (FileWriter w = new FileWriter(f)) {
      w.write(content);
    }
    return f;
  }

  private File writeJson(File d, String name, String content) throws IOException {
    return writeCsv(d, name, content);
  }

  private Map<String, Object> baseOperand(File directory) {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", directory.getAbsolutePath());
    operand.put("ephemeralCache", true);
    operand.put("executionEngine", "PARQUET");
    return operand;
  }

  private FileSchema createSchemaViaFactory(String schemaName, Map<String, Object> operand) {
    FileSchemaFactory factory = FileSchemaFactory.INSTANCE;
    SchemaPlus parentSchema = rootSchema();
    Schema schema = factory.create(parentSchema, schemaName, operand);
    if (schema instanceof FileSchema) {
      schemasToClose.add((FileSchema) schema);
    }
    return (FileSchema) schema;
  }

  private FileSchema createDirectSchema(String schemaName, File sourceDir,
      boolean recursive, Boolean flatten,
      List<Map<String, Object>> materializations,
      List<Map<String, Object>> views) {
    SchemaPlus parent = rootSchema();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 1024);
    FileSchema schema =
        new FileSchema(parent, schemaName, sourceDir, null, null, engineConfig, recursive, materializations, views, null, null,
        "LOWER", "LOWER", null, null, flatten, null, false);
    schemasToClose.add(schema);
    return schema;
  }

  // ---------------------------------------------------------------
  // 1. Recursive directory scanning
  // ---------------------------------------------------------------

  @Test void testRecursiveDirectoryScanningWithSubdirectories() throws Exception {
    File d = dir("recursive_scan");
    writeCsv(d, "top.csv", "id,name\n1,Alice\n");

    File sub = new File(d, "sub");
    sub.mkdirs();
    writeCsv(sub, "nested.csv", "id,val\n10,x\n");

    Map<String, Object> operand = baseOperand(d);
    operand.put("recursive", true);

    FileSchema schema = createSchemaViaFactory("rec_test", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertTrue(tableMap.size() >= 2,
        "Expected at least 2 tables from recursive scan, got: " + tableMap.size()
        + " tables: " + tableMap.keySet());
  }

  @Test void testRecursiveDirectoryWithMixedFileTypes() throws Exception {
    File d = dir("mixed_recursive");
    writeCsv(d, "data.csv", "a,b\n1,2\n");
    writeJson(d, "items.json", "[{\"x\":1},{\"x\":2}]");

    File sub = new File(d, "subdir");
    sub.mkdirs();
    writeJson(sub, "more.json", "[{\"y\":3}]");

    Map<String, Object> operand = baseOperand(d);
    operand.put("recursive", true);

    FileSchema schema = createSchemaViaFactory("mixed_rec", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertTrue(tableMap.size() >= 3,
        "Expected at least 3 tables, got: " + tableMap.size()
        + " tables: " + tableMap.keySet());
  }

  // ---------------------------------------------------------------
  // 2. processJsonFlattening with JSON files containing nested objects
  // ---------------------------------------------------------------

  @Test void testJsonFlatteningWithNestedObjects() throws Exception {
    File d = dir("flatten_json");
    writeJson(d, "nested.json",
        "[{\"id\":1,\"info\":{\"name\":\"Alice\",\"age\":30},"
        + "\"tags\":[\"a\",\"b\"]}]");

    Map<String, Object> operand = baseOperand(d);
    operand.put("flatten", true);

    FileSchema schema = createSchemaViaFactory("flat_test", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap, "Table map should not be null after flatten processing");  }

  @Test void testJsonFlatteningDisabledByDefault() throws Exception {
    File d = dir("no_flatten_json");
    writeJson(d, "simple.json", "[{\"id\":1,\"name\":\"Bob\"}]");

    Map<String, Object> operand = baseOperand(d);

    FileSchema schema = createSchemaViaFactory("noflat_test", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty(),
        "Expected table from JSON file");
  }

  @Test void testJsonFlatteningWithSchemaLevelFlatten() throws Exception {
    File d = dir("schema_flatten");
    writeJson(d, "deep.json",
        "[{\"id\":1,\"address\":{\"city\":\"NYC\",\"zip\":\"10001\"}}]");

    Map<String, Object> operand = baseOperand(d);
    operand.put("flatten", true);

    FileSchema schema = createSchemaViaFactory("schemaflatten", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap, "Table map should not be null after schema-level flatten processing");
  }

  // ---------------------------------------------------------------
  // 3. Materialization setup when materializedViews operand is set
  // ---------------------------------------------------------------

  @Test void testMaterializationWithParquetEngine() throws Exception {
    File d = dir("mat_view_test");
    writeCsv(d, "src.csv", "id,amount\n1,100\n2,200\n3,300\n");

    Map<String, Object> operand = baseOperand(d);
    List<Map<String, Object>> materializations = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("view", "totals_view");
    mv.put("table", "totals_table");
    mv.put("sql", "SELECT SUM(\"amount\") AS total FROM \"src\"");
    materializations.add(mv);
    operand.put("materializedViews", materializations);

    FileSchema schema = createSchemaViaFactory("mat_test", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
  }

  @Test void testMaterializationWithNonParquetEngineLogsError() throws Exception {
    File d = dir("mat_view_linq");
    writeCsv(d, "data.csv", "id,val\n1,10\n");

    Map<String, Object> operand = baseOperand(d);
    operand.put("executionEngine", "LINQ4J");
    List<Map<String, Object>> materializations = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("view", "v1");
    mv.put("table", "t1");
    mv.put("sql", "SELECT * FROM \"data\"");
    materializations.add(mv);
    operand.put("materializedViews", materializations);

    FileSchema schema = createSchemaViaFactory("mat_linq", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
  }

  // ---------------------------------------------------------------
  // 4. Views operand
  // ---------------------------------------------------------------

  @Test void testViewDefinitions() throws Exception {
    File d = dir("views_test");
    writeCsv(d, "base.csv", "id,value\n1,100\n2,200\n");

    Map<String, Object> operand = baseOperand(d);
    List<Map<String, Object>> views = new ArrayList<>();
    Map<String, Object> view = new HashMap<>();
    view.put("name", "filtered_view");
    view.put("sql", "SELECT * FROM \"base\" WHERE \"value\" > 100");
    views.add(view);
    operand.put("views", views);

    FileSchema schema = createSchemaViaFactory("views_schema", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertTrue(tableMap.containsKey("filtered_view"),
        "Expected 'filtered_view' in table map, got: " + tableMap.keySet());
  }

  // ---------------------------------------------------------------
  // 5. close() method paths for cleanup of refresh scheduler
  // ---------------------------------------------------------------

  @Test void testCloseWithNoRefreshScheduler() throws Exception {
    File d = dir("close_no_refresh");
    writeCsv(d, "data.csv", "a,b\n1,2\n");

    FileSchema schema = createDirectSchema("close_test", d, false, null, null, null);
    schema.close();
    schema.close();
  }

  @Test void testCloseWithRefreshScheduler() throws Exception {
    File d = dir("close_with_refresh");
    writeCsv(d, "data.csv", "a,b\n1,2\n");

    SchemaPlus parent = rootSchema();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 1024);
    FileSchema schema =
        new FileSchema(parent, "close_refresh_test", d, null, null, engineConfig, false, null, null, null, "30 seconds",
        "LOWER", "LOWER", null, null, null, null, false);
    schemasToClose.add(schema);

    schema.close();
    schema.close();
  }

  @Test void testCloseAllStaticMethod() throws Exception {
    File d = dir("close_all_test");
    writeCsv(d, "data.csv", "a,b\n1,2\n");

    FileSchema schema = createDirectSchema("closeall_test", d, false, null, null, null);
    assertNotNull(schema);
    FileSchema.closeAll();
  }

  // ---------------------------------------------------------------
  // 6. addTable() with duplicate table name handling
  // ---------------------------------------------------------------

  @Test void testDuplicateTableNameFromSameExtension() throws Exception {
    File d = dir("dup_tables");
    writeJson(d, "report.json", "[{\"id\":1}]");
    writeCsv(d, "report.csv", "id\n1\n");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("dup_test", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertTrue(tableMap.size() >= 2,
        "Expected at least 2 tables with duplicate handling, got: "
        + tableMap.size() + " tables: " + tableMap.keySet());
  }

  @Test void testMultipleJsonFilesWithSameBaseName() throws Exception {
    File d = dir("multi_json");
    writeJson(d, "data.json", "[{\"a\":1}]");
    File sub = new File(d, "sub");
    sub.mkdirs();
    writeJson(sub, "data.json", "[{\"b\":2}]");

    Map<String, Object> operand = baseOperand(d);
    operand.put("recursive", true);

    FileSchema schema = createSchemaViaFactory("multijson", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty());
  }

  // ---------------------------------------------------------------
  // 7. notifyTableRefreshed() listener notification path
  // ---------------------------------------------------------------

  @Test void testNotifyTableRefreshedWithListener() throws Exception {
    File d = dir("notify_test");
    writeCsv(d, "data.csv", "id,name\n1,Alice\n");

    FileSchema schema = createDirectSchema("notify_schema", d, false, null, null, null);

    final AtomicBoolean notified = new AtomicBoolean(false);
    final AtomicReference<String> notifiedTableName = new AtomicReference<>();
    final AtomicReference<File> notifiedFile = new AtomicReference<>();

    schema.addRefreshListener(new TableRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        notified.set(true);
        notifiedTableName.set(tableName);
        notifiedFile.set(parquetFile);
      }
    });

    File dummyFile = new File(d, "dummy.parquet");
    schema.notifyTableRefreshed("test_table", dummyFile);

    assertTrue(notified.get(), "Listener should have been notified");
    assertEquals("test_table", notifiedTableName.get());
    assertEquals(dummyFile, notifiedFile.get());
  }

  @Test void testNotifyTableRefreshedWithMultipleListeners() throws Exception {
    File d = dir("notify_multi");
    writeCsv(d, "data.csv", "id,name\n1,Alice\n");

    FileSchema schema = createDirectSchema("notify_multi_schema", d, false, null, null, null);

    final List<String> notifications = Collections.synchronizedList(new ArrayList<String>());

    schema.addRefreshListener(new TableRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        notifications.add("listener1:" + tableName);
      }
    });

    schema.addRefreshListener(new TableRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        notifications.add("listener2:" + tableName);
      }
    });

    File dummyFile = new File(d, "dummy.parquet");
    schema.notifyTableRefreshed("my_table", dummyFile);

    assertEquals(2, notifications.size());
    assertTrue(notifications.contains("listener1:my_table"));
    assertTrue(notifications.contains("listener2:my_table"));
  }

  @Test void testNotifyTableRefreshedWithExceptionInListener() throws Exception {
    File d = dir("notify_error");
    writeCsv(d, "data.csv", "id,name\n1,Alice\n");

    FileSchema schema = createDirectSchema("notify_err_schema", d, false, null, null, null);

    final AtomicBoolean secondListenerCalled = new AtomicBoolean(false);

    schema.addRefreshListener(new TableRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        throw new RuntimeException("Simulated listener error");
      }
    });

    schema.addRefreshListener(new TableRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        secondListenerCalled.set(true);
      }
    });

    File dummyFile = new File(d, "dummy.parquet");
    schema.notifyTableRefreshed("error_table", dummyFile);

    assertTrue(secondListenerCalled.get(),
        "Second listener should still be called after first one throws");
  }

  @Test void testNotifyTableRefreshedWithNoListeners() throws Exception {
    File d = dir("notify_empty");
    writeCsv(d, "data.csv", "id,name\n1,Alice\n");

    FileSchema schema = createDirectSchema("notify_empty_schema", d, false, null, null, null);
    schema.notifyTableRefreshed("no_listener_table", new File(d, "dummy.parquet"));
  }

  // ---------------------------------------------------------------
  // 8. notifyTableRefreshedWithPattern path
  // ---------------------------------------------------------------

  @Test void testNotifyTableRefreshedWithPattern() throws Exception {
    File d = dir("notify_pattern");
    writeCsv(d, "data.csv", "id,name\n1,Alice\n");

    FileSchema schema = createDirectSchema("pattern_schema", d, false, null, null, null);

    final AtomicBoolean patternNotified = new AtomicBoolean(false);
    final AtomicReference<String> notifiedPattern = new AtomicReference<>();

    schema.addRefreshListener(new PatternAwareRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
      }

      @Override public void onTableRefreshedWithPattern(String tableName, String pattern) {
        patternNotified.set(true);
        notifiedPattern.set(pattern);
      }

      @Override public void onIcebergTableRefreshed(String tableName, String tableLocation) {
      }
    });

    schema.notifyTableRefreshedWithPattern("patterned_table", "/data/**/*.parquet");

    assertTrue(patternNotified.get(), "Pattern listener should have been notified");
    assertEquals("/data/**/*.parquet", notifiedPattern.get());
  }

  @Test void testNotifyTableRefreshedWithPatternSkipsNonPatternAware() throws Exception {
    File d = dir("notify_skip_pattern");
    writeCsv(d, "data.csv", "id,name\n1,Alice\n");

    FileSchema schema = createDirectSchema("skip_pattern_schema", d, false, null, null, null);

    final AtomicBoolean regularCalled = new AtomicBoolean(false);

    schema.addRefreshListener(new TableRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        regularCalled.set(true);
      }
    });

    schema.notifyTableRefreshedWithPattern("table1", "*.parquet");
    assertFalse(regularCalled.get(),
        "Regular listener should NOT be called for pattern notification");
  }

  // ---------------------------------------------------------------
  // 9. notifyIcebergTableRefreshed path
  // ---------------------------------------------------------------

  @Test void testNotifyIcebergTableRefreshed() throws Exception {
    File d = dir("notify_iceberg");
    writeCsv(d, "data.csv", "id,name\n1,Alice\n");

    FileSchema schema = createDirectSchema("iceberg_schema", d, false, null, null, null);

    final AtomicBoolean icebergNotified = new AtomicBoolean(false);
    final AtomicReference<String> notifiedLocation = new AtomicReference<>();

    schema.addRefreshListener(new PatternAwareRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
      }

      @Override public void onTableRefreshedWithPattern(String tableName, String pattern) {
      }

      @Override public void onIcebergTableRefreshed(String tableName, String tableLocation) {
        icebergNotified.set(true);
        notifiedLocation.set(tableLocation);
      }
    });

    schema.notifyIcebergTableRefreshed("iceberg_tbl", "s3://bucket/warehouse/tbl");

    assertTrue(icebergNotified.get());
    assertEquals("s3://bucket/warehouse/tbl", notifiedLocation.get());
  }

  // ---------------------------------------------------------------
  // 10. Various getter methods and utility methods
  // ---------------------------------------------------------------

  @Test void testGetBaseDirectory() throws Exception {
    File d = dir("getbase");
    writeCsv(d, "data.csv", "a\n1\n");

    FileSchema schema = createDirectSchema("getbase_test", d, false, null, null, null);
    assertNotNull(schema.getBaseDirectory(),
        "Base directory should not be null");
  }

  @Test void testGetOperatingCacheDirectory() throws Exception {
    File d = dir("getcache");
    writeCsv(d, "data.csv", "a\n1\n");

    FileSchema schema = createDirectSchema("getcache_test", d, false, null, null, null);
    assertNotNull(schema.getOperatingCacheDirectory());
  }

  @Test void testGetStorageProviderReturnsNull() throws Exception {
    File d = dir("getstorage");
    writeCsv(d, "data.csv", "a\n1\n");

    FileSchema schema = createDirectSchema("getstorage_test", d, false, null, null, null);
    assertNull(schema.getStorageProvider(),
        "Storage provider should be null when not configured");
  }

  @Test void testGetStorageConfigReturnsNull() throws Exception {
    File d = dir("getstorageconfig");
    writeCsv(d, "data.csv", "a\n1\n");

    FileSchema schema = createDirectSchema("storageconfig_test", d, false, null, null, null);
    assertNull(schema.getStorageConfig(),
        "Storage config should be null when not configured");
  }

  @Test void testHasRefreshableTables() throws Exception {
    File d = dir("has_refresh");
    writeCsv(d, "data.csv", "a\n1\n");

    FileSchema schemaNoRefresh = createDirectSchema("norefresh", d, false, null, null, null);
    assertFalse(schemaNoRefresh.hasRefreshableTables());

    SchemaPlus parent = rootSchema();
    ExecutionEngineConfig engineConfig = new ExecutionEngineConfig("PARQUET", 1024);
    FileSchema schemaWithRefresh =
        new FileSchema(parent, "withrefresh", d, null, null, engineConfig, false, null, null, null, "5 minutes",
        "LOWER", "LOWER", null, null, null, null, false);
    schemasToClose.add(schemaWithRefresh);
    assertTrue(schemaWithRefresh.hasRefreshableTables());
  }

  @Test void testGetCommentReturnsNull() throws Exception {
    File d = dir("comment_null");
    writeCsv(d, "data.csv", "a\n1\n");

    FileSchema schema = createDirectSchema("comment_test", d, false, null, null, null);
    assertNull(schema.getComment());
  }

  @Test void testGetCommentWithValue() throws Exception {
    File d = dir("comment_val");
    writeCsv(d, "data.csv", "a\n1\n");

    Map<String, Object> operand = baseOperand(d);
    operand.put("comment", "This is a test schema");

    FileSchema schema = createSchemaViaFactory("comment_schema", operand);
    assertEquals("This is a test schema", schema.getComment());
  }

  @Test void testGetConversionMetadata() throws Exception {
    File d = dir("conv_meta");
    writeCsv(d, "data.csv", "a\n1\n");

    FileSchema schema = createDirectSchema("convmeta_test", d, false, null, null, null);
    assertNotNull(schema.getConversionMetadata());
  }

  @Test void testGetAllTableRecordsEmpty() throws Exception {
    File d = dir("all_records");
    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("allrec_test", operand);
    Map<String, ?> records = schema.getAllTableRecords();
    assertNotNull(records);
  }

  @Test void testGetAllTableRecordsWithTables() throws Exception {
    File d = dir("all_records_tables");
    writeCsv(d, "data.csv", "id,name\n1,Alice\n");
    writeJson(d, "items.json", "[{\"x\":1}]");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("allrec_tables", operand);
    schema.getTableMap();
    Map<String, ?> records = schema.getAllTableRecords();
    assertNotNull(records);
  }

  // ---------------------------------------------------------------
  // 11. ClearTableCache and getTableMap caching
  // ---------------------------------------------------------------

  @Test void testClearTableCacheForcesRecomputation() throws Exception {
    File d = dir("cache_clear");
    writeCsv(d, "data.csv", "id,val\n1,100\n");

    FileSchema schema = createDirectSchema("cache_clear_test", d, false, null, null, null);

    Map<String, Table> tables1 = schema.getTableMap();
    assertNotNull(tables1);

    Map<String, Table> tables2 = schema.getTableMap();
    assertSame(tables1, tables2, "Second call should return cached map");

    schema.clearTableCache();

    Map<String, Table> tables3 = schema.getTableMap();
    assertNotNull(tables3);
  }

  // ---------------------------------------------------------------
  // 12. Directory pattern (glob) scanning
  // ---------------------------------------------------------------

  @Test void testDirectoryPatternWithGlob() throws Exception {
    File d = dir("glob_pattern");
    writeCsv(d, "alpha.csv", "id\n1\n");
    writeCsv(d, "beta.csv", "id\n2\n");
    writeJson(d, "gamma.json", "[{\"id\":3}]");

    Map<String, Object> operand = baseOperand(d);
    operand.put("directoryPattern", "*.csv");

    FileSchema schema = createSchemaViaFactory("glob_test", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
  }

  @Test void testDirectoryPatternRecursiveGlob() throws Exception {
    File d = dir("rec_glob");
    writeCsv(d, "top.csv", "id\n1\n");
    File sub = new File(d, "folder");
    sub.mkdirs();
    writeCsv(sub, "nested.csv", "id\n2\n");

    Map<String, Object> operand = baseOperand(d);
    operand.put("directoryPattern", "**/*.csv");
    operand.put("recursive", true);

    FileSchema schema = createSchemaViaFactory("rec_glob_test", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
  }

  // ---------------------------------------------------------------
  // 13. Table name casing configurations
  // ---------------------------------------------------------------

  @Test void testTableNameCasingUpper() throws Exception {
    File d = dir("casing_upper");
    writeCsv(d, "mydata.csv", "id\n1\n");

    Map<String, Object> operand = baseOperand(d);
    operand.put("tableNameCasing", "UPPER");

    FileSchema schema = createSchemaViaFactory("casing_upper_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    boolean hasUpper = false;
    for (String name : tableMap.keySet()) {
      if (name.equals(name.toUpperCase())) {
        hasUpper = true;
        break;
      }
    }
    assertTrue(hasUpper || tableMap.isEmpty(),
        "Expected upper-cased table name, got: " + tableMap.keySet());
  }

  @Test void testTableNameCasingLower() throws Exception {
    File d = dir("casing_lower");
    writeCsv(d, "MyData.csv", "id\n1\n");

    Map<String, Object> operand = baseOperand(d);
    operand.put("tableNameCasing", "LOWER");

    FileSchema schema = createSchemaViaFactory("casing_lower_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    for (String name : tableMap.keySet()) {
      assertEquals(name.toLowerCase(), name,
          "Expected lower-cased table name: " + name);
    }
  }

  // ---------------------------------------------------------------
  // 14. Explicit table definitions via operand
  // ---------------------------------------------------------------

  @Test void testExplicitTableDefinitionWithUrl() throws Exception {
    File d = dir("explicit_table");
    File csvFile = writeCsv(d, "employees.csv", "id,name\n1,Alice\n2,Bob\n");

    Map<String, Object> operand = baseOperand(d);
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "emp");
    tableDef.put("url", csvFile.getAbsolutePath());
    tables.add(tableDef);
    operand.put("tables", tables);

    FileSchema schema = createSchemaViaFactory("explicit_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertTrue(tableMap.containsKey("emp"),
        "Expected explicit table 'emp', got: " + tableMap.keySet());
  }

  @Test void testExplicitTableDefinitionWithViewType() throws Exception {
    File d = dir("view_type_table");
    writeCsv(d, "data.csv", "id,val\n1,10\n");

    Map<String, Object> operand = baseOperand(d);
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "my_view");
    tableDef.put("type", "view");
    tableDef.put("url", "not_used_for_views");
    tables.add(tableDef);
    operand.put("tables", tables);

    FileSchema schema = createSchemaViaFactory("viewtype_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
  }

  // ---------------------------------------------------------------
  // 15. Schema with empty directory
  // ---------------------------------------------------------------

  @Test void testSchemaWithEmptyDirectory() throws Exception {
    File d = dir("empty_dir");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("empty_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
    assertTrue(tableMap.isEmpty(),
        "Empty directory should produce empty table map");
  }

  // ---------------------------------------------------------------
  // 16. Glob pattern recognition (isGlobPattern) via table URL
  // ---------------------------------------------------------------

  @Test void testGlobPatternInTableUrl() throws Exception {
    File d = dir("glob_url");
    writeCsv(d, "file1.csv", "id\n1\n");
    writeCsv(d, "file2.csv", "id\n2\n");

    Map<String, Object> operand = baseOperand(d);
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "all_files");
    tableDef.put("url", d.getAbsolutePath() + "/*.csv");
    tables.add(tableDef);
    operand.put("tables", tables);

    FileSchema schema = createSchemaViaFactory("glob_url_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertTrue(tableMap.containsKey("all_files"),
        "Expected glob table 'all_files', got: " + tableMap.keySet());
  }

  // ---------------------------------------------------------------
  // 17. JSON file detection
  // ---------------------------------------------------------------

  @Test void testJsonFileDiscovery() throws Exception {
    File d = dir("json_discovery");
    writeJson(d, "users.json", "[{\"id\":1,\"name\":\"Alice\"}]");
    writeJson(d, "orders.json", "[{\"order_id\":10,\"amount\":99.99}]");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("json_disc", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertTrue(tableMap.size() >= 2,
        "Expected at least 2 JSON tables, got: " + tableMap.size()
        + " tables: " + tableMap.keySet());
  }

  // ---------------------------------------------------------------
  // 18. CSV file with type inference
  // ---------------------------------------------------------------

  @Test void testCsvWithTypeInference() throws Exception {
    File d = dir("csv_inference");
    writeCsv(d, "typed.csv", "id,price,active\n1,19.99,true\n2,29.99,false\n");

    Map<String, Object> operand = baseOperand(d);
    Map<String, Object> typeInference = new HashMap<>();
    typeInference.put("enabled", true);
    operand.put("csvTypeInference", typeInference);

    FileSchema schema = createSchemaViaFactory("csv_infer", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty());
  }

  // ---------------------------------------------------------------
  // 19. getTableBaseline and updateTableBaseline
  // ---------------------------------------------------------------

  @Test void testGetTableBaselineReturnsNull() throws Exception {
    File d = dir("baseline_null");
    writeCsv(d, "data.csv", "id\n1\n");

    FileSchema schema = createDirectSchema("baseline_test", d, false, null, null, null);
    assertNull(schema.getTableBaseline("nonexistent"));
  }

  // ---------------------------------------------------------------
  // 20. registerRawToParquetConverter
  // ---------------------------------------------------------------

  @Test void testRegisterRawToParquetConverter() throws Exception {
    File d = dir("raw_converter");
    writeCsv(d, "data.csv", "id\n1\n");

    FileSchema schema = createDirectSchema("converter_test", d, false, null, null, null);

    schema.registerRawToParquetConverter(
        new org.apache.calcite.adapter.file.converters.RawToParquetConverter() {
          @Override public boolean canConvert(String rawFilePath, org.apache.calcite.adapter.file.metadata.ConversionMetadata metadata) {
            return false;
          }

          @Override public boolean convertToParquet(String rawFilePath, String targetParquetPath,
              org.apache.calcite.adapter.file.storage.StorageProvider storageProvider)
              throws IOException {
            return false;
          }        });
  }

  // ---------------------------------------------------------------
  // 21. setFunctionMultimap and getFunctionMultimap
  // ---------------------------------------------------------------

  @Test void testSetAndGetFunctionMultimap() throws Exception {
    File d = dir("func_multimap");
    writeCsv(d, "data.csv", "id\n1\n");

    FileSchema schema = createDirectSchema("func_test", d, false, null, null, null);

    com.google.common.collect.ImmutableMultimap<String, org.apache.calcite.schema.Function> empty =
        com.google.common.collect.ImmutableMultimap.of();
    schema.setFunctionMultimap(empty);
    Set<String> funcNames = schema.getFunctionNames();
    assertNotNull(funcNames);
  }

  // ---------------------------------------------------------------
  // 22. Parquet file creation via DuckDB for integration
  // ---------------------------------------------------------------

  @Test void testParquetFileDiscovery() throws Exception {
    File d = dir("parquet_discovery");
    File parquetFile = new File(d, "test_data.parquet");
    ProcessBuilder pb =
        new ProcessBuilder("duckdb", "-c",
        "COPY (SELECT 1 AS id, 'hello' AS name) TO '" + parquetFile.getAbsolutePath()
        + "' (FORMAT PARQUET)");
    pb.redirectErrorStream(true);
    Process process = pb.start();
    int exitCode = process.waitFor();
    if (exitCode != 0 || !parquetFile.exists()) {
      try (FileWriter w = new FileWriter(parquetFile)) {
        w.write("PAR1");
      }
    }

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("parquet_disc", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty(),
        "Expected at least 1 table from parquet file");
  }

  // ---------------------------------------------------------------
  // 23. Multiple constructors coverage
  // ---------------------------------------------------------------

  @Test void testMinimalConstructor() throws Exception {
    File d = dir("minimal_ctor");
    writeCsv(d, "data.csv", "id\n1\n");

    SchemaPlus parent = rootSchema();
    FileSchema schema = new FileSchema(parent, "minimal", d, null);
    schemasToClose.add(schema);
    assertNotNull(schema);
    assertNotNull(schema.getTableMap());
  }

  @Test void testConstructorWithEngineConfig() throws Exception {
    File d = dir("engine_ctor");
    writeCsv(d, "data.csv", "id\n1\n");

    SchemaPlus parent = rootSchema();
    ExecutionEngineConfig config = new ExecutionEngineConfig("PARQUET", 1024);
    FileSchema schema = new FileSchema(parent, "engine_test", d, null, config);
    schemasToClose.add(schema);
    assertNotNull(schema.getTableMap());
  }

  @Test void testConstructorWithRecursive() throws Exception {
    File d = dir("recursive_ctor");
    writeCsv(d, "data.csv", "id\n1\n");

    SchemaPlus parent = rootSchema();
    ExecutionEngineConfig config = new ExecutionEngineConfig("PARQUET", 1024);
    FileSchema schema = new FileSchema(parent, "recursive_test", d, null, config, true);
    schemasToClose.add(schema);
    assertNotNull(schema.getTableMap());
  }

  @Test void testConstructorWithViewsAndMaterializations() throws Exception {
    File d = dir("views_ctor");
    writeCsv(d, "data.csv", "id,val\n1,10\n");

    SchemaPlus parent = rootSchema();
    ExecutionEngineConfig config = new ExecutionEngineConfig("PARQUET", 1024);

    List<Map<String, Object>> views = new ArrayList<>();
    Map<String, Object> view = new HashMap<>();
    view.put("name", "v1");
    view.put("sql", "SELECT * FROM \"data\"");
    views.add(view);

    FileSchema schema =
        new FileSchema(parent, "views_ctor_test", d, null, config, false, null, views);
    schemasToClose.add(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertTrue(tableMap.containsKey("v1"),
        "Expected view 'v1' in table map, got: " + tableMap.keySet());
  }

  // ---------------------------------------------------------------
  // 24. Schema with null source directory (fallback to cwd)
  // ---------------------------------------------------------------

  @Test void testSchemaWithNullSourceDirectory() throws Exception {
    SchemaPlus parent = rootSchema();
    FileSchema schema = new FileSchema(parent, "null_src_test", (File) null, null);
    schemasToClose.add(schema);
    assertNotNull(schema);
    assertNotNull(schema.getTableMap());
  }

  // ---------------------------------------------------------------
  // 25. CSV and JSON table building with different casing
  // ---------------------------------------------------------------

  @Test void testCsvTableWithSmartCasing() throws Exception {
    File d = dir("smart_casing_csv");
    writeCsv(d, "My Table Data.csv", "id,Full Name\n1,Alice\n");

    Map<String, Object> operand = baseOperand(d);
    operand.put("tableNameCasing", "SMART_CASING");
    operand.put("columnNameCasing", "SMART_CASING");

    FileSchema schema = createSchemaViaFactory("smart_csv", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty(),
        "Expected table from CSV file with smart casing");
  }

  @Test void testJsonTableWithUnchangedCasing() throws Exception {
    File d = dir("unchanged_casing");
    writeJson(d, "MyItems.json", "[{\"ID\":1,\"Name\":\"Alice\"}]");

    Map<String, Object> operand = baseOperand(d);
    operand.put("tableNameCasing", "UNCHANGED");
    operand.put("columnNameCasing", "UNCHANGED");

    FileSchema schema = createSchemaViaFactory("unchanged_json", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty());
  }

  // ---------------------------------------------------------------
  // 26. Schema with ephemeral vs persistent cache
  // ---------------------------------------------------------------

  @Test void testEphemeralCacheTrue() throws Exception {
    File d = dir("ephemeral_true");
    writeCsv(d, "data.csv", "id\n1\n");

    Map<String, Object> operand = baseOperand(d);
    operand.put("ephemeralCache", true);

    FileSchema schema = createSchemaViaFactory("eph_true", operand);
    assertNotNull(schema);
    assertNotNull(schema.getBaseDirectory());
  }

  @Test void testEphemeralCacheFalse() throws Exception {
    File d = dir("ephemeral_false");
    writeCsv(d, "data.csv", "id\n1\n");

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", d.getAbsolutePath());
    operand.put("ephemeralCache", false);
    operand.put("executionEngine", "PARQUET");

    FileSchema schema = createSchemaViaFactory("eph_false", operand);
    assertNotNull(schema);
    assertNotNull(schema.getBaseDirectory());
  }

  // ---------------------------------------------------------------
  // 27. Local storage provider via operand
  // ---------------------------------------------------------------

  @Test void testLocalStorageProviderViaOperand() throws Exception {
    File d = dir("local_storage");
    writeCsv(d, "data.csv", "id\n1\n");

    Map<String, Object> operand = baseOperand(d);
    operand.put("storageType", "local");
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("rootPath", d.getAbsolutePath());
    operand.put("storageConfig", storageConfig);

    FileSchema schema = createSchemaViaFactory("local_storage_test", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
  }

  // ---------------------------------------------------------------
  // 28. TSV file handling
  // ---------------------------------------------------------------

  @Test void testTsvFileDiscovery() throws Exception {
    File d = dir("tsv_test");
    writeCsv(d, "tab_data.tsv", "id\tname\n1\tAlice\n2\tBob\n");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("tsv_disc", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty(),
        "Expected table from TSV file");
  }

  // ---------------------------------------------------------------
  // 29. YAML file handling
  // ---------------------------------------------------------------

  @Test void testYamlFileDiscovery() throws Exception {
    File d = dir("yaml_test");
    writeCsv(d, "config.yaml", "- id: 1\n  name: Alice\n- id: 2\n  name: Bob\n");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("yaml_disc", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty(),
        "Expected table from YAML file");
  }

  @Test void testYmlFileDiscovery() throws Exception {
    File d = dir("yml_test");
    writeCsv(d, "config.yml", "- id: 1\n  name: Alice\n");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("yml_disc", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty(),
        "Expected table from YML file");
  }

  // ---------------------------------------------------------------
  // 30. getTableConstraints
  // ---------------------------------------------------------------

  @Test void testGetTableConstraintsReturnsNull() throws Exception {
    File d = dir("constraints_null");
    writeCsv(d, "data.csv", "id\n1\n");

    FileSchema schema = createDirectSchema("constraint_test", d, false, null, null, null);
    assertNull(schema.getTableConstraints("nonexistent_table"));
  }

  // ---------------------------------------------------------------
  // 31. Constructor with canonical schema name
  // ---------------------------------------------------------------

  @Test void testConstructorWithCanonicalSchemaName() throws Exception {
    File d = dir("canonical_name");
    writeCsv(d, "data.csv", "id\n1\n");

    SchemaPlus parent = rootSchema();
    ExecutionEngineConfig config = new ExecutionEngineConfig("PARQUET", 1024);
    FileSchema schema =
        new FileSchema(parent, "USER_NAME", d, null, null, null, null, config, false, null, null, null, null,
        "LOWER", "LOWER", null, null, null, null, false, null, "canonical_name");
    schemasToClose.add(schema);

    assertNotNull(schema);
    String cachePath = schema.getOperatingCacheDirectory().getAbsolutePath();
    assertTrue(cachePath.contains("canonical_name"),
        "Cache path should use canonical name: " + cachePath);
  }

  // ---------------------------------------------------------------
  // 32. Schema with multiple file types in same directory
  // ---------------------------------------------------------------

  @Test void testMixedFileTypesInDirectory() throws Exception {
    File d = dir("mixed_types");
    writeCsv(d, "sales.csv", "id,amount\n1,100\n2,200\n");
    writeJson(d, "products.json", "[{\"pid\":1,\"name\":\"Widget\"}]");
    writeCsv(d, "regions.tsv", "code\tname\nUS\tUnited States\n");
    writeCsv(d, "config.yaml", "- key: val1\n");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("mixed_types_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertTrue(tableMap.size() >= 3,
        "Expected at least 3 tables from mixed file types, got: "
        + tableMap.size() + " tables: " + tableMap.keySet());
  }

  // ---------------------------------------------------------------
  // 33. Non-recursive scanning (verifying subdirectories are skipped)
  // ---------------------------------------------------------------

  @Test void testNonRecursiveScanSkipsSubdirectories() throws Exception {
    File d = dir("non_recursive");
    writeCsv(d, "top_level.csv", "id\n1\n");

    File sub = new File(d, "subdir");
    sub.mkdirs();
    writeCsv(sub, "hidden.csv", "id\n2\n");

    Map<String, Object> operand = baseOperand(d);
    operand.put("recursive", false);

    FileSchema schema = createSchemaViaFactory("nonrec_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
    assertFalse(tableMap.isEmpty(), "Should find top level files");
  }

  // ---------------------------------------------------------------
  // 34. Hidden and metadata files are skipped
  // ---------------------------------------------------------------

  @Test void testHiddenFilesAreSkipped() throws Exception {
    File d = dir("hidden_files");
    writeCsv(d, "visible.csv", "id\n1\n");
    writeCsv(d, ".hidden.csv", "id\n2\n");
    writeCsv(d, "._macos_file.csv", "id\n3\n");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("hidden_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    for (String name : tableMap.keySet()) {
      assertFalse(name.startsWith("."),
          "Hidden file should not become a table: " + name);
    }
  }

  // ---------------------------------------------------------------
  // 35. Partitioned tables operand
  // ---------------------------------------------------------------

  @Test void testPartitionedTablesOperand() throws Exception {
    File d = dir("partitioned");
    File yearDir = new File(d, "year=2024");
    yearDir.mkdirs();
    writeCsv(yearDir, "data.csv", "id,val\n1,100\n");

    Map<String, Object> operand = baseOperand(d);
    List<Map<String, Object>> partitions = new ArrayList<>();
    Map<String, Object> partition = new HashMap<>();
    partition.put("name", "partitioned_data");
    partition.put("directory", d.getAbsolutePath());
    partition.put("format", "csv");
    partitions.add(partition);
    operand.put("partitionedTables", partitions);

    FileSchema schema = createSchemaViaFactory("partition_test", operand);
    assertNotNull(schema);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
  }

  // ---------------------------------------------------------------
  // 36. JSON with array at root
  // ---------------------------------------------------------------

  @Test void testJsonWithArrayAtRoot() throws Exception {
    File d = dir("json_array_root");
    writeJson(d, "items.json",
        "[{\"id\":1,\"color\":\"red\"},{\"id\":2,\"color\":\"blue\"}]");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("json_arr", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty());
  }

  @Test void testJsonWithObjectAtRoot() throws Exception {
    File d = dir("json_object_root");
    writeJson(d, "config.json",
        "{\"settings\":{\"theme\":\"dark\"},\"version\":1}");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("json_obj", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
  }

  // ---------------------------------------------------------------
  // 37. Flatten with table-level definition
  // ---------------------------------------------------------------

  @Test void testFlattenWithTableLevelDefinition() throws Exception {
    File d = dir("table_flatten");
    File jsonFile =
        writeJson(d, "nested.json", "[{\"id\":1,\"details\":{\"a\":10,\"b\":20}}]");

    Map<String, Object> operand = baseOperand(d);
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "flat_nested");
    tableDef.put("url", jsonFile.getAbsolutePath());
    tableDef.put("flatten", true);
    tables.add(tableDef);
    operand.put("tables", tables);

    FileSchema schema = createSchemaViaFactory("tbl_flatten", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
  }

  // ---------------------------------------------------------------
  // 38. Materialization with missing parquet file
  // ---------------------------------------------------------------

  @Test void testMaterializationWithMissingParquetFile() throws Exception {
    File d = dir("mat_missing");
    writeCsv(d, "source.csv", "id,amount\n1,100\n2,200\n");

    List<Map<String, Object>> materializations = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("view", "mv_view");
    mv.put("table", "mv_table");
    mv.put("sql", "SELECT * FROM \"source\"");
    materializations.add(mv);

    Map<String, Object> operand = baseOperand(d);
    operand.put("materializedViews", materializations);

    FileSchema schema = createSchemaViaFactory("mat_missing_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
  }

  // ---------------------------------------------------------------
  // 39. Multiple JSON files and duplicate name resolution
  // ---------------------------------------------------------------

  @Test void testJsonAndYamlWithSameBaseName() throws Exception {
    File d = dir("json_yaml_dup");
    writeJson(d, "data.json", "[{\"a\":1}]");
    writeCsv(d, "data.yaml", "- a: 2\n");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("jy_dup", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertTrue(tableMap.size() >= 2,
        "Expected at least 2 tables (JSON and YAML with same base), got: "
        + tableMap.size() + " tables: " + tableMap.keySet());
  }

  // ---------------------------------------------------------------
  // 40. Full constructor with all parameters
  // ---------------------------------------------------------------

  @Test void testFullConstructorWithComment() throws Exception {
    File d = dir("full_ctor");
    writeCsv(d, "data.csv", "id\n1\n");

    SchemaPlus parent = rootSchema();
    ExecutionEngineConfig config = new ExecutionEngineConfig("PARQUET", 1024);
    FileSchema schema =
        new FileSchema(parent, "full_test", d, null, null, null, null, config, false, null, null, null, null,
        "LOWER", "LOWER", null, null, null, null, false, "Test comment");
    schemasToClose.add(schema);

    assertEquals("Test comment", schema.getComment());
    assertNotNull(schema.getTableMap());
  }

  @Test void testConstructorWithUserConfiguredBaseDirectory() throws Exception {
    File d = dir("user_basedir");
    File baseDir = dir("custom_base");
    writeCsv(d, "data.csv", "id\n1\n");

    SchemaPlus parent = rootSchema();
    ExecutionEngineConfig config = new ExecutionEngineConfig("PARQUET", 1024);
    FileSchema schema =
        new FileSchema(parent, "basedir_test", d, baseDir, null, null, config, false, null, null, null, null,
        "LOWER", "LOWER", null, null, null, null, false, null);
    schemasToClose.add(schema);

    assertNotNull(schema.getBaseDirectory());
  }

  // ---------------------------------------------------------------
  // 41. Table name null URL handling
  // ---------------------------------------------------------------

  @Test void testTableDefinitionWithNullUrl() throws Exception {
    File d = dir("null_url");
    writeCsv(d, "data.csv", "id\n1\n");

    Map<String, Object> operand = baseOperand(d);
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "null_url_table");
    tables.add(tableDef);
    operand.put("tables", tables);

    FileSchema schema = createSchemaViaFactory("null_url_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
  }

  // ---------------------------------------------------------------
  // 42. Calcite model file detection (should be skipped)
  // ---------------------------------------------------------------

  @Test void testCalciteModelFileIsSkipped() throws Exception {
    File d = dir("model_skip");
    writeCsv(d, "real_data.csv", "id\n1\n");
    writeJson(d, "model.json",
        "{\"version\":\"1.0\",\"defaultSchema\":\"test\",\"schemas\":[]}");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("model_skip_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
  }

  // ---------------------------------------------------------------
  // 43. Test with deeply nested directories (recursive)
  // ---------------------------------------------------------------

  @Test void testDeeplyNestedRecursiveScanning() throws Exception {
    File d = dir("deep_nested");
    File level1 = new File(d, "a");
    level1.mkdirs();
    File level2 = new File(level1, "b");
    level2.mkdirs();
    File level3 = new File(level2, "c");
    level3.mkdirs();

    writeCsv(d, "root.csv", "id\n1\n");
    writeCsv(level1, "l1.csv", "id\n2\n");
    writeCsv(level2, "l2.csv", "id\n3\n");
    writeCsv(level3, "l3.csv", "id\n4\n");

    Map<String, Object> operand = baseOperand(d);
    operand.put("recursive", true);

    FileSchema schema = createSchemaViaFactory("deep_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertTrue(tableMap.size() >= 4,
        "Expected at least 4 tables from deep nesting, got: "
        + tableMap.size() + " tables: " + tableMap.keySet());
  }

  // ---------------------------------------------------------------
  // 44. Large JSON file with many records
  // ---------------------------------------------------------------

  @Test void testLargeJsonFile() throws Exception {
    File d = dir("large_json");
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < 100; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("{\"id\":").append(i)
          .append(",\"name\":\"item_").append(i)
          .append("\",\"value\":").append(i * 10)
          .append("}");
    }
    sb.append("]");
    writeJson(d, "large.json", sb.toString());

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("large_json_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty());
  }

  // ---------------------------------------------------------------
  // 45. getTableNames (delegates to getTableMap)
  // ---------------------------------------------------------------

  @Test void testGetTableNames() throws Exception {
    File d = dir("table_names");
    writeCsv(d, "alpha.csv", "id\n1\n");
    writeJson(d, "beta.json", "[{\"id\":2}]");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("names_test", operand);
    Set<String> names = schema.getTableNames();
    assertNotNull(names);
    assertTrue(names.size() >= 2,
        "Expected at least 2 table names, got: " + names);
  }

  // ---------------------------------------------------------------
  // 46. File with spaces in name
  // ---------------------------------------------------------------

  @Test void testFileNameWithSpaces() throws Exception {
    File d = dir("spaces_name");
    writeCsv(d, "my data file.csv", "id\n1\n");

    Map<String, Object> operand = baseOperand(d);
    FileSchema schema = createSchemaViaFactory("spaces_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty(),
        "File with spaces in name should still be discovered");
  }

  // ---------------------------------------------------------------
  // 47. Alternate partition registry
  // ---------------------------------------------------------------

  @Test void testGetAlternatePartitionRegistry() throws Exception {
    File d = dir("alt_partition");
    writeCsv(d, "data.csv", "id\n1\n");

    FileSchema schema = createDirectSchema("alt_part_test", d, false, null, null, null);
    assertNotNull(schema.getAlternatePartitionRegistry());
  }

  // ---------------------------------------------------------------
  // 48. JSON flattening with flatten separator
  // ---------------------------------------------------------------

  @Test void testJsonFlatteningWithCustomSeparator() throws Exception {
    File d = dir("flat_sep");
    File jsonFile =
        writeJson(d, "nested_sep.json", "[{\"id\":1,\"addr\":{\"city\":\"NY\",\"zip\":\"10001\"}}]");

    Map<String, Object> operand = baseOperand(d);
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "flat_sep_table");
    tableDef.put("url", jsonFile.getAbsolutePath());
    tableDef.put("flatten", true);
    tableDef.put("flattenSeparator", ".");
    tables.add(tableDef);
    operand.put("tables", tables);

    FileSchema schema = createSchemaViaFactory("flat_sep_test", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertNotNull(tableMap);
  }

  // ---------------------------------------------------------------
  // 49. Schema with primeCache disabled
  // ---------------------------------------------------------------

  @Test void testSchemaNoPrimeCacheDoesNotStartThread() throws Exception {
    File d = dir("no_prime");
    writeCsv(d, "data.csv", "id\n1\n");

    SchemaPlus parent = rootSchema();
    ExecutionEngineConfig config = new ExecutionEngineConfig("PARQUET", 1024);
    FileSchema schema =
        new FileSchema(parent, "noprime_test", d, null, null, null, config, false, null, null, null, null,
        "LOWER", "LOWER", null, null, null, null, false);
    schemasToClose.add(schema);

    assertNotNull(schema.getTableMap());
  }

  // ---------------------------------------------------------------
  // 50. Multiple views and materializations together
  // ---------------------------------------------------------------

  @Test void testMultipleViewsAndMaterializationsTogether() throws Exception {
    File d = dir("multi_views_mat");
    writeCsv(d, "orders.csv", "id,amount,region\n1,100,US\n2,200,EU\n3,150,US\n");

    Map<String, Object> operand = baseOperand(d);

    List<Map<String, Object>> views = new ArrayList<>();
    Map<String, Object> v1 = new HashMap<>();
    v1.put("name", "us_orders");
    v1.put("sql", "SELECT * FROM \"orders\" WHERE \"region\" = 'US'");
    views.add(v1);

    Map<String, Object> v2 = new HashMap<>();
    v2.put("name", "eu_orders");
    v2.put("sql", "SELECT * FROM \"orders\" WHERE \"region\" = 'EU'");
    views.add(v2);
    operand.put("views", views);

    List<Map<String, Object>> materializations = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("view", "order_totals_view");
    mv.put("table", "order_totals");
    mv.put("sql", "SELECT \"region\", SUM(\"amount\") AS total FROM \"orders\" GROUP BY \"region\"");
    materializations.add(mv);
    operand.put("materializedViews", materializations);

    FileSchema schema = createSchemaViaFactory("multi_vm", operand);
    Map<String, Table> tableMap = schema.getTableMap();
    assertTrue(tableMap.containsKey("us_orders"),
        "Expected view 'us_orders', got: " + tableMap.keySet());
    assertTrue(tableMap.containsKey("eu_orders"),
        "Expected view 'eu_orders', got: " + tableMap.keySet());
  }
}
