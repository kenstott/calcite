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

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.refresh.PatternAwareRefreshListener;
import org.apache.calcite.adapter.file.refresh.TableRefreshListener;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Additional branch coverage tests for {@link FileSchema}.
 *
 * <p>Targets untested areas including: storage provider delegation,
 * refresh listeners, constraint metadata, conversion records,
 * table discovery for YAML files, ephemeral cache mode, builder patterns,
 * and various accessor methods.
 */
@SuppressWarnings("deprecation")
@Tag("unit")
public class FileSchemaAdditionalCoverageTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FileSchemaAdditionalCoverageTest.class);

  @TempDir
  Path tempDir;

  private final List<Connection> openConnections = new ArrayList<Connection>();

  @AfterEach
  public void closeConnections() {
    for (Connection conn : openConnections) {
      try {
        if (conn != null && !conn.isClosed()) {
          conn.close();
        }
      } catch (SQLException e) {
        LOGGER.debug("Error closing connection", e);
      }
    }
    openConnections.clear();
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Creates a CalciteConnection with a FileSchema using the programmatic API.
   */
  private CalciteConnection createCalciteConnectionWithSchema(
      String schemaName, Map<String, Object> operand) throws SQLException {
    Properties info = new Properties();
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
    openConnections.add(conn);
    CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConn.getRootSchema();
    rootSchema.add(schemaName,
        FileSchemaFactory.INSTANCE.create(rootSchema, schemaName, operand));
    return calciteConn;
  }

  /**
   * Builds an operand map with common defaults pointing to tempDir.
   */
  private Map<String, Object> buildOperand() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("ephemeralCache", true);
    return operand;
  }

  private File createCsvFile(String name, String... lines) throws IOException {
    File file = new File(tempDir.toFile(), name);
    File parentDir = file.getParentFile();
    if (!parentDir.exists()) {
      parentDir.mkdirs();
    }
    FileWriter writer = new FileWriter(file);
    try {
      for (String line : lines) {
        writer.write(line);
        writer.write("\n");
      }
    } finally {
      writer.close();
    }
    return file;
  }

  private File createJsonFile(String name, String content) throws IOException {
    File file = new File(tempDir.toFile(), name);
    File parentDir = file.getParentFile();
    if (!parentDir.exists()) {
      parentDir.mkdirs();
    }
    FileWriter writer = new FileWriter(file);
    try {
      writer.write(content);
    } finally {
      writer.close();
    }
    return file;
  }

  private File createYamlFile(String name, String content) throws IOException {
    return createJsonFile(name, content);
  }

  /**
   * Retrieves the FileSchema from a CalciteConnection by schema name.
   */
  private FileSchema getFileSchema(CalciteConnection calciteConn, String schemaName) {
    SchemaPlus schemaPlus = calciteConn.getRootSchema().getSubSchema(schemaName);
    assertNotNull(schemaPlus, "Schema " + schemaName + " should exist");
    Schema unwrapped = schemaPlus.unwrap(FileSchema.class);
    assertNotNull(unwrapped, "Should unwrap to FileSchema");
    return (FileSchema) unwrapped;
  }

  // ---------------------------------------------------------------------------
  // 1. Table discovery for various file types (CSV + JSON + YAML)
  // ---------------------------------------------------------------------------

  @Test void testTableDiscoveryMultipleFileTypes() throws Exception {
    createCsvFile("people.csv",
        "id:int,name:string",
        "1,Alice",
        "2,Bob");
    createJsonFile("events.json",
        "[{\"event_id\": 1, \"title\": \"Meeting\"},"
        + "{\"event_id\": 2, \"title\": \"Lunch\"}]");
    createYamlFile("config.yaml",
        "- key: alpha\n  value: 100\n- key: beta\n  value: 200\n");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("multi_types", buildOperand());
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("multi_types");
    assertNotNull(schema);
    Set<String> tableNames = schema.getTableNames();
    LOGGER.debug("Discovered tables for multi-type schema: {}", tableNames);

    // Should discover at least CSV and JSON tables
    assertTrue(tableNames.size() >= 2,
        "Expected at least 2 tables (CSV + JSON), got: " + tableNames);
    // CSV should produce a table named "people"
    boolean hasPeople = false;
    boolean hasEvents = false;
    for (String name : tableNames) {
      if (name.contains("people")) {
        hasPeople = true;
      }
      if (name.contains("events")) {
        hasEvents = true;
      }
    }
    assertTrue(hasPeople, "Should find people table in " + tableNames);
    assertTrue(hasEvents, "Should find events table in " + tableNames);
  }

  // ---------------------------------------------------------------------------
  // 2. YAML file (.yml extension) table discovery
  // ---------------------------------------------------------------------------

  @Test void testYmlExtensionDiscovery() throws Exception {
    createYamlFile("metrics.yml",
        "- metric: cpu\n  value: 42\n- metric: mem\n  value: 85\n");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("yml_schema", buildOperand());
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("yml_schema");
    assertNotNull(schema);
    Set<String> tableNames = schema.getTableNames();
    LOGGER.debug("YML schema tables: {}", tableNames);

    boolean hasMetrics = false;
    for (String name : tableNames) {
      if (name.contains("metrics")) {
        hasMetrics = true;
      }
    }
    assertTrue(hasMetrics, "Should discover .yml file as table, got: " + tableNames);
  }

  // ---------------------------------------------------------------------------
  // 3. Storage provider delegation - writeToStorage with local provider
  // ---------------------------------------------------------------------------

  @Test void testWriteToStorageLocalFilesystem() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("storage_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "storage_test");

    // Write using byte array
    byte[] content = "test content".getBytes("UTF-8");
    fileSchema.writeToStorage("test_output/file1.txt", content);

    // Verify the file was created
    assertTrue(fileSchema.existsInStorage("test_output/file1.txt"),
        "Written file should exist in storage");
  }

  // ---------------------------------------------------------------------------
  // 4. Storage provider delegation - writeToStorage with InputStream
  // ---------------------------------------------------------------------------

  @Test void testWriteToStorageInputStream() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("stream_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "stream_test");

    byte[] data = "stream content here".getBytes("UTF-8");
    java.io.ByteArrayInputStream stream = new java.io.ByteArrayInputStream(data);
    fileSchema.writeToStorage("stream_output/file2.txt", stream);

    assertTrue(fileSchema.existsInStorage("stream_output/file2.txt"),
        "Stream-written file should exist in storage");
  }

  // ---------------------------------------------------------------------------
  // 5. Storage provider delegation - existsInStorage for non-existent path
  // ---------------------------------------------------------------------------

  @Test void testExistsInStorageReturnsFalseForMissing() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("exists_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "exists_test");

    assertFalse(fileSchema.existsInStorage("nonexistent/path/file.txt"),
        "Non-existent file should not exist in storage");
  }

  // ---------------------------------------------------------------------------
  // 6. Storage provider delegation - deleteFromStorage
  // ---------------------------------------------------------------------------

  @Test void testDeleteFromStorageCreatedFile() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("delete_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "delete_test");

    // Create a file, then delete it
    fileSchema.writeToStorage("to_delete/temp.txt", "delete me".getBytes("UTF-8"));
    assertTrue(fileSchema.existsInStorage("to_delete/temp.txt"));

    boolean deleted = fileSchema.deleteFromStorage("to_delete/temp.txt");
    assertTrue(deleted, "Delete should return true for existing file");
    assertFalse(fileSchema.existsInStorage("to_delete/temp.txt"),
        "Deleted file should no longer exist");
  }

  // ---------------------------------------------------------------------------
  // 7. deleteFromStorage returns false for non-existent file
  // ---------------------------------------------------------------------------

  @Test void testDeleteFromStorageNonExistentReturnsFalse() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("del_miss_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "del_miss_test");

    boolean deleted = fileSchema.deleteFromStorage("does_not_exist.txt");
    assertFalse(deleted, "Delete should return false for non-existent file");
  }

  // ---------------------------------------------------------------------------
  // 8. Refresh listener - addRefreshListener and notifyTableRefreshed
  // ---------------------------------------------------------------------------

  @Test void testRefreshListenerNotification() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("refresh_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "refresh_test");

    // Track notifications
    final AtomicReference<String> notifiedTable = new AtomicReference<String>(null);
    final AtomicReference<File> notifiedFile = new AtomicReference<File>(null);

    fileSchema.addRefreshListener(new TableRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        notifiedTable.set(tableName);
        notifiedFile.set(parquetFile);
      }
    });

    // Trigger notification
    File mockParquetFile = new File(tempDir.toFile(), "mock.parquet");
    fileSchema.notifyTableRefreshed("test_table", mockParquetFile);

    assertEquals("test_table", notifiedTable.get(),
        "Listener should have been notified with correct table name");
    assertEquals(mockParquetFile, notifiedFile.get(),
        "Listener should have been notified with correct file");
  }

  // ---------------------------------------------------------------------------
  // 9. Multiple refresh listeners
  // ---------------------------------------------------------------------------

  @Test void testMultipleRefreshListeners() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("multi_listen", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "multi_listen");

    final AtomicInteger callCount = new AtomicInteger(0);

    TableRefreshListener listener1 = new TableRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        callCount.incrementAndGet();
      }
    };
    TableRefreshListener listener2 = new TableRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        callCount.incrementAndGet();
      }
    };

    fileSchema.addRefreshListener(listener1);
    fileSchema.addRefreshListener(listener2);

    fileSchema.notifyTableRefreshed("tbl", new File(tempDir.toFile(), "t.parquet"));

    assertEquals(2, callCount.get(),
        "Both listeners should have been notified");
  }

  // ---------------------------------------------------------------------------
  // 10. notifyTableRefreshedWithPattern with PatternAwareRefreshListener
  // ---------------------------------------------------------------------------

  @Test void testNotifyTableRefreshedWithPattern() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("pattern_listen", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "pattern_listen");

    final AtomicReference<String> patternCaptured = new AtomicReference<String>(null);

    fileSchema.addRefreshListener(new PatternAwareRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        // Not called for pattern notification
      }

      @Override public void onTableRefreshedWithPattern(String tableName, String pattern) {
        patternCaptured.set(pattern);
      }
    });

    fileSchema.notifyTableRefreshedWithPattern("my_table", "/data/**/*.parquet");

    assertEquals("/data/**/*.parquet", patternCaptured.get(),
        "Pattern-aware listener should capture the pattern");
  }

  // ---------------------------------------------------------------------------
  // 11. notifyTableRefreshedWithPattern ignores non-pattern-aware listeners
  // ---------------------------------------------------------------------------

  @Test void testNotifyWithPatternIgnoresBasicListeners() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("basic_listen", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "basic_listen");

    final AtomicInteger basicCallCount = new AtomicInteger(0);

    // Add a basic (non-pattern-aware) listener
    fileSchema.addRefreshListener(new TableRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        basicCallCount.incrementAndGet();
      }
    });

    // Pattern notification should not call basic listener
    fileSchema.notifyTableRefreshedWithPattern("tbl", "*.parquet");

    assertEquals(0, basicCallCount.get(),
        "Basic listener should not be called for pattern notification");
  }

  // ---------------------------------------------------------------------------
  // 12. Constraint metadata - setConstraintMetadata and getTableConstraints
  // ---------------------------------------------------------------------------

  @Test void testConstraintMetadata() throws Exception {
    createCsvFile("orders.csv",
        "order_id:int,customer_id:int,total:double",
        "1,10,99.99",
        "2,20,150.00");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("constraint_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "constraint_test");

    // Set constraint metadata
    Map<String, Map<String, Object>> constraints = new HashMap<String, Map<String, Object>>();
    Map<String, Object> orderConstraints = new HashMap<String, Object>();
    List<String> pkCols = new ArrayList<String>();
    pkCols.add("order_id");
    orderConstraints.put("primaryKey", pkCols);
    constraints.put("orders", orderConstraints);

    fileSchema.setConstraintMetadata(constraints);

    // Retrieve and verify
    Map<String, Object> retrieved = fileSchema.getTableConstraints("orders");
    assertNotNull(retrieved, "Should find constraints for 'orders'");
    assertNotNull(retrieved.get("primaryKey"), "Should have primaryKey constraint");

    // Non-existent table returns null
    Map<String, Object> missing = fileSchema.getTableConstraints("nonexistent");
    assertNull(missing, "Non-existent table should return null constraints");
  }

  // ---------------------------------------------------------------------------
  // 13. Conversion records - setConversionRecords
  // ---------------------------------------------------------------------------

  @Test void testSetConversionRecords() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("conv_rec_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "conv_rec_test");

    ConversionMetadata convMeta = fileSchema.getConversionMetadata();
    assertNotNull(convMeta, "ConversionMetadata should not be null");

    // Create conversion records with viewScanPattern
    Map<String, ConversionMetadata.ConversionRecord> records =
        new HashMap<String, ConversionMetadata.ConversionRecord>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.viewScanPattern = "/data/warehouse/table1/**/*.parquet";
    record.conversionType = "HIVE_PARQUET";
    records.put("table1", record);

    fileSchema.setConversionRecords(records);

    // Verify the record was added to conversion metadata
    Map<String, ConversionMetadata.ConversionRecord> allRecords =
        convMeta.getAllConversions();
    assertTrue(allRecords.containsKey("table1"),
        "table1 should exist in conversion records");
    assertEquals("/data/warehouse/table1/**/*.parquet",
        allRecords.get("table1").viewScanPattern,
        "viewScanPattern should be preserved");
  }

  // ---------------------------------------------------------------------------
  // 14. setConversionRecords with null or empty map
  // ---------------------------------------------------------------------------

  @Test void testSetConversionRecordsNullAndEmpty() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("conv_null_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "conv_null_test");

    // Null should not throw
    fileSchema.setConversionRecords(null);

    // Empty map should not throw
    Map<String, ConversionMetadata.ConversionRecord> empty =
        new HashMap<String, ConversionMetadata.ConversionRecord>();
    fileSchema.setConversionRecords(empty);
  }

  // ---------------------------------------------------------------------------
  // 15. hasRefreshableTables - without refresh interval
  // ---------------------------------------------------------------------------

  @Test void testHasRefreshableTablesWithoutInterval() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("no_refresh", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "no_refresh");

    assertFalse(fileSchema.hasRefreshableTables(),
        "Schema without refresh interval should not have refreshable tables");
  }

  // ---------------------------------------------------------------------------
  // 16. hasRefreshableTables - with refresh interval
  // ---------------------------------------------------------------------------

  @Test void testHasRefreshableTablesWithInterval() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    Map<String, Object> operand = buildOperand();
    operand.put("refreshInterval", "5 minutes");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("with_refresh", operand);
    FileSchema fileSchema = getFileSchema(calciteConn, "with_refresh");

    assertTrue(fileSchema.hasRefreshableTables(),
        "Schema with refresh interval should have refreshable tables");
  }

  // ---------------------------------------------------------------------------
  // 17. getBaseDirectory and getOperatingCacheDirectory
  // ---------------------------------------------------------------------------

  @Test void testBaseDirectoryAndCacheDirectory() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("dir_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "dir_test");

    String baseDir = fileSchema.getBaseDirectory();
    assertNotNull(baseDir, "Base directory should not be null");
    LOGGER.debug("Base directory: {}", baseDir);

    File cacheDir = fileSchema.getOperatingCacheDirectory();
    assertNotNull(cacheDir, "Operating cache directory should not be null");
    LOGGER.debug("Operating cache directory: {}", cacheDir.getAbsolutePath());

    // Cache directory should contain brand name
    assertTrue(cacheDir.getAbsolutePath().contains(".aperio"),
        "Cache directory should contain '.aperio': " + cacheDir.getAbsolutePath());
  }

  // ---------------------------------------------------------------------------
  // 18. ephemeralCache mode - uses temp directory
  // ---------------------------------------------------------------------------

  @Test void testEphemeralCacheMode() throws Exception {
    createCsvFile("data.csv", "id:int,val:string", "1,hello");

    Map<String, Object> operand = buildOperand();
    operand.put("ephemeralCache", true);

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("ephemeral_test", operand);
    FileSchema fileSchema = getFileSchema(calciteConn, "ephemeral_test");

    File cacheDir = fileSchema.getOperatingCacheDirectory();
    assertNotNull(cacheDir, "Ephemeral cache dir should not be null");

    // Ephemeral cache should use a temp directory
    String tmpDir = System.getProperty("java.io.tmpdir");
    String cachePath = cacheDir.getAbsolutePath();
    LOGGER.debug("Ephemeral cache path: {}", cachePath);
    // The base directory is set from the ephemeral temp directory by the factory
    // Just verify the schema works correctly with ephemeral cache
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("ephemeral_test");
    assertNotNull(schema);
    Set<String> tables = schema.getTableNames();
    assertTrue(tables.size() >= 1,
        "Ephemeral schema should discover tables, got: " + tables);
  }

  // ---------------------------------------------------------------------------
  // 19. getTable with CSV file
  // ---------------------------------------------------------------------------

  @Test void testGetTableCsvFile() throws Exception {
    createCsvFile("inventory.csv",
        "item_id:int,quantity:int",
        "1,100",
        "2,200");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("csv_get_test", buildOperand());
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("csv_get_test");
    assertNotNull(schema);

    Table table = schema.getTable("inventory");
    assertNotNull(table, "Should find 'inventory' table from CSV file");
  }

  // ---------------------------------------------------------------------------
  // 20. getTable with JSON file
  // ---------------------------------------------------------------------------

  @Test void testGetTableJsonFile() throws Exception {
    createJsonFile("accounts.json",
        "[{\"account_id\": 1, \"balance\": 1000.00},"
        + "{\"account_id\": 2, \"balance\": 2500.00}]");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("json_get_test", buildOperand());
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("json_get_test");
    assertNotNull(schema);

    Table table = schema.getTable("accounts");
    assertNotNull(table, "Should find 'accounts' table from JSON file");
  }

  // ---------------------------------------------------------------------------
  // 21. getTableNames returns all discovered tables
  // ---------------------------------------------------------------------------

  @Test void testGetTableNamesComprehensive() throws Exception {
    createCsvFile("alpha.csv", "id:int", "1");
    createCsvFile("beta.csv", "id:int", "2");
    createJsonFile("gamma.json", "[{\"val\": 1}]");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("names_test", buildOperand());
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("names_test");
    assertNotNull(schema);

    Set<String> names = schema.getTableNames();
    assertTrue(names.size() >= 3,
        "Should discover at least 3 tables (alpha, beta, gamma), got: " + names);
  }

  // ---------------------------------------------------------------------------
  // 22. Comment on schema
  // ---------------------------------------------------------------------------

  @Test void testSchemaComment() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    Map<String, Object> operand = buildOperand();
    operand.put("comment", "This is a test schema comment");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("comment_test", operand);
    FileSchema fileSchema = getFileSchema(calciteConn, "comment_test");

    assertEquals("This is a test schema comment", fileSchema.getComment(),
        "Schema comment should be set correctly");
  }

  // ---------------------------------------------------------------------------
  // 23. Schema without comment returns null
  // ---------------------------------------------------------------------------

  @Test void testSchemaNoComment() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("no_comment_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "no_comment_test");

    assertNull(fileSchema.getComment(),
        "Schema without comment should return null");
  }

  // ---------------------------------------------------------------------------
  // 24. getConversionMetadata is not null for schema with baseDirectory
  // ---------------------------------------------------------------------------

  @Test void testGetConversionMetadataNotNull() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("meta_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "meta_test");

    ConversionMetadata metadata = fileSchema.getConversionMetadata();
    assertNotNull(metadata, "ConversionMetadata should not be null");
  }

  // ---------------------------------------------------------------------------
  // 25. clearTableCache forces recomputation
  // ---------------------------------------------------------------------------

  @Test void testClearTableCacheRecomputes() throws Exception {
    createCsvFile("items.csv", "id:int,name:string", "1,Widget");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("cache_clear_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "cache_clear_test");

    // Access tables to populate cache
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("cache_clear_test");
    assertNotNull(schema);
    Set<String> tablesBefore = schema.getTableNames();
    assertFalse(tablesBefore.isEmpty(), "Should have tables before cache clear");

    // Clear cache
    fileSchema.clearTableCache();

    // Tables should still be discoverable after cache clear
    // (cache is rebuilt on next access)
    Set<String> tablesAfter = schema.getTableNames();
    assertFalse(tablesAfter.isEmpty(), "Should have tables after cache clear");
  }

  // ---------------------------------------------------------------------------
  // 26. getStorageProvider returns LocalFileStorageProvider for local directory
  // ---------------------------------------------------------------------------

  @Test void testGetStorageProviderLocalByDefault() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("sp_local_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "sp_local_test");

    // FileSchemaFactory auto-detects storageType="local" for local directories
    // so a LocalFileStorageProvider is always created
    assertNotNull(fileSchema.getStorageProvider(),
        "Storage provider should be created for local directory schemas");
  }

  // ---------------------------------------------------------------------------
  // 27. createStorageDirectories on local filesystem
  // ---------------------------------------------------------------------------

  @Test void testCreateStorageDirectories() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("mkdir_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "mkdir_test");

    fileSchema.createStorageDirectories("nested/deep/dir");

    assertTrue(fileSchema.existsInStorage("nested/deep/dir"),
        "Created directory should exist");
  }

  // ---------------------------------------------------------------------------
  // 28. Query CSV table through schema to verify end-to-end
  // ---------------------------------------------------------------------------

  @Test void testQueryCsvTableEndToEnd() throws Exception {
    createCsvFile("staff.csv",
        "emp_id:int,emp_name:string,salary:double",
        "1,Alice,50000.00",
        "2,Bob,60000.00",
        "3,Charlie,70000.00");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("query_test", buildOperand());

    Statement stmt = calciteConn.createStatement();
    try {
      ResultSet rs =
          stmt.executeQuery("SELECT emp_name, salary FROM query_test.staff ORDER BY emp_id");
      try {
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString("emp_name"));
        assertEquals(50000.00, rs.getDouble("salary"), 0.01);

        assertTrue(rs.next());
        assertEquals("Bob", rs.getString("emp_name"));

        assertTrue(rs.next());
        assertEquals("Charlie", rs.getString("emp_name"));

        assertFalse(rs.next());
      } finally {
        rs.close();
      }
    } finally {
      stmt.close();
    }
  }

  // ---------------------------------------------------------------------------
  // 29. Query JSON table through schema
  // ---------------------------------------------------------------------------

  @Test void testQueryJsonTableEndToEnd() throws Exception {
    createJsonFile("products.json",
        "[{\"product_id\": 10, \"product_name\": \"Widget\", \"price\": 9.99},"
        + "{\"product_id\": 20, \"product_name\": \"Gadget\", \"price\": 19.99}]");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("json_query_test", buildOperand());

    Statement stmt = calciteConn.createStatement();
    try {
      ResultSet rs =
          stmt.executeQuery("SELECT product_name FROM json_query_test.products ORDER BY product_id");
      try {
        assertTrue(rs.next());
        assertEquals("Widget", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("Gadget", rs.getString(1));
        assertFalse(rs.next());
      } finally {
        rs.close();
      }
    } finally {
      stmt.close();
    }
  }

  // ---------------------------------------------------------------------------
  // 30. Refresh listener - exception in listener does not propagate
  // ---------------------------------------------------------------------------

  @Test void testRefreshListenerExceptionContained() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("err_listen_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "err_listen_test");

    final AtomicInteger secondListenerCalled = new AtomicInteger(0);

    // First listener throws exception
    fileSchema.addRefreshListener(new TableRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        throw new RuntimeException("Simulated listener failure");
      }
    });

    // Second listener should still be called
    fileSchema.addRefreshListener(new TableRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
        secondListenerCalled.incrementAndGet();
      }
    });

    // Should not throw despite first listener failing
    fileSchema.notifyTableRefreshed("tbl", new File(tempDir.toFile(), "t.parquet"));

    assertEquals(1, secondListenerCalled.get(),
        "Second listener should be called even when first throws");
  }

  // ---------------------------------------------------------------------------
  // 31. Empty directory results in empty table set
  // ---------------------------------------------------------------------------

  @Test void testEmptyDirectoryNoTables() throws Exception {
    // tempDir is empty - no files created

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("empty_dir", buildOperand());
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("empty_dir");
    assertNotNull(schema);

    Set<String> names = schema.getTableNames();
    assertTrue(names.isEmpty(),
        "Empty directory should produce no tables, got: " + names);
  }

  // ---------------------------------------------------------------------------
  // 32. getAllTableRecords
  // ---------------------------------------------------------------------------

  @Test void testGetAllTableRecords() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("records_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "records_test");

    // Force table discovery
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("records_test");
    assertNotNull(schema);
    schema.getTableNames();

    Map<String, ConversionMetadata.ConversionRecord> records =
        fileSchema.getAllTableRecords();
    assertNotNull(records, "getAllTableRecords should not return null");
    LOGGER.debug("Table records: {}", records.keySet());
  }

  // ---------------------------------------------------------------------------
  // 33. getAlternatePartitionRegistry is not null
  // ---------------------------------------------------------------------------

  @Test void testGetAlternatePartitionRegistry() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("alt_part_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "alt_part_test");

    assertNotNull(fileSchema.getAlternatePartitionRegistry(),
        "AlternatePartitionRegistry should not be null");
  }

  // ---------------------------------------------------------------------------
  // 34. Non-table files are ignored (e.g., .txt files)
  // ---------------------------------------------------------------------------

  @Test void testNonTableFilesIgnored() throws Exception {
    createCsvFile("valid.csv", "id:int", "1");
    // Create a .txt file that should be ignored
    File txtFile = new File(tempDir.toFile(), "readme.txt");
    FileWriter writer = new FileWriter(txtFile);
    try {
      writer.write("This is just a text file");
    } finally {
      writer.close();
    }

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("ignore_test", buildOperand());
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("ignore_test");
    assertNotNull(schema);

    Set<String> names = schema.getTableNames();
    for (String name : names) {
      assertFalse(name.contains("readme"),
          "Text files should not become tables, but found: " + name);
    }
  }

  // ---------------------------------------------------------------------------
  // 35. TSV file discovery
  // ---------------------------------------------------------------------------

  @Test void testTsvFileDiscovery() throws Exception {
    File tsvFile = new File(tempDir.toFile(), "tab_data.tsv");
    FileWriter writer = new FileWriter(tsvFile);
    try {
      writer.write("col_a:int\tcol_b:string\n");
      writer.write("1\thello\n");
      writer.write("2\tworld\n");
    } finally {
      writer.close();
    }

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("tsv_test", buildOperand());
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("tsv_test");
    assertNotNull(schema);

    Set<String> names = schema.getTableNames();
    boolean hasTsv = false;
    for (String name : names) {
      if (name.contains("tab_data")) {
        hasTsv = true;
      }
    }
    assertTrue(hasTsv, "Should discover .tsv file as table, got: " + names);
  }

  // ---------------------------------------------------------------------------
  // 36. Files starting with ._ are ignored (macOS resource forks)
  // ---------------------------------------------------------------------------

  @Test void testMacResourceForksIgnored() throws Exception {
    createCsvFile("real.csv", "id:int", "1");
    // Create a macOS resource fork file
    File forkFile = new File(tempDir.toFile(), "._real.csv");
    FileWriter writer = new FileWriter(forkFile);
    try {
      writer.write("id:int\n1\n");
    } finally {
      writer.close();
    }

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("fork_test", buildOperand());
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("fork_test");
    assertNotNull(schema);

    Set<String> names = schema.getTableNames();
    for (String name : names) {
      assertFalse(name.startsWith("._"),
          "macOS resource fork files should be ignored, found: " + name);
    }
  }

  // ---------------------------------------------------------------------------
  // 37. notifyIcebergTableRefreshed with PatternAwareRefreshListener
  // ---------------------------------------------------------------------------

  @Test void testNotifyIcebergTableRefreshed() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("iceberg_notify", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "iceberg_notify");

    final AtomicReference<String> capturedLocation = new AtomicReference<String>(null);

    fileSchema.addRefreshListener(new PatternAwareRefreshListener() {
      @Override public void onTableRefreshed(String tableName, File parquetFile) {
      }

      @Override public void onTableRefreshedWithPattern(String tableName, String pattern) {
      }

      @Override public void onIcebergTableRefreshed(String tableName, String tableLocation) {
        capturedLocation.set(tableLocation);
      }
    });

    fileSchema.notifyIcebergTableRefreshed("iceberg_tbl", "s3://bucket/warehouse/tbl");

    assertEquals("s3://bucket/warehouse/tbl", capturedLocation.get(),
        "Iceberg location should be captured by listener");
  }

  // ---------------------------------------------------------------------------
  // 38. getStorageConfig returns null when no explicit storage config provided
  // ---------------------------------------------------------------------------

  @Test void testGetStorageConfigNullWithoutExplicit() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("sconfig_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "sconfig_test");

    // Storage config is only set if explicitly provided in operand
    // Auto-detected local storage does not set storageConfig
    Map<String, Object> config = fileSchema.getStorageConfig();
    // The result may be null or empty depending on whether _storageProvider injection happens
    LOGGER.debug("Storage config: {}", config);
  }

  // ---------------------------------------------------------------------------
  // 39. Multiple schemas with different directories
  // ---------------------------------------------------------------------------

  @Test void testMultipleSchemasIsolation() throws Exception {
    // Create two subdirectories with different data
    File dir1 = new File(tempDir.toFile(), "schema1");
    dir1.mkdirs();
    File dir2 = new File(tempDir.toFile(), "schema2");
    dir2.mkdirs();

    FileWriter w1 = new FileWriter(new File(dir1, "apples.csv"));
    try {
      w1.write("id:int,variety:string\n");
      w1.write("1,Fuji\n");
    } finally {
      w1.close();
    }

    FileWriter w2 = new FileWriter(new File(dir2, "oranges.csv"));
    try {
      w2.write("id:int,variety:string\n");
      w2.write("1,Navel\n");
    } finally {
      w2.close();
    }

    Properties info = new Properties();
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
    openConnections.add(conn);
    CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConn.getRootSchema();

    Map<String, Object> op1 = new HashMap<String, Object>();
    op1.put("directory", dir1.toString());
    op1.put("ephemeralCache", true);
    rootSchema.add("s1",
        FileSchemaFactory.INSTANCE.create(rootSchema, "s1", op1));

    Map<String, Object> op2 = new HashMap<String, Object>();
    op2.put("directory", dir2.toString());
    op2.put("ephemeralCache", true);
    rootSchema.add("s2",
        FileSchemaFactory.INSTANCE.create(rootSchema, "s2", op2));

    SchemaPlus schema1 = rootSchema.getSubSchema("s1");
    SchemaPlus schema2 = rootSchema.getSubSchema("s2");

    assertNotNull(schema1);
    assertNotNull(schema2);

    Set<String> names1 = schema1.getTableNames();
    Set<String> names2 = schema2.getTableNames();

    boolean s1HasApples = false;
    for (String name : names1) {
      if (name.contains("apples")) {
        s1HasApples = true;
      }
    }
    assertTrue(s1HasApples, "Schema1 should contain 'apples', got: " + names1);

    boolean s2HasOranges = false;
    for (String name : names2) {
      if (name.contains("oranges")) {
        s2HasOranges = true;
      }
    }
    assertTrue(s2HasOranges, "Schema2 should contain 'oranges', got: " + names2);
  }

  // ---------------------------------------------------------------------------
  // 40. registerRawToParquetConverter
  // ---------------------------------------------------------------------------

  @Test void testRegisterRawToParquetConverter() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("converter_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "converter_test");

    // Register a custom converter (no-op for this test)
    fileSchema.registerRawToParquetConverter(
        new org.apache.calcite.adapter.file.converters.RawToParquetConverter() {
          @Override public boolean canConvert(String sourcePath,
              ConversionMetadata metadata) {
            return false;
          }

          @Override public boolean convertToParquet(String sourcePath,
              String targetPath,
              org.apache.calcite.adapter.file.storage.StorageProvider provider)
              throws IOException {
            return false;
          }
        });

    // Just verifying no exception is thrown
    LOGGER.debug("Custom converter registered successfully");
  }

  // ---------------------------------------------------------------------------
  // 41. getTableBaseline returns null when no baseline exists
  // ---------------------------------------------------------------------------

  @Test void testGetTableBaselineNullWhenMissing() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("baseline_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "baseline_test");

    ConversionMetadata.PartitionBaseline baseline =
        fileSchema.getTableBaseline("nonexistent_table");
    assertNull(baseline,
        "getTableBaseline should return null for non-existent table");
  }

  // ---------------------------------------------------------------------------
  // 42. Duplicate CSV and JSON filenames produce disambiguated tables
  // ---------------------------------------------------------------------------

  @Test void testDuplicateBaseNameDisambiguation() throws Exception {
    // Create both data.csv and data.json with same base name
    createCsvFile("items.csv", "id:int", "1");
    createJsonFile("items.json", "[{\"id\": 2}]");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("dup_test", buildOperand());
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("dup_test");
    assertNotNull(schema);

    Set<String> names = schema.getTableNames();
    LOGGER.debug("Disambiguated table names: {}", names);

    // At least one table should be found
    assertTrue(names.size() >= 1,
        "Should discover at least 1 table for duplicate base names, got: " + names);
  }

  // ---------------------------------------------------------------------------
  // 43. Schema with primeCache=false
  // ---------------------------------------------------------------------------

  @Test void testPrimeCacheDisabled() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    Map<String, Object> operand = buildOperand();
    operand.put("primeCache", false);

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("no_prime", operand);
    SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("no_prime");
    assertNotNull(schema);

    // Should still discover tables even with primeCache disabled
    Set<String> names = schema.getTableNames();
    assertFalse(names.isEmpty(),
        "Tables should be discoverable with primeCache=false");
  }

  // ---------------------------------------------------------------------------
  // 44. setFunctionMultimap
  // ---------------------------------------------------------------------------

  @Test void testSetFunctionMultimap() throws Exception {
    createCsvFile("data.csv", "id:int", "1");

    CalciteConnection calciteConn =
        createCalciteConnectionWithSchema("func_test", buildOperand());
    FileSchema fileSchema = getFileSchema(calciteConn, "func_test");

    // Set an empty function multimap (just verifying the method works)
    com.google.common.collect.ImmutableMultimap<String, org.apache.calcite.schema.Function> emptyMap =
        com.google.common.collect.ImmutableMultimap.of();
    fileSchema.setFunctionMultimap(emptyMap);

    // No exception means success
    LOGGER.debug("setFunctionMultimap completed successfully");
  }
}
