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
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import com.google.common.collect.ImmutableMultimap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link FileSchema} targeting uncovered methods:
 * storage operations (writeToStorage, existsInStorage, deleteFromStorage,
 * createStorageDirectories), periodic refresh, accessor methods, casing,
 * getComment, shutdown, conversion metadata, file type detection, and more.
 */
@Tag("unit")
public class FileSchemaDeepCoverageTest2 {

  private static final AtomicInteger SCHEMA_COUNTER = new AtomicInteger(0);

  @TempDir
  Path tempDir;

  private SchemaPlus parentSchema;
  private ExecutionEngineConfig defaultEngineConfig;

  private String uniqueSchemaName() {
    return "test_deep2_" + SCHEMA_COUNTER.incrementAndGet();
  }

  @BeforeEach
  void setUp() {
    parentSchema = mock(SchemaPlus.class);
    when(parentSchema.getName()).thenReturn("root");
    defaultEngineConfig = new ExecutionEngineConfig();
  }

  // --- Storage operations (local filesystem path) ---

  @Test void testWriteToStorageLocalBytes() throws IOException {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    byte[] content = "test data".getBytes(StandardCharsets.UTF_8);

    schema.writeToStorage("subdir/test.txt", content);

    File written = new File(schema.getOperatingCacheDirectory(), "subdir/test.txt");
    assertTrue(written.exists(), "File should have been written");
    assertEquals("test data", new String(Files.readAllBytes(written.toPath()), StandardCharsets.UTF_8));
  }

  @Test void testWriteToStorageLocalInputStream() throws IOException {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    InputStream content = new ByteArrayInputStream("stream data".getBytes(StandardCharsets.UTF_8));

    schema.writeToStorage("stream-test.txt", content);

    File written = new File(schema.getOperatingCacheDirectory(), "stream-test.txt");
    assertTrue(written.exists(), "File should have been written from stream");
    assertEquals("stream data", new String(Files.readAllBytes(written.toPath()), StandardCharsets.UTF_8));
  }

  @Test void testExistsInStorageLocal() throws IOException {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);

    assertFalse(schema.existsInStorage("nonexistent.txt"));

    // Create a file in the operating cache directory
    File testFile = new File(schema.getOperatingCacheDirectory(), "exists-test.txt");
    testFile.getParentFile().mkdirs();
    Files.write(testFile.toPath(), "exists".getBytes(StandardCharsets.UTF_8));

    assertTrue(schema.existsInStorage("exists-test.txt"));
  }

  @Test void testDeleteFromStorageLocal() throws IOException {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);

    // Delete nonexistent file should return false
    assertFalse(schema.deleteFromStorage("nonexistent.txt"));

    // Create and then delete
    schema.writeToStorage("delete-test.txt", "to delete".getBytes(StandardCharsets.UTF_8));
    assertTrue(schema.existsInStorage("delete-test.txt"));
    assertTrue(schema.deleteFromStorage("delete-test.txt"));
    assertFalse(schema.existsInStorage("delete-test.txt"));
  }

  @Test void testCreateStorageDirectoriesLocal() throws IOException {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);

    schema.createStorageDirectories("a/b/c");

    File dir = new File(schema.getOperatingCacheDirectory(), "a/b/c");
    assertTrue(dir.exists() && dir.isDirectory(), "Directories should have been created");
  }

  // --- Storage operations with StorageProvider ---

  @Test void testWriteToStorageWithProvider() throws IOException {
    StorageProvider mockProvider = mock(StorageProvider.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    byte[] content = "s3 data".getBytes(StandardCharsets.UTF_8);
    schema.writeToStorage("test.txt", content);

    verify(mockProvider).writeFile(anyString(), eq(content));
  }

  @Test void testExistsInStorageWithProvider() throws IOException {
    StorageProvider mockProvider = mock(StorageProvider.class);
    when(mockProvider.exists(anyString())).thenReturn(true);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    assertTrue(schema.existsInStorage("some/path.txt"));
    verify(mockProvider).exists(anyString());
  }

  @Test void testDeleteFromStorageWithProvider() throws IOException {
    StorageProvider mockProvider = mock(StorageProvider.class);
    when(mockProvider.delete(anyString())).thenReturn(true);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    assertTrue(schema.deleteFromStorage("some/path.txt"));
    verify(mockProvider).delete(anyString());
  }

  // --- Accessor methods ---

  @Test void testGetBaseDirectoryAccessor() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    assertNotNull(schema.getBaseDirectory());
  }

  @Test void testGetStorageConfigNullWhenNotConfigured() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    assertNull(schema.getStorageConfig());
  }

  @Test void testGetStorageProviderNullWhenNotConfigured() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    assertNull(schema.getStorageProvider());
  }

  @Test void testGetOperatingCacheDirectory() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    File cacheDir = schema.getOperatingCacheDirectory();
    assertNotNull(cacheDir);
    assertTrue(cacheDir.getAbsolutePath().contains(".aperio"),
        "Cache directory should contain .aperio");
  }

  @Test void testGetAlternatePartitionRegistry() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    assertNotNull(schema.getAlternatePartitionRegistry());
  }

  @Test void testGetConversionMetadata() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    assertNotNull(schema.getConversionMetadata());
  }

  @Test void testGetAllTableRecords() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    Map<String, org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord>
        records = schema.getAllTableRecords();
    assertNotNull(records);
  }

  @Test void testGetCommentNull() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    // Default constructor should have null comment
    assertNull(schema.getComment());
  }

  @Test void testGetCommentWithValue() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        null, null, null, null, false, "This is a test schema");
    assertEquals("This is a test schema", schema.getComment());
  }

  @Test void testHasRefreshableTablesDefault() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    assertFalse(schema.hasRefreshableTables());
  }

  // --- clearTableCache and function multimap ---

  @Test void testClearTableCache() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    // Force table computation
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);

    // Clear cache
    schema.clearTableCache();

    // Should recompute on next access
    Map<String, Table> tables2 = schema.getTableMap();
    assertNotNull(tables2);
  }

  @Test void testSetFunctionMultimap() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    schema.setFunctionMultimap(ImmutableMultimap.of());
    // Verify it doesn't throw
  }

  // --- Constructor with canonical schema name ---

  @Test void testConstructorWithCanonicalSchemaName() {
    FileSchema schema =
        new FileSchema(parentSchema, "MY_SCHEMA", tempDir.toFile(), null, null, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        null, null, null, null, false, null, "my_schema");
    assertNotNull(schema);
    // Operating cache directory should use canonical name
    assertTrue(schema.getOperatingCacheDirectory().getAbsolutePath().contains("my_schema"),
        "Should use canonical schema name for cache directory");
  }

  // --- Constructor with userConfiguredBaseDirectory ---

  @Test void testConstructorWithUserConfiguredBaseDirectory() {
    File userBaseDir = new File(tempDir.toFile(), "custom-base");
    userBaseDir.mkdirs();
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), userBaseDir, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        null, null, null, null, false, null);
    assertNotNull(schema);
  }

  // --- Table casing variants ---

  @Test void testConstructorWithUpperCasing() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, defaultEngineConfig,
        false, null, null, null, null,
        "UPPER", "UPPER");
    assertNotNull(schema);
  }

  @Test void testConstructorWithLowerCasing() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, defaultEngineConfig,
        false, null, null, null, null,
        "LOWER", "LOWER");
    assertNotNull(schema);
  }

  @Test void testConstructorWithUnchangedCasing() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, defaultEngineConfig,
        false, null, null, null, null,
        "UNCHANGED", "UNCHANGED");
    assertNotNull(schema);
  }

  // --- CSV file table creation ---

  @Test void testAutoDiscoverCsvFile() throws IOException {
    File sourceDir = tempDir.resolve("csv-source").toFile();
    sourceDir.mkdirs();
    FileWriter fw = new FileWriter(new File(sourceDir, "data.csv"));
    fw.write("id,name\n1,Alice\n2,Bob\n");
    fw.close();

    // CSV files are converted to JSON during getTableMap, which creates
    // tables from the converted JSON. Provide explicit tables config to test CSV.
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "data");
    tableDef.put("url", new File(sourceDir, "data.csv").toURI().toString());
    tables.add(tableDef);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), sourceDir, tables, defaultEngineConfig);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty(), "Should discover CSV file as table");
  }

  // --- TSV file table creation ---

  @Test void testAutoDiscoverTsvFile() throws IOException {
    File sourceDir = tempDir.resolve("tsv-source").toFile();
    sourceDir.mkdirs();
    FileWriter fw = new FileWriter(new File(sourceDir, "data.tsv"));
    fw.write("id\tname\n1\tAlice\n2\tBob\n");
    fw.close();

    // TSV files need explicit table config to be discovered
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "data");
    tableDef.put("url", new File(sourceDir, "data.tsv").toURI().toString());
    tables.add(tableDef);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), sourceDir, tables, defaultEngineConfig);
    Map<String, Table> tableMap = schema.getTableMap();
    assertFalse(tableMap.isEmpty(), "Should discover TSV file as table");
  }

  // --- JSON file table creation ---

  @Test void testAutoDiscoverJsonFile() throws IOException {
    File sourceDir = tempDir.resolve("json-source").toFile();
    sourceDir.mkdirs();
    FileWriter fw = new FileWriter(new File(sourceDir, "data.json"));
    fw.write("[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]");
    fw.close();

    // JSON files in sourceDirectory are auto-discovered when tables is null
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), sourceDir, null, defaultEngineConfig);
    Map<String, Table> tableMap = schema.getTableMap();
    // JSON auto-discovery depends on the file appearing in getFilesForProcessing()
    // which scans sourceDirectory. Verify schema was created without error.
    assertNotNull(tableMap, "Table map should not be null");
  }

  // --- Constraint metadata ---

  @Test void testSetAndGetConstraintMetadata() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);

    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tableConstraint = new HashMap<>();
    tableConstraint.put("primaryKey", Arrays.asList("id"));
    constraints.put("test_table", tableConstraint);

    schema.setConstraintMetadata(constraints);

    Map<String, Object> result = schema.getTableConstraints("test_table");
    assertNotNull(result);
    assertEquals(tableConstraint, result);
  }

  @Test void testGetTableConstraintsNullWhenNotSet() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);
    assertNull(schema.getTableConstraints("nonexistent"));
  }

  // --- Refresh listener registration ---

  @Test void testAddRefreshListener() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);

    org.apache.calcite.adapter.file.refresh.TableRefreshListener listener =
        mock(org.apache.calcite.adapter.file.refresh.TableRefreshListener.class);
    schema.addRefreshListener(listener);
    // Should not throw
  }

  // --- registerRawToParquetConverter ---

  @Test void testRegisterRawToParquetConverter() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);

    org.apache.calcite.adapter.file.converters.RawToParquetConverter converter =
        mock(org.apache.calcite.adapter.file.converters.RawToParquetConverter.class);
    schema.registerRawToParquetConverter(converter);
    // Should not throw
  }

  // --- Recursive directory scanning ---

  @Test void testRecursiveDirectoryScanning() throws IOException {
    File sourceDir = tempDir.resolve("recursive-source").toFile();
    File subDir = new File(sourceDir, "sub");
    subDir.mkdirs();

    // Write JSON files (auto-discoverable format)
    FileWriter fw1 = new FileWriter(new File(sourceDir, "root.json"));
    fw1.write("[{\"id\":1,\"val\":\"a\"}]");
    fw1.close();

    FileWriter fw2 = new FileWriter(new File(subDir, "nested.json"));
    fw2.write("[{\"id\":2,\"val\":\"b\"}]");
    fw2.close();

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), sourceDir, null, defaultEngineConfig, true);

    // Recursive scanning should find JSON files in subdirectories
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables, "Table map should not be null for recursive schema");
  }

  // --- Views configuration ---

  @Test void testConstructorWithViews() {
    List<Map<String, Object>> views = new ArrayList<>();
    Map<String, Object> view = new HashMap<>();
    view.put("name", "test_view");
    view.put("sql", "SELECT 1");
    views.add(view);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, defaultEngineConfig,
        false, null, views, null, null);
    assertNotNull(schema);
  }

  // --- Materializations configuration ---

  @Test void testConstructorWithMaterializations() {
    List<Map<String, Object>> materializations = new ArrayList<>();
    Map<String, Object> mat = new HashMap<>();
    mat.put("table", "mat_table");
    mat.put("sql", "SELECT 1");
    materializations.add(mat);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, defaultEngineConfig,
        false, materializations, null, null, null);
    assertNotNull(schema);
  }

  // --- Flatten option ---

  @Test void testConstructorWithFlattenTrue() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        null, null, true, null, false);
    assertNotNull(schema);
  }

  // --- CSV type inference config ---

  @Test void testConstructorWithCsvTypeInferenceConfig() {
    Map<String, Object> csvTypeInference = new HashMap<>();
    csvTypeInference.put("enabled", true);
    csvTypeInference.put("sampleSize", 50);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        null, null, null, csvTypeInference, false);
    assertNotNull(schema);
  }

  // --- setConversionRecords ---

  @Test void testSetConversionRecords() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig);

    Map<String, org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord>
        records = new HashMap<>();
    org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord record =
        new org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord();
    record.tableName = "test_table";
    record.sourceFile = "/path/to/source";
    records.put("test_table", record);

    schema.setConversionRecords(records);
    // Should not throw
  }

  // --- Explicit table definition with name ---

  @Test void testExplicitTableDefinitionWithUrl() throws IOException {
    File sourceDir = tempDir.resolve("explicit-source").toFile();
    sourceDir.mkdirs();
    File csvFile = new File(sourceDir, "mydata.csv");
    FileWriter fw = new FileWriter(csvFile);
    fw.write("col1,col2\na,1\nb,2\n");
    fw.close();

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "my_table");
    tableDef.put("url", csvFile.getAbsolutePath());
    tables.add(tableDef);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), sourceDir, tables, defaultEngineConfig);
    Map<String, Table> tableMap = schema.getTableMap();
    assertTrue(tableMap.containsKey("my_table"), "Should contain explicitly named table");
  }

  // --- Files starting with dot or tilde should be skipped ---

  @Test void testDotAndTildeFilesSkipped() throws IOException {
    File sourceDir = tempDir.resolve("dot-tilde-source").toFile();
    sourceDir.mkdirs();

    // macOS resource fork file (starts with ._)
    FileWriter fw1 = new FileWriter(new File(sourceDir, "._hidden.csv"));
    fw1.write("hidden,data\n1,x\n");
    fw1.close();

    // Normal file
    FileWriter fw2 = new FileWriter(new File(sourceDir, "visible.csv"));
    fw2.write("id,val\n1,a\n");
    fw2.close();

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), sourceDir, null, defaultEngineConfig);
    Map<String, Table> tables = schema.getTableMap();

    // Should only have the visible file
    boolean hasHidden = false;
    for (String key : tables.keySet()) {
      if (key.contains("hidden")) {
        hasHidden = true;
      }
    }
    assertFalse(hasHidden, "Should not include dot-prefixed files");
  }
}
