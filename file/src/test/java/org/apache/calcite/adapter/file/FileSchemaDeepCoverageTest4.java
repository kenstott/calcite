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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.refresh.PatternAwareRefreshListener;
import org.apache.calcite.adapter.file.refresh.RefreshableParquetCacheTable;
import org.apache.calcite.adapter.file.refresh.TableRefreshListener;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests for {@link FileSchema} - Round 4.
 * Targets uncovered lines not exercised by Tests 1-3:
 * constructor branches (canonicalSchemaName, ephemeral base, directoryPath,
 * storageType with directoryPath/sourceDirectory), getTableName(),
 * trim()/trimOrNull(), applyCasing(), buildConvertibleFilesGlobPattern(),
 * buildConvertibleFilesGlobPatternRecursive(), getFilesInDir() scanning,
 * findJsonFiles(), createTableFromSource() format branches,
 * createEnhancedCsvTable() engine type branches,
 * createEnhancedJsonTable() engine branches, addTable(tableDef) view type,
 * resolveSource() relative/absolute, format override branches in addTable,
 * shouldUseLazyInitialization() branches, validateForeignKeyConstraints()
 * branches, checkTableExists(), getTableBaseline(), updateTableBaseline(),
 * setConstraintMetadata(), setConversionRecords(), getAllTableRecords(),
 * notifyIcebergTableRefreshed(), notifyTableRefreshedWithPattern(),
 * resolvePath() branches, readAllBytes(), refreshAllTables(),
 * extractFieldConfigurations(), setFunctionMultimap(), generateModelFile(),
 * and various public accessor coverage.
 */
@SuppressWarnings("deprecation")
@Tag("unit")
public class FileSchemaDeepCoverageTest4 {

  private static final AtomicInteger SCHEMA_COUNTER = new AtomicInteger(0);

  @TempDir
  Path tempDir;

  private SchemaPlus parentSchema;
  private ExecutionEngineConfig defaultEngineConfig;

  private String uniqueSchemaName() {
    return "test_deep4_" + SCHEMA_COUNTER.incrementAndGet();
  }

  @BeforeEach
  void setUp() {
    parentSchema = mock(SchemaPlus.class);
    when(parentSchema.getName()).thenReturn("root");
    defaultEngineConfig = new ExecutionEngineConfig();
  }

  /**
   * Helper: create a FileSchema with source directory.
   */
  private FileSchema createSchema(File sourceDir) {
    return new FileSchema(parentSchema, uniqueSchemaName(),
        sourceDir, null, defaultEngineConfig);
  }

  /**
   * Helper: create a FileSchema with source directory and tables.
   */
  private FileSchema createSchemaWithTables(File sourceDir,
      List<Map<String, Object>> tables) {
    return new FileSchema(parentSchema, uniqueSchemaName(),
        sourceDir, tables, defaultEngineConfig);
  }

  /**
   * Helper: invoke a private method on FileSchema via reflection.
   */
  private Object invokePrivate(FileSchema schema, String methodName,
      Class<?>[] paramTypes, Object... args) throws Exception {
    Method method = FileSchema.class.getDeclaredMethod(methodName, paramTypes);
    method.setAccessible(true);
    try {
      return method.invoke(schema, args);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof Exception) {
        throw (Exception) e.getCause();
      }
      throw e;
    }
  }

  /**
   * Helper: get a private field value from FileSchema via reflection.
   */
  private Object getPrivateField(FileSchema schema, String fieldName) throws Exception {
    Field field = FileSchema.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(schema);
  }

  /**
   * Helper: set a private field value on FileSchema via reflection.
   */
  private void setPrivateField(FileSchema schema, String fieldName, Object value)
      throws Exception {
    Field field = FileSchema.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(schema, value);
  }

  // -----------------------------------------------------------------------
  // Constructor branch: canonicalSchemaName for .aperio directory naming
  // -----------------------------------------------------------------------

  @Test void testConstructorWithCanonicalSchemaName() throws Exception {
    String userAssignedName = "ECON_UPPER";
    String canonicalName = "econ";
    FileSchema schema =
        new FileSchema(parentSchema, userAssignedName, tempDir.toFile(), null, null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        null, null, null, null, false, null, canonicalName);

    File cacheDir = schema.getOperatingCacheDirectory();
    assertNotNull(cacheDir);
    assertTrue(cacheDir.getAbsolutePath().contains(canonicalName),
        "Cache directory should use canonical name: " + cacheDir.getAbsolutePath());
  }

  @Test void testConstructorWithoutCanonicalSchemaNameUsesName() throws Exception {
    String schemaName = "my_schema";
    FileSchema schema =
        new FileSchema(parentSchema, schemaName, tempDir.toFile(), null, null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        null, null, null, null, false, null, null);

    File cacheDir = schema.getOperatingCacheDirectory();
    assertNotNull(cacheDir);
    assertTrue(cacheDir.getAbsolutePath().contains(schemaName),
        "Cache directory should use schema name: " + cacheDir.getAbsolutePath());
  }

  // -----------------------------------------------------------------------
  // Constructor branch: ephemeral temp directory for operating cache
  // -----------------------------------------------------------------------

  @Test void testConstructorWithEphemeralTempDirectory() {
    File tmpBase = new File(System.getProperty("java.io.tmpdir"), "testEphemeral" + System.nanoTime());
    tmpBase.mkdirs();
    try {
      FileSchema schema =
          new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), tmpBase, null, null,
          defaultEngineConfig, false, null, null, null,
          null, "SMART_CASING", "SMART_CASING",
          null, null, null, null, false);

      File cacheDir = schema.getOperatingCacheDirectory();
      assertNotNull(cacheDir);
      assertTrue(cacheDir.getAbsolutePath().startsWith(tmpBase.getAbsolutePath()),
          "Cache should use ephemeral temp dir: " + cacheDir.getAbsolutePath());
    } finally {
      tmpBase.delete();
    }
  }

  // -----------------------------------------------------------------------
  // Constructor branch: storageType with directoryPath (cloud storage URI)
  // -----------------------------------------------------------------------

  @Test void testConstructorWithStorageTypeAndDirectoryPath() {
    String s3Path = "s3://my-bucket/data-prefix";
    StorageProvider mockProvider = mock(StorageProvider.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), null, null, s3Path, null,
        null, defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    assertEquals(s3Path, schema.getBaseDirectory());
    assertNotNull(schema.getStorageProvider());
  }

  // -----------------------------------------------------------------------
  // Constructor branch: storageType with sourceDirectory (local storage)
  // -----------------------------------------------------------------------

  @Test void testConstructorWithStorageTypeAndSourceDirectory() {
    StorageProvider mockProvider = mock(StorageProvider.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        "local", storageConfig, null, null, false);

    assertEquals(tempDir.toFile().getAbsolutePath(), schema.getBaseDirectory());
  }

  // -----------------------------------------------------------------------
  // Constructor branch: userConfiguredBaseDirectory explicitly set
  // -----------------------------------------------------------------------

  @Test void testConstructorWithUserConfiguredBaseDirectory() {
    File userBaseDir = new File(tempDir.toFile(), "custom-base");
    userBaseDir.mkdirs();

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), userBaseDir, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        null, null, null, null, false);

    assertEquals(userBaseDir.getAbsolutePath(), schema.getBaseDirectory());
  }

  // -----------------------------------------------------------------------
  // Constructor branch: null sourceDirectory falls back to working dir
  // -----------------------------------------------------------------------

  @Test void testConstructorWithNullSourceDirectory() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), (File) null, null, defaultEngineConfig);

    assertNotNull(schema);
    assertNotNull(schema.getOperatingCacheDirectory());
  }

  // -----------------------------------------------------------------------
  // getTableName - explicit name vs derived name
  // -----------------------------------------------------------------------

  @Test void testGetTableNameExplicit() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    String result =
        (String) invokePrivate(schema, "getTableName", new Class[]{String.class, String.class, String.class},
        "EXPLICIT_NAME", "derived_name", "UPPER");

    assertEquals("EXPLICIT_NAME", result);
  }

  @Test void testGetTableNameDerived() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    String result =
        (String) invokePrivate(schema, "getTableName", new Class[]{String.class, String.class, String.class},
        null, "MyTable", "LOWER");

    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // trim() and trimOrNull() - static methods
  // -----------------------------------------------------------------------

  @Test void testTrimWithMatchingSuffix() throws Exception {
    Method method = FileSchema.class.getDeclaredMethod("trim", String.class, String.class);
    method.setAccessible(true);
    String result = (String) method.invoke(null, "hello.csv", ".csv");
    assertEquals("hello", result);
  }

  @Test void testTrimWithNonMatchingSuffix() throws Exception {
    Method method = FileSchema.class.getDeclaredMethod("trim", String.class, String.class);
    method.setAccessible(true);
    String result = (String) method.invoke(null, "hello.csv", ".json");
    assertEquals("hello.csv", result);
  }

  @Test void testTrimOrNullWithMatch() throws Exception {
    Method method = FileSchema.class.getDeclaredMethod("trimOrNull", String.class, String.class);
    method.setAccessible(true);
    String result = (String) method.invoke(null, "data.json", ".json");
    assertEquals("data", result);
  }

  @Test void testTrimOrNullWithNoMatch() throws Exception {
    Method method = FileSchema.class.getDeclaredMethod("trimOrNull", String.class, String.class);
    method.setAccessible(true);
    Object result = method.invoke(null, "data.json", ".csv");
    assertNull(result);
  }

  // -----------------------------------------------------------------------
  // buildConvertibleFilesGlobPattern
  // -----------------------------------------------------------------------

  @Test void testBuildConvertibleFilesGlobPatternNonRecursive() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    String pattern =
        (String) invokePrivate(schema, "buildConvertibleFilesGlobPattern",
        new Class[]{boolean.class}, false);

    assertNotNull(pattern);
    assertTrue(pattern.startsWith("*.{"), "Pattern should start with *.{: " + pattern);
    assertTrue(pattern.contains("xlsx"), "Pattern should contain xlsx");
    assertTrue(pattern.contains("docx"), "Pattern should contain docx");
    assertTrue(pattern.contains("html"), "Pattern should contain html");
    assertTrue(pattern.contains("md"), "Pattern should contain md");
  }

  @Test void testBuildConvertibleFilesGlobPatternRecursive() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    String pattern =
        (String) invokePrivate(schema, "buildConvertibleFilesGlobPatternRecursive",
        new Class[]{});

    assertNotNull(pattern);
    assertTrue(pattern.startsWith("**/*.{"),
        "Recursive pattern should start with **/*.{: " + pattern);
    assertTrue(pattern.contains("xlsx"), "Pattern should contain xlsx");
  }

  // -----------------------------------------------------------------------
  // getFilesInDir - recursive scanning with file type filtering
  // -----------------------------------------------------------------------

  @Test void testGetFilesInDirWithSupportedFiles() throws Exception {
    File srcDir = new File(tempDir.toFile(), "src");
    srcDir.mkdirs();
    Files.write(new File(srcDir, "data.csv").toPath(), "a,b\n1,2".getBytes(StandardCharsets.UTF_8));
    Files.write(new File(srcDir, "info.json").toPath(), "[{\"x\":1}]".getBytes(StandardCharsets.UTF_8));
    Files.write(new File(srcDir, "readme.txt").toPath(), "text".getBytes(StandardCharsets.UTF_8));
    Files.write(new File(srcDir, "._hidden.csv").toPath(), "hidden".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(srcDir);
    File[] files =
        (File[]) invokePrivate(schema, "getFilesInDir", new Class[]{File.class}, srcDir);

    assertNotNull(files);
    boolean hasCsv = false;
    boolean hasJson = false;
    boolean hasTxt = false;
    boolean hasHidden = false;
    for (File f : files) {
      if (f.getName().equals("data.csv")) {
        hasCsv = true;
      }
      if (f.getName().equals("info.json")) {
        hasJson = true;
      }
      if (f.getName().equals("readme.txt")) {
        hasTxt = true;
      }
      if (f.getName().equals("._hidden.csv")) {
        hasHidden = true;
      }
    }
    assertTrue(hasCsv, "Should find CSV file");
    assertTrue(hasJson, "Should find JSON file");
    assertFalse(hasTxt, "Should not find TXT file");
    assertFalse(hasHidden, "Should not find ._ prefixed file");
  }

  @Test void testGetFilesInDirRecursive() throws Exception {
    File srcDir = new File(tempDir.toFile(), "srcRec");
    srcDir.mkdirs();
    File subDir = new File(srcDir, "sub");
    subDir.mkdirs();
    Files.write(new File(srcDir, "top.csv").toPath(), "a\n1".getBytes(StandardCharsets.UTF_8));
    Files.write(new File(subDir, "nested.json").toPath(), "[{}]".getBytes(StandardCharsets.UTF_8));

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), srcDir, (List<Map<String, Object>>) null, defaultEngineConfig, true);

    File[] files =
        (File[]) invokePrivate(schema, "getFilesInDir", new Class[]{File.class}, srcDir);

    assertNotNull(files);
    assertTrue(files.length >= 2, "Should find files in root and subdirectory");
  }

  @Test void testGetFilesInDirNonExistent() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File[] files =
        (File[]) invokePrivate(schema, "getFilesInDir", new Class[]{File.class}, new File("/nonexistent/dir/path"));

    assertNotNull(files);
    assertEquals(0, files.length);
  }

  // -----------------------------------------------------------------------
  // findJsonFiles - recursive JSON file discovery
  // -----------------------------------------------------------------------

  @Test void testFindJsonFiles() throws Exception {
    File srcDir = new File(tempDir.toFile(), "jsonSearch");
    srcDir.mkdirs();
    Files.write(new File(srcDir, "a.json").toPath(), "{}".getBytes(StandardCharsets.UTF_8));
    Files.write(new File(srcDir, "b.json.gz").toPath(), "gz".getBytes(StandardCharsets.UTF_8));
    Files.write(new File(srcDir, "c.csv").toPath(), "x".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(srcDir);
    List<File> jsonFiles = new ArrayList<>();
    invokePrivate(schema, "findJsonFiles",
        new Class[]{File.class, List.class}, srcDir, jsonFiles);

    assertEquals(2, jsonFiles.size());
  }

  @Test void testFindJsonFilesRecursive() throws Exception {
    File srcDir = new File(tempDir.toFile(), "jsonRecSearch");
    srcDir.mkdirs();
    File subDir = new File(srcDir, "nested");
    subDir.mkdirs();
    Files.write(new File(srcDir, "top.json").toPath(), "{}".getBytes(StandardCharsets.UTF_8));
    Files.write(new File(subDir, "deep.json").toPath(), "{}".getBytes(StandardCharsets.UTF_8));

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), srcDir, (List<Map<String, Object>>) null, defaultEngineConfig, true);

    List<File> jsonFiles = new ArrayList<>();
    invokePrivate(schema, "findJsonFiles",
        new Class[]{File.class, List.class}, srcDir, jsonFiles);

    assertEquals(2, jsonFiles.size());
  }

  // -----------------------------------------------------------------------
  // resolveSource - absolute and relative URI handling
  // -----------------------------------------------------------------------

  @Test void testResolveSourceAbsolutePath() throws Exception {
    File csvFile = new File(tempDir.toFile(), "abs_test.csv");
    Files.write(csvFile.toPath(), "a\n1".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source =
        (Source) invokePrivate(schema, "resolveSource", new Class[]{String.class}, csvFile.getAbsolutePath());

    assertNotNull(source);
  }

  @Test void testResolveSourceRelativePath() throws Exception {
    File csvFile = new File(tempDir.toFile(), "rel_test.csv");
    Files.write(csvFile.toPath(), "a\n1".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source =
        (Source) invokePrivate(schema, "resolveSource", new Class[]{String.class}, "rel_test.csv");

    assertNotNull(source);
  }

  // -----------------------------------------------------------------------
  // createTableFromSource - format override branches
  // -----------------------------------------------------------------------

  @Test void testCreateTableFromSourceJsonFormat() throws Exception {
    File jsonFile = new File(tempDir.toFile(), "formattest.json");
    Files.write(jsonFile.toPath(), "[{\"id\":1}]".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(jsonFile);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "json");
    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, tableDef);

    assertNotNull(table);
  }

  @Test void testCreateTableFromSourceCsvFormat() throws Exception {
    File csvFile = new File(tempDir.toFile(), "formattest.csv");
    Files.write(csvFile.toPath(), "a,b\n1,2".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(csvFile);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "csv");
    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, tableDef);

    assertNotNull(table);
  }

  @Test void testCreateTableFromSourceTsvFormat() throws Exception {
    File tsvFile = new File(tempDir.toFile(), "formattest.tsv");
    Files.write(tsvFile.toPath(), "a\tb\n1\t2".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(tsvFile);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "tsv");
    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, tableDef);

    assertNotNull(table);
  }

  @Test void testCreateTableFromSourceYamlFormat() throws Exception {
    File yamlFile = new File(tempDir.toFile(), "formattest.yaml");
    Files.write(yamlFile.toPath(), "- id: 1\n".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(yamlFile);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "yaml");
    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, tableDef);

    assertNotNull(table);
  }

  @Test void testCreateTableFromSourceYmlFormat() throws Exception {
    File ymlFile = new File(tempDir.toFile(), "formattest.yml");
    Files.write(ymlFile.toPath(), "- id: 1\n".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(ymlFile);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "yml");
    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, tableDef);

    assertNotNull(table);
  }

  @Test void testCreateTableFromSourceJsonFormatWithFlatten() throws Exception {
    File jsonFile = new File(tempDir.toFile(), "formatflatten.json");
    Files.write(jsonFile.toPath(), "[{\"id\":1,\"nested\":{\"val\":2}}]".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(jsonFile);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "json");
    tableDef.put("flatten", true);
    tableDef.put("flattenSeparator", ".");
    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, tableDef);

    assertNotNull(table);
  }

  @Test void testCreateTableFromSourceUnsupportedFormat() throws Exception {
    File txtFile = new File(tempDir.toFile(), "formattest.txt");
    Files.write(txtFile.toPath(), "text".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(txtFile);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "unsupported_format");
    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, tableDef);

    assertNull(table, "Unsupported format should return null");
  }

  @Test void testCreateTableFromSourceAutoDetectJson() throws Exception {
    File jsonFile = new File(tempDir.toFile(), "auto.json");
    Files.write(jsonFile.toPath(), "[{\"id\":1}]".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(jsonFile);

    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, (Map<String, Object>) null);

    assertNotNull(table);
  }

  @Test void testCreateTableFromSourceAutoDetectYaml() throws Exception {
    File yamlFile = new File(tempDir.toFile(), "auto.yaml");
    Files.write(yamlFile.toPath(), "- id: 1\n".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(yamlFile);

    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, (Map<String, Object>) null);

    assertNotNull(table);
  }

  @Test void testCreateTableFromSourceAutoDetectYml() throws Exception {
    File ymlFile = new File(tempDir.toFile(), "auto.yml");
    Files.write(ymlFile.toPath(), "- id: 1\n".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(ymlFile);

    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, (Map<String, Object>) null);

    assertNotNull(table);
  }

  @Test void testCreateTableFromSourceAutoDetectCsv() throws Exception {
    File csvFile = new File(tempDir.toFile(), "auto.csv");
    Files.write(csvFile.toPath(), "a,b\n1,2".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(csvFile);

    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, (Map<String, Object>) null);

    assertNotNull(table);
  }

  @Test void testCreateTableFromSourceAutoDetectTsv() throws Exception {
    File tsvFile = new File(tempDir.toFile(), "auto.tsv");
    Files.write(tsvFile.toPath(), "a\tb\n1\t2".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(tsvFile);

    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, (Map<String, Object>) null);

    assertNotNull(table);
  }

  @Test void testCreateTableFromSourceAutoDetectNoMatch() throws Exception {
    File txtFile = new File(tempDir.toFile(), "auto.txt");
    Files.write(txtFile.toPath(), "text".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(txtFile);

    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, (Map<String, Object>) null);

    assertNull(table);
  }

  @Test void testCreateTableFromSourceAutoDetectJsonWithSchemaFlatten() throws Exception {
    File jsonFile = new File(tempDir.toFile(), "schemaflatten.json");
    Files.write(jsonFile.toPath(), "[{\"id\":1}]".getBytes(StandardCharsets.UTF_8));

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        null, null, true, null, false);

    Source source = Sources.of(jsonFile);

    Table table =
        (Table) invokePrivate(schema, "createTableFromSource", new Class[]{Source.class, Map.class}, source, (Map<String, Object>) null);

    assertNotNull(table);
  }

  // -----------------------------------------------------------------------
  // addTable(tableDef) - view type skipping
  // -----------------------------------------------------------------------

  @Test void testAddTableViewTypeSkipped() throws Exception {
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "my_view");
    tableDef.put("type", "view");
    tableDef.put("url", "/some/path");

    FileSchema schema = createSchema(tempDir.toFile());

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Boolean result =
        (Boolean) invokePrivate(schema, "addTable", new Class[]{com.google.common.collect.ImmutableMap.Builder.class, Map.class},
        builder, tableDef);

    assertFalse(result);
  }

  @Test void testAddTableNullUrlSkipped() throws Exception {
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "no_url_table");

    FileSchema schema = createSchema(tempDir.toFile());

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Boolean result =
        (Boolean) invokePrivate(schema, "addTable", new Class[]{com.google.common.collect.ImmutableMap.Builder.class, Map.class},
        builder, tableDef);

    assertFalse(result);
  }

  // -----------------------------------------------------------------------
  // addTable(source) - format override error branches
  // -----------------------------------------------------------------------

  @Test void testAddTableFormatOverrideExcelThrows() throws Exception {
    File xlsxFile = new File(tempDir.toFile(), "test.xlsx");
    Files.write(xlsxFile.toPath(), "fake".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(xlsxFile);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "excel");

    try {
      invokePrivate(schema, "addTable",
          new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
              Source.class, String.class, Map.class},
          builder, source, "test_excel", tableDef);
      fail("Should throw RuntimeException for Excel format");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Excel files cannot be used in explicit table definitions"));
    }
  }

  @Test void testAddTableFormatOverrideMarkdownThrows() throws Exception {
    File mdFile = new File(tempDir.toFile(), "test.md");
    Files.write(mdFile.toPath(), "# title".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(mdFile);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "md");

    try {
      invokePrivate(schema, "addTable",
          new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
              Source.class, String.class, Map.class},
          builder, source, "test_md", tableDef);
      fail("Should throw RuntimeException for Markdown format");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Markdown files cannot be used"));
    }
  }

  @Test void testAddTableFormatOverrideDocxThrows() throws Exception {
    File docxFile = new File(tempDir.toFile(), "test.docx");
    Files.write(docxFile.toPath(), "fake".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(docxFile);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "docx");

    try {
      invokePrivate(schema, "addTable",
          new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
              Source.class, String.class, Map.class},
          builder, source, "test_docx", tableDef);
      fail("Should throw RuntimeException for DOCX format");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("DOCX files cannot be used"));
    }
  }

  @Test void testAddTableFormatOverrideUnsupportedThrows() throws Exception {
    File file = new File(tempDir.toFile(), "test.bin");
    Files.write(file.toPath(), "bin".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(file);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "binary");

    try {
      invokePrivate(schema, "addTable",
          new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
              Source.class, String.class, Map.class},
          builder, source, "test_bin", tableDef);
      fail("Should throw RuntimeException for unsupported format");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Unsupported format override"));
    }
  }

  @Test void testAddTableFormatOverrideCsv() throws Exception {
    File csvFile = new File(tempDir.toFile(), "testformat.csv");
    Files.write(csvFile.toPath(), "a,b\n1,2".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(csvFile);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "csv");

    Boolean result =
        (Boolean) invokePrivate(schema, "addTable", new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
            Source.class, String.class, Map.class},
        builder, source, "csv_table", tableDef);

    assertTrue(result);
  }

  @Test void testAddTableFormatOverrideTsv() throws Exception {
    File tsvFile = new File(tempDir.toFile(), "testformat.tsv");
    Files.write(tsvFile.toPath(), "a\tb\n1\t2".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(tsvFile);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "tsv");

    Boolean result =
        (Boolean) invokePrivate(schema, "addTable", new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
            Source.class, String.class, Map.class},
        builder, source, "tsv_table", tableDef);

    assertTrue(result);
  }

  @Test void testAddTableFormatOverrideYaml() throws Exception {
    File yamlFile = new File(tempDir.toFile(), "testformat.yaml");
    Files.write(yamlFile.toPath(), "- id: 1\n  name: test\n".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(yamlFile);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "yaml");

    Boolean result =
        (Boolean) invokePrivate(schema, "addTable", new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
            Source.class, String.class, Map.class},
        builder, source, "yaml_table", tableDef);

    assertTrue(result);
  }

  @Test void testAddTableFormatOverrideJson() throws Exception {
    File jsonFile = new File(tempDir.toFile(), "testformat.json");
    Files.write(jsonFile.toPath(), "[{\"id\":1}]".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(jsonFile);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "json");

    Boolean result =
        (Boolean) invokePrivate(schema, "addTable", new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
            Source.class, String.class, Map.class},
        builder, source, "json_table", tableDef);

    assertTrue(result);
  }

  @Test void testAddTableFormatOverrideJsonWithFlatten() throws Exception {
    File jsonFile = new File(tempDir.toFile(), "flatformat.json");
    Files.write(jsonFile.toPath(), "[{\"id\":1}]".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(jsonFile);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "json");
    tableDef.put("flatten", true);
    tableDef.put("flattenSeparator", "__");

    Boolean result =
        (Boolean) invokePrivate(schema, "addTable", new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
            Source.class, String.class, Map.class},
        builder, source, "flat_json_table", tableDef);

    assertTrue(result);
  }

  // -----------------------------------------------------------------------
  // addTable extension-based detection (no format override)
  // -----------------------------------------------------------------------

  @Test void testAddTableAutoDetectParquetExtension() throws Exception {
    File parquetFile = new File(tempDir.toFile(), "auto_detect.parquet");
    Files.write(parquetFile.toPath(), "PAR1".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(parquetFile);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Boolean result =
        (Boolean) invokePrivate(schema, "addTable", new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
            Source.class, String.class, Map.class},
        builder, source, "parquet_table", (Map<String, Object>) null);

    assertTrue(result);
  }

  @Test void testAddTableAutoDetectXlsxWithTableDef() throws Exception {
    File xlsxFile = new File(tempDir.toFile(), "auto_detect.xlsx");
    Files.write(xlsxFile.toPath(), "fake".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(xlsxFile);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "xlsx_table");

    try {
      invokePrivate(schema, "addTable",
          new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
              Source.class, String.class, Map.class},
          builder, source, "xlsx_table", tableDef);
      fail("XLSX with tableDef should throw");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Excel files should not be used"));
    }
  }

  @Test void testAddTableAutoDetectXlsxDirectoryScan() throws Exception {
    File xlsxFile = new File(tempDir.toFile(), "scan.xlsx");
    Files.write(xlsxFile.toPath(), "fake".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(xlsxFile);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Boolean result =
        (Boolean) invokePrivate(schema, "addTable", new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
            Source.class, String.class, Map.class},
        builder, source, "scan_xlsx", (Map<String, Object>) null);

    assertFalse(result);
  }

  // -----------------------------------------------------------------------
  // shouldUseLazyInitialization - various branches
  // -----------------------------------------------------------------------

  @Test void testShouldUseLazyInitializationNullEngineConfig() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    setPrivateField(schema, "engineConfig", null);

    org.apache.calcite.adapter.file.partition.PartitionedTableConfig config =
        mock(org.apache.calcite.adapter.file.partition.PartitionedTableConfig.class);
    when(config.getName()).thenReturn("test_table");

    Boolean result =
        (Boolean) invokePrivate(schema, "shouldUseLazyInitialization", new Class[]{org.apache.calcite.adapter.file.partition.PartitionedTableConfig.class}, config);

    assertFalse(result);
  }

  @Test void testShouldUseLazyInitializationNoRefreshInterval() throws Exception {
    ExecutionEngineConfig duckConfig = mock(ExecutionEngineConfig.class);
    when(duckConfig.getEngineType()).thenReturn(ExecutionEngineConfig.ExecutionEngineType.DUCKDB);
    when(duckConfig.getParquetCacheDirectory()).thenReturn(null);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, duckConfig, false, null, null, null, null);

    org.apache.calcite.adapter.file.partition.PartitionedTableConfig config =
        mock(org.apache.calcite.adapter.file.partition.PartitionedTableConfig.class);
    when(config.getName()).thenReturn("test_table");

    Boolean result =
        (Boolean) invokePrivate(schema, "shouldUseLazyInitialization", new Class[]{org.apache.calcite.adapter.file.partition.PartitionedTableConfig.class}, config);

    assertFalse(result);
  }

  @Test void testShouldUseLazyInitializationNonDuckDB() throws Exception {
    ExecutionEngineConfig parquetConfig = mock(ExecutionEngineConfig.class);
    when(parquetConfig.getEngineType()).thenReturn(ExecutionEngineConfig.ExecutionEngineType.PARQUET);
    when(parquetConfig.getDuckDBConfig()).thenReturn(null);
    when(parquetConfig.getParquetCacheDirectory()).thenReturn(null);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, parquetConfig, false,
        null, null, null, "5 minutes");

    org.apache.calcite.adapter.file.partition.PartitionedTableConfig config =
        mock(org.apache.calcite.adapter.file.partition.PartitionedTableConfig.class);
    when(config.getName()).thenReturn("test_table");

    Boolean result =
        (Boolean) invokePrivate(schema, "shouldUseLazyInitialization", new Class[]{org.apache.calcite.adapter.file.partition.PartitionedTableConfig.class}, config);

    assertFalse(result);
  }

  // -----------------------------------------------------------------------
  // validateForeignKeyConstraints - various branches
  // -----------------------------------------------------------------------

  @Test void testValidateForeignKeyConstraintsEmptyMetadata() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    schema.setConstraintMetadata(new HashMap<>());
    invokePrivate(schema, "validateForeignKeyConstraints",
        new Class[]{Map.class}, Collections.emptyMap());
  }

  @Test void testValidateForeignKeyConstraintsNullConstraints() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Map<String, Object>> metadata = new HashMap<>();
    metadata.put("table1", null);
    schema.setConstraintMetadata(metadata);
    invokePrivate(schema, "validateForeignKeyConstraints",
        new Class[]{Map.class}, Collections.emptyMap());
  }

  @Test void testValidateForeignKeyConstraintsNoForeignKeys() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Map<String, Object>> metadata = new HashMap<>();
    Map<String, Object> constraints = new HashMap<>();
    constraints.put("primaryKey", Arrays.asList("id"));
    metadata.put("table1", constraints);
    schema.setConstraintMetadata(metadata);
    invokePrivate(schema, "validateForeignKeyConstraints",
        new Class[]{Map.class}, Collections.emptyMap());
  }

  @Test void testValidateForeignKeyConstraintsWithInvalidFk() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());

    Map<String, Map<String, Object>> metadata = new HashMap<>();
    Map<String, Object> constraints = new HashMap<>();
    List<Map<String, Object>> foreignKeys = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetTable", "nonexistent_table");
    fk.put("columns", Arrays.asList("fk_id"));
    fk.put("targetColumns", Arrays.asList("id"));
    foreignKeys.add(fk);
    constraints.put("foreignKeys", foreignKeys);
    metadata.put("source_table", constraints);
    schema.setConstraintMetadata(metadata);

    invokePrivate(schema, "validateForeignKeyConstraints",
        new Class[]{Map.class}, Collections.emptyMap());

    assertTrue(foreignKeys.isEmpty(), "Invalid FK should be removed");
  }

  @Test void testValidateForeignKeyConstraintsWithValidFk() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());

    Map<String, Map<String, Object>> metadata = new HashMap<>();
    Map<String, Object> constraints = new HashMap<>();
    List<Map<String, Object>> foreignKeys = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetTable", "existing_table");
    fk.put("columns", Arrays.asList("fk_id"));
    foreignKeys.add(fk);
    constraints.put("foreignKeys", foreignKeys);
    metadata.put("source_table", constraints);
    schema.setConstraintMetadata(metadata);

    Map<String, Table> tables = new HashMap<>();
    tables.put("existing_table", mock(Table.class));

    invokePrivate(schema, "validateForeignKeyConstraints",
        new Class[]{Map.class}, tables);

    assertEquals(1, foreignKeys.size(), "Valid FK should remain");
  }

  @Test void testValidateForeignKeyConstraintsWithQualifiedTargetTable() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());

    Map<String, Map<String, Object>> metadata = new HashMap<>();
    Map<String, Object> constraints = new HashMap<>();
    List<Map<String, Object>> foreignKeys = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetTable", Arrays.asList("other_schema", "other_table"));
    fk.put("columns", Arrays.asList("fk_id"));
    foreignKeys.add(fk);
    constraints.put("foreignKeys", foreignKeys);
    metadata.put("source_table", constraints);
    schema.setConstraintMetadata(metadata);

    invokePrivate(schema, "validateForeignKeyConstraints",
        new Class[]{Map.class}, Collections.emptyMap());

    assertTrue(foreignKeys.isEmpty());
  }

  @Test void testValidateForeignKeyConstraintsWithSingleElementList() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());

    Map<String, Map<String, Object>> metadata = new HashMap<>();
    Map<String, Object> constraints = new HashMap<>();
    List<Map<String, Object>> foreignKeys = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetTable", Arrays.asList("target_table"));
    fk.put("columns", Arrays.asList("fk_id"));
    foreignKeys.add(fk);
    constraints.put("foreignKeys", foreignKeys);
    metadata.put("source_table", constraints);
    schema.setConstraintMetadata(metadata);

    Map<String, Table> tables = new HashMap<>();
    tables.put("target_table", mock(Table.class));

    invokePrivate(schema, "validateForeignKeyConstraints",
        new Class[]{Map.class}, tables);

    assertEquals(1, foreignKeys.size());
  }

  @Test void testValidateForeignKeyWithNullTargetTable() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());

    Map<String, Map<String, Object>> metadata = new HashMap<>();
    Map<String, Object> constraints = new HashMap<>();
    List<Map<String, Object>> foreignKeys = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetTable", null);
    foreignKeys.add(fk);
    constraints.put("foreignKeys", foreignKeys);
    metadata.put("source_table", constraints);
    schema.setConstraintMetadata(metadata);

    invokePrivate(schema, "validateForeignKeyConstraints",
        new Class[]{Map.class}, Collections.emptyMap());

    assertEquals(1, foreignKeys.size());
  }

  // -----------------------------------------------------------------------
  // checkTableExists - cross-schema lookup
  // -----------------------------------------------------------------------

  @Test void testCheckTableExistsLocalSchema() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Table> localTables = new HashMap<>();
    localTables.put("my_table", mock(Table.class));

    Boolean result =
        (Boolean) invokePrivate(schema, "checkTableExists", new Class[]{String.class, String.class, Map.class},
        null, "my_table", localTables);
    assertTrue(result);

    result =
        (Boolean) invokePrivate(schema, "checkTableExists", new Class[]{String.class, String.class, Map.class},
        null, "nonexistent", localTables);
    assertFalse(result);
  }

  @Test void testCheckTableExistsCrossSchemaNotFound() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Table> localTables = new HashMap<>();

    Boolean result =
        (Boolean) invokePrivate(schema, "checkTableExists", new Class[]{String.class, String.class, Map.class},
        "nonexistent_schema", "other_table", localTables);
    assertFalse(result);
  }

  // -----------------------------------------------------------------------
  // getTableBaseline and updateTableBaseline
  // -----------------------------------------------------------------------

  @Test void testGetTableBaselineNullMetadata() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    setPrivateField(schema, "conversionMetadata", null);
    assertNull(schema.getTableBaseline("any_table"));
  }

  @Test void testGetTableBaselineNoRecord() {
    FileSchema schema = createSchema(tempDir.toFile());
    assertNull(schema.getTableBaseline("nonexistent"));
  }

  @Test void testUpdateTableBaselineNullMetadata() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    setPrivateField(schema, "conversionMetadata", null);
    schema.updateTableBaseline("any_table", new ConversionMetadata.PartitionBaseline());
  }

  @Test void testUpdateTableBaselineNoRecord() {
    FileSchema schema = createSchema(tempDir.toFile());
    schema.updateTableBaseline("nonexistent_table", new ConversionMetadata.PartitionBaseline());
  }

  // -----------------------------------------------------------------------
  // setConstraintMetadata and getTableConstraints
  // -----------------------------------------------------------------------

  @Test void testSetAndGetConstraintMetadata() {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Map<String, Object>> metadata = new HashMap<>();
    Map<String, Object> constraints = new HashMap<>();
    constraints.put("primaryKey", Arrays.asList("id"));
    metadata.put("users", constraints);
    schema.setConstraintMetadata(metadata);

    assertNotNull(schema.getTableConstraints("users"));
    assertEquals(constraints, schema.getTableConstraints("users"));
  }

  @Test void testGetTableConstraintsNotFound() {
    FileSchema schema = createSchema(tempDir.toFile());
    schema.setConstraintMetadata(new HashMap<>());
    assertNull(schema.getTableConstraints("nonexistent"));
  }

  // -----------------------------------------------------------------------
  // setConversionRecords
  // -----------------------------------------------------------------------

  @Test void testSetConversionRecordsNull() {
    FileSchema schema = createSchema(tempDir.toFile());
    schema.setConversionRecords(null);
  }

  @Test void testSetConversionRecordsEmpty() {
    FileSchema schema = createSchema(tempDir.toFile());
    schema.setConversionRecords(Collections.emptyMap());
  }

  @Test void testSetConversionRecordsWithRecords() {
    FileSchema schema = createSchema(tempDir.toFile());

    Map<String, ConversionMetadata.ConversionRecord> records = new HashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "test_table";
    record.viewScanPattern = "s3://bucket/**/*.parquet";
    records.put("test_table", record);

    schema.setConversionRecords(records);
    assertNotNull(schema.getConversionMetadata());
  }

  // -----------------------------------------------------------------------
  // getAllTableRecords
  // -----------------------------------------------------------------------

  @Test void testGetAllTableRecordsEmpty() {
    FileSchema schema = createSchema(tempDir.toFile());
    assertNotNull(schema.getAllTableRecords());
  }

  @Test void testGetAllTableRecordsNullMetadata() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    setPrivateField(schema, "conversionMetadata", null);
    Map<String, ConversionMetadata.ConversionRecord> records = schema.getAllTableRecords();
    assertNotNull(records);
    assertTrue(records.isEmpty());
  }

  // -----------------------------------------------------------------------
  // notifyIcebergTableRefreshed
  // -----------------------------------------------------------------------

  @Test void testNotifyIcebergTableRefreshed() {
    FileSchema schema = createSchema(tempDir.toFile());
    PatternAwareRefreshListener mockListener = mock(PatternAwareRefreshListener.class);
    schema.addRefreshListener(mockListener);
    schema.notifyIcebergTableRefreshed("my_table", "s3://bucket/warehouse/my_table");
    verify(mockListener).onIcebergTableRefreshed("my_table", "s3://bucket/warehouse/my_table");
  }

  @Test void testNotifyIcebergTableRefreshedWithException() {
    FileSchema schema = createSchema(tempDir.toFile());
    PatternAwareRefreshListener mockListener = mock(PatternAwareRefreshListener.class);
    doThrow(new RuntimeException("test error")).when(mockListener)
        .onIcebergTableRefreshed(anyString(), anyString());
    schema.addRefreshListener(mockListener);
    schema.notifyIcebergTableRefreshed("my_table", "s3://bucket/warehouse/my_table");
  }

  @Test void testNotifyIcebergTableRefreshedNonPatternListener() {
    FileSchema schema = createSchema(tempDir.toFile());
    TableRefreshListener simpleListener = mock(TableRefreshListener.class);
    schema.addRefreshListener(simpleListener);
    schema.notifyIcebergTableRefreshed("my_table", "s3://bucket/warehouse/my_table");
    verify(simpleListener, never()).onTableRefreshed(anyString(), any(File.class));
  }

  // -----------------------------------------------------------------------
  // notifyTableRefreshedWithPattern
  // -----------------------------------------------------------------------

  @Test void testNotifyTableRefreshedWithPattern() {
    FileSchema schema = createSchema(tempDir.toFile());
    PatternAwareRefreshListener mockListener = mock(PatternAwareRefreshListener.class);
    schema.addRefreshListener(mockListener);
    schema.notifyTableRefreshedWithPattern("my_table", "**/*.parquet");
    verify(mockListener).onTableRefreshedWithPattern("my_table", "**/*.parquet");
  }

  @Test void testNotifyTableRefreshedWithPatternException() {
    FileSchema schema = createSchema(tempDir.toFile());
    PatternAwareRefreshListener mockListener = mock(PatternAwareRefreshListener.class);
    doThrow(new RuntimeException("test error")).when(mockListener)
        .onTableRefreshedWithPattern(anyString(), anyString());
    schema.addRefreshListener(mockListener);
    schema.notifyTableRefreshedWithPattern("my_table", "**/*.parquet");
  }

  // -----------------------------------------------------------------------
  // notifyTableRefreshed
  // -----------------------------------------------------------------------

  @Test void testNotifyTableRefreshed() {
    FileSchema schema = createSchema(tempDir.toFile());
    TableRefreshListener mockListener = mock(TableRefreshListener.class);
    schema.addRefreshListener(mockListener);
    File parquetFile = new File(tempDir.toFile(), "test.parquet");
    schema.notifyTableRefreshed("my_table", parquetFile);
    verify(mockListener).onTableRefreshed("my_table", parquetFile);
  }

  @Test void testNotifyTableRefreshedWithException() {
    FileSchema schema = createSchema(tempDir.toFile());
    TableRefreshListener mockListener = mock(TableRefreshListener.class);
    doThrow(new RuntimeException("test error")).when(mockListener)
        .onTableRefreshed(anyString(), any(File.class));
    schema.addRefreshListener(mockListener);
    schema.notifyTableRefreshed("my_table", new File("test.parquet"));
  }

  // -----------------------------------------------------------------------
  // resolvePath and readAllBytes
  // -----------------------------------------------------------------------

  @Test void testResolvePathWithoutStorageProvider() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    String result =
        (String) invokePrivate(schema, "resolvePath", new Class[]{String.class}, "relative/path.txt");
    assertEquals("relative/path.txt", result);
  }

  @Test void testReadAllBytes() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    byte[] testData = "Hello, World!".getBytes(StandardCharsets.UTF_8);
    InputStream is = new ByteArrayInputStream(testData);
    byte[] result =
        (byte[]) invokePrivate(schema, "readAllBytes", new Class[]{InputStream.class}, is);
    assertEquals("Hello, World!", new String(result, StandardCharsets.UTF_8));
  }

  @Test void testReadAllBytesEmpty() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    InputStream is = new ByteArrayInputStream(new byte[0]);
    byte[] result =
        (byte[]) invokePrivate(schema, "readAllBytes", new Class[]{InputStream.class}, is);
    assertEquals(0, result.length);
  }

  @Test void testReadAllBytesLargeData() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    byte[] largeData = new byte[20000];
    Arrays.fill(largeData, (byte) 'X');
    InputStream is = new ByteArrayInputStream(largeData);
    byte[] result =
        (byte[]) invokePrivate(schema, "readAllBytes", new Class[]{InputStream.class}, is);
    assertEquals(20000, result.length);
  }

  // -----------------------------------------------------------------------
  // refreshAllTables - private method
  // -----------------------------------------------------------------------

  @Test void testRefreshAllTablesEmpty() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    invokePrivate(schema, "refreshAllTables", new Class[]{});
  }

  @Test void testRefreshAllTablesWithMock() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    RefreshableParquetCacheTable mockTable = mock(RefreshableParquetCacheTable.class);

    @SuppressWarnings("unchecked")
    List<RefreshableParquetCacheTable> refreshableTables =
        (List<RefreshableParquetCacheTable>) getPrivateField(schema, "refreshableTables");
    refreshableTables.add(mockTable);

    invokePrivate(schema, "refreshAllTables", new Class[]{});
    verify(mockTable).refresh();
  }

  @Test void testRefreshAllTablesWithException() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    RefreshableParquetCacheTable mockTable = mock(RefreshableParquetCacheTable.class);
    doThrow(new RuntimeException("refresh failed")).when(mockTable).refresh();

    @SuppressWarnings("unchecked")
    List<RefreshableParquetCacheTable> refreshableTables =
        (List<RefreshableParquetCacheTable>) getPrivateField(schema, "refreshableTables");
    refreshableTables.add(mockTable);

    invokePrivate(schema, "refreshAllTables", new Class[]{});
  }

  // -----------------------------------------------------------------------
  // extractFieldConfigurations - static method
  // -----------------------------------------------------------------------

  @Test void testExtractFieldConfigurationsNullTableDef() throws Exception {
    Method method =
        FileSchema.class.getDeclaredMethod("extractFieldConfigurations", Map.class, String.class);
    method.setAccessible(true);
    assertNull(method.invoke(null, null, "tableName"));
  }

  @Test void testExtractFieldConfigurationsWithFields() throws Exception {
    Method method =
        FileSchema.class.getDeclaredMethod("extractFieldConfigurations", Map.class, String.class);
    method.setAccessible(true);

    Map<String, Object> tableDef = new HashMap<>();
    List<Map<String, Object>> fields = new ArrayList<>();
    Map<String, Object> field = new HashMap<>();
    field.put("name", "col1");
    field.put("type", "VARCHAR");
    fields.add(field);
    tableDef.put("fields", fields);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result = (List<Map<String, Object>>)
        method.invoke(null, tableDef, "myTable");

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals("col1", result.get(0).get("name"));
  }

  @Test void testExtractFieldConfigurationsNoFields() throws Exception {
    Method method =
        FileSchema.class.getDeclaredMethod("extractFieldConfigurations", Map.class, String.class);
    method.setAccessible(true);
    assertNull(method.invoke(null, new HashMap<>(), "myTable"));
  }

  // -----------------------------------------------------------------------
  // setFunctionMultimap and clearTableCache
  // -----------------------------------------------------------------------

  @Test void testSetFunctionMultimap() {
    FileSchema schema = createSchema(tempDir.toFile());
    Multimap<String, Function> functions = ImmutableMultimap.of();
    schema.setFunctionMultimap(functions);
    assertNotNull(schema);
  }

  @Test void testClearTableCache() {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    schema.clearTableCache();
    Map<String, Table> tables2 = schema.getTableMap();
    assertNotNull(tables2);
  }

  // -----------------------------------------------------------------------
  // hasRefreshableTables and getComment
  // -----------------------------------------------------------------------

  @Test void testHasRefreshableTablesWithInterval() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, defaultEngineConfig, false,
        null, null, null, "5 minutes");
    assertTrue(schema.hasRefreshableTables());
  }

  @Test void testHasRefreshableTablesWithoutInterval() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, defaultEngineConfig, false,
        null, null, null, null);
    assertFalse(schema.hasRefreshableTables());
  }

  @Test void testGetCommentWithValue() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        null, null, null, null, false, "My Schema Comment");
    assertEquals("My Schema Comment", schema.getComment());
  }

  @Test void testGetCommentNull() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        null, null, null, null, false, null);
    assertNull(schema.getComment());
  }

  // -----------------------------------------------------------------------
  // Accessor methods
  // -----------------------------------------------------------------------

  @Test void testGetBaseDirectoryDefault() {
    FileSchema schema = createSchema(tempDir.toFile());
    assertNotNull(schema.getBaseDirectory());
  }

  @Test void testGetStorageConfigNull() {
    FileSchema schema = createSchema(tempDir.toFile());
    assertNull(schema.getStorageConfig());
  }

  @Test void testGetStorageProviderNull() {
    FileSchema schema = createSchema(tempDir.toFile());
    assertNull(schema.getStorageProvider());
  }

  @Test void testGetAlternatePartitionRegistry() {
    FileSchema schema = createSchema(tempDir.toFile());
    assertNotNull(schema.getAlternatePartitionRegistry());
  }

  @Test void testGetConversionMetadata() {
    FileSchema schema = createSchema(tempDir.toFile());
    assertNotNull(schema.getConversionMetadata());
  }

  @Test void testGetOperatingCacheDirectory() {
    FileSchema schema = createSchema(tempDir.toFile());
    File cacheDir = schema.getOperatingCacheDirectory();
    assertNotNull(cacheDir);
    assertTrue(cacheDir.getAbsolutePath().contains(".aperio"));
  }

  // -----------------------------------------------------------------------
  // registerRawToParquetConverter
  // -----------------------------------------------------------------------

  @Test void testRegisterRawToParquetConverter() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    org.apache.calcite.adapter.file.converters.RawToParquetConverter mockConverter =
        mock(org.apache.calcite.adapter.file.converters.RawToParquetConverter.class);
    schema.registerRawToParquetConverter(mockConverter);

    @SuppressWarnings("unchecked")
    List<org.apache.calcite.adapter.file.converters.RawToParquetConverter> converters =
        (List<org.apache.calcite.adapter.file.converters.RawToParquetConverter>)
            getPrivateField(schema, "rawToParquetConverters");
    assertEquals(1, converters.size());
  }

  // -----------------------------------------------------------------------
  // getTableMap with views and materializations
  // -----------------------------------------------------------------------

  @Test void testGetTableMapWithViews() {
    List<Map<String, Object>> views = new ArrayList<>();
    Map<String, Object> view = new HashMap<>();
    view.put("name", "my_view");
    view.put("sql", "SELECT 1 AS id");
    views.add(view);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, defaultEngineConfig, false,
        null, views, null, null);

    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    assertTrue(tables.containsKey("my_view"), "Should contain the view: " + tables.keySet());
  }

  @Test void testGetTableMapWithViewMissingName() {
    List<Map<String, Object>> views = new ArrayList<>();
    Map<String, Object> view = new HashMap<>();
    view.put("sql", "SELECT 1 AS id");
    views.add(view);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, defaultEngineConfig, false,
        null, views, null, null);

    assertNotNull(schema.getTableMap());
  }

  @Test void testGetTableMapWithMaterializationsNonParquetEngine() {
    List<Map<String, Object>> materializations = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("view", "mv_view");
    mv.put("table", "mv_table");
    mv.put("sql", "SELECT 1 AS id");
    materializations.add(mv);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, defaultEngineConfig, false,
        materializations, null, null, null);

    assertNotNull(schema.getTableMap());
  }

  @Test void testGetTableMapReturnsCachedResult() {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Table> first = schema.getTableMap();
    Map<String, Table> second = schema.getTableMap();
    assertTrue(first == second);
  }

  // -----------------------------------------------------------------------
  // Various constructor overloads
  // -----------------------------------------------------------------------

  @Test void testSimplestConstructor() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null);
    assertNotNull(schema);
  }

  @Test void testConstructorWithRecursiveFlag() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig, true);
    assertNotNull(schema);
  }

  @Test void testConstructorWithMaterializationsAndViews() {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, defaultEngineConfig, false,
        null, null);
    assertNotNull(schema);
  }

  @Test void testConstructorNullCasingDefaults() throws Exception {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, null, null,
        null, null, null, null, false);

    String tableNameCasing = (String) getPrivateField(schema, "tableNameCasing");
    String columnNameCasing = (String) getPrivateField(schema, "columnNameCasing");
    assertEquals("SMART_CASING", tableNameCasing);
    assertEquals("SMART_CASING", columnNameCasing);
  }

  // -----------------------------------------------------------------------
  // applyCasing
  // -----------------------------------------------------------------------

  @Test void testApplyCasingUpper() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    String result =
        (String) invokePrivate(schema, "applyCasing", new Class[]{String.class, String.class}, "my_table", "UPPER");
    assertNotNull(result);
  }

  @Test void testApplyCasingLower() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    String result =
        (String) invokePrivate(schema, "applyCasing", new Class[]{String.class, String.class}, "MY_TABLE", "LOWER");
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // Storage operations without baseDirectory
  // -----------------------------------------------------------------------

  @Test void testWriteToStorageWithoutBaseDirectory() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    setPrivateField(schema, "baseDirectory", null);
    File targetFile = new File(tempDir.toFile(), "nobase_test.txt");
    schema.writeToStorage(targetFile.getAbsolutePath(), "test".getBytes(StandardCharsets.UTF_8));
    assertTrue(targetFile.exists());
  }

  @Test void testExistsInStorageWithoutBaseDirectory() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    setPrivateField(schema, "baseDirectory", null);
    File testFile = new File(tempDir.toFile(), "exists_nobase.txt");
    assertFalse(schema.existsInStorage(testFile.getAbsolutePath()));
    Files.write(testFile.toPath(), "data".getBytes(StandardCharsets.UTF_8));
    assertTrue(schema.existsInStorage(testFile.getAbsolutePath()));
  }

  @Test void testDeleteFromStorageWithoutBaseDirectory() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    setPrivateField(schema, "baseDirectory", null);
    File testFile = new File(tempDir.toFile(), "delete_nobase.txt");
    Files.write(testFile.toPath(), "data".getBytes(StandardCharsets.UTF_8));
    assertTrue(schema.deleteFromStorage(testFile.getAbsolutePath()));
    assertFalse(testFile.exists());
  }

  @Test void testCreateStorageDirectoriesWithoutBaseDirectory() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    setPrivateField(schema, "baseDirectory", null);
    File targetDir = new File(tempDir.toFile(), "nobase_dirs/sub");
    schema.createStorageDirectories(targetDir.getAbsolutePath());
    assertTrue(targetDir.exists() && targetDir.isDirectory());
  }

  // -----------------------------------------------------------------------
  // Storage operations with StorageProvider (InputStream, createDirectories)
  // -----------------------------------------------------------------------

  @Test void testWriteToStorageInputStreamWithProvider() throws Exception {
    StorageProvider mockProvider = mock(StorageProvider.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    InputStream content = new ByteArrayInputStream("stream data".getBytes(StandardCharsets.UTF_8));
    schema.writeToStorage("test.txt", content);
    verify(mockProvider).writeFile(anyString(), any(InputStream.class));
  }

  @Test void testCreateStorageDirectoriesWithProvider() throws Exception {
    StorageProvider mockProvider = mock(StorageProvider.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    schema.createStorageDirectories("some/path");
    verify(mockProvider).createDirectories(anyString());
  }

  // -----------------------------------------------------------------------
  // findMatchingFiles edge cases
  // -----------------------------------------------------------------------

  @Test void testFindMatchingFilesNullPattern() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    @SuppressWarnings("unchecked")
    List<String> result =
        (List<String>) invokePrivate(schema, "findMatchingFiles", new Class[]{String.class}, (String) null);
    assertTrue(result.isEmpty());
  }

  @Test void testFindMatchingFilesNullStorageProvider() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    @SuppressWarnings("unchecked")
    List<String> result =
        (List<String>) invokePrivate(schema, "findMatchingFiles", new Class[]{String.class}, "*.csv");
    assertTrue(result.isEmpty());
  }

  // -----------------------------------------------------------------------
  // addTable auto-detect for HTML without tableDef
  // -----------------------------------------------------------------------

  @Test void testAddTableAutoDetectHtmlWithoutTableDef() throws Exception {
    File htmlFile = new File(tempDir.toFile(), "page.html");
    Files.write(htmlFile.toPath(), "<html><body><table><tr><td>1</td></tr></table></body></html>"
        .getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(tempDir.toFile());
    Source source = Sources.of(htmlFile);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();

    Boolean result =
        (Boolean) invokePrivate(schema, "addTable", new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
            Source.class, String.class, Map.class},
        builder, source, "html_page", (Map<String, Object>) null);

    assertFalse(result);
  }

  // -----------------------------------------------------------------------
  // storeExplicitTableMapping and getExplicitTableName
  // -----------------------------------------------------------------------

  @Test void testStoreAndGetExplicitTableMapping() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File testFile = new File(tempDir.toFile(), "mapped.csv");
    Files.write(testFile.toPath(), "a\n1".getBytes(StandardCharsets.UTF_8));

    Source source = Sources.of(testFile);
    invokePrivate(schema, "storeExplicitTableMapping",
        new Class[]{String.class, Source.class}, "explicit_name", source);

    String result =
        (String) invokePrivate(schema, "getExplicitTableName", new Class[]{File.class}, testFile);
    assertEquals("explicit_name", result);
  }

  @Test void testGetExplicitTableNameNotFound() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    String result =
        (String) invokePrivate(schema, "getExplicitTableName", new Class[]{File.class}, new File("/nonexistent/file.csv"));
    assertNull(result);
  }

  // -----------------------------------------------------------------------
  // getTableNameForFile
  // -----------------------------------------------------------------------

  @Test void testGetTableNameForFileNoMatch() throws Exception {
    FileSchema schema = createSchemaWithTables(tempDir.toFile(), null);
    String result =
        (String) invokePrivate(schema, "getTableNameForFile", new Class[]{File.class}, new File("/some/random/file.json"));
    assertNull(result);
  }

  // -----------------------------------------------------------------------
  // processJsonFlattening edge cases
  // -----------------------------------------------------------------------

  @Test void testProcessJsonFlatteningNullBaseDirectory() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    setPrivateField(schema, "baseDirectory", null);
    invokePrivate(schema, "processJsonFlattening",
        new Class[]{File.class}, tempDir.toFile());
  }

  @Test void testProcessJsonFlatteningNoFlatten() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    invokePrivate(schema, "processJsonFlattening",
        new Class[]{File.class}, tempDir.toFile());
  }

  // -----------------------------------------------------------------------
  // convertSupportedFilesToJson edge cases
  // -----------------------------------------------------------------------

  @Test void testConvertSupportedFilesToJsonNullDir() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    invokePrivate(schema, "convertSupportedFilesToJson",
        new Class[]{File.class}, (File) null);
  }

  @Test void testConvertSupportedFilesToJsonNonExistentDir() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    invokePrivate(schema, "convertSupportedFilesToJson",
        new Class[]{File.class}, new File("/nonexistent/dir"));
  }

  @Test void testConvertStorageProviderFilesToJsonNullProvider() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    invokePrivate(schema, "convertStorageProviderFilesToJson", new Class[]{});
  }

  // -----------------------------------------------------------------------
  // getFilesForProcessing and getFilesFromStorageProvider
  // -----------------------------------------------------------------------

  @Test void testGetFilesForProcessingNullSourceDir() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    setPrivateField(schema, "sourceDirectory", null);
    File[] files = (File[]) invokePrivate(schema, "getFilesForProcessing", new Class[]{});
    assertEquals(0, files.length);
  }

  @Test void testGetFilesFromStorageProviderNull() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File[] files = (File[]) invokePrivate(schema, "getFilesFromStorageProvider", new Class[]{});
    assertEquals(0, files.length);
  }

  @Test void testProcessStorageProviderFilesNullProvider() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();
    invokePrivate(schema, "processStorageProviderFiles",
        new Class[]{com.google.common.collect.ImmutableMap.Builder.class, Map.class},
        builder, new HashMap<>());
  }

  // -----------------------------------------------------------------------
  // getFlattteningOptionsForFile
  // -----------------------------------------------------------------------

  @Test void testGetFlattteningOptionsForFileSchemaLevel() throws Exception {
    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), tempDir.toFile(), null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        null, null, true, null, false);

    File jsonFile = new File(tempDir.toFile(), "schemaflat.json");
    Files.write(jsonFile.toPath(), "[{\"a\":1}]".getBytes(StandardCharsets.UTF_8));

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) invokePrivate(schema, "getFlattteningOptionsForFile",
        new Class[]{File.class}, jsonFile);

    assertNotNull(result);
    assertEquals(true, result.get("flatten"));
    assertEquals("_", result.get("flattenSeparator"));
  }

  @Test void testGetFlattteningOptionsForFileNoFlatten() throws Exception {
    FileSchema schema = createSchemaWithTables(tempDir.toFile(), new ArrayList<>());
    File jsonFile = new File(tempDir.toFile(), "noflatten.json");
    Files.write(jsonFile.toPath(), "[{\"a\":1}]".getBytes(StandardCharsets.UTF_8));
    assertNull(
        invokePrivate(schema, "getFlattteningOptionsForFile",
        new Class[]{File.class}, jsonFile));
  }

  // -----------------------------------------------------------------------
  // materializeAlternatePartitions and processPartitionedTables edge cases
  // -----------------------------------------------------------------------

  @Test void testMaterializeAlternatePartitionsNullPartitions() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    invokePrivate(schema, "materializeAlternatePartitions", new Class[]{});
  }

  @Test void testProcessPartitionedTablesNullPartitions() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();
    invokePrivate(schema, "processPartitionedTables",
        new Class[]{com.google.common.collect.ImmutableMap.Builder.class}, builder);
  }

  // -----------------------------------------------------------------------
  // createAlternatePartitionTables and addIcebergMetadataTables edge cases
  // -----------------------------------------------------------------------

  @Test void testCreateAlternatePartitionTablesNullAlternates() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());

    org.apache.calcite.adapter.file.partition.PartitionedTableConfig config =
        mock(org.apache.calcite.adapter.file.partition.PartitionedTableConfig.class);
    when(config.getAlternatePartitions()).thenReturn(null);

    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();
    java.util.Set<String> processed = new java.util.HashSet<>();

    invokePrivate(schema, "createAlternatePartitionTables",
        new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
            java.util.Set.class,
            org.apache.calcite.adapter.file.partition.PartitionedTableConfig.class,
            boolean.class,
            org.apache.calcite.adapter.file.partition.PartitionDetector.PartitionInfo.class,
            Map.class},
        builder, processed, config, false, null, null);
  }

  @Test void testAddIcebergMetadataTablesNonIceberg() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    com.google.common.collect.ImmutableMap.Builder<String, Table> builder =
        com.google.common.collect.ImmutableMap.builder();
    Table nonIcebergTable = mock(Table.class);
    invokePrivate(schema, "addIcebergMetadataTables",
        new Class[]{com.google.common.collect.ImmutableMap.Builder.class,
            String.class, Table.class},
        builder, "test_table", nonIcebergTable);
    assertTrue(builder.build().isEmpty());
  }

  // -----------------------------------------------------------------------
  // discoverExistingIcebergTable edge cases
  // -----------------------------------------------------------------------

  @Test void testDiscoverExistingIcebergTableNullMetadata() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    setPrivateField(schema, "conversionMetadata", null);
    invokePrivate(schema, "discoverExistingIcebergTable",
        new Class[]{String.class, Map.class}, "test", new HashMap<>());
  }

  @Test void testDiscoverExistingIcebergTableNoMaterializeConfig() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    invokePrivate(schema, "discoverExistingIcebergTable",
        new Class[]{String.class, Map.class}, "test", new HashMap<>());
  }

  @Test void testDiscoverExistingIcebergTableNonIcebergFormat() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Object> config = new HashMap<>();
    Map<String, Object> materialize = new HashMap<>();
    materialize.put("format", "parquet");
    config.put("materialize", materialize);
    invokePrivate(schema, "discoverExistingIcebergTable",
        new Class[]{String.class, Map.class}, "test", config);
  }

  @Test void testDiscoverExistingIcebergTableNoIcebergConfig() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Object> config = new HashMap<>();
    Map<String, Object> materialize = new HashMap<>();
    materialize.put("format", "iceberg");
    config.put("materialize", materialize);
    invokePrivate(schema, "discoverExistingIcebergTable",
        new Class[]{String.class, Map.class}, "test", config);
  }

  // -----------------------------------------------------------------------
  // findOriginalSource
  // -----------------------------------------------------------------------

  @Test void testFindOriginalSourceNoMetadata() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File jsonFile = new File(tempDir.toFile(), "no_meta.json");
    Files.write(jsonFile.toPath(), "{}".getBytes(StandardCharsets.UTF_8));
    assertNull(
        invokePrivate(schema, "findOriginalSource",
        new Class[]{File.class}, jsonFile));
  }

  @Test void testFindOriginalSourceNullBaseDirectory() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    setPrivateField(schema, "baseDirectory", null);
    File jsonFile = new File(tempDir.toFile(), "no_base.json");
    Files.write(jsonFile.toPath(), "{}".getBytes(StandardCharsets.UTF_8));
    assertNull(
        invokePrivate(schema, "findOriginalSource",
        new Class[]{File.class}, jsonFile));
  }

  // -----------------------------------------------------------------------
  // generateModelFile coverage via getTableMap
  // -----------------------------------------------------------------------

  @Test void testGenerateModelFileWithTables() throws Exception {
    File srcDir = new File(tempDir.toFile(), "modelgen");
    srcDir.mkdirs();
    Files.write(new File(srcDir, "test.csv").toPath(),
        "a,b\n1,2".getBytes(StandardCharsets.UTF_8));

    FileSchema schema =
        new FileSchema(parentSchema, uniqueSchemaName(), srcDir, null, defaultEngineConfig);

    schema.getTableMap();

    File modelFile = new File(schema.getOperatingCacheDirectory(), ".generated-model.json");
    assertTrue(modelFile.exists(), "Model file should be generated");
  }
}
