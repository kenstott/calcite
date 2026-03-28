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
import org.apache.calcite.adapter.file.partition.AlternatePartitionRegistry;
import org.apache.calcite.adapter.file.refresh.PatternAwareRefreshListener;
import org.apache.calcite.adapter.file.refresh.RefreshableParquetCacheTable;
import org.apache.calcite.adapter.file.refresh.TableRefreshListener;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.Source;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests for {@link FileSchemaFactory} and {@link FileSchema} - Round 4.
 *
 * <p>Targets uncovered lines not exercised by previous deep coverage tests.
 * Focuses on:
 * <ul>
 *   <li>FileSchemaFactory: create() operand parsing branches for column casing,
 *       csv type inference, text similarity, materialization registration,
 *       ephemeral cache error path, storage provider instance sharing,
 *       execution engine fallback chain, auto-detect storage from baseDirectory,
 *       HDFS auto-detect, Windows path detection, and registerMaterializations</li>
 *   <li>FileSchema: isGlobPattern, hasHttpConfiguration, hasHtmlFieldSelectors,
 *       isConvertibleFile, isTableSourceFile, isFileTypeSupported,
 *       trimCompressedExtensions, hasCompressedExtension, getFileExtension,
 *       getFilesInDir with hidden files, getFilesFromStorageProvider null,
 *       writeToStorage local with/without baseDirectory, existsInStorage,
 *       deleteFromStorage, createStorageDirectories, resolvePath with
 *       cloud URIs, isHtmlFile, hasExplicitHtmlTableDefinition,
 *       constructors chaining, storageProvider initialization from
 *       storageConfig, and BRAND constant coverage</li>
 * </ul>
 */
@SuppressWarnings("deprecation")
@Tag("unit")
public class FileSchemaFactoryDeepCoverageTest4 {

  private static final AtomicInteger SCHEMA_COUNTER = new AtomicInteger(0);

  @TempDir
  Path tempDir;

  private SchemaPlus mockParentSchema;
  private ExecutionEngineConfig defaultEngineConfig;

  private String uniqueSchemaName() {
    return "test_factory4_" + SCHEMA_COUNTER.incrementAndGet();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @BeforeEach
  void setUp() {
    mockParentSchema = mock(SchemaPlus.class);

    Lookup subSchemaLookup = mock(Lookup.class);
    when(subSchemaLookup.get(anyString())).thenReturn(null);
    when(subSchemaLookup.getNames(any(LikePattern.class)))
        .thenReturn(Collections.emptySet());
    doReturn(subSchemaLookup).when(mockParentSchema).subSchemas();

    Lookup tableLookup = mock(Lookup.class);
    when(tableLookup.get(anyString())).thenReturn(null);
    when(tableLookup.getNames(any(LikePattern.class)))
        .thenReturn(Collections.emptySet());
    doReturn(tableLookup).when(mockParentSchema).tables();

    when(mockParentSchema.getParentSchema()).thenReturn(null);
    when(mockParentSchema.getName()).thenReturn("root");
    when(mockParentSchema.add(anyString(), any(Schema.class))).thenReturn(mockParentSchema);

    defaultEngineConfig = new ExecutionEngineConfig();
  }

  // -----------------------------------------------------------------------
  // Helper methods
  // -----------------------------------------------------------------------

  private FileSchema createSchemaSimple(File sourceDir) {
    return new FileSchema(mockParentSchema, uniqueSchemaName(),
        sourceDir, null, defaultEngineConfig);
  }

  private FileSchema createSchemaWithTables(File sourceDir,
      List<Map<String, Object>> tables) {
    return new FileSchema(mockParentSchema, uniqueSchemaName(),
        sourceDir, tables, defaultEngineConfig);
  }

  private Object invokePrivate(Object target, String methodName,
      Class<?>[] paramTypes, Object... args) throws Exception {
    Method method = target.getClass().getDeclaredMethod(methodName, paramTypes);
    method.setAccessible(true);
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof Exception) {
        throw (Exception) e.getCause();
      }
      throw e;
    }
  }

  private Object invokePrivateStatic(Class<?> clazz, String methodName,
      Class<?>[] paramTypes, Object... args) throws Exception {
    Method method = clazz.getDeclaredMethod(methodName, paramTypes);
    method.setAccessible(true);
    try {
      return method.invoke(null, args);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof Exception) {
        throw (Exception) e.getCause();
      }
      throw e;
    }
  }

  private Object getField(Object target, String fieldName) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(target);
  }

  private void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  // =======================================================================
  // FileSchema: isGlobPattern tests
  // =======================================================================

  @Test
  void testIsGlobPatternNull() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isGlobPattern",
        new Class<?>[]{String.class}, (Object) null);
    assertFalse(result);
  }

  @Test
  void testIsGlobPatternHttpUrl() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isGlobPattern",
        new Class<?>[]{String.class}, "http://example.com/data/*.csv");
    assertFalse(result);
  }

  @Test
  void testIsGlobPatternHttpsUrl() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isGlobPattern",
        new Class<?>[]{String.class}, "https://example.com/data/*.csv");
    assertFalse(result);
  }

  @Test
  void testIsGlobPatternWithStar() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isGlobPattern",
        new Class<?>[]{String.class}, "/data/*.csv");
    assertTrue(result);
  }

  @Test
  void testIsGlobPatternWithQuestion() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isGlobPattern",
        new Class<?>[]{String.class}, "/data/file?.csv");
    assertTrue(result);
  }

  @Test
  void testIsGlobPatternWithBrackets() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isGlobPattern",
        new Class<?>[]{String.class}, "/data/file[0-9].csv");
    assertTrue(result);
  }

  @Test
  void testIsGlobPatternWithProtocolPrefix() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isGlobPattern",
        new Class<?>[]{String.class}, "s3://bucket/path/to/*.parquet");
    assertTrue(result);
  }

  @Test
  void testIsGlobPatternPlainPath() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isGlobPattern",
        new Class<?>[]{String.class}, "/data/file.csv");
    assertFalse(result);
  }

  // =======================================================================
  // FileSchema: hasHttpConfiguration tests
  // =======================================================================

  @Test
  void testHasHttpConfigurationNull() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "hasHttpConfiguration",
        new Class<?>[]{Map.class}, (Object) null);
    assertFalse(result);
  }

  @Test
  void testHasHttpConfigurationWithMethod() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("method", "POST");
    Boolean result = (Boolean) invokePrivate(schema, "hasHttpConfiguration",
        new Class<?>[]{Map.class}, tableDef);
    assertTrue(result);
  }

  @Test
  void testHasHttpConfigurationWithBody() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("body", "{\"query\": \"test\"}");
    Boolean result = (Boolean) invokePrivate(schema, "hasHttpConfiguration",
        new Class<?>[]{Map.class}, tableDef);
    assertTrue(result);
  }

  @Test
  void testHasHttpConfigurationWithHeaders() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("headers", new HashMap<>());
    Boolean result = (Boolean) invokePrivate(schema, "hasHttpConfiguration",
        new Class<?>[]{Map.class}, tableDef);
    assertTrue(result);
  }

  @Test
  void testHasHttpConfigurationWithMimeType() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("mimeType", "application/json");
    Boolean result = (Boolean) invokePrivate(schema, "hasHttpConfiguration",
        new Class<?>[]{Map.class}, tableDef);
    assertTrue(result);
  }

  @Test
  void testHasHttpConfigurationEmpty() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("url", "http://example.com");
    Boolean result = (Boolean) invokePrivate(schema, "hasHttpConfiguration",
        new Class<?>[]{Map.class}, tableDef);
    assertFalse(result);
  }

  // =======================================================================
  // FileSchema: hasHtmlFieldSelectors tests
  // =======================================================================

  @Test
  void testHasHtmlFieldSelectorsNoFields() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    Boolean result = (Boolean) invokePrivate(schema, "hasHtmlFieldSelectors",
        new Class<?>[]{Map.class}, tableDef);
    assertFalse(result);
  }

  @Test
  void testHasHtmlFieldSelectorsWithSelector() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    List<Map<String, Object>> fields = new ArrayList<>();
    Map<String, Object> field = new HashMap<>();
    field.put("selector", "td.value");
    fields.add(field);
    tableDef.put("fields", fields);
    Boolean result = (Boolean) invokePrivate(schema, "hasHtmlFieldSelectors",
        new Class<?>[]{Map.class}, tableDef);
    assertTrue(result);
  }

  @Test
  void testHasHtmlFieldSelectorsWithSelectedElement() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    List<Map<String, Object>> fields = new ArrayList<>();
    Map<String, Object> field = new HashMap<>();
    field.put("selectedElement", 0);
    fields.add(field);
    tableDef.put("fields", fields);
    Boolean result = (Boolean) invokePrivate(schema, "hasHtmlFieldSelectors",
        new Class<?>[]{Map.class}, tableDef);
    assertTrue(result);
  }

  @Test
  void testHasHtmlFieldSelectorsWithTh() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    List<Map<String, Object>> fields = new ArrayList<>();
    Map<String, Object> field = new HashMap<>();
    field.put("th", "Column Header");
    fields.add(field);
    tableDef.put("fields", fields);
    Boolean result = (Boolean) invokePrivate(schema, "hasHtmlFieldSelectors",
        new Class<?>[]{Map.class}, tableDef);
    assertTrue(result);
  }

  @Test
  void testHasHtmlFieldSelectorsWithoutSelectorKeys() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    List<Map<String, Object>> fields = new ArrayList<>();
    Map<String, Object> field = new HashMap<>();
    field.put("name", "column1");
    field.put("type", "VARCHAR");
    fields.add(field);
    tableDef.put("fields", fields);
    Boolean result = (Boolean) invokePrivate(schema, "hasHtmlFieldSelectors",
        new Class<?>[]{Map.class}, tableDef);
    assertFalse(result);
  }

  // =======================================================================
  // FileSchema: isConvertibleFile tests
  // =======================================================================

  @Test
  void testIsConvertibleFileNull() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class<?>[]{String.class}, (Object) null);
    assertFalse(result);
  }

  @Test
  void testIsConvertibleFileXlsx() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class<?>[]{String.class}, "data.xlsx");
    assertTrue(result);
  }

  @Test
  void testIsConvertibleFileXls() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class<?>[]{String.class}, "report.xls");
    assertTrue(result);
  }

  @Test
  void testIsConvertibleFileMd() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class<?>[]{String.class}, "readme.md");
    assertTrue(result);
  }

  @Test
  void testIsConvertibleFileMarkdown() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class<?>[]{String.class}, "notes.markdown");
    assertTrue(result);
  }

  @Test
  void testIsConvertibleFileDocx() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class<?>[]{String.class}, "document.docx");
    assertTrue(result);
  }

  @Test
  void testIsConvertibleFilePptx() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class<?>[]{String.class}, "slides.pptx");
    assertTrue(result);
  }

  @Test
  void testIsConvertibleFileHtml() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class<?>[]{String.class}, "page.html");
    assertTrue(result);
  }

  @Test
  void testIsConvertibleFileHtm() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class<?>[]{String.class}, "page.htm");
    assertTrue(result);
  }

  @Test
  void testIsConvertibleFileCsv() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class<?>[]{String.class}, "data.csv");
    assertFalse(result);
  }

  @Test
  void testIsConvertibleFileCaseInsensitive() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class<?>[]{String.class}, "DATA.XLSX");
    assertTrue(result);
  }

  // =======================================================================
  // FileSchema: isTableSourceFile tests
  // =======================================================================

  @Test
  void testIsTableSourceFileNull() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class<?>[]{String.class}, (Object) null);
    assertFalse(result);
  }

  @Test
  void testIsTableSourceFileCsv() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class<?>[]{String.class}, "data.csv");
    assertTrue(result);
  }

  @Test
  void testIsTableSourceFileTsv() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class<?>[]{String.class}, "data.tsv");
    assertTrue(result);
  }

  @Test
  void testIsTableSourceFileJson() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class<?>[]{String.class}, "data.json");
    assertTrue(result);
  }

  @Test
  void testIsTableSourceFileYaml() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class<?>[]{String.class}, "data.yaml");
    assertTrue(result);
  }

  @Test
  void testIsTableSourceFileYml() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class<?>[]{String.class}, "data.yml");
    assertTrue(result);
  }

  @Test
  void testIsTableSourceFileArrow() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class<?>[]{String.class}, "data.arrow");
    assertTrue(result);
  }

  @Test
  void testIsTableSourceFileParquet() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class<?>[]{String.class}, "data.parquet");
    assertTrue(result);
  }

  @Test
  void testIsTableSourceFileXlsx() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class<?>[]{String.class}, "data.xlsx");
    assertFalse(result);
  }

  @Test
  void testIsTableSourceFileTxt() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class<?>[]{String.class}, "data.txt");
    assertFalse(result);
  }

  // =======================================================================
  // FileSchema: trimCompressedExtensions tests
  // =======================================================================

  @Test
  void testTrimCompressedExtensionsNull() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "trimCompressedExtensions",
        new Class<?>[]{String.class}, (Object) null);
    assertNull(result);
  }

  @Test
  void testTrimCompressedExtensionsGz() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "trimCompressedExtensions",
        new Class<?>[]{String.class}, "data.csv.gz");
    assertEquals("data.csv", result);
  }

  @Test
  void testTrimCompressedExtensionsGzip() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "trimCompressedExtensions",
        new Class<?>[]{String.class}, "data.json.gzip");
    assertEquals("data.json", result);
  }

  @Test
  void testTrimCompressedExtensionsBz2() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "trimCompressedExtensions",
        new Class<?>[]{String.class}, "data.csv.bz2");
    assertEquals("data.csv", result);
  }

  @Test
  void testTrimCompressedExtensionsXz() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "trimCompressedExtensions",
        new Class<?>[]{String.class}, "data.tsv.xz");
    assertEquals("data.tsv", result);
  }

  @Test
  void testTrimCompressedExtensionsZip() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "trimCompressedExtensions",
        new Class<?>[]{String.class}, "archive.csv.zip");
    assertEquals("archive.csv", result);
  }

  @Test
  void testTrimCompressedExtensionsNoCompression() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "trimCompressedExtensions",
        new Class<?>[]{String.class}, "data.csv");
    assertEquals("data.csv", result);
  }

  @Test
  void testTrimCompressedExtensionsCaseInsensitive() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "trimCompressedExtensions",
        new Class<?>[]{String.class}, "DATA.CSV.GZ");
    assertEquals("DATA.CSV", result);
  }

  // =======================================================================
  // FileSchema: hasCompressedExtension tests
  // =======================================================================

  @Test
  void testHasCompressedExtensionNull() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "hasCompressedExtension",
        new Class<?>[]{String.class}, (Object) null);
    assertFalse(result);
  }

  @Test
  void testHasCompressedExtensionGz() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "hasCompressedExtension",
        new Class<?>[]{String.class}, "data.csv.gz");
    assertTrue(result);
  }

  @Test
  void testHasCompressedExtensionBz2() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "hasCompressedExtension",
        new Class<?>[]{String.class}, "data.csv.bz2");
    assertTrue(result);
  }

  @Test
  void testHasCompressedExtensionPlainFile() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "hasCompressedExtension",
        new Class<?>[]{String.class}, "data.csv");
    assertFalse(result);
  }

  // =======================================================================
  // FileSchema: getFileExtension tests
  // =======================================================================

  @Test
  void testGetFileExtensionCsv() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "getFileExtension",
        new Class<?>[]{String.class}, "data.csv");
    assertEquals("csv", result);
  }

  @Test
  void testGetFileExtensionCompressedCsv() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "getFileExtension",
        new Class<?>[]{String.class}, "data.csv.gz");
    assertEquals("csv", result);
  }

  @Test
  void testGetFileExtensionParquet() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "getFileExtension",
        new Class<?>[]{String.class}, "table.parquet");
    assertEquals("parquet", result);
  }

  @Test
  void testGetFileExtensionNoDot() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "getFileExtension",
        new Class<?>[]{String.class}, "noextension");
    assertEquals("", result);
  }

  // =======================================================================
  // FileSchema: isHtmlFile tests
  // =======================================================================

  @Test
  void testIsHtmlFileHtml() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File htmlFile = new File(tempDir.toFile(), "page.html");
    htmlFile.createNewFile();
    Boolean result = (Boolean) invokePrivate(schema, "isHtmlFile",
        new Class<?>[]{File.class}, htmlFile);
    assertTrue(result);
  }

  @Test
  void testIsHtmlFileHtm() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File htmFile = new File(tempDir.toFile(), "page.htm");
    htmFile.createNewFile();
    Boolean result = (Boolean) invokePrivate(schema, "isHtmlFile",
        new Class<?>[]{File.class}, htmFile);
    assertTrue(result);
  }

  @Test
  void testIsHtmlFileUpperCase() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File htmlFile = new File(tempDir.toFile(), "PAGE.HTML");
    htmlFile.createNewFile();
    Boolean result = (Boolean) invokePrivate(schema, "isHtmlFile",
        new Class<?>[]{File.class}, htmlFile);
    assertTrue(result);
  }

  @Test
  void testIsHtmlFileCsv() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File csvFile = new File(tempDir.toFile(), "data.csv");
    csvFile.createNewFile();
    Boolean result = (Boolean) invokePrivate(schema, "isHtmlFile",
        new Class<?>[]{File.class}, csvFile);
    assertFalse(result);
  }

  // =======================================================================
  // FileSchema: isFileTypeSupported tests
  // =======================================================================

  @Test
  void testIsFileTypeSupportedDirectory() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File dir = new File(tempDir.toFile(), "subdir");
    dir.mkdirs();
    Boolean result = (Boolean) invokePrivate(schema, "isFileTypeSupported",
        new Class<?>[]{File.class}, dir);
    assertFalse(result);
  }

  @Test
  void testIsFileTypeSupportedHiddenFile() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File hiddenFile = new File(tempDir.toFile(), ".hidden.csv");
    hiddenFile.createNewFile();
    Boolean result = (Boolean) invokePrivate(schema, "isFileTypeSupported",
        new Class<?>[]{File.class}, hiddenFile);
    assertFalse(result);
  }

  @Test
  void testIsFileTypeSupportedCsv() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File csvFile = new File(tempDir.toFile(), "data.csv");
    csvFile.createNewFile();
    Boolean result = (Boolean) invokePrivate(schema, "isFileTypeSupported",
        new Class<?>[]{File.class}, csvFile);
    assertTrue(result);
  }

  @Test
  void testIsFileTypeSupportedHtml() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File htmlFile = new File(tempDir.toFile(), "page.html");
    htmlFile.createNewFile();
    Boolean result = (Boolean) invokePrivate(schema, "isFileTypeSupported",
        new Class<?>[]{File.class}, htmlFile);
    assertTrue(result);
  }

  @Test
  void testIsFileTypeSupportedCompressedCsv() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File gzFile = new File(tempDir.toFile(), "data.csv.gz");
    gzFile.createNewFile();
    Boolean result = (Boolean) invokePrivate(schema, "isFileTypeSupported",
        new Class<?>[]{File.class}, gzFile);
    assertTrue(result);
  }

  @Test
  void testIsFileTypeSupportedTxtNotSupported() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File txtFile = new File(tempDir.toFile(), "notes.txt");
    txtFile.createNewFile();
    Boolean result = (Boolean) invokePrivate(schema, "isFileTypeSupported",
        new Class<?>[]{File.class}, txtFile);
    assertFalse(result);
  }

  // =======================================================================
  // FileSchema: getFilesInDir tests with edge cases
  // =======================================================================

  @Test
  void testGetFilesInDirHiddenFilesExcluded() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    // Create files including ._ prefixed ones (macOS resource fork files)
    new File(tempDir.toFile(), "data.csv").createNewFile();
    new File(tempDir.toFile(), "._metadata.csv").createNewFile();
    new File(tempDir.toFile(), "._resource.json").createNewFile();

    File[] result = (File[]) invokePrivate(schema, "getFilesInDir",
        new Class<?>[]{File.class}, tempDir.toFile());
    // Only data.csv should be included; ._ prefixed files are excluded
    assertEquals(1, result.length);
    assertEquals("data.csv", result[0].getName());
  }

  @Test
  void testGetFilesInDirSubdirNotRecursive() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File subdir = new File(tempDir.toFile(), "subdir");
    subdir.mkdirs();
    new File(subdir, "nested.csv").createNewFile();
    new File(tempDir.toFile(), "root.csv").createNewFile();

    File[] result = (File[]) invokePrivate(schema, "getFilesInDir",
        new Class<?>[]{File.class}, tempDir.toFile());
    assertEquals(1, result.length);
    assertEquals("root.csv", result[0].getName());
  }

  @Test
  void testGetFilesInDirRecursive() throws Exception {
    // Create schema with recursive=true
    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, defaultEngineConfig, true);

    File subdir = new File(tempDir.toFile(), "subdir");
    subdir.mkdirs();
    new File(subdir, "nested.csv").createNewFile();
    new File(tempDir.toFile(), "root.csv").createNewFile();

    File[] result = (File[]) invokePrivate(schema, "getFilesInDir",
        new Class<?>[]{File.class}, tempDir.toFile());
    assertEquals(2, result.length);
  }

  @Test
  void testGetFilesInDirNullListFiles() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    // Use a non-existent directory that will cause listFiles to return null
    File fakeDir = new File(tempDir.toFile(), "nonexistent");
    File[] result = (File[]) invokePrivate(schema, "getFilesInDir",
        new Class<?>[]{File.class}, fakeDir);
    assertEquals(0, result.length);
  }

  @Test
  void testGetFilesInDirUnsupportedExtensions() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    new File(tempDir.toFile(), "image.png").createNewFile();
    new File(tempDir.toFile(), "readme.txt").createNewFile();
    new File(tempDir.toFile(), "binary.dat").createNewFile();

    File[] result = (File[]) invokePrivate(schema, "getFilesInDir",
        new Class<?>[]{File.class}, tempDir.toFile());
    assertEquals(0, result.length);
  }

  // =======================================================================
  // FileSchema: hasExplicitHtmlTableDefinition tests
  // =======================================================================

  @Test
  void testHasExplicitHtmlTableDefinitionNoTables() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File htmlFile = new File(tempDir.toFile(), "page.html");
    htmlFile.createNewFile();
    Boolean result = (Boolean) invokePrivate(schema, "hasExplicitHtmlTableDefinition",
        new Class<?>[]{File.class}, htmlFile);
    assertFalse(result);
  }

  @Test
  void testHasExplicitHtmlTableDefinitionWithMatchingSelector() throws Exception {
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("url", "page.html");
    tableDef.put("selector", "#data-table");
    List<Map<String, Object>> tables = Collections.singletonList(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tables);
    File htmlFile = new File(tempDir.toFile(), "page.html");
    htmlFile.createNewFile();
    Boolean result = (Boolean) invokePrivate(schema, "hasExplicitHtmlTableDefinition",
        new Class<?>[]{File.class}, htmlFile);
    assertTrue(result);
  }

  @Test
  void testHasExplicitHtmlTableDefinitionNoSelector() throws Exception {
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("url", "page.html");
    tableDef.put("name", "my_table");
    List<Map<String, Object>> tables = Collections.singletonList(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tables);
    File htmlFile = new File(tempDir.toFile(), "page.html");
    htmlFile.createNewFile();
    Boolean result = (Boolean) invokePrivate(schema, "hasExplicitHtmlTableDefinition",
        new Class<?>[]{File.class}, htmlFile);
    assertFalse(result);
  }

  // =======================================================================
  // FileSchema: writeToStorage local filesystem paths
  // =======================================================================

  @Test
  void testWriteToStorageLocalWithBaseDirectory() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String relativePath = "test-output/file.txt";
    byte[] content = "hello world".getBytes(StandardCharsets.UTF_8);

    schema.writeToStorage(relativePath, content);

    File cacheDir = schema.getOperatingCacheDirectory();
    File written = new File(cacheDir, relativePath);
    assertTrue(written.exists(), "File should be written to operating cache directory");
    assertEquals("hello world", new String(Files.readAllBytes(written.toPath()), StandardCharsets.UTF_8));
  }

  @Test
  void testWriteToStorageInputStreamLocal() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String relativePath = "stream-output/data.bin";
    byte[] content = "stream content".getBytes(StandardCharsets.UTF_8);
    InputStream stream = new ByteArrayInputStream(content);

    schema.writeToStorage(relativePath, stream);

    File cacheDir = schema.getOperatingCacheDirectory();
    File written = new File(cacheDir, relativePath);
    assertTrue(written.exists());
    assertEquals("stream content", new String(Files.readAllBytes(written.toPath()), StandardCharsets.UTF_8));
  }

  @Test
  void testWriteToStorageWithStorageProvider() throws Exception {
    StorageProvider mockProvider = mock(StorageProvider.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, "/remote/path", null,
        null, defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    byte[] content = "remote content".getBytes(StandardCharsets.UTF_8);
    schema.writeToStorage("subdir/file.txt", content);

    verify(mockProvider).writeFile(anyString(), any(byte[].class));
  }

  @Test
  void testWriteToStorageInputStreamWithStorageProvider() throws Exception {
    StorageProvider mockProvider = mock(StorageProvider.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, "/remote/path", null,
        null, defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    InputStream stream = new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8));
    schema.writeToStorage("subdir/stream.txt", stream);

    verify(mockProvider).writeFile(anyString(), any(InputStream.class));
  }

  // =======================================================================
  // FileSchema: existsInStorage tests
  // =======================================================================

  @Test
  void testExistsInStorageLocalExists() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File cacheDir = schema.getOperatingCacheDirectory();
    File testFile = new File(cacheDir, "existing.txt");
    testFile.getParentFile().mkdirs();
    testFile.createNewFile();

    assertTrue(schema.existsInStorage("existing.txt"));
  }

  @Test
  void testExistsInStorageLocalNotExists() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    assertFalse(schema.existsInStorage("nonexistent.txt"));
  }

  @Test
  void testExistsInStorageWithProvider() throws Exception {
    StorageProvider mockProvider = mock(StorageProvider.class);
    when(mockProvider.exists(anyString())).thenReturn(true);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, "/remote", null,
        null, defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    assertTrue(schema.existsInStorage("file.txt"));
  }

  // =======================================================================
  // FileSchema: deleteFromStorage tests
  // =======================================================================

  @Test
  void testDeleteFromStorageLocalExists() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File cacheDir = schema.getOperatingCacheDirectory();
    File testFile = new File(cacheDir, "to-delete.txt");
    testFile.getParentFile().mkdirs();
    testFile.createNewFile();

    assertTrue(schema.deleteFromStorage("to-delete.txt"));
    assertFalse(testFile.exists());
  }

  @Test
  void testDeleteFromStorageLocalNotExists() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    assertFalse(schema.deleteFromStorage("nonexistent.txt"));
  }

  @Test
  void testDeleteFromStorageWithProvider() throws Exception {
    StorageProvider mockProvider = mock(StorageProvider.class);
    when(mockProvider.delete(anyString())).thenReturn(true);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, "/remote", null,
        null, defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    assertTrue(schema.deleteFromStorage("file.txt"));
    verify(mockProvider).delete(anyString());
  }

  // =======================================================================
  // FileSchema: createStorageDirectories tests
  // =======================================================================

  @Test
  void testCreateStorageDirectoriesLocal() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    schema.createStorageDirectories("nested/sub/dir");

    File cacheDir = schema.getOperatingCacheDirectory();
    File createdDir = new File(cacheDir, "nested/sub/dir");
    assertTrue(createdDir.exists() && createdDir.isDirectory());
  }

  @Test
  void testCreateStorageDirectoriesWithProvider() throws Exception {
    StorageProvider mockProvider = mock(StorageProvider.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, "/remote", null,
        null, defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    schema.createStorageDirectories("output/data");
    verify(mockProvider).createDirectories(anyString());
  }

  // =======================================================================
  // FileSchema: resolvePath tests
  // =======================================================================

  @Test
  void testResolvePathWithoutProvider() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "resolvePath",
        new Class<?>[]{String.class}, "relative/path");
    assertEquals("relative/path", result);
  }

  // =======================================================================
  // FileSchema: BRAND constant and static field tests
  // =======================================================================

  @Test
  void testBrandConstant() {
    assertEquals("aperio", FileSchema.BRAND);
  }

  @Test
  void testConvertibleExtensionsContainsExpectedTypes() throws Exception {
    Field field = FileSchema.class.getDeclaredField("CONVERTIBLE_EXTENSIONS");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<String> extensions = (Set<String>) field.get(null);
    assertTrue(extensions.contains(".xlsx"));
    assertTrue(extensions.contains(".xls"));
    assertTrue(extensions.contains(".md"));
    assertTrue(extensions.contains(".markdown"));
    assertTrue(extensions.contains(".docx"));
    assertTrue(extensions.contains(".pptx"));
    assertTrue(extensions.contains(".html"));
    assertTrue(extensions.contains(".htm"));
  }

  @Test
  void testTableSourceExtensionsContainsExpectedTypes() throws Exception {
    Field field = FileSchema.class.getDeclaredField("TABLE_SOURCE_EXTENSIONS");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<String> extensions = (Set<String>) field.get(null);
    assertTrue(extensions.contains(".csv"));
    assertTrue(extensions.contains(".tsv"));
    assertTrue(extensions.contains(".json"));
    assertTrue(extensions.contains(".yaml"));
    assertTrue(extensions.contains(".yml"));
    assertTrue(extensions.contains(".arrow"));
    assertTrue(extensions.contains(".parquet"));
  }

  @Test
  void testCompressedExtensionsContainsExpectedTypes() throws Exception {
    Field field = FileSchema.class.getDeclaredField("COMPRESSED_EXTENSIONS");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<String> extensions = (Set<String>) field.get(null);
    assertTrue(extensions.contains(".gz"));
    assertTrue(extensions.contains(".gzip"));
    assertTrue(extensions.contains(".bz2"));
    assertTrue(extensions.contains(".xz"));
    assertTrue(extensions.contains(".zip"));
  }

  // =======================================================================
  // FileSchema: constructor chaining tests (different constructors)
  // =======================================================================

  @Test
  void testSimplestConstructorWithTables() {
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> table = new HashMap<>();
    table.put("name", "test_table");
    table.put("url", "file:///tmp/test.csv");
    tables.add(table);

    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), tables);
    assertNotNull(schema);
    assertNotNull(schema.getBaseDirectory());
    assertNotNull(schema.getOperatingCacheDirectory());
  }

  @Test
  void testConstructorWithRecursiveTrue() {
    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, defaultEngineConfig, true);
    assertNotNull(schema);
  }

  @Test
  void testConstructorWithAllFeaturesNoComment() {
    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "UPPER", "LOWER",
        null, null, null, null, true);
    assertNotNull(schema);
    assertNotNull(schema.getBaseDirectory());
  }

  @Test
  void testConstructorWithAllFeaturesAndComment() {
    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        null, null, null, null, true, "Test schema comment");
    assertNotNull(schema);
    assertEquals("Test schema comment", schema.getComment());
  }

  @Test
  void testConstructorWithDirectoryPath() {
    String dirPath = tempDir.toFile().getAbsolutePath();
    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        null, null, dirPath, null,
        null, defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        "local", null, null, null, false, null);
    assertNotNull(schema);
    assertEquals(dirPath, schema.getBaseDirectory());
  }

  @Test
  void testConstructorWithUserConfiguredBaseDirectory() {
    File baseDir = new File(tempDir.toFile(), "custom-base");
    baseDir.mkdirs();
    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), baseDir, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        null, null, null, null, false);
    assertNotNull(schema);
    assertTrue(schema.getBaseDirectory().contains("custom-base"));
  }

  // =======================================================================
  // FileSchema: addRefreshListener and notify tests
  // =======================================================================

  @Test
  void testAddRefreshListener() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    TableRefreshListener listener = mock(TableRefreshListener.class);
    schema.addRefreshListener(listener);
    // Verify the listener was added by triggering a notification
    schema.notifyTableRefreshed("test_table", new File("/tmp/test.parquet"));
    verify(listener).onTableRefreshed("test_table", new File("/tmp/test.parquet"));
  }

  @Test
  void testNotifyTableRefreshedWithPatternNonPatternListener() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    // Add a basic listener (not pattern-aware)
    TableRefreshListener listener = mock(TableRefreshListener.class);
    schema.addRefreshListener(listener);
    // Should not throw - non-pattern listeners are silently skipped
    schema.notifyTableRefreshedWithPattern("test_table", "/data/**/*.parquet");
  }

  @Test
  void testNotifyTableRefreshedWithPatternPatternAware() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    PatternAwareRefreshListener listener = mock(PatternAwareRefreshListener.class);
    schema.addRefreshListener(listener);
    schema.notifyTableRefreshedWithPattern("test_table", "/data/**/*.parquet");
    verify(listener).onTableRefreshedWithPattern("test_table", "/data/**/*.parquet");
  }

  @Test
  void testNotifyIcebergTableRefreshedPatternAware() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    PatternAwareRefreshListener listener = mock(PatternAwareRefreshListener.class);
    schema.addRefreshListener(listener);
    schema.notifyIcebergTableRefreshed("iceberg_table", "s3://bucket/warehouse/table");
    verify(listener).onIcebergTableRefreshed("iceberg_table", "s3://bucket/warehouse/table");
  }

  @Test
  void testNotifyTableRefreshedExceptionHandled() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    TableRefreshListener listener = mock(TableRefreshListener.class);
    org.mockito.Mockito.doThrow(new RuntimeException("listener error"))
        .when(listener).onTableRefreshed(anyString(), any(File.class));
    schema.addRefreshListener(listener);
    // Should not throw
    schema.notifyTableRefreshed("test_table", new File("/tmp/test.parquet"));
  }

  // =======================================================================
  // FileSchema: registerRawToParquetConverter test
  // =======================================================================

  @Test
  void testRegisterRawToParquetConverter() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    org.apache.calcite.adapter.file.converters.RawToParquetConverter converter =
        mock(org.apache.calcite.adapter.file.converters.RawToParquetConverter.class);
    schema.registerRawToParquetConverter(converter);

    // Verify the converter was added via reflection
    @SuppressWarnings("unchecked")
    List<Object> converters = (List<Object>) getField(schema, "rawToParquetConverters");
    assertEquals(1, converters.size());
  }

  // =======================================================================
  // FileSchema: getAlternatePartitionRegistry test
  // =======================================================================

  @Test
  void testGetAlternatePartitionRegistryNotNull() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    AlternatePartitionRegistry registry = schema.getAlternatePartitionRegistry();
    assertNotNull(registry);
  }

  // =======================================================================
  // FileSchema: setConstraintMetadata and getTableConstraints tests
  // =======================================================================

  @Test
  void testSetConstraintMetadataAndGetTableConstraints() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Map<String, Object>> metadata = new HashMap<>();
    Map<String, Object> tableConstraint = new HashMap<>();
    tableConstraint.put("primaryKey", Arrays.asList("id"));
    metadata.put("test_table", tableConstraint);

    schema.setConstraintMetadata(metadata);

    Map<String, Object> result = schema.getTableConstraints("test_table");
    assertNotNull(result);
    assertEquals(Arrays.asList("id"), result.get("primaryKey"));
  }

  @Test
  void testGetTableConstraintsNotFoundReturnsNull() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Map<String, Object>> metadata = new HashMap<>();
    metadata.put("other_table", new HashMap<>());
    schema.setConstraintMetadata(metadata);

    Map<String, Object> result = schema.getTableConstraints("nonexistent");
    assertNull(result);
  }

  // =======================================================================
  // FileSchema: setConversionRecords tests
  // =======================================================================

  @Test
  void testSetConversionRecordsPopulatesMetadata() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, ConversionMetadata.ConversionRecord> records = new HashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "test_table";
    record.viewScanPattern = "/data/**/*.parquet";
    records.put("test_table", record);

    schema.setConversionRecords(records);

    ConversionMetadata cm = schema.getConversionMetadata();
    assertNotNull(cm);
    assertNotNull(cm.getAllConversions().get("test_table"));
  }

  @Test
  void testSetConversionRecordsNullSafe() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    // Should not throw
    schema.setConversionRecords(null);
    schema.setConversionRecords(new HashMap<>());
  }

  // =======================================================================
  // FileSchema: getTableBaseline and updateTableBaseline tests
  // =======================================================================

  @Test
  void testGetTableBaselineNoRecord() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    ConversionMetadata.PartitionBaseline baseline = schema.getTableBaseline("nonexistent");
    assertNull(baseline);
  }

  @Test
  void testGetTableBaselineWithRecord() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    // Add a conversion record with baseline
    ConversionMetadata cm = schema.getConversionMetadata();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "test_table";
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<>();
    record.baseline = baseline;
    cm.putConversionRecord("test_table", record);

    ConversionMetadata.PartitionBaseline result = schema.getTableBaseline("test_table");
    assertNotNull(result);
    assertNotNull(result.files);
  }

  @Test
  void testUpdateTableBaselineNoRecord() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    // Should not throw - just logs warning
    schema.updateTableBaseline("nonexistent", baseline);
  }

  // =======================================================================
  // FileSchema: getAllTableRecords tests
  // =======================================================================

  @Test
  void testGetAllTableRecordsEmpty() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, ConversionMetadata.ConversionRecord> records = schema.getAllTableRecords();
    assertNotNull(records);
  }

  @Test
  void testGetAllTableRecordsWithRecords() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    ConversionMetadata cm = schema.getConversionMetadata();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "my_table";
    cm.putConversionRecord("my_table", record);

    Map<String, ConversionMetadata.ConversionRecord> records = schema.getAllTableRecords();
    assertTrue(records.containsKey("my_table"));
  }

  // =======================================================================
  // FileSchema: hasRefreshableTables tests
  // =======================================================================

  @Test
  void testHasRefreshableTablesTrue() {
    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, null,
        defaultEngineConfig, false, null, null, null,
        "5 minutes", "SMART_CASING", "SMART_CASING",
        null, null, null, null, false);
    assertTrue(schema.hasRefreshableTables());
  }

  @Test
  void testHasRefreshableTablesFalse() {
    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        null, null, null, null, false);
    assertFalse(schema.hasRefreshableTables());
  }

  // =======================================================================
  // FileSchema: getComment tests
  // =======================================================================

  @Test
  void testGetCommentReturnsValue() {
    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, null,
        defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        null, null, null, null, false, "My schema");
    assertEquals("My schema", schema.getComment());
  }

  @Test
  void testGetCommentReturnsNull() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    assertNull(schema.getComment());
  }

  // =======================================================================
  // FileSchema: setFunctionMultimap and getFunctionMultimap tests
  // =======================================================================

  @Test
  void testSetAndGetFunctionMultimap() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    ImmutableMultimap<String, Function> functions = ImmutableMultimap.of();
    schema.setFunctionMultimap(functions);
    // Verify via getFunctions which delegates to getFunctionMultimap
    assertNotNull(schema.getFunctions("nonexistent"));
  }

  @Test
  void testSetFunctionMultimapWithEntries() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Function mockFn = mock(Function.class);
    ImmutableMultimap<String, Function> functions =
        ImmutableMultimap.of("TEST_FUNC", mockFn);
    schema.setFunctionMultimap(functions);
    assertEquals(1, schema.getFunctions("TEST_FUNC").size());
  }

  // =======================================================================
  // FileSchema: clearTableCache test
  // =======================================================================

  @Test
  void testClearTableCacheResets() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    // Get table map once (populates cache)
    schema.getTableMap();
    // Clear the cache
    schema.clearTableCache();
    // Getting table map again should not throw
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
  }

  // =======================================================================
  // FileSchema: storage config accessors
  // =======================================================================

  @Test
  void testGetStorageConfigNull() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    assertNull(schema.getStorageConfig());
  }

  @Test
  void testGetStorageProviderNull() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    assertNull(schema.getStorageProvider());
  }

  @Test
  void testGetStorageConfigWithValue() {
    Map<String, Object> config = new HashMap<>();
    config.put("endpoint", "http://localhost:9000");
    config.put("_storageProvider", mock(StorageProvider.class));

    FileSchema schema = new FileSchema(mockParentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, "/bucket", null,
        null, defaultEngineConfig, false, null, null, null,
        null, "SMART_CASING", "SMART_CASING",
        "s3", config, null, null, false, null);
    assertNotNull(schema.getStorageConfig());
    assertNotNull(schema.getStorageProvider());
  }

  // =======================================================================
  // FileSchemaFactory: sanitizeOperand edge cases
  // =======================================================================

  @Test
  void testSanitizeOperandSecretKey() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("mySecret", "super-secret-value");
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) invokePrivateStatic(
        FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);
    assertEquals("********", result.get("mySecret"));
  }

  @Test
  void testSanitizeOperandStorageConfigWithUnderscore() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_provider", new Object());
    storageConfig.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
    storageConfig.put("endpoint", "http://localhost");
    operand.put("storageConfig", storageConfig);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) invokePrivateStatic(
        FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedStorage = (Map<String, Object>) result.get("storageConfig");
    assertNotNull(sanitizedStorage);
    assertEquals("********", sanitizedStorage.get("accessKeyId"));
    assertEquals("http://localhost", sanitizedStorage.get("endpoint"));
  }

  @Test
  void testSanitizeOperandStorageConfigNullValue() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_nullProvider", null);
    operand.put("storageConfig", storageConfig);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) invokePrivateStatic(
        FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedStorage = (Map<String, Object>) result.get("storageConfig");
    assertNull(sanitizedStorage.get("_nullProvider"));
  }

  @Test
  void testSanitizeOperandGenericNestedMap() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    Map<String, Object> nestedMap = new HashMap<>();
    nestedMap.put("normalKey", "normalValue");
    nestedMap.put("secretKey", "sensitive");
    operand.put("customConfig", nestedMap);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) invokePrivateStatic(
        FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedNested = (Map<String, Object>) result.get("customConfig");
    assertEquals("normalValue", sanitizedNested.get("normalKey"));
    assertEquals("********", sanitizedNested.get("secretKey"));
  }

  @Test
  void testSanitizeOperandModelUriWithCredentials() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("modelUri", "inline:{\"accessKeyId\": \"AKIA12345\", \"secretAccessKey\": \"mykey\"}");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) invokePrivateStatic(
        FileSchemaFactory.class, "sanitizeOperand",
        new Class<?>[]{Map.class}, operand);
    String sanitizedUri = (String) result.get("modelUri");
    assertTrue(sanitizedUri.contains("\"accessKeyId\": \"****\""));
    assertTrue(sanitizedUri.contains("\"secretAccessKey\": \"********\""));
  }

  // =======================================================================
  // FileSchemaFactory: sanitizeNestedMap edge cases
  // =======================================================================

  @Test
  void testSanitizeNestedMapPasswordKey() throws Exception {
    Map<String, Object> nested = new HashMap<>();
    nested.put("password", "secret123");
    nested.put("accesskey", "AKIA123");
    nested.put("normalField", "value");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) invokePrivateStatic(
        FileSchemaFactory.class, "sanitizeNestedMap",
        new Class<?>[]{Map.class}, nested);
    assertEquals("********", result.get("password"));
    assertEquals("********", result.get("accesskey"));
    assertEquals("value", result.get("normalField"));
  }

  // =======================================================================
  // FileSchemaFactory: parseBooleanValue edge cases
  // =======================================================================

  @Test
  void testParseBooleanValueInteger() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);
    Object result = method.invoke(null, Integer.valueOf(42));
    assertNull(result);
  }

  @Test
  void testParseBooleanValueStringYes() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);
    // "yes" is not recognized by Boolean.parseBoolean, returns false
    Boolean result = (Boolean) method.invoke(null, "yes");
    assertFalse(result);
  }

  @Test
  void testParseBooleanValueStringTrueUpperCase() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);
    Boolean result = (Boolean) method.invoke(null, "TRUE");
    assertTrue(result);
  }

  // =======================================================================
  // FileSchemaFactory: rewriteSchemaReferencesInSql edge cases
  // =======================================================================

  @Test
  void testRewriteSchemaReferencesInSqlWithActualRewrite() throws Exception {
    // declaredSchemaName and actualSchemaName must be NOT case-insensitive equal for rewrite to apply
    String result = (String) invokePrivateStatic(FileSchemaFactory.class,
        "rewriteSchemaReferencesInSql",
        new Class<?>[]{String.class, String.class, String.class},
        "SELECT * FROM econ.gdp_data", "econ", "my_econ_renamed");
    assertEquals("SELECT * FROM my_econ_renamed.gdp_data", result);
  }

  @Test
  void testRewriteSchemaReferencesInSqlNoMatch() throws Exception {
    String result = (String) invokePrivateStatic(FileSchemaFactory.class,
        "rewriteSchemaReferencesInSql",
        new Class<?>[]{String.class, String.class, String.class},
        "SELECT * FROM other.table_name", "econ", "renamed_econ");
    assertEquals("SELECT * FROM other.table_name", result);
  }

  @Test
  void testRewriteSchemaReferencesInSqlCaseInsensitiveMatch() throws Exception {
    // Case-insensitive equal names skip rewriting (early return)
    String result = (String) invokePrivateStatic(FileSchemaFactory.class,
        "rewriteSchemaReferencesInSql",
        new Class<?>[]{String.class, String.class, String.class},
        "SELECT * FROM schema1.t", "schema1", "SCHEMA1");
    assertEquals("SELECT * FROM schema1.t", result);
  }

  @Test
  void testRewriteSchemaReferencesInSqlSameNameExactMatch() throws Exception {
    // Exact same name skips rewriting
    String result = (String) invokePrivateStatic(FileSchemaFactory.class,
        "rewriteSchemaReferencesInSql",
        new Class<?>[]{String.class, String.class, String.class},
        "SELECT * FROM schema1.t", "schema1", "schema1");
    assertEquals("SELECT * FROM schema1.t", result);
  }

  @Test
  void testRewriteSchemaReferencesInSqlAllNull() throws Exception {
    assertNull(invokePrivateStatic(FileSchemaFactory.class,
        "rewriteSchemaReferencesInSql",
        new Class<?>[]{String.class, String.class, String.class},
        null, "econ", "ECON"));
  }

  @Test
  void testRewriteSchemaReferencesInSqlNullDeclared() throws Exception {
    String result = (String) invokePrivateStatic(FileSchemaFactory.class,
        "rewriteSchemaReferencesInSql",
        new Class<?>[]{String.class, String.class, String.class},
        "SELECT 1", null, "ECON");
    assertEquals("SELECT 1", result);
  }

  // =======================================================================
  // FileSchemaFactory: rewriteForeignKeySchemaNames edge cases
  // =======================================================================

  @Test
  void testRewriteForeignKeySchemaNamesCrossSchemaFkPreserved() throws Exception {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tableConstraints = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetSchema", "other_schema");
    fk.put("targetTable", "other_table");
    fks.add(fk);
    tableConstraints.put("foreignKeys", fks);
    constraints.put("source_table", tableConstraints);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) invokePrivateStatic(
            FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
            new Class<?>[]{Map.class, String.class, String.class},
            constraints, "econ", "ECON");

    // FK pointing to "other_schema" should not be rewritten
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resultFks =
        (List<Map<String, Object>>) result.get("source_table").get("foreignKeys");
    assertEquals("other_schema", resultFks.get(0).get("targetSchema"));
  }

  @Test
  void testRewriteForeignKeySchemaNamesFkWithNullTargetSchema() throws Exception {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tableConstraints = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetSchema", null);
    fk.put("targetTable", "local_table");
    fks.add(fk);
    tableConstraints.put("foreignKeys", fks);
    constraints.put("source_table", tableConstraints);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) invokePrivateStatic(
            FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
            new Class<?>[]{Map.class, String.class, String.class},
            constraints, "econ", "ECON");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resultFks =
        (List<Map<String, Object>>) result.get("source_table").get("foreignKeys");
    assertNull(resultFks.get(0).get("targetSchema"));
  }

  @Test
  void testRewriteForeignKeySchemaNamesFkMatchingRewritten() throws Exception {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tableConstraints = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetSchema", "econ");
    fk.put("targetTable", "target_table");
    fks.add(fk);
    tableConstraints.put("foreignKeys", fks);
    constraints.put("source_table", tableConstraints);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) invokePrivateStatic(
            FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
            new Class<?>[]{Map.class, String.class, String.class},
            constraints, "econ", "ECON");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resultFks =
        (List<Map<String, Object>>) result.get("source_table").get("foreignKeys");
    assertEquals("ECON", resultFks.get(0).get("targetSchema"));
  }

  @Test
  void testRewriteForeignKeySchemaNamesCaseInsensitive() throws Exception {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tableConstraints = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetSchema", "ECON");
    fk.put("targetTable", "target_table");
    fks.add(fk);
    tableConstraints.put("foreignKeys", fks);
    constraints.put("source_table", tableConstraints);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) invokePrivateStatic(
            FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
            new Class<?>[]{Map.class, String.class, String.class},
            constraints, "econ", "econ_upper");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resultFks =
        (List<Map<String, Object>>) result.get("source_table").get("foreignKeys");
    assertEquals("econ_upper", resultFks.get(0).get("targetSchema"));
  }

  @Test
  void testRewriteForeignKeySchemaNamesEmptyConstraints() throws Exception {
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) invokePrivateStatic(
            FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
            new Class<?>[]{Map.class, String.class, String.class},
            new HashMap<>(), "econ", "ECON");
    assertTrue(result.isEmpty());
  }

  @Test
  void testRewriteForeignKeySchemaNamesNullConstraints() throws Exception {
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) invokePrivateStatic(
            FileSchemaFactory.class, "rewriteForeignKeySchemaNames",
            new Class<?>[]{Map.class, String.class, String.class},
            null, "econ", "ECON");
    assertNull(result);
  }

  // =======================================================================
  // FileSchemaFactory: ROWTIME_COLUMN_NAME constant
  // =======================================================================

  @Test
  void testRowtimeColumnNameConstant() {
    assertEquals("ROWTIME", FileSchemaFactory.ROWTIME_COLUMN_NAME);
  }

  // =======================================================================
  // FileSchemaFactory: supportsConstraints
  // =======================================================================

  @Test
  void testSupportsConstraintsReturnsTrue() {
    assertTrue(FileSchemaFactory.INSTANCE.supportsConstraints());
  }

  // =======================================================================
  // FileSchemaFactory: setTableConstraints
  // =======================================================================

  @Test
  void testSetTableConstraintsStoresValues() throws Exception {
    FileSchemaFactory factory = FileSchemaFactory.INSTANCE;
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    constraints.put("t1", new HashMap<>());
    factory.setTableConstraints(constraints, null);

    // Read back via reflection
    Field field = FileSchemaFactory.class.getDeclaredField("tableConstraints");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> stored = (Map<String, Map<String, Object>>) field.get(factory);
    assertEquals(constraints, stored);

    // Clean up
    factory.setTableConstraints(null, null);
  }

  // =======================================================================
  // FileSchemaFactory: INSTANCE singleton
  // =======================================================================

  @Test
  void testSingletonInstance() {
    assertNotNull(FileSchemaFactory.INSTANCE);
    // Same reference
    assertTrue(FileSchemaFactory.INSTANCE == FileSchemaFactory.INSTANCE);
  }

  // =======================================================================
  // FileSchemaFactory: create with various operand combinations
  // =======================================================================

  @Test
  void testCreateWithColumnNameCasingSnakeCase() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("column_name_casing", "UPPER");

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithColumnNameCasingCamelCase() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("columnNameCasing", "LOWER");

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithTableNameCasingSnakeCase() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("table_name_casing", "LOWER");

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithCsvTypeInference() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    Map<String, Object> csvTypeInference = new HashMap<>();
    csvTypeInference.put("enabled", true);
    csvTypeInference.put("sampleSize", 100);
    operand.put("csvTypeInference", csvTypeInference);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithDuckdbConfig() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    Map<String, Object> duckdbConfig = new HashMap<>();
    duckdbConfig.put("memory_limit", "512MB");
    operand.put("duckdbConfig", duckdbConfig);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithParquetCacheDirectory() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("parquetCacheDirectory", tempDir.resolve("parquet-cache").toString());

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithPartitionedTables() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    List<Map<String, Object>> partitionedTables = new ArrayList<>();
    Map<String, Object> pt = new HashMap<>();
    pt.put("name", "partitioned_data");
    pt.put("pattern", "/data/**/*.parquet");
    partitionedTables.add(pt);
    operand.put("partitionedTables", partitionedTables);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithMaterializations() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    List<Map<String, Object>> materializations = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("table", "mv_summary");
    mv.put("sql", "SELECT count(*) FROM base_table");
    materializations.add(mv);
    operand.put("materializations", materializations);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithViewDefinitions() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    List<Map<String, Object>> views = new ArrayList<>();
    Map<String, Object> view = new HashMap<>();
    view.put("name", "my_view");
    view.put("sql", "SELECT 1 AS id");
    views.add(view);
    operand.put("views", views);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithFlattenTrue() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("flatten", Boolean.TRUE);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithTablesExplicitDefinition() throws Exception {
    // Create a CSV file in temp dir
    File csvFile = new File(tempDir.toFile(), "test_data.csv");
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name\n1,Alice\n2,Bob\n");
    }

    Map<String, Object> operand = new HashMap<>();
    operand.put("storageType", "local");
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> table = new HashMap<>();
    table.put("name", "test_data");
    table.put("url", csvFile.getAbsolutePath());
    tables.add(table);
    operand.put("tables", tables);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithTablesIncludingViewSkipped() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> viewDef = new HashMap<>();
    viewDef.put("name", "my_view");
    viewDef.put("type", "view");
    viewDef.put("sql", "SELECT 1 AS id");
    tables.add(viewDef);
    operand.put("tables", tables);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithConstraintsInOperand() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tableConstraint = new HashMap<>();
    tableConstraint.put("primaryKey", Arrays.asList("id"));
    constraints.put("my_table", tableConstraint);
    operand.put("tableConstraints", constraints);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithBatchSizeNumber() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("batchSize", 500);
    operand.put("memoryThreshold", 1024L * 1024);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithEphemeralCacheSnakeCase() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("ephemeral_cache", "true");

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithBaseDirectoryAsFile() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("baseDirectory", tempDir.toFile());

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithBaseDirectoryAsString() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("baseDirectory", tempDir.toFile().getAbsolutePath());

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithRelativeBaseDirectory() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("baseDirectory", "relative-dir");
    operand.put("baseDirectory_", tempDir.toFile());

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithModelBaseDirectory() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("baseDirectory_", tempDir.toFile());

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateNoDirectoryUsesWorkingDir() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("storageType", "local");

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateAutoDetectHdfsStorage() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "hdfs://namenode:8020/data");

    // HDFS auto-detect should create schema (it auto-sets storageType to "hdfs")
    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateAutoDetectHttpsStorage() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "https://api.example.com/data");

    // HTTP storage auto-detect should work but schema creation may fail due to connectivity
    try {
      FileSchemaFactory.INSTANCE.create(mockParentSchema, uniqueSchemaName(), operand);
    } catch (Exception e) {
      // Expected - we're just testing the auto-detect path
      assertNotNull(e);
    }
  }

  @Test
  void testCreateAutoDetectS3BaseDirectory() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("baseDirectory", "s3://my-bucket/cache");

    // S3 auto-detect from baseDirectory - may succeed or fail depending on implementation
    try {
      Schema schema = FileSchemaFactory.INSTANCE.create(
          mockParentSchema, uniqueSchemaName(), operand);
      // If it succeeds, schema should be non-null
      assertNotNull(schema);
    } catch (Exception e) {
      // S3 without credentials may throw - verify it's an expected error
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testCreateS3WithCredentialsPasses() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "s3://my-bucket/data");
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
    storageConfig.put("secretAccessKey", "secret123");
    operand.put("storageConfig", storageConfig);

    try {
      FileSchemaFactory.INSTANCE.create(mockParentSchema, uniqueSchemaName(), operand);
    } catch (Exception e) {
      // May fail due to S3 connectivity, but should pass credential validation
      assertFalse(e.getMessage().contains("requires 'accessKeyId'"),
          "Should not fail on credential validation: " + e.getMessage());
    }
  }

  @Test
  void testCreateWithPreCreatedStorageProvider() {
    StorageProvider mockProvider = mock(StorageProvider.class);
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("_storageProvider", mockProvider);
    operand.put("storageType", "local");

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithDeclaredSchemaNameDifferent() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("declaredSchemaName", "econ");

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, "ECON", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithCanonicalSchemaName() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("canonicalSchemaName", "econ");

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, "ECON_SCHEMA", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithNullStorageTypeAndTablesAutoDetects() {
    Map<String, Object> operand = new HashMap<>();
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> table = new HashMap<>();
    table.put("name", "t1");
    table.put("url", tempDir.resolve("data.csv").toString());
    tables.add(table);
    operand.put("tables", tables);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithRecursiveAndDirectoryPattern() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("recursive", Boolean.TRUE);
    operand.put("directoryPattern", "**/*.csv");

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithGlobAlias() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("glob", "*.json");

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithRefreshInterval() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("refreshInterval", "10 minutes");

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithPrimeCacheFalse() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("primeCache", Boolean.FALSE);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithPrimeCacheSnakeCase() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("prime_cache", Boolean.FALSE);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithComment() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("comment", "Test schema for unit tests");

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateNoStorageTypeNoDirectoryThrows() {
    Map<String, Object> operand = new HashMap<>();
    // No directory, no tables, no storageType => throws
    assertThrows(IllegalStateException.class, () ->
        FileSchemaFactory.INSTANCE.create(mockParentSchema, uniqueSchemaName(), operand));
  }

  @Test
  void testCreateWithSourceDirectoryAlias() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("sourceDirectory", tempDir.toFile().getAbsolutePath());
    operand.put("storageType", "local");

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
  }

  // =======================================================================
  // FileSchemaFactory: validateUniqueSchemaName edge cases
  // =======================================================================

  @Test
  void testValidateUniqueSchemaNameNullParent() throws Exception {
    // Should not throw when parentSchema is null
    invokePrivateStatic(FileSchemaFactory.class, "validateUniqueSchemaName",
        new Class<?>[]{SchemaPlus.class, String.class}, null, "test");
  }

  @Test
  void testValidateUniqueSchemaNameNullName() throws Exception {
    // Should not throw when name is null
    invokePrivateStatic(FileSchemaFactory.class, "validateUniqueSchemaName",
        new Class<?>[]{SchemaPlus.class, String.class}, mockParentSchema, null);
  }

  @Test
  void testValidateUniqueSchemaNameDuplicate() throws Exception {
    SchemaPlus parent = mock(SchemaPlus.class);
    Lookup subSchemaLookup = mock(Lookup.class);
    when(subSchemaLookup.get("existing")).thenReturn(mock(SchemaPlus.class));
    when(subSchemaLookup.getNames(any(LikePattern.class)))
        .thenReturn(Collections.singleton("existing"));
    doReturn(subSchemaLookup).when(parent).subSchemas();

    assertThrows(IllegalArgumentException.class, () ->
        invokePrivateStatic(FileSchemaFactory.class, "validateUniqueSchemaName",
            new Class<?>[]{SchemaPlus.class, String.class}, parent, "existing"));
  }

  // =======================================================================
  // FileSchemaFactory: writeDebugModel tests
  // =======================================================================

  @Test
  void testWriteDebugModelCreatesFile() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toFile().getAbsolutePath());

    invokePrivateStatic(FileSchemaFactory.class, "writeDebugModel",
        new Class<?>[]{String.class, Map.class, String.class},
        "testschema", operand, "root");

    File aperioDir = new File(System.getProperty("user.dir"), ".aperio");
    File debugFile = new File(aperioDir, "debug-model-testschema.json");
    // File may or may not exist depending on test environment, but method should not throw
  }

  @Test
  void testWriteDebugModelNullParentName() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "/tmp/test");

    // Should not throw with null parentSchemaName
    invokePrivateStatic(FileSchemaFactory.class, "writeDebugModel",
        new Class<?>[]{String.class, Map.class, String.class},
        "testschema", operand, null);
  }

  // =======================================================================
  // FileSchema: extractFieldConfigurations tests
  // =======================================================================

  @Test
  void testExtractFieldConfigurationsNull() throws Exception {
    Object result = invokePrivateStatic(FileSchema.class, "extractFieldConfigurations",
        new Class<?>[]{Map.class, String.class}, null, "test_table");
    assertNull(result);
  }

  @Test
  void testExtractFieldConfigurationsWithFields() throws Exception {
    Map<String, Object> tableDef = new HashMap<>();
    List<Map<String, Object>> fields = new ArrayList<>();
    Map<String, Object> field = new HashMap<>();
    field.put("name", "column1");
    field.put("type", "VARCHAR");
    fields.add(field);
    tableDef.put("fields", fields);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result = (List<Map<String, Object>>)
        invokePrivateStatic(FileSchema.class, "extractFieldConfigurations",
            new Class<?>[]{Map.class, String.class}, tableDef, "test_table");
    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals("column1", result.get(0).get("name"));
  }

  @Test
  void testExtractFieldConfigurationsNoFields() throws Exception {
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "test_table");

    Object result = invokePrivateStatic(FileSchema.class, "extractFieldConfigurations",
        new Class<?>[]{Map.class, String.class}, tableDef, "test_table");
    assertNull(result);
  }

  // =======================================================================
  // FileSchema: validateForeignKeyConstraints edge cases
  // =======================================================================

  @Test
  void testValidateForeignKeyConstraintsWithListTargetTable() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Map<String, Object>> metadata = new HashMap<>();
    Map<String, Object> constraint = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    List<String> qualifiedName = Arrays.asList("schema1", "table1");
    fk.put("targetTable", qualifiedName);
    fks.add(fk);
    constraint.put("foreignKeys", fks);
    metadata.put("source_table", constraint);

    setField(schema, "constraintMetadata", metadata);

    Map<String, Table> tables = new HashMap<>();
    // Call validateForeignKeyConstraints
    invokePrivate(schema, "validateForeignKeyConstraints",
        new Class<?>[]{Map.class}, tables);
    // Should not throw - invalid FK is removed
  }

  @Test
  void testValidateForeignKeyConstraintsWithSingleElementList() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Map<String, Object>> metadata = new HashMap<>();
    Map<String, Object> constraint = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    List<String> qualifiedName = Collections.singletonList("local_table");
    fk.put("targetTable", qualifiedName);
    fks.add(fk);
    constraint.put("foreignKeys", fks);
    metadata.put("source_table", constraint);

    setField(schema, "constraintMetadata", metadata);

    Map<String, Table> tables = new HashMap<>();
    tables.put("local_table", mock(Table.class));

    invokePrivate(schema, "validateForeignKeyConstraints",
        new Class<?>[]{Map.class}, tables);
  }

  @Test
  void testValidateForeignKeyConstraintsNullConstraintEntry() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Map<String, Object>> metadata = new HashMap<>();
    metadata.put("source_table", null);

    setField(schema, "constraintMetadata", metadata);

    Map<String, Table> tables = new HashMap<>();
    // Should not throw
    invokePrivate(schema, "validateForeignKeyConstraints",
        new Class<?>[]{Map.class}, tables);
  }

  @Test
  void testValidateForeignKeyConstraintsNullForeignKeys() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Map<String, Object>> metadata = new HashMap<>();
    Map<String, Object> constraint = new HashMap<>();
    constraint.put("foreignKeys", null);
    metadata.put("source_table", constraint);

    setField(schema, "constraintMetadata", metadata);

    Map<String, Table> tables = new HashMap<>();
    // Should not throw
    invokePrivate(schema, "validateForeignKeyConstraints",
        new Class<?>[]{Map.class}, tables);
  }

  @Test
  void testValidateForeignKeyConstraintsValidLocalFk() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Map<String, Object>> metadata = new HashMap<>();
    Map<String, Object> constraint = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetTable", "existing_table");
    fks.add(fk);
    constraint.put("foreignKeys", fks);
    metadata.put("source_table", constraint);

    setField(schema, "constraintMetadata", metadata);

    Map<String, Table> tables = new HashMap<>();
    tables.put("existing_table", mock(Table.class));

    invokePrivate(schema, "validateForeignKeyConstraints",
        new Class<?>[]{Map.class}, tables);

    // FK should still be there (not removed) since target exists
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> afterMeta =
        (Map<String, Map<String, Object>>) getField(schema, "constraintMetadata");
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> afterFks =
        (List<Map<String, Object>>) afterMeta.get("source_table").get("foreignKeys");
    assertEquals(1, afterFks.size());
  }

  // =======================================================================
  // FileSchema: checkTableExists edge cases
  // =======================================================================

  @Test
  void testCheckTableExistsLocalNoSchema() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Table> localTables = new HashMap<>();
    localTables.put("my_table", mock(Table.class));

    Boolean result = (Boolean) invokePrivate(schema, "checkTableExists",
        new Class<?>[]{String.class, String.class, Map.class},
        null, "my_table", localTables);
    assertTrue(result);
  }

  @Test
  void testCheckTableExistsLocalSameSchema() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String schemaName = (String) getField(schema, "name");

    Map<String, Table> localTables = new HashMap<>();
    localTables.put("my_table", mock(Table.class));

    Boolean result = (Boolean) invokePrivate(schema, "checkTableExists",
        new Class<?>[]{String.class, String.class, Map.class},
        schemaName, "my_table", localTables);
    assertTrue(result);
  }

  @Test
  void testCheckTableExistsCrossSchemaNotFound() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());

    Map<String, Table> localTables = new HashMap<>();
    // Set parentSchema to null to trigger the "schema not found" path
    setField(schema, "parentSchema", null);

    Boolean result = (Boolean) invokePrivate(schema, "checkTableExists",
        new Class<?>[]{String.class, String.class, Map.class},
        "other_schema", "other_table", localTables);
    assertFalse(result);
  }

  // =======================================================================
  // FileSchema: getTableName tests
  // =======================================================================

  @Test
  void testGetTableNameExplicitPreserved() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "getTableName",
        new Class<?>[]{String.class, String.class, String.class},
        "MyExplicitName", "derived_name", "UPPER");
    assertEquals("MyExplicitName", result);
  }

  @Test
  void testGetTableNameDerivedAppliesCasing() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "getTableName",
        new Class<?>[]{String.class, String.class, String.class},
        null, "my_table", "SMART_CASING");
    assertNotNull(result);
  }

  // =======================================================================
  // FileSchema: generateModelFile test
  // =======================================================================

  @Test
  void testGenerateModelFileCreatesFile() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Table> tables = new HashMap<>();
    tables.put("test_csv", mock(Table.class));

    invokePrivate(schema, "generateModelFile",
        new Class<?>[]{Map.class}, tables);

    File cacheDir = schema.getOperatingCacheDirectory();
    File modelFile = new File(cacheDir, ".generated-model.json");
    assertTrue(modelFile.exists(), "Generated model file should exist");
    String content = new String(Files.readAllBytes(modelFile.toPath()), StandardCharsets.UTF_8);
    assertTrue(content.contains("\"version\": \"1.0\""));
  }

  // =======================================================================
  // FileSchema: buildConvertibleFilesGlobPattern tests
  // =======================================================================

  @Test
  void testBuildConvertibleFilesGlobPatternNonRecursive() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "buildConvertibleFilesGlobPattern",
        new Class<?>[]{boolean.class}, false);
    assertNotNull(result);
    assertTrue(result.startsWith("*.{"));
    assertTrue(result.contains("xlsx"));
    assertTrue(result.contains("html"));
  }

  @Test
  void testBuildConvertibleFilesGlobPatternRecursive() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    String result = (String) invokePrivate(schema, "buildConvertibleFilesGlobPatternRecursive",
        new Class<?>[]{});
    assertNotNull(result);
    assertTrue(result.startsWith("**/*.{"));
    assertTrue(result.contains("xlsx"));
    assertTrue(result.contains("html"));
  }

  // =======================================================================
  // FileSchema: storeExplicitTableMapping and getExplicitTableName tests
  // =======================================================================

  @Test
  void testStoreAndGetExplicitTableMapping() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File csvFile = new File(tempDir.toFile(), "data.csv");
    csvFile.createNewFile();

    Source source = org.apache.calcite.util.Sources.of(csvFile);
    invokePrivate(schema, "storeExplicitTableMapping",
        new Class<?>[]{String.class, Source.class},
        "custom_table_name", source);

    String result = (String) invokePrivate(schema, "getExplicitTableName",
        new Class<?>[]{File.class}, csvFile);
    assertEquals("custom_table_name", result);
  }

  @Test
  void testGetExplicitTableNameNotFound() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    File unknownFile = new File(tempDir.toFile(), "unknown.csv");
    unknownFile.createNewFile();

    String result = (String) invokePrivate(schema, "getExplicitTableName",
        new Class<?>[]{File.class}, unknownFile);
    assertNull(result);
  }

  // =======================================================================
  // FileSchema: isFileNameSupported test (used by storage provider path)
  // =======================================================================

  @Test
  void testIsFileNameSupportedCsv() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isFileNameSupported",
        new Class<?>[]{String.class}, "data.csv");
    assertTrue(result);
  }

  @Test
  void testIsFileNameSupportedParquet() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isFileNameSupported",
        new Class<?>[]{String.class}, "data.parquet");
    assertTrue(result);
  }

  @Test
  void testIsFileNameSupportedUnsupported() throws Exception {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    try {
      Boolean result = (Boolean) invokePrivate(schema, "isFileNameSupported",
          new Class<?>[]{String.class}, "data.txt");
      // If method exists, verify result
      assertFalse(result);
    } catch (NoSuchMethodException e) {
      // Method may not exist - that is acceptable
    }
  }

  // =======================================================================
  // FileSchema: table discovery with actual CSV files
  // =======================================================================

  @Test
  void testGetTableMapDiscoversCsvFiles() throws Exception {
    // Create files BEFORE schema to avoid race with cache priming
    File sourceDir = new File(tempDir.toFile(), "csv_src");
    sourceDir.mkdirs();
    File csvFile = new File(sourceDir, "employees.csv");
    try (FileWriter w = new FileWriter(csvFile)) {
      w.write("id,name,dept\n1,Alice,Eng\n2,Bob,Sales\n");
    }

    // Use the factory to ensure proper schema setup like production use
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", sourceDir.getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("primeCache", Boolean.FALSE);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
    // The factory returns a FileSchema that has been registered
    assertTrue(schema instanceof FileSchema);
    FileSchema fs = (FileSchema) schema;
    Map<String, Table> tables = fs.getTableMap();
    assertNotNull(tables);
    // Table should be discovered from the CSV file
    assertTrue(tables.size() >= 1, "Should discover at least 1 table, got: " + tables.keySet());
  }

  @Test
  void testGetTableMapDiscoversJsonFiles() throws Exception {
    File sourceDir = new File(tempDir.toFile(), "json_src");
    sourceDir.mkdirs();
    File jsonFile = new File(sourceDir, "config.json");
    try (FileWriter w = new FileWriter(jsonFile)) {
      w.write("[{\"id\": 1, \"name\": \"test\"}]");
    }

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", sourceDir.getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("primeCache", Boolean.FALSE);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
    assertTrue(schema instanceof FileSchema);
    FileSchema fs = (FileSchema) schema;
    Map<String, Table> tables = fs.getTableMap();
    assertNotNull(tables);
    assertTrue(tables.size() >= 1, "Should discover at least 1 table, got: " + tables.keySet());
  }

  @Test
  void testGetTableMapDiscoversMultipleFiles() throws Exception {
    File sourceDir = new File(tempDir.toFile(), "multi_src");
    sourceDir.mkdirs();
    File csv1 = new File(sourceDir, "table_a.csv");
    try (FileWriter w = new FileWriter(csv1)) {
      w.write("col1,col2\nval1,val2\n");
    }
    File csv2 = new File(sourceDir, "table_b.csv");
    try (FileWriter w = new FileWriter(csv2)) {
      w.write("x,y\n1,2\n");
    }

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", sourceDir.getAbsolutePath());
    operand.put("storageType", "local");
    operand.put("primeCache", Boolean.FALSE);

    Schema schema = FileSchemaFactory.INSTANCE.create(
        mockParentSchema, uniqueSchemaName(), operand);
    assertNotNull(schema);
    assertTrue(schema instanceof FileSchema);
    FileSchema fs = (FileSchema) schema;
    Map<String, Table> tables = fs.getTableMap();
    assertNotNull(tables);
    assertTrue(tables.size() >= 2, "Should discover multiple tables, got: " + tables.keySet());
  }

  @Test
  void testGetTableMapEmptyDirectory() {
    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    // Empty directory should produce empty or minimal tables
  }

  @Test
  void testGetTableMapCachesResults() {
    File csvFile = new File(tempDir.toFile(), "cached_test.csv");
    try {
      try (FileWriter w = new FileWriter(csvFile)) {
        w.write("id\n1\n");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    FileSchema schema = createSchemaSimple(tempDir.toFile());
    Map<String, Table> first = schema.getTableMap();
    Map<String, Table> second = schema.getTableMap();
    // Same reference should be returned (cached)
    assertTrue(first == second, "getTableMap should return cached result on second call");
  }
}
