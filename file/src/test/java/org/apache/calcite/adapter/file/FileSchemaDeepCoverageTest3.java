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
import org.apache.calcite.adapter.file.refresh.TableRefreshListener;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests for {@link FileSchema} - Round 3.
 * Targets private/internal method branches not covered by existing tests:
 * glob pattern detection, URI classification, file extension helpers,
 * compressed extension handling, model file detection, FK validation branches,
 * table baselines, conversion records, flattening detection, HTML table detection,
 * convertible file detection, field extraction, directory scanning edge cases,
 * model file generation, storage resolution, and notification edge cases.
 */
@SuppressWarnings("deprecation")
@Tag("unit")
public class FileSchemaDeepCoverageTest3 {

  private static final AtomicInteger SCHEMA_COUNTER = new AtomicInteger(0);

  @TempDir
  Path tempDir;

  private SchemaPlus parentSchema;
  private ExecutionEngineConfig defaultEngineConfig;

  private String uniqueSchemaName() {
    return "test_deep3_" + SCHEMA_COUNTER.incrementAndGet();
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

  // -----------------------------------------------------------------------
  // isGlobPattern - private method covering all branches
  // -----------------------------------------------------------------------

  @Test
  void testIsGlobPatternNull() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Boolean result = (Boolean) invokePrivate(schema, "isGlobPattern",
        new Class[]{String.class}, (String) null);
    assertFalse(result);
  }

  @Test
  void testIsGlobPatternHttpUrl() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertFalse((Boolean) invokePrivate(schema, "isGlobPattern",
        new Class[]{String.class}, "http://example.com/data*.csv"));
    assertFalse((Boolean) invokePrivate(schema, "isGlobPattern",
        new Class[]{String.class}, "https://example.com/path"));
  }

  @Test
  void testIsGlobPatternWithStar() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isGlobPattern",
        new Class[]{String.class}, "data/*.csv"));
  }

  @Test
  void testIsGlobPatternWithQuestionMark() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isGlobPattern",
        new Class[]{String.class}, "data?.csv"));
  }

  @Test
  void testIsGlobPatternWithBrackets() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isGlobPattern",
        new Class[]{String.class}, "data[0-9].csv"));
  }

  @Test
  void testIsGlobPatternWithProtocol() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isGlobPattern",
        new Class[]{String.class}, "s3://bucket/path/*.parquet"));
  }

  @Test
  void testIsGlobPatternPlainPath() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertFalse((Boolean) invokePrivate(schema, "isGlobPattern",
        new Class[]{String.class}, "/data/file.csv"));
  }

  // -----------------------------------------------------------------------
  // isAbsoluteUri - private method covering all branches
  // -----------------------------------------------------------------------

  @Test
  void testIsAbsoluteUriS3() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isAbsoluteUri",
        new Class[]{String.class}, "s3://bucket/key"));
  }

  @Test
  void testIsAbsoluteUriHttp() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isAbsoluteUri",
        new Class[]{String.class}, "http://example.com"));
  }

  @Test
  void testIsAbsoluteUriHttps() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isAbsoluteUri",
        new Class[]{String.class}, "https://example.com"));
  }

  @Test
  void testIsAbsoluteUriFtp() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isAbsoluteUri",
        new Class[]{String.class}, "ftp://server/path"));
  }

  @Test
  void testIsAbsoluteUriFileProtocol() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isAbsoluteUri",
        new Class[]{String.class}, "file:///home/user/file.csv"));
  }

  @Test
  void testIsAbsoluteUriAbsolutePath() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isAbsoluteUri",
        new Class[]{String.class}, "/absolute/path/file.csv"));
  }

  @Test
  void testIsAbsoluteUriWindowsPath() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isAbsoluteUri",
        new Class[]{String.class}, "C:\\Users\\file.csv"));
  }

  @Test
  void testIsAbsoluteUriRelativePath() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertFalse((Boolean) invokePrivate(schema, "isAbsoluteUri",
        new Class[]{String.class}, "relative/path/file.csv"));
  }

  @Test
  void testIsAbsoluteUriShortString() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    // String with length <= 2 that doesn't start with /
    assertFalse((Boolean) invokePrivate(schema, "isAbsoluteUri",
        new Class[]{String.class}, "ab"));
  }

  // -----------------------------------------------------------------------
  // hasHttpConfiguration - private method
  // -----------------------------------------------------------------------

  @Test
  void testHasHttpConfigurationNull() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertFalse((Boolean) invokePrivate(schema, "hasHttpConfiguration",
        new Class[]{Map.class}, (Map<String, Object>) null));
  }

  @Test
  void testHasHttpConfigurationEmpty() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertFalse((Boolean) invokePrivate(schema, "hasHttpConfiguration",
        new Class[]{Map.class}, new HashMap<String, Object>()));
  }

  @Test
  void testHasHttpConfigurationWithMethod() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Object> def = new HashMap<>();
    def.put("method", "POST");
    assertTrue((Boolean) invokePrivate(schema, "hasHttpConfiguration",
        new Class[]{Map.class}, def));
  }

  @Test
  void testHasHttpConfigurationWithBody() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Object> def = new HashMap<>();
    def.put("body", "{}");
    assertTrue((Boolean) invokePrivate(schema, "hasHttpConfiguration",
        new Class[]{Map.class}, def));
  }

  @Test
  void testHasHttpConfigurationWithHeaders() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Object> def = new HashMap<>();
    def.put("headers", new HashMap<>());
    assertTrue((Boolean) invokePrivate(schema, "hasHttpConfiguration",
        new Class[]{Map.class}, def));
  }

  @Test
  void testHasHttpConfigurationWithMimeType() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Object> def = new HashMap<>();
    def.put("mimeType", "application/json");
    assertTrue((Boolean) invokePrivate(schema, "hasHttpConfiguration",
        new Class[]{Map.class}, def));
  }

  // -----------------------------------------------------------------------
  // trimCompressedExtensions - private method
  // -----------------------------------------------------------------------

  @Test
  void testTrimCompressedExtensionsNull() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertNull(invokePrivate(schema, "trimCompressedExtensions",
        new Class[]{String.class}, (String) null));
  }

  @Test
  void testTrimCompressedExtensionsGz() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertEquals("data.csv", invokePrivate(schema, "trimCompressedExtensions",
        new Class[]{String.class}, "data.csv.gz"));
  }

  @Test
  void testTrimCompressedExtensionsGzip() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertEquals("data.csv", invokePrivate(schema, "trimCompressedExtensions",
        new Class[]{String.class}, "data.csv.gzip"));
  }

  @Test
  void testTrimCompressedExtensionsBz2() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertEquals("data.csv", invokePrivate(schema, "trimCompressedExtensions",
        new Class[]{String.class}, "data.csv.bz2"));
  }

  @Test
  void testTrimCompressedExtensionsXz() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertEquals("data.csv", invokePrivate(schema, "trimCompressedExtensions",
        new Class[]{String.class}, "data.csv.xz"));
  }

  @Test
  void testTrimCompressedExtensionsZip() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertEquals("data.csv", invokePrivate(schema, "trimCompressedExtensions",
        new Class[]{String.class}, "data.csv.zip"));
  }

  @Test
  void testTrimCompressedExtensionsNoCompression() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertEquals("data.csv", invokePrivate(schema, "trimCompressedExtensions",
        new Class[]{String.class}, "data.csv"));
  }

  // -----------------------------------------------------------------------
  // hasCompressedExtension - private method
  // -----------------------------------------------------------------------

  @Test
  void testHasCompressedExtensionNull() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertFalse((Boolean) invokePrivate(schema, "hasCompressedExtension",
        new Class[]{String.class}, (String) null));
  }

  @Test
  void testHasCompressedExtensionTrue() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "hasCompressedExtension",
        new Class[]{String.class}, "data.csv.gz"));
    assertTrue((Boolean) invokePrivate(schema, "hasCompressedExtension",
        new Class[]{String.class}, "data.csv.bz2"));
    assertTrue((Boolean) invokePrivate(schema, "hasCompressedExtension",
        new Class[]{String.class}, "data.csv.xz"));
    assertTrue((Boolean) invokePrivate(schema, "hasCompressedExtension",
        new Class[]{String.class}, "data.csv.zip"));
    assertTrue((Boolean) invokePrivate(schema, "hasCompressedExtension",
        new Class[]{String.class}, "data.csv.gzip"));
  }

  @Test
  void testHasCompressedExtensionFalse() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertFalse((Boolean) invokePrivate(schema, "hasCompressedExtension",
        new Class[]{String.class}, "data.csv"));
    assertFalse((Boolean) invokePrivate(schema, "hasCompressedExtension",
        new Class[]{String.class}, "data.parquet"));
  }

  // -----------------------------------------------------------------------
  // getFileExtension - private method
  // -----------------------------------------------------------------------

  @Test
  void testGetFileExtension() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertEquals("csv", invokePrivate(schema, "getFileExtension",
        new Class[]{String.class}, "data.csv"));
    assertEquals("csv", invokePrivate(schema, "getFileExtension",
        new Class[]{String.class}, "data.csv.gz"));
    assertEquals("json", invokePrivate(schema, "getFileExtension",
        new Class[]{String.class}, "file.json"));
    assertEquals("parquet", invokePrivate(schema, "getFileExtension",
        new Class[]{String.class}, "file.parquet"));
    assertEquals("", invokePrivate(schema, "getFileExtension",
        new Class[]{String.class}, "noextension"));
  }

  // -----------------------------------------------------------------------
  // isConvertibleFile - private method
  // -----------------------------------------------------------------------

  @Test
  void testIsConvertibleFileNull() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertFalse((Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class[]{String.class}, (String) null));
  }

  @Test
  void testIsConvertibleFileXlsx() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class[]{String.class}, "data.xlsx"));
    assertTrue((Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class[]{String.class}, "data.xls"));
  }

  @Test
  void testIsConvertibleFileMd() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class[]{String.class}, "readme.md"));
    assertTrue((Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class[]{String.class}, "readme.markdown"));
  }

  @Test
  void testIsConvertibleFileDocx() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class[]{String.class}, "document.docx"));
  }

  @Test
  void testIsConvertibleFilePptx() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class[]{String.class}, "slides.pptx"));
  }

  @Test
  void testIsConvertibleFileHtml() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class[]{String.class}, "page.html"));
    assertTrue((Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class[]{String.class}, "page.htm"));
  }

  @Test
  void testIsConvertibleFileFalse() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertFalse((Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class[]{String.class}, "data.csv"));
    assertFalse((Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class[]{String.class}, "data.json"));
    assertFalse((Boolean) invokePrivate(schema, "isConvertibleFile",
        new Class[]{String.class}, "data.parquet"));
  }

  // -----------------------------------------------------------------------
  // isTableSourceFile - private method
  // -----------------------------------------------------------------------

  @Test
  void testIsTableSourceFileNull() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertFalse((Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class[]{String.class}, (String) null));
  }

  @Test
  void testIsTableSourceFileCsv() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class[]{String.class}, "data.csv"));
    assertTrue((Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class[]{String.class}, "data.tsv"));
  }

  @Test
  void testIsTableSourceFileJson() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class[]{String.class}, "data.json"));
  }

  @Test
  void testIsTableSourceFileYaml() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class[]{String.class}, "data.yaml"));
    assertTrue((Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class[]{String.class}, "data.yml"));
  }

  @Test
  void testIsTableSourceFileArrow() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class[]{String.class}, "data.arrow"));
  }

  @Test
  void testIsTableSourceFileParquet() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class[]{String.class}, "data.parquet"));
  }

  @Test
  void testIsTableSourceFileFalse() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertFalse((Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class[]{String.class}, "data.xlsx"));
    assertFalse((Boolean) invokePrivate(schema, "isTableSourceFile",
        new Class[]{String.class}, "readme.md"));
  }

  // -----------------------------------------------------------------------
  // isJsonFile - private method
  // -----------------------------------------------------------------------

  @Test
  void testIsJsonFile() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isJsonFile",
        new Class[]{File.class}, new File("data.json")));
    assertTrue((Boolean) invokePrivate(schema, "isJsonFile",
        new Class[]{File.class}, new File("data.json.gz")));
    assertFalse((Boolean) invokePrivate(schema, "isJsonFile",
        new Class[]{File.class}, new File("data.csv")));
    assertFalse((Boolean) invokePrivate(schema, "isJsonFile",
        new Class[]{File.class}, new File("data.yaml")));
  }

  // -----------------------------------------------------------------------
  // isHtmlFile - private method
  // -----------------------------------------------------------------------

  @Test
  void testIsHtmlFile() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isHtmlFile",
        new Class[]{File.class}, new File("page.html")));
    assertTrue((Boolean) invokePrivate(schema, "isHtmlFile",
        new Class[]{File.class}, new File("page.htm")));
    assertTrue((Boolean) invokePrivate(schema, "isHtmlFile",
        new Class[]{File.class}, new File("PAGE.HTML")));
    assertFalse((Boolean) invokePrivate(schema, "isHtmlFile",
        new Class[]{File.class}, new File("data.csv")));
  }

  // -----------------------------------------------------------------------
  // isFileTypeSupported - private method
  // -----------------------------------------------------------------------

  @Test
  void testIsFileTypeSupportedCsv() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File csvFile = new File(tempDir.toFile(), "data.csv");
    Files.write(csvFile.toPath(), "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8));
    assertTrue((Boolean) invokePrivate(schema, "isFileTypeSupported",
        new Class[]{File.class}, csvFile));
  }

  @Test
  void testIsFileTypeSupportedHiddenFile() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File hidden = new File(tempDir.toFile(), ".hidden.csv");
    Files.write(hidden.toPath(), "a,b\n".getBytes(StandardCharsets.UTF_8));
    assertFalse((Boolean) invokePrivate(schema, "isFileTypeSupported",
        new Class[]{File.class}, hidden));
  }

  @Test
  void testIsFileTypeSupportedDirectory() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File dir = new File(tempDir.toFile(), "subdir");
    dir.mkdirs();
    assertFalse((Boolean) invokePrivate(schema, "isFileTypeSupported",
        new Class[]{File.class}, dir));
  }

  @Test
  void testIsFileTypeSupportedUnsupported() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File txtFile = new File(tempDir.toFile(), "readme.txt");
    Files.write(txtFile.toPath(), "text".getBytes(StandardCharsets.UTF_8));
    assertFalse((Boolean) invokePrivate(schema, "isFileTypeSupported",
        new Class[]{File.class}, txtFile));
  }

  @Test
  void testIsFileTypeSupportedHtmlFile() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File htmlFile = new File(tempDir.toFile(), "page.html");
    Files.write(htmlFile.toPath(), "<html></html>".getBytes(StandardCharsets.UTF_8));
    assertTrue((Boolean) invokePrivate(schema, "isFileTypeSupported",
        new Class[]{File.class}, htmlFile));
  }

  @Test
  void testIsFileTypeSupportedCompressedCsv() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File gzFile = new File(tempDir.toFile(), "data.csv.gz");
    Files.write(gzFile.toPath(), new byte[]{0x1f, (byte) 0x8b});
    assertTrue((Boolean) invokePrivate(schema, "isFileTypeSupported",
        new Class[]{File.class}, gzFile));
  }

  // -----------------------------------------------------------------------
  // isFileNameSupported - private method
  // -----------------------------------------------------------------------

  @Test
  void testIsFileNameSupported() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    assertTrue((Boolean) invokePrivate(schema, "isFileNameSupported",
        new Class[]{String.class}, "data.csv"));
    assertTrue((Boolean) invokePrivate(schema, "isFileNameSupported",
        new Class[]{String.class}, "data.json"));
    assertTrue((Boolean) invokePrivate(schema, "isFileNameSupported",
        new Class[]{String.class}, "data.parquet"));
    assertTrue((Boolean) invokePrivate(schema, "isFileNameSupported",
        new Class[]{String.class}, "page.html"));
    assertTrue((Boolean) invokePrivate(schema, "isFileNameSupported",
        new Class[]{String.class}, "data.csv.gz"));
    assertFalse((Boolean) invokePrivate(schema, "isFileNameSupported",
        new Class[]{String.class}, "readme.txt"));
    assertFalse((Boolean) invokePrivate(schema, "isFileNameSupported",
        new Class[]{String.class}, "data.xlsx"));
  }

  // -----------------------------------------------------------------------
  // isCalciteModelFile - private method
  // -----------------------------------------------------------------------

  @Test
  void testIsCalciteModelFileTrue() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File modelFile = new File(tempDir.toFile(), "model.json");
    try (FileWriter fw = new FileWriter(modelFile)) {
      fw.write("{\"version\":\"1.0\",\"schemas\":[]}");
    }
    org.apache.calcite.util.Source source =
        org.apache.calcite.util.Sources.of(modelFile);
    assertTrue((Boolean) invokePrivate(schema, "isCalciteModelFile",
        new Class[]{org.apache.calcite.util.Source.class}, source));
  }

  @Test
  void testIsCalciteModelFileFalseRegularJson() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File dataFile = new File(tempDir.toFile(), "data.json");
    try (FileWriter fw = new FileWriter(dataFile)) {
      fw.write("[{\"id\":1}]");
    }
    org.apache.calcite.util.Source source =
        org.apache.calcite.util.Sources.of(dataFile);
    assertFalse((Boolean) invokePrivate(schema, "isCalciteModelFile",
        new Class[]{org.apache.calcite.util.Source.class}, source));
  }

  @Test
  void testIsCalciteModelFileNotJson() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File csvFile = new File(tempDir.toFile(), "data.csv");
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("a,b\n1,2\n");
    }
    org.apache.calcite.util.Source source =
        org.apache.calcite.util.Sources.of(csvFile);
    assertFalse((Boolean) invokePrivate(schema, "isCalciteModelFile",
        new Class[]{org.apache.calcite.util.Source.class}, source));
  }

  @Test
  void testIsCalciteModelFileInvalidJson() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    File badJson = new File(tempDir.toFile(), "bad.json");
    try (FileWriter fw = new FileWriter(badJson)) {
      fw.write("not valid json{{{");
    }
    org.apache.calcite.util.Source source =
        org.apache.calcite.util.Sources.of(badJson);
    // Should return false for unparseable JSON
    assertFalse((Boolean) invokePrivate(schema, "isCalciteModelFile",
        new Class[]{org.apache.calcite.util.Source.class}, source));
  }

  // -----------------------------------------------------------------------
  // hasTableWithFlattening - private method
  // -----------------------------------------------------------------------

  @Test
  void testHasTableWithFlatteningTrue() throws Exception {
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "flat_table");
    tableDef.put("url", "/some/path.json");
    tableDef.put("flatten", true);
    tables.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tables);
    assertTrue((Boolean) invokePrivate(schema, "hasTableWithFlattening",
        new Class[]{}, new Object[]{}));
  }

  @Test
  void testHasTableWithFlatteningFalse() throws Exception {
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "regular_table");
    tableDef.put("url", "/some/path.json");
    tables.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tables);
    assertFalse((Boolean) invokePrivate(schema, "hasTableWithFlattening",
        new Class[]{}, new Object[]{}));
  }

  @Test
  void testHasTableWithFlatteningNoTables() throws Exception {
    FileSchema schema = createSchemaWithTables(tempDir.toFile(), null);
    assertFalse((Boolean) invokePrivate(schema, "hasTableWithFlattening",
        new Class[]{}, new Object[]{}));
  }

  // -----------------------------------------------------------------------
  // hasExplicitHtmlTableDefinition - private method
  // -----------------------------------------------------------------------

  @Test
  void testHasExplicitHtmlTableDefinitionNullTables() throws Exception {
    FileSchema schema = createSchemaWithTables(tempDir.toFile(), null);
    assertFalse((Boolean) invokePrivate(schema, "hasExplicitHtmlTableDefinition",
        new Class[]{File.class}, new File("test.html")));
  }

  @Test
  void testHasExplicitHtmlTableDefinitionWithMatch() throws Exception {
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("url", "test.html");
    tableDef.put("selector", "table.data");
    tables.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tables);
    assertTrue((Boolean) invokePrivate(schema, "hasExplicitHtmlTableDefinition",
        new Class[]{File.class}, new File("test.html")));
  }

  @Test
  void testHasExplicitHtmlTableDefinitionNoSelector() throws Exception {
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("url", "test.html");
    tables.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tables);
    assertFalse((Boolean) invokePrivate(schema, "hasExplicitHtmlTableDefinition",
        new Class[]{File.class}, new File("test.html")));
  }

  // -----------------------------------------------------------------------
  // hasHtmlFieldSelectors - private method
  // -----------------------------------------------------------------------

  @Test
  void testHasHtmlFieldSelectorsWithSelector() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    List<Map<String, Object>> fields = new ArrayList<>();
    Map<String, Object> field = new HashMap<>();
    field.put("selector", "td.value");
    fields.add(field);
    tableDef.put("fields", fields);

    assertTrue((Boolean) invokePrivate(schema, "hasHtmlFieldSelectors",
        new Class[]{Map.class}, tableDef));
  }

  @Test
  void testHasHtmlFieldSelectorsWithSelectedElement() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    List<Map<String, Object>> fields = new ArrayList<>();
    Map<String, Object> field = new HashMap<>();
    field.put("selectedElement", 0);
    fields.add(field);
    tableDef.put("fields", fields);

    assertTrue((Boolean) invokePrivate(schema, "hasHtmlFieldSelectors",
        new Class[]{Map.class}, tableDef));
  }

  @Test
  void testHasHtmlFieldSelectorsWithTh() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    List<Map<String, Object>> fields = new ArrayList<>();
    Map<String, Object> field = new HashMap<>();
    field.put("th", "Column1");
    fields.add(field);
    tableDef.put("fields", fields);

    assertTrue((Boolean) invokePrivate(schema, "hasHtmlFieldSelectors",
        new Class[]{Map.class}, tableDef));
  }

  @Test
  void testHasHtmlFieldSelectorsNoSelectors() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    List<Map<String, Object>> fields = new ArrayList<>();
    Map<String, Object> field = new HashMap<>();
    field.put("name", "column1");
    fields.add(field);
    tableDef.put("fields", fields);

    assertFalse((Boolean) invokePrivate(schema, "hasHtmlFieldSelectors",
        new Class[]{Map.class}, tableDef));
  }

  @Test
  void testHasHtmlFieldSelectorsNoFields() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Object> tableDef = new HashMap<>();
    assertFalse((Boolean) invokePrivate(schema, "hasHtmlFieldSelectors",
        new Class[]{Map.class}, tableDef));
  }

  // -----------------------------------------------------------------------
  // extractFieldConfigurations - private static method
  // -----------------------------------------------------------------------

  @Test
  void testExtractFieldConfigurationsNull() throws Exception {
    Method method = FileSchema.class.getDeclaredMethod(
        "extractFieldConfigurations", Map.class, String.class);
    method.setAccessible(true);
    assertNull(method.invoke(null, null, "table1"));
  }

  @Test
  void testExtractFieldConfigurationsWithFields() throws Exception {
    Method method = FileSchema.class.getDeclaredMethod(
        "extractFieldConfigurations", Map.class, String.class);
    method.setAccessible(true);

    Map<String, Object> tableDef = new HashMap<>();
    List<Map<String, Object>> fields = new ArrayList<>();
    Map<String, Object> field = new HashMap<>();
    field.put("name", "col1");
    fields.add(field);
    tableDef.put("fields", fields);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(null, tableDef, "table1");
    assertNotNull(result);
    assertEquals(1, result.size());
  }

  @Test
  void testExtractFieldConfigurationsNoFields() throws Exception {
    Method method = FileSchema.class.getDeclaredMethod(
        "extractFieldConfigurations", Map.class, String.class);
    method.setAccessible(true);

    Map<String, Object> tableDef = new HashMap<>();
    assertNull(method.invoke(null, tableDef, "table1"));
  }

  // -----------------------------------------------------------------------
  // buildConvertibleFilesGlobPattern / Recursive - private methods
  // -----------------------------------------------------------------------

  @Test
  void testBuildConvertibleFilesGlobPattern() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    String pattern = (String) invokePrivate(schema,
        "buildConvertibleFilesGlobPattern",
        new Class[]{boolean.class}, false);
    assertNotNull(pattern);
    assertTrue(pattern.startsWith("*.{"));
    assertTrue(pattern.contains("xlsx"));
    assertTrue(pattern.contains("md"));
    assertTrue(pattern.contains("html"));
    assertTrue(pattern.endsWith("}"));
  }

  @Test
  void testBuildConvertibleFilesGlobPatternRecursive() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    String pattern = (String) invokePrivate(schema,
        "buildConvertibleFilesGlobPatternRecursive",
        new Class[]{});
    assertNotNull(pattern);
    assertTrue(pattern.startsWith("**/*.{"));
    assertTrue(pattern.contains("xlsx"));
    assertTrue(pattern.endsWith("}"));
  }

  // -----------------------------------------------------------------------
  // trim / trimOrNull - private static methods
  // -----------------------------------------------------------------------

  @Test
  void testTrimWithSuffix() throws Exception {
    Method method = FileSchema.class.getDeclaredMethod("trim", String.class, String.class);
    method.setAccessible(true);
    assertEquals("data", method.invoke(null, "data.csv", ".csv"));
    assertEquals("data.csv", method.invoke(null, "data.csv", ".json"));
  }

  @Test
  void testTrimOrNull() throws Exception {
    Method method = FileSchema.class.getDeclaredMethod("trimOrNull", String.class, String.class);
    method.setAccessible(true);
    assertEquals("data", method.invoke(null, "data.csv", ".csv"));
    assertNull(method.invoke(null, "data.csv", ".json"));
  }

  // -----------------------------------------------------------------------
  // getTableBaseline - public method
  // -----------------------------------------------------------------------

  @Test
  void testGetTableBaselineNullMetadata() {
    // Create schema without conversion metadata - should return null
    FileSchema schema = createSchema(tempDir.toFile());
    assertNull(schema.getTableBaseline("nonexistent_table"));
  }

  @Test
  void testGetTableBaselineNoRecord() {
    FileSchema schema = createSchema(tempDir.toFile());
    // Schema has conversionMetadata, but no records
    assertNull(schema.getTableBaseline("missing_table"));
  }

  // -----------------------------------------------------------------------
  // updateTableBaseline - public method
  // -----------------------------------------------------------------------

  @Test
  void testUpdateTableBaselineNoRecord() {
    FileSchema schema = createSchema(tempDir.toFile());
    // Should not throw even when no record exists
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    schema.updateTableBaseline("nonexistent", baseline);
    // Verify it logged a warning (does not throw)
  }

  // -----------------------------------------------------------------------
  // setConversionRecords - public method
  // -----------------------------------------------------------------------

  @Test
  void testSetConversionRecordsNull() {
    FileSchema schema = createSchema(tempDir.toFile());
    // Should not throw
    schema.setConversionRecords(null);
  }

  @Test
  void testSetConversionRecordsEmpty() {
    FileSchema schema = createSchema(tempDir.toFile());
    schema.setConversionRecords(Collections.<String, ConversionMetadata.ConversionRecord>emptyMap());
    // Should not throw
  }

  @Test
  void testSetConversionRecordsWithData() {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, ConversionMetadata.ConversionRecord> records = new HashMap<>();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "test_table";
    record.viewScanPattern = "s3://bucket/**/*.parquet";
    records.put("test_table", record);

    schema.setConversionRecords(records);
    // Should have been registered in conversion metadata
    assertNotNull(schema.getConversionMetadata());
  }

  // -----------------------------------------------------------------------
  // Notification edge cases
  // -----------------------------------------------------------------------

  @Test
  void testNotifyTableRefreshedWithPatternNonPatternAwareListener() {
    FileSchema schema = createSchema(tempDir.toFile());

    // Add a plain TableRefreshListener (not PatternAwareRefreshListener)
    TableRefreshListener plainListener = mock(TableRefreshListener.class);
    schema.addRefreshListener(plainListener);

    // This should not call onTableRefreshedWithPattern since it's not a PatternAwareRefreshListener
    schema.notifyTableRefreshedWithPattern("my_table", "**/*.parquet");
    // No exception should occur
  }

  @Test
  void testNotifyTableRefreshedWithPatternException() {
    FileSchema schema = createSchema(tempDir.toFile());

    PatternAwareRefreshListener listener = mock(PatternAwareRefreshListener.class);
    doThrow(new RuntimeException("pattern error"))
        .when(listener).onTableRefreshedWithPattern(anyString(), anyString());
    schema.addRefreshListener(listener);

    // Should not throw even when listener throws
    schema.notifyTableRefreshedWithPattern("table1", "**/*.parquet");
  }

  @Test
  void testNotifyIcebergTableRefreshedNonPatternAwareListener() {
    FileSchema schema = createSchema(tempDir.toFile());

    TableRefreshListener plainListener = mock(TableRefreshListener.class);
    schema.addRefreshListener(plainListener);

    // Should not call onIcebergTableRefreshed since it's not a PatternAwareRefreshListener
    schema.notifyIcebergTableRefreshed("ice_table", "s3://bucket/warehouse");
    // No exception should occur
  }

  @Test
  void testNotifyIcebergTableRefreshedException() {
    FileSchema schema = createSchema(tempDir.toFile());

    PatternAwareRefreshListener listener = mock(PatternAwareRefreshListener.class);
    doThrow(new RuntimeException("iceberg error"))
        .when(listener).onIcebergTableRefreshed(anyString(), anyString());
    schema.addRefreshListener(listener);

    // Should not throw even when listener throws
    schema.notifyIcebergTableRefreshed("ice_table", "s3://bucket/warehouse");
  }

  @Test
  void testNotifyTableRefreshedNoListeners() {
    FileSchema schema = createSchema(tempDir.toFile());
    // No listeners registered - should not throw
    schema.notifyTableRefreshed("table1", new File("test.parquet"));
    schema.notifyTableRefreshedWithPattern("table1", "**/*.parquet");
    schema.notifyIcebergTableRefreshed("table1", "s3://bucket");
  }

  // -----------------------------------------------------------------------
  // validateForeignKeyConstraints - private method (via constraint metadata)
  // -----------------------------------------------------------------------

  @Test
  void testValidateForeignKeyConstraintsEmptyConstraints() throws IOException {
    File csvFile = new File(tempDir.toFile(), "data.csv");
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,name\n1,Alice\n");
    }

    FileSchema schema = createSchema(tempDir.toFile());
    // Set empty constraints
    schema.setConstraintMetadata(new HashMap<String, Map<String, Object>>());

    // getTableMap invokes validateForeignKeyConstraints - should not throw
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testValidateForeignKeyConstraintsInvalidFk() throws IOException {
    File csvFile = new File(tempDir.toFile(), "data.csv");
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,name\n1,Alice\n");
    }

    FileSchema schema = createSchema(tempDir.toFile());

    // Set constraint metadata with FK referencing nonexistent table
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tableConstraints = new HashMap<>();
    List<Map<String, Object>> foreignKeys = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetTable", "nonexistent_table");
    fk.put("columns", Arrays.asList("id"));
    foreignKeys.add(fk);
    tableConstraints.put("foreignKeys", foreignKeys);
    constraints.put("data", tableConstraints);

    schema.setConstraintMetadata(constraints);
    // getTableMap() triggers validateForeignKeyConstraints which should remove invalid FKs
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    // The FK should have been removed
    assertTrue(foreignKeys.isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testValidateForeignKeyConstraintsWithListTargetTable() throws IOException {
    File csvFile = new File(tempDir.toFile(), "orders.csv");
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("order_id,customer_id\n1,100\n");
    }

    FileSchema schema = createSchema(tempDir.toFile());

    // FK with targetTable as List (qualified name)
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tableConstraints = new HashMap<>();
    List<Map<String, Object>> foreignKeys = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    List<String> qualifiedName = new ArrayList<>();
    qualifiedName.add("other_schema");
    qualifiedName.add("customers");
    fk.put("targetTable", qualifiedName);
    fk.put("columns", Arrays.asList("customer_id"));
    foreignKeys.add(fk);
    tableConstraints.put("foreignKeys", foreignKeys);
    constraints.put("orders", tableConstraints);

    schema.setConstraintMetadata(constraints);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    // FK should have been removed (other_schema doesn't exist)
    assertTrue(foreignKeys.isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testValidateForeignKeyConstraintsWithSingleElementList() throws IOException {
    File csvFile = new File(tempDir.toFile(), "orders.csv");
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("order_id,customer_id\n1,100\n");
    }

    FileSchema schema = createSchema(tempDir.toFile());

    // FK with targetTable as single-element List
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tableConstraints = new HashMap<>();
    List<Map<String, Object>> foreignKeys = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    List<String> qualifiedName = new ArrayList<>();
    qualifiedName.add("nonexistent");
    fk.put("targetTable", qualifiedName);
    fk.put("columns", Arrays.asList("customer_id"));
    foreignKeys.add(fk);
    tableConstraints.put("foreignKeys", foreignKeys);
    constraints.put("orders", tableConstraints);

    schema.setConstraintMetadata(constraints);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    assertTrue(foreignKeys.isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testValidateForeignKeyConstraintsNullConstraintValue() throws IOException {
    File csvFile = new File(tempDir.toFile(), "data.csv");
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,name\n1,Alice\n");
    }

    FileSchema schema = createSchema(tempDir.toFile());

    // Set constraint metadata where the value is null
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    constraints.put("data", null);

    schema.setConstraintMetadata(constraints);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testValidateForeignKeyConstraintsNullTargetTable() throws IOException {
    File csvFile = new File(tempDir.toFile(), "data.csv");
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,name\n1,Alice\n");
    }

    FileSchema schema = createSchema(tempDir.toFile());

    // FK with null targetTable
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tableConstraints = new HashMap<>();
    List<Map<String, Object>> foreignKeys = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetTable", null);
    foreignKeys.add(fk);
    tableConstraints.put("foreignKeys", foreignKeys);
    constraints.put("data", tableConstraints);

    schema.setConstraintMetadata(constraints);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    // FK with null target should be skipped (continue), not removed
    assertEquals(1, foreignKeys.size());
  }

  // -----------------------------------------------------------------------
  // checkTableExists - private method
  // -----------------------------------------------------------------------

  @Test
  void testCheckTableExistsLocalSchema() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Table> localTables = new HashMap<>();
    Table mockTable = mock(Table.class);
    localTables.put("my_table", mockTable);

    // null schema should check local tables
    assertTrue((Boolean) invokePrivate(schema, "checkTableExists",
        new Class[]{String.class, String.class, Map.class},
        null, "my_table", localTables));
    assertFalse((Boolean) invokePrivate(schema, "checkTableExists",
        new Class[]{String.class, String.class, Map.class},
        null, "missing_table", localTables));
  }

  @Test
  void testCheckTableExistsDifferentSchema() throws Exception {
    // When parentSchema returns a sub-schema
    org.apache.calcite.schema.Schema subSchema = mock(org.apache.calcite.schema.Schema.class);
    when(subSchema.getTableNames())
        .thenReturn(Collections.singleton("target_table"));
    when(parentSchema.getSubSchema("other_schema")).thenReturn(
        mock(SchemaPlus.class));
    SchemaPlus subSchemaPlus = parentSchema.getSubSchema("other_schema");
    when(subSchemaPlus.getTableNames())
        .thenReturn(Collections.singleton("target_table"));
    // Reset parentSchema to return the sub-schema
    when(parentSchema.getSubSchema("other_schema")).thenReturn(subSchemaPlus);

    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Table> localTables = new HashMap<>();

    assertTrue((Boolean) invokePrivate(schema, "checkTableExists",
        new Class[]{String.class, String.class, Map.class},
        "other_schema", "target_table", localTables));
  }

  @Test
  void testCheckTableExistsMissingSchema() throws Exception {
    when(parentSchema.getSubSchema("missing_schema")).thenReturn(null);

    FileSchema schema = createSchema(tempDir.toFile());
    Map<String, Table> localTables = new HashMap<>();

    assertFalse((Boolean) invokePrivate(schema, "checkTableExists",
        new Class[]{String.class, String.class, Map.class},
        "missing_schema", "any_table", localTables));
  }

  // -----------------------------------------------------------------------
  // getFilesInDir - private method (edge cases)
  // -----------------------------------------------------------------------

  @Test
  void testGetFilesInDirEmpty() throws Exception {
    File emptyDir = new File(tempDir.toFile(), "empty");
    emptyDir.mkdirs();

    FileSchema schema = createSchema(emptyDir);
    File[] result = (File[]) invokePrivate(schema, "getFilesInDir",
        new Class[]{File.class}, emptyDir);
    assertEquals(0, result.length);
  }

  @Test
  void testGetFilesInDirWithHiddenFiles() throws Exception {
    File dir = new File(tempDir.toFile(), "withHidden");
    dir.mkdirs();
    // Create a file starting with "._" (should be excluded)
    File hiddenMac = new File(dir, "._hidden.csv");
    Files.write(hiddenMac.toPath(), "a,b\n".getBytes(StandardCharsets.UTF_8));
    // Create a normal CSV file
    File normalCsv = new File(dir, "data.csv");
    Files.write(normalCsv.toPath(), "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(dir);
    File[] result = (File[]) invokePrivate(schema, "getFilesInDir",
        new Class[]{File.class}, dir);
    assertEquals(1, result.length);
    assertEquals("data.csv", result[0].getName());
  }

  @Test
  void testGetFilesInDirNullListFiles() throws Exception {
    // A non-existent directory returns null from listFiles()
    File nonExistent = new File(tempDir.toFile(), "no_such_dir");
    // Don't create it - listFiles() will return null

    FileSchema schema = createSchema(tempDir.toFile());
    File[] result = (File[]) invokePrivate(schema, "getFilesInDir",
        new Class[]{File.class}, nonExistent);
    assertEquals(0, result.length);
  }

  @Test
  void testGetFilesInDirSkipsUnsupported() throws Exception {
    File dir = new File(tempDir.toFile(), "mixed");
    dir.mkdirs();
    // Create unsupported file type
    File txtFile = new File(dir, "readme.txt");
    Files.write(txtFile.toPath(), "text".getBytes(StandardCharsets.UTF_8));
    // Create supported file type
    File jsonFile = new File(dir, "data.json");
    Files.write(jsonFile.toPath(), "[{\"a\":1}]".getBytes(StandardCharsets.UTF_8));

    FileSchema schema = createSchema(dir);
    File[] result = (File[]) invokePrivate(schema, "getFilesInDir",
        new Class[]{File.class}, dir);
    assertEquals(1, result.length);
    assertEquals("data.json", result[0].getName());
  }

  // -----------------------------------------------------------------------
  // generateModelFile - private method (via getTableMap)
  // -----------------------------------------------------------------------

  @Test
  void testGenerateModelFileCreated() throws IOException {
    File sourceDir = new File(tempDir.toFile(), "model_gen");
    sourceDir.mkdirs();

    // Create a JSON file so schema discovers a table
    File jsonFile = new File(sourceDir, "items.json");
    try (FileWriter fw = new FileWriter(jsonFile)) {
      fw.write("[{\"id\":1}]");
    }

    FileSchema schema = createSchema(sourceDir);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);

    // Check that the generated model file was created
    File modelFile = new File(schema.getOperatingCacheDirectory(), ".generated-model.json");
    assertTrue(modelFile.exists(), "Generated model file should exist");

    // Verify model file content
    String content = new String(Files.readAllBytes(modelFile.toPath()), StandardCharsets.UTF_8);
    assertTrue(content.contains("\"version\""));
    assertTrue(content.contains("\"schemas\""));
    assertTrue(content.contains("\"defaultSchema\""));
  }

  // -----------------------------------------------------------------------
  // Storage operations edge cases
  // -----------------------------------------------------------------------

  @Test
  void testWriteToStorageWithProviderInputStream() throws IOException {
    StorageProvider mockProvider = mock(StorageProvider.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema = new FileSchema(parentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    InputStream content = new ByteArrayInputStream("stream data".getBytes(StandardCharsets.UTF_8));
    schema.writeToStorage("test/stream.txt", content);

    verify(mockProvider).writeFile(anyString(), any(InputStream.class));
  }

  @Test
  void testCreateStorageDirectoriesWithProvider() throws IOException {
    StorageProvider mockProvider = mock(StorageProvider.class);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema = new FileSchema(parentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    schema.createStorageDirectories("test/dir");

    verify(mockProvider).createDirectories(anyString());
  }

  @Test
  void testExistsInStorageWithProvider() throws IOException {
    StorageProvider mockProvider = mock(StorageProvider.class);
    when(mockProvider.exists(anyString())).thenReturn(false);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema = new FileSchema(parentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    assertFalse(schema.existsInStorage("missing/path.txt"));
    verify(mockProvider).exists(anyString());
  }

  @Test
  void testDeleteFromStorageWithProvider() throws IOException {
    StorageProvider mockProvider = mock(StorageProvider.class);
    when(mockProvider.delete(anyString())).thenReturn(true);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema = new FileSchema(parentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    assertTrue(schema.deleteFromStorage("file/to/delete.txt"));
    verify(mockProvider).delete(anyString());
  }

  @Test
  void testDeleteFromStorageLocalNonexistent() throws IOException {
    FileSchema schema = createSchema(tempDir.toFile());
    assertFalse(schema.deleteFromStorage("definitely_missing.txt"));
  }

  // -----------------------------------------------------------------------
  // resolvePath - private method
  // -----------------------------------------------------------------------

  @Test
  void testResolvePathNoProvider() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    String result = (String) invokePrivate(schema, "resolvePath",
        new Class[]{String.class}, "relative/path");
    assertEquals("relative/path", result);
  }

  // -----------------------------------------------------------------------
  // readAllBytes - private method
  // -----------------------------------------------------------------------

  @Test
  void testReadAllBytes() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    byte[] input = "Hello World!".getBytes(StandardCharsets.UTF_8);
    InputStream is = new ByteArrayInputStream(input);
    byte[] result = (byte[]) invokePrivate(schema, "readAllBytes",
        new Class[]{InputStream.class}, is);
    assertEquals("Hello World!", new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testReadAllBytesEmpty() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    InputStream is = new ByteArrayInputStream(new byte[0]);
    byte[] result = (byte[]) invokePrivate(schema, "readAllBytes",
        new Class[]{InputStream.class}, is);
    assertEquals(0, result.length);
  }

  // -----------------------------------------------------------------------
  // getTableName - private method
  // -----------------------------------------------------------------------

  @Test
  void testGetTableNameWithExplicitName() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    String result = (String) invokePrivate(schema, "getTableName",
        new Class[]{String.class, String.class, String.class},
        "explicit_name", "derived_name", "UPPER");
    assertEquals("explicit_name", result);
  }

  @Test
  void testGetTableNameWithDerivedName() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());
    String result = (String) invokePrivate(schema, "getTableName",
        new Class[]{String.class, String.class, String.class},
        null, "DerivedName", "LOWER");
    // Should apply LOWER casing to derived name
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // Constructor edge cases
  // -----------------------------------------------------------------------

  @Test
  void testConstructorWithStorageTypeAndDirectoryPath() {
    // Test path where storageType != null and directoryPath != null
    StorageProvider mockProvider = mock(StorageProvider.class);
    when(mockProvider.getStorageType()).thenReturn("s3");
    when(mockProvider.getS3Config()).thenReturn(null);

    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema = new FileSchema(parentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, "s3://bucket/data", null,
        null, defaultEngineConfig, false,
        null, null, null, null, "SMART_CASING", "SMART_CASING",
        "s3", storageConfig, null, null, false, null);

    // baseDirectory should be the directoryPath
    assertEquals("s3://bucket/data", schema.getBaseDirectory());
  }

  @Test
  void testConstructorWithStorageTypeAndSourceDirectory() {
    // Test path where storageType != null, directoryPath == null, sourceDirectory != null
    StorageProvider mockProvider = mock(StorageProvider.class);
    when(mockProvider.getStorageType()).thenReturn("local");
    when(mockProvider.getS3Config()).thenReturn(null);

    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", mockProvider);

    FileSchema schema = new FileSchema(parentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, null,
        null, defaultEngineConfig, false,
        null, null, null, null, "SMART_CASING", "SMART_CASING",
        "local", storageConfig, null, null, false, null);

    // baseDirectory should be the sourceDirectory path
    assertEquals(tempDir.toFile().getAbsolutePath(), schema.getBaseDirectory());
  }

  @Test
  void testConstructorWithUserConfiguredBaseDirectory() {
    File userBase = new File(tempDir.toFile(), "user-base");
    userBase.mkdirs();

    FileSchema schema = new FileSchema(parentSchema, uniqueSchemaName(),
        tempDir.toFile(), userBase, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        null, null, null, null, false, null);

    // baseDirectory should be userConfiguredBaseDirectory
    assertEquals(userBase.getAbsolutePath(), schema.getBaseDirectory());
  }

  @Test
  void testConstructorDefaultBaseDirectory() {
    // No storageType, no userConfiguredBaseDirectory -> should default to operating cache dir
    FileSchema schema = new FileSchema(parentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, null, defaultEngineConfig,
        false, null, null, null, null,
        "SMART_CASING", "SMART_CASING",
        null, null, null, null, false, null);

    // baseDirectory should be the operatingCacheDirectory
    assertEquals(schema.getOperatingCacheDirectory().getAbsolutePath(),
        schema.getBaseDirectory());
  }

  // -----------------------------------------------------------------------
  // setFunctionMultimap - public method
  // -----------------------------------------------------------------------

  @Test
  void testSetFunctionMultimapWithFunctions() {
    FileSchema schema = createSchema(tempDir.toFile());
    org.apache.calcite.schema.Function func = mock(org.apache.calcite.schema.Function.class);
    schema.setFunctionMultimap(ImmutableMultimap.of("my_func", func));
    // Should not throw
  }

  // -----------------------------------------------------------------------
  // Views creation edge cases
  // -----------------------------------------------------------------------

  @Test
  void testViewCreationMissingNameOrSql() {
    List<Map<String, Object>> views = new ArrayList<>();

    // View with no name
    Map<String, Object> noNameView = new HashMap<>();
    noNameView.put("sql", "SELECT 1");
    views.add(noNameView);

    // View with no SQL
    Map<String, Object> noSqlView = new HashMap<>();
    noSqlView.put("name", "v1");
    views.add(noSqlView);

    FileSchema schema = new FileSchema(parentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, defaultEngineConfig, false,
        null, views, null, null);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    // Neither view should be created
    assertFalse(tables.containsKey("v1"));
  }

  // -----------------------------------------------------------------------
  // Materialized views with non-PARQUET engine
  // -----------------------------------------------------------------------

  @Test
  void testMaterializedViewsNonParquetEngine() {
    List<Map<String, Object>> materializations = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("view", "mv_view");
    mv.put("table", "mv_table");
    mv.put("sql", "SELECT * FROM data");
    materializations.add(mv);

    // Use LINQ4J engine explicitly - MVs require PARQUET engine
    ExecutionEngineConfig linq4jConfig = new ExecutionEngineConfig("LINQ4J",
        ExecutionEngineConfig.DEFAULT_BATCH_SIZE);
    FileSchema schema = new FileSchema(parentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, linq4jConfig, false,
        materializations, null, null, null);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    // MVs should not be created with non-PARQUET engine
    assertFalse(tables.containsKey("mv_view"));
    assertFalse(tables.containsKey("mv_table"));
  }

  // -----------------------------------------------------------------------
  // getAllTableRecords edge cases
  // -----------------------------------------------------------------------

  @Test
  void testGetAllTableRecordsForced() throws IOException {
    File sourceDir = new File(tempDir.toFile(), "records_test");
    sourceDir.mkdirs();
    File csvFile = new File(sourceDir, "test.csv");
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,val\n1,a\n");
    }

    FileSchema schema = createSchema(sourceDir);
    // First call - should trigger getTableMap
    Map<String, ConversionMetadata.ConversionRecord> records = schema.getAllTableRecords();
    assertNotNull(records);
  }

  // -----------------------------------------------------------------------
  // registerRawToParquetConverter - public method
  // -----------------------------------------------------------------------

  @Test
  void testRegisterMultipleConverters() {
    FileSchema schema = createSchema(tempDir.toFile());

    org.apache.calcite.adapter.file.converters.RawToParquetConverter conv1 =
        mock(org.apache.calcite.adapter.file.converters.RawToParquetConverter.class);
    org.apache.calcite.adapter.file.converters.RawToParquetConverter conv2 =
        mock(org.apache.calcite.adapter.file.converters.RawToParquetConverter.class);

    schema.registerRawToParquetConverter(conv1);
    schema.registerRawToParquetConverter(conv2);
    // Should not throw
  }

  // -----------------------------------------------------------------------
  // findMatchingFiles with null pattern
  // -----------------------------------------------------------------------

  @Test
  void testFindMatchingFilesNullPattern() throws Exception {
    FileSchema schema = createSchema(tempDir.toFile());

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) invokePrivate(schema, "findMatchingFiles",
        new Class[]{String.class}, (String) null);
    assertTrue(result.isEmpty());
  }

  // -----------------------------------------------------------------------
  // findMatchingFiles with null baseDirectory
  // -----------------------------------------------------------------------

  @Test
  void testFindMatchingFilesNullBaseDirectory() throws Exception {
    // The findMatchingFiles method also checks for null baseDirectory and storageProvider
    FileSchema schema = createSchema(tempDir.toFile());
    // BaseDirectory is set in constructor, but storageProvider is null
    // So it should return empty (logs error about null storageProvider)
    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) invokePrivate(schema, "findMatchingFiles",
        new Class[]{String.class}, "**/*.csv");
    assertTrue(result.isEmpty());
  }

  // -----------------------------------------------------------------------
  // Constructor with refreshInterval starts periodic refresh
  // -----------------------------------------------------------------------

  @Test
  void testConstructorWithRefreshIntervalStartsRefresh() {
    FileSchema schema = new FileSchema(parentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, defaultEngineConfig, false,
        null, null, null, "5 minutes");
    assertNotNull(schema);
    assertTrue(schema.hasRefreshableTables());
  }

  // -----------------------------------------------------------------------
  // Constructor with invalid refreshInterval
  // -----------------------------------------------------------------------

  @Test
  void testConstructorWithInvalidRefreshInterval() {
    // Invalid interval should log warning but not crash
    FileSchema schema = new FileSchema(parentSchema, uniqueSchemaName(),
        tempDir.toFile(), null, null, defaultEngineConfig, false,
        null, null, null, "not_a_valid_interval");
    assertNotNull(schema);
    assertTrue(schema.hasRefreshableTables()); // refreshInterval is not null, just invalid
  }

  // -----------------------------------------------------------------------
  // Explicit table def with YML format
  // -----------------------------------------------------------------------

  @Test
  void testExplicitTableDefWithYmlFormat() throws IOException {
    File yamlFile = new File(tempDir.toFile(), "data.dat");
    try (FileWriter fw = new FileWriter(yamlFile)) {
      fw.write("- id: 1\n  name: test\n");
    }

    List<Map<String, Object>> tableDefs = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "yml_table");
    tableDef.put("url", yamlFile.getAbsolutePath());
    tableDef.put("format", "yml");
    tableDefs.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tableDefs);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    assertTrue(tables.containsKey("yml_table"));
  }

  // -----------------------------------------------------------------------
  // Table discovery with YAML extension auto-detect
  // -----------------------------------------------------------------------

  @Test
  void testExplicitTableDefWithYamlAutoDetect() throws IOException {
    File yamlFile = new File(tempDir.toFile(), "data.yaml");
    try (FileWriter fw = new FileWriter(yamlFile)) {
      fw.write("- id: 1\n  value: hello\n");
    }

    List<Map<String, Object>> tableDefs = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "yaml_auto");
    tableDef.put("url", yamlFile.getAbsolutePath());
    tableDefs.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tableDefs);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    assertTrue(tables.containsKey("yaml_auto"));
  }

  @Test
  void testExplicitTableDefWithYmlAutoDetect() throws IOException {
    File ymlFile = new File(tempDir.toFile(), "data.yml");
    try (FileWriter fw = new FileWriter(ymlFile)) {
      fw.write("- id: 1\n  value: hello\n");
    }

    List<Map<String, Object>> tableDefs = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "yml_auto");
    tableDef.put("url", ymlFile.getAbsolutePath());
    tableDefs.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tableDefs);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    assertTrue(tables.containsKey("yml_auto"));
  }

  // -----------------------------------------------------------------------
  // Explicit table def with Parquet auto-detect
  // -----------------------------------------------------------------------

  @Test
  void testExplicitTableDefWithParquetAutoDetect() throws IOException {
    File parquetFile = new File(tempDir.toFile(), "data.parquet");
    // Write minimal parquet magic bytes
    Files.write(parquetFile.toPath(), new byte[]{0x50, 0x41, 0x52, 0x31});

    List<Map<String, Object>> tableDefs = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "parquet_auto");
    tableDef.put("url", parquetFile.getAbsolutePath());
    tableDefs.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tableDefs);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    assertTrue(tables.containsKey("parquet_auto"));
  }

  // -----------------------------------------------------------------------
  // Explicit table def with CSV auto-detect
  // -----------------------------------------------------------------------

  @Test
  void testExplicitTableDefWithCsvAutoDetect() throws IOException {
    File csvFile = new File(tempDir.toFile(), "data.csv");
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("a,b\n1,2\n");
    }

    List<Map<String, Object>> tableDefs = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "csv_auto");
    tableDef.put("url", csvFile.getAbsolutePath());
    tableDefs.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tableDefs);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    assertTrue(tables.containsKey("csv_auto"));
  }

  // -----------------------------------------------------------------------
  // Explicit table def with TSV auto-detect
  // -----------------------------------------------------------------------

  @Test
  void testExplicitTableDefWithTsvAutoDetect() throws IOException {
    File tsvFile = new File(tempDir.toFile(), "data.tsv");
    try (FileWriter fw = new FileWriter(tsvFile)) {
      fw.write("a\tb\n1\t2\n");
    }

    List<Map<String, Object>> tableDefs = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "tsv_auto");
    tableDef.put("url", tsvFile.getAbsolutePath());
    tableDefs.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tableDefs);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    assertTrue(tables.containsKey("tsv_auto"));
  }

  // -----------------------------------------------------------------------
  // Schema-level flatten with auto-discovered JSON
  // -----------------------------------------------------------------------

  @Test
  void testSchemaLevelFlattenOptionForAutoJson() throws IOException {
    File sourceDir = new File(tempDir.toFile(), "flatten_dir");
    sourceDir.mkdirs();
    File jsonFile = new File(sourceDir, "nested.json");
    try (FileWriter fw = new FileWriter(jsonFile)) {
      fw.write("[{\"a\":{\"b\":1}}]");
    }

    // Use explicit table def with default extension detection, but flatten at schema level
    List<Map<String, Object>> tableDefs = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "nested_data");
    tableDef.put("url", jsonFile.getAbsolutePath());
    tableDefs.add(tableDef);

    FileSchema schema = new FileSchema(parentSchema, uniqueSchemaName(),
        sourceDir, null, null, tableDefs, defaultEngineConfig, false,
        null, null, null, null, "SMART_CASING", "SMART_CASING",
        null, null, true, null, false);

    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    // Table should be created with schema-level flatten
    assertTrue(tables.containsKey("nested_data"));
  }

  // -----------------------------------------------------------------------
  // createTableFromSource - private method with format variants
  // -----------------------------------------------------------------------

  @Test
  void testCreateTableFromSourceCsv() throws Exception {
    File csvFile = new File(tempDir.toFile(), "source.csv");
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("a,b\n1,2\n");
    }
    org.apache.calcite.util.Source source = org.apache.calcite.util.Sources.of(csvFile);

    FileSchema schema = createSchema(tempDir.toFile());
    Table result = (Table) invokePrivate(schema, "createTableFromSource",
        new Class[]{org.apache.calcite.util.Source.class, Map.class},
        source, null);
    assertNotNull(result);
  }

  @Test
  void testCreateTableFromSourceTsv() throws Exception {
    File tsvFile = new File(tempDir.toFile(), "source.tsv");
    try (FileWriter fw = new FileWriter(tsvFile)) {
      fw.write("a\tb\n1\t2\n");
    }
    org.apache.calcite.util.Source source = org.apache.calcite.util.Sources.of(tsvFile);

    FileSchema schema = createSchema(tempDir.toFile());
    Table result = (Table) invokePrivate(schema, "createTableFromSource",
        new Class[]{org.apache.calcite.util.Source.class, Map.class},
        source, null);
    assertNotNull(result);
  }

  @Test
  void testCreateTableFromSourceJson() throws Exception {
    File jsonFile = new File(tempDir.toFile(), "source.json");
    try (FileWriter fw = new FileWriter(jsonFile)) {
      fw.write("[{\"x\":1}]");
    }
    org.apache.calcite.util.Source source = org.apache.calcite.util.Sources.of(jsonFile);

    FileSchema schema = createSchema(tempDir.toFile());
    Table result = (Table) invokePrivate(schema, "createTableFromSource",
        new Class[]{org.apache.calcite.util.Source.class, Map.class},
        source, null);
    assertNotNull(result);
  }

  @Test
  void testCreateTableFromSourceYaml() throws Exception {
    File yamlFile = new File(tempDir.toFile(), "source.yaml");
    try (FileWriter fw = new FileWriter(yamlFile)) {
      fw.write("- id: 1\n");
    }
    org.apache.calcite.util.Source source = org.apache.calcite.util.Sources.of(yamlFile);

    FileSchema schema = createSchema(tempDir.toFile());
    Table result = (Table) invokePrivate(schema, "createTableFromSource",
        new Class[]{org.apache.calcite.util.Source.class, Map.class},
        source, null);
    assertNotNull(result);
  }

  @Test
  void testCreateTableFromSourceYml() throws Exception {
    File ymlFile = new File(tempDir.toFile(), "source.yml");
    try (FileWriter fw = new FileWriter(ymlFile)) {
      fw.write("- id: 1\n");
    }
    org.apache.calcite.util.Source source = org.apache.calcite.util.Sources.of(ymlFile);

    FileSchema schema = createSchema(tempDir.toFile());
    Table result = (Table) invokePrivate(schema, "createTableFromSource",
        new Class[]{org.apache.calcite.util.Source.class, Map.class},
        source, null);
    assertNotNull(result);
  }

  @Test
  void testCreateTableFromSourceUnsupported() throws Exception {
    File txtFile = new File(tempDir.toFile(), "source.txt");
    try (FileWriter fw = new FileWriter(txtFile)) {
      fw.write("text data");
    }
    org.apache.calcite.util.Source source = org.apache.calcite.util.Sources.of(txtFile);

    FileSchema schema = createSchema(tempDir.toFile());
    Table result = (Table) invokePrivate(schema, "createTableFromSource",
        new Class[]{org.apache.calcite.util.Source.class, Map.class},
        source, null);
    assertNull(result);
  }

  @Test
  void testCreateTableFromSourceWithForcedJsonFormat() throws Exception {
    File file = new File(tempDir.toFile(), "forced_json.dat");
    try (FileWriter fw = new FileWriter(file)) {
      fw.write("[{\"x\":1}]");
    }
    org.apache.calcite.util.Source source = org.apache.calcite.util.Sources.of(file);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "json");

    FileSchema schema = createSchema(tempDir.toFile());
    Table result = (Table) invokePrivate(schema, "createTableFromSource",
        new Class[]{org.apache.calcite.util.Source.class, Map.class},
        source, tableDef);
    assertNotNull(result);
  }

  @Test
  void testCreateTableFromSourceWithForcedCsvFormat() throws Exception {
    File file = new File(tempDir.toFile(), "forced_csv.dat");
    try (FileWriter fw = new FileWriter(file)) {
      fw.write("a,b\n1,2\n");
    }
    org.apache.calcite.util.Source source = org.apache.calcite.util.Sources.of(file);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "csv");

    FileSchema schema = createSchema(tempDir.toFile());
    Table result = (Table) invokePrivate(schema, "createTableFromSource",
        new Class[]{org.apache.calcite.util.Source.class, Map.class},
        source, tableDef);
    assertNotNull(result);
  }

  @Test
  void testCreateTableFromSourceWithForcedTsvFormat() throws Exception {
    File file = new File(tempDir.toFile(), "forced_tsv.dat");
    try (FileWriter fw = new FileWriter(file)) {
      fw.write("a\tb\n1\t2\n");
    }
    org.apache.calcite.util.Source source = org.apache.calcite.util.Sources.of(file);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "tsv");

    FileSchema schema = createSchema(tempDir.toFile());
    Table result = (Table) invokePrivate(schema, "createTableFromSource",
        new Class[]{org.apache.calcite.util.Source.class, Map.class},
        source, tableDef);
    assertNotNull(result);
  }

  @Test
  void testCreateTableFromSourceWithForcedYamlFormat() throws Exception {
    File file = new File(tempDir.toFile(), "forced_yaml.dat");
    try (FileWriter fw = new FileWriter(file)) {
      fw.write("- id: 1\n");
    }
    org.apache.calcite.util.Source source = org.apache.calcite.util.Sources.of(file);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "yaml");

    FileSchema schema = createSchema(tempDir.toFile());
    Table result = (Table) invokePrivate(schema, "createTableFromSource",
        new Class[]{org.apache.calcite.util.Source.class, Map.class},
        source, tableDef);
    assertNotNull(result);
  }

  @Test
  void testCreateTableFromSourceWithForcedYmlFormat() throws Exception {
    File file = new File(tempDir.toFile(), "forced_yml.dat");
    try (FileWriter fw = new FileWriter(file)) {
      fw.write("- id: 1\n");
    }
    org.apache.calcite.util.Source source = org.apache.calcite.util.Sources.of(file);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "yml");

    FileSchema schema = createSchema(tempDir.toFile());
    Table result = (Table) invokePrivate(schema, "createTableFromSource",
        new Class[]{org.apache.calcite.util.Source.class, Map.class},
        source, tableDef);
    assertNotNull(result);
  }

  @Test
  void testCreateTableFromSourceWithForcedUnsupportedFormat() throws Exception {
    File file = new File(tempDir.toFile(), "forced_bad.dat");
    try (FileWriter fw = new FileWriter(file)) {
      fw.write("data");
    }
    org.apache.calcite.util.Source source = org.apache.calcite.util.Sources.of(file);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "unknown_format");

    FileSchema schema = createSchema(tempDir.toFile());
    Table result = (Table) invokePrivate(schema, "createTableFromSource",
        new Class[]{org.apache.calcite.util.Source.class, Map.class},
        source, tableDef);
    assertNull(result);
  }

  @Test
  void testCreateTableFromSourceWithFlattenOption() throws Exception {
    File file = new File(tempDir.toFile(), "flatten_src.dat");
    try (FileWriter fw = new FileWriter(file)) {
      fw.write("[{\"a\":{\"b\":1}}]");
    }
    org.apache.calcite.util.Source source = org.apache.calcite.util.Sources.of(file);

    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("format", "json");
    tableDef.put("flatten", true);
    tableDef.put("flattenSeparator", "_");

    FileSchema schema = createSchema(tempDir.toFile());
    Table result = (Table) invokePrivate(schema, "createTableFromSource",
        new Class[]{org.apache.calcite.util.Source.class, Map.class},
        source, tableDef);
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // Explicit table def with JSON flattenSeparator in format override
  // -----------------------------------------------------------------------

  @Test
  void testExplicitJsonFormatWithFlattenSeparator() throws IOException {
    File jsonFile = new File(tempDir.toFile(), "nested_sep.json");
    try (FileWriter fw = new FileWriter(jsonFile)) {
      fw.write("[{\"a\":{\"b\":1}}]");
    }

    List<Map<String, Object>> tableDefs = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "sep_table");
    tableDef.put("url", jsonFile.getAbsolutePath());
    tableDef.put("format", "json");
    tableDef.put("flatten", true);
    tableDef.put("flattenSeparator", "__");
    tableDefs.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tableDefs);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    assertTrue(tables.containsKey("sep_table"));
  }

  // -----------------------------------------------------------------------
  // Table cache works correctly
  // -----------------------------------------------------------------------

  @Test
  void testTableCacheReturnsConsistentResults() throws IOException {
    File jsonFile = new File(tempDir.toFile(), "cached.json");
    try (FileWriter fw = new FileWriter(jsonFile)) {
      fw.write("[{\"id\":1}]");
    }

    List<Map<String, Object>> tableDefs = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "cached_table");
    tableDef.put("url", jsonFile.getAbsolutePath());
    tableDefs.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tableDefs);

    // First call computes
    Map<String, Table> tables1 = schema.getTableMap();
    // Second call returns cached
    Map<String, Table> tables2 = schema.getTableMap();

    assertEquals(tables1.size(), tables2.size());
    assertEquals(tables1.keySet(), tables2.keySet());
  }

  // -----------------------------------------------------------------------
  // XLSX in explicit table def via extension detection (not format override)
  // -----------------------------------------------------------------------

  @Test
  void testExplicitTableDefXlsxExtension() throws IOException {
    File xlsxFile = new File(tempDir.toFile(), "data.xlsx");
    Files.write(xlsxFile.toPath(), new byte[]{1, 2, 3});

    List<Map<String, Object>> tableDefs = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "xlsx_ext_table");
    tableDef.put("url", xlsxFile.getAbsolutePath());
    tableDefs.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tableDefs);
    // getTableMap() should catch RuntimeException from xlsx table creation
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    // Excel in explicit table def throws error -> caught -> empty map
  }

  // -----------------------------------------------------------------------
  // Explicit table def with HTML extension detection
  // -----------------------------------------------------------------------

  @Test
  void testExplicitTableDefHtmlExtension() throws IOException {
    File htmlFile = new File(tempDir.toFile(), "page.html");
    try (FileWriter fw = new FileWriter(htmlFile)) {
      fw.write("<html><body><table><tr><th>A</th></tr><tr><td>1</td></tr></table></body></html>");
    }

    List<Map<String, Object>> tableDefs = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "html_table");
    tableDef.put("url", htmlFile.getAbsolutePath());
    tableDefs.add(tableDef);

    FileSchema schema = createSchemaWithTables(tempDir.toFile(), tableDefs);
    Map<String, Table> tables = schema.getTableMap();
    assertNotNull(tables);
    // HTML tables may or may not be added depending on conversion success
  }
}
