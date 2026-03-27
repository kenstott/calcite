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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.sql.parser.SqlParser;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for DuckDBJdbcSchemaFactory focusing on uncovered branches:
 * isTempDirectory, determineCatalogPath, isHivePartitioned, deriveGlobPattern,
 * getParserConfig, createParquetView error path, and createDuckDBDialectWithCustomLex.
 */
@Tag("unit")
public class DuckDBJdbcSchemaFactoryDeepCoverageTest4 {

  @TempDir
  Path tempDir;

  // ====== isTempDirectory tests ======

  @Test
  void testIsTempDirectoryNull() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, (String) null));
  }

  @Test
  void testIsTempDirectoryUnixTmp() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "/tmp/data"));
    assertTrue((Boolean) method.invoke(null, "/tmp"));
    assertTrue((Boolean) method.invoke(null, "/some/path/tmp/here"));
  }

  @Test
  void testIsTempDirectoryWindowsTemp() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "C:\\Users\\temp\\data"));
    assertTrue((Boolean) method.invoke(null, "D:\\temp\\files"));
  }

  @Test
  void testIsTempDirectoryUnixTemp() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "/some/path/temp/here"));
  }

  @Test
  void testIsTempDirectoryJavaIoTmpDir() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "/var/java.io.tmpdir/files"));
  }

  @Test
  void testIsTempDirectoryNonTemp() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, "/home/user/data"));
    assertFalse((Boolean) method.invoke(null, "/var/lib/data"));
    assertFalse((Boolean) method.invoke(null, "s3://my-bucket/data"));
  }

  @Test
  void testIsTempDirectoryStartsWithTmp() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "/tmp"));
  }

  @Test
  void testIsTempDirectoryBackslashTmp() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isTempDirectory", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "\\tmp\\data"));
  }

  // ====== determineCatalogPath tests ======

  @Test
  void testDetermineCatalogPathNullDirectory() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "determineCatalogPath", String.class, String.class);
    method.setAccessible(true);
    assertNull(method.invoke(null, "test", null));
  }

  @Test
  void testDetermineCatalogPathTempDirectory() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "determineCatalogPath", String.class, String.class);
    method.setAccessible(true);
    assertNull(method.invoke(null, "test", "/tmp/data"));
  }

  @Test
  void testDetermineCatalogPathPersistentDirectory() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "determineCatalogPath", String.class, String.class);
    method.setAccessible(true);

    String dirPath = tempDir.toAbsolutePath().toString();
    String result = (String) method.invoke(null, "test_schema", dirPath);

    assertNotNull(result);
    assertTrue(result.contains("test_schema_db.duckdb"));
    assertTrue(result.contains(".duckdb"));
  }

  // ====== isHivePartitioned tests ======

  @Test
  void testIsHivePartitionedNull() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, (String) null));
  }

  @Test
  void testIsHivePartitionedEmpty() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, ""));
  }

  @Test
  void testIsHivePartitionedSingleFile() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, "/data/file.parquet"));
  }

  @Test
  void testIsHivePartitionedBracketedList() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);

    String fileList =
        "['/data/year=2020/state=CA/file.parquet','/data/year=2021/state=NY/file.parquet']";
    assertTrue((Boolean) method.invoke(null, fileList));
  }

  @Test
  void testIsHivePartitionedCurlyBracketedList() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);

    String fileList =
        "{'/data/year=2020/file.parquet','/data/year=2021/file.parquet'}";
    assertTrue((Boolean) method.invoke(null, fileList));
  }

  @Test
  void testIsHivePartitionedNotPartitioned() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);

    String fileList = "/data/file1.parquet,/data/file2.parquet";
    assertFalse((Boolean) method.invoke(null, fileList));
  }

  @Test
  void testIsHivePartitionedQuotedFiles() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);

    String fileList =
        "'/data/year=2020/file.parquet','/data/year=2021/file.parquet'";
    assertTrue((Boolean) method.invoke(null, fileList));
  }

  @Test
  void testIsHivePartitionedMixedPartitionStatus() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("isHivePartitioned", String.class);
    method.setAccessible(true);

    // Only 1 out of 3 files is partitioned (< 50%)
    String fileList =
        "/data/year=2020/file.parquet,/plain/file1.parquet,/plain/file2.parquet";
    assertFalse((Boolean) method.invoke(null, fileList));
  }

  // ====== deriveGlobPattern tests ======

  @Test
  void testDeriveGlobPatternNull() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);
    assertNull(method.invoke(null, (String) null));
  }

  @Test
  void testDeriveGlobPatternEmpty() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);
    assertNull(method.invoke(null, ""));
  }

  @Test
  void testDeriveGlobPatternBracketedList() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);

    String fileList =
        "['/data/year=2020/state=CA/data.parquet','/data/year=2021/state=NY/data.parquet']";
    String result = (String) method.invoke(null, fileList);
    assertNotNull(result);
    assertTrue(result.contains("/**/*"));
    assertTrue(result.endsWith(".parquet"));
  }

  @Test
  void testDeriveGlobPatternCurlyBrackets() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);

    String fileList =
        "{'/data/year=2020/file.parquet','/data/year=2021/file.parquet'}";
    String result = (String) method.invoke(null, fileList);
    assertNotNull(result);
    assertTrue(result.contains("/**/*"));
  }

  @Test
  void testDeriveGlobPatternQuotedFile() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);

    String fileList = "'/data/year=2020/state=CA/file.parquet'";
    String result = (String) method.invoke(null, fileList);
    assertNotNull(result);
    assertTrue(result.contains("/**/*"));
  }

  @Test
  void testDeriveGlobPatternNoPartition() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);

    String fileList = "/data/plain/file.parquet";
    String result = (String) method.invoke(null, fileList);
    assertNotNull(result);
    // Should fall back to parent directory glob
    assertTrue(result.contains("/*.parquet"));
  }

  @Test
  void testDeriveGlobPatternNoSlash() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);

    String fileList = "file.parquet";
    String result = (String) method.invoke(null, fileList);
    // When no partition and no slash in path, returns the file itself
    assertEquals("file.parquet", result);
  }

  @Test
  void testDeriveGlobPatternCustomExtension() throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("deriveGlobPattern", String.class);
    method.setAccessible(true);

    String fileList = "/data/year=2020/file.csv";
    String result = (String) method.invoke(null, fileList);
    assertNotNull(result);
    assertTrue(result.endsWith(".csv"));
  }

  // ====== getParserConfig tests ======

  @Test
  void testGetParserConfig() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    assertNotNull(config);
  }

  // ====== createParquetView tests ======

  @Test
  void testCreateParquetViewWithNullConnection() {
    try {
      DuckDBJdbcSchemaFactory.createParquetView(null, "test_view", "/data/test.parquet");
      fail("Expected NullPointerException");
    } catch (RuntimeException e) {
      // Expected
    }
  }
}
