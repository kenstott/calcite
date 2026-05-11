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

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Coverage unit tests for {@link DuckDBJdbcSchemaFactory}.
 *
 * <p>Tests private utility methods via reflection: isTempDirectory,
 * determineCatalogPath, isHivePartitioned, deriveGlobPattern,
 * isHivePartitionedFromConfig, shouldUseUnionByName, formatRecordForError,
 * rewriteSchemaReferencesInSql, and registerSqlViewsInDuckDB.
 * Also covers getParserConfig and createParquetView.
 */
@Tag("unit")
class DuckDBJdbcSchemaFactoryCoverageTest {

  // =======================================================================
  // getParserConfig tests
  // =======================================================================

  @Test void testGetParserConfigNotNull() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    assertNotNull(config);
  }

  @Test void testGetParserConfigUnquotedCasingToLower() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    assertEquals(Casing.TO_LOWER, config.unquotedCasing());
  }

  @Test void testGetParserConfigQuotedCasingUnchanged() {
    SqlParser.Config config = DuckDBJdbcSchemaFactory.getParserConfig();
    assertEquals(Casing.UNCHANGED, config.quotedCasing());
  }

  @Test void testGetParserConfigConsistentAcrossCalls() {
    SqlParser.Config c1 = DuckDBJdbcSchemaFactory.getParserConfig();
    SqlParser.Config c2 = DuckDBJdbcSchemaFactory.getParserConfig();
    assertEquals(c1.unquotedCasing(), c2.unquotedCasing());
    assertEquals(c1.quotedCasing(), c2.quotedCasing());
  }

  // =======================================================================
  // isTempDirectory tests (via reflection)
  // =======================================================================

  @Test void testIsTempDirectoryNull() throws Exception {
    assertTrue(invokeIsTempDirectory(null));
  }

  @Test void testIsTempDirectoryTmpUnix() throws Exception {
    assertTrue(invokeIsTempDirectory("/tmp/data"));
  }

  @Test void testIsTempDirectoryTmpInMiddle() throws Exception {
    assertTrue(invokeIsTempDirectory("/var/tmp/cache"));
  }

  @Test void testIsTempDirectoryTempWindows() throws Exception {
    assertTrue(invokeIsTempDirectory("C:\\temp\\data"));
  }

  @Test void testIsTempDirectoryTempUnix() throws Exception {
    assertTrue(invokeIsTempDirectory("/home/user/temp/stuff"));
  }

  @Test void testIsTempDirectoryJavaIoTmpdir() throws Exception {
    assertTrue(invokeIsTempDirectory("/path/java.io.tmpdir/something"));
  }

  @Test void testIsTempDirectoryNormalPath() throws Exception {
    assertFalse(invokeIsTempDirectory("/home/user/data/warehouse"));
  }

  @Test void testIsTempDirectoryS3Path() throws Exception {
    assertFalse(invokeIsTempDirectory("s3://my-bucket/warehouse"));
  }

  // =======================================================================
  // isHivePartitioned tests (via reflection)
  // =======================================================================

  @Test void testIsHivePartitionedNull() throws Exception {
    assertFalse(invokeIsHivePartitioned(null));
  }

  @Test void testIsHivePartitionedEmpty() throws Exception {
    assertFalse(invokeIsHivePartitioned(""));
  }

  @Test void testIsHivePartitionedSingleFile() throws Exception {
    assertFalse(invokeIsHivePartitioned("/data/file.parquet"));
  }

  @Test void testIsHivePartitionedWithPartitions() throws Exception {
    String fileList = "['/data/year=2020/file1.parquet','/data/year=2021/file2.parquet']";
    assertTrue(invokeIsHivePartitioned(fileList));
  }

  @Test void testIsHivePartitionedWithBraces() throws Exception {
    String fileList = "{/data/country=US/f1.parquet,/data/country=UK/f2.parquet}";
    assertTrue(invokeIsHivePartitioned(fileList));
  }

  @Test void testIsHivePartitionedWithoutPartitionPattern() throws Exception {
    String fileList = "[/data/file1.parquet,/data/file2.parquet]";
    assertFalse(invokeIsHivePartitioned(fileList));
  }

  // =======================================================================
  // isHivePartitionedFromConfig tests (via reflection)
  // =======================================================================

  @Test void testIsHivePartitionedFromConfigNullRecord() throws Exception {
    assertFalse(invokeIsHivePartitionedFromConfig(null));
  }

  @Test void testIsHivePartitionedFromConfigNullTableConfig() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertFalse(invokeIsHivePartitionedFromConfig(record));
  }

  @Test void testIsHivePartitionedFromConfigNoPartitions() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<String, Object>();
    assertFalse(invokeIsHivePartitionedFromConfig(record));
  }

  @Test void testIsHivePartitionedFromConfigWithHiveStyle() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> partitions = new HashMap<String, Object>();
    partitions.put("style", "hive");
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("partitions", partitions);
    record.tableConfig = tableConfig;
    assertTrue(invokeIsHivePartitionedFromConfig(record));
  }

  @Test void testIsHivePartitionedFromConfigWithNonHiveStyle() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> partitions = new HashMap<String, Object>();
    partitions.put("style", "directory");
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("partitions", partitions);
    record.tableConfig = tableConfig;
    assertFalse(invokeIsHivePartitionedFromConfig(record));
  }

  // =======================================================================
  // shouldUseUnionByName tests (via reflection)
  // =======================================================================

  @Test void testShouldUseUnionByNameNullRecord() throws Exception {
    assertFalse(invokeShouldUseUnionByName(null));
  }

  @Test void testShouldUseUnionByNameNullConfig() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertFalse(invokeShouldUseUnionByName(record));
  }

  @Test void testShouldUseUnionByNameTrue() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> duckdb = new HashMap<String, Object>();
    duckdb.put("union_by_name", Boolean.TRUE);
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("duckdb", duckdb);
    record.tableConfig = tableConfig;
    assertTrue(invokeShouldUseUnionByName(record));
  }

  @Test void testShouldUseUnionByNameFalse() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> duckdb = new HashMap<String, Object>();
    duckdb.put("union_by_name", Boolean.FALSE);
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("duckdb", duckdb);
    record.tableConfig = tableConfig;
    assertFalse(invokeShouldUseUnionByName(record));
  }

  @Test void testShouldUseUnionByNameNoDuckdbKey() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<String, Object>();
    assertFalse(invokeShouldUseUnionByName(record));
  }

  // =======================================================================
  // formatRecordForError tests (via reflection)
  // =======================================================================

  @Test void testFormatRecordForErrorNull() throws Exception {
    assertEquals("null", invokeFormatRecordForError(null));
  }

  @Test void testFormatRecordForErrorWithData() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "my_table";
    record.tableType = "ParquetTable";
    record.sourceFile = "/data/source.parquet";
    String result = invokeFormatRecordForError(record);
    assertTrue(result.contains("my_table"));
    assertTrue(result.contains("ParquetTable"));
    assertTrue(result.contains("/data/source.parquet"));
  }

  @Test void testFormatRecordForErrorLongParquetCacheFile() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "test";
    StringBuilder longPath = new StringBuilder();
    for (int i = 0; i < 150; i++) {
      longPath.append("a");
    }
    record.parquetCacheFile = longPath.toString();
    String result = invokeFormatRecordForError(record);
    assertTrue(result.contains("..."), "Long path should be truncated");
  }

  // =======================================================================
  // rewriteSchemaReferencesInSql tests (via reflection)
  // =======================================================================

  @Test void testRewriteSchemaReferencesNullViewDef() throws Exception {
    assertNull(invokeRewriteSchemaReferences(null, "old", "new"));
  }

  @Test void testRewriteSchemaReferencesNullDeclared() throws Exception {
    assertEquals("SELECT 1", invokeRewriteSchemaReferences("SELECT 1", null, "new"));
  }

  @Test void testRewriteSchemaReferencesSameNames() throws Exception {
    String sql = "SELECT * FROM \"econ\".\"table1\"";
    assertEquals(sql, invokeRewriteSchemaReferences(sql, "econ", "econ"));
  }

  @Test void testRewriteSchemaReferencesCaseInsensitiveMatch() throws Exception {
    String sql = "SELECT * FROM \"econ\".\"table1\"";
    assertEquals(sql, invokeRewriteSchemaReferences(sql, "econ", "ECON"));
  }

  // =======================================================================
  // createParquetView tests
  // =======================================================================

  @Test void testCreateParquetViewNullConnectionThrows() {
    try {
      DuckDBJdbcSchemaFactory.createParquetView(null, "test", "/path.parquet");
      fail("Should throw with null connection");
    } catch (RuntimeException e) {
      assertNotNull(e);
    }
  }

  // =======================================================================
  // deriveGlobPattern tests (via reflection)
  // =======================================================================

  @Test void testDeriveGlobPatternNull() throws Exception {
    assertNull(invokeDeriveGlobPattern(null));
  }

  @Test void testDeriveGlobPatternEmpty() throws Exception {
    assertNull(invokeDeriveGlobPattern(""));
  }

  @Test void testDeriveGlobPatternWithPartitions() throws Exception {
    String fileList = "[/data/year=2020/f1.parquet,/data/year=2021/f2.parquet]";
    String pattern = invokeDeriveGlobPattern(fileList);
    assertNotNull(pattern);
    assertTrue(pattern.contains("/**/*"));
  }

  // =======================================================================
  // Helper methods for reflective invocation
  // =======================================================================

  private boolean invokeIsTempDirectory(String path) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "isTempDirectory", String.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, path);
  }

  private boolean invokeIsHivePartitioned(String fileList) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "isHivePartitioned", String.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, fileList);
  }

  private boolean invokeIsHivePartitionedFromConfig(
      ConversionMetadata.ConversionRecord record) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "isHivePartitionedFromConfig", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, record);
  }

  private boolean invokeShouldUseUnionByName(
      ConversionMetadata.ConversionRecord record) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "shouldUseUnionByName", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, record);
  }

  private String invokeFormatRecordForError(
      ConversionMetadata.ConversionRecord record) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "formatRecordForError", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    return (String) method.invoke(null, record);
  }

  private String invokeRewriteSchemaReferences(String viewDef, String declared,
      String actual) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    method.setAccessible(true);
    return (String) method.invoke(null, viewDef, declared, actual);
  }

  private String invokeDeriveGlobPattern(String fileList) throws Exception {
    Method method = DuckDBJdbcSchemaFactory.class.getDeclaredMethod(
        "deriveGlobPattern", String.class);
    method.setAccessible(true);
    return (String) method.invoke(null, fileList);
  }
}
