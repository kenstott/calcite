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
package org.apache.calcite.adapter.file.jdbc;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link TrinoDialect}.
 * Exercises private methods via reflection and edge cases not covered
 * by {@link JdbcDialectTest}.
 */
@Tag("unit")
public class TrinoDialectCoverageTest {

  private final TrinoDialect dialect = TrinoDialect.INSTANCE;

  // =========================================================================
  // pathToTableName - private method via reflection
  // =========================================================================

  @Test public void testPathToTableNameNull() throws Exception {
    Method method = TrinoDialect.class.getDeclaredMethod("pathToTableName", String.class);
    method.setAccessible(true);
    assertEquals("unknown_table", method.invoke(null, (String) null));
  }

  @Test public void testPathToTableNameEmpty() throws Exception {
    Method method = TrinoDialect.class.getDeclaredMethod("pathToTableName", String.class);
    method.setAccessible(true);
    assertEquals("unknown_table", method.invoke(null, ""));
  }

  @Test public void testPathToTableNameSimpleFile() throws Exception {
    Method method = TrinoDialect.class.getDeclaredMethod("pathToTableName", String.class);
    method.setAccessible(true);
    assertEquals("events", method.invoke(null, "/data/events.parquet"));
  }

  @Test public void testPathToTableNameGlobPattern() throws Exception {
    Method method = TrinoDialect.class.getDeclaredMethod("pathToTableName", String.class);
    method.setAccessible(true);
    assertEquals("data", method.invoke(null, "s3://bucket/data/*.parquet"));
  }

  @Test public void testPathToTableNameNoSlashes() throws Exception {
    Method method = TrinoDialect.class.getDeclaredMethod("pathToTableName", String.class);
    method.setAccessible(true);
    assertEquals("myfile", method.invoke(null, "myfile.parquet"));
  }

  @Test public void testPathToTableNameTrailingSlash() throws Exception {
    Method method = TrinoDialect.class.getDeclaredMethod("pathToTableName", String.class);
    method.setAccessible(true);
    assertEquals("data", method.invoke(null, "s3://bucket/data/"));
  }

  @Test public void testPathToTableNameBackslash() throws Exception {
    Method method = TrinoDialect.class.getDeclaredMethod("pathToTableName", String.class);
    method.setAccessible(true);
    assertEquals("data", method.invoke(null, "C:\\Users\\test\\data\\"));
  }

  @Test public void testPathToTableNameSpecialChars() throws Exception {
    Method method = TrinoDialect.class.getDeclaredMethod("pathToTableName", String.class);
    method.setAccessible(true);
    assertEquals("my_table_2024", method.invoke(null, "/data/my-table-2024.parquet"));
  }

  @Test public void testPathToTableNameNoExtension() throws Exception {
    Method method = TrinoDialect.class.getDeclaredMethod("pathToTableName", String.class);
    method.setAccessible(true);
    assertEquals("events", method.invoke(null, "/data/events"));
  }

  // =========================================================================
  // getConfigValue - private method via reflection
  // =========================================================================

  @Test public void testGetConfigValueNullConfig() throws Exception {
    Method method =
        TrinoDialect.class.getDeclaredMethod("getConfigValue", Map.class, String.class, String.class);
    method.setAccessible(true);
    assertEquals("default", method.invoke(null, null, "key", "default"));
  }

  @Test public void testGetConfigValueMissingKey() throws Exception {
    Method method =
        TrinoDialect.class.getDeclaredMethod("getConfigValue", Map.class, String.class, String.class);
    method.setAccessible(true);
    Map<String, String> config = new HashMap<String, String>();
    assertEquals("default", method.invoke(null, config, "missing", "default"));
  }

  @Test public void testGetConfigValueEmptyValue() throws Exception {
    Method method =
        TrinoDialect.class.getDeclaredMethod("getConfigValue", Map.class, String.class, String.class);
    method.setAccessible(true);
    Map<String, String> config = new HashMap<String, String>();
    config.put("key", "");
    assertEquals("default", method.invoke(null, config, "key", "default"));
  }

  @Test public void testGetConfigValuePresentValue() throws Exception {
    Method method =
        TrinoDialect.class.getDeclaredMethod("getConfigValue", Map.class, String.class, String.class);
    method.setAccessible(true);
    Map<String, String> config = new HashMap<String, String>();
    config.put("key", "myvalue");
    assertEquals("myvalue", method.invoke(null, config, "key", "default"));
  }

  // =========================================================================
  // formatColumns - private method via reflection
  // =========================================================================

  @Test public void testFormatColumnsNull() throws Exception {
    Method method = TrinoDialect.class.getDeclaredMethod("formatColumns", List.class);
    method.setAccessible(true);
    assertEquals("*", method.invoke(null, (List<String>) null));
  }

  @Test public void testFormatColumnsEmpty() throws Exception {
    Method method = TrinoDialect.class.getDeclaredMethod("formatColumns", List.class);
    method.setAccessible(true);
    assertEquals("*", method.invoke(null, Collections.emptyList()));
  }

  @Test public void testFormatColumnsSingle() throws Exception {
    Method method = TrinoDialect.class.getDeclaredMethod("formatColumns", List.class);
    method.setAccessible(true);
    assertEquals("id", method.invoke(null, Collections.singletonList("id")));
  }

  @Test public void testFormatColumnsMultiple() throws Exception {
    Method method = TrinoDialect.class.getDeclaredMethod("formatColumns", List.class);
    method.setAccessible(true);
    assertEquals("id, name, age", method.invoke(null, Arrays.asList("id", "name", "age")));
  }

  // =========================================================================
  // Public method edge cases
  // =========================================================================

  @Test public void testBuildJdbcUrlNullConfig() {
    String url = dialect.buildJdbcUrl(null);
    assertEquals("jdbc:trino://localhost:8080/hive/default", url);
  }

  @Test public void testBuildJdbcUrlPartialConfig() {
    Map<String, String> config = new HashMap<String, String>();
    config.put("host", "trino.internal");
    String url = dialect.buildJdbcUrl(config);
    assertEquals("jdbc:trino://trino.internal:8080/hive/default", url);
  }

  @Test public void testReadParquetSqlNullColumns() {
    String sql = dialect.readParquetSql("s3://bucket/path/*.parquet", null);
    assertTrue(sql.contains("SELECT *"));
  }

  @Test public void testCreateParquetViewSqlWithHivePartitioning() {
    String sql = dialect.createParquetViewSql("schema", "view", "s3://bucket/data/", true);
    assertTrue(sql.contains("partitioned_by"));
    assertTrue(sql.contains("format = 'PARQUET'"));
  }

  @Test public void testCreateParquetViewSqlWithoutHivePartitioning() {
    String sql = dialect.createParquetViewSql("schema", "view", "s3://bucket/data/", false);
    assertFalse(sql.contains("partitioned_by"));
  }

  @Test public void testCreateParquetViewSqlNullSchema() {
    String sql = dialect.createParquetViewSql(null, "view", "s3://bucket/data/", false);
    assertNotNull(sql);
    assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS"));
  }

  @Test public void testCreateIcebergViewSqlNullSchema() {
    String sql = dialect.createIcebergViewSql(null, "mytable", "s3://bucket/warehouse/tbl");
    assertNotNull(sql);
    assertTrue(sql.contains("register_table"));
    assertTrue(sql.contains("default"));
  }

  @Test public void testCreateIcebergViewSqlWithSchema() {
    String sql = dialect.createIcebergViewSql("myschema", "mytable", "s3://bucket/warehouse/tbl");
    assertTrue(sql.contains("myschema"));
    assertTrue(sql.contains("mytable"));
  }

  @Test public void testDropViewSqlWithSchema() {
    String sql = dialect.dropViewSql("myschema", "mytable");
    assertEquals("DROP TABLE IF EXISTS \"myschema\".\"mytable\"", sql);
  }

  @Test public void testDropViewSqlWithoutSchema() {
    String sql = dialect.dropViewSql(null, "mytable");
    assertEquals("DROP TABLE IF EXISTS \"mytable\"", sql);
  }

  @Test public void testGetName() {
    assertEquals("Trino", dialect.getName());
  }

  @Test public void testGetDriverClassName() {
    assertEquals("io.trino.jdbc.TrinoDriver", dialect.getDriverClassName());
  }

  @Test public void testSupportsDirectGlob() {
    assertFalse(dialect.supportsDirectGlob());
  }

  @Test public void testSupportsIceberg() {
    assertTrue(dialect.supportsIceberg());
  }

  @Test public void testRegisterTableSql() {
    String sql = dialect.registerTableSql("my_table", "s3://bucket/data/", "parquet");
    assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS"));
    assertTrue(sql.contains("PARQUET"));
  }

  @Test public void testSingletonInstance() {
    assertNotNull(TrinoDialect.INSTANCE);
  }

  @Test public void testReadIcebergSqlReturnsNull() {
    assertNull(dialect.readIcebergSql("s3://bucket/table", Collections.emptyList()));
  }

  @Test public void testCreateSchemaSql() {
    String sql = dialect.createSchemaSql("myschema");
    assertEquals("CREATE SCHEMA IF NOT EXISTS \"myschema\"", sql);
  }

  @Test public void testQualifyNameWithSchema() {
    assertEquals("\"schema\".\"table\"", dialect.qualifyName("schema", "table"));
  }

  @Test public void testQualifyNameEmptySchema() {
    assertEquals("\"table\"", dialect.qualifyName("", "table"));
  }
}
