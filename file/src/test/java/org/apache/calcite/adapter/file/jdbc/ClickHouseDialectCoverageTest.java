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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link ClickHouseDialect}.
 * Exercises private methods via reflection and edge cases.
 */
@Tag("unit")
public class ClickHouseDialectCoverageTest {

  private final ClickHouseDialect dialect = ClickHouseDialect.INSTANCE;

  // =========================================================================
  // isS3Path - private method via reflection
  // =========================================================================

  @Test public void testIsS3PathWithS3Prefix() throws Exception {
    Method method = ClickHouseDialect.class.getDeclaredMethod("isS3Path", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "s3://bucket/data"));
  }

  @Test public void testIsS3PathWithS3aPrefix() throws Exception {
    Method method = ClickHouseDialect.class.getDeclaredMethod("isS3Path", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(null, "s3a://bucket/data"));
  }

  @Test public void testIsS3PathWithLocalPath() throws Exception {
    Method method = ClickHouseDialect.class.getDeclaredMethod("isS3Path", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, "/local/data"));
  }

  @Test public void testIsS3PathWithNull() throws Exception {
    Method method = ClickHouseDialect.class.getDeclaredMethod("isS3Path", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, (String) null));
  }

  @Test public void testIsS3PathWithHttpPath() throws Exception {
    Method method = ClickHouseDialect.class.getDeclaredMethod("isS3Path", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(null, "http://example.com/data"));
  }

  // =========================================================================
  // getConfigValue - private method via reflection
  // =========================================================================

  @Test public void testGetConfigValueNullConfig() throws Exception {
    Method method =
        ClickHouseDialect.class.getDeclaredMethod("getConfigValue", Map.class, String.class, String.class);
    method.setAccessible(true);
    assertEquals("default", method.invoke(null, null, "key", "default"));
  }

  @Test public void testGetConfigValueMissingKey() throws Exception {
    Method method =
        ClickHouseDialect.class.getDeclaredMethod("getConfigValue", Map.class, String.class, String.class);
    method.setAccessible(true);
    Map<String, String> config = new HashMap<String, String>();
    assertEquals("default", method.invoke(null, config, "missing", "default"));
  }

  @Test public void testGetConfigValueEmptyValue() throws Exception {
    Method method =
        ClickHouseDialect.class.getDeclaredMethod("getConfigValue", Map.class, String.class, String.class);
    method.setAccessible(true);
    Map<String, String> config = new HashMap<String, String>();
    config.put("key", "");
    assertEquals("default", method.invoke(null, config, "key", "default"));
  }

  @Test public void testGetConfigValuePresent() throws Exception {
    Method method =
        ClickHouseDialect.class.getDeclaredMethod("getConfigValue", Map.class, String.class, String.class);
    method.setAccessible(true);
    Map<String, String> config = new HashMap<String, String>();
    config.put("key", "value");
    assertEquals("value", method.invoke(null, config, "key", "default"));
  }

  // =========================================================================
  // formatColumns - private method via reflection
  // =========================================================================

  @Test public void testFormatColumnsNull() throws Exception {
    Method method = ClickHouseDialect.class.getDeclaredMethod("formatColumns", List.class);
    method.setAccessible(true);
    assertEquals("*", method.invoke(null, (List<String>) null));
  }

  @Test public void testFormatColumnsEmpty() throws Exception {
    Method method = ClickHouseDialect.class.getDeclaredMethod("formatColumns", List.class);
    method.setAccessible(true);
    assertEquals("*", method.invoke(null, Collections.emptyList()));
  }

  @Test public void testFormatColumnsSingle() throws Exception {
    Method method = ClickHouseDialect.class.getDeclaredMethod("formatColumns", List.class);
    method.setAccessible(true);
    assertEquals("id", method.invoke(null, Collections.singletonList("id")));
  }

  @Test public void testFormatColumnsMultiple() throws Exception {
    Method method = ClickHouseDialect.class.getDeclaredMethod("formatColumns", List.class);
    method.setAccessible(true);
    assertEquals("a, b, c", method.invoke(null, Arrays.asList("a", "b", "c")));
  }

  // =========================================================================
  // Public method edge cases
  // =========================================================================

  @Test public void testBuildJdbcUrlNullConfig() {
    String url = dialect.buildJdbcUrl(null);
    assertEquals("jdbc:clickhouse://localhost:8123/default", url);
  }

  @Test public void testBuildJdbcUrlPartialConfig() {
    Map<String, String> config = new HashMap<String, String>();
    config.put("host", "ch.internal");
    String url = dialect.buildJdbcUrl(config);
    assertEquals("jdbc:clickhouse://ch.internal:8123/default", url);
  }

  @Test public void testReadParquetSqlNullColumns() {
    String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", null);
    assertEquals("SELECT * FROM s3('s3://bucket/data/*.parquet', 'Parquet')", sql);
  }

  @Test public void testReadIcebergSqlNullColumns() {
    String sql = dialect.readIcebergSql("s3://bucket/table", null);
    assertEquals("SELECT * FROM iceberg('s3://bucket/table')", sql);
  }

  @Test public void testReadIcebergSqlWithColumns() {
    List<String> cols = Arrays.asList("id", "val");
    String sql = dialect.readIcebergSql("s3://bucket/table", cols);
    assertEquals("SELECT id, val FROM iceberg('s3://bucket/table')", sql);
  }

  @Test public void testCreateParquetViewSqlLocalPath() {
    String sql = dialect.createParquetViewSql("schema", "view", "/local/data/*.parquet", false);
    assertTrue(sql.contains("file("));
    assertFalse(sql.contains("s3("));
  }

  @Test public void testCreateParquetViewSqlS3Path() {
    String sql = dialect.createParquetViewSql("schema", "view", "s3://bucket/data/*.parquet", false);
    assertTrue(sql.contains("s3("));
    assertFalse(sql.contains("file("));
  }

  @Test public void testCreateParquetViewSqlS3aPath() {
    String sql = dialect.createParquetViewSql("schema", "view", "s3a://bucket/data/*.parquet", false);
    assertTrue(sql.contains("s3("));
  }

  @Test public void testCreateOrReplaceParquetViewSqlDelegatesToCreate() {
    String createSql = dialect.createParquetViewSql("s", "v", "s3://b/d", false);
    String replaceSql = dialect.createOrReplaceParquetViewSql("s", "v", "s3://b/d", false);
    assertEquals(createSql, replaceSql);
  }

  @Test public void testCreateIcebergViewSql() {
    String sql = dialect.createIcebergViewSql("schema", "view", "s3://bucket/table");
    assertTrue(sql.contains("CREATE OR REPLACE VIEW"));
    assertTrue(sql.contains("iceberg("));
  }

  @Test public void testDropViewSql() {
    String sql = dialect.dropViewSql("schema", "view");
    assertEquals("DROP VIEW IF EXISTS \"schema\".\"view\"", sql);
  }

  @Test public void testDropViewSqlNullSchema() {
    String sql = dialect.dropViewSql(null, "view");
    assertEquals("DROP VIEW IF EXISTS \"view\"", sql);
  }

  @Test public void testSingletonInstance() {
    assertNotNull(ClickHouseDialect.INSTANCE);
  }

  @Test public void testSupportsDirectGlob() {
    assertTrue(dialect.supportsDirectGlob());
  }

  @Test public void testSupportsIceberg() {
    assertTrue(dialect.supportsIceberg());
  }

  @Test public void testGetName() {
    assertEquals("ClickHouse", dialect.getName());
  }

  @Test public void testGetDriverClassName() {
    assertEquals("com.clickhouse.jdbc.ClickHouseDriver", dialect.getDriverClassName());
  }
}
