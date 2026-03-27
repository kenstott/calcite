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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link SparkSqlDialect}.
 * Exercises private methods via reflection and edge cases.
 */
@Tag("unit")
public class SparkSqlDialectCoverageTest {

  private final SparkSqlDialect dialect = SparkSqlDialect.INSTANCE;

  // =========================================================================
  // getConfigValue - private method via reflection
  // =========================================================================

  @Test public void testGetConfigValueNullConfig() throws Exception {
    Method method = SparkSqlDialect.class.getDeclaredMethod(
        "getConfigValue", Map.class, String.class, String.class);
    method.setAccessible(true);
    assertEquals("default", method.invoke(null, null, "key", "default"));
  }

  @Test public void testGetConfigValueMissingKey() throws Exception {
    Method method = SparkSqlDialect.class.getDeclaredMethod(
        "getConfigValue", Map.class, String.class, String.class);
    method.setAccessible(true);
    Map<String, String> config = new HashMap<String, String>();
    assertEquals("default", method.invoke(null, config, "missing", "default"));
  }

  @Test public void testGetConfigValueEmptyValue() throws Exception {
    Method method = SparkSqlDialect.class.getDeclaredMethod(
        "getConfigValue", Map.class, String.class, String.class);
    method.setAccessible(true);
    Map<String, String> config = new HashMap<String, String>();
    config.put("key", "");
    assertEquals("default", method.invoke(null, config, "key", "default"));
  }

  @Test public void testGetConfigValuePresent() throws Exception {
    Method method = SparkSqlDialect.class.getDeclaredMethod(
        "getConfigValue", Map.class, String.class, String.class);
    method.setAccessible(true);
    Map<String, String> config = new HashMap<String, String>();
    config.put("key", "value");
    assertEquals("value", method.invoke(null, config, "key", "default"));
  }

  // =========================================================================
  // formatColumns - private method via reflection
  // =========================================================================

  @Test public void testFormatColumnsNull() throws Exception {
    Method method = SparkSqlDialect.class.getDeclaredMethod("formatColumns", List.class);
    method.setAccessible(true);
    assertEquals("*", method.invoke(null, (List<String>) null));
  }

  @Test public void testFormatColumnsEmpty() throws Exception {
    Method method = SparkSqlDialect.class.getDeclaredMethod("formatColumns", List.class);
    method.setAccessible(true);
    assertEquals("*", method.invoke(null, Collections.emptyList()));
  }

  @Test public void testFormatColumnsSingle() throws Exception {
    Method method = SparkSqlDialect.class.getDeclaredMethod("formatColumns", List.class);
    method.setAccessible(true);
    assertEquals("col1", method.invoke(null, Collections.singletonList("col1")));
  }

  @Test public void testFormatColumnsMultiple() throws Exception {
    Method method = SparkSqlDialect.class.getDeclaredMethod("formatColumns", List.class);
    method.setAccessible(true);
    assertEquals("a, b, c", method.invoke(null, Arrays.asList("a", "b", "c")));
  }

  // =========================================================================
  // Public method edge cases
  // =========================================================================

  @Test public void testBuildJdbcUrlNullConfig() {
    String url = dialect.buildJdbcUrl(null);
    assertEquals("jdbc:hive2://localhost:10000/default", url);
  }

  @Test public void testBuildJdbcUrlPartialConfig() {
    Map<String, String> config = new HashMap<String, String>();
    config.put("host", "spark.internal");
    String url = dialect.buildJdbcUrl(config);
    assertEquals("jdbc:hive2://spark.internal:10000/default", url);
  }

  @Test public void testBuildJdbcUrlDatabaseOnly() {
    Map<String, String> config = new HashMap<String, String>();
    config.put("database", "mydb");
    String url = dialect.buildJdbcUrl(config);
    assertEquals("jdbc:hive2://localhost:10000/mydb", url);
  }

  @Test public void testReadParquetSqlNullColumns() {
    String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", null);
    assertEquals("SELECT * FROM parquet.`s3://bucket/data/*.parquet`", sql);
  }

  @Test public void testCreateParquetViewSqlWithHivePartitioning() {
    // hivePartitioning parameter is auto-detected by Spark, so output is same
    String sql = dialect.createParquetViewSql("schema", "view", "s3://bucket/data/*.parquet", true);
    assertTrue(sql.contains("CREATE OR REPLACE VIEW"));
    assertTrue(sql.contains("parquet.`"));
  }

  @Test public void testCreateParquetViewSqlWithoutSchema() {
    String sql = dialect.createParquetViewSql(null, "view", "s3://data/*.parquet", false);
    assertTrue(sql.contains("CREATE OR REPLACE VIEW"));
    assertTrue(sql.contains("\"view\""));
  }

  @Test public void testCreateIcebergViewSql() {
    String sql = dialect.createIcebergViewSql("schema", "view", "s3://bucket/table");
    assertTrue(sql.contains("CREATE OR REPLACE VIEW"));
    assertTrue(sql.contains("iceberg.`"));
  }

  @Test public void testCreateIcebergViewSqlNullSchema() {
    String sql = dialect.createIcebergViewSql(null, "view", "s3://bucket/table");
    assertNotNull(sql);
    assertTrue(sql.contains("\"view\""));
  }

  @Test public void testCreateOrReplaceParquetViewSqlDelegatesToCreate() {
    String createSql = dialect.createParquetViewSql("s", "v", "s3://b/d", false);
    String replaceSql = dialect.createOrReplaceParquetViewSql("s", "v", "s3://b/d", false);
    assertEquals(createSql, replaceSql);
  }

  @Test public void testSingletonInstance() {
    assertNotNull(SparkSqlDialect.INSTANCE);
  }

  @Test public void testSupportsDirectGlob() {
    assertTrue(dialect.supportsDirectGlob());
  }

  @Test public void testSupportsIceberg() {
    assertTrue(dialect.supportsIceberg());
  }

  @Test public void testGetName() {
    assertEquals("Spark SQL", dialect.getName());
  }

  @Test public void testGetDriverClassName() {
    assertEquals("org.apache.hive.jdbc.HiveDriver", dialect.getDriverClassName());
  }

  @Test public void testDropViewSqlDefault() {
    // SparkSqlDialect uses the JdbcDialect default dropViewSql
    String sql = dialect.dropViewSql("schema", "view");
    assertEquals("DROP VIEW IF EXISTS \"schema\".\"view\"", sql);
  }

  @Test public void testDropViewSqlNullSchema() {
    String sql = dialect.dropViewSql(null, "view");
    assertEquals("DROP VIEW IF EXISTS \"view\"", sql);
  }
}
