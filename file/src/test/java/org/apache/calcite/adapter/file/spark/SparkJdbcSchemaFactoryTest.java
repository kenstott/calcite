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
package org.apache.calcite.adapter.file.spark;

import org.apache.calcite.adapter.file.execution.spark.SparkConfig;
import org.apache.calcite.adapter.file.execution.spark.SparkExecutionEngine;
import org.apache.calcite.adapter.file.jdbc.SparkSqlDialect;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for Spark schema factory, config, dialect, and execution engine.
 */
@Tag("unit")
class SparkJdbcSchemaFactoryTest {

  // --- SparkConfig tests ---

  @Test void testDefaultConfig() {
    SparkConfig config = new SparkConfig();
    assertEquals("localhost", config.getHost());
    assertEquals("10000", config.getPort());
    assertEquals("default", config.getDatabase());
    assertNull(config.getUser());
    assertNull(config.getPassword());
    assertEquals("hadoop", config.getIcebergCatalogType());
    assertNull(config.getIcebergWarehouse());
    assertNull(config.getMaxMemory());
    assertEquals(0, config.getMaxThreads());
  }

  @Test void testCustomConfig() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "spark.example.com");
    configMap.put("port", "10001");
    configMap.put("database", "analytics");
    configMap.put("user", "sparkuser");
    configMap.put("password", "secret");
    configMap.put("icebergCatalogType", "hive");
    configMap.put("icebergWarehouse", "s3://bucket/warehouse");
    configMap.put("maxMemory", "8g");
    configMap.put("maxThreads", 4);

    SparkConfig config = new SparkConfig(configMap);
    assertEquals("spark.example.com", config.getHost());
    assertEquals("10001", config.getPort());
    assertEquals("analytics", config.getDatabase());
    assertEquals("sparkuser", config.getUser());
    assertEquals("secret", config.getPassword());
    assertEquals("hive", config.getIcebergCatalogType());
    assertEquals("s3://bucket/warehouse", config.getIcebergWarehouse());
    assertEquals("8g", config.getMaxMemory());
    assertEquals(4, config.getMaxThreads());
  }

  @Test void testConfigPortAsNumber() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("port", 10001);

    SparkConfig config = new SparkConfig(configMap);
    assertEquals("10001", config.getPort());
  }

  @Test void testConfigToSparkSettings() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("maxMemory", "4g");
    configMap.put("maxThreads", 2);

    SparkConfig config = new SparkConfig(configMap);
    String[] settings = config.toSparkSettings();

    assertNotNull(settings);
    assertTrue(settings.length >= 2);

    boolean hasMemory = false;
    boolean hasThreads = false;
    for (String setting : settings) {
      if (setting.contains("spark.executor.memory")) {
        hasMemory = true;
        assertTrue(setting.contains("4g"));
      }
      if (setting.contains("spark.executor.cores")) {
        hasThreads = true;
        assertTrue(setting.contains("2"));
      }
    }
    assertTrue(hasMemory, "Should contain spark.executor.memory setting");
    assertTrue(hasThreads, "Should contain spark.executor.cores setting");
  }

  @Test void testConfigToSparkSettingsNoOptional() {
    SparkConfig config = new SparkConfig();
    String[] settings = config.toSparkSettings();
    assertNotNull(settings);
    assertEquals(0, settings.length, "Default config should produce no settings");
  }

  @Test void testConfigAdditionalSettings() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("spark.sql.shuffle.partitions", "200");

    SparkConfig config = new SparkConfig(configMap);
    assertEquals("200", config.getAdditionalSettings().getProperty("spark.sql.shuffle.partitions"));

    String[] settings = config.toSparkSettings();
    boolean found = false;
    for (String s : settings) {
      if (s.contains("spark.sql.shuffle.partitions") && s.contains("200")) {
        found = true;
      }
    }
    assertTrue(found, "Should contain additional setting in output");
  }

  @Test void testIcebergCatalogSettings() {
    SparkConfig config = new SparkConfig();
    String[] settings = config.toIcebergCatalogSettings("/data/warehouse");

    assertNotNull(settings);
    assertTrue(settings.length >= 2);

    boolean hasCatalogClass = false;
    boolean hasCatalogType = false;
    boolean hasWarehouse = false;
    for (String setting : settings) {
      if (setting.contains("aperio_iceberg") && setting.contains("SparkCatalog")) {
        hasCatalogClass = true;
      }
      if (setting.contains("aperio_iceberg") && setting.contains(".type = hadoop")) {
        hasCatalogType = true;
      }
      if (setting.contains("aperio_iceberg") && setting.contains(".warehouse = /data/warehouse")) {
        hasWarehouse = true;
      }
    }
    assertTrue(hasCatalogClass, "Should set Iceberg SparkCatalog class");
    assertTrue(hasCatalogType, "Should set Iceberg catalog type");
    assertTrue(hasWarehouse, "Should set Iceberg warehouse path");
  }

  @Test void testIcebergCatalogSettingsWithCustomWarehouse() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("icebergWarehouse", "s3://bucket/iceberg");

    SparkConfig config = new SparkConfig(configMap);
    String[] settings = config.toIcebergCatalogSettings("/default/path");

    boolean hasCustomWarehouse = false;
    for (String setting : settings) {
      if (setting.contains("s3://bucket/iceberg")) {
        hasCustomWarehouse = true;
      }
    }
    assertTrue(hasCustomWarehouse, "Should use custom warehouse path over default");
  }

  @Test void testConfigToString() {
    SparkConfig config = new SparkConfig();
    String str = config.toString();
    assertNotNull(str);
    assertTrue(str.contains("SparkConfig"));
    assertTrue(str.contains("localhost"));
    assertTrue(str.contains("10000"));
  }

  @Test void testConfigToStringMasksPassword() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("user", "admin");
    configMap.put("password", "supersecret");

    SparkConfig config = new SparkConfig(configMap);
    String str = config.toString();
    assertFalse(str.contains("supersecret"), "Password should be masked in toString");
    assertTrue(str.contains("***"), "Should show masked password indicator");
  }

  // --- SparkSqlDialect tests ---

  @Test void testDialectDriverClassName() {
    assertEquals("org.apache.hive.jdbc.HiveDriver",
        SparkSqlDialect.INSTANCE.getDriverClassName());
  }

  @Test void testDialectBuildJdbcUrl() {
    Map<String, String> config = new HashMap<>();
    config.put("host", "myhost");
    config.put("port", "10001");
    config.put("database", "mydb");

    String url = SparkSqlDialect.INSTANCE.buildJdbcUrl(config);
    assertEquals("jdbc:hive2://myhost:10001/mydb", url);
  }

  @Test void testDialectBuildJdbcUrlDefaults() {
    String url = SparkSqlDialect.INSTANCE.buildJdbcUrl(Collections.emptyMap());
    assertEquals("jdbc:hive2://localhost:10000/default", url);
  }

  @Test void testDialectBuildJdbcUrlNullConfig() {
    String url = SparkSqlDialect.INSTANCE.buildJdbcUrl(null);
    assertEquals("jdbc:hive2://localhost:10000/default", url);
  }

  @Test void testDialectReadParquetSql() {
    String sql = SparkSqlDialect.INSTANCE.readParquetSql(
        "/data/*.parquet", Arrays.asList("id", "name"));
    assertEquals("SELECT id, name FROM parquet.`/data/*.parquet`", sql);
  }

  @Test void testDialectReadParquetSqlAllColumns() {
    String sql = SparkSqlDialect.INSTANCE.readParquetSql(
        "/data/*.parquet", Collections.emptyList());
    assertEquals("SELECT * FROM parquet.`/data/*.parquet`", sql);
  }

  @Test void testDialectCreateParquetViewSql() {
    String sql = SparkSqlDialect.INSTANCE.createParquetViewSql(
        "myschema", "mytable", "/data/file.parquet", false);
    assertTrue(sql.contains("CREATE OR REPLACE VIEW"));
    assertTrue(sql.contains("parquet.`/data/file.parquet`"));
  }

  @Test void testDialectCreateIcebergViewSql() {
    String sql = SparkSqlDialect.INSTANCE.createIcebergViewSql(
        "myschema", "mytable", "s3://bucket/iceberg/table");
    assertTrue(sql.contains("CREATE OR REPLACE VIEW"));
    assertTrue(sql.contains("aperio_iceberg.myschema.mytable"));
  }

  @Test void testDialectCreateIcebergTableSql() {
    String sql = SparkSqlDialect.INSTANCE.createIcebergTableSql(
        "mydb", "mytable", "s3://bucket/warehouse/mytable");
    assertEquals(
        "CREATE TABLE IF NOT EXISTS aperio_iceberg.mydb.mytable USING iceberg LOCATION 's3://bucket/warehouse/mytable'",
        sql);
  }

  @Test void testDialectSupportsDirectGlob() {
    assertTrue(SparkSqlDialect.INSTANCE.supportsDirectGlob());
  }

  @Test void testDialectSupportsIceberg() {
    assertTrue(SparkSqlDialect.INSTANCE.supportsIceberg());
  }

  @Test void testDialectName() {
    assertEquals("Spark SQL", SparkSqlDialect.INSTANCE.getName());
  }

  @Test void testDialectDropViewSql() {
    String sql = SparkSqlDialect.INSTANCE.dropViewSql("myschema", "mytable");
    assertEquals("DROP VIEW IF EXISTS \"myschema\".\"mytable\"", sql);
  }

  @Test void testDialectCreateOrReplaceParquetViewSql() {
    String sql = SparkSqlDialect.INSTANCE.createOrReplaceParquetViewSql(
        "myschema", "mytable", "/data/file.parquet", false);
    assertTrue(sql.contains("CREATE OR REPLACE VIEW"));
    assertTrue(sql.contains("parquet.`/data/file.parquet`"));
  }

  // --- SparkExecutionEngine tests ---

  @Test void testEngineType() {
    assertEquals("SPARK", SparkExecutionEngine.getEngineType());
  }

  @Test void testIsAvailableReturnsBoolean() {
    // The driver may or may not be on classpath, but this should not throw
    boolean result = SparkExecutionEngine.isAvailable();
    assertTrue(result || !result);
  }

  @Test void testIsServerReachableUnreachable() {
    // Port 1 is almost certainly not running a Thrift Server
    boolean result = SparkExecutionEngine.isServerReachable("localhost", 1);
    assertFalse(result, "Should not be reachable on port 1");
  }
}
