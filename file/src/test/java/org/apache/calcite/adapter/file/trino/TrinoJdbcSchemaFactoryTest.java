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
package org.apache.calcite.adapter.file.trino;

import org.apache.calcite.adapter.file.execution.trino.TrinoConfig;
import org.apache.calcite.adapter.file.execution.trino.TrinoExecutionEngine;
import org.apache.calcite.adapter.file.jdbc.TrinoDialect;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for Trino schema factory, config, dialect, and execution engine.
 */
@Tag("unit")
class TrinoJdbcSchemaFactoryTest {

  // --- TrinoConfig tests ---

  @Test void testDefaultConfig() {
    TrinoConfig config = new TrinoConfig();
    assertEquals("localhost", config.getHost());
    assertEquals("8080", config.getPort());
    assertEquals("hive", config.getCatalog());
    assertEquals("default", config.getSchema());
    assertNull(config.getUser());
    assertNull(config.getPassword());
    assertEquals("iceberg", config.getIcebergCatalog());
    assertEquals("/data/warehouse", config.getWarehouseDir());
    assertNull(config.getS3AccessKey());
    assertNull(config.getS3SecretKey());
    assertNull(config.getS3Endpoint());
  }

  @Test void testCustomConfig() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "trino.example.com");
    configMap.put("port", "8443");
    configMap.put("catalog", "datalake");
    configMap.put("schema", "analytics");
    configMap.put("user", "trinouser");
    configMap.put("password", "secret");
    configMap.put("icebergCatalog", "iceberg_prod");
    configMap.put("warehouseDir", "s3://bucket/warehouse");

    TrinoConfig config = new TrinoConfig(configMap);
    assertEquals("trino.example.com", config.getHost());
    assertEquals("8443", config.getPort());
    assertEquals("datalake", config.getCatalog());
    assertEquals("analytics", config.getSchema());
    assertEquals("trinouser", config.getUser());
    assertEquals("secret", config.getPassword());
    assertEquals("iceberg_prod", config.getIcebergCatalog());
    assertEquals("s3://bucket/warehouse", config.getWarehouseDir());
  }

  @Test void testConfigPortAsNumber() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("port", 8443);

    TrinoConfig config = new TrinoConfig(configMap);
    assertEquals("8443", config.getPort());
  }

  @Test void testConfigToSessionSettings() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("query_max_memory", "1GB");
    configMap.put("join_distribution_type", "AUTOMATIC");

    TrinoConfig config = new TrinoConfig(configMap);
    String[] settings = config.toSessionSettings();

    assertNotNull(settings);
    assertEquals(2, settings.length);

    boolean hasMemory = false;
    boolean hasJoin = false;
    for (String setting : settings) {
      assertTrue(setting.startsWith("SET SESSION "));
      if (setting.contains("query_max_memory")) {
        hasMemory = true;
        assertTrue(setting.contains("1GB"));
      }
      if (setting.contains("join_distribution_type")) {
        hasJoin = true;
        assertTrue(setting.contains("AUTOMATIC"));
      }
    }
    assertTrue(hasMemory, "Should contain query_max_memory setting");
    assertTrue(hasJoin, "Should contain join_distribution_type setting");
  }

  @Test void testConfigToSessionSettingsNoAdditional() {
    TrinoConfig config = new TrinoConfig();
    String[] settings = config.toSessionSettings();
    assertNotNull(settings);
    assertEquals(0, settings.length, "Default config should produce no session settings");
  }

  @Test void testConfigAdditionalSettings() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("query_max_run_time", "30m");

    TrinoConfig config = new TrinoConfig(configMap);
    assertEquals("30m", config.getAdditionalSettings().getProperty("query_max_run_time"));
  }

  @Test void testConfigToString() {
    TrinoConfig config = new TrinoConfig();
    String str = config.toString();
    assertNotNull(str);
    assertTrue(str.contains("TrinoConfig"));
    assertTrue(str.contains("localhost"));
    assertTrue(str.contains("8080"));
    assertTrue(str.contains("hive"));
  }

  @Test void testConfigToStringMasksPassword() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("user", "admin");
    configMap.put("password", "supersecret");

    TrinoConfig config = new TrinoConfig(configMap);
    String str = config.toString();
    assertFalse(str.contains("supersecret"), "Password should be masked in toString");
    assertTrue(str.contains("***"), "Should show masked password indicator");
  }

  // --- Catalog file generation tests ---

  @Test void testHiveCatalogProperties() {
    TrinoConfig config = new TrinoConfig();
    Properties props = config.getHiveCatalogProperties();
    assertEquals("hive", props.getProperty("connector.name"));
    assertEquals("file", props.getProperty("hive.metastore"));
    assertEquals("/data/warehouse", props.getProperty("hive.metastore.catalog.dir"));
    assertNull(props.getProperty("hive.s3.aws-access-key"));
  }

  @Test void testHiveCatalogPropertiesWithS3() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("s3AccessKey", "AKIAIOSFODNN7EXAMPLE");
    configMap.put("s3SecretKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    configMap.put("s3Endpoint", "http://minio:9000");

    TrinoConfig config = new TrinoConfig(configMap);
    Properties props = config.getHiveCatalogProperties();
    assertEquals("AKIAIOSFODNN7EXAMPLE", props.getProperty("hive.s3.aws-access-key"));
    assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", props.getProperty("hive.s3.aws-secret-key"));
    assertEquals("http://minio:9000", props.getProperty("hive.s3.endpoint"));
    assertEquals("true", props.getProperty("hive.s3.path-style-access"));
  }

  @Test void testIcebergCatalogProperties() {
    TrinoConfig config = new TrinoConfig();
    Properties props = config.getIcebergCatalogProperties();
    assertEquals("iceberg", props.getProperty("connector.name"));
    assertEquals("hive", props.getProperty("iceberg.catalog.type"));
    assertEquals("file", props.getProperty("hive.metastore"));
    assertEquals("/data/warehouse", props.getProperty("hive.metastore.catalog.dir"));
  }

  @Test void testGenerateCatalogFiles(@TempDir File tempDir) throws Exception {
    TrinoConfig config = new TrinoConfig();
    String outputDir = tempDir.getAbsolutePath() + "/catalogs";
    config.generateCatalogFiles(outputDir);

    // Verify hive.properties was created
    File hiveFile = new File(outputDir, "hive.properties");
    assertTrue(hiveFile.exists(), "hive.properties should exist");
    Properties hiveProps = new Properties();
    try (FileInputStream in = new FileInputStream(hiveFile)) {
      hiveProps.load(in);
    }
    assertEquals("hive", hiveProps.getProperty("connector.name"));

    // Verify iceberg.properties was created
    File icebergFile = new File(outputDir, "iceberg.properties");
    assertTrue(icebergFile.exists(), "iceberg.properties should exist");
    Properties icebergProps = new Properties();
    try (FileInputStream in = new FileInputStream(icebergFile)) {
      icebergProps.load(in);
    }
    assertEquals("iceberg", icebergProps.getProperty("connector.name"));
  }

  @Test void testGenerateCatalogFilesCustomCatalogNames(@TempDir File tempDir) throws Exception {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("catalog", "my_hive");
    configMap.put("icebergCatalog", "my_iceberg");

    TrinoConfig config = new TrinoConfig(configMap);
    String outputDir = tempDir.getAbsolutePath() + "/catalogs";
    config.generateCatalogFiles(outputDir);

    assertTrue(new File(outputDir, "my_hive.properties").exists());
    assertTrue(new File(outputDir, "my_iceberg.properties").exists());
  }

  // --- TrinoDialect tests ---

  @Test void testDialectDriverClassName() {
    assertEquals("io.trino.jdbc.TrinoDriver",
        TrinoDialect.INSTANCE.getDriverClassName());
  }

  @Test void testDialectBuildJdbcUrl() {
    Map<String, String> config = new HashMap<>();
    config.put("host", "myhost");
    config.put("port", "8443");
    config.put("catalog", "datalake");
    config.put("schema", "analytics");

    String url = TrinoDialect.INSTANCE.buildJdbcUrl(config);
    assertEquals("jdbc:trino://myhost:8443/datalake/analytics", url);
  }

  @Test void testDialectBuildJdbcUrlDefaults() {
    String url = TrinoDialect.INSTANCE.buildJdbcUrl(Collections.emptyMap());
    assertEquals("jdbc:trino://localhost:8080/hive/default", url);
  }

  @Test void testDialectBuildJdbcUrlNullConfig() {
    String url = TrinoDialect.INSTANCE.buildJdbcUrl(null);
    assertEquals("jdbc:trino://localhost:8080/hive/default", url);
  }

  @Test void testDialectReadParquetSql() {
    String sql = TrinoDialect.INSTANCE.readParquetSql(
        "/data/sales.parquet", Arrays.asList("id", "amount"));
    assertEquals("SELECT id, amount FROM sales", sql);
  }

  @Test void testDialectReadParquetSqlAllColumns() {
    String sql = TrinoDialect.INSTANCE.readParquetSql(
        "/data/sales.parquet", Collections.emptyList());
    assertEquals("SELECT * FROM sales", sql);
  }

  @Test void testDialectCreateParquetViewSql() {
    String sql = TrinoDialect.INSTANCE.createParquetViewSql(
        "myschema", "mytable", "/data/file.parquet", false);
    assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS"));
    assertTrue(sql.contains("external_location"));
    assertTrue(sql.contains("PARQUET"));
    assertTrue(sql.contains("/data/file.parquet"));
  }

  @Test void testDialectCreateIcebergViewSql() {
    String sql = TrinoDialect.INSTANCE.createIcebergViewSql(
        "myschema", "mytable", "s3://bucket/iceberg/table");
    assertTrue(sql.contains("CALL iceberg.system.register_table"));
    assertTrue(sql.contains("myschema"));
    assertTrue(sql.contains("mytable"));
    assertTrue(sql.contains("s3://bucket/iceberg/table"));
  }

  @Test void testDialectDropViewSql() {
    String sql = TrinoDialect.INSTANCE.dropViewSql("myschema", "mytable");
    assertEquals("DROP TABLE IF EXISTS \"myschema\".\"mytable\"", sql);
  }

  @Test void testDialectSupportsDirectGlob() {
    assertFalse(TrinoDialect.INSTANCE.supportsDirectGlob());
  }

  @Test void testDialectSupportsIceberg() {
    assertTrue(TrinoDialect.INSTANCE.supportsIceberg());
  }

  @Test void testDialectName() {
    assertEquals("Trino", TrinoDialect.INSTANCE.getName());
  }

  @Test void testDialectRegisterTableSql() {
    String sql = TrinoDialect.INSTANCE.registerTableSql(
        "sales", "/data/sales", "PARQUET");
    assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS sales"));
    assertTrue(sql.contains("external_location"));
    assertTrue(sql.contains("PARQUET"));
  }

  // --- TrinoExecutionEngine tests ---

  @Test void testEngineType() {
    assertEquals("TRINO", TrinoExecutionEngine.getEngineType());
  }

  @Test void testIsAvailableReturnsBoolean() {
    // The driver may or may not be on classpath, but this should not throw
    boolean result = TrinoExecutionEngine.isAvailable();
    assertTrue(result || !result);
  }

  @Test void testIsServerReachableUnreachable() {
    // Port 1 is almost certainly not running a Trino server
    boolean result = TrinoExecutionEngine.isServerReachable("localhost", 1);
    assertFalse(result, "Should not be reachable on port 1");
  }
}
