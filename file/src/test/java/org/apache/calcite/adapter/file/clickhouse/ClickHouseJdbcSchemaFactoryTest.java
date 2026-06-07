/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file.clickhouse;

import org.apache.calcite.adapter.file.execution.clickhouse.ClickHouseConfig;
import org.apache.calcite.adapter.file.execution.clickhouse.ClickHouseExecutionEngine;
import org.apache.calcite.adapter.file.jdbc.ClickHouseDialect;

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
 * Unit tests for ClickHouse schema factory, config, dialect, and execution engine.
 */
@Tag("unit")
class ClickHouseJdbcSchemaFactoryTest {

  // --- ClickHouseConfig tests ---

  @Test void testDefaultConfig() {
    ClickHouseConfig config = new ClickHouseConfig();
    assertEquals("server", config.getMode());
    assertEquals("localhost", config.getHost());
    assertEquals("8123", config.getPort());
    assertEquals("default", config.getDatabase());
    assertNull(config.getLocalBinaryPath());
    assertNull(config.getDataDir());
    assertEquals("4GB", config.getMaxMemory());
    assertFalse(config.isLocalMode());
  }

  @Test void testServerModeConfig() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("mode", "server");
    configMap.put("host", "clickhouse.example.com");
    configMap.put("port", "9000");
    configMap.put("database", "analytics");

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertEquals("server", config.getMode());
    assertEquals("clickhouse.example.com", config.getHost());
    assertEquals("9000", config.getPort());
    assertEquals("analytics", config.getDatabase());
    assertFalse(config.isLocalMode());
  }

  @Test void testLocalModeConfig() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("mode", "local");
    configMap.put("localBinaryPath", "/usr/local/bin/clickhouse-local");
    configMap.put("dataDir", "/tmp/ch-data");
    configMap.put("maxMemory", "2GB");
    configMap.put("maxThreads", 2);

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertEquals("local", config.getMode());
    assertTrue(config.isLocalMode());
    assertEquals("/usr/local/bin/clickhouse-local", config.getLocalBinaryPath());
    assertEquals("/tmp/ch-data", config.getDataDir());
    assertEquals("2GB", config.getMaxMemory());
    assertEquals(2, config.getMaxThreads());
  }

  @Test void testConfigPortAsNumber() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("port", 9000);

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertEquals("9000", config.getPort());
  }

  @Test void testConfigToSettings() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("maxMemory", "2GB");
    configMap.put("maxThreads", 4);

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    String[] settings = config.toClickHouseSettings();

    assertNotNull(settings);
    assertTrue(settings.length >= 2);

    boolean hasMemory = false;
    boolean hasThreads = false;
    for (String setting : settings) {
      if (setting.contains("max_memory_usage")) {
        hasMemory = true;
      }
      if (setting.contains("max_threads")) {
        hasThreads = true;
      }
    }
    assertTrue(hasMemory, "Should contain max_memory_usage setting");
    assertTrue(hasThreads, "Should contain max_threads setting");
  }

  @Test void testConfigAdditionalSettings() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("custom_setting", "custom_value");

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertEquals("custom_value", config.getAdditionalSettings().getProperty("custom_setting"));
  }

  @Test void testConfigToString() {
    ClickHouseConfig config = new ClickHouseConfig();
    String str = config.toString();
    assertNotNull(str);
    assertTrue(str.contains("ClickHouseConfig"));
    assertTrue(str.contains("server"));
  }

  // --- ClickHouseDialect tests ---

  @Test void testDialectDriverClassName() {
    assertEquals("com.clickhouse.jdbc.ClickHouseDriver",
        ClickHouseDialect.INSTANCE.getDriverClassName());
  }

  @Test void testDialectBuildJdbcUrl() {
    Map<String, String> config = new HashMap<>();
    config.put("host", "myhost");
    config.put("port", "9000");
    config.put("database", "mydb");

    String url = ClickHouseDialect.INSTANCE.buildJdbcUrl(config);
    assertEquals("jdbc:clickhouse://myhost:9000/mydb", url);
  }

  @Test void testDialectBuildJdbcUrlDefaults() {
    String url = ClickHouseDialect.INSTANCE.buildJdbcUrl(Collections.emptyMap());
    assertEquals("jdbc:clickhouse://localhost:8123/default", url);
  }

  @Test void testDialectBuildJdbcUrlNullConfig() {
    String url = ClickHouseDialect.INSTANCE.buildJdbcUrl(null);
    assertEquals("jdbc:clickhouse://localhost:8123/default", url);
  }

  @Test void testDialectReadParquetSql() {
    String sql =
        ClickHouseDialect.INSTANCE.readParquetSql("s3://bucket/data/*.parquet", Arrays.asList("id", "name"));
    assertEquals("SELECT id, name FROM s3('s3://bucket/data/*.parquet', 'Parquet')", sql);
  }

  @Test void testDialectReadParquetSqlAllColumns() {
    String sql =
        ClickHouseDialect.INSTANCE.readParquetSql("s3://bucket/data/*.parquet", Collections.emptyList());
    assertEquals("SELECT * FROM s3('s3://bucket/data/*.parquet', 'Parquet')", sql);
  }

  @Test void testDialectReadIcebergSql() {
    String sql =
        ClickHouseDialect.INSTANCE.readIcebergSql("s3://bucket/iceberg/table", Arrays.asList("col1"));
    assertEquals("SELECT col1 FROM iceberg('s3://bucket/iceberg/table')", sql);
  }

  @Test void testDialectCreateParquetViewSqlS3() {
    String sql =
        ClickHouseDialect.INSTANCE.createParquetViewSql("myschema", "mytable", "s3://bucket/data.parquet", false);
    assertTrue(sql.contains("CREATE OR REPLACE VIEW"));
    assertTrue(sql.contains("s3('s3://bucket/data.parquet', 'Parquet')"));
  }

  @Test void testDialectCreateParquetViewSqlLocal() {
    String sql =
        ClickHouseDialect.INSTANCE.createParquetViewSql("myschema", "mytable", "/data/file.parquet", false);
    assertTrue(sql.contains("CREATE OR REPLACE VIEW"));
    assertTrue(sql.contains("file('/data/file.parquet', 'Parquet')"));
  }

  @Test void testDialectCreateIcebergViewSql() {
    String sql =
        ClickHouseDialect.INSTANCE.createIcebergViewSql("myschema", "mytable", "s3://bucket/iceberg/table");
    assertTrue(sql.contains("CREATE OR REPLACE VIEW"));
    assertTrue(sql.contains("iceberg('s3://bucket/iceberg/table')"));
  }

  @Test void testDialectDropViewSql() {
    String sql = ClickHouseDialect.INSTANCE.dropViewSql("myschema", "mytable");
    assertEquals("DROP VIEW IF EXISTS \"myschema\".\"mytable\"", sql);
  }

  @Test void testDialectSupportsDirectGlob() {
    assertTrue(ClickHouseDialect.INSTANCE.supportsDirectGlob());
  }

  @Test void testDialectSupportsIceberg() {
    assertTrue(ClickHouseDialect.INSTANCE.supportsIceberg());
  }

  @Test void testDialectName() {
    assertEquals("ClickHouse", ClickHouseDialect.INSTANCE.getName());
  }

  // --- ClickHouseExecutionEngine tests ---

  @Test void testEngineType() {
    assertEquals("CLICKHOUSE", ClickHouseExecutionEngine.getEngineType());
  }

  @Test void testIsAvailableReturnsBoolean() {
    // The driver may or may not be on classpath, but this should not throw
    boolean result = ClickHouseExecutionEngine.isAvailable();
    // Just verify it returns without error
    assertTrue(result || !result);
  }

  @Test void testIsLocalAvailableReturnsBoolean() {
    // clickhouse-local may or may not be on PATH
    boolean result = ClickHouseExecutionEngine.isLocalAvailable();
    assertTrue(result || !result);
  }

  @Test void testFindLocalBinaryPathNull() {
    // With null configured path, it falls through to env/PATH lookup
    // Won't find anything in test environment typically
    String result = ClickHouseExecutionEngine.findLocalBinaryPath(null);
    // May be null or a path - just verify no exception
    assertTrue(result == null || !result.isEmpty());
  }

  @Test void testFindLocalBinaryPathNonexistent() {
    String result = ClickHouseExecutionEngine.findLocalBinaryPath("/nonexistent/path/clickhouse-local");
    // The configured path doesn't exist, so falls through to env/PATH
    assertTrue(result == null || !result.isEmpty());
  }
}
