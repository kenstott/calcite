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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link TrinoConfig}.
 */
@Tag("unit")
public class TrinoConfigTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TrinoConfigTest.class);

  @Test
  public void testDefaultConstructorUsesDefaults() {
    TrinoConfig config = new TrinoConfig();
    assertEquals(TrinoConfig.DEFAULT_HOST, config.getHost());
    assertEquals(TrinoConfig.DEFAULT_PORT, config.getPort());
    assertEquals(TrinoConfig.DEFAULT_CATALOG, config.getCatalog());
    assertEquals(TrinoConfig.DEFAULT_SCHEMA, config.getSchema());
    assertNull(config.getUser());
    assertNull(config.getPassword());
    assertEquals(TrinoConfig.DEFAULT_ICEBERG_CATALOG, config.getIcebergCatalog());
    assertEquals(TrinoConfig.DEFAULT_WAREHOUSE_DIR, config.getWarehouseDir());
    assertNull(config.getS3AccessKey());
    assertNull(config.getS3SecretKey());
    assertNull(config.getS3Endpoint());
    assertNotNull(config.getAdditionalSettings());
    assertTrue(config.getAdditionalSettings().isEmpty());
    LOGGER.debug("Default config: {}", config);
  }

  @Test
  public void testMapConstructorWithAllFields() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("host", "trino-server");
    configMap.put("port", "9090");
    configMap.put("catalog", "my_hive");
    configMap.put("schema", "analytics");
    configMap.put("user", "admin");
    configMap.put("password", "secret");
    configMap.put("icebergCatalog", "my_iceberg");
    configMap.put("warehouseDir", "/opt/warehouse");
    configMap.put("s3AccessKey", "AKIAIOSFODNN7EXAMPLE");
    configMap.put("s3SecretKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    configMap.put("s3Endpoint", "http://minio:9000");

    TrinoConfig config = new TrinoConfig(configMap);
    assertEquals("trino-server", config.getHost());
    assertEquals("9090", config.getPort());
    assertEquals("my_hive", config.getCatalog());
    assertEquals("analytics", config.getSchema());
    assertEquals("admin", config.getUser());
    assertEquals("secret", config.getPassword());
    assertEquals("my_iceberg", config.getIcebergCatalog());
    assertEquals("/opt/warehouse", config.getWarehouseDir());
    assertEquals("AKIAIOSFODNN7EXAMPLE", config.getS3AccessKey());
    assertEquals("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", config.getS3SecretKey());
    assertEquals("http://minio:9000", config.getS3Endpoint());
  }

  @Test
  public void testMapConstructorWithDefaults() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    TrinoConfig config = new TrinoConfig(configMap);
    assertEquals(TrinoConfig.DEFAULT_HOST, config.getHost());
    assertEquals(TrinoConfig.DEFAULT_PORT, config.getPort());
    assertEquals(TrinoConfig.DEFAULT_CATALOG, config.getCatalog());
    assertEquals(TrinoConfig.DEFAULT_SCHEMA, config.getSchema());
    assertNull(config.getUser());
    assertNull(config.getPassword());
    assertEquals(TrinoConfig.DEFAULT_ICEBERG_CATALOG, config.getIcebergCatalog());
    assertEquals(TrinoConfig.DEFAULT_WAREHOUSE_DIR, config.getWarehouseDir());
  }

  @Test
  public void testMapConstructorWithNumericPort() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("port", Integer.valueOf(8443));
    TrinoConfig config = new TrinoConfig(configMap);
    assertEquals("8443", config.getPort());
  }

  @Test
  public void testMapConstructorAdditionalSettings() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("host", "localhost");
    configMap.put("custom.setting", "custom_value");
    configMap.put("another.setting", "another_value");

    TrinoConfig config = new TrinoConfig(configMap);
    Properties additional = config.getAdditionalSettings();
    assertEquals("custom_value", additional.getProperty("custom.setting"));
    assertEquals("another_value", additional.getProperty("another.setting"));
    // Known settings should NOT be in additional settings
    assertNull(additional.getProperty("host"));
  }

  @Test
  public void testFullConstructor() {
    Properties extra = new Properties();
    extra.setProperty("query.max-memory", "4GB");

    TrinoConfig config = new TrinoConfig(
        "remote-host", "9999", "prod_hive", "prod_schema",
        "user1", "pass1", "prod_iceberg", "/mnt/warehouse",
        "access123", "secret456", "http://s3:9000", extra);

    assertEquals("remote-host", config.getHost());
    assertEquals("9999", config.getPort());
    assertEquals("prod_hive", config.getCatalog());
    assertEquals("prod_schema", config.getSchema());
    assertEquals("user1", config.getUser());
    assertEquals("pass1", config.getPassword());
    assertEquals("prod_iceberg", config.getIcebergCatalog());
    assertEquals("/mnt/warehouse", config.getWarehouseDir());
    assertEquals("access123", config.getS3AccessKey());
    assertEquals("secret456", config.getS3SecretKey());
    assertEquals("http://s3:9000", config.getS3Endpoint());
    assertEquals("4GB", config.getAdditionalSettings().getProperty("query.max-memory"));
  }

  @Test
  public void testFullConstructorNullDefaultsToDefaults() {
    TrinoConfig config = new TrinoConfig(
        null, null, null, null,
        null, null, null, null,
        null, null, null, null);

    assertEquals(TrinoConfig.DEFAULT_HOST, config.getHost());
    assertEquals(TrinoConfig.DEFAULT_PORT, config.getPort());
    assertEquals(TrinoConfig.DEFAULT_CATALOG, config.getCatalog());
    assertEquals(TrinoConfig.DEFAULT_SCHEMA, config.getSchema());
    assertNull(config.getUser());
    assertNull(config.getPassword());
    assertEquals(TrinoConfig.DEFAULT_ICEBERG_CATALOG, config.getIcebergCatalog());
    assertEquals(TrinoConfig.DEFAULT_WAREHOUSE_DIR, config.getWarehouseDir());
    assertNotNull(config.getAdditionalSettings());
    assertTrue(config.getAdditionalSettings().isEmpty());
  }

  @Test
  public void testToSessionSettingsEmpty() {
    TrinoConfig config = new TrinoConfig();
    String[] settings = config.toSessionSettings();
    assertNotNull(settings);
    assertEquals(0, settings.length);
  }

  @Test
  public void testToSessionSettingsWithAdditionalProperties() {
    Properties extra = new Properties();
    extra.setProperty("query.max-memory", "4GB");

    TrinoConfig config = new TrinoConfig(
        "localhost", "8080", "hive", "default",
        null, null, "iceberg", "/data/warehouse",
        null, null, null, extra);

    String[] settings = config.toSessionSettings();
    assertEquals(1, settings.length);
    assertTrue(settings[0].startsWith("SET SESSION "));
    assertTrue(settings[0].contains("query.max-memory"));
    assertTrue(settings[0].contains("4GB"));
  }

  @Test
  public void testGetHiveCatalogPropertiesWithoutS3() {
    TrinoConfig config = new TrinoConfig();
    Properties props = config.getHiveCatalogProperties();
    assertEquals("hive", props.getProperty("connector.name"));
    assertEquals("file", props.getProperty("hive.metastore"));
    assertEquals(TrinoConfig.DEFAULT_WAREHOUSE_DIR,
        props.getProperty("hive.metastore.catalog.dir"));
    assertEquals("allow-all", props.getProperty("hive.security"));
    assertEquals("true", props.getProperty("hive.non-managed-table-writes-enabled"));
    assertNull(props.getProperty("hive.s3.aws-access-key"));
    assertNull(props.getProperty("hive.s3.aws-secret-key"));
  }

  @Test
  public void testGetHiveCatalogPropertiesWithS3() {
    TrinoConfig config = new TrinoConfig(
        "localhost", "8080", "hive", "default",
        null, null, "iceberg", "/data/warehouse",
        "myAccessKey", "mySecretKey", "http://minio:9000", null);

    Properties props = config.getHiveCatalogProperties();
    assertEquals("hive", props.getProperty("connector.name"));
    assertEquals("myAccessKey", props.getProperty("hive.s3.aws-access-key"));
    assertEquals("mySecretKey", props.getProperty("hive.s3.aws-secret-key"));
    assertEquals("http://minio:9000", props.getProperty("hive.s3.endpoint"));
    assertEquals("true", props.getProperty("hive.s3.path-style-access"));
  }

  @Test
  public void testGetIcebergCatalogPropertiesWithoutS3() {
    TrinoConfig config = new TrinoConfig();
    Properties props = config.getIcebergCatalogProperties();
    assertEquals("iceberg", props.getProperty("connector.name"));
    assertEquals("TESTING_FILE_METASTORE", props.getProperty("iceberg.catalog.type"));
    assertEquals(TrinoConfig.DEFAULT_WAREHOUSE_DIR,
        props.getProperty("hive.metastore.catalog.dir"));
    assertNull(props.getProperty("hive.s3.aws-access-key"));
  }

  @Test
  public void testGetIcebergCatalogPropertiesWithS3() {
    TrinoConfig config = new TrinoConfig(
        "localhost", "8080", "hive", "default",
        null, null, "iceberg", "/data/warehouse",
        "accessKey", "secretKey", "http://s3:9000", null);

    Properties props = config.getIcebergCatalogProperties();
    assertEquals("iceberg", props.getProperty("connector.name"));
    assertEquals("accessKey", props.getProperty("hive.s3.aws-access-key"));
    assertEquals("secretKey", props.getProperty("hive.s3.aws-secret-key"));
    assertEquals("http://s3:9000", props.getProperty("hive.s3.endpoint"));
    assertEquals("true", props.getProperty("hive.s3.path-style-access"));
  }

  @Test
  public void testGetHiveCatalogPropertiesWithS3NoEndpoint() {
    TrinoConfig config = new TrinoConfig(
        "localhost", "8080", "hive", "default",
        null, null, "iceberg", "/data/warehouse",
        "accessKey", "secretKey", null, null);

    Properties props = config.getHiveCatalogProperties();
    assertEquals("accessKey", props.getProperty("hive.s3.aws-access-key"));
    assertEquals("secretKey", props.getProperty("hive.s3.aws-secret-key"));
    assertNull(props.getProperty("hive.s3.endpoint"));
    assertNull(props.getProperty("hive.s3.path-style-access"));
  }

  @Test
  public void testGenerateCatalogFiles(@TempDir Path tempDir) throws IOException {
    TrinoConfig config = new TrinoConfig(
        "localhost", "8080", "myhive", "default",
        null, null, "myiceberg", "/data/warehouse",
        null, null, null, null);

    String outputDir = tempDir.toString();
    config.generateCatalogFiles(outputDir);

    File hiveFile = new File(outputDir, "myhive.properties");
    assertTrue(hiveFile.exists(), "Hive catalog file should exist");

    File icebergFile = new File(outputDir, "myiceberg.properties");
    assertTrue(icebergFile.exists(), "Iceberg catalog file should exist");

    // Verify hive properties content
    Properties hiveProps = new Properties();
    try (FileInputStream fis = new FileInputStream(hiveFile)) {
      hiveProps.load(fis);
    }
    assertEquals("hive", hiveProps.getProperty("connector.name"));
    assertEquals("file", hiveProps.getProperty("hive.metastore"));

    // Verify iceberg properties content
    Properties icebergProps = new Properties();
    try (FileInputStream fis = new FileInputStream(icebergFile)) {
      icebergProps.load(fis);
    }
    assertEquals("iceberg", icebergProps.getProperty("connector.name"));
    assertEquals("TESTING_FILE_METASTORE", icebergProps.getProperty("iceberg.catalog.type"));
  }

  @Test
  public void testToStringMasksUser() {
    TrinoConfig configWithUser = new TrinoConfig(
        "localhost", "8080", "hive", "default",
        "admin", null, "iceberg", "/data/warehouse",
        null, null, null, null);

    String result = configWithUser.toString();
    assertFalse(result.contains("admin"),
        "toString should not expose the user name");
    assertTrue(result.contains("***"),
        "toString should mask the user with ***");

    TrinoConfig configNoUser = new TrinoConfig();
    String resultNoUser = configNoUser.toString();
    assertTrue(resultNoUser.contains("null"),
        "toString should show 'null' when no user is set");
    assertFalse(resultNoUser.contains("***"),
        "toString should not mask when user is null");
  }

  @Test
  public void testMapConstructorWithNullValueInMap() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("host", "localhost");
    configMap.put("custom_key", null);

    TrinoConfig config = new TrinoConfig(configMap);
    assertNull(config.getAdditionalSettings().getProperty("custom_key"),
        "Null values in map should not appear in additional settings");
  }

  @Test
  public void testGetHiveCatalogPropertiesWithPartialS3OnlyAccessKey() {
    TrinoConfig config = new TrinoConfig(
        "localhost", "8080", "hive", "default",
        null, null, "iceberg", "/data/warehouse",
        "accessKey", null, null, null);

    Properties props = config.getHiveCatalogProperties();
    // Both access key AND secret key are required for S3 props to be set
    assertNull(props.getProperty("hive.s3.aws-access-key"),
        "Should not set S3 props when only access key is provided");
  }

  @Test
  public void testGetIcebergCatalogPropertiesWithPartialS3OnlySecretKey() {
    TrinoConfig config = new TrinoConfig(
        "localhost", "8080", "hive", "default",
        null, null, "iceberg", "/data/warehouse",
        null, "secretKey", null, null);

    Properties props = config.getIcebergCatalogProperties();
    assertNull(props.getProperty("hive.s3.aws-secret-key"),
        "Should not set S3 props when only secret key is provided");
  }

  @Test
  public void testGenerateCatalogFilesWithS3Config(@TempDir Path tempDir)
      throws IOException {
    TrinoConfig config = new TrinoConfig(
        "localhost", "8080", "myhive", "default",
        null, null, "myiceberg", "/data/warehouse",
        "ak123", "sk456", "http://minio:9000", null);

    config.generateCatalogFiles(tempDir.toString());

    // Verify hive catalog file has S3 config
    File hiveFile = new File(tempDir.toString(), "myhive.properties");
    assertTrue(hiveFile.exists());
    Properties hiveProps = new Properties();
    try (FileInputStream fis = new FileInputStream(hiveFile)) {
      hiveProps.load(fis);
    }
    assertEquals("ak123", hiveProps.getProperty("hive.s3.aws-access-key"));
    assertEquals("sk456", hiveProps.getProperty("hive.s3.aws-secret-key"));
    assertEquals("http://minio:9000", hiveProps.getProperty("hive.s3.endpoint"));
    assertEquals("true", hiveProps.getProperty("hive.s3.path-style-access"));
  }

  @Test
  public void testToSessionSettingsWithMultipleProperties() {
    Properties extra = new Properties();
    extra.setProperty("query.max-memory", "4GB");
    extra.setProperty("query.max-total-memory", "8GB");

    TrinoConfig config = new TrinoConfig(
        "localhost", "8080", "hive", "default",
        null, null, "iceberg", "/data/warehouse",
        null, null, null, extra);

    String[] settings = config.toSessionSettings();
    assertEquals(2, settings.length,
        "Should have two SET SESSION statements");
    for (String s : settings) {
      assertTrue(s.startsWith("SET SESSION "),
          "Each setting should start with SET SESSION");
    }
  }

  @Test
  public void testToStringShowsAdditionalSettingsCount() {
    Properties extra = new Properties();
    extra.setProperty("k1", "v1");
    extra.setProperty("k2", "v2");
    extra.setProperty("k3", "v3");

    TrinoConfig config = new TrinoConfig(
        "localhost", "8080", "hive", "default",
        null, null, "iceberg", "/data/warehouse",
        null, null, null, extra);

    String str = config.toString();
    assertTrue(str.contains("3 items"),
        "toString should show count of additional settings");
  }
}
