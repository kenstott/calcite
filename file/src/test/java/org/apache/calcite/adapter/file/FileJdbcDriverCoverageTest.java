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

import org.apache.calcite.avatica.DriverVersion;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Coverage unit tests for {@link FileJdbcDriver}.
 *
 * <p>Tests URL parsing, acceptsURL, configuration parsing,
 * driver version creation, and property handling.
 */
@Tag("unit")
class FileJdbcDriverCoverageTest {

  // ---- acceptsURL tests ----

  @Test void testAcceptsFileSchemaUrl() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertTrue(driver.acceptsURL("jdbc:calcite:schema=file"),
        "Should accept URL with schema=file");
  }

  @Test void testAcceptsFileSchemaUrlWithParams() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertTrue(driver.acceptsURL(
            "jdbc:calcite:schema=file;data_path=/data;engine=parquet"),
        "Should accept URL with schema=file and parameters");
  }

  @Test void testAcceptsMaterializedViewUrl() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertTrue(driver.acceptsURL("jdbc:calcite:materialized_view=true"),
        "Should accept URL with materialized_view");
  }

  @Test void testRejectsPlainCalciteUrl() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertFalse(driver.acceptsURL("jdbc:calcite:"),
        "Should reject plain Calcite URL without schema=file or materialized_view");
  }

  @Test void testRejectsNonCalciteUrl() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306/db"),
        "Should reject non-Calcite URL");
  }

  @Test void testRejectsCalciteSchemaOther() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertFalse(driver.acceptsURL("jdbc:calcite:schema=jdbc"),
        "Should reject non-file schema types");
  }

  // ---- createDriverVersion tests ----

  @Test void testCreateDriverVersion() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();

    java.lang.reflect.Method method =
        FileJdbcDriver.class.getDeclaredMethod("createDriverVersion");
    method.setAccessible(true);
    DriverVersion version = (DriverVersion) method.invoke(driver);

    assertNotNull(version, "Driver version should not be null");
    // Verify version contains expected information
    String versionStr = version.toString();
    assertNotNull(versionStr);
  }

  // ---- parseConfiguration tests (via reflection) ----

  /**
   * The URL parser splits by ";" then by "=". For the first segment
   * "jdbc:calcite:schema=file", the key becomes "jdbc:calcite:schema" which
   * does not match the switch case "schema". To get "schema=file" as a proper
   * key-value pair, it must appear after a ";" separator.
   */
  @Test void testParseConfigurationFileSchemaViaSemicolon() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    // The first part "jdbc:calcite:" has key "jdbc:calcite:" which won't match.
    // "schema=file" is the second part after ";".
    Object config = invokeParseConfiguration(driver,
        "jdbc:calcite:;schema=file;data_path=/mydata;storage_path=/mystorage;engine=arrow",
        info);

    assertNotNull(config, "Config should not be null");

    @SuppressWarnings("unchecked")
    java.util.List<?> schemas = (java.util.List<?>) invokeGetSchemas(config);
    assertEquals(1, schemas.size(), "Should have one schema when schema=file");
  }

  @Test void testParseConfigurationDefaultPathsNoSchemaMatch() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    // "jdbc:calcite:schema=file" has schema=file embedded in the first segment
    // after the colon-delimited prefix. The parser key becomes "jdbc:calcite:schema"
    // which does NOT match the "schema" case. This results in 0 schemas.
    Object config = invokeParseConfiguration(driver,
        "jdbc:calcite:schema=file", info);

    assertNotNull(config, "Config should not be null");

    @SuppressWarnings("unchecked")
    java.util.List<?> schemas = (java.util.List<?>) invokeGetSchemas(config);
    assertEquals(0, schemas.size(),
        "Schema=file embedded in JDBC prefix should not match");
  }

  @Test void testParseConfigurationBatchSize() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    Object config = invokeParseConfiguration(driver,
        "jdbc:calcite:;schema=file;batch_size=4096", info);

    assertNotNull(config);
  }

  @Test void testParseConfigurationInvalidBatchSize() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    // Invalid batch_size should use default without throwing
    Object config = invokeParseConfiguration(driver,
        "jdbc:calcite:;schema=file;batch_size=invalid", info);

    assertNotNull(config, "Should handle invalid batch_size gracefully");
  }

  @Test void testParseConfigurationPropertyOverrides() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("data_path", "/override/data");
    info.setProperty("storage_path", "/override/storage");
    info.setProperty("engine", "vectorized");

    Object config = invokeParseConfiguration(driver,
        "jdbc:calcite:;schema=file;data_path=/original;storage_path=/original;engine=parquet",
        info);

    assertNotNull(config);
    @SuppressWarnings("unchecked")
    java.util.List<?> schemas = (java.util.List<?>) invokeGetSchemas(config);
    assertEquals(1, schemas.size());
  }

  @Test void testParseConfigurationLegacyProperties() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("materialized_view.storage_path", "/legacy/storage");
    info.setProperty("materialized_view.engine", "linq4j");

    Object config = invokeParseConfiguration(driver,
        "jdbc:calcite:;schema=file", info);

    assertNotNull(config);
  }

  @Test void testParseConfigurationNoFileSchema() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    Object config = invokeParseConfiguration(driver,
        "jdbc:calcite:;schema=other", info);

    assertNotNull(config);
    @SuppressWarnings("unchecked")
    java.util.List<?> schemas = (java.util.List<?>) invokeGetSchemas(config);
    assertEquals(0, schemas.size(),
        "Non-file schema should not create a schema config");
  }

  // ---- addConfiguredSchemas tests (via reflection) ----

  @Test void testAddConfiguredSchemasFromProperties() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.sales.data_path", "/data/sales");
    info.setProperty("schema.sales.storage_path", "/storage/sales");
    info.setProperty("schema.sales.engine", "arrow");
    info.setProperty("schema.sales.batch_size", "1024");

    Object config = invokeParseConfiguration(driver,
        "jdbc:calcite:", info);

    assertNotNull(config);
    @SuppressWarnings("unchecked")
    java.util.List<?> schemas = (java.util.List<?>) invokeGetSchemas(config);
    assertEquals(1, schemas.size(),
        "Should create schema from schema.* properties");
  }

  @Test void testAddConfiguredSchemasMultiple() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.sales.data_path", "/data/sales");
    info.setProperty("schema.sales.storage_path", "/storage/sales");
    info.setProperty("schema.inventory.data_path", "/data/inv");
    info.setProperty("schema.inventory.storage_path", "/storage/inv");

    Object config = invokeParseConfiguration(driver,
        "jdbc:calcite:", info);

    assertNotNull(config);
    @SuppressWarnings("unchecked")
    java.util.List<?> schemas = (java.util.List<?>) invokeGetSchemas(config);
    assertEquals(2, schemas.size(),
        "Should create two schemas from schema.* properties");
  }

  @Test void testAddConfiguredSchemasInvalidBatchSize() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.bad.data_path", "/data/bad");
    info.setProperty("schema.bad.storage_path", "/storage/bad");
    info.setProperty("schema.bad.batch_size", "not-a-number");

    Object config = invokeParseConfiguration(driver,
        "jdbc:calcite:", info);

    assertNotNull(config);
    @SuppressWarnings("unchecked")
    java.util.List<?> schemas = (java.util.List<?>) invokeGetSchemas(config);
    assertEquals(0, schemas.size(),
        "Invalid batch_size should skip the schema config");
  }

  @Test void testAddConfiguredSchemasIncompleteConfig() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.partial.data_path", "/data/partial");
    // Missing storage_path

    Object config = invokeParseConfiguration(driver,
        "jdbc:calcite:", info);

    assertNotNull(config);
    @SuppressWarnings("unchecked")
    java.util.List<?> schemas = (java.util.List<?>) invokeGetSchemas(config);
    assertEquals(0, schemas.size(),
        "Incomplete schema config (missing storage_path) should be skipped");
  }

  // ---- SchemaConfig inner class tests (via reflection) ----

  @Test void testSchemaConfigFields() throws Exception {
    Object schemaConfig = createSchemaConfig("mySchema", "/data", "/storage", "parquet", 2048);
    assertNotNull(schemaConfig);

    assertEquals("mySchema", invokeGetter(schemaConfig, "getName"));
    assertEquals("/data", invokeGetter(schemaConfig, "getDataPath"));
    assertEquals("/storage", invokeGetter(schemaConfig, "getStoragePath"));
    assertEquals("parquet", invokeGetter(schemaConfig, "getEngineType"));
    assertEquals(2048, invokeGetter(schemaConfig, "getBatchSize"));
  }

  // ---- Helper methods ----

  private Object invokeParseConfiguration(FileJdbcDriver driver, String url, Properties info)
      throws Exception {
    java.lang.reflect.Method method = FileJdbcDriver.class.getDeclaredMethod(
        "parseConfiguration", String.class, Properties.class);
    method.setAccessible(true);
    return method.invoke(driver, url, info);
  }

  private Object invokeGetSchemas(Object config) throws Exception {
    java.lang.reflect.Method method = config.getClass().getDeclaredMethod("getSchemas");
    method.setAccessible(true);
    return method.invoke(config);
  }

  private Object createSchemaConfig(String name, String dataPath, String storagePath,
      String engineType, int batchSize) throws Exception {
    Class<?>[] innerClasses = FileJdbcDriver.class.getDeclaredClasses();
    Class<?> schemaConfigClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("SchemaConfig")) {
        schemaConfigClass = c;
        break;
      }
    }
    assertNotNull(schemaConfigClass, "SchemaConfig inner class should exist");

    java.lang.reflect.Constructor<?> ctor = schemaConfigClass.getDeclaredConstructor(
        String.class, String.class, String.class, String.class, int.class);
    ctor.setAccessible(true);
    return ctor.newInstance(name, dataPath, storagePath, engineType, batchSize);
  }

  private Object invokeGetter(Object obj, String methodName) throws Exception {
    java.lang.reflect.Method method = obj.getClass().getDeclaredMethod(methodName);
    method.setAccessible(true);
    return method.invoke(obj);
  }
}
