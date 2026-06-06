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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage unit tests for {@link FileJdbcDriver}.
 *
 * <p>Targets missed lines in addConfiguredSchemas(), acceptsURL()
 * with materialized_view URL formats, parseConfiguration() batch_size
 * parsing and NumberFormatException, and Properties-based overrides.
 */
@Tag("unit")
class FileJdbcDriverDeepCoverageTest {

  // =====================================================================
  // acceptsURL() deep tests
  // =====================================================================

  @Test void testAcceptsUrlMaterializedViewInUrl() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertTrue(driver.acceptsURL("jdbc:calcite:materialized_view=true;data_path=/data"),
        "Should accept URL containing materialized_view with additional params");
  }

  @Test void testAcceptsUrlMaterializedViewMidUrl() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertTrue(
        driver.acceptsURL(
            "jdbc:calcite:data_path=/data;materialized_view=enabled;engine=parquet"),
        "Should accept URL with materialized_view in the middle");
  }

  @Test void testAcceptsUrlMaterializedViewOnly() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertTrue(driver.acceptsURL("jdbc:calcite:materialized_view"),
        "Should accept URL with just materialized_view keyword");
  }

  @Test void testAcceptsUrlBothSchemaFileAndMaterializedView() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertTrue(
        driver.acceptsURL(
            "jdbc:calcite:schema=file;materialized_view=true"),
        "Should accept URL with both schema=file and materialized_view");
  }

  @Test void testRejectsUrlWithNoFileOrMaterializedView() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertFalse(driver.acceptsURL("jdbc:calcite:schema=memory"),
        "Should reject URL without schema=file or materialized_view");
  }

  @Test void testRejectsUrlNotStartingWithJdbcCalcite() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertFalse(driver.acceptsURL("jdbc:h2:schema=file"),
        "Should reject URL not starting with jdbc:calcite:");
  }

  @Test void testRejectsUrlWithMaterializedViewInWrongPrefix() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertFalse(driver.acceptsURL("jdbc:other:materialized_view=true"),
        "Should reject URL with materialized_view but wrong prefix");
  }

  // =====================================================================
  // parseConfiguration() - batch_size parsing with valid values
  // =====================================================================

  @Test void testParseConfigBatchSizeSmallValue() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;batch_size=512;data_path=/d;storage_path=/s", info);

    assertNotNull(config);
    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    // Verify the batch size was parsed correctly
    Object schemaConfig = schemas.get(0);
    int batchSize = (int) invokeGetter(schemaConfig, "getBatchSize");
    assertEquals(512, batchSize, "Batch size should be 512");
  }

  @Test void testParseConfigBatchSizeLargeValue() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;batch_size=8192;data_path=/d;storage_path=/s", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    int batchSize = (int) invokeGetter(schemaConfig, "getBatchSize");
    assertEquals(8192, batchSize, "Batch size should be 8192");
  }

  @Test void testParseConfigBatchSizeDefaultWhenNotSpecified() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;data_path=/d;storage_path=/s", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    int batchSize = (int) invokeGetter(schemaConfig, "getBatchSize");
    assertEquals(2048, batchSize, "Default batch size should be 2048");
  }

  // =====================================================================
  // parseConfiguration() - NumberFormatException on batch_size
  // =====================================================================

  @Test void testParseConfigBatchSizeEmptyString() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    // Empty string should trigger NumberFormatException and use default
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;batch_size=;data_path=/d;storage_path=/s", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    int batchSize = (int) invokeGetter(schemaConfig, "getBatchSize");
    assertEquals(2048, batchSize,
        "Empty batch_size should fall back to default 2048");
  }

  @Test void testParseConfigBatchSizeNonNumeric() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;batch_size=abc;data_path=/d;storage_path=/s", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    int batchSize = (int) invokeGetter(schemaConfig, "getBatchSize");
    assertEquals(2048, batchSize,
        "Non-numeric batch_size should fall back to default 2048");
  }

  @Test void testParseConfigBatchSizeFloat() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    // Floating point value should trigger NumberFormatException
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;batch_size=10.5;data_path=/d;storage_path=/s", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    int batchSize = (int) invokeGetter(schemaConfig, "getBatchSize");
    assertEquals(2048, batchSize,
        "Float batch_size should fall back to default 2048");
  }

  // =====================================================================
  // parseConfiguration() - Properties-based overrides
  // =====================================================================

  @Test void testPropertiesOverrideEngine() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("engine", "duckdb");

    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;engine=parquet;data_path=/d;storage_path=/s", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    String engine = (String) invokeGetter(schemaConfig, "getEngineType");
    assertEquals("duckdb", engine,
        "Properties engine should override URL engine");
  }

  @Test void testPropertiesOverrideStoragePath() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("storage_path", "/override/storage");

    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;storage_path=/url/storage;data_path=/d", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    String storagePath = (String) invokeGetter(schemaConfig, "getStoragePath");
    assertEquals("/override/storage", storagePath,
        "Properties storage_path should override URL storage_path");
  }

  @Test void testPropertiesOverrideDataPath() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("data_path", "/override/data");

    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;data_path=/url/data;storage_path=/s", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    String dataPath = (String) invokeGetter(schemaConfig, "getDataPath");
    assertEquals("/override/data", dataPath,
        "Properties data_path should override URL data_path");
  }

  @Test void testLegacyMaterializedViewStoragePathOverride() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("materialized_view.storage_path", "/legacy/mv/storage");

    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;storage_path=/url/storage;data_path=/d", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    String storagePath = (String) invokeGetter(schemaConfig, "getStoragePath");
    assertEquals("/legacy/mv/storage", storagePath,
        "Legacy materialized_view.storage_path should override storage_path");
  }

  @Test void testLegacyMaterializedViewEngineOverride() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("materialized_view.engine", "linq4j");

    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;engine=parquet;data_path=/d;storage_path=/s", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    String engine = (String) invokeGetter(schemaConfig, "getEngineType");
    assertEquals("linq4j", engine,
        "Legacy materialized_view.engine should override engine");
  }

  @Test void testPropertiesAndLegacyOverridePrecedence() throws Exception {
    // Both properties and legacy materialized_view properties set.
    // Legacy runs after regular properties, so it takes precedence.
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("storage_path", "/prop/storage");
    info.setProperty("materialized_view.storage_path", "/legacy/storage");

    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;data_path=/d", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    String storagePath = (String) invokeGetter(schemaConfig, "getStoragePath");
    assertEquals("/legacy/storage", storagePath,
        "Legacy materialized_view.storage_path should take final precedence");
  }

  // =====================================================================
  // parseConfiguration() - default paths
  // =====================================================================

  @Test void testDefaultStoragePathWhenNoneProvided() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    String storagePath = (String) invokeGetter(schemaConfig, "getStoragePath");
    String expectedDefault =
        System.getProperty("java.io.tmpdir") + "/calcite_file_storage";
    assertEquals(expectedDefault, storagePath,
        "Default storage path should use java.io.tmpdir");
  }

  @Test void testDefaultDataPathWhenNoneProvided() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    String dataPath = (String) invokeGetter(schemaConfig, "getDataPath");
    String expectedDefault = System.getProperty("user.dir") + "/data";
    assertEquals(expectedDefault, dataPath,
        "Default data path should use user.dir/data");
  }

  // =====================================================================
  // parseConfiguration() - URL key-value parsing edge cases
  // =====================================================================

  @Test void testParseUrlPartWithoutEquals() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    // Parts without "=" should be skipped
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;noequalssign;data_path=/d;storage_path=/s", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());
  }

  @Test void testParseUrlPartWithValueContainingEquals() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    // split("=", 2) should handle value containing "="
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;data_path=/d/path=special;storage_path=/s", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    String dataPath = (String) invokeGetter(schemaConfig, "getDataPath");
    assertEquals("/d/path=special", dataPath,
        "Data path with '=' in value should be preserved");
  }

  // =====================================================================
  // addConfiguredSchemas() - deep field verification
  // =====================================================================

  @Test void testAddConfiguredSchemasVerifyFieldValues() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.myschema.data_path", "/data/myschema");
    info.setProperty("schema.myschema.storage_path", "/storage/myschema");
    info.setProperty("schema.myschema.engine", "duckdb");
    info.setProperty("schema.myschema.batch_size", "4096");

    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    assertEquals("myschema", invokeGetter(schemaConfig, "getName"),
        "Schema name should be 'myschema'");
    assertEquals("/data/myschema", invokeGetter(schemaConfig, "getDataPath"),
        "Data path should match property");
    assertEquals("/storage/myschema", invokeGetter(schemaConfig, "getStoragePath"),
        "Storage path should match property");
    assertEquals("duckdb", invokeGetter(schemaConfig, "getEngineType"),
        "Engine should be 'duckdb'");
    assertEquals(4096, invokeGetter(schemaConfig, "getBatchSize"),
        "Batch size should be 4096");
  }

  @Test void testAddConfiguredSchemasDefaultEngineAndBatchSize() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.defaults.data_path", "/data/defaults");
    info.setProperty("schema.defaults.storage_path", "/storage/defaults");
    // No engine or batch_size set

    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    assertEquals("parquet", invokeGetter(schemaConfig, "getEngineType"),
        "Default engine should be 'parquet'");
    assertEquals(2048, invokeGetter(schemaConfig, "getBatchSize"),
        "Default batch size should be 2048");
  }

  @Test void testAddConfiguredSchemasMissingDataPath() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    // Only storage_path, missing data_path
    info.setProperty("schema.nodata.storage_path", "/storage/nodata");
    info.setProperty("schema.nodata.engine", "parquet");

    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(0, schemas.size(),
        "Schema without data_path should be skipped");
  }

  @Test void testAddConfiguredSchemasMissingStoragePath() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    // Only data_path, missing storage_path
    info.setProperty("schema.nostorage.data_path", "/data/nostorage");
    info.setProperty("schema.nostorage.engine", "parquet");

    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(0, schemas.size(),
        "Schema without storage_path should be skipped");
  }

  @Test void testAddConfiguredSchemasInvalidBatchSizeSkipsSchema() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.badbatch.data_path", "/data/badbatch");
    info.setProperty("schema.badbatch.storage_path", "/storage/badbatch");
    info.setProperty("schema.badbatch.batch_size", "xyz");

    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(0, schemas.size(),
        "Schema with invalid batch_size should be skipped entirely");
  }

  @Test void testAddConfiguredSchemasKeyWithOnlyTwoParts() throws Exception {
    // A property like "schema.twodot" (no third part after second dot)
    // should still identify the schema name via parts[1]
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.twodot", "somevalue");

    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(0, schemas.size(),
        "Schema key with no subproperty and no data/storage paths should produce no schema");
  }

  @Test void testAddConfiguredSchemasMultipleSchemasVerifyAll() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    // Schema alpha
    info.setProperty("schema.alpha.data_path", "/data/alpha");
    info.setProperty("schema.alpha.storage_path", "/storage/alpha");
    info.setProperty("schema.alpha.engine", "arrow");
    info.setProperty("schema.alpha.batch_size", "1024");

    // Schema beta
    info.setProperty("schema.beta.data_path", "/data/beta");
    info.setProperty("schema.beta.storage_path", "/storage/beta");
    info.setProperty("schema.beta.engine", "linq4j");
    info.setProperty("schema.beta.batch_size", "512");

    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(2, schemas.size(), "Should have two configured schemas");

    // Verify each schema (order may vary since Properties iteration is not ordered)
    boolean foundAlpha = false;
    boolean foundBeta = false;
    for (Object sc : schemas) {
      String name = (String) invokeGetter(sc, "getName");
      if ("alpha".equals(name)) {
        foundAlpha = true;
        assertEquals("/data/alpha", invokeGetter(sc, "getDataPath"));
        assertEquals("/storage/alpha", invokeGetter(sc, "getStoragePath"));
        assertEquals("arrow", invokeGetter(sc, "getEngineType"));
        assertEquals(1024, invokeGetter(sc, "getBatchSize"));
      } else if ("beta".equals(name)) {
        foundBeta = true;
        assertEquals("/data/beta", invokeGetter(sc, "getDataPath"));
        assertEquals("/storage/beta", invokeGetter(sc, "getStoragePath"));
        assertEquals("linq4j", invokeGetter(sc, "getEngineType"));
        assertEquals(512, invokeGetter(sc, "getBatchSize"));
      }
    }
    assertTrue(foundAlpha, "Should find alpha schema");
    assertTrue(foundBeta, "Should find beta schema");
  }

  @Test void testAddConfiguredSchemasOnlyKeyPrefix() throws Exception {
    // Properties that start with "schema." but don't actually configure
    // a schema should not cause errors
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.test.data_path", "/data/test");
    // Missing storage_path, so schema should not be created
    info.setProperty("schema.other.engine", "parquet");
    // Only engine, no data_path or storage_path

    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(0, schemas.size(),
        "Incomplete schema properties should not create schemas");
  }

  // =====================================================================
  // parseConfiguration() combined with addConfiguredSchemas()
  // =====================================================================

  @Test void testUrlSchemaFileAndPropertySchemasCombined() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    // Property-based schema
    info.setProperty("schema.extra.data_path", "/data/extra");
    info.setProperty("schema.extra.storage_path", "/storage/extra");

    // URL has schema=file which creates a "files" schema
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;data_path=/url/data;storage_path=/url/storage", info);

    List<?> schemas = invokeGetSchemas(config);
    // Should have "files" from URL schema=file + "extra" from properties
    assertEquals(2, schemas.size(),
        "Should have URL schema 'files' plus property schema 'extra'");

    boolean foundFiles = false;
    boolean foundExtra = false;
    for (Object sc : schemas) {
      String name = (String) invokeGetter(sc, "getName");
      if ("files".equals(name)) {
        foundFiles = true;
      } else if ("extra".equals(name)) {
        foundExtra = true;
      }
    }
    assertTrue(foundFiles, "Should find 'files' schema from URL");
    assertTrue(foundExtra, "Should find 'extra' schema from properties");
  }

  @Test void testUrlSchemaFileWithDefaultEngineType() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    // No engine specified in URL or properties
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;data_path=/d;storage_path=/s", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    String engine = (String) invokeGetter(schemaConfig, "getEngineType");
    assertEquals("parquet", engine,
        "Default engine type should be 'parquet'");
  }

  @Test void testSchemaNameFromUrlIsFiles() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();

    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;data_path=/d;storage_path=/s", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());

    Object schemaConfig = schemas.get(0);
    String name = (String) invokeGetter(schemaConfig, "getName");
    assertEquals("files", name,
        "URL schema=file should create schema named 'files'");
  }

  // =====================================================================
  // Non-matching schema.* property keys
  // =====================================================================

  @Test void testNonSchemaDotPropertyIgnored() throws Exception {
    // Properties not starting with "schema." should be ignored
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("other.myschema.data_path", "/data/other");
    info.setProperty("other.myschema.storage_path", "/storage/other");

    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(0, schemas.size(),
        "Properties not starting with schema. should be ignored");
  }

  @Test void testSchemaDotWithoutSecondDotIgnored() throws Exception {
    // A property key "schema" without any dot after it should be ignored
    // because keyStr.startsWith("schema.") requires the dot
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema", "somevalue");

    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);

    List<?> schemas = invokeGetSchemas(config);
    assertEquals(0, schemas.size(),
        "Property key 'schema' without dot should be ignored");
  }

  // =====================================================================
  // FileAdapterConfig inner class coverage
  // =====================================================================

  @Test void testFileAdapterConfigAddAndGetSchemas() throws Exception {
    // Access FileAdapterConfig directly to verify addSchema/getSchemas
    Class<?>[] innerClasses = FileJdbcDriver.class.getDeclaredClasses();
    Class<?> configClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("FileAdapterConfig")) {
        configClass = c;
        break;
      }
    }
    assertNotNull(configClass, "FileAdapterConfig inner class should exist");

    Constructor<?> ctor = configClass.getDeclaredConstructor();
    ctor.setAccessible(true);
    Object configObj = ctor.newInstance();

    // getSchemas should return empty list initially
    Method getSchemasMethod = configClass.getDeclaredMethod("getSchemas");
    getSchemasMethod.setAccessible(true);
    List<?> schemas = (List<?>) getSchemasMethod.invoke(configObj);
    assertNotNull(schemas);
    assertEquals(0, schemas.size(), "New config should have empty schema list");

    // Create a SchemaConfig and add it
    Object schemaConfig = createSchemaConfig("test", "/d", "/s", "parquet", 2048);
    Method addSchemaMethod =
        configClass.getDeclaredMethod("addSchema", schemaConfig.getClass());
    addSchemaMethod.setAccessible(true);
    addSchemaMethod.invoke(configObj, schemaConfig);

    schemas = (List<?>) getSchemasMethod.invoke(configObj);
    assertEquals(1, schemas.size(), "After addSchema, should have 1 schema");
  }

  // =====================================================================
  // Helper methods
  // =====================================================================

  private Object invokeParseConfiguration(FileJdbcDriver driver,
      String url, Properties info) throws Exception {
    Method method =
        FileJdbcDriver.class.getDeclaredMethod("parseConfiguration", String.class, Properties.class);
    method.setAccessible(true);
    return method.invoke(driver, url, info);
  }

  @SuppressWarnings("unchecked")
  private List<?> invokeGetSchemas(Object config) throws Exception {
    Method method = config.getClass().getDeclaredMethod("getSchemas");
    method.setAccessible(true);
    return (List<?>) method.invoke(config);
  }

  private Object invokeGetter(Object obj, String methodName) throws Exception {
    Method method = obj.getClass().getDeclaredMethod(methodName);
    method.setAccessible(true);
    return method.invoke(obj);
  }

  private Object createSchemaConfig(String name, String dataPath,
      String storagePath, String engineType, int batchSize) throws Exception {
    Class<?>[] innerClasses = FileJdbcDriver.class.getDeclaredClasses();
    Class<?> schemaConfigClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("SchemaConfig")) {
        schemaConfigClass = c;
        break;
      }
    }
    assertNotNull(schemaConfigClass, "SchemaConfig inner class should exist");

    Constructor<?> ctor =
        schemaConfigClass.getDeclaredConstructor(String.class, String.class, String.class, String.class, int.class);
    ctor.setAccessible(true);
    return ctor.newInstance(name, dataPath, storagePath, engineType, batchSize);
  }
}
