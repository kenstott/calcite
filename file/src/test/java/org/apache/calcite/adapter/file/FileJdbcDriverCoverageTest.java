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

import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage unit tests for {@link FileJdbcDriver}.
 *
 * <p>Tests URL parsing, acceptsURL, configuration parsing,
 * driver version creation, property handling, connect(),
 * setupFileSchemas(), setupSchema(), and inner class behavior.
 */
@Tag("unit")
class FileJdbcDriverCoverageTest {

  @TempDir
  Path tempDir;

  @Test void testAcceptsFileSchemaUrl() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertTrue(driver.acceptsURL("jdbc:calcite:schema=file"));
  }

  @Test void testAcceptsFileSchemaUrlWithParams() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertTrue(
        driver.acceptsURL(
        "jdbc:calcite:schema=file;data_path=/data;engine=parquet"));
  }

  @Test void testAcceptsMaterializedViewUrl() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertTrue(driver.acceptsURL("jdbc:calcite:materialized_view=true"));
  }

  @Test void testAcceptsMaterializedViewMidUrl() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertTrue(
        driver.acceptsURL(
        "jdbc:calcite:data_path=/data;materialized_view=enabled;engine=parquet"));
  }

  @Test void testRejectsPlainCalciteUrl() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertFalse(driver.acceptsURL("jdbc:calcite:"));
  }

  @Test void testRejectsNonCalciteUrl() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306/db"));
  }

  @Test void testRejectsCalciteSchemaOther() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertFalse(driver.acceptsURL("jdbc:calcite:schema=jdbc"));
  }

  @Test void testRejectsCalciteSchemaMemory() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertFalse(driver.acceptsURL("jdbc:calcite:schema=memory"));
  }

  @Test void testConnectReturnsNullForNonMatchingUrl() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    Connection conn = driver.connect("jdbc:mysql://localhost/db", new Properties());
    assertNull(conn);
  }

  @Test void testConnectReturnsNullForNonFileSchema() throws SQLException {
    FileJdbcDriver driver = new FileJdbcDriver();
    Connection conn = driver.connect("jdbc:calcite:schema=memory", new Properties());
    assertNull(conn);
  }

  @Test void testConnectWithFileSchemaUrlAndTempDir() throws Exception {
    Path dataPath = tempDir.resolve("data");
    Files.createDirectories(dataPath);
    Path csvFile = dataPath.resolve("test.csv");
    FileWriter fw = new FileWriter(csvFile.toFile());
    try {
      fw.write("id,name\n1,Alice\n2,Bob\n");
    } finally {
      fw.close();
    }
    Path storagePath = tempDir.resolve("storage");
    Files.createDirectories(storagePath);
    clearInitializedSchemas();
    String url = "jdbc:calcite:;schema=file;data_path=" + dataPath.toAbsolutePath()
        + ";storage_path=" + storagePath.toAbsolutePath() + ";engine=parquet";
    FileJdbcDriver driver = new FileJdbcDriver();
    Connection conn = null;
    try {
      conn = driver.connect(url, new Properties());
      assertNotNull(conn);
      assertFalse(conn.isClosed());
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      assertNotNull(calciteConn);
      assertNotNull(calciteConn.getRootSchema());
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }

  @SuppressWarnings("deprecation")
  @Test void testConnectWithModelJson() throws Exception {
    Path dataPath = tempDir.resolve("modeldata");
    Files.createDirectories(dataPath);
    Path csvFile = dataPath.resolve("people.csv");
    FileWriter csvWriter = new FileWriter(csvFile.toFile());
    try {
      csvWriter.write("id,name\n1,Alice\n");
    } finally {
      csvWriter.close();
    }
    Path modelFile = tempDir.resolve("model.json");
    String dirPath = dataPath.toAbsolutePath().toString().replace("\\", "/");
    String modelJson = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"testschema\",\n"
        + "  \"schemas\": [{\n"
        + "    \"name\": \"testschema\",\n"
        + "    \"type\": \"custom\",\n"
        + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "    \"operand\": { \"directory\": \"" + dirPath + "\", \"ephemeralCache\": true }\n"
        + "  }]\n"
        + "}\n";
    FileWriter modelWriter = new FileWriter(modelFile.toFile());
    try {
      modelWriter.write(modelJson);
    } finally {
      modelWriter.close();
    }
    String modelPath = modelFile.toAbsolutePath().toString().replace("\\", "/");
    Connection conn = null;
    try {
      conn = DriverManager.getConnection("jdbc:calcite:model=" + modelPath);
      assertNotNull(conn);
      CalciteConnection cc = conn.unwrap(CalciteConnection.class);
      assertNotNull(cc.getRootSchema().getSubSchema("testschema"));
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }

  @SuppressWarnings("deprecation")
  @Test void testSetupFileSchemasViaReflection() throws Exception {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection("jdbc:calcite:");
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      FileJdbcDriver driver = new FileJdbcDriver();
      Path dataPath = tempDir.resolve("setup_data");
      Files.createDirectories(dataPath);
      Path storagePath = tempDir.resolve("setup_storage");
      Files.createDirectories(storagePath);
      clearInitializedSchemas();
      String testUrl = "jdbc:calcite:;schema=file;data_path=" + dataPath.toAbsolutePath()
          + ";storage_path=" + storagePath.toAbsolutePath() + ";engine=parquet";
      Method setupMethod =
          FileJdbcDriver.class.getDeclaredMethod("setupFileSchemas", CalciteConnection.class, String.class, Properties.class);
      setupMethod.setAccessible(true);
      setupMethod.invoke(driver, calciteConn, testUrl, new Properties());
      assertNotNull(calciteConn.getRootSchema().getSubSchema("files"));
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }

  @SuppressWarnings("deprecation")
  @Test void testSetupSchemaSkipsDuplicate() throws Exception {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection("jdbc:calcite:");
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      FileJdbcDriver driver = new FileJdbcDriver();
      Path dataPath = tempDir.resolve("dup_data");
      Files.createDirectories(dataPath);
      Path storagePath = tempDir.resolve("dup_storage");
      Files.createDirectories(storagePath);
      clearInitializedSchemas();
      Object schemaConfig =
          createSchemaConfig("duptest", dataPath.toAbsolutePath().toString(),
          storagePath.toAbsolutePath().toString(), "parquet", 2048);
      Method setupSchemaMethod =
          FileJdbcDriver.class.getDeclaredMethod("setupSchema", SchemaPlus.class, schemaConfig.getClass());
      setupSchemaMethod.setAccessible(true);
      SchemaPlus rootSchema = calciteConn.getRootSchema();
      setupSchemaMethod.invoke(driver, rootSchema, schemaConfig);
      assertNotNull(rootSchema.getSubSchema("duptest"));
      // Second call exercises duplicate-skip path
      setupSchemaMethod.invoke(driver, rootSchema, schemaConfig);
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }

  @Test void testCreateDriverVersion() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Method method = FileJdbcDriver.class.getDeclaredMethod("createDriverVersion");
    method.setAccessible(true);
    DriverVersion version = (DriverVersion) method.invoke(driver);
    assertNotNull(version);
    assertNotNull(version.toString());
  }

  @Test void testParseConfigurationFileSchemaViaSemicolon() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;data_path=/mydata;storage_path=/mystorage;engine=arrow",
        new Properties());
    assertNotNull(config);
    assertEquals(1, invokeGetSchemas(config).size());
  }

  @Test void testParseConfigurationDefaultPathsNoSchemaMatch() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Object config = invokeParseConfiguration(driver, "jdbc:calcite:schema=file", new Properties());
    assertNotNull(config);
    assertEquals(0, invokeGetSchemas(config).size());
  }

  @Test void testParseConfigurationBatchSize() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;batch_size=4096;data_path=/d;storage_path=/s",
        new Properties());
    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());
    assertEquals(4096, invokeGetter(schemas.get(0), "getBatchSize"));
  }

  @Test void testParseConfigurationInvalidBatchSizeUsesDefault() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;batch_size=invalid;data_path=/d;storage_path=/s",
        new Properties());
    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());
    assertEquals(2048, invokeGetter(schemas.get(0), "getBatchSize"));
  }

  @Test void testParseConfigurationPropertyOverrides() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("data_path", "/override/data");
    info.setProperty("storage_path", "/override/storage");
    info.setProperty("engine", "vectorized");
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;data_path=/original;storage_path=/original;engine=parquet",
        info);
    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());
    assertEquals("/override/data", invokeGetter(schemas.get(0), "getDataPath"));
    assertEquals("/override/storage", invokeGetter(schemas.get(0), "getStoragePath"));
    assertEquals("vectorized", invokeGetter(schemas.get(0), "getEngineType"));
  }

  @Test void testParseConfigurationLegacyProperties() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("materialized_view.storage_path", "/legacy/storage");
    info.setProperty("materialized_view.engine", "linq4j");
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;data_path=/d;storage_path=/s;engine=parquet", info);
    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());
    assertEquals("/legacy/storage", invokeGetter(schemas.get(0), "getStoragePath"));
    assertEquals("linq4j", invokeGetter(schemas.get(0), "getEngineType"));
  }

  @Test void testParseConfigurationDefaultStoragePath() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file", new Properties());
    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());
    assertEquals(System.getProperty("java.io.tmpdir") + "/calcite_file_storage",
        invokeGetter(schemas.get(0), "getStoragePath"));
  }

  @Test void testParseConfigurationDefaultDataPath() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file", new Properties());
    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());
    assertEquals(System.getProperty("user.dir") + "/data",
        invokeGetter(schemas.get(0), "getDataPath"));
  }

  @Test void testParseConfigurationNoFileSchema() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=other", new Properties());
    assertEquals(0, invokeGetSchemas(config).size());
  }

  @Test void testParseConfigurationSchemaNameIsFiles() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;data_path=/d;storage_path=/s", new Properties());
    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());
    assertEquals("files", invokeGetter(schemas.get(0), "getName"));
  }

  @Test void testAddConfiguredSchemasFromProperties() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.sales.data_path", "/data/sales");
    info.setProperty("schema.sales.storage_path", "/storage/sales");
    info.setProperty("schema.sales.engine", "arrow");
    info.setProperty("schema.sales.batch_size", "1024");
    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);
    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());
    assertEquals("sales", invokeGetter(schemas.get(0), "getName"));
  }

  @Test void testAddConfiguredSchemasMultiple() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.sales.data_path", "/data/sales");
    info.setProperty("schema.sales.storage_path", "/storage/sales");
    info.setProperty("schema.inventory.data_path", "/data/inv");
    info.setProperty("schema.inventory.storage_path", "/storage/inv");
    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);
    assertEquals(2, invokeGetSchemas(config).size());
  }

  @Test void testAddConfiguredSchemasInvalidBatchSize() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.bad.data_path", "/data/bad");
    info.setProperty("schema.bad.storage_path", "/storage/bad");
    info.setProperty("schema.bad.batch_size", "not-a-number");
    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);
    assertEquals(0, invokeGetSchemas(config).size());
  }

  @Test void testAddConfiguredSchemasIncompleteConfig() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.partial.data_path", "/data/partial");
    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);
    assertEquals(0, invokeGetSchemas(config).size());
  }

  @Test void testUrlSchemaFileAndPropertySchemasCombined() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("schema.extra.data_path", "/data/extra");
    info.setProperty("schema.extra.storage_path", "/storage/extra");
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;data_path=/url/data;storage_path=/url/storage", info);
    assertEquals(2, invokeGetSchemas(config).size());
  }

  @Test void testSchemaConfigFields() throws Exception {
    Object sc = createSchemaConfig("mySchema", "/data", "/storage", "parquet", 2048);
    assertNotNull(sc);
    assertEquals("mySchema", invokeGetter(sc, "getName"));
    assertEquals("/data", invokeGetter(sc, "getDataPath"));
    assertEquals("/storage", invokeGetter(sc, "getStoragePath"));
    assertEquals("parquet", invokeGetter(sc, "getEngineType"));
    assertEquals(2048, invokeGetter(sc, "getBatchSize"));
  }

  @Test void testFileAdapterConfigAddAndGetSchemas() throws Exception {
    Class<?>[] innerClasses = FileJdbcDriver.class.getDeclaredClasses();
    Class<?> configClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("FileAdapterConfig")) {
        configClass = c;
        break;
      }
    }
    assertNotNull(configClass);
    Constructor<?> ctor = configClass.getDeclaredConstructor();
    ctor.setAccessible(true);
    Object configObj = ctor.newInstance();
    Method getSchemasMethod = configClass.getDeclaredMethod("getSchemas");
    getSchemasMethod.setAccessible(true);
    List<?> schemas = (List<?>) getSchemasMethod.invoke(configObj);
    assertEquals(0, schemas.size());
    Object sc = createSchemaConfig("test", "/d", "/s", "parquet", 2048);
    Method addSchemaMethod = configClass.getDeclaredMethod("addSchema", sc.getClass());
    addSchemaMethod.setAccessible(true);
    addSchemaMethod.invoke(configObj, sc);
    schemas = (List<?>) getSchemasMethod.invoke(configObj);
    assertEquals(1, schemas.size());
  }

  @Test void testInitializedSchemasSetBehavior() throws Exception {
    Field field = FileJdbcDriver.class.getDeclaredField("INITIALIZED_SCHEMAS");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<String> initializedSchemas = (Set<String>) field.get(null);
    assertNotNull(initializedSchemas);
    String testKey = "testCoverageSchema:/nonexistent/" + System.nanoTime();
    initializedSchemas.add(testKey);
    assertTrue(initializedSchemas.contains(testKey));
    initializedSchemas.remove(testKey);
    assertFalse(initializedSchemas.contains(testKey));
  }

  @Test void testLegacyMaterializedViewTakesPrecedence() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("storage_path", "/prop/storage");
    info.setProperty("materialized_view.storage_path", "/legacy/storage");
    Object config =
        invokeParseConfiguration(driver, "jdbc:calcite:;schema=file;data_path=/d", info);
    List<?> schemas = invokeGetSchemas(config);
    assertEquals(1, schemas.size());
    assertEquals("/legacy/storage", invokeGetter(schemas.get(0), "getStoragePath"));
  }

  @Test void testNonSchemaDotPropertyIgnored() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    Properties info = new Properties();
    info.setProperty("other.myschema.data_path", "/data/other");
    info.setProperty("other.myschema.storage_path", "/storage/other");
    Object config = invokeParseConfiguration(driver, "jdbc:calcite:", info);
    assertEquals(0, invokeGetSchemas(config).size());
  }

  private Object invokeParseConfiguration(FileJdbcDriver driver, String url, Properties info)
      throws Exception {
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
    assertNotNull(schemaConfigClass);
    Constructor<?> ctor =
        schemaConfigClass.getDeclaredConstructor(String.class, String.class, String.class, String.class, int.class);
    ctor.setAccessible(true);
    return ctor.newInstance(name, dataPath, storagePath, engineType, batchSize);
  }

  private void clearInitializedSchemas() throws Exception {
    Field field = FileJdbcDriver.class.getDeclaredField("INITIALIZED_SCHEMAS");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<String> initializedSchemas = (Set<String>) field.get(null);
    initializedSchemas.clear();
  }
}
