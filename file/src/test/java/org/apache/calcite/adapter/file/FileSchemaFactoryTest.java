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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link FileSchemaFactory}.
 *
 * <p>Tests singleton behavior, create() method with various operand configurations,
 * sanitizeOperand() credential redaction, writeDebugModel() file output,
 * schema creation with different file types, and error handling.
 */
@SuppressWarnings("deprecation")
@Tag("unit")
public class FileSchemaFactoryTest {

  @TempDir
  Path tempDir;

  // ---------------------------------------------------------------------------
  // 1. INSTANCE singleton tests
  // ---------------------------------------------------------------------------

  @Test public void testInstanceIsNotNull() {
    assertNotNull(FileSchemaFactory.INSTANCE,
        "FileSchemaFactory.INSTANCE must not be null");
  }

  @Test public void testInstanceIsSameSingleton() {
    assertSame(FileSchemaFactory.INSTANCE, FileSchemaFactory.INSTANCE,
        "FileSchemaFactory.INSTANCE must return the same object");
  }

  @Test public void testInstanceImplementsSchemaFactory() {
    assertTrue(FileSchemaFactory.INSTANCE instanceof org.apache.calcite.schema.SchemaFactory,
        "FileSchemaFactory.INSTANCE must implement SchemaFactory");
  }

  @Test public void testSupportsConstraints() {
    assertTrue(FileSchemaFactory.INSTANCE.supportsConstraints(),
        "FileSchemaFactory should support constraints");
  }

  @Test public void testRowtimeColumnNameConstant() {
    assertEquals("ROWTIME", FileSchemaFactory.ROWTIME_COLUMN_NAME,
        "ROWTIME_COLUMN_NAME constant must be 'ROWTIME'");
  }

  // ---------------------------------------------------------------------------
  // 2. create() with minimal operand (directory only)
  // ---------------------------------------------------------------------------

  @Test public void testCreateWithMinimalOperand() throws Exception {
    writeCsvFile("products.csv",
        "name:string,price:double\n"
        + "Widget,9.99\n"
        + "Gadget,19.99\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "test", operand);
      assertNotNull(schema, "Schema must not be null");
    }
  }

  @Test public void testCreateReturnsSchemaWithTables() throws Exception {
    writeCsvFile("orders.csv",
        "id:int,total:double\n"
        + "1,100.50\n"
        + "2,200.75\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "mydata", operand);

      Set<String> tableNames = schema.getTableNames();
      assertNotNull(tableNames, "Table names must not be null");
      assertTrue(tableNames.contains("orders"),
          "Schema should discover 'orders' table from orders.csv");
    }
  }

  // ---------------------------------------------------------------------------
  // 3. create() with various operand options
  // ---------------------------------------------------------------------------

  @Test public void testCreateWithEphemeralCacheTrue() throws Exception {
    writeCsvFile("items.csv",
        "name:string,qty:int\n"
        + "bolt,100\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "ephtest", operand);
      assertNotNull(schema);
      assertFalse(schema.getTableNames().isEmpty(),
          "Schema with ephemeralCache=true should still discover tables");
    }
  }

  @Test public void testCreateWithEphemeralCacheAsString() throws Exception {
    writeCsvFile("data.csv",
        "col:string\n"
        + "hello\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      // Environment variable substitution can produce String "true"
      operand.put("ephemeralCache", "true");

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "streph", operand);
      assertNotNull(schema);
    }
  }

  @Test public void testCreateWithSnakeCaseEphemeralCache() throws Exception {
    writeCsvFile("rows.csv",
        "val:int\n"
        + "42\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeral_cache", true);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "snakeeph", operand);
      assertNotNull(schema);
    }
  }

  @Test public void testCreateWithBatchSizeAndMemoryThreshold() throws Exception {
    writeCsvFile("metrics.csv",
        "name:string,value:double\n"
        + "cpu,0.75\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);
      operand.put("batchSize", 4096);
      operand.put("memoryThreshold", 128L * 1024 * 1024);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "batchtest", operand);
      assertNotNull(schema);
    }
  }

  @Test public void testCreateWithTableNameCasing() throws Exception {
    writeCsvFile("MixedCase.csv",
        "A:string\n"
        + "x\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);
      operand.put("tableNameCasing", "SMART_CASING");
      operand.put("columnNameCasing", "SMART_CASING");

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "casetest", operand);
      assertNotNull(schema);
      // SMART_CASING converts MixedCase -> mixed_case
      assertFalse(schema.getTableNames().isEmpty(),
          "Schema with casing config should discover tables");
    }
  }

  @Test public void testCreateWithRecursiveTrue() throws Exception {
    // Create a subdirectory with a CSV file
    File subDir = new File(tempDir.toFile(), "subdir");
    subDir.mkdirs();
    File csvFile = new File(subDir, "nested.csv");
    try (FileWriter writer = new FileWriter(csvFile, StandardCharsets.UTF_8)) {
      writer.write("key:string\n");
      writer.write("abc\n");
    }

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);
      operand.put("recursive", Boolean.TRUE);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "rectest", operand);
      assertNotNull(schema);
    }
  }

  @Test public void testCreateWithFlattenOption() throws Exception {
    writeCsvFile("flat.csv",
        "k:string\n"
        + "v\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);
      operand.put("flatten", Boolean.TRUE);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "flattest", operand);
      assertNotNull(schema);
    }
  }

  @Test public void testCreateWithPrimeCacheFalse() throws Exception {
    writeCsvFile("lazy.csv",
        "x:int\n"
        + "1\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);
      operand.put("primeCache", Boolean.FALSE);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "lazytest", operand);
      assertNotNull(schema);
    }
  }

  @Test public void testCreateWithComment() throws Exception {
    writeCsvFile("commented.csv",
        "a:string\n"
        + "b\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);
      operand.put("comment", "Test schema for unit tests");

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "commenttest", operand);
      assertNotNull(schema);
    }
  }

  @Test public void testCreateWithCanonicalSchemaName() throws Exception {
    writeCsvFile("canon.csv",
        "v:string\n"
        + "z\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);
      operand.put("canonicalSchemaName", "mycanon");

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "MYCANON", operand);
      assertNotNull(schema);
    }
  }

  // ---------------------------------------------------------------------------
  // 4. sanitizeOperand() - verify credential fields are redacted
  // ---------------------------------------------------------------------------

  @Test public void testSanitizeOperandMasksPassword() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "/some/path");
    operand.put("password", "secret123");
    operand.put("dbPassword", "mysecret");

    Map<String, Object> sanitized = invokeSanitizeOperand(operand);

    assertEquals("********", sanitized.get("password"),
        "password field must be masked");
    assertEquals("********", sanitized.get("dbPassword"),
        "dbPassword field must be masked");
    assertEquals("/some/path", sanitized.get("directory"),
        "Non-sensitive fields should be preserved");
  }

  @Test public void testSanitizeOperandMasksSecret() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("secretKey", "abc123");
    operand.put("apiSecret", "def456");

    Map<String, Object> sanitized = invokeSanitizeOperand(operand);

    assertEquals("********", sanitized.get("secretKey"),
        "secretKey field must be masked");
    assertEquals("********", sanitized.get("apiSecret"),
        "apiSecret field must be masked");
  }

  @Test public void testSanitizeOperandHandlesNullValue() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "/path");
    operand.put("nullField", null);

    Map<String, Object> sanitized = invokeSanitizeOperand(operand);

    assertTrue(sanitized.containsKey("nullField"),
        "Null-valued key should be present");
    assertEquals(null, sanitized.get("nullField"),
        "Null value should remain null");
  }

  @Test public void testSanitizeOperandHandlesUnderscorePrefix() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("_storageProvider", new Object());

    Map<String, Object> sanitized = invokeSanitizeOperand(operand);

    // Underscore-prefixed keys should record class name only
    assertEquals("Object", sanitized.get("_storageProvider"),
        "Underscore-prefixed keys should record class SimpleName");
  }

  @Test public void testSanitizeOperandHandlesS3Config() throws Exception {
    Map<String, Object> s3Config = new HashMap<>();
    s3Config.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
    s3Config.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    s3Config.put("bucket", "my-bucket");
    s3Config.put("region", "us-east-1");

    Map<String, Object> operand = new HashMap<>();
    operand.put("s3Config", s3Config);

    Map<String, Object> sanitized = invokeSanitizeOperand(operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) sanitized.get("s3Config");
    assertNotNull(sanitizedS3, "s3Config map must be present");

    // accessKeyId should be partially masked (last 4 chars visible)
    String maskedKey = (String) sanitizedS3.get("accessKeyId");
    assertTrue(maskedKey.startsWith("****"),
        "accessKeyId should start with ****");
    assertTrue(maskedKey.endsWith("MPLE"),
        "accessKeyId should show last 4 chars");

    // secretAccessKey should be fully masked
    assertEquals("********", sanitizedS3.get("secretAccessKey"),
        "secretAccessKey must be fully masked");

    // Non-sensitive keys preserved
    assertEquals("my-bucket", sanitizedS3.get("bucket"),
        "bucket should be preserved");
    assertEquals("us-east-1", sanitizedS3.get("region"),
        "region should be preserved");
  }

  @Test public void testSanitizeOperandHandlesS3ConfigShortAccessKey() throws Exception {
    Map<String, Object> s3Config = new HashMap<>();
    s3Config.put("accessKeyId", "AB");
    s3Config.put("secretAccessKey", "mykey");

    Map<String, Object> operand = new HashMap<>();
    operand.put("s3Config", s3Config);

    Map<String, Object> sanitized = invokeSanitizeOperand(operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) sanitized.get("s3Config");

    // Short accessKeyId (<=4 chars) should be fully masked
    assertEquals("****", sanitizedS3.get("accessKeyId"),
        "Short accessKeyId should be fully masked");
  }

  @Test public void testSanitizeOperandHandlesStorageConfig() throws Exception {
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
    storageConfig.put("secretAccessKey", "secretKey");
    storageConfig.put("password", "pass");
    storageConfig.put("bucket", "my-bucket");
    storageConfig.put("_storageProvider", new Object());

    Map<String, Object> operand = new HashMap<>();
    operand.put("storageConfig", storageConfig);

    Map<String, Object> sanitized = invokeSanitizeOperand(operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedStorage =
        (Map<String, Object>) sanitized.get("storageConfig");
    assertNotNull(sanitizedStorage, "storageConfig map must be present");

    // accesskey, secret, password should be masked
    assertEquals("********", sanitizedStorage.get("accessKeyId"),
        "accessKeyId in storageConfig must be masked");
    assertEquals("********", sanitizedStorage.get("secretAccessKey"),
        "secretAccessKey in storageConfig must be masked");
    assertEquals("********", sanitizedStorage.get("password"),
        "password in storageConfig must be masked");

    // Non-sensitive preserved
    assertEquals("my-bucket", sanitizedStorage.get("bucket"),
        "bucket should be preserved");

    // Underscore-prefixed records class name
    assertEquals("Object", sanitizedStorage.get("_storageProvider"),
        "Underscore-prefixed keys in storageConfig should record class name");
  }

  @Test public void testSanitizeOperandHandlesModelUri() throws Exception {
    String modelUri = "inline:{\"schemas\":[{\"operand\":{\"accessKeyId\":\"AKIA1234\","
        + "\"secretAccessKey\":\"mySecretKey\"}}]}";

    Map<String, Object> operand = new HashMap<>();
    operand.put("modelUri", modelUri);

    Map<String, Object> sanitized = invokeSanitizeOperand(operand);

    String sanitizedUri = (String) sanitized.get("modelUri");
    assertFalse(sanitizedUri.contains("AKIA1234"),
        "modelUri should not contain original accessKeyId");
    assertFalse(sanitizedUri.contains("mySecretKey"),
        "modelUri should not contain original secretAccessKey");
    assertTrue(sanitizedUri.contains("\"accessKeyId\": \"****\""),
        "modelUri should contain masked accessKeyId");
    assertTrue(sanitizedUri.contains("\"secretAccessKey\": \"********\""),
        "modelUri should contain masked secretAccessKey");
  }

  @Test public void testSanitizeOperandHandlesNestedMap() throws Exception {
    Map<String, Object> nested = new HashMap<>();
    nested.put("someSecret", "hidden");
    nested.put("password", "hidden2");
    nested.put("normalKey", "visible");

    Map<String, Object> operand = new HashMap<>();
    operand.put("customConfig", nested);

    Map<String, Object> sanitized = invokeSanitizeOperand(operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedNested =
        (Map<String, Object>) sanitized.get("customConfig");
    assertNotNull(sanitizedNested);

    assertEquals("********", sanitizedNested.get("someSecret"),
        "Nested secret keys should be masked");
    assertEquals("********", sanitizedNested.get("password"),
        "Nested password keys should be masked");
    assertEquals("visible", sanitizedNested.get("normalKey"),
        "Nested normal keys should be preserved");
  }

  @Test public void testSanitizeOperandPassesThroughNonSensitiveValues() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "/data/files");
    operand.put("ephemeralCache", true);
    operand.put("batchSize", 2048);
    operand.put("recursive", false);

    Map<String, Object> sanitized = invokeSanitizeOperand(operand);

    assertEquals("/data/files", sanitized.get("directory"));
    assertEquals(true, sanitized.get("ephemeralCache"));
    assertEquals(2048, sanitized.get("batchSize"));
    assertEquals(false, sanitized.get("recursive"));
  }

  // ---------------------------------------------------------------------------
  // 5. writeDebugModel() - verify it writes to .aperio directory
  // ---------------------------------------------------------------------------

  @Test public void testWriteDebugModelCreatesFile() throws Exception {
    // Save original user.dir and set to tempDir
    String originalUserDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", tempDir.toString());

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", "/some/path");
      operand.put("ephemeralCache", true);

      invokeWriteDebugModel("testschema", operand, "root");

      File aperioDir = new File(tempDir.toFile(), ".aperio");
      assertTrue(aperioDir.exists(), ".aperio directory must be created");

      File debugFile = new File(aperioDir, "debug-model-testschema.json");
      assertTrue(debugFile.exists(), "debug-model-testschema.json must be created");
      assertTrue(debugFile.length() > 0, "Debug model file must not be empty");

      // Verify the JSON content is parseable and contains expected keys
      ObjectMapper mapper = new ObjectMapper();
      @SuppressWarnings("unchecked")
      Map<String, Object> debugModel = mapper.readValue(debugFile, Map.class);
      assertEquals("testschema", debugModel.get("schemaName"));
      assertEquals("root", debugModel.get("parentSchema"));
      assertNotNull(debugModel.get("generatedAt"),
          "generatedAt timestamp must be present");
      assertNotNull(debugModel.get("operands"),
          "operands must be present");
    } finally {
      System.setProperty("user.dir", originalUserDir);
    }
  }

  @Test public void testWriteDebugModelSanitizesCredentials() throws Exception {
    String originalUserDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", tempDir.toString());

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", "/data");
      operand.put("password", "supersecret");
      operand.put("secretKey", "topsecret");

      invokeWriteDebugModel("sanitized", operand, "parent");

      File debugFile = new File(tempDir.toFile(), ".aperio/debug-model-sanitized.json");
      assertTrue(debugFile.exists(), "Debug model file must be created");

      ObjectMapper mapper = new ObjectMapper();
      @SuppressWarnings("unchecked")
      Map<String, Object> debugModel = mapper.readValue(debugFile, Map.class);
      @SuppressWarnings("unchecked")
      Map<String, Object> operands = (Map<String, Object>) debugModel.get("operands");

      assertEquals("********", operands.get("password"),
          "password must be sanitized in debug model");
      assertEquals("********", operands.get("secretKey"),
          "secretKey must be sanitized in debug model");
      assertEquals("/data", operands.get("directory"),
          "directory should be preserved in debug model");
    } finally {
      System.setProperty("user.dir", originalUserDir);
    }
  }

  @Test public void testWriteDebugModelHandlesNullParentSchema() throws Exception {
    String originalUserDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", tempDir.toString());

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", "/path");

      invokeWriteDebugModel("nullparent", operand, null);

      File debugFile = new File(tempDir.toFile(), ".aperio/debug-model-nullparent.json");
      assertTrue(debugFile.exists(), "Debug file must be created even with null parent");

      ObjectMapper mapper = new ObjectMapper();
      @SuppressWarnings("unchecked")
      Map<String, Object> debugModel = mapper.readValue(debugFile, Map.class);
      assertEquals("", debugModel.get("parentSchema"),
          "Null parent should be stored as empty string");
    } finally {
      System.setProperty("user.dir", originalUserDir);
    }
  }

  @Test public void testWriteDebugModelLowercasesSchemaName() throws Exception {
    String originalUserDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", tempDir.toString());

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", "/path");

      invokeWriteDebugModel("MySchema", operand, "root");

      // File name should be lowercased
      File debugFile = new File(tempDir.toFile(), ".aperio/debug-model-myschema.json");
      assertTrue(debugFile.exists(),
          "Debug model file name should use lowercase schema name");
    } finally {
      System.setProperty("user.dir", originalUserDir);
    }
  }

  // ---------------------------------------------------------------------------
  // 6. Schema creation with different file types in directory
  // ---------------------------------------------------------------------------

  @Test public void testCreateWithCsvFileDiscovery() throws Exception {
    writeCsvFile("employees.csv",
        "name:string,department:string,salary:double\n"
        + "Alice,Engineering,120000\n"
        + "Bob,Marketing,95000\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "csvtest", operand);

      assertTrue(schema.getTableNames().contains("employees"),
          "Schema should discover 'employees' table from CSV");
    }
  }

  @Test public void testCreateWithJsonFile() throws Exception {
    writeJsonFile("config.json",
        "[{\"key\":\"db_host\",\"value\":\"localhost\"},{\"key\":\"db_port\",\"value\":\"5432\"}]");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "jsontest", operand);

      Set<String> tableNames = schema.getTableNames();
      assertNotNull(tableNames);
      // JSON files should be discovered as tables
      assertTrue(tableNames.contains("config"),
          "Schema should discover 'config' table from JSON file");
    }
  }

  @Test public void testCreateWithMultipleFileTypes() throws Exception {
    writeCsvFile("sales.csv",
        "product:string,amount:double\n"
        + "Widget,500\n");
    writeJsonFile("inventory.json",
        "[{\"item\":\"Widget\",\"count\":100}]");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "multitype", operand);

      Set<String> tableNames = schema.getTableNames();
      assertTrue(tableNames.contains("sales"),
          "Schema should discover 'sales' from CSV");
      assertTrue(tableNames.contains("inventory"),
          "Schema should discover 'inventory' from JSON");
    }
  }

  @Test public void testCreateWithMultipleCsvFiles() throws Exception {
    writeCsvFile("customers.csv",
        "id:int,name:string\n"
        + "1,Alice\n"
        + "2,Bob\n");
    writeCsvFile("orders.csv",
        "id:int,customer_id:int,total:double\n"
        + "100,1,50.00\n"
        + "101,2,75.50\n");
    writeCsvFile("products.csv",
        "id:int,name:string,price:double\n"
        + "10,Widget,9.99\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "multicsv", operand);

      Set<String> tableNames = schema.getTableNames();
      assertTrue(tableNames.contains("customers"),
          "Should discover 'customers' table");
      assertTrue(tableNames.contains("orders"),
          "Should discover 'orders' table");
      assertTrue(tableNames.contains("products"),
          "Should discover 'products' table");
      assertTrue(tableNames.size() >= 3,
          "Should have at least 3 tables");
    }
  }

  @Test public void testCreateWithEmptyDirectory() throws Exception {
    // No files in tempDir

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "emptydir", operand);

      assertNotNull(schema, "Schema should be created even for empty directory");
      assertTrue(schema.getTableNames().isEmpty(),
          "Empty directory should produce no tables");
    }
  }

  // ---------------------------------------------------------------------------
  // 7. Error handling tests
  // ---------------------------------------------------------------------------

  @Test public void testCreateWithMissingDirectoryThrowsForS3WithoutCredentials() throws Exception {
    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", "s3://my-bucket/data");
      operand.put("storageType", "s3");
      // No credentials provided

      assertThrows(IllegalArgumentException.class, () ->
              FileSchemaFactory.INSTANCE.create(rootSchema, "s3noauth", operand),
          "S3 storage without credentials should throw IllegalArgumentException");
    }
  }

  @Test public void testCreateWithNullStorageTypeAndNoDirectoryThrows() throws Exception {
    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      // No directory, no storageType, no tables
      operand.put("ephemeralCache", true);

      assertThrows(IllegalStateException.class, () ->
              FileSchemaFactory.INSTANCE.create(rootSchema, "nostorage", operand),
          "Missing storageType should throw IllegalStateException");
    }
  }

  @Test public void testCreateWithDuplicateSchemaNameThrows() throws Exception {
    writeCsvFile("dup.csv",
        "x:int\n"
        + "1\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);

      // Create first schema
      FileSchemaFactory.INSTANCE.create(rootSchema, "dupschema", operand);

      // Attempt to create second schema with same name should fail
      assertThrows(IllegalArgumentException.class, () ->
              FileSchemaFactory.INSTANCE.create(rootSchema, "dupschema", operand),
          "Duplicate schema name should throw IllegalArgumentException");
    }
  }

  @Test public void testAutoDetectStorageTypeS3() throws Exception {
    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", "s3://bucket/path");
      // No explicit storageType - should auto-detect
      Map<String, Object> storageConfig = new HashMap<>();
      storageConfig.put("accessKeyId", "AKIA1234");
      storageConfig.put("secretAccessKey", "mysecret");
      operand.put("storageConfig", storageConfig);
      operand.put("ephemeralCache", true);

      // This will attempt to create the schema with auto-detected S3 storage
      // It may fail for other reasons (no actual S3 bucket), but should not
      // fail due to storageType detection
      try {
        FileSchemaFactory.INSTANCE.create(rootSchema, "s3auto", operand);
      } catch (Exception e) {
        // Expected - we don't have a real S3 bucket
        // But verify it did not throw IllegalStateException for missing storageType
        assertFalse(e instanceof IllegalStateException
                && e.getMessage() != null
                && e.getMessage().contains("storageType must be configured"),
            "S3 auto-detection should work, should not fail on missing storageType");
      }
    }
  }

  @Test public void testAutoDetectStorageTypeHdfs() throws Exception {
    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", "hdfs://namenode:8020/data");
      operand.put("ephemeralCache", true);

      // Should auto-detect HDFS storage type; actual creation may fail
      // but storageType should not be null
      try {
        FileSchemaFactory.INSTANCE.create(rootSchema, "hdfsauto", operand);
      } catch (Exception e) {
        assertFalse(e instanceof IllegalStateException
                && e.getMessage() != null
                && e.getMessage().contains("storageType must be configured"),
            "HDFS auto-detection should set storageType");
      }
    }
  }

  @Test public void testAutoDetectStorageTypeHttp() throws Exception {
    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", "https://example.com/data");
      operand.put("ephemeralCache", true);

      try {
        FileSchemaFactory.INSTANCE.create(rootSchema, "httpauto", operand);
      } catch (Exception e) {
        assertFalse(e instanceof IllegalStateException
                && e.getMessage() != null
                && e.getMessage().contains("storageType must be configured"),
            "HTTP auto-detection should set storageType");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 8. setTableConstraints() tests
  // ---------------------------------------------------------------------------

  @Test public void testSetTableConstraints() {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tableConstraint = new HashMap<>();
    tableConstraint.put("primaryKey", "id");
    constraints.put("orders", tableConstraint);

    // Should not throw
    FileSchemaFactory.INSTANCE.setTableConstraints(constraints, null);
  }

  @Test public void testSetTableConstraintsWithNull() {
    // Should not throw
    FileSchemaFactory.INSTANCE.setTableConstraints(null, null);
  }

  // ---------------------------------------------------------------------------
  // 9. parseBooleanValue tests (via reflection)
  // ---------------------------------------------------------------------------

  @Test public void testParseBooleanValueWithBooleanTrue() throws Exception {
    assertEquals(Boolean.TRUE, invokeParseBooleanValue(Boolean.TRUE));
  }

  @Test public void testParseBooleanValueWithBooleanFalse() throws Exception {
    assertEquals(Boolean.FALSE, invokeParseBooleanValue(Boolean.FALSE));
  }

  @Test public void testParseBooleanValueWithStringTrue() throws Exception {
    assertEquals(Boolean.TRUE, invokeParseBooleanValue("true"));
  }

  @Test public void testParseBooleanValueWithStringFalse() throws Exception {
    assertEquals(Boolean.FALSE, invokeParseBooleanValue("false"));
  }

  @Test public void testParseBooleanValueWithNull() throws Exception {
    assertEquals(null, invokeParseBooleanValue(null));
  }

  @Test public void testParseBooleanValueWithInteger() throws Exception {
    // Non-Boolean, non-String should return null
    assertEquals(null, invokeParseBooleanValue(42));
  }

  // ---------------------------------------------------------------------------
  // 10. SourceDirectory and directory alias tests
  // ---------------------------------------------------------------------------

  @Test public void testCreateWithSourceDirectoryAlias() throws Exception {
    writeCsvFile("alias.csv",
        "col:string\n"
        + "val\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      // Use "sourceDirectory" instead of "directory"
      operand.put("sourceDirectory", tempDir.toString());
      operand.put("ephemeralCache", true);

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "srcdir", operand);
      assertNotNull(schema);
      assertTrue(schema.getTableNames().contains("alias"),
          "sourceDirectory alias should work same as directory");
    }
  }

  @Test public void testCreateWithBaseDirectory() throws Exception {
    writeCsvFile("base.csv",
        "k:int\n"
        + "1\n");

    Path baseDir = tempDir.resolve("mybase");
    baseDir.toFile().mkdirs();

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("baseDirectory", baseDir.toString());

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "basedir", operand);
      assertNotNull(schema);
    }
  }

  // ---------------------------------------------------------------------------
  // 11. Execution engine configuration via operand
  // ---------------------------------------------------------------------------

  @Test public void testCreateWithExplicitParquetEngine() throws Exception {
    writeCsvFile("eng.csv",
        "v:string\n"
        + "test\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);
      operand.put("executionEngine", "PARQUET");

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "parqeng", operand);
      assertNotNull(schema);
      assertTrue(schema instanceof FileSchema,
          "PARQUET engine should produce a FileSchema");
    }
  }

  @Test public void testCreateWithDirectoryPattern() throws Exception {
    writeCsvFile("log_2024.csv",
        "msg:string\n"
        + "hello\n");
    writeCsvFile("log_2025.csv",
        "msg:string\n"
        + "world\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);
      operand.put("directoryPattern", "log_*.csv");

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "pattest", operand);
      assertNotNull(schema);
    }
  }

  @Test public void testCreateWithGlobAlias() throws Exception {
    writeCsvFile("report_jan.csv",
        "month:string\n"
        + "January\n");

    try (Connection conn = createCalciteConnection();
         CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class)) {
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", tempDir.toString());
      operand.put("ephemeralCache", true);
      operand.put("glob", "report_*.csv");

      Schema schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "globtest", operand);
      assertNotNull(schema);
    }
  }

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  /**
   * Creates a Calcite connection with ORACLE lex and TO_LOWER unquoted casing.
   */
  private static Connection createCalciteConnection() throws Exception {
    Properties info = new Properties();
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    return DriverManager.getConnection("jdbc:calcite:", info);
  }

  /**
   * Writes a CSV file with the given content into tempDir.
   */
  private void writeCsvFile(String fileName, String content) throws Exception {
    File file = new File(tempDir.toFile(), fileName);
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }

  /**
   * Writes a JSON file with the given content into tempDir.
   */
  private void writeJsonFile(String fileName, String content) throws Exception {
    File file = new File(tempDir.toFile(), fileName);
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }

  /**
   * Invokes the private static {@code sanitizeOperand} method via reflection.
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> invokeSanitizeOperand(
      Map<String, Object> operand) throws Exception {
    Method method =
        FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);
    return (Map<String, Object>) method.invoke(null, operand);
  }

  /**
   * Invokes the private static {@code writeDebugModel} method via reflection.
   */
  private static void invokeWriteDebugModel(String schemaName,
      Map<String, Object> operand, String parentSchemaName) throws Exception {
    Method method =
        FileSchemaFactory.class.getDeclaredMethod("writeDebugModel", String.class, Map.class, String.class);
    method.setAccessible(true);
    method.invoke(null, schemaName, operand, parentSchemaName);
  }

  /**
   * Invokes the private static {@code parseBooleanValue} method via reflection.
   */
  private static Boolean invokeParseBooleanValue(Object value) throws Exception {
    Method method =
        FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, value);
  }
}
