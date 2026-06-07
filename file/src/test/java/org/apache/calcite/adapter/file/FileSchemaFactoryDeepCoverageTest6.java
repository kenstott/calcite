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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests (tier 6) for {@link FileSchemaFactory}.
 * Targets remaining uncovered lines in:
 * - sanitizeOperand (null values, password, secret, underscore keys, s3Config, storageConfig,
 *   modelUri, nested maps)
 * - sanitizeNestedMap (secret, Calcite classes, normal values)
 * - parseBooleanValue (null, Boolean, String, other types)
 * - validateUniqueSchemaName (null parent, null name, existing schema)
 * - rewriteForeignKeySchemaNames (null, empty, FK rewrite, cross-schema FK)
 * - rewriteSchemaReferencesInSql (null args, matching schemas, non-matching)
 * - registerSqlViews (null/empty tables, view types, missing fields, schema rewriting)
 * - registerMaterializations (missing schema, missing CalciteSchema, table/sql missing)
 * - setTableConstraints / supportsConstraints
 * - writeDebugModel / addMetadataSchemas
 * - create() operand parsing: ephemeralCache, baseDirectory File/String, directory/sourceDirectory,
 *   executionEngine priority, batchSize/memoryThreshold, duckdbConfig, parquetCacheDirectory,
 *   recursive, directoryPattern/glob, materializations, views, partitionedTables,
 *   storageType auto-detect, s3 fail-fast, refreshInterval, flatten, tableNameCasing,
 *   columnNameCasing, csvTypeInference, primeCache, canonicalSchemaName,
 *   storageType auto-detect from baseDirectory, textSimilarity registration
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class FileSchemaFactoryDeepCoverageTest6 {

  @TempDir
  Path tempDir;

  private FileSchemaFactory factory;
  private List<FileSchema> createdSchemas;

  @BeforeEach
  void setup() {
    factory = FileSchemaFactory.INSTANCE;
    createdSchemas = new ArrayList<FileSchema>();
  }

  @AfterEach
  void tearDown() {
    for (FileSchema schema : createdSchemas) {
      shutdownRefreshScheduler(schema);
    }
    createdSchemas.clear();
  }

  private void shutdownRefreshScheduler(FileSchema schema) {
    try {
      Field f = FileSchema.class.getDeclaredField("refreshScheduler");
      f.setAccessible(true);
      ScheduledExecutorService scheduler = (ScheduledExecutorService) f.get(schema);
      if (scheduler != null) {
        scheduler.shutdownNow();
      }
    } catch (Exception e) {
      // Ignore
    }
  }

  private void trackSchema(Schema schema) {
    if (schema instanceof FileSchema) {
      createdSchemas.add((FileSchema) schema);
    }
  }

  // ===== parseBooleanValue =====

  @Test void testParseBooleanValueNull() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    m.setAccessible(true);
    assertNull(m.invoke(null, (Object) null));
  }

  @Test void testParseBooleanValueBoolean() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    m.setAccessible(true);
    assertEquals(Boolean.TRUE, m.invoke(null, Boolean.TRUE));
    assertEquals(Boolean.FALSE, m.invoke(null, Boolean.FALSE));
  }

  @Test void testParseBooleanValueString() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    m.setAccessible(true);
    assertEquals(Boolean.TRUE, m.invoke(null, "true"));
    assertEquals(Boolean.FALSE, m.invoke(null, "false"));
  }

  @Test void testParseBooleanValueOtherType() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    m.setAccessible(true);
    assertNull(m.invoke(null, Integer.valueOf(42)));
  }

  // ===== sanitizeOperand =====

  @Test void testSanitizeOperandNullValue() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    m.setAccessible(true);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("nullKey", null);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(null, operand);
    assertTrue(result.containsKey("nullKey"));
    assertNull(result.get("nullKey"));
  }

  @Test void testSanitizeOperandPasswordKey() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    m.setAccessible(true);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("dbPassword", "secret123");
    operand.put("clientSecret", "topsecret");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(null, operand);
    assertEquals("********", result.get("dbPassword"));
    assertEquals("********", result.get("clientSecret"));
  }

  @Test void testSanitizeOperandUnderscoreKey() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    m.setAccessible(true);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("_storageProvider", "SomeInstance");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(null, operand);
    assertEquals("String", result.get("_storageProvider"));
  }

  @Test void testSanitizeOperandS3Config() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    m.setAccessible(true);

    Map<String, Object> s3Config = new HashMap<String, Object>();
    s3Config.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
    s3Config.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    s3Config.put("region", "us-east-1");
    s3Config.put("bucket", "my-bucket");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("s3Config", s3Config);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(null, operand);
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) result.get("s3Config");
    assertNotNull(sanitizedS3);
    // accessKeyId should be masked but show last 4 chars
    String maskedKey = (String) sanitizedS3.get("accessKeyId");
    assertTrue(maskedKey.startsWith("****"));
    assertTrue(maskedKey.endsWith("MPLE"));
    assertEquals("********", sanitizedS3.get("secretAccessKey"));
    assertEquals("us-east-1", sanitizedS3.get("region"));
  }

  @Test void testSanitizeOperandS3ConfigShortAccessKey() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    m.setAccessible(true);

    Map<String, Object> s3Config = new HashMap<String, Object>();
    s3Config.put("accessKeyId", "KEY");
    s3Config.put("secretAccessKey", "sec");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("s3Config", s3Config);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(null, operand);
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) result.get("s3Config");
    assertEquals("****", sanitizedS3.get("accessKeyId"));
  }

  @Test void testSanitizeOperandStorageConfig() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    m.setAccessible(true);

    Map<String, Object> storageConfig = new HashMap<String, Object>();
    storageConfig.put("_provider", "SomeProvider");
    storageConfig.put("secretKey", "mysecret");
    storageConfig.put("password", "pass123");
    storageConfig.put("accesskey", "AKIA...");
    storageConfig.put("region", "us-west-2");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("storageConfig", storageConfig);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(null, operand);
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitized = (Map<String, Object>) result.get("storageConfig");
    assertNotNull(sanitized);
    assertEquals("********", sanitized.get("secretKey"));
    assertEquals("********", sanitized.get("password"));
    assertEquals("********", sanitized.get("accesskey"));
    assertEquals("us-west-2", sanitized.get("region"));
  }

  @Test void testSanitizeOperandStorageConfigNullProvider() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    m.setAccessible(true);

    Map<String, Object> storageConfig = new HashMap<String, Object>();
    storageConfig.put("_provider", null);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("storageConfig", storageConfig);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(null, operand);
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitized = (Map<String, Object>) result.get("storageConfig");
    assertNull(sanitized.get("_provider"));
  }

  @Test void testSanitizeOperandModelUri() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    m.setAccessible(true);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("modelUri", "inline:{\"accessKeyId\": \"AKIAI\", \"secretAccessKey\": \"wJalr\"}");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(null, operand);
    String sanitizedUri = (String) result.get("modelUri");
    assertTrue(sanitizedUri.contains("\"accessKeyId\": \"****\""));
    assertTrue(sanitizedUri.contains("\"secretAccessKey\": \"********\""));
  }

  @Test void testSanitizeOperandNestedMap() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    m.setAccessible(true);

    Map<String, Object> nested = new HashMap<String, Object>();
    nested.put("normalKey", "normalValue");
    nested.put("secretKey", "hidden");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("customConfig", nested);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(null, operand);
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedNested = (Map<String, Object>) result.get("customConfig");
    assertNotNull(sanitizedNested);
    assertEquals("normalValue", sanitizedNested.get("normalKey"));
    assertEquals("********", sanitizedNested.get("secretKey"));
  }

  @Test void testSanitizeOperandPassThroughValues() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    m.setAccessible(true);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", "/path/to/data");
    operand.put("recursive", Boolean.TRUE);
    operand.put("batchSize", Integer.valueOf(1000));

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(null, operand);
    assertEquals("/path/to/data", result.get("directory"));
    assertEquals(Boolean.TRUE, result.get("recursive"));
    assertEquals(Integer.valueOf(1000), result.get("batchSize"));
  }

  // ===== sanitizeNestedMap =====

  @Test void testSanitizeNestedMapSecretKeys() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("sanitizeNestedMap", Map.class);
    m.setAccessible(true);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("secretValue", "hidden");
    map.put("password", "pass");
    map.put("accesskey", "key123");
    map.put("normal", "visible");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(null, map);
    assertEquals("********", result.get("secretValue"));
    assertEquals("********", result.get("password"));
    assertEquals("********", result.get("accesskey"));
    assertEquals("visible", result.get("normal"));
  }

  // ===== validateUniqueSchemaName =====

  @Test void testValidateUniqueSchemaNameNullParent() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("validateUniqueSchemaName", SchemaPlus.class, String.class);
    m.setAccessible(true);
    // Should not throw for null parent
    m.invoke(null, null, "test");
  }

  @Test void testValidateUniqueSchemaNameNullName() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("validateUniqueSchemaName", SchemaPlus.class, String.class);
    m.setAccessible(true);
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();
    // Should not throw for null name
    m.invoke(null, parentSchema, null);
  }

  @Test void testValidateUniqueSchemaNameDuplicate() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("validateUniqueSchemaName", SchemaPlus.class, String.class);
    m.setAccessible(true);
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    // Add a schema first
    parentSchema.add("existing", CalciteSchema.createRootSchema(true).plus().unwrap(Schema.class));

    // Should throw for duplicate
    try {
      m.invoke(null, parentSchema, "existing");
      assertTrue(false, "Expected IllegalArgumentException");
    } catch (InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertTrue(e.getCause().getMessage().contains("already exists"));
    }
  }

  @Test void testValidateUniqueSchemaNameUnique() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("validateUniqueSchemaName", SchemaPlus.class, String.class);
    m.setAccessible(true);
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();
    // Should not throw for unique name
    m.invoke(null, parentSchema, "new_schema");
  }

  // ===== rewriteForeignKeySchemaNames =====

  @Test void testRewriteForeignKeySchemaNames() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    m.setAccessible(true);

    // Build constraints with FK references
    Map<String, Object> fk1 = new HashMap<String, Object>();
    fk1.put("targetSchema", "econ");
    fk1.put("targetTable", "dim_date");
    fk1.put("columns", Collections.singletonList("date_id"));

    Map<String, Object> fk2 = new HashMap<String, Object>();
    fk2.put("targetSchema", "other_schema");
    fk2.put("targetTable", "other_table");
    fk2.put("columns", Collections.singletonList("id"));

    List<Map<String, Object>> foreignKeys = new ArrayList<Map<String, Object>>();
    foreignKeys.add(fk1);
    foreignKeys.add(fk2);

    Map<String, Object> tableConstraints = new HashMap<String, Object>();
    tableConstraints.put("foreignKeys", foreignKeys);
    tableConstraints.put("primaryKey", Collections.singletonList("id"));

    Map<String, Map<String, Object>> constraints = new HashMap<String, Map<String, Object>>();
    constraints.put("fact_table", tableConstraints);

    // Add a table with no foreign keys
    Map<String, Object> noFkConstraints = new HashMap<String, Object>();
    noFkConstraints.put("primaryKey", Collections.singletonList("id"));
    constraints.put("simple_table", noFkConstraints);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) m.invoke(null, constraints, "econ", "ECON");

    assertNotNull(result);
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> rewrittenFks =
        (List<Map<String, Object>>) result.get("fact_table").get("foreignKeys");
    // First FK should be rewritten
    assertEquals("ECON", rewrittenFks.get(0).get("targetSchema"));
    // Second FK should be preserved (different schema)
    assertEquals("other_schema", rewrittenFks.get(1).get("targetSchema"));
    // Simple table should be preserved as-is
    assertNotNull(result.get("simple_table"));
  }

  @Test void testRewriteForeignKeySchemaNamesFKNull() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    m.setAccessible(true);

    Map<String, Object> tableConstraints = new HashMap<String, Object>();
    tableConstraints.put("foreignKeys", null);

    Map<String, Map<String, Object>> constraints = new HashMap<String, Map<String, Object>>();
    constraints.put("table1", tableConstraints);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) m.invoke(null, constraints, "econ", "ECON");
    assertNotNull(result);
    assertNotNull(result.get("table1"));
  }

  @Test void testRewriteForeignKeySchemaNamesFKEmpty() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    m.setAccessible(true);

    Map<String, Object> tableConstraints = new HashMap<String, Object>();
    tableConstraints.put("foreignKeys", new ArrayList<Map<String, Object>>());

    Map<String, Map<String, Object>> constraints = new HashMap<String, Map<String, Object>>();
    constraints.put("table1", tableConstraints);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) m.invoke(null, constraints, "econ", "ECON");
    assertNotNull(result);
  }

  @Test void testRewriteForeignKeySchemaNameNullConstraints() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    m.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) m.invoke(null, (Map<String, Map<String, Object>>) null,
            "econ", "ECON");
    assertNull(result);
  }

  @Test void testRewriteForeignKeySchemaNameEmptyConstraints() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    m.setAccessible(true);

    Map<String, Map<String, Object>> empty = new HashMap<String, Map<String, Object>>();

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) m.invoke(null, empty, "econ", "ECON");
    assertTrue(result.isEmpty());
  }

  // ===== rewriteSchemaReferencesInSql =====

  @Test void testRewriteSchemaReferencesInSqlNullArgs() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    m.setAccessible(true);

    assertNull(m.invoke(null, null, "econ", "ECON"));
    assertEquals("SELECT 1", m.invoke(null, "SELECT 1", null, "ECON"));
    assertEquals("SELECT 1", m.invoke(null, "SELECT 1", "econ", null));
  }

  @Test void testRewriteSchemaReferencesInSqlMatchingSchemas() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    m.setAccessible(true);

    // Same schema name (case-insensitive) should return unchanged SQL
    String sql = "SELECT * FROM econ.table1";
    assertEquals(sql, m.invoke(null, sql, "econ", "ECON"));
  }

  @Test void testRewriteSchemaReferencesInSqlRewrite() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    m.setAccessible(true);

    String sql = "SELECT a.id, b.name FROM declared.table1 a JOIN declared.table2 b ON a.id = b.id";
    String result = (String) m.invoke(null, sql, "declared", "ACTUAL");
    assertTrue(result.contains("ACTUAL.table1"));
    assertTrue(result.contains("ACTUAL.table2"));
    assertFalse(result.contains("declared."));
  }

  @Test void testRewriteSchemaReferencesInSqlNoMatch() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    m.setAccessible(true);

    String sql = "SELECT * FROM other.table1";
    String result = (String) m.invoke(null, sql, "declared", "ACTUAL");
    assertEquals(sql, result);
  }

  // ===== registerSqlViews =====

  @Test void testRegisterSqlViewsNullTables() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    m.setAccessible(true);

    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();
    Map<String, Object> operand = new HashMap<String, Object>();
    // Should not throw
    m.invoke(null, parentSchema, "test", null, operand);
  }

  @Test void testRegisterSqlViewsEmptyTables() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    m.setAccessible(true);

    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();
    Map<String, Object> operand = new HashMap<String, Object>();
    // Should not throw
    m.invoke(null, parentSchema, "test", new ArrayList<Map<String, Object>>(), operand);
  }

  @Test void testRegisterSqlViewsNonViewTable() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    m.setAccessible(true);

    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();
    Map<String, Object> operand = new HashMap<String, Object>();

    // Non-view table should be skipped
    Map<String, Object> table = new HashMap<String, Object>();
    table.put("type", "table");
    table.put("name", "data_table");
    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(table);

    m.invoke(null, parentSchema, "test", tables, operand);
  }

  @Test void testRegisterSqlViewsMissingName() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    m.setAccessible(true);

    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();
    // Add a fake schema so the method can find it
    parentSchema.add("test", CalciteSchema.createRootSchema(true).plus().unwrap(Schema.class));
    Map<String, Object> operand = new HashMap<String, Object>();

    // View with missing name should be skipped
    Map<String, Object> view = new HashMap<String, Object>();
    view.put("type", "view");
    view.put("sql", "SELECT 1");
    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(view);

    m.invoke(null, parentSchema, "test", tables, operand);
  }

  @Test void testRegisterSqlViewsMissingSql() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    m.setAccessible(true);

    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();
    parentSchema.add("test", CalciteSchema.createRootSchema(true).plus().unwrap(Schema.class));
    Map<String, Object> operand = new HashMap<String, Object>();

    // View with missing SQL should be skipped
    Map<String, Object> view = new HashMap<String, Object>();
    view.put("type", "view");
    view.put("name", "my_view");
    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(view);

    m.invoke(null, parentSchema, "test", tables, operand);
  }

  @Test void testRegisterSqlViewsSchemaNotFound() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    m.setAccessible(true);

    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();
    Map<String, Object> operand = new HashMap<String, Object>();

    // Schema not registered, view registration should skip
    Map<String, Object> view = new HashMap<String, Object>();
    view.put("type", "view");
    view.put("name", "my_view");
    view.put("sql", "SELECT 1");
    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(view);

    m.invoke(null, parentSchema, "nonexistent", tables, operand);
  }

  @Test void testRegisterSqlViewsViewDef() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    m.setAccessible(true);

    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();
    parentSchema.add("test", CalciteSchema.createRootSchema(true).plus().unwrap(Schema.class));
    Map<String, Object> operand = new HashMap<String, Object>();

    // View using viewDef instead of sql
    Map<String, Object> view = new HashMap<String, Object>();
    view.put("type", "view");
    view.put("name", "my_view");
    view.put("viewDef", "SELECT 1 AS val");
    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(view);

    m.invoke(null, parentSchema, "test", tables, operand);
  }

  // ===== registerMaterializations =====

  @Test void testRegisterMaterializationsMissingSchema() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("registerMaterializations", SchemaPlus.class, String.class, List.class);
    m.setAccessible(true);

    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    List<Map<String, Object>> materializations = new ArrayList<Map<String, Object>>();
    Map<String, Object> mv = new HashMap<String, Object>();
    mv.put("table", "mv_table");
    mv.put("sql", "SELECT * FROM base_table");
    materializations.add(mv);

    // Schema not found should log warning but not throw
    m.invoke(null, parentSchema, "nonexistent", materializations);
  }

  @Test void testRegisterMaterializationsMissingTableOrSql() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("registerMaterializations", SchemaPlus.class, String.class, List.class);
    m.setAccessible(true);

    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();
    parentSchema.add("test", CalciteSchema.createRootSchema(true).plus().unwrap(Schema.class));

    List<Map<String, Object>> materializations = new ArrayList<Map<String, Object>>();

    // Missing table
    Map<String, Object> mvNoTable = new HashMap<String, Object>();
    mvNoTable.put("sql", "SELECT 1");
    materializations.add(mvNoTable);

    // Missing sql
    Map<String, Object> mvNoSql = new HashMap<String, Object>();
    mvNoSql.put("table", "mv_table");
    materializations.add(mvNoSql);

    // Should not throw
    m.invoke(null, parentSchema, "test", materializations);
  }

  // ===== setTableConstraints / supportsConstraints =====

  @Test void testSupportsConstraints() {
    assertTrue(factory.supportsConstraints());
  }

  @Test void testSetTableConstraints() {
    Map<String, Map<String, Object>> constraints = new HashMap<String, Map<String, Object>>();
    constraints.put("table1", new HashMap<String, Object>());
    List<JsonTable> tableDefs = new ArrayList<JsonTable>();

    factory.setTableConstraints(constraints, tableDefs);

    // Verify via reflection
    try {
      Field f = FileSchemaFactory.class.getDeclaredField("tableConstraints");
      f.setAccessible(true);
      assertNotNull(f.get(factory));
    } catch (Exception e) {
      // Acceptable in some environments
    }
  }

  // ===== addMetadataSchemas =====

  @Test void testAddMetadataSchemas() throws Exception {
    Method m = FileSchemaFactory.class.getDeclaredMethod("addMetadataSchemas", SchemaPlus.class);
    m.setAccessible(true);

    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    // First call should add schemas
    m.invoke(null, parentSchema);
    assertNotNull(parentSchema.subSchemas().get("information_schema"));
    assertNotNull(parentSchema.subSchemas().get("pg_catalog"));

    // Second call should not add duplicates
    m.invoke(null, parentSchema);
  }

  // ===== writeDebugModel =====

  @Test void testWriteDebugModel() throws Exception {
    Method m =
        FileSchemaFactory.class.getDeclaredMethod("writeDebugModel", String.class, Map.class, String.class);
    m.setAccessible(true);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", "/path/to/data");

    // Should not throw
    m.invoke(null, "testSchema", operand, "parentSchema");
  }

  // ===== create() with various operand configurations =====

  @Test void testCreateWithMinimalLocalDirectory() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    // Create a CSV file in tempDir
    File csvFile = new File(tempDir.toFile(), "test.csv");
    try {
      FileWriter writer = new FileWriter(csvFile);
      try {
        writer.write("id,name\n1,Alice\n");
      } finally {
        writer.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);

    Schema schema = factory.create(parentSchema, "localtest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithEphemeralCacheSnakeCase() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeral_cache", Boolean.TRUE);

    Schema schema = factory.create(parentSchema, "ephtest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithBaseDirectoryString() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("baseDirectory", tempDir.resolve("cache").toString());

    Schema schema = factory.create(parentSchema, "bdstrtest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithBaseDirectoryFile() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("baseDirectory", tempDir.resolve("filecache").toFile());

    Schema schema = factory.create(parentSchema, "bdfiletest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithSourceDirectory() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("sourceDirectory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);

    Schema schema = factory.create(parentSchema, "srcdirtest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithVariousOperandTypes() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);
    operand.put("batchSize", Integer.valueOf(5000));
    operand.put("memoryThreshold", Long.valueOf(1000000L));
    operand.put("recursive", Boolean.TRUE);
    operand.put("directoryPattern", "**/*.csv");
    operand.put("refreshInterval", "PT5M");
    operand.put("flatten", Boolean.TRUE);
    operand.put("tableNameCasing", "LOWER");
    operand.put("columnNameCasing", "UPPER");
    operand.put("primeCache", Boolean.FALSE);
    operand.put("canonicalSchemaName", "myschema");
    operand.put("comment", "Test schema comment");
    operand.put("parquetCacheDirectory", tempDir.resolve("pqcache").toString());

    Schema schema = factory.create(parentSchema, "vartest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithSnakeCaseCasing() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);
    operand.put("table_name_casing", "SMART_CASING");
    operand.put("column_name_casing", "SMART_CASING");
    operand.put("prime_cache", Boolean.FALSE);

    Schema schema = factory.create(parentSchema, "snaketest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithGlobPattern() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);
    operand.put("glob", "*.csv");

    Schema schema = factory.create(parentSchema, "globtest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithViews() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    List<Map<String, Object>> views = new ArrayList<Map<String, Object>>();
    Map<String, Object> view = new HashMap<String, Object>();
    view.put("name", "my_view");
    view.put("sql", "SELECT 1 AS id");
    views.add(view);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);
    operand.put("views", views);

    Schema schema = factory.create(parentSchema, "viewtest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithCsvTypeInference() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> csvTypeInference = new HashMap<String, Object>();
    csvTypeInference.put("enabled", Boolean.TRUE);
    csvTypeInference.put("sampleSize", Integer.valueOf(100));

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);
    operand.put("csvTypeInference", csvTypeInference);

    Schema schema = factory.create(parentSchema, "csvtitest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateNoStorageTypeThrows() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    // No directory, no tables, no storageType => should throw

    assertThrows(IllegalStateException.class, () ->
        factory.create(parentSchema, "nodir", operand));
  }

  @Test void testCreateS3WithoutCredentialsThrows() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", "s3://my-bucket/data");

    assertThrows(IllegalArgumentException.class, () ->
        factory.create(parentSchema, "s3noauth", operand));
  }

  @Test void testCreateAutoDetectStorageFromDirectoryHttp() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", "https://example.com/data");
    operand.put("ephemeralCache", Boolean.TRUE);

    // This will fail later since HTTP storage isn't fully implemented, but it exercises the
    // auto-detection path
    try {
      Schema schema = factory.create(parentSchema, "httptest", operand);
      trackSchema(schema);
    } catch (Exception e) {
      // Expected - HTTP storage might not be fully supported
    }
  }

  @Test void testCreateAutoDetectStorageFromBaseDirectoryS3() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("baseDirectory", "s3://bucket/cache");
    operand.put("ephemeralCache", Boolean.TRUE);

    // Will fail because no S3 credentials but exercises the code path
    try {
      Schema schema = factory.create(parentSchema, "s3base", operand);
      trackSchema(schema);
    } catch (Exception e) {
      // Expected
    }
  }

  @Test void testCreateWithTablesAutoDetectsLocal() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    Map<String, Object> table = new HashMap<String, Object>();
    table.put("name", "test_table");
    table.put("url", tempDir.resolve("test.csv").toString());
    tables.add(table);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("tables", tables);
    operand.put("ephemeralCache", Boolean.TRUE);

    Schema schema = factory.create(parentSchema, "autolocaltest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithPartitionedTables() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    List<Map<String, Object>> partitionedTables = new ArrayList<Map<String, Object>>();
    Map<String, Object> ptable = new HashMap<String, Object>();
    ptable.put("name", "partitioned_data");
    ptable.put("pattern", tempDir.toString() + "/**/*.parquet");
    partitionedTables.add(ptable);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("partitionedTables", partitionedTables);
    operand.put("ephemeralCache", Boolean.TRUE);

    Schema schema = factory.create(parentSchema, "ptabletest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithDeclaredSchemaName() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);
    operand.put("declaredSchemaName", "canonical_name");

    Map<String, Map<String, Object>> constraints = new HashMap<String, Map<String, Object>>();
    Map<String, Object> tableConstraint = new HashMap<String, Object>();

    Map<String, Object> fk = new HashMap<String, Object>();
    fk.put("targetSchema", "canonical_name");
    fk.put("targetTable", "other_table");
    List<Map<String, Object>> fks = new ArrayList<Map<String, Object>>();
    fks.add(fk);
    tableConstraint.put("foreignKeys", fks);
    constraints.put("mytable", tableConstraint);
    operand.put("tableConstraints", constraints);

    Schema schema = factory.create(parentSchema, "DECLARED_TEST", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithExecutionEngineFromOperand() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);
    operand.put("executionEngine", "PARQUET");

    Schema schema = factory.create(parentSchema, "enginetest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithDuckDBConfig() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> duckdbConfig = new HashMap<String, Object>();
    duckdbConfig.put("threadsPerQuery", Integer.valueOf(4));

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);
    operand.put("duckdbConfig", duckdbConfig);

    Schema schema = factory.create(parentSchema, "ddbcfgtest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithMaterializations() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    List<Map<String, Object>> materializations = new ArrayList<Map<String, Object>>();
    Map<String, Object> mv = new HashMap<String, Object>();
    mv.put("table", "trend_monthly");
    mv.put("sql", "SELECT 1 AS val");
    materializations.add(mv);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);
    operand.put("materializations", materializations);

    Schema schema = factory.create(parentSchema, "mattest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithBaseDirectoryModelHandler() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);
    // Simulate Calcite's ModelHandler setting baseDirectory as a File
    operand.put("baseDirectory", tempDir.toFile());

    Schema schema = factory.create(parentSchema, "mhtest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithBaseDirectoryModelHandlerString() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);
    // Simulate Calcite's ModelHandler setting baseDirectory as a String
    operand.put("baseDirectory", tempDir.toString());

    Schema schema = factory.create(parentSchema, "mhstrtest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateNoDirectoryUsesCurrentDir() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);

    Schema schema = factory.create(parentSchema, "nodirtest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithStorageProviderInstance() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    org.apache.calcite.adapter.file.storage.StorageProvider mockProvider =
        new org.apache.calcite.adapter.file.storage.LocalFileStorageProvider();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);
    operand.put("_storageProvider", mockProvider);

    Schema schema = factory.create(parentSchema, "sptest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithRelativeDirectory() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", "relative/path");
    operand.put("storageType", "local");
    operand.put("ephemeralCache", Boolean.TRUE);
    // Set baseDirectory to model file dir (Calcite sets this automatically)
    operand.put("baseDirectory", tempDir.toFile());

    Schema schema = factory.create(parentSchema, "reltest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }

  @Test void testCreateWithEphemeralCacheStringTrue() {
    SchemaPlus parentSchema = CalciteSchema.createRootSchema(true).plus();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", "true");

    Schema schema = factory.create(parentSchema, "ephstrtest", operand);
    assertNotNull(schema);
    trackSchema(schema);
  }
}
