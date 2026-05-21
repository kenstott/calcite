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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Line coverage tests for {@link FileSchemaFactory} targeting specific uncovered paths:
 * sanitizeOperand, writeDebugModel, parseBooleanValue, constraint handling,
 * rewriteForeignKeySchemaNames, rewriteSchemaReferencesInSql, validateUniqueSchemaName,
 * and various create() operand combinations.
 */
@Tag("unit")
public class FileSchemaFactoryLineCoverageTest {

  @TempDir
  Path tempDir;

  private SchemaPlus parentSchema;

  @SuppressWarnings({"deprecation", "unchecked"})
  @BeforeEach
  void setUp() {
    parentSchema = mock(SchemaPlus.class);
    when(parentSchema.getName()).thenReturn("root");
    when(parentSchema.getSubSchemaNames()).thenReturn(Collections.<String>emptySet());

    Lookup<SchemaPlus> subSchemasLookup = mock(Lookup.class);
    when(subSchemasLookup.get(any(String.class))).thenReturn(null);
    when(subSchemasLookup.getNames(any(LikePattern.class)))
        .thenReturn(Collections.<String>emptySet());
    doReturn(subSchemasLookup).when(parentSchema).subSchemas();

    Lookup<Table> tablesLookup = mock(Lookup.class);
    when(tablesLookup.get(any(String.class))).thenReturn(null);
    when(tablesLookup.getNames(any(LikePattern.class)))
        .thenReturn(Collections.<String>emptySet());
    when(parentSchema.tables()).thenReturn(tablesLookup);

    when(parentSchema.getParentSchema()).thenReturn(null);
  }

  // --- sanitizeOperand tests via reflection ---

  @Test void testSanitizeOperandMasksPasswordAndSecretFields() throws Exception {
    Method sanitize =
        FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    sanitize.setAccessible(true);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("password", "mySecretPassword");
    operand.put("dbPassword", "anotherSecret");
    operand.put("apiSecret", "verySecret");
    operand.put("normalKey", "normalValue");
    operand.put("nullKey", null);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) sanitize.invoke(null, operand);

    assertEquals("********", result.get("password"));
    assertEquals("********", result.get("dbPassword"));
    assertEquals("********", result.get("apiSecret"));
    assertEquals("normalValue", result.get("normalKey"));
    assertNull(result.get("nullKey"));
  }

  @Test void testSanitizeOperandHandlesUnderscorePrefixedKeys() throws Exception {
    Method sanitize =
        FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    sanitize.setAccessible(true);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("_storageProvider", new Object());
    operand.put("_cacheStorageProvider", "someString");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) sanitize.invoke(null, operand);

    assertEquals("Object", result.get("_storageProvider"));
    assertEquals("String", result.get("_cacheStorageProvider"));
  }

  @Test void testSanitizeOperandHandlesS3Config() throws Exception {
    Method sanitize =
        FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    sanitize.setAccessible(true);

    Map<String, Object> s3Config = new HashMap<String, Object>();
    s3Config.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
    s3Config.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    s3Config.put("region", "us-east-1");
    s3Config.put("endpoint", "https://s3.amazonaws.com");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("s3Config", s3Config);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) sanitize.invoke(null, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) result.get("s3Config");
    assertNotNull(sanitizedS3);
    String maskedKey = (String) sanitizedS3.get("accessKeyId");
    assertTrue(maskedKey.startsWith("****"), "accessKeyId should start with ****");
    assertTrue(maskedKey.endsWith("MPLE"), "accessKeyId should end with last 4 chars");
    assertEquals("********", sanitizedS3.get("secretAccessKey"));
    assertEquals("us-east-1", sanitizedS3.get("region"));
  }

  @Test void testSanitizeOperandHandlesS3ConfigShortAccessKey() throws Exception {
    Method sanitize =
        FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    sanitize.setAccessible(true);

    Map<String, Object> s3Config = new HashMap<String, Object>();
    s3Config.put("accessKeyId", "AB");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("s3Config", s3Config);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) sanitize.invoke(null, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) result.get("s3Config");
    assertEquals("****", sanitizedS3.get("accessKeyId"));
  }

  @Test void testSanitizeOperandHandlesStorageConfig() throws Exception {
    Method sanitize =
        FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    sanitize.setAccessible(true);

    Map<String, Object> storageConfig = new HashMap<String, Object>();
    storageConfig.put("accesskey", "AKID123456");
    storageConfig.put("secretkey", "secret123");
    storageConfig.put("password", "pass123");
    storageConfig.put("bucket", "my-bucket");
    storageConfig.put("_storageProvider", new Object());
    storageConfig.put("_nullProvider", null);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("storageConfig", storageConfig);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) sanitize.invoke(null, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedStorage = (Map<String, Object>) result.get("storageConfig");
    assertNotNull(sanitizedStorage);
    assertEquals("********", sanitizedStorage.get("accesskey"));
    assertEquals("********", sanitizedStorage.get("secretkey"));
    assertEquals("********", sanitizedStorage.get("password"));
    assertEquals("my-bucket", sanitizedStorage.get("bucket"));
    assertEquals("Object", sanitizedStorage.get("_storageProvider"));
    assertNull(sanitizedStorage.get("_nullProvider"));
  }

  @Test void testSanitizeOperandHandlesModelUri() throws Exception {
    Method sanitize =
        FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    sanitize.setAccessible(true);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("modelUri",
        "inline:{\"schemas\":[{\"operand\":{\"accessKeyId\": \"AKID123\", "
        + "\"secretAccessKey\": \"SECRET456\"}}]}");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) sanitize.invoke(null, operand);

    String sanitizedUri = (String) result.get("modelUri");
    assertNotNull(sanitizedUri);
    assertFalse(sanitizedUri.contains("AKID123"), "accessKeyId should be redacted");
    assertFalse(sanitizedUri.contains("SECRET456"), "secretAccessKey should be redacted");
    assertTrue(sanitizedUri.contains("\"accessKeyId\": \"****\""));
    assertTrue(sanitizedUri.contains("\"secretAccessKey\": \"********\""));
  }

  @Test void testSanitizeOperandHandlesGenericNestedMap() throws Exception {
    Method sanitize =
        FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    sanitize.setAccessible(true);

    Map<String, Object> nestedMap = new HashMap<String, Object>();
    nestedMap.put("password", "secret");
    nestedMap.put("secretKey", "hidden");
    nestedMap.put("accesskey", "hidden2");
    nestedMap.put("normalField", "visible");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("customConfig", nestedMap);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) sanitize.invoke(null, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedNested = (Map<String, Object>) result.get("customConfig");
    assertNotNull(sanitizedNested);
    assertEquals("********", sanitizedNested.get("password"));
    assertEquals("********", sanitizedNested.get("secretKey"));
    assertEquals("********", sanitizedNested.get("accesskey"));
    assertEquals("visible", sanitizedNested.get("normalField"));
  }

  // --- writeDebugModel tests via reflection ---

  @Test void testWriteDebugModelCreatesFile() throws Exception {
    Method writeDebugModel =
        FileSchemaFactory.class.getDeclaredMethod("writeDebugModel", String.class, Map.class, String.class);
    writeDebugModel.setAccessible(true);

    String originalUserDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", tempDir.toString());

      Map<String, Object> operand = new HashMap<String, Object>();
      operand.put("directory", "/some/dir");
      operand.put("executionEngine", "PARQUET");

      writeDebugModel.invoke(null, "TestSchema", operand, "root");

      File aperioDir = new File(tempDir.toFile(), ".aperio");
      File debugFile = new File(aperioDir, "debug-model-testschema.json");
      assertTrue(debugFile.exists(), "Debug model file should have been created");
      assertTrue(debugFile.length() > 0, "Debug model file should not be empty");
    } finally {
      System.setProperty("user.dir", originalUserDir);
    }
  }

  @Test void testWriteDebugModelWithNullParent() throws Exception {
    Method writeDebugModel =
        FileSchemaFactory.class.getDeclaredMethod("writeDebugModel", String.class, Map.class, String.class);
    writeDebugModel.setAccessible(true);

    String originalUserDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", tempDir.toString());

      Map<String, Object> operand = new HashMap<String, Object>();
      operand.put("directory", "/some/dir");

      writeDebugModel.invoke(null, "NullParent", operand, null);

      File aperioDir = new File(tempDir.toFile(), ".aperio");
      File debugFile = new File(aperioDir, "debug-model-nullparent.json");
      assertTrue(debugFile.exists(), "Debug model file should be created with null parent");
    } finally {
      System.setProperty("user.dir", originalUserDir);
    }
  }

  // --- parseBooleanValue tests via reflection ---

  @Test void testParseBooleanValueWithVariousTypes() throws Exception {
    Method parse =
        FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    parse.setAccessible(true);

    assertNull(parse.invoke(null, (Object) null));
    assertEquals(Boolean.TRUE, parse.invoke(null, Boolean.TRUE));
    assertEquals(Boolean.FALSE, parse.invoke(null, Boolean.FALSE));
    assertEquals(Boolean.TRUE, parse.invoke(null, "true"));
    assertEquals(Boolean.FALSE, parse.invoke(null, "false"));
    assertNull(parse.invoke(null, 42));
    assertNull(parse.invoke(null, new ArrayList<Object>()));
  }

  // --- supportsConstraints and setTableConstraints ---

  @Test void testSupportsConstraintsReturnsTrue() {
    assertTrue(FileSchemaFactory.INSTANCE.supportsConstraints());
  }

  @Test void testSetTableConstraints() {
    Map<String, Map<String, Object>> constraints = new HashMap<String, Map<String, Object>>();
    Map<String, Object> tableConstraint = new HashMap<String, Object>();
    tableConstraint.put("primaryKey", Collections.singletonList("id"));
    constraints.put("my_table", tableConstraint);

    FileSchemaFactory.INSTANCE.setTableConstraints(constraints, null);
  }

  // --- rewriteForeignKeySchemaNames tests via reflection ---

  @Test void testRewriteForeignKeySchemaNames() throws Exception {
    Method rewrite =
        FileSchemaFactory.class.getDeclaredMethod("rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    rewrite.setAccessible(true);

    Map<String, Object> fk = new HashMap<String, Object>();
    fk.put("targetSchema", "econ");
    fk.put("targetTable", "countries");

    List<Map<String, Object>> foreignKeys = new ArrayList<Map<String, Object>>();
    foreignKeys.add(fk);

    Map<String, Object> tableConstraint = new HashMap<String, Object>();
    tableConstraint.put("foreignKeys", foreignKeys);

    Map<String, Map<String, Object>> constraints = new HashMap<String, Map<String, Object>>();
    constraints.put("orders", tableConstraint);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) rewrite.invoke(null, constraints, "econ", "ECON");

    assertNotNull(result);
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> rewrittenFKs =
        (List<Map<String, Object>>) result.get("orders").get("foreignKeys");
    assertEquals("ECON", rewrittenFKs.get(0).get("targetSchema"));
  }

  @Test void testRewriteForeignKeySchemaNamesCrossSchemaFK() throws Exception {
    Method rewrite =
        FileSchemaFactory.class.getDeclaredMethod("rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    rewrite.setAccessible(true);

    Map<String, Object> fk = new HashMap<String, Object>();
    fk.put("targetSchema", "other_schema");
    fk.put("targetTable", "categories");

    List<Map<String, Object>> foreignKeys = new ArrayList<Map<String, Object>>();
    foreignKeys.add(fk);

    Map<String, Object> tableConstraint = new HashMap<String, Object>();
    tableConstraint.put("foreignKeys", foreignKeys);

    Map<String, Map<String, Object>> constraints = new HashMap<String, Map<String, Object>>();
    constraints.put("products", tableConstraint);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) rewrite.invoke(null, constraints, "econ", "ECON");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> rewrittenFKs =
        (List<Map<String, Object>>) result.get("products").get("foreignKeys");
    assertEquals("other_schema", rewrittenFKs.get(0).get("targetSchema"));
  }

  @Test void testRewriteForeignKeySchemaNameNoForeignKeys() throws Exception {
    Method rewrite =
        FileSchemaFactory.class.getDeclaredMethod("rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    rewrite.setAccessible(true);

    Map<String, Object> tableConstraint = new HashMap<String, Object>();
    tableConstraint.put("primaryKey", Collections.singletonList("id"));

    Map<String, Map<String, Object>> constraints = new HashMap<String, Map<String, Object>>();
    constraints.put("simple_table", tableConstraint);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) rewrite.invoke(null, constraints, "econ", "ECON");

    assertNotNull(result.get("simple_table"));
  }

  @Test void testRewriteForeignKeySchemaNameNullAndEmpty() throws Exception {
    Method rewrite =
        FileSchemaFactory.class.getDeclaredMethod("rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    rewrite.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> nullResult =
        (Map<String, Map<String, Object>>) rewrite.invoke(null, null, "econ", "ECON");
    assertNull(nullResult);

    Map<String, Map<String, Object>> emptyConstraints =
        new HashMap<String, Map<String, Object>>();
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> emptyResult =
        (Map<String, Map<String, Object>>) rewrite.invoke(null, emptyConstraints, "econ", "ECON");
    assertSame(emptyConstraints, emptyResult);
  }

  // --- sanitizeNestedMap tests via reflection ---

  @Test void testSanitizeNestedMapDirectly() throws Exception {
    Method sanitizeNested =
        FileSchemaFactory.class.getDeclaredMethod("sanitizeNestedMap", Map.class);
    sanitizeNested.setAccessible(true);

    Map<String, Object> nestedMap = new HashMap<String, Object>();
    nestedMap.put("accesskey", "AKID");
    nestedMap.put("password", "pass");
    nestedMap.put("secret", "sec");
    nestedMap.put("normalField", "value");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) sanitizeNested.invoke(null, nestedMap);

    assertEquals("********", result.get("accesskey"));
    assertEquals("********", result.get("password"));
    assertEquals("********", result.get("secret"));
    assertEquals("value", result.get("normalField"));
  }

  // --- rewriteSchemaReferencesInSql tests via reflection ---

  @Test void testRewriteSchemaReferencesInSql() throws Exception {
    Method rewriteSql =
        FileSchemaFactory.class.getDeclaredMethod("rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    rewriteSql.setAccessible(true);

    // Rewrite with actually different schema names (not just case-different)
    String result =
        (String) rewriteSql.invoke(null, "SELECT * FROM econ.countries WHERE econ.countries.id > 0",
        "econ", "my_economy");
    assertTrue(result.contains("my_economy."),
        "Should rewrite 'econ.' to 'my_economy.'");

    // Case-insensitive match returns original (no rewrite when names match ignoring case)
    String result2 =
        (String) rewriteSql.invoke(null, "SELECT * FROM econ.countries", "econ", "ECON");
    assertEquals("SELECT * FROM econ.countries", result2,
        "Should not rewrite when schema names match case-insensitively");

    // Identical names return original
    String result3 =
        (String) rewriteSql.invoke(null, "SELECT * FROM econ.countries", "econ", "econ");
    assertEquals("SELECT * FROM econ.countries", result3);

    // Null inputs
    assertNull(rewriteSql.invoke(null, null, "econ", "my_economy"));
    assertNull(rewriteSql.invoke(null, null, null, null));
  }

  // --- validateUniqueSchemaName tests via reflection ---

  @Test void testValidateUniqueSchemaNameNullInputs() throws Exception {
    Method validate =
        FileSchemaFactory.class.getDeclaredMethod("validateUniqueSchemaName", SchemaPlus.class, String.class);
    validate.setAccessible(true);

    validate.invoke(null, null, "test");
    validate.invoke(null, parentSchema, null);
  }

  @Test void testValidateUniqueSchemaNameDuplicate() throws Exception {
    Method validate =
        FileSchemaFactory.class.getDeclaredMethod("validateUniqueSchemaName", SchemaPlus.class, String.class);
    validate.setAccessible(true);

    SchemaPlus dupParent = mock(SchemaPlus.class);
    @SuppressWarnings("unchecked")
    Lookup<SchemaPlus> subSchemasLookup = mock(Lookup.class);
    when(subSchemasLookup.get("existing_schema")).thenReturn(mock(SchemaPlus.class));
    when(subSchemasLookup.getNames(any(LikePattern.class)))
        .thenReturn(Collections.singleton("existing_schema"));
    doReturn(subSchemasLookup).when(dupParent).subSchemas();

    try {
      validate.invoke(null, dupParent, "existing_schema");
      fail("Should have thrown");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertTrue(e.getCause().getMessage().contains("already exists"));
    }
  }

  // --- create() with operand combinations for line coverage ---

  @Test void testCreateWithTableConstraintsInOperand() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");

    Map<String, Map<String, Object>> tableConstraints =
        new HashMap<String, Map<String, Object>>();
    Map<String, Object> constraint = new HashMap<String, Object>();
    constraint.put("primaryKey", Collections.singletonList("id"));
    tableConstraints.put("some_table", constraint);
    operand.put("tableConstraints", tableConstraints);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_constraints_op", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithDeclaredSchemaNameDifferentTriggersRewrite() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    operand.put("declaredSchemaName", "econ");

    Map<String, Object> fk = new HashMap<String, Object>();
    fk.put("targetSchema", "econ");
    fk.put("targetTable", "countries");

    List<Map<String, Object>> foreignKeys = new ArrayList<Map<String, Object>>();
    foreignKeys.add(fk);

    Map<String, Object> constraint = new HashMap<String, Object>();
    constraint.put("foreignKeys", foreignKeys);

    Map<String, Map<String, Object>> tableConstraints =
        new HashMap<String, Map<String, Object>>();
    tableConstraints.put("orders", constraint);
    operand.put("tableConstraints", tableConstraints);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "ECON_REWRITE", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithComment() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    operand.put("comment", "Test schema comment");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_comment_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithFlatten() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    operand.put("flatten", Boolean.TRUE);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_flatten_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithCasingOptions() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    operand.put("tableNameCasing", "LOWER_CASE");
    operand.put("columnNameCasing", "LOWER_CASE");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_casing_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithSnakeCaseCasingOptions() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    operand.put("table_name_casing", "UPPER_CASE");
    operand.put("column_name_casing", "UPPER_CASE");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_casing_snake_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithBatchSizeAndMemoryThreshold() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    operand.put("batchSize", 5000);
    operand.put("memoryThreshold", 536870912L);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_batch_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithCsvTypeInference() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    Map<String, Object> csvTypeInference = new HashMap<String, Object>();
    csvTypeInference.put("enabled", true);
    operand.put("csvTypeInference", csvTypeInference);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_csv_infer_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithDirectoryPattern() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    operand.put("directoryPattern", "*.csv");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_dirpat_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithGlobPattern() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    operand.put("glob", "**/*.parquet");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_glob_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithRefreshInterval() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    operand.put("refreshInterval", "30m");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_refresh_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithCanonicalSchemaName() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    operand.put("canonicalSchemaName", "canonical_name");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_canonical_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithPrimeCacheFalse() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    operand.put("primeCache", Boolean.FALSE);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_primef_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithPrimeCacheSnakeCase() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    operand.put("prime_cache", Boolean.FALSE);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_prime_snake_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithBaseDirectoryAsFile() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");
    operand.put("baseDirectory", tempDir.toFile());

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_basedir_file_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithViewsDefinition() throws IOException {
    File sourceDir = tempDir.resolve("views-lc").toFile();
    sourceDir.mkdirs();
    File csvFile = new File(sourceDir, "base_data.csv");
    FileWriter fw = new FileWriter(csvFile);
    fw.write("id,name,amount\n1,Alice,100\n2,Bob,200\n");
    fw.close();

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    Map<String, Object> tableDef = new HashMap<String, Object>();
    tableDef.put("name", "base_data");
    tableDef.put("url", csvFile.getAbsolutePath());
    tables.add(tableDef);

    Map<String, Object> viewDef = new HashMap<String, Object>();
    viewDef.put("name", "high_view");
    viewDef.put("type", "view");
    viewDef.put("sql", "SELECT * FROM base_data WHERE amount > 150");
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", sourceDir.getAbsolutePath());
    operand.put("tables", tables);
    operand.put("executionEngine", "PARQUET");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_views_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithMaterializations() throws IOException {
    File sourceDir = tempDir.resolve("mat-lc").toFile();
    sourceDir.mkdirs();
    File csvFile = new File(sourceDir, "detail.csv");
    FileWriter fw = new FileWriter(csvFile);
    fw.write("id,name\n1,Alice\n");
    fw.close();

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    Map<String, Object> tableDef = new HashMap<String, Object>();
    tableDef.put("name", "detail");
    tableDef.put("url", csvFile.getAbsolutePath());
    tables.add(tableDef);

    List<Map<String, Object>> materializations = new ArrayList<Map<String, Object>>();
    Map<String, Object> mv = new HashMap<String, Object>();
    mv.put("table", "detail_summary");
    mv.put("sql", "SELECT * FROM detail");
    materializations.add(mv);

    Map<String, Object> mvBad = new HashMap<String, Object>();
    mvBad.put("table", "bad_mv");
    materializations.add(mvBad);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", sourceDir.getAbsolutePath());
    operand.put("tables", tables);
    operand.put("materializations", materializations);
    operand.put("executionEngine", "PARQUET");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_mat_lc", operand);
    assertNotNull(schema);
  }

  @Test void testCreateAddsMetadataSchemas() {
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_meta_lc", operand);
    assertNotNull(schema);

    verify(parentSchema, atLeastOnce()).add(eq("information_schema"), any(Schema.class));
    verify(parentSchema, atLeastOnce()).add(eq("pg_catalog"), any(Schema.class));
  }
}
