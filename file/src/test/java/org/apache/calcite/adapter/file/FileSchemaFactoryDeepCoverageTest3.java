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
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.schema.lookup.LikePattern;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for FileSchemaFactory focusing on uncovered branches:
 * sanitizeOperand, sanitizeNestedMap, parseBooleanValue, validateUniqueSchemaName,
 * rewriteSchemaReferencesInSql, rewriteForeignKeySchemaNames, registerSqlViews,
 * and addMetadataSchemas.
 */
@Tag("unit")
public class FileSchemaFactoryDeepCoverageTest3 {

  @TempDir
  Path tempDir;

  private SchemaPlus mockParentSchema;

  @SuppressWarnings({"unchecked", "rawtypes"})
  @BeforeEach
  void setUp() {
    mockParentSchema = mock(SchemaPlus.class);

    Lookup subSchemaLookup = mock(Lookup.class);
    when(subSchemaLookup.get(anyString())).thenReturn(null);
    when(subSchemaLookup.getNames(any(LikePattern.class))).thenReturn(Collections.emptySet());
    doReturn(subSchemaLookup).when(mockParentSchema).subSchemas();

    Lookup tableLookup = mock(Lookup.class);
    when(tableLookup.get(anyString())).thenReturn(null);
    when(tableLookup.getNames(any(LikePattern.class))).thenReturn(Collections.emptySet());
    doReturn(tableLookup).when(mockParentSchema).tables();

    when(mockParentSchema.getParentSchema()).thenReturn(null);
    when(mockParentSchema.getName()).thenReturn("root");
  }

  // ====== parseBooleanValue tests ======

  @Test
  void testParseBooleanValueNull() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);
    assertNull(method.invoke(null, (Object) null));
  }

  @Test
  void testParseBooleanValueBooleanTrue() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);
    assertEquals(Boolean.TRUE, method.invoke(null, Boolean.TRUE));
  }

  @Test
  void testParseBooleanValueBooleanFalse() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);
    assertEquals(Boolean.FALSE, method.invoke(null, Boolean.FALSE));
  }

  @Test
  void testParseBooleanValueStringTrue() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);
    assertEquals(Boolean.TRUE, method.invoke(null, "true"));
  }

  @Test
  void testParseBooleanValueStringFalse() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);
    assertEquals(Boolean.FALSE, method.invoke(null, "false"));
  }

  @Test
  void testParseBooleanValueUnsupportedType() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);
    assertNull(method.invoke(null, 42));
  }

  // ====== sanitizeOperand tests ======

  @Test
  void testSanitizeOperandNullValue() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);
    Map<String, Object> operand = new HashMap<>();
    operand.put("someKey", null);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);
    assertTrue(result.containsKey("someKey"));
    assertNull(result.get("someKey"));
  }

  @Test
  void testSanitizeOperandPasswordKey() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);
    Map<String, Object> operand = new HashMap<>();
    operand.put("password", "secret123");
    operand.put("secretToken", "tok-xxx");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);
    assertEquals("********", result.get("password"));
    assertEquals("********", result.get("secretToken"));
  }

  @Test
  void testSanitizeOperandUnderscoreKey() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);
    Map<String, Object> operand = new HashMap<>();
    operand.put("_storageProvider", new Object());

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);
    assertEquals("Object", result.get("_storageProvider"));
  }

  @Test
  void testSanitizeOperandS3Config() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);
    Map<String, Object> s3Config = new HashMap<>();
    s3Config.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
    s3Config.put("secretAccessKey", "wJalrXUtnFEMI");
    s3Config.put("region", "us-west-2");
    s3Config.put("endpoint", "https://s3.amazonaws.com");

    Map<String, Object> operand = new HashMap<>();
    operand.put("s3Config", s3Config);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) result.get("s3Config");

    assertTrue(sanitizedS3.get("accessKeyId").toString().startsWith("****"));
    assertTrue(sanitizedS3.get("accessKeyId").toString().endsWith("MPLE"));
    assertEquals("********", sanitizedS3.get("secretAccessKey"));
    assertEquals("us-west-2", sanitizedS3.get("region"));
  }

  @Test
  void testSanitizeOperandS3ConfigShortAccessKey() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);
    Map<String, Object> s3Config = new HashMap<>();
    s3Config.put("accessKeyId", "AB");
    s3Config.put("secretAccessKey", "secret");

    Map<String, Object> operand = new HashMap<>();
    operand.put("s3Config", s3Config);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) result.get("s3Config");
    assertEquals("****", sanitizedS3.get("accessKeyId"));
  }

  @Test
  void testSanitizeOperandS3ConfigPasswordInS3() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);
    Map<String, Object> s3Config = new HashMap<>();
    s3Config.put("password", "s3pass");

    Map<String, Object> operand = new HashMap<>();
    operand.put("s3Config", s3Config);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) result.get("s3Config");
    assertEquals("********", sanitizedS3.get("password"));
  }

  @Test
  void testSanitizeOperandStorageConfig() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", new Object());
    storageConfig.put("_nullProvider", null);
    storageConfig.put("secretKey", "hidden");
    storageConfig.put("password", "hidden2");
    storageConfig.put("accessKeyId", "AKIA1234");
    storageConfig.put("bucket", "my-bucket");

    Map<String, Object> operand = new HashMap<>();
    operand.put("storageConfig", storageConfig);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedStorage = (Map<String, Object>) result.get("storageConfig");

    assertEquals("Object", sanitizedStorage.get("_storageProvider"));
    assertNull(sanitizedStorage.get("_nullProvider"));
    assertEquals("********", sanitizedStorage.get("secretKey"));
    assertEquals("********", sanitizedStorage.get("password"));
    assertEquals("********", sanitizedStorage.get("accessKeyId"));
    assertEquals("my-bucket", sanitizedStorage.get("bucket"));
  }

  @Test
  void testSanitizeOperandNestedMap() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);
    Map<String, Object> nestedMap = new HashMap<>();
    nestedMap.put("someKey", "someValue");
    nestedMap.put("secretToken", "hidden");

    Map<String, Object> operand = new HashMap<>();
    operand.put("customConfig", nestedMap);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);
    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedNested = (Map<String, Object>) result.get("customConfig");

    assertEquals("someValue", sanitizedNested.get("someKey"));
    assertEquals("********", sanitizedNested.get("secretToken"));
  }

  @Test
  void testSanitizeOperandModelUri() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);
    Map<String, Object> operand = new HashMap<>();
    operand.put("modelUri",
        "inline:{\"accessKeyId\": \"AKIA123\", \"secretAccessKey\": \"secretXYZ\"}");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);
    String sanitizedUri = (String) result.get("modelUri");

    assertTrue(sanitizedUri.contains("\"accessKeyId\": \"****\""));
    assertTrue(sanitizedUri.contains("\"secretAccessKey\": \"********\""));
  }

  @Test
  void testSanitizeOperandPlainValue() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "/tmp/data");
    operand.put("recursive", Boolean.TRUE);
    operand.put("batchSize", 2048);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);
    assertEquals("/tmp/data", result.get("directory"));
    assertEquals(Boolean.TRUE, result.get("recursive"));
    assertEquals(2048, result.get("batchSize"));
  }

  // ====== sanitizeNestedMap tests ======

  @Test
  void testSanitizeNestedMapCalciteClass() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeNestedMap", Map.class);
    method.setAccessible(true);

    Map<String, Object> nestedMap = new HashMap<>();
    nestedMap.put("schema", mockParentSchema);
    nestedMap.put("accesskey", "hidden");
    nestedMap.put("normalKey", "normalValue");
    nestedMap.put("nullValue", null);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, nestedMap);

    assertEquals("********", result.get("accesskey"));
    assertEquals("normalValue", result.get("normalKey"));
    assertNull(result.get("nullValue"));
  }

  // ====== validateUniqueSchemaName tests ======

  @Test
  void testValidateUniqueSchemaNameNullParent() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "validateUniqueSchemaName", SchemaPlus.class, String.class);
    method.setAccessible(true);
    method.invoke(null, null, "test");
  }

  @Test
  void testValidateUniqueSchemaNameNullName() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "validateUniqueSchemaName", SchemaPlus.class, String.class);
    method.setAccessible(true);
    method.invoke(null, mockParentSchema, null);
  }

  @Test
  void testValidateUniqueSchemaNameUnique() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "validateUniqueSchemaName", SchemaPlus.class, String.class);
    method.setAccessible(true);
    method.invoke(null, mockParentSchema, "test");
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  void testValidateUniqueSchemaNameDuplicate() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "validateUniqueSchemaName", SchemaPlus.class, String.class);
    method.setAccessible(true);

    Lookup subSchemaLookup = mock(Lookup.class);
    when(subSchemaLookup.get("existingSchema")).thenReturn(mock(SchemaPlus.class));
    when(subSchemaLookup.getNames(any(LikePattern.class)))
        .thenReturn(new HashSet<>(Arrays.asList("existingSchema")));
    doReturn(subSchemaLookup).when(mockParentSchema).subSchemas();

    try {
      method.invoke(null, mockParentSchema, "existingSchema");
      fail("Expected IllegalArgumentException via InvocationTargetException");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertTrue(e.getCause().getMessage().contains("already exists"));
    }
  }

  // ====== rewriteSchemaReferencesInSql tests ======

  @Test
  void testRewriteSchemaReferencesInSqlNull() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    method.setAccessible(true);

    assertNull(method.invoke(null, null, "econ", "ECON"));

    String sql = "SELECT * FROM econ.table1";
    assertEquals(sql, method.invoke(null, sql, null, "ECON"));
    assertEquals(sql, method.invoke(null, sql, "econ", null));
  }

  @Test
  void testRewriteSchemaReferencesInSqlMatchingCase() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    method.setAccessible(true);

    String sql = "SELECT * FROM econ.table1";
    assertEquals(sql, method.invoke(null, sql, "econ", "ECON"));
  }

  @Test
  void testRewriteSchemaReferencesInSqlDifferentNames() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    method.setAccessible(true);

    String sql = "SELECT * FROM myschema.table1 WHERE myschema.table1.id = 1";
    String result = (String) method.invoke(null, sql, "myschema", "ACTUAL_SCHEMA");
    assertTrue(result.contains("ACTUAL_SCHEMA.table1"));
  }

  @Test
  void testRewriteSchemaReferencesInSqlNoMatch() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    method.setAccessible(true);

    String sql = "SELECT * FROM other.table1";
    String result = (String) method.invoke(null, sql, "myschema", "ACTUAL_SCHEMA");
    assertEquals(sql, result);
  }

  // ====== rewriteForeignKeySchemaNames tests ======

  @Test
  void testRewriteForeignKeySchemaNameNull() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    method.setAccessible(true);

    assertNull(method.invoke(null, null, "econ", "ECON"));
    assertEquals(Collections.emptyMap(),
        method.invoke(null, Collections.emptyMap(), "econ", "ECON"));
  }

  @Test
  void testRewriteForeignKeySchemaNameNoForeignKeys() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    method.setAccessible(true);

    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tableConstraints = new HashMap<>();
    tableConstraints.put("primaryKey", Arrays.asList("id"));
    constraints.put("table1", tableConstraints);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) method.invoke(null, constraints, "econ", "ECON");
    assertEquals(tableConstraints, result.get("table1"));
  }

  @Test
  void testRewriteForeignKeySchemaNameEmptyForeignKeys() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    method.setAccessible(true);

    Map<String, Map<String, Object>> constraints = new HashMap<>();
    Map<String, Object> tableConstraints = new HashMap<>();
    tableConstraints.put("foreignKeys", Collections.emptyList());
    constraints.put("table1", tableConstraints);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) method.invoke(null, constraints, "econ", "ECON");
    assertEquals(tableConstraints, result.get("table1"));
  }

  @Test
  void testRewriteForeignKeySchemaNameMatchingFK() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    method.setAccessible(true);

    Map<String, Object> fk1 = new HashMap<>();
    fk1.put("targetSchema", "econ");
    fk1.put("targetTable", "ref_table");

    Map<String, Object> fk2 = new HashMap<>();
    fk2.put("targetSchema", "other_schema");
    fk2.put("targetTable", "other_table");

    List<Map<String, Object>> foreignKeys = new ArrayList<>();
    foreignKeys.add(fk1);
    foreignKeys.add(fk2);

    Map<String, Object> tableConstraints = new HashMap<>();
    tableConstraints.put("foreignKeys", foreignKeys);

    Map<String, Map<String, Object>> constraints = new HashMap<>();
    constraints.put("my_table", tableConstraints);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) method.invoke(null, constraints, "econ", "ECON");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resultFKs =
        (List<Map<String, Object>>) result.get("my_table").get("foreignKeys");

    assertEquals(2, resultFKs.size());
    assertEquals("ECON", resultFKs.get(0).get("targetSchema"));
    assertEquals("other_schema", resultFKs.get(1).get("targetSchema"));
  }

  @Test
  void testRewriteForeignKeySchemaNameNullTargetSchema() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    method.setAccessible(true);

    Map<String, Object> fk1 = new HashMap<>();
    fk1.put("targetSchema", null);
    fk1.put("targetTable", "ref_table");

    List<Map<String, Object>> foreignKeys = new ArrayList<>();
    foreignKeys.add(fk1);

    Map<String, Object> tableConstraints = new HashMap<>();
    tableConstraints.put("foreignKeys", foreignKeys);

    Map<String, Map<String, Object>> constraints = new HashMap<>();
    constraints.put("my_table", tableConstraints);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result =
        (Map<String, Map<String, Object>>) method.invoke(null, constraints, "econ", "ECON");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resultFKs =
        (List<Map<String, Object>>) result.get("my_table").get("foreignKeys");

    assertEquals(1, resultFKs.size());
    assertNull(resultFKs.get(0).get("targetSchema"));
  }

  // ====== registerSqlViews tests ======

  @Test
  void testRegisterSqlViewsNullTables() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    method.setAccessible(true);
    method.invoke(null, mockParentSchema, "test", null, new HashMap<>());
  }

  @Test
  void testRegisterSqlViewsEmptyTables() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    method.setAccessible(true);
    method.invoke(null, mockParentSchema, "test", Collections.emptyList(), new HashMap<>());
  }

  @Test
  void testRegisterSqlViewsSchemaNotFound() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    method.setAccessible(true);

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> viewDef = new HashMap<>();
    viewDef.put("type", "view");
    viewDef.put("name", "myview");
    viewDef.put("sql", "SELECT 1");
    tables.add(viewDef);

    method.invoke(null, mockParentSchema, "test", tables, new HashMap<>());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  void testRegisterSqlViewsNonViewSkipped() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    method.setAccessible(true);

    SchemaPlus foundSchema = mock(SchemaPlus.class);
    Lookup subSchemaLookup = mock(Lookup.class);
    when(subSchemaLookup.get("test")).thenReturn(foundSchema);
    when(subSchemaLookup.getNames(any(LikePattern.class))).thenReturn(Collections.emptySet());
    doReturn(subSchemaLookup).when(mockParentSchema).subSchemas();

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("type", "custom");
    tableDef.put("name", "mytable");
    tables.add(tableDef);

    method.invoke(null, mockParentSchema, "test", tables, new HashMap<>());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  void testRegisterSqlViewsMissingViewName() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    method.setAccessible(true);

    SchemaPlus foundSchema = mock(SchemaPlus.class);
    Lookup subSchemaLookup = mock(Lookup.class);
    when(subSchemaLookup.get("test")).thenReturn(foundSchema);
    when(subSchemaLookup.getNames(any(LikePattern.class))).thenReturn(Collections.emptySet());
    doReturn(subSchemaLookup).when(mockParentSchema).subSchemas();

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> viewDef = new HashMap<>();
    viewDef.put("type", "view");
    viewDef.put("sql", "SELECT 1");
    tables.add(viewDef);

    method.invoke(null, mockParentSchema, "test", tables, new HashMap<>());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  void testRegisterSqlViewsWithViewDefFallback() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    method.setAccessible(true);

    SchemaPlus foundSchema = mock(SchemaPlus.class);
    Lookup subSchemaLookup = mock(Lookup.class);
    when(subSchemaLookup.get("test")).thenReturn(foundSchema);
    when(subSchemaLookup.getNames(any(LikePattern.class))).thenReturn(Collections.emptySet());
    doReturn(subSchemaLookup).when(mockParentSchema).subSchemas();

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> viewDef = new HashMap<>();
    viewDef.put("type", "view");
    viewDef.put("name", "my_view");
    viewDef.put("viewDef", "SELECT 1 AS col");
    tables.add(viewDef);

    Map<String, Object> operand = new HashMap<>();

    // The registerSqlViews method attempts ViewTable.viewMacro() which internally calls
    // CalciteSchema.from(schema), requiring a real SchemaPlusImpl rather than a mock.
    // With a mock SchemaPlus, it will throw ClassCastException which is caught in the
    // catch block, so schema.add() is never called. The method should complete without
    // throwing (graceful error handling).
    method.invoke(null, mockParentSchema, "test", tables, operand);
    // Verify the method did not throw - view registration failed gracefully
    // The add() is never called because ViewTable.viewMacro() fails with mock schema
    verify(foundSchema, never()).add(eq("my_view"), any(org.apache.calcite.schema.TableMacro.class));
  }

  // ====== supportsConstraints / setTableConstraints tests ======

  @Test
  void testSupportsConstraints() {
    assertTrue(FileSchemaFactory.INSTANCE.supportsConstraints());
  }

  @Test
  void testSetTableConstraints() {
    Map<String, Map<String, Object>> constraints = new HashMap<>();
    constraints.put("table1", new HashMap<>());
    FileSchemaFactory.INSTANCE.setTableConstraints(constraints, null);
  }

  @Test
  void testSetTableConstraintsNull() {
    FileSchemaFactory.INSTANCE.setTableConstraints(null, null);
  }

  // ====== addMetadataSchemas tests ======

  @Test
  void testAddMetadataSchemas() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("addMetadataSchemas", SchemaPlus.class);
    method.setAccessible(true);

    method.invoke(null, mockParentSchema);

    verify(mockParentSchema, atLeastOnce()).add(eq("information_schema"), any(Schema.class));
    verify(mockParentSchema, atLeastOnce()).add(eq("pg_catalog"), any(Schema.class));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  void testAddMetadataSchemasAlreadyExist() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("addMetadataSchemas", SchemaPlus.class);
    method.setAccessible(true);

    Lookup subSchemaLookup = mock(Lookup.class);
    when(subSchemaLookup.get("information_schema")).thenReturn(mock(SchemaPlus.class));
    when(subSchemaLookup.get("pg_catalog")).thenReturn(mock(SchemaPlus.class));
    when(subSchemaLookup.get("metadata")).thenReturn(null);
    when(subSchemaLookup.getNames(any(LikePattern.class))).thenReturn(Collections.emptySet());
    doReturn(subSchemaLookup).when(mockParentSchema).subSchemas();

    method.invoke(null, mockParentSchema);

    verify(mockParentSchema, never()).add(eq("information_schema"), any(Schema.class));
    verify(mockParentSchema, never()).add(eq("pg_catalog"), any(Schema.class));
  }

  // ====== S3 validation test ======

  @Test
  void testCreateSchemaS3WithoutCredentialsFails() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("storageType", "s3");
    operand.put("directory", "s3://my-bucket/data");

    try {
      FileSchemaFactory.INSTANCE.create(mockParentSchema, "test_schema", operand);
      fail("Expected exception for S3 without credentials");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("accessKeyId"));
    }
  }

  // ====== storageType missing test ======

  @Test
  void testCreateSchemaNoStorageTypeFails() {
    Map<String, Object> operand = new HashMap<>();

    try {
      FileSchemaFactory.INSTANCE.create(mockParentSchema, "test_schema", operand);
      fail("Expected exception for missing storageType");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("storageType"));
    }
  }

  // ====== writeDebugModel test ======

  @Test
  void testWriteDebugModel() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "writeDebugModel", String.class, Map.class, String.class);
    method.setAccessible(true);

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "/tmp/test");

    method.invoke(null, "test", operand, "root");
  }
}
