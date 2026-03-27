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
import org.apache.calcite.schema.lookup.Named;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doReturn;

/**
 * Deep coverage tests for {@link FileSchemaFactory}.
 * Tests sanitizeOperand, parseBooleanValue, writeDebugModel, validateUniqueSchemaName,
 * create() with various operand configurations, rewriteSchemaReferencesInSql,
 * rewriteForeignKeySchemaNames, registerSqlViews, and other utility methods.
 */
@Tag("unit")
class FileSchemaFactoryDeepCoverageTest2 {

  @TempDir
  Path tempDir;

  private FileSchemaFactory factory;

  @BeforeEach
  void setup() {
    factory = FileSchemaFactory.INSTANCE;
  }

  // ======= parseBooleanValue =======

  @Test
  void testParseBooleanValueNull() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);

    assertNull(method.invoke(null, (Object) null));
  }

  @Test
  void testParseBooleanValueBoolean() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);

    assertEquals(Boolean.TRUE, method.invoke(null, Boolean.TRUE));
    assertEquals(Boolean.FALSE, method.invoke(null, Boolean.FALSE));
  }

  @Test
  void testParseBooleanValueString() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);

    assertEquals(Boolean.TRUE, method.invoke(null, "true"));
    assertEquals(Boolean.FALSE, method.invoke(null, "false"));
    assertEquals(Boolean.FALSE, method.invoke(null, "not_a_boolean"));
  }

  @Test
  void testParseBooleanValueOther() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("parseBooleanValue", Object.class);
    method.setAccessible(true);

    assertNull(method.invoke(null, 42));
    assertNull(method.invoke(null, 3.14));
  }

  // ======= sanitizeOperand =======

  @Test
  void testSanitizeOperandBasic() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", "/tmp/data");
    operand.put("password", "secret123");
    operand.put("secretKey", "xyz");
    operand.put("nullValue", null);
    operand.put("_internalObj", new Object());

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);

    assertEquals("/tmp/data", result.get("directory"));
    assertEquals("********", result.get("password"));
    assertEquals("********", result.get("secretKey"));
    assertNull(result.get("nullValue"));
    // _internalObj should have class simple name
    assertNotNull(result.get("_internalObj"));
  }

  @Test
  void testSanitizeOperandS3Config() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);

    Map<String, Object> s3Config = new HashMap<String, Object>();
    s3Config.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
    s3Config.put("secretAccessKey", "secret");
    s3Config.put("region", "us-east-1");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("s3Config", s3Config);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) result.get("s3Config");
    assertNotNull(sanitizedS3);
    assertTrue(((String) sanitizedS3.get("accessKeyId")).startsWith("****"));
    assertEquals("********", sanitizedS3.get("secretAccessKey"));
    assertEquals("us-east-1", sanitizedS3.get("region"));
  }

  @Test
  void testSanitizeOperandS3ConfigShortAccessKey() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);

    Map<String, Object> s3Config = new HashMap<String, Object>();
    s3Config.put("accessKeyId", "AK"); // Short key (< 4 chars)
    s3Config.put("region", "eu-west-1");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("s3Config", s3Config);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) result.get("s3Config");
    assertEquals("****", sanitizedS3.get("accessKeyId"));
  }

  @Test
  void testSanitizeOperandStorageConfig() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);

    Map<String, Object> storageConfig = new HashMap<String, Object>();
    storageConfig.put("_provider", new Object());
    storageConfig.put("secretKey", "secret");
    storageConfig.put("passwordValue", "pass");
    storageConfig.put("accessKeyId", "AKID");
    storageConfig.put("region", "us-west-2");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("storageConfig", storageConfig);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedStorage = (Map<String, Object>) result.get("storageConfig");
    assertNotNull(sanitizedStorage);
    assertEquals("********", sanitizedStorage.get("secretKey"));
    assertEquals("********", sanitizedStorage.get("passwordValue"));
    assertEquals("********", sanitizedStorage.get("accessKeyId"));
    assertEquals("us-west-2", sanitizedStorage.get("region"));
  }

  @Test
  void testSanitizeOperandModelUri() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("modelUri", "inline:{\"accessKeyId\": \"AKID\", \"secretAccessKey\": \"secret\"}");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);

    String sanitizedUri = (String) result.get("modelUri");
    assertNotNull(sanitizedUri);
    // The accessKeyId value "AKID" should be replaced with "****"
    assertFalse(sanitizedUri.contains("AKID"));
    // The key name "secretAccessKey" will still be present, but the value "secret" is replaced
    // Check that the secret value is no longer present as a standalone JSON value
    assertFalse(sanitizedUri.contains("\"secretAccessKey\": \"secret\""));
    assertTrue(sanitizedUri.contains("****"));
    assertTrue(sanitizedUri.contains("********"));
  }

  @Test
  void testSanitizeOperandNestedMap() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);

    Map<String, Object> nested = new HashMap<String, Object>();
    nested.put("normalKey", "normalValue");
    nested.put("secretField", "hidden");

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("someConfig", nested);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedNested = (Map<String, Object>) result.get("someConfig");
    assertNotNull(sanitizedNested);
    assertEquals("normalValue", sanitizedNested.get("normalKey"));
    assertEquals("********", sanitizedNested.get("secretField"));
  }

  // ======= sanitizeNestedMap =======

  @Test
  void testSanitizeNestedMapWithCalciteClass() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeNestedMap", Map.class);
    method.setAccessible(true);

    Map<String, Object> nested = new HashMap<String, Object>();
    nested.put("normalKey", "val");
    nested.put("accessKey", "secret_key");
    // Cannot easily create an org.apache.calcite class, but we can test with null values
    nested.put("nullKey", null);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, nested);
    assertEquals("val", result.get("normalKey"));
    assertEquals("********", result.get("accessKey"));
  }

  // ======= writeDebugModel =======

  @Test
  void testWriteDebugModel() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "writeDebugModel", String.class, Map.class, String.class);
    method.setAccessible(true);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");

    // This should not throw - failures are caught internally
    method.invoke(null, "test_schema", operand, "parent");
  }

  // ======= validateUniqueSchemaName =======

  @Test
  void testValidateUniqueSchemaNameNull() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "validateUniqueSchemaName", SchemaPlus.class, String.class);
    method.setAccessible(true);

    // Should not throw for null parent or null name
    method.invoke(null, (SchemaPlus) null, "test");
    method.invoke(null, mock(SchemaPlus.class), (String) null);
  }

  @Test
  void testValidateUniqueSchemaNameDuplicate() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "validateUniqueSchemaName", SchemaPlus.class, String.class);
    method.setAccessible(true);

    SchemaPlus parentSchema = mock(SchemaPlus.class);
    @SuppressWarnings("unchecked")
    Lookup<SchemaPlus> subSchemaLookup = mock(Lookup.class);
    doReturn(subSchemaLookup).when(parentSchema).subSchemas();
    when(subSchemaLookup.get("existing")).thenReturn(mock(SchemaPlus.class));
    Set<String> names = new HashSet<String>();
    names.add("existing");
    when(subSchemaLookup.getNames(any(LikePattern.class))).thenReturn(names);

    try {
      method.invoke(null, parentSchema, "existing");
      fail("Should throw for duplicate schema name");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertTrue(e.getCause().getMessage().contains("already exists"));
    }
  }

  // ======= rewriteSchemaReferencesInSql =======

  @Test
  void testRewriteSchemaReferencesInSql() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "rewriteSchemaReferencesInSql", String.class, String.class, String.class);
    method.setAccessible(true);

    // Null inputs
    assertNull(method.invoke(null, null, "econ", "ECON"));
    assertEquals("SELECT * FROM tbl", method.invoke(null, "SELECT * FROM tbl", null, "ECON"));
    assertEquals("SELECT * FROM tbl", method.invoke(null, "SELECT * FROM tbl", "econ", null));

    // Same schema names (case insensitive)
    String sql = "SELECT * FROM econ.gdp";
    assertEquals(sql, method.invoke(null, sql, "econ", "Econ"));

    // Schema names that match case-insensitively are NOT rewritten
    assertEquals("SELECT * FROM econ.gdp",
        method.invoke(null, "SELECT * FROM econ.gdp", "econ", "ECON"));
  }

  // ======= rewriteForeignKeySchemaNames =======

  @Test
  void testRewriteForeignKeySchemaNames() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "rewriteForeignKeySchemaNames", Map.class, String.class, String.class);
    method.setAccessible(true);

    // Null or empty
    assertNull(method.invoke(null, null, "econ", "ECON"));
    assertTrue(((Map<?, ?>) method.invoke(null, Collections.emptyMap(), "econ", "ECON")).isEmpty());

    // Table with no foreign keys
    Map<String, Map<String, Object>> constraints = new HashMap<String, Map<String, Object>>();
    Map<String, Object> tableConstraint = new HashMap<String, Object>();
    tableConstraint.put("primaryKey", "id");
    constraints.put("table1", tableConstraint);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result1 =
        (Map<String, Map<String, Object>>) method.invoke(null, constraints, "econ", "ECON");
    assertNotNull(result1);

    // Table with foreign keys matching declared schema
    Map<String, Object> fk1 = new HashMap<String, Object>();
    fk1.put("targetSchema", "econ");
    fk1.put("targetTable", "gdp");
    List<Map<String, Object>> foreignKeys = new ArrayList<Map<String, Object>>();
    foreignKeys.add(fk1);

    Map<String, Object> tc2 = new HashMap<String, Object>();
    tc2.put("foreignKeys", foreignKeys);
    constraints.put("table2", tc2);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result2 =
        (Map<String, Map<String, Object>>) method.invoke(null, constraints, "econ", "ECON");
    assertNotNull(result2);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> rewrittenFks =
        (List<Map<String, Object>>) result2.get("table2").get("foreignKeys");
    assertEquals("ECON", rewrittenFks.get(0).get("targetSchema"));

    // Foreign key with different schema (should not be rewritten)
    Map<String, Object> fk2 = new HashMap<String, Object>();
    fk2.put("targetSchema", "other_schema");
    fk2.put("targetTable", "other_table");
    foreignKeys.add(fk2);

    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> result3 =
        (Map<String, Map<String, Object>>) method.invoke(null, constraints, "econ", "ECON");
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> rewrittenFks2 =
        (List<Map<String, Object>>) result3.get("table2").get("foreignKeys");
    assertEquals("other_schema", rewrittenFks2.get(1).get("targetSchema"));
  }

  // ======= registerSqlViews =======

  @Test
  void testRegisterSqlViewsNullTables() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    method.setAccessible(true);

    SchemaPlus parentSchema = mock(SchemaPlus.class);

    // Should not throw for null tables
    method.invoke(null, parentSchema, "test", null, Collections.emptyMap());
    method.invoke(null, parentSchema, "test", Collections.emptyList(), Collections.emptyMap());
  }

  @Test
  void testRegisterSqlViewsSchemaNotFound() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    method.setAccessible(true);

    SchemaPlus parentSchema = mock(SchemaPlus.class);
    @SuppressWarnings("unchecked")
    Lookup<SchemaPlus> subSchemaLookup = mock(Lookup.class);
    doReturn(subSchemaLookup).when(parentSchema).subSchemas();
    when(subSchemaLookup.get("mySchema")).thenReturn(null); // Schema not found

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    Map<String, Object> viewDef = new HashMap<String, Object>();
    viewDef.put("type", "view");
    viewDef.put("name", "myView");
    viewDef.put("sql", "SELECT * FROM t1");
    tables.add(viewDef);

    // Should not throw - just logs a warning
    method.invoke(null, parentSchema, "mySchema", tables, Collections.emptyMap());
  }

  @Test
  void testRegisterSqlViewsSkipsNonViews() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod(
        "registerSqlViews", SchemaPlus.class, String.class, List.class, Map.class);
    method.setAccessible(true);

    SchemaPlus parentSchema = mock(SchemaPlus.class);

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    Map<String, Object> tableDef = new HashMap<String, Object>();
    tableDef.put("type", "table");
    tableDef.put("name", "myTable");
    tables.add(tableDef);

    @SuppressWarnings("unchecked")
    Lookup<SchemaPlus> subSchemaLookup = mock(Lookup.class);
    doReturn(subSchemaLookup).when(parentSchema).subSchemas();
    SchemaPlus schemaPlus = mock(SchemaPlus.class);
    when(subSchemaLookup.get("testSchema")).thenReturn(schemaPlus);

    // Should skip non-view entries without error
    method.invoke(null, parentSchema, "testSchema", tables, new HashMap<String, Object>());
  }

  // ======= supportsConstraints =======

  @Test
  void testSupportsConstraints() {
    assertTrue(factory.supportsConstraints());
  }

  // ======= setTableConstraints =======

  @Test
  void testSetTableConstraints() {
    Map<String, Map<String, Object>> constraints = new HashMap<String, Map<String, Object>>();
    Map<String, Object> tc = new HashMap<String, Object>();
    tc.put("primaryKey", "id");
    constraints.put("myTable", tc);

    factory.setTableConstraints(constraints, null);
    // Constraints are stored internally - no direct getter, but this exercises the code
  }

  // ======= create() with local storageType and ephemeralCache =======

  @Test
  void testCreateWithEphemeralCache() {
    // Create a data file
    File csvFile = tempDir.resolve("test.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,name\n1,Alice\n2,Bob\n");
    } catch (Exception e) {
      fail("Failed to create test CSV file: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeralCache", true);

    Schema schema = factory.create(parentSchema, "ephemeral_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithEphemeralCacheString() {
    File csvFile = tempDir.resolve("test2.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,value\n1,10\n2,20\n");
    } catch (Exception e) {
      fail("Failed to create test CSV file: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("ephemeral_cache", "true"); // snake_case string

    Schema schema = factory.create(parentSchema, "ephemeral_test2", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithBaseDirectoryString() {
    File csvFile = tempDir.resolve("data.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("col1,col2\na,b\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("baseDirectory", tempDir.resolve("base").toString());

    Schema schema = factory.create(parentSchema, "base_dir_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithBaseDirectoryFile() {
    File csvFile = tempDir.resolve("file_data.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("x,y\n1,2\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("baseDirectory", tempDir.resolve("base_file").toFile());

    Schema schema = factory.create(parentSchema, "base_dir_file_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateAutoDetectsLocalStorage() {
    File csvFile = tempDir.resolve("auto.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("a,b\n1,2\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    // No explicit storageType - should auto-detect "local"

    Schema schema = factory.create(parentSchema, "autodetect_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithSourceDirectoryAlternative() {
    File csvFile = tempDir.resolve("alt.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("c1,c2\nv1,v2\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("sourceDirectory", tempDir.toString()); // Use sourceDirectory instead of directory
    operand.put("storageType", "local");

    Schema schema = factory.create(parentSchema, "src_dir_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithBatchSizeAndMemoryThreshold() {
    File csvFile = tempDir.resolve("batch.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,val\n1,100\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("batchSize", 500);
    operand.put("memoryThreshold", 1024L * 1024L);

    Schema schema = factory.create(parentSchema, "batch_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithRecursiveAndGlob() {
    File csvFile = tempDir.resolve("recurse.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,name\n1,Test\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("recursive", Boolean.TRUE);
    operand.put("glob", "*.csv");

    Schema schema = factory.create(parentSchema, "recursive_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithRefreshInterval() {
    File csvFile = tempDir.resolve("refresh.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,value\n1,42\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("refreshInterval", "5m");

    Schema schema = factory.create(parentSchema, "refresh_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithFlatten() {
    File csvFile = tempDir.resolve("flat.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,nested\n1,val\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("flatten", Boolean.TRUE);

    Schema schema = factory.create(parentSchema, "flatten_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithTableNameCasingSnakeCase() {
    File csvFile = tempDir.resolve("casing.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,val\n1,2\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("table_name_casing", "LOWER_CASE");
    operand.put("column_name_casing", "LOWER_CASE");

    Schema schema = factory.create(parentSchema, "casing_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithPrimeCacheFalse() {
    File csvFile = tempDir.resolve("nocache.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("x,y\n1,2\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("primeCache", Boolean.FALSE);

    Schema schema = factory.create(parentSchema, "nocache_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithPrimeCacheSnakeCase() {
    File csvFile = tempDir.resolve("nocache2.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("a,b\n1,2\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("prime_cache", Boolean.FALSE);

    Schema schema = factory.create(parentSchema, "nocache2_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithComment() {
    File csvFile = tempDir.resolve("comment.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,text\n1,hello\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("comment", "This is a test schema");

    Schema schema = factory.create(parentSchema, "comment_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithCanonicalSchemaName() {
    File csvFile = tempDir.resolve("canonical.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,val\n1,42\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("canonicalSchemaName", "my_canonical");

    Schema schema = factory.create(parentSchema, "canonical_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithDeclaredSchemaNameDifferent() {
    File csvFile = tempDir.resolve("declared.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,val\n1,42\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    // Set up constraints with FK references
    Map<String, Map<String, Object>> constraints = new HashMap<String, Map<String, Object>>();
    Map<String, Object> tc = new HashMap<String, Object>();
    List<Map<String, Object>> fks = new ArrayList<Map<String, Object>>();
    Map<String, Object> fk = new HashMap<String, Object>();
    fk.put("targetSchema", "econ");
    fk.put("targetTable", "gdp");
    fks.add(fk);
    tc.put("foreignKeys", fks);
    constraints.put("declared", tc);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("declaredSchemaName", "econ");
    operand.put("tableConstraints", constraints);

    Schema schema = factory.create(parentSchema, "ECON_UPPER", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithNoDirectory() {
    SchemaPlus parentSchema = createMockParentSchema();

    // When no directory and no model file path, uses cwd
    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("storageType", "local");

    Schema schema = factory.create(parentSchema, "nodir_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateS3RequiresCredentials() {
    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", "s3://my-bucket/data");
    // No storageConfig with credentials

    assertThrows(IllegalArgumentException.class, () ->
        factory.create(parentSchema, "s3_no_creds", operand));
  }

  @Test
  void testCreateWithCsvTypeInference() {
    File csvFile = tempDir.resolve("types.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("id,amount\n1,3.14\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> csvTypeInference = new HashMap<String, Object>();
    csvTypeInference.put("sampleSize", 100);

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("csvTypeInference", csvTypeInference);

    Schema schema = factory.create(parentSchema, "csv_type_test", operand);
    assertNotNull(schema);
  }

  @Test
  void testCreateWithModelBaseDirectory() {
    File csvFile = tempDir.resolve("model.csv").toFile();
    try (FileWriter fw = new FileWriter(csvFile)) {
      fw.write("a,b\n1,2\n");
    } catch (Exception e) {
      fail("Failed to create test CSV: " + e.getMessage());
    }

    SchemaPlus parentSchema = createMockParentSchema();

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("directory", tempDir.toString());
    operand.put("storageType", "local");
    operand.put("baseDirectory", tempDir.toFile()); // File type baseDirectory

    Schema schema = factory.create(parentSchema, "model_base_test", operand);
    assertNotNull(schema);
  }

  // ======= addMetadataSchemas =======

  @Test
  void testAddMetadataSchemasIdempotent() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("addMetadataSchemas", SchemaPlus.class);
    method.setAccessible(true);

    SchemaPlus parentSchema = mock(SchemaPlus.class);
    @SuppressWarnings("unchecked")
    Lookup<SchemaPlus> subSchemaLookup = mock(Lookup.class);
    doReturn(subSchemaLookup).when(parentSchema).subSchemas();
    when(subSchemaLookup.get("information_schema")).thenReturn(null);
    when(subSchemaLookup.get("pg_catalog")).thenReturn(null);
    when(subSchemaLookup.get("metadata")).thenReturn(null);
    Set<String> emptyNames = Collections.emptySet();
    when(subSchemaLookup.getNames(any(LikePattern.class))).thenReturn(emptyNames);
    when(parentSchema.getParentSchema()).thenReturn(null);

    // Mock tables() lookup - needed by InformationSchema constructor
    @SuppressWarnings("unchecked")
    Lookup<Table> tableLookup = mock(Lookup.class);
    when(parentSchema.tables()).thenReturn(tableLookup);
    when(tableLookup.getNames(any(LikePattern.class))).thenReturn(Collections.<String>emptySet());

    // Mock add() to return a mock SchemaPlus
    when(parentSchema.add(anyString(), any(Schema.class))).thenReturn(mock(SchemaPlus.class));

    method.invoke(null, parentSchema);

    // Second call should not add again (simulated by returning non-null)
    SchemaPlus mockInfoSchema = mock(SchemaPlus.class);
    when(subSchemaLookup.get("information_schema")).thenReturn(mockInfoSchema);
    SchemaPlus mockPgSchema = mock(SchemaPlus.class);
    when(subSchemaLookup.get("pg_catalog")).thenReturn(mockPgSchema);

    method.invoke(null, parentSchema);
    // Should not throw and should be idempotent
  }

  // ======= Helper methods =======

  private SchemaPlus createMockParentSchema() {
    SchemaPlus parentSchema = mock(SchemaPlus.class);

    @SuppressWarnings("unchecked")
    Lookup<SchemaPlus> subSchemaLookup = mock(Lookup.class);
    doReturn(subSchemaLookup).when(parentSchema).subSchemas();
    when(subSchemaLookup.get(anyString())).thenReturn(null);
    Set<String> emptyNames = Collections.emptySet();
    when(subSchemaLookup.getNames(any(LikePattern.class))).thenReturn(emptyNames);

    @SuppressWarnings("unchecked")
    Lookup<Table> tableLookup = mock(Lookup.class);
    when(parentSchema.tables()).thenReturn(tableLookup);
    when(tableLookup.getNames(any(LikePattern.class))).thenReturn(Collections.<String>emptySet());

    when(parentSchema.getParentSchema()).thenReturn(null);
    when(parentSchema.getName()).thenReturn("root");

    // When add is called, return a mock SchemaPlus
    when(parentSchema.add(anyString(), any(Schema.class))).thenReturn(mock(SchemaPlus.class));

    return parentSchema;
  }
}
