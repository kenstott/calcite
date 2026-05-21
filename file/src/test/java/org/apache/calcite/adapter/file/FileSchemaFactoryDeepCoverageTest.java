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
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link FileSchemaFactory} targeting uncovered code paths:
 * create() method branches, sanitizeOperand, sanitizeNestedMap, writeDebugModel,
 * parseBooleanValue, ephemeral vs persistent cache, storage type auto-detection,
 * execution engine resolution, directory pattern, and operand handling.
 */
@Tag("unit")
public class FileSchemaFactoryDeepCoverageTest {

  @TempDir
  Path tempDir;

  private SchemaPlus parentSchema;

  @SuppressWarnings({"deprecation", "unchecked"})
  @BeforeEach
  void setUp() {
    parentSchema = mock(SchemaPlus.class);
    when(parentSchema.getName()).thenReturn("root");
    when(parentSchema.getSubSchemaNames()).thenReturn(Collections.<String>emptySet());

    // Mock subSchemas() to return a Lookup that returns null for get() (no existing sub-schemas)
    Lookup<SchemaPlus> subSchemasLookup = mock(Lookup.class);
    when(subSchemasLookup.get(any(String.class))).thenReturn(null);
    when(subSchemasLookup.getNames(any(LikePattern.class)))
        .thenReturn(Collections.<String>emptySet());
    doReturn(subSchemasLookup).when(parentSchema).subSchemas();

    // Mock tables() to return a Lookup that returns empty names
    Lookup<Table> tablesLookup = mock(Lookup.class);
    when(tablesLookup.get(any(String.class))).thenReturn(null);
    when(tablesLookup.getNames(any(LikePattern.class)))
        .thenReturn(Collections.<String>emptySet());
    when(parentSchema.tables()).thenReturn(tablesLookup);

    // Mock getParentSchema() to return null (this is the root)
    when(parentSchema.getParentSchema()).thenReturn(null);
  }

  // --- create with minimal operand ---

  @Test void testCreateWithMinimalOperand() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_schema", operand);
    assertNotNull(schema);
    assertTrue(schema instanceof FileSchema);
  }

  // --- create with sourceDirectory ---

  @Test void testCreateWithSourceDirectory() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("sourceDirectory", tempDir.toString());

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_srcdir", operand);
    assertNotNull(schema);
  }

  // --- create with ephemeralCache ---

  @Test void testCreateWithEphemeralCache() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("ephemeralCache", true);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_ephemeral", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithEphemeralCacheSnakeCase() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("ephemeral_cache", true);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_ephemeral2", operand);
    assertNotNull(schema);
  }

  // --- create with explicit baseDirectory ---

  @Test void testCreateWithBaseDirectory() {
    File baseDir = tempDir.resolve("custom-base").toFile();
    baseDir.mkdirs();

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("baseDirectory", baseDir.getAbsolutePath());

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_basedir", operand);
    assertNotNull(schema);
  }

  // --- create with recursive ---

  @Test void testCreateWithRecursive() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("recursive", Boolean.TRUE);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_recursive", operand);
    assertNotNull(schema);
  }

  // --- create with executionEngine ---

  @Test void testCreateWithExecutionEngineLinq4j() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "LINQ4J");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_linq4j", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithExecutionEngineParquet() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("executionEngine", "PARQUET");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_parquet", operand);
    assertNotNull(schema);
  }

  // --- create with tables list ---

  @Test void testCreateWithTablesOperand() throws IOException {
    File sourceDir = tempDir.resolve("factory-tables").toFile();
    sourceDir.mkdirs();
    File csvFile = new File(sourceDir, "data.csv");
    FileWriter fw = new FileWriter(csvFile);
    fw.write("id,name\n1,Alice\n");
    fw.close();

    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", "my_table");
    tableDef.put("url", csvFile.getAbsolutePath());
    tables.add(tableDef);

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", sourceDir.getAbsolutePath());
    operand.put("tables", tables);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_tables", operand);
    assertNotNull(schema);
  }

  // --- create with materializations ---

  @Test void testCreateWithMaterializations() {
    List<Map<String, Object>> materializations = new ArrayList<>();
    Map<String, Object> mat = new HashMap<>();
    mat.put("table", "mat_table");
    mat.put("sql", "SELECT 1");
    materializations.add(mat);

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("materializations", materializations);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_mat", operand);
    assertNotNull(schema);
  }

  // --- create with views ---

  @Test void testCreateWithViews() {
    List<Map<String, Object>> views = new ArrayList<>();
    Map<String, Object> view = new HashMap<>();
    view.put("name", "my_view");
    view.put("sql", "SELECT 1");
    views.add(view);

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("views", views);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_views", operand);
    assertNotNull(schema);
  }

  // --- create with comment ---

  @Test void testCreateWithComment() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("comment", "This is a test schema");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_comment", operand);
    assertNotNull(schema);
    if (schema instanceof FileSchema) {
      assertEquals("This is a test schema", ((FileSchema) schema).getComment());
    }
  }

  // --- create with flatten ---

  @Test void testCreateWithFlatten() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("flatten", true);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_flatten", operand);
    assertNotNull(schema);
  }

  // --- create with directoryPattern ---

  @Test void testCreateWithDirectoryPattern() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("directoryPattern", "**/*.csv");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_pattern", operand);
    assertNotNull(schema);
  }

  @Test void testCreateWithGlob() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("glob", "*.json");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_glob", operand);
    assertNotNull(schema);
  }

  // --- create with batchSize and memoryThreshold ---

  @Test void testCreateWithBatchSizeAndMemoryThreshold() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("batchSize", 5000);
    operand.put("memoryThreshold", 1024L * 1024L * 512L);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_batch", operand);
    assertNotNull(schema);
  }

  // --- create with refreshInterval ---

  @Test void testCreateWithRefreshInterval() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("refreshInterval", "5 minutes");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_refresh", operand);
    assertNotNull(schema);
  }

  // --- create with tableNameCasing ---

  @Test void testCreateWithTableNameCasing() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("tableNameCasing", "UPPER");
    operand.put("columnNameCasing", "LOWER");

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_casing", operand);
    assertNotNull(schema);
  }

  // --- create with primeCache disabled ---

  @Test void testCreateWithPrimeCacheDisabled() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("primeCache", false);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_nocache", operand);
    assertNotNull(schema);
  }

  // --- sanitizeOperand via reflection ---

  @Test void testSanitizeOperand() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "/data");
    operand.put("password", "secret123");
    operand.put("secretKey", "supersecret");
    operand.put("normalKey", "normalValue");
    operand.put("nullKey", null);
    operand.put("_internalRef", new Object());

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);

    assertEquals("/data", result.get("directory"));
    assertEquals("********", result.get("password"));
    assertEquals("********", result.get("secretKey"));
    assertEquals("normalValue", result.get("normalKey"));
    assertNull(result.get("nullKey"));
    assertTrue(result.get("_internalRef") instanceof String, "Internal refs should be class name");
  }

  @Test void testSanitizeOperandWithS3Config() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);

    Map<String, Object> s3Config = new HashMap<>();
    s3Config.put("accessKeyId", "AKIAIOSFODNN7EXAMPLE");
    s3Config.put("secretAccessKey", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    s3Config.put("region", "us-east-1");

    Map<String, Object> operand = new HashMap<>();
    operand.put("s3Config", s3Config);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedS3 = (Map<String, Object>) result.get("s3Config");
    assertNotNull(sanitizedS3);
    // AccessKeyId should be masked (last 4 chars visible)
    String maskedKey = (String) sanitizedS3.get("accessKeyId");
    assertTrue(maskedKey.startsWith("****"));
    // SecretAccessKey should be fully masked
    assertEquals("********", sanitizedS3.get("secretAccessKey"));
    // Region should be unchanged
    assertEquals("us-east-1", sanitizedS3.get("region"));
  }

  @Test void testSanitizeOperandWithStorageConfig() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);

    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("endpoint", "https://s3.example.com");
    storageConfig.put("secretKey", "secret");
    storageConfig.put("_provider", new Object());

    Map<String, Object> operand = new HashMap<>();
    operand.put("storageConfig", storageConfig);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);

    @SuppressWarnings("unchecked")
    Map<String, Object> sanitizedStorage = (Map<String, Object>) result.get("storageConfig");
    assertNotNull(sanitizedStorage);
    assertEquals("https://s3.example.com", sanitizedStorage.get("endpoint"));
    assertEquals("********", sanitizedStorage.get("secretKey"));
  }

  // --- sanitizeNestedMap via reflection ---

  @Test void testSanitizeNestedMap() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeNestedMap", Map.class);
    method.setAccessible(true);

    Map<String, Object> nested = new HashMap<>();
    nested.put("normalKey", "value");
    nested.put("password", "secret");
    nested.put("accesskey", "key123");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, nested);

    assertEquals("value", result.get("normalKey"));
    assertEquals("********", result.get("password"));
    assertEquals("********", result.get("accesskey"));
  }

  // --- INSTANCE singleton ---

  @Test void testSingleton() {
    assertNotNull(FileSchemaFactory.INSTANCE);
    assertSame(FileSchemaFactory.INSTANCE, FileSchemaFactory.INSTANCE);
  }

  // --- create with baseDirectory as File object ---

  @Test void testCreateWithBaseDirectoryAsFile() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("baseDirectory", tempDir.toFile());

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_basedir_file", operand);
    assertNotNull(schema);
  }

  // --- create with partitionedTables ---

  @Test void testCreateWithPartitionedTables() {
    List<Map<String, Object>> partitioned = new ArrayList<>();
    Map<String, Object> pt = new HashMap<>();
    pt.put("name", "partitioned_table");
    pt.put("pattern", "*.parquet");
    partitioned.add(pt);

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("partitionedTables", partitioned);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_partitioned", operand);
    assertNotNull(schema);
  }

  // --- create with csvTypeInference ---

  @Test void testCreateWithCsvTypeInference() {
    Map<String, Object> csvTypeInference = new HashMap<>();
    csvTypeInference.put("enabled", true);
    csvTypeInference.put("sampleSize", 100);

    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", tempDir.toString());
    operand.put("csvTypeInference", csvTypeInference);

    Schema schema = FileSchemaFactory.INSTANCE.create(parentSchema, "test_csv_inference", operand);
    assertNotNull(schema);
  }

  // --- create with modelUri (inline model) ---

  @Test void testSanitizeOperandWithModelUri() throws Exception {
    Method method = FileSchemaFactory.class.getDeclaredMethod("sanitizeOperand", Map.class);
    method.setAccessible(true);

    Map<String, Object> operand = new HashMap<>();
    operand.put("modelUri", "inline:{\"schemas\":[{\"operands\":{\"accessKeyId\":\"AKID\",\"secretAccessKey\":\"SECRET\"}}]}");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(null, operand);

    String sanitizedUri = (String) result.get("modelUri");
    assertNotNull(sanitizedUri);
    assertFalse(sanitizedUri.contains("AKID"));
    assertFalse(sanitizedUri.contains("SECRET"));
  }
}
