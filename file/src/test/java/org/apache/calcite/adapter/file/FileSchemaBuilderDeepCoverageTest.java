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

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link FileSchemaBuilder}.
 * Covers builder fluent API, env var resolution, operand overrides, and error conditions.
 */
@Tag("unit")
class FileSchemaBuilderDeepCoverageTest {

  @TempDir
  Path tempDir;

  // ===== Basic Builder API =====

  @Test void testCreateBuilder() {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertNotNull(builder);
  }

  @Test void testSchemaConfigFromMap() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", "/data");

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config);
    assertNotNull(builder);
  }

  @Test void testOperandSingleValue() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .operand("key1", "value1")
        .operand("key2", 42);
    assertNotNull(builder);

    Map<String, Object> operand = builder.getOperand();
    assertEquals("value1", operand.get("key1"));
    assertEquals(42, operand.get("key2"));
  }

  @Test void testOperandMap() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");

    Map<String, Object> overrides = new HashMap<String, Object>();
    overrides.put("key1", "value1");
    overrides.put("key2", "value2");

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .operand(overrides);

    Map<String, Object> operand = builder.getOperand();
    assertEquals("value1", operand.get("key1"));
    assertEquals("value2", operand.get("key2"));
  }

  @Test void testExcludeTable() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .excludeTable("table1");

    Map<String, Object> operand = builder.getOperand();
    assertTrue(operand.containsKey("_excludedTables"));
  }

  @Test void testExcludeTables() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .excludeTables(Arrays.asList("table1", "table2"));

    Map<String, Object> operand = builder.getOperand();
    assertTrue(operand.containsKey("_excludedTables"));
    @SuppressWarnings("unchecked")
    List<String> excluded = (List<String>) operand.get("_excludedTables");
    assertEquals(2, excluded.size());
  }

  // ===== Storage Provider =====

  @Test void testStorageProvider() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");

    StorageProvider mockProvider =
        org.mockito.Mockito.mock(StorageProvider.class);

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .storageProvider(mockProvider);

    Map<String, Object> operand = builder.getOperand();
    assertEquals(mockProvider, operand.get("_storageProvider"));
  }

  @Test void testCacheStorageProvider() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");

    StorageProvider mockProvider =
        org.mockito.Mockito.mock(StorageProvider.class);

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .cacheStorageProvider(mockProvider);

    Map<String, Object> operand = builder.getOperand();
    assertEquals(mockProvider, operand.get("_cacheStorageProvider"));
  }

  @Test void testIncrementalTracker() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .incrementalTracker(IncrementalTracker.NOOP);

    Map<String, Object> operand = builder.getOperand();
    assertEquals(IncrementalTracker.NOOP, operand.get("_incrementalTracker"));
  }

  // ===== Error conditions =====

  @Test void testGetOperandNoConfig() {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertThrows(IllegalStateException.class, builder::getOperand);
  }

  @Test void testBuildNoConfig() {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertThrows(IllegalStateException.class, () ->
        builder.build(null, "test"));
  }

  @Test void testRunEtlNoConfig() {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertThrows(IllegalStateException.class, builder::runEtl);
  }

  // ===== resolveAllEnvVars via Reflection =====

  @Test void testResolveAllEnvVarsViaReflection() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();

    Method resolveAll = FileSchemaBuilder.class.getDeclaredMethod(
        "resolveAllEnvVars", Map.class);
    resolveAll.setAccessible(true);

    Map<String, Object> input = new LinkedHashMap<String, Object>();
    input.put("plain", "hello");
    input.put("number", 42);

    @SuppressWarnings("unchecked")
    Map<String, Object> resolved = (Map<String, Object>) resolveAll.invoke(builder, input);
    assertEquals("hello", resolved.get("plain"));
    assertEquals(42, resolved.get("number"));
  }

  @Test void testResolveAllEnvVarsNestedMap() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();

    Method resolveAll = FileSchemaBuilder.class.getDeclaredMethod(
        "resolveAllEnvVars", Map.class);
    resolveAll.setAccessible(true);

    Map<String, Object> nested = new LinkedHashMap<String, Object>();
    nested.put("inner", "value");

    Map<String, Object> input = new LinkedHashMap<String, Object>();
    input.put("outer", nested);

    @SuppressWarnings("unchecked")
    Map<String, Object> resolved = (Map<String, Object>) resolveAll.invoke(builder, input);
    assertTrue(resolved.get("outer") instanceof Map);
  }

  @Test void testResolveAllEnvVarsWithList() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();

    Method resolveAll = FileSchemaBuilder.class.getDeclaredMethod(
        "resolveAllEnvVars", Map.class);
    resolveAll.setAccessible(true);

    List<Object> list = new ArrayList<Object>();
    list.add("item1");
    list.add(42);

    Map<String, Object> input = new LinkedHashMap<String, Object>();
    input.put("items", list);

    @SuppressWarnings("unchecked")
    Map<String, Object> resolved = (Map<String, Object>) resolveAll.invoke(builder, input);
    assertTrue(resolved.get("items") instanceof List);
  }

  // ===== resolveListEnvVars via Reflection =====

  @Test void testResolveListEnvVarsViaReflection() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();

    Method resolveList = FileSchemaBuilder.class.getDeclaredMethod(
        "resolveListEnvVars", java.util.List.class);
    resolveList.setAccessible(true);

    List<Object> input = new ArrayList<Object>();
    input.add("string_item");
    input.add(123);

    Map<String, Object> nestedMap = new HashMap<String, Object>();
    nestedMap.put("key", "value");
    input.add(nestedMap);

    List<Object> nestedList = new ArrayList<Object>();
    nestedList.add("nested");
    input.add(nestedList);

    @SuppressWarnings("unchecked")
    List<Object> resolved = (List<Object>) resolveList.invoke(builder, input);
    assertEquals(4, resolved.size());
    assertEquals("string_item", resolved.get(0));
    assertEquals(123, resolved.get(1));
  }

  // ===== autoDownload =====

  @Test void testAutoDownloadFlag() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .autoDownload(false);
    assertNotNull(builder);

    // Should not throw - ETL not executed
    Map<String, Object> operand = builder.getOperand();
    assertNotNull(operand);
  }

  // ===== materializeDirectory in getOperand =====

  @Test void testMaterializeDirectoryOverridesDirectory() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", "/original");
    config.put("materializeDirectory", "/materialized");

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config);

    Map<String, Object> operand = builder.getOperand();
    assertEquals("/materialized", operand.get("directory"));
  }
}
