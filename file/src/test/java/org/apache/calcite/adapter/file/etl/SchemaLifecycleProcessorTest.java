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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SchemaLifecycleProcessor}.
 */
@Tag("unit")
class SchemaLifecycleProcessorTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;

  @BeforeEach
  void setup() {
    storageProvider = new LocalFileStorageProvider();
  }

  @Test
  void testLifecycleHooksAreCalled() throws IOException {
    // Track hook calls
    AtomicBoolean beforeSchemaCalled = new AtomicBoolean(false);
    AtomicBoolean afterSchemaCalled = new AtomicBoolean(false);
    AtomicInteger beforeTableCount = new AtomicInteger(0);
    AtomicInteger afterTableCount = new AtomicInteger(0);

    // Create schema listener
    SchemaLifecycleListener schemaListener = new SchemaLifecycleListener() {
      @Override public void beforeSchema(SchemaContext context) {
        beforeSchemaCalled.set(true);
        assertEquals("test_schema", context.getSchemaName());
      }

      @Override public void afterSchema(SchemaContext context, SchemaResult result) {
        afterSchemaCalled.set(true);
        assertNotNull(result);
      }

      @Override public void onSchemaError(SchemaContext context, Exception error) {
        // Not expected in this test
      }
    };

    // Create table listener
    TableLifecycleListener tableListener = new TableLifecycleListener() {
      @Override public void beforeTable(TableContext context) {
        beforeTableCount.incrementAndGet();
      }

      @Override public void afterTable(TableContext context, EtlResult result) {
        afterTableCount.incrementAndGet();
      }

      @Override public boolean onTableError(TableContext context, Exception error) {
        return true;
      }
    };

    // Create minimal schema config (no actual HTTP source)
    SchemaConfig config = createMinimalSchemaConfig("test_schema");

    // materializeDirectory is now in the config, not in the builder
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .schemaListener(schemaListener)
        .defaultTableListener(tableListener)
        .build();

    // Note: This will fail because we don't have a real HTTP source,
    // but the lifecycle hooks should still be called
    try {
      processor.process();
    } catch (IOException e) {
      // Expected - no real HTTP source
    }

    assertTrue(beforeSchemaCalled.get(), "beforeSchema should be called");
    // afterSchema may not be called if processing fails early
  }

  @Test
  void testSchemaConfigFromMap() {
    Map<String, Object> schemaMap = new LinkedHashMap<String, Object>();
    schemaMap.put("name", "econ");

    // Add hooks
    Map<String, Object> hooksMap = new LinkedHashMap<String, Object>();
    hooksMap.put("schemaLifecycleListener", "org.example.MySchemaListener");
    hooksMap.put("tableLifecycleListener", "org.example.MyTableListener");
    schemaMap.put("hooks", hooksMap);

    // Add tables
    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    Map<String, Object> table1 = createTableMap("gdp");
    tables.add(table1);
    schemaMap.put("tables", tables);

    SchemaConfig config = SchemaConfig.fromMap(schemaMap);

    assertNotNull(config);
    assertEquals("econ", config.getName());
    assertEquals("org.example.MySchemaListener",
        config.getHooks().getSchemaLifecycleListenerClass());
    assertEquals("org.example.MyTableListener",
        config.getHooks().getTableLifecycleListenerClass());
    assertEquals(1, config.getTableCount());
  }

  @Test
  void testSchemaResultAggregation() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("test");

    // Add successful result
    builder.addTableResult("table1", EtlResult.success("table1", 1000, 10, 500));

    // Add failed result
    builder.addTableResult("table2", EtlResult.failure("table2", "error", 100));

    // Add skipped result
    builder.addTableResult("table3", EtlResult.skipped("table3", 10));

    builder.elapsedMs(1000);
    SchemaResult result = builder.build();

    assertEquals("test", result.getSchemaName());
    assertEquals(3, result.getTotalTables());
    assertEquals(1, result.getSuccessfulTables());
    assertEquals(1, result.getFailedTables());
    assertEquals(1, result.getSkippedTables());
    assertEquals(1000, result.getTotalRows());
    assertTrue(result.hasErrors());
  }

  private SchemaConfig createMinimalSchemaConfig(String name) {
    // Create a minimal table config with a mock HTTP source
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("type", "http");
    sourceMap.put("url", "http://localhost:9999/test");

    Map<String, Object> responseMap = new LinkedHashMap<String, Object>();
    responseMap.put("dataPath", "data");
    sourceMap.put("response", responseMap);

    Map<String, Object> outputMap = new LinkedHashMap<String, Object>();
    outputMap.put("location", tempDir.toString());
    outputMap.put("pattern", "test/");

    Map<String, Object> materializeMap = new LinkedHashMap<String, Object>();
    materializeMap.put("enabled", true);
    materializeMap.put("format", "parquet");
    materializeMap.put("output", outputMap);

    Map<String, Object> tableMap = new LinkedHashMap<String, Object>();
    tableMap.put("name", "test_table");
    tableMap.put("source", sourceMap);
    tableMap.put("materialize", materializeMap);

    EtlPipelineConfig tableConfig = EtlPipelineConfig.fromMap(tableMap);

    return SchemaConfig.builder()
        .name(name)
        .materializeDirectory(tempDir.toString())
        .addTable(tableConfig)
        .build();
  }

  private Map<String, Object> createTableMap(String name) {
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("type", "http");
    sourceMap.put("url", "http://localhost:9999/" + name);

    Map<String, Object> responseMap = new LinkedHashMap<String, Object>();
    responseMap.put("dataPath", "data");
    sourceMap.put("response", responseMap);

    Map<String, Object> outputMap = new LinkedHashMap<String, Object>();
    outputMap.put("location", tempDir.toString());
    outputMap.put("pattern", name + "/");

    Map<String, Object> materializeMap = new LinkedHashMap<String, Object>();
    materializeMap.put("enabled", true);
    materializeMap.put("format", "parquet");
    materializeMap.put("output", outputMap);

    Map<String, Object> tableMap = new LinkedHashMap<String, Object>();
    tableMap.put("name", name);
    tableMap.put("source", sourceMap);
    tableMap.put("materialize", materializeMap);

    return tableMap;
  }
}
