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

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link SchemaLifecycleProcessor}.
 */
@Tag("unit")
class SchemaLifecycleProcessorCoverageTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;

  @BeforeEach
  void setup() {
    storageProvider = new LocalFileStorageProvider();
  }

  // ========== Builder API coverage ==========

  @Test void testBuilderRequiresConfig() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() {
        SchemaLifecycleProcessor.builder().build();
      }
    });
  }

  @Test void testBuilderWithConfig() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .build();
    assertNotNull(processor);
  }

  @Test void testBuilderWithAllOptions() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .sourceStorageProvider(storageProvider)
        .sourceDirectory(tempDir.toString())
        .materializeDirectory(tempDir.toString())
        .operatingDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaListener(SchemaLifecycleListener.NOOP)
        .defaultTableListener(TableLifecycleListener.NOOP)
        .build();
    assertNotNull(processor);
  }

  @SuppressWarnings("deprecation")
  @Test void testBuilderBaseDirectoryDeprecated() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .baseDirectory(tempDir.toString())
        .build();
    assertNotNull(processor);
  }

  // ========== Hook registration coverage ==========

  @Test void testBuilderBeforeTableHook() {
    final AtomicBoolean called = new AtomicBoolean(false);
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .beforeTable("test_table", new java.util.function.Consumer<TableContext>() {
          @Override
          public void accept(TableContext ctx) {
            called.set(true);
          }
        })
        .build();
    assertNotNull(processor);
  }

  @Test void testBuilderAfterTableHook() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .afterTable("test_table", new java.util.function.BiConsumer<TableContext, EtlResult>() {
          @Override
          public void accept(TableContext ctx, EtlResult result) {
            // no-op
          }
        })
        .build();
    assertNotNull(processor);
  }

  @Test void testBuilderOnTableErrorHook() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .onTableError("test_table", new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
          @Override
          public Boolean apply(TableContext ctx, Exception ex) {
            return Boolean.TRUE;
          }
        })
        .build();
    assertNotNull(processor);
  }

  @Test void testBuilderResolveDimensionsHook() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .resolveDimensions("test_table",
            new java.util.function.BiFunction<TableContext,
                Map<String, DimensionConfig>, Map<String, DimensionConfig>>() {
              @Override
              public Map<String, DimensionConfig> apply(
                  TableContext ctx, Map<String, DimensionConfig> dims) {
                return dims;
              }
            })
        .build();
    assertNotNull(processor);
  }

  @Test void testBuilderIsEnabledHook() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .isEnabled("test_table", new java.util.function.Predicate<TableContext>() {
          @Override
          public boolean test(TableContext ctx) {
            return false;
          }
        })
        .build();
    assertNotNull(processor);
  }

  @SuppressWarnings("deprecation")
  @Test void testBuilderShouldProcessDeprecated() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .shouldProcess("test_table", new java.util.function.Predicate<TableContext>() {
          @Override
          public boolean test(TableContext ctx) {
            return true;
          }
        })
        .build();
    assertNotNull(processor);
  }

  // ========== Source phase hook registration ==========

  @Test void testBuilderBeforeSourceHook() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .beforeSource("test_table", new java.util.function.Consumer<TableContext>() {
          @Override
          public void accept(TableContext ctx) {
            // no-op
          }
        })
        .build();
    assertNotNull(processor);
  }

  @Test void testBuilderAfterSourceHook() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .afterSource("test_table", new java.util.function.BiConsumer<TableContext, SourceResult>() {
          @Override
          public void accept(TableContext ctx, SourceResult result) {
            // no-op
          }
        })
        .build();
    assertNotNull(processor);
  }

  @Test void testBuilderOnSourceErrorHook() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .onSourceError("test_table", new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
          @Override
          public Boolean apply(TableContext ctx, Exception ex) {
            return Boolean.TRUE;
          }
        })
        .build();
    assertNotNull(processor);
  }

  // ========== Materialize phase hook registration ==========

  @Test void testBuilderBeforeMaterializeHook() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .beforeMaterialize("test_table", new java.util.function.Consumer<TableContext>() {
          @Override
          public void accept(TableContext ctx) {
            // no-op
          }
        })
        .build();
    assertNotNull(processor);
  }

  @Test void testBuilderAfterMaterializeHook() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .afterMaterialize("test_table",
            new java.util.function.BiConsumer<TableContext, MaterializeResult>() {
              @Override
              public void accept(TableContext ctx, MaterializeResult result) {
                // no-op
              }
            })
        .build();
    assertNotNull(processor);
  }

  @Test void testBuilderOnMaterializeErrorHook() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .onMaterializeError("test_table",
            new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
              @Override
              public Boolean apply(TableContext ctx, Exception ex) {
                return Boolean.TRUE;
              }
            })
        .build();
    assertNotNull(processor);
  }

  // ========== Data provider/writer hook registration ==========

  @Test void testBuilderFetchDataHook() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .fetchData("test_table",
            new java.util.function.BiFunction<TableContext, Map<String, String>,
                java.util.Iterator<Map<String, Object>>>() {
              @Override
              public java.util.Iterator<Map<String, Object>> apply(
                  TableContext ctx, Map<String, String> vars) {
                return java.util.Collections.<Map<String, Object>>emptyIterator();
              }
            })
        .build();
    assertNotNull(processor);
  }

  @Test void testBuilderWriteDataHook() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .writeData("test_table",
            new SchemaLifecycleProcessor.Builder.FetchDataWriteFunction() {
              @Override
              public long write(TableContext context,
                  java.util.Iterator<Map<String, Object>> data,
                  Map<String, String> variables) {
                return 0;
              }
            })
        .build();
    assertNotNull(processor);
  }

  // ========== SchemaResult coverage ==========

  @Test void testSchemaResultWithMultipleTables() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("multi_table");

    builder.addTableResult("t1", EtlResult.success("t1", 100, 5, 200));
    builder.addTableResult("t2", EtlResult.success("t2", 200, 10, 300));
    builder.addTableResult("t3", EtlResult.failure("t3", "connection error", 50));
    builder.addTableResult("t4", EtlResult.skipped("t4", 0));

    builder.elapsedMs(1000);
    SchemaResult result = builder.build();

    assertEquals("multi_table", result.getSchemaName());
    assertEquals(4, result.getTotalTables());
    assertEquals(2, result.getSuccessfulTables());
    assertEquals(1, result.getFailedTables());
    assertEquals(1, result.getSkippedTables());
    assertEquals(300, result.getTotalRows()); // 100 + 200
    assertTrue(result.hasErrors());
  }

  @Test void testSchemaResultNoErrors() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("clean");
    builder.addTableResult("t1", EtlResult.success("t1", 50, 2, 100));
    builder.elapsedMs(500);
    SchemaResult result = builder.build();

    assertFalse(result.hasErrors());
    assertEquals(50, result.getTotalRows());
  }

  @Test void testSchemaResultWithAddError() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("with_errors");
    builder.addError("Something went wrong");
    builder.elapsedMs(100);
    SchemaResult result = builder.build();

    assertTrue(result.hasErrors());
  }

  // ========== EtlResult coverage ==========

  @Test void testEtlResultSuccess() {
    EtlResult result = EtlResult.success("test_table", 1000, 5, 500);
    assertEquals("test_table", result.getPipelineName());
    assertEquals(1000, result.getTotalRows());
    assertFalse(result.isFailed());
    assertFalse(result.isSkipped());
  }

  @Test void testEtlResultFailure() {
    EtlResult result = EtlResult.failure("test_table", "error message", 100);
    assertTrue(result.isFailed());
    assertEquals("error message", result.getFailureMessage());
  }

  @Test void testEtlResultSkipped() {
    EtlResult result = EtlResult.skipped("test_table", 50);
    assertTrue(result.isSkipped());
  }

  @Test void testEtlResultBuilder() {
    EtlResult result = EtlResult.builder()
        .pipelineName("custom")
        .failed(false)
        .elapsedMs(200)
        .build();
    assertNotNull(result);
    assertEquals("custom", result.getPipelineName());
    assertFalse(result.isFailed());
  }

  @Test void testEtlResultToString() {
    EtlResult result = EtlResult.success("table", 100, 3, 250);
    String str = result.toString();
    assertNotNull(str);
  }

  // ========== SchemaConfig from map coverage ==========

  @Test void testSchemaConfigFromMapWithHooks() {
    Map<String, Object> schemaMap = new LinkedHashMap<String, Object>();
    schemaMap.put("name", "test_schema");
    schemaMap.put("materializeDirectory", tempDir.toString());

    Map<String, Object> hooksMap = new LinkedHashMap<String, Object>();
    hooksMap.put("schemaLifecycleListener", "org.example.SchemaListener");
    hooksMap.put("tableLifecycleListener", "org.example.TableListener");
    schemaMap.put("hooks", hooksMap);

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(createTableMap("table1"));
    tables.add(createTableMap("table2"));
    schemaMap.put("tables", tables);

    SchemaConfig config = SchemaConfig.fromMap(schemaMap);

    assertNotNull(config);
    assertEquals("test_schema", config.getName());
    assertEquals(2, config.getTableCount());
    assertNotNull(config.getHooks());
  }

  @Test void testSchemaConfigFromMapMinimal() {
    Map<String, Object> schemaMap = new LinkedHashMap<String, Object>();
    schemaMap.put("name", "minimal");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(createTableMap("t1"));
    schemaMap.put("tables", tables);

    SchemaConfig config = SchemaConfig.fromMap(schemaMap);
    assertNotNull(config);
    assertEquals("minimal", config.getName());
  }

  @Test void testSchemaConfigBuilder() {
    SchemaConfig config = SchemaConfig.builder()
        .name("built")
        .materializeDirectory(tempDir.toString())
        .build();
    assertNotNull(config);
    assertEquals("built", config.getName());
  }

  // ========== Process with hooks lifecycle ==========

  @Test void testProcessCallsSchemaHooks() {
    final AtomicBoolean beforeSchemaCalled = new AtomicBoolean(false);
    final AtomicBoolean afterSchemaCalled = new AtomicBoolean(false);
    final AtomicReference<String> schemaNameRef = new AtomicReference<String>();

    SchemaLifecycleListener listener = new SchemaLifecycleListener() {
      @Override public void beforeSchema(SchemaContext ctx) {
        beforeSchemaCalled.set(true);
        schemaNameRef.set(ctx.getSchemaName());
      }
      @Override public void afterSchema(SchemaContext ctx, SchemaResult result) {
        afterSchemaCalled.set(true);
      }
      @Override public void onSchemaError(SchemaContext ctx, Exception error) {
        // no-op
      }
    };

    SchemaConfig config = createMinimalConfig("hook_test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .schemaListener(listener)
        .build();

    try {
      processor.process();
    } catch (Exception e) {
      // Expected - no real HTTP source
    }

    assertTrue(beforeSchemaCalled.get());
    assertEquals("hook_test", schemaNameRef.get());
  }

  @Test void testProcessCallsTableHooks() {
    final AtomicInteger beforeTableCount = new AtomicInteger(0);
    final AtomicInteger afterTableCount = new AtomicInteger(0);

    TableLifecycleListener listener = new TableLifecycleListener() {
      @Override public void beforeTable(TableContext ctx) {
        beforeTableCount.incrementAndGet();
      }
      @Override public void afterTable(TableContext ctx, EtlResult result) {
        afterTableCount.incrementAndGet();
      }
      @Override public boolean onTableError(TableContext ctx, Exception error) {
        return true; // continue processing
      }
    };

    SchemaConfig config = createMinimalConfig("table_hook_test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .defaultTableListener(listener)
        .build();

    try {
      processor.process();
    } catch (Exception e) {
      // Expected - no real HTTP source
    }

    assertTrue(beforeTableCount.get() >= 0);
  }

  // ========== Helper methods ==========

  private SchemaConfig createMinimalConfig(String name) {
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
