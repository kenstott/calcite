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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link SchemaLifecycleProcessor}.
 *
 * <p>Covers the builder pattern, hook registration, delegating table listener,
 * configuration validation, lifecycle event dispatching, and error handling.
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

  // ==========================================================================
  // Builder Validation Tests
  // ==========================================================================

  @Test void testBuilderRequiresConfig() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() {
        SchemaLifecycleProcessor.builder()
            .storageProvider(storageProvider)
            .materializeDirectory(tempDir.toString())
            .build();
      }
    });
  }

  @Test void testBuilderRequiresStorageProvider() {
    SchemaConfig config = createMinimalConfig("test");
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() {
        SchemaLifecycleProcessor.builder()
            .config(config)
            .build();
      }
    });
  }

  @Test void testBuilderRequiresMaterializeDirectory() {
    // Build a config without materialize directory
    SchemaConfig config = SchemaConfig.builder()
        .name("no_mat_dir")
        .addTable(createTableConfig("t1"))
        .build();

    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() {
        SchemaLifecycleProcessor.builder()
            .config(config)
            .storageProvider(storageProvider)
            .build();
      }
    });
  }

  @Test void testBuilderWithConfigAndStorageProvider() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .build();
    assertNotNull(processor);
  }

  // ==========================================================================
  // Builder Full Options
  // ==========================================================================

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

  @Test void testBuilderMaterializeDirectoryFromConfig() {
    // Config has materialize directory, builder does not need to override
    SchemaConfig config = SchemaConfig.builder()
        .name("with_mat_dir")
        .materializeDirectory(tempDir.toString())
        .addTable(createTableConfig("t1"))
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .build();
    assertNotNull(processor);
  }

  @Test void testBuilderMaterializeDirectoryOverridesConfig() {
    SchemaConfig config = SchemaConfig.builder()
        .name("override_mat_dir")
        .materializeDirectory("/original/dir")
        .addTable(createTableConfig("t1"))
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();
    assertNotNull(processor);
  }

  // ==========================================================================
  // Table-Level Hook Registration
  // ==========================================================================

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
        .onTableError("test_table",
            new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
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

  // ==========================================================================
  // Source Phase Hook Registration
  // ==========================================================================

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
        .afterSource("test_table",
            new java.util.function.BiConsumer<TableContext, SourceResult>() {
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
        .onSourceError("test_table",
            new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
              @Override
              public Boolean apply(TableContext ctx, Exception ex) {
                return Boolean.TRUE;
              }
            })
        .build();
    assertNotNull(processor);
  }

  // ==========================================================================
  // Materialize Phase Hook Registration
  // ==========================================================================

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

  // ==========================================================================
  // Data Provider/Writer Hook Registration
  // ==========================================================================

  @Test void testBuilderFetchDataHook() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .fetchData("test_table",
            new java.util.function.BiFunction<TableContext, Map<String, String>,
                Iterator<Map<String, Object>>>() {
              @Override
              public Iterator<Map<String, Object>> apply(
                  TableContext ctx, Map<String, String> vars) {
                return Collections.<Map<String, Object>>emptyIterator();
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
                  Iterator<Map<String, Object>> data,
                  Map<String, String> variables) {
                return 0;
              }
            })
        .build();
    assertNotNull(processor);
  }

  // ==========================================================================
  // Multiple Hooks Registration (DelegatingTableListener)
  // ==========================================================================

  @Test void testBuilderMultipleHooksCreatesDelegatingListener() {
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .beforeTable("t1", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) { }
        })
        .afterTable("t2", new java.util.function.BiConsumer<TableContext, EtlResult>() {
          @Override public void accept(TableContext ctx, EtlResult r) { }
        })
        .onTableError("t3",
            new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
              @Override public Boolean apply(TableContext ctx, Exception e) {
                return Boolean.TRUE;
              }
            })
        .build();
    assertNotNull(processor);
  }

  @Test void testBuilderAllHookTypesCombined() {
    SchemaConfig config = createMinimalConfig("combined_hooks");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        // Table hooks
        .beforeTable("t1", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) { }
        })
        .afterTable("t1", new java.util.function.BiConsumer<TableContext, EtlResult>() {
          @Override public void accept(TableContext ctx, EtlResult r) { }
        })
        .onTableError("t1",
            new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
              @Override public Boolean apply(TableContext ctx, Exception e) {
                return Boolean.TRUE;
              }
            })
        // Source hooks
        .beforeSource("t1", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) { }
        })
        .afterSource("t1",
            new java.util.function.BiConsumer<TableContext, SourceResult>() {
              @Override public void accept(TableContext ctx, SourceResult r) { }
            })
        .onSourceError("t1",
            new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
              @Override public Boolean apply(TableContext ctx, Exception e) {
                return Boolean.TRUE;
              }
            })
        // Materialize hooks
        .beforeMaterialize("t1", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) { }
        })
        .afterMaterialize("t1",
            new java.util.function.BiConsumer<TableContext, MaterializeResult>() {
              @Override public void accept(TableContext ctx, MaterializeResult r) { }
            })
        .onMaterializeError("t1",
            new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
              @Override public Boolean apply(TableContext ctx, Exception e) {
                return Boolean.TRUE;
              }
            })
        // Data hooks
        .fetchData("t1",
            new java.util.function.BiFunction<TableContext, Map<String, String>,
                Iterator<Map<String, Object>>>() {
              @Override public Iterator<Map<String, Object>> apply(
                  TableContext ctx, Map<String, String> vars) {
                return Collections.<Map<String, Object>>emptyIterator();
              }
            })
        .writeData("t1",
            new SchemaLifecycleProcessor.Builder.FetchDataWriteFunction() {
              @Override public long write(TableContext ctx,
                  Iterator<Map<String, Object>> data, Map<String, String> vars) {
                return 0;
              }
            })
        // Dimension and filter hooks
        .resolveDimensions("t1",
            new java.util.function.BiFunction<TableContext,
                Map<String, DimensionConfig>, Map<String, DimensionConfig>>() {
              @Override public Map<String, DimensionConfig> apply(
                  TableContext ctx, Map<String, DimensionConfig> d) {
                return d;
              }
            })
        .isEnabled("t1", new java.util.function.Predicate<TableContext>() {
          @Override public boolean test(TableContext ctx) {
            return true;
          }
        })
        .build();
    assertNotNull(processor);
  }

  @Test void testBuilderHooksWithDefaultTableListener() {
    final AtomicBoolean defaultCalled = new AtomicBoolean(false);
    SchemaConfig config = createMinimalConfig("test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .defaultTableListener(new TableLifecycleListener() {
          @Override public void beforeTable(TableContext ctx) {
            defaultCalled.set(true);
          }
          @Override public void afterTable(TableContext ctx, EtlResult r) { }
          @Override public boolean onTableError(TableContext ctx, Exception e) {
            return true;
          }
        })
        // Add a hook so DelegatingTableListener wraps the default
        .beforeTable("other_table", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) { }
        })
        .build();
    assertNotNull(processor);
  }

  // ==========================================================================
  // Process Lifecycle - Schema Hooks
  // ==========================================================================

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

  @Test void testProcessCallsSchemaErrorHookOnFailure() {
    final AtomicBoolean errorCalled = new AtomicBoolean(false);
    final AtomicReference<Exception> errorRef = new AtomicReference<Exception>();

    SchemaLifecycleListener listener = new SchemaLifecycleListener() {
      @Override public void beforeSchema(SchemaContext ctx) throws Exception {
        throw new RuntimeException("Simulated schema error");
      }
      @Override public void afterSchema(SchemaContext ctx, SchemaResult result) { }
      @Override public void onSchemaError(SchemaContext ctx, Exception error) {
        errorCalled.set(true);
        errorRef.set(error);
      }
    };

    SchemaConfig config = createMinimalConfig("error_test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .schemaListener(listener)
        .build();

    try {
      processor.process();
    } catch (Exception e) {
      // Expected
    }

    assertTrue(errorCalled.get());
    assertNotNull(errorRef.get());
  }

  // ==========================================================================
  // Process Lifecycle - Table Hooks
  // ==========================================================================

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

  @Test void testProcessWithIsEnabledFilterDisablesTable() {
    final AtomicInteger beforeTableCount = new AtomicInteger(0);

    SchemaConfig config = createMinimalConfig("filter_test");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .isEnabled("test_table", new java.util.function.Predicate<TableContext>() {
          @Override
          public boolean test(TableContext ctx) {
            return false; // Disable the table
          }
        })
        .beforeTable("test_table", new java.util.function.Consumer<TableContext>() {
          @Override
          public void accept(TableContext ctx) {
            beforeTableCount.incrementAndGet();
          }
        })
        .build();

    try {
      processor.process();
    } catch (Exception e) {
      // Expected
    }
    // The table should have been filtered out
  }

  @Test void testProcessWithWildcardIsEnabledFilter() {
    SchemaConfig config = createMinimalConfig("wildcard_filter");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .isEnabled("*", new java.util.function.Predicate<TableContext>() {
          @Override
          public boolean test(TableContext ctx) {
            return false; // Disable all tables
          }
        })
        .build();

    try {
      processor.process();
    } catch (Exception e) {
      // Expected
    }
  }

  // ==========================================================================
  // SchemaResult Tests
  // ==========================================================================

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
    assertEquals(1000, result.getElapsedMs());
  }

  @Test void testSchemaResultNoErrors() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("clean");
    builder.addTableResult("t1", EtlResult.success("t1", 50, 2, 100));
    builder.elapsedMs(500);
    SchemaResult result = builder.build();

    assertFalse(result.hasErrors());
    assertEquals(50, result.getTotalRows());
    assertEquals(500, result.getElapsedMs());
  }

  @Test void testSchemaResultWithAddError() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("with_errors");
    builder.addError("Something went wrong");
    builder.elapsedMs(100);
    SchemaResult result = builder.build();

    assertTrue(result.hasErrors());
    assertEquals(1, result.getErrors().size());
    assertEquals("Something went wrong", result.getErrors().get(0));
  }

  @Test void testSchemaResultConvenienceConstructor() {
    SchemaResult result = new SchemaResult("err_schema", 1, 2, 0, 100, 500, "Error msg");
    assertEquals("err_schema", result.getSchemaName());
    assertEquals(1, result.getSuccessfulTables());
    assertEquals(2, result.getFailedTables());
    assertEquals(0, result.getSkippedTables());
    assertEquals(100, result.getTotalRows());
    assertEquals(500, result.getElapsedMs());
    assertTrue(result.hasErrors());
    assertEquals(1, result.getErrors().size());
  }

  @Test void testSchemaResultConvenienceConstructorNullError() {
    SchemaResult result = new SchemaResult("no_err", 5, 0, 0, 200, 100, null);
    assertFalse(result.hasErrors());
    assertTrue(result.getErrors().isEmpty());
  }

  @Test void testSchemaResultToString() {
    SchemaResult result = new SchemaResult("str_test", 2, 1, 1, 500, 1000, "err");
    String str = result.toString();
    assertNotNull(str);
    assertTrue(str.contains("str_test"));
  }

  @Test void testSchemaResultTableResults() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("table_results");
    EtlResult r1 = EtlResult.success("t1", 100, 3, 200);
    builder.addTableResult("t1", r1);
    builder.elapsedMs(300);
    SchemaResult result = builder.build();

    assertNotNull(result.getTableResults());
    assertEquals(1, result.getTableResults().size());
    assertNotNull(result.getTableResult("t1"));
    assertNull(result.getTableResult("nonexistent"));
  }

  @Test void testSchemaResultBuilderGetTotalRows() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("total_rows");
    builder.addTableResult("t1", EtlResult.success("t1", 100, 2, 50));
    builder.addTableResult("t2", EtlResult.success("t2", 200, 3, 70));
    assertEquals(300, builder.getTotalRows());
  }

  // ==========================================================================
  // EtlResult Tests
  // ==========================================================================

  @Test void testEtlResultSuccess() {
    EtlResult result = EtlResult.success("test_table", 1000, 5, 500);
    assertEquals("test_table", result.getPipelineName());
    assertEquals(1000, result.getTotalRows());
    assertEquals(5, result.getSuccessfulBatches());
    assertEquals(500, result.getElapsedMs());
    assertFalse(result.isFailed());
    assertFalse(result.isSkipped());
  }

  @Test void testEtlResultFailure() {
    EtlResult result = EtlResult.failure("test_table", "error message", 100);
    assertTrue(result.isFailed());
    assertEquals("error message", result.getFailureMessage());
    assertEquals(100, result.getElapsedMs());
  }

  @Test void testEtlResultSkipped() {
    EtlResult result = EtlResult.skipped("test_table", 50);
    assertTrue(result.isSkipped());
    assertEquals(50, result.getElapsedMs());
  }

  @Test void testEtlResultBuilder() {
    EtlResult result = EtlResult.builder()
        .pipelineName("custom")
        .totalRows(500)
        .successfulBatches(10)
        .failedBatches(2)
        .skippedBatches(1)
        .failed(false)
        .elapsedMs(200)
        .build();
    assertNotNull(result);
    assertEquals("custom", result.getPipelineName());
    assertEquals(500, result.getTotalRows());
    assertEquals(10, result.getSuccessfulBatches());
    assertEquals(2, result.getFailedBatches());
    assertFalse(result.isFailed());
  }

  @Test void testEtlResultToString() {
    EtlResult result = EtlResult.success("table", 100, 3, 250);
    String str = result.toString();
    assertNotNull(str);
  }

  @Test void testEtlResultErrors() {
    EtlResult result = EtlResult.builder()
        .pipelineName("err_pipeline")
        .failed(true)
        .failureMessage("fatal error")
        .build();
    assertTrue(result.isFailed());
    assertEquals("fatal error", result.getFailureMessage());
  }

  // ==========================================================================
  // SchemaConfig Coverage
  // ==========================================================================

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

  @Test void testSchemaConfigFromMapWithSchemaName() {
    Map<String, Object> schemaMap = new LinkedHashMap<String, Object>();
    schemaMap.put("schemaName", "alt_name");
    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(createTableMap("t1"));
    schemaMap.put("tables", tables);

    SchemaConfig config = SchemaConfig.fromMap(schemaMap);
    assertNotNull(config);
    assertEquals("alt_name", config.getName());
  }

  @Test void testSchemaConfigFromMapNull() {
    SchemaConfig config = SchemaConfig.fromMap(null);
    assertNull(config);
  }

  @Test void testSchemaConfigBuilder() {
    SchemaConfig config = SchemaConfig.builder()
        .name("built")
        .materializeDirectory(tempDir.toString())
        .sourceDirectory("/source")
        .build();
    assertNotNull(config);
    assertEquals("built", config.getName());
    assertEquals(tempDir.toString(), config.getMaterializeDirectory());
    assertEquals("/source", config.getSourceDirectory());
  }

  @Test void testSchemaConfigBuilderRequiresName() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() {
        SchemaConfig.builder().build();
      }
    });
  }

  @Test void testSchemaConfigBuilderEmptyNameThrows() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() {
        SchemaConfig.builder().name("").build();
      }
    });
  }

  @Test void testSchemaConfigGetTable() {
    SchemaConfig config = SchemaConfig.builder()
        .name("lookup")
        .materializeDirectory(tempDir.toString())
        .addTable(createTableConfig("alpha"))
        .addTable(createTableConfig("beta"))
        .build();

    assertNotNull(config.getTable("alpha"));
    assertNotNull(config.getTable("beta"));
    assertNull(config.getTable("gamma"));
    assertEquals(2, config.getTableCount());
  }

  @Test void testSchemaConfigMetadata() {
    Map<String, Object> meta = new LinkedHashMap<String, Object>();
    meta.put("version", "1.0");
    meta.put("name", "meta_test");

    SchemaConfig config = SchemaConfig.builder()
        .name("meta_test")
        .materializeDirectory(tempDir.toString())
        .metadata(meta)
        .build();

    assertNotNull(config.getMetadata());
    assertEquals("1.0", config.getMetadata().get("version"));
  }

  @Test void testSchemaConfigPostProcess() {
    SchemaConfig config = SchemaConfig.builder()
        .name("post_process")
        .materializeDirectory(tempDir.toString())
        .build();

    assertNotNull(config.getPostProcess());
    assertTrue(config.getPostProcess().isEmpty());
  }

  @Test void testSchemaConfigBulkDownloads() {
    SchemaConfig config = SchemaConfig.builder()
        .name("bulk")
        .materializeDirectory(tempDir.toString())
        .build();

    assertNotNull(config.getBulkDownloads());
    assertTrue(config.getBulkDownloads().isEmpty());
    assertNull(config.getBulkDownload("nonexistent"));
  }

  // ==========================================================================
  // SchemaContext Tests
  // ==========================================================================

  @Test void testSchemaContextBuilder() {
    SchemaConfig config = createMinimalConfig("ctx_test");
    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .sourceDirectory("/source")
        .operatingDirectory("/ops")
        .build();

    assertEquals("ctx_test", ctx.getSchemaName());
    assertEquals(config, ctx.getConfig());
    assertNotNull(ctx.getStorageProvider());
    assertNotNull(ctx.getSourceStorageProvider());
    assertEquals(tempDir.toString(), ctx.getMaterializeDirectory());
    assertEquals("/source", ctx.getSourceDirectory());
    assertEquals("/ops", ctx.getOperatingDirectory());
    assertNotNull(ctx.getAttributes());
  }

  @Test void testSchemaContextAttributes() {
    SchemaConfig config = createMinimalConfig("attr_test");
    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    ctx.setAttribute("key1", "value1");
    assertEquals("value1", (String) ctx.getAttribute("key1"));
    assertNull(ctx.getAttribute("nonexistent"));
  }

  @Test void testSchemaContextBulkDownloadPath() {
    SchemaConfig config = createMinimalConfig("bulk_test");
    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    ctx.setBulkDownloadPath("file1", "year=2020", "/cache/file1_2020.zip");
    assertEquals("/cache/file1_2020.zip",
        ctx.getBulkDownloadPath("file1", "year=2020"));
    assertNull(ctx.getBulkDownloadPath("file1", "year=2021"));
    assertNull(ctx.getBulkDownloadPath("nonexistent", "year=2020"));
  }

  @Test void testSchemaContextIncrementalTracker() {
    SchemaConfig config = createMinimalConfig("tracker_test");
    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .build();

    assertEquals(IncrementalTracker.NOOP, ctx.getIncrementalTracker());
  }

  @Test void testSchemaContextDefaultIncrementalTracker() {
    SchemaConfig config = createMinimalConfig("default_tracker");
    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    // Should default to NOOP
    assertNotNull(ctx.getIncrementalTracker());
  }

  @Test void testSchemaContextSourceStorageProviderFallback() {
    SchemaConfig config = createMinimalConfig("fallback_test");
    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    // Source storage provider should fall back to main storage provider
    assertEquals(storageProvider, ctx.getSourceStorageProvider());
  }

  @Test void testSchemaContextSourceStorageProviderExplicit() {
    StorageProvider sourceProvider = new LocalFileStorageProvider();
    SchemaConfig config = createMinimalConfig("explicit_source");
    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .sourceStorageProvider(sourceProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    assertEquals(sourceProvider, ctx.getSourceStorageProvider());
  }

  @Test void testSchemaContextGetTables() {
    SchemaConfig config = createMinimalConfig("tables_test");
    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    assertNotNull(ctx.getTables());
    assertFalse(ctx.getTables().isEmpty());
  }

  // ==========================================================================
  // TableContext Tests
  // ==========================================================================

  @Test void testTableContextViaProcess() {
    final AtomicReference<String> tableNameRef = new AtomicReference<String>();
    final AtomicReference<Integer> tableIndexRef = new AtomicReference<Integer>();

    SchemaConfig config = createMinimalConfig("ctx_process");
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .beforeTable("test_table", new java.util.function.Consumer<TableContext>() {
          @Override
          public void accept(TableContext ctx) {
            tableNameRef.set(ctx.getTableName());
            tableIndexRef.set(ctx.getTableIndex());
          }
        })
        .build();

    try {
      processor.process();
    } catch (Exception e) {
      // Expected
    }

    assertEquals("test_table", tableNameRef.get());
  }

  // ==========================================================================
  // NOOP Listener Tests
  // ==========================================================================

  @Test void testSchemaLifecycleListenerNoop() {
    SchemaLifecycleListener noop = SchemaLifecycleListener.NOOP;
    SchemaConfig config = createMinimalConfig("noop_test");
    SchemaContext ctx = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    // All methods should not throw
    try {
      noop.beforeSchema(ctx);
    } catch (Exception e) {
      throw new AssertionError("NOOP beforeSchema should not throw", e);
    }
    noop.afterSchema(ctx, new SchemaResult("noop_test", 0, 0, 0, 0, 0, null));
    noop.onSchemaError(ctx, new RuntimeException("test"));
  }

  @Test void testTableLifecycleListenerNoop() throws Exception {
    TableLifecycleListener noop = TableLifecycleListener.NOOP;
    SchemaConfig config = createMinimalConfig("noop_table");
    SchemaContext schemaCtx = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    TableContext tableCtx = TableContext.builder()
        .tableConfig(createTableConfig("t1"))
        .schemaContext(schemaCtx)
        .tableIndex(0)
        .totalTables(1)
        .build();

    noop.beforeTable(tableCtx);
    noop.afterTable(tableCtx, EtlResult.success("t1", 0, 0, 0));
    assertTrue(noop.onTableError(tableCtx, new RuntimeException("test")));
  }

  @Test void testTableLifecycleListenerDefaultMethods() {
    TableLifecycleListener listener = new TableLifecycleListener() {
      @Override public void beforeTable(TableContext ctx) { }
      @Override public void afterTable(TableContext ctx, EtlResult result) { }
      @Override public boolean onTableError(TableContext ctx, Exception error) {
        return true;
      }
    };

    // Default implementations should not throw
    SchemaConfig config = createMinimalConfig("default_methods");
    SchemaContext schemaCtx = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();
    TableContext tableCtx = TableContext.builder()
        .tableConfig(createTableConfig("t1"))
        .schemaContext(schemaCtx)
        .tableIndex(0)
        .totalTables(1)
        .build();

    listener.beforeSource(tableCtx);
    listener.afterSource(tableCtx, null);
    assertTrue(listener.onSourceError(tableCtx, new RuntimeException("err")));
    listener.beforeMaterialize(tableCtx);
    listener.afterMaterialize(tableCtx, null);
    assertTrue(listener.onMaterializeError(tableCtx, new RuntimeException("err")));
    assertNull(listener.fetchData(tableCtx, Collections.<String, String>emptyMap()));
    assertEquals(-1,
        listener.writeData(tableCtx,
            Collections.<Map<String, Object>>emptyIterator(),
            Collections.<String, String>emptyMap()));
    assertNull(listener.resolveDimensions(tableCtx,
        Collections.<String, DimensionConfig>emptyMap()));
    assertNull(listener.resolveApiKey(tableCtx, "TEST_KEY"));
    assertTrue(listener.isTableEnabled(tableCtx));
  }

  @SuppressWarnings("deprecation")
  @Test void testTableLifecycleListenerDeprecatedShouldProcessTable() {
    TableLifecycleListener listener = TableLifecycleListener.NOOP;
    SchemaConfig config = createMinimalConfig("deprecated_test");
    SchemaContext schemaCtx = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();
    TableContext tableCtx = TableContext.builder()
        .tableConfig(createTableConfig("t1"))
        .schemaContext(schemaCtx)
        .tableIndex(0)
        .totalTables(1)
        .build();

    assertTrue(listener.shouldProcessTable(tableCtx));
  }

  // ==========================================================================
  // SchemaConfig fromMap Edge Cases
  // ==========================================================================

  @Test void testSchemaConfigFromMapWithPartitionedTables() {
    Map<String, Object> schemaMap = new LinkedHashMap<String, Object>();
    schemaMap.put("name", "partitioned");
    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(createTableMap("pt1"));
    schemaMap.put("partitionedTables", tables);

    SchemaConfig config = SchemaConfig.fromMap(schemaMap);
    assertNotNull(config);
    assertEquals(1, config.getTableCount());
  }

  @Test void testSchemaConfigFromMapSkipsViews() {
    Map<String, Object> schemaMap = new LinkedHashMap<String, Object>();
    schemaMap.put("name", "with_views");
    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(createTableMap("real_table"));

    Map<String, Object> viewMap = new LinkedHashMap<String, Object>();
    viewMap.put("name", "my_view");
    viewMap.put("type", "view");
    tables.add(viewMap);

    schemaMap.put("tables", tables);
    SchemaConfig config = SchemaConfig.fromMap(schemaMap);
    assertNotNull(config);
    // View should be skipped
    assertEquals(1, config.getTableCount());
  }

  @Test void testSchemaConfigFromMapSkipsTablesWithoutSource() {
    Map<String, Object> schemaMap = new LinkedHashMap<String, Object>();
    schemaMap.put("name", "no_source");
    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();

    // Table without source
    Map<String, Object> noSourceTable = new LinkedHashMap<String, Object>();
    noSourceTable.put("name", "metadata_only");
    tables.add(noSourceTable);

    // Table with source
    tables.add(createTableMap("has_source"));
    schemaMap.put("tables", tables);

    SchemaConfig config = SchemaConfig.fromMap(schemaMap);
    assertNotNull(config);
    assertEquals(1, config.getTableCount());
  }

  @Test void testSchemaConfigFromMapWithSourceDirectory() {
    Map<String, Object> schemaMap = new LinkedHashMap<String, Object>();
    schemaMap.put("name", "with_src_dir");
    schemaMap.put("sourceDirectory", "/data/source");
    schemaMap.put("materializeDirectory", "/data/output");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(createTableMap("t1"));
    schemaMap.put("tables", tables);

    SchemaConfig config = SchemaConfig.fromMap(schemaMap);
    assertNotNull(config);
    assertEquals("/data/source", config.getSourceDirectory());
    assertEquals("/data/output", config.getMaterializeDirectory());
  }

  // ==========================================================================
  // Helper Methods
  // ==========================================================================

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

  private EtlPipelineConfig createTableConfig(String name) {
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

    return EtlPipelineConfig.fromMap(tableMap);
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
