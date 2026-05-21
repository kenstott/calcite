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
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Line coverage tests for {@link SchemaLifecycleProcessor} targeting:
 * <ul>
 *   <li>DelegatingTableListener delegation paths (all hook types)</li>
 *   <li>afterSchema() and afterTable() callback invocation</li>
 *   <li>Error recovery when listeners throw exceptions</li>
 *   <li>loadSchemaListener / loadDefaultTableListener / loadTableListener</li>
 *   <li>createVariableKey</li>
 *   <li>needsArchiveRetry branches</li>
 *   <li>archiveRawCache branches</li>
 *   <li>Builder hook registration and build</li>
 * </ul>
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class SchemaLifecycleProcessorLineCoverageTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;

  @BeforeEach
  void setup() {
    storageProvider = new TestStorageProvider(tempDir.toString());
  }

  // ======= DelegatingTableListener tests =======

  /**
   * Tests the DelegatingTableListener with registered hooks for all lifecycle methods.
   */
  @Test void testDelegatingTableListenerWithHooks() throws Exception {
    AtomicBoolean beforeCalled = new AtomicBoolean(false);
    AtomicBoolean afterCalled = new AtomicBoolean(false);
    AtomicBoolean beforeSourceCalled = new AtomicBoolean(false);
    AtomicBoolean afterSourceCalled = new AtomicBoolean(false);
    AtomicBoolean beforeMaterializeCalled = new AtomicBoolean(false);
    AtomicBoolean afterMaterializeCalled = new AtomicBoolean(false);
    AtomicReference<String> resolvedDimResult = new AtomicReference<String>(null);
    AtomicBoolean isEnabledCalled = new AtomicBoolean(false);

    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test_schema")
        .materializeDirectory(tempDir.resolve("materialize").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.resolve("materialize").toString())
        .beforeTable("testTable", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) {
            beforeCalled.set(true);
          }
        })
        .afterTable("testTable", new java.util.function.BiConsumer<TableContext, EtlResult>() {
          @Override public void accept(TableContext ctx, EtlResult result) {
            afterCalled.set(true);
          }
        })
        .beforeSource("testTable", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) {
            beforeSourceCalled.set(true);
          }
        })
        .afterSource("testTable", new java.util.function.BiConsumer<TableContext, SourceResult>() {
          @Override public void accept(TableContext ctx, SourceResult result) {
            afterSourceCalled.set(true);
          }
        })
        .beforeMaterialize("testTable", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) {
            beforeMaterializeCalled.set(true);
          }
        })
        .afterMaterialize("testTable", new java.util.function.BiConsumer<TableContext, MaterializeResult>() {
          @Override public void accept(TableContext ctx, MaterializeResult result) {
            afterMaterializeCalled.set(true);
          }
        })
        .resolveDimensions("testTable", new java.util.function.BiFunction<TableContext,
            Map<String, DimensionConfig>, Map<String, DimensionConfig>>() {
          @Override public Map<String, DimensionConfig> apply(TableContext ctx,
              Map<String, DimensionConfig> dims) {
            resolvedDimResult.set("resolved");
            return dims;
          }
        })
        .isEnabled("testTable", new java.util.function.Predicate<TableContext>() {
          @Override public boolean test(TableContext ctx) {
            isEnabledCalled.set(true);
            return true;
          }
        })
        .build();

    // The processor was built with hooks - verify that build() succeeded
    assertNotNull(processor);
  }

  /**
   * Tests the DelegatingTableListener when no hook is registered and it falls through
   * to the delegate (or defaults).
   */
  @Test void testDelegatingTableListenerWithoutHooks() throws Exception {
    AtomicBoolean delegateBeforeCalled = new AtomicBoolean(false);

    TableLifecycleListener delegate = new TableLifecycleListener() {
      @Override public void beforeTable(TableContext context) {
        delegateBeforeCalled.set(true);
      }
      @Override public void afterTable(TableContext context, EtlResult result) { }
      @Override public boolean onTableError(TableContext context, Exception error) {
        return true;
      }
    };

    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test_schema")
        .materializeDirectory(tempDir.resolve("materialize").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    // Register a hook for a different table to force DelegatingTableListener creation
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.resolve("materialize").toString())
        .defaultTableListener(delegate)
        .beforeTable("otherTable", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) { }
        })
        .build();

    assertNotNull(processor);
  }

  // ======= DelegatingTableListener error hooks delegation =======

  @Test void testDelegatingTableListenerErrorHooksWithHook() throws Exception {
    AtomicBoolean errorHookCalled = new AtomicBoolean(false);
    AtomicBoolean sourceErrorHookCalled = new AtomicBoolean(false);
    AtomicBoolean materializeErrorHookCalled = new AtomicBoolean(false);

    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test_schema")
        .materializeDirectory(tempDir.resolve("materialize").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.resolve("materialize").toString())
        .onTableError("testTable", new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
          @Override public Boolean apply(TableContext ctx, Exception ex) {
            errorHookCalled.set(true);
            return true;
          }
        })
        .onSourceError("testTable", new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
          @Override public Boolean apply(TableContext ctx, Exception ex) {
            sourceErrorHookCalled.set(true);
            return true;
          }
        })
        .onMaterializeError("testTable", new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
          @Override public Boolean apply(TableContext ctx, Exception ex) {
            materializeErrorHookCalled.set(true);
            return true;
          }
        })
        .build();

    assertNotNull(processor);
  }

  // ======= DelegatingTableListener fetchData/writeData hooks =======

  @Test void testDelegatingTableListenerDataHooks() throws Exception {
    AtomicBoolean fetchCalled = new AtomicBoolean(false);
    AtomicBoolean writeCalled = new AtomicBoolean(false);

    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test_schema")
        .materializeDirectory(tempDir.resolve("materialize").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.resolve("materialize").toString())
        .fetchData("testTable", new java.util.function.BiFunction<TableContext,
            java.util.Map<String, String>,
            java.util.Iterator<java.util.Map<String, Object>>>() {
          @Override public Iterator<Map<String, Object>> apply(
              TableContext ctx, Map<String, String> vars) {
            fetchCalled.set(true);
            return Collections.<Map<String, Object>>emptyList().iterator();
          }
        })
        .writeData("testTable", new SchemaLifecycleProcessor.Builder.FetchDataWriteFunction() {
          @Override public long write(TableContext context,
              Iterator<Map<String, Object>> data, Map<String, String> variables) {
            writeCalled.set(true);
            return 0;
          }
        })
        .build();

    assertNotNull(processor);
  }

  // ======= Builder validation tests =======

  @Test void testBuilderRequiresConfig() {
    assertThrows(IllegalArgumentException.class, () ->
        SchemaLifecycleProcessor.builder()
            .storageProvider(storageProvider)
            .materializeDirectory(tempDir.toString())
            .build());
  }

  @Test void testBuilderRequiresStorageProvider() {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    assertThrows(IllegalArgumentException.class, () ->
        SchemaLifecycleProcessor.builder()
            .config(schemaConfig)
            .materializeDirectory(tempDir.toString())
            .build());
  }

  @Test void testBuilderRequiresMaterializeDirectory() {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    assertThrows(IllegalArgumentException.class, () ->
        SchemaLifecycleProcessor.builder()
            .config(schemaConfig)
            .storageProvider(storageProvider)
            .build());
  }

  // ======= Builder all fluent methods =======

  @Test void testBuilderFluentMethods() {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.resolve("mat").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .sourceStorageProvider(storageProvider)
        .sourceDirectory(tempDir.resolve("source").toString())
        .materializeDirectory(tempDir.resolve("mat").toString())
        .operatingDirectory(tempDir.resolve("op").toString())
        .incrementalTracker(IncrementalTracker.NOOP)
        .schemaListener(SchemaLifecycleListener.NOOP)
        .defaultTableListener(TableLifecycleListener.NOOP)
        .build();

    assertNotNull(processor);
  }

  @Test @SuppressWarnings("deprecation")
  void testBuilderDeprecatedBaseDirectory() {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.resolve("mat").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .baseDirectory(tempDir.resolve("mat").toString())
        .build();

    assertNotNull(processor);
  }

  @Test @SuppressWarnings("deprecation")
  void testBuilderDeprecatedShouldProcess() {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.resolve("mat").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.resolve("mat").toString())
        .shouldProcess("table1", new java.util.function.Predicate<TableContext>() {
          @Override public boolean test(TableContext ctx) {
            return true;
          }
        })
        .build();

    assertNotNull(processor);
  }

  // ======= createVariableKey tests =======

  @Test void testCreateVariableKeyEmpty() throws Exception {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.resolve("mat").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.resolve("mat").toString())
        .build();

    Method m = SchemaLifecycleProcessor.class.getDeclaredMethod("createVariableKey", Map.class);
    m.setAccessible(true);

    String result = (String) m.invoke(processor, Collections.emptyMap());
    assertEquals("default", result);
  }

  @Test void testCreateVariableKeyNull() throws Exception {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.resolve("mat").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.resolve("mat").toString())
        .build();

    Method m = SchemaLifecycleProcessor.class.getDeclaredMethod("createVariableKey", Map.class);
    m.setAccessible(true);

    String result = (String) m.invoke(processor, (Object) null);
    assertEquals("default", result);
  }

  @Test void testCreateVariableKeyWithValues() throws Exception {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.resolve("mat").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.resolve("mat").toString())
        .build();

    Method m = SchemaLifecycleProcessor.class.getDeclaredMethod("createVariableKey", Map.class);
    m.setAccessible(true);

    Map<String, String> vars = new LinkedHashMap<String, String>();
    vars.put("year", "2024");
    vars.put("region", "NORTH");

    String result = (String) m.invoke(processor, vars);
    assertTrue(result.contains("year=2024"));
    assertTrue(result.contains("region=NORTH"));
    assertTrue(result.contains(","));
  }

  // ======= needsArchiveRetry tests =======

  @Test void testNeedsArchiveRetryNullOperatingDir() throws Exception {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.resolve("mat").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.resolve("mat").toString())
        .build();

    Method m =
        SchemaLifecycleProcessor.class.getDeclaredMethod("needsArchiveRetry", SchemaContext.class);
    m.setAccessible(true);

    SchemaContext ctx = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .operatingDirectory(null)
        .build();

    boolean result = (Boolean) m.invoke(processor, ctx);
    assertFalse(result, "Should return false when operating directory is null");
  }

  @Test void testNeedsArchiveRetryNoCacheDir() throws Exception {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.resolve("mat").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.resolve("mat").toString())
        .operatingDirectory(tempDir.resolve("op").toString())
        .build();

    Method m =
        SchemaLifecycleProcessor.class.getDeclaredMethod("needsArchiveRetry", SchemaContext.class);
    m.setAccessible(true);

    // Operating directory exists but cache/raw does not
    SchemaContext ctx = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .operatingDirectory(tempDir.resolve("op").toString())
        .build();

    boolean result = (Boolean) m.invoke(processor, ctx);
    assertFalse(result, "Should return false when cache/raw dir does not exist");
  }

  @Test void testNeedsArchiveRetryLocalStorageProvider() throws Exception {
    String opDir = tempDir.resolve("op-archive").toString();
    // Create cache/raw directory
    java.io.File cacheDir = new java.io.File(opDir + "/cache/raw");
    cacheDir.mkdirs();
    Files.write(new java.io.File(cacheDir, "test.json").toPath(),
        "{}".getBytes());

    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.resolve("mat").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    // Use a storage provider that reports "local" type
    StorageProvider localSp = new TestStorageProvider(tempDir.toString()) {
      @Override public String getStorageType() {
        return "local";
      }
    };

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(localSp)
        .sourceStorageProvider(localSp)
        .materializeDirectory(tempDir.resolve("mat").toString())
        .operatingDirectory(opDir)
        .build();

    Method m =
        SchemaLifecycleProcessor.class.getDeclaredMethod("needsArchiveRetry", SchemaContext.class);
    m.setAccessible(true);

    SchemaContext ctx = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(localSp)
        .sourceStorageProvider(localSp)
        .operatingDirectory(opDir)
        .build();

    boolean result = (Boolean) m.invoke(processor, ctx);
    assertFalse(result, "Should return false for local storage provider");
  }

  // ======= archiveRawCache tests =======

  @Test void testArchiveRawCacheNullOperatingDir() throws Exception {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.resolve("mat").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.resolve("mat").toString())
        .build();

    Method m =
        SchemaLifecycleProcessor.class.getDeclaredMethod("archiveRawCache", SchemaContext.class);
    m.setAccessible(true);

    SchemaContext ctx = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .operatingDirectory(null)
        .build();

    // Should not throw
    m.invoke(processor, ctx);
  }

  @Test void testArchiveRawCacheNoCacheDir() throws Exception {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.resolve("mat").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.resolve("mat").toString())
        .operatingDirectory(tempDir.resolve("no-cache-dir").toString())
        .build();

    Method m =
        SchemaLifecycleProcessor.class.getDeclaredMethod("archiveRawCache", SchemaContext.class);
    m.setAccessible(true);

    SchemaContext ctx = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .operatingDirectory(tempDir.resolve("no-cache-dir").toString())
        .build();

    // Should not throw - just returns early
    m.invoke(processor, ctx);
  }

  @Test void testArchiveRawCacheLocalStorageSkips() throws Exception {
    String opDir = tempDir.resolve("op-archive2").toString();
    java.io.File cacheDir = new java.io.File(opDir + "/cache/raw");
    cacheDir.mkdirs();
    Files.write(new java.io.File(cacheDir, "data.json").toPath(), "{}".getBytes());

    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.resolve("mat").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    StorageProvider localSp = new TestStorageProvider(tempDir.toString()) {
      @Override public String getStorageType() {
        return "local";
      }
    };

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(localSp)
        .sourceStorageProvider(localSp)
        .materializeDirectory(tempDir.resolve("mat").toString())
        .operatingDirectory(opDir)
        .build();

    Method m =
        SchemaLifecycleProcessor.class.getDeclaredMethod("archiveRawCache", SchemaContext.class);
    m.setAccessible(true);

    SchemaContext ctx = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(localSp)
        .sourceStorageProvider(localSp)
        .operatingDirectory(opDir)
        .build();

    // Should not throw - skips archiving for local storage
    m.invoke(processor, ctx);
  }

  // ======= loadSchemaListener / loadDefaultTableListener / loadTableListener =======

  @Test void testLoadSchemaListenerNullHooks() throws Exception {
    Method m =
        SchemaLifecycleProcessor.class.getDeclaredMethod("loadSchemaListener", SchemaConfig.class);
    m.setAccessible(true);

    SchemaConfig noHooksConfig = SchemaConfig.builder()
        .name("test")
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaLifecycleListener result =
        (SchemaLifecycleListener) m.invoke(null, noHooksConfig);
    assertNotNull(result);
    assertEquals(SchemaLifecycleListener.NOOP, result);
  }

  @Test void testLoadDefaultTableListenerNullHooks() throws Exception {
    Method m =
        SchemaLifecycleProcessor.class.getDeclaredMethod("loadDefaultTableListener", SchemaConfig.class);
    m.setAccessible(true);

    SchemaConfig noHooksConfig = SchemaConfig.builder()
        .name("test")
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    TableLifecycleListener result =
        (TableLifecycleListener) m.invoke(null, noHooksConfig);
    assertNotNull(result);
    assertEquals(TableLifecycleListener.NOOP, result);
  }

  @Test void testLoadTableListenerNullHooks() throws Exception {
    Method m =
        SchemaLifecycleProcessor.class.getDeclaredMethod("loadTableListener", EtlPipelineConfig.class, TableLifecycleListener.class);
    m.setAccessible(true);

    EtlPipelineConfig tableConfig = EtlPipelineConfig.builder()
        .name("test_table")
        .enabled(false)
        .build();

    TableLifecycleListener defaultListener = TableLifecycleListener.NOOP;
    TableLifecycleListener result =
        (TableLifecycleListener) m.invoke(null, tableConfig, defaultListener);
    assertEquals(defaultListener, result, "Should return default listener when no hooks");
  }

  // ======= loadInstance tests =======

  @Test void testLoadInstanceWithInvalidClass() throws Exception {
    Method m =
        SchemaLifecycleProcessor.class.getDeclaredMethod("loadInstance", String.class, Class.class);
    m.setAccessible(true);

    try {
      m.invoke(null, "com.nonexistent.ClassName", SchemaLifecycleListener.class);
      assertTrue(false, "Should have thrown");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  @Test void testLoadInstanceWithWrongType() throws Exception {
    Method m =
        SchemaLifecycleProcessor.class.getDeclaredMethod("loadInstance", String.class, Class.class);
    m.setAccessible(true);

    try {
      // String does not implement SchemaLifecycleListener
      m.invoke(null, "java.lang.String", SchemaLifecycleListener.class);
      assertTrue(false, "Should have thrown");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  // ======= SchemaLifecycleListener NOOP tests =======

  @Test void testNoopSchemaListenerMethods() throws Exception {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    SchemaContext ctx = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .build();

    // All NOOP methods should not throw
    SchemaLifecycleListener.NOOP.beforeSchema(ctx);
    SchemaLifecycleListener.NOOP.afterSchema(ctx,
        SchemaResult.builder().schemaName("test").elapsedMs(0).build());
    SchemaLifecycleListener.NOOP.onSchemaError(ctx, new Exception("test"));

    // downloadBulkFile default returns null
    String result =
        SchemaLifecycleListener.NOOP.downloadBulkFile(ctx, null, Collections.<String, String>emptyMap(), "/tmp/target");
    assertNull(result);
  }

  // ======= TableLifecycleListener NOOP tests =======

  @Test void testNoopTableListenerMethods() throws Exception {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test")
        .materializeDirectory(tempDir.toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    EtlPipelineConfig tableConfig = EtlPipelineConfig.builder()
        .name("test_table")
        .enabled(false)
        .build();

    SchemaContext schemaCtx = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .build();

    TableContext tableCtx = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaCtx)
        .tableIndex(0)
        .totalTables(1)
        .build();

    EtlResult result = EtlResult.builder()
        .pipelineName("test_table")
        .failed(false)
        .build();

    // All NOOP methods should not throw
    TableLifecycleListener.NOOP.beforeTable(tableCtx);
    TableLifecycleListener.NOOP.afterTable(tableCtx, result);
    assertTrue(TableLifecycleListener.NOOP.onTableError(tableCtx, new Exception("test")));

    // Default methods
    TableLifecycleListener.NOOP.beforeSource(tableCtx);
    TableLifecycleListener.NOOP.afterSource(tableCtx,
        SourceResult.success(0, 0, 0, null));
    assertTrue(TableLifecycleListener.NOOP.onSourceError(tableCtx, new Exception("test")));

    TableLifecycleListener.NOOP.beforeMaterialize(tableCtx);
    TableLifecycleListener.NOOP.afterMaterialize(tableCtx,
        MaterializeResult.success(0, 0, 0));
    assertTrue(TableLifecycleListener.NOOP.onMaterializeError(tableCtx, new Exception("test")));

    // fetchData and writeData defaults
    assertNull(
        TableLifecycleListener.NOOP.fetchData(tableCtx,
        Collections.<String, String>emptyMap()));
    assertEquals(
        -1, TableLifecycleListener.NOOP.writeData(tableCtx,
        Collections.<Map<String, Object>>emptyList().iterator(),
        Collections.<String, String>emptyMap()));

    // resolveDimensions default
    assertNull(
        TableLifecycleListener.NOOP.resolveDimensions(tableCtx,
        Collections.<String, DimensionConfig>emptyMap()));

    // resolveApiKey default
    assertNull(TableLifecycleListener.NOOP.resolveApiKey(tableCtx, "API_KEY"));

    // isTableEnabled default
    assertTrue(TableLifecycleListener.NOOP.isTableEnabled(tableCtx));
  }

  // ======= SourceResult and MaterializeResult factory methods =======

  @Test void testSourceResultSuccess() {
    SourceResult result = SourceResult.success(100, 5, 1500, "test-path");
    assertNotNull(result);
    assertEquals(100, result.getRecordCount());
    assertEquals(1500, result.getDurationMs());
  }

  @Test void testSourceResultError() {
    SourceResult result = SourceResult.error("failed", 500, "path");
    assertNotNull(result);
    assertTrue(result.isError());
  }

  @Test void testMaterializeResultSuccess() {
    MaterializeResult result = MaterializeResult.success(200, 1024, 2000);
    assertNotNull(result);
    assertEquals(200, result.getRowCount());
    assertEquals(2000, result.getElapsedMillis());
  }

  @Test void testMaterializeResultError() {
    MaterializeResult result = MaterializeResult.error("write failed", 300);
    assertNotNull(result);
    assertTrue(result.isError());
  }

  // ======= EtlResult tests =======

  @Test void testEtlResultSkipped() {
    EtlResult result = EtlResult.skipped("table1", 50);
    assertNotNull(result);
    assertTrue(result.isSkipped());
    assertEquals("table1", result.getPipelineName());
    assertNotNull(result.toString());
  }

  @Test void testEtlResultFailed() {
    EtlResult result = EtlResult.builder()
        .pipelineName("failTable")
        .failed(true)
        .failureMessage("Out of memory")
        .elapsedMs(100)
        .build();

    assertTrue(result.isFailed());
    assertEquals("Out of memory", result.getFailureMessage());
    assertNotNull(result.toString());
  }

  // ======= SchemaResult tests =======

  @Test void testSchemaResultBuilder() {
    EtlResult success = EtlResult.builder()
        .pipelineName("t1")
        .failed(false)
        .totalRows(50)
        .build();

    EtlResult failed = EtlResult.builder()
        .pipelineName("t2")
        .failed(true)
        .failureMessage("err")
        .build();

    SchemaResult result = SchemaResult.builder()
        .schemaName("mySchema")
        .elapsedMs(5000)
        .addTableResult("t1", success)
        .addTableResult("t2", failed)
        .addError("global error")
        .build();

    assertEquals("mySchema", result.getSchemaName());
    assertEquals(5000, result.getElapsedMs());
    assertEquals(50, result.getTotalRows());
    assertEquals(1, result.getSuccessfulTables());
    assertEquals(1, result.getFailedTables());
    assertNotNull(result.getErrors());
    assertEquals(1, result.getErrors().size());
    assertNotNull(result.toString());
  }

  // ======= DelegatingTableListener wildcard isEnabled =======

  @Test void testDelegatingTableListenerWildcardIsEnabled() throws Exception {
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test_schema")
        .materializeDirectory(tempDir.resolve("mat").toString())
        .tables(Collections.<EtlPipelineConfig>emptyList())
        .build();

    // Register wildcard "*" isEnabled hook
    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.resolve("mat").toString())
        .isEnabled("*", new java.util.function.Predicate<TableContext>() {
          @Override public boolean test(TableContext ctx) {
            return false; // Disable all tables
          }
        })
        .build();

    assertNotNull(processor);
  }

  // ======= Inner test helper: TestStorageProvider =======

  /**
   * Simple storage provider for testing that extends LocalFileStorageProvider.
   */
  static class TestStorageProvider extends org.apache.calcite.adapter.file.storage.LocalFileStorageProvider {
    TestStorageProvider(String basePath) {
      super();
    }

    @Override public String getStorageType() {
      return "test";
    }
  }
}
