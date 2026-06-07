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

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.TableContext;
import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage tests for {@link FileSchemaBuilder} focusing on all builder methods,
 * operand overrides, env var resolution, validation, exclusion, and hooks.
 */
@Tag("unit")
public class FileSchemaBuilderCoverageTest {

  @TempDir
  Path tempDir;

  // ====================================================================
  // Factory method
  // ====================================================================

  @Test void testCreateReturnsBuilder() {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertNotNull(builder);
  }

  @Test void testCreateReturnsDifferentInstances() {
    FileSchemaBuilder b1 = FileSchemaBuilder.create();
    FileSchemaBuilder b2 = FileSchemaBuilder.create();
    assertNotSame(b1, b2);
  }

  // ====================================================================
  // schemaConfig
  // ====================================================================

  @Test void testSchemaConfigSetsConfig() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());
    FileSchemaBuilder builder = FileSchemaBuilder.create().schemaConfig(config);
    assertNotNull(builder);
  }

  @Test void testSchemaConfigPreservesAllKeys() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("first", "1");
    config.put("second", "2");
    config.put("third", "3");
    FileSchemaBuilder builder = FileSchemaBuilder.create().schemaConfig(config);
    Map<String, Object> operand = builder.getOperand();
    // All keys should be present
    assertTrue(operand.containsKey("first"));
    assertTrue(operand.containsKey("second"));
    assertTrue(operand.containsKey("third"));
    assertEquals("1", operand.get("first"));
    assertEquals("2", operand.get("second"));
    assertEquals("3", operand.get("third"));
  }

  @Test void testSchemaConfigWithStringValues() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test_schema");
    config.put("directory", "/data/path");
    FileSchemaBuilder builder = FileSchemaBuilder.create().schemaConfig(config);
    Map<String, Object> operand = builder.getOperand();
    assertEquals("test_schema", operand.get("name"));
    assertEquals("/data/path", operand.get("directory"));
  }

  @Test void testSchemaConfigWithNestedMap() {
    Map<String, Object> nested = new LinkedHashMap<String, Object>();
    nested.put("key", "value");
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("nested", nested);
    FileSchemaBuilder builder = FileSchemaBuilder.create().schemaConfig(config);
    Map<String, Object> operand = builder.getOperand();
    assertNotNull(operand.get("nested"));
    assertTrue(operand.get("nested") instanceof Map);
  }

  @Test void testSchemaConfigWithList() {
    List<Object> items = new ArrayList<Object>();
    items.add("item1");
    items.add("item2");
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("items", items);
    FileSchemaBuilder builder = FileSchemaBuilder.create().schemaConfig(config);
    Map<String, Object> operand = builder.getOperand();
    assertNotNull(operand.get("items"));
  }

  @Test void testSchemaConfigWithNestedListOfMaps() {
    Map<String, Object> itemMap = new LinkedHashMap<String, Object>();
    itemMap.put("type", "csv");
    List<Object> items = new ArrayList<Object>();
    items.add(itemMap);
    items.add("plain_string");
    items.add(Integer.valueOf(42));
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("tables", items);
    FileSchemaBuilder builder = FileSchemaBuilder.create().schemaConfig(config);
    Map<String, Object> operand = builder.getOperand();
    assertNotNull(operand.get("tables"));
  }

  @Test void testSchemaConfigWithNestedListOfLists() {
    List<Object> inner = new ArrayList<Object>();
    inner.add("a");
    inner.add("b");
    List<Object> outer = new ArrayList<Object>();
    outer.add(inner);
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("nested_lists", outer);
    FileSchemaBuilder builder = FileSchemaBuilder.create().schemaConfig(config);
    Map<String, Object> operand = builder.getOperand();
    assertNotNull(operand.get("nested_lists"));
  }

  @Test void testSchemaConfigWithIntegerAndBooleanValues() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("port", Integer.valueOf(8080));
    config.put("enabled", Boolean.TRUE);
    FileSchemaBuilder builder = FileSchemaBuilder.create().schemaConfig(config);
    Map<String, Object> operand = builder.getOperand();
    assertEquals(8080, operand.get("port"));
    assertEquals(true, operand.get("enabled"));
  }

  // ====================================================================
  // schemaResource
  // ====================================================================

  @Test void testSchemaResourceNotFound() {
    final FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertThrows(RuntimeException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        builder.schemaResource("/nonexistent/path.yaml");
      }
    });
  }

  // ====================================================================
  // operand overrides
  // ====================================================================

  @Test void testOperandSingleOverride() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .operand("key1", "value1");
    Map<String, Object> operand = builder.getOperand();
    assertEquals("value1", operand.get("key1"));
  }

  @Test void testOperandSingleOverrideOverwritesConfig() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "original");
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .operand("name", "overridden");
    Map<String, Object> operand = builder.getOperand();
    assertEquals("overridden", operand.get("name"));
  }

  @Test void testOperandMapOverride() {
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

  @Test void testOperandMapOverrideMerge() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("existing", "keep");
    Map<String, Object> overrides = new HashMap<String, Object>();
    overrides.put("new_key", "new_value");
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .operand(overrides);
    Map<String, Object> operand = builder.getOperand();
    assertEquals("keep", operand.get("existing"));
    assertEquals("new_value", operand.get("new_key"));
  }

  @Test void testOperandNullValue() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .operand("nullKey", null);
    Map<String, Object> operand = builder.getOperand();
    assertTrue(operand.containsKey("nullKey"));
    assertNull(operand.get("nullKey"));
  }

  // ====================================================================
  // autoDownload
  // ====================================================================

  @Test void testAutoDownloadTrue() {
    FileSchemaBuilder builder = FileSchemaBuilder.create().autoDownload(true);
    assertNotNull(builder);
  }

  @Test void testAutoDownloadFalse() {
    FileSchemaBuilder builder = FileSchemaBuilder.create().autoDownload(false);
    assertNotNull(builder);
  }

  // ====================================================================
  // excludeTable / excludeTables
  // ====================================================================

  @Test void testExcludeTable() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .excludeTable("excluded");
    Map<String, Object> operand = builder.getOperand();
    assertNotNull(operand.get("_excludedTables"));
    @SuppressWarnings("unchecked")
    List<String> excluded = (List<String>) operand.get("_excludedTables");
    assertTrue(excluded.contains("excluded"));
  }

  @Test void testExcludeMultipleTables() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .excludeTable("t1")
        .excludeTable("t2");
    Map<String, Object> operand = builder.getOperand();
    @SuppressWarnings("unchecked")
    List<String> excluded = (List<String>) operand.get("_excludedTables");
    assertEquals(2, excluded.size());
    assertTrue(excluded.contains("t1"));
    assertTrue(excluded.contains("t2"));
  }

  @Test void testExcludeTables() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .excludeTables(Arrays.asList("a", "b", "c"));
    Map<String, Object> operand = builder.getOperand();
    @SuppressWarnings("unchecked")
    List<String> excluded = (List<String>) operand.get("_excludedTables");
    assertEquals(3, excluded.size());
  }

  @Test void testExcludeTablesDuplicate() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .excludeTable("dup")
        .excludeTable("dup");
    Map<String, Object> operand = builder.getOperand();
    @SuppressWarnings("unchecked")
    List<String> excluded = (List<String>) operand.get("_excludedTables");
    // Set-based, so no duplicate
    assertEquals(1, excluded.size());
  }

  @Test void testNoExcludedTablesOmitsKey() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .getOperand();
    assertNull(operand.get("_excludedTables"));
  }

  // ====================================================================
  // getOperand validation
  // ====================================================================

  @Test void testGetOperandWithoutConfig() {
    final FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertThrows(IllegalStateException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        builder.getOperand();
      }
    });
  }

  @Test void testGetOperandBasic() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());
    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .getOperand();
    assertNotNull(operand);
    assertEquals("test", operand.get("name"));
    assertEquals(tempDir.toString(), operand.get("directory"));
  }

  @Test void testGetOperandWithMaterializeDirectory() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("materializeDirectory", "/data/output");
    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .getOperand();
    assertEquals("/data/output", operand.get("directory"));
  }

  @Test void testGetOperandMaterializeDirOverridesDirectory() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", "/original");
    config.put("materializeDirectory", "/materialized");
    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .getOperand();
    assertEquals("/materialized", operand.get("directory"));
  }

  // ====================================================================
  // runEtl validation
  // ====================================================================

  @Test void testRunEtlWithoutConfig() {
    final FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertThrows(IllegalStateException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        builder.runEtl();
      }
    });
  }

  // ====================================================================
  // build validation
  // ====================================================================

  @Test void testBuildWithoutConfig() {
    final FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertThrows(IllegalStateException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        builder.build(null, "test");
      }
    });
  }

  // ====================================================================
  // storageProvider
  // ====================================================================

  @Test void testStorageProviderSetsProvider() {
    StorageProvider provider = new LocalFileStorageProvider();
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .storageProvider(provider);
    assertNotNull(builder);
  }

  @Test void testStorageProviderNull() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .storageProvider(null);
    assertNotNull(builder);
  }

  @Test void testStorageProviderInOperand() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    StorageProvider provider = new LocalFileStorageProvider();
    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .storageProvider(provider)
        .getOperand();
    assertSame(provider, operand.get("_storageProvider"));
  }

  @Test void testStorageProviderNullNotInOperand() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .getOperand();
    assertNull(operand.get("_storageProvider"));
  }

  // ====================================================================
  // cacheStorageProvider
  // ====================================================================

  @Test void testCacheStorageProviderSetsProvider() {
    StorageProvider provider = new LocalFileStorageProvider();
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .cacheStorageProvider(provider);
    assertNotNull(builder);
  }

  @Test void testCacheStorageProviderInOperand() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    StorageProvider provider = new LocalFileStorageProvider();
    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .cacheStorageProvider(provider)
        .getOperand();
    assertSame(provider, operand.get("_cacheStorageProvider"));
  }

  // ====================================================================
  // incrementalTracker
  // ====================================================================

  @Test void testIncrementalTrackerSetsTracker() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .incrementalTracker(IncrementalTracker.NOOP);
    assertNotNull(builder);
  }

  @Test void testIncrementalTrackerInOperand() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .incrementalTracker(IncrementalTracker.NOOP)
        .getOperand();
    assertNotNull(operand.get("_incrementalTracker"));
  }

  // ====================================================================
  // Hook registration tests
  // ====================================================================

  @Test void testResolveDimensionsHook() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .resolveDimensions("table1",
            new BiFunction<TableContext, Map<String, DimensionConfig>,
                Map<String, DimensionConfig>>() {
              @Override public Map<String, DimensionConfig> apply(
                  TableContext ctx, Map<String, DimensionConfig> dims) {
                return dims;
              }
            });
    assertNotNull(builder);
  }

  @Test void testIsEnabledHook() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .isEnabled("table1", new Predicate<TableContext>() {
          @Override public boolean test(TableContext ctx) {
            return true;
          }
        });
    assertNotNull(builder);
  }

  @SuppressWarnings("deprecation")
  @Test void testShouldProcessDeprecated() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .shouldProcess("table1", new Predicate<TableContext>() {
          @Override public boolean test(TableContext ctx) {
            return false;
          }
        });
    assertNotNull(builder);
  }

  @Test void testBeforeSourceHook() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .beforeSource("table1", new Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) {
            // no-op
          }
        });
    assertNotNull(builder);
  }

  @Test void testBeforeMaterializeHook() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .beforeMaterialize("table1", new Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) {
            // no-op
          }
        });
    assertNotNull(builder);
  }

  // ====================================================================
  // Fluent API chaining
  // ====================================================================

  @Test void testFluentApiChaining() {
    StorageProvider provider = new LocalFileStorageProvider();
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .operand("k", "v")
        .storageProvider(provider)
        .cacheStorageProvider(provider)
        .incrementalTracker(IncrementalTracker.NOOP)
        .autoDownload(false)
        .excludeTable("t1")
        .excludeTables(Arrays.asList("t2", "t3"));

    assertNotNull(builder);
    Map<String, Object> operand = builder.getOperand();
    assertEquals("v", operand.get("k"));
    assertSame(provider, operand.get("_storageProvider"));
    assertSame(provider, operand.get("_cacheStorageProvider"));
    assertNotNull(operand.get("_incrementalTracker"));
    @SuppressWarnings("unchecked")
    List<String> excluded = (List<String>) operand.get("_excludedTables");
    assertEquals(3, excluded.size());
  }

  @Test void testFluentApiReturnsBuilder() {
    FileSchemaBuilder b = FileSchemaBuilder.create();
    assertSame(b.getClass(), FileSchemaBuilder.class);
  }

  // ====================================================================
  // resolveAllEnvVars via reflection
  // ====================================================================

  @Test void testResolveAllEnvVarsPlainString() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    Method m = FileSchemaBuilder.class.getDeclaredMethod("resolveAllEnvVars", Map.class);
    m.setAccessible(true);

    Map<String, Object> input = new LinkedHashMap<String, Object>();
    input.put("key", "/plain/path");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(builder, input);
    assertEquals("/plain/path", result.get("key"));
  }

  @Test void testResolveAllEnvVarsNestedMap() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    Method m = FileSchemaBuilder.class.getDeclaredMethod("resolveAllEnvVars", Map.class);
    m.setAccessible(true);

    Map<String, Object> nested = new LinkedHashMap<String, Object>();
    nested.put("innerKey", "innerValue");
    Map<String, Object> input = new LinkedHashMap<String, Object>();
    input.put("outer", nested);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(builder, input);
    assertNotNull(result.get("outer"));
  }

  @Test void testResolveAllEnvVarsNonStringValue() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    Method m = FileSchemaBuilder.class.getDeclaredMethod("resolveAllEnvVars", Map.class);
    m.setAccessible(true);

    Map<String, Object> input = new LinkedHashMap<String, Object>();
    input.put("count", Integer.valueOf(42));
    input.put("flag", Boolean.TRUE);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(builder, input);
    assertEquals(42, result.get("count"));
    assertEquals(true, result.get("flag"));
  }

  // ====================================================================
  // resolveListEnvVars via reflection
  // ====================================================================

  @Test void testResolveListEnvVarsStrings() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    Method m = FileSchemaBuilder.class.getDeclaredMethod("resolveListEnvVars", java.util.List.class);
    m.setAccessible(true);

    List<Object> input = new ArrayList<Object>();
    input.add("plain1");
    input.add("plain2");

    @SuppressWarnings("unchecked")
    List<Object> result = (List<Object>) m.invoke(builder, input);
    assertEquals(2, result.size());
    assertEquals("plain1", result.get(0));
  }

  @Test void testResolveListEnvVarsWithMap() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    Method m = FileSchemaBuilder.class.getDeclaredMethod("resolveListEnvVars", java.util.List.class);
    m.setAccessible(true);

    Map<String, Object> mapItem = new LinkedHashMap<String, Object>();
    mapItem.put("key", "val");
    List<Object> input = new ArrayList<Object>();
    input.add(mapItem);

    @SuppressWarnings("unchecked")
    List<Object> result = (List<Object>) m.invoke(builder, input);
    assertEquals(1, result.size());
    assertTrue(result.get(0) instanceof Map);
  }

  @Test void testResolveListEnvVarsWithNestedList() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    Method m = FileSchemaBuilder.class.getDeclaredMethod("resolveListEnvVars", java.util.List.class);
    m.setAccessible(true);

    List<Object> inner = new ArrayList<Object>();
    inner.add("a");
    List<Object> input = new ArrayList<Object>();
    input.add(inner);

    @SuppressWarnings("unchecked")
    List<Object> result = (List<Object>) m.invoke(builder, input);
    assertEquals(1, result.size());
    assertTrue(result.get(0) instanceof List);
  }

  @Test void testResolveListEnvVarsWithMixed() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    Method m = FileSchemaBuilder.class.getDeclaredMethod("resolveListEnvVars", java.util.List.class);
    m.setAccessible(true);

    List<Object> input = new ArrayList<Object>();
    input.add("string");
    input.add(Integer.valueOf(99));
    input.add(Boolean.FALSE);

    @SuppressWarnings("unchecked")
    List<Object> result = (List<Object>) m.invoke(builder, input);
    assertEquals(3, result.size());
    assertEquals("string", result.get(0));
    assertEquals(99, result.get(1));
    assertEquals(false, result.get(2));
  }

  // ====================================================================
  // Private field access tests
  // ====================================================================

  @Test void testEtlExecutedFlagDefault() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    Field f = FileSchemaBuilder.class.getDeclaredField("etlExecuted");
    f.setAccessible(true);
    assertFalse((Boolean) f.get(builder));
  }

  @Test void testAutoDownloadFlagDefault() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    Field f = FileSchemaBuilder.class.getDeclaredField("autoDownload");
    f.setAccessible(true);
    assertFalse((Boolean) f.get(builder));
  }

  @Test void testAutoDownloadFlagSet() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create().autoDownload(true);
    Field f = FileSchemaBuilder.class.getDeclaredField("autoDownload");
    f.setAccessible(true);
    assertTrue((Boolean) f.get(builder));
  }

  @Test void testExcludedTablesFieldInitialized() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    Field f = FileSchemaBuilder.class.getDeclaredField("excludedTables");
    f.setAccessible(true);
    assertNotNull(f.get(builder));
  }

  @Test void testOperandOverridesFieldInitialized() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    Field f = FileSchemaBuilder.class.getDeclaredField("operandOverrides");
    f.setAccessible(true);
    assertNotNull(f.get(builder));
    assertTrue(((Map<?, ?>) f.get(builder)).isEmpty());
  }

  @Test void testConfigFieldNullBeforeSet() throws Exception {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    Field f = FileSchemaBuilder.class.getDeclaredField("config");
    f.setAccessible(true);
    assertNull(f.get(builder));
  }

  @Test void testConfigFieldSetAfterSchemaConfig() throws Exception {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    FileSchemaBuilder builder = FileSchemaBuilder.create().schemaConfig(config);
    Field f = FileSchemaBuilder.class.getDeclaredField("config");
    f.setAccessible(true);
    assertNotNull(f.get(builder));
  }

  // ====================================================================
  // Both storageProviders in operand
  // ====================================================================

  @Test void testBothStorageProvidersInOperand() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    StorageProvider sp = new LocalFileStorageProvider();
    StorageProvider csp = new LocalFileStorageProvider();
    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .storageProvider(sp)
        .cacheStorageProvider(csp)
        .getOperand();
    assertSame(sp, operand.get("_storageProvider"));
    assertSame(csp, operand.get("_cacheStorageProvider"));
  }

  // ====================================================================
  // Env var resolution with no actual env vars
  // ====================================================================

  @Test void testSchemaConfigEnvVarNoSubstitution() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", "/plain/path");
    FileSchemaBuilder builder = FileSchemaBuilder.create().schemaConfig(config);
    Map<String, Object> operand = builder.getOperand();
    assertEquals("/plain/path", operand.get("directory"));
  }
}
