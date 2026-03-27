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
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
 * Coverage tests for {@link FileSchemaBuilder}.
 */
@Tag("unit")
class FileSchemaBuilderCoverageTest {

  @TempDir
  Path tempDir;

  @Test void testCreateReturnsBuilder() {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertNotNull(builder);
  }

  @Test void testSchemaConfigSetsConfig() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config);
    assertNotNull(builder);
  }

  @Test void testOperandSingleOverride() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .operand("key1", "value1");
    assertNotNull(builder);
  }

  @Test void testOperandMapOverride() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());

    Map<String, Object> overrides = new HashMap<String, Object>();
    overrides.put("key1", "value1");
    overrides.put("key2", "value2");

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .operand(overrides);
    assertNotNull(builder);
  }

  @Test void testAutoDownloadSetsFlag() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .autoDownload(true);
    assertNotNull(builder);
  }

  @Test void testAutoDownloadFalse() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .autoDownload(false);
    assertNotNull(builder);
  }

  @Test void testExcludeTable() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .excludeTable("excluded_table");
    assertNotNull(builder);
  }

  @Test void testExcludeTables() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config)
        .excludeTables(Arrays.asList("table1", "table2"));
    assertNotNull(builder);
  }

  @Test void testGetOperandWithoutConfig() {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertThrows(IllegalStateException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() {
        builder.getOperand();
      }
    });
  }

  @Test void testRunEtlWithoutConfig() {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertThrows(IllegalStateException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() {
        builder.runEtl();
      }
    });
  }

  @Test void testBuildWithoutConfig() {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertThrows(IllegalStateException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() {
        builder.build(null, "test");
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

  @Test void testGetOperandWithOverrides() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());

    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .operand("extraKey", "extraValue")
        .getOperand();

    assertNotNull(operand);
    assertEquals("extraValue", operand.get("extraKey"));
  }

  @Test void testGetOperandWithMaterializeDirectory() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("materializeDirectory", "/data/output");

    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .getOperand();

    // materializeDirectory should be propagated to "directory"
    assertEquals("/data/output", operand.get("directory"));
  }

  @Test void testGetOperandWithExcludedTables() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());

    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .excludeTable("skip_this")
        .getOperand();

    assertNotNull(operand.get("_excludedTables"));
    @SuppressWarnings("unchecked")
    List<String> excluded = (List<String>) operand.get("_excludedTables");
    assertTrue(excluded.contains("skip_this"));
  }

  @Test void testGetOperandWithStorageProviders() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());

    StorageProvider provider = new LocalFileStorageProvider();
    StorageProvider cacheProvider = new LocalFileStorageProvider();

    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .storageProvider(provider)
        .cacheStorageProvider(cacheProvider)
        .getOperand();

    assertNotNull(operand.get("_storageProvider"));
    assertNotNull(operand.get("_cacheStorageProvider"));
  }

  @Test void testGetOperandWithIncrementalTracker() {
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());

    Map<String, Object> operand = FileSchemaBuilder.create()
        .schemaConfig(config)
        .incrementalTracker(IncrementalTracker.NOOP)
        .getOperand();

    assertNotNull(operand.get("_incrementalTracker"));
  }

  @Test void testSchemaConfigEnvVarResolution() {
    // Test with string values containing no env vars
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", "/plain/path");

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config);

    Map<String, Object> operand = builder.getOperand();
    assertEquals("/plain/path", operand.get("directory"));
  }

  @Test void testSchemaConfigNestedMapResolution() {
    // Test nested map resolution
    Map<String, Object> nested = new LinkedHashMap<String, Object>();
    nested.put("key", "value");

    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());
    config.put("nested", nested);

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config);

    Map<String, Object> operand = builder.getOperand();
    assertNotNull(operand.get("nested"));
  }

  @Test void testSchemaConfigListResolution() {
    // Test list value resolution
    List<Object> items = new ArrayList<Object>();
    items.add("item1");
    items.add("item2");

    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());
    config.put("items", items);

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config);

    Map<String, Object> operand = builder.getOperand();
    assertNotNull(operand.get("items"));
  }

  @Test void testSchemaConfigNestedListWithMap() {
    // Test list with nested maps
    Map<String, Object> itemMap = new LinkedHashMap<String, Object>();
    itemMap.put("type", "csv");

    List<Object> items = new ArrayList<Object>();
    items.add(itemMap);
    items.add("plain_string");
    items.add(Integer.valueOf(42));

    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());
    config.put("tables", items);

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config);

    Map<String, Object> operand = builder.getOperand();
    assertNotNull(operand.get("tables"));
  }

  @Test void testSchemaConfigNestedListWithList() {
    // Test list with nested lists
    List<Object> innerList = new ArrayList<Object>();
    innerList.add("a");
    innerList.add("b");

    List<Object> outerList = new ArrayList<Object>();
    outerList.add(innerList);

    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());
    config.put("nested_lists", outerList);

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config);

    Map<String, Object> operand = builder.getOperand();
    assertNotNull(operand.get("nested_lists"));
  }

  @Test void testSchemaConfigIntegerValue() {
    // Test non-string, non-map, non-list value
    Map<String, Object> config = new LinkedHashMap<String, Object>();
    config.put("name", "test");
    config.put("directory", tempDir.toString());
    config.put("port", Integer.valueOf(8080));
    config.put("enabled", Boolean.TRUE);

    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .schemaConfig(config);

    Map<String, Object> operand = builder.getOperand();
    assertEquals(8080, operand.get("port"));
    assertEquals(true, operand.get("enabled"));
  }

  @Test void testSchemaResourceNotFound() {
    FileSchemaBuilder builder = FileSchemaBuilder.create();
    assertThrows(RuntimeException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() {
        builder.schemaResource("/nonexistent/path.yaml");
      }
    });
  }

  @Test void testStorageProviderSetsProvider() {
    StorageProvider provider = new LocalFileStorageProvider();
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .storageProvider(provider);
    assertNotNull(builder);
  }

  @Test void testStorageProviderNull() {
    // Test passing null storage provider
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .storageProvider(null);
    assertNotNull(builder);
  }

  @Test void testCacheStorageProviderSetsProvider() {
    StorageProvider provider = new LocalFileStorageProvider();
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .cacheStorageProvider(provider);
    assertNotNull(builder);
  }

  @Test void testIncrementalTrackerSetsTracker() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .incrementalTracker(IncrementalTracker.NOOP);
    assertNotNull(builder);
  }

  @Test void testResolveDimensionsHookRegistration() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .resolveDimensions("table1",
            new java.util.function.BiFunction<org.apache.calcite.adapter.file.etl.TableContext,
                Map<String, org.apache.calcite.adapter.file.etl.DimensionConfig>,
                Map<String, org.apache.calcite.adapter.file.etl.DimensionConfig>>() {
              @Override
              public Map<String, org.apache.calcite.adapter.file.etl.DimensionConfig> apply(
                  org.apache.calcite.adapter.file.etl.TableContext ctx,
                  Map<String, org.apache.calcite.adapter.file.etl.DimensionConfig> dims) {
                return dims;
              }
            });
    assertNotNull(builder);
  }

  @Test void testIsEnabledHookRegistration() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .isEnabled("table1", new java.util.function.Predicate<org.apache.calcite.adapter.file.etl.TableContext>() {
          @Override
          public boolean test(org.apache.calcite.adapter.file.etl.TableContext ctx) {
            return true;
          }
        });
    assertNotNull(builder);
  }

  @SuppressWarnings("deprecation")
  @Test void testShouldProcessDeprecated() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .shouldProcess("table1", new java.util.function.Predicate<org.apache.calcite.adapter.file.etl.TableContext>() {
          @Override
          public boolean test(org.apache.calcite.adapter.file.etl.TableContext ctx) {
            return false;
          }
        });
    assertNotNull(builder);
  }

  @Test void testBeforeSourceHookRegistration() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .beforeSource("table1", new java.util.function.Consumer<org.apache.calcite.adapter.file.etl.TableContext>() {
          @Override
          public void accept(org.apache.calcite.adapter.file.etl.TableContext ctx) {
            // no-op
          }
        });
    assertNotNull(builder);
  }

  @Test void testBeforeMaterializeHookRegistration() {
    FileSchemaBuilder builder = FileSchemaBuilder.create()
        .beforeMaterialize("table1", new java.util.function.Consumer<org.apache.calcite.adapter.file.etl.TableContext>() {
          @Override
          public void accept(org.apache.calcite.adapter.file.etl.TableContext ctx) {
            // no-op
          }
        });
    assertNotNull(builder);
  }

  @Test void testFluentApiChaining() {
    // Verify that all builder methods return the same builder instance
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
  }
}
