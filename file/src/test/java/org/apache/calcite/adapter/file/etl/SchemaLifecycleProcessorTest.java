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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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

  @Test void testLifecycleHooksAreCalled() throws IOException {
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

  @Test void testSchemaConfigFromMap() {
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

  @Test void testSchemaResultAggregation() {
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

  // ---------------------------------------------------------------
  // Builder validation - required fields
  // ---------------------------------------------------------------

  @Test void testBuilderRequiresConfig() {
    try {
      SchemaLifecycleProcessor.builder()
          .storageProvider(storageProvider)
          .materializeDirectory(tempDir.toString())
          .build();
      fail("Should throw when config is null");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("config"),
          "Error should mention config: " + e.getMessage());
    }
  }

  @Test void testBuilderRequiresStorageProvider() {
    SchemaConfig config = createMinimalSchemaConfig("sp_test");
    try {
      SchemaLifecycleProcessor.builder()
          .config(config)
          .build();
      fail("Should throw when storageProvider is null");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().toLowerCase().contains("storage"),
          "Error should mention storage provider: " + e.getMessage());
    }
  }

  @Test void testBuilderRequiresMaterializeDirectory() {
    // Build a config without materializeDirectory at either level
    // Use createTableMap which includes a materialize section in the table itself
    // but the schema-level materializeDirectory is null
    SchemaConfig config = SchemaConfig.builder()
        .name("no_mat_dir")
        .addTable(EtlPipelineConfig.fromMap(createTableMap("test")))
        .build();

    // Verify schema-level materializeDirectory is null
    assertNull(config.getMaterializeDirectory(),
        "Schema-level materializeDirectory should be null");

    try {
      SchemaLifecycleProcessor.builder()
          .config(config)
          .storageProvider(storageProvider)
          .build();
      fail("Should throw when materializeDirectory is null");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().toLowerCase().contains("materialize")
              || e.getMessage().toLowerCase().contains("directory"),
          "Error should mention materialize directory: " + e.getMessage());
    }
  }

  // ---------------------------------------------------------------
  // SchemaConfig.builder() with multiple tables
  // ---------------------------------------------------------------

  @Test void testSchemaConfigWithMultipleTables() {
    EtlPipelineConfig table1 = EtlPipelineConfig.fromMap(createTableMap("gdp"));
    EtlPipelineConfig table2 = EtlPipelineConfig.fromMap(createTableMap("employment"));
    EtlPipelineConfig table3 = EtlPipelineConfig.fromMap(createTableMap("inflation"));

    SchemaConfig config = SchemaConfig.builder()
        .name("multi_table")
        .materializeDirectory(tempDir.toString())
        .addTable(table1)
        .addTable(table2)
        .addTable(table3)
        .build();

    assertEquals(3, config.getTableCount());
    assertNotNull(config.getTable("gdp"), "Should find table 'gdp'");
    assertNotNull(config.getTable("employment"), "Should find table 'employment'");
    assertNotNull(config.getTable("inflation"), "Should find table 'inflation'");
    assertNull(config.getTable("nonexistent"), "Should not find unknown table");
  }

  @Test void testSchemaConfigBuilderWithTablesList() {
    List<EtlPipelineConfig> tables = new ArrayList<EtlPipelineConfig>();
    tables.add(EtlPipelineConfig.fromMap(createTableMap("t1")));
    tables.add(EtlPipelineConfig.fromMap(createTableMap("t2")));

    SchemaConfig config = SchemaConfig.builder()
        .name("list_tables")
        .materializeDirectory(tempDir.toString())
        .tables(tables)
        .build();

    assertEquals(2, config.getTableCount());
    assertEquals("list_tables", config.getName());
  }

  @Test void testSchemaConfigRequiresName() {
    try {
      SchemaConfig.builder()
          .materializeDirectory(tempDir.toString())
          .build();
      fail("Should throw when name is null");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("name"),
          "Error should mention name: " + e.getMessage());
    }
  }

  // ---------------------------------------------------------------
  // SchemaConfig.fromMap() with hooks configuration
  // ---------------------------------------------------------------

  @Test void testSchemaConfigFromMapWithHooks() {
    Map<String, Object> schemaMap = new LinkedHashMap<String, Object>();
    schemaMap.put("name", "hooked_schema");
    schemaMap.put("sourceDirectory", "/data/raw");
    schemaMap.put("materializeDirectory", "/data/parquet");

    Map<String, Object> hooksMap = new LinkedHashMap<String, Object>();
    hooksMap.put("schemaLifecycleListener", "com.example.SchemaHook");
    hooksMap.put("tableLifecycleListener", "com.example.TableHook");
    schemaMap.put("hooks", hooksMap);

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(createTableMap("sales"));
    schemaMap.put("tables", tables);

    SchemaConfig config = SchemaConfig.fromMap(schemaMap);

    assertNotNull(config);
    assertEquals("hooked_schema", config.getName());
    assertEquals("/data/raw", config.getSourceDirectory());
    assertEquals("/data/parquet", config.getMaterializeDirectory());
    assertEquals("com.example.SchemaHook",
        config.getHooks().getSchemaLifecycleListenerClass());
    assertEquals("com.example.TableHook",
        config.getHooks().getTableLifecycleListenerClass());
  }

  @Test void testSchemaConfigFromMapWithNullHooks() {
    Map<String, Object> schemaMap = new LinkedHashMap<String, Object>();
    schemaMap.put("name", "no_hooks");

    List<Map<String, Object>> tables = new ArrayList<Map<String, Object>>();
    tables.add(createTableMap("data"));
    schemaMap.put("tables", tables);

    SchemaConfig config = SchemaConfig.fromMap(schemaMap);

    assertNotNull(config);
    assertNotNull(config.getHooks(), "Hooks should never be null");
    assertNull(config.getHooks().getSchemaLifecycleListenerClass());
    assertNull(config.getHooks().getTableLifecycleListenerClass());
  }

  @Test void testSchemaConfigFromMapReturnsNullForNull() {
    assertNull(SchemaConfig.fromMap(null));
  }

  // ---------------------------------------------------------------
  // SchemaResult aggregation - all success, all failure, mixed
  // ---------------------------------------------------------------

  @Test void testSchemaResultAllSuccess() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("all_success");

    builder.addTableResult("t1", EtlResult.success("t1", 500, 5, 200));
    builder.addTableResult("t2", EtlResult.success("t2", 300, 3, 150));
    builder.addTableResult("t3", EtlResult.success("t3", 200, 2, 100));
    builder.elapsedMs(500);

    SchemaResult result = builder.build();

    assertEquals("all_success", result.getSchemaName());
    assertEquals(3, result.getTotalTables());
    assertEquals(3, result.getSuccessfulTables());
    assertEquals(0, result.getFailedTables());
    assertEquals(0, result.getSkippedTables());
    assertEquals(1000, result.getTotalRows());
    assertEquals(500, result.getElapsedMs());
    assertFalse(result.hasErrors(), "All-success result should not have errors");
    assertTrue(result.getErrors().isEmpty());
  }

  @Test void testSchemaResultAllFailure() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("all_fail");

    builder.addTableResult("t1", EtlResult.failure("t1", "timeout", 100));
    builder.addTableResult("t2", EtlResult.failure("t2", "404", 50));
    builder.elapsedMs(200);

    SchemaResult result = builder.build();

    assertEquals(2, result.getTotalTables());
    assertEquals(0, result.getSuccessfulTables());
    assertEquals(2, result.getFailedTables());
    assertEquals(0, result.getSkippedTables());
    assertEquals(0, result.getTotalRows());
    assertTrue(result.hasErrors(), "All-failure result should have errors");
  }

  @Test void testSchemaResultMixedResults() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("mixed");

    builder.addTableResult("ok", EtlResult.success("ok", 1000, 10, 500));
    builder.addTableResult("fail", EtlResult.failure("fail", "error", 100));
    builder.addTableResult("skip", EtlResult.skipped("skip", 10));
    builder.elapsedMs(700);

    SchemaResult result = builder.build();

    assertEquals(3, result.getTotalTables());
    assertEquals(1, result.getSuccessfulTables());
    assertEquals(1, result.getFailedTables());
    assertEquals(1, result.getSkippedTables());
    assertEquals(1000, result.getTotalRows());
    assertTrue(result.hasErrors());
  }

  @Test void testSchemaResultEmptyResults() {
    SchemaResult result = SchemaResult.builder()
        .schemaName("empty")
        .elapsedMs(0)
        .build();

    assertEquals(0, result.getTotalTables());
    assertEquals(0, result.getSuccessfulTables());
    assertEquals(0, result.getFailedTables());
    assertEquals(0, result.getSkippedTables());
    assertEquals(0, result.getTotalRows());
    assertFalse(result.hasErrors());
  }

  @Test void testSchemaResultAddError() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("with_error");
    builder.addError("Something went wrong");
    builder.elapsedMs(100);

    SchemaResult result = builder.build();
    assertTrue(result.hasErrors());
    assertEquals(1, result.getErrors().size());
    assertEquals("Something went wrong", result.getErrors().get(0));
  }

  // ---------------------------------------------------------------
  // SchemaResult.toString() verification
  // ---------------------------------------------------------------

  @Test void testSchemaResultToString() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("display");

    builder.addTableResult("t1", EtlResult.success("t1", 500, 5, 200));
    builder.addTableResult("t2", EtlResult.failure("t2", "err", 50));
    builder.elapsedMs(300);

    SchemaResult result = builder.build();
    String str = result.toString();

    assertNotNull(str);
    assertTrue(str.contains("display"), "toString should contain schema name");
    assertTrue(str.contains("2"), "toString should contain table count");
    assertTrue(str.contains("1 ok"), "toString should contain success count");
    assertTrue(str.contains("1 failed"), "toString should contain failure count");
    assertTrue(str.contains("500"), "toString should contain row count");
    assertTrue(str.contains("300ms"), "toString should contain elapsed time");
  }

  @Test void testSchemaResultToStringEmptySchema() {
    SchemaResult result = SchemaResult.builder()
        .schemaName("empty_str")
        .elapsedMs(0)
        .build();
    String str = result.toString();

    assertNotNull(str);
    assertTrue(str.contains("empty_str"));
    assertTrue(str.contains("0"));
  }

  // ---------------------------------------------------------------
  // SchemaResult convenience constructor for error cases
  // ---------------------------------------------------------------

  @Test void testSchemaResultConvenienceConstructor() {
    SchemaResult result = new SchemaResult("err_schema", 2, 1, 0, 5000, 1500, "global error");

    assertEquals("err_schema", result.getSchemaName());
    assertEquals(2, result.getSuccessfulTables());
    assertEquals(1, result.getFailedTables());
    assertEquals(0, result.getSkippedTables());
    assertEquals(3, result.getTotalTables());
    assertEquals(5000, result.getTotalRows());
    assertEquals(1500, result.getElapsedMs());
    assertTrue(result.hasErrors());
    assertEquals(1, result.getErrors().size());
    assertEquals("global error", result.getErrors().get(0));
  }

  @Test void testSchemaResultConvenienceConstructorNullError() {
    SchemaResult result = new SchemaResult("ok_schema", 3, 0, 0, 10000, 2000, null);

    assertFalse(result.hasErrors());
    assertTrue(result.getErrors().isEmpty());
  }

  // ---------------------------------------------------------------
  // TableContext creation and field access
  // ---------------------------------------------------------------

  @Test void testTableContextCreation() {
    SchemaConfig schemaConfig = createMinimalSchemaConfig("ctx_schema");
    SchemaContext schemaContext = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    EtlPipelineConfig tableConfig = schemaConfig.getTables().get(0);

    TableContext tableContext = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .tableIndex(0)
        .totalTables(1)
        .build();

    assertNotNull(tableContext);
    assertEquals("test_table", tableContext.getTableName());
    assertEquals(0, tableContext.getTableIndex());
    assertEquals(1, tableContext.getTotalTables());
    assertEquals("ctx_schema", tableContext.getSchemaName());
    assertNotNull(tableContext.getSchemaContext());
    assertNotNull(tableContext.getTableConfig());
    assertNotNull(tableContext.getAttributes());
    assertTrue(tableContext.getAttributes().isEmpty());
  }

  @Test void testTableContextAttributes() {
    SchemaConfig schemaConfig = createMinimalSchemaConfig("attr_schema");
    SchemaContext schemaContext = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    EtlPipelineConfig tableConfig = schemaConfig.getTables().get(0);

    TableContext tableContext = TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .tableIndex(0)
        .totalTables(1)
        .build();

    // Set and get attributes
    tableContext.setAttribute("key1", "value1");
    tableContext.setAttribute("key2", 42);

    assertEquals("value1", (String) tableContext.getAttribute("key1"));
    assertEquals(42, (int) (Integer) tableContext.getAttribute("key2"));
    assertNull(tableContext.getAttribute("nonexistent"));
  }

  @Test void testTableContextRequiresTableConfig() {
    SchemaConfig schemaConfig = createMinimalSchemaConfig("tc_req");
    SchemaContext schemaContext = SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    try {
      TableContext.builder()
          .schemaContext(schemaContext)
          .build();
      fail("Should throw when tableConfig is null");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().toLowerCase().contains("table"),
          "Error should mention table config: " + e.getMessage());
    }
  }

  @Test void testTableContextRequiresSchemaContext() {
    SchemaConfig schemaConfig = createMinimalSchemaConfig("sc_req");
    EtlPipelineConfig tableConfig = schemaConfig.getTables().get(0);

    try {
      TableContext.builder()
          .tableConfig(tableConfig)
          .build();
      fail("Should throw when schemaContext is null");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().toLowerCase().contains("schema"),
          "Error should mention schema context: " + e.getMessage());
    }
  }

  // ---------------------------------------------------------------
  // SchemaContext creation and field access
  // ---------------------------------------------------------------

  @Test void testSchemaContextCreation() {
    SchemaConfig config = createMinimalSchemaConfig("sc_test");

    SchemaContext context = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .sourceDirectory("/data/source")
        .materializeDirectory("/data/output")
        .operatingDirectory("/data/.aperio/sc_test")
        .build();

    assertNotNull(context);
    assertEquals("sc_test", context.getSchemaName());
    assertEquals(config, context.getConfig());
    assertEquals(storageProvider, context.getStorageProvider());
    assertEquals("/data/source", context.getSourceDirectory());
    assertEquals("/data/output", context.getMaterializeDirectory());
    assertEquals("/data/.aperio/sc_test", context.getOperatingDirectory());
    assertNotNull(context.getAttributes());
    assertTrue(context.getAttributes().isEmpty());
    assertNotNull(context.getIncrementalTracker());
  }

  @Test void testSchemaContextAttributes() {
    SchemaConfig config = createMinimalSchemaConfig("sc_attr");

    SchemaContext context = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    context.setAttribute("myKey", "myValue");
    assertEquals("myValue", (String) context.getAttribute("myKey"));
    assertNull(context.getAttribute("missing"));
  }

  @Test void testSchemaContextBulkDownloadPaths() {
    SchemaConfig config = createMinimalSchemaConfig("sc_bulk");

    SchemaContext context = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    context.setBulkDownloadPath("census", "year=2020", "/cache/census_2020.zip");
    context.setBulkDownloadPath("census", "year=2021", "/cache/census_2021.zip");

    assertEquals("/cache/census_2020.zip",
        context.getBulkDownloadPath("census", "year=2020"));
    assertEquals("/cache/census_2021.zip",
        context.getBulkDownloadPath("census", "year=2021"));
    assertNull(context.getBulkDownloadPath("census", "year=2022"));
    assertNull(context.getBulkDownloadPath("nonexistent", "default"));
  }

  @Test void testSchemaContextRequiresConfig() {
    try {
      SchemaContext.builder()
          .storageProvider(storageProvider)
          .build();
      fail("Should throw when config is null");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().toLowerCase().contains("config"),
          "Error should mention config: " + e.getMessage());
    }
  }

  @Test void testSchemaContextRequiresStorageProvider() {
    SchemaConfig config = createMinimalSchemaConfig("sc_sp");
    try {
      SchemaContext.builder()
          .config(config)
          .build();
      fail("Should throw when storageProvider is null");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().toLowerCase().contains("storage"),
          "Error should mention storage provider: " + e.getMessage());
    }
  }

  @Test void testSchemaContextSourceStorageProviderFallback() {
    SchemaConfig config = createMinimalSchemaConfig("sc_fallback");

    SchemaContext context = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    // sourceStorageProvider should fall back to main storageProvider
    assertEquals(storageProvider, context.getSourceStorageProvider());
  }

  @Test void testSchemaContextSourceStorageProviderExplicit() {
    SchemaConfig config = createMinimalSchemaConfig("sc_explicit");
    StorageProvider sourceProvider = new LocalFileStorageProvider();

    SchemaContext context = SchemaContext.builder()
        .config(config)
        .storageProvider(storageProvider)
        .sourceStorageProvider(sourceProvider)
        .materializeDirectory(tempDir.toString())
        .build();

    assertEquals(sourceProvider, context.getSourceStorageProvider());
  }

  // ---------------------------------------------------------------
  // HooksConfig.fromMap() with all fields populated
  // ---------------------------------------------------------------

  @Test void testHooksConfigFromMapAllFields() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("enabled", true);
    map.put("responseTransformer", "com.example.MyTransformer");
    map.put("dimensionResolver", "com.example.MyResolver");
    map.put("dataProvider", "com.example.MyDataProvider");
    map.put("tableLifecycleListener", "com.example.MyTableListener");
    map.put("variableNormalizer", "com.example.MyNormalizer");

    Map<String, Object> normalizerConfig = new LinkedHashMap<String, Object>();
    normalizerConfig.put("mappingFile", "/path/to/mapping.yaml");
    map.put("variableNormalizerConfig", normalizerConfig);

    Map<String, Object> errorMap = new LinkedHashMap<String, Object>();
    errorMap.put("responseTransformer", "fail");
    errorMap.put("rowTransformer", "skip_row");
    errorMap.put("validator", "continue");
    map.put("errorHandling", errorMap);

    // Add row transformers
    List<Map<String, Object>> rowTransformers = new ArrayList<Map<String, Object>>();
    Map<String, Object> rt = new LinkedHashMap<String, Object>();
    rt.put("type", "class");
    rt.put("class", "com.example.RowTransformerImpl");
    rowTransformers.add(rt);
    map.put("rowTransformers", rowTransformers);

    // Add validators
    List<Map<String, Object>> validators = new ArrayList<Map<String, Object>>();
    Map<String, Object> v = new LinkedHashMap<String, Object>();
    v.put("type", "expression");
    v.put("condition", "amount > 0");
    v.put("action", "drop");
    validators.add(v);
    map.put("validators", validators);

    HooksConfig config = HooksConfig.fromMap(map);

    assertNotNull(config);
    assertTrue(config.isEnabled());
    assertEquals("com.example.MyTransformer", config.getResponseTransformerClass());
    assertEquals("com.example.MyResolver", config.getDimensionResolverClass());
    assertEquals("com.example.MyDataProvider", config.getDataProviderClass());
    assertEquals("com.example.MyTableListener", config.getTableLifecycleListenerClass());
    assertEquals("com.example.MyNormalizer", config.getVariableNormalizerClass());
    assertEquals("/path/to/mapping.yaml",
        config.getVariableNormalizerConfig().get("mappingFile"));
    assertTrue(config.hasHooks());

    // Row transformers
    assertEquals(1, config.getRowTransformers().size());
    assertTrue(config.getRowTransformers().get(0).isClassBased());
    assertEquals("com.example.RowTransformerImpl",
        config.getRowTransformers().get(0).getClassName());

    // Validators
    assertEquals(1, config.getValidators().size());
    assertTrue(config.getValidators().get(0).isExpressionBased());
    assertEquals("amount > 0", config.getValidators().get(0).getCondition());
    assertEquals("drop", config.getValidators().get(0).getAction());

    // Error handling
    assertNotNull(config.getErrorHandling());
    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.FAIL,
        config.getErrorHandling().getResponseTransformerAction());
    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.SKIP_ROW,
        config.getErrorHandling().getRowTransformerAction());
    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.CONTINUE,
        config.getErrorHandling().getValidatorAction());
  }

  @Test void testHooksConfigFromMapNull() {
    HooksConfig config = HooksConfig.fromMap(null);
    assertNotNull(config);
    assertFalse(config.hasHooks());
    assertTrue(config.isEnabled());
  }

  @Test void testHooksConfigEmpty() {
    HooksConfig config = HooksConfig.empty();
    assertNotNull(config);
    assertFalse(config.hasHooks());
    assertTrue(config.isEnabled());
    assertNull(config.getResponseTransformerClass());
    assertNull(config.getDimensionResolverClass());
    assertNull(config.getDataProviderClass());
    assertTrue(config.getRowTransformers().isEmpty());
    assertTrue(config.getValidators().isEmpty());
    assertTrue(config.getPostProcess().isEmpty());
  }

  @Test void testHooksConfigDisabled() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("enabled", false);

    HooksConfig config = HooksConfig.fromMap(map);
    assertFalse(config.isEnabled());
  }

  @Test void testHooksConfigEnabledAsString() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("enabled", "true");

    HooksConfig config = HooksConfig.fromMap(map);
    assertTrue(config.isEnabled());
  }

  @Test void testHooksConfigToString() {
    HooksConfig config = HooksConfig.builder()
        .responseTransformerClass("com.example.RT")
        .dimensionResolverClass("com.example.DR")
        .build();

    String str = config.toString();
    assertNotNull(str);
    assertTrue(str.contains("com.example.RT"));
    assertTrue(str.contains("com.example.DR"));
  }

  @Test void testHooksConfigErrorHandlingDefaults() {
    HooksConfig.HookErrorHandling defaults = HooksConfig.HookErrorHandling.defaults();
    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.FAIL,
        defaults.getResponseTransformerAction());
    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.SKIP_ROW,
        defaults.getRowTransformerAction());
    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.CONTINUE,
        defaults.getValidatorAction());
  }

  @Test void testHooksConfigErrorHandlingFromMapNull() {
    HooksConfig.HookErrorHandling handling = HooksConfig.HookErrorHandling.fromMap(null);
    // Should return defaults
    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.FAIL,
        handling.getResponseTransformerAction());
  }

  // ---------------------------------------------------------------
  // Multiple table listeners registered via builder
  // ---------------------------------------------------------------

  @Test void testMultipleTableListenersViaBuilder() throws IOException {
    AtomicInteger beforeCount = new AtomicInteger(0);
    AtomicInteger afterCount = new AtomicInteger(0);

    SchemaConfig config = createMinimalSchemaConfig("multi_listener");

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .defaultTableListener(new TableLifecycleListener() {
          @Override public void beforeTable(TableContext context) {
            beforeCount.incrementAndGet();
          }

          @Override public void afterTable(TableContext context, EtlResult result) {
            afterCount.incrementAndGet();
          }

          @Override public boolean onTableError(TableContext context, Exception error) {
            return true; // Continue after errors
          }
        })
        .build();

    try {
      processor.process();
    } catch (IOException e) {
      // Expected - no real HTTP source
    }

    assertTrue(beforeCount.get() > 0,
        "beforeTable should be called at least once");
  }

  @Test void testBuilderWithPerTableBeforeAndAfterHooks() throws IOException {
    AtomicBoolean testTableBeforeCalled = new AtomicBoolean(false);
    AtomicBoolean testTableAfterCalled = new AtomicBoolean(false);

    SchemaConfig config = createMinimalSchemaConfig("per_table_hooks");

    SchemaLifecycleProcessor processor = SchemaLifecycleProcessor.builder()
        .config(config)
        .storageProvider(storageProvider)
        .beforeTable("test_table", new java.util.function.Consumer<TableContext>() {
          @Override public void accept(TableContext ctx) {
            testTableBeforeCalled.set(true);
          }
        })
        .afterTable("test_table",
            new java.util.function.BiConsumer<TableContext, EtlResult>() {
              @Override public void accept(TableContext ctx, EtlResult result) {
                testTableAfterCalled.set(true);
              }
            })
        .onTableError("test_table",
            new java.util.function.BiFunction<TableContext, Exception, Boolean>() {
              @Override public Boolean apply(TableContext ctx, Exception ex) {
                return true;
              }
            })
        .build();

    try {
      processor.process();
    } catch (IOException e) {
      // Expected - no real HTTP source
    }

    assertTrue(testTableBeforeCalled.get(),
        "Per-table beforeTable hook should be called");
  }

  // ---------------------------------------------------------------
  // EtlResult factory methods and toString
  // ---------------------------------------------------------------

  @Test void testEtlResultSuccess() {
    EtlResult result = EtlResult.success("pipeline1", 5000, 10, 1500);

    assertEquals("pipeline1", result.getPipelineName());
    assertEquals(5000, result.getTotalRows());
    assertEquals(10, result.getSuccessfulBatches());
    assertEquals(1500, result.getElapsedMs());
    assertTrue(result.isSuccessful());
    assertTrue(result.isCompleteSuccess());
    assertFalse(result.isFailed());
    assertFalse(result.isSkipped());
  }

  @Test void testEtlResultFailure() {
    EtlResult result = EtlResult.failure("pipeline2", "Connection refused", 200);

    assertEquals("pipeline2", result.getPipelineName());
    assertTrue(result.isFailed());
    assertFalse(result.isSuccessful());
    assertEquals("Connection refused", result.getFailureMessage());
    assertEquals(200, result.getElapsedMs());
  }

  @Test void testEtlResultSkipped() {
    EtlResult result = EtlResult.skipped("pipeline3", 50);

    assertEquals("pipeline3", result.getPipelineName());
    assertTrue(result.isSkipped());
    assertTrue(result.isSkippedEntirePipeline());
    assertFalse(result.isFailed());
    assertEquals(50, result.getElapsedMs());
  }

  @Test void testEtlResultToStringSuccess() {
    EtlResult result = EtlResult.success("test_pipeline", 1000, 5, 500);
    String str = result.toString();
    assertNotNull(str);
    assertTrue(str.contains("test_pipeline"));
    assertTrue(str.contains("1000"));
  }

  @Test void testEtlResultToStringFailure() {
    EtlResult result = EtlResult.failure("fail_pipeline", "timeout", 100);
    String str = result.toString();
    assertNotNull(str);
    assertTrue(str.contains("FAILED"));
    assertTrue(str.contains("timeout"));
  }

  @Test void testEtlResultToStringSkipped() {
    EtlResult result = EtlResult.skipped("skip_pipeline", 10);
    String str = result.toString();
    assertNotNull(str);
    assertTrue(str.contains("SKIPPED"));
  }

  @Test void testEtlResultRowsPerSecond() {
    EtlResult result = EtlResult.success("perf", 10000, 10, 2000);
    // 10000 rows / 2 seconds = 5000.0 rows/sec
    assertEquals(5000.0, result.getRowsPerSecond(), 0.1);
  }

  @Test void testEtlResultRowsPerSecondZeroElapsed() {
    EtlResult result = EtlResult.success("zero", 100, 1, 0);
    assertEquals(0.0, result.getRowsPerSecond(), 0.01);
  }

  @Test void testSchemaResultGetTableResult() {
    SchemaResult.Builder builder = SchemaResult.builder()
        .schemaName("lookup_test");

    EtlResult t1Result = EtlResult.success("t1", 100, 1, 50);
    EtlResult t2Result = EtlResult.failure("t2", "error", 10);
    builder.addTableResult("t1", t1Result);
    builder.addTableResult("t2", t2Result);
    builder.elapsedMs(100);

    SchemaResult result = builder.build();

    assertEquals(t1Result, result.getTableResult("t1"));
    assertEquals(t2Result, result.getTableResult("t2"));
    assertNull(result.getTableResult("nonexistent"));
    assertEquals(2, result.getTableResults().size());
  }

  // ---------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------

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
