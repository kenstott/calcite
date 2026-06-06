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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Comprehensive unit tests for all ETL interfaces in the
 * {@code org.apache.calcite.adapter.file.etl} package.
 *
 * <p>Since these are interfaces, tests verify default methods and
 * contracts through lambda/mock/stub implementations.
 */
@Tag("unit")
class EtlInterfacesCoverageTest {

  // ========== DataProvider ==========

  @Test void testDataProviderLambdaReturnsIterator() throws IOException {
    final List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("name", "test");
    records.add(row);

    DataProvider provider = (config, variables) -> records.iterator();

    EtlPipelineConfig config = createMinimalPipelineConfig("test_table");
    Map<String, String> variables = Collections.singletonMap("year", "2024");

    Iterator<Map<String, Object>> result = provider.fetch(config, variables);
    assertNotNull(result);
    assertTrue(result.hasNext());
    Map<String, Object> fetched = result.next();
    assertEquals(1, fetched.get("id"));
    assertEquals("test", fetched.get("name"));
    assertFalse(result.hasNext());
  }

  @Test void testDataProviderLambdaWithEmptyVariables() throws IOException {
    DataProvider provider = (config, variables) ->
        Collections.<Map<String, Object>>emptyList().iterator();

    Iterator<Map<String, Object>> result =
        provider.fetch(createMinimalPipelineConfig("empty"), Collections.<String, String>emptyMap());
    assertNotNull(result);
    assertFalse(result.hasNext());
  }

  @Test void testDataProviderDefaultReturnsNull() throws IOException {
    DataProvider defaultProvider = DataProvider.DEFAULT;
    assertNotNull(defaultProvider);
    Iterator<Map<String, Object>> result =
        defaultProvider.fetch(createMinimalPipelineConfig("test"), Collections.<String, String>emptyMap());
    assertNull(result);
  }

  @Test void testDataProviderLambdaThatThrowsIOException() {
    DataProvider provider = (config, variables) -> {
      throw new IOException("Connection refused");
    };

    try {
      provider.fetch(createMinimalPipelineConfig("fail"), Collections.<String, String>emptyMap());
      assertTrue(false, "Expected IOException");
    } catch (IOException e) {
      assertEquals("Connection refused", e.getMessage());
    }
  }

  // ========== DataSource ==========

  @Test void testDataSourceStubImplementation() throws IOException {
    DataSource source = new DataSource() {
      @Override public Iterator<Map<String, Object>> fetch(Map<String, String> variables)
          throws IOException {
        List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("value", 42);
        data.add(row);
        return data.iterator();
      }

      @Override public String getType() {
        return "test";
      }
    };

    assertEquals("test", source.getType());
    Iterator<Map<String, Object>> iter = source.fetch(Collections.<String, String>emptyMap());
    assertTrue(iter.hasNext());
    assertEquals(42, iter.next().get("value"));
  }

  @Test void testDataSourceDefaultEstimateRowCount() {
    DataSource source = new DataSource() {
      @Override public Iterator<Map<String, Object>> fetch(Map<String, String> variables) {
        return Collections.<Map<String, Object>>emptyList().iterator();
      }

      @Override public String getType() {
        return "file";
      }
    };

    assertEquals(-1L, source.estimateRowCount());
  }

  @Test void testDataSourceOverriddenEstimateRowCount() {
    DataSource source = new DataSource() {
      @Override public Iterator<Map<String, Object>> fetch(Map<String, String> variables) {
        return Collections.<Map<String, Object>>emptyList().iterator();
      }

      @Override public String getType() {
        return "http";
      }

      @Override public long estimateRowCount() {
        return 5000L;
      }
    };

    assertEquals(5000L, source.estimateRowCount());
  }

  @Test void testDataSourceDefaultCloseIsNoOp() throws IOException {
    DataSource source = new DataSource() {
      @Override public Iterator<Map<String, Object>> fetch(Map<String, String> variables) {
        return Collections.<Map<String, Object>>emptyList().iterator();
      }

      @Override public String getType() {
        return "query";
      }
    };

    // Default close should not throw
    source.close();
  }

  @Test void testDataSourceOverriddenClose() throws IOException {
    final boolean[] closed = {false};
    DataSource source = new DataSource() {
      @Override public Iterator<Map<String, Object>> fetch(Map<String, String> variables) {
        return Collections.<Map<String, Object>>emptyList().iterator();
      }

      @Override public String getType() {
        return "closeable";
      }

      @Override public void close() throws IOException {
        closed[0] = true;
      }
    };

    assertFalse(closed[0]);
    source.close();
    assertTrue(closed[0]);
  }

  // ========== DataWriter ==========

  @Test void testDataWriterLambdaWritesData() throws IOException {
    DataWriter writer = (config, data, variables) -> {
      long count = 0;
      while (data.hasNext()) {
        data.next();
        count++;
      }
      return count;
    };

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    records.add(Collections.<String, Object>singletonMap("a", 1));
    records.add(Collections.<String, Object>singletonMap("a", 2));
    records.add(Collections.<String, Object>singletonMap("a", 3));

    long written =
        writer.write(createMinimalPipelineConfig("writer_test"),
        records.iterator(),
        Collections.<String, String>emptyMap());
    assertEquals(3L, written);
  }

  @Test void testDataWriterLambdaWithEmptyData() throws IOException {
    DataWriter writer = (config, data, variables) -> {
      long count = 0;
      while (data.hasNext()) {
        data.next();
        count++;
      }
      return count;
    };

    long written =
        writer.write(createMinimalPipelineConfig("empty_write"),
        Collections.<Map<String, Object>>emptyList().iterator(),
        Collections.<String, String>emptyMap());
    assertEquals(0L, written);
  }

  @Test void testDataWriterDefaultReturnsNegativeOne() throws IOException {
    DataWriter defaultWriter = DataWriter.DEFAULT;
    assertNotNull(defaultWriter);
    long result =
        defaultWriter.write(createMinimalPipelineConfig("test"),
        Collections.<Map<String, Object>>emptyList().iterator(),
        Collections.<String, String>emptyMap());
    assertEquals(-1L, result);
  }

  @Test void testDataWriterLambdaThatThrowsIOException() {
    DataWriter writer = (config, data, variables) -> {
      throw new IOException("Disk full");
    };

    try {
      writer.write(
          createMinimalPipelineConfig("fail"),
          Collections.<Map<String, Object>>emptyList().iterator(),
          Collections.<String, String>emptyMap());
      assertTrue(false, "Expected IOException");
    } catch (IOException e) {
      assertEquals("Disk full", e.getMessage());
    }
  }

  // ========== Validator and ValidationResult ==========

  @Test void testValidatorWithValidRow() {
    Validator validator = new Validator() {
      @Override public ValidationResult validate(Map<String, Object> row) {
        if (row.containsKey("id")) {
          return ValidationResult.valid();
        }
        return ValidationResult.drop("Missing id");
      }
    };

    Map<String, Object> validRow = Collections.<String, Object>singletonMap("id", 1);
    ValidationResult result = validator.validate(validRow);
    assertTrue(result.isValid());
    assertTrue(result.shouldContinue());
    assertTrue(result.shouldInclude());
    assertNull(result.getMessage());
    assertEquals(ValidationResult.Action.VALID, result.getAction());
  }

  @Test void testValidationResultValid() {
    ValidationResult result = ValidationResult.valid();
    assertNotNull(result);
    assertTrue(result.isValid());
    assertTrue(result.shouldContinue());
    assertTrue(result.shouldInclude());
    assertNull(result.getMessage());
    assertEquals(ValidationResult.Action.VALID, result.getAction());
  }

  @Test void testValidationResultValidIsSingleton() {
    ValidationResult result1 = ValidationResult.valid();
    ValidationResult result2 = ValidationResult.valid();
    assertSame(result1, result2);
  }

  @Test void testValidationResultDrop() {
    ValidationResult result = ValidationResult.drop("Bad data");
    assertNotNull(result);
    assertFalse(result.isValid());
    assertTrue(result.shouldContinue());
    assertFalse(result.shouldInclude());
    assertEquals("Bad data", result.getMessage());
    assertEquals(ValidationResult.Action.DROP, result.getAction());
  }

  @Test void testValidationResultWarn() {
    ValidationResult result = ValidationResult.warn("Suspicious value");
    assertNotNull(result);
    assertFalse(result.isValid());
    assertTrue(result.shouldContinue());
    assertTrue(result.shouldInclude());
    assertEquals("Suspicious value", result.getMessage());
    assertEquals(ValidationResult.Action.WARN, result.getAction());
  }

  @Test void testValidationResultFail() {
    ValidationResult result = ValidationResult.fail("Critical error");
    assertNotNull(result);
    assertFalse(result.isValid());
    assertFalse(result.shouldContinue());
    assertFalse(result.shouldInclude());
    assertEquals("Critical error", result.getMessage());
    assertEquals(ValidationResult.Action.FAIL, result.getAction());
  }

  @Test void testValidationResultToStringValid() {
    ValidationResult result = ValidationResult.valid();
    assertEquals("ValidationResult{VALID}", result.toString());
  }

  @Test void testValidationResultToStringDrop() {
    ValidationResult result = ValidationResult.drop("Missing field");
    String str = result.toString();
    assertTrue(str.contains("DROP"));
    assertTrue(str.contains("Missing field"));
  }

  @Test void testValidationResultToStringWarn() {
    ValidationResult result = ValidationResult.warn("Negative value");
    String str = result.toString();
    assertTrue(str.contains("WARN"));
    assertTrue(str.contains("Negative value"));
  }

  @Test void testValidationResultToStringFail() {
    ValidationResult result = ValidationResult.fail("Constraint violation");
    String str = result.toString();
    assertTrue(str.contains("FAIL"));
    assertTrue(str.contains("Constraint violation"));
  }

  @Test void testValidationResultAllActions() {
    // Verify all enum values exist and are used
    ValidationResult.Action[] actions = ValidationResult.Action.values();
    assertEquals(4, actions.length);

    assertEquals(ValidationResult.Action.VALID,
        ValidationResult.Action.valueOf("VALID"));
    assertEquals(ValidationResult.Action.DROP,
        ValidationResult.Action.valueOf("DROP"));
    assertEquals(ValidationResult.Action.WARN,
        ValidationResult.Action.valueOf("WARN"));
    assertEquals(ValidationResult.Action.FAIL,
        ValidationResult.Action.valueOf("FAIL"));
  }

  @Test void testValidatorLambdaImplementation() {
    // Validator is not @FunctionalInterface but has single abstract method
    Validator validator = new Validator() {
      @Override public ValidationResult validate(Map<String, Object> row) {
        Object val = row.get("value");
        if (val instanceof Number && ((Number) val).doubleValue() < 0) {
          return ValidationResult.warn("Negative value: " + val);
        }
        return ValidationResult.valid();
      }
    };

    Map<String, Object> positiveRow = Collections.<String, Object>singletonMap("value", 42);
    assertTrue(validator.validate(positiveRow).isValid());

    Map<String, Object> negativeRow = Collections.<String, Object>singletonMap("value", -5);
    ValidationResult negResult = validator.validate(negativeRow);
    assertFalse(negResult.isValid());
    assertEquals(ValidationResult.Action.WARN, negResult.getAction());
    assertTrue(negResult.getMessage().contains("-5"));
  }

  // ========== RowTransformer ==========

  @Test void testRowTransformerModifiesRow() {
    RowTransformer transformer = new RowTransformer() {
      @Override public Map<String, Object> transform(Map<String, Object> row,
          RowContext context) {
        row.put("enriched", true);
        return row;
      }
    };

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);

    RowContext context = RowContext.builder().rowNumber(0).build();
    Map<String, Object> result = transformer.transform(row, context);
    assertNotNull(result);
    assertEquals(1, result.get("id"));
    assertEquals(true, result.get("enriched"));
  }

  @Test void testRowTransformerReturnsNewMap() {
    RowTransformer transformer = new RowTransformer() {
      @Override public Map<String, Object> transform(Map<String, Object> row,
          RowContext context) {
        Map<String, Object> newRow = new LinkedHashMap<String, Object>(row);
        newRow.put("upper_name", ((String) row.get("name")).toUpperCase());
        return newRow;
      }
    };

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("name", "hello");

    RowContext context = RowContext.builder().rowNumber(5).build();
    Map<String, Object> result = transformer.transform(row, context);
    assertEquals("hello", result.get("name"));
    assertEquals("HELLO", result.get("upper_name"));
  }

  @Test void testRowTransformerReturnsNullToDropRow() {
    RowTransformer transformer = new RowTransformer() {
      @Override public Map<String, Object> transform(Map<String, Object> row,
          RowContext context) {
        if (row.get("id") == null) {
          return null;
        }
        return row;
      }
    };

    Map<String, Object> rowWithoutId = new HashMap<String, Object>();
    rowWithoutId.put("name", "orphan");

    RowContext context = RowContext.builder().rowNumber(0).build();
    assertNull(transformer.transform(rowWithoutId, context));
  }

  @Test void testRowTransformerUsesContext() {
    RowTransformer transformer = new RowTransformer() {
      @Override public Map<String, Object> transform(Map<String, Object> row,
          RowContext context) {
        row.put("row_num", context.getRowNumber());
        String year = context.getDimensionValues().get("year");
        if (year != null) {
          row.put("year", year);
        }
        return row;
      }
    };

    Map<String, String> dims = Collections.singletonMap("year", "2024");
    RowContext context = RowContext.builder()
        .rowNumber(42)
        .dimensionValues(dims)
        .build();

    Map<String, Object> row = new HashMap<String, Object>();
    Map<String, Object> result = transformer.transform(row, context);
    assertEquals(42L, result.get("row_num"));
    assertEquals("2024", result.get("year"));
  }

  // ========== RowContext ==========

  @Test void testRowContextBuilderDefaults() {
    RowContext context = RowContext.builder().build();
    assertNotNull(context);
    assertEquals(0L, context.getRowNumber());
    assertNotNull(context.getDimensionValues());
    assertTrue(context.getDimensionValues().isEmpty());
    assertNull(context.getTableConfig());
  }

  @Test void testRowContextBuilderWithAllFields() {
    Map<String, String> dims = new LinkedHashMap<String, String>();
    dims.put("year", "2024");
    dims.put("region", "US");

    EtlPipelineConfig config = createMinimalPipelineConfig("test_table");

    RowContext context = RowContext.builder()
        .rowNumber(100)
        .dimensionValues(dims)
        .tableConfig(config)
        .build();

    assertEquals(100L, context.getRowNumber());
    assertEquals("2024", context.getDimensionValues().get("year"));
    assertEquals("US", context.getDimensionValues().get("region"));
    assertEquals("test_table", context.getTableConfig().getName());
  }

  @Test void testRowContextDimensionValuesAreUnmodifiable() {
    Map<String, String> dims = new HashMap<String, String>();
    dims.put("year", "2024");

    RowContext context = RowContext.builder()
        .dimensionValues(dims)
        .build();

    try {
      context.getDimensionValues().put("extra", "value");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testRowContextNullDimensionValuesDefaultsToEmptyMap() {
    RowContext context = RowContext.builder()
        .dimensionValues(null)
        .build();

    assertNotNull(context.getDimensionValues());
    assertTrue(context.getDimensionValues().isEmpty());
  }

  @Test void testRowContextToStringWithoutConfig() {
    RowContext context = RowContext.builder()
        .rowNumber(5)
        .build();

    String str = context.toString();
    assertTrue(str.contains("rowNumber=5"));
    assertFalse(str.contains("tableName"));
  }

  @Test void testRowContextToStringWithDimensions() {
    Map<String, String> dims = Collections.singletonMap("year", "2024");
    RowContext context = RowContext.builder()
        .rowNumber(10)
        .dimensionValues(dims)
        .build();

    String str = context.toString();
    assertTrue(str.contains("rowNumber=10"));
    assertTrue(str.contains("dimensionValues="));
    assertTrue(str.contains("year"));
  }

  @Test void testRowContextToStringWithConfig() {
    EtlPipelineConfig config = createMinimalPipelineConfig("my_table");
    RowContext context = RowContext.builder()
        .rowNumber(0)
        .tableConfig(config)
        .build();

    String str = context.toString();
    assertTrue(str.contains("tableName='my_table'"));
  }

  @Test void testRowContextDimensionValuesAreDefensiveCopy() {
    Map<String, String> dims = new HashMap<String, String>();
    dims.put("year", "2024");

    RowContext context = RowContext.builder()
        .dimensionValues(dims)
        .build();

    // Mutating original should not affect context
    dims.put("region", "EU");
    assertFalse(context.getDimensionValues().containsKey("region"));
  }

  // ========== DimensionResolver ==========

  @Test void testDimensionResolverMockImplementation() {
    DimensionResolver resolver = new DimensionResolver() {
      @Override public List<String> resolve(String dimensionName, DimensionConfig config,
          Map<String, String> context, StorageProvider storageProvider) {
        if ("year".equals(dimensionName)) {
          return Arrays.asList("2022", "2023", "2024");
        }
        return Collections.emptyList();
      }
    };

    StorageProvider sp = mock(StorageProvider.class);
    DimensionConfig dimConfig = DimensionConfig.builder()
        .name("year")
        .build();

    List<String> years =
        resolver.resolve("year", dimConfig, Collections.<String, String>emptyMap(), sp);
    assertEquals(3, years.size());
    assertEquals("2022", years.get(0));
    assertEquals("2024", years.get(2));
  }

  @Test void testDimensionResolverWithContext() {
    DimensionResolver resolver = new DimensionResolver() {
      @Override public List<String> resolve(String dimensionName, DimensionConfig config,
          Map<String, String> context, StorageProvider storageProvider) {
        if ("line_code".equals(dimensionName)) {
          String tableName = context.get("tablename");
          if ("SAINC1".equals(tableName)) {
            return Arrays.asList("1", "2", "3");
          }
          return Arrays.asList("10", "20");
        }
        return Collections.emptyList();
      }
    };

    StorageProvider sp = mock(StorageProvider.class);
    DimensionConfig dimConfig = DimensionConfig.builder()
        .name("line_code")
        .build();

    Map<String, String> context = Collections.singletonMap("tablename", "SAINC1");
    List<String> codes = resolver.resolve("line_code", dimConfig, context, sp);
    assertEquals(3, codes.size());

    Map<String, String> context2 = Collections.singletonMap("tablename", "OTHER");
    List<String> codes2 = resolver.resolve("line_code", dimConfig, context2, sp);
    assertEquals(2, codes2.size());
  }

  @Test void testDimensionResolverReturnsEmptyList() {
    DimensionResolver resolver = new DimensionResolver() {
      @Override public List<String> resolve(String dimensionName, DimensionConfig config,
          Map<String, String> context, StorageProvider storageProvider) {
        return Collections.emptyList();
      }
    };

    StorageProvider sp = mock(StorageProvider.class);
    DimensionConfig dimConfig = DimensionConfig.builder().name("unknown").build();

    List<String> result =
        resolver.resolve("unknown", dimConfig, Collections.<String, String>emptyMap(), sp);
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  // ========== SchemaLifecycleListener ==========

  @Test void testSchemaLifecycleListenerNoopDoesNotThrow() throws Exception {
    SchemaLifecycleListener noop = SchemaLifecycleListener.NOOP;
    assertNotNull(noop);

    SchemaContext context = createMinimalSchemaContext();
    SchemaResult result = new SchemaResult("test", 1, 0, 0, 100, 50, null);

    // None of these should throw
    noop.beforeSchema(context);
    noop.afterSchema(context, result);
    noop.onSchemaError(context, new RuntimeException("test error"));
  }

  @Test void testSchemaLifecycleListenerNoopDownloadBulkFileReturnsNull() throws Exception {
    SchemaLifecycleListener noop = SchemaLifecycleListener.NOOP;
    SchemaContext context = createMinimalSchemaContext();

    // The default downloadBulkFile should return null
    String path =
        noop.downloadBulkFile(context, null, Collections.<String, String>emptyMap(), "/tmp/target");
    assertNull(path);
  }

  @Test void testSchemaLifecycleListenerDefaultDownloadBulkFileReturnsNull() throws Exception {
    SchemaLifecycleListener listener = new SchemaLifecycleListener() {
      @Override public void beforeSchema(SchemaContext context) { }
      @Override public void afterSchema(SchemaContext context, SchemaResult result) { }
      @Override public void onSchemaError(SchemaContext context, Exception error) { }
    };

    SchemaContext context = createMinimalSchemaContext();
    String path =
        listener.downloadBulkFile(context, null, Collections.<String, String>emptyMap(), "/tmp/file.zip");
    assertNull(path);
  }

  @Test void testSchemaLifecycleListenerCustomImplementation() throws Exception {
    final boolean[] beforeCalled = {false};
    final boolean[] afterCalled = {false};
    final boolean[] errorCalled = {false};

    SchemaLifecycleListener listener = new SchemaLifecycleListener() {
      @Override public void beforeSchema(SchemaContext context) {
        beforeCalled[0] = true;
      }

      @Override public void afterSchema(SchemaContext context, SchemaResult result) {
        afterCalled[0] = true;
      }

      @Override public void onSchemaError(SchemaContext context, Exception error) {
        errorCalled[0] = true;
      }
    };

    SchemaContext context = createMinimalSchemaContext();
    listener.beforeSchema(context);
    assertTrue(beforeCalled[0]);

    SchemaResult result = new SchemaResult("test", 1, 0, 0, 100, 50, null);
    listener.afterSchema(context, result);
    assertTrue(afterCalled[0]);

    listener.onSchemaError(context, new RuntimeException("oops"));
    assertTrue(errorCalled[0]);
  }

  @Test void testSchemaLifecycleListenerOverriddenDownloadBulkFile() throws Exception {
    SchemaLifecycleListener listener = new SchemaLifecycleListener() {
      @Override public void beforeSchema(SchemaContext context) { }
      @Override public void afterSchema(SchemaContext context, SchemaResult result) { }
      @Override public void onSchemaError(SchemaContext context, Exception error) { }

      @Override public String downloadBulkFile(SchemaContext context,
          BulkDownloadConfig bulkConfig, Map<String, String> variables,
          String targetPath) throws Exception {
        return "/custom/downloaded/file.zip";
      }
    };

    SchemaContext context = createMinimalSchemaContext();
    String path =
        listener.downloadBulkFile(context, null, Collections.<String, String>emptyMap(), "/tmp/target.zip");
    assertEquals("/custom/downloaded/file.zip", path);
  }

  // ========== TableLifecycleListener ==========

  @Test void testTableLifecycleListenerNoopDoesNotThrow() throws Exception {
    TableLifecycleListener noop = TableLifecycleListener.NOOP;
    assertNotNull(noop);

    TableContext tableContext = createMinimalTableContext("noop_table");
    EtlResult etlResult = EtlResult.success("noop_table", 100, 1, 50);

    // None of these should throw
    noop.beforeTable(tableContext);
    noop.afterTable(tableContext, etlResult);
    boolean continueProcessing = noop.onTableError(tableContext, new RuntimeException("err"));
    assertTrue(continueProcessing);
  }

  @Test void testTableLifecycleListenerNoopDefaultMethods() {
    TableLifecycleListener noop = TableLifecycleListener.NOOP;
    TableContext tableContext = createMinimalTableContext("noop_test");

    // Test all default methods
    noop.beforeSource(tableContext);
    noop.afterSource(tableContext, SourceResult.success(100, 1024, 50, "http://example.com"));
    assertTrue(noop.onSourceError(tableContext, new RuntimeException("source err")));
    noop.beforeMaterialize(tableContext);
    noop.afterMaterialize(tableContext, MaterializeResult.success(100, 1, 50));
    assertTrue(noop.onMaterializeError(tableContext, new RuntimeException("mat err")));
  }

  @Test void testTableLifecycleListenerDefaultBeforeSource() {
    TableLifecycleListener listener = createBaseTableListener();
    TableContext ctx = createMinimalTableContext("test");
    // Should not throw
    listener.beforeSource(ctx);
  }

  @Test void testTableLifecycleListenerDefaultAfterSource() {
    TableLifecycleListener listener = createBaseTableListener();
    TableContext ctx = createMinimalTableContext("test");
    SourceResult sr = SourceResult.success(10, 512, 30, "http://api.example.com");
    // Should not throw
    listener.afterSource(ctx, sr);
  }

  @Test void testTableLifecycleListenerDefaultOnSourceErrorReturnsTrue() {
    TableLifecycleListener listener = createBaseTableListener();
    TableContext ctx = createMinimalTableContext("test");
    boolean result = listener.onSourceError(ctx, new RuntimeException("source failed"));
    assertTrue(result);
  }

  @Test void testTableLifecycleListenerDefaultBeforeMaterialize() {
    TableLifecycleListener listener = createBaseTableListener();
    TableContext ctx = createMinimalTableContext("test");
    // Should not throw
    listener.beforeMaterialize(ctx);
  }

  @Test void testTableLifecycleListenerDefaultAfterMaterialize() {
    TableLifecycleListener listener = createBaseTableListener();
    TableContext ctx = createMinimalTableContext("test");
    MaterializeResult mr = MaterializeResult.success(50, 2, 100);
    // Should not throw
    listener.afterMaterialize(ctx, mr);
  }

  @Test void testTableLifecycleListenerDefaultOnMaterializeErrorReturnsTrue() {
    TableLifecycleListener listener = createBaseTableListener();
    TableContext ctx = createMinimalTableContext("test");
    boolean result = listener.onMaterializeError(ctx, new RuntimeException("mat error"));
    assertTrue(result);
  }

  @Test void testTableLifecycleListenerDefaultFetchDataReturnsNull() {
    TableLifecycleListener listener = createBaseTableListener();
    TableContext ctx = createMinimalTableContext("test");
    Iterator<Map<String, Object>> data =
        listener.fetchData(ctx, Collections.<String, String>emptyMap());
    assertNull(data);
  }

  @Test void testTableLifecycleListenerDefaultWriteDataReturnsNegativeOne() {
    TableLifecycleListener listener = createBaseTableListener();
    TableContext ctx = createMinimalTableContext("test");
    long result =
        listener.writeData(ctx, Collections.<Map<String, Object>>emptyList().iterator(),
        Collections.<String, String>emptyMap());
    assertEquals(-1L, result);
  }

  @Test void testTableLifecycleListenerDefaultResolveDimensionsReturnsNull() {
    TableLifecycleListener listener = createBaseTableListener();
    TableContext ctx = createMinimalTableContext("test");
    Map<String, DimensionConfig> resolved =
        listener.resolveDimensions(ctx, Collections.<String, DimensionConfig>emptyMap());
    assertNull(resolved);
  }

  @Test void testTableLifecycleListenerDefaultResolveApiKeyReturnsNull() {
    TableLifecycleListener listener = createBaseTableListener();
    TableContext ctx = createMinimalTableContext("test");
    String key = listener.resolveApiKey(ctx, "BLS_API_KEY");
    assertNull(key);
  }

  @Test void testTableLifecycleListenerDefaultIsTableEnabledReturnsTrue() {
    TableLifecycleListener listener = createBaseTableListener();
    TableContext ctx = createMinimalTableContext("test");
    assertTrue(listener.isTableEnabled(ctx));
  }

  @SuppressWarnings("deprecation")
  @Test void testTableLifecycleListenerDefaultShouldProcessTableReturnsTrue() {
    TableLifecycleListener listener = createBaseTableListener();
    TableContext ctx = createMinimalTableContext("test");
    assertTrue(listener.shouldProcessTable(ctx));
  }

  @SuppressWarnings("deprecation")
  @Test void testTableLifecycleListenerShouldProcessTableDelegatesToIsTableEnabled() {
    // shouldProcessTable is deprecated and delegates to isTableEnabled
    TableLifecycleListener listener = new TableLifecycleListener() {
      @Override public void beforeTable(TableContext context) { }
      @Override public void afterTable(TableContext context, EtlResult result) { }
      @Override public boolean onTableError(TableContext context, Exception error) {
        return true;
      }

      @Override public boolean isTableEnabled(TableContext context) {
        return false;
      }
    };

    TableContext ctx = createMinimalTableContext("test");
    assertFalse(listener.isTableEnabled(ctx));
    assertFalse(listener.shouldProcessTable(ctx));
  }

  @Test void testTableLifecycleListenerOverrideFetchData() {
    TableLifecycleListener listener = new TableLifecycleListener() {
      @Override public void beforeTable(TableContext context) { }
      @Override public void afterTable(TableContext context, EtlResult result) { }
      @Override public boolean onTableError(TableContext context, Exception error) {
        return true;
      }

      @Override public Iterator<Map<String, Object>> fetchData(TableContext context,
          Map<String, String> variables) {
        List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("custom", "data");
        data.add(row);
        return data.iterator();
      }
    };

    TableContext ctx = createMinimalTableContext("custom_source");
    Iterator<Map<String, Object>> iter =
        listener.fetchData(ctx, Collections.<String, String>emptyMap());
    assertNotNull(iter);
    assertTrue(iter.hasNext());
    assertEquals("data", iter.next().get("custom"));
  }

  @Test void testTableLifecycleListenerOverrideWriteData() {
    TableLifecycleListener listener = new TableLifecycleListener() {
      @Override public void beforeTable(TableContext context) { }
      @Override public void afterTable(TableContext context, EtlResult result) { }
      @Override public boolean onTableError(TableContext context, Exception error) {
        return true;
      }

      @Override public long writeData(TableContext context,
          Iterator<Map<String, Object>> data, Map<String, String> variables) {
        long count = 0;
        while (data.hasNext()) {
          data.next();
          count++;
        }
        return count;
      }
    };

    TableContext ctx = createMinimalTableContext("custom_writer");
    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    records.add(Collections.<String, Object>singletonMap("a", 1));
    records.add(Collections.<String, Object>singletonMap("a", 2));

    long written =
        listener.writeData(ctx, records.iterator(), Collections.<String, String>emptyMap());
    assertEquals(2L, written);
  }

  @Test void testTableLifecycleListenerOverrideResolveApiKey() {
    TableLifecycleListener listener = new TableLifecycleListener() {
      @Override public void beforeTable(TableContext context) { }
      @Override public void afterTable(TableContext context, EtlResult result) { }
      @Override public boolean onTableError(TableContext context, Exception error) {
        return true;
      }

      @Override public String resolveApiKey(TableContext context, String keyName) {
        if ("BLS_API_KEY".equals(keyName)) {
          return "secret-key-123";
        }
        return null;
      }
    };

    TableContext ctx = createMinimalTableContext("bls_table");
    assertEquals("secret-key-123", listener.resolveApiKey(ctx, "BLS_API_KEY"));
    assertNull(listener.resolveApiKey(ctx, "UNKNOWN_KEY"));
  }

  @Test void testTableLifecycleListenerOverrideResolveDimensions() {
    TableLifecycleListener listener = new TableLifecycleListener() {
      @Override public void beforeTable(TableContext context) { }
      @Override public void afterTable(TableContext context, EtlResult result) { }
      @Override public boolean onTableError(TableContext context, Exception error) {
        return true;
      }

      @Override public Map<String, DimensionConfig> resolveDimensions(TableContext context,
          Map<String, DimensionConfig> staticDimensions) {
        Map<String, DimensionConfig> resolved = new LinkedHashMap<String, DimensionConfig>();
        resolved.put("year", DimensionConfig.builder()
            .name("year")
            .values(Arrays.asList("2023", "2024"))
            .build());
        return resolved;
      }
    };

    TableContext ctx = createMinimalTableContext("dynamic_dims");
    Map<String, DimensionConfig> result =
        listener.resolveDimensions(ctx, Collections.<String, DimensionConfig>emptyMap());
    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.containsKey("year"));
    assertEquals(2, result.get("year").getValues().size());
  }

  @Test void testTableLifecycleListenerNoopOnTableErrorReturnsTrue() {
    TableLifecycleListener noop = TableLifecycleListener.NOOP;
    TableContext ctx = createMinimalTableContext("error_table");
    assertTrue(noop.onTableError(ctx, new RuntimeException("table error")));
  }

  // ========== Helper methods ==========

  /**
   * Creates a minimal EtlPipelineConfig for testing (disabled to skip validation).
   */
  private EtlPipelineConfig createMinimalPipelineConfig(String name) {
    return EtlPipelineConfig.builder()
        .name(name)
        .enabled(false)
        .build();
  }

  /**
   * Creates a minimal SchemaContext backed by a mock StorageProvider.
   */
  private SchemaContext createMinimalSchemaContext() {
    StorageProvider sp = mock(StorageProvider.class);
    SchemaConfig schemaConfig = SchemaConfig.builder()
        .name("test_schema")
        .build();

    return SchemaContext.builder()
        .config(schemaConfig)
        .storageProvider(sp)
        .materializeDirectory("/tmp/output")
        .sourceDirectory("/tmp/source")
        .build();
  }

  /**
   * Creates a minimal TableContext backed by a mock StorageProvider.
   */
  private TableContext createMinimalTableContext(String tableName) {
    SchemaContext schemaContext = createMinimalSchemaContext();
    EtlPipelineConfig tableConfig = createMinimalPipelineConfig(tableName);

    return TableContext.builder()
        .tableConfig(tableConfig)
        .schemaContext(schemaContext)
        .tableIndex(0)
        .totalTables(1)
        .build();
  }

  /**
   * Creates a base TableLifecycleListener with required abstract methods implemented.
   * All defaults are preserved for default method testing.
   */
  private TableLifecycleListener createBaseTableListener() {
    return new TableLifecycleListener() {
      @Override public void beforeTable(TableContext context) { }
      @Override public void afterTable(TableContext context, EtlResult result) { }
      @Override public boolean onTableError(TableContext context, Exception error) {
        return true;
      }
    };
  }
}
