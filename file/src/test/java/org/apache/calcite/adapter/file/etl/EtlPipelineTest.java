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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for ETL Pipeline configuration and result classes.
 */
@Tag("unit")
public class EtlPipelineTest {

  @Test void testEtlPipelineConfigBuilder() {
    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2024)
        .build());

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    MaterializeConfig materialize = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder()
            .location("/output")
            .build())
        .build();

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_pipeline")
        .source(source)
        .dimensions(dimensions)
        .materialize(materialize)
        .build();

    assertEquals("test_pipeline", config.getName());
    assertNotNull(config.getSource());
    assertEquals(1, config.getDimensions().size());
    assertNotNull(config.getMaterialize());
    assertNotNull(config.getErrorHandling());
  }

  @Test void testEtlPipelineConfigRequiresName() {
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    MaterializeConfig materialize = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder()
            .location("/output")
            .build())
        .build();

    assertThrows(IllegalArgumentException.class, () -> {
      EtlPipelineConfig.builder()
          .source(source)
          .materialize(materialize)
          .build();
    });
  }

  @Test void testEtlPipelineConfigRequiresSource() {
    MaterializeConfig materialize = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder()
            .location("/output")
            .build())
        .build();

    assertThrows(IllegalArgumentException.class, () -> {
      EtlPipelineConfig.builder()
          .name("test")
          .materialize(materialize)
          .build();
    });
  }

  @Test void testEtlPipelineConfigRequiresMaterialize() {
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    assertThrows(IllegalArgumentException.class, () -> {
      EtlPipelineConfig.builder()
          .name("test")
          .source(source)
          .build();
    });
  }

  @Test void testEtlPipelineConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "sales_pipeline");

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com/v1/sales");
    sourceMap.put("method", "GET");
    map.put("source", sourceMap);

    Map<String, Object> dimensionsMap = new LinkedHashMap<String, Object>();
    Map<String, Object> yearDim = new HashMap<String, Object>();
    yearDim.put("type", "range");
    yearDim.put("start", 2020);
    yearDim.put("end", 2024);
    dimensionsMap.put("year", yearDim);
    map.put("dimensions", dimensionsMap);

    Map<String, Object> materializeMap = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data/output");
    materializeMap.put("output", outputMap);
    map.put("materialize", materializeMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);

    assertEquals("sales_pipeline", config.getName());
    assertEquals("https://api.example.com/v1/sales", config.getSource().getUrl());
    assertEquals(1, config.getDimensions().size());
    assertTrue(config.getDimensions().containsKey("year"));
    assertEquals("/data/output", config.getMaterialize().getOutput().getLocation());
  }

  @Test void testErrorHandlingConfigDefaults() {
    EtlPipelineConfig.ErrorHandlingConfig config =
        EtlPipelineConfig.ErrorHandlingConfig.defaults();

    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        config.getTransientErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        config.getNotFoundAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        config.getApiErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        config.getAuthErrorAction());
    assertEquals(3, config.getTransientRetries());
    assertEquals(1000, config.getTransientBackoffMs());
    assertEquals(7, config.getNotFoundRetryDays());
  }

  @Test void testErrorHandlingConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("transientErrorAction", "retry");
    map.put("notFoundAction", "warn");
    map.put("apiErrorAction", "fail");
    map.put("authErrorAction", "fail");
    map.put("transientRetries", 5);
    map.put("transientBackoffMs", 2000);
    map.put("notFoundRetryDays", 14);

    EtlPipelineConfig.ErrorHandlingConfig config =
        EtlPipelineConfig.ErrorHandlingConfig.fromMap(map);

    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        config.getTransientErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN,
        config.getNotFoundAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        config.getApiErrorAction());
    assertEquals(5, config.getTransientRetries());
    assertEquals(2000, config.getTransientBackoffMs());
    assertEquals(14, config.getNotFoundRetryDays());
  }

  @Test void testEtlResultBuilder() {
    List<String> errors = new ArrayList<String>();
    errors.add("Error 1");
    errors.add("Error 2");

    EtlResult result = EtlResult.builder()
        .pipelineName("test_pipeline")
        .totalRows(10000)
        .successfulBatches(8)
        .failedBatches(1)
        .skippedBatches(1)
        .elapsedMs(5000)
        .errors(errors)
        .build();

    assertEquals("test_pipeline", result.getPipelineName());
    assertEquals(10000, result.getTotalRows());
    assertEquals(8, result.getSuccessfulBatches());
    assertEquals(1, result.getFailedBatches());
    assertEquals(1, result.getSkippedBatches());
    assertEquals(10, result.getTotalBatches());
    assertEquals(5000, result.getElapsedMs());
    assertEquals(2, result.getErrors().size());
    assertTrue(result.isSuccessful());
    assertFalse(result.isCompleteSuccess());  // Has failed batches
    assertEquals(2000.0, result.getRowsPerSecond(), 0.01);
  }

  @Test void testEtlResultSuccess() {
    EtlResult result = EtlResult.success("my_pipeline", 5000, 10, 2500);

    assertEquals("my_pipeline", result.getPipelineName());
    assertEquals(5000, result.getTotalRows());
    assertEquals(10, result.getSuccessfulBatches());
    assertEquals(2500, result.getElapsedMs());
    assertTrue(result.isSuccessful());
    assertTrue(result.isCompleteSuccess());
  }

  @Test void testEtlResultFailure() {
    EtlResult result = EtlResult.failure("failed_pipeline", "Connection timeout", 1000);

    assertEquals("failed_pipeline", result.getPipelineName());
    assertEquals("Connection timeout", result.getFailureMessage());
    assertEquals(1000, result.getElapsedMs());
    assertFalse(result.isSuccessful());
    assertFalse(result.isCompleteSuccess());
  }

  @Test void testEtlResultToString() {
    EtlResult success = EtlResult.success("test", 1000, 5, 1000);
    String successStr = success.toString();
    assertTrue(successStr.contains("test"));
    assertTrue(successStr.contains("1000"));

    EtlResult failure = EtlResult.failure("test", "Error!", 500);
    String failStr = failure.toString();
    assertTrue(failStr.contains("FAILED"));
    assertTrue(failStr.contains("Error!"));
  }

  @Test void testEtlResultRowsPerSecond() {
    // 10000 rows in 2000ms = 5000 rows/sec
    EtlResult result = EtlResult.builder()
        .pipelineName("test")
        .totalRows(10000)
        .elapsedMs(2000)
        .build();

    assertEquals(5000.0, result.getRowsPerSecond(), 0.01);

    // Zero elapsed time should return 0
    EtlResult zeroElapsed = EtlResult.builder()
        .pipelineName("test")
        .totalRows(1000)
        .elapsedMs(0)
        .build();

    assertEquals(0.0, zeroElapsed.getRowsPerSecond(), 0.01);
  }

  @Test void testLoggingProgressListener() {
    // Just verify it doesn't throw
    EtlPipeline.LoggingProgressListener listener = new EtlPipeline.LoggingProgressListener();

    listener.onPhaseStart("test_phase", 10);
    listener.onPhaseComplete("test_phase", 10);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    listener.onBatchStart(1, 10, vars);
    listener.onBatchComplete(1, 10, 100, null);
    listener.onBatchComplete(2, 10, 0, new Exception("Test error"));
  }

  @Test void testEtlPipelineConfigWithColumns() {
    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(ColumnConfig.builder()
        .name("region_code")
        .type("VARCHAR")
        .source("regionCode")
        .build());
    columns.add(ColumnConfig.builder()
        .name("quarter")
        .type("VARCHAR")
        .expression("SUBSTR(period, 1, 2)")
        .build());

    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    MaterializeConfig materialize = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder()
            .location("/output")
            .build())
        .build();

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test")
        .source(source)
        .columns(columns)
        .materialize(materialize)
        .build();

    assertEquals(2, config.getColumns().size());
    assertEquals("region_code", config.getColumns().get(0).getName());
    assertEquals("quarter", config.getColumns().get(1).getName());
  }

  // --- New tests for expanded code path coverage ---

  @Test void testDimensionConfigBuilderRange() {
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2025)
        .step(1)
        .build();

    assertEquals("year", config.getName());
    assertEquals(DimensionType.RANGE, config.getType());
    assertEquals(Integer.valueOf(2020), config.getStart());
    assertEquals(Integer.valueOf(2025), config.getEnd());
    assertEquals(Integer.valueOf(1), config.getStep());
    assertTrue(config.getValues().isEmpty());
    assertNull(config.getSql());
  }

  @Test void testDimensionConfigBuilderList() {
    List<String> values = Arrays.asList("NORTH", "SOUTH", "EAST", "WEST");
    DimensionConfig config = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(values)
        .build();

    assertEquals("region", config.getName());
    assertEquals(DimensionType.LIST, config.getType());
    assertEquals(4, config.getValues().size());
    assertEquals("NORTH", config.getValues().get(0));
    assertEquals("WEST", config.getValues().get(3));
    assertNull(config.getStart());
    assertNull(config.getEnd());
  }

  @Test void testDimensionConfigBuilderYearRangeWithDataLag() {
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2000)
        .end(null)
        .dataLag(1)
        .releaseMonth(9)
        .build();

    assertEquals("year", config.getName());
    assertEquals(DimensionType.YEAR_RANGE, config.getType());
    assertEquals(Integer.valueOf(2000), config.getStart());
    assertNull(config.getEnd());
    assertEquals(Integer.valueOf(1), config.getDataLag());
    assertEquals(Integer.valueOf(9), config.getReleaseMonth());
  }

  @Test void testDimensionConfigBuilderCustomWithProperties() {
    Map<String, String> props = new LinkedHashMap<String, String>();
    props.put("referenceDirectory", "/data/catalogs");
    props.put("pattern", "*.json");

    DimensionConfig config = DimensionConfig.builder()
        .name("series_id")
        .type(DimensionType.CUSTOM)
        .properties(props)
        .build();

    assertEquals(DimensionType.CUSTOM, config.getType());
    assertEquals(2, config.getProperties().size());
    assertEquals("/data/catalogs", config.getProperties().get("referenceDirectory"));
  }

  @Test void testDimensionConfigBuilderPropertyMethod() {
    DimensionConfig config = DimensionConfig.builder()
        .name("indicator")
        .type(DimensionType.JSON_CATALOG)
        .source("/worldbank/indicators.json")
        .path("indicators[*].code")
        .property("group", "economic")
        .property("limit", "100")
        .build();

    assertEquals(DimensionType.JSON_CATALOG, config.getType());
    assertEquals("/worldbank/indicators.json", config.getSource());
    assertEquals("indicators[*].code", config.getPath());
    assertEquals(2, config.getProperties().size());
  }

  @Test void testDimensionConfigFromMapListDimension() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "list");
    map.put("values", Arrays.asList("A", "M", "Q"));

    DimensionConfig config = DimensionConfig.fromMap("frequency", map);

    assertEquals("frequency", config.getName());
    assertEquals(DimensionType.LIST, config.getType());
    assertEquals(3, config.getValues().size());
    assertEquals("A", config.getValues().get(0));
    assertEquals("M", config.getValues().get(1));
    assertEquals("Q", config.getValues().get(2));
  }

  @Test void testDimensionConfigFromMapDynamicQuery() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "query");
    map.put("sql", "SELECT DISTINCT region FROM regions WHERE active = true");

    DimensionConfig config = DimensionConfig.fromMap("region", map);

    assertEquals("region", config.getName());
    assertEquals(DimensionType.QUERY, config.getType());
    assertEquals("SELECT DISTINCT region FROM regions WHERE active = true", config.getSql());
  }

  @Test void testDimensionConfigFromMapYearRange() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "yearRange");
    map.put("start", 2010);
    map.put("end", "current");
    map.put("dataLag", 1);

    DimensionConfig config = DimensionConfig.fromMap("year", map);

    assertEquals(DimensionType.YEAR_RANGE, config.getType());
    assertEquals(Integer.valueOf(2010), config.getStart());
    assertNull(config.getEnd());
    assertEquals(Integer.valueOf(1), config.getDataLag());
  }

  @Test void testDimensionConfigFromMapExcludeYears() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "yearRange");
    map.put("start", 2010);
    map.put("end", 2025);
    map.put("excludeYears", Arrays.asList(2011, 2013));

    DimensionConfig config = DimensionConfig.fromMap("year", map);

    assertEquals(2, config.getExcludeYears().size());
    assertEquals(Integer.valueOf(2011), config.getExcludeYears().get(0));
    assertEquals(Integer.valueOf(2013), config.getExcludeYears().get(1));
  }

  @Test void testDimensionConfigFromMapNull() {
    DimensionConfig config = DimensionConfig.fromMap("test", null);
    assertNull(config);
  }

  @Test void testDimensionConfigFromDimensionsMapWithListShorthand() {
    Map<String, Object> dimensionsMap = new LinkedHashMap<String, Object>();
    dimensionsMap.put("status", Arrays.asList("active", "pending", "closed"));

    Map<String, DimensionConfig> result = DimensionConfig.fromDimensionsMap(dimensionsMap);

    assertEquals(1, result.size());
    DimensionConfig statusDim = result.get("status");
    assertNotNull(statusDim);
    assertEquals(DimensionType.LIST, statusDim.getType());
    assertEquals(3, statusDim.getValues().size());
  }

  @Test void testDimensionConfigFromDimensionsMapWithStringShorthand() {
    Map<String, Object> dimensionsMap = new LinkedHashMap<String, Object>();
    dimensionsMap.put("format", "csv");

    Map<String, DimensionConfig> result = DimensionConfig.fromDimensionsMap(dimensionsMap);

    assertEquals(1, result.size());
    DimensionConfig formatDim = result.get("format");
    assertNotNull(formatDim);
    assertEquals(DimensionType.LIST, formatDim.getType());
    assertEquals(1, formatDim.getValues().size());
    assertEquals("csv", formatDim.getValues().get(0));
  }

  @Test void testDimensionConfigToString() {
    DimensionConfig rangeDim = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2024)
        .build();
    String str = rangeDim.toString();
    assertTrue(str.contains("year"));
    assertTrue(str.contains("RANGE"));
    assertTrue(str.contains("2020"));

    DimensionConfig listDim = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("N", "S"))
        .build();
    String listStr = listDim.toString();
    assertTrue(listStr.contains("LIST"));
    assertTrue(listStr.contains("[N, S]"));
  }

  @Test void testDimensionConfigRequiresName() {
    assertThrows(IllegalArgumentException.class, () -> {
      DimensionConfig.builder()
          .type(DimensionType.LIST)
          .values(Arrays.asList("A", "B"))
          .build();
    });
  }

  @Test void testMaterializeConfigBuilderWithAllOptions() {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .trigger(MaterializeConfig.Trigger.MANUAL)
        .output(MaterializeOutputConfig.builder()
            .location("/data/output")
            .pattern("type=sales/year=STAR/")
            .format("parquet")
            .compression("zstd")
            .build())
        .build();

    assertTrue(config.isEnabled());
    assertEquals(MaterializeConfig.Format.PARQUET, config.getFormat());
    assertEquals(MaterializeConfig.Trigger.MANUAL, config.getTrigger());
    assertEquals("/data/output", config.getOutput().getLocation());
    assertEquals("type=sales/year=STAR/", config.getOutput().getPattern());
    assertEquals("parquet", config.getOutput().getFormat());
    assertEquals("zstd", config.getOutput().getCompression());
  }

  @Test void testMaterializeConfigFromMapWithFormat() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("enabled", true);
    map.put("format", "parquet");
    map.put("trigger", "manual");

    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data/out");
    outputMap.put("compression", "gzip");
    map.put("output", outputMap);

    MaterializeConfig config = MaterializeConfig.fromMap(map);

    assertTrue(config.isEnabled());
    assertEquals(MaterializeConfig.Format.PARQUET, config.getFormat());
    assertEquals(MaterializeConfig.Trigger.MANUAL, config.getTrigger());
    assertEquals("gzip", config.getOutput().getCompression());
  }

  @Test void testMaterializeConfigDefaultFormat() {
    MaterializeConfig config = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().location("/out").build())
        .build();

    assertEquals(MaterializeConfig.Format.ICEBERG, config.getFormat());
    assertEquals(MaterializeConfig.Trigger.AUTO, config.getTrigger());
    assertTrue(config.isEnabled());
  }

  @Test void testMaterializeOutputConfigWithPattern() {
    MaterializeOutputConfig output = MaterializeOutputConfig.builder()
        .location("s3://bucket/data/")
        .pattern("indicator={indicator}/country={country_code}/year={year}/")
        .compression("snappy")
        .build();

    assertEquals("s3://bucket/data/", output.getLocation());
    assertEquals("indicator={indicator}/country={country_code}/year={year}/", output.getPattern());
    assertEquals("snappy", output.getCompression());
    assertEquals("parquet", output.getFormat());
  }

  @Test void testMaterializeOutputConfigDefaults() {
    MaterializeOutputConfig output = MaterializeOutputConfig.builder().build();

    assertNull(output.getLocation());
    assertNull(output.getPattern());
    assertEquals("parquet", output.getFormat());
    assertEquals("snappy", output.getCompression());
  }

  @Test void testMaterializeOutputConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("location", "/out/data");
    map.put("pattern", "year=STAR/");
    map.put("format", "parquet");
    map.put("compression", "lz4");

    MaterializeOutputConfig output = MaterializeOutputConfig.fromMap(map);

    assertEquals("/out/data", output.getLocation());
    assertEquals("year=STAR/", output.getPattern());
    assertEquals("parquet", output.getFormat());
    assertEquals("lz4", output.getCompression());
  }

  @Test void testColumnConfigBuilderWithExpression() {
    ColumnConfig col = ColumnConfig.builder()
        .name("quarter")
        .type("VARCHAR")
        .expression("SUBSTR(period, 1, 2)")
        .build();

    assertEquals("quarter", col.getName());
    assertEquals("VARCHAR", col.getType());
    assertNull(col.getSource());
    assertEquals("SUBSTR(period, 1, 2)", col.getExpression());
    assertTrue(col.isComputed());
    assertTrue(col.isRequired());
    assertEquals("quarter", col.getEffectiveSource());
  }

  @Test void testColumnConfigBuilderWithSource() {
    ColumnConfig col = ColumnConfig.builder()
        .name("region_code")
        .type("VARCHAR")
        .source("regionCode")
        .required(false)
        .build();

    assertEquals("region_code", col.getName());
    assertEquals("VARCHAR", col.getType());
    assertEquals("regionCode", col.getSource());
    assertNull(col.getExpression());
    assertFalse(col.isComputed());
    assertFalse(col.isRequired());
    assertEquals("regionCode", col.getEffectiveSource());
  }

  @Test void testColumnConfigBuildSelectExpression() {
    ColumnConfig computed = ColumnConfig.builder()
        .name("quarter")
        .type("VARCHAR")
        .expression("SUBSTR(period, 1, 2)")
        .build();
    assertEquals("SUBSTR(period, 1, 2) AS quarter", computed.buildSelectExpression());

    ColumnConfig renamed = ColumnConfig.builder()
        .name("region_code")
        .type("VARCHAR")
        .source("regionCode")
        .build();
    assertEquals("\"regionCode\" AS region_code", renamed.buildSelectExpression());

    ColumnConfig direct = ColumnConfig.builder()
        .name("amount")
        .type("DECIMAL")
        .build();
    assertEquals("amount", direct.buildSelectExpression());
  }

  @Test void testColumnConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "value");
    map.put("type", "DECIMAL(15,2)");
    map.put("source", "DataValue");
    map.put("required", false);

    ColumnConfig col = ColumnConfig.fromMap(map);

    assertEquals("value", col.getName());
    assertEquals("DECIMAL(15,2)", col.getType());
    assertEquals("DataValue", col.getSource());
    assertFalse(col.isRequired());
    assertFalse(col.isComputed());
  }

  @Test void testColumnConfigFromMapWithExpression() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "clean_value");
    map.put("type", "DECIMAL(15,2)");
    map.put("expression",
        "CASE WHEN DataValue IN ('(NA)') THEN NULL ELSE CAST(DataValue AS DECIMAL(15,2)) END");

    ColumnConfig col = ColumnConfig.fromMap(map);

    assertEquals("clean_value", col.getName());
    assertTrue(col.isComputed());
    assertTrue(col.isRequired());
  }

  @Test void testColumnConfigFromMapNull() {
    ColumnConfig col = ColumnConfig.fromMap(null);
    assertNull(col);
  }

  @Test void testColumnConfigFromList() {
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

    Map<String, Object> col1 = new HashMap<String, Object>();
    col1.put("name", "id");
    col1.put("type", "INTEGER");
    list.add(col1);

    Map<String, Object> col2 = new HashMap<String, Object>();
    col2.put("name", "amount");
    col2.put("type", "DECIMAL");
    col2.put("source", "rawAmount");
    list.add(col2);

    List<ColumnConfig> columns = ColumnConfig.fromList(list);
    assertEquals(2, columns.size());
    assertEquals("id", columns.get(0).getName());
    assertEquals("amount", columns.get(1).getName());
    assertEquals("rawAmount", columns.get(1).getSource());
  }

  @Test void testColumnConfigRequiresName() {
    assertThrows(IllegalArgumentException.class, () -> {
      ColumnConfig.builder()
          .type("VARCHAR")
          .source("field1")
          .build();
    });
  }

  @Test void testEtlPipelineConfigFromMapRoundTrip() {
    // Build a config via fromMap
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "round_trip_pipeline");

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com/v2/data");
    sourceMap.put("method", "POST");
    map.put("source", sourceMap);

    Map<String, Object> dimensionsMap = new LinkedHashMap<String, Object>();
    Map<String, Object> yearDim = new HashMap<String, Object>();
    yearDim.put("type", "range");
    yearDim.put("start", 2015);
    yearDim.put("end", 2025);
    yearDim.put("step", 1);
    dimensionsMap.put("year", yearDim);

    Map<String, Object> regionDim = new HashMap<String, Object>();
    regionDim.put("type", "list");
    regionDim.put("values", Arrays.asList("US", "EU", "APAC"));
    dimensionsMap.put("region", regionDim);
    map.put("dimensions", dimensionsMap);

    List<Map<String, Object>> columnsListMap = new ArrayList<Map<String, Object>>();
    Map<String, Object> col1Map = new HashMap<String, Object>();
    col1Map.put("name", "region_code");
    col1Map.put("type", "VARCHAR");
    col1Map.put("source", "regionCode");
    columnsListMap.add(col1Map);
    map.put("columns", columnsListMap);

    Map<String, Object> materializeMap = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("location", "/data/output");
    outputMap.put("pattern", "year=STAR/region=STAR/");
    materializeMap.put("output", outputMap);
    map.put("materialize", materializeMap);

    Map<String, Object> errorHandlingMap = new HashMap<String, Object>();
    errorHandlingMap.put("transientErrorAction", "retry");
    errorHandlingMap.put("notFoundAction", "warn");
    errorHandlingMap.put("transientRetries", 5);
    map.put("errorHandling", errorHandlingMap);

    // Parse first time
    EtlPipelineConfig config1 = EtlPipelineConfig.fromMap(map);

    assertEquals("round_trip_pipeline", config1.getName());
    assertEquals("https://api.example.com/v2/data", config1.getSource().getUrl());
    assertEquals(HttpSourceConfig.HttpMethod.POST, config1.getSource().getMethod());
    assertEquals(2, config1.getDimensions().size());
    assertTrue(config1.getDimensions().containsKey("year"));
    assertTrue(config1.getDimensions().containsKey("region"));
    assertEquals(1, config1.getColumns().size());
    assertEquals("region_code", config1.getColumns().get(0).getName());
    assertEquals("/data/output", config1.getMaterialize().getOutput().getLocation());

    // Reconstruct a map from the parsed config's getters and re-parse
    Map<String, Object> map2 = new HashMap<String, Object>();
    map2.put("name", config1.getName());

    Map<String, Object> sourceMap2 = new HashMap<String, Object>();
    sourceMap2.put("url", config1.getSource().getUrl());
    sourceMap2.put("method", config1.getSource().getMethod().name());
    map2.put("source", sourceMap2);

    Map<String, Object> dimensionsMap2 = new LinkedHashMap<String, Object>();
    for (Map.Entry<String, DimensionConfig> entry : config1.getDimensions().entrySet()) {
      DimensionConfig dim = entry.getValue();
      Map<String, Object> dimMap = new HashMap<String, Object>();
      dimMap.put("type", dim.getType().name().toLowerCase());
      if (dim.getStart() != null) {
        dimMap.put("start", dim.getStart());
      }
      if (dim.getEnd() != null) {
        dimMap.put("end", dim.getEnd());
      }
      if (!dim.getValues().isEmpty()) {
        dimMap.put("values", dim.getValues());
      }
      dimensionsMap2.put(entry.getKey(), dimMap);
    }
    map2.put("dimensions", dimensionsMap2);

    List<Map<String, Object>> columnsListMap2 = new ArrayList<Map<String, Object>>();
    for (ColumnConfig col : config1.getColumns()) {
      Map<String, Object> colMap = new HashMap<String, Object>();
      colMap.put("name", col.getName());
      colMap.put("type", col.getType());
      if (col.getSource() != null) {
        colMap.put("source", col.getSource());
      }
      columnsListMap2.add(colMap);
    }
    map2.put("columns", columnsListMap2);

    Map<String, Object> materializeMap2 = new HashMap<String, Object>();
    Map<String, Object> outputMap2 = new HashMap<String, Object>();
    outputMap2.put("location", config1.getMaterialize().getOutput().getLocation());
    outputMap2.put("pattern", config1.getMaterialize().getOutput().getPattern());
    materializeMap2.put("output", outputMap2);
    map2.put("materialize", materializeMap2);

    // Parse second time from reconstructed map
    EtlPipelineConfig config2 = EtlPipelineConfig.fromMap(map2);

    assertEquals(config1.getName(), config2.getName());
    assertEquals(config1.getSource().getUrl(), config2.getSource().getUrl());
    assertEquals(config1.getSource().getMethod(), config2.getSource().getMethod());
    assertEquals(config1.getDimensions().size(), config2.getDimensions().size());
    assertEquals(config1.getColumns().size(), config2.getColumns().size());
    assertEquals(config1.getColumns().get(0).getName(), config2.getColumns().get(0).getName());
    assertEquals(
        config1.getMaterialize().getOutput().getLocation(),
        config2.getMaterialize().getOutput().getLocation());
  }

  @Test void testErrorHandlingConfigCustomActions() {
    EtlPipelineConfig.ErrorHandlingConfig config =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .transientErrorAction(
                EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .notFoundAction(
                EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL)
            .apiErrorAction(
                EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY)
            .authErrorAction(
                EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .transientRetries(10)
            .transientBackoffMs(5000)
            .notFoundRetryDays(30)
            .apiErrorRetryDays(14)
            .build();

    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN,
        config.getTransientErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        config.getNotFoundAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        config.getApiErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN,
        config.getAuthErrorAction());
    assertEquals(10, config.getTransientRetries());
    assertEquals(5000, config.getTransientBackoffMs());
    assertEquals(30, config.getNotFoundRetryDays());
    assertEquals(14, config.getApiErrorRetryDays());
  }

  @Test void testErrorHandlingConfigFromMapAllActions() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("transientErrorAction", "fail");
    map.put("notFoundAction", "retry");
    map.put("apiErrorAction", "warn");
    map.put("authErrorAction", "skip");
    map.put("transientRetries", 1);
    map.put("transientBackoffMs", 500);
    map.put("notFoundRetryDays", 3);
    map.put("apiErrorRetryDays", 21);

    EtlPipelineConfig.ErrorHandlingConfig config =
        EtlPipelineConfig.ErrorHandlingConfig.fromMap(map);

    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        config.getTransientErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        config.getNotFoundAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN,
        config.getApiErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        config.getAuthErrorAction());
    assertEquals(1, config.getTransientRetries());
    assertEquals(500, config.getTransientBackoffMs());
    assertEquals(3, config.getNotFoundRetryDays());
    assertEquals(21, config.getApiErrorRetryDays());
  }

  @Test void testErrorHandlingConfigFromMapNull() {
    EtlPipelineConfig.ErrorHandlingConfig config =
        EtlPipelineConfig.ErrorHandlingConfig.fromMap(null);

    // Should return defaults
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        config.getTransientErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        config.getNotFoundAction());
  }

  @Test void testEtlResultSkippedFactory() {
    EtlResult result = EtlResult.skipped("skipped_pipeline", 50);

    assertEquals("skipped_pipeline", result.getPipelineName());
    assertEquals(50, result.getElapsedMs());
    assertTrue(result.isSkippedEntirePipeline());
    assertTrue(result.isSkipped());
    assertTrue(result.isSuccessful());
    assertEquals(0, result.getTotalRows());
    assertEquals(0, result.getSuccessfulBatches());
    assertEquals(0, result.getFailedBatches());
    assertFalse(result.isFailed());
  }

  @Test void testEtlResultSkippedToString() {
    EtlResult result = EtlResult.skipped("my_pipeline", 100);
    String str = result.toString();
    assertTrue(str.contains("my_pipeline"));
    assertTrue(str.contains("SKIPPED"));
    assertTrue(str.contains("100ms"));
  }

  @Test void testEtlResultZeroRows() {
    EtlResult result = EtlResult.builder()
        .pipelineName("empty_pipeline")
        .totalRows(0)
        .successfulBatches(5)
        .failedBatches(0)
        .skippedBatches(0)
        .elapsedMs(100)
        .build();

    assertEquals(0, result.getTotalRows());
    assertEquals(5, result.getSuccessfulBatches());
    assertEquals(5, result.getTotalBatches());
    assertTrue(result.isSuccessful());
    assertTrue(result.isCompleteSuccess());
    assertEquals(0.0, result.getRowsPerSecond(), 0.01);
  }

  @Test void testEtlResultAllBatchesSkipped() {
    EtlResult result = EtlResult.builder()
        .pipelineName("all_skipped")
        .totalRows(0)
        .successfulBatches(0)
        .failedBatches(0)
        .skippedBatches(10)
        .elapsedMs(200)
        .build();

    assertEquals(0, result.getSuccessfulBatches());
    assertEquals(10, result.getSkippedBatches());
    assertEquals(10, result.getTotalBatches());
    assertTrue(result.isSuccessful());
    assertTrue(result.isCompleteSuccess());
  }

  @Test void testEtlResultTableLocationAndFormat() {
    EtlResult result = EtlResult.builder()
        .pipelineName("iceberg_pipeline")
        .totalRows(5000)
        .successfulBatches(3)
        .elapsedMs(1000)
        .tableLocation("s3://bucket/warehouse/my_table")
        .materializeFormat(MaterializeConfig.Format.ICEBERG)
        .build();

    assertEquals("s3://bucket/warehouse/my_table", result.getTableLocation());
    assertEquals(MaterializeConfig.Format.ICEBERG, result.getMaterializeFormat());
  }

  @Test void testEtlResultWithErrorsList() {
    List<String> errors = new ArrayList<String>();
    errors.add("Timeout on batch 3");
    errors.add("404 on batch 7");
    errors.add("Rate limited on batch 9");

    EtlResult result = EtlResult.builder()
        .pipelineName("partial_pipeline")
        .totalRows(7000)
        .successfulBatches(7)
        .failedBatches(3)
        .elapsedMs(5000)
        .errors(errors)
        .build();

    assertEquals(3, result.getErrors().size());
    assertTrue(result.isSuccessful());
    assertFalse(result.isCompleteSuccess());
    assertEquals(10, result.getTotalBatches());
  }

  @Test void testEtlPipelineConfigEnabledDefault() {
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig materialize = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().location("/output").build())
        .build();

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test")
        .source(source)
        .materialize(materialize)
        .build();

    assertTrue(config.isEnabled());
    assertEquals(EtlPipelineConfig.SOURCE_TYPE_HTTP, config.getSourceType());
  }

  @Test void testEtlPipelineConfigDisabledSkipsValidation() {
    // Disabled pipeline should not require source or materialize
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("disabled_pipeline")
        .enabled(false)
        .build();

    assertFalse(config.isEnabled());
    assertEquals("disabled_pipeline", config.getName());
    assertNull(config.getSource());
    assertNull(config.getMaterialize());
  }

  @Test void testDimensionTypeFromStringVariants() {
    assertEquals(DimensionType.RANGE, DimensionType.fromString("range"));
    assertEquals(DimensionType.LIST, DimensionType.fromString("list"));
    assertEquals(DimensionType.QUERY, DimensionType.fromString("query"));
    assertEquals(DimensionType.YEAR_RANGE, DimensionType.fromString("yearRange"));
    assertEquals(DimensionType.YEAR_RANGE, DimensionType.fromString("year_range"));
    assertEquals(DimensionType.CUSTOM, DimensionType.fromString("custom"));
    assertEquals(DimensionType.CUSTOM, DimensionType.fromString("catalog"));
    assertEquals(DimensionType.CUSTOM, DimensionType.fromString("resolver"));
    assertEquals(DimensionType.JSON_CATALOG, DimensionType.fromString("json_catalog"));
    assertEquals(DimensionType.JSON_CATALOG, DimensionType.fromString("jsoncatalog"));
    assertEquals(DimensionType.LIST, DimensionType.fromString("unknown"));
    assertEquals(DimensionType.LIST, DimensionType.fromString(null));
    assertEquals(DimensionType.LIST, DimensionType.fromString(""));
  }
}
