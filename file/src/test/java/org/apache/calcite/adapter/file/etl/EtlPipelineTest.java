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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
}
