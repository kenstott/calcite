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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link EtlPipelineConfig}.
 */
@Tag("unit")
class EtlPipelineConfigTest {

  @Test void testBuilderMinimal() {
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_table")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder()
                .pattern("year=STAR/")
                .build())
            .build())
        .build();

    assertEquals("test_table", config.getName());
    assertTrue(config.isEnabled());
    assertEquals(EtlPipelineConfig.SOURCE_TYPE_HTTP, config.getSourceType());
    assertNotNull(config.getSource());
    assertNotNull(config.getMaterialize());
    assertNotNull(config.getErrorHandling());
    assertNotNull(config.getHooks());
    assertTrue(config.getDimensions().isEmpty());
    assertTrue(config.getColumns().isEmpty());
  }

  @Test void testBuilderDisabled() {
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test")
        .enabled(false)
        .source(HttpSourceConfig.builder().url("http://test").build())
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .build();

    assertFalse(config.isEnabled());
  }

  @Test void testBuilderMissingNameThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        EtlPipelineConfig.builder()
            .source(HttpSourceConfig.builder().url("http://test").build())
            .materialize(MaterializeConfig.builder()
                .output(MaterializeOutputConfig.builder().build())
                .build())
            .build());
  }

  @Test void testBuilderMissingSourceAndMaterializeThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        EtlPipelineConfig.builder()
            .name("test")
            .build());
  }

  @Test void testFromMapBasic() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "sales_data");

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com/data");
    map.put("source", sourceMap);

    Map<String, Object> materializeMap = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "year=STAR/");
    materializeMap.put("output", outputMap);
    map.put("materialize", materializeMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("sales_data", config.getName());
    assertEquals(EtlPipelineConfig.SOURCE_TYPE_HTTP, config.getSourceType());
    assertNotNull(config.getSource());
    assertNotNull(config.getMaterialize());
  }

  @Test void testFromMapNull() {
    assertNull(EtlPipelineConfig.fromMap(null));
  }

  @Test void testFromMapWithSourceType() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "const_data");

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("type", "constants");
    sourceMap.put("file", "/data/constants.yaml");
    map.put("source", sourceMap);

    Map<String, Object> materializeMap = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    materializeMap.put("output", outputMap);
    map.put("materialize", materializeMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(EtlPipelineConfig.SOURCE_TYPE_CONSTANTS, config.getSourceType());
    assertNull(config.getSource()); // Not HTTP, so no HttpSourceConfig
    assertNotNull(config.getRawSourceConfig());
  }

  @Test void testFromMapWithDimensions() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test");

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com/{year}/data");
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
    outputMap.put("pattern", "year=STAR/");
    materializeMap.put("output", outputMap);
    map.put("materialize", materializeMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(1, config.getDimensions().size());
    assertNotNull(config.getDimensions().get("year"));
  }

  @Test void testFromMapWithColumns() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test");

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com/data");
    map.put("source", sourceMap);

    java.util.List<Map<String, Object>> columnsList = new java.util.ArrayList<Map<String, Object>>();
    Map<String, Object> col1 = new HashMap<String, Object>();
    col1.put("name", "region_code");
    col1.put("type", "VARCHAR");
    col1.put("source", "regionCode");
    columnsList.add(col1);
    map.put("columns", columnsList);

    Map<String, Object> materializeMap = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    materializeMap.put("output", outputMap);
    map.put("materialize", materializeMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(1, config.getColumns().size());
    assertEquals("region_code", config.getColumns().get(0).getName());
  }

  @Test void testFromMapWithEnabledBoolean() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test");
    map.put("enabled", Boolean.FALSE);

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com/data");
    map.put("source", sourceMap);

    Map<String, Object> materializeMap = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    materializeMap.put("output", outputMap);
    map.put("materialize", materializeMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertFalse(config.isEnabled());
  }

  @Test void testFromMapWithEnabledString() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test");
    map.put("enabled", "false");

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com/data");
    map.put("source", sourceMap);

    Map<String, Object> materializeMap = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    materializeMap.put("output", outputMap);
    map.put("materialize", materializeMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertFalse(config.isEnabled());
  }

  @Test void testFromMapWithErrorHandling() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test");

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com/data");
    map.put("source", sourceMap);

    Map<String, Object> errorMap = new HashMap<String, Object>();
    errorMap.put("transientErrorAction", "FAIL");
    errorMap.put("transientRetries", 5);
    map.put("errorHandling", errorMap);

    Map<String, Object> materializeMap = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    materializeMap.put("output", outputMap);
    map.put("materialize", materializeMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        config.getErrorHandling().getTransientErrorAction());
    assertEquals(5, config.getErrorHandling().getTransientRetries());
  }

  @Test void testErrorHandlingConfigDefaults() {
    EtlPipelineConfig.ErrorHandlingConfig defaults =
        EtlPipelineConfig.ErrorHandlingConfig.defaults();

    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        defaults.getTransientErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        defaults.getNotFoundAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        defaults.getApiErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        defaults.getAuthErrorAction());
    assertEquals(3, defaults.getTransientRetries());
    assertEquals(1000, defaults.getTransientBackoffMs());
    assertEquals(7, defaults.getNotFoundRetryDays());
    assertEquals(7, defaults.getApiErrorRetryDays());
  }

  @Test void testErrorHandlingConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("transientErrorAction", "FAIL");
    map.put("notFoundAction", "WARN");
    map.put("apiErrorAction", "RETRY");
    map.put("authErrorAction", "SKIP");
    map.put("transientRetries", 5);
    map.put("transientBackoffMs", 2000L);
    map.put("notFoundRetryDays", 14);
    map.put("apiErrorRetryDays", 30);

    EtlPipelineConfig.ErrorHandlingConfig config =
        EtlPipelineConfig.ErrorHandlingConfig.fromMap(map);

    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL,
        config.getTransientErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN,
        config.getNotFoundAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        config.getApiErrorAction());
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP,
        config.getAuthErrorAction());
    assertEquals(5, config.getTransientRetries());
    assertEquals(2000, config.getTransientBackoffMs());
    assertEquals(14, config.getNotFoundRetryDays());
    assertEquals(30, config.getApiErrorRetryDays());
  }

  @Test void testErrorHandlingConfigFromMapNull() {
    EtlPipelineConfig.ErrorHandlingConfig config =
        EtlPipelineConfig.ErrorHandlingConfig.fromMap(null);
    assertNotNull(config);
    // Should return defaults
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.RETRY,
        config.getTransientErrorAction());
  }

  @Test void testFromMapWithTableComment() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("name", "test");
    map.put("comment", "This is a test table");

    Map<String, Object> sourceMap = new HashMap<String, Object>();
    sourceMap.put("url", "https://api.example.com/data");
    map.put("source", sourceMap);

    Map<String, Object> materializeMap = new HashMap<String, Object>();
    Map<String, Object> outputMap = new HashMap<String, Object>();
    outputMap.put("pattern", "data/");
    materializeMap.put("output", outputMap);
    map.put("materialize", materializeMap);

    EtlPipelineConfig config = EtlPipelineConfig.fromMap(map);
    assertNotNull(config);
    // The table comment should be propagated to materialize config
    assertEquals("This is a test table", config.getMaterialize().getTableComment());
  }

  @Test void testSourceTypeConstants() {
    assertEquals("http", EtlPipelineConfig.SOURCE_TYPE_HTTP);
    assertEquals("file", EtlPipelineConfig.SOURCE_TYPE_FILE);
    assertEquals("constants", EtlPipelineConfig.SOURCE_TYPE_CONSTANTS);
    assertEquals("document", EtlPipelineConfig.SOURCE_TYPE_DOCUMENT);
  }
}
