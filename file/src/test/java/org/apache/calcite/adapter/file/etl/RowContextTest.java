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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for RowContext.
 */
@Tag("unit")
public class RowContextTest {

  @Test void testEmptyRowContext() {
    RowContext context = RowContext.builder().build();

    assertTrue(context.getDimensionValues().isEmpty());
    assertNull(context.getTableConfig());
    assertEquals(0, context.getRowNumber());
  }

  @Test void testRowContextWithDimensionValues() {
    Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("year", "2024");
    dimensions.put("region", "NORTH");

    RowContext context = RowContext.builder()
        .dimensionValues(dimensions)
        .build();

    assertEquals(2, context.getDimensionValues().size());
    assertEquals("2024", context.getDimensionValues().get("year"));
    assertEquals("NORTH", context.getDimensionValues().get("region"));
  }

  @Test void testRowContextWithRowNumber() {
    RowContext context = RowContext.builder()
        .rowNumber(42)
        .build();

    assertEquals(42, context.getRowNumber());
  }

  @Test void testRowContextWithTableConfig() {
    EtlPipelineConfig pipelineConfig = EtlPipelineConfig.builder()
        .name("test_pipeline")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder()
                .location("/data/output")
                .build())
            .build())
        .build();

    RowContext context = RowContext.builder()
        .tableConfig(pipelineConfig)
        .build();

    assertEquals(pipelineConfig, context.getTableConfig());
    assertEquals("test_pipeline", context.getTableConfig().getName());
  }

  @Test void testRowContextFullBuilder() {
    Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("year", "2024");

    EtlPipelineConfig pipelineConfig = EtlPipelineConfig.builder()
        .name("test_pipeline")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder()
                .location("/data/output")
                .build())
            .build())
        .build();

    RowContext context = RowContext.builder()
        .dimensionValues(dimensions)
        .tableConfig(pipelineConfig)
        .rowNumber(100)
        .build();

    assertEquals("2024", context.getDimensionValues().get("year"));
    assertEquals("test_pipeline", context.getTableConfig().getName());
    assertEquals(100, context.getRowNumber());
  }

  @Test void testDimensionValuesAreUnmodifiable() {
    Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("year", "2024");

    RowContext context = RowContext.builder()
        .dimensionValues(dimensions)
        .build();

    assertThrows(UnsupportedOperationException.class, () -> {
      context.getDimensionValues().put("newKey", "newValue");
    });
  }

  @Test void testToStringWithAllFields() {
    Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("year", "2024");

    EtlPipelineConfig pipelineConfig = EtlPipelineConfig.builder()
        .name("test_pipeline")
        .source(HttpSourceConfig.builder()
            .url("https://api.example.com/data")
            .build())
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder()
                .location("/data/output")
                .build())
            .build())
        .build();

    RowContext context = RowContext.builder()
        .dimensionValues(dimensions)
        .tableConfig(pipelineConfig)
        .rowNumber(42)
        .build();

    String toString = context.toString();
    assertTrue(toString.contains("RowContext"));
    assertTrue(toString.contains("rowNumber=42"));
    assertTrue(toString.contains("dimensionValues"));
    assertTrue(toString.contains("test_pipeline"));
  }

  @Test void testToStringMinimal() {
    RowContext context = RowContext.builder()
        .rowNumber(0)
        .build();

    String toString = context.toString();
    assertTrue(toString.contains("RowContext"));
    assertTrue(toString.contains("rowNumber=0"));
  }

  @Test void testLargeRowNumber() {
    RowContext context = RowContext.builder()
        .rowNumber(Long.MAX_VALUE)
        .build();

    assertEquals(Long.MAX_VALUE, context.getRowNumber());
  }
}
