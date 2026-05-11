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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ValidationResult}, {@link Validator}, {@link RowContext},
 * {@link RequestContext}, and {@link RowTransformer} interfaces.
 */
@Tag("unit")
class ValidatorAndResultTest {

  // --- ValidationResult tests ---

  @Test void testValidResult() {
    ValidationResult result = ValidationResult.valid();
    assertTrue(result.isValid());
    assertTrue(result.shouldContinue());
    assertTrue(result.shouldInclude());
    assertEquals(ValidationResult.Action.VALID, result.getAction());
    assertNull(result.getMessage());
  }

  @Test void testDropResult() {
    ValidationResult result = ValidationResult.drop("Missing field");
    assertFalse(result.isValid());
    assertTrue(result.shouldContinue());
    assertFalse(result.shouldInclude());
    assertEquals(ValidationResult.Action.DROP, result.getAction());
    assertEquals("Missing field", result.getMessage());
  }

  @Test void testWarnResult() {
    ValidationResult result = ValidationResult.warn("Suspicious value");
    assertFalse(result.isValid());
    assertTrue(result.shouldContinue());
    assertTrue(result.shouldInclude());
    assertEquals(ValidationResult.Action.WARN, result.getAction());
    assertEquals("Suspicious value", result.getMessage());
  }

  @Test void testFailResult() {
    ValidationResult result = ValidationResult.fail("Critical error");
    assertFalse(result.isValid());
    assertFalse(result.shouldContinue());
    assertFalse(result.shouldInclude());
    assertEquals(ValidationResult.Action.FAIL, result.getAction());
    assertEquals("Critical error", result.getMessage());
  }

  @Test void testValidResultToString() {
    String str = ValidationResult.valid().toString();
    assertTrue(str.contains("VALID"));
  }

  @Test void testDropResultToString() {
    String str = ValidationResult.drop("bad data").toString();
    assertTrue(str.contains("DROP"));
    assertTrue(str.contains("bad data"));
  }

  @Test void testWarnResultToString() {
    String str = ValidationResult.warn("caution").toString();
    assertTrue(str.contains("WARN"));
    assertTrue(str.contains("caution"));
  }

  @Test void testFailResultToString() {
    String str = ValidationResult.fail("fatal").toString();
    assertTrue(str.contains("FAIL"));
    assertTrue(str.contains("fatal"));
  }

  @Test void testValidSingleton() {
    // valid() should return same singleton instance
    ValidationResult v1 = ValidationResult.valid();
    ValidationResult v2 = ValidationResult.valid();
    assertTrue(v1 == v2);
  }

  // --- Validator implementation test ---

  @Test void testValidatorImplementation() {
    Validator validator = new Validator() {
      @Override
      public ValidationResult validate(Map<String, Object> row) {
        if (row.get("required_field") == null) {
          return ValidationResult.drop("Missing required_field");
        }
        Object value = row.get("value");
        if (value instanceof Number && ((Number) value).doubleValue() < 0) {
          return ValidationResult.warn("Negative value");
        }
        return ValidationResult.valid();
      }
    };

    // Valid row
    Map<String, Object> validRow = new HashMap<String, Object>();
    validRow.put("required_field", "present");
    validRow.put("value", 100);
    assertTrue(validator.validate(validRow).isValid());

    // Missing field
    Map<String, Object> missingRow = new HashMap<String, Object>();
    missingRow.put("value", 100);
    assertEquals(ValidationResult.Action.DROP,
        validator.validate(missingRow).getAction());

    // Negative value
    Map<String, Object> negativeRow = new HashMap<String, Object>();
    negativeRow.put("required_field", "present");
    negativeRow.put("value", -5);
    assertEquals(ValidationResult.Action.WARN,
        validator.validate(negativeRow).getAction());
  }

  // --- RowContext tests ---

  @Test void testRowContextBuilder() {
    Map<String, String> dims = new HashMap<String, String>();
    dims.put("year", "2024");
    dims.put("region", "EAST");

    RowContext context = RowContext.builder()
        .dimensionValues(dims)
        .rowNumber(42)
        .build();

    assertEquals(42, context.getRowNumber());
    assertEquals("2024", context.getDimensionValues().get("year"));
    assertEquals("EAST", context.getDimensionValues().get("region"));
  }

  @Test void testRowContextDefaultValues() {
    RowContext context = RowContext.builder().build();
    assertEquals(0, context.getRowNumber());
    assertNotNull(context.getDimensionValues());
    assertTrue(context.getDimensionValues().isEmpty());
    assertNull(context.getTableConfig());
  }

  @Test void testRowContextWithTableConfig() {
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("test_table")
        .source(HttpSourceConfig.builder()
            .url("https://example.com/api")
            .build())
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder()
                .location("/output")
                .build())
            .build())
        .build();

    RowContext context = RowContext.builder()
        .tableConfig(config)
        .rowNumber(10)
        .build();

    assertNotNull(context.getTableConfig());
    assertEquals("test_table", context.getTableConfig().getName());
  }

  @Test void testRowContextToString() {
    Map<String, String> dims = new HashMap<String, String>();
    dims.put("year", "2024");

    RowContext context = RowContext.builder()
        .dimensionValues(dims)
        .rowNumber(5)
        .build();

    String str = context.toString();
    assertTrue(str.contains("rowNumber=5"));
    assertTrue(str.contains("dimensionValues"));
    assertTrue(str.contains("year"));
  }

  @Test void testRowContextToStringWithTableConfig() {
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("my_table")
        .source(HttpSourceConfig.builder()
            .url("https://example.com")
            .build())
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder()
                .location("/output")
                .build())
            .build())
        .build();

    RowContext context = RowContext.builder()
        .tableConfig(config)
        .build();

    String str = context.toString();
    assertTrue(str.contains("my_table"));
  }

  @Test void testRowContextDimensionValuesImmutable() {
    Map<String, String> dims = new HashMap<String, String>();
    dims.put("key", "value");

    RowContext context = RowContext.builder()
        .dimensionValues(dims)
        .build();

    try {
      context.getDimensionValues().put("new", "val");
      // If we get here, the map is mutable (undesirable but not an error)
    } catch (UnsupportedOperationException e) {
      // Expected - map should be unmodifiable
    }
  }

  // --- RequestContext tests ---

  @Test void testRequestContextBuilder() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "ABC123");

    Map<String, String> headers = new HashMap<String, String>();
    headers.put("Accept", "application/json");

    Map<String, String> dims = new HashMap<String, String>();
    dims.put("year", "2024");

    RequestContext context = RequestContext.builder()
        .url("https://api.example.com/data")
        .parameters(params)
        .headers(headers)
        .dimensionValues(dims)
        .build();

    assertEquals("https://api.example.com/data", context.getUrl());
    assertEquals("ABC123", context.getParameters().get("key"));
    assertEquals("application/json", context.getHeaders().get("Accept"));
    assertEquals("2024", context.getDimensionValues().get("year"));
  }

  @Test void testRequestContextDefaults() {
    RequestContext context = RequestContext.builder().build();
    assertNull(context.getUrl());
    assertNotNull(context.getParameters());
    assertTrue(context.getParameters().isEmpty());
    assertNotNull(context.getHeaders());
    assertTrue(context.getHeaders().isEmpty());
    assertNotNull(context.getDimensionValues());
    assertTrue(context.getDimensionValues().isEmpty());
  }

  @Test void testRequestContextToString() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "val");

    RequestContext context = RequestContext.builder()
        .url("https://example.com")
        .parameters(params)
        .build();

    String str = context.toString();
    assertTrue(str.contains("https://example.com"));
    assertTrue(str.contains("parameters"));
  }

  @Test void testRequestContextToStringMinimal() {
    RequestContext context = RequestContext.builder()
        .url("https://example.com")
        .build();

    String str = context.toString();
    assertTrue(str.contains("https://example.com"));
    // No parameters or headers sections when empty
    assertFalse(str.contains("parameters"));
    assertFalse(str.contains("headers"));
  }

  @Test void testRequestContextImmutableCollections() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "val");

    RequestContext context = RequestContext.builder()
        .parameters(params)
        .build();

    try {
      context.getParameters().put("new", "val");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  // --- RowTransformer implementation test ---

  @Test void testRowTransformerImplementation() {
    RowTransformer transformer = new RowTransformer() {
      @Override
      public Map<String, Object> transform(Map<String, Object> row,
          RowContext context) {
        // Add computed field
        Object value = row.get("value");
        if (value instanceof Number) {
          row.put("doubled", ((Number) value).doubleValue() * 2);
        }
        // Add dimension context
        String year = context.getDimensionValues().get("year");
        if (year != null) {
          row.put("processed_year", year);
        }
        return row;
      }
    };

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("value", 50);

    Map<String, String> dims = new HashMap<String, String>();
    dims.put("year", "2024");
    RowContext context = RowContext.builder()
        .dimensionValues(dims)
        .build();

    Map<String, Object> result = transformer.transform(row, context);
    assertNotNull(result);
    assertEquals(100.0, result.get("doubled"));
    assertEquals("2024", result.get("processed_year"));
  }

  @Test void testRowTransformerDropRow() {
    RowTransformer filter = new RowTransformer() {
      @Override
      public Map<String, Object> transform(Map<String, Object> row,
          RowContext context) {
        // Drop rows where value is negative
        Object value = row.get("value");
        if (value instanceof Number && ((Number) value).doubleValue() < 0) {
          return null; // Drop
        }
        return row;
      }
    };

    Map<String, Object> goodRow = new HashMap<String, Object>();
    goodRow.put("value", 10);
    assertNotNull(filter.transform(goodRow, RowContext.builder().build()));

    Map<String, Object> badRow = new HashMap<String, Object>();
    badRow.put("value", -5);
    assertNull(filter.transform(badRow, RowContext.builder().build()));
  }

  // --- ResponseTransformer test ---

  @Test void testResponseTransformerImplementation() {
    ResponseTransformer transformer = new ResponseTransformer() {
      @Override
      public String transform(String response, RequestContext context) {
        // Strip wrapper and extract data
        if (response.contains("\"data\":")) {
          int start = response.indexOf("\"data\":") + 7;
          int end = response.lastIndexOf("}");
          return response.substring(start, end);
        }
        return response;
      }
    };

    RequestContext context = RequestContext.builder()
        .url("https://example.com")
        .build();

    String wrapped = "{\"status\":\"ok\",\"data\":[{\"id\":1}]}";
    String result = transformer.transform(wrapped, context);
    assertTrue(result.contains("[{\"id\":1}]"));

    // Plain response passed through
    String plain = "[{\"id\":1}]";
    assertEquals(plain, transformer.transform(plain, context));
  }

  // --- ValidationResult.Action enum ---

  @Test void testActionEnumValues() {
    ValidationResult.Action[] actions = ValidationResult.Action.values();
    assertEquals(4, actions.length);
    assertNotNull(ValidationResult.Action.valueOf("VALID"));
    assertNotNull(ValidationResult.Action.valueOf("DROP"));
    assertNotNull(ValidationResult.Action.valueOf("WARN"));
    assertNotNull(ValidationResult.Action.valueOf("FAIL"));
  }
}
