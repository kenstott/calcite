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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for RequestContext.
 */
@Tag("unit")
public class RequestContextTest {

  @Test void testEmptyRequestContext() {
    RequestContext context = RequestContext.builder().build();

    assertNull(context.getUrl());
    assertTrue(context.getParameters().isEmpty());
    assertTrue(context.getHeaders().isEmpty());
    assertTrue(context.getDimensionValues().isEmpty());
  }

  @Test void testRequestContextWithUrl() {
    RequestContext context = RequestContext.builder()
        .url("https://api.example.com/data")
        .build();

    assertEquals("https://api.example.com/data", context.getUrl());
  }

  @Test void testRequestContextWithParameters() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("year", "2024");
    params.put("region", "NORTH");

    RequestContext context = RequestContext.builder()
        .url("https://api.example.com/data")
        .parameters(params)
        .build();

    assertEquals(2, context.getParameters().size());
    assertEquals("2024", context.getParameters().get("year"));
    assertEquals("NORTH", context.getParameters().get("region"));
  }

  @Test void testRequestContextWithHeaders() {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("Accept", "application/json");
    headers.put("Authorization", "Bearer token123");

    RequestContext context = RequestContext.builder()
        .url("https://api.example.com/data")
        .headers(headers)
        .build();

    assertEquals(2, context.getHeaders().size());
    assertEquals("application/json", context.getHeaders().get("Accept"));
    assertEquals("Bearer token123", context.getHeaders().get("Authorization"));
  }

  @Test void testRequestContextWithDimensionValues() {
    Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("year", "2024");
    dimensions.put("frequency", "A");
    dimensions.put("geo", "STATE");

    RequestContext context = RequestContext.builder()
        .url("https://api.example.com/data")
        .dimensionValues(dimensions)
        .build();

    assertEquals(3, context.getDimensionValues().size());
    assertEquals("2024", context.getDimensionValues().get("year"));
    assertEquals("A", context.getDimensionValues().get("frequency"));
    assertEquals("STATE", context.getDimensionValues().get("geo"));
  }

  @Test void testRequestContextFullBuilder() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("format", "json");

    Map<String, String> headers = new HashMap<String, String>();
    headers.put("Accept", "application/json");

    Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("year", "2024");

    RequestContext context = RequestContext.builder()
        .url("https://api.example.com/data")
        .parameters(params)
        .headers(headers)
        .dimensionValues(dimensions)
        .build();

    assertEquals("https://api.example.com/data", context.getUrl());
    assertEquals("json", context.getParameters().get("format"));
    assertEquals("application/json", context.getHeaders().get("Accept"));
    assertEquals("2024", context.getDimensionValues().get("year"));
  }

  @Test void testParametersAreUnmodifiable() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("year", "2024");

    RequestContext context = RequestContext.builder()
        .parameters(params)
        .build();

    assertThrows(UnsupportedOperationException.class, () -> {
      context.getParameters().put("newKey", "newValue");
    });
  }

  @Test void testHeadersAreUnmodifiable() {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("Accept", "application/json");

    RequestContext context = RequestContext.builder()
        .headers(headers)
        .build();

    assertThrows(UnsupportedOperationException.class, () -> {
      context.getHeaders().put("newKey", "newValue");
    });
  }

  @Test void testDimensionValuesAreUnmodifiable() {
    Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("year", "2024");

    RequestContext context = RequestContext.builder()
        .dimensionValues(dimensions)
        .build();

    assertThrows(UnsupportedOperationException.class, () -> {
      context.getDimensionValues().put("newKey", "newValue");
    });
  }

  @Test void testToStringWithAllFields() {
    Map<String, String> params = new HashMap<String, String>();
    params.put("year", "2024");

    Map<String, String> headers = new HashMap<String, String>();
    headers.put("Accept", "application/json");

    Map<String, String> dimensions = new HashMap<String, String>();
    dimensions.put("region", "NORTH");

    RequestContext context = RequestContext.builder()
        .url("https://api.example.com/data")
        .parameters(params)
        .headers(headers)
        .dimensionValues(dimensions)
        .build();

    String toString = context.toString();
    assertTrue(toString.contains("RequestContext"));
    assertTrue(toString.contains("api.example.com"));
    assertTrue(toString.contains("parameters"));
    assertTrue(toString.contains("headers"));
    assertTrue(toString.contains("dimensionValues"));
  }

  @Test void testToStringMinimal() {
    RequestContext context = RequestContext.builder()
        .url("https://api.example.com/data")
        .build();

    String toString = context.toString();
    assertTrue(toString.contains("RequestContext"));
    assertTrue(toString.contains("api.example.com"));
  }
}
