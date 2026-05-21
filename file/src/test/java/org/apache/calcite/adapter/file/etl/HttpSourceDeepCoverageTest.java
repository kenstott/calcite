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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link HttpSource} targeting uncovered code paths:
 * variable substitution, environment variable resolution, URL building,
 * cache key generation, response parsing, pagination types, batching,
 * constructor variants, normalizeRecords, and error handling.
 */
@Tag("unit")
public class HttpSourceDeepCoverageTest {

  @TempDir
  Path tempDir;

  // --- Constructor variants ---

  @Test void testConstructorMinimal() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    assertEquals("http", source.getType());
    source.close();
  }

  @Test void testConstructorWithHooksConfig() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config, (HooksConfig) null);
    assertEquals("http", source.getType());
    source.close();
  }

  @Test void testConstructorWithStorageProvider() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    StorageProvider mockProvider = mock(StorageProvider.class);

    HttpSource source = new HttpSource(config, null, mockProvider, "/raw/cache");
    assertEquals("http", source.getType());
    source.close();
  }

  @Test void testConstructorWithOperatingDirectory() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    StorageProvider mockProvider = mock(StorageProvider.class);

    HttpSource source =
        new HttpSource(config, null, mockProvider, "/raw/cache", tempDir.toString());
    assertEquals("http", source.getType());
    source.close();
  }

  @Test void testConstructorWithResponseTransformer() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    ResponseTransformer transformer = new ResponseTransformer() {
      @Override public String transform(String response, RequestContext context) {
        return response;
      }
    };

    HttpSource source = new HttpSource(config, transformer);
    assertEquals("http", source.getType());
    source.close();
  }

  // --- Close with cache enabled ---

  @Test void testCloseWithCacheEnabled() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .cache(
            HttpSourceConfig.CacheConfig.fromMap(
            createMap("enabled", true, "ttlSeconds", 60)))
        .build();

    HttpSource source = new HttpSource(config);
    // Close should clear cache
    source.close();
    // Should not throw
  }

  @Test void testCloseWithCacheDisabled() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    source.close();
    // Close without cache should not throw
  }

  // --- Variable substitution via reflection ---

  @Test void testSubstituteVariablesSimple() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    try {
      Method method =
          HttpSource.class.getDeclaredMethod("substituteVariables", String.class, Map.class);
      method.setAccessible(true);

      Map<String, String> vars = new HashMap<>();
      vars.put("year", "2024");
      vars.put("country", "US");

      String result =
          (String) method.invoke(source, "https://api.example.com/{year}/{country}", vars);
      assertEquals("https://api.example.com/2024/US", result);
    } finally {
      source.close();
    }
  }

  @Test void testSubstituteVariablesNoPlaceholders() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    try {
      Method method =
          HttpSource.class.getDeclaredMethod("substituteVariables", String.class, Map.class);
      method.setAccessible(true);

      Map<String, String> vars = new HashMap<>();
      String result = (String) method.invoke(source, "https://api.example.com/static", vars);
      assertEquals("https://api.example.com/static", result);
    } finally {
      source.close();
    }
  }

  // --- buildCacheKey via reflection ---

  @Test void testBuildCacheKey() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    try {
      Method method =
          HttpSource.class.getDeclaredMethod("buildCacheKey", String.class, Map.class);
      method.setAccessible(true);

      Map<String, String> params = new LinkedHashMap<>();
      params.put("year", "2024");
      params.put("month", "01");

      String key = (String) method.invoke(source, "https://api.example.com/data", params);
      assertNotNull(key);
      assertTrue(key.contains("api.example.com"));
    } finally {
      source.close();
    }
  }

  // --- parseResponse via reflection ---

  @Test void testParseResponseJsonArray() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    try {
      Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
      method.setAccessible(true);

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result =
          (List<Map<String, Object>>) method.invoke(source, "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]");
      assertEquals(2, result.size());
      assertEquals(1, result.get(0).get("id"));
    } finally {
      source.close();
    }
  }

  @Test void testParseResponseJsonObject() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    try {
      Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
      method.setAccessible(true);

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result =
          (List<Map<String, Object>>) method.invoke(source, "{\"id\":1,\"name\":\"Alice\"}");
      assertEquals(1, result.size());
    } finally {
      source.close();
    }
  }

  @Test void testParseResponseEmptyArray() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    try {
      Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
      method.setAccessible(true);

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>) method.invoke(source, "[]");
      assertTrue(result.isEmpty());
    } finally {
      source.close();
    }
  }

  // --- normalizeRecords via reflection ---

  @Test void testNormalizeRecordsNoNormalizer() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    try {
      Method method =
          HttpSource.class.getDeclaredMethod("normalizeRecords", List.class, Map.class);
      method.setAccessible(true);

      List<Map<String, Object>> records = new ArrayList<>();
      Map<String, Object> record = new HashMap<>();
      record.put("field1", "value1");
      records.add(record);

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result =
          (List<Map<String, Object>>) method.invoke(source, records, new HashMap<String, String>());

      // Without a normalizer, records should be returned as-is
      assertEquals(1, result.size());
      assertEquals("value1", result.get(0).get("field1"));
    } finally {
      source.close();
    }
  }

  @Test void testNormalizeRecordsEmptyList() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    try {
      Method method =
          HttpSource.class.getDeclaredMethod("normalizeRecords", List.class, Map.class);
      method.setAccessible(true);

      List<Map<String, Object>> records = new ArrayList<>();

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result =
          (List<Map<String, Object>>) method.invoke(source, records, new HashMap<String, String>());

      assertTrue(result.isEmpty());
    } finally {
      source.close();
    }
  }

  // --- loadResponseTransformer ---

  @Test void testInvalidResponseTransformerClass() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HooksConfig hooks = mock(HooksConfig.class);
    when(hooks.getResponseTransformerClass()).thenReturn("com.nonexistent.Class");

    assertThrows(IllegalArgumentException.class, () ->
        new HttpSource(config, hooks));
  }

  // --- loadVariableNormalizer ---

  @Test void testInvalidVariableNormalizerClass() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HooksConfig hooks = mock(HooksConfig.class);
    when(hooks.getResponseTransformerClass()).thenReturn(null);
    when(hooks.getVariableNormalizerClass()).thenReturn("com.nonexistent.Class");
    when(hooks.getVariableNormalizerConfig()).thenReturn(null);

    assertThrows(IllegalArgumentException.class, () ->
        new HttpSource(config, hooks));
  }

  // --- createBatches via reflection ---

  @Test void testCreateBatches() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    try {
      Method method = HttpSource.class.getDeclaredMethod("createBatches", List.class, int.class);
      method.setAccessible(true);

      List<String> values = new ArrayList<>();
      for (int i = 0; i < 7; i++) {
        values.add("item" + i);
      }

      @SuppressWarnings("unchecked")
      List<List<String>> batches = (List<List<String>>) method.invoke(source, values, 3);
      assertEquals(3, batches.size());
      assertEquals(3, batches.get(0).size());
      assertEquals(3, batches.get(1).size());
      assertEquals(1, batches.get(2).size());
    } finally {
      source.close();
    }
  }

  // --- isRawCacheEnabled via reflection ---

  @Test void testIsRawCacheEnabledFalse() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    try {
      Method method = HttpSource.class.getDeclaredMethod("isRawCacheEnabled");
      method.setAccessible(true);

      Boolean result = (Boolean) method.invoke(source);
      assertFalse(result);
    } finally {
      source.close();
    }
  }

  // --- getType ---

  @Test void testGetType() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);
    assertEquals("http", source.getType());
    source.close();
  }

  // --- HttpSourceConfig variations to trigger different fetch paths ---

  @Test void testConfigWithPostMethod() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .method(HttpSourceConfig.HttpMethod.POST)
        .build();
    HttpSource source = new HttpSource(config);
    assertEquals("http", source.getType());
    source.close();
  }

  @Test void testConfigWithHeaders() {
    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Bearer token123");
    headers.put("Accept", "application/json");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .headers(headers)
        .build();
    HttpSource source = new HttpSource(config);
    assertEquals("http", source.getType());
    source.close();
  }

  @Test void testConfigWithParameters() {
    Map<String, String> params = new HashMap<>();
    params.put("year", "{year}");
    params.put("apiKey", "test-key");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .parameters(params)
        .build();
    HttpSource source = new HttpSource(config);
    assertEquals("http", source.getType());
    source.close();
  }

  @Test void testConfigWithJsonPathResponse() {
    Map<String, Object> responseMap = new HashMap<>();
    responseMap.put("dataPath", "data.items");
    responseMap.put("format", "JSON");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(HttpSourceConfig.ResponseConfig.fromMap(responseMap))
        .build();
    HttpSource source = new HttpSource(config);
    assertEquals("http", source.getType());
    source.close();
  }

  // --- Helper methods ---

  private static Map<String, Object> createMap(Object... keysAndValues) {
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }
}
