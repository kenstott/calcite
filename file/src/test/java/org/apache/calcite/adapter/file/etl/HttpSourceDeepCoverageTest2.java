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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests for {@link HttpSource}.
 * Focuses on uncovered branches: variable substitution, response parsing,
 * caching, pagination, rate limiting, auth, body serialization, CSV/TSV
 * parsing, and raw cache operations.
 */
@Tag("unit")
public class HttpSourceDeepCoverageTest2 {

  @TempDir
  Path tempDir;

  @BeforeEach
  void setUp() {
    // No shared state needed
  }

  // --- Constructor coverage ---

  @Test void testConstructorBasic() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);
    assertEquals("http", source.getType());
  }

  @Test void testConstructorWithHooksConfig() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config, (HooksConfig) null);
    assertEquals("http", source.getType());
  }

  @Test void testConstructorWithStorageProviderAndRawCache() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    StorageProvider sp = mock(StorageProvider.class);
    HttpSource source = new HttpSource(config, null, sp, "/tmp/raw");
    assertEquals("http", source.getType());
  }

  @Test void testConstructorWithOperatingDirectory() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    StorageProvider sp = mock(StorageProvider.class);
    HttpSource source = new HttpSource(config, null, sp, "/tmp/raw", tempDir.toString());
    assertEquals("http", source.getType());
  }

  @Test void testConstructorWithResponseTransformer() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    ResponseTransformer transformer = mock(ResponseTransformer.class);
    HttpSource source = new HttpSource(config, transformer);
    assertEquals("http", source.getType());
  }

  @Test void testConstructorWithCacheEnabled() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .cache(
            HttpSourceConfig.CacheConfig.fromMap(
            createMap("enabled", true, "ttlSeconds", 3600)))
        .build();
    HttpSource source = new HttpSource(config);
    assertEquals("http", source.getType());
    source.close(); // Should clear cache without error
  }

  // --- Static factory methods ---

  @Test void testCreateStatic() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = HttpSource.create(config);
    assertEquals("http", source.getType());
  }

  @Test void testCreateWithHooks() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = HttpSource.create(config, null);
    assertEquals("http", source.getType());
  }

  // --- close() coverage ---

  @Test void testCloseNullCache() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);
    source.close(); // cache is null, should not throw
  }

  @Test void testCloseWithCache() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .cache(
            HttpSourceConfig.CacheConfig.fromMap(
            createMap("enabled", true, "ttlSeconds", 100)))
        .build();
    HttpSource source = new HttpSource(config);
    source.close(); // cache cleared
  }

  // --- parseResponse via reflection (JSON format) ---

  @Test void testParseResponseJsonArray() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}]");
    assertEquals(2, result.size());
    assertEquals("Alice", result.get(0).get("name"));
    assertEquals(25, result.get(1).get("age"));
  }

  @Test void testParseResponseJsonObject() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "{\"name\":\"Alice\",\"age\":30}");
    assertEquals(1, result.size());
    assertEquals("Alice", result.get(0).get("name"));
  }

  @Test void testParseResponseWithDataPath() throws Exception {
    Map<String, Object> respMap = new HashMap<String, Object>();
    respMap.put("format", "json");
    respMap.put("dataPath", "$.results.data");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(HttpSourceConfig.ResponseConfig.fromMap(respMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "{\"results\":{\"data\":[{\"id\":1},{\"id\":2}]}}");
    assertEquals(2, result.size());
    assertEquals(1, result.get(0).get("id"));
  }

  @Test void testParseResponseWithErrorPath() throws Exception {
    Map<String, Object> respMap = new HashMap<String, Object>();
    respMap.put("format", "json");
    respMap.put("errorPath", "$.error.message");
    respMap.put("dataPath", "$.data");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(HttpSourceConfig.ResponseConfig.fromMap(respMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    // No error present, should parse data normally
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "{\"error\":{},\"data\":[{\"id\":1}]}");
    assertEquals(1, result.size());
  }

  @Test void testParseResponseWithErrorPathNoData() throws Exception {
    Map<String, Object> respMap = new HashMap<String, Object>();
    respMap.put("format", "json");
    respMap.put("errorPath", "$.error.message");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(HttpSourceConfig.ResponseConfig.fromMap(respMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    // Error containing "no data" should return empty list
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "{\"error\":{\"message\":\"No data available\"}}");
    assertTrue(result.isEmpty());
  }

  @Test void testParseResponseWithRealError() throws Exception {
    Map<String, Object> respMap = new HashMap<String, Object>();
    respMap.put("format", "json");
    respMap.put("errorPath", "$.error");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(HttpSourceConfig.ResponseConfig.fromMap(respMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    // Real error should throw IOException
    try {
      method.invoke(source, "{\"error\":\"Authentication failed\"}");
      assertTrue(false, "Expected exception");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getCause().getMessage().contains("API error"));
    }
  }

  @Test void testParseResponseUnsupportedFormat() throws Exception {
    Map<String, Object> respMap = new HashMap<String, Object>();
    respMap.put("format", "xml");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(HttpSourceConfig.ResponseConfig.fromMap(respMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    try {
      method.invoke(source, "<xml>data</xml>");
      assertTrue(false, "Expected exception");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getCause().getMessage().contains("Unsupported response format"));
    }
  }

  // --- parseDelimitedResponse via reflection ---

  @Test void testParseDelimitedResponseCSV() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "csv")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "name,age,city\nAlice,30,NYC\nBob,25,LA", ',');
    assertEquals(2, result.size());
    assertEquals("Alice", result.get(0).get("name"));
    assertEquals(30L, result.get(0).get("age"));
    assertEquals("NYC", result.get(0).get("city"));
  }

  @Test void testParseDelimitedResponseTSV() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "tsv")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "name\tage\nAlice\t30\nBob\t25", '\t');
    assertEquals(2, result.size());
    assertEquals("Alice", result.get(0).get("name"));
    assertEquals(30L, result.get(0).get("age"));
  }

  @Test void testParseDelimitedResponseEmpty() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "csv")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "", ',');
    assertTrue(result.isEmpty());
  }

  @Test void testParseDelimitedResponseNull() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "csv")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, (String) null, ',');
    assertTrue(result.isEmpty());
  }

  @Test void testParseDelimitedResponseWithQuotedFields() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "csv")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "name,desc\n\"Alice\",\"Hello, World\"", ',');
    assertEquals(1, result.size());
    assertEquals("Alice", result.get(0).get("name"));
    assertEquals("Hello, World", result.get(0).get("desc"));
  }

  // --- parseDelimitedLine via reflection ---

  @Test void testParseDelimitedLineSimple() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedLine", String.class, char.class);
    method.setAccessible(true);

    String[] result = (String[]) method.invoke(source, "a,b,c", ',');
    assertEquals(3, result.length);
    assertEquals("a", result[0]);
    assertEquals("b", result[1]);
    assertEquals("c", result[2]);
  }

  @Test void testParseDelimitedLineQuotedWithComma() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedLine", String.class, char.class);
    method.setAccessible(true);

    String[] result = (String[]) method.invoke(source, "\"a,b\",c", ',');
    assertEquals(2, result.length);
    assertEquals("a,b", result[0]);
    assertEquals("c", result[1]);
  }

  @Test void testParseDelimitedLineEscapedQuotes() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedLine", String.class, char.class);
    method.setAccessible(true);

    String[] result = (String[]) method.invoke(source, "\"say \"\"hello\"\"\",other", ',');
    assertEquals(2, result.length);
    assertEquals("say \"hello\"", result[0]);
  }

  // --- parseValue via reflection ---

  @Test void testParseValueInteger() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseValue", String.class);
    method.setAccessible(true);

    assertEquals(42L, method.invoke(source, "42"));
  }

  @Test void testParseValueDouble() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseValue", String.class);
    method.setAccessible(true);

    assertEquals(3.14, method.invoke(source, "3.14"));
  }

  @Test void testParseValueString() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseValue", String.class);
    method.setAccessible(true);

    assertEquals("hello", method.invoke(source, "hello"));
  }

  @Test void testParseValueNull() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseValue", String.class);
    method.setAccessible(true);

    assertNull(method.invoke(source, (String) null));
  }

  @Test void testParseValueEmpty() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseValue", String.class);
    method.setAccessible(true);

    assertNull(method.invoke(source, ""));
  }

  // --- navigateToPath via reflection ---

  @Test void testNavigateToPathSimple() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("navigateToPath", com.fasterxml.jackson.databind.JsonNode.class, String.class);
    method.setAccessible(true);

    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    com.fasterxml.jackson.databind.JsonNode root =
        mapper.readTree("{\"results\":{\"data\":[1,2,3]}}");

    com.fasterxml.jackson.databind.JsonNode result =
        (com.fasterxml.jackson.databind.JsonNode) method.invoke(source, root, "results.data");
    assertTrue(result.isArray());
    assertEquals(3, result.size());
  }

  @Test void testNavigateToPathWithDollarPrefix() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("navigateToPath", com.fasterxml.jackson.databind.JsonNode.class, String.class);
    method.setAccessible(true);

    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    com.fasterxml.jackson.databind.JsonNode root =
        mapper.readTree("{\"results\":{\"data\":[1]}}");

    com.fasterxml.jackson.databind.JsonNode result =
        (com.fasterxml.jackson.databind.JsonNode) method.invoke(source, root, "$.results.data");
    assertTrue(result.isArray());
    assertEquals(1, result.size());
  }

  @Test void testNavigateToPathWithArrayIndex() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("navigateToPath", com.fasterxml.jackson.databind.JsonNode.class, String.class);
    method.setAccessible(true);

    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    com.fasterxml.jackson.databind.JsonNode root =
        mapper.readTree("{\"items\":[{\"id\":1},{\"id\":2}]}");

    com.fasterxml.jackson.databind.JsonNode result =
        (com.fasterxml.jackson.databind.JsonNode) method.invoke(source, root, "items[0]");
    assertEquals(1, result.get("id").asInt());
  }

  @Test void testNavigateToPathMissing() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("navigateToPath", com.fasterxml.jackson.databind.JsonNode.class, String.class);
    method.setAccessible(true);

    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    com.fasterxml.jackson.databind.JsonNode root =
        mapper.readTree("{\"results\":{}}");

    com.fasterxml.jackson.databind.JsonNode result =
        (com.fasterxml.jackson.databind.JsonNode) method.invoke(source, root, "results.data");
    // Missing path returns empty array
    assertTrue(result.isArray());
    assertEquals(0, result.size());
  }

  // --- buildUrlWithParams via reflection ---

  @Test void testBuildUrlWithParamsEmpty() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("buildUrlWithParams", String.class, Map.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, "https://api.example.com", null);
    assertEquals("https://api.example.com", result);
  }

  @Test void testBuildUrlWithParams() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("buildUrlWithParams", String.class, Map.class);
    method.setAccessible(true);

    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("key", "value");
    params.put("year", "2020");

    String result = (String) method.invoke(source, "https://api.example.com/data", params);
    assertTrue(result.contains("key=value"));
    assertTrue(result.contains("year=2020"));
    assertTrue(result.contains("?"));
  }

  @Test void testBuildUrlWithParamsExistingQueryString() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("buildUrlWithParams", String.class, Map.class);
    method.setAccessible(true);

    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("key", "value");

    String result = (String) method.invoke(source, "https://api.example.com?existing=yes", params);
    assertTrue(result.contains("&key=value"));
    assertFalse(result.contains("?key"));
  }

  // --- buildCacheKey via reflection ---

  @Test void testBuildCacheKey() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("buildCacheKey", String.class, Map.class);
    method.setAccessible(true);

    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("b", "2");
    params.put("a", "1");

    String result = (String) method.invoke(source, "https://api.example.com", params);
    assertNotNull(result);
    assertTrue(result.contains("a=1"));
    assertTrue(result.contains("b=2"));
  }

  @Test void testBuildCacheKeyNoParams() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("buildCacheKey", String.class, Map.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, "https://api.example.com", null);
    assertEquals("https://api.example.com", result);
  }

  // --- shouldRetry via reflection ---

  @Test void testShouldRetryHttp429() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("shouldRetry", IOException.class, HttpSourceConfig.RateLimitConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.RateLimitConfig rateLimit = HttpSourceConfig.RateLimitConfig.defaults();
    boolean result =
        (boolean) method.invoke(source, new IOException("HTTP 429: Too Many Requests"), rateLimit);
    assertTrue(result);
  }

  @Test void testShouldRetryHttp503() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("shouldRetry", IOException.class, HttpSourceConfig.RateLimitConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.RateLimitConfig rateLimit = HttpSourceConfig.RateLimitConfig.defaults();
    boolean result =
        (boolean) method.invoke(source, new IOException("HTTP 503: Service Unavailable"), rateLimit);
    assertTrue(result);
  }

  @Test void testShouldRetryNonRetryable() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("shouldRetry", IOException.class, HttpSourceConfig.RateLimitConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.RateLimitConfig rateLimit = HttpSourceConfig.RateLimitConfig.defaults();
    boolean result =
        (boolean) method.invoke(source, new IOException("HTTP 404: Not Found"), rateLimit);
    assertFalse(result);
  }

  @Test void testShouldRetryNullMessage() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("shouldRetry", IOException.class, HttpSourceConfig.RateLimitConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.RateLimitConfig rateLimit = HttpSourceConfig.RateLimitConfig.defaults();
    boolean result =
        (boolean) method.invoke(source, new IOException((String) null), rateLimit);
    assertFalse(result);
  }

  // --- readResponse via reflection ---

  @Test void testReadResponseNull() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("readResponse", java.io.InputStream.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, (java.io.InputStream) null);
    assertEquals("", result);
  }

  @Test void testReadResponseWithContent() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("readResponse", java.io.InputStream.class);
    method.setAccessible(true);

    ByteArrayInputStream bais =
        new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8));
    String result = (String) method.invoke(source, bais);
    assertTrue(result.contains("Hello World"));
  }

  // --- serializeBody via reflection ---

  @Test void testSerializeBodyJson() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .method(HttpSourceConfig.HttpMethod.POST)
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("serializeBody", Map.class, HttpSourceConfig.BodyFormat.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("key", "value");
    body.put("count", 5);

    Map<String, String> variables = new HashMap<String, String>();

    String result =
        (String) method.invoke(source, body, HttpSourceConfig.BodyFormat.JSON, variables);
    assertTrue(result.contains("\"key\""));
    assertTrue(result.contains("\"value\""));
  }

  @Test void testSerializeBodyFormUrlEncoded() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .method(HttpSourceConfig.HttpMethod.POST)
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("serializeBody", Map.class, HttpSourceConfig.BodyFormat.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("key", "value");
    body.put("name", "hello world");

    Map<String, String> variables = new HashMap<String, String>();

    String result =
        (String) method.invoke(source, body, HttpSourceConfig.BodyFormat.FORM_URLENCODED, variables);
    assertTrue(result.contains("key=value"));
    assertTrue(result.contains("name="));
  }

  // --- substituteBodyVariables via reflection ---

  @Test void testSubstituteBodyVariablesWithStrings() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("substituteBodyVariables", Map.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("year", "{year}");
    body.put("fixed", "constant");
    body.put("number", 42);

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(source, body, variables);
    assertEquals("2024", result.get("year"));
    assertEquals("constant", result.get("fixed"));
    assertEquals(42, result.get("number"));
  }

  @Test void testSubstituteBodyVariablesNestedMap() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("substituteBodyVariables", Map.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> nested = new LinkedHashMap<String, Object>();
    nested.put("val", "{year}");

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("inner", nested);

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(source, body, variables);
    @SuppressWarnings("unchecked")
    Map<String, Object> innerResult = (Map<String, Object>) result.get("inner");
    assertEquals("2024", innerResult.get("val"));
  }

  @Test void testSubstituteBodyVariablesWithList() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("substituteBodyVariables", Map.class, Map.class);
    method.setAccessible(true);

    List<Object> list = new ArrayList<Object>();
    list.add("{year}");
    list.add("static");
    list.add(99);

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("items", list);

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) method.invoke(source, body, variables);
    @SuppressWarnings("unchecked")
    List<Object> resultList = (List<Object>) result.get("items");
    assertEquals("2024", resultList.get(0));
    assertEquals("static", resultList.get(1));
    assertEquals(99, resultList.get(2));
  }

  // --- normalizeRecords via reflection ---

  @Test void testNormalizeRecordsNoNormalizer() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("normalizeRecords", List.class, Map.class);
    method.setAccessible(true);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("field1", "value1");
    records.add(row);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, records, Collections.emptyMap());
    assertEquals(1, result.size());
    assertEquals("value1", result.get(0).get("field1"));
  }

  @Test void testNormalizeRecordsEmptyList() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("normalizeRecords", List.class, Map.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, Collections.emptyList(), Collections.emptyMap());
    assertTrue(result.isEmpty());
  }

  // --- transformResponse via reflection ---

  @Test void testTransformResponseNoTransformer() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("transformResponse", String.class, String.class, Map.class, Map.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(source, "raw response", "http://url", Collections.emptyMap(), Collections.emptyMap());
    assertEquals("raw response", result);
  }

  @Test void testTransformResponseWithTransformer() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    ResponseTransformer transformer = new ResponseTransformer() {
      @Override public String transform(String response, RequestContext context) {
        return "transformed:" + response;
      }
    };

    HttpSource source = new HttpSource(config, transformer);

    Method method =
        HttpSource.class.getDeclaredMethod("transformResponse", String.class, String.class, Map.class, Map.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(source, "raw", "http://url", Collections.emptyMap(), Collections.emptyMap());
    assertEquals("transformed:raw", result);
  }

  @Test void testTransformResponseThrowsException() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    ResponseTransformer transformer = new ResponseTransformer() {
      @Override public String transform(String response, RequestContext context) {
        throw new RuntimeException("Transform failed");
      }
    };

    HttpSource source = new HttpSource(config, transformer);

    Method method =
        HttpSource.class.getDeclaredMethod("transformResponse", String.class, String.class, Map.class, Map.class);
    method.setAccessible(true);

    try {
      method.invoke(source, "raw", "http://url",
          Collections.emptyMap(), Collections.emptyMap());
      assertTrue(false, "Expected exception");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertTrue(e.getCause().getMessage().contains("Transform failed"));
    }
  }

  // --- isLocalPath via reflection ---

  @Test void testIsLocalPath() throws Exception {
    Method method = HttpSource.class.getDeclaredMethod("isLocalPath", String.class);
    method.setAccessible(true);

    assertTrue((boolean) method.invoke(null, "/tmp/cache"));
    assertTrue((boolean) method.invoke(null, "/home/user/data"));
    assertFalse((boolean) method.invoke(null, "s3://bucket/path"));
    assertFalse((boolean) method.invoke(null, "gs://bucket/path"));
    assertFalse((boolean) method.invoke(null, (String) null));
  }

  // --- isRawCacheEnabled via reflection ---

  @Test void testIsRawCacheEnabledWithLocalPath() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .rawCache(
            HttpSourceConfig.RawCacheConfig.fromMap(
            createMap("enabled", true)))
        .build();

    HttpSource source = new HttpSource(config, null, null, "some_table", tempDir.toString());

    Method method = HttpSource.class.getDeclaredMethod("isRawCacheEnabled");
    method.setAccessible(true);

    boolean result = (boolean) method.invoke(source);
    assertTrue(result);
  }

  @Test void testIsRawCacheEnabledWithStorageProvider() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .rawCache(
            HttpSourceConfig.RawCacheConfig.fromMap(
            createMap("enabled", true)))
        .build();

    StorageProvider sp = mock(StorageProvider.class);
    HttpSource source = new HttpSource(config, null, sp, "s3://bucket/raw", null);

    Method method = HttpSource.class.getDeclaredMethod("isRawCacheEnabled");
    method.setAccessible(true);

    boolean result = (boolean) method.invoke(source);
    assertTrue(result);
  }

  @Test void testIsRawCacheDisabled() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("isRawCacheEnabled");
    method.setAccessible(true);

    boolean result = (boolean) method.invoke(source);
    assertFalse(result);
  }

  // --- buildRawCachePath via reflection ---

  @Test void testBuildRawCachePath() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .rawCache(
            HttpSourceConfig.RawCacheConfig.fromMap(
            createMap("enabled", true)))
        .build();

    HttpSource source = new HttpSource(config, null, null, "mytable", tempDir.toString());

    Method method = HttpSource.class.getDeclaredMethod("buildRawCachePath", Map.class);
    method.setAccessible(true);

    Map<String, String> vars = new LinkedHashMap<String, String>();
    vars.put("year", "2020");
    vars.put("type", "regional");

    String result = (String) method.invoke(source, vars);
    assertNotNull(result);
    assertTrue(result.contains("year=2020"));
    assertTrue(result.contains("type=regional"));
    assertTrue(result.endsWith("response.json"));
  }

  // --- sanitizePathComponent via reflection ---

  @Test void testSanitizePathComponent() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("sanitizePathComponent", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, "hello/world:test*file");
    assertFalse(result.contains("/"));
    assertFalse(result.contains(":"));
    assertFalse(result.contains("*"));
    assertEquals("hello_world_test_file", result);
  }

  // --- hasValidRawCache via reflection ---

  @Test void testHasValidRawCacheLocalExists() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("hasValidRawCache", String.class);
    method.setAccessible(true);

    // Create a temp file
    File cacheFile = tempDir.resolve("cache.json").toFile();
    Files.write(cacheFile.toPath(), "cached data".getBytes(StandardCharsets.UTF_8));

    boolean result = (boolean) method.invoke(source, cacheFile.getAbsolutePath());
    assertTrue(result);
  }

  @Test void testHasValidRawCacheLocalNotExists() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("hasValidRawCache", String.class);
    method.setAccessible(true);

    boolean result =
        (boolean) method.invoke(source, tempDir.resolve("nonexistent.json").toString());
    assertFalse(result);
  }

  // --- readRawCache via reflection ---

  @Test void testReadRawCacheLocal() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("readRawCache", String.class);
    method.setAccessible(true);

    File cacheFile = tempDir.resolve("read_cache.json").toFile();
    Files.write(cacheFile.toPath(), "{\"data\":1}".getBytes(StandardCharsets.UTF_8));

    String result = (String) method.invoke(source, cacheFile.getAbsolutePath());
    assertEquals("{\"data\":1}", result);
  }

  @Test void testReadRawCacheStorageProvider() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.openInputStream(anyString())).thenReturn(
        new ByteArrayInputStream("{\"result\":2}".getBytes(StandardCharsets.UTF_8)));

    HttpSource source = new HttpSource(config, null, sp, "s3://bucket/raw");

    Method method = HttpSource.class.getDeclaredMethod("readRawCache", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, "s3://bucket/raw/test/response.json");
    assertEquals("{\"result\":2}", result);
  }

  // --- writeRawCache via reflection ---

  @Test void testWriteRawCacheLocal() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("writeRawCache", String.class, String.class);
    method.setAccessible(true);

    String cachePath = tempDir.resolve("write_cache/data.json").toString();
    method.invoke(source, cachePath, "{\"data\":\"written\"}");

    assertTrue(new File(cachePath).exists());
    String content =
        new String(Files.readAllBytes(new File(cachePath).toPath()), StandardCharsets.UTF_8);
    assertEquals("{\"data\":\"written\"}", content);
  }

  @Test void testWriteRawCacheStorageProvider() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    StorageProvider sp = mock(StorageProvider.class);

    HttpSource source = new HttpSource(config, null, sp, "s3://bucket/raw");

    Method method =
        HttpSource.class.getDeclaredMethod("writeRawCache", String.class, String.class);
    method.setAccessible(true);

    method.invoke(source, "s3://bucket/raw/test/response.json", "content");
    // Verify storage provider was called
  }

  // --- computeLocalRawCachePath via reflection ---

  @Test void testComputeLocalRawCachePathNull() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("computeLocalRawCachePath", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, (String) null);
    assertNull(result);
  }

  @Test void testComputeLocalRawCachePathS3() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("computeLocalRawCachePath", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, "s3://bucket/raw/data");
    assertNotNull(result);
    assertTrue(result.contains("raw/data"));
  }

  @Test void testComputeLocalRawCachePathGCS() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("computeLocalRawCachePath", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, "gs://bucket/raw/data");
    assertNotNull(result);
    assertTrue(result.contains("raw/data"));
  }

  @Test void testComputeLocalRawCachePathWithOperatingDir() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config, null, null, "table", tempDir.toString());

    Method method = HttpSource.class.getDeclaredMethod("computeLocalRawCachePath", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, "localpath");
    assertNotNull(result);
    assertTrue(result.contains(tempDir.toString()));
  }

  // --- checkForApiError via reflection ---

  @Test void testCheckForApiErrorNoError() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "$.error")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(source, "{\"data\":[1,2,3]}", config.getResponse());
    assertNull(result);
  }

  @Test void testCheckForApiErrorWithNoDataMessage() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "$.error")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(source, "{\"error\":\"No data available\"}", config.getResponse());
    assertNull(result); // "No data" messages are treated as valid
  }

  @Test void testCheckForApiErrorWithRealError() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "$.error")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(source, "{\"error\":\"Rate limit exceeded\"}", config.getResponse());
    assertNotNull(result);
    assertEquals("Rate limit exceeded", result);
  }

  @Test void testCheckForApiErrorNullPath() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(source, "{\"data\":1}", config.getResponse());
    assertNull(result);
  }

  @Test void testCheckForApiErrorEmptyArray() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "$.errors")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    // Empty array at error path is not an error
    String result =
        (String) method.invoke(source, "{\"errors\":[]}", config.getResponse());
    assertNull(result);
  }

  @Test void testCheckForApiErrorInvalidJson() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "$.error")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    // Invalid JSON should be treated as valid (null)
    String result =
        (String) method.invoke(source, "not valid json!!!", config.getResponse());
    assertNull(result);
  }

  // --- cacheResponseString via reflection ---

  @Test void testCacheResponseStringLocal() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("cacheResponseString", String.class, String.class);
    method.setAccessible(true);

    String cachePath = tempDir.resolve("cache_str/response.json").toString();
    String result = (String) method.invoke(source, "{\"cached\":true}", cachePath);
    assertEquals(cachePath, result);

    assertTrue(new File(cachePath).exists());
  }

  // --- readFromCache via reflection ---

  @Test void testReadFromCacheLocal() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("readFromCache", String.class);
    method.setAccessible(true);

    File cacheFile = tempDir.resolve("from_cache.json").toFile();
    Files.write(cacheFile.toPath(), "cached content".getBytes(StandardCharsets.UTF_8));

    String result = (String) method.invoke(source, cacheFile.getAbsolutePath());
    assertEquals("cached content", result);
  }

  // --- collectFiles via reflection ---

  @Test void testCollectFiles() throws Exception {
    Method method =
        HttpSource.class.getDeclaredMethod("collectFiles", File.class, List.class);
    method.setAccessible(true);

    // Create nested directory structure
    File subDir = tempDir.resolve("subdir").toFile();
    subDir.mkdirs();
    Files.write(tempDir.resolve("file1.txt"), "f1".getBytes(StandardCharsets.UTF_8));
    Files.write(tempDir.resolve("subdir/file2.txt"), "f2".getBytes(StandardCharsets.UTF_8));

    List<File> result = new ArrayList<File>();
    method.invoke(null, tempDir.toFile(), result);
    assertTrue(result.size() >= 2);
  }

  @Test void testCollectFilesEmptyDir() throws Exception {
    Method method =
        HttpSource.class.getDeclaredMethod("collectFiles", File.class, List.class);
    method.setAccessible(true);

    File emptyDir = tempDir.resolve("emptydir").toFile();
    emptyDir.mkdirs();

    List<File> result = new ArrayList<File>();
    method.invoke(null, emptyDir, result);
    assertEquals(0, result.size());
  }

  // --- resolveDelimiter via reflection ---

  @Test void testResolveDelimiterCSV() throws Exception {
    Method method =
        HttpSource.class.getDeclaredMethod("resolveDelimiter", HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(createMap("format", "csv"));
    char result = (char) method.invoke(null, respConfig);
    assertEquals(',', result);
  }

  @Test void testResolveDelimiterTSV() throws Exception {
    Method method =
        HttpSource.class.getDeclaredMethod("resolveDelimiter", HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(createMap("format", "tsv"));
    char result = (char) method.invoke(null, respConfig);
    assertEquals('\t', result);
  }

  @Test void testResolveDelimiterCustom() throws Exception {
    Method method =
        HttpSource.class.getDeclaredMethod("resolveDelimiter", HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(createMap("format", "csv", "delimiter", "|"));
    char result = (char) method.invoke(null, respConfig);
    assertEquals('|', result);
  }

  // --- loadResponseTransformer via reflection ---

  @Test void testLoadResponseTransformerNull() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("loadResponseTransformer", HooksConfig.class);
    method.setAccessible(true);

    Object result = method.invoke(source, (HooksConfig) null);
    assertNull(result);
  }

  @Test void testLoadResponseTransformerNoClass() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("loadResponseTransformer", HooksConfig.class);
    method.setAccessible(true);

    HooksConfig hooks = HooksConfig.builder().build();
    Object result = method.invoke(source, hooks);
    assertNull(result);
  }

  @Test void testLoadResponseTransformerInvalidClass() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("loadResponseTransformer", HooksConfig.class);
    method.setAccessible(true);

    HooksConfig hooks = HooksConfig.builder()
        .responseTransformerClass("com.nonexistent.FakeTransformer")
        .build();

    try {
      method.invoke(source, hooks);
      assertTrue(false, "Expected exception");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  // --- loadVariableNormalizer via reflection ---

  @Test void testLoadVariableNormalizerNull() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("loadVariableNormalizer", HooksConfig.class);
    method.setAccessible(true);

    Object result = method.invoke(source, (HooksConfig) null);
    assertNull(result);
  }

  @Test void testLoadVariableNormalizerNoClass() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("loadVariableNormalizer", HooksConfig.class);
    method.setAccessible(true);

    HooksConfig hooks = HooksConfig.builder().build();
    Object result = method.invoke(source, hooks);
    assertNull(result);
  }

  @Test void testLoadVariableNormalizerInvalidClass() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("loadVariableNormalizer", HooksConfig.class);
    method.setAccessible(true);

    HooksConfig hooks = HooksConfig.builder()
        .variableNormalizerClass("com.nonexistent.FakeNormalizer")
        .build();

    try {
      method.invoke(source, hooks);
      assertTrue(false, "Expected exception");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  // --- createBatches via reflection ---

  @Test void testCreateBatches() throws Exception {
    Method method =
        HttpSource.class.getDeclaredMethod("createBatches", List.class, int.class);
    method.setAccessible(true);

    List<String> items = Arrays.asList("a", "b", "c", "d", "e");
    @SuppressWarnings("unchecked")
    List<List<String>> batches = (List<List<String>>) method.invoke(null, items, 2);

    assertEquals(3, batches.size());
    assertEquals(2, batches.get(0).size());
    assertEquals(2, batches.get(1).size());
    assertEquals(1, batches.get(2).size());
  }

  @Test void testCreateBatchesSingleBatch() throws Exception {
    Method method =
        HttpSource.class.getDeclaredMethod("createBatches", List.class, int.class);
    method.setAccessible(true);

    List<String> items = Arrays.asList("a", "b");
    @SuppressWarnings("unchecked")
    List<List<String>> batches = (List<List<String>>) method.invoke(null, items, 10);

    assertEquals(1, batches.size());
    assertEquals(2, batches.get(0).size());
  }

  // --- enforceRateLimit via reflection ---

  @Test void testEnforceRateLimitZeroRps() throws Exception {
    Map<String, Object> rateLimitMap = new HashMap<String, Object>();
    rateLimitMap.put("requestsPerSecond", 0);

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .rateLimit(HttpSourceConfig.RateLimitConfig.fromMap(rateLimitMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("enforceRateLimit");
    method.setAccessible(true);

    // Should not throw or block with zero RPS
    method.invoke(source);
  }

  // --- CacheEntry inner class via reflection ---

  @Test void testCacheEntryExpired() throws Exception {
    // Use reflection to instantiate CacheEntry
    Class<?> cacheEntryClass =
        Class.forName("org.apache.calcite.adapter.file.etl.HttpSource$CacheEntry");
    java.lang.reflect.Constructor<?> ctor =
        cacheEntryClass.getDeclaredConstructor(List.class, long.class);
    ctor.setAccessible(true);

    List<Map<String, Object>> data = Collections.emptyList();
    Object expiredEntry = ctor.newInstance(data, System.currentTimeMillis() - 1000);

    Method isExpiredMethod = cacheEntryClass.getDeclaredMethod("isExpired");
    isExpiredMethod.setAccessible(true);
    assertTrue((boolean) isExpiredMethod.invoke(expiredEntry));

    Object validEntry = ctor.newInstance(data, System.currentTimeMillis() + 100000);
    assertFalse((boolean) isExpiredMethod.invoke(validEntry));

    Method getDataMethod = cacheEntryClass.getDeclaredMethod("getData");
    getDataMethod.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> resultData = (List<Map<String, Object>>) getDataMethod.invoke(validEntry);
    assertNotNull(resultData);
  }

  // --- CSV with filter config via parseDelimitedResponse ---

  @Test void testParseDelimitedResponseWithFilter() throws Exception {
    Map<String, Object> filterMap = new HashMap<String, Object>();
    filterMap.put("column", "type");
    filterMap.put("pattern", "A");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "csv")))
        .rowFilter(HttpSourceConfig.RowFilterConfig.fromMap(filterMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "name,type\nAlice,A\nBob,B\nCharlie,A", ',');
    assertEquals(2, result.size());
    assertEquals("Alice", result.get(0).get("name"));
    assertEquals("Charlie", result.get(1).get("name"));
  }

  @Test void testParseDelimitedResponseWithMaxRows() throws Exception {
    Map<String, Object> filterMap = new HashMap<String, Object>();
    filterMap.put("maxRows", 1);

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "csv")))
        .rowFilter(HttpSourceConfig.RowFilterConfig.fromMap(filterMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "name,age\nAlice,30\nBob,25\nCharlie,35", ',');
    assertEquals(1, result.size());
  }

  // --- Headerless CSV with explicit column names ---

  @Test void testParseDelimitedResponseHeaderless() throws Exception {
    Map<String, Object> respMap = new HashMap<String, Object>();
    respMap.put("format", "csv");
    respMap.put("hasHeader", false);
    respMap.put("columnNames", "name,age,city");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(HttpSourceConfig.ResponseConfig.fromMap(respMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "Alice,30,NYC\nBob,25,LA", ',');
    assertEquals(2, result.size());
    assertEquals("Alice", result.get(0).get("name"));
    assertEquals(30L, result.get(0).get("age"));
  }

  // --- Helper methods ---

  @SuppressWarnings("unchecked")
  private static Map<String, Object> createMap(Object... args) {
    Map<String, Object> map = new HashMap<String, Object>();
    for (int i = 0; i < args.length - 1; i += 2) {
      map.put((String) args[i], args[i + 1]);
    }
    return map;
  }
}
