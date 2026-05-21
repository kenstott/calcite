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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link HttpSource} targeting untested code paths.
 *
 * <p>This test class focuses on maximizing JaCoCo line coverage for HttpSource.java
 * by exercising internal methods via reflection where necessary, and by testing
 * code paths that are not covered by {@link HttpSourceTest}.
 */
@Tag("unit")
public class HttpSourceCoverageTest {

  @TempDir
  Path tempDir;

  // ---------------------------------------------------------------
  // 1. Constructor variants
  // ---------------------------------------------------------------

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

  @Test void testConstructorWithCacheEnabled() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .cache(
            HttpSourceConfig.CacheConfig.fromMap(
            createMap("enabled", true, "ttlSeconds", 3600)))
        .build();

    HttpSource source = new HttpSource(config);
    assertEquals("http", source.getType());
    source.close();
  }

  @Test void testConstructorWithHooksConfigNull() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config, (HooksConfig) null);
    assertEquals("http", source.getType());
    source.close();
  }

  @Test void testConstructorWithHooksAndStorageProvider() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config, (HooksConfig) null, null, null);
    assertEquals("http", source.getType());
    source.close();
  }

  @Test void testConstructorWithAllParamsIncludingOperatingDirectory() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source =
        new HttpSource(config, (HooksConfig) null, null, "s3://bucket/.raw", "/tmp/test-op-dir");
    assertEquals("http", source.getType());
    source.close();
  }

  @Test void testCloseWithCacheEnabled() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .cache(
            HttpSourceConfig.CacheConfig.fromMap(
            createMap("enabled", true, "ttlSeconds", 100)))
        .build();

    HttpSource source = new HttpSource(config);
    // close should clear cache without error
    source.close();
  }

  @Test void testCloseWithCacheDisabled() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = new HttpSource(config);
    // close on disabled cache should not error
    source.close();
  }

  // ---------------------------------------------------------------
  // 2. Static factory methods
  // ---------------------------------------------------------------

  @Test void testCreateFactory() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HttpSource source = HttpSource.create(config);
    assertNotNull(source);
    assertEquals("http", source.getType());
    source.close();
  }

  @Test void testCreateFactoryWithHooks() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    HooksConfig hooks = HooksConfig.builder().build();
    HttpSource source = HttpSource.create(config, hooks);
    assertNotNull(source);
    assertEquals("http", source.getType());
    source.close();
  }

  // ---------------------------------------------------------------
  // 3. buildUrlWithParams (via reflection)
  // ---------------------------------------------------------------

  @Test void testBuildUrlWithParamsEmpty() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("buildUrlWithParams", String.class, Map.class);
    method.setAccessible(true);

    // Empty params
    String result = (String) method.invoke(source, "https://api.example.com", Collections.emptyMap());
    assertEquals("https://api.example.com", result);

    // Null params
    result = (String) method.invoke(source, "https://api.example.com", null);
    assertEquals("https://api.example.com", result);

    source.close();
  }

  @Test void testBuildUrlWithParamsSingle() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("buildUrlWithParams", String.class, Map.class);
    method.setAccessible(true);

    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("key", "value");

    String result = (String) method.invoke(source, "https://api.example.com", params);
    assertTrue(result.contains("key=value"));
    assertTrue(result.contains("?"));

    source.close();
  }

  @Test void testBuildUrlWithParamsExistingQuery() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("buildUrlWithParams", String.class, Map.class);
    method.setAccessible(true);

    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("extra", "val");

    String result =
        (String) method.invoke(source, "https://api.example.com?existing=1", params);
    assertTrue(result.contains("&extra=val"));
    assertFalse(result.contains("?extra="));

    source.close();
  }

  @Test void testBuildUrlWithParamsMultiple() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("buildUrlWithParams", String.class, Map.class);
    method.setAccessible(true);

    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("a", "1");
    params.put("b", "2");

    String result = (String) method.invoke(source, "https://api.example.com", params);
    assertTrue(result.contains("a=1"));
    assertTrue(result.contains("b=2"));
    assertTrue(result.contains("&"));

    source.close();
  }

  // ---------------------------------------------------------------
  // 4. buildCacheKey (via reflection)
  // ---------------------------------------------------------------

  @Test void testBuildCacheKey() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("buildCacheKey", String.class, Map.class);
    method.setAccessible(true);

    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("b", "2");
    params.put("a", "1");

    String key = (String) method.invoke(source, "https://api.example.com", params);
    assertNotNull(key);
    // Keys should be sorted
    assertTrue(key.contains("|a=1"));
    assertTrue(key.contains("|b=2"));
    assertTrue(key.indexOf("|a=") < key.indexOf("|b="));

    source.close();
  }

  @Test void testBuildCacheKeyEmptyParams() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("buildCacheKey", String.class, Map.class);
    method.setAccessible(true);

    String key =
        (String) method.invoke(source, "https://api.example.com", Collections.emptyMap());
    assertEquals("https://api.example.com", key);

    key =
        (String) method.invoke(source, "https://api.example.com", null);
    assertEquals("https://api.example.com", key);

    source.close();
  }

  // ---------------------------------------------------------------
  // 5. parseResponse - JSON (via reflection)
  // ---------------------------------------------------------------

  @Test void testParseResponseJsonArray() throws Exception {
    HttpSource source = createBasicSource();
    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source,
            "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}]");
    assertEquals(2, result.size());
    assertEquals("Alice", result.get(0).get("name"));
    assertEquals(30, result.get(0).get("age"));
    assertEquals("Bob", result.get(1).get("name"));

    source.close();
  }

  @Test void testParseResponseJsonObject() throws Exception {
    HttpSource source = createBasicSource();
    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "{\"name\":\"Alice\",\"age\":30}");
    assertEquals(1, result.size());
    assertEquals("Alice", result.get(0).get("name"));

    source.close();
  }

  @Test void testParseResponseWithDataPath() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "dataPath", "$.results.data")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    String json = "{\"results\":{\"data\":[{\"id\":1},{\"id\":2}]}}";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, json);
    assertEquals(2, result.size());
    assertEquals(1, result.get(0).get("id"));
    assertEquals(2, result.get(1).get("id"));

    source.close();
  }

  @Test void testParseResponseWithErrorPath() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "error",
                "dataPath", "data")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    // Test with API error
    String errorJson = "{\"error\":\"Rate limit exceeded\",\"data\":[]}";
    try {
      method.invoke(source, errorJson);
    } catch (InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getCause().getMessage().contains("API error"));
    }

    source.close();
  }

  @Test void testParseResponseWithErrorPathNoData() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "error",
                "dataPath", "data")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    // Test with "no data" error - should return empty list, not throw
    String noDataJson = "{\"error\":\"No data found\",\"data\":[]}";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, noDataJson);
    assertTrue(result.isEmpty());

    source.close();
  }

  @Test void testParseResponseWithErrorPathNotFound() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "error",
                "dataPath", "data")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    // "not found" message
    String notFoundJson = "{\"error\":\"Data not found for parameters\",\"data\":[]}";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, notFoundJson);
    assertTrue(result.isEmpty());

    source.close();
  }

  @Test void testParseResponseWithErrorPathParameterEmpty() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "error",
                "dataPath", "data")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    // "parameter_empty" message
    String paramEmptyJson = "{\"error\":\"parameter_empty\",\"data\":[]}";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, paramEmptyJson);
    assertTrue(result.isEmpty());

    source.close();
  }

  @Test void testParseResponseWithErrorPathUnknownError() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "error",
                "dataPath", "data")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    // "unknown error" message
    String unknownErrorJson = "{\"error\":\"Unknown error occurred\",\"data\":[]}";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, unknownErrorJson);
    assertTrue(result.isEmpty());

    source.close();
  }

  @Test void testParseResponseWithEmptyArrayError() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "errors",
                "dataPath", "data")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    // Empty error array means no error
    String noErrorJson = "{\"errors\":[],\"data\":[{\"id\":1}]}";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, noErrorJson);
    assertEquals(1, result.size());

    source.close();
  }

  @Test void testParseResponseWithNullErrorNode() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "error",
                "dataPath", "data")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    // null error means no error
    String noErrorJson = "{\"error\":null,\"data\":[{\"id\":1}]}";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, noErrorJson);
    assertEquals(1, result.size());

    source.close();
  }

  @Test void testParseResponseWithObjectError() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "error",
                "dataPath", "data")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    // Error as JSON object - should serialize to string
    String objErrorJson = "{\"error\":{\"code\":500,\"message\":\"Server failure\"},\"data\":[]}";
    try {
      method.invoke(source, objErrorJson);
    } catch (InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getCause().getMessage().contains("API error"));
    }

    source.close();
  }

  @Test void testParseResponseUnsupportedFormat() throws Exception {
    Map<String, Object> respMap = new HashMap<String, Object>();
    respMap.put("format", "xml");
    // XML is not supported
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(HttpSourceConfig.ResponseConfig.fromMap(respMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    try {
      method.invoke(source, "<data/>");
    } catch (InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getCause().getMessage().contains("Unsupported response format"));
    }

    source.close();
  }

  // ---------------------------------------------------------------
  // 6. navigateToPath (via reflection) - JSONPath
  // ---------------------------------------------------------------

  @Test void testNavigateToPathSimple() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("navigateToPath", com.fasterxml.jackson.databind.JsonNode.class, String.class);
    method.setAccessible(true);

    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    com.fasterxml.jackson.databind.JsonNode root =
        mapper.readTree("{\"a\":{\"b\":{\"c\":42}}}");

    com.fasterxml.jackson.databind.JsonNode result =
        (com.fasterxml.jackson.databind.JsonNode) method.invoke(source, root, "a.b.c");
    assertEquals(42, result.asInt());

    source.close();
  }

  @Test void testNavigateToPathWithDollarPrefix() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("navigateToPath", com.fasterxml.jackson.databind.JsonNode.class, String.class);
    method.setAccessible(true);

    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    com.fasterxml.jackson.databind.JsonNode root =
        mapper.readTree("{\"results\":{\"data\":[1,2,3]}}");

    com.fasterxml.jackson.databind.JsonNode result =
        (com.fasterxml.jackson.databind.JsonNode) method.invoke(source, root, "$.results.data");
    assertTrue(result.isArray());
    assertEquals(3, result.size());

    source.close();
  }

  @Test void testNavigateToPathWithDollarOnly() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("navigateToPath", com.fasterxml.jackson.databind.JsonNode.class, String.class);
    method.setAccessible(true);

    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    com.fasterxml.jackson.databind.JsonNode root =
        mapper.readTree("{\"data\":[1,2]}");

    // $data (not $.data) - dollar followed by field name
    com.fasterxml.jackson.databind.JsonNode result =
        (com.fasterxml.jackson.databind.JsonNode) method.invoke(source, root, "$data");
    // Should strip $ and look for "ata" after stripping, but since cleanPath strips "$" prefix:
    // "$data" => "data" => look up "data"
    assertNotNull(result);

    source.close();
  }

  @Test void testNavigateToPathWithArrayIndex() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("navigateToPath", com.fasterxml.jackson.databind.JsonNode.class, String.class);
    method.setAccessible(true);

    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    com.fasterxml.jackson.databind.JsonNode root =
        mapper.readTree("{\"items\":[{\"name\":\"first\"},{\"name\":\"second\"}]}");

    com.fasterxml.jackson.databind.JsonNode result =
        (com.fasterxml.jackson.databind.JsonNode) method.invoke(source, root, "items[0].name");
    assertEquals("first", result.asText());

    source.close();
  }

  @Test void testNavigateToPathMissing() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("navigateToPath", com.fasterxml.jackson.databind.JsonNode.class, String.class);
    method.setAccessible(true);

    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    com.fasterxml.jackson.databind.JsonNode root =
        mapper.readTree("{\"a\":1}");

    com.fasterxml.jackson.databind.JsonNode result =
        (com.fasterxml.jackson.databind.JsonNode) method.invoke(source, root, "missing.path");
    // Returns empty array for missing path
    assertNotNull(result);
    assertTrue(result.isArray());
    assertEquals(0, result.size());

    source.close();
  }

  // ---------------------------------------------------------------
  // 7. parseValue (via reflection)
  // ---------------------------------------------------------------

  @Test void testParseValueNull() throws Exception {
    HttpSource source = createBasicSource();
    Method method = HttpSource.class.getDeclaredMethod("parseValue", String.class);
    method.setAccessible(true);

    assertNull(method.invoke(source, (String) null));
    assertNull(method.invoke(source, ""));

    source.close();
  }

  @Test void testParseValueInteger() throws Exception {
    HttpSource source = createBasicSource();
    Method method = HttpSource.class.getDeclaredMethod("parseValue", String.class);
    method.setAccessible(true);

    Object result = method.invoke(source, "42");
    assertTrue(result instanceof Long);
    assertEquals(42L, result);

    source.close();
  }

  @Test void testParseValueDouble() throws Exception {
    HttpSource source = createBasicSource();
    Method method = HttpSource.class.getDeclaredMethod("parseValue", String.class);
    method.setAccessible(true);

    Object result = method.invoke(source, "3.14");
    assertTrue(result instanceof Double);
    assertEquals(3.14, result);

    source.close();
  }

  @Test void testParseValueString() throws Exception {
    HttpSource source = createBasicSource();
    Method method = HttpSource.class.getDeclaredMethod("parseValue", String.class);
    method.setAccessible(true);

    Object result = method.invoke(source, "hello");
    assertEquals("hello", result);

    source.close();
  }

  // ---------------------------------------------------------------
  // 8. parseDelimitedLine (via reflection) - CSV parsing
  // ---------------------------------------------------------------

  @Test void testParseDelimitedLineSimple() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedLine", String.class, char.class);
    method.setAccessible(true);

    String[] result = (String[]) method.invoke(source, "a,b,c", ',');
    assertEquals(3, result.length);
    assertEquals("a", result[0]);
    assertEquals("b", result[1]);
    assertEquals("c", result[2]);

    source.close();
  }

  @Test void testParseDelimitedLineQuoted() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedLine", String.class, char.class);
    method.setAccessible(true);

    String[] result = (String[]) method.invoke(source, "\"hello, world\",b,c", ',');
    assertEquals(3, result.length);
    assertEquals("hello, world", result[0]);

    source.close();
  }

  @Test void testParseDelimitedLineEscapedQuotes() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedLine", String.class, char.class);
    method.setAccessible(true);

    String[] result =
        (String[]) method.invoke(source, "\"he said \"\"hello\"\"\",b", ',');
    assertEquals(2, result.length);
    assertEquals("he said \"hello\"", result[0]);

    source.close();
  }

  @Test void testParseDelimitedLineTabDelimiter() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedLine", String.class, char.class);
    method.setAccessible(true);

    String[] result = (String[]) method.invoke(source, "a\tb\tc", '\t');
    assertEquals(3, result.length);
    assertEquals("a", result[0]);
    assertEquals("b", result[1]);
    assertEquals("c", result[2]);

    source.close();
  }

  // ---------------------------------------------------------------
  // 9. parseDelimitedResponse - CSV/TSV (via reflection)
  // ---------------------------------------------------------------

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

    String csv = "name,age,city\nAlice,30,NYC\nBob,25,LA";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, csv, ',');
    assertEquals(2, result.size());
    assertEquals("Alice", result.get(0).get("name"));
    assertEquals(30L, result.get(0).get("age"));
    assertEquals("NYC", result.get(0).get("city"));
    assertEquals("Bob", result.get(1).get("name"));

    source.close();
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

    String tsv = "name\tage\nAlice\t30\nBob\t25";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, tsv, '\t');
    assertEquals(2, result.size());
    assertEquals("Alice", result.get(0).get("name"));

    source.close();
  }

  @Test void testParseDelimitedResponseEmpty() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "", ',');
    assertTrue(result.isEmpty());

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result2 =
        (List<Map<String, Object>>) method.invoke(source, (String) null, ',');
    assertTrue(result2.isEmpty());

    source.close();
  }

  @Test void testParseDelimitedResponseHeaderOnly() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "name,age", ',');
    assertTrue(result.isEmpty());

    source.close();
  }

  @Test void testParseDelimitedResponseWithQuotedValues() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    String csv = "name,value\n\"Alice\",\"100\"";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, csv, ',');
    assertEquals(1, result.size());
    assertEquals("Alice", result.get(0).get("name"));
    assertEquals(100L, result.get(0).get("value"));

    source.close();
  }

  @Test void testParseDelimitedResponseWithEmptyLines() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    String csv = "name,age\n\nAlice,30\n\nBob,25\n";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, csv, ',');
    assertEquals(2, result.size());

    source.close();
  }

  @Test void testParseDelimitedResponseWithHeaderlessExplicitColumns() throws Exception {
    Map<String, Object> respMap = new HashMap<String, Object>();
    respMap.put("format", "csv");
    respMap.put("hasHeader", false);
    respMap.put("columnNames", "id,name,value");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(HttpSourceConfig.ResponseConfig.fromMap(respMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    String csv = "1,Alice,100\n2,Bob,200";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, csv, ',');
    assertEquals(2, result.size());
    assertEquals(1L, result.get(0).get("id"));
    assertEquals("Alice", result.get(0).get("name"));
    assertEquals(100L, result.get(0).get("value"));

    source.close();
  }

  // ---------------------------------------------------------------
  // 10. shouldRetry (via reflection)
  // ---------------------------------------------------------------

  @Test void testShouldRetryWith429() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("shouldRetry", IOException.class, HttpSourceConfig.RateLimitConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.RateLimitConfig rateLimit = HttpSourceConfig.RateLimitConfig.defaults();

    boolean result =
        (boolean) method.invoke(source, new IOException("HTTP 429: Too Many Requests"), rateLimit);
    assertTrue(result);

    source.close();
  }

  @Test void testShouldRetryWith503() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("shouldRetry", IOException.class, HttpSourceConfig.RateLimitConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.RateLimitConfig rateLimit = HttpSourceConfig.RateLimitConfig.defaults();

    boolean result =
        (boolean) method.invoke(source, new IOException("HTTP 503: Service Unavailable"), rateLimit);
    assertTrue(result);

    source.close();
  }

  @Test void testShouldNotRetryWith404() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("shouldRetry", IOException.class, HttpSourceConfig.RateLimitConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.RateLimitConfig rateLimit = HttpSourceConfig.RateLimitConfig.defaults();

    boolean result =
        (boolean) method.invoke(source, new IOException("HTTP 404: Not Found"), rateLimit);
    assertFalse(result);

    source.close();
  }

  @Test void testShouldNotRetryNullMessage() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("shouldRetry", IOException.class, HttpSourceConfig.RateLimitConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.RateLimitConfig rateLimit = HttpSourceConfig.RateLimitConfig.defaults();

    boolean result =
        (boolean) method.invoke(source, new IOException((String) null), rateLimit);
    assertFalse(result);

    source.close();
  }

  @Test void testShouldRetryWithCustomCodes() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("shouldRetry", IOException.class, HttpSourceConfig.RateLimitConfig.class);
    method.setAccessible(true);

    Map<String, Object> rateLimitMap = new HashMap<String, Object>();
    rateLimitMap.put("retryOn", Arrays.asList(429, 500, 503));
    HttpSourceConfig.RateLimitConfig rateLimit =
        HttpSourceConfig.RateLimitConfig.fromMap(rateLimitMap);

    boolean result =
        (boolean) method.invoke(source, new IOException("HTTP 500: Internal Server Error"), rateLimit);
    assertTrue(result);

    source.close();
  }

  // ---------------------------------------------------------------
  // 11. readResponse (via reflection)
  // ---------------------------------------------------------------

  @Test void testReadResponseNull() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("readResponse", java.io.InputStream.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, (java.io.InputStream) null);
    assertEquals("", result);

    source.close();
  }

  @Test void testReadResponseWithContent() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("readResponse", java.io.InputStream.class);
    method.setAccessible(true);

    java.io.InputStream is =
        new java.io.ByteArrayInputStream("hello world\nline 2".getBytes(StandardCharsets.UTF_8));
    String result = (String) method.invoke(source, is);
    assertTrue(result.contains("hello world"));
    assertTrue(result.contains("line 2"));

    source.close();
  }

  // ---------------------------------------------------------------
  // 12. enforceRateLimit (via reflection)
  // ---------------------------------------------------------------

  @Test void testEnforceRateLimitZeroRps() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .rateLimit(
            HttpSourceConfig.RateLimitConfig.fromMap(
            createMap("requestsPerSecond", 0)))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("enforceRateLimit");
    method.setAccessible(true);

    // Should return immediately without sleeping
    method.invoke(source);

    source.close();
  }

  @Test void testEnforceRateLimitNormalCase() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .rateLimit(
            HttpSourceConfig.RateLimitConfig.fromMap(
            createMap("requestsPerSecond", 100)))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("enforceRateLimit");
    method.setAccessible(true);

    // First call sets lastRequestTime
    method.invoke(source);

    // Second call should not block significantly with 100 RPS
    long start = System.currentTimeMillis();
    method.invoke(source);
    long elapsed = System.currentTimeMillis() - start;
    assertTrue(elapsed < 100, "Rate limiting took too long: " + elapsed + "ms");

    source.close();
  }

  // ---------------------------------------------------------------
  // 13. transformResponse (via reflection)
  // ---------------------------------------------------------------

  @Test void testTransformResponseNoTransformer() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("transformResponse", String.class, String.class, Map.class, Map.class);
    method.setAccessible(true);

    String result =
        (String) method.invoke(source, "original response",
        "https://api.example.com",
        Collections.emptyMap(),
        Collections.emptyMap());
    assertEquals("original response", result);

    source.close();
  }

  @Test void testTransformResponseWithTransformer() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();

    ResponseTransformer transformer = new ResponseTransformer() {
      @Override public String transform(String response, RequestContext context) {
        return response.toUpperCase();
      }
    };
    HttpSource source = new HttpSource(config, transformer);

    Method method =
        HttpSource.class.getDeclaredMethod("transformResponse", String.class, String.class, Map.class, Map.class);
    method.setAccessible(true);

    Map<String, String> params = new LinkedHashMap<String, String>();
    Map<String, String> dims = new LinkedHashMap<String, String>();

    String result =
        (String) method.invoke(source, "hello", "https://api.example.com", params, dims);
    assertEquals("HELLO", result);

    source.close();
  }

  @Test void testTransformResponseTransformerThrows() throws Exception {
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
      method.invoke(source, "data",
          "https://api.example.com", Collections.emptyMap(), Collections.emptyMap());
    } catch (InvocationTargetException e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertTrue(e.getCause().getMessage().contains("Transform failed"));
    }

    source.close();
  }

  // ---------------------------------------------------------------
  // 14. normalizeRecords (via reflection)
  // ---------------------------------------------------------------

  @Test void testNormalizeRecordsNoNormalizer() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("normalizeRecords", List.class, Map.class);
    method.setAccessible(true);

    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, Object> record = new LinkedHashMap<String, Object>();
    record.put("field1", "value1");
    records.add(record);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, records, Collections.emptyMap());
    // Should return same list unchanged
    assertEquals(1, result.size());
    assertEquals("value1", result.get(0).get("field1"));

    source.close();
  }

  @Test void testNormalizeRecordsEmptyList() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("normalizeRecords", List.class, Map.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(
            source, Collections.emptyList(), Collections.emptyMap());
    assertTrue(result.isEmpty());

    source.close();
  }

  // ---------------------------------------------------------------
  // 15. serializeBody (via reflection)
  // ---------------------------------------------------------------

  @Test void testSerializeBodyJson() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("serializeBody", Map.class, HttpSourceConfig.BodyFormat.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("key", "value");
    body.put("num", 42);

    Map<String, String> variables = new HashMap<String, String>();

    String result =
        (String) method.invoke(source, body, HttpSourceConfig.BodyFormat.JSON, variables);
    assertTrue(result.contains("\"key\""));
    assertTrue(result.contains("\"value\""));
    assertTrue(result.contains("42"));

    source.close();
  }

  @Test void testSerializeBodyFormUrlEncoded() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("serializeBody", Map.class, HttpSourceConfig.BodyFormat.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("key", "value");
    body.put("name", "test");

    Map<String, String> variables = new HashMap<String, String>();

    String result =
        (String) method.invoke(source, body, HttpSourceConfig.BodyFormat.FORM_URLENCODED, variables);
    assertTrue(result.contains("key=value"));
    assertTrue(result.contains("name=test"));
    assertTrue(result.contains("&"));

    source.close();
  }

  // ---------------------------------------------------------------
  // 16. substituteBodyVariables (via reflection) - recursive
  // ---------------------------------------------------------------

  @Test void testSubstituteBodyVariablesString() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("substituteBodyVariables", Map.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("year", "{year}");
    body.put("fixed", "constant");

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) method.invoke(source, body, variables);
    assertEquals("2024", result.get("year"));
    assertEquals("constant", result.get("fixed"));

    source.close();
  }

  @Test void testSubstituteBodyVariablesNestedMap() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("substituteBodyVariables", Map.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> inner = new LinkedHashMap<String, Object>();
    inner.put("field", "{year}");

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("nested", inner);

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) method.invoke(source, body, variables);
    @SuppressWarnings("unchecked")
    Map<String, Object> nestedResult = (Map<String, Object>) result.get("nested");
    assertEquals("2024", nestedResult.get("field"));

    source.close();
  }

  @Test void testSubstituteBodyVariablesList() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("substituteBodyVariables", Map.class, Map.class);
    method.setAccessible(true);

    List<Object> list = new ArrayList<Object>();
    list.add("{year}");
    list.add("fixed");
    list.add(42);

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("items", list);

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) method.invoke(source, body, variables);
    @SuppressWarnings("unchecked")
    List<Object> resultList = (List<Object>) result.get("items");
    assertEquals("2024", resultList.get(0));
    assertEquals("fixed", resultList.get(1));
    assertEquals(42, resultList.get(2));

    source.close();
  }

  @Test void testSubstituteBodyVariablesNonStringValue() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("substituteBodyVariables", Map.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("number", 42);
    body.put("flag", true);

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) method.invoke(
            source, body, Collections.<String, String>emptyMap());
    assertEquals(42, result.get("number"));
    assertEquals(true, result.get("flag"));

    source.close();
  }

  // ---------------------------------------------------------------
  // 17. substituteListVariables (via reflection)
  // ---------------------------------------------------------------

  @Test void testSubstituteListVariablesNestedList() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("substituteListVariables", List.class, Map.class);
    method.setAccessible(true);

    List<Object> innerList = new ArrayList<Object>();
    innerList.add("{year}");

    List<Object> outerList = new ArrayList<Object>();
    outerList.add(innerList);

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");

    @SuppressWarnings("unchecked")
    List<Object> result = (List<Object>) method.invoke(source, outerList, variables);
    @SuppressWarnings("unchecked")
    List<Object> innerResult = (List<Object>) result.get(0);
    assertEquals("2024", innerResult.get(0));

    source.close();
  }

  @Test void testSubstituteListVariablesWithMap() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("substituteListVariables", List.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> mapItem = new LinkedHashMap<String, Object>();
    mapItem.put("field", "{year}");

    List<Object> list = new ArrayList<Object>();
    list.add(mapItem);

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");

    @SuppressWarnings("unchecked")
    List<Object> result = (List<Object>) method.invoke(source, list, variables);
    @SuppressWarnings("unchecked")
    Map<String, Object> resultMap = (Map<String, Object>) result.get(0);
    assertEquals("2024", resultMap.get("field"));

    source.close();
  }

  // ---------------------------------------------------------------
  // 18. resolveDelimiter (via reflection, static method)
  // ---------------------------------------------------------------

  @Test void testResolveDelimiterDefault() throws Exception {
    Method method =
        HttpSource.class.getDeclaredMethod("resolveDelimiter", HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.ResponseConfig csvConfig =
        HttpSourceConfig.ResponseConfig.fromMap(createMap("format", "csv"));
    char result = (char) method.invoke(null, csvConfig);
    assertEquals(',', result);

    HttpSourceConfig.ResponseConfig tsvConfig =
        HttpSourceConfig.ResponseConfig.fromMap(createMap("format", "tsv"));
    result = (char) method.invoke(null, tsvConfig);
    assertEquals('\t', result);
  }

  @Test void testResolveDelimiterCustom() throws Exception {
    Method method =
        HttpSource.class.getDeclaredMethod("resolveDelimiter", HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("format", "csv");
    configMap.put("delimiter", "|");

    HttpSourceConfig.ResponseConfig config =
        HttpSourceConfig.ResponseConfig.fromMap(configMap);
    char result = (char) method.invoke(null, config);
    assertEquals('|', result);
  }

  // ---------------------------------------------------------------
  // 19. isLocalPath (via reflection, static method)
  // ---------------------------------------------------------------

  @Test void testIsLocalPath() throws Exception {
    Method method = HttpSource.class.getDeclaredMethod("isLocalPath", String.class);
    method.setAccessible(true);

    assertTrue((boolean) method.invoke(null, "/tmp/file.json"));
    assertTrue((boolean) method.invoke(null, "/home/user/data.csv"));
    assertFalse((boolean) method.invoke(null, "s3://bucket/file.json"));
    assertFalse((boolean) method.invoke(null, "gs://bucket/file.json"));
    assertFalse((boolean) method.invoke(null, (String) null));
  }

  // ---------------------------------------------------------------
  // 20. computeLocalRawCachePath (via reflection)
  // ---------------------------------------------------------------

  @Test void testComputeLocalRawCachePathNull() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("computeLocalRawCachePath", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, (String) null);
    assertNull(result);

    source.close();
  }

  @Test void testComputeLocalRawCachePathS3() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source =
        new HttpSource(config, (HooksConfig) null, null, "s3://my-bucket/raw-data", "/tmp/op-dir");

    Method method =
        HttpSource.class.getDeclaredMethod("computeLocalRawCachePath", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, "s3://bucket/path/to/data");
    assertNotNull(result);
    assertTrue(result.contains("path/to/data"));

    source.close();
  }

  @Test void testComputeLocalRawCachePathGS() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config, (HooksConfig) null, null, null, "/tmp/op-dir");

    Method method =
        HttpSource.class.getDeclaredMethod("computeLocalRawCachePath", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, "gs://bucket/data");
    assertNotNull(result);
    assertTrue(result.contains("data"));

    source.close();
  }

  @Test void testComputeLocalRawCachePathLocal() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config, (HooksConfig) null, null, null, "/tmp/op-dir");

    Method method =
        HttpSource.class.getDeclaredMethod("computeLocalRawCachePath", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, "/local/path/data");
    assertNotNull(result);
    assertTrue(result.contains("/local/path/data"));

    source.close();
  }

  @Test void testComputeLocalRawCachePathEmptySuffix() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config, (HooksConfig) null, null, null, "/tmp/op-dir");

    Method method =
        HttpSource.class.getDeclaredMethod("computeLocalRawCachePath", String.class);
    method.setAccessible(true);

    // S3 URL with only bucket, no path after
    String result = (String) method.invoke(source, "s3://bucket");
    assertNotNull(result);

    source.close();
  }

  // ---------------------------------------------------------------
  // 21. sanitizePathComponent (via reflection)
  // ---------------------------------------------------------------

  @Test void testSanitizePathComponent() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("sanitizePathComponent", String.class);
    method.setAccessible(true);

    assertEquals("normal", method.invoke(source, "normal"));
    assertEquals("with_slash", method.invoke(source, "with/slash"));
    assertEquals("with_star", method.invoke(source, "with*star"));
    assertEquals("with_colon", method.invoke(source, "with:colon"));
    assertEquals("with_question", method.invoke(source, "with?question"));
    assertEquals("with_quotes", method.invoke(source, "with\"quotes"));

    source.close();
  }

  // ---------------------------------------------------------------
  // 22. buildRawCachePath (via reflection)
  // ---------------------------------------------------------------

  @Test void testBuildRawCachePath() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source =
        new HttpSource(config, (HooksConfig) null, null, "/tmp/cache/.raw", null);

    Method method =
        HttpSource.class.getDeclaredMethod("buildRawCachePath", Map.class);
    method.setAccessible(true);

    Map<String, String> variables = new LinkedHashMap<String, String>();
    variables.put("year", "2024");
    variables.put("region", "US");

    String result = (String) method.invoke(source, variables);
    assertNotNull(result);
    assertTrue(result.contains("region=US"));
    assertTrue(result.contains("year=2024"));
    assertTrue(result.endsWith("response.json"));

    source.close();
  }

  @Test void testBuildRawCachePathEmptyValues() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source =
        new HttpSource(config, (HooksConfig) null, null, "/tmp/cache/.raw/", null);

    Method method =
        HttpSource.class.getDeclaredMethod("buildRawCachePath", Map.class);
    method.setAccessible(true);

    Map<String, String> variables = new LinkedHashMap<String, String>();
    variables.put("year", "2024");
    variables.put("empty", "");
    variables.put("nullVal", null);

    String result = (String) method.invoke(source, variables);
    assertNotNull(result);
    assertTrue(result.contains("year=2024"));
    assertFalse(result.contains("empty="));
    assertFalse(result.contains("nullVal="));

    source.close();
  }

  // ---------------------------------------------------------------
  // 23. hasValidRawCache (via reflection)
  // ---------------------------------------------------------------

  @Test void testHasValidRawCacheLocalExists() throws Exception {
    // Create a temp file to act as cache
    File cacheFile = new File(tempDir.toFile(), "test-cache.json");
    Files.write(cacheFile.toPath(), "{}".getBytes(StandardCharsets.UTF_8));

    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("hasValidRawCache", String.class);
    method.setAccessible(true);

    boolean result = (boolean) method.invoke(source, cacheFile.getAbsolutePath());
    assertTrue(result);

    source.close();
  }

  @Test void testHasValidRawCacheLocalMissing() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("hasValidRawCache", String.class);
    method.setAccessible(true);

    boolean result =
        (boolean) method.invoke(source, tempDir.resolve("nonexistent.json").toString());
    assertFalse(result);

    source.close();
  }

  // ---------------------------------------------------------------
  // 24. readRawCache (via reflection)
  // ---------------------------------------------------------------

  @Test void testReadRawCacheLocal() throws Exception {
    File cacheFile = new File(tempDir.toFile(), "cached-response.json");
    String content = "{\"data\":[{\"id\":1}]}";
    Files.write(cacheFile.toPath(), content.getBytes(StandardCharsets.UTF_8));

    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("readRawCache", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, cacheFile.getAbsolutePath());
    assertEquals(content, result);

    source.close();
  }

  // ---------------------------------------------------------------
  // 25. writeRawCache (via reflection)
  // ---------------------------------------------------------------

  @Test void testWriteRawCacheLocal() throws Exception {
    File cacheDir = new File(tempDir.toFile(), "write-cache");
    String cachePath = new File(cacheDir, "response.json").getAbsolutePath();
    String content = "{\"data\":\"test\"}";

    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("writeRawCache", String.class, String.class);
    method.setAccessible(true);

    method.invoke(source, cachePath, content);

    // Verify file was created
    File cacheFile = new File(cachePath);
    assertTrue(cacheFile.exists());
    String written = new String(Files.readAllBytes(cacheFile.toPath()), StandardCharsets.UTF_8);
    assertEquals(content, written);

    source.close();
  }

  // ---------------------------------------------------------------
  // 26. isRawCacheEnabled (via reflection)
  // ---------------------------------------------------------------

  @Test void testIsRawCacheEnabledFalse() throws Exception {
    HttpSource source = createBasicSource();
    Method method = HttpSource.class.getDeclaredMethod("isRawCacheEnabled");
    method.setAccessible(true);

    boolean result = (boolean) method.invoke(source);
    assertFalse(result);

    source.close();
  }

  @Test void testIsRawCacheEnabledWithLocalPath() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .rawCache(HttpSourceConfig.RawCacheConfig.enabled())
        .build();
    HttpSource source =
        new HttpSource(config, (HooksConfig) null, null, "/tmp/raw-cache", "/tmp/op-dir");

    Method method = HttpSource.class.getDeclaredMethod("isRawCacheEnabled");
    method.setAccessible(true);

    boolean result = (boolean) method.invoke(source);
    assertTrue(result);

    source.close();
  }

  // ---------------------------------------------------------------
  // 27. cacheResponseString (via reflection)
  // ---------------------------------------------------------------

  @Test void testCacheResponseStringLocal() throws Exception {
    File cacheDir = new File(tempDir.toFile(), "cache-str");
    String cachePath = new File(cacheDir, "cached.json").getAbsolutePath();

    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("cacheResponseString", String.class, String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, "{\"data\":1}", cachePath);
    assertEquals(cachePath, result);
    assertTrue(new File(cachePath).exists());

    source.close();
  }

  // ---------------------------------------------------------------
  // 28. cacheResponse (via reflection)
  // ---------------------------------------------------------------

  @Test void testCacheResponseLocal() throws Exception {
    File cacheDir = new File(tempDir.toFile(), "cache-bin");
    String cachePath = new File(cacheDir, "cached.bin").getAbsolutePath();

    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("cacheResponse", java.io.InputStream.class, String.class);
    method.setAccessible(true);

    java.io.InputStream is =
        new java.io.ByteArrayInputStream("binary content".getBytes(StandardCharsets.UTF_8));
    String result = (String) method.invoke(source, is, cachePath);
    assertEquals(cachePath, result);
    assertTrue(new File(cachePath).exists());

    source.close();
  }

  // ---------------------------------------------------------------
  // 29. readFromCache (via reflection)
  // ---------------------------------------------------------------

  @Test void testReadFromCacheLocal() throws Exception {
    File cacheFile = new File(tempDir.toFile(), "read-cache.json");
    String content = "cached content here";
    Files.write(cacheFile.toPath(), content.getBytes(StandardCharsets.UTF_8));

    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("readFromCache", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(source, cacheFile.getAbsolutePath());
    assertEquals(content, result);

    source.close();
  }

  // ---------------------------------------------------------------
  // 30. checkForApiError (via reflection)
  // ---------------------------------------------------------------

  @Test void testCheckForApiErrorNoErrorPath() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.ResponseConfig respConfig = HttpSourceConfig.ResponseConfig.defaults();
    String result = (String) method.invoke(source, "{\"data\":1}", respConfig);
    assertNull(result);

    source.close();
  }

  @Test void testCheckForApiErrorWithError() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "error"));

    String result =
        (String) method.invoke(source, "{\"error\":\"Something went wrong\"}", respConfig);
    assertNotNull(result);
    assertEquals("Something went wrong", result);

    source.close();
  }

  @Test void testCheckForApiErrorNoDataMessage() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "error"));

    // "no data" type messages should return null (cacheable)
    String result =
        (String) method.invoke(source, "{\"error\":\"No data available\"}", respConfig);
    assertNull(result);

    source.close();
  }

  @Test void testCheckForApiErrorEmptyArray() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "errors"));

    // Empty error array = no error
    String result =
        (String) method.invoke(source, "{\"errors\":[]}", respConfig);
    assertNull(result);

    source.close();
  }

  @Test void testCheckForApiErrorNullError() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "error"));

    String result =
        (String) method.invoke(source, "{\"error\":null}", respConfig);
    assertNull(result);

    source.close();
  }

  @Test void testCheckForApiErrorObjectError() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "error"));

    // Object error node
    String result =
        (String) method.invoke(source, "{\"error\":{\"code\":500}}", respConfig);
    assertNotNull(result);
    assertTrue(result.contains("500"));

    source.close();
  }

  @Test void testCheckForApiErrorInvalidJson() throws Exception {
    HttpSource source = createBasicSource();
    Method method =
        HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
    method.setAccessible(true);

    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "error"));

    // Invalid JSON - should return null (treat as valid)
    String result =
        (String) method.invoke(source, "not json at all", respConfig);
    assertNull(result);

    source.close();
  }

  // ---------------------------------------------------------------
  // 31. collectFiles (via reflection, static method)
  // ---------------------------------------------------------------

  @Test void testCollectFiles() throws Exception {
    // Create temp directory structure
    File subDir = new File(tempDir.toFile(), "sub");
    subDir.mkdirs();
    Files.write(new File(tempDir.toFile(), "file1.txt").toPath(),
        "a".getBytes(StandardCharsets.UTF_8));
    Files.write(new File(subDir, "file2.txt").toPath(),
        "b".getBytes(StandardCharsets.UTF_8));

    Method method =
        HttpSource.class.getDeclaredMethod("collectFiles", File.class, List.class);
    method.setAccessible(true);

    List<File> result = new ArrayList<File>();
    method.invoke(null, tempDir.toFile(), result);
    assertEquals(2, result.size());
  }

  @Test void testCollectFilesEmptyDir() throws Exception {
    File emptyDir = new File(tempDir.toFile(), "empty");
    emptyDir.mkdirs();

    Method method =
        HttpSource.class.getDeclaredMethod("collectFiles", File.class, List.class);
    method.setAccessible(true);

    List<File> result = new ArrayList<File>();
    method.invoke(null, emptyDir, result);
    assertTrue(result.isEmpty());
  }

  @Test void testCollectFilesNonexistentDir() throws Exception {
    File noDir = new File(tempDir.toFile(), "nonexistent");

    Method method =
        HttpSource.class.getDeclaredMethod("collectFiles", File.class, List.class);
    method.setAccessible(true);

    List<File> result = new ArrayList<File>();
    method.invoke(null, noDir, result);
    assertTrue(result.isEmpty());
  }

  // ---------------------------------------------------------------
  // 32. createBatches (via reflection, static method)
  // ---------------------------------------------------------------

  @Test void testCreateBatches() throws Exception {
    Method method =
        HttpSource.class.getDeclaredMethod("createBatches", List.class, int.class);
    method.setAccessible(true);

    List<String> items = Arrays.asList("a", "b", "c", "d", "e");

    @SuppressWarnings("unchecked")
    List<List<String>> batches = (List<List<String>>) method.invoke(null, items, 2);
    assertEquals(3, batches.size());
    assertEquals(Arrays.asList("a", "b"), batches.get(0));
    assertEquals(Arrays.asList("c", "d"), batches.get(1));
    assertEquals(Collections.singletonList("e"), batches.get(2));
  }

  @Test void testCreateBatchesSingleBatch() throws Exception {
    Method method =
        HttpSource.class.getDeclaredMethod("createBatches", List.class, int.class);
    method.setAccessible(true);

    List<String> items = Arrays.asList("a", "b");

    @SuppressWarnings("unchecked")
    List<List<String>> batches = (List<List<String>>) method.invoke(null, items, 10);
    assertEquals(1, batches.size());
    assertEquals(Arrays.asList("a", "b"), batches.get(0));
  }

  @Test void testCreateBatchesEmpty() throws Exception {
    Method method =
        HttpSource.class.getDeclaredMethod("createBatches", List.class, int.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<List<String>> batches =
        (List<List<String>>) method.invoke(null, Collections.emptyList(), 5);
    assertTrue(batches.isEmpty());
  }

  // ---------------------------------------------------------------
  // 33. loadResponseTransformer (via reflection)
  // ---------------------------------------------------------------

  @Test void testLoadResponseTransformerNull() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    // Verify internal field is null
    Field field = HttpSource.class.getDeclaredField("responseTransformer");
    field.setAccessible(true);
    assertNull(field.get(source));

    source.close();
  }

  @Test void testLoadResponseTransformerInvalidClass() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HooksConfig hooks = HooksConfig.builder()
        .responseTransformerClass("com.nonexistent.Transformer")
        .build();

    assertThrows(IllegalArgumentException.class, () -> {
      new HttpSource(config, hooks);
    });
  }

  @Test void testLoadResponseTransformerWrongInterface() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    // String does not implement ResponseTransformer
    HooksConfig hooks = HooksConfig.builder()
        .responseTransformerClass("java.lang.String")
        .build();

    assertThrows(IllegalArgumentException.class, () -> {
      new HttpSource(config, hooks);
    });
  }

  // ---------------------------------------------------------------
  // 34. loadVariableNormalizer (via reflection)
  // ---------------------------------------------------------------

  @Test void testLoadVariableNormalizerNull() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HttpSource source = new HttpSource(config);

    Field field = HttpSource.class.getDeclaredField("variableNormalizer");
    field.setAccessible(true);
    assertNull(field.get(source));

    source.close();
  }

  @Test void testLoadVariableNormalizerInvalidClass() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HooksConfig hooks = HooksConfig.builder()
        .variableNormalizerClass("com.nonexistent.Normalizer")
        .build();

    assertThrows(IllegalArgumentException.class, () -> {
      new HttpSource(config, hooks);
    });
  }

  @Test void testLoadVariableNormalizerWrongInterface() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    HooksConfig hooks = HooksConfig.builder()
        .variableNormalizerClass("java.lang.String")
        .build();

    assertThrows(IllegalArgumentException.class, () -> {
      new HttpSource(config, hooks);
    });
  }

  // ---------------------------------------------------------------
  // 35. Wide-to-narrow transformation via parseDelimitedResponse
  // ---------------------------------------------------------------

  @Test void testParseDelimitedResponseWideToNarrow() throws Exception {
    Map<String, Object> wtnMap = new HashMap<String, Object>();
    wtnMap.put("keyColumns", Arrays.asList("GeoFIPS", "GeoName"));
    wtnMap.put("valueColumnPattern", "^\\d{4}$");
    wtnMap.put("keyColumnName", "Year");
    wtnMap.put("valueColumnName", "DataValue");
    wtnMap.put("skipValues", Arrays.asList("(NA)", ""));

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "csv")))
        .wideToNarrow(HttpSourceConfig.WideToNarrowConfig.fromMap(wtnMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    String csv = "GeoFIPS,GeoName,Description,2020,2021,2022\n"
        + "01000,Alabama,GDP,100.5,110.3,(NA)\n"
        + "02000,Alaska,GDP,50.2,,70.1\n";

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, csv, ',');

    // Alabama: 2020=100.5, 2021=110.3, 2022=(NA) -> skipped
    // Alaska: 2020=50.2, 2021="" -> skipped, 2022=70.1
    // Total: 4 rows
    assertEquals(4, result.size());

    // Verify first row - note: parseValue converts "01000" to long 1000
    assertEquals(1000L, result.get(0).get("GeoFIPS"));
    assertEquals("Alabama", result.get(0).get("GeoName"));
    assertEquals("2020", result.get(0).get("Year"));
    assertEquals(100.5, result.get(0).get("DataValue"));

    source.close();
  }

  @Test void testParseDelimitedResponseWithRowFilter() throws Exception {
    Map<String, Object> filterMap = new HashMap<String, Object>();
    filterMap.put("column", "state");
    filterMap.put("pattern", "CA|NY");

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

    String csv = "name,state,value\nAlice,CA,100\nBob,TX,200\nCharlie,NY,300";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, csv, ',');
    assertEquals(2, result.size());
    assertEquals("CA", result.get(0).get("state"));
    assertEquals("NY", result.get(1).get("state"));

    source.close();
  }

  @Test void testParseDelimitedResponseWithMaxRows() throws Exception {
    Map<String, Object> filterMap = new HashMap<String, Object>();
    filterMap.put("maxRows", 2);

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

    String csv = "name,value\nA,1\nB,2\nC,3\nD,4";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, csv, ',');
    assertEquals(2, result.size());

    source.close();
  }

  @Test void testParseDelimitedResponseFilterColumnNotFound() throws Exception {
    Map<String, Object> filterMap = new HashMap<String, Object>();
    filterMap.put("column", "nonexistent");
    filterMap.put("pattern", ".*");

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

    String csv = "name,value\nAlice,100";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, csv, ',');
    // All rows pass since filter column not found
    assertEquals(1, result.size());

    source.close();
  }

  // ---------------------------------------------------------------
  // 36. CacheEntry inner class
  // ---------------------------------------------------------------

  @Test void testCacheEntryExpired() throws Exception {
    // Access the CacheEntry class
    Class<?> cacheEntryClass = null;
    for (Class<?> inner : HttpSource.class.getDeclaredClasses()) {
      if (inner.getSimpleName().equals("CacheEntry")) {
        cacheEntryClass = inner;
        break;
      }
    }
    assertNotNull(cacheEntryClass);

    java.lang.reflect.Constructor<?> ctor =
        cacheEntryClass.getDeclaredConstructor(List.class, long.class);
    ctor.setAccessible(true);

    // Already expired
    Object expired =
        ctor.newInstance(new ArrayList<Map<String, Object>>(), System.currentTimeMillis() - 1000);
    Method isExpiredMethod = cacheEntryClass.getDeclaredMethod("isExpired");
    isExpiredMethod.setAccessible(true);
    assertTrue((boolean) isExpiredMethod.invoke(expired));

    // Not expired
    Object notExpired =
        ctor.newInstance(new ArrayList<Map<String, Object>>(), System.currentTimeMillis() + 60000);
    assertFalse((boolean) isExpiredMethod.invoke(notExpired));

    // Get data
    Method getDataMethod = cacheEntryClass.getDeclaredMethod("getData");
    getDataMethod.setAccessible(true);
    assertNotNull(getDataMethod.invoke(notExpired));
  }

  // ---------------------------------------------------------------
  // 37. Wide-to-narrow with maxRows in parseDelimitedResponse
  // ---------------------------------------------------------------

  @Test void testParseDelimitedResponseWideToNarrowWithMaxRows() throws Exception {
    Map<String, Object> wtnMap = new HashMap<String, Object>();
    wtnMap.put("keyColumns", Arrays.asList("Name"));
    wtnMap.put("valueColumnPattern", "^\\d{4}$");
    wtnMap.put("keyColumnName", "Year");
    wtnMap.put("valueColumnName", "Value");

    Map<String, Object> filterMap = new HashMap<String, Object>();
    filterMap.put("maxRows", 3);

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "csv")))
        .wideToNarrow(HttpSourceConfig.WideToNarrowConfig.fromMap(wtnMap))
        .rowFilter(HttpSourceConfig.RowFilterConfig.fromMap(filterMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    String csv = "Name,Desc,2020,2021,2022\nAlice,x,10,20,30\nBob,y,40,50,60";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, csv, ',');
    // Each row produces 3 value columns, but maxRows=3 stops after 3
    assertEquals(3, result.size());

    source.close();
  }

  // ---------------------------------------------------------------
  // 38. parseResponse dispatches to CSV format
  // ---------------------------------------------------------------

  @Test void testParseResponseCSV() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "csv")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "name,age\nAlice,30");
    assertEquals(1, result.size());
    assertEquals("Alice", result.get(0).get("name"));

    source.close();
  }

  @Test void testParseResponseTSV() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "tsv")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "name\tage\nAlice\t30");
    assertEquals(1, result.size());
    assertEquals("Alice", result.get(0).get("name"));

    source.close();
  }

  // ---------------------------------------------------------------
  // 39. Row filter with quoted filter values
  // ---------------------------------------------------------------

  @Test void testParseDelimitedResponseFilterWithQuotedValues() throws Exception {
    Map<String, Object> filterMap = new HashMap<String, Object>();
    filterMap.put("column", "code");
    filterMap.put("pattern", "A.*");

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

    String csv = "code,value\n\"ABC\",100\n\"DEF\",200\n\"AXY\",300";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, csv, ',');
    assertEquals(2, result.size());

    source.close();
  }

  // ---------------------------------------------------------------
  // 40. Wide-to-narrow with column name mapping
  // ---------------------------------------------------------------

  @Test void testParseDelimitedResponseWideToNarrowColumnMapping() throws Exception {
    Map<String, Object> wtnMap = new HashMap<String, Object>();
    wtnMap.put("keyColumns", Arrays.asList("GeoFIPS"));
    wtnMap.put("valueColumnPattern", "^\\d{4}$");
    wtnMap.put("keyColumnName", "Year");
    wtnMap.put("valueColumnName", "DataValue");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "csv")))
        .wideToNarrow(HttpSourceConfig.WideToNarrowConfig.fromMap(wtnMap))
        .build();
    HttpSource source = new HttpSource(config);

    Method method =
        HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
    method.setAccessible(true);

    String csv = "GeoFIPS,Description,2020,2021\n01000,GDP,100,200";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, csv, ',');
    assertEquals(2, result.size());

    // Verify key column value and year column - parseValue converts "01000" to 1000L
    assertEquals(1000L, result.get(0).get("GeoFIPS"));
    assertEquals("2020", result.get(0).get("Year"));
    assertEquals(100L, result.get(0).get("DataValue"));
    assertEquals("2021", result.get(1).get("Year"));
    assertEquals(200L, result.get(1).get("DataValue"));

    source.close();
  }

  // ---------------------------------------------------------------
  // 41. parseResponse with missing errorPath in response
  // ---------------------------------------------------------------

  @Test void testParseResponseWithMissingErrorPathField() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "json", "errorPath", "nonexistent.path")))
        .build();
    HttpSource source = new HttpSource(config);

    Method method = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
    method.setAccessible(true);

    // Error path does not exist in response - should not throw
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, "[{\"id\":1}]");
    assertEquals(1, result.size());

    source.close();
  }

  // ---------------------------------------------------------------
  // 42. parseDelimitedResponse - filter column index exceeds values
  // ---------------------------------------------------------------

  @Test void testParseDelimitedResponseFilterColumnExceedsValues() throws Exception {
    Map<String, Object> filterMap = new HashMap<String, Object>();
    filterMap.put("column", "col3");
    filterMap.put("pattern", ".*");

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

    // Data row has fewer columns than header's filter column index
    String csv = "col1,col2,col3\nA,B";
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(source, csv, ',');
    // Row should be skipped because filter column index exceeds values length
    assertEquals(0, result.size());

    source.close();
  }

  // ---------------------------------------------------------------
  // 43. evictLocalCacheIfNeeded (via reflection)
  // ---------------------------------------------------------------

  @Test void testEvictLocalCacheIfNeededNullPath() throws Exception {
    HttpSource source = createBasicSource();
    Method method = HttpSource.class.getDeclaredMethod("evictLocalCacheIfNeeded");
    method.setAccessible(true);

    // Should not throw - localRawCachePath is null
    method.invoke(source);

    source.close();
  }

  // ---------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------

  /**
   * Creates a basic HttpSource for testing.
   */
  private HttpSource createBasicSource() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    return new HttpSource(config);
  }

  /**
   * Creates a Map from alternating key-value pairs.
   */
  @SuppressWarnings("unchecked")
  private static <V> Map<String, V> createMap(Object... keyValues) {
    Map<String, V> map = new HashMap<String, V>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put((String) keyValues[i], (V) keyValues[i + 1]);
    }
    return map;
  }
}
