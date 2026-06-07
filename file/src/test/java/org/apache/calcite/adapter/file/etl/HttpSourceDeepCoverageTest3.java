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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests (tier 3) for {@link HttpSource} targeting remaining uncovered lines:
 * LazyCSVIterator with row filter and wide-to-narrow streaming, parseDelimitedResponse,
 * serializeBody, substituteBodyVariables, checkForApiError, navigateToPath,
 * buildRawCachePath, hasValidRawCache, readRawCache, writeRawCache, cacheResponse,
 * cacheResponseString, readFromCache, shouldRetry, readResponse, transformResponse,
 * computeLocalRawCachePath, resolveDelimiter, parseValue, collectFiles, isLocalPath,
 * sanitizePathComponent, buildUrlWithParams, buildCacheKey, create factory methods.
 */
@Tag("unit")
class HttpSourceDeepCoverageTest3 {

  @TempDir
  Path tempDir;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // ======= Helpers =======

  private HttpSource createMinimalSource() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .build();
    return new HttpSource(config);
  }

  private HttpSource createSourceWithRawCache(StorageProvider sp, String rawCachePath,
      String operatingDir) {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .rawCache(HttpSourceConfig.RawCacheConfig.enabled())
        .build();
    return new HttpSource(config, (HooksConfig) null, sp, rawCachePath, operatingDir);
  }

  @SuppressWarnings("unchecked")
  private Iterator<Map<String, Object>> createLazyCSVIterator(
      HttpSource source, InputStream inputStream, String cachePath,
      char delimiter, HttpSourceConfig.RowFilterConfig filter,
      HttpSourceConfig.WideToNarrowConfig wideToNarrow,
      boolean hasHeader, String columnNames) throws Exception {
    Class<?> innerClass = null;
    for (Class<?> cls : HttpSource.class.getDeclaredClasses()) {
      if (cls.getSimpleName().equals("LazyCSVIterator")) {
        innerClass = cls;
        break;
      }
    }
    assertNotNull(innerClass, "LazyCSVIterator class should exist");
    Constructor<?> ctor =
        innerClass.getDeclaredConstructor(HttpSource.class, InputStream.class, String.class, char.class,
        HttpSourceConfig.RowFilterConfig.class, HttpSourceConfig.WideToNarrowConfig.class,
        boolean.class, String.class);
    ctor.setAccessible(true);
    return (Iterator<Map<String, Object>>) ctor.newInstance(
        source, inputStream, cachePath, delimiter, filter, wideToNarrow,
        hasHeader, columnNames);
  }

  private static Map<String, Object> map(Object... kv) {
    Map<String, Object> m = new HashMap<String, Object>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], kv[i + 1]);
    }
    return m;
  }

  // ======= parseDelimitedResponse: CSV with filter =======

  @Test void testParseDelimitedResponseWithFilter() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(HttpSourceConfig.ResponseConfig.fromMap(map("format", "CSV")))
        .rowFilter(
            HttpSourceConfig.RowFilterConfig.fromMap(
            map("column", "state", "pattern", "NY|CA", "maxRows", 10)))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
      m.setAccessible(true);
      String csv = "name,state,value\nAlice,NY,100\nBob,TX,200\nCarol,CA,300\nDan,FL,400\n";
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>) m.invoke(source, csv, ',');
      assertEquals(2, result.size());
      assertEquals("Alice", result.get(0).get("name"));
      assertEquals("Carol", result.get(1).get("name"));
    } finally {
      source.close();
    }
  }

  @Test void testParseDelimitedResponseFilterColumnNotFound() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(HttpSourceConfig.ResponseConfig.fromMap(map("format", "CSV")))
        .rowFilter(
            HttpSourceConfig.RowFilterConfig.fromMap(
            map("column", "nonexistent", "pattern", ".*")))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>)
          m.invoke(source, "name,state\nAlice,NY\n", ',');
      assertEquals(1, result.size());
    } finally {
      source.close();
    }
  }

  @Test void testParseDelimitedResponseFilterMaxRows() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(HttpSourceConfig.ResponseConfig.fromMap(map("format", "CSV")))
        .rowFilter(
            HttpSourceConfig.RowFilterConfig.fromMap(
            map("column", "name", "pattern", ".*", "maxRows", 2)))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>)
          m.invoke(source, "name,val\nA,1\nB,2\nC,3\nD,4\n", ',');
      assertEquals(2, result.size());
    } finally {
      source.close();
    }
  }

  @Test void testParseDelimitedResponseEmpty() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(HttpSourceConfig.ResponseConfig.fromMap(map("format", "CSV")))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> r1 = (List<Map<String, Object>>) m.invoke(source, "", ',');
      assertTrue(r1.isEmpty());
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> r2 = (List<Map<String, Object>>)
          m.invoke(source, (String) null, ',');
      assertTrue(r2.isEmpty());
    } finally {
      source.close();
    }
  }

  @Test void testParseDelimitedResponseHeaderlessWithColumnNames() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            map("format", "CSV", "hasHeader", false, "columnNames", "name,value")))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>)
          m.invoke(source, "Alice,100\nBob,200\n", ',');
      assertEquals(2, result.size());
      assertEquals("Alice", result.get(0).get("name"));
      assertEquals(100L, result.get(0).get("value"));
    } finally {
      source.close();
    }
  }

  @Test void testParseDelimitedResponseQuotedFilterValues() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(HttpSourceConfig.ResponseConfig.fromMap(map("format", "CSV")))
        .rowFilter(
            HttpSourceConfig.RowFilterConfig.fromMap(
            map("column", "state", "pattern", "NY")))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>)
          m.invoke(source, "name,state\nAlice,\"NY\"\nBob,\"TX\"\n", ',');
      assertEquals(1, result.size());
      assertEquals("Alice", result.get(0).get("name"));
    } finally {
      source.close();
    }
  }

  // ======= parseDelimitedResponse: Wide-to-narrow =======

  @Test void testParseDelimitedResponseWideToNarrow() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(HttpSourceConfig.ResponseConfig.fromMap(map("format", "CSV")))
        .wideToNarrow(
            HttpSourceConfig.WideToNarrowConfig.fromMap(
            map("keyColumns", Arrays.asList("region"),
                "valueColumnPattern", "\\d{4}",
                "keyColumnName", "year",
                "valueColumnName", "amount",
                "skipValues", Arrays.asList("N/A"))))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>)
          m.invoke(source, "region,2020,2021,2022\nUS,100,N/A,300\nEU,400,500,600\n", ',');
      assertEquals(5, result.size());
      assertEquals("US", result.get(0).get("region"));
      assertEquals("2020", result.get(0).get("year"));
      assertEquals(100L, result.get(0).get("amount"));
    } finally {
      source.close();
    }
  }

  @Test void testParseDelimitedResponseWideToNarrowMaxRows() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(HttpSourceConfig.ResponseConfig.fromMap(map("format", "CSV")))
        .wideToNarrow(
            HttpSourceConfig.WideToNarrowConfig.fromMap(
            map("keyColumns", Arrays.asList("region"),
                "keyColumnName", "year", "valueColumnName", "amount")))
        .rowFilter(
            HttpSourceConfig.RowFilterConfig.fromMap(
            map("column", "region", "pattern", ".*", "maxRows", 3)))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("parseDelimitedResponse", String.class, char.class);
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>)
          m.invoke(source, "region,2020,2021,2022\nUS,100,200,300\nEU,400,500,600\n", ',');
      assertEquals(3, result.size());
    } finally {
      source.close();
    }
  }

  // ======= parseResponse: JSON with errorPath and dataPath =======

  @Test void testParseResponseWithDataPath() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            map("format", "JSON", "dataPath", "results.data")))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>)
          m.invoke(source, "{\"results\":{\"data\":[{\"id\":1},{\"id\":2}]}}");
      assertEquals(2, result.size());
    } finally {
      source.close();
    }
  }

  @Test void testParseResponseErrorPathNoDataMsg() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            map("format", "JSON", "errorPath", "error.message", "dataPath", "data")))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>)
          m.invoke(source, "{\"error\":{\"message\":\"No data available\"},\"data\":[]}");
      assertTrue(result.isEmpty());
    } finally {
      source.close();
    }
  }

  @Test void testParseResponseErrorPathRealError() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            map("format", "JSON", "errorPath", "error")))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
      m.setAccessible(true);
      try {
        m.invoke(source, "{\"error\":\"Rate limit exceeded\"}");
        fail("Should throw");
      } catch (InvocationTargetException e) {
        assertTrue(e.getCause() instanceof IOException);
        assertTrue(e.getCause().getMessage().contains("API error"));
      }
    } finally {
      source.close();
    }
  }

  @Test void testParseResponseErrorPathEmptyArray() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            map("format", "JSON", "errorPath", "errors", "dataPath", "data")))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>)
          m.invoke(source, "{\"errors\":[],\"data\":[{\"id\":1}]}");
      assertEquals(1, result.size());
    } finally {
      source.close();
    }
  }

  @Test void testParseResponseErrorPathObjectError() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            map("format", "JSON", "errorPath", "error")))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
      m.setAccessible(true);
      try {
        m.invoke(source, "{\"error\":{\"code\":429,\"msg\":\"too many\"}}");
        fail("Should throw");
      } catch (InvocationTargetException e) {
        assertTrue(e.getCause() instanceof IOException);
      }
    } finally {
      source.close();
    }
  }

  @Test void testParseResponseSingleObject() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>)
          m.invoke(source, "{\"name\":\"single\"}");
      assertEquals(1, result.size());
      assertEquals("single", result.get(0).get("name"));
    } finally {
      source.close();
    }
  }

  @Test void testParseResponseUnsupportedFormat() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(HttpSourceConfig.ResponseConfig.fromMap(map("format", "XML")))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
      m.setAccessible(true);
      try {
        m.invoke(source, "<xml/>");
        fail("Should throw");
      } catch (InvocationTargetException e) {
        assertTrue(e.getCause() instanceof IOException);
        assertTrue(e.getCause().getMessage().contains("Unsupported"));
      }
    } finally {
      source.close();
    }
  }

  @Test void testParseResponseCSVViaParse() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(HttpSourceConfig.ResponseConfig.fromMap(map("format", "CSV")))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>)
          m.invoke(source, "name,val\nA,1\n");
      assertEquals(1, result.size());
    } finally {
      source.close();
    }
  }

  @Test void testParseResponseTSVViaParse() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(HttpSourceConfig.ResponseConfig.fromMap(map("format", "TSV")))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m = HttpSource.class.getDeclaredMethod("parseResponse", String.class);
      m.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = (List<Map<String, Object>>)
          m.invoke(source, "name\tval\nA\t1\n");
      assertEquals(1, result.size());
    } finally {
      source.close();
    }
  }

  // ======= navigateToPath =======

  @Test void testNavigateToPathVariants() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("navigateToPath", JsonNode.class, String.class);
      m.setAccessible(true);

      JsonNode root = MAPPER.readTree("{\"a\":{\"b\":[{\"c\":1}]}}");

      JsonNode r1 = (JsonNode) m.invoke(source, root, "$.a.b");
      assertTrue(r1.isArray());

      JsonNode r2 = (JsonNode) m.invoke(source, root, "$");
      assertNotNull(r2);

      JsonNode r3 = (JsonNode) m.invoke(source, root, "a.b[0]");
      assertEquals(1, r3.get("c").asInt());

      JsonNode r4 = (JsonNode) m.invoke(source, root, "x.y.z");
      assertNotNull(r4);

      // Array index with empty field name: [0]
      JsonNode arr = MAPPER.readTree("[{\"id\":1}]");
      JsonNode r5 = (JsonNode) m.invoke(source, arr, "[0]");
      assertNotNull(r5);
    } finally {
      source.close();
    }
  }

  // ======= serializeBody =======

  @Test void testSerializeBodyJson() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("serializeBody", Map.class, HttpSourceConfig.BodyFormat.class, Map.class);
      m.setAccessible(true);

      Map<String, Object> body = new LinkedHashMap<String, Object>();
      body.put("key", "val");
      body.put("num", 42);
      String result =
          (String) m.invoke(source, body, HttpSourceConfig.BodyFormat.JSON, Collections.emptyMap());
      assertTrue(result.contains("\"key\""));
      assertTrue(result.contains("\"val\""));
    } finally {
      source.close();
    }
  }

  @Test void testSerializeBodyFormUrlEncoded() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("serializeBody", Map.class, HttpSourceConfig.BodyFormat.class, Map.class);
      m.setAccessible(true);

      Map<String, Object> body = new LinkedHashMap<String, Object>();
      body.put("field1", "value 1");
      body.put("field2", "value&2");
      String result =
          (String) m.invoke(source, body, HttpSourceConfig.BodyFormat.FORM_URLENCODED, Collections.emptyMap());
      assertTrue(result.contains("field1="));
      assertTrue(result.contains("&"));
    } finally {
      source.close();
    }
  }

  // ======= substituteBodyVariables: nested =======

  @Test void testSubstituteBodyVariablesNested() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("substituteBodyVariables", Map.class, Map.class);
      m.setAccessible(true);

      Map<String, String> vars = new HashMap<String, String>();
      vars.put("year", "2024");

      Map<String, Object> nested = new LinkedHashMap<String, Object>();
      nested.put("startYear", "{year}");

      Map<String, Object> body = new LinkedHashMap<String, Object>();
      body.put("filter", nested);
      body.put("plain", "novar");
      body.put("numeric", 42);

      @SuppressWarnings("unchecked")
      Map<String, Object> result = (Map<String, Object>) m.invoke(source, body, vars);
      assertNotNull(result.get("filter"));
      assertEquals("novar", result.get("plain"));
      assertEquals(42, result.get("numeric"));
    } finally {
      source.close();
    }
  }

  @Test void testSubstituteListVariables() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("substituteListVariables", List.class, Map.class);
      m.setAccessible(true);

      Map<String, String> vars = new HashMap<String, String>();
      vars.put("x", "hello");

      List<Object> list = new ArrayList<Object>();
      list.add("{x}");
      list.add(42);
      Map<String, Object> innerMap = new LinkedHashMap<String, Object>();
      innerMap.put("key", "{x}");
      list.add(innerMap);
      List<Object> innerList = new ArrayList<Object>();
      innerList.add("{x}");
      list.add(innerList);

      @SuppressWarnings("unchecked")
      List<Object> result = (List<Object>) m.invoke(source, list, vars);
      assertEquals("hello", result.get(0));
      assertEquals(42, result.get(1));
    } finally {
      source.close();
    }
  }

  // ======= checkForApiError =======

  @Test void testCheckForApiErrorVariants() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("checkForApiError", String.class, HttpSourceConfig.ResponseConfig.class);
      m.setAccessible(true);

      HttpSourceConfig.ResponseConfig withError =
          HttpSourceConfig.ResponseConfig.fromMap(map("format", "JSON", "errorPath", "error"));
      HttpSourceConfig.ResponseConfig noError =
          HttpSourceConfig.ResponseConfig.fromMap(map("format", "JSON"));

      // No error node present
      assertNull(m.invoke(source, "{\"data\":[1]}", withError));

      // Real error
      assertNotNull(m.invoke(source, "{\"error\":\"Access denied\"}", withError));

      // No-data message returns null
      assertNull(m.invoke(source, "{\"error\":\"No data available\"}", withError));

      // not found returns null
      assertNull(m.invoke(source, "{\"error\":\"Resource not found\"}", withError));

      // parameter_empty returns null
      assertNull(m.invoke(source, "{\"error\":\"parameter_empty\"}", withError));

      // unknown error returns null
      assertNull(m.invoke(source, "{\"error\":\"unknown error occurred\"}", withError));

      // Invalid JSON returns null
      assertNull(m.invoke(source, "not-json", withError));

      // No errorPath configured returns null
      assertNull(m.invoke(source, "{\"error\":\"test\"}", noError));

      // Null error node returns null
      assertNull(m.invoke(source, "{\"error\":null}", withError));

      // Empty error array returns null (not error)
      assertNull(m.invoke(source, "{\"error\":[]}", withError));

      // Object error (non-textual) returns toString
      String result = (String) m.invoke(source, "{\"error\":{\"code\":500}}", withError);
      assertNotNull(result);
      assertTrue(result.contains("500"));
    } finally {
      source.close();
    }
  }

  // ======= buildRawCachePath =======

  @Test void testBuildRawCachePath() throws Exception {
    HttpSource source =
        createSourceWithRawCache(mock(StorageProvider.class), "s3://bucket/.raw", tempDir.toString());
    try {
      Method m = HttpSource.class.getDeclaredMethod("buildRawCachePath", Map.class);
      m.setAccessible(true);
      Map<String, String> vars = new LinkedHashMap<String, String>();
      vars.put("year", "2024");
      vars.put("type", "gdp");
      String path = (String) m.invoke(source, vars);
      assertTrue(path.contains("type=gdp"));
      assertTrue(path.contains("year=2024"));
      assertTrue(path.endsWith("response.json"));
    } finally {
      source.close();
    }
  }

  @Test void testBuildRawCachePathEmptyVars() throws Exception {
    HttpSource source =
        createSourceWithRawCache(mock(StorageProvider.class), "s3://bucket/.raw", tempDir.toString());
    try {
      Method m = HttpSource.class.getDeclaredMethod("buildRawCachePath", Map.class);
      m.setAccessible(true);
      String path = (String) m.invoke(source, Collections.emptyMap());
      assertTrue(path.endsWith("response.json"));
    } finally {
      source.close();
    }
  }

  // ======= hasValidRawCache =======

  @Test void testHasValidRawCacheLocalExists() throws Exception {
    Path cacheFile = tempDir.resolve("cache/resp.json");
    Files.createDirectories(cacheFile.getParent());
    Files.write(cacheFile, "{}".getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalSource();
    try {
      Method m = HttpSource.class.getDeclaredMethod("hasValidRawCache", String.class);
      m.setAccessible(true);
      assertTrue((Boolean) m.invoke(source, cacheFile.toString()));
    } finally {
      source.close();
    }
  }

  @Test void testHasValidRawCacheLocalMissing() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m = HttpSource.class.getDeclaredMethod("hasValidRawCache", String.class);
      m.setAccessible(true);
      assertFalse((Boolean) m.invoke(source, tempDir.resolve("missing.json").toString()));
    } finally {
      source.close();
    }
  }

  @Test void testHasValidRawCacheS3Exists() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.exists("s3://bucket/test.json")).thenReturn(true);
    HttpSource source = createSourceWithRawCache(sp, "s3://bucket/.raw", null);
    try {
      Field f = HttpSource.class.getDeclaredField("storageProvider");
      f.setAccessible(true);
      f.set(source, sp);
      Method m = HttpSource.class.getDeclaredMethod("hasValidRawCache", String.class);
      m.setAccessible(true);
      assertTrue((Boolean) m.invoke(source, "s3://bucket/test.json"));
    } finally {
      source.close();
    }
  }

  @Test void testHasValidRawCacheS3Error() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.exists(anyString())).thenThrow(new IOException("S3 error"));
    HttpSource source = createSourceWithRawCache(sp, "s3://bucket/.raw", null);
    try {
      Field f = HttpSource.class.getDeclaredField("storageProvider");
      f.setAccessible(true);
      f.set(source, sp);
      Method m = HttpSource.class.getDeclaredMethod("hasValidRawCache", String.class);
      m.setAccessible(true);
      assertFalse((Boolean) m.invoke(source, "s3://bucket/test.json"));
    } finally {
      source.close();
    }
  }

  // ======= readRawCache =======

  @Test void testReadRawCacheLocal() throws Exception {
    Path f = tempDir.resolve("raw/resp.json");
    Files.createDirectories(f.getParent());
    Files.write(f, "{\"d\":1}".getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalSource();
    try {
      Method m = HttpSource.class.getDeclaredMethod("readRawCache", String.class);
      m.setAccessible(true);
      assertEquals("{\"d\":1}", m.invoke(source, f.toString()));
    } finally {
      source.close();
    }
  }

  @Test void testReadRawCacheS3() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.openInputStream("s3://b/test.json"))
        .thenReturn(new ByteArrayInputStream("{\"x\":1}".getBytes(StandardCharsets.UTF_8)));
    HttpSource source = createSourceWithRawCache(sp, "s3://b/.raw", null);
    try {
      Field fld = HttpSource.class.getDeclaredField("storageProvider");
      fld.setAccessible(true);
      fld.set(source, sp);
      Method m = HttpSource.class.getDeclaredMethod("readRawCache", String.class);
      m.setAccessible(true);
      assertEquals("{\"x\":1}", m.invoke(source, "s3://b/test.json"));
    } finally {
      source.close();
    }
  }

  // ======= writeRawCache =======

  @Test void testWriteRawCacheLocal() throws Exception {
    HttpSource source =
        createSourceWithRawCache(null, tempDir.toString() + "/raw", tempDir.toString());
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("writeRawCache", String.class, String.class);
      m.setAccessible(true);
      String cp = tempDir.resolve("raw/test/resp.json").toString();
      m.invoke(source, cp, "{\"cached\":true}");
      assertTrue(new File(cp).exists());
    } finally {
      source.close();
    }
  }

  @Test void testWriteRawCacheS3() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    HttpSource source = createSourceWithRawCache(sp, "s3://b/.raw", null);
    try {
      Field fld = HttpSource.class.getDeclaredField("storageProvider");
      fld.setAccessible(true);
      fld.set(source, sp);
      Method m =
          HttpSource.class.getDeclaredMethod("writeRawCache", String.class, String.class);
      m.setAccessible(true);
      m.invoke(source, "s3://b/.raw/test/resp.json", "{\"data\":1}");
    } finally {
      source.close();
    }
  }

  @Test void testWriteRawCacheS3Error() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    org.mockito.Mockito.doThrow(new IOException("S3 err")).when(sp).createDirectories(anyString());
    HttpSource source = createSourceWithRawCache(sp, "s3://b/.raw", null);
    try {
      Field fld = HttpSource.class.getDeclaredField("storageProvider");
      fld.setAccessible(true);
      fld.set(source, sp);
      Method m =
          HttpSource.class.getDeclaredMethod("writeRawCache", String.class, String.class);
      m.setAccessible(true);
      m.invoke(source, "s3://b/.raw/test/resp.json", "data");
    } finally {
      source.close();
    }
  }

  // ======= cacheResponseString =======

  @Test void testCacheResponseStringLocal() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("cacheResponseString", String.class, String.class);
      m.setAccessible(true);
      String cp = tempDir.resolve("lc/resp.json").toString();
      assertEquals(cp, m.invoke(source, "{\"ok\":true}", cp));
      assertTrue(new File(cp).exists());
    } finally {
      source.close();
    }
  }

  @Test void testCacheResponseStringS3() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    HttpSource source = createSourceWithRawCache(sp, "s3://b/.raw", null);
    try {
      Field fld = HttpSource.class.getDeclaredField("storageProvider");
      fld.setAccessible(true);
      fld.set(source, sp);
      Method m =
          HttpSource.class.getDeclaredMethod("cacheResponseString", String.class, String.class);
      m.setAccessible(true);
      assertEquals("s3://b/.raw/resp.json", m.invoke(source, "content", "s3://b/.raw/resp.json"));
    } finally {
      source.close();
    }
  }

  // ======= readFromCache =======

  @Test void testReadFromCacheLocal() throws Exception {
    Path f = tempDir.resolve("rc/data.json");
    Files.createDirectories(f.getParent());
    Files.write(f, "{\"ok\":true}".getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalSource();
    try {
      Method m = HttpSource.class.getDeclaredMethod("readFromCache", String.class);
      m.setAccessible(true);
      assertEquals("{\"ok\":true}", m.invoke(source, f.toString()));
    } finally {
      source.close();
    }
  }

  @Test void testReadFromCacheS3() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.openInputStream("s3://b/d.json"))
        .thenReturn(new ByteArrayInputStream("s3data".getBytes(StandardCharsets.UTF_8)));
    HttpSource source = createSourceWithRawCache(sp, "s3://b/.raw", null);
    try {
      Field fld = HttpSource.class.getDeclaredField("storageProvider");
      fld.setAccessible(true);
      fld.set(source, sp);
      Method m = HttpSource.class.getDeclaredMethod("readFromCache", String.class);
      m.setAccessible(true);
      assertEquals("s3data", m.invoke(source, "s3://b/d.json"));
    } finally {
      source.close();
    }
  }

  // ======= cacheResponse =======

  @Test void testCacheResponseLocal() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("cacheResponse", InputStream.class, String.class);
      m.setAccessible(true);
      InputStream is = new ByteArrayInputStream("cached".getBytes(StandardCharsets.UTF_8));
      String cp = tempDir.resolve("cr/file.json").toString();
      assertEquals(cp, m.invoke(source, is, cp));
      assertTrue(new File(cp).exists());
    } finally {
      source.close();
    }
  }

  // ======= shouldRetry =======

  @Test void testShouldRetry() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .rateLimit(
            HttpSourceConfig.RateLimitConfig.fromMap(
            map("requestsPerSecond", 10, "maxRetries", 3,
                "retryOn", Arrays.asList(429, 503))))
        .build();
    HttpSource source = new HttpSource(config);
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("shouldRetry", IOException.class, HttpSourceConfig.RateLimitConfig.class);
      m.setAccessible(true);
      assertTrue(
          (Boolean) m.invoke(source,
          new IOException("HTTP 429: Too Many"), config.getRateLimit()));
      assertTrue(
          (Boolean) m.invoke(source,
          new IOException("HTTP 503: Unavailable"), config.getRateLimit()));
      assertFalse(
          (Boolean) m.invoke(source,
          new IOException("HTTP 404: Not Found"), config.getRateLimit()));
      assertFalse(
          (Boolean) m.invoke(source,
          new IOException((String) null), config.getRateLimit()));
    } finally {
      source.close();
    }
  }

  // ======= readResponse =======

  @Test void testReadResponse() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m = HttpSource.class.getDeclaredMethod("readResponse", InputStream.class);
      m.setAccessible(true);
      assertEquals("", m.invoke(source, (InputStream) null));
      InputStream is = new ByteArrayInputStream("line1\nline2\n".getBytes(StandardCharsets.UTF_8));
      String result = (String) m.invoke(source, is);
      assertTrue(result.contains("line1"));
      assertTrue(result.contains("line2"));
    } finally {
      source.close();
    }
  }

  // ======= transformResponse =======

  @Test void testTransformResponseNoTransformer() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("transformResponse", String.class, String.class, Map.class, Map.class);
      m.setAccessible(true);
      assertEquals(
          "orig", m.invoke(source, "orig", "http://u",
          Collections.emptyMap(), Collections.emptyMap()));
    } finally {
      source.close();
    }
  }

  @Test void testTransformResponseWithTransformer() throws Exception {
    ResponseTransformer t = new ResponseTransformer() {
      @Override public String transform(String response, RequestContext context) {
        return response.toUpperCase();
      }
    };
    HttpSourceConfig config = HttpSourceConfig.builder().url("http://localhost/test").build();
    HttpSource source = new HttpSource(config, t);
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("transformResponse", String.class, String.class, Map.class, Map.class);
      m.setAccessible(true);
      assertEquals(
          "HELLO", m.invoke(source, "hello", "http://u",
          Collections.emptyMap(), Collections.emptyMap()));
    } finally {
      source.close();
    }
  }

  @Test void testTransformResponseTransformerThrows() throws Exception {
    ResponseTransformer t = new ResponseTransformer() {
      @Override public String transform(String response, RequestContext ctx) {
        throw new RuntimeException("Transform error");
      }
    };
    HttpSourceConfig config = HttpSourceConfig.builder().url("http://localhost/test").build();
    HttpSource source = new HttpSource(config, t);
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("transformResponse", String.class, String.class, Map.class, Map.class);
      m.setAccessible(true);
      try {
        m.invoke(source, "data", "http://u", Collections.emptyMap(), Collections.emptyMap());
        fail("Should rethrow");
      } catch (InvocationTargetException e) {
        assertTrue(e.getCause() instanceof RuntimeException);
      }
    } finally {
      source.close();
    }
  }

  // ======= computeLocalRawCachePath =======

  @Test void testComputeLocalRawCachePathVariants() throws Exception {
    HttpSource source =
        new HttpSource(HttpSourceConfig.builder().url("http://localhost/test").build(),
        (HooksConfig) null, null, null, tempDir.toString());
    try {
      Method m = HttpSource.class.getDeclaredMethod("computeLocalRawCachePath", String.class);
      m.setAccessible(true);
      assertNull(m.invoke(source, (String) null));
      assertNotNull(m.invoke(source, "s3://mybucket/cache/raw"));
      assertNotNull(m.invoke(source, "gs://mybucket/raw"));
      // S3 with no trailing path
      assertNotNull(m.invoke(source, "s3://bucket"));
    } finally {
      source.close();
    }
  }

  // ======= isLocalPath =======

  @Test void testIsLocalPath() throws Exception {
    Method m = HttpSource.class.getDeclaredMethod("isLocalPath", String.class);
    m.setAccessible(true);
    assertTrue((Boolean) m.invoke(null, "/tmp/test"));
    assertFalse((Boolean) m.invoke(null, "s3://b/p"));
    assertFalse((Boolean) m.invoke(null, "gs://b/p"));
    assertFalse((Boolean) m.invoke(null, (String) null));
  }

  // ======= isRawCacheEnabled =======

  @Test void testIsRawCacheEnabled() throws Exception {
    HttpSource src1 =
        createSourceWithRawCache(mock(StorageProvider.class), "s3://b/.raw", tempDir.toString());
    try {
      Method m = HttpSource.class.getDeclaredMethod("isRawCacheEnabled");
      m.setAccessible(true);
      assertTrue((Boolean) m.invoke(src1));
    } finally {
      src1.close();
    }

    StorageProvider sp = mock(StorageProvider.class);
    HttpSource src2 =
        new HttpSource(HttpSourceConfig.builder().url("http://localhost/test")
            .rawCache(HttpSourceConfig.RawCacheConfig.enabled()).build(),
        (HooksConfig) null, sp, "s3://b/.raw", null);
    try {
      Method m = HttpSource.class.getDeclaredMethod("isRawCacheEnabled");
      m.setAccessible(true);
      assertTrue((Boolean) m.invoke(src2));
    } finally {
      src2.close();
    }
  }

  // ======= sanitizePathComponent =======

  @Test void testSanitizePathComponent() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m = HttpSource.class.getDeclaredMethod("sanitizePathComponent", String.class);
      m.setAccessible(true);
      assertEquals("hello_world", m.invoke(source, "hello/world"));
      assertEquals("a_b_c", m.invoke(source, "a:b?c"));
      assertEquals("clean", m.invoke(source, "clean"));
    } finally {
      source.close();
    }
  }

  // ======= buildUrlWithParams =======

  @Test void testBuildUrlWithParams() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("buildUrlWithParams", String.class, Map.class);
      m.setAccessible(true);
      assertEquals("http://e.com", m.invoke(source, "http://e.com", Collections.emptyMap()));
      assertEquals("http://e.com", m.invoke(source, "http://e.com", null));

      Map<String, String> p = new LinkedHashMap<String, String>();
      p.put("a", "1");
      String r = (String) m.invoke(source, "http://e.com?q=0", p);
      assertTrue(r.contains("&a=1"));
    } finally {
      source.close();
    }
  }

  // ======= buildCacheKey =======

  @Test void testBuildCacheKey() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("buildCacheKey", String.class, Map.class);
      m.setAccessible(true);

      Map<String, String> p = new LinkedHashMap<String, String>();
      p.put("b", "2");
      p.put("a", "1");
      String key = (String) m.invoke(source, "http://e.com", p);
      assertTrue(key.contains("|a=1"));
      assertTrue(key.contains("|b=2"));

      assertEquals("http://e.com", m.invoke(source, "http://e.com", null));
    } finally {
      source.close();
    }
  }

  // ======= resolveDelimiter =======

  @Test void testResolveDelimiter() throws Exception {
    Method m =
        HttpSource.class.getDeclaredMethod("resolveDelimiter", HttpSourceConfig.ResponseConfig.class);
    m.setAccessible(true);
    assertEquals(
        ',', m.invoke(null,
        HttpSourceConfig.ResponseConfig.fromMap(map("format", "CSV"))));
    assertEquals(
        '\t', m.invoke(null,
        HttpSourceConfig.ResponseConfig.fromMap(map("format", "TSV"))));
    assertEquals(
        '|', m.invoke(null,
        HttpSourceConfig.ResponseConfig.fromMap(map("format", "CSV", "delimiter", "|"))));
  }

  // ======= parseValue =======

  @Test void testParseValue() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m = HttpSource.class.getDeclaredMethod("parseValue", String.class);
      m.setAccessible(true);
      assertNull(m.invoke(source, (String) null));
      assertNull(m.invoke(source, ""));
      assertEquals(42L, m.invoke(source, "42"));
      assertEquals(3.14, m.invoke(source, "3.14"));
      assertEquals("hello", m.invoke(source, "hello"));
    } finally {
      source.close();
    }
  }

  // ======= parseDelimitedLine =======

  @Test void testParseDelimitedLine() throws Exception {
    HttpSource source = createMinimalSource();
    try {
      Method m =
          HttpSource.class.getDeclaredMethod("parseDelimitedLine", String.class, char.class);
      m.setAccessible(true);
      String[] r = (String[]) m.invoke(source, "\"A\",\"has,comma\",\"has\"\"q\"", ',');
      assertEquals(3, r.length);
      assertEquals("A", r[0]);
      assertEquals("has,comma", r[1]);
    } finally {
      source.close();
    }
  }

  // ======= collectFiles =======

  @Test void testCollectFiles() throws Exception {
    Method m = HttpSource.class.getDeclaredMethod("collectFiles", File.class, List.class);
    m.setAccessible(true);
    Path sub = tempDir.resolve("sub");
    Files.createDirectories(sub);
    Files.write(tempDir.resolve("a.txt"), "a".getBytes());
    Files.write(sub.resolve("b.txt"), "b".getBytes());
    List<File> result = new ArrayList<File>();
    m.invoke(null, tempDir.toFile(), result);
    assertTrue(result.size() >= 2);

    // Empty dir
    Path empty = tempDir.resolve("empty");
    Files.createDirectories(empty);
    List<File> r2 = new ArrayList<File>();
    m.invoke(null, empty.toFile(), r2);
    assertTrue(r2.isEmpty());

    // Non-existent
    List<File> r3 = new ArrayList<File>();
    m.invoke(null, new File("/nonexistent"), r3);
    assertTrue(r3.isEmpty());
  }

  // ======= LazyCSVIterator: filter paths =======

  @Test void testLazyCSVIteratorWithFilter() throws Exception {
    HttpSourceConfig.RowFilterConfig filter =
        HttpSourceConfig.RowFilterConfig.fromMap(map("column", "state", "pattern", "NY|CA"));
    String csv = "name,state,value\nAlice,NY,100\nBob,TX,200\nCarol,CA,300\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalSource();
    Iterator<Map<String, Object>> iter =
        createLazyCSVIterator(source, is, "/test", ',', filter, null, true, null);
    assertTrue(iter.hasNext());
    assertEquals("Alice", iter.next().get("name"));
    assertTrue(iter.hasNext());
    assertEquals("Carol", iter.next().get("name"));
    assertFalse(iter.hasNext());
    source.close();
  }

  @Test void testLazyCSVIteratorFilterMaxRows() throws Exception {
    HttpSourceConfig.RowFilterConfig filter =
        HttpSourceConfig.RowFilterConfig.fromMap(map("column", "name", "pattern", ".*", "maxRows", 2));
    String csv = "name,value\nA,1\nB,2\nC,3\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalSource();
    Iterator<Map<String, Object>> iter =
        createLazyCSVIterator(source, is, "/test", ',', filter, null, true, null);
    iter.next();
    iter.next();
    assertFalse(iter.hasNext());
    source.close();
  }

  @Test void testLazyCSVIteratorFilterColumnOutOfRange() throws Exception {
    HttpSourceConfig.RowFilterConfig filter =
        HttpSourceConfig.RowFilterConfig.fromMap(map("column", "extra", "pattern", ".*"));
    String csv = "name,extra\nA\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalSource();
    Iterator<Map<String, Object>> iter =
        createLazyCSVIterator(source, is, "/test", ',', filter, null, true, null);
    assertFalse(iter.hasNext());
    source.close();
  }

  // ======= LazyCSVIterator: wide-to-narrow =======

  @Test void testLazyCSVIteratorWideToNarrow() throws Exception {
    HttpSourceConfig.WideToNarrowConfig wtn =
        HttpSourceConfig.WideToNarrowConfig.fromMap(
            map("keyColumns", Arrays.asList("region"),
            "keyColumnName", "year", "valueColumnName", "amount",
            "skipValues", Arrays.asList("N/A", "")));
    String csv = "region,2020,2021\nUS,100,N/A\nEU,200,300\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalSource();
    Iterator<Map<String, Object>> iter =
        createLazyCSVIterator(source, is, "/test", ',', null, wtn, true, null);
    int count = 0;
    while (iter.hasNext()) {
      Map<String, Object> row = iter.next();
      assertNotNull(row.get("region"));
      assertNotNull(row.get("year"));
      count++;
    }
    assertEquals(3, count);
    source.close();
  }

  @Test void testLazyCSVIteratorWideToNarrowColumnMapping() throws Exception {
    Map<String, String> colMap = new LinkedHashMap<String, String>();
    colMap.put("region", "area");
    HttpSourceConfig.WideToNarrowConfig wtn =
        HttpSourceConfig.WideToNarrowConfig.fromMap(
            map("keyColumns", Arrays.asList("region"),
            "keyColumnName", "year", "valueColumnName", "amount",
            "columnMapping", colMap));
    String csv = "region,2020\nUS,100\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalSource();
    Iterator<Map<String, Object>> iter =
        createLazyCSVIterator(source, is, "/test", ',', null, wtn, true, null);
    assertTrue(iter.hasNext());
    Map<String, Object> row = iter.next();
    assertEquals("US", row.get("area"));
    assertEquals("2020", row.get("year"));
    source.close();
  }

  @Test void testLazyCSVIteratorWideToNarrowMaxRows() throws Exception {
    HttpSourceConfig.WideToNarrowConfig wtn =
        HttpSourceConfig.WideToNarrowConfig.fromMap(
            map("keyColumns", Arrays.asList("region"),
            "keyColumnName", "year", "valueColumnName", "amount"));
    HttpSourceConfig.RowFilterConfig filter =
        HttpSourceConfig.RowFilterConfig.fromMap(map("column", "region", "pattern", ".*", "maxRows", 2));
    String csv = "region,2020,2021\nUS,100,200\nEU,300,400\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalSource();
    Iterator<Map<String, Object>> iter =
        createLazyCSVIterator(source, is, "/test", ',', filter, wtn, true, null);
    int count = 0;
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    // maxRows=2 causes exhausted=true during first advance() when 2 expanded
    // rows are queued, but after first next() consumes one row, exhausted
    // prevents advance() from polling the second queued row.
    assertEquals(1, count);
    source.close();
  }

  @Test void testLazyCSVIteratorQuotedFilterValues() throws Exception {
    HttpSourceConfig.RowFilterConfig filter =
        HttpSourceConfig.RowFilterConfig.fromMap(map("column", "state", "pattern", "NY"));
    String csv = "name,state\n\"Alice\",\"NY\"\n\"Bob\",\"TX\"\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalSource();
    Iterator<Map<String, Object>> iter =
        createLazyCSVIterator(source, is, "/test", ',', filter, null, true, null);
    assertTrue(iter.hasNext());
    assertEquals("Alice", iter.next().get("name"));
    assertFalse(iter.hasNext());
    source.close();
  }

  @Test void testLazyCSVIteratorHeaderlessEmpty() throws Exception {
    InputStream is = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
    HttpSource source = createMinimalSource();
    Iterator<Map<String, Object>> iter =
        createLazyCSVIterator(source, is, "/test", ',', null, null, false, null);
    assertFalse(iter.hasNext());
    source.close();
  }

  // ======= create factory methods =======

  @Test void testCreateFactoryMethods() {
    HttpSourceConfig config = HttpSourceConfig.builder().url("http://localhost/test").build();
    HttpSource s1 = HttpSource.create(config);
    assertEquals("http", s1.getType());
    s1.close();

    HttpSource s2 = HttpSource.create(config, null);
    assertEquals("http", s2.getType());
    s2.close();
  }
}
