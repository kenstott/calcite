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
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Line coverage tests for {@link HttpSource} targeting remaining uncovered paths:
 * local raw cache reading, env var substitution, POST form-urlencoded body,
 * ZIP response extraction, pagination, retry logic, and more.
 */
@Tag("unit")
class HttpSourceLineCoverageTest {

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

  private HttpSource createSourceWithRawCache(String rawCachePath, String operatingDir) {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .rawCache(HttpSourceConfig.RawCacheConfig.enabled())
        .build();
    return new HttpSource(config, (HooksConfig) null, null, rawCachePath, operatingDir);
  }

  private Object invokePrivate(Object target, String methodName, Class<?>[] paramTypes,
      Object... args) throws Exception {
    Method m = target.getClass().getDeclaredMethod(methodName, paramTypes);
    m.setAccessible(true);
    return m.invoke(target, args);
  }

  private Object getPrivateField(Object target, String fieldName) throws Exception {
    Field f = target.getClass().getDeclaredField(fieldName);
    f.setAccessible(true);
    return f.get(target);
  }

  // ======= computeLocalRawCachePath tests =======

  @Test void testComputeLocalRawCachePathWithNullRawCachePath() throws Exception {
    HttpSource source = createSourceWithRawCache(null, null);
    Object localPath = getPrivateField(source, "localRawCachePath");
    assertNull(localPath, "localRawCachePath should be null when rawCachePath is null");
    source.close();
  }

  @Test void testComputeLocalRawCachePathWithS3Path() throws Exception {
    HttpSource source = createSourceWithRawCache("s3://my-bucket/raw/data", null);
    Object localPath = getPrivateField(source, "localRawCachePath");
    assertNotNull(localPath, "localRawCachePath should be computed for S3 paths");
    assertTrue(localPath.toString().contains("raw/data"),
        "Should contain the S3 suffix: " + localPath);
    source.close();
  }

  @Test void testComputeLocalRawCachePathWithGsPath() throws Exception {
    HttpSource source = createSourceWithRawCache("gs://my-bucket/raw/data", null);
    Object localPath = getPrivateField(source, "localRawCachePath");
    assertNotNull(localPath, "localRawCachePath should be computed for GCS paths");
    assertTrue(localPath.toString().contains("raw/data"),
        "Should contain the GCS suffix: " + localPath);
    source.close();
  }

  @Test void testComputeLocalRawCachePathWithOperatingDirectory() throws Exception {
    String opDir = tempDir.resolve("opdir").toString();
    HttpSource source = createSourceWithRawCache("s3://bucket/path", opDir);
    Object localPath = getPrivateField(source, "localRawCachePath");
    assertNotNull(localPath);
    assertTrue(localPath.toString().contains(opDir + "/cache/raw"),
        "Should use operating directory for cache base: " + localPath);
    source.close();
  }

  @Test void testComputeLocalRawCachePathWithEmptySuffix() throws Exception {
    HttpSource source = createSourceWithRawCache("s3://bucket", null);
    Object localPath = getPrivateField(source, "localRawCachePath");
    assertNotNull(localPath);
    source.close();
  }

  @Test void testComputeLocalRawCachePathWithLocalPath() throws Exception {
    String localDir = tempDir.resolve("local-cache").toString();
    HttpSource source = createSourceWithRawCache(localDir, null);
    Object localPath = getPrivateField(source, "localRawCachePath");
    assertNotNull(localPath);
    source.close();
  }

  // ======= buildRawCachePath tests =======

  @Test void testBuildRawCachePath() throws Exception {
    String opDir = tempDir.resolve("op").toString();
    HttpSource source = createSourceWithRawCache("s3://bucket/raw", opDir);

    Map<String, String> vars = new LinkedHashMap<String, String>();
    vars.put("year", "2024");
    vars.put("region", "NORTH");

    String result =
        (String) invokePrivate(source, "buildRawCachePath", new Class[]{Map.class}, vars);
    assertNotNull(result);
    assertTrue(result.contains("region=NORTH"), "Should contain region partition: " + result);
    assertTrue(result.contains("year=2024"), "Should contain year partition: " + result);
    assertTrue(result.endsWith("response.json"), "Should end with response.json: " + result);
    source.close();
  }

  @Test void testBuildRawCachePathWithEmptyVariables() throws Exception {
    String opDir = tempDir.resolve("op2").toString();
    HttpSource source = createSourceWithRawCache("s3://bucket/raw", opDir);

    Map<String, String> vars = new HashMap<String, String>();
    String result =
        (String) invokePrivate(source, "buildRawCachePath", new Class[]{Map.class}, vars);
    assertNotNull(result);
    assertTrue(result.endsWith("response.json"));
    source.close();
  }

  // ======= hasValidRawCache tests =======

  @Test void testHasValidRawCacheLocalFileExists() throws Exception {
    File cacheFile = tempDir.resolve("cached-response.json").toFile();
    Files.write(cacheFile.toPath(), "{}".getBytes(StandardCharsets.UTF_8));

    HttpSource source = createMinimalSource();
    Boolean result =
        (Boolean) invokePrivate(source, "hasValidRawCache", new Class[]{String.class}, cacheFile.getAbsolutePath());
    assertTrue(result, "Should return true when local file exists");
    source.close();
  }

  @Test void testHasValidRawCacheLocalFileNotExists() throws Exception {
    HttpSource source = createMinimalSource();
    Boolean result =
        (Boolean) invokePrivate(source, "hasValidRawCache", new Class[]{String.class}, tempDir.resolve("nonexistent.json").toString());
    assertFalse(result, "Should return false when local file does not exist");
    source.close();
  }

  // ======= readRawCache tests =======

  @Test void testReadRawCacheLocalFile() throws Exception {
    String content = "{\"data\": [1, 2, 3]}";
    File cacheFile = tempDir.resolve("raw-cache.json").toFile();
    Files.write(cacheFile.toPath(), content.getBytes(StandardCharsets.UTF_8));

    HttpSource source = createMinimalSource();
    String result =
        (String) invokePrivate(source, "readRawCache", new Class[]{String.class}, cacheFile.getAbsolutePath());
    assertEquals(content, result);
    source.close();
  }

  // ======= writeRawCache tests =======

  @Test void testWriteRawCacheLocalFile() throws Exception {
    String content = "{\"key\": \"value\"}";
    File cacheFile = tempDir.resolve("subdir/write-cache.json").toFile();

    HttpSource source = createMinimalSource();
    invokePrivate(source, "writeRawCache",
        new Class[]{String.class, String.class},
        cacheFile.getAbsolutePath(), content);

    assertTrue(cacheFile.exists(), "Cache file should be created");
    String written = new String(Files.readAllBytes(cacheFile.toPath()), StandardCharsets.UTF_8);
    assertEquals(content, written);
    source.close();
  }

  // ======= cacheResponseString for local path =======

  @Test void testCacheResponseStringLocalPath() throws Exception {
    String content = "{\"result\": true}";
    String path = tempDir.resolve("cache-str/resp.json").toString();

    HttpSource source = createMinimalSource();
    String result =
        (String) invokePrivate(source, "cacheResponseString", new Class[]{String.class, String.class}, content, path);
    assertEquals(path, result);

    String written = new String(Files.readAllBytes(new File(path).toPath()), StandardCharsets.UTF_8);
    assertEquals(content, written);
    source.close();
  }

  // ======= cacheResponse for local path =======

  @Test void testCacheResponseLocalPath() throws Exception {
    byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);
    InputStream input = new ByteArrayInputStream(data);
    String path = tempDir.resolve("cache-resp/data.bin").toString();

    HttpSource source = createMinimalSource();
    String result =
        (String) invokePrivate(source, "cacheResponse", new Class[]{InputStream.class, String.class}, input, path);
    assertEquals(path, result);

    byte[] written = Files.readAllBytes(new File(path).toPath());
    assertEquals("hello world", new String(written, StandardCharsets.UTF_8));
    source.close();
  }

  // ======= readFromCache for local path =======

  @Test void testReadFromCacheLocalPath() throws Exception {
    String content = "{\"cached\": true}";
    File file = tempDir.resolve("read-cache.json").toFile();
    Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));

    HttpSource source = createMinimalSource();
    String result =
        (String) invokePrivate(source, "readFromCache", new Class[]{String.class}, file.getAbsolutePath());
    assertEquals(content, result);
    source.close();
  }

  // ======= isRawCacheEnabled tests =======

  @Test void testIsRawCacheEnabledWithLocalRawCachePath() throws Exception {
    HttpSource source = createSourceWithRawCache("s3://bucket/raw", tempDir.toString());
    Boolean result = (Boolean) invokePrivate(source, "isRawCacheEnabled", new Class[]{});
    assertTrue(result, "Should be enabled when localRawCachePath is set and rawCache is enabled");
    source.close();
  }

  @Test void testIsRawCacheEnabledWithoutConfig() throws Exception {
    HttpSource source = createMinimalSource();
    Boolean result = (Boolean) invokePrivate(source, "isRawCacheEnabled", new Class[]{});
    assertFalse(result, "Should be disabled when rawCache is not configured");
    source.close();
  }

  // ======= isLocalPath tests =======

  @Test void testIsLocalPath() throws Exception {
    Method m = HttpSource.class.getDeclaredMethod("isLocalPath", String.class);
    m.setAccessible(true);
    assertTrue((Boolean) m.invoke(null, "/tmp/file.json"));
    assertTrue((Boolean) m.invoke(null, "C:\\file.json"));
    assertFalse((Boolean) m.invoke(null, "s3://bucket/file.json"));
    assertFalse((Boolean) m.invoke(null, "gs://bucket/file.json"));
    assertFalse((Boolean) m.invoke(null, (Object) null));
  }

  // ======= sanitizePathComponent tests =======

  @Test void testSanitizePathComponent() throws Exception {
    HttpSource source = createMinimalSource();
    String result =
        (String) invokePrivate(source, "sanitizePathComponent", new Class[]{String.class}, "hello/world:test*file");
    assertEquals("hello_world_test_file", result);
    source.close();
  }

  // ======= buildUrlWithParams tests =======

  @Test void testBuildUrlWithParamsEmpty() throws Exception {
    HttpSource source = createMinimalSource();
    String result =
        (String) invokePrivate(source, "buildUrlWithParams", new Class[]{String.class, Map.class},
        "http://example.com/api", Collections.emptyMap());
    assertEquals("http://example.com/api", result);
    source.close();
  }

  @Test void testBuildUrlWithParams() throws Exception {
    HttpSource source = createMinimalSource();
    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("key", "value");
    params.put("year", "2024");
    String result =
        (String) invokePrivate(source, "buildUrlWithParams", new Class[]{String.class, Map.class},
        "http://example.com/api", params);
    assertTrue(result.contains("key=value"));
    assertTrue(result.contains("year=2024"));
    assertTrue(result.contains("?"));
    source.close();
  }

  @Test void testBuildUrlWithParamsExistingQuery() throws Exception {
    HttpSource source = createMinimalSource();
    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("extra", "param");
    String result =
        (String) invokePrivate(source, "buildUrlWithParams", new Class[]{String.class, Map.class},
        "http://example.com/api?existing=true", params);
    assertTrue(result.contains("&extra=param"),
        "Should append with & when URL already has query: " + result);
    source.close();
  }

  // ======= buildCacheKey tests =======

  @Test void testBuildCacheKey() throws Exception {
    HttpSource source = createMinimalSource();
    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("b", "2");
    params.put("a", "1");
    String result =
        (String) invokePrivate(source, "buildCacheKey", new Class[]{String.class, Map.class},
        "http://api.com/data", params);
    assertNotNull(result);
    assertTrue(result.indexOf("a=1") < result.indexOf("b=2"),
        "Keys should be sorted: " + result);
    source.close();
  }

  @Test void testBuildCacheKeyNoParams() throws Exception {
    HttpSource source = createMinimalSource();
    String result =
        (String) invokePrivate(source, "buildCacheKey", new Class[]{String.class, Map.class},
        "http://api.com/data", (Object) null);
    assertEquals("http://api.com/data", result);
    source.close();
  }

  // ======= shouldRetry tests =======

  @Test void testShouldRetryWith429() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .rateLimit(
            HttpSourceConfig.RateLimitConfig.fromMap(
            createMap("maxRetries", 3, "retryOn", Arrays.asList(429, 503))))
        .build();
    HttpSource source = new HttpSource(config);

    HttpSourceConfig.RateLimitConfig rateLimit = config.getRateLimit();
    IOException e429 = new IOException("HTTP 429: Too Many Requests");
    Boolean result =
        (Boolean) invokePrivate(source, "shouldRetry", new Class[]{IOException.class, HttpSourceConfig.RateLimitConfig.class},
        e429, rateLimit);
    assertTrue(result, "Should retry on 429");
    source.close();
  }

  @Test void testShouldRetryWith503() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .rateLimit(
            HttpSourceConfig.RateLimitConfig.fromMap(
            createMap("maxRetries", 3, "retryOn", Arrays.asList(429, 503))))
        .build();
    HttpSource source = new HttpSource(config);

    HttpSourceConfig.RateLimitConfig rateLimit = config.getRateLimit();
    IOException e503 = new IOException("HTTP 503: Service Unavailable");
    Boolean result =
        (Boolean) invokePrivate(source, "shouldRetry", new Class[]{IOException.class, HttpSourceConfig.RateLimitConfig.class},
        e503, rateLimit);
    assertTrue(result, "Should retry on 503");
    source.close();
  }

  @Test void testShouldRetryWithNonRetryable() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .rateLimit(
            HttpSourceConfig.RateLimitConfig.fromMap(
            createMap("maxRetries", 3, "retryOn", Arrays.asList(429, 503))))
        .build();
    HttpSource source = new HttpSource(config);

    HttpSourceConfig.RateLimitConfig rateLimit = config.getRateLimit();
    IOException e404 = new IOException("HTTP 404: Not Found");
    Boolean result =
        (Boolean) invokePrivate(source, "shouldRetry", new Class[]{IOException.class, HttpSourceConfig.RateLimitConfig.class},
        e404, rateLimit);
    assertFalse(result, "Should not retry on 404");
    source.close();
  }

  @Test void testShouldRetryWithNullMessage() throws Exception {
    HttpSource source = createMinimalSource();
    HttpSourceConfig.RateLimitConfig rateLimit =
        HttpSourceConfig.RateLimitConfig.defaults();
    IOException eNull = new IOException((String) null);
    Boolean result =
        (Boolean) invokePrivate(source, "shouldRetry", new Class[]{IOException.class, HttpSourceConfig.RateLimitConfig.class},
        eNull, rateLimit);
    assertFalse(result, "Should not retry when message is null");
    source.close();
  }

  // ======= readResponse tests =======

  @Test void testReadResponseWithNull() throws Exception {
    HttpSource source = createMinimalSource();
    String result =
        (String) invokePrivate(source, "readResponse", new Class[]{InputStream.class}, (Object) null);
    assertEquals("", result);
    source.close();
  }

  @Test void testReadResponseWithContent() throws Exception {
    HttpSource source = createMinimalSource();
    byte[] data = "line1\nline2\n".getBytes(StandardCharsets.UTF_8);
    InputStream input = new ByteArrayInputStream(data);
    String result =
        (String) invokePrivate(source, "readResponse", new Class[]{InputStream.class}, input);
    assertTrue(result.contains("line1"));
    assertTrue(result.contains("line2"));
    source.close();
  }

  // ======= serializeBody tests (form-urlencoded) =======

  @Test void testSerializeBodyFormUrlencoded() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .method(HttpSourceConfig.HttpMethod.POST)
        .bodyFormat(HttpSourceConfig.BodyFormat.FORM_URLENCODED)
        .build();
    HttpSource source = new HttpSource(config);

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("username", "admin");
    body.put("password", "secret");
    Map<String, String> vars = Collections.emptyMap();

    String result =
        (String) invokePrivate(source, "serializeBody", new Class[]{Map.class, HttpSourceConfig.BodyFormat.class, Map.class},
        body, HttpSourceConfig.BodyFormat.FORM_URLENCODED, vars);

    assertTrue(result.contains("username=admin"), "Should contain username: " + result);
    assertTrue(result.contains("password=secret"), "Should contain password: " + result);
    assertTrue(result.contains("&"), "Should use & separator: " + result);
    source.close();
  }

  @Test void testSerializeBodyJson() throws Exception {
    HttpSource source = createMinimalSource();

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("key", "value");
    body.put("count", 42);
    Map<String, String> vars = Collections.emptyMap();

    String result =
        (String) invokePrivate(source, "serializeBody", new Class[]{Map.class, HttpSourceConfig.BodyFormat.class, Map.class},
        body, HttpSourceConfig.BodyFormat.JSON, vars);

    assertTrue(result.contains("\"key\""), "Should be JSON: " + result);
    assertTrue(result.contains("\"value\""), "Should contain value: " + result);
    source.close();
  }

  // ======= substituteBodyVariables tests =======

  @Test void testSubstituteBodyVariablesWithNestedMap() throws Exception {
    HttpSource source = createMinimalSource();

    Map<String, Object> nested = new LinkedHashMap<String, Object>();
    nested.put("nestedKey", "{year}");

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("top", "{year}");
    body.put("inner", nested);
    body.put("number", 42);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) invokePrivate(source, "substituteBodyVariables",
        new Class[]{Map.class, Map.class}, body, vars);

    assertEquals("2024", result.get("top"));
    assertEquals(42, result.get("number"));
    @SuppressWarnings("unchecked")
    Map<String, Object> innerResult = (Map<String, Object>) result.get("inner");
    assertEquals("2024", innerResult.get("nestedKey"));
    source.close();
  }

  @Test void testSubstituteBodyVariablesWithList() throws Exception {
    HttpSource source = createMinimalSource();

    List<Object> items = new ArrayList<Object>();
    items.add("{year}");
    items.add(100);

    Map<String, Object> body = new LinkedHashMap<String, Object>();
    body.put("items", items);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) invokePrivate(source, "substituteBodyVariables",
        new Class[]{Map.class, Map.class}, body, vars);

    @SuppressWarnings("unchecked")
    List<Object> resultItems = (List<Object>) result.get("items");
    assertEquals("2024", resultItems.get(0));
    assertEquals(100, resultItems.get(1));
    source.close();
  }

  // ======= substituteListVariables with nested lists and maps =======

  @Test void testSubstituteListVariablesNested() throws Exception {
    HttpSource source = createMinimalSource();

    Map<String, Object> innerMap = new LinkedHashMap<String, Object>();
    innerMap.put("k", "{year}");

    List<Object> innerList = new ArrayList<Object>();
    innerList.add("{year}");

    List<Object> outerList = new ArrayList<Object>();
    outerList.add("{year}");
    outerList.add(innerMap);
    outerList.add(innerList);
    outerList.add(99);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2025");

    @SuppressWarnings("unchecked")
    List<Object> result =
        (List<Object>) invokePrivate(source, "substituteListVariables", new Class[]{List.class, Map.class}, outerList, vars);

    assertEquals("2025", result.get(0));
    @SuppressWarnings("unchecked")
    Map<String, Object> resMap = (Map<String, Object>) result.get(1);
    assertEquals("2025", resMap.get("k"));
    @SuppressWarnings("unchecked")
    List<Object> resList = (List<Object>) result.get(2);
    assertEquals("2025", resList.get(0));
    assertEquals(99, result.get(3));
    source.close();
  }

  // ======= navigateToPath tests =======

  @Test void testNavigateToPathWithJsonPath() throws Exception {
    HttpSource source = createMinimalSource();
    String json = "{\"results\":{\"data\":[{\"id\":1}]}}";
    com.fasterxml.jackson.databind.JsonNode root = MAPPER.readTree(json);

    com.fasterxml.jackson.databind.JsonNode result =
        (com.fasterxml.jackson.databind.JsonNode) invokePrivate(source, "navigateToPath",
            new Class[]{com.fasterxml.jackson.databind.JsonNode.class, String.class},
            root, "$.results.data");
    assertTrue(result.isArray());
    assertEquals(1, result.size());
    source.close();
  }

  @Test void testNavigateToPathWithArrayIndex() throws Exception {
    HttpSource source = createMinimalSource();
    String json = "{\"data\":[{\"id\":1},{\"id\":2}]}";
    com.fasterxml.jackson.databind.JsonNode root = MAPPER.readTree(json);

    com.fasterxml.jackson.databind.JsonNode result =
        (com.fasterxml.jackson.databind.JsonNode) invokePrivate(source, "navigateToPath",
            new Class[]{com.fasterxml.jackson.databind.JsonNode.class, String.class},
            root, "data[0]");
    assertTrue(result.isObject());
    assertEquals(1, result.get("id").asInt());
    source.close();
  }

  @Test void testNavigateToPathMissingNode() throws Exception {
    HttpSource source = createMinimalSource();
    String json = "{\"data\":{}}";
    com.fasterxml.jackson.databind.JsonNode root = MAPPER.readTree(json);

    com.fasterxml.jackson.databind.JsonNode result =
        (com.fasterxml.jackson.databind.JsonNode) invokePrivate(source, "navigateToPath",
            new Class[]{com.fasterxml.jackson.databind.JsonNode.class, String.class},
            root, "missing.path");
    assertNotNull(result, "Should return empty array for missing path");
    source.close();
  }

  @Test void testNavigateToPathDollarOnly() throws Exception {
    HttpSource source = createMinimalSource();
    String json = "[{\"id\":1}]";
    com.fasterxml.jackson.databind.JsonNode root = MAPPER.readTree(json);

    com.fasterxml.jackson.databind.JsonNode result =
        (com.fasterxml.jackson.databind.JsonNode) invokePrivate(source, "navigateToPath",
            new Class[]{com.fasterxml.jackson.databind.JsonNode.class, String.class},
            root, "$");
    assertNotNull(result);
    source.close();
  }

  // ======= checkForApiError tests =======

  @Test void testCheckForApiErrorWithNoError() throws Exception {
    HttpSource source = createMinimalSource();
    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "JSON", "errorPath", "error"));

    String result =
        (String) invokePrivate(source, "checkForApiError", new Class[]{String.class, HttpSourceConfig.ResponseConfig.class},
        "{\"data\": []}", respConfig);
    assertNull(result, "Should return null when no error");
    source.close();
  }

  @Test void testCheckForApiErrorWithRealError() throws Exception {
    HttpSource source = createMinimalSource();
    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "JSON", "errorPath", "error"));

    String result =
        (String) invokePrivate(source, "checkForApiError", new Class[]{String.class, HttpSourceConfig.ResponseConfig.class},
        "{\"error\": \"Rate limit exceeded\"}", respConfig);
    assertNotNull(result, "Should return error message");
    assertTrue(result.contains("Rate limit exceeded"));
    source.close();
  }

  @Test void testCheckForApiErrorWithNoDataError() throws Exception {
    HttpSource source = createMinimalSource();
    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "JSON", "errorPath", "error"));

    String result =
        (String) invokePrivate(source, "checkForApiError", new Class[]{String.class, HttpSourceConfig.ResponseConfig.class},
        "{\"error\": \"no data available\"}", respConfig);
    assertNull(result, "Should return null for 'no data' type errors");
    source.close();
  }

  @Test void testCheckForApiErrorWithEmptyArray() throws Exception {
    HttpSource source = createMinimalSource();
    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "JSON", "errorPath", "errors"));

    String result =
        (String) invokePrivate(source, "checkForApiError", new Class[]{String.class, HttpSourceConfig.ResponseConfig.class},
        "{\"errors\": []}", respConfig);
    assertNull(result, "Should return null for empty error array");
    source.close();
  }

  @Test void testCheckForApiErrorWithNullErrorNode() throws Exception {
    HttpSource source = createMinimalSource();
    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "JSON", "errorPath", "error"));

    String result =
        (String) invokePrivate(source, "checkForApiError", new Class[]{String.class, HttpSourceConfig.ResponseConfig.class},
        "{\"error\": null}", respConfig);
    assertNull(result, "Should return null for null error node");
    source.close();
  }

  @Test void testCheckForApiErrorWithNonJsonContent() throws Exception {
    HttpSource source = createMinimalSource();
    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "JSON", "errorPath", "error"));

    String result =
        (String) invokePrivate(source, "checkForApiError", new Class[]{String.class, HttpSourceConfig.ResponseConfig.class},
        "not json at all", respConfig);
    assertNull(result, "Should return null for unparseable content");
    source.close();
  }

  @Test void testCheckForApiErrorWithObjectError() throws Exception {
    HttpSource source = createMinimalSource();
    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "JSON", "errorPath", "error"));

    String result =
        (String) invokePrivate(source, "checkForApiError", new Class[]{String.class, HttpSourceConfig.ResponseConfig.class},
        "{\"error\": {\"code\": 500, \"message\": \"Server error\"}}", respConfig);
    assertNotNull(result, "Should return error for object error node");
    source.close();
  }

  @Test void testCheckForApiErrorNoErrorPath() throws Exception {
    HttpSource source = createMinimalSource();
    HttpSourceConfig.ResponseConfig respConfig =
        HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "JSON"));

    String result =
        (String) invokePrivate(source, "checkForApiError", new Class[]{String.class, HttpSourceConfig.ResponseConfig.class},
        "{\"data\": [1,2,3]}", respConfig);
    assertNull(result, "Should return null when no errorPath configured");
    source.close();
  }

  // ======= parseResponse tests (JSON) =======

  @Test void testParseResponseJsonArray() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "JSON")))
        .build();
    HttpSource source = new HttpSource(config);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) invokePrivate(source, "parseResponse", new Class[]{String.class},
        "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]");
    assertEquals(2, result.size());
    assertEquals(1, result.get(0).get("id"));
    assertEquals("Alice", result.get(0).get("name"));
    source.close();
  }

  @Test void testParseResponseJsonSingleObject() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "JSON")))
        .build();
    HttpSource source = new HttpSource(config);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) invokePrivate(source, "parseResponse", new Class[]{String.class},
        "{\"id\":1,\"name\":\"single\"}");
    assertEquals(1, result.size());
    assertEquals("single", result.get(0).get("name"));
    source.close();
  }

  @Test void testParseResponseJsonWithDataPath() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "JSON", "dataPath", "results.items")))
        .build();
    HttpSource source = new HttpSource(config);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) invokePrivate(source, "parseResponse", new Class[]{String.class},
        "{\"results\":{\"items\":[{\"v\":1}]}}");
    assertEquals(1, result.size());
    source.close();
  }

  // ======= parseDelimitedResponse tests =======

  @Test void testParseDelimitedResponseEmpty() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(HttpSourceConfig.ResponseConfig.fromMap(createMap("format", "CSV")))
        .build();
    HttpSource source = new HttpSource(config);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) invokePrivate(source, "parseDelimitedResponse", new Class[]{String.class, char.class},
        "", ',');
    assertTrue(result.isEmpty(), "Empty input should yield empty result");
    source.close();
  }

  @Test void testParseDelimitedResponseNull() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(HttpSourceConfig.ResponseConfig.fromMap(createMap("format", "CSV")))
        .build();
    HttpSource source = new HttpSource(config);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) invokePrivate(source, "parseDelimitedResponse", new Class[]{String.class, char.class},
        (Object) null, ',');
    assertTrue(result.isEmpty());
    source.close();
  }

  // ======= parseValue tests =======

  @Test void testParseValueNull() throws Exception {
    HttpSource source = createMinimalSource();
    Object result =
        invokePrivate(source, "parseValue", new Class[]{String.class}, (Object) null);
    assertNull(result);
    source.close();
  }

  @Test void testParseValueEmpty() throws Exception {
    HttpSource source = createMinimalSource();
    Object result =
        invokePrivate(source, "parseValue", new Class[]{String.class}, "");
    assertNull(result);
    source.close();
  }

  @Test void testParseValueLong() throws Exception {
    HttpSource source = createMinimalSource();
    Object result =
        invokePrivate(source, "parseValue", new Class[]{String.class}, "12345");
    assertEquals(12345L, result);
    source.close();
  }

  @Test void testParseValueDouble() throws Exception {
    HttpSource source = createMinimalSource();
    Object result =
        invokePrivate(source, "parseValue", new Class[]{String.class}, "3.14");
    assertEquals(3.14, result);
    source.close();
  }

  @Test void testParseValueString() throws Exception {
    HttpSource source = createMinimalSource();
    Object result =
        invokePrivate(source, "parseValue", new Class[]{String.class}, "hello");
    assertEquals("hello", result);
    source.close();
  }

  // ======= resolveDelimiter tests =======

  @Test void testResolveDelimiterCSV() throws Exception {
    Method m =
        HttpSource.class.getDeclaredMethod("resolveDelimiter", HttpSourceConfig.ResponseConfig.class);
    m.setAccessible(true);

    HttpSourceConfig.ResponseConfig csvConfig =
        HttpSourceConfig.ResponseConfig.fromMap(createMap("format", "CSV"));
    char result = (Character) m.invoke(null, csvConfig);
    assertEquals(',', result);
  }

  @Test void testResolveDelimiterTSV() throws Exception {
    Method m =
        HttpSource.class.getDeclaredMethod("resolveDelimiter", HttpSourceConfig.ResponseConfig.class);
    m.setAccessible(true);

    HttpSourceConfig.ResponseConfig tsvConfig =
        HttpSourceConfig.ResponseConfig.fromMap(createMap("format", "TSV"));
    char result = (Character) m.invoke(null, tsvConfig);
    assertEquals('\t', result);
  }

  @Test void testResolveDelimiterCustom() throws Exception {
    Method m =
        HttpSource.class.getDeclaredMethod("resolveDelimiter", HttpSourceConfig.ResponseConfig.class);
    m.setAccessible(true);

    HttpSourceConfig.ResponseConfig customConfig =
        HttpSourceConfig.ResponseConfig.fromMap(createMap("format", "CSV", "delimiter", "|"));
    char result = (Character) m.invoke(null, customConfig);
    assertEquals('|', result);
  }

  // ======= collectFiles tests =======

  @Test void testCollectFiles() throws Exception {
    File root = tempDir.resolve("collect-test").toFile();
    root.mkdirs();
    File sub = new File(root, "sub");
    sub.mkdirs();
    Files.write(new File(root, "file1.json").toPath(), "{}".getBytes());
    Files.write(new File(sub, "file2.json").toPath(), "{}".getBytes());

    Method m = HttpSource.class.getDeclaredMethod("collectFiles", File.class, List.class);
    m.setAccessible(true);

    List<File> result = new ArrayList<File>();
    m.invoke(null, root, result);
    assertEquals(2, result.size());
  }

  @Test void testCollectFilesOnNonExistent() throws Exception {
    Method m = HttpSource.class.getDeclaredMethod("collectFiles", File.class, List.class);
    m.setAccessible(true);

    List<File> result = new ArrayList<File>();
    m.invoke(null, new File("/nonexistent/path"), result);
    assertTrue(result.isEmpty());
  }

  // ======= extractFromZip tests =======

  @Test void testExtractFromZipMatching() throws Exception {
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(baos)) {
      ZipEntry entry = new ZipEntry("data.csv");
      zos.putNextEntry(entry);
      zos.write("id,name\n1,Alice\n2,Bob\n".getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();

      ZipEntry entry2 = new ZipEntry("readme.txt");
      zos.putNextEntry(entry2);
      zos.write("This is a readme".getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }

    ByteArrayInputStream zipInput = new ByteArrayInputStream(baos.toByteArray());
    String cachePath = tempDir.resolve("zip-extract/extracted.csv").toString();

    HttpSource source = createMinimalSource();
    String result =
        (String) invokePrivate(source, "extractFromZip", new Class[]{InputStream.class, String.class, String.class},
        zipInput, "*.csv", cachePath);
    assertEquals(cachePath, result);

    String extracted =
        new String(Files.readAllBytes(new File(cachePath).toPath()), StandardCharsets.UTF_8);
    assertTrue(extracted.contains("id,name"));
    assertTrue(extracted.contains("Alice"));
    source.close();
  }

  @Test void testExtractFromZipNoMatch() throws Exception {
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(baos)) {
      ZipEntry entry = new ZipEntry("readme.txt");
      zos.putNextEntry(entry);
      zos.write("readme content".getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }

    ByteArrayInputStream zipInput = new ByteArrayInputStream(baos.toByteArray());
    String cachePath = tempDir.resolve("zip-nomatch/out.csv").toString();

    HttpSource source = createMinimalSource();
    try {
      invokePrivate(source, "extractFromZip",
          new Class[]{InputStream.class, String.class, String.class},
          zipInput, "*.csv", cachePath);
      assertTrue(false, "Should have thrown");
    } catch (Exception e) {
      // Expected - InvocationTargetException wrapping IOException
      assertTrue(e.getCause() instanceof IOException
          || e instanceof IOException);
    }
    source.close();
  }

  // ======= transformResponse tests =======

  @Test void testTransformResponseWithNullTransformer() throws Exception {
    HttpSource source = createMinimalSource();
    String response = "{\"data\": [1]}";
    Map<String, String> params = Collections.emptyMap();
    Map<String, String> vars = Collections.emptyMap();

    String result =
        (String) invokePrivate(source, "transformResponse", new Class[]{String.class, String.class, Map.class, Map.class},
        response, "http://test.com", params, vars);
    assertEquals(response, result, "Should return original when no transformer");
    source.close();
  }

  @Test void testTransformResponseWithTransformer() throws Exception {
    ResponseTransformer transformer = new ResponseTransformer() {
      @Override public String transform(String response, RequestContext context) {
        return response.replace("raw", "transformed");
      }
    };

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .build();
    HttpSource source = new HttpSource(config, transformer);

    Map<String, String> params = Collections.emptyMap();
    Map<String, String> vars = Collections.emptyMap();

    String result =
        (String) invokePrivate(source, "transformResponse", new Class[]{String.class, String.class, Map.class, Map.class},
        "raw data", "http://test.com", params, vars);
    assertEquals("transformed data", result);
    source.close();
  }

  @Test void testTransformResponseWithTransformerThatThrows() throws Exception {
    ResponseTransformer transformer = new ResponseTransformer() {
      @Override public String transform(String response, RequestContext context) {
        throw new RuntimeException("Transform error");
      }
    };

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .build();
    HttpSource source = new HttpSource(config, transformer);

    Map<String, String> params = Collections.emptyMap();
    Map<String, String> vars = Collections.emptyMap();

    try {
      invokePrivate(source, "transformResponse",
          new Class[]{String.class, String.class, Map.class, Map.class},
          "data", "http://test.com", params, vars);
      assertTrue(false, "Should have thrown");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof RuntimeException
          || e instanceof RuntimeException);
    }
    source.close();
  }

  // ======= CacheEntry inner class tests =======

  @Test void testCacheEntryNotExpired() throws Exception {
    Class<?> cacheEntryClass = null;
    for (Class<?> cls : HttpSource.class.getDeclaredClasses()) {
      if (cls.getSimpleName().equals("CacheEntry")) {
        cacheEntryClass = cls;
        break;
      }
    }
    assertNotNull(cacheEntryClass, "CacheEntry should exist");

    Constructor<?> ctor = cacheEntryClass.getDeclaredConstructor(List.class, long.class);
    ctor.setAccessible(true);

    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(createMap("id", 1));
    Object entry = ctor.newInstance(data, System.currentTimeMillis() + 60000);

    Method getData = cacheEntryClass.getDeclaredMethod("getData");
    getData.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result = (List<Map<String, Object>>) getData.invoke(entry);
    assertEquals(1, result.size());

    Method isExpired = cacheEntryClass.getDeclaredMethod("isExpired");
    isExpired.setAccessible(true);
    assertFalse((Boolean) isExpired.invoke(entry));
  }

  @Test void testCacheEntryExpired() throws Exception {
    Class<?> cacheEntryClass = null;
    for (Class<?> cls : HttpSource.class.getDeclaredClasses()) {
      if (cls.getSimpleName().equals("CacheEntry")) {
        cacheEntryClass = cls;
        break;
      }
    }
    assertNotNull(cacheEntryClass);

    Constructor<?> ctor = cacheEntryClass.getDeclaredConstructor(List.class, long.class);
    ctor.setAccessible(true);

    Object entry =
        ctor.newInstance(new ArrayList<Map<String, Object>>(), System.currentTimeMillis() - 1000);

    Method isExpired = cacheEntryClass.getDeclaredMethod("isExpired");
    isExpired.setAccessible(true);
    assertTrue((Boolean) isExpired.invoke(entry));
  }

  // ======= parseDelimitedLine tests =======

  @Test void testParseDelimitedLineWithQuotes() throws Exception {
    HttpSource source = createMinimalSource();
    Method m =
        HttpSource.class.getDeclaredMethod("parseDelimitedLine", String.class, char.class);
    m.setAccessible(true);

    String[] result =
        (String[]) m.invoke(source, "\"hello, world\",simple,\"with \"\"escaped\"\" quotes\"", ',');
    assertEquals(3, result.length);
    assertEquals("hello, world", result[0]);
    assertEquals("simple", result[1]);
    assertTrue(result[2].contains("escaped"));
    source.close();
  }

  @Test void testParseDelimitedLineTSV() throws Exception {
    HttpSource source = createMinimalSource();
    Method m =
        HttpSource.class.getDeclaredMethod("parseDelimitedLine", String.class, char.class);
    m.setAccessible(true);

    String[] result = (String[]) m.invoke(source, "a\tb\tc", '\t');
    assertEquals(3, result.length);
    assertEquals("a", result[0]);
    assertEquals("b", result[1]);
    assertEquals("c", result[2]);
    source.close();
  }

  // ======= create factory methods =======

  @Test void testCreateFactoryMethod() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .build();
    HttpSource source = HttpSource.create(config);
    assertNotNull(source);
    assertEquals("http", source.getType());
    source.close();
  }

  @Test void testCreateFactoryMethodWithHooks() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .build();
    HttpSource source = HttpSource.create(config, (HooksConfig) null);
    assertNotNull(source);
    source.close();
  }

  // ======= close tests =======

  @Test void testCloseWithCache() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .cache(
            HttpSourceConfig.CacheConfig.fromMap(
            createMap("enabled", true, "ttlSeconds", 60)))
        .build();
    HttpSource source = new HttpSource(config);
    source.close();
  }

  @Test void testCloseWithoutCache() {
    HttpSource source = createMinimalSource();
    source.close();
  }

  // ======= getType test =======

  @Test void testGetType() {
    HttpSource source = createMinimalSource();
    assertEquals("http", source.getType());
    source.close();
  }

  // ======= normalizeRecords tests =======

  @Test void testNormalizeRecordsWithNullNormalizer() throws Exception {
    HttpSource source = createMinimalSource();
    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    records.add(createMap("key", "value"));
    Map<String, String> vars = Collections.emptyMap();

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) invokePrivate(source, "normalizeRecords",
        new Class[]{List.class, Map.class}, records, vars);
    assertEquals(1, result.size());
    assertEquals("value", result.get(0).get("key"));
    source.close();
  }

  @Test void testNormalizeRecordsWithEmptyList() throws Exception {
    HttpSource source = createMinimalSource();
    List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
    Map<String, String> vars = Collections.emptyMap();

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) invokePrivate(source, "normalizeRecords",
        new Class[]{List.class, Map.class}, records, vars);
    assertTrue(result.isEmpty());
    source.close();
  }

  // ======= Constructor tests =======

  @Test void testConstructorWithHooksConfig() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .build();
    HooksConfig hooks = null;
    HttpSource source = new HttpSource(config, hooks);
    assertNotNull(source);
    source.close();
  }

  @Test void testConstructorWithStorageProvider() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .rawCache(HttpSourceConfig.RawCacheConfig.enabled())
        .build();
    HttpSource source = new HttpSource(config, null, null, "s3://bucket/raw");
    assertNotNull(source);
    source.close();
  }

  @Test void testConstructorWithResponseTransformer() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .build();
    ResponseTransformer transformer = new ResponseTransformer() {
      @Override public String transform(String response, RequestContext context) {
        return response;
      }
    };
    HttpSource source = new HttpSource(config, transformer);
    assertNotNull(source);
    source.close();
  }

  // ======= parseResponse with error path and error message types =======

  @Test void testParseResponseWithErrorPathNotFoundMessage() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "JSON", "errorPath", "error")))
        .build();
    HttpSource source = new HttpSource(config);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) invokePrivate(source, "parseResponse", new Class[]{String.class},
        "{\"error\": \"not found for this query\"}");
    assertTrue(result.isEmpty(), "Should return empty for 'not found' errors");
    source.close();
  }

  @Test void testParseResponseWithErrorPathParameterEmpty() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "JSON", "errorPath", "error")))
        .build();
    HttpSource source = new HttpSource(config);

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result =
        (List<Map<String, Object>>) invokePrivate(source, "parseResponse", new Class[]{String.class},
        "{\"error\": \"parameter_empty\"}");
    assertTrue(result.isEmpty(), "Should return empty for 'parameter_empty' errors");
    source.close();
  }

  @Test void testParseResponseWithUnsupportedFormat() throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost/test")
        .response(
            HttpSourceConfig.ResponseConfig.fromMap(
            createMap("format", "XML")))
        .build();
    HttpSource source = new HttpSource(config);

    try {
      invokePrivate(source, "parseResponse", new Class[]{String.class},
          "<root/>");
      assertTrue(false, "Should have thrown for XML format");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof IOException
          || e instanceof IOException);
    }
    source.close();
  }

  // ======= Utility =======

  @SuppressWarnings("unchecked")
  private static <K, V> Map<K, V> createMap(Object... kv) {
    Map<K, V> m = new LinkedHashMap<K, V>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((K) kv[i], (V) kv[i + 1]);
    }
    return m;
  }
}
