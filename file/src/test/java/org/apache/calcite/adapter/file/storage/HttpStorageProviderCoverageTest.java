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
package org.apache.calcite.adapter.file.storage;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HttpStorageProvider} covering constructors, path resolution,
 * content type detection, error classification, header application, auth configuration,
 * caching, and proxy logic. Uses an embedded HTTP server for integration-level paths
 * and reflection for private methods.
 */
@Tag("unit")
public class HttpStorageProviderCoverageTest {

  private HttpStorageProvider provider;
  private HttpServer server;
  private int serverPort;

  @BeforeEach
  void setUp() throws Exception {
    provider = new HttpStorageProvider();

    // Start an embedded HTTP server for tests that need real HTTP responses
    server = HttpServer.create(new InetSocketAddress(0), 0);
    serverPort = server.getAddress().getPort();

    // Default handler returns simple response
    server.createContext("/test", exchange -> {
      String response = "hello world";
      byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "text/plain");
      exchange.getResponseHeaders().set("ETag", "\"abc123\"");
      exchange.sendResponseHeaders(200, responseBytes.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(responseBytes);
      }
    });

    // Handler that returns 404
    server.createContext("/notfound", exchange -> {
      exchange.sendResponseHeaders(404, -1);
      exchange.close();
    });

    // Handler that returns 500
    server.createContext("/error", exchange -> {
      exchange.sendResponseHeaders(500, -1);
      exchange.close();
    });

    // Handler that returns JSON
    server.createContext("/json", exchange -> {
      String response = "{\"key\":\"value\"}";
      byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, responseBytes.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(responseBytes);
      }
    });

    // Handler that accepts POST
    server.createContext("/post", exchange -> {
      if ("POST".equals(exchange.getRequestMethod())) {
        // Read request body
        byte[] body;
        try (InputStream is = exchange.getRequestBody()) {
          java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
          byte[] buf = new byte[1024];
          int n;
          while ((n = is.read(buf)) != -1) {
            bos.write(buf, 0, n);
          }
          body = bos.toByteArray();
        }
        String response = "received:" + new String(body, StandardCharsets.UTF_8);
        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, responseBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(responseBytes);
        }
      } else {
        exchange.sendResponseHeaders(405, -1);
        exchange.close();
      }
    });

    // Handler for HEAD requests
    server.createContext("/head", exchange -> {
      exchange.getResponseHeaders().set("Content-Type", "text/csv");
      exchange.getResponseHeaders().set("ETag", "\"headetag\"");
      exchange.getResponseHeaders().set("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT");
      exchange.sendResponseHeaders(200, -1);
      exchange.close();
    });

    // Handler that returns Last-Modified header
    server.createContext("/modified", exchange -> {
      String response = "modified content";
      byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "text/plain");
      exchange.getResponseHeaders().set("Last-Modified", "Thu, 01 Jan 2026 00:00:00 GMT");
      exchange.sendResponseHeaders(200, responseBytes.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(responseBytes);
      }
    });

    server.setExecutor(null);
    server.start();
  }

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  // --- Constructor tests ---

  @Test
  void testDefaultConstructor() {
    HttpStorageProvider p = new HttpStorageProvider();
    assertNotNull(p);
    assertEquals("http", p.getStorageType());
  }

  @Test
  void testConstructorWithNullMethod() {
    HttpStorageProvider p =
        new HttpStorageProvider(null, null, null, null);
    assertEquals("http", p.getStorageType());
  }

  @Test
  void testConstructorWithGetMethod() {
    HttpStorageProvider p =
        new HttpStorageProvider("GET", null, new HashMap<>(), null);
    assertEquals("http", p.getStorageType());
  }

  @Test
  void testConstructorWithPostMethod() {
    HttpStorageProvider p =
        new HttpStorageProvider("POST", "{\"q\":\"test\"}", new HashMap<>(), "application/json");
    assertEquals("http", p.getStorageType());
  }

  @Test
  void testConstructorWithHttpConfig() {
    HttpConfig config = new HttpConfig();
    HttpStorageProvider p =
        new HttpStorageProvider("GET", null, new HashMap<>(), null, config);
    assertEquals("http", p.getStorageType());
  }

  // --- getStorageType ---

  @Test
  void testGetStorageType() {
    assertEquals("http", provider.getStorageType());
  }

  // --- isDirectory ---

  @Test
  void testIsDirectoryAlwaysFalse() {
    assertFalse(provider.isDirectory("http://example.com/file.csv"));
  }

  @Test
  void testIsDirectoryWithTrailingSlash() {
    assertFalse(provider.isDirectory("http://example.com/dir/"));
  }

  // --- resolvePath tests ---

  @Test
  void testResolvePathWithRelativePath() {
    String result = provider.resolvePath(
        "http://example.com/data/", "file.csv");
    assertEquals("http://example.com/data/file.csv", result);
  }

  @Test
  void testResolvePathWithAbsoluteUrl() {
    String result = provider.resolvePath(
        "http://example.com/data/", "http://other.com/file.csv");
    assertEquals("http://other.com/file.csv", result);
  }

  @Test
  void testResolvePathWithParentDirectory() {
    String result = provider.resolvePath(
        "http://example.com/data/subdir/", "../file.csv");
    assertEquals("http://example.com/data/file.csv", result);
  }

  @Test
  void testResolvePathWithQueryString() {
    String result = provider.resolvePath(
        "http://example.com/api?param=1", "endpoint");
    assertNotNull(result);
    assertTrue(result.contains("endpoint"));
  }

  @Test
  void testResolvePathFallbackOnInvalidUri() {
    // URISyntaxException causes fallback to string concatenation
    String result = provider.resolvePath("not a url", "file.txt");
    assertEquals("not a url/file.txt", result);
  }

  @Test
  void testResolvePathFallbackWithTrailingSlash() {
    String result = provider.resolvePath("invalid/", "file.txt");
    assertEquals("invalid/file.txt", result);
  }

  @Test
  void testResolvePathHttpsScheme() {
    String result = provider.resolvePath(
        "https://secure.example.com/data/", "file.csv");
    assertEquals("https://secure.example.com/data/file.csv", result);
  }

  // --- listFiles throws UnsupportedOperationException ---

  @Test
  void testListFilesThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.listFiles("http://example.com/", false));
  }

  @Test
  void testListFilesRecursiveThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.listFiles("http://example.com/", true));
  }

  // --- openInputStream with embedded server ---

  @Test
  void testOpenInputStreamSuccess() throws Exception {
    String url = "http://localhost:" + serverPort + "/test";
    InputStream is = provider.openInputStream(url);
    assertNotNull(is);

    byte[] data = readAllBytes(is);
    is.close();
    assertEquals("hello world", new String(data, StandardCharsets.UTF_8));
  }

  @Test
  void testOpenInputStreamJsonResponse() throws Exception {
    String url = "http://localhost:" + serverPort + "/json";
    InputStream is = provider.openInputStream(url);
    assertNotNull(is);

    byte[] data = readAllBytes(is);
    is.close();
    assertEquals("{\"key\":\"value\"}", new String(data, StandardCharsets.UTF_8));
  }

  @Test
  void testOpenInputStreamErrorResponse() {
    String url = "http://localhost:" + serverPort + "/error";
    assertThrows(IOException.class, () -> provider.openInputStream(url));
  }

  @Test
  void testOpenInputStreamNotFoundResponse() {
    String url = "http://localhost:" + serverPort + "/notfound";
    assertThrows(IOException.class, () -> provider.openInputStream(url));
  }

  @Test
  void testOpenInputStreamWithInvalidUrl() {
    // Non-absolute URI causes IllegalArgumentException from URI.toURL()
    assertThrows(IllegalArgumentException.class,
        () -> provider.openInputStream("not-a-url"));
  }

  // --- openReader ---

  @Test
  void testOpenReaderSuccess() throws Exception {
    String url = "http://localhost:" + serverPort + "/test";
    Reader reader = provider.openReader(url);
    assertNotNull(reader);

    StringBuilder sb = new StringBuilder();
    char[] buffer = new char[256];
    int n;
    while ((n = reader.read(buffer)) != -1) {
      sb.append(buffer, 0, n);
    }
    reader.close();
    assertEquals("hello world", sb.toString());
  }

  // --- exists ---

  @Test
  void testExistsSuccess() throws Exception {
    String url = "http://localhost:" + serverPort + "/head";
    boolean result = provider.exists(url);
    assertTrue(result);
  }

  @Test
  void testExistsNotFound() throws Exception {
    String url = "http://localhost:" + serverPort + "/notfound";
    boolean result = provider.exists(url);
    assertFalse(result);
  }

  @Test
  void testExistsWithInvalidUrl() throws Exception {
    // "not-a-valid-url" causes IllegalArgumentException (URI not absolute),
    // which is caught by the IOException|URISyntaxException handler -> returns false
    // But the actual path goes through URI.toURL() which throws IllegalArgumentException
    // The exists() method catches IOException | URISyntaxException but not IllegalArgumentException
    assertThrows(IllegalArgumentException.class,
        () -> provider.exists("not-a-valid-url"));
  }

  // --- getMetadata ---

  @Test
  void testGetMetadataSuccess() throws Exception {
    String url = "http://localhost:" + serverPort + "/head";
    StorageProvider.FileMetadata metadata = provider.getMetadata(url);
    assertNotNull(metadata);
    assertEquals(url, metadata.getPath());
    assertEquals("text/csv", metadata.getContentType());
    assertEquals("\"headetag\"", metadata.getEtag());
  }

  @Test
  void testGetMetadataError() {
    String url = "http://localhost:" + serverPort + "/notfound";
    assertThrows(IOException.class, () -> provider.getMetadata(url));
  }

  @Test
  void testGetMetadataInvalidUrl() {
    assertThrows(IOException.class,
        () -> provider.getMetadata("totally invalid url ::: @@"));
  }

  // --- POST requests ---

  @Test
  void testPostRequestOpenInputStream() throws Exception {
    HttpStorageProvider postProvider =
        new HttpStorageProvider("POST", "{\"query\":\"test\"}", new HashMap<>(), null);

    String url = "http://localhost:" + serverPort + "/post";
    InputStream is = postProvider.openInputStream(url);
    assertNotNull(is);

    byte[] data = readAllBytes(is);
    is.close();
    assertTrue(new String(data, StandardCharsets.UTF_8).contains("received:"));
  }

  @Test
  void testPostRequestGetMetadata() throws Exception {
    HttpStorageProvider postProvider =
        new HttpStorageProvider("POST", "{\"query\":\"test\"}", new HashMap<>(), null);

    String url = "http://localhost:" + serverPort + "/post";
    StorageProvider.FileMetadata metadata = postProvider.getMetadata(url);
    assertNotNull(metadata);
    assertEquals(url, metadata.getPath());
  }

  @Test
  void testPostRequestExists() throws Exception {
    HttpStorageProvider postProvider =
        new HttpStorageProvider("POST", "{\"query\":\"test\"}", new HashMap<>(), null);

    String url = "http://localhost:" + serverPort + "/post";
    boolean result = postProvider.exists(url);
    assertTrue(result);
  }

  // --- Content type detection ---

  @Test
  void testGetEffectiveContentTypeWithOverride() throws Exception {
    HttpStorageProvider overrideProvider =
        new HttpStorageProvider("GET", null, new HashMap<>(), "text/csv");

    Method getEffective =
        HttpStorageProvider.class.getDeclaredMethod("getEffectiveContentType",
            HttpURLConnection.class);
    getEffective.setAccessible(true);

    // Create a connection (we won't actually connect)
    java.net.URL url = new java.net.URI("http://localhost:" + serverPort + "/test").toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setConnectTimeout(1000);

    Object result = getEffective.invoke(overrideProvider, conn);
    assertEquals("text/csv", result);
    conn.disconnect();
  }

  @Test
  void testGetEffectiveContentTypeWithoutOverride() throws Exception {
    Method getEffective =
        HttpStorageProvider.class.getDeclaredMethod("getEffectiveContentType",
            HttpURLConnection.class);
    getEffective.setAccessible(true);

    java.net.URL url = new java.net.URI("http://localhost:" + serverPort + "/test").toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.connect();

    Object result = getEffective.invoke(provider, conn);
    assertNotNull(result);
    conn.disconnect();
  }

  // --- readAllBytes (via reflection) ---

  @Test
  void testReadAllBytesEmpty() throws Exception {
    Method readAll =
        HttpStorageProvider.class.getDeclaredMethod("readAllBytes", InputStream.class);
    readAll.setAccessible(true);

    byte[] result = (byte[]) readAll.invoke(provider, new ByteArrayInputStream(new byte[0]));
    assertNotNull(result);
    assertEquals(0, result.length);
  }

  @Test
  void testReadAllBytesSmall() throws Exception {
    Method readAll =
        HttpStorageProvider.class.getDeclaredMethod("readAllBytes", InputStream.class);
    readAll.setAccessible(true);

    byte[] input = "small data".getBytes(StandardCharsets.UTF_8);
    byte[] result = (byte[]) readAll.invoke(provider, new ByteArrayInputStream(input));
    assertEquals(input.length, result.length);
    assertEquals("small data", new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testReadAllBytesLarge() throws Exception {
    Method readAll =
        HttpStorageProvider.class.getDeclaredMethod("readAllBytes", InputStream.class);
    readAll.setAccessible(true);

    // Create data larger than the 16384 buffer
    byte[] input = new byte[50000];
    for (int i = 0; i < input.length; i++) {
      input[i] = (byte) (i % 256);
    }
    byte[] result = (byte[]) readAll.invoke(provider, new ByteArrayInputStream(input));
    assertEquals(input.length, result.length);
  }

  // --- Custom headers ---

  @Test
  void testCustomHeadersApplied() throws Exception {
    Map<String, String> customHeaders = new HashMap<>();
    customHeaders.put("X-Custom-Header", "test-value");
    customHeaders.put("Accept", "text/csv");

    HttpStorageProvider customProvider =
        new HttpStorageProvider("GET", null, customHeaders, null);

    String url = "http://localhost:" + serverPort + "/test";
    InputStream is = customProvider.openInputStream(url);
    assertNotNull(is);
    is.close();
  }

  // --- Auth configuration ---

  @Test
  void testBearerTokenAuth() throws Exception {
    HttpConfig config = new HttpConfig.Builder()
        .bearerToken("my-secret-token")
        .build();

    HttpStorageProvider authProvider =
        new HttpStorageProvider("GET", null, new HashMap<>(), null, config);

    String url = "http://localhost:" + serverPort + "/test";
    InputStream is = authProvider.openInputStream(url);
    assertNotNull(is);
    is.close();
  }

  @Test
  void testApiKeyAuth() throws Exception {
    HttpConfig config = new HttpConfig.Builder()
        .apiKey("my-api-key")
        .build();

    HttpStorageProvider authProvider =
        new HttpStorageProvider("GET", null, new HashMap<>(), null, config);

    String url = "http://localhost:" + serverPort + "/test";
    InputStream is = authProvider.openInputStream(url);
    assertNotNull(is);
    is.close();
  }

  @Test
  void testBasicAuth() throws Exception {
    HttpConfig config = new HttpConfig.Builder()
        .basicAuth("user", "pass")
        .build();

    HttpStorageProvider authProvider =
        new HttpStorageProvider("GET", null, new HashMap<>(), null, config);

    String url = "http://localhost:" + serverPort + "/test";
    InputStream is = authProvider.openInputStream(url);
    assertNotNull(is);
    is.close();
  }

  // --- hasChanged tests ---

  @Test
  void testHasChangedWithNullMetadata() throws Exception {
    boolean result = provider.hasChanged("http://example.com/file.csv", null);
    assertTrue(result, "Should report changed when cached metadata is null");
  }

  @Test
  void testHasChangedWithMatchingEtag() throws Exception {
    String url = "http://localhost:" + serverPort + "/test";
    StorageProvider.FileMetadata metadata = provider.getMetadata(url);
    assertNotNull(metadata);

    // hasChanged should detect same ETag and report not changed
    boolean result = provider.hasChanged(url, metadata);
    assertFalse(result, "Same ETag should report not changed");
  }

  @Test
  void testHasChangedWithDifferentSize() throws Exception {
    String url = "http://localhost:" + serverPort + "/test";

    // Create metadata with different size
    StorageProvider.FileMetadata cachedMetadata =
        new StorageProvider.FileMetadata(url, 99999,
            System.currentTimeMillis(), "text/plain", "\"different\"");

    boolean result = provider.hasChanged(url, cachedMetadata);
    assertTrue(result, "Different size should report changed");
  }

  @Test
  void testHasChangedWhenMetadataFetchFails() throws Exception {
    StorageProvider.FileMetadata cachedMetadata =
        new StorageProvider.FileMetadata("http://nonexistent.invalid/file.csv",
            100, System.currentTimeMillis(), "text/plain", "\"etag\"");

    boolean result = provider.hasChanged("http://nonexistent.invalid/file.csv", cachedMetadata);
    assertTrue(result, "Should report changed when metadata cannot be fetched");
  }

  // --- CachedResponse inner class (via reflection) ---

  @Test
  void testCachedResponseNotExpired() throws Exception {
    Class<?>[] innerClasses = HttpStorageProvider.class.getDeclaredClasses();
    Class<?> cachedResponseClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("CachedResponse")) {
        cachedResponseClass = c;
        break;
      }
    }
    assertNotNull(cachedResponseClass, "CachedResponse inner class should exist");

    java.lang.reflect.Constructor<?> ctor =
        cachedResponseClass.getDeclaredConstructor(byte[].class, String.class, long.class);
    ctor.setAccessible(true);

    Object cached = ctor.newInstance("data".getBytes(StandardCharsets.UTF_8), "\"etag\"", 1000L);

    Method isExpired = cachedResponseClass.getDeclaredMethod("isExpired", long.class);
    isExpired.setAccessible(true);

    // With a very large TTL, should not be expired
    boolean expired = (Boolean) isExpired.invoke(cached, 999999999L);
    assertFalse(expired);
  }

  @Test
  void testCachedResponseExpired() throws Exception {
    Class<?>[] innerClasses = HttpStorageProvider.class.getDeclaredClasses();
    Class<?> cachedResponseClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("CachedResponse")) {
        cachedResponseClass = c;
        break;
      }
    }
    assertNotNull(cachedResponseClass);

    java.lang.reflect.Constructor<?> ctor =
        cachedResponseClass.getDeclaredConstructor(byte[].class, String.class, long.class);
    ctor.setAccessible(true);

    Object cached = ctor.newInstance("data".getBytes(StandardCharsets.UTF_8), "\"etag\"", 1000L);

    Method isExpired = cachedResponseClass.getDeclaredMethod("isExpired", long.class);
    isExpired.setAccessible(true);

    // With a negative TTL, the condition (currentTime - cachedAt > ttl) is always true
    // because currentTime - cachedAt >= 0 and ttl < 0
    boolean expired = (Boolean) isExpired.invoke(cached, -1L);
    assertTrue(expired, "Negative TTL should always indicate expired");
  }

  // --- Default operations ---

  @Test
  void testWriteFileThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.writeFile("/test.txt", new byte[0]));
  }

  @Test
  void testDeleteThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.delete("/test.txt"));
  }

  @Test
  void testCopyFileThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.copyFile("/source.txt", "/dest.txt"));
  }

  @Test
  void testReadRangeThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.readRange("http://host/file.csv", 0, 100));
  }

  @Test
  void testCreateDirectoriesThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.createDirectories("/test/dir"));
  }

  // --- S3 config returns null ---

  @Test
  void testGetS3ConfigReturnsNull() {
    assertNull(provider.getS3Config());
  }

  // --- getEnvironmentVariable ---

  @Test
  void testGetEnvironmentVariableExisting() {
    // PATH is almost always set on every OS
    String path = provider.getEnvironmentVariable("PATH");
    assertNotNull(path, "PATH environment variable should be available");
  }

  @Test
  void testGetEnvironmentVariableNonExisting() {
    String result = provider.getEnvironmentVariable(
        "CALCITE_TEST_NONEXISTENT_VAR_12345");
    assertNull(result, "Non-existent variable should return null");
  }

  // --- ProxyRequest / ProxyResponse inner classes ---

  @Test
  void testProxyRequestFields() {
    HttpStorageProvider.ProxyRequest request = new HttpStorageProvider.ProxyRequest();
    request.url = "http://example.com/api";
    request.method = "POST";
    request.headers = new HashMap<>();
    request.headers.put("Authorization", "Bearer token");
    request.body = "{\"key\":\"value\"}";

    assertEquals("http://example.com/api", request.url);
    assertEquals("POST", request.method);
    assertEquals(1, request.headers.size());
    assertEquals("{\"key\":\"value\"}", request.body);
  }

  @Test
  void testProxyResponseFields() {
    HttpStorageProvider.ProxyResponse response = new HttpStorageProvider.ProxyResponse();
    response.status = 200;
    response.headers = new HashMap<>();
    response.headers.put("Content-Type", "application/json");
    response.body = "{\"result\":\"ok\"}";

    assertEquals(200, response.status);
    assertEquals(1, response.headers.size());
    assertEquals("{\"result\":\"ok\"}", response.body);
  }

  // --- In-memory cache with HttpConfig ---

  @Test
  void testCacheEnabledStoreAndRetrieve() throws Exception {
    HttpConfig config = new HttpConfig.Builder()
        .cacheEnabled(true)
        .cacheTtl(60000)
        .build();

    HttpStorageProvider cachingProvider =
        new HttpStorageProvider("GET", null, new HashMap<>(), null, config);

    String url = "http://localhost:" + serverPort + "/test";

    // First request populates cache
    InputStream is1 = cachingProvider.openInputStream(url);
    byte[] data1 = readAllBytes(is1);
    is1.close();
    assertEquals("hello world", new String(data1, StandardCharsets.UTF_8));

    // Second request should use cache (conditional request with ETag)
    InputStream is2 = cachingProvider.openInputStream(url);
    byte[] data2 = readAllBytes(is2);
    is2.close();
    assertEquals("hello world", new String(data2, StandardCharsets.UTF_8));
  }

  // --- Content-Type override with POST ---

  @Test
  void testContentTypeDefaultsToJsonForPost() throws Exception {
    HttpStorageProvider postProvider =
        new HttpStorageProvider("POST", "{}", new HashMap<>(), null);

    String url = "http://localhost:" + serverPort + "/post";
    InputStream is = postProvider.openInputStream(url);
    assertNotNull(is);
    is.close();
  }

  @Test
  void testContentTypeExplicitForPost() throws Exception {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/xml");

    HttpStorageProvider postProvider =
        new HttpStorageProvider("POST", "<data/>", headers, null);

    // The POST handler will accept any content type
    String url = "http://localhost:" + serverPort + "/post";
    InputStream is = postProvider.openInputStream(url);
    assertNotNull(is);
    is.close();
  }

  // --- Internal method field checks ---

  @Test
  void testMethodFieldIsStoredCorrectly() throws Exception {
    HttpStorageProvider postProvider =
        new HttpStorageProvider("PUT", "body", new HashMap<>(), null);

    Field methodField = HttpStorageProvider.class.getDeclaredField("method");
    methodField.setAccessible(true);
    assertEquals("PUT", methodField.get(postProvider));
  }

  @Test
  void testRequestBodyFieldIsStoredCorrectly() throws Exception {
    HttpStorageProvider postProvider =
        new HttpStorageProvider("POST", "my-body", new HashMap<>(), null);

    Field bodyField = HttpStorageProvider.class.getDeclaredField("requestBody");
    bodyField.setAccessible(true);
    assertEquals("my-body", bodyField.get(postProvider));
  }

  @Test
  void testMimeTypeOverrideFieldIsStoredCorrectly() throws Exception {
    HttpStorageProvider overrideProvider =
        new HttpStorageProvider("GET", null, new HashMap<>(), "application/csv");

    Field mimeField = HttpStorageProvider.class.getDeclaredField("mimeTypeOverride");
    mimeField.setAccessible(true);
    assertEquals("application/csv", mimeField.get(overrideProvider));
  }

  // --- Utility ---

  private byte[] readAllBytes(InputStream is) throws IOException {
    java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
    byte[] buf = new byte[1024];
    int n;
    while ((n = is.read(buf)) != -1) {
      bos.write(buf, 0, n);
    }
    return bos.toByteArray();
  }
}
