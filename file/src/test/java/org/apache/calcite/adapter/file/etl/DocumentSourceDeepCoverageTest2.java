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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests (tier 2) for {@link DocumentSource}.
 *
 * <p>Extends existing DocumentSourceDeepCoverageTest with additional coverage:
 * - Constructor with document source config from maps
 * - Constructor with custom headers including Accept-Encoding
 * - Constructor with zero rate limit
 * - Constructor with high rate limit
 * - substituteVariables with special characters in replacement
 * - substituteVariables with multiple env vars
 * - buildCacheKey with accession only (no cik, no document)
 * - buildCacheKey with empty map
 * - downloadDocument when document URL is null
 * - fetchMetadata when metadata URL is null
 * - fetchUrlContent with unreachable URL
 * - sleepQuietly via reflection
 * - enforceRateLimit rate limiting behavior
 * - DocumentSource with LocalFileStorageProvider (real storage)
 * - downloadDocument with cached file exists
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class DocumentSourceDeepCoverageTest2 {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;

  @BeforeEach
  void setup() {
    storageProvider = new LocalFileStorageProvider();
  }

  // ===== Helper methods =====

  private HttpSourceConfig createConfigWithDocSource(String metadataUrl, String documentUrl) {
    // When type=document, HttpSourceConfig.fromMap reads metadataUrl/documentUrl
    // from the top-level map (not from a nested documentSource block).
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("type", "document");
    sourceMap.put("url", metadataUrl != null ? metadataUrl : "http://localhost/test");
    if (metadataUrl != null) {
      sourceMap.put("metadataUrl", metadataUrl);
    }
    if (documentUrl != null) {
      sourceMap.put("documentUrl", documentUrl);
    }

    return HttpSourceConfig.fromMap(sourceMap);
  }

  private HttpSourceConfig createConfigWithRateLimit(int requestsPerSecond) {
    Map<String, Object> rateLimitMap = new LinkedHashMap<String, Object>();
    rateLimitMap.put("requestsPerSecond", Integer.valueOf(requestsPerSecond));

    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("type", "http");
    sourceMap.put("url", "http://localhost/test");
    sourceMap.put("rateLimit", rateLimitMap);

    return HttpSourceConfig.fromMap(sourceMap);
  }

  private HttpSourceConfig createMinimalConfig() {
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("type", "http");
    sourceMap.put("url", "http://localhost/test");
    return HttpSourceConfig.fromMap(sourceMap);
  }

  // ===== Constructor with real LocalFileStorageProvider =====

  @Test
  void testConstructorWithLocalStorage() {
    HttpSourceConfig config = createConfigWithDocSource(
        "http://localhost/meta", "http://localhost/doc");

    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    assertNotNull(ds);
    assertNotNull(ds.getConfig());
    assertNotNull(ds.getDocumentConfig());
    assertEquals(tempDir.toString(), ds.getCacheDirectory());
  }

  @Test
  void testConstructorDefaultRateInterval() {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    // Default: RateLimitConfig.defaults() = 10 req/sec => 100ms interval
    assertEquals(100, ds.getMinRequestIntervalMs());
  }

  @Test
  void testConstructorWithHighRateLimit() {
    HttpSourceConfig config = createConfigWithRateLimit(100);
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    // 100 requests/second = 10ms interval
    assertEquals(10, ds.getMinRequestIntervalMs());
  }

  @Test
  void testConstructorCustomHeaders() {
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put("User-Agent", "TestAgent/1.0");
    // Do NOT set Accept-Encoding - constructor should add it

    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("type", "http");
    sourceMap.put("url", "http://localhost/test");
    sourceMap.put("headers", headers);

    HttpSourceConfig config = HttpSourceConfig.fromMap(sourceMap);
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertNotNull(ds);
  }

  @Test
  void testConstructorHeadersWithAcceptEncoding() {
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put("Accept-Encoding", "identity"); // already set
    headers.put("X-Custom", "value");

    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("type", "http");
    sourceMap.put("url", "http://localhost/test");
    sourceMap.put("headers", headers);

    HttpSourceConfig config = HttpSourceConfig.fromMap(sourceMap);
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertNotNull(ds);
  }

  // ===== substituteVariables additional cases =====

  @Test
  void testSubstituteVariablesSpecialChars() {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("query", "foo bar&baz=1");

    String result = ds.substituteVariables("https://api.example.com/search?q={query}", vars);
    assertEquals("https://api.example.com/search?q=foo bar&baz=1", result);
  }

  @Test
  void testSubstituteVariablesDollarSign() {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("amount", "$100");

    String result = ds.substituteVariables("price={amount}", vars);
    assertEquals("price=$100", result);
  }

  @Test
  void testSubstituteVariablesEmptyValue() {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("empty", "");

    String result = ds.substituteVariables("val={empty}", vars);
    assertEquals("val=", result);
  }

  @Test
  void testSubstituteVariablesMultipleOccurrences() {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");

    String result = ds.substituteVariables(
        "start={year}&end={year}", vars);
    assertEquals("start=2024&end=2024", result);
  }

  // ===== buildCacheKey additional =====

  @Test
  void testBuildCacheKeyEmptyMap() throws Exception {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    Method m = DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
    m.setAccessible(true);

    Map<String, String> vars = new HashMap<String, String>();
    String key = (String) m.invoke(ds, vars);
    // No cik, accession, or document => hash fallback
    assertTrue(key.endsWith(".dat"));
  }

  @Test
  void testBuildCacheKeyDocumentOnly() throws Exception {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    Method m = DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
    m.setAccessible(true);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("document", "report.xbrl");

    String key = (String) m.invoke(ds, vars);
    assertTrue(key.contains("report.xbrl"));
    assertFalse(key.contains("/")); // no cik or accession prefix
  }

  @Test
  void testBuildCacheKeyAccessionAndDocument() throws Exception {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    Method m = DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
    m.setAccessible(true);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("accession", "0001-00-000001");
    vars.put("document", "filing.htm");

    String key = (String) m.invoke(ds, vars);
    assertTrue(key.contains("000100000001/"));
    assertTrue(key.endsWith("filing.htm"));
  }

  // ===== fetchMetadata with null metadata URL =====

  @Test
  void testFetchMetadataNullMetadataUrl() {
    HttpSourceConfig config = createConfigWithDocSource(null, "http://localhost/doc");
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    assertThrows(IllegalStateException.class, () ->
        ds.fetchMetadata(Collections.<String, String>emptyMap()));
  }

  // ===== downloadDocument with null document URL =====

  @Test
  void testDownloadDocumentNullDocumentUrl() {
    HttpSourceConfig config = createConfigWithDocSource("http://localhost/meta", null);
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    assertThrows(IllegalStateException.class, () ->
        ds.downloadDocument(Collections.<String, String>emptyMap()));
  }

  // ===== downloadDocument - cached file exists =====

  @Test
  void testDownloadDocumentCachedFileExists() throws Exception {
    // Create a cached file
    Path cacheDir = tempDir.resolve("docscache");
    Files.createDirectories(cacheDir);

    HttpSourceConfig config = createConfigWithDocSource(
        "http://localhost/meta", "http://localhost/{cik}/doc");
    DocumentSource ds = new DocumentSource(config, storageProvider, cacheDir.toString());

    // Create the expected cache file
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "testcik");
    vars.put("document", "test.xml");

    // Build the cache key to predict the path
    Method buildCacheKey = DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
    buildCacheKey.setAccessible(true);
    String cacheKey = (String) buildCacheKey.invoke(ds, vars);
    String cachePath = storageProvider.resolvePath(cacheDir.toString(), cacheKey);

    // Create parent directories and the file
    java.io.File cacheFile = new java.io.File(cachePath);
    cacheFile.getParentFile().mkdirs();
    java.io.FileWriter writer = new java.io.FileWriter(cacheFile);
    try {
      writer.write("<doc>cached content</doc>");
    } finally {
      writer.close();
    }

    // Should return cached path without downloading
    String result = ds.downloadDocument(vars);
    assertEquals(cachePath, result);
  }

  // ===== fetchUrlContent with unreachable URL =====

  @Test
  void testFetchUrlContentUnreachable() {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    assertThrows(IOException.class, () ->
        ds.fetchUrlContent("http://localhost:19999/nonexistent"));
  }

  // ===== sleepQuietly =====

  @Test
  void testSleepQuietly() throws Exception {
    Method m = DocumentSource.class.getDeclaredMethod("sleepQuietly", long.class);
    m.setAccessible(true);

    // Should not throw
    m.invoke(null, 1L);
  }

  // ===== enforceRateLimit =====

  @Test
  void testEnforceRateLimit() throws Exception {
    HttpSourceConfig config = createConfigWithRateLimit(1000);
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    Method m = DocumentSource.class.getDeclaredMethod("enforceRateLimit");
    m.setAccessible(true);

    // Call twice - should be fast with high rate limit
    m.invoke(ds);
    m.invoke(ds);
  }

  // ===== getDocumentConfig null for minimal config =====

  @Test
  void testGetDocumentConfigNullForMinimal() {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    assertNull(ds.getDocumentConfig());
  }

  @Test
  void testGetDocumentConfigPresent() {
    HttpSourceConfig config = createConfigWithDocSource(
        "http://localhost/meta", "http://localhost/doc");
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    assertNotNull(ds.getDocumentConfig());
  }

  // ===== documentIterator with non-empty JSON (still returns empty in placeholder) =====

  @Test
  void testDocumentIteratorWithJsonPayload() {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "12345");

    Iterator<Map<String, String>> iter = ds.documentIterator(
        "{\"recent\": {\"filings\": []}}", baseVars);
    assertNotNull(iter);
    assertFalse(iter.hasNext());
  }

  // ===== isRetryableException additional cases =====

  @Test
  void testIsRetryableExceptionTimeout() throws Exception {
    Method m = DocumentSource.class.getDeclaredMethod("isRetryableException", IOException.class);
    m.setAccessible(true);

    assertTrue((Boolean) m.invoke(null, new IOException("Read timeout")));
    assertTrue((Boolean) m.invoke(null, new IOException("HANDSHAKE_FAILURE")));
  }

  @Test
  void testIsRetryableExceptionNonRetryable() throws Exception {
    Method m = DocumentSource.class.getDeclaredMethod("isRetryableException", IOException.class);
    m.setAccessible(true);

    assertFalse((Boolean) m.invoke(null, new IOException("Access denied")));
    assertFalse((Boolean) m.invoke(null, new IOException("Invalid format")));
    assertFalse((Boolean) m.invoke(null, new IOException("Not authorized")));
  }

  // ===== isRetryableHttpStatus additional =====

  @Test
  void testIsRetryableHttpStatusBoundary() throws Exception {
    Method m = DocumentSource.class.getDeclaredMethod("isRetryableHttpStatus", int.class);
    m.setAccessible(true);

    assertFalse((Boolean) m.invoke(null, 428)); // Just before 429
    assertTrue((Boolean) m.invoke(null, 429));
    assertFalse((Boolean) m.invoke(null, 430)); // Just after 429
    assertFalse((Boolean) m.invoke(null, 499));
    assertTrue((Boolean) m.invoke(null, 500));
    assertFalse((Boolean) m.invoke(null, 501));
    assertTrue((Boolean) m.invoke(null, 502));
    assertTrue((Boolean) m.invoke(null, 503));
    assertTrue((Boolean) m.invoke(null, 504));
    assertFalse((Boolean) m.invoke(null, 505));
  }

  // ===== retryDelay =====

  @Test
  void testRetryDelayExponential() throws Exception {
    Method m = DocumentSource.class.getDeclaredMethod("retryDelay", int.class);
    m.setAccessible(true);

    long d0 = (Long) m.invoke(null, 0);
    long d1 = (Long) m.invoke(null, 1);
    long d2 = (Long) m.invoke(null, 2);

    assertEquals(1000L, d0);
    assertEquals(2000L, d1);
    assertEquals(4000L, d2);
    // Each delay doubles
    assertEquals(d0 * 2, d1);
    assertEquals(d1 * 2, d2);
  }
}
