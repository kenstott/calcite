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

import java.io.File;
import java.io.FileWriter;
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
 * Coverage tests for {@link DocumentSource}.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class DocumentSourceCoverageTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;

  @BeforeEach void setup() {
    storageProvider = new LocalFileStorageProvider();
  }

  private HttpSourceConfig createConfigWithDocSource(String metadataUrl, String documentUrl) {
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

  private HttpSourceConfig createConfigWithHeaders(Map<String, Object> headers) {
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("type", "http");
    sourceMap.put("url", "http://localhost/test");
    sourceMap.put("headers", headers);
    return HttpSourceConfig.fromMap(sourceMap);
  }

  private HttpSourceConfig createConfigWithZeroRateLimit() {
    Map<String, Object> rateLimitMap = new LinkedHashMap<String, Object>();
    rateLimitMap.put("requestsPerSecond", Integer.valueOf(0));
    Map<String, Object> sourceMap = new LinkedHashMap<String, Object>();
    sourceMap.put("type", "http");
    sourceMap.put("url", "http://localhost/test");
    sourceMap.put("rateLimit", rateLimitMap);
    return HttpSourceConfig.fromMap(sourceMap);
  }

  @Test void testConstructorBasicWithLocalStorage() {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertNotNull(ds);
    assertNotNull(ds.getConfig());
    assertNull(ds.getDocumentConfig());
    assertEquals(tempDir.toString(), ds.getCacheDirectory());
  }

  @Test void testConstructorWithDocumentSourceConfig() {
    HttpSourceConfig config =
        createConfigWithDocSource("http://localhost/meta/{cik}", "http://localhost/doc/{cik}/{document}");
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertNotNull(ds.getDocumentConfig());
    assertEquals("http://localhost/meta/{cik}", ds.getDocumentConfig().getMetadataUrl());
    assertEquals("http://localhost/doc/{cik}/{document}", ds.getDocumentConfig().getDocumentUrl());
  }

  @Test void testConstructorWithRateLimitCalculation() {
    HttpSourceConfig config = createConfigWithRateLimit(10);
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertEquals(100, ds.getMinRequestIntervalMs());
  }

  @Test void testConstructorWithHighRateLimitCalculation() {
    HttpSourceConfig config = createConfigWithRateLimit(200);
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertEquals(5, ds.getMinRequestIntervalMs());
  }

  @Test void testConstructorWithZeroRateLimitUsesDefault() {
    HttpSourceConfig config = createConfigWithZeroRateLimit();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertEquals(125, ds.getMinRequestIntervalMs());
  }

  @Test void testConstructorDefaultRateLimitFromDefaults() {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertEquals(100, ds.getMinRequestIntervalMs());
  }

  @Test void testConstructorAddsAcceptEncodingWhenMissing() {
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put("User-Agent", "TestAgent/1.0");
    HttpSourceConfig config = createConfigWithHeaders(headers);
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertNotNull(ds);
  }

  @Test void testConstructorPreservesExistingAcceptEncoding() {
    Map<String, Object> headers = new LinkedHashMap<String, Object>();
    headers.put("Accept-Encoding", "identity");
    headers.put("X-Custom-Header", "custom-value");
    HttpSourceConfig config = createConfigWithHeaders(headers);
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertNotNull(ds);
  }

  @Test void testSubstituteVariablesNull() {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    assertNull(ds.substituteVariables(null, Collections.<String, String>emptyMap()));
  }

  @Test void testSubstituteVariablesNoPlaceholders() {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    assertEquals("https://api.example.com/static",
        ds.substituteVariables("https://api.example.com/static", Collections.<String, String>emptyMap()));
  }

  @Test void testSubstituteVariablesWithRegularVars() {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0000070502");
    vars.put("accession", "0001-23-000001");
    vars.put("document", "filing.xml");
    String result = ds.substituteVariables("https://api.sec.gov/{cik}/{accession}/{document}", vars);
    assertEquals("https://api.sec.gov/0000070502/0001-23-000001/filing.xml", result);
  }

  @Test void testSubstituteVariablesMissingVarResolvesToEmpty() {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "123");
    assertEquals("https://api.example.com/123/",
        ds.substituteVariables("https://api.example.com/{cik}/{missing}", vars));
  }

  @Test void testSubstituteVariablesEnvVarFromSystemProperty() {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    System.setProperty("TEST_DOC_SOURCE_COVERAGE_VAR", "prop_value");
    try {
      assertEquals("https://api.example.com/prop_value/data",
          ds.substituteVariables("https://api.example.com/{env:TEST_DOC_SOURCE_COVERAGE_VAR}/data",
              Collections.<String, String>emptyMap()));
    } finally {
      System.clearProperty("TEST_DOC_SOURCE_COVERAGE_VAR");
    }
  }

  @Test void testSubstituteVariablesEnvVarNotFound() {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    assertEquals("https://api.example.com//data",
        ds.substituteVariables("https://api.example.com/{env:NONEXISTENT_DOCSOURCE_12345}/data",
            Collections.<String, String>emptyMap()));
  }

  @Test void testSubstituteVariablesMultipleOccurrences() {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("id", "42");
    assertEquals("/api/42/details/42/info",
        ds.substituteVariables("/api/{id}/details/{id}/info", vars));
  }

  @Test void testFetchMetadataNoDocumentConfigThrows() {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    assertThrows(IllegalStateException.class, () ->
        ds.fetchMetadata(Collections.<String, String>emptyMap()));
  }

  @Test void testFetchMetadataNullMetadataUrlThrows() {
    HttpSourceConfig config = createConfigWithDocSource(null, "http://localhost/doc");
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertThrows(IllegalStateException.class, () ->
        ds.fetchMetadata(Collections.<String, String>emptyMap()));
  }

  @Test void testDownloadDocumentNoDocumentConfigThrows() {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    assertThrows(IllegalStateException.class, () ->
        ds.downloadDocument(Collections.<String, String>emptyMap()));
  }

  @Test void testDownloadDocumentNullDocumentUrlThrows() {
    HttpSourceConfig config = createConfigWithDocSource("http://localhost/meta", null);
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertThrows(IllegalStateException.class, () ->
        ds.downloadDocument(Collections.<String, String>emptyMap()));
  }

  @Test void testDownloadDocumentReturnsCachedPath() throws Exception {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);
    HttpSourceConfig config =
        createConfigWithDocSource("http://localhost/meta", "http://localhost/{cik}/{document}");
    DocumentSource ds = new DocumentSource(config, storageProvider, cacheDir.toString());
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "testcik");
    vars.put("document", "test.xml");
    Method buildCacheKey = DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
    buildCacheKey.setAccessible(true);
    String cacheKey = (String) buildCacheKey.invoke(ds, vars);
    String cachePath = storageProvider.resolvePath(cacheDir.toString(), cacheKey);
    File cacheFile = new File(cachePath);
    cacheFile.getParentFile().mkdirs();
    FileWriter writer = new FileWriter(cacheFile);
    try {
      writer.write("<doc>cached content</doc>");
    } finally {
      writer.close();
    }
    assertEquals(cachePath, ds.downloadDocument(vars));
  }

  @Test void testDownloadDocumentCachedFileZeroSize() throws Exception {
    Path cacheDir = tempDir.resolve("cache_empty");
    Files.createDirectories(cacheDir);
    HttpSourceConfig config =
        createConfigWithDocSource("http://localhost/meta", "http://localhost/{cik}/{document}");
    DocumentSource ds = new DocumentSource(config, storageProvider, cacheDir.toString());
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "testcik");
    vars.put("document", "empty.xml");
    Method buildCacheKey = DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
    buildCacheKey.setAccessible(true);
    String cacheKey = (String) buildCacheKey.invoke(ds, vars);
    String cachePath = storageProvider.resolvePath(cacheDir.toString(), cacheKey);
    File cacheFile = new File(cachePath);
    cacheFile.getParentFile().mkdirs();
    cacheFile.createNewFile();
    assertThrows(IOException.class, () -> ds.downloadDocument(vars));
  }

  @Test void testDocumentIteratorReturnsEmpty() {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    Iterator<Map<String, String>> iter = ds.documentIterator("{}", Collections.<String, String>emptyMap());
    assertNotNull(iter);
    assertFalse(iter.hasNext());
  }

  @Test void testBuildCacheKeyWithAllVariables() throws Exception {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    Method buildCacheKey = DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
    buildCacheKey.setAccessible(true);
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0001234567");
    vars.put("accession", "0001234567-20-123456");
    vars.put("document", "filing.xml");
    String key = (String) buildCacheKey.invoke(ds, vars);
    assertNotNull(key);
    assertTrue(key.startsWith("0001234567/"));
    assertTrue(key.endsWith("filing.xml"));
  }

  @Test void testBuildCacheKeyNoCik() throws Exception {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    Method buildCacheKey = DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
    buildCacheKey.setAccessible(true);
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("document", "report.xbrl");
    String key = (String) buildCacheKey.invoke(ds, vars);
    assertNotNull(key);
    assertTrue(key.contains("report.xbrl"));
  }

  @Test void testBuildCacheKeyNoDocument() throws Exception {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    Method buildCacheKey = DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
    buildCacheKey.setAccessible(true);
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "123");
    vars.put("accession", "0001234567-20-123456");
    String key = (String) buildCacheKey.invoke(ds, vars);
    assertTrue(key.endsWith(".dat"));
  }

  @Test void testBuildCacheKeyEmptyMap() throws Exception {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    Method buildCacheKey = DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
    buildCacheKey.setAccessible(true);
    String key = (String) buildCacheKey.invoke(ds, new HashMap<String, String>());
    assertTrue(key.endsWith(".dat"));
  }

  @Test void testIsRetryableHttpStatus() throws Exception {
    Method m = DocumentSource.class.getDeclaredMethod("isRetryableHttpStatus", int.class);
    m.setAccessible(true);
    assertTrue((Boolean) m.invoke(null, 429));
    assertTrue((Boolean) m.invoke(null, 500));
    assertTrue((Boolean) m.invoke(null, 502));
    assertTrue((Boolean) m.invoke(null, 503));
    assertTrue((Boolean) m.invoke(null, 504));
    assertFalse((Boolean) m.invoke(null, 200));
    assertFalse((Boolean) m.invoke(null, 404));
    assertFalse((Boolean) m.invoke(null, 403));
  }

  @Test void testIsRetryableExceptionRetryableMessages() throws Exception {
    Method m = DocumentSource.class.getDeclaredMethod("isRetryableException", IOException.class);
    m.setAccessible(true);
    assertTrue((Boolean) m.invoke(null, new IOException("connection reset")));
    assertTrue((Boolean) m.invoke(null, new IOException("TLS error")));
    assertTrue((Boolean) m.invoke(null, new IOException("SSL handshake")));
    assertTrue((Boolean) m.invoke(null, new IOException("timed out")));
    assertTrue((Boolean) m.invoke(null, new IOException("broken pipe")));
    assertTrue((Boolean) m.invoke(null, new IOException("connection refused")));
    assertTrue((Boolean) m.invoke(null, new IOException((String) null)));
    assertFalse((Boolean) m.invoke(null, new IOException("File not found")));
  }

  @Test void testRetryDelayExponentialBackoff() throws Exception {
    Method m = DocumentSource.class.getDeclaredMethod("retryDelay", int.class);
    m.setAccessible(true);
    assertEquals(1000L, m.invoke(null, 0));
    assertEquals(2000L, m.invoke(null, 1));
    assertEquals(4000L, m.invoke(null, 2));
    assertEquals(8000L, m.invoke(null, 3));
  }

  @Test void testSleepQuietly() throws Exception {
    Method m = DocumentSource.class.getDeclaredMethod("sleepQuietly", long.class);
    m.setAccessible(true);
    m.invoke(null, 1L);
  }

  @Test void testSleepQuietlyInterrupted() throws Exception {
    Method m = DocumentSource.class.getDeclaredMethod("sleepQuietly", long.class);
    m.setAccessible(true);
    Thread.currentThread().interrupt();
    m.invoke(null, 10L);
    assertTrue(Thread.interrupted());
  }

  @Test void testEnforceRateLimitDoesNotThrow() throws Exception {
    HttpSourceConfig config = createConfigWithRateLimit(1000);
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    Method m = DocumentSource.class.getDeclaredMethod("enforceRateLimit");
    m.setAccessible(true);
    m.invoke(ds);
    m.invoke(ds);
  }

  @Test void testFetchUrlContentUnreachableThrowsIOException() {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    assertThrows(IOException.class, () -> ds.fetchUrlContent("http://localhost:19999/nonexistent"));
  }

  @Test void testGetConfig() {
    HttpSourceConfig config = createMinimalConfig();
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertEquals(config, ds.getConfig());
  }

  @Test void testGetDocumentConfigNull() {
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, tempDir.toString());
    assertNull(ds.getDocumentConfig());
  }

  @Test void testGetDocumentConfigPresent() {
    HttpSourceConfig config = createConfigWithDocSource("http://localhost/meta", "http://localhost/doc");
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertNotNull(ds.getDocumentConfig());
  }

  @Test void testGetCacheDirectory() {
    String cacheDir = tempDir.resolve("my-cache").toString();
    DocumentSource ds = new DocumentSource(createMinimalConfig(), storageProvider, cacheDir);
    assertEquals(cacheDir, ds.getCacheDirectory());
  }

  @Test void testGetMinRequestIntervalMs() {
    HttpSourceConfig config = createConfigWithRateLimit(5);
    DocumentSource ds = new DocumentSource(config, storageProvider, tempDir.toString());
    assertEquals(200L, ds.getMinRequestIntervalMs());
  }
}
