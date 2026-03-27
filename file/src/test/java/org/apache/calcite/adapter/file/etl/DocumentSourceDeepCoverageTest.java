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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link DocumentSource}.
 * Covers variable substitution, cache key building, rate limiting config,
 * retry helpers, and accessor methods.
 */
@Tag("unit")
class DocumentSourceDeepCoverageTest {

  @TempDir
  Path tempDir;

  // ===== Variable Substitution Tests =====

  @Test void testSubstituteVariablesSimple() {
    DocumentSource source = createDocumentSource(null);

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("cik", "0001234567");
    variables.put("year", "2020");

    String result = source.substituteVariables(
        "https://api.example.com/{cik}/filings/{year}", variables);
    assertEquals("https://api.example.com/0001234567/filings/2020", result);
  }

  @Test void testSubstituteVariablesNullPattern() {
    DocumentSource source = createDocumentSource(null);
    assertNull(source.substituteVariables(null, Collections.<String, String>emptyMap()));
  }

  @Test void testSubstituteVariablesMissingVar() {
    DocumentSource source = createDocumentSource(null);
    Map<String, String> variables = new HashMap<String, String>();
    variables.put("cik", "123");

    String result = source.substituteVariables(
        "https://api.example.com/{cik}/filings/{missing}", variables);
    assertEquals("https://api.example.com/123/filings/", result);
  }

  @Test void testSubstituteVariablesNoPlaceholders() {
    DocumentSource source = createDocumentSource(null);
    String result = source.substituteVariables(
        "https://api.example.com/static", Collections.<String, String>emptyMap());
    assertEquals("https://api.example.com/static", result);
  }

  @Test void testSubstituteVariablesEnvVar() {
    DocumentSource source = createDocumentSource(null);
    Map<String, String> variables = new HashMap<String, String>();

    // Set a system property for env: prefix test
    System.setProperty("TEST_COVERAGE_VAR", "test_value");
    try {
      String result = source.substituteVariables(
          "https://api.example.com/{env:TEST_COVERAGE_VAR}/data", variables);
      assertEquals("https://api.example.com/test_value/data", result);
    } finally {
      System.clearProperty("TEST_COVERAGE_VAR");
    }
  }

  @Test void testSubstituteVariablesEnvVarMissing() {
    DocumentSource source = createDocumentSource(null);
    Map<String, String> variables = new HashMap<String, String>();

    String result = source.substituteVariables(
        "https://api.example.com/{env:NONEXISTENT_COVERAGE_TEST_VAR}/data", variables);
    assertEquals("https://api.example.com//data", result);
  }

  // ===== buildCacheKey via Reflection =====

  @Test void testBuildCacheKeyViaReflection() throws Exception {
    DocumentSource source = createDocumentSource(null);

    Method buildCacheKey = DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
    buildCacheKey.setAccessible(true);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0001234567");
    vars.put("accession", "0001234567-20-123456");
    vars.put("document", "filing.xml");

    String key = (String) buildCacheKey.invoke(source, vars);
    assertNotNull(key);
    assertTrue(key.contains("0001234567"));
    assertTrue(key.contains("filing.xml"));
  }

  @Test void testBuildCacheKeyNoDocument() throws Exception {
    DocumentSource source = createDocumentSource(null);

    Method buildCacheKey = DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
    buildCacheKey.setAccessible(true);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "123");
    vars.put("accession", "0001234567-20-123456");

    String key = (String) buildCacheKey.invoke(source, vars);
    assertNotNull(key);
    assertTrue(key.contains("123"));
    assertTrue(key.endsWith(".dat"));
  }

  @Test void testBuildCacheKeyNoCik() throws Exception {
    DocumentSource source = createDocumentSource(null);

    Method buildCacheKey = DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
    buildCacheKey.setAccessible(true);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("document", "file.xml");

    String key = (String) buildCacheKey.invoke(source, vars);
    assertNotNull(key);
    assertTrue(key.contains("file.xml"));
  }

  // ===== documentIterator test =====

  @Test void testDocumentIteratorReturnsEmpty() {
    DocumentSource source = createDocumentSource(null);
    Iterator<Map<String, String>> iter = source.documentIterator(
        "{}", Collections.<String, String>emptyMap());
    assertNotNull(iter);
    assertFalse(iter.hasNext());
  }

  // ===== Accessor tests =====

  @Test void testAccessors() {
    DocumentSource source = createDocumentSource(null);
    assertNotNull(source.getConfig());
    assertEquals("/cache", source.getCacheDirectory());
    assertTrue(source.getMinRequestIntervalMs() > 0);
  }

  @Test void testAccessorsWithRateLimit() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("url", "https://example.com");
    Map<String, Object> rateLimit = new HashMap<String, Object>();
    rateLimit.put("requestsPerSecond", 5);
    configMap.put("rateLimit", rateLimit);
    HttpSourceConfig config = HttpSourceConfig.fromMap(configMap);

    StorageProvider mockStorage =
        org.mockito.Mockito.mock(StorageProvider.class);
    DocumentSource source = new DocumentSource(config, mockStorage, "/cache");
    // 1000 / 5 = 200ms
    assertEquals(200L, source.getMinRequestIntervalMs());
  }

  // ===== isRetryableHttpStatus via Reflection =====

  @Test void testIsRetryableHttpStatusViaReflection() throws Exception {
    Method isRetryable = DocumentSource.class.getDeclaredMethod(
        "isRetryableHttpStatus", int.class);
    isRetryable.setAccessible(true);

    assertTrue((Boolean) isRetryable.invoke(null, 429));
    assertTrue((Boolean) isRetryable.invoke(null, 500));
    assertTrue((Boolean) isRetryable.invoke(null, 502));
    assertTrue((Boolean) isRetryable.invoke(null, 503));
    assertTrue((Boolean) isRetryable.invoke(null, 504));
    assertFalse((Boolean) isRetryable.invoke(null, 200));
    assertFalse((Boolean) isRetryable.invoke(null, 404));
    assertFalse((Boolean) isRetryable.invoke(null, 403));
  }

  // ===== isRetryableException via Reflection =====

  @Test void testIsRetryableExceptionViaReflection() throws Exception {
    Method isRetryable = DocumentSource.class.getDeclaredMethod(
        "isRetryableException", java.io.IOException.class);
    isRetryable.setAccessible(true);

    assertTrue((Boolean) isRetryable.invoke(null, new java.io.IOException("connection reset")));
    assertTrue((Boolean) isRetryable.invoke(null, new java.io.IOException("TLS error")));
    assertTrue((Boolean) isRetryable.invoke(null, new java.io.IOException("SSL handshake failed")));
    assertTrue((Boolean) isRetryable.invoke(null, new java.io.IOException("timed out")));
    assertTrue((Boolean) isRetryable.invoke(null, new java.io.IOException("broken pipe")));
    assertTrue((Boolean) isRetryable.invoke(null, new java.io.IOException("connection refused")));
    assertTrue((Boolean) isRetryable.invoke(null, new java.io.IOException("no route to host")));
    assertTrue((Boolean) isRetryable.invoke(null, new java.io.IOException("network is unreachable")));
    assertTrue((Boolean) isRetryable.invoke(null, new java.io.IOException("unexpected end of stream")));

    // Null message = retryable (unknown)
    assertTrue((Boolean) isRetryable.invoke(null, new java.io.IOException((String) null)));

    // Not retryable
    assertFalse((Boolean) isRetryable.invoke(null, new java.io.IOException("file not found")));
  }

  // ===== retryDelay via Reflection =====

  @Test void testRetryDelayViaReflection() throws Exception {
    Method retryDelay = DocumentSource.class.getDeclaredMethod("retryDelay", int.class);
    retryDelay.setAccessible(true);

    assertEquals(1000L, retryDelay.invoke(null, 0));
    assertEquals(2000L, retryDelay.invoke(null, 1));
    assertEquals(4000L, retryDelay.invoke(null, 2));
  }

  // ===== fetchMetadata without config =====

  @Test void testFetchMetadataNoDocumentConfig() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("url", "https://example.com");
    HttpSourceConfig config = HttpSourceConfig.fromMap(configMap);

    StorageProvider mockStorage =
        org.mockito.Mockito.mock(StorageProvider.class);
    DocumentSource source = new DocumentSource(config, mockStorage, "/cache");

    assertThrows(IllegalStateException.class, () ->
        source.fetchMetadata(Collections.<String, String>emptyMap()));
  }

  // ===== downloadDocument without config =====

  @Test void testDownloadDocumentNoDocumentConfig() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("url", "https://example.com");
    HttpSourceConfig config = HttpSourceConfig.fromMap(configMap);

    StorageProvider mockStorage =
        org.mockito.Mockito.mock(StorageProvider.class);
    DocumentSource source = new DocumentSource(config, mockStorage, "/cache");

    assertThrows(IllegalStateException.class, () ->
        source.downloadDocument(Collections.<String, String>emptyMap()));
  }

  private DocumentSource createDocumentSource(HttpSourceConfig.RateLimitConfig rateLimit) {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("url", "https://example.com");
    HttpSourceConfig config = HttpSourceConfig.fromMap(configMap);

    StorageProvider mockStorage =
        org.mockito.Mockito.mock(StorageProvider.class);
    return new DocumentSource(config, mockStorage, "/cache");
  }
}
