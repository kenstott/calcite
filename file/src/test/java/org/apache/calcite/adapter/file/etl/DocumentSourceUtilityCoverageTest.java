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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for static utility methods in {@link DocumentSource}
 * and {@link DocumentETLProcessor}.
 *
 * <p>These tests use reflection to access private static methods in order to
 * verify their behavior in isolation, covering HTTP retry logic, exception
 * classification, exponential backoff, URL variable substitution, cache key
 * generation, JSON parsing utilities, bracket matching, accession year
 * extraction, and transient error detection.
 */
@Tag("unit")
class DocumentSourceUtilityCoverageTest {

  // -- Reflection handles for DocumentSource private static methods --
  private static Method isRetryableHttpStatus;
  private static Method isRetryableException;
  private static Method retryDelay;
  private static Method sleepQuietly;

  // -- Reflection handles for DocumentETLProcessor private methods --
  private static Method extractYearFromAccession;
  private static Method isTransientError;

  @BeforeAll
  static void initReflection() throws Exception {
    // DocumentSource methods
    isRetryableHttpStatus =
        DocumentSource.class.getDeclaredMethod("isRetryableHttpStatus", int.class);
    isRetryableHttpStatus.setAccessible(true);

    isRetryableException =
        DocumentSource.class.getDeclaredMethod("isRetryableException", IOException.class);
    isRetryableException.setAccessible(true);

    retryDelay = DocumentSource.class.getDeclaredMethod("retryDelay", int.class);
    retryDelay.setAccessible(true);

    sleepQuietly = DocumentSource.class.getDeclaredMethod("sleepQuietly", long.class);
    sleepQuietly.setAccessible(true);

    // DocumentETLProcessor private methods
    extractYearFromAccession =
        DocumentETLProcessor.class.getDeclaredMethod("extractYearFromAccession", String.class);
    extractYearFromAccession.setAccessible(true);

    isTransientError =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    isTransientError.setAccessible(true);
  }

  // -----------------------------------------------------------------------
  // Helper: invoke private static boolean methods without checked exceptions
  // -----------------------------------------------------------------------

  private static boolean invokeRetryableHttpStatus(int code) {
    try {
      return (Boolean) isRetryableHttpStatus.invoke(null, code);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean invokeRetryableException(IOException ex) {
    try {
      return (Boolean) isRetryableException.invoke(null, ex);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private static long invokeRetryDelay(int attempt) {
    try {
      return (Long) retryDelay.invoke(null, attempt);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private static void invokeSleepQuietly(long millis) {
    try {
      sleepQuietly.invoke(null, millis);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private static String invokeExtractYearFromAccession(String accession) {
    try {
      return (String) extractYearFromAccession.invoke(null, accession);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean invokeIsTransientError(IOException ex) {
    try {
      return (Boolean) isTransientError.invoke(null, ex);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  // =======================================================================
  // DocumentSource.isRetryableHttpStatus(int)
  // =======================================================================

  @Test void testRetryableHttpStatus429() {
    assertTrue(invokeRetryableHttpStatus(429),
        "HTTP 429 Too Many Requests should be retryable");
  }

  @Test void testRetryableHttpStatus500() {
    assertTrue(invokeRetryableHttpStatus(500),
        "HTTP 500 Internal Server Error should be retryable");
  }

  @Test void testRetryableHttpStatus502() {
    assertTrue(invokeRetryableHttpStatus(502),
        "HTTP 502 Bad Gateway should be retryable");
  }

  @Test void testRetryableHttpStatus503() {
    assertTrue(invokeRetryableHttpStatus(503),
        "HTTP 503 Service Unavailable should be retryable");
  }

  @Test void testRetryableHttpStatus504() {
    assertTrue(invokeRetryableHttpStatus(504),
        "HTTP 504 Gateway Timeout should be retryable");
  }

  @Test void testNonRetryableHttpStatus200() {
    assertFalse(invokeRetryableHttpStatus(200),
        "HTTP 200 OK should not be retryable");
  }

  @Test void testNonRetryableHttpStatus401() {
    assertFalse(invokeRetryableHttpStatus(401),
        "HTTP 401 Unauthorized should not be retryable");
  }

  @Test void testNonRetryableHttpStatus403() {
    assertFalse(invokeRetryableHttpStatus(403),
        "HTTP 403 Forbidden should not be retryable");
  }

  @Test void testNonRetryableHttpStatus404() {
    assertFalse(invokeRetryableHttpStatus(404),
        "HTTP 404 Not Found should not be retryable");
  }

  @Test void testNonRetryableHttpStatus301() {
    assertFalse(invokeRetryableHttpStatus(301),
        "HTTP 301 Moved Permanently should not be retryable");
  }

  @Test void testNonRetryableHttpStatus418() {
    assertFalse(invokeRetryableHttpStatus(418),
        "HTTP 418 should not be retryable");
  }

  // =======================================================================
  // DocumentSource.isRetryableException(IOException)
  // =======================================================================

  @Test void testRetryableExceptionConnectionReset() {
    assertTrue(invokeRetryableException(new IOException("Connection reset by peer")));
  }

  @Test void testRetryableExceptionTimedOut() {
    assertTrue(invokeRetryableException(new IOException("Read timed out")));
  }

  @Test void testRetryableExceptionTimeout() {
    assertTrue(invokeRetryableException(new IOException("Connection timeout")));
  }

  @Test void testRetryableExceptionTls() {
    assertTrue(invokeRetryableException(new IOException("TLS handshake failure")));
  }

  @Test void testRetryableExceptionSsl() {
    assertTrue(invokeRetryableException(new IOException("SSL connection reset")));
  }

  @Test void testRetryableExceptionHandshake() {
    assertTrue(invokeRetryableException(new IOException("Remote handshake failed")));
  }

  @Test void testRetryableExceptionBrokenPipe() {
    assertTrue(invokeRetryableException(new IOException("Broken pipe")));
  }

  @Test void testRetryableExceptionConnectionRefused() {
    assertTrue(invokeRetryableException(new IOException("Connection refused")));
  }

  @Test void testRetryableExceptionNoRouteToHost() {
    assertTrue(invokeRetryableException(new IOException("No route to host")));
  }

  @Test void testRetryableExceptionNetworkUnreachable() {
    assertTrue(invokeRetryableException(new IOException("Network is unreachable")));
  }

  @Test void testRetryableExceptionUnexpectedEndOfStream() {
    assertTrue(invokeRetryableException(new IOException("unexpected end of stream")));
  }

  @Test void testRetryableExceptionNullMessage() {
    // Null message => unknown, worth retrying
    assertTrue(invokeRetryableException(new IOException((String) null)));
  }

  @Test void testNonRetryableExceptionFileNotFound() {
    assertFalse(invokeRetryableException(new IOException("File not found: /tmp/missing")));
  }

  @Test void testNonRetryableExceptionPermission() {
    assertFalse(invokeRetryableException(new IOException("Permission denied")));
  }

  @Test void testRetryableExceptionSocketTimeout() {
    assertTrue(invokeRetryableException(new SocketTimeoutException("Read timed out")));
  }

  @Test void testNonRetryableExceptionMalformedUrl() {
    assertFalse(invokeRetryableException(new IOException("Malformed URL")));
  }

  @Test void testRetryableExceptionCaseInsensitiveTimeout() {
    assertTrue(invokeRetryableException(new IOException("CONNECTION TIMED OUT")));
  }

  // =======================================================================
  // DocumentSource.retryDelay(int)
  // =======================================================================

  @Test void testRetryDelayAttempt0() {
    assertEquals(1000L, invokeRetryDelay(0),
        "Attempt 0: 1000 * 2^0 = 1000ms");
  }

  @Test void testRetryDelayAttempt1() {
    assertEquals(2000L, invokeRetryDelay(1),
        "Attempt 1: 1000 * 2^1 = 2000ms");
  }

  @Test void testRetryDelayAttempt2() {
    assertEquals(4000L, invokeRetryDelay(2),
        "Attempt 2: 1000 * 2^2 = 4000ms");
  }

  @Test void testRetryDelayAttempt3() {
    assertEquals(8000L, invokeRetryDelay(3),
        "Attempt 3: 1000 * 2^3 = 8000ms");
  }

  @Test void testRetryDelayAttempt4() {
    assertEquals(16000L, invokeRetryDelay(4),
        "Attempt 4: 1000 * 2^4 = 16000ms");
  }

  @Test void testRetryDelayAttempt5() {
    assertEquals(32000L, invokeRetryDelay(5),
        "Attempt 5: 1000 * 2^5 = 32000ms");
  }

  @Test void testRetryDelayExponentialGrowth() {
    long delay0 = invokeRetryDelay(0);
    long delay1 = invokeRetryDelay(1);
    long delay2 = invokeRetryDelay(2);
    assertEquals(delay0 * 2, delay1, "Each attempt should double the delay");
    assertEquals(delay1 * 2, delay2, "Each attempt should double the delay");
  }

  // =======================================================================
  // DocumentSource.sleepQuietly(long)
  // =======================================================================

  @Test void testSleepQuietlyShortDuration() {
    long start = System.currentTimeMillis();
    invokeSleepQuietly(1L);
    long elapsed = System.currentTimeMillis() - start;
    // Should sleep at least 1ms but not unreasonably long
    assertTrue(elapsed < 500, "sleepQuietly(1) should complete quickly");
  }

  @Test void testSleepQuietlyInterruptHandling() {
    final Thread current = Thread.currentThread();

    // Schedule an interrupt from a separate thread
    Thread interrupter = new Thread(new Runnable() {
      @Override public void run() {
        try {
          Thread.sleep(50);
        } catch (InterruptedException ignored) {
          // ignore
        }
        current.interrupt();
      }
    });
    interrupter.start();

    invokeSleepQuietly(5000L);

    // After sleepQuietly handles the interrupt, the interrupt flag should be restored
    assertTrue(Thread.interrupted(),
        "sleepQuietly should restore interrupt flag when interrupted");
  }

  // =======================================================================
  // DocumentSource.substituteVariables(String, Map) -- public, no reflection
  // =======================================================================

  /**
   * Creates a minimal DocumentSource for testing the public substituteVariables method.
   * Uses mocks for dependencies that are not exercised by this method.
   */
  private DocumentSource createDocumentSourceForSubstitution() {
    HttpSourceConfig mockConfig =
        org.mockito.Mockito.mock(HttpSourceConfig.class);
    org.apache.calcite.adapter.file.storage.StorageProvider mockStorage =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    HttpSourceConfig.DocumentSourceConfig mockDocConfig =
        org.mockito.Mockito.mock(HttpSourceConfig.DocumentSourceConfig.class);
    HttpSourceConfig.RateLimitConfig mockRateLimit =
        org.mockito.Mockito.mock(HttpSourceConfig.RateLimitConfig.class);

    org.mockito.Mockito.when(mockConfig.getDocumentSource()).thenReturn(mockDocConfig);
    org.mockito.Mockito.when(mockConfig.getHeaders())
        .thenReturn(new HashMap<String, String>());
    org.mockito.Mockito.when(mockConfig.getRateLimit()).thenReturn(mockRateLimit);
    org.mockito.Mockito.when(mockRateLimit.getRequestsPerSecond()).thenReturn(10);

    return new DocumentSource(mockConfig, mockStorage, "/tmp/cache");
  }

  @Test void testSubstituteVariablesSimple() {
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0001234567");
    String result = ds.substituteVariables(
        "https://data.sec.gov/submissions/CIK{cik}.json", vars);
    assertEquals("https://data.sec.gov/submissions/CIK0001234567.json", result);
  }

  @Test void testSubstituteVariablesMultiple() {
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "123");
    vars.put("accession", "456");
    String result = ds.substituteVariables("{cik}/filings/{accession}", vars);
    assertEquals("123/filings/456", result);
  }

  @Test void testSubstituteVariablesMissing() {
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = new HashMap<String, String>();
    String result = ds.substituteVariables("prefix/{missing}/suffix", vars);
    assertEquals("prefix//suffix", result);
  }

  @Test void testSubstituteVariablesNullPattern() {
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = new HashMap<String, String>();
    assertNull(ds.substituteVariables(null, vars));
  }

  @Test void testSubstituteVariablesNoPlaceholders() {
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "123");
    String result = ds.substituteVariables("https://example.com/plain", vars);
    assertEquals("https://example.com/plain", result);
  }

  @Test void testSubstituteVariablesEmptyMap() {
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = Collections.emptyMap();
    String result = ds.substituteVariables("{a}{b}{c}", vars);
    assertEquals("", result);
  }

  @Test void testSubstituteVariablesEnvPrefix() {
    // The env: prefix falls back to System.getProperty if env var is not set
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = new HashMap<String, String>();

    // Use a property that almost certainly does not exist as env var
    String propKey = "test.substitute.env.var." + System.nanoTime();
    System.setProperty(propKey, "propValue");
    try {
      String result = ds.substituteVariables("{env:" + propKey + "}", vars);
      assertEquals("propValue", result);
    } finally {
      System.clearProperty(propKey);
    }
  }

  @Test void testSubstituteVariablesEnvPrefixMissing() {
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = new HashMap<String, String>();
    // Use an env var name that almost certainly does not exist
    String result = ds.substituteVariables(
        "{env:CALCITE_NONEXISTENT_VAR_XYZ_12345}", vars);
    assertEquals("", result);
  }

  @Test void testSubstituteVariablesSpecialCharsInValue() {
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("path", "a/b$c");
    String result = ds.substituteVariables("prefix/{path}/suffix", vars);
    assertEquals("prefix/a/b$c/suffix", result);
  }

  // =======================================================================
  // DocumentSource.buildCacheKey(Map) -- private, uses reflection
  // =======================================================================

  private static Method buildCacheKeyMethod;

  static {
    try {
      buildCacheKeyMethod =
          DocumentSource.class.getDeclaredMethod("buildCacheKey", Map.class);
      buildCacheKeyMethod.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private String invokeBuildCacheKey(DocumentSource ds, Map<String, String> vars) {
    try {
      return (String) buildCacheKeyMethod.invoke(ds, vars);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  @Test void testBuildCacheKeyWithAllFields() {
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0001234567");
    vars.put("accession", "0001234567-21-000042");
    vars.put("document", "filing.htm");
    String key = invokeBuildCacheKey(ds, vars);
    assertEquals("0001234567/000123456721000042/filing.htm", key);
  }

  @Test void testBuildCacheKeyWithCikOnly() {
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "999");
    // No accession, no document => fallback hash
    String key = invokeBuildCacheKey(ds, vars);
    assertTrue(key.startsWith("999/"), "Should start with cik path");
    assertTrue(key.endsWith(".dat"), "Should fall back to hash-based .dat name");
  }

  @Test void testBuildCacheKeyWithAccessionNoDashes() {
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("accession", "1234-56-789");
    vars.put("document", "index.html");
    String key = invokeBuildCacheKey(ds, vars);
    assertEquals("123456789/index.html", key);
  }

  @Test void testBuildCacheKeyEmptyMap() {
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = new HashMap<String, String>();
    String key = invokeBuildCacheKey(ds, vars);
    // No cik, no accession, no document => just fallback hash.dat
    assertTrue(key.endsWith(".dat"), "Empty map should produce hash-based key");
  }

  @Test void testBuildCacheKeyDocumentOnly() {
    DocumentSource ds = createDocumentSourceForSubstitution();
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("document", "report.xbrl");
    String key = invokeBuildCacheKey(ds, vars);
    assertEquals("report.xbrl", key);
  }

  // =======================================================================
  // DocumentETLProcessor.extractJsonStringField(String, String) -- static
  // =======================================================================

  @Test void testExtractJsonStringFieldSimple() {
    String json = "{\"name\":\"hello\"}";
    assertEquals("hello", DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonStringFieldMissing() {
    String json = "{\"name\":\"hello\"}";
    assertNull(DocumentETLProcessor.extractJsonStringField(json, "other"));
  }

  @Test void testExtractJsonStringFieldMultipleFields() {
    String json = "{\"a\":\"first\",\"b\":\"second\"}";
    assertEquals("first", DocumentETLProcessor.extractJsonStringField(json, "a"));
    assertEquals("second", DocumentETLProcessor.extractJsonStringField(json, "b"));
  }

  @Test void testExtractJsonStringFieldWithSpaces() {
    String json = "{\"name\" : \"spaced value\"}";
    assertEquals("spaced value",
        DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonStringFieldEmptyValue() {
    String json = "{\"name\":\"\"}";
    assertEquals("", DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonStringFieldNullValue() {
    // JSON null (not quoted) -- no opening quote found after colon
    String json = "{\"name\":null}";
    assertNull(DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonStringFieldNoColon() {
    // Malformed JSON -- key present but no colon
    String json = "{\"name\" \"value\"}";
    assertNull(DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonStringFieldDateValue() {
    String json = "{\"filingFrom\":\"2020-01-15\",\"filingTo\":\"2023-12-31\"}";
    assertEquals("2020-01-15",
        DocumentETLProcessor.extractJsonStringField(json, "filingFrom"));
    assertEquals("2023-12-31",
        DocumentETLProcessor.extractJsonStringField(json, "filingTo"));
  }

  @Test void testExtractJsonStringFieldWithNestedObjects() {
    String json = "{\"outer\":{\"inner\":\"deep\"},\"name\":\"top\"}";
    assertEquals("top", DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  // =======================================================================
  // DocumentETLProcessor.extractJsonIntField(String, String) -- static
  // =======================================================================

  @Test void testExtractJsonIntFieldSimple() {
    String json = "{\"count\":42}";
    assertEquals(42, DocumentETLProcessor.extractJsonIntField(json, "count"));
  }

  @Test void testExtractJsonIntFieldMissing() {
    String json = "{\"count\":42}";
    assertEquals(0, DocumentETLProcessor.extractJsonIntField(json, "other"));
  }

  @Test void testExtractJsonIntFieldNegative() {
    String json = "{\"value\":-7}";
    assertEquals(-7, DocumentETLProcessor.extractJsonIntField(json, "value"));
  }

  @Test void testExtractJsonIntFieldZero() {
    String json = "{\"filingCount\":0}";
    assertEquals(0, DocumentETLProcessor.extractJsonIntField(json, "filingCount"));
  }

  @Test void testExtractJsonIntFieldWithSpaces() {
    String json = "{\"count\" : 100}";
    assertEquals(100, DocumentETLProcessor.extractJsonIntField(json, "count"));
  }

  @Test void testExtractJsonIntFieldLargeNumber() {
    String json = "{\"filingCount\":866}";
    assertEquals(866, DocumentETLProcessor.extractJsonIntField(json, "filingCount"));
  }

  @Test void testExtractJsonIntFieldStringValue() {
    // Value is a string not a number => no digits found in expected position
    String json = "{\"count\":\"notanumber\"}";
    assertEquals(0, DocumentETLProcessor.extractJsonIntField(json, "count"));
  }

  @Test void testExtractJsonIntFieldNoColon() {
    String json = "{\"count\" 42}";
    assertEquals(0, DocumentETLProcessor.extractJsonIntField(json, "count"));
  }

  // =======================================================================
  // DocumentETLProcessor.findMatchingBracket(String, int) -- static
  // =======================================================================

  @Test void testFindMatchingBracketSquare() {
    String json = "[1,2,3]";
    assertEquals(6, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketCurly() {
    String json = "{\"a\":1}";
    assertEquals(6, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketNested() {
    String json = "[[1,[2]],3]";
    assertEquals(10, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketNestedCurly() {
    String json = "{\"a\":{\"b\":{\"c\":1}}}";
    assertEquals(json.length() - 1,
        DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketStringContainingBrackets() {
    // Brackets inside strings should be ignored
    String json = "{\"key\":\"value[0]{1}\"}";
    assertEquals(json.length() - 1,
        DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketEscapedQuote() {
    // Escaped quote inside string should not end the string
    String json = "{\"key\":\"val\\\"ue\"}";
    assertEquals(json.length() - 1,
        DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketEmpty() {
    assertEquals(1, DocumentETLProcessor.findMatchingBracket("[]", 0));
    assertEquals(1, DocumentETLProcessor.findMatchingBracket("{}", 0));
  }

  @Test void testFindMatchingBracketUnmatched() {
    assertEquals(-1, DocumentETLProcessor.findMatchingBracket("[1,2,3", 0));
  }

  @Test void testFindMatchingBracketInvalidChar() {
    // Starting character is not a bracket
    assertEquals(-1, DocumentETLProcessor.findMatchingBracket("abc", 0));
  }

  @Test void testFindMatchingBracketMidString() {
    String json = "prefix[inner]suffix";
    assertEquals(12, DocumentETLProcessor.findMatchingBracket(json, 6));
  }

  @Test void testFindMatchingBracketDeepNesting() {
    String json = "[[[[]]]]";
    assertEquals(7, DocumentETLProcessor.findMatchingBracket(json, 0));
    assertEquals(6, DocumentETLProcessor.findMatchingBracket(json, 1));
    assertEquals(5, DocumentETLProcessor.findMatchingBracket(json, 2));
    assertEquals(4, DocumentETLProcessor.findMatchingBracket(json, 3));
  }

  // =======================================================================
  // DocumentETLProcessor.extractYearFromAccession(String) -- private
  // =======================================================================

  @Test void testExtractYearModernAccession() {
    // Standard modern accession: XXXXXXXXXX-21-NNNNNN => 2021
    assertEquals("2021", invokeExtractYearFromAccession("0001234567-21-000042"));
  }

  @Test void testExtractYearOlderAccession() {
    // 99 => 1999 (> 50)
    assertEquals("1999", invokeExtractYearFromAccession("0001234567-99-000001"));
  }

  @Test void testExtractYearBoundary50() {
    // 50 => 2050 (<= 50)
    assertEquals("2050", invokeExtractYearFromAccession("0001234567-50-000001"));
  }

  @Test void testExtractYearBoundary51() {
    // 51 => 1951 (> 50)
    assertEquals("1951", invokeExtractYearFromAccession("0001234567-51-000001"));
  }

  @Test void testExtractYearBoundary00() {
    // 00 => 2000
    assertEquals("2000", invokeExtractYearFromAccession("0001234567-00-000001"));
  }

  @Test void testExtractYearNullAccession() {
    assertNull(invokeExtractYearFromAccession(null));
  }

  @Test void testExtractYearShortAccession() {
    assertNull(invokeExtractYearFromAccession("short"));
  }

  @Test void testExtractYearNoDash() {
    assertNull(invokeExtractYearFromAccession("1234567890123"));
  }

  @Test void testExtractYearDashAtEnd() {
    // Dash at end, not enough chars after
    assertNull(invokeExtractYearFromAccession("123456789012-"));
  }

  @Test void testExtractYearNonNumericAfterDash() {
    // Non-numeric after dash
    assertNull(invokeExtractYearFromAccession("0001234567-AB-000001"));
  }

  // =======================================================================
  // DocumentETLProcessor.isTransientError(IOException) -- private static
  // =======================================================================

  @Test void testIsTransientErrorHttp500() {
    assertTrue(invokeIsTransientError(new IOException("HTTP 500 Internal Server Error")));
  }

  @Test void testIsTransientErrorHttp502() {
    assertTrue(invokeIsTransientError(new IOException("HTTP 502 Bad Gateway")));
  }

  @Test void testIsTransientErrorHttp503() {
    assertTrue(invokeIsTransientError(new IOException("HTTP 503 Service Unavailable")));
  }

  @Test void testIsTransientErrorHttp504() {
    assertTrue(invokeIsTransientError(new IOException("HTTP 504 Gateway Timeout")));
  }

  @Test void testIsTransientErrorTimedOut() {
    assertTrue(invokeIsTransientError(new IOException("Read timed out")));
  }

  @Test void testIsTransientErrorTimeout() {
    assertTrue(invokeIsTransientError(new IOException("Connection timeout")));
  }

  @Test void testIsTransientErrorConnectionReset() {
    assertTrue(invokeIsTransientError(new IOException("Connection reset by peer")));
  }

  @Test void testIsTransientErrorConnectionClosed() {
    assertTrue(invokeIsTransientError(new IOException("Connection closed prematurely")));
  }

  @Test void testIsTransientErrorBrokenPipe() {
    assertTrue(invokeIsTransientError(new IOException("Broken pipe")));
  }

  @Test void testIsTransientErrorPrematureEnd() {
    assertTrue(invokeIsTransientError(new IOException("Premature end of stream")));
  }

  @Test void testIsTransientErrorPrematureEof() {
    assertTrue(invokeIsTransientError(new IOException("Premature EOF")));
  }

  @Test void testIsTransientErrorUnexpectedEnd() {
    assertTrue(invokeIsTransientError(new IOException("Unexpected end of input")));
  }

  @Test void testIsTransientErrorEndOfZlib() {
    assertTrue(invokeIsTransientError(new IOException("End of ZLIB input stream")));
  }

  @Test void testIsTransientErrorEndOfContentLength() {
    assertTrue(invokeIsTransientError(
        new IOException("end of content-length delimited message body")));
  }

  @Test void testIsTransientErrorEofException() {
    assertTrue(invokeIsTransientError(new EOFException("stream ended")));
  }

  @Test void testIsTransientErrorWrappedCause() {
    // Transient root cause wrapped in non-transient outer exception
    IOException inner = new IOException("Connection reset");
    IOException outer = new IOException("Request failed", inner);
    assertTrue(invokeIsTransientError(outer),
        "Should detect transient error in cause chain");
  }

  @Test void testIsTransientErrorWrappedEofException() {
    EOFException inner = new EOFException("stream ended");
    IOException outer = new IOException("Read failed", inner);
    assertTrue(invokeIsTransientError(outer),
        "Should detect EOFException in cause chain");
  }

  @Test void testIsNotTransientErrorFileNotFound() {
    assertFalse(invokeIsTransientError(new IOException("File not found")));
  }

  @Test void testIsNotTransientErrorPermissionDenied() {
    assertFalse(invokeIsTransientError(new IOException("Permission denied")));
  }

  @Test void testIsNotTransientErrorHttp404() {
    assertFalse(invokeIsTransientError(new IOException("HTTP 404 Not Found")));
  }

  @Test void testIsNotTransientErrorNullMessage() {
    // Null message with no cause and not EOFException => false
    assertFalse(invokeIsTransientError(new IOException((String) null)));
  }

  @Test void testIsTransientErrorCaseInsensitive() {
    assertTrue(invokeIsTransientError(new IOException("CONNECTION RESET BY PEER")));
  }
}
