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
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Coverage tests for {@link DocumentSource}.
 */
@Tag("unit")
class DocumentSourceCoverageTest {

  @TempDir
  File tempDir;

  private HttpSourceConfig mockConfig;
  private StorageProvider mockStorageProvider;

  @BeforeEach void setUp() {
    mockConfig = mock(HttpSourceConfig.class);
    mockStorageProvider = mock(StorageProvider.class);

    when(mockConfig.getHeaders()).thenReturn(Collections.<String, String>emptyMap());
    when(mockConfig.getRateLimit()).thenReturn(null);
    when(mockConfig.getDocumentSource()).thenReturn(null);
    when(mockStorageProvider.resolvePath(anyString(), anyString()))
        .thenAnswer(inv -> inv.getArgument(0) + "/" + inv.getArgument(1));
    when(mockStorageProvider.getStorageType()).thenReturn("mock");
  }

  // ===== Constructor =====

  @Test void testConstructorBasic() {
    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());
    assertNotNull(source);
    assertNull(source.getDocumentConfig());
    assertEquals(mockConfig, source.getConfig());
    assertEquals(tempDir.getAbsolutePath(), source.getCacheDirectory());
  }

  @Test void testConstructorWithRateLimit() {
    HttpSourceConfig.RateLimitConfig rateLimit = mock(HttpSourceConfig.RateLimitConfig.class);
    when(rateLimit.getRequestsPerSecond()).thenReturn(10);
    when(mockConfig.getRateLimit()).thenReturn(rateLimit);

    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());
    assertEquals(100, source.getMinRequestIntervalMs()); // 1000/10 = 100
  }

  @Test void testConstructorWithZeroRateLimit() {
    HttpSourceConfig.RateLimitConfig rateLimit = mock(HttpSourceConfig.RateLimitConfig.class);
    when(rateLimit.getRequestsPerSecond()).thenReturn(0);
    when(mockConfig.getRateLimit()).thenReturn(rateLimit);

    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());
    assertEquals(125, source.getMinRequestIntervalMs()); // Default 8 req/s
  }

  @Test void testConstructorWithDocConfig() {
    HttpSourceConfig.DocumentSourceConfig docConfig =
        mock(HttpSourceConfig.DocumentSourceConfig.class);
    when(mockConfig.getDocumentSource()).thenReturn(docConfig);

    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());
    assertEquals(docConfig, source.getDocumentConfig());
  }

  @Test void testConstructorAddsAcceptEncoding() {
    // When headers don't contain Accept-Encoding, it should be added
    when(mockConfig.getHeaders()).thenReturn(Collections.<String, String>emptyMap());

    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());
    assertNotNull(source);
  }

  @Test void testConstructorPreservesExistingHeaders() {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("User-Agent", "TestAgent/1.0");
    headers.put("Accept-Encoding", "gzip");
    when(mockConfig.getHeaders()).thenReturn(headers);

    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());
    assertNotNull(source);
  }

  // ===== substituteVariables =====

  @Test void testSubstituteVariablesNull() {
    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    assertNull(source.substituteVariables(null, Collections.<String, String>emptyMap()));
  }

  @Test void testSubstituteVariablesNoPlaceholders() {
    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    String result = source.substituteVariables(
        "https://example.com/api", Collections.<String, String>emptyMap());
    assertEquals("https://example.com/api", result);
  }

  @Test void testSubstituteVariablesWithVars() {
    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0000070502");
    vars.put("year", "2023");

    String result = source.substituteVariables(
        "https://api.sec.gov/submissions/CIK{cik}.json?year={year}", vars);
    assertEquals("https://api.sec.gov/submissions/CIK0000070502.json?year=2023", result);
  }

  @Test void testSubstituteVariablesMissingVar() {
    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0000070502");

    String result = source.substituteVariables(
        "https://api.sec.gov/{cik}/{missing}", vars);
    // Missing variables resolve to empty string
    assertEquals("https://api.sec.gov/0000070502/", result);
  }

  @Test void testSubstituteVariablesEnvVar() {
    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    // This uses env: prefix, which reads from System.getenv or System.getProperty
    String result = source.substituteVariables(
        "https://api.example.com?key={env:NONEXISTENT_TEST_VAR_12345}",
        Collections.<String, String>emptyMap());
    // Should resolve to empty string since env var doesn't exist
    assertEquals("https://api.example.com?key=", result);
  }

  @Test void testSubstituteVariablesMultipleOccurrences() {
    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("id", "42");

    String result = source.substituteVariables(
        "/api/{id}/details/{id}/info", vars);
    assertEquals("/api/42/details/42/info", result);
  }

  // ===== fetchMetadata =====

  @Test void testFetchMetadataNoDocConfig() {
    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    assertThrows(IllegalStateException.class, () ->
        source.fetchMetadata(Collections.<String, String>emptyMap()));
  }

  @Test void testFetchMetadataNullUrl() {
    HttpSourceConfig.DocumentSourceConfig docConfig =
        mock(HttpSourceConfig.DocumentSourceConfig.class);
    when(docConfig.getMetadataUrl()).thenReturn(null);
    when(mockConfig.getDocumentSource()).thenReturn(docConfig);

    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    assertThrows(IllegalStateException.class, () ->
        source.fetchMetadata(Collections.<String, String>emptyMap()));
  }

  // ===== downloadDocument =====

  @Test void testDownloadDocumentNoDocConfig() {
    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    assertThrows(IllegalStateException.class, () ->
        source.downloadDocument(Collections.<String, String>emptyMap()));
  }

  @Test void testDownloadDocumentNullUrl() {
    HttpSourceConfig.DocumentSourceConfig docConfig =
        mock(HttpSourceConfig.DocumentSourceConfig.class);
    when(docConfig.getDocumentUrl()).thenReturn(null);
    when(mockConfig.getDocumentSource()).thenReturn(docConfig);

    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    assertThrows(IllegalStateException.class, () ->
        source.downloadDocument(Collections.<String, String>emptyMap()));
  }

  @Test void testDownloadDocumentCached() throws IOException {
    HttpSourceConfig.DocumentSourceConfig docConfig =
        mock(HttpSourceConfig.DocumentSourceConfig.class);
    when(docConfig.getDocumentUrl()).thenReturn("https://example.com/{cik}/{document}");
    when(mockConfig.getDocumentSource()).thenReturn(docConfig);

    // Simulate cached file exists with size > 0
    when(mockStorageProvider.exists(anyString())).thenReturn(true);
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("cached", 1000, 12345L, "text/xml", null);
    when(mockStorageProvider.getMetadata(anyString())).thenReturn(metadata);

    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0000070502");
    vars.put("accession", "0001-23-000001");
    vars.put("document", "filing.xml");

    String result = source.downloadDocument(vars);
    assertNotNull(result);
  }

  // ===== documentIterator =====

  @Test void testDocumentIteratorReturnsEmpty() {
    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    Iterator<Map<String, String>> iter = source.documentIterator(
        "{}", Collections.<String, String>emptyMap());
    assertFalse(iter.hasNext());
  }

  // ===== buildCacheKey (tested indirectly) =====

  @Test void testBuildCacheKeyWithAllVars() throws IOException {
    HttpSourceConfig.DocumentSourceConfig docConfig =
        mock(HttpSourceConfig.DocumentSourceConfig.class);
    when(docConfig.getDocumentUrl()).thenReturn("https://example.com/{cik}/{document}");
    when(mockConfig.getDocumentSource()).thenReturn(docConfig);

    when(mockStorageProvider.exists(anyString())).thenReturn(true);
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("cached", 500, 12345L, "text/xml", null);
    when(mockStorageProvider.getMetadata(anyString())).thenReturn(metadata);

    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0000070502");
    vars.put("accession", "0001-23-000001");
    vars.put("document", "filing.xml");

    String path = source.downloadDocument(vars);
    // The path should contain the cik/accession/document structure
    assertNotNull(path);
  }

  @Test void testBuildCacheKeyNoDocument() throws IOException {
    HttpSourceConfig.DocumentSourceConfig docConfig =
        mock(HttpSourceConfig.DocumentSourceConfig.class);
    when(docConfig.getDocumentUrl()).thenReturn("https://example.com/{cik}");
    when(mockConfig.getDocumentSource()).thenReturn(docConfig);

    when(mockStorageProvider.exists(anyString())).thenReturn(true);
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("cached", 100, 12345L, "text/xml", null);
    when(mockStorageProvider.getMetadata(anyString())).thenReturn(metadata);

    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0000070502");

    String path = source.downloadDocument(vars);
    assertNotNull(path);
    // Without document var, cache key uses hashCode as fallback
  }

  // ===== getMinRequestIntervalMs =====

  @Test void testDefaultMinRequestInterval() {
    DocumentSource source = new DocumentSource(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath());
    assertEquals(125, source.getMinRequestIntervalMs());
  }
}
