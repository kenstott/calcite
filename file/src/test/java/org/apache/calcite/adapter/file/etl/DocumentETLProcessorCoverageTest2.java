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

import org.apache.calcite.adapter.file.converters.FileConverter;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Extended coverage tests for {@link DocumentETLProcessor} focusing on
 * processEntity, processEntities, processEntitiesParallel,
 * processDocumentWithRetry, isTransientError deeper paths,
 * fetchPaginationFile, readStorageFile, and variable handling.
 */
@Tag("unit")
class DocumentETLProcessorCoverageTest2 {

  @TempDir
  Path tempDir;

  private HttpSourceConfig mockConfig;
  private StorageProvider mockStorageProvider;
  private FileConverter mockConverter;

  @BeforeEach
  void setUp() throws IOException {
    mockConfig = mock(HttpSourceConfig.class);
    mockStorageProvider = mock(StorageProvider.class);
    mockConverter = mock(FileConverter.class);

    when(mockConfig.getHeaders()).thenReturn(Collections.<String, String>emptyMap());
    when(mockConfig.getRateLimit()).thenReturn(null);
    when(mockConfig.getDocumentSource()).thenReturn(null);
    when(mockStorageProvider.resolvePath(anyString(), anyString()))
        .thenAnswer(inv -> inv.getArgument(0) + "/" + inv.getArgument(1));
    when(mockStorageProvider.getStorageType()).thenReturn("mock");
  }

  // ====================================================================
  // isTransientError - deeper coverage
  // ====================================================================

  @Test void testIsTransientErrorHttp503() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(null, new IOException("HTTP 503 Service Unavailable")));
  }

  @Test void testIsTransientErrorHttp504() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(null, new IOException("HTTP 504 Gateway Timeout")));
  }

  @Test void testIsTransientErrorTimeout() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(null, new IOException("Read timed out")));
    assertTrue((Boolean) method.invoke(null, new IOException("Connect timeout occurred")));
  }

  @Test void testIsTransientErrorEndOfZlib() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(null, new IOException("end of zlib stream")));
  }

  @Test void testIsTransientErrorEndOfContentLength() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(null, new IOException("end of content-length delimited")));
  }

  @Test void testIsTransientErrorBrokenPipe() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(null, new IOException("Broken pipe")));
  }

  @Test void testIsTransientErrorPrematureEof() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(null, new IOException("Premature EOF")));
  }

  @Test void testIsTransientErrorUnexpectedEnd() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(null, new IOException("Unexpected end of stream")));
  }

  @Test void testIsTransientErrorWrappedEofException() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    java.io.EOFException eof = new java.io.EOFException("data ended");
    IOException wrapper = new IOException("wrapper", eof);
    assertTrue((Boolean) method.invoke(null, wrapper));
  }

  @Test void testIsTransientErrorDeeplyNested() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    // Two levels of nesting, transient at the root cause
    Exception rootCause = new IOException("connection reset by peer");
    IOException level1 = new IOException("read failed", rootCause);
    IOException level2 = new IOException("outer wrapper", level1);
    assertTrue((Boolean) method.invoke(null, level2));
  }

  @Test void testIsTransientErrorNonTransient() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    assertFalse((Boolean) method.invoke(null, new IOException("Permission denied")));
    assertFalse((Boolean) method.invoke(null, new IOException("No such file or directory")));
    assertFalse((Boolean) method.invoke(null, new IOException("Malformed XML")));
  }

  @Test void testIsTransientErrorNullMessage() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    assertFalse((Boolean) method.invoke(null, new IOException((String) null)));
  }

  @Test void testIsTransientErrorConnectionClosed() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(null, new IOException("Connection closed prematurely")));
  }

  @Test void testIsTransientErrorPrematureEnd() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(null, new IOException("premature end of data")));
  }

  // ====================================================================
  // extractYearFromAccession - deeper coverage
  // ====================================================================

  @Test void testExtractYearFromAccessionBoundary50() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("extractYearFromAccession", String.class);
    method.setAccessible(true);

    DocumentETLProcessor processor = createProcessor();

    // Year 50 maps to 2050
    assertEquals("2050", method.invoke(processor, "0001234567-50-123456"));
    // Year 51 maps to 1951
    assertEquals("1951", method.invoke(processor, "0001234567-51-123456"));
    // Year 00 maps to 2000
    assertEquals("2000", method.invoke(processor, "0001234567-00-123456"));
    // Year 99 maps to 1999
    assertEquals("1999", method.invoke(processor, "0001234567-99-123456"));
  }

  @Test void testExtractYearFromAccessionNull() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("extractYearFromAccession", String.class);
    method.setAccessible(true);

    DocumentETLProcessor processor = createProcessor();
    assertNull(method.invoke(processor, (Object) null));
  }

  @Test void testExtractYearFromAccessionTooShort() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("extractYearFromAccession", String.class);
    method.setAccessible(true);

    DocumentETLProcessor processor = createProcessor();
    assertNull(method.invoke(processor, "short"));
  }

  @Test void testExtractYearFromAccessionNoDash() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("extractYearFromAccession", String.class);
    method.setAccessible(true);

    DocumentETLProcessor processor = createProcessor();
    assertNull(method.invoke(processor, "1234567890123"));
  }

  @Test void testExtractYearFromAccessionDashTooClose() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("extractYearFromAccession", String.class);
    method.setAccessible(true);

    DocumentETLProcessor processor = createProcessor();
    // Dash at position that leaves insufficient chars after
    assertNull(method.invoke(processor, "01234567890-1"));
  }

  @Test void testExtractYearFromAccessionNonNumericYear() throws Exception {
    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("extractYearFromAccession", String.class);
    method.setAccessible(true);

    DocumentETLProcessor processor = createProcessor();
    assertNull(method.invoke(processor, "0001234567-AB-123456"));
  }

  // ====================================================================
  // processDocumentWithRetry - via reflection
  // ====================================================================

  @Test void testProcessDocumentWithRetrySuccess() throws Exception {
    when(mockConverter.convert(anyString(), anyString(), any(ConversionMetadata.class)))
        .thenReturn(Arrays.asList("/output/file.parquet"));

    DocumentETLProcessor processor = createProcessor();

    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("processDocumentWithRetry", Map.class, DocumentSource.class, String.class);
    method.setAccessible(true);

    DocumentSource mockDocSource = mock(DocumentSource.class);
    when(mockDocSource.downloadDocument(any(Map.class))).thenReturn("/tmp/doc.xml");

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("document", "test.xml");

    @SuppressWarnings("unchecked")
    List<String> result =
        (List<String>) method.invoke(processor, docVars, mockDocSource, tempDir.toString());

    assertEquals(1, result.size());
    assertEquals("/output/file.parquet", result.get(0));
  }

  @Test void testProcessDocumentWithRetryNonTransientFailure() throws Exception {
    when(mockConverter.convert(anyString(), anyString(), any(ConversionMetadata.class)))
        .thenThrow(new IOException("Malformed XML document"));

    DocumentETLProcessor processor = createProcessor();

    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("processDocumentWithRetry", Map.class, DocumentSource.class, String.class);
    method.setAccessible(true);

    DocumentSource mockDocSource = mock(DocumentSource.class);
    when(mockDocSource.downloadDocument(any(Map.class))).thenReturn("/tmp/doc.xml");

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("document", "bad.xml");

    try {
      method.invoke(processor, docVars, mockDocSource, tempDir.toString());
      fail("Expected IOException to be thrown");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IOException);
    }
  }

  // ====================================================================
  // readStorageFile - via reflection
  // ====================================================================

  @Test void testReadStorageFile() throws Exception {
    String content = "{\"test\":\"data\"}";
    when(mockStorageProvider.openInputStream(anyString()))
        .thenReturn(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));

    DocumentETLProcessor processor = createProcessor();

    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("readStorageFile", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(processor, "/cache/test.json");
    assertTrue(result.contains("\"test\":\"data\""));
  }

  @Test void testReadStorageFileMultipleLines() throws Exception {
    String content = "line1\nline2\nline3";
    when(mockStorageProvider.openInputStream(anyString()))
        .thenReturn(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));

    DocumentETLProcessor processor = createProcessor();

    Method method =
        DocumentETLProcessor.class.getDeclaredMethod("readStorageFile", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(processor, "/cache/multi.txt");
    assertTrue(result.contains("line1"));
    assertTrue(result.contains("line2"));
    assertTrue(result.contains("line3"));
  }

  // ====================================================================
  // parseDocuments - with pagination (three-arg version)
  // ====================================================================

  @Test void testParseDocumentsWithNullDocumentSource() {
    DocumentETLProcessor processor = createProcessor();

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    String json = "{\"filings\":{\"recent\":{"
        + "\"accessionNumber\":[\"0001-23-000001\"],"
        + "\"form\":[\"10-K\"],"
        + "\"filingDate\":[\"2023-01-15\"],"
        + "\"primaryDocument\":[\"doc.htm\"]"
        + "},\"files\":[]}}";

    // Three-arg overload with null documentSource
    Iterator<Map<String, String>> docs = processor.parseDocuments(json, baseVars, null);
    assertTrue(docs.hasNext());
  }

  @Test void testParseDocumentsEmptyJson() {
    DocumentETLProcessor processor = createProcessor();

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    Iterator<Map<String, String>> docs = processor.parseDocuments("{}", baseVars);
    assertFalse(docs.hasNext());
  }

  @Test void testParseDocumentsWithForm4SlashPrefix() {
    DocumentETLProcessor processor = createProcessor();

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    String json = "{\"filings\":{\"recent\":{"
        + "\"accessionNumber\":[\"0001-23-000001\"],"
        + "\"form\":[\"4/A\"],"
        + "\"filingDate\":[\"2023-01-15\"],"
        + "\"primaryDocument\":[\"xslF345X03/wf-form4a_test.xml\"]"
        + "},\"files\":[]}}";

    Iterator<Map<String, String>> docs = processor.parseDocuments(json, baseVars);
    assertTrue(docs.hasNext());
    Map<String, String> doc = docs.next();
    assertEquals("wf-form4a_test.xml", doc.get("document"));
  }

  @Test void testParseDocumentsWithForm5SlashPrefix() {
    DocumentETLProcessor processor = createProcessor();

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    String json = "{\"filings\":{\"recent\":{"
        + "\"accessionNumber\":[\"0001-23-000001\"],"
        + "\"form\":[\"5/A\"],"
        + "\"filingDate\":[\"2023-01-15\"],"
        + "\"primaryDocument\":[\"xslF345X05/wf-form5a_test.xml\"]"
        + "},\"files\":[]}}";

    Iterator<Map<String, String>> docs = processor.parseDocuments(json, baseVars);
    assertTrue(docs.hasNext());
    Map<String, String> doc = docs.next();
    assertEquals("wf-form5a_test.xml", doc.get("document"));
  }

  @Test void testParseDocumentsWithForm3SlashPrefix() {
    DocumentETLProcessor processor = createProcessor();

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    String json = "{\"filings\":{\"recent\":{"
        + "\"accessionNumber\":[\"0001-23-000001\"],"
        + "\"form\":[\"3/A\"],"
        + "\"filingDate\":[\"2023-01-15\"],"
        + "\"primaryDocument\":[\"xslF345X01/wf-form3a_test.xml\"]"
        + "},\"files\":[]}}";

    Iterator<Map<String, String>> docs = processor.parseDocuments(json, baseVars);
    assertTrue(docs.hasNext());
    Map<String, String> doc = docs.next();
    assertEquals("wf-form3a_test.xml", doc.get("document"));
  }

  @Test void testParseDocumentsNullCik() {
    DocumentETLProcessor processor = createProcessor();

    Map<String, String> baseVars = new HashMap<String, String>();
    // No cik

    String json = "{\"filings\":{\"recent\":{"
        + "\"accessionNumber\":[\"0001-23-000001\"],"
        + "\"form\":[\"10-K\"],"
        + "\"filingDate\":[\"2023-01-15\"],"
        + "\"primaryDocument\":[\"doc.htm\"]"
        + "},\"files\":[]}}";

    Iterator<Map<String, String>> docs = processor.parseDocuments(json, baseVars);
    assertTrue(docs.hasNext());
    Map<String, String> doc = docs.next();
    // cik_url should not be set
    assertNull(doc.get("cik_url"));
  }

  @Test void testParseDocumentsBadFilingDate() {
    HttpSourceConfig.DocumentSourceConfig docConfig =
        mock(HttpSourceConfig.DocumentSourceConfig.class);
    when(docConfig.getStartYear()).thenReturn(2020);
    when(docConfig.getEndYear()).thenReturn(2025);
    when(docConfig.getDocumentTypes()).thenReturn(null);
    when(mockConfig.getDocumentSource()).thenReturn(docConfig);

    DocumentETLProcessor processor = createProcessor();

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    // Filing date that cannot be parsed
    String json = "{\"filings\":{\"recent\":{"
        + "\"accessionNumber\":[\"0001-23-000001\"],"
        + "\"form\":[\"10-K\"],"
        + "\"filingDate\":[\"invalid-date\"],"
        + "\"primaryDocument\":[\"doc.htm\"]"
        + "},\"files\":[]}}";

    Iterator<Map<String, String>> docs = processor.parseDocuments(json, baseVars);
    // Should be filtered out due to unparseable date
    assertFalse(docs.hasNext());
  }

  // ====================================================================
  // isAlreadyProcessed - cache behavior
  // ====================================================================

  @Test void testIsAlreadyProcessedExistsCacheHit() throws IOException {
    // First call finds file, caches it
    when(mockStorageProvider.exists(anyString())).thenReturn(true);

    DocumentETLProcessor processor = createProcessor();

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    docVars.put("accession", "0001234567-23-000001");

    assertTrue(processor.isAlreadyProcessed(docVars));
    // Second call should use cache, not storage
    assertTrue(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedNotExistsCacheHit() throws IOException {
    // First call does not find file, caches negative
    when(mockStorageProvider.exists(anyString())).thenReturn(false);

    DocumentETLProcessor processor = createProcessor();

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    docVars.put("accession", "0001234567-23-000001");

    assertFalse(processor.isAlreadyProcessed(docVars));
    // Second call uses notExistsCache
    assertFalse(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedExceptionHandling() throws IOException {
    when(mockStorageProvider.exists(anyString()))
        .thenThrow(new RuntimeException("unexpected error"));

    DocumentETLProcessor processor = createProcessor();

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    docVars.put("accession", "0001234567-23-000001");

    assertFalse(processor.isAlreadyProcessed(docVars));
  }

  // ====================================================================
  // DocumentETLResult - additional coverage
  // ====================================================================

  @Test void testDocumentETLResultZeroDuration() {
    DocumentETLProcessor.DocumentETLResult result =
        new DocumentETLProcessor.DocumentETLResult(
            0, 0, 0,
            Collections.<String>emptyList(),
            Collections.<String>emptyList(),
            0L);

    assertEquals(0, result.getDocumentsProcessed());
    assertEquals(0, result.getDocumentsSkipped());
    assertEquals(0, result.getDocumentsFailed());
    assertTrue(result.getOutputFiles().isEmpty());
    assertTrue(result.getErrors().isEmpty());
    assertEquals(0L, result.getDurationMs());
    assertTrue(result.isSuccess());
  }

  @Test void testDocumentETLResultLargeNumbers() {
    DocumentETLProcessor.DocumentETLResult result =
        new DocumentETLProcessor.DocumentETLResult(
            100000, 50000, 500,
            Collections.singletonList("output.parquet"),
            Collections.singletonList("some error"),
            999999L);

    assertEquals(100000, result.getDocumentsProcessed());
    assertEquals(50000, result.getDocumentsSkipped());
    assertEquals(500, result.getDocumentsFailed());
    assertFalse(result.isSuccess());

    String str = result.toString();
    assertTrue(str.contains("processed=100000"));
  }

  // ====================================================================
  // PaginationFileRef - additional coverage
  // ====================================================================

  @Test void testPaginationFileRefBoundaryYearOverlap() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2020-01-01", "2020-12-31", 50);

    // Exact match
    assertTrue(ref.overlapsYearRange(2020, 2020));
    // One year before
    assertFalse(ref.overlapsYearRange(2021, 2025));
    // One year after
    assertFalse(ref.overlapsYearRange(2015, 2019));
  }

  @Test void testPaginationFileRefStartYearOnly() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2010-01-01", "2015-12-31", 100);

    // Only startYear, no endYear
    assertTrue(ref.overlapsYearRange(2012, null));
    // Start year after file range ends
    assertFalse(ref.overlapsYearRange(2020, null));
  }

  @Test void testPaginationFileRefEndYearOnly() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2020-01-01", "2025-12-31", 100);

    // Only endYear, no startYear
    assertTrue(ref.overlapsYearRange(null, 2022));
    // End year before file range starts
    assertFalse(ref.overlapsYearRange(null, 2019));
  }

  // ====================================================================
  // extractPaginationFiles - edge cases
  // ====================================================================

  @Test void testExtractPaginationFilesNoArrayStart() {
    DocumentETLProcessor processor = createProcessor();

    // files key exists but no array bracket
    String json = "{\"filings\":{\"files\":\"notArray\"}}";
    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    assertTrue(refs.isEmpty());
  }

  @Test void testExtractPaginationFilesUnmatchedBracket() {
    DocumentETLProcessor processor = createProcessor();

    // Unmatched array bracket
    String json = "{\"filings\":{\"files\":[{\"name\":\"test.json\"";
    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    assertTrue(refs.isEmpty());
  }

  @Test void testExtractPaginationFilesEmptyObjectsInArray() {
    DocumentETLProcessor processor = createProcessor();

    String json = "{\"filings\":{\"files\":[{},{}]}}";
    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    // Empty objects have no name, should be filtered
    assertTrue(refs.isEmpty());
  }

  // ====================================================================
  // findMatchingBracket - additional edge cases
  // ====================================================================

  @Test void testFindMatchingBracketDeeplyNested() {
    String json = "[[[[1]]]]";
    assertEquals(8, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketMixedTypes() {
    String json = "[{\"a\":[1,{\"b\":2}]}]";
    assertEquals(json.length() - 1, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketEmptyArray() {
    String json = "[]";
    assertEquals(1, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketEmptyObject() {
    String json = "{}";
    assertEquals(1, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  // ====================================================================
  // extractJsonStringField - additional edge cases
  // ====================================================================

  @Test void testExtractJsonStringFieldEmptyValue() {
    String json = "{\"name\":\"\"}";
    assertEquals("", DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonStringFieldWithSpecialChars() {
    String json = "{\"name\":\"test/path.json\"}";
    assertEquals("test/path.json", DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonStringFieldMultipleKeys() {
    String json = "{\"first\":\"aaa\",\"second\":\"bbb\"}";
    assertEquals("aaa", DocumentETLProcessor.extractJsonStringField(json, "first"));
    assertEquals("bbb", DocumentETLProcessor.extractJsonStringField(json, "second"));
  }

  // ====================================================================
  // extractJsonIntField - additional edge cases
  // ====================================================================

  @Test void testExtractJsonIntFieldZero() {
    String json = "{\"count\":0}";
    assertEquals(0, DocumentETLProcessor.extractJsonIntField(json, "count"));
  }

  @Test void testExtractJsonIntFieldLargeNumber() {
    String json = "{\"count\":999999}";
    assertEquals(999999, DocumentETLProcessor.extractJsonIntField(json, "count"));
  }

  // ====================================================================
  // Constructor variants
  // ====================================================================

  @Test void testAllConstructorVariants() {
    // 5-arg
    DocumentETLProcessor p1 =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.toString(), tempDir.toString(), mockConverter);
    assertNotNull(p1);

    // 6-arg with listener
    DocumentETLProcessor.ProgressListener listener =
        new DocumentETLProcessor.ProgressListener() {
          @Override public void onProgress(int processed, int total, String message) {
            // no-op
          }
        };
    DocumentETLProcessor p2 =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.toString(), tempDir.toString(), mockConverter, listener);
    assertNotNull(p2);

    // 7-arg with tracker
    ProcessedDocumentTracker tracker = mock(ProcessedDocumentTracker.class);
    DocumentETLProcessor p3 =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.toString(), tempDir.toString(), mockConverter, listener, tracker);
    assertNotNull(p3);

    // 8-arg with index provider
    FilingIndexProvider indexProvider = mock(FilingIndexProvider.class);
    DocumentETLProcessor p4 =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.toString(), tempDir.toString(), mockConverter,
        listener, tracker, indexProvider);
    assertNotNull(p4);
  }

  // ====================================================================
  // Helper methods
  // ====================================================================

  private DocumentETLProcessor createProcessor() {
    return new DocumentETLProcessor(
        mockConfig, mockStorageProvider,
        tempDir.toString(), tempDir.toString(), mockConverter);
  }
}
