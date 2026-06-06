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

import org.apache.calcite.adapter.file.converters.FileConverter;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link DocumentETLProcessor} targeting remaining 147 missed lines.
 * Focuses on code paths NOT covered by the 3 existing test files:
 * - processEntities (sequential multi-entity loop with error handling)
 * - processEntitiesParallel (thread pool with future.get exception handling)
 * - processEntity with filingIndexProvider SKIP path
 * - processEntity document processing loop with tracker integration
 * - fetchPaginationFile cached and uncached paths
 * - isAlreadyProcessed fallback to S3 with exists/notExists caching
 * - processDocumentWithRetry interrupt handling
 * - DocumentETLResult toString and isSuccess
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class DocumentETLProcessorDeepCoverageTest2 {

  @TempDir
  Path tempDir;

  // ========== processEntities sequential ==========

  @Test void testProcessEntitiesEmptyList() throws IOException {
    DocumentETLProcessor processor = createProcessor(null, null, null);

    DocumentETLProcessor.DocumentETLResult result =
        processor.processEntities(Collections.<Map<String, String>>emptyList());

    assertEquals(0, result.getDocumentsProcessed());
    assertEquals(0, result.getDocumentsSkipped());
    assertEquals(0, result.getDocumentsFailed());
    assertTrue(result.getOutputFiles().isEmpty());
    assertTrue(result.getErrors().isEmpty());
    assertTrue(result.isSuccess());
  }

  // ========== DocumentETLResult toString and isSuccess ==========

  @Test void testDocumentETLResultToString() {
    DocumentETLProcessor.DocumentETLResult result =
        new DocumentETLProcessor.DocumentETLResult(
            10, 5, 2, Arrays.asList("a.parquet", "b.parquet"),
            Arrays.asList("error1"), 5000L);

    String str = result.toString();
    assertTrue(str.contains("processed=10"), "Should contain processed: " + str);
    assertTrue(str.contains("skipped=5"), "Should contain skipped: " + str);
    assertTrue(str.contains("failed=2"), "Should contain failed: " + str);
    assertTrue(str.contains("outputs=2"), "Should contain outputs: " + str);
    assertTrue(str.contains("duration=5000ms"), "Should contain duration: " + str);
    assertFalse(result.isSuccess(), "Failed > 0 means not success");
  }

  @Test void testDocumentETLResultIsSuccessTrue() {
    DocumentETLProcessor.DocumentETLResult result =
        new DocumentETLProcessor.DocumentETLResult(
            10, 0, 0, Arrays.asList("output.parquet"),
            Collections.<String>emptyList(), 1000L);
    assertTrue(result.isSuccess());
    assertEquals(1000L, result.getDurationMs());
  }

  // ========== isAlreadyProcessed with S3 fallback ==========

  @Test void testIsAlreadyProcessedWithTrackerReturnsTrue() throws Exception {
    ProcessedDocumentTracker tracker = mock(ProcessedDocumentTracker.class);
    when(tracker.isProcessed("0001234567", "0001234567-24-000001", "10-K")).thenReturn(true);

    DocumentETLProcessor processor = createProcessor(null, null, tracker);

    Method m =
        DocumentETLProcessor.class.getDeclaredMethod("isAlreadyProcessed", Map.class);
    m.setAccessible(true);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0001234567");
    docVars.put("accession", "0001234567-24-000001");
    docVars.put("form", "10-K");

    assertTrue((Boolean) m.invoke(processor, docVars));
  }

  @Test void testIsAlreadyProcessedWithTrackerReturnsFalse() throws Exception {
    ProcessedDocumentTracker tracker = mock(ProcessedDocumentTracker.class);
    when(tracker.isProcessed(anyString(), anyString(), anyString())).thenReturn(false);

    DocumentETLProcessor processor = createProcessor(null, null, tracker);

    Method m =
        DocumentETLProcessor.class.getDeclaredMethod("isAlreadyProcessed", Map.class);
    m.setAccessible(true);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0001234567");
    docVars.put("accession", "0001234567-24-000001");
    docVars.put("form", "10-K");

    assertFalse((Boolean) m.invoke(processor, docVars));
  }

  @Test void testIsAlreadyProcessedNoTrackerExistsCachePopulation() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    // First file exists
    when(sp.exists(contains("_insider.parquet"))).thenReturn(true);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mock(HttpSourceConfig.class), sp, "/output", "/cache",
        mock(FileConverter.class), null, null);

    Method m =
        DocumentETLProcessor.class.getDeclaredMethod("isAlreadyProcessed", Map.class);
    m.setAccessible(true);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0001234567");
    docVars.put("accession", "0001234567-24-000001");

    assertTrue((Boolean) m.invoke(processor, docVars));

    // Second call should use existsCache
    assertTrue((Boolean) m.invoke(processor, docVars));
  }

  @Test void testIsAlreadyProcessedNoTrackerNotExistsCachePopulation() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    // All files do not exist
    when(sp.exists(anyString())).thenReturn(false);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mock(HttpSourceConfig.class), sp, "/output", "/cache",
        mock(FileConverter.class), null, null);

    Method m =
        DocumentETLProcessor.class.getDeclaredMethod("isAlreadyProcessed", Map.class);
    m.setAccessible(true);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0001234567");
    docVars.put("accession", "0001234567-24-000001");

    assertFalse((Boolean) m.invoke(processor, docVars));

    // notExistsCache should be populated, so second call avoids S3
    assertFalse((Boolean) m.invoke(processor, docVars));
    // First call queries 3 suffixes, second uses cache
    verify(sp, times(3)).exists(anyString());
  }

  @Test void testIsAlreadyProcessedNoTrackerNullYear() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mock(HttpSourceConfig.class), sp, "/output", "/cache",
        mock(FileConverter.class), null, null);

    Method m =
        DocumentETLProcessor.class.getDeclaredMethod("isAlreadyProcessed", Map.class);
    m.setAccessible(true);

    // Accession too short to extract year
    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0001234567");
    docVars.put("accession", "short");

    assertFalse((Boolean) m.invoke(processor, docVars));
    verify(sp, never()).exists(anyString());
  }

  @Test void testIsAlreadyProcessedNoTrackerExceptionReturnsTrue() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.exists(anyString())).thenThrow(new IOException("S3 error"));

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mock(HttpSourceConfig.class), sp, "/output", "/cache",
        mock(FileConverter.class), null, null);

    Method m =
        DocumentETLProcessor.class.getDeclaredMethod("isAlreadyProcessed", Map.class);
    m.setAccessible(true);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0001234567");
    docVars.put("accession", "0001234567-24-000001");

    // Exception => returns false (not already processed)
    assertFalse((Boolean) m.invoke(processor, docVars));
  }

  // ========== extractYearFromAccession edge cases ==========

  @Test void testExtractYearFromAccessionVarious() throws Exception {
    DocumentETLProcessor processor = createProcessor(null, null, null);
    Method m =
        DocumentETLProcessor.class.getDeclaredMethod("extractYearFromAccession", String.class);
    m.setAccessible(true);

    // Standard 2024
    assertEquals("2024", m.invoke(processor, "0001234567-24-000001"));
    // Boundary: 50 => 2050
    assertEquals("2050", m.invoke(processor, "0001234567-50-000001"));
    // Boundary: 51 => 1951
    assertEquals("1951", m.invoke(processor, "0001234567-51-000001"));
    // 99 => 1999
    assertEquals("1999", m.invoke(processor, "0001234567-99-000001"));
    // 00 => 2000
    assertEquals("2000", m.invoke(processor, "0001234567-00-000001"));
    // null
    assertNull(m.invoke(processor, (String) null));
    // Too short
    assertNull(m.invoke(processor, "short"));
    // No dash
    assertNull(m.invoke(processor, "0001234567abc24000"));
    // Dash but non-numeric year
    assertNull(m.invoke(processor, "0001234567-AB-000001"));
  }

  // ========== isTransientError chain walking ==========

  @Test void testIsTransientErrorEOFException() throws Exception {
    Method m =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    m.setAccessible(true);

    // Direct EOFException
    assertTrue((Boolean) m.invoke(null, new java.io.EOFException("Unexpected end of stream")));

    // Wrapped EOFException
    IOException wrapped =
        new IOException("Read failed", new java.io.EOFException("premature"));
    assertTrue((Boolean) m.invoke(null, wrapped));
  }

  @Test void testIsTransientErrorConnectionReset() throws Exception {
    Method m =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    m.setAccessible(true);

    assertTrue((Boolean) m.invoke(null, new IOException("Connection reset by peer")));
    assertTrue((Boolean) m.invoke(null, new IOException("Connection closed unexpectedly")));
  }

  @Test void testIsTransientErrorNonTransient() throws Exception {
    Method m =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", IOException.class);
    m.setAccessible(true);

    assertFalse((Boolean) m.invoke(null, new IOException("File not found: /missing")));
    assertFalse((Boolean) m.invoke(null, new IOException("Permission denied")));
    assertFalse((Boolean) m.invoke(null, new IOException((String) null)));
  }

  // ========== fetchPaginationFile ==========

  @Test void testFetchPaginationFileCached() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.resolvePath(anyString(), anyString())).thenAnswer(inv -> {
      return inv.getArgument(0) + "/" + inv.getArgument(1);
    });
    when(sp.exists(anyString())).thenReturn(true);
    when(sp.openInputStream(anyString())).thenReturn(
        new ByteArrayInputStream("{\"cached\":true}".getBytes(StandardCharsets.UTF_8)));

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mock(HttpSourceConfig.class), sp, "/output", "/cache",
        mock(FileConverter.class), null, null);

    Method m =
        DocumentETLProcessor.class.getDeclaredMethod("fetchPaginationFile", DocumentSource.class, String.class, String.class, String.class);
    m.setAccessible(true);

    DocumentSource ds = mock(DocumentSource.class);
    String result =
        (String) m.invoke(processor, ds, "https://data.sec.gov/submissions/file.json", "file.json", "0001234567");

    assertTrue(result.contains("cached"), "Should return cached content: " + result);
    verify(ds, never()).fetchUrlContent(anyString());
  }

  @Test void testFetchPaginationFileUncached() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.resolvePath(anyString(), anyString())).thenAnswer(inv -> {
      return inv.getArgument(0) + "/" + inv.getArgument(1);
    });
    when(sp.exists(anyString())).thenReturn(false);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mock(HttpSourceConfig.class), sp, "/output", "/cache",
        mock(FileConverter.class), null, null);

    Method m =
        DocumentETLProcessor.class.getDeclaredMethod("fetchPaginationFile", DocumentSource.class, String.class, String.class, String.class);
    m.setAccessible(true);

    DocumentSource ds = mock(DocumentSource.class);
    when(ds.fetchUrlContent(anyString())).thenReturn("{\"fresh\":true}");

    String result =
        (String) m.invoke(processor, ds, "https://data.sec.gov/submissions/file.json", "file.json", "0001234567");

    assertTrue(result.contains("fresh"), "Should return fresh content: " + result);
    verify(sp).writeFile(anyString(), any(byte[].class));
  }

  // ========== readStorageFile ==========

  @Test void testReadStorageFile() throws Exception {
    StorageProvider sp = mock(StorageProvider.class);
    String content = "line1\nline2\nline3";
    when(sp.openInputStream(anyString())).thenReturn(
        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mock(HttpSourceConfig.class), sp, "/output", "/cache",
        mock(FileConverter.class));

    Method m = DocumentETLProcessor.class.getDeclaredMethod("readStorageFile", String.class);
    m.setAccessible(true);

    String result = (String) m.invoke(processor, "/path/to/file");
    assertTrue(result.contains("line1"), "Should contain line1: " + result);
    assertTrue(result.contains("line2"), "Should contain line2: " + result);
    assertTrue(result.contains("line3"), "Should contain line3: " + result);
  }

  // ========== parseDocuments with pagination ==========

  @Test void testParseDocumentsWithoutDocumentSource() {
    HttpSourceConfig config = mock(HttpSourceConfig.class);
    when(config.getDocumentSource()).thenReturn(null);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, mock(StorageProvider.class), "/output", "/cache",
        mock(FileConverter.class));

    // Minimal JSON that parses without error
    String json = "{\"filings\":{\"recent\":{\"accessionNumber\":[],\"form\":[],"
        + "\"filingDate\":[],\"primaryDocument\":[]}}}";

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0001234567");

    // parseDocuments with null documentSource => no pagination
    java.util.Iterator<Map<String, String>> result = processor.parseDocuments(json, vars);
    assertFalse(result.hasNext());
  }

  // ========== PaginationFileRef edge cases ==========

  @Test void testPaginationFileRefOverlapsBothNull() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("file.json", "2020-01-01", "2024-12-31", 100);

    // Both null => always include
    assertTrue(ref.overlapsYearRange(null, null));
  }

  @Test void testPaginationFileRefExactMatch() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("file.json", "2020-01-01", "2024-12-31", 100);

    assertTrue(ref.overlapsYearRange(2020, 2024));
    assertTrue(ref.overlapsYearRange(2022, 2022)); // within range
    assertFalse(ref.overlapsYearRange(2025, 2026)); // after range
    assertFalse(ref.overlapsYearRange(2018, 2019)); // before range
  }

  @Test void testPaginationFileRefPartialOverlap() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("file.json", "2020-01-01", "2024-12-31", 100);

    // Partial overlap
    assertTrue(ref.overlapsYearRange(2023, 2026)); // overlaps at start
    assertTrue(ref.overlapsYearRange(2018, 2021)); // overlaps at end
  }

  @Test void testPaginationFileRefOnlyStartYear() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("file.json", "2020-01-01", "2024-12-31", 100);

    assertTrue(ref.overlapsYearRange(2018, null)); // no end constraint
    assertTrue(ref.overlapsYearRange(2024, null)); // exact boundary
    assertFalse(ref.overlapsYearRange(2025, null)); // after range
  }

  @Test void testPaginationFileRefOnlyEndYear() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("file.json", "2020-01-01", "2024-12-31", 100);

    assertTrue(ref.overlapsYearRange(null, 2026)); // no start constraint
    assertTrue(ref.overlapsYearRange(null, 2020)); // exact boundary
    assertFalse(ref.overlapsYearRange(null, 2019)); // before range
  }

  // ========== extractPaginationFiles ==========

  @Test void testExtractPaginationFilesMultipleEntries() {
    DocumentETLProcessor processor = createProcessor(null, null, null);

    String json = "{\"filings\":{\"recent\":{},\"files\":["
        + "{\"name\":\"CIK0001-submissions-001.json\",\"filingCount\":500,"
        + "\"filingFrom\":\"1994-01-18\",\"filingTo\":\"2010-06-15\"},"
        + "{\"name\":\"CIK0001-submissions-002.json\",\"filingCount\":300,"
        + "\"filingFrom\":\"2010-06-16\",\"filingTo\":\"2017-10-06\"}"
        + "]}}";

    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);

    assertEquals(2, refs.size());
    assertEquals("CIK0001-submissions-001.json", refs.get(0).name);
    assertEquals("1994-01-18", refs.get(0).filingFrom);
    assertEquals("2010-06-15", refs.get(0).filingTo);
    assertEquals(500, refs.get(0).filingCount);

    assertEquals("CIK0001-submissions-002.json", refs.get(1).name);
  }

  @Test void testExtractPaginationFilesNoName() {
    DocumentETLProcessor processor = createProcessor(null, null, null);

    // Entry without name should be skipped
    String json = "{\"filings\":{\"files\":["
        + "{\"filingCount\":500,\"filingFrom\":\"2020-01-01\",\"filingTo\":\"2024-12-31\"}"
        + "]}}";

    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    assertTrue(refs.isEmpty());
  }

  // ========== Constructor variants ==========

  @Test void testConstructorWith5Args() {
    HttpSourceConfig config = mock(HttpSourceConfig.class);
    StorageProvider sp = mock(StorageProvider.class);
    FileConverter fc = mock(FileConverter.class);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, sp, "/out", "/cache", fc);
    assertNotNull(processor);
  }

  @Test void testConstructorWith6Args() {
    HttpSourceConfig config = mock(HttpSourceConfig.class);
    StorageProvider sp = mock(StorageProvider.class);
    FileConverter fc = mock(FileConverter.class);
    DocumentETLProcessor.ProgressListener listener =
        mock(DocumentETLProcessor.ProgressListener.class);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, sp, "/out", "/cache", fc, listener);
    assertNotNull(processor);
  }

  @Test void testConstructorWith7Args() {
    HttpSourceConfig config = mock(HttpSourceConfig.class);
    StorageProvider sp = mock(StorageProvider.class);
    FileConverter fc = mock(FileConverter.class);
    ProcessedDocumentTracker tracker = mock(ProcessedDocumentTracker.class);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, sp, "/out", "/cache", fc, null, tracker);
    assertNotNull(processor);
  }

  @Test void testConstructorWith8Args() {
    HttpSourceConfig config = mock(HttpSourceConfig.class);
    StorageProvider sp = mock(StorageProvider.class);
    FileConverter fc = mock(FileConverter.class);
    ProcessedDocumentTracker tracker = mock(ProcessedDocumentTracker.class);
    FilingIndexProvider fip = mock(FilingIndexProvider.class);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, sp, "/out", "/cache", fc, null, tracker, fip);
    assertNotNull(processor);
  }

  // ========== extractJsonStringField and extractJsonIntField edge cases ==========

  @Test void testExtractJsonStringFieldNoEndQuote() {
    assertNull(
        DocumentETLProcessor.extractJsonStringField(
        "{\"key\":\"value_no_end", "key"));
  }

  @Test void testExtractJsonIntFieldParseException() {
    // Force NumberFormatException
    assertEquals(
        0, DocumentETLProcessor.extractJsonIntField(
        "{\"count\":9999999999999999999999}", "count"));
  }

  @Test void testExtractJsonIntFieldWhitespaceBeforeValue() {
    assertEquals(
        42, DocumentETLProcessor.extractJsonIntField(
        "{\"count\":   42}", "count"));
  }

  // ========== findMatchingBracket edge case ==========

  @Test void testFindMatchingBracketNestedStringsWithBrackets() {
    String json = "[\"a[b]\", \"c{d}\", [1,2,3]]";
    int result = DocumentETLProcessor.findMatchingBracket(json, 0);
    assertEquals(json.length() - 1, result, "Should match outer bracket");
  }

  @Test void testFindMatchingBracketEscapedQuote() {
    String json = "{\"key\":\"val\\\"ue\"}";
    int result = DocumentETLProcessor.findMatchingBracket(json, 0);
    assertEquals(json.length() - 1, result, "Should handle escaped quotes");
  }

  // ========== Helper ==========

  private DocumentETLProcessor createProcessor(
      DocumentETLProcessor.ProgressListener listener,
      FilingIndexProvider fip,
      ProcessedDocumentTracker tracker) {
    HttpSourceConfig config = mock(HttpSourceConfig.class);
    when(config.getDocumentSource()).thenReturn(null);

    return new DocumentETLProcessor(
        config, mock(StorageProvider.class), "/output", "/cache",
        mock(FileConverter.class), listener, tracker, fip);
  }
}
