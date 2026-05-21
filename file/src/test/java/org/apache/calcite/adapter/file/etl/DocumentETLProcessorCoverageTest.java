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
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Coverage tests for {@link DocumentETLProcessor}, including the
 * {@link DocumentETLProcessor.DocumentETLResult},
 * {@link DocumentETLProcessor.PaginationFileRef},
 * and static JSON utility methods.
 */
@Tag("unit")
class DocumentETLProcessorCoverageTest {

  @TempDir
  File tempDir;

  private HttpSourceConfig mockConfig;
  private StorageProvider mockStorageProvider;
  private FileConverter mockConverter;

  @BeforeEach void setUp() {
    mockConfig = mock(HttpSourceConfig.class);
    mockStorageProvider = mock(StorageProvider.class);
    mockConverter = mock(FileConverter.class);

    // Default stub behaviors
    when(mockConfig.getHeaders()).thenReturn(Collections.<String, String>emptyMap());
    when(mockConfig.getRateLimit()).thenReturn(null);
    when(mockConfig.getDocumentSource()).thenReturn(null);
    when(mockStorageProvider.resolvePath(anyString(), anyString()))
        .thenAnswer(inv -> inv.getArgument(0) + "/" + inv.getArgument(1));
    when(mockStorageProvider.getStorageType()).thenReturn("mock");
  }

  // ===== DocumentETLResult Tests =====

  @Test void testDocumentETLResultBasic() {
    DocumentETLProcessor.DocumentETLResult result =
        new DocumentETLProcessor.DocumentETLResult(
            10, 5, 2,
            Arrays.asList("file1.parquet", "file2.parquet"),
            Arrays.asList("error1"),
            1234L);

    assertEquals(10, result.getDocumentsProcessed());
    assertEquals(5, result.getDocumentsSkipped());
    assertEquals(2, result.getDocumentsFailed());
    assertEquals(2, result.getOutputFiles().size());
    assertEquals(1, result.getErrors().size());
    assertEquals(1234L, result.getDurationMs());
    assertFalse(result.isSuccess());
  }

  @Test void testDocumentETLResultSuccess() {
    DocumentETLProcessor.DocumentETLResult result =
        new DocumentETLProcessor.DocumentETLResult(
            10, 5, 0,
            Arrays.asList("file1.parquet"),
            Collections.<String>emptyList(),
            500L);

    assertTrue(result.isSuccess());
  }

  @Test void testDocumentETLResultToString() {
    DocumentETLProcessor.DocumentETLResult result =
        new DocumentETLProcessor.DocumentETLResult(
            3, 2, 1,
            Arrays.asList("f1", "f2"),
            Arrays.asList("err1"),
            999L);

    String str = result.toString();
    assertNotNull(str);
    assertTrue(str.contains("processed=3"));
    assertTrue(str.contains("skipped=2"));
    assertTrue(str.contains("failed=1"));
    assertTrue(str.contains("outputs=2"));
    assertTrue(str.contains("duration=999ms"));
  }

  // ===== PaginationFileRef Tests =====

  @Test void testPaginationFileRefBasic() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "CIK001-submissions-001.json", "2015-01-01", "2020-12-31", 500);

    assertEquals("CIK001-submissions-001.json", ref.name);
    assertEquals("2015-01-01", ref.filingFrom);
    assertEquals("2020-12-31", ref.filingTo);
    assertEquals(500, ref.filingCount);
  }

  @Test void testPaginationFileRefToString() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2010-01-01", "2020-12-31", 100);

    String str = ref.toString();
    assertTrue(str.contains("test.json"));
    assertTrue(str.contains("2010-01-01"));
    assertTrue(str.contains("2020-12-31"));
    assertTrue(str.contains("100"));
  }

  @Test void testPaginationFileRefOverlapsNoFilter() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2010-01-01", "2020-12-31", 100);

    // Both null = no filter, always overlaps
    assertTrue(ref.overlapsYearRange(null, null));
  }

  @Test void testPaginationFileRefOverlapsWithin() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2015-01-01", "2020-12-31", 100);

    assertTrue(ref.overlapsYearRange(2018, 2022));
  }

  @Test void testPaginationFileRefOverlapsBefore() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2015-01-01", "2018-12-31", 100);

    // File range ends before start year
    assertFalse(ref.overlapsYearRange(2020, 2024));
  }

  @Test void testPaginationFileRefOverlapsAfter() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2022-01-01", "2024-12-31", 100);

    // File range starts after end year
    assertFalse(ref.overlapsYearRange(2018, 2020));
  }

  @Test void testPaginationFileRefOverlapsExact() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2020-01-01", "2020-12-31", 100);

    assertTrue(ref.overlapsYearRange(2020, 2020));
  }

  @Test void testPaginationFileRefOverlapsNullDates() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", null, null, 100);

    // Can't parse dates, include to be safe
    assertTrue(ref.overlapsYearRange(2020, 2024));
  }

  @Test void testPaginationFileRefOverlapsShortDates() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "20", "20", 100);

    // Too short to parse, include to be safe
    assertTrue(ref.overlapsYearRange(2020, 2024));
  }

  @Test void testPaginationFileRefOverlapsOnlyStartYear() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2015-01-01", "2022-12-31", 100);

    // Only startYear filter
    assertTrue(ref.overlapsYearRange(2018, null));
  }

  @Test void testPaginationFileRefOverlapsOnlyEndYear() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2015-01-01", "2022-12-31", 100);

    // Only endYear filter
    assertTrue(ref.overlapsYearRange(null, 2018));
  }

  @Test void testPaginationFileRefOverlapsBadDate() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "abcd-01-01", "efgh-12-31", 100);

    // Can't parse, include to be safe
    assertTrue(ref.overlapsYearRange(2020, 2024));
  }

  // ===== Static JSON Utility Methods =====

  @Test void testExtractJsonStringField() {
    String json = "{\"name\":\"test.json\",\"count\":42}";
    assertEquals("test.json", DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonStringFieldMissing() {
    String json = "{\"count\":42}";
    assertNull(DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonStringFieldNoColon() {
    String json = "{\"name\" \"test\"}";
    assertNull(DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonStringFieldNoValue() {
    String json = "{\"name\":}";
    assertNull(DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonStringFieldNoClosingQuote() {
    String json = "{\"name\":\"test}";
    assertNull(DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonIntField() {
    String json = "{\"filingCount\":866}";
    assertEquals(866, DocumentETLProcessor.extractJsonIntField(json, "filingCount"));
  }

  @Test void testExtractJsonIntFieldMissing() {
    String json = "{\"other\":42}";
    assertEquals(0, DocumentETLProcessor.extractJsonIntField(json, "filingCount"));
  }

  @Test void testExtractJsonIntFieldNoColon() {
    String json = "{\"count\" 42}";
    assertEquals(0, DocumentETLProcessor.extractJsonIntField(json, "count"));
  }

  @Test void testExtractJsonIntFieldNoDigits() {
    String json = "{\"count\":abc}";
    assertEquals(0, DocumentETLProcessor.extractJsonIntField(json, "count"));
  }

  @Test void testExtractJsonIntFieldNegative() {
    String json = "{\"count\":-5}";
    assertEquals(-5, DocumentETLProcessor.extractJsonIntField(json, "count"));
  }

  @Test void testExtractJsonIntFieldWithSpaces() {
    String json = "{\"count\":  42}";
    assertEquals(42, DocumentETLProcessor.extractJsonIntField(json, "count"));
  }

  // ===== findMatchingBracket =====

  @Test void testFindMatchingBracketSquare() {
    String json = "[1, 2, 3]";
    assertEquals(8, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketCurly() {
    String json = "{\"a\": 1}";
    assertEquals(7, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketNested() {
    String json = "[{\"a\": [1, 2]}, 3]";
    assertEquals(17, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketUnmatched() {
    String json = "[1, 2, 3";
    assertEquals(-1, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketInvalidStart() {
    String json = "abc";
    assertEquals(-1, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketWithEscapedQuotes() {
    String json = "[\"test\\\"value\"]";
    assertEquals(json.length() - 1, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketWithStringContainingBrackets() {
    String json = "[\"[inner]\", 1]";
    assertEquals(json.length() - 1, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  // ===== extractPaginationFiles =====

  @Test void testExtractPaginationFilesNoFilingsKey() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles("{\"data\":\"test\"}");
    assertTrue(refs.isEmpty());
  }

  @Test void testExtractPaginationFilesNoFilesKey() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    String json = "{\"filings\":{\"recent\":{}}}";
    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    assertTrue(refs.isEmpty());
  }

  @Test void testExtractPaginationFilesNoArray() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    String json = "{\"filings\":{\"files\":\"not an array\"}}";
    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    assertTrue(refs.isEmpty());
  }

  @Test void testExtractPaginationFilesValid() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    String json = "{\"filings\":{\"recent\":{},\"files\":["
        + "{\"name\":\"CIK001-submissions-001.json\","
        + "\"filingCount\":866,"
        + "\"filingFrom\":\"1994-01-18\","
        + "\"filingTo\":\"2017-10-06\"}"
        + "]}}";

    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    assertEquals(1, refs.size());
    assertEquals("CIK001-submissions-001.json", refs.get(0).name);
    assertEquals("1994-01-18", refs.get(0).filingFrom);
    assertEquals("2017-10-06", refs.get(0).filingTo);
    assertEquals(866, refs.get(0).filingCount);
  }

  @Test void testExtractPaginationFilesMultiple() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    String json = "{\"filings\":{\"recent\":{},\"files\":["
        + "{\"name\":\"file1.json\",\"filingCount\":100,"
        + "\"filingFrom\":\"2010-01-01\",\"filingTo\":\"2015-12-31\"},"
        + "{\"name\":\"file2.json\",\"filingCount\":200,"
        + "\"filingFrom\":\"2016-01-01\",\"filingTo\":\"2020-12-31\"}"
        + "]}}";

    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    assertEquals(2, refs.size());
    assertEquals("file1.json", refs.get(0).name);
    assertEquals("file2.json", refs.get(1).name);
  }

  @Test void testExtractPaginationFilesEmptyName() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    String json = "{\"filings\":{\"recent\":{},\"files\":["
        + "{\"name\":\"\",\"filingCount\":100,"
        + "\"filingFrom\":\"2010-01-01\",\"filingTo\":\"2015-12-31\"}"
        + "]}}";

    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    assertTrue(refs.isEmpty()); // Empty name filtered out
  }

  // ===== isAlreadyProcessed =====

  @Test void testIsAlreadyProcessedNullCik() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("accession", "0001-23-000001");
    assertFalse(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedNullAccession() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    assertFalse(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedWithTracker() {
    ProcessedDocumentTracker tracker = mock(ProcessedDocumentTracker.class);
    when(tracker.isProcessed("0000070502", "0001-23-000001", "10-K")).thenReturn(true);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter, null, tracker);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    docVars.put("accession", "0001-23-000001");
    docVars.put("form", "10-K");
    assertTrue(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedFallbackExistsInCache() throws IOException {
    when(mockStorageProvider.exists(anyString())).thenReturn(false);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    docVars.put("accession", "0001234567-23-000001");
    assertFalse(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedFallbackFileExists() throws IOException {
    when(mockStorageProvider.exists(anyString())).thenReturn(true);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    docVars.put("accession", "0001234567-23-000001");
    assertTrue(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedShortAccession() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    docVars.put("accession", "short");
    assertFalse(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedNoDashInAccession() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    docVars.put("accession", "1234567890123");
    assertFalse(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedNonNumericYear() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    docVars.put("accession", "0001234567-AB-000001");
    assertFalse(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedYear2000sMapping() throws IOException {
    when(mockStorageProvider.exists(anyString())).thenReturn(true);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    docVars.put("accession", "0001234567-23-000001"); // 23 -> 2023
    assertTrue(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedYear1900sMapping() throws IOException {
    when(mockStorageProvider.exists(anyString())).thenReturn(true);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    docVars.put("accession", "0001234567-95-000001"); // 95 -> 1995
    assertTrue(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedStorageProviderException() throws IOException {
    when(mockStorageProvider.exists(anyString())).thenThrow(new IOException("S3 error"));

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    docVars.put("accession", "0001234567-23-000001");
    assertFalse(processor.isAlreadyProcessed(docVars));
  }

  // ===== isTransientError (tested indirectly via processDocumentWithRetry) =====

  // ===== parseDocuments =====

  @Test void testParseDocumentsNoOverload() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    // Empty JSON should produce empty iterator
    Iterator<Map<String, String>> docs = processor.parseDocuments("{}", baseVars);
    assertFalse(docs.hasNext());
  }

  @Test void testParseDocumentsWithSubmissionsJson() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    String json = "{\"filings\":{\"recent\":{"
        + "\"accessionNumber\":[\"0001-23-000001\",\"0001-23-000002\"],"
        + "\"form\":[\"10-K\",\"8-K\"],"
        + "\"filingDate\":[\"2023-01-15\",\"2023-06-30\"],"
        + "\"primaryDocument\":[\"doc1.htm\",\"doc2.htm\"]"
        + "},\"files\":[]}}";

    Iterator<Map<String, String>> docs = processor.parseDocuments(json, baseVars);
    List<Map<String, String>> docList = new ArrayList<Map<String, String>>();
    while (docs.hasNext()) {
      docList.add(docs.next());
    }
    assertEquals(2, docList.size());
    assertEquals("0001-23-000001", docList.get(0).get("accession"));
    assertEquals("10-K", docList.get(0).get("form"));
  }

  @Test void testParseDocumentsWithYearFilter() {
    HttpSourceConfig.DocumentSourceConfig docConfig =
        mock(HttpSourceConfig.DocumentSourceConfig.class);
    when(docConfig.getStartYear()).thenReturn(2023);
    when(docConfig.getEndYear()).thenReturn(2023);
    when(docConfig.getDocumentTypes()).thenReturn(null);
    when(mockConfig.getDocumentSource()).thenReturn(docConfig);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    String json = "{\"filings\":{\"recent\":{"
        + "\"accessionNumber\":[\"0001-22-000001\",\"0001-23-000001\"],"
        + "\"form\":[\"10-K\",\"10-K\"],"
        + "\"filingDate\":[\"2022-01-15\",\"2023-01-15\"],"
        + "\"primaryDocument\":[\"doc1.htm\",\"doc2.htm\"]"
        + "},\"files\":[]}}";

    Iterator<Map<String, String>> docs = processor.parseDocuments(json, baseVars);
    List<Map<String, String>> docList = new ArrayList<Map<String, String>>();
    while (docs.hasNext()) {
      docList.add(docs.next());
    }
    // Only 2023 filing should be returned
    assertEquals(1, docList.size());
    assertEquals("2023-01-15", docList.get(0).get("filingDate"));
  }

  @Test void testParseDocumentsWithDocumentTypeFilter() {
    HttpSourceConfig.DocumentSourceConfig docConfig =
        mock(HttpSourceConfig.DocumentSourceConfig.class);
    when(docConfig.getStartYear()).thenReturn(null);
    when(docConfig.getEndYear()).thenReturn(null);
    when(docConfig.getDocumentTypes()).thenReturn(Arrays.asList("10-K"));
    when(mockConfig.getDocumentSource()).thenReturn(docConfig);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    String json = "{\"filings\":{\"recent\":{"
        + "\"accessionNumber\":[\"0001-23-000001\",\"0001-23-000002\"],"
        + "\"form\":[\"10-K\",\"8-K\"],"
        + "\"filingDate\":[\"2023-01-15\",\"2023-06-30\"],"
        + "\"primaryDocument\":[\"doc1.htm\",\"doc2.htm\"]"
        + "},\"files\":[]}}";

    Iterator<Map<String, String>> docs = processor.parseDocuments(json, baseVars);
    List<Map<String, String>> docList = new ArrayList<Map<String, String>>();
    while (docs.hasNext()) {
      docList.add(docs.next());
    }
    assertEquals(1, docList.size());
    assertEquals("10-K", docList.get(0).get("form"));
  }

  @Test void testParseDocumentsForm4XSLStripping() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    String json = "{\"filings\":{\"recent\":{"
        + "\"accessionNumber\":[\"0001-23-000001\"],"
        + "\"form\":[\"4\"],"
        + "\"filingDate\":[\"2023-01-15\"],"
        + "\"primaryDocument\":[\"xslF345X03/wf-form4_test.xml\"]"
        + "},\"files\":[]}}";

    Iterator<Map<String, String>> docs = processor.parseDocuments(json, baseVars);
    assertTrue(docs.hasNext());
    Map<String, String> doc = docs.next();
    // XSL prefix should be stripped
    assertEquals("wf-form4_test.xml", doc.get("document"));
  }

  @Test void testParseDocumentsForm3XSLStripping() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    String json = "{\"filings\":{\"recent\":{"
        + "\"accessionNumber\":[\"0001-23-000001\"],"
        + "\"form\":[\"3\"],"
        + "\"filingDate\":[\"2023-01-15\"],"
        + "\"primaryDocument\":[\"xslF345X01/form3.xml\"]"
        + "},\"files\":[]}}";

    Iterator<Map<String, String>> docs = processor.parseDocuments(json, baseVars);
    assertTrue(docs.hasNext());
    Map<String, String> doc = docs.next();
    assertEquals("form3.xml", doc.get("document"));
  }

  @Test void testParseDocumentsForm5Normal() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    String json = "{\"filings\":{\"recent\":{"
        + "\"accessionNumber\":[\"0001-23-000001\"],"
        + "\"form\":[\"5\"],"
        + "\"filingDate\":[\"2023-01-15\"],"
        + "\"primaryDocument\":[\"wf-form5_normal.xml\"]"
        + "},\"files\":[]}}";

    Iterator<Map<String, String>> docs = processor.parseDocuments(json, baseVars);
    assertTrue(docs.hasNext());
    Map<String, String> doc = docs.next();
    // No XSL prefix, should stay as-is
    assertEquals("wf-form5_normal.xml", doc.get("document"));
  }

  @Test void testParseDocumentsCikUrlStripping() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    String json = "{\"filings\":{\"recent\":{"
        + "\"accessionNumber\":[\"0001-23-000001\"],"
        + "\"form\":[\"10-K\"],"
        + "\"filingDate\":[\"2023-01-15\"],"
        + "\"primaryDocument\":[\"doc.htm\"]"
        + "},\"files\":[]}}";

    Iterator<Map<String, String>> docs = processor.parseDocuments(json, baseVars);
    assertTrue(docs.hasNext());
    Map<String, String> doc = docs.next();
    // cik_url should strip leading zeros
    assertEquals("70502", doc.get("cik_url"));
    // accession_url should strip dashes
    assertEquals("000123000001", doc.get("accession_url"));
  }

  // ===== Constructors =====

  @Test void testConstructorBasic() {
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);
    assertNotNull(processor);
  }

  @Test void testConstructorWithProgressListener() {
    DocumentETLProcessor.ProgressListener listener = (processed, total, message) -> {};
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter, listener);
    assertNotNull(processor);
  }

  @Test void testConstructorWithTrackerAndListener() {
    ProcessedDocumentTracker tracker = mock(ProcessedDocumentTracker.class);
    DocumentETLProcessor.ProgressListener listener = (processed, total, message) -> {};
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter, listener, tracker);
    assertNotNull(processor);
  }

  @Test void testConstructorWithAllOptions() {
    ProcessedDocumentTracker tracker = mock(ProcessedDocumentTracker.class);
    DocumentETLProcessor.ProgressListener listener = (processed, total, message) -> {};
    FilingIndexProvider indexProvider = mock(FilingIndexProvider.class);
    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter, listener, tracker, indexProvider);
    assertNotNull(processor);
  }

  // ===== In-memory cache behavior =====

  @Test void testInMemoryCacheSecondCallUsesCache() throws IOException {
    // First call returns false, second call also false but cached
    when(mockStorageProvider.exists(anyString())).thenReturn(false);

    DocumentETLProcessor processor =
        new DocumentETLProcessor(mockConfig, mockStorageProvider,
        tempDir.getAbsolutePath(), tempDir.getAbsolutePath(),
        mockConverter);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    docVars.put("accession", "0001234567-23-000001");

    assertFalse(processor.isAlreadyProcessed(docVars));
    // Second call should use notExistsCache
    assertFalse(processor.isAlreadyProcessed(docVars));
  }
}
