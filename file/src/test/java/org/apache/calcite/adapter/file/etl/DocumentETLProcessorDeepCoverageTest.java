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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link DocumentETLProcessor}.
 * Covers JSON parsing utilities, pagination handling, cache logic, and result classes.
 */
@Tag("unit")
class DocumentETLProcessorDeepCoverageTest {

  @TempDir
  Path tempDir;

  // ===== extractJsonStringField tests =====

  @Test void testExtractJsonStringFieldFound() {
    String json = "{\"name\":\"CIK0001.json\",\"count\":5}";
    assertEquals("CIK0001.json", DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonStringFieldNotFound() {
    String json = "{\"name\":\"CIK0001.json\"}";
    assertNull(DocumentETLProcessor.extractJsonStringField(json, "missing"));
  }

  @Test void testExtractJsonStringFieldNoColon() {
    // Malformed JSON with no colon
    String json = "{\"name\" \"value\"}";
    // Should handle gracefully - colon search fails
    assertNull(DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  @Test void testExtractJsonStringFieldNoValueQuotes() {
    String json = "{\"name\":12345}";
    // No quote for value start
    assertNull(DocumentETLProcessor.extractJsonStringField(json, "name"));
  }

  // ===== extractJsonIntField tests =====

  @Test void testExtractJsonIntFieldFound() {
    String json = "{\"filingCount\":866}";
    assertEquals(866, DocumentETLProcessor.extractJsonIntField(json, "filingCount"));
  }

  @Test void testExtractJsonIntFieldNegative() {
    String json = "{\"offset\":-1}";
    assertEquals(-1, DocumentETLProcessor.extractJsonIntField(json, "offset"));
  }

  @Test void testExtractJsonIntFieldNotFound() {
    String json = "{\"name\":\"test\"}";
    assertEquals(0, DocumentETLProcessor.extractJsonIntField(json, "filingCount"));
  }

  @Test void testExtractJsonIntFieldNoColon() {
    String json = "{\"count\" 42}";
    assertEquals(0, DocumentETLProcessor.extractJsonIntField(json, "count"));
  }

  @Test void testExtractJsonIntFieldNonNumeric() {
    String json = "{\"count\":\"abc\"}";
    assertEquals(0, DocumentETLProcessor.extractJsonIntField(json, "count"));
  }

  @Test void testExtractJsonIntFieldWithSpaces() {
    String json = "{\"count\":   42}";
    assertEquals(42, DocumentETLProcessor.extractJsonIntField(json, "count"));
  }

  // ===== findMatchingBracket tests =====

  @Test void testFindMatchingBracketSquare() {
    String json = "[1,2,3]";
    assertEquals(6, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketCurly() {
    String json = "{\"a\":1}";
    assertEquals(6, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketNested() {
    String json = "{\"a\":{\"b\":[1,2]}}";
    assertEquals(json.length() - 1, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketWithStrings() {
    // Brackets inside strings should be ignored
    String json = "[\"[\",\"]\"]";
    assertEquals(json.length() - 1, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketWithEscapes() {
    String json = "[\"test\\\"]\",\"end\"]";
    assertEquals(json.length() - 1, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketNotBracket() {
    String json = "abc";
    assertEquals(-1, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  @Test void testFindMatchingBracketUnmatched() {
    String json = "[1,2,3";
    assertEquals(-1, DocumentETLProcessor.findMatchingBracket(json, 0));
  }

  // ===== extractPaginationFiles tests =====

  @Test void testExtractPaginationFilesWithFiles() {
    org.apache.calcite.adapter.file.storage.StorageProvider mockStorage =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    org.apache.calcite.adapter.file.converters.FileConverter mockConverter =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.converters.FileConverter.class);

    HttpSourceConfig config = createMinimalConfig();

    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, mockStorage, "/output", "/cache", mockConverter);

    String json = "{\"filings\":{\"recent\":{},\"files\":[" +
        "{\"name\":\"CIK001-001.json\",\"filingCount\":100,\"filingFrom\":\"2010-01-01\",\"filingTo\":\"2015-12-31\"}," +
        "{\"name\":\"CIK001-002.json\",\"filingCount\":200,\"filingFrom\":\"2000-01-01\",\"filingTo\":\"2009-12-31\"}" +
        "]}}";

    List<DocumentETLProcessor.PaginationFileRef> refs = processor.extractPaginationFiles(json);
    assertEquals(2, refs.size());
    assertEquals("CIK001-001.json", refs.get(0).name);
    assertEquals("2010-01-01", refs.get(0).filingFrom);
    assertEquals("2015-12-31", refs.get(0).filingTo);
    assertEquals(100, refs.get(0).filingCount);
  }

  @Test void testExtractPaginationFilesNoFilings() {
    org.apache.calcite.adapter.file.storage.StorageProvider mockStorage =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    org.apache.calcite.adapter.file.converters.FileConverter mockConverter =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.converters.FileConverter.class);

    HttpSourceConfig config = createMinimalConfig();
    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, mockStorage, "/output", "/cache", mockConverter);

    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles("{\"data\":[]}");
    assertTrue(refs.isEmpty());
  }

  @Test void testExtractPaginationFilesNoFiles() {
    org.apache.calcite.adapter.file.storage.StorageProvider mockStorage =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    org.apache.calcite.adapter.file.converters.FileConverter mockConverter =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.converters.FileConverter.class);

    HttpSourceConfig config = createMinimalConfig();
    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, mockStorage, "/output", "/cache", mockConverter);

    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles("{\"filings\":{\"recent\":{}}}");
    assertTrue(refs.isEmpty());
  }

  @Test void testExtractPaginationFilesEmptyArray() {
    org.apache.calcite.adapter.file.storage.StorageProvider mockStorage =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    org.apache.calcite.adapter.file.converters.FileConverter mockConverter =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.converters.FileConverter.class);

    HttpSourceConfig config = createMinimalConfig();
    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, mockStorage, "/output", "/cache", mockConverter);

    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles("{\"filings\":{\"files\":[]}}");
    assertTrue(refs.isEmpty());
  }

  // ===== PaginationFileRef.overlapsYearRange tests =====

  @Test void testPaginationFileRefOverlapsYearRangeNoBounds() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("test.json", "2010-01-01", "2015-12-31", 100);
    // No year filtering = always include
    assertTrue(ref.overlapsYearRange(null, null));
  }

  @Test void testPaginationFileRefOverlapsYearRangeWithinRange() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("test.json", "2010-01-01", "2015-12-31", 100);
    assertTrue(ref.overlapsYearRange(2012, 2020));
  }

  @Test void testPaginationFileRefOverlapsYearRangeAfterRange() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("test.json", "2010-01-01", "2015-12-31", 100);
    // File ends at 2015, range starts at 2018
    assertFalse(ref.overlapsYearRange(2018, 2020));
  }

  @Test void testPaginationFileRefOverlapsYearRangeBeforeRange() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("test.json", "2020-01-01", "2023-12-31", 100);
    // File starts at 2020, range ends at 2018
    assertFalse(ref.overlapsYearRange(2015, 2018));
  }

  @Test void testPaginationFileRefOverlapsYearRangeUnparseable() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("test.json", "abc", "xyz", 100);
    // Unparseable = include for safety
    assertTrue(ref.overlapsYearRange(2010, 2020));
  }

  @Test void testPaginationFileRefOverlapsYearRangeNullDates() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("test.json", null, null, 100);
    // Null dates = include for safety
    assertTrue(ref.overlapsYearRange(2010, 2020));
  }

  @Test void testPaginationFileRefOverlapsYearRangeShortDates() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("test.json", "20", "30", 100);
    // Too short = include for safety
    assertTrue(ref.overlapsYearRange(2010, 2020));
  }

  @Test void testPaginationFileRefOverlapsOnlyStartYear() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("test.json", "2000-01-01", "2005-12-31", 100);
    assertTrue(ref.overlapsYearRange(2003, null));
    assertFalse(ref.overlapsYearRange(2010, null));
  }

  @Test void testPaginationFileRefOverlapsOnlyEndYear() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("test.json", "2020-01-01", "2025-12-31", 100);
    assertTrue(ref.overlapsYearRange(null, 2022));
    assertFalse(ref.overlapsYearRange(null, 2019));
  }

  @Test void testPaginationFileRefToString() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef("test.json", "2010-01-01", "2015-12-31", 100);
    String str = ref.toString();
    assertTrue(str.contains("test.json"));
    assertTrue(str.contains("2010-01-01"));
    assertTrue(str.contains("2015-12-31"));
    assertTrue(str.contains("100"));
  }

  // ===== DocumentETLResult tests =====

  @Test void testDocumentETLResultSuccess() {
    DocumentETLProcessor.DocumentETLResult result =
        new DocumentETLProcessor.DocumentETLResult(5, 2, 0,
        Arrays.asList("/out/file1.parquet", "/out/file2.parquet"),
        Collections.<String>emptyList(),
        1000L);

    assertEquals(5, result.getDocumentsProcessed());
    assertEquals(2, result.getDocumentsSkipped());
    assertEquals(0, result.getDocumentsFailed());
    assertEquals(2, result.getOutputFiles().size());
    assertTrue(result.getErrors().isEmpty());
    assertEquals(1000L, result.getDurationMs());
    assertTrue(result.isSuccess());
  }

  @Test void testDocumentETLResultWithErrors() {
    DocumentETLProcessor.DocumentETLResult result =
        new DocumentETLProcessor.DocumentETLResult(3, 1, 2,
        Arrays.asList("/out/file1.parquet"),
        Arrays.asList("Error 1", "Error 2"),
        2000L);

    assertFalse(result.isSuccess());
    assertEquals(2, result.getErrors().size());
    String str = result.toString();
    assertTrue(str.contains("processed=3"));
    assertTrue(str.contains("skipped=1"));
    assertTrue(str.contains("failed=2"));
  }

  // ===== isAlreadyProcessed tests =====

  @Test void testIsAlreadyProcessedNullCik() {
    org.apache.calcite.adapter.file.storage.StorageProvider mockStorage =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    org.apache.calcite.adapter.file.converters.FileConverter mockConverter =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.converters.FileConverter.class);

    HttpSourceConfig config = createMinimalConfig();
    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, mockStorage, "/output", "/cache", mockConverter);

    Map<String, String> docVars = new HashMap<String, String>();
    // Missing cik
    docVars.put("accession", "0001234567-20-123456");
    assertFalse(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedNullAccession() {
    org.apache.calcite.adapter.file.storage.StorageProvider mockStorage =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    org.apache.calcite.adapter.file.converters.FileConverter mockConverter =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.converters.FileConverter.class);

    HttpSourceConfig config = createMinimalConfig();
    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, mockStorage, "/output", "/cache", mockConverter);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0001234567");
    // Missing accession
    assertFalse(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedWithTracker() {
    org.apache.calcite.adapter.file.storage.StorageProvider mockStorage =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    org.apache.calcite.adapter.file.converters.FileConverter mockConverter =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.converters.FileConverter.class);
    ProcessedDocumentTracker mockTracker =
        org.mockito.Mockito.mock(ProcessedDocumentTracker.class);

    HttpSourceConfig config = createMinimalConfig();
    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, mockStorage, "/output", "/cache", mockConverter, null, mockTracker);

    org.mockito.Mockito.when(mockTracker.isProcessed("123", "0001234567-20-123456", "10-K"))
        .thenReturn(true);

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "123");
    docVars.put("accession", "0001234567-20-123456");
    docVars.put("form", "10-K");
    assertTrue(processor.isAlreadyProcessed(docVars));
  }

  // ===== extractYearFromAccession via Reflection =====

  @Test void testExtractYearFromAccessionViaReflection() throws Exception {
    org.apache.calcite.adapter.file.storage.StorageProvider mockStorage =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    org.apache.calcite.adapter.file.converters.FileConverter mockConverter =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.converters.FileConverter.class);

    HttpSourceConfig config = createMinimalConfig();
    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, mockStorage, "/output", "/cache", mockConverter);

    Method extractYear =
        DocumentETLProcessor.class.getDeclaredMethod("extractYearFromAccession", String.class);
    extractYear.setAccessible(true);

    // Normal accessions
    assertEquals("2020", extractYear.invoke(processor, "0001234567-20-123456"));
    assertEquals("1999", extractYear.invoke(processor, "0001234567-99-123456"));
    assertEquals("2050", extractYear.invoke(processor, "0001234567-50-123456"));
    assertEquals("1951", extractYear.invoke(processor, "0001234567-51-123456"));

    // Edge cases
    assertNull(extractYear.invoke(processor, (Object) null));
    assertNull(extractYear.invoke(processor, "short"));
    assertNull(extractYear.invoke(processor, "nodashhere123"));
  }

  // ===== isTransientError via Reflection =====

  @Test void testIsTransientErrorViaReflection() throws Exception {
    Method isTransient =
        DocumentETLProcessor.class.getDeclaredMethod("isTransientError", java.io.IOException.class);
    isTransient.setAccessible(true);

    assertTrue((Boolean) isTransient.invoke(null, new java.io.IOException("HTTP 500 error")));
    assertTrue((Boolean) isTransient.invoke(null, new java.io.IOException("HTTP 502 bad gateway")));
    assertTrue((Boolean) isTransient.invoke(null, new java.io.IOException("timed out")));
    assertTrue((Boolean) isTransient.invoke(null, new java.io.IOException("connection reset")));
    assertTrue((Boolean) isTransient.invoke(null, new java.io.IOException("connection closed")));
    assertTrue((Boolean) isTransient.invoke(null, new java.io.IOException("broken pipe")));
    assertTrue((Boolean) isTransient.invoke(null, new java.io.IOException("premature end")));
    assertTrue((Boolean) isTransient.invoke(null, new java.io.IOException("premature EOF")));
    assertTrue((Boolean) isTransient.invoke(null, new java.io.IOException("unexpected end")));
    assertTrue((Boolean) isTransient.invoke(null, new java.io.IOException("end of zlib")));
    assertTrue((Boolean) isTransient.invoke(null, new java.io.IOException("end of content-length")));

    // Nested cause
    java.io.EOFException eof = new java.io.EOFException("eof");
    java.io.IOException wrapped = new java.io.IOException("wrapper", eof);
    assertTrue((Boolean) isTransient.invoke(null, wrapped));

    // Not transient
    assertFalse((Boolean) isTransient.invoke(null, new java.io.IOException("file not found")));

    // Null message
    java.io.IOException nullMsg = new java.io.IOException((String) null);
    assertFalse((Boolean) isTransient.invoke(null, nullMsg));
  }

  // ===== Constructor tests =====

  @Test void testConstructorVariants() {
    org.apache.calcite.adapter.file.storage.StorageProvider mockStorage =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    org.apache.calcite.adapter.file.converters.FileConverter mockConverter =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.converters.FileConverter.class);

    HttpSourceConfig config = createMinimalConfig();

    // 5-arg constructor
    DocumentETLProcessor p1 =
        new DocumentETLProcessor(config, mockStorage, "/output", "/cache", mockConverter);
    assertNotNull(p1);

    // 6-arg constructor with progress listener
    DocumentETLProcessor.ProgressListener listener =
        new DocumentETLProcessor.ProgressListener() {
          @Override public void onProgress(int processed, int total, String message) {
            // no-op for test
          }
        };
    DocumentETLProcessor p2 =
        new DocumentETLProcessor(config, mockStorage, "/output", "/cache", mockConverter, listener);
    assertNotNull(p2);

    // 7-arg constructor with document tracker
    ProcessedDocumentTracker mockTracker =
        org.mockito.Mockito.mock(ProcessedDocumentTracker.class);
    DocumentETLProcessor p3 =
        new DocumentETLProcessor(config, mockStorage, "/output", "/cache", mockConverter, listener, mockTracker);
    assertNotNull(p3);

    // 8-arg constructor with filing index provider
    FilingIndexProvider mockIndex =
        org.mockito.Mockito.mock(FilingIndexProvider.class);
    DocumentETLProcessor p4 =
        new DocumentETLProcessor(config, mockStorage, "/output", "/cache", mockConverter, listener, mockTracker, mockIndex);
    assertNotNull(p4);
  }

  private HttpSourceConfig createMinimalConfig() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("url", "https://example.com/api");
    return HttpSourceConfig.fromMap(configMap);
  }
}
