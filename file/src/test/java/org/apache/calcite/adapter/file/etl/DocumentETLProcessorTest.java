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

import java.io.IOException;
import java.lang.reflect.Method;
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

/**
 * Unit tests for {@link DocumentETLProcessor} focusing on uncovered lines
 * in JSON parsing, pagination, year extraction, isAlreadyProcessed,
 * isTransientError, and DocumentETLResult.
 */
@Tag("unit")
class DocumentETLProcessorTest {

  // -----------------------------------------------------------------------
  // extractJsonStringField
  // -----------------------------------------------------------------------

  @Test void testExtractJsonStringFieldSimple() {
    String json = "{\"name\":\"CIK001.json\",\"count\":10}";
    String result = DocumentETLProcessor.extractJsonStringField(json, "name");
    assertEquals("CIK001.json", result);
  }

  @Test void testExtractJsonStringFieldMissing() {
    String json = "{\"name\":\"CIK001.json\"}";
    String result = DocumentETLProcessor.extractJsonStringField(json, "missing");
    assertNull(result);
  }

  @Test void testExtractJsonStringFieldNoColon() {
    // Edge case: key found but no colon after it
    String json = "{\"name\" \"noColon\"}";
    // indexOf(":") after key should fail to find, because there is no colon
    // However the method looks for : after the key, it will find no colon
    String result = DocumentETLProcessor.extractJsonStringField(json, "name");
    // Should return null or the value - depends on where the colon search starts
    // Since there is no colon, it returns null
    assertNull(result);
  }

  @Test void testExtractJsonStringFieldNoValue() {
    String json = "{\"name\":}";
    String result = DocumentETLProcessor.extractJsonStringField(json, "name");
    // No opening quote after the colon
    assertNull(result);
  }

  // -----------------------------------------------------------------------
  // extractJsonIntField
  // -----------------------------------------------------------------------

  @Test void testExtractJsonIntFieldSimple() {
    String json = "{\"filingCount\":866}";
    int result = DocumentETLProcessor.extractJsonIntField(json, "filingCount");
    assertEquals(866, result);
  }

  @Test void testExtractJsonIntFieldMissing() {
    String json = "{\"name\":\"test\"}";
    int result = DocumentETLProcessor.extractJsonIntField(json, "filingCount");
    assertEquals(0, result);
  }

  @Test void testExtractJsonIntFieldNegative() {
    String json = "{\"value\":-42}";
    int result = DocumentETLProcessor.extractJsonIntField(json, "value");
    assertEquals(-42, result);
  }

  @Test void testExtractJsonIntFieldNoColon() {
    String json = "{\"value\" 42}";
    int result = DocumentETLProcessor.extractJsonIntField(json, "value");
    assertEquals(0, result);
  }

  @Test void testExtractJsonIntFieldWithWhitespace() {
    String json = "{\"value\":  99}";
    int result = DocumentETLProcessor.extractJsonIntField(json, "value");
    assertEquals(99, result);
  }

  @Test void testExtractJsonIntFieldInvalidNumber() {
    String json = "{\"value\": abc}";
    int result = DocumentETLProcessor.extractJsonIntField(json, "value");
    assertEquals(0, result);
  }

  @Test void testExtractJsonIntFieldNoDigits() {
    String json = "{\"value\": ,}";
    int result = DocumentETLProcessor.extractJsonIntField(json, "value");
    assertEquals(0, result);
  }

  // -----------------------------------------------------------------------
  // findMatchingBracket
  // -----------------------------------------------------------------------

  @Test void testFindMatchingBracketArray() {
    String json = "[1, 2, 3]";
    int result = DocumentETLProcessor.findMatchingBracket(json, 0);
    assertEquals(8, result);
  }

  @Test void testFindMatchingBracketObject() {
    String json = "{\"a\": 1}";
    int result = DocumentETLProcessor.findMatchingBracket(json, 0);
    assertEquals(7, result);
  }

  @Test void testFindMatchingBracketNested() {
    String json = "{\"a\": [1, 2], \"b\": {\"c\": 3}}";
    int result = DocumentETLProcessor.findMatchingBracket(json, 0);
    assertEquals(json.length() - 1, result);
  }

  @Test void testFindMatchingBracketUnclosed() {
    String json = "[1, 2, 3";
    int result = DocumentETLProcessor.findMatchingBracket(json, 0);
    assertEquals(-1, result);
  }

  @Test void testFindMatchingBracketNotABracket() {
    String json = "abc";
    int result = DocumentETLProcessor.findMatchingBracket(json, 0);
    assertEquals(-1, result);
  }

  @Test void testFindMatchingBracketWithStrings() {
    String json = "[\"hello]\", \"world\"]";
    int result = DocumentETLProcessor.findMatchingBracket(json, 0);
    assertEquals(json.length() - 1, result);
  }

  @Test void testFindMatchingBracketWithEscapedQuotes() {
    String json = "[\"he\\\"llo\", \"world\"]";
    int result = DocumentETLProcessor.findMatchingBracket(json, 0);
    assertEquals(json.length() - 1, result);
  }

  // -----------------------------------------------------------------------
  // extractPaginationFiles
  // -----------------------------------------------------------------------

  @Test void testExtractPaginationFilesEmptyJson() {
    DocumentETLProcessor processor = createMinimalProcessor();
    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles("{}");
    assertNotNull(refs);
    assertTrue(refs.isEmpty());
  }

  @Test void testExtractPaginationFilesNoFilings() {
    DocumentETLProcessor processor = createMinimalProcessor();
    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles("{\"other\": 1}");
    assertTrue(refs.isEmpty());
  }

  @Test void testExtractPaginationFilesNoFilesKey() {
    DocumentETLProcessor processor = createMinimalProcessor();
    String json = "{\"filings\": {\"recent\": {}}}";
    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    assertTrue(refs.isEmpty());
  }

  @Test void testExtractPaginationFilesEmptyArray() {
    DocumentETLProcessor processor = createMinimalProcessor();
    String json = "{\"filings\": {\"files\": []}}";
    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    assertTrue(refs.isEmpty());
  }

  @Test void testExtractPaginationFilesOneEntry() {
    DocumentETLProcessor processor = createMinimalProcessor();
    String json = "{\"filings\": {\"files\": ["
        + "{\"name\":\"CIK001-submissions-001.json\","
        + "\"filingCount\":100,"
        + "\"filingFrom\":\"2000-01-01\","
        + "\"filingTo\":\"2010-12-31\"}"
        + "]}}";
    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    assertEquals(1, refs.size());
    assertEquals("CIK001-submissions-001.json", refs.get(0).name);
    assertEquals("2000-01-01", refs.get(0).filingFrom);
    assertEquals("2010-12-31", refs.get(0).filingTo);
    assertEquals(100, refs.get(0).filingCount);
  }

  @Test void testExtractPaginationFilesMultipleEntries() {
    DocumentETLProcessor processor = createMinimalProcessor();
    String json = "{\"filings\": {\"files\": ["
        + "{\"name\":\"file1.json\",\"filingCount\":50,"
        + "\"filingFrom\":\"2000-01-01\",\"filingTo\":\"2005-12-31\"},"
        + "{\"name\":\"file2.json\",\"filingCount\":75,"
        + "\"filingFrom\":\"2006-01-01\",\"filingTo\":\"2010-12-31\"}"
        + "]}}";
    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    assertEquals(2, refs.size());
    assertEquals("file1.json", refs.get(0).name);
    assertEquals("file2.json", refs.get(1).name);
  }

  @Test void testExtractPaginationFilesNoArrayBracket() {
    DocumentETLProcessor processor = createMinimalProcessor();
    // "files" key exists but no [ after it
    String json = "{\"filings\": {\"files\": \"noarray\"}}";
    List<DocumentETLProcessor.PaginationFileRef> refs =
        processor.extractPaginationFiles(json);
    assertTrue(refs.isEmpty());
  }

  // -----------------------------------------------------------------------
  // PaginationFileRef.overlapsYearRange
  // -----------------------------------------------------------------------

  @Test void testOverlapsYearRangeNoFiltering() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2000-01-01", "2010-12-31", 100);
    assertTrue(ref.overlapsYearRange(null, null));
  }

  @Test void testOverlapsYearRangeOverlapping() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2005-01-01", "2010-12-31", 100);
    assertTrue(ref.overlapsYearRange(2008, 2012));
  }

  @Test void testOverlapsYearRangeBeforeRange() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2000-01-01", "2005-12-31", 100);
    assertFalse(ref.overlapsYearRange(2010, 2020));
  }

  @Test void testOverlapsYearRangeAfterRange() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2015-01-01", "2020-12-31", 100);
    assertFalse(ref.overlapsYearRange(2000, 2010));
  }

  @Test void testOverlapsYearRangeNullDates() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", null, null, 100);
    // Null dates means can't parse => returns true (include to be safe)
    assertTrue(ref.overlapsYearRange(2010, 2020));
  }

  @Test void testOverlapsYearRangeShortDates() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "20", "21", 100);
    // Too short to parse => returns true
    assertTrue(ref.overlapsYearRange(2010, 2020));
  }

  @Test void testOverlapsYearRangeOnlyStartYear() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2005-01-01", "2010-12-31", 100);
    assertTrue(ref.overlapsYearRange(2008, null));
    assertFalse(ref.overlapsYearRange(2015, null));
  }

  @Test void testOverlapsYearRangeOnlyEndYear() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2005-01-01", "2010-12-31", 100);
    assertTrue(ref.overlapsYearRange(null, 2008));
    assertFalse(ref.overlapsYearRange(null, 2003));
  }

  @Test void testOverlapsYearRangeInvalidDateFormat() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "XXXX-01-01", "YYYY-12-31", 100);
    // Unparseable => returns true (safe fallback)
    assertTrue(ref.overlapsYearRange(2010, 2020));
  }

  @Test void testPaginationFileRefToString() {
    DocumentETLProcessor.PaginationFileRef ref =
        new DocumentETLProcessor.PaginationFileRef(
            "test.json", "2000-01-01", "2010-12-31", 100);
    String str = ref.toString();
    assertNotNull(str);
    assertTrue(str.contains("test.json"));
    assertTrue(str.contains("2000-01-01"));
    assertTrue(str.contains("100"));
  }

  // -----------------------------------------------------------------------
  // isTransientError via reflection
  // -----------------------------------------------------------------------

  @Test void testIsTransientErrorHttp500() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "isTransientError", IOException.class);
    m.setAccessible(true);

    assertTrue((Boolean) m.invoke(null, new IOException("HTTP 500 Server Error")));
    assertTrue((Boolean) m.invoke(null, new IOException("HTTP 502 Bad Gateway")));
    assertTrue((Boolean) m.invoke(null, new IOException("HTTP 503 Service Unavailable")));
    assertTrue((Boolean) m.invoke(null, new IOException("HTTP 504 Gateway Timeout")));
  }

  @Test void testIsTransientErrorTimeout() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "isTransientError", IOException.class);
    m.setAccessible(true);

    assertTrue((Boolean) m.invoke(null, new IOException("Read timed out")));
    assertTrue((Boolean) m.invoke(null, new IOException("Connection timeout")));
  }

  @Test void testIsTransientErrorConnectionReset() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "isTransientError", IOException.class);
    m.setAccessible(true);

    assertTrue((Boolean) m.invoke(null, new IOException("Connection reset")));
    assertTrue((Boolean) m.invoke(null, new IOException("Connection closed prematurely")));
    assertTrue((Boolean) m.invoke(null, new IOException("Broken pipe")));
  }

  @Test void testIsTransientErrorPrematureEnd() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "isTransientError", IOException.class);
    m.setAccessible(true);

    assertTrue((Boolean) m.invoke(null, new IOException("Premature end of stream")));
    assertTrue((Boolean) m.invoke(null, new IOException("Premature EOF")));
    assertTrue((Boolean) m.invoke(null, new IOException("Unexpected end of file")));
    assertTrue((Boolean) m.invoke(null, new IOException("End of ZLIB input stream")));
    assertTrue((Boolean) m.invoke(null, new IOException("End of content-length body")));
  }

  @Test void testIsTransientErrorEOFException() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "isTransientError", IOException.class);
    m.setAccessible(true);

    // EOFException is a subclass of IOException
    assertTrue((Boolean) m.invoke(null, new java.io.EOFException("premature end")));
  }

  @Test void testIsTransientErrorCauseChain() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "isTransientError", IOException.class);
    m.setAccessible(true);

    // Wrap a transient error in a non-transient wrapper
    IOException wrapper = new IOException("Wrapper",
        new IOException("Connection reset by peer"));
    assertTrue((Boolean) m.invoke(null, wrapper));
  }

  @Test void testIsTransientErrorNonTransient() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "isTransientError", IOException.class);
    m.setAccessible(true);

    assertFalse((Boolean) m.invoke(null, new IOException("File not found")));
    assertFalse((Boolean) m.invoke(null, new IOException("Permission denied")));
  }

  @Test void testIsTransientErrorNullMessage() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "isTransientError", IOException.class);
    m.setAccessible(true);

    assertFalse((Boolean) m.invoke(null, new IOException((String) null)));
  }

  // -----------------------------------------------------------------------
  // extractYearFromAccession via reflection
  // -----------------------------------------------------------------------

  @Test void testExtractYearFromAccession2020s() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "extractYearFromAccession", String.class);
    m.setAccessible(true);

    DocumentETLProcessor processor = createMinimalProcessor();

    // Typical SEC accession: 0000070502-23-000456
    String result = (String) m.invoke(processor, "0000070502-23-000456");
    assertEquals("2023", result);
  }

  @Test void testExtractYearFromAccession1990s() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "extractYearFromAccession", String.class);
    m.setAccessible(true);

    DocumentETLProcessor processor = createMinimalProcessor();

    // 1990s accession
    String result = (String) m.invoke(processor, "0000070502-95-000456");
    assertEquals("1995", result);
  }

  @Test void testExtractYearFromAccession2000() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "extractYearFromAccession", String.class);
    m.setAccessible(true);

    DocumentETLProcessor processor = createMinimalProcessor();

    String result = (String) m.invoke(processor, "0000070502-00-000456");
    assertEquals("2000", result);
  }

  @Test void testExtractYearFromAccession2050() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "extractYearFromAccession", String.class);
    m.setAccessible(true);

    DocumentETLProcessor processor = createMinimalProcessor();

    String result = (String) m.invoke(processor, "0000070502-50-000456");
    assertEquals("2050", result);
  }

  @Test void testExtractYearFromAccession1951() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "extractYearFromAccession", String.class);
    m.setAccessible(true);

    DocumentETLProcessor processor = createMinimalProcessor();

    String result = (String) m.invoke(processor, "0000070502-51-000456");
    assertEquals("1951", result);
  }

  @Test void testExtractYearFromAccessionNull() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "extractYearFromAccession", String.class);
    m.setAccessible(true);

    DocumentETLProcessor processor = createMinimalProcessor();
    assertNull(m.invoke(processor, (String) null));
  }

  @Test void testExtractYearFromAccessionTooShort() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "extractYearFromAccession", String.class);
    m.setAccessible(true);

    DocumentETLProcessor processor = createMinimalProcessor();
    assertNull(m.invoke(processor, "short"));
  }

  @Test void testExtractYearFromAccessionNoDash() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "extractYearFromAccession", String.class);
    m.setAccessible(true);

    DocumentETLProcessor processor = createMinimalProcessor();
    assertNull(m.invoke(processor, "000007050225000456"));
  }

  @Test void testExtractYearFromAccessionNonNumericYear() throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "extractYearFromAccession", String.class);
    m.setAccessible(true);

    DocumentETLProcessor processor = createMinimalProcessor();
    assertNull(m.invoke(processor, "0000070502-XX-000456"));
  }

  // -----------------------------------------------------------------------
  // isAlreadyProcessed — with no tracker, exercises in-memory cache path
  // -----------------------------------------------------------------------

  @Test void testIsAlreadyProcessedMissingCik() {
    DocumentETLProcessor processor = createMinimalProcessor();

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("accession", "0000070502-23-000456");
    // No cik

    assertFalse(processor.isAlreadyProcessed(docVars));
  }

  @Test void testIsAlreadyProcessedMissingAccession() {
    DocumentETLProcessor processor = createMinimalProcessor();

    Map<String, String> docVars = new HashMap<String, String>();
    docVars.put("cik", "0000070502");
    // No accession

    assertFalse(processor.isAlreadyProcessed(docVars));
  }

  // -----------------------------------------------------------------------
  // DocumentETLResult
  // -----------------------------------------------------------------------

  @Test void testDocumentETLResultSuccess() {
    List<String> outputs = Arrays.asList("file1.parquet", "file2.parquet");
    List<String> errors = Collections.emptyList();
    DocumentETLProcessor.DocumentETLResult result =
        new DocumentETLProcessor.DocumentETLResult(5, 3, 0, outputs, errors, 1000L);

    assertEquals(5, result.getDocumentsProcessed());
    assertEquals(3, result.getDocumentsSkipped());
    assertEquals(0, result.getDocumentsFailed());
    assertEquals(2, result.getOutputFiles().size());
    assertTrue(result.getErrors().isEmpty());
    assertEquals(1000L, result.getDurationMs());
    assertTrue(result.isSuccess());
  }

  @Test void testDocumentETLResultFailure() {
    List<String> outputs = Arrays.asList("file1.parquet");
    List<String> errors = Arrays.asList("Error: failed");
    DocumentETLProcessor.DocumentETLResult result =
        new DocumentETLProcessor.DocumentETLResult(2, 1, 3, outputs, errors, 2000L);

    assertEquals(3, result.getDocumentsFailed());
    assertFalse(result.isSuccess());
  }

  @Test void testDocumentETLResultToString() {
    DocumentETLProcessor.DocumentETLResult result =
        new DocumentETLProcessor.DocumentETLResult(
            10, 5, 2,
            Arrays.asList("a.parquet", "b.parquet"),
            Arrays.asList("error1"),
            5000L);

    String str = result.toString();
    assertNotNull(str);
    assertTrue(str.contains("processed=10"));
    assertTrue(str.contains("skipped=5"));
    assertTrue(str.contains("failed=2"));
    assertTrue(str.contains("outputs=2"));
    assertTrue(str.contains("duration=5000ms"));
  }

  // -----------------------------------------------------------------------
  // parseDocuments (protected, 2-arg overload for backward compat)
  // -----------------------------------------------------------------------

  @Test void testParseDocumentsTwoArgOverload() {
    DocumentETLProcessor processor = createMinimalProcessor();

    // Empty metadata
    Map<String, String> baseVars = new HashMap<String, String>();
    baseVars.put("cik", "0000070502");

    Iterator<Map<String, String>> docs = processor.parseDocuments("{}", baseVars);
    assertNotNull(docs);
    assertFalse(docs.hasNext());
  }

  // -----------------------------------------------------------------------
  // Helper methods
  // -----------------------------------------------------------------------

  /**
   * Creates a minimal DocumentETLProcessor for unit testing static/utility methods.
   * Uses a simple in-memory StorageProvider stub.
   */
  private DocumentETLProcessor createMinimalProcessor() {
    // Create minimal HttpSourceConfig with a document source
    Map<String, Object> docSourceMap = new HashMap<String, Object>();
    docSourceMap.put("metadataUrl", "https://example.com/{cik}.json");
    docSourceMap.put("documentUrl", "https://example.com/{accession}/{document}");
    HttpSourceConfig.DocumentSourceConfig docConfig =
        HttpSourceConfig.DocumentSourceConfig.fromMap(docSourceMap);

    HttpSourceConfig config = HttpSourceConfig.builder()
        .documentSource(docConfig)
        .build();

    // Minimal StorageProvider stub
    org.apache.calcite.adapter.file.storage.StorageProvider storageProvider =
        new StubStorageProvider();

    // Minimal FileConverter
    org.apache.calcite.adapter.file.converters.FileConverter converter =
        new StubFileConverter();

    return new DocumentETLProcessor(
        config, storageProvider, "/output", "/cache", converter);
  }

  /**
   * Stub StorageProvider for testing.
   */
  private static class StubStorageProvider
      implements org.apache.calcite.adapter.file.storage.StorageProvider {

    @Override public java.util.List<FileEntry> listFiles(String path,
        boolean recursive) {
      return Collections.emptyList();
    }

    @Override public FileMetadata getMetadata(String path) {
      return new FileMetadata(path, 0, 0, null, null);
    }

    @Override public java.io.InputStream openInputStream(String path) throws IOException {
      throw new IOException("Stub: not implemented");
    }

    @Override public java.io.Reader openReader(String path) throws IOException {
      throw new IOException("Stub: not implemented");
    }

    @Override public boolean exists(String path) {
      return false;
    }

    @Override public boolean isDirectory(String path) {
      return false;
    }

    @Override public String getStorageType() {
      return "stub";
    }

    @Override public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }

    @Override public void writeFile(String path, byte[] content) {
      // no-op
    }
  }

  /**
   * Stub FileConverter for testing.
   */
  private static class StubFileConverter
      implements org.apache.calcite.adapter.file.converters.FileConverter {

    @Override public boolean canConvert(String sourceFormat, String targetFormat) {
      return true;
    }

    @Override public java.util.List<String> convert(String sourcePath,
        String targetDirectory,
        org.apache.calcite.adapter.file.metadata.ConversionMetadata metadata)
        throws IOException {
      return Collections.singletonList(targetDirectory + "/converted.parquet");
    }

    @Override public String getSourceFormat() {
      return "xml";
    }

    @Override public String getTargetFormat() {
      return "parquet";
    }
  }
}
