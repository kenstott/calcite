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
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for ConversionMetadata focusing on ConversionRecord,
 * PartitionBaseline, FileBaseline, metadata persistence, and change detection.
 */
@Tag("unit")
public class ConversionMetadataDeepCoverageTest3 {

  @TempDir
  Path tempDir;

  private ConversionMetadata metadata;

  @BeforeEach
  void setUp() {
    metadata = new ConversionMetadata(tempDir.toFile());
  }

  // ====================================================================
  // Tests for ConversionRecord constructors
  // ====================================================================

  @Test void testConversionRecordDefaultConstructor() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertNull(record.tableName);
    assertNull(record.sourceFile);
    assertNull(record.conversionType);
    assertNull(record.parquetCacheFile);
  }

  @Test void testConversionRecordThreeArgConstructor() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/orig.xlsx", "/conv.json", "EXCEL_TO_JSON");
    assertEquals("/orig.xlsx", record.originalFile);
    assertEquals("/conv.json", record.convertedFile);
    assertEquals("EXCEL_TO_JSON", record.conversionType);
    assertTrue(record.timestamp > 0);
  }

  @Test void testConversionRecordFourArgConstructor() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/orig.xlsx", "/conv.json",
            "EXCEL_TO_JSON", "/cache.parquet");
    assertEquals("/orig.xlsx", record.originalFile);
    assertEquals("/conv.json", record.convertedFile);
    assertEquals("EXCEL_TO_JSON", record.conversionType);
    assertEquals("/cache.parquet", record.parquetCacheFile);
  }

  @Test void testConversionRecordSevenArgConstructor() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/orig.csv", "/conv.parquet",
            "DIRECT", "/cache.parquet", "etag123", 5000L, "application/parquet");
    assertEquals("etag123", record.etag);
    assertEquals(Long.valueOf(5000L), record.contentLength);
    assertEquals("application/parquet", record.contentType);
  }

  @Test void testConversionRecordComprehensiveConstructor() {
    Map<String, Object> tableConfig = new HashMap<>();
    tableConfig.put("pattern", "**/*.parquet");

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("my_table", "ParquetTranslatableTable",
            "/src/data.parquet", "parquet",
            "/orig.csv", "/conv.json", "CSV_TO_JSON",
            "/cache.parquet", "s3://bucket/**/*.parquet",
            true, "PT5M",
            "etag456", 10000L, "text/csv", tableConfig);

    assertEquals("my_table", record.tableName);
    assertEquals("ParquetTranslatableTable", record.tableType);
    assertEquals("/src/data.parquet", record.sourceFile);
    assertEquals("parquet", record.sourceType);
    assertEquals("/orig.csv", record.originalFile);
    assertEquals("/conv.json", record.convertedFile);
    assertEquals("CSV_TO_JSON", record.conversionType);
    assertEquals("/cache.parquet", record.parquetCacheFile);
    assertEquals("s3://bucket/**/*.parquet", record.viewScanPattern);
    assertTrue(record.refreshEnabled);
    assertEquals("PT5M", record.refreshInterval);
    assertEquals("etag456", record.etag);
    assertEquals(Long.valueOf(10000L), record.contentLength);
    assertEquals("text/csv", record.contentType);
    assertSame(tableConfig, record.tableConfig);
  }

  // ====================================================================
  // Tests for ConversionRecord getter methods
  // ====================================================================

  @Test void testGetOriginalPath() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/orig.xlsx", "/conv.json", "EXCEL_TO_JSON");
    assertEquals("/orig.xlsx", record.getOriginalPath());
  }

  @Test void testGetConversionType() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/orig.xlsx", "/conv.json", "HTML_TO_JSON");
    assertEquals("HTML_TO_JSON", record.getConversionType());
  }

  @Test void testGetTableName() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "my_table";
    assertEquals("my_table", record.getTableName());
  }

  @Test void testGetSourceFile() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.sourceFile = "/data/source.csv";
    assertEquals("/data/source.csv", record.getSourceFile());
  }

  @Test void testGetParquetCacheFile() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.parquetCacheFile = "/cache/data.parquet";
    assertEquals("/cache/data.parquet", record.getParquetCacheFile());
  }

  @Test void testGetConvertedFile() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = "/conv/data.json";
    assertEquals("/conv/data.json", record.getConvertedFile());
  }

  // ====================================================================
  // Tests for isIcebergFormat
  // ====================================================================

  @Test void testIsIcebergFormatWithNullTableConfig() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = null;
    assertFalse(record.isIcebergFormat());
  }

  @Test void testIsIcebergFormatWithNoMaterializeKey() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<>();
    assertFalse(record.isIcebergFormat());
  }

  @Test void testIsIcebergFormatWithNonMapMaterialize() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<>();
    record.tableConfig.put("materialize", "invalid");
    assertFalse(record.isIcebergFormat());
  }

  @Test void testIsIcebergFormatWithIcebergFormat() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<>();
    Map<String, Object> materialize = new HashMap<>();
    materialize.put("format", "iceberg");
    record.tableConfig.put("materialize", materialize);
    assertTrue(record.isIcebergFormat());
  }

  @Test void testIsIcebergFormatWithIcebergFormatCaseInsensitive() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<>();
    Map<String, Object> materialize = new HashMap<>();
    materialize.put("format", "ICEBERG");
    record.tableConfig.put("materialize", materialize);
    assertTrue(record.isIcebergFormat());
  }

  @Test void testIsIcebergFormatWithParquetFormat() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<>();
    Map<String, Object> materialize = new HashMap<>();
    materialize.put("format", "parquet");
    record.tableConfig.put("materialize", materialize);
    assertFalse(record.isIcebergFormat());
  }

  // ====================================================================
  // Tests for hasChanged
  // ====================================================================

  @Test void testHasChangedLocalFileExists() throws IOException {
    File sourceFile = new File(tempDir.toFile(), "source.csv");
    Files.write(sourceFile.toPath(), "data".getBytes());

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = sourceFile.getAbsolutePath();
    // Set timestamp to far future so file has NOT changed
    record.timestamp = System.currentTimeMillis() + 100000;

    assertFalse(record.hasChanged());
  }

  @Test void testHasChangedLocalFileNonExistent() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "/nonexistent/file.csv";
    record.timestamp = System.currentTimeMillis();

    // Non-existent local file returns false (no change possible)
    assertFalse(record.hasChanged());
  }

  @Test void testHasChangedRemoteFile() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "https://example.com/data.csv";
    record.timestamp = System.currentTimeMillis();

    // Remote files always return true (conservative: assume changed)
    assertTrue(record.hasChanged());
  }

  @Test void testHasChangedS3File() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "s3://bucket/data.csv";
    record.timestamp = System.currentTimeMillis();

    assertTrue(record.hasChanged());
  }

  // ====================================================================
  // Tests for hasChangedViaMetadata
  // ====================================================================

  @Test void testHasChangedViaMetadataWithNullMetadata() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertTrue(record.hasChangedViaMetadata(null));
  }

  @Test void testHasChangedViaMetadataEtagMatch() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = "abc123";
    record.originalFile = "file.csv";

    StorageProvider.FileMetadata meta = mock(StorageProvider.FileMetadata.class);
    when(meta.getEtag()).thenReturn("abc123");

    assertFalse(record.hasChangedViaMetadata(meta));
  }

  @Test void testHasChangedViaMetadataEtagMismatch() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = "abc123";
    record.originalFile = "file.csv";

    StorageProvider.FileMetadata meta = mock(StorageProvider.FileMetadata.class);
    when(meta.getEtag()).thenReturn("def456");

    assertTrue(record.hasChangedViaMetadata(meta));
  }

  @Test void testHasChangedViaMetadataSizeChanged() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = null;
    record.contentLength = 1000L;
    record.originalFile = "file.csv";

    StorageProvider.FileMetadata meta = mock(StorageProvider.FileMetadata.class);
    when(meta.getEtag()).thenReturn(null);
    when(meta.getSize()).thenReturn(2000L);

    assertTrue(record.hasChangedViaMetadata(meta));
  }

  @Test void testHasChangedViaMetadataTimestampSame() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = null;
    record.contentLength = null;
    record.timestamp = 1000000L;
    record.originalFile = "file.csv";

    StorageProvider.FileMetadata meta = mock(StorageProvider.FileMetadata.class);
    when(meta.getEtag()).thenReturn(null);
    when(meta.getSize()).thenReturn(0L);
    when(meta.getLastModified()).thenReturn(1000500L); // within 1 second tolerance

    assertFalse(record.hasChangedViaMetadata(meta));
  }

  @Test void testHasChangedViaMetadataTimestampDifferent() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = null;
    record.contentLength = null;
    record.timestamp = 1000000L;
    record.originalFile = "file.csv";

    StorageProvider.FileMetadata meta = mock(StorageProvider.FileMetadata.class);
    when(meta.getEtag()).thenReturn(null);
    when(meta.getSize()).thenReturn(0L);
    when(meta.getLastModified()).thenReturn(1005000L); // 5 second diff > 1 second tolerance

    assertTrue(record.hasChangedViaMetadata(meta));
  }

  // ====================================================================
  // Tests for updateMetadata
  // ====================================================================

  @Test void testUpdateMetadata() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "test.csv";

    StorageProvider.FileMetadata meta = mock(StorageProvider.FileMetadata.class);
    when(meta.getEtag()).thenReturn("new_etag");
    when(meta.getSize()).thenReturn(4096L);
    when(meta.getContentType()).thenReturn("text/csv");
    when(meta.getLastModified()).thenReturn(2000000L);

    record.updateMetadata(meta);

    assertEquals("new_etag", record.etag);
    assertEquals(Long.valueOf(4096L), record.contentLength);
    assertEquals("text/csv", record.contentType);
    assertEquals(2000000L, record.timestamp);
  }

  @Test void testUpdateMetadataWithNull() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = "old_etag";
    record.originalFile = "test.csv";

    record.updateMetadata(null);
    // Should not change when metadata is null
    assertEquals("old_etag", record.etag);
  }

  // ====================================================================
  // Tests for FileBaseline
  // ====================================================================

  @Test void testFileBaselineDefaultConstructor() {
    ConversionMetadata.FileBaseline baseline = new ConversionMetadata.FileBaseline();
    assertNull(baseline.size);
    assertNull(baseline.etag);
    assertNull(baseline.lastModified);
  }

  @Test void testFileBaselineThreeArgConstructor() {
    ConversionMetadata.FileBaseline baseline =
        new ConversionMetadata.FileBaseline(1024L, "etag123", 5000L);
    assertEquals(Long.valueOf(1024L), baseline.size);
    assertEquals("etag123", baseline.etag);
    assertEquals(Long.valueOf(5000L), baseline.lastModified);
  }

  @Test void testFileBaselineHasChangedWithNull() {
    ConversionMetadata.FileBaseline baseline =
        new ConversionMetadata.FileBaseline(1024L, "etag123", 5000L);
    assertTrue(baseline.hasChanged(null));
  }

  @Test void testFileBaselineHasChangedEtagMatch() {
    ConversionMetadata.FileBaseline old = new ConversionMetadata.FileBaseline(1024L, "etag1", 5000L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(1024L, "etag1", 5000L);
    assertFalse(old.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedEtagMismatch() {
    ConversionMetadata.FileBaseline old = new ConversionMetadata.FileBaseline(1024L, "etag1", 5000L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(1024L, "etag2", 5000L);
    assertTrue(old.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedSizeDifferent() {
    ConversionMetadata.FileBaseline old = new ConversionMetadata.FileBaseline(1024L, null, 5000L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(2048L, null, 5000L);
    assertTrue(old.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedTimestampDifferent() {
    ConversionMetadata.FileBaseline old = new ConversionMetadata.FileBaseline(1024L, null, 5000L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(1024L, null, 10000L);
    assertTrue(old.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedTimestampWithinTolerance() {
    ConversionMetadata.FileBaseline old = new ConversionMetadata.FileBaseline(1024L, null, 5000L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(1024L, null, 5500L);
    assertFalse(old.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedAllNull() {
    ConversionMetadata.FileBaseline old = new ConversionMetadata.FileBaseline(null, null, null);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(null, null, null);
    assertFalse(old.hasChanged(current));
  }

  // ====================================================================
  // Tests for PartitionBaseline
  // ====================================================================

  @Test void testPartitionBaselineDefaultConstructor() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    assertTrue(baseline.isEmpty());
  }

  @Test void testPartitionBaselineIsEmptyWithNullFiles() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = null;
    assertTrue(baseline.isEmpty());
  }

  @Test void testPartitionBaselineIsEmptyWithEmptyMap() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<>();
    assertTrue(baseline.isEmpty());
  }

  @Test void testPartitionBaselineIsNotEmpty() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<>();
    baseline.files.put("/file.parquet", new ConversionMetadata.FileBaseline(100L, "e", 1000L));
    assertFalse(baseline.isEmpty());
  }

  // ====================================================================
  // Tests for isHivePartitioned (private static method)
  // ====================================================================

  @Test void testIsHivePartitionedWithNullList() throws Exception {
    boolean result = invokeIsHivePartitioned(null);
    assertFalse(result);
  }

  @Test void testIsHivePartitionedWithEmptyList() throws Exception {
    boolean result = invokeIsHivePartitioned(Collections.emptyList());
    assertFalse(result);
  }

  @Test void testIsHivePartitionedWithSingleFile() throws Exception {
    boolean result =
        invokeIsHivePartitioned(Collections.singletonList("/data/year=2020/file.parquet"));
    assertFalse(result, "Need at least 2 files");
  }

  @Test void testIsHivePartitionedWithHiveFiles() throws Exception {
    List<String> files =
        Arrays.asList("/data/year=2020/file1.parquet",
        "/data/year=2021/file2.parquet",
        "/data/year=2022/file3.parquet");
    boolean result = invokeIsHivePartitioned(files);
    assertTrue(result);
  }

  @Test void testIsHivePartitionedWithNonHiveFiles() throws Exception {
    List<String> files =
        Arrays.asList("/data/2020/file1.parquet",
        "/data/2021/file2.parquet");
    boolean result = invokeIsHivePartitioned(files);
    assertFalse(result);
  }

  @Test void testIsHivePartitionedMixed() throws Exception {
    // Only 1 of 3 files is Hive-partitioned (< 50%)
    List<String> files =
        Arrays.asList("/data/year=2020/file1.parquet",
        "/data/plain/file2.parquet",
        "/data/flat/file3.parquet");
    boolean result = invokeIsHivePartitioned(files);
    assertFalse(result);
  }

  // ====================================================================
  // Tests for extractTableSpecificPattern (private static method)
  // ====================================================================

  @Test void testExtractTableSpecificPatternWithNull() throws Exception {
    String result = invokeExtractTableSpecificPattern(null);
    assertNull(result);
  }

  @Test void testExtractTableSpecificPatternWithEmpty() throws Exception {
    String result = invokeExtractTableSpecificPattern(Collections.emptyList());
    assertNull(result);
  }

  @Test void testExtractTableSpecificPatternWithHiveFiles() throws Exception {
    List<String> files =
        Arrays.asList("s3://bucket/schema/table/year=2020/file1.parquet",
        "s3://bucket/schema/table/year=2021/file2.parquet");
    String result = invokeExtractTableSpecificPattern(files);
    assertNotNull(result);
    assertTrue(result.contains("/**/*"));
    assertTrue(result.endsWith(".parquet"));
  }

  @Test void testExtractTableSpecificPatternWithNonPartitionedFiles() throws Exception {
    List<String> files =
        Arrays.asList("/data/table/file1.parquet",
        "/data/table/file2.parquet");
    String result = invokeExtractTableSpecificPattern(files);
    assertNotNull(result);
    assertTrue(result.contains("/**/*"));
  }

  // ====================================================================
  // Tests for findLongestCommonPrefix (private static method)
  // ====================================================================

  @Test void testFindLongestCommonPrefixEmpty() throws Exception {
    String result = invokeFindLongestCommonPrefix(Collections.emptyList());
    assertEquals("", result);
  }

  @Test void testFindLongestCommonPrefixSingleFile() throws Exception {
    String result =
        invokeFindLongestCommonPrefix(Collections.singletonList("/data/file.parquet"));
    assertEquals("/data/file.parquet", result);
  }

  @Test void testFindLongestCommonPrefixMultipleFiles() throws Exception {
    List<String> files =
        Arrays.asList("/data/schema/table/file1.parquet",
        "/data/schema/table/file2.parquet",
        "/data/schema/other/file3.parquet");
    String result = invokeFindLongestCommonPrefix(files);
    assertTrue(result.startsWith("/data/schema/"));
  }

  @Test void testFindLongestCommonPrefixNoCommon() throws Exception {
    List<String> files =
        Arrays.asList("/alpha/file1.parquet",
        "/beta/file2.parquet");
    String result = invokeFindLongestCommonPrefix(files);
    assertEquals("/", result);
  }

  // ====================================================================
  // Tests for hints
  // ====================================================================

  @Test void testSetAndGetHint() {
    metadata.setHint("cik", "0001234567");
    assertEquals("0001234567", metadata.getHint("cik"));
  }

  @Test void testSetHintWithNullValueIgnored() {
    metadata.setHint("key", null);
    assertNull(metadata.getHint("key"));
  }

  @Test void testGetHintNonExistent() {
    assertNull(metadata.getHint("nonexistent"));
  }

  // ====================================================================
  // Tests for recordConversion and retrieval
  // ====================================================================

  @Test void testRecordConversionTwoFileArgs() throws IOException {
    File original = new File(tempDir.toFile(), "source.xlsx");
    File converted = new File(tempDir.toFile(), "output.json");
    Files.write(original.toPath(), "data".getBytes());
    Files.write(converted.toPath(), "{}".getBytes());

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertEquals("EXCEL_TO_JSON", record.getConversionType());
  }

  @Test void testRecordConversionWithCacheFile() throws IOException {
    File original = new File(tempDir.toFile(), "source.csv");
    File converted = new File(tempDir.toFile(), "output.json");
    File cache = new File(tempDir.toFile(), "cache.parquet");
    Files.write(original.toPath(), "data".getBytes());
    Files.write(converted.toPath(), "{}".getBytes());
    Files.write(cache.toPath(), new byte[0]);

    metadata.recordConversion(original, converted, "CSV_TO_JSON", cache);

    ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertNotNull(record.parquetCacheFile);
  }

  @Test void testRecordConversionWithRecord() throws IOException {
    File converted = new File(tempDir.toFile(), "output.json");
    Files.write(converted.toPath(), "{}".getBytes());

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/orig.html", converted.getAbsolutePath(),
            "HTML_TO_JSON", null, "etag789", 2048L, "text/html");

    metadata.recordConversion(converted, record);

    ConversionMetadata.ConversionRecord retrieved = metadata.getConversionRecord(converted);
    assertNotNull(retrieved);
    assertEquals("etag789", retrieved.etag);
  }

  // ====================================================================
  // Tests for findOriginalSource
  // ====================================================================

  @Test void testFindOriginalSourceExisting() throws IOException {
    File original = new File(tempDir.toFile(), "source.xlsx");
    File converted = new File(tempDir.toFile(), "output.json");
    Files.write(original.toPath(), "data".getBytes());
    Files.write(converted.toPath(), "{}".getBytes());

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    File found = metadata.findOriginalSource(converted);
    assertNotNull(found);
    assertEquals(original.getCanonicalPath(), found.getCanonicalPath());
  }

  @Test void testFindOriginalSourceNotRecorded() throws IOException {
    File notRecorded = new File(tempDir.toFile(), "unknown.json");
    Files.write(notRecorded.toPath(), "{}".getBytes());

    File found = metadata.findOriginalSource(notRecorded);
    assertNull(found);
  }

  @Test void testFindOriginalSourceDeletedOriginal() throws IOException {
    File original = new File(tempDir.toFile(), "temp_source.xlsx");
    File converted = new File(tempDir.toFile(), "output2.json");
    Files.write(original.toPath(), "data".getBytes());
    Files.write(converted.toPath(), "{}".getBytes());

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    // Delete the original file
    assertTrue(original.delete());

    // Should return null and clean up the stale record
    File found = metadata.findOriginalSource(converted);
    assertNull(found);
  }

  // ====================================================================
  // Tests for findDerivedFiles
  // ====================================================================

  @Test void testFindDerivedFiles() throws IOException {
    File original = new File(tempDir.toFile(), "source.html");
    File derived1 = new File(tempDir.toFile(), "table1.json");
    File derived2 = new File(tempDir.toFile(), "table2.json");
    Files.write(original.toPath(), "<html>".getBytes());
    Files.write(derived1.toPath(), "{}".getBytes());
    Files.write(derived2.toPath(), "{}".getBytes());

    metadata.recordConversion(original, derived1, "HTML_TO_JSON");
    metadata.recordConversion(original, derived2, "HTML_TO_JSON");

    List<File> derivedFiles = metadata.findDerivedFiles(original);
    assertEquals(2, derivedFiles.size());
  }

  @Test void testFindDerivedFilesNone() throws IOException {
    File original = new File(tempDir.toFile(), "no_derived.html");
    Files.write(original.toPath(), "<html>".getBytes());

    List<File> derivedFiles = metadata.findDerivedFiles(original);
    assertTrue(derivedFiles.isEmpty());
  }

  // ====================================================================
  // Tests for updateCachedFile
  // ====================================================================

  @Test void testUpdateCachedFile() throws IOException {
    File original = new File(tempDir.toFile(), "src.csv");
    File converted = new File(tempDir.toFile(), "out.json");
    File cache = new File(tempDir.toFile(), "cache.parquet");
    Files.write(original.toPath(), "data".getBytes());
    Files.write(converted.toPath(), "{}".getBytes());
    Files.write(cache.toPath(), new byte[0]);

    metadata.recordConversion(original, converted, "CSV_TO_JSON");
    metadata.updateCachedFile(converted, cache);

    ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertNotNull(record.parquetCacheFile);
  }

  @Test void testUpdateCachedFileNoExistingRecord() throws IOException {
    File converted = new File(tempDir.toFile(), "no_record.json");
    File cache = new File(tempDir.toFile(), "cache2.parquet");
    Files.write(converted.toPath(), "{}".getBytes());
    Files.write(cache.toPath(), new byte[0]);

    // Should not throw, just silently skip
    metadata.updateCachedFile(converted, cache);
  }

  // ====================================================================
  // Tests for updateMaterializationInfo
  // ====================================================================

  @Test void testUpdateMaterializationInfoExistingRecord() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "my_table";
    record.conversionType = "DIRECT";
    record.sourceFile = "/data/original";
    metadata.putConversionRecord("my_table", record);

    metadata.updateMaterializationInfo("my_table", "/data/iceberg/my_table",
        "ICEBERG_PARQUET", 50000L);

    Map<String, ConversionMetadata.ConversionRecord> all = metadata.getAllConversions();
    ConversionMetadata.ConversionRecord updated = all.get("my_table");
    assertNotNull(updated);
    assertEquals("ICEBERG_PARQUET", updated.conversionType);
    assertEquals("/data/iceberg/my_table", updated.sourceFile);
    assertEquals("IcebergTable", updated.tableType);
    assertEquals(Long.valueOf(50000L), updated.rowCount);
    assertNull(updated.viewScanPattern);
    assertNull(updated.convertedFile);
    assertNull(updated.parquetCacheFile);
  }

  @Test void testUpdateMaterializationInfoNewRecord() {
    metadata.updateMaterializationInfo("new_table", "/data/new_table",
        "PARQUET", 1000L);

    ConversionMetadata.ConversionRecord record = metadata.getAllConversions().get("new_table");
    assertNotNull(record);
    assertEquals("PARQUET", record.conversionType);
    assertEquals("ParquetTable", record.tableType);
    assertEquals(Long.valueOf(1000L), record.rowCount);
  }

  @Test void testUpdateMaterializationInfoWithNullRowCount() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "table_with_count";
    record.rowCount = 5000L;
    metadata.putConversionRecord("table_with_count", record);

    // Null rowCount should preserve existing
    metadata.updateMaterializationInfo("table_with_count", "/data/loc",
        "PARQUET", null);

    ConversionMetadata.ConversionRecord updated = metadata.getAllConversions().get("table_with_count");
    assertEquals(Long.valueOf(5000L), updated.rowCount);
  }

  @Test void testUpdateMaterializationInfoThreeArgOverload() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "table3arg";
    metadata.putConversionRecord("table3arg", record);

    metadata.updateMaterializationInfo("table3arg", "/data/loc", "PARQUET");

    ConversionMetadata.ConversionRecord updated = metadata.getAllConversions().get("table3arg");
    assertEquals("PARQUET", updated.conversionType);
  }

  // ====================================================================
  // Tests for updateRecordWithParquetFile
  // ====================================================================

  @Test void testUpdateRecordWithParquetFile() throws IOException {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "table_parquet";
    metadata.putConversionRecord("table_parquet", record);

    File parquetFile = new File(tempDir.toFile(), "data.parquet");
    Files.write(parquetFile.toPath(), new byte[0]);

    metadata.updateRecordWithParquetFile("table_parquet", parquetFile);

    ConversionMetadata.ConversionRecord updated = metadata.getAllConversions().get("table_parquet");
    assertNotNull(updated.parquetCacheFile);
    assertEquals("ParquetTranslatableTable", updated.tableType);
  }

  @Test void testUpdateRecordWithParquetFileNoExistingRecord() throws IOException {
    File parquetFile = new File(tempDir.toFile(), "orphan.parquet");
    Files.write(parquetFile.toPath(), new byte[0]);

    // Should not throw, just warn
    metadata.updateRecordWithParquetFile("nonexistent_table", parquetFile);
  }

  // ====================================================================
  // Tests for hasTableMetadata and getAllConversions
  // ====================================================================

  @Test void testHasTableMetadata() {
    assertFalse(metadata.hasTableMetadata("missing"));

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    metadata.putConversionRecord("present", record);

    assertTrue(metadata.hasTableMetadata("present"));
  }

  @Test void testGetAllConversionsUnmodifiable() {
    Map<String, ConversionMetadata.ConversionRecord> all = metadata.getAllConversions();
    try {
      all.put("key", new ConversionMetadata.ConversionRecord());
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // expected
    }
  }

  // ====================================================================
  // Tests for clear and reload
  // ====================================================================

  @Test void testClear() throws IOException {
    File original = new File(tempDir.toFile(), "src_clear.csv");
    File converted = new File(tempDir.toFile(), "out_clear.json");
    Files.write(original.toPath(), "data".getBytes());
    Files.write(converted.toPath(), "{}".getBytes());

    metadata.recordConversion(original, converted, "DIRECT");
    assertFalse(metadata.getAllConversions().isEmpty());

    metadata.clear();
    assertTrue(metadata.getAllConversions().isEmpty());
  }

  @Test void testReload() throws IOException {
    File original = new File(tempDir.toFile(), "src_reload.csv");
    File converted = new File(tempDir.toFile(), "out_reload.json");
    Files.write(original.toPath(), "data".getBytes());
    Files.write(converted.toPath(), "{}".getBytes());

    metadata.recordConversion(original, converted, "CSV_TO_JSON");

    // Create another instance and verify it loads the saved metadata
    ConversionMetadata metadata2 = new ConversionMetadata(tempDir.toFile());
    assertFalse(metadata2.getAllConversions().isEmpty());

    // Reload should work without error
    metadata2.reload();
  }

  // ====================================================================
  // Tests for formatRecord (private method)
  // ====================================================================

  @Test void testFormatRecordNull() throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("formatRecord", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    String result = (String) method.invoke(metadata, (ConversionMetadata.ConversionRecord) null);
    assertEquals("null", result);
  }

  @Test void testFormatRecordTruncatesLongParquetCacheFile() throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "test";
    StringBuilder longPath = new StringBuilder();
    for (int i = 0; i < 250; i++) {
      longPath.append("x");
    }
    record.parquetCacheFile = longPath.toString();

    Method method =
        ConversionMetadata.class.getDeclaredMethod("formatRecord", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    String result = (String) method.invoke(metadata, record);
    assertTrue(result.contains("..."));
  }

  // ====================================================================
  // Tests for isRemoteFile and isGlobPattern (private methods)
  // ====================================================================

  @Test void testIsRemoteFile() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("isRemoteFile", String.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(metadata, "http://example.com/data.csv"));
    assertTrue((Boolean) method.invoke(metadata, "https://example.com/data.csv"));
    assertTrue((Boolean) method.invoke(metadata, "s3://bucket/data.csv"));
    assertTrue((Boolean) method.invoke(metadata, "ftp://server/data.csv"));
    assertTrue((Boolean) method.invoke(metadata, "sftp://server/data.csv"));
    assertFalse((Boolean) method.invoke(metadata, "/local/data.csv"));
    assertFalse((Boolean) method.invoke(metadata, (String) null));
  }

  @Test void testIsGlobPattern() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("isGlobPattern", String.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(metadata, "/data/**/*.parquet"));
    assertTrue((Boolean) method.invoke(metadata, "/data/file?.parquet"));
    assertTrue((Boolean) method.invoke(metadata, "/data/[a-z].parquet"));
    assertTrue((Boolean) method.invoke(metadata, "/data/test].parquet"));
    assertFalse((Boolean) method.invoke(metadata, "/data/file.parquet"));
    assertFalse((Boolean) method.invoke(metadata, (String) null));
  }

  // ====================================================================
  // Tests for detectSourceType (private method)
  // ====================================================================

  @Test void testDetectSourceTypeNull() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("detectSourceType", String.class);
    method.setAccessible(true);
    assertEquals("unknown", method.invoke(metadata, (String) null));
  }

  @Test void testDetectSourceTypeKnownTypes() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("detectSourceType", String.class);
    method.setAccessible(true);

    assertEquals("parquet", method.invoke(metadata, "data.parquet"));
    assertEquals("csv", method.invoke(metadata, "data.csv"));
    assertEquals("json", method.invoke(metadata, "data.json"));
  }

  // ====================================================================
  // Tests for getConversionRecordByConvertedFile
  // ====================================================================

  @Test void testGetConversionRecordByConvertedFileDirect() throws IOException {
    File original = new File(tempDir.toFile(), "src_byconv.csv");
    File converted = new File(tempDir.toFile(), "out_byconv.json");
    Files.write(original.toPath(), "data".getBytes());
    Files.write(converted.toPath(), "{}".getBytes());

    metadata.recordConversion(original, converted, "CSV_TO_JSON");

    ConversionMetadata.ConversionRecord found =
        metadata.getConversionRecordByConvertedFile(converted.getAbsolutePath());
    assertNotNull(found);
    assertEquals("CSV_TO_JSON", found.conversionType);
  }

  @Test void testGetConversionRecordByConvertedFileSearches() throws IOException {
    // Store record under table name key, then search by converted file
    File converted = new File(tempDir.toFile(), "search_conv.json");
    Files.write(converted.toPath(), "{}".getBytes());

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.convertedFile = converted.getCanonicalPath();
    record.conversionType = "DIRECT";
    metadata.putConversionRecord("my_table_search", record);

    ConversionMetadata.ConversionRecord found =
        metadata.getConversionRecordByConvertedFile(converted.getAbsolutePath());
    assertNotNull(found);
  }

  @Test void testGetConversionRecordByConvertedFileNotFound() {
    ConversionMetadata.ConversionRecord found =
        metadata.getConversionRecordByConvertedFile("/nonexistent/file.json");
    assertNull(found);
  }

  // ====================================================================
  // Reflection helpers
  // ====================================================================

  @SuppressWarnings("unchecked")
  private boolean invokeIsHivePartitioned(List<String> filePaths) throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("isHivePartitioned", java.util.List.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(null, filePaths);
  }

  @SuppressWarnings("unchecked")
  private String invokeExtractTableSpecificPattern(List<String> filePaths) throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("extractTableSpecificPattern", java.util.List.class);
    method.setAccessible(true);
    return (String) method.invoke(null, filePaths);
  }

  @SuppressWarnings("unchecked")
  private String invokeFindLongestCommonPrefix(List<String> filePaths) throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("findLongestCommonPrefix", java.util.List.class);
    method.setAccessible(true);
    return (String) method.invoke(null, filePaths);
  }
}
