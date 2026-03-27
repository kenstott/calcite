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
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link ConversionMetadata} targeting uncovered
 * methods: ConversionRecord inner class methods, PartitionBaseline,
 * FileBaseline, hint accessors, recordTable, recordConversion variants,
 * findOriginalSource, reload, S3 path handling, and change detection.
 */
@Tag("unit")
public class ConversionMetadataDeepCoverageTest {

  @TempDir
  Path tempDir;

  private File metadataDir;

  @BeforeEach
  void setUp() {
    metadataDir = tempDir.resolve("metadata").toFile();
    metadataDir.mkdirs();
  }

  // --- Constructor tests ---

  @Test
  void testConstructorWithLocalDirectory() {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    assertNotNull(cm);
    assertNotNull(cm.getAllConversions());
    assertTrue(cm.getAllConversions().isEmpty());
  }

  @Test
  void testConstructorWithStringLocalPath() {
    ConversionMetadata cm = new ConversionMetadata(metadataDir.getAbsolutePath());
    assertNotNull(cm);
  }

  @Test
  void testConstructorWithS3Path() {
    // S3 path should not attempt to load from local filesystem
    ConversionMetadata cm = new ConversionMetadata("s3://my-bucket/my-prefix");
    assertNotNull(cm);
  }

  // --- Hint accessors ---

  @Test
  void testSetAndGetHint() {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    assertNull(cm.getHint("cik"));

    cm.setHint("cik", "0001234567");
    assertEquals("0001234567", cm.getHint("cik"));
  }

  @Test
  void testSetHintNullValueIgnored() {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    cm.setHint("key", null);
    assertNull(cm.getHint("key"));
  }

  // --- recordConversion variants ---

  @Test
  void testRecordConversionTwoArgs() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File original = new File(metadataDir, "source.xlsx");
    original.createNewFile();
    File converted = new File(metadataDir, "source.json");
    converted.createNewFile();

    cm.recordConversion(original, converted, "EXCEL_TO_JSON");

    Map<String, ConversionMetadata.ConversionRecord> records = cm.getAllConversions();
    assertFalse(records.isEmpty());
  }

  @Test
  void testRecordConversionWithParquetCache() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File original = new File(metadataDir, "data.html");
    original.createNewFile();
    File converted = new File(metadataDir, "data.json");
    converted.createNewFile();
    File parquetCache = new File(metadataDir, "data.parquet");
    parquetCache.createNewFile();

    cm.recordConversion(original, converted, "HTML_TO_JSON", parquetCache);

    Map<String, ConversionMetadata.ConversionRecord> records = cm.getAllConversions();
    assertFalse(records.isEmpty());
  }

  @Test
  void testRecordConversionWithPreBuiltRecord() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File converted = new File(metadataDir, "result.json");
    converted.createNewFile();

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("http://example.com/data",
            converted.getAbsolutePath(), "HTTP_DOWNLOAD", null,
            "etag123", 1024L, "application/json");

    cm.recordConversion(converted, record);

    Map<String, ConversionMetadata.ConversionRecord> records = cm.getAllConversions();
    assertFalse(records.isEmpty());
  }

  // --- recordConversionWithTableName ---

  @Test
  void testRecordConversionWithTableName() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File original = new File(metadataDir, "data.xlsx");
    original.createNewFile();
    File converted = new File(metadataDir, "data.json");
    converted.createNewFile();

    cm.recordConversionWithTableName("my_table", original, converted, "EXCEL_TO_JSON");

    Map<String, ConversionMetadata.ConversionRecord> records = cm.getAllConversions();
    assertFalse(records.isEmpty());
  }

  // --- findOriginalSource ---

  @Test
  void testFindOriginalSourceNotFound() {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File jsonFile = new File(metadataDir, "unknown.json");
    File result = cm.findOriginalSource(jsonFile);
    assertNull(result);
  }

  @Test
  void testFindOriginalSourceFound() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File original = new File(metadataDir, "original.xlsx");
    original.createNewFile();
    File converted = new File(metadataDir, "original.json");
    converted.createNewFile();

    cm.recordConversion(original, converted, "EXCEL_TO_JSON");

    File result = cm.findOriginalSource(converted);
    assertNotNull(result);
  }

  // --- reload ---

  @Test
  void testReload() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File original = new File(metadataDir, "before.xlsx");
    original.createNewFile();
    File converted = new File(metadataDir, "before.json");
    converted.createNewFile();

    cm.recordConversion(original, converted, "EXCEL_TO_JSON");
    int sizeBefore = cm.getAllConversions().size();

    // Reload should re-read from disk
    cm.reload();
    int sizeAfter = cm.getAllConversions().size();
    assertEquals(sizeBefore, sizeAfter);
  }

  // --- updateRecordWithParquetFile ---

  @Test
  void testUpdateRecordWithParquetFile() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File original = new File(metadataDir, "update_test.xlsx");
    original.createNewFile();
    File converted = new File(metadataDir, "update_test.json");
    converted.createNewFile();

    cm.recordConversionWithTableName("update_table", original, converted, "EXCEL_TO_JSON");

    File parquetFile = new File(metadataDir, "update_test.parquet");
    parquetFile.createNewFile();

    cm.updateRecordWithParquetFile("update_table", parquetFile);
    // Should not throw
  }

  // --- ConversionRecord inner class ---

  @Test
  void testConversionRecordDefaultConstructor() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertNull(record.tableName);
    assertNull(record.sourceFile);
    assertNull(record.conversionType);
  }

  @Test
  void testConversionRecordThreeArgConstructor() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/path/original", "/path/converted", "EXCEL_TO_JSON");
    assertEquals("/path/original", record.getOriginalPath());
    assertEquals("EXCEL_TO_JSON", record.getConversionType());
    assertEquals("/path/converted", record.getConvertedFile());
    assertTrue(record.timestamp > 0);
  }

  @Test
  void testConversionRecordFourArgConstructor() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/original", "/converted", "HTML_TO_JSON", "/cache.parquet");
    assertEquals("/original", record.getOriginalPath());
    assertEquals("/cache.parquet", record.getParquetCacheFile());
  }

  @Test
  void testConversionRecordHttpMetadataConstructor() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("http://source",
            "/converted", "HTTP_DOWNLOAD", "/cache.parquet",
            "etag-123", 2048L, "application/json");
    assertEquals("etag-123", record.etag);
    assertEquals(Long.valueOf(2048L), record.contentLength);
    assertEquals("application/json", record.contentType);
  }

  @Test
  void testConversionRecordComprehensiveConstructor() {
    Map<String, Object> tableConfig = new HashMap<>();
    tableConfig.put("key", "value");

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("myTable", "ParquetTable",
            "/source.csv", "csv", "/original.xlsx", "/converted.json",
            "EXCEL_TO_JSON", "/cache.parquet", "s3://bucket/**/*.parquet",
            true, "5 minutes", "etag-abc", 4096L, "text/csv", tableConfig);

    assertEquals("myTable", record.getTableName());
    assertEquals("ParquetTable", record.tableType);
    assertEquals("/source.csv", record.getSourceFile());
    assertEquals("csv", record.sourceType);
    assertEquals("/original.xlsx", record.getOriginalPath());
    assertEquals("/converted.json", record.getConvertedFile());
    assertEquals("EXCEL_TO_JSON", record.getConversionType());
    assertEquals("/cache.parquet", record.getParquetCacheFile());
    assertEquals("s3://bucket/**/*.parquet", record.viewScanPattern);
    assertTrue(record.refreshEnabled);
    assertEquals("5 minutes", record.refreshInterval);
  }

  // --- isIcebergFormat ---

  @Test
  void testIsIcebergFormatNullConfig() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertFalse(record.isIcebergFormat());
  }

  @Test
  void testIsIcebergFormatNoMaterialize() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<>();
    assertFalse(record.isIcebergFormat());
  }

  @Test
  void testIsIcebergFormatTrue() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> tableConfig = new HashMap<>();
    Map<String, Object> materialize = new HashMap<>();
    materialize.put("format", "iceberg");
    tableConfig.put("materialize", materialize);
    record.tableConfig = tableConfig;
    assertTrue(record.isIcebergFormat());
  }

  @Test
  void testIsIcebergFormatFalseOtherFormat() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> tableConfig = new HashMap<>();
    Map<String, Object> materialize = new HashMap<>();
    materialize.put("format", "parquet");
    tableConfig.put("materialize", materialize);
    record.tableConfig = tableConfig;
    assertFalse(record.isIcebergFormat());
  }

  // --- hasChanged ---

  @Test
  void testHasChangedLocalFileNotExists() throws IOException {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "/nonexistent/file.xlsx";
    record.timestamp = System.currentTimeMillis();
    // Local file that doesn't exist - returns false
    assertFalse(record.hasChanged());
  }

  @Test
  void testHasChangedLocalFileUnchanged() throws IOException {
    File testFile = new File(metadataDir, "unchanged.xlsx");
    testFile.createNewFile();

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = testFile.getAbsolutePath();
    record.timestamp = testFile.lastModified() + 1; // Set timestamp after file
    assertFalse(record.hasChanged());
  }

  @Test
  void testHasChangedRemoteFile() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "https://example.com/data.xlsx";
    record.timestamp = System.currentTimeMillis();
    // Remote files return true (conservative)
    assertTrue(record.hasChanged());
  }

  // --- hasChangedViaMetadata ---

  @Test
  void testHasChangedViaMetadataNullMetadata() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertTrue(record.hasChangedViaMetadata(null));
  }

  @Test
  void testHasChangedViaMetadataEtagMatch() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = "abc123";
    record.originalFile = "s3://bucket/file.csv";

    StorageProvider.FileMetadata metadata = mock(StorageProvider.FileMetadata.class);
    when(metadata.getEtag()).thenReturn("abc123");

    assertFalse(record.hasChangedViaMetadata(metadata));
  }

  @Test
  void testHasChangedViaMetadataEtagMismatch() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = "abc123";
    record.originalFile = "s3://bucket/file.csv";

    StorageProvider.FileMetadata metadata = mock(StorageProvider.FileMetadata.class);
    when(metadata.getEtag()).thenReturn("xyz789");

    assertTrue(record.hasChangedViaMetadata(metadata));
  }

  @Test
  void testHasChangedViaMetadataSizeChanged() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.contentLength = 1000L;
    record.originalFile = "s3://bucket/file.csv";

    StorageProvider.FileMetadata metadata = mock(StorageProvider.FileMetadata.class);
    when(metadata.getEtag()).thenReturn(null);
    when(metadata.getSize()).thenReturn(2000L);

    assertTrue(record.hasChangedViaMetadata(metadata));
  }

  @Test
  void testHasChangedViaMetadataTimestampWithinTolerance() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    long now = System.currentTimeMillis();
    record.timestamp = now;
    record.originalFile = "s3://bucket/file.csv";

    StorageProvider.FileMetadata metadata = mock(StorageProvider.FileMetadata.class);
    when(metadata.getEtag()).thenReturn(null);
    when(metadata.getSize()).thenReturn(0L);
    when(metadata.getLastModified()).thenReturn(now + 500); // Within 1 second tolerance

    assertFalse(record.hasChangedViaMetadata(metadata));
  }

  // --- updateMetadata ---

  @Test
  void testUpdateMetadata() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "test.csv";

    StorageProvider.FileMetadata metadata = mock(StorageProvider.FileMetadata.class);
    when(metadata.getEtag()).thenReturn("new-etag");
    when(metadata.getSize()).thenReturn(5000L);
    when(metadata.getContentType()).thenReturn("text/csv");
    when(metadata.getLastModified()).thenReturn(12345L);

    record.updateMetadata(metadata);

    assertEquals("new-etag", record.etag);
    assertEquals(Long.valueOf(5000L), record.contentLength);
    assertEquals("text/csv", record.contentType);
    assertEquals(12345L, record.timestamp);
  }

  @Test
  void testUpdateMetadataNull() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = "old-etag";
    record.updateMetadata(null);
    // Should not change
    assertEquals("old-etag", record.etag);
  }

  // --- PartitionBaseline ---

  @Test
  void testPartitionBaselineEmpty() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    assertTrue(baseline.isEmpty());
  }

  @Test
  void testPartitionBaselineWithNullFiles() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = null;
    assertTrue(baseline.isEmpty());
  }

  @Test
  void testPartitionBaselineNotEmpty() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<>();
    baseline.files.put("file1.parquet",
        new ConversionMetadata.FileBaseline(100L, "etag1", System.currentTimeMillis()));
    baseline.snapshotTimestamp = System.currentTimeMillis();
    assertFalse(baseline.isEmpty());
  }

  // --- FileBaseline ---

  @Test
  void testFileBaselineDefaultConstructor() {
    ConversionMetadata.FileBaseline fb = new ConversionMetadata.FileBaseline();
    assertNull(fb.size);
    assertNull(fb.etag);
    assertNull(fb.lastModified);
  }

  @Test
  void testFileBaselineConstructor() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, "etag-abc", 12345L);
    assertEquals(Long.valueOf(1024L), fb.size);
    assertEquals("etag-abc", fb.etag);
    assertEquals(Long.valueOf(12345L), fb.lastModified);
  }

  @Test
  void testFileBaselineHasChangedNull() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, "etag-abc", 12345L);
    assertTrue(fb.hasChanged(null));
  }

  @Test
  void testFileBaselineHasChangedEtagMatch() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, "etag-abc", 12345L);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(1024L, "etag-abc", 12345L);
    assertFalse(fb.hasChanged(current));
  }

  @Test
  void testFileBaselineHasChangedEtagMismatch() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, "etag-abc", 12345L);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(1024L, "etag-xyz", 12345L);
    assertTrue(fb.hasChanged(current));
  }

  @Test
  void testFileBaselineHasChangedSizeChanged() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, null, 12345L);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(2048L, null, 12345L);
    assertTrue(fb.hasChanged(current));
  }

  @Test
  void testFileBaselineHasChangedTimestampChanged() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, null, 10000L);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(1024L, null, 20000L);
    assertTrue(fb.hasChanged(current));
  }

  @Test
  void testFileBaselineHasChangedTimestampWithinTolerance() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, null, 10000L);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(1024L, null, 10500L);
    assertFalse(fb.hasChanged(current));
  }

  @Test
  void testFileBaselineNoChangeDetectable() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(null, null, null);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(null, null, null);
    assertFalse(fb.hasChanged(current));
  }

  // --- getConversionRecordByConvertedFile ---

  @Test
  void testGetConversionRecordByConvertedFile() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File original = new File(metadataDir, "lookup_original.xlsx");
    original.createNewFile();
    File converted = new File(metadataDir, "lookup_converted.json");
    converted.createNewFile();

    cm.recordConversion(original, converted, "EXCEL_TO_JSON");

    ConversionMetadata.ConversionRecord result =
        cm.getConversionRecordByConvertedFile(converted.getCanonicalPath());
    assertNotNull(result);
    assertEquals("EXCEL_TO_JSON", result.getConversionType());
  }

  @Test
  void testGetConversionRecordByConvertedFileNotFound() {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    ConversionMetadata.ConversionRecord result =
        cm.getConversionRecordByConvertedFile("/nonexistent/path.json");
    assertNull(result);
  }
}
