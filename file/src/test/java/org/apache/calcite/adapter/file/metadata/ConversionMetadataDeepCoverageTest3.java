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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
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

  @Test
  void testConversionRecordDefaultConstructor() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertNull(record.tableName);
    assertNull(record.sourceFile);
    assertNull(record.conversionType);
    assertNull(record.parquetCacheFile);
    assertNull(record.etag);
  }

  @Test
  void testConversionRecordThreeArgConstructor() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/path/original.xlsx", "/path/converted.json", "EXCEL_TO_JSON");
    assertEquals("/path/original.xlsx", record.originalFile);
    assertEquals("/path/converted.json", record.convertedFile);
    assertEquals("EXCEL_TO_JSON", record.conversionType);
    assertTrue(record.timestamp > 0);
  }

  @Test
  void testConversionRecordFourArgConstructor() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/path/original.xlsx", "/path/converted.json", "EXCEL_TO_JSON", "/path/cache.parquet");
    assertEquals("/path/cache.parquet", record.parquetCacheFile);
  }

  @Test
  void testConversionRecordSevenArgConstructor() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/path/original.xlsx", "/path/converted.json", "EXCEL_TO_JSON",
        "/path/cache.parquet", "etag123", 1000L, "application/json");
    assertEquals("etag123", record.etag);
    assertEquals(Long.valueOf(1000L), record.contentLength);
    assertEquals("application/json", record.contentType);
  }

  @Test
  void testConversionRecordComprehensiveConstructor() {
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("key", "value");

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "my_table", "ParquetTranslatableTable", "/path/source.csv", "csv",
        "/path/original.xlsx", "/path/converted.json", "EXCEL_TO_JSON",
        "/path/cache.parquet", "s3://bucket/**/*.parquet",
        true, "PT5M", "etag456", 2000L, "text/csv", tableConfig);

    assertEquals("my_table", record.tableName);
    assertEquals("ParquetTranslatableTable", record.tableType);
    assertEquals("/path/source.csv", record.sourceFile);
    assertEquals("csv", record.sourceType);
    assertEquals("s3://bucket/**/*.parquet", record.viewScanPattern);
    assertTrue(record.refreshEnabled);
    assertEquals("PT5M", record.refreshInterval);
    assertEquals(tableConfig, record.tableConfig);
  }

  // ====================================================================
  // Tests for ConversionRecord getter methods
  // ====================================================================

  @Test
  void testConversionRecordGetters() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/path/original.xlsx", "/path/converted.json", "EXCEL_TO_JSON", "/path/cache.parquet");

    assertEquals("/path/original.xlsx", record.getOriginalPath());
    assertEquals("EXCEL_TO_JSON", record.getConversionType());
    assertNull(record.getTableName());
    assertNull(record.getSourceFile());
    assertEquals("/path/cache.parquet", record.getParquetCacheFile());
    assertEquals("/path/converted.json", record.getConvertedFile());
  }

  // ====================================================================
  // Tests for isIcebergFormat
  // ====================================================================

  @Test
  void testIsIcebergFormatNullConfig() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = null;
    assertFalse(record.isIcebergFormat());
  }

  @Test
  void testIsIcebergFormatNoMaterialize() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<String, Object>();
    assertFalse(record.isIcebergFormat());
  }

  @Test
  void testIsIcebergFormatNotMap() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<String, Object>();
    record.tableConfig.put("materialize", "not_a_map");
    assertFalse(record.isIcebergFormat());
  }

  @Test
  void testIsIcebergFormatIceberg() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<String, Object>();
    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", "iceberg");
    record.tableConfig.put("materialize", materialize);
    assertTrue(record.isIcebergFormat());
  }

  @Test
  void testIsIcebergFormatParquet() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<String, Object>();
    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", "parquet");
    record.tableConfig.put("materialize", materialize);
    assertFalse(record.isIcebergFormat());
  }

  // ====================================================================
  // Tests for hasChanged (local file detection)
  // ====================================================================

  @Test
  void testHasChangedLocalFile() throws Exception {
    File testFile = tempDir.resolve("test.txt").toFile();
    Files.write(testFile.toPath(), "test data".getBytes());

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        testFile.getCanonicalPath(), "/converted.json", "DIRECT");
    // The timestamp is now, so file hasn't changed
    assertFalse(record.hasChanged());
  }

  @Test
  void testHasChangedLocalFileDoesNotExist() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/nonexistent/file.txt", "/converted.json", "DIRECT");
    assertFalse(record.hasChanged());
  }

  @Test
  void testHasChangedRemoteFile() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "https://example.com/data.csv", "/converted.json", "HTTP");
    // Remote files always return true (conservative)
    assertTrue(record.hasChanged());
  }

  @Test
  void testHasChangedS3File() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "s3://bucket/data.csv", "/converted.json", "S3");
    assertTrue(record.hasChanged());
  }

  // ====================================================================
  // Tests for hasChangedViaMetadata
  // ====================================================================

  @Test
  void testHasChangedViaMetadataNullCurrent() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/path/file.txt", "/path/out.json", "DIRECT");
    assertTrue(record.hasChangedViaMetadata(null));
  }

  @Test
  void testHasChangedViaMetadataEtagMatch() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/path/file.txt", "/path/out.json", "DIRECT",
        null, "etag123", null, null);

    StorageProvider.FileMetadata current = mock(StorageProvider.FileMetadata.class);
    when(current.getEtag()).thenReturn("etag123");
    assertFalse(record.hasChangedViaMetadata(current));
  }

  @Test
  void testHasChangedViaMetadataEtagMismatch() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/path/file.txt", "/path/out.json", "DIRECT",
        null, "etag123", null, null);

    StorageProvider.FileMetadata current = mock(StorageProvider.FileMetadata.class);
    when(current.getEtag()).thenReturn("etag456");
    assertTrue(record.hasChangedViaMetadata(current));
  }

  @Test
  void testHasChangedViaMetadataSizeChanged() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/path/file.txt", "/path/out.json", "DIRECT",
        null, null, 1000L, null);

    StorageProvider.FileMetadata current = mock(StorageProvider.FileMetadata.class);
    when(current.getEtag()).thenReturn(null);
    when(current.getSize()).thenReturn(2000L);
    assertTrue(record.hasChangedViaMetadata(current));
  }

  @Test
  void testHasChangedViaMetadataTimestampComparison() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/path/file.txt", "/path/out.json", "DIRECT");
    long now = System.currentTimeMillis();
    record.timestamp = now;
    record.contentLength = null;

    StorageProvider.FileMetadata current = mock(StorageProvider.FileMetadata.class);
    when(current.getEtag()).thenReturn(null);
    when(current.getSize()).thenReturn(0L);
    // Same timestamp within 1 second tolerance
    when(current.getLastModified()).thenReturn(now + 500);
    assertFalse(record.hasChangedViaMetadata(current));

    // Timestamp beyond tolerance
    when(current.getLastModified()).thenReturn(now + 2000);
    assertTrue(record.hasChangedViaMetadata(current));
  }

  // ====================================================================
  // Tests for updateMetadata
  // ====================================================================

  @Test
  void testUpdateMetadata() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/path/file.txt", "/path/out.json", "DIRECT");

    StorageProvider.FileMetadata meta = mock(StorageProvider.FileMetadata.class);
    when(meta.getEtag()).thenReturn("newEtag");
    when(meta.getSize()).thenReturn(5000L);
    when(meta.getContentType()).thenReturn("text/plain");
    when(meta.getLastModified()).thenReturn(123456789L);

    record.updateMetadata(meta);
    assertEquals("newEtag", record.etag);
    assertEquals(Long.valueOf(5000L), record.contentLength);
    assertEquals("text/plain", record.contentType);
    assertEquals(123456789L, record.timestamp);
  }

  @Test
  void testUpdateMetadataNull() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/path/file.txt", "/path/out.json", "DIRECT");
    String origEtag = record.etag;
    record.updateMetadata(null);
    // Should not change
    assertEquals(origEtag, record.etag);
  }

  // ====================================================================
  // Tests for PartitionBaseline
  // ====================================================================

  @Test
  void testPartitionBaselineEmpty() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    assertTrue(baseline.isEmpty());

    baseline.files = new HashMap<String, ConversionMetadata.FileBaseline>();
    assertTrue(baseline.isEmpty());
  }

  @Test
  void testPartitionBaselineNotEmpty() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<String, ConversionMetadata.FileBaseline>();
    baseline.files.put("file1.parquet", new ConversionMetadata.FileBaseline(1000L, "etag", 123L));
    baseline.snapshotTimestamp = System.currentTimeMillis();
    assertFalse(baseline.isEmpty());
  }

  // ====================================================================
  // Tests for FileBaseline
  // ====================================================================

  @Test
  void testFileBaselineDefaultConstructor() {
    ConversionMetadata.FileBaseline baseline = new ConversionMetadata.FileBaseline();
    assertNull(baseline.size);
    assertNull(baseline.etag);
    assertNull(baseline.lastModified);
  }

  @Test
  void testFileBaselineParameterizedConstructor() {
    ConversionMetadata.FileBaseline baseline = new ConversionMetadata.FileBaseline(1000L, "etag1", 123456L);
    assertEquals(Long.valueOf(1000L), baseline.size);
    assertEquals("etag1", baseline.etag);
    assertEquals(Long.valueOf(123456L), baseline.lastModified);
  }

  @Test
  void testFileBaselineHasChangedNull() {
    ConversionMetadata.FileBaseline baseline = new ConversionMetadata.FileBaseline(1000L, "etag1", 123L);
    assertTrue(baseline.hasChanged(null));
  }

  @Test
  void testFileBaselineHasChangedEtagMatch() {
    ConversionMetadata.FileBaseline baseline = new ConversionMetadata.FileBaseline(1000L, "etag1", 123L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(1000L, "etag1", 123L);
    assertFalse(baseline.hasChanged(current));
  }

  @Test
  void testFileBaselineHasChangedEtagMismatch() {
    ConversionMetadata.FileBaseline baseline = new ConversionMetadata.FileBaseline(1000L, "etag1", 123L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(1000L, "etag2", 123L);
    assertTrue(baseline.hasChanged(current));
  }

  @Test
  void testFileBaselineHasChangedSizeMismatch() {
    ConversionMetadata.FileBaseline baseline = new ConversionMetadata.FileBaseline(1000L, null, 123L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(2000L, null, 123L);
    assertTrue(baseline.hasChanged(current));
  }

  @Test
  void testFileBaselineHasChangedTimestamp() {
    ConversionMetadata.FileBaseline baseline = new ConversionMetadata.FileBaseline(null, null, 1000L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(null, null, 1500L);
    assertFalse(baseline.hasChanged(current)); // Within 1s tolerance

    ConversionMetadata.FileBaseline changed = new ConversionMetadata.FileBaseline(null, null, 3000L);
    assertTrue(baseline.hasChanged(changed)); // Beyond tolerance
  }

  @Test
  void testFileBaselineHasChangedNoDetectable() {
    ConversionMetadata.FileBaseline baseline = new ConversionMetadata.FileBaseline(null, null, null);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(null, null, null);
    assertFalse(baseline.hasChanged(current));
  }

  // ====================================================================
  // Tests for ConversionMetadata constructors
  // ====================================================================

  @Test
  void testConstructorWithDirectory() {
    ConversionMetadata cm = new ConversionMetadata(tempDir.toFile());
    assertNotNull(cm);
    assertTrue(cm.getAllConversions().isEmpty());
  }

  @Test
  void testConstructorWithStringLocalPath() {
    ConversionMetadata cm = new ConversionMetadata(tempDir.toString());
    assertNotNull(cm);
  }

  @Test
  void testConstructorWithS3Path() {
    ConversionMetadata cm = new ConversionMetadata("s3://bucket/path");
    assertNotNull(cm);
  }

  // ====================================================================
  // Tests for recordConversion methods
  // ====================================================================

  @Test
  void testRecordConversionBasic() throws Exception {
    File orig = tempDir.resolve("original.xlsx").toFile();
    File conv = tempDir.resolve("converted.json").toFile();
    Files.write(orig.toPath(), "data".getBytes());
    Files.write(conv.toPath(), "{}".getBytes());

    metadata.recordConversion(orig, conv, "EXCEL_TO_JSON");
    assertEquals(1, metadata.getAllConversions().size());
  }

  @Test
  void testRecordConversionWithCache() throws Exception {
    File orig = tempDir.resolve("original2.xlsx").toFile();
    File conv = tempDir.resolve("converted2.json").toFile();
    File cache = tempDir.resolve("cache2.parquet").toFile();
    Files.write(orig.toPath(), "data".getBytes());
    Files.write(conv.toPath(), "{}".getBytes());
    Files.write(cache.toPath(), "binary".getBytes());

    metadata.recordConversion(orig, conv, "EXCEL_TO_JSON", cache);
    assertEquals(1, metadata.getAllConversions().size());
  }

  @Test
  void testRecordConversionWithRecord() throws Exception {
    File conv = tempDir.resolve("converted3.json").toFile();
    Files.write(conv.toPath(), "{}".getBytes());

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord(
        "/path/original.xlsx", conv.getCanonicalPath(), "EXCEL_TO_JSON");
    metadata.recordConversion(conv, record);
    assertEquals(1, metadata.getAllConversions().size());
  }

  // ====================================================================
  // Tests for hints
  // ====================================================================

  @Test
  void testHints() {
    metadata.setHint("cik", "0001234567");
    assertEquals("0001234567", metadata.getHint("cik"));
    assertNull(metadata.getHint("nonexistent"));

    // Null value should not be stored
    metadata.setHint("nullKey", null);
    assertNull(metadata.getHint("nullKey"));
  }

  // ====================================================================
  // Tests for save/load metadata persistence
  // ====================================================================

  @Test
  void testMetadataPersistence() throws Exception {
    File orig = tempDir.resolve("persist_orig.xlsx").toFile();
    File conv = tempDir.resolve("persist_conv.json").toFile();
    Files.write(orig.toPath(), "data".getBytes());
    Files.write(conv.toPath(), "{}".getBytes());

    metadata.recordConversion(orig, conv, "EXCEL_TO_JSON");
    assertEquals(1, metadata.getAllConversions().size());

    // Create new metadata instance to load from disk
    ConversionMetadata loaded = new ConversionMetadata(tempDir.toFile());
    assertEquals(1, loaded.getAllConversions().size());
  }

  // ====================================================================
  // Tests for isLocalFile (via hasChanged)
  // ====================================================================

  @Test
  void testIsLocalFileVariants() {
    // Test via hasChanged which calls isLocalFile
    ConversionMetadata.ConversionRecord localRecord = new ConversionMetadata.ConversionRecord(
        "/local/file.txt", "/out.json", "DIRECT");
    // Local file check (won't throw)
    localRecord.hasChanged();

    ConversionMetadata.ConversionRecord httpRecord = new ConversionMetadata.ConversionRecord(
        "http://example.com/file.txt", "/out.json", "HTTP");
    assertTrue(httpRecord.hasChanged());

    ConversionMetadata.ConversionRecord httpsRecord = new ConversionMetadata.ConversionRecord(
        "https://example.com/file.txt", "/out.json", "HTTP");
    assertTrue(httpsRecord.hasChanged());

    ConversionMetadata.ConversionRecord ftpRecord = new ConversionMetadata.ConversionRecord(
        "ftp://server/file.txt", "/out.json", "FTP");
    assertTrue(ftpRecord.hasChanged());

    ConversionMetadata.ConversionRecord sftpRecord = new ConversionMetadata.ConversionRecord(
        "sftp://server/file.txt", "/out.json", "SFTP");
    assertTrue(sftpRecord.hasChanged());
  }

  // ====================================================================
  // Tests for detectTypeFromTestFile (static)
  // ====================================================================

  @Test
  void testDetectConvertibleTypes() throws Exception {
    java.lang.reflect.Method method = ConversionMetadata.class.getDeclaredMethod(
        "detectConvertibleType", String.class);
    method.setAccessible(true);

    assertEquals("excel", method.invoke(null, "test.xlsx"));
    assertEquals("excel", method.invoke(null, "test.xls"));
    assertEquals("html", method.invoke(null, "test.html"));
    assertEquals("html", method.invoke(null, "test.htm"));
    assertEquals("xml", method.invoke(null, "test.xml"));
    assertEquals("markdown", method.invoke(null, "test.md"));
    assertEquals("docx", method.invoke(null, "test.docx"));
    assertEquals("pptx", method.invoke(null, "test.pptx"));
    assertEquals("unknown", method.invoke(null, "test.unknown"));
  }

  @Test
  void testDetectDirectTypes() throws Exception {
    java.lang.reflect.Method method = ConversionMetadata.class.getDeclaredMethod(
        "detectDirectType", String.class);
    method.setAccessible(true);

    assertEquals("csv", method.invoke(null, "test.csv"));
    assertEquals("tsv", method.invoke(null, "test.tsv"));
    assertEquals("json", method.invoke(null, "test.json"));
    assertEquals("parquet", method.invoke(null, "test.parquet"));
    assertEquals("yaml", method.invoke(null, "test.yaml"));
    assertEquals("yaml", method.invoke(null, "test.yml"));
    assertEquals("arrow", method.invoke(null, "test.arrow"));
    assertEquals("unknown", method.invoke(null, "test.unknown"));
  }

  // ====================================================================
  // Tests for detectSourceType
  // ====================================================================

  @Test
  void testDetectSourceType() throws Exception {
    java.lang.reflect.Method method = ConversionMetadata.class.getDeclaredMethod(
        "detectSourceType", String.class);
    method.setAccessible(true);

    // Method takes a path and returns a type
    assertNotNull(method.invoke(metadata, "/path/to/file.csv"));
    assertNotNull(method.invoke(metadata, "/path/to/file.parquet"));
  }
}
