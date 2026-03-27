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
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deep coverage tests for {@link ConversionMetadata}.
 * Targets uncovered methods: ConversionRecord constructors and methods,
 * PartitionBaseline, FileBaseline, recordConversion, findOriginalSource,
 * findDerivedFiles, updateCachedFile, buildComprehensiveMapping,
 * updateMaterializationInfo, updateExistingRecord, recordConversionWithTableName,
 * saveMetadata, loadMetadata, clear, reload, and static file type detection methods.
 */
@Tag("unit")
class ConversionMetadataDeepCoverageTest2 {

  @TempDir
  Path tempDir;

  private ConversionMetadata metadata;

  @BeforeEach
  void setup() {
    metadata = new ConversionMetadata(tempDir.toFile());
  }

  // ======= Constructor tests =======

  @Test
  void testConstructorWithDirectory() {
    ConversionMetadata cm = new ConversionMetadata(tempDir.toFile());
    assertNotNull(cm);
  }

  @Test
  void testConstructorWithStringPath() {
    ConversionMetadata cm = new ConversionMetadata(tempDir.toString());
    assertNotNull(cm);
  }

  @Test
  void testConstructorWithS3Path() {
    // S3 path should not try to load metadata from disk
    ConversionMetadata cm = new ConversionMetadata("s3://my-bucket/data");
    assertNotNull(cm);
  }

  // ======= Hint methods =======

  @Test
  void testSetAndGetHint() {
    metadata.setHint("cik", "0001234");
    assertEquals("0001234", metadata.getHint("cik"));
    assertNull(metadata.getHint("nonexistent"));
  }

  @Test
  void testSetHintNullValue() {
    metadata.setHint("key", null);
    assertNull(metadata.getHint("key"));
  }

  // ======= ConversionRecord constructors =======

  @Test
  void testConversionRecordDefaultConstructor() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertNull(record.tableName);
    assertNull(record.sourceFile);
  }

  @Test
  void testConversionRecordThreeArgConstructor() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/src/file.xlsx", "/dst/file.json", "EXCEL_TO_JSON");
    assertEquals("/src/file.xlsx", record.originalFile);
    assertEquals("/dst/file.json", record.convertedFile);
    assertEquals("EXCEL_TO_JSON", record.conversionType);
    assertTrue(record.timestamp > 0);
  }

  @Test
  void testConversionRecordFourArgConstructor() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/src.xlsx", "/dst.json", "EXCEL_TO_JSON", "/cache.parquet");
    assertEquals("/cache.parquet", record.parquetCacheFile);
  }

  @Test
  void testConversionRecordSevenArgConstructor() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/src.html", "/dst.json", "HTML_TO_JSON", "/cache.parquet",
            "etag123", 1024L, "text/html");
    assertEquals("etag123", record.etag);
    assertEquals(Long.valueOf(1024L), record.contentLength);
    assertEquals("text/html", record.contentType);
  }

  @Test
  void testConversionRecordComprehensiveConstructor() {
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("name", "test_table");

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord(
            "test_table", "ParquetTranslatableTable", "/data/test.parquet",
            "parquet", "/src.csv", "/converted.json", "CSV_TO_JSON",
            "/cache.parquet", "s3://bucket/**/*.parquet", true, "5m",
            "etag-abc", 2048L, "application/json", tableConfig);

    assertEquals("test_table", record.tableName);
    assertEquals("ParquetTranslatableTable", record.tableType);
    assertEquals("/data/test.parquet", record.sourceFile);
    assertEquals("parquet", record.sourceType);
    assertEquals("/src.csv", record.originalFile);
    assertEquals("/converted.json", record.convertedFile);
    assertEquals("CSV_TO_JSON", record.conversionType);
    assertEquals("/cache.parquet", record.parquetCacheFile);
    assertEquals("s3://bucket/**/*.parquet", record.viewScanPattern);
    assertTrue(record.refreshEnabled);
    assertEquals("5m", record.refreshInterval);
    assertEquals("etag-abc", record.etag);
    assertEquals(Long.valueOf(2048L), record.contentLength);
    assertEquals("application/json", record.contentType);
    assertEquals(tableConfig, record.tableConfig);
  }

  // ======= ConversionRecord getter methods =======

  @Test
  void testConversionRecordGetters() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/orig.xlsx", "/conv.json", "EXCEL_TO_JSON", "/cache.parquet");
    record.tableName = "my_table";
    record.sourceFile = "/data/source.csv";

    assertEquals("/orig.xlsx", record.getOriginalPath());
    assertEquals("EXCEL_TO_JSON", record.getConversionType());
    assertEquals("my_table", record.getTableName());
    assertEquals("/data/source.csv", record.getSourceFile());
    assertEquals("/cache.parquet", record.getParquetCacheFile());
    assertEquals("/conv.json", record.getConvertedFile());
  }

  // ======= isIcebergFormat =======

  @Test
  void testIsIcebergFormatTrue() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", "iceberg");
    tableConfig.put("materialize", materialize);
    record.tableConfig = tableConfig;

    assertTrue(record.isIcebergFormat());
  }

  @Test
  void testIsIcebergFormatFalse() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = null;
    assertFalse(record.isIcebergFormat());

    record.tableConfig = new HashMap<String, Object>();
    assertFalse(record.isIcebergFormat());

    Map<String, Object> tableConfig = new HashMap<String, Object>();
    tableConfig.put("materialize", "not_a_map");
    record.tableConfig = tableConfig;
    assertFalse(record.isIcebergFormat());

    Map<String, Object> matConfig = new HashMap<String, Object>();
    matConfig.put("format", "parquet");
    tableConfig.put("materialize", matConfig);
    record.tableConfig = tableConfig;
    assertFalse(record.isIcebergFormat());
  }

  // ======= hasChanged =======

  @Test
  void testHasChangedLocalFile() throws IOException {
    File sourceFile = tempDir.resolve("source.csv").toFile();
    try (FileWriter fw = new FileWriter(sourceFile)) {
      fw.write("test");
    }

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = sourceFile.getAbsolutePath();
    record.timestamp = System.currentTimeMillis() + 10000; // future timestamp

    // File exists but has older timestamp - should not be "changed"
    assertFalse(record.hasChanged());
  }

  @Test
  void testHasChangedLocalFileNonExistent() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = tempDir.resolve("nonexistent.csv").toString();

    // File doesn't exist - hasChanged returns false (no change possible)
    assertFalse(record.hasChanged());
  }

  @Test
  void testHasChangedRemoteFile() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "https://example.com/data.csv";

    // Remote files return true (conservative)
    assertTrue(record.hasChanged());
  }

  // ======= hasChangedViaMetadata =======

  @Test
  void testHasChangedViaMetadataNull() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertTrue(record.hasChangedViaMetadata(null));
  }

  @Test
  void testHasChangedViaMetadataEtagMatch() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = "etag123";

    StorageProvider.FileMetadata currentMetadata = mock(StorageProvider.FileMetadata.class);
    when(currentMetadata.getEtag()).thenReturn("etag123");

    assertFalse(record.hasChangedViaMetadata(currentMetadata));
  }

  @Test
  void testHasChangedViaMetadataEtagChanged() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = "etag123";

    StorageProvider.FileMetadata currentMetadata = mock(StorageProvider.FileMetadata.class);
    when(currentMetadata.getEtag()).thenReturn("etag456");

    assertTrue(record.hasChangedViaMetadata(currentMetadata));
  }

  @Test
  void testHasChangedViaMetadataSizeChanged() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.contentLength = 1024L;

    StorageProvider.FileMetadata currentMetadata = mock(StorageProvider.FileMetadata.class);
    when(currentMetadata.getEtag()).thenReturn(null);
    when(currentMetadata.getSize()).thenReturn(2048L);

    assertTrue(record.hasChangedViaMetadata(currentMetadata));
  }

  @Test
  void testHasChangedViaMetadataTimestampChanged() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.timestamp = 1000000L;

    StorageProvider.FileMetadata currentMetadata = mock(StorageProvider.FileMetadata.class);
    when(currentMetadata.getEtag()).thenReturn(null);
    when(currentMetadata.getSize()).thenReturn(0L);
    when(currentMetadata.getLastModified()).thenReturn(1005000L); // 5 seconds later

    assertTrue(record.hasChangedViaMetadata(currentMetadata));
  }

  @Test
  void testHasChangedViaMetadataTimestampWithinTolerance() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.timestamp = 1000000L;

    StorageProvider.FileMetadata currentMetadata = mock(StorageProvider.FileMetadata.class);
    when(currentMetadata.getEtag()).thenReturn(null);
    when(currentMetadata.getSize()).thenReturn(0L);
    when(currentMetadata.getLastModified()).thenReturn(1000500L); // 500ms later

    assertFalse(record.hasChangedViaMetadata(currentMetadata));
  }

  // ======= updateMetadata =======

  @Test
  void testUpdateMetadata() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "/test.csv";

    StorageProvider.FileMetadata fileMetadata = mock(StorageProvider.FileMetadata.class);
    when(fileMetadata.getEtag()).thenReturn("new-etag");
    when(fileMetadata.getSize()).thenReturn(4096L);
    when(fileMetadata.getContentType()).thenReturn("text/csv");
    when(fileMetadata.getLastModified()).thenReturn(2000000L);

    record.updateMetadata(fileMetadata);
    assertEquals("new-etag", record.etag);
    assertEquals(Long.valueOf(4096L), record.contentLength);
    assertEquals("text/csv", record.contentType);
    assertEquals(2000000L, record.timestamp);
  }

  @Test
  void testUpdateMetadataNull() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = "old-etag";

    record.updateMetadata(null);
    assertEquals("old-etag", record.etag); // Should not change
  }

  // ======= isLocalFile (via hasChanged) =======

  @Test
  void testIsLocalFileVariousProtocols() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();

    // HTTP
    record.originalFile = "http://example.com/file.csv";
    assertTrue(record.hasChanged()); // Remote returns true

    // HTTPS
    record.originalFile = "https://example.com/file.csv";
    assertTrue(record.hasChanged());

    // S3
    record.originalFile = "s3://bucket/file.csv";
    assertTrue(record.hasChanged());

    // FTP
    record.originalFile = "ftp://server/file.csv";
    assertTrue(record.hasChanged());

    // SFTP
    record.originalFile = "sftp://server/file.csv";
    assertTrue(record.hasChanged());
  }

  // ======= PartitionBaseline =======

  @Test
  void testPartitionBaselineEmpty() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    assertTrue(baseline.isEmpty());

    baseline.files = new HashMap<String, ConversionMetadata.FileBaseline>();
    assertTrue(baseline.isEmpty());

    baseline.files.put("file1", new ConversionMetadata.FileBaseline(100L, "etag1", 1000L));
    assertFalse(baseline.isEmpty());
  }

  // ======= FileBaseline =======

  @Test
  void testFileBaselineDefaultConstructor() {
    ConversionMetadata.FileBaseline fb = new ConversionMetadata.FileBaseline();
    assertNull(fb.size);
    assertNull(fb.etag);
    assertNull(fb.lastModified);
  }

  @Test
  void testFileBaselineParameterizedConstructor() {
    ConversionMetadata.FileBaseline fb = new ConversionMetadata.FileBaseline(100L, "etag1", 1000L);
    assertEquals(Long.valueOf(100L), fb.size);
    assertEquals("etag1", fb.etag);
    assertEquals(Long.valueOf(1000L), fb.lastModified);
  }

  @Test
  void testFileBaselineHasChangedNull() {
    ConversionMetadata.FileBaseline fb = new ConversionMetadata.FileBaseline(100L, "etag1", 1000L);
    assertTrue(fb.hasChanged(null));
  }

  @Test
  void testFileBaselineHasChangedEtagMatch() {
    ConversionMetadata.FileBaseline fb = new ConversionMetadata.FileBaseline(100L, "etag1", 1000L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(100L, "etag1", 1000L);
    assertFalse(fb.hasChanged(current));
  }

  @Test
  void testFileBaselineHasChangedEtagMismatch() {
    ConversionMetadata.FileBaseline fb = new ConversionMetadata.FileBaseline(100L, "etag1", 1000L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(100L, "etag2", 1000L);
    assertTrue(fb.hasChanged(current));
  }

  @Test
  void testFileBaselineHasChangedSizeChanged() {
    ConversionMetadata.FileBaseline fb = new ConversionMetadata.FileBaseline(100L, null, 1000L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(200L, null, 1000L);
    assertTrue(fb.hasChanged(current));
  }

  @Test
  void testFileBaselineHasChangedTimestampChanged() {
    ConversionMetadata.FileBaseline fb = new ConversionMetadata.FileBaseline(100L, null, 1000L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(100L, null, 3000L);
    assertTrue(fb.hasChanged(current));
  }

  @Test
  void testFileBaselineHasChangedTimestampWithinTolerance() {
    ConversionMetadata.FileBaseline fb = new ConversionMetadata.FileBaseline(100L, null, 1000L);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(100L, null, 1500L);
    assertFalse(fb.hasChanged(current));
  }

  @Test
  void testFileBaselineHasChangedNoChange() {
    ConversionMetadata.FileBaseline fb = new ConversionMetadata.FileBaseline(null, null, null);
    ConversionMetadata.FileBaseline current = new ConversionMetadata.FileBaseline(null, null, null);
    assertFalse(fb.hasChanged(current));
  }

  // ======= recordConversion (File, File, String) =======

  @Test
  void testRecordConversion() throws IOException {
    File original = createTempFile("original.xlsx");
    File converted = createTempFile("converted.json");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertEquals("EXCEL_TO_JSON", record.conversionType);
  }

  // ======= recordConversion with parquet cache =======

  @Test
  void testRecordConversionWithParquetCache() throws IOException {
    File original = createTempFile("src.html");
    File converted = createTempFile("result.json");
    File parquetCache = createTempFile("cache.parquet");

    metadata.recordConversion(original, converted, "HTML_TO_JSON", parquetCache);

    ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertEquals(parquetCache.getCanonicalPath(), record.parquetCacheFile);
  }

  @Test
  void testRecordConversionWithNullParquetCache() throws IOException {
    File original = createTempFile("src2.html");
    File converted = createTempFile("result2.json");

    metadata.recordConversion(original, converted, "HTML_TO_JSON", null);

    ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertNull(record.parquetCacheFile);
  }

  // ======= recordConversion with pre-built record =======

  @Test
  void testRecordConversionPreBuiltRecord() throws IOException {
    File converted = createTempFile("prebuilt.json");

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/src", "/dst", "TEST", null, "etag", 100L, "text/plain");

    metadata.recordConversion(converted, record);

    ConversionMetadata.ConversionRecord retrieved = metadata.getConversionRecord(converted);
    assertNotNull(retrieved);
    assertEquals("etag", retrieved.etag);
  }

  // ======= findOriginalSource =======

  @Test
  void testFindOriginalSource() throws IOException {
    File original = createTempFile("findme.xlsx");
    File converted = createTempFile("found.json");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    File found = metadata.findOriginalSource(converted);
    assertNotNull(found);
    assertEquals(original.getCanonicalPath(), found.getCanonicalPath());
  }

  @Test
  void testFindOriginalSourceNotFound() throws IOException {
    File converted = createTempFile("orphan.json");
    assertNull(metadata.findOriginalSource(converted));
  }

  @Test
  void testFindOriginalSourceDeletedOriginal() throws IOException {
    File original = createTempFile("deleted.xlsx");
    File converted = createTempFile("remaining.json");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    // Delete the original file
    assertTrue(original.delete());

    // Should return null since original no longer exists
    assertNull(metadata.findOriginalSource(converted));
  }

  // ======= findDerivedFiles =======

  @Test
  void testFindDerivedFiles() throws IOException {
    File source = createTempFile("source.xlsx");
    File derived1 = createTempFile("derived1.json");
    File derived2 = createTempFile("derived2.json");

    metadata.recordConversion(source, derived1, "EXCEL_TO_JSON");
    metadata.recordConversion(source, derived2, "EXCEL_TO_JSON");

    List<File> derivedFiles = metadata.findDerivedFiles(source);
    assertEquals(2, derivedFiles.size());
  }

  @Test
  void testFindDerivedFilesEmpty() throws IOException {
    File source = createTempFile("nodeps.csv");
    List<File> derivedFiles = metadata.findDerivedFiles(source);
    assertTrue(derivedFiles.isEmpty());
  }

  // ======= updateCachedFile =======

  @Test
  void testUpdateCachedFile() throws IOException {
    File original = createTempFile("updatecache_orig.xlsx");
    File converted = createTempFile("updatecache_conv.json");
    File parquet = createTempFile("updatecache.parquet");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");
    metadata.updateCachedFile(converted, parquet);

    ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertEquals(parquet.getCanonicalPath(), record.parquetCacheFile);
  }

  @Test
  void testUpdateCachedFileNotFound() throws IOException {
    File converted = createTempFile("notexist.json");
    File parquet = createTempFile("notexist.parquet");

    // Should not throw - just logs
    metadata.updateCachedFile(converted, parquet);
  }

  @Test
  void testUpdateCachedFileNull() throws IOException {
    File original = createTempFile("nullcache_orig.xlsx");
    File converted = createTempFile("nullcache_conv.json");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");
    metadata.updateCachedFile(converted, null);

    ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertNull(record.parquetCacheFile);
  }

  // ======= getAllConversions =======

  @Test
  void testGetAllConversions() throws IOException {
    File orig = createTempFile("all_orig.csv");
    File conv = createTempFile("all_conv.json");

    metadata.recordConversion(orig, conv, "CSV_TO_JSON");
    Map<String, ConversionMetadata.ConversionRecord> all = metadata.getAllConversions();
    assertFalse(all.isEmpty());
  }

  // ======= hasTableMetadata =======

  @Test
  void testHasTableMetadata() {
    assertFalse(metadata.hasTableMetadata("nonexistent"));

    metadata.putConversionRecord("test_table", new ConversionMetadata.ConversionRecord());
    assertTrue(metadata.hasTableMetadata("test_table"));
  }

  // ======= putConversionRecord =======

  @Test
  void testPutConversionRecord() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "put_table";
    record.sourceFile = "/data/test.parquet";

    metadata.putConversionRecord("put_table", record);
    assertTrue(metadata.hasTableMetadata("put_table"));
  }

  // ======= updateMaterializationInfo =======

  @Test
  void testUpdateMaterializationInfoExistingRecord() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "mat_table";
    record.conversionType = "DIRECT";
    metadata.putConversionRecord("mat_table", record);

    metadata.updateMaterializationInfo("mat_table", "s3://bucket/table", "ICEBERG_PARQUET", 1000L);

    ConversionMetadata.ConversionRecord updated = metadata.getAllConversions().get("mat_table");
    assertEquals("ICEBERG_PARQUET", updated.conversionType);
    assertEquals("s3://bucket/table", updated.sourceFile);
    assertEquals("IcebergTable", updated.tableType);
    assertEquals(Long.valueOf(1000L), updated.rowCount);
    // For ICEBERG_PARQUET, these should be cleared
    assertNull(updated.viewScanPattern);
    assertNull(updated.convertedFile);
    assertNull(updated.parquetCacheFile);
  }

  @Test
  void testUpdateMaterializationInfoNewRecord() {
    metadata.updateMaterializationInfo("new_mat_table", "s3://bucket/new", "PARQUET", 500L);

    ConversionMetadata.ConversionRecord record = metadata.getAllConversions().get("new_mat_table");
    assertNotNull(record);
    assertEquals("PARQUET", record.conversionType);
    assertEquals("ParquetTable", record.tableType);
    assertEquals(Long.valueOf(500L), record.rowCount);
  }

  @Test
  void testUpdateMaterializationInfoNullRowCount() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "keep_rc";
    record.rowCount = 999L;
    metadata.putConversionRecord("keep_rc", record);

    metadata.updateMaterializationInfo("keep_rc", "s3://path", "PARQUET", null);

    ConversionMetadata.ConversionRecord updated = metadata.getAllConversions().get("keep_rc");
    // rowCount should be preserved since null was passed
    assertEquals(Long.valueOf(999L), updated.rowCount);
  }

  @Test
  void testUpdateMaterializationInfoTwoArgs() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "two_arg";
    metadata.putConversionRecord("two_arg", record);

    metadata.updateMaterializationInfo("two_arg", "s3://path", "PARQUET");

    ConversionMetadata.ConversionRecord updated = metadata.getAllConversions().get("two_arg");
    assertEquals("PARQUET", updated.conversionType);
  }

  // ======= updateRecordWithParquetFile =======

  @Test
  void testUpdateRecordWithParquetFile() throws IOException {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "parq_update";
    metadata.putConversionRecord("parq_update", record);

    File parquetFile = createTempFile("updated.parquet");
    metadata.updateRecordWithParquetFile("parq_update", parquetFile);

    ConversionMetadata.ConversionRecord updated = metadata.getAllConversions().get("parq_update");
    assertEquals(parquetFile.getCanonicalPath(), updated.parquetCacheFile);
    assertEquals("ParquetTranslatableTable", updated.tableType);
  }

  @Test
  void testUpdateRecordWithParquetFileNotFound() throws IOException {
    File parquetFile = createTempFile("orphan.parquet");
    // Should not throw - just logs warning
    metadata.updateRecordWithParquetFile("nonexistent_table", parquetFile);
  }

  // ======= recordConversionWithTableName =======

  @Test
  void testRecordConversionWithTableNameNew() throws IOException {
    File source = createTempFile("tn_source.html");
    File converted = createTempFile("tn_converted.json");

    metadata.recordConversionWithTableName("my_table", source, converted, "HTML_TO_JSON");

    assertTrue(metadata.hasTableMetadata("my_table"));
    ConversionMetadata.ConversionRecord record = metadata.getAllConversions().get("my_table");
    assertEquals("my_table", record.tableName);
    assertEquals("HTML_TO_JSON", record.conversionType);
  }

  @Test
  void testRecordConversionWithTableNameUpdate() throws IOException {
    // Pre-populate with a record
    ConversionMetadata.ConversionRecord existing = new ConversionMetadata.ConversionRecord();
    existing.tableName = "upd_table";
    existing.sourceFile = "/old/source.csv";
    metadata.putConversionRecord("upd_table", existing);

    File source = createTempFile("upd_source.html");
    File converted = createTempFile("upd_converted.json");

    metadata.recordConversionWithTableName("upd_table", source, converted, "HTML_TO_JSON");

    ConversionMetadata.ConversionRecord updated = metadata.getAllConversions().get("upd_table");
    assertEquals("upd_table", updated.tableName);
    assertEquals("HTML_TO_JSON", updated.conversionType);
    assertEquals(source.getCanonicalPath(), updated.originalFile);
  }

  // ======= updateExistingRecord =======

  @Test
  void testUpdateExistingRecordFound() throws IOException {
    File sourceFile = createTempFile("uer_source.html");

    ConversionMetadata.ConversionRecord existing = new ConversionMetadata.ConversionRecord();
    existing.tableName = null;
    existing.originalFile = sourceFile.getCanonicalPath();
    existing.sourceFile = sourceFile.getCanonicalPath();
    metadata.putConversionRecord(sourceFile.getCanonicalPath(), existing);

    File convertedFile = createTempFile("uer_converted.json");

    metadata.updateExistingRecord(sourceFile, convertedFile, "HTML_TO_JSON",
        tempDir.toFile(), "generated_name");

    // The existing record should be updated
    ConversionMetadata.ConversionRecord updated =
        metadata.getAllConversions().get(sourceFile.getCanonicalPath());
    assertNotNull(updated);
    assertEquals("generated_name", updated.tableName);
  }

  @Test
  void testUpdateExistingRecordNotFoundCreatesNew() throws IOException {
    File sourceFile = createTempFile("uer_new_source.html");
    File convertedFile = createTempFile("uer_new_converted.json");

    metadata.updateExistingRecord(sourceFile, convertedFile, "HTML_TO_JSON",
        tempDir.toFile(), "new_table_name");

    assertTrue(metadata.hasTableMetadata("new_table_name"));
  }

  @Test
  void testUpdateExistingRecordNotFoundNoTableName() throws IOException {
    File sourceFile = createTempFile("uer_noname_source.html");
    File convertedFile = createTempFile("uer_noname_converted.json");

    metadata.updateExistingRecord(sourceFile, convertedFile, "HTML_TO_JSON",
        tempDir.toFile(), null);

    // Should use recordConversion without table name
    ConversionMetadata.ConversionRecord record = metadata.getConversionRecord(convertedFile);
    assertNotNull(record);
  }

  @Test
  void testUpdateExistingRecordTwoArgs() throws IOException {
    File sourceFile = createTempFile("uer2_source.html");
    File convertedFile = createTempFile("uer2_converted.json");

    metadata.updateExistingRecord(sourceFile, convertedFile, "HTML_TO_JSON", tempDir.toFile());
    // Should call the 5-arg version with null generatedTableName
  }

  @Test
  void testUpdateExistingRecordPreservesTableName() throws IOException {
    File sourceFile = createTempFile("uer_preserve_source.html");

    ConversionMetadata.ConversionRecord existing = new ConversionMetadata.ConversionRecord();
    existing.tableName = "preserved_name";
    existing.originalFile = sourceFile.getCanonicalPath();
    existing.sourceFile = sourceFile.getCanonicalPath();
    metadata.putConversionRecord(sourceFile.getCanonicalPath(), existing);

    File convertedFile = createTempFile("uer_preserve_converted.json");

    metadata.updateExistingRecord(sourceFile, convertedFile, "HTML_TO_JSON",
        tempDir.toFile(), "new_name");

    // Table name should be preserved (not overwritten)
    ConversionMetadata.ConversionRecord updated =
        metadata.getAllConversions().get(sourceFile.getCanonicalPath());
    assertEquals("preserved_name", updated.tableName);
  }

  // ======= getConversionRecordByConvertedFile =======

  @Test
  void testGetConversionRecordByConvertedFile() throws IOException {
    File original = createTempFile("gcrbc_orig.xlsx");
    File converted = createTempFile("gcrbc_conv.json");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    ConversionMetadata.ConversionRecord record =
        metadata.getConversionRecordByConvertedFile(converted.getCanonicalPath());
    assertNotNull(record);
  }

  @Test
  void testGetConversionRecordByConvertedFileNotFound() {
    ConversionMetadata.ConversionRecord record =
        metadata.getConversionRecordByConvertedFile(tempDir.resolve("nonexistent.json").toString());
    assertNull(record);
  }

  // ======= findRecordBySourceFile =======

  @Test
  void testFindRecordBySourceFile() throws IOException {
    File source = createTempFile("frbsf_source.xlsx");
    File converted = createTempFile("frbsf_converted.json");

    metadata.recordConversion(source, converted, "EXCEL_TO_JSON");

    ConversionMetadata.ConversionRecord record = metadata.findRecordBySourceFile(source);
    assertNotNull(record);
    assertEquals("EXCEL_TO_JSON", record.conversionType);
  }

  @Test
  void testFindRecordBySourceFileNotFound() throws IOException {
    File source = createTempFile("not_recorded.xlsx");
    assertNull(metadata.findRecordBySourceFile(source));
  }

  // ======= clear =======

  @Test
  void testClear() throws IOException {
    File orig = createTempFile("clear_orig.csv");
    File conv = createTempFile("clear_conv.json");

    metadata.recordConversion(orig, conv, "TEST");
    assertFalse(metadata.getAllConversions().isEmpty());

    metadata.clear();
    assertTrue(metadata.getAllConversions().isEmpty());
  }

  // ======= reload =======

  @Test
  void testReload() throws IOException {
    File orig = createTempFile("reload_orig.csv");
    File conv = createTempFile("reload_conv.json");

    metadata.recordConversion(orig, conv, "TEST");

    // Create a second metadata instance pointing to the same directory
    ConversionMetadata metadata2 = new ConversionMetadata(tempDir.toFile());

    // metadata2 should have loaded the same records
    assertFalse(metadata2.getAllConversions().isEmpty());

    // Reload should re-read from disk
    metadata2.reload();
    assertFalse(metadata2.getAllConversions().isEmpty());
  }

  // ======= saveMetadata and loadMetadata roundtrip =======

  @Test
  void testSaveAndLoadRoundtrip() throws IOException {
    File orig = createTempFile("rt_orig.xlsx");
    File conv = createTempFile("rt_conv.json");

    metadata.recordConversion(orig, conv, "EXCEL_TO_JSON");
    metadata.saveMetadata();

    // Create new instance to force load from disk
    ConversionMetadata loaded = new ConversionMetadata(tempDir.toFile());
    ConversionMetadata.ConversionRecord record = loaded.getConversionRecord(conv);
    assertNotNull(record);
    assertEquals("EXCEL_TO_JSON", record.conversionType);
  }

  // ======= Static file type detection methods =======

  @Test
  void testDetectConvertibleType() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("detectConvertibleType", String.class);
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
  void testDetectDirectType() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("detectDirectType", String.class);
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

  @Test
  void testExtractExtension() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("extractExtension", String.class);
    method.setAccessible(true);

    assertEquals("csv", method.invoke(null, "test.csv"));
    assertNull(method.invoke(null, "noprefix.csv"));
  }

  // ======= isRemoteFile and isGlobPattern =======

  @Test
  void testIsRemoteFile() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("isRemoteFile", String.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(metadata, "http://example.com/file"));
    assertTrue((Boolean) method.invoke(metadata, "https://example.com/file"));
    assertTrue((Boolean) method.invoke(metadata, "s3://bucket/file"));
    assertTrue((Boolean) method.invoke(metadata, "ftp://server/file"));
    assertTrue((Boolean) method.invoke(metadata, "sftp://server/file"));
    assertFalse((Boolean) method.invoke(metadata, "/local/path/file"));
    assertFalse((Boolean) method.invoke(metadata, (String) null));
  }

  @Test
  void testIsGlobPattern() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("isGlobPattern", String.class);
    method.setAccessible(true);

    assertTrue((Boolean) method.invoke(metadata, "s3://bucket/**/*.parquet"));
    assertTrue((Boolean) method.invoke(metadata, "/path/to/*.csv"));
    assertTrue((Boolean) method.invoke(metadata, "/path/file[0-9].csv"));
    assertTrue((Boolean) method.invoke(metadata, "/path/file?.csv"));
    assertFalse((Boolean) method.invoke(metadata, "/path/to/file.csv"));
    assertFalse((Boolean) method.invoke(metadata, (String) null));
  }

  // ======= isHivePartitioned =======

  @Test
  void testIsHivePartitioned() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("isHivePartitioned", List.class);
    method.setAccessible(true);

    // Null
    assertFalse((Boolean) method.invoke(null, (List<?>) null));

    // Empty
    assertFalse((Boolean) method.invoke(null, Collections.emptyList()));

    // Single file
    List<String> singleFile = Collections.singletonList("/data/file.parquet");
    assertFalse((Boolean) method.invoke(null, singleFile));

    // Non-partitioned files
    List<String> nonPartitioned = new ArrayList<String>();
    nonPartitioned.add("/data/file1.parquet");
    nonPartitioned.add("/data/file2.parquet");
    assertFalse((Boolean) method.invoke(null, nonPartitioned));

    // Hive-partitioned files
    List<String> partitioned = new ArrayList<String>();
    partitioned.add("/data/year=2020/file1.parquet");
    partitioned.add("/data/year=2021/file2.parquet");
    assertTrue((Boolean) method.invoke(null, partitioned));
  }

  // ======= formatRecord =======

  @Test
  void testFormatRecord() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("formatRecord", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);

    assertEquals("null", method.invoke(metadata, (ConversionMetadata.ConversionRecord) null));

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "test";
    record.conversionType = "DIRECT";
    String formatted = (String) method.invoke(metadata, record);
    assertTrue(formatted.contains("test"));
    assertTrue(formatted.contains("DIRECT"));

    // Test with long parquetCacheFile (> 200 chars)
    StringBuilder longPath = new StringBuilder();
    for (int i = 0; i < 300; i++) {
      longPath.append("x");
    }
    record.parquetCacheFile = longPath.toString();
    String truncated = (String) method.invoke(metadata, record);
    assertTrue(truncated.contains("..."));
  }

  // ======= buildComprehensiveMapping =======

  @Test
  void testBuildComprehensiveMappingNullDir() {
    Map<String, String> result = ConversionMetadata.buildComprehensiveMapping(null, new HashMap<String, String>());
    assertTrue(result.isEmpty());
  }

  @Test
  void testBuildComprehensiveMappingNoAperioDir() {
    Map<String, String> result = ConversionMetadata.buildComprehensiveMapping(tempDir.toFile(), new HashMap<String, String>());
    assertTrue(result.isEmpty());
  }

  // ======= detectSourceType =======

  @Test
  void testDetectSourceType() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("detectSourceType", String.class);
    method.setAccessible(true);

    assertEquals("unknown", method.invoke(metadata, (String) null));
    // CSV is directly usable
    assertEquals("csv", method.invoke(metadata, "/path/to/file.csv"));
    // XLSX requires conversion
    assertEquals("excel", method.invoke(metadata, "/path/to/file.xlsx"));
    // Unknown extension
    assertEquals("unknown", method.invoke(metadata, "/path/to/file.xyz"));
  }

  // ======= Helper methods =======

  private File createTempFile(String name) throws IOException {
    File file = tempDir.resolve(name).toFile();
    if (!file.exists()) {
      file.createNewFile();
    }
    return file;
  }
}
