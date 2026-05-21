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

import org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata.FileBaseline;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata.PartitionBaseline;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Comprehensive unit tests for {@link ConversionMetadata}.
 *
 * <p>Covers file extension detection, metadata recording and reading,
 * concurrent access with JVM locks, file lock handling, metadata persistence
 * across calls, and edge cases.
 */
@Tag("unit")
public class ConversionMetadataTest {

  @TempDir
  public File tempDir;

  // =========================================================================
  // 1. File Extension Detection / Classification
  // =========================================================================

  @Test void testDetectTypeFromTestFileExcel() throws Exception {
    assertEquals("excel", invokeDetectTypeFromTestFile("test.xlsx"));
    assertEquals("excel", invokeDetectTypeFromTestFile("test.xls"));
  }

  @Test void testDetectTypeFromTestFileHtml() throws Exception {
    assertEquals("html", invokeDetectTypeFromTestFile("test.html"));
    assertEquals("html", invokeDetectTypeFromTestFile("test.htm"));
  }

  @Test void testDetectTypeFromTestFileXml() throws Exception {
    assertEquals("xml", invokeDetectTypeFromTestFile("test.xml"));
  }

  @Test void testDetectTypeFromTestFileMarkdown() throws Exception {
    assertEquals("markdown", invokeDetectTypeFromTestFile("test.md"));
  }

  @Test void testDetectTypeFromTestFileDocx() throws Exception {
    assertEquals("docx", invokeDetectTypeFromTestFile("test.docx"));
  }

  @Test void testDetectTypeFromTestFilePptx() throws Exception {
    assertEquals("pptx", invokeDetectTypeFromTestFile("test.pptx"));
  }

  @Test void testDetectTypeFromTestFileDirectlyUsableCsv() throws Exception {
    assertEquals("csv", invokeDetectTypeFromTestFile("test.csv"));
  }

  @Test void testDetectTypeFromTestFileDirectlyUsableTsv() throws Exception {
    assertEquals("tsv", invokeDetectTypeFromTestFile("test.tsv"));
  }

  @Test void testDetectTypeFromTestFileDirectlyUsableJson() throws Exception {
    assertEquals("json", invokeDetectTypeFromTestFile("test.json"));
  }

  @Test void testDetectTypeFromTestFileDirectlyUsableParquet() throws Exception {
    assertEquals("parquet", invokeDetectTypeFromTestFile("test.parquet"));
  }

  @Test void testDetectTypeFromTestFileDirectlyUsableYaml() throws Exception {
    assertEquals("yaml", invokeDetectTypeFromTestFile("test.yaml"));
    assertEquals("yaml", invokeDetectTypeFromTestFile("test.yml"));
  }

  @Test void testDetectTypeFromTestFileDirectlyUsableArrow() throws Exception {
    assertEquals("arrow", invokeDetectTypeFromTestFile("test.arrow"));
  }

  @Test void testDetectTypeFromTestFileUnknownExtension() throws Exception {
    assertEquals("unknown", invokeDetectTypeFromTestFile("test.xyz"));
    assertEquals("unknown", invokeDetectTypeFromTestFile("test.txt"));
  }

  @Test void testDetectTypeFromExtensionDelegates() throws Exception {
    // detectTypeFromExtension("csv") calls detectTypeFromTestFile("test.csv")
    assertEquals("csv", invokeDetectTypeFromExtension("csv"));
    assertEquals("excel", invokeDetectTypeFromExtension("xlsx"));
    assertEquals("html", invokeDetectTypeFromExtension("html"));
    assertEquals("parquet", invokeDetectTypeFromExtension("parquet"));
    assertEquals("unknown", invokeDetectTypeFromExtension("bogus"));
  }

  @Test void testGenerateFileExtensionsIncludesKnownTypes() throws Exception {
    @SuppressWarnings("unchecked")
    List<Map.Entry<String, String>> extensions = invokeGenerateFileExtensions();

    // Collect extension -> type for verification
    Map<String, String> extMap = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : extensions) {
      extMap.put(entry.getKey(), entry.getValue());
    }

    // Verify known extensions appear
    assertTrue(extMap.containsKey("csv"), "Should contain csv extension");
    assertEquals("csv", extMap.get("csv"));
    assertTrue(extMap.containsKey("parquet"), "Should contain parquet extension");
    assertEquals("parquet", extMap.get("parquet"));
    assertTrue(extMap.containsKey("json"), "Should contain json extension");
    assertEquals("json", extMap.get("json"));
  }

  @Test void testGenerateFileExtensionsIncludesCompressedVariants() throws Exception {
    @SuppressWarnings("unchecked")
    List<Map.Entry<String, String>> extensions = invokeGenerateFileExtensions();

    Map<String, String> extMap = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : extensions) {
      extMap.put(entry.getKey(), entry.getValue());
    }

    // Compressed variants should be generated for directly usable types
    assertTrue(extMap.containsKey("csv.gz"), "Should contain csv.gz compressed variant");
    assertEquals("csv", extMap.get("csv.gz"));
    assertTrue(extMap.containsKey("json.gz"), "Should contain json.gz compressed variant");
    assertEquals("json", extMap.get("json.gz"));
    assertTrue(extMap.containsKey("csv.bz2"), "Should contain csv.bz2 compressed variant");
    assertTrue(extMap.containsKey("csv.xz"), "Should contain csv.xz compressed variant");
    assertTrue(extMap.containsKey("csv.zip"), "Should contain csv.zip compressed variant");
  }

  @Test void testExtractExtensionWithTestPrefix() throws Exception {
    assertEquals("csv", invokeExtractExtension("test.csv"));
    assertEquals("csv.gz", invokeExtractExtension("test.csv.gz"));
    assertNull(invokeExtractExtension("nottest.csv"));
  }

  // =========================================================================
  // 2. Metadata Recording and Reading - Basic CRUD
  // =========================================================================

  @Test void testConstructorCreatesInstance() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);
    assertNotNull(metadata);
  }

  @Test void testConstructorWithStringPath() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir.getAbsolutePath());
    assertNotNull(metadata);
  }

  @Test void testConstructorWithS3PathDoesNotLoadMetadata() {
    // S3 paths should not try to load local file metadata
    ConversionMetadata metadata = new ConversionMetadata("s3://bucket/path");
    assertNotNull(metadata);
    assertTrue(metadata.getAllConversions().isEmpty());
  }

  @Test void testRecordConversionBasic() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("source.xlsx");
    File converted = createTempFile("source.json");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertEquals(original.getCanonicalPath(), record.originalFile);
    assertEquals(converted.getCanonicalPath(), record.convertedFile);
    assertEquals("EXCEL_TO_JSON", record.conversionType);
    assertTrue(record.timestamp > 0, "Timestamp should be set");
  }

  @Test void testRecordConversionWithParquetCacheFile() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("data.xlsx");
    File converted = createTempFile("data.json");
    File parquetCache = createTempFile("data.parquet");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON", parquetCache);

    ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertEquals(parquetCache.getCanonicalPath(), record.parquetCacheFile);
  }

  @Test void testRecordConversionWithNullParquetCacheFile() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("data2.xlsx");
    File converted = createTempFile("data2.json");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON", null);

    ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertNull(record.parquetCacheFile);
  }

  @Test void testRecordConversionWithPrebuiltRecord() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File converted = createTempFile("prebuilt.json");
    ConversionRecord record =
        new ConversionRecord("/origin/source.html", converted.getCanonicalPath(), "HTML_TO_JSON",
        null, "etag-abc", 5000L, "text/html");

    metadata.recordConversion(converted, record);

    ConversionRecord stored = metadata.getConversionRecord(converted);
    assertNotNull(stored);
    assertEquals("etag-abc", stored.etag);
    assertEquals(Long.valueOf(5000L), stored.contentLength);
    assertEquals("text/html", stored.contentType);
  }

  @Test void testFindOriginalSourceReverseLookup() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("report.html");
    File converted = createTempFile("report.json");

    metadata.recordConversion(original, converted, "HTML_TO_JSON");

    File found = metadata.findOriginalSource(converted);
    assertNotNull(found);
    assertEquals(original.getCanonicalPath(), found.getCanonicalPath());
  }

  @Test void testFindOriginalSourceReturnsNullWhenOriginalDeleted() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("will_delete.html");
    File converted = createTempFile("will_delete.json");

    metadata.recordConversion(original, converted, "HTML_TO_JSON");

    // Delete the original
    assertTrue(original.delete(), "Should be able to delete temp file");

    File found = metadata.findOriginalSource(converted);
    assertNull(found, "Should return null when original file is deleted");
    // Stale record should be cleaned up
    assertNull(metadata.getConversionRecord(converted),
        "Stale conversion record should be removed");
  }

  @Test void testFindOriginalSourceReturnsNullForUnknown() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);
    File unknown = createTempFile("unknown.json");
    assertNull(metadata.findOriginalSource(unknown));
  }

  @Test void testFindDerivedFiles() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("multi_source.xlsx");
    File derived1 = createTempFile("sheet1.json");
    File derived2 = createTempFile("sheet2.json");

    metadata.recordConversion(original, derived1, "EXCEL_TO_JSON");
    metadata.recordConversion(original, derived2, "EXCEL_TO_JSON");

    List<File> derivedFiles = metadata.findDerivedFiles(original);
    assertEquals(2, derivedFiles.size());
  }

  @Test void testFindDerivedFilesReturnsEmptyForNoDerivatives() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);
    File unrelated = createTempFile("unrelated.csv");
    List<File> derivedFiles = metadata.findDerivedFiles(unrelated);
    assertTrue(derivedFiles.isEmpty());
  }

  @Test void testFindRecordBySourceFile() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("input.csv");
    File converted = createTempFile("input.json");

    metadata.recordConversion(original, converted, "CSV_TO_JSON");

    ConversionRecord record = metadata.findRecordBySourceFile(original);
    assertNotNull(record);
    assertEquals(converted.getCanonicalPath(), record.convertedFile);
  }

  @Test void testFindRecordBySourceFileReturnsNullWhenNotFound() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);
    File unknown = createTempFile("no_record.csv");
    assertNull(metadata.findRecordBySourceFile(unknown));
  }

  @Test void testGetConversionRecordByConvertedFilePath() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("lookup.html");
    File converted = createTempFile("lookup.json");

    metadata.recordConversion(original, converted, "HTML_TO_JSON");

    ConversionRecord record =
        metadata.getConversionRecordByConvertedFile(converted.getAbsolutePath());
    assertNotNull(record);
    assertEquals("HTML_TO_JSON", record.conversionType);
  }

  @Test void testGetConversionRecordByConvertedFilePathNotFound() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);
    assertNull(metadata.getConversionRecordByConvertedFile("/nonexistent/path.json"));
  }

  @Test void testGetAllConversionsReturnsAllRecords() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original1 = createTempFile("file1.xlsx");
    File converted1 = createTempFile("file1.json");
    File original2 = createTempFile("file2.html");
    File converted2 = createTempFile("file2.json");

    metadata.recordConversion(original1, converted1, "EXCEL_TO_JSON");
    metadata.recordConversion(original2, converted2, "HTML_TO_JSON");

    Map<String, ConversionRecord> all = metadata.getAllConversions();
    assertEquals(2, all.size());
  }

  @Test void testGetAllConversionsReturnsUnmodifiableMap() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("unmod.xlsx");
    File converted = createTempFile("unmod.json");
    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    Map<String, ConversionRecord> all = metadata.getAllConversions();
    try {
      all.put("new_key", new ConversionRecord());
      fail("Should throw UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testClearRemovesAllRecords() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("clear_me.xlsx");
    File converted = createTempFile("clear_me.json");
    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    metadata.clear();

    Map<String, ConversionRecord> all = metadata.getAllConversions();
    assertTrue(all.isEmpty(), "All conversions should be cleared");
  }

  @Test void testClearDeletesMetadataFile() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("delete_file.xlsx");
    File converted = createTempFile("delete_file.json");
    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    File metadataFile = new File(tempDir, ".conversions.json");
    assertTrue(metadataFile.exists(), "Metadata file should exist after recording");

    metadata.clear();

    assertFalse(metadataFile.exists(), "Metadata file should be deleted after clear");
  }

  // =========================================================================
  // 3. PutConversionRecord / HasTableMetadata / UpdateRecordWithParquetFile
  // =========================================================================

  @Test void testHasTableMetadataAfterPutConversionRecord() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    assertFalse(metadata.hasTableMetadata("my_table"));

    ConversionRecord record = new ConversionRecord("/orig", "/conv", "DIRECT");
    metadata.putConversionRecord("my_table", record);

    assertTrue(metadata.hasTableMetadata("my_table"));
  }

  @Test void testUpdateRecordWithParquetFile() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    ConversionRecord record = new ConversionRecord("/orig.csv", "/conv.json", "CSV_TO_JSON");
    metadata.putConversionRecord("sales_data", record);

    File parquetFile = createTempFile("sales_data.parquet");
    metadata.updateRecordWithParquetFile("sales_data", parquetFile);

    Map<String, ConversionRecord> all = metadata.getAllConversions();
    ConversionRecord updated = all.get("sales_data");
    assertNotNull(updated);
    assertEquals(parquetFile.getCanonicalPath(), updated.parquetCacheFile);
    assertEquals("ParquetTranslatableTable", updated.tableType);
  }

  @Test void testUpdateRecordWithParquetFileNonExistentTable() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);
    File parquetFile = createTempFile("orphan.parquet");

    // Should not throw, just log a warning
    metadata.updateRecordWithParquetFile("nonexistent_table", parquetFile);

    assertFalse(metadata.hasTableMetadata("nonexistent_table"));
  }

  @Test void testUpdateCachedFile() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("cached_source.html");
    File converted = createTempFile("cached_conv.json");
    metadata.recordConversion(original, converted, "HTML_TO_JSON");

    File parquetCache = createTempFile("cached_conv.parquet");
    metadata.updateCachedFile(converted, parquetCache);

    ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertEquals(parquetCache.getCanonicalPath(), record.parquetCacheFile);
  }

  @Test void testUpdateCachedFileWithNull() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("null_cache_src.html");
    File converted = createTempFile("null_cache_conv.json");
    metadata.recordConversion(original, converted, "HTML_TO_JSON", createTempFile("old.parquet"));

    metadata.updateCachedFile(converted, null);

    ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertNull(record.parquetCacheFile);
  }

  // =========================================================================
  // 4. RecordConversionWithTableName
  // =========================================================================

  @Test void testRecordConversionWithTableNameCreatesNewRecord() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File source = createTempFile("source_named.html");
    File converted = createTempFile("converted_named.json");

    metadata.recordConversionWithTableName("my_table", source, converted, "HTML_TO_JSON");

    // Should be accessible by table name
    assertTrue(metadata.hasTableMetadata("my_table"));
    ConversionRecord record = metadata.getAllConversions().get("my_table");
    assertNotNull(record);
    assertEquals("my_table", record.tableName);
    assertEquals("HTML_TO_JSON", record.conversionType);
    assertEquals(source.getCanonicalPath(), record.originalFile);
    assertEquals(converted.getCanonicalPath(), record.convertedFile);

    // Should also be accessible by converted file path
    ConversionRecord byConvertedFile =
        metadata.getConversionRecordByConvertedFile(converted.getAbsolutePath());
    assertNotNull(byConvertedFile, "Should be accessible via converted file path as alternate key");
  }

  @Test void testRecordConversionWithTableNameUpdatesExisting() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File source1 = createTempFile("source_v1.html");
    File converted1 = createTempFile("converted_v1.json");

    // Create initial record
    metadata.recordConversionWithTableName("update_table", source1, converted1, "HTML_TO_JSON");

    // Update with new conversion
    File source2 = createTempFile("source_v2.html");
    File converted2 = createTempFile("converted_v2.json");
    metadata.recordConversionWithTableName("update_table", source2, converted2, "HTML_TO_JSON_V2");

    ConversionRecord record = metadata.getAllConversions().get("update_table");
    assertNotNull(record);
    assertEquals(source2.getCanonicalPath(), record.originalFile);
    assertEquals(converted2.getCanonicalPath(), record.convertedFile);
    assertEquals("HTML_TO_JSON_V2", record.conversionType);
  }

  // =========================================================================
  // 5. UpdateExistingRecord
  // =========================================================================

  @Test void testUpdateExistingRecordUpdatesExisting() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File source = createTempFile("existing_source.csv");
    File converted1 = createTempFile("existing_conv1.json");
    metadata.recordConversion(source, converted1, "CSV_TO_JSON");

    File converted2 = createTempFile("existing_conv2.json");
    metadata.updateExistingRecord(source, converted2, "CSV_TO_JSON_V2", tempDir, "updated_table");

    ConversionRecord record = metadata.findRecordBySourceFile(source);
    assertNotNull(record, "Updated record should be findable by source file");
    assertEquals(converted2.getCanonicalPath(), record.convertedFile);
    assertEquals("CSV_TO_JSON_V2", record.conversionType);
  }

  @Test void testUpdateExistingRecordCreatesNewWhenNotFound() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File source = createTempFile("new_source.csv");
    File converted = createTempFile("new_conv.json");

    metadata.updateExistingRecord(source, converted, "CSV_TO_JSON", tempDir, "new_table");

    assertTrue(metadata.hasTableMetadata("new_table"),
        "Should create new record when no existing one found");
  }

  @Test void testUpdateExistingRecordWithNullGeneratedTableName() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File source = createTempFile("null_name_source.csv");
    File converted = createTempFile("null_name_conv.json");

    // When generatedTableName is null, recordConversion (not recordConversionWithTableName) is used
    metadata.updateExistingRecord(source, converted, "CSV_TO_JSON", tempDir, null);

    // Record should still be findable by converted file
    ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record, "Record should be created even with null table name");
  }

  @Test void testUpdateExistingRecordPreservesExistingTableName() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File source = createTempFile("preserve_name_src.csv");
    File converted1 = createTempFile("preserve_name_conv1.json");

    // Create with explicit table name
    metadata.recordConversionWithTableName("explicit_name", source, converted1, "CSV_TO_JSON");

    // Update with a generated name - should preserve existing
    File converted2 = createTempFile("preserve_name_conv2.json");
    metadata.updateExistingRecord(source, converted2, "CSV_TO_JSON_V2", tempDir, "generated_name");

    ConversionRecord record = metadata.findRecordBySourceFile(source);
    assertNotNull(record);
    // The existing table name "explicit_name" should be preserved
    assertEquals("explicit_name", record.tableName);
  }

  // =========================================================================
  // 6. Materialization Info
  // =========================================================================

  @Test void testUpdateMaterializationInfoExistingRecord() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    ConversionRecord record = new ConversionRecord();
    record.tableName = "mat_table";
    record.conversionType = "DIRECT";
    metadata.putConversionRecord("mat_table", record);
    metadata.saveMetadata();

    metadata.updateMaterializationInfo("mat_table", "s3://bucket/table", "ICEBERG_PARQUET", 50000L);

    ConversionRecord updated = metadata.getAllConversions().get("mat_table");
    assertNotNull(updated);
    assertEquals("ICEBERG_PARQUET", updated.conversionType);
    assertEquals("s3://bucket/table", updated.sourceFile);
    assertEquals("IcebergTable", updated.tableType);
    assertEquals(Long.valueOf(50000L), updated.rowCount);
    // Iceberg conversion should clear parquet fields
    assertNull(updated.viewScanPattern);
    assertNull(updated.convertedFile);
    assertNull(updated.parquetCacheFile);
  }

  @Test void testUpdateMaterializationInfoCreatesNewRecord() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    metadata.updateMaterializationInfo("new_mat", "s3://bucket/new", "ICEBERG_PARQUET", 1000L);

    ConversionRecord record = metadata.getAllConversions().get("new_mat");
    assertNotNull(record, "New record should be created for materialization");
    assertEquals("new_mat", record.tableName);
    assertEquals("ICEBERG_PARQUET", record.conversionType);
    assertEquals(Long.valueOf(1000L), record.rowCount);
  }

  @Test void testUpdateMaterializationInfoPreservesRowCountWhenNull() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    ConversionRecord record = new ConversionRecord();
    record.tableName = "preserve_count";
    record.rowCount = 99999L;
    metadata.putConversionRecord("preserve_count", record);
    metadata.saveMetadata();

    // Update with null rowCount - should preserve existing
    metadata.updateMaterializationInfo("preserve_count", "s3://bucket/t", "PARQUET", null);

    ConversionRecord updated = metadata.getAllConversions().get("preserve_count");
    assertEquals(Long.valueOf(99999L), updated.rowCount, "Existing rowCount should be preserved");
  }

  @Test void testUpdateMaterializationInfoWithoutRowCountOverload() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    ConversionRecord record = new ConversionRecord();
    record.tableName = "no_count_overload";
    metadata.putConversionRecord("no_count_overload", record);
    metadata.saveMetadata();

    // Use the 3-arg overload that delegates to 4-arg with null rowCount
    metadata.updateMaterializationInfo("no_count_overload", "/local/path", "PARQUET");

    ConversionRecord updated = metadata.getAllConversions().get("no_count_overload");
    assertNotNull(updated);
    assertEquals("PARQUET", updated.conversionType);
    assertEquals("ParquetTable", updated.tableType);
  }

  // =========================================================================
  // 7. Persistence Across Calls (Write to Temp Dir, Read Back)
  // =========================================================================

  @Test void testPersistenceAcrossReload() throws IOException {
    File original = createTempFile("persist.xlsx");
    File converted = createTempFile("persist.json");

    // Write with first instance
    ConversionMetadata metadata1 = new ConversionMetadata(tempDir);
    metadata1.recordConversion(original, converted, "EXCEL_TO_JSON");

    // Read with second instance (simulates restart)
    ConversionMetadata metadata2 = new ConversionMetadata(tempDir);
    ConversionRecord record = metadata2.getConversionRecord(converted);
    assertNotNull(record, "Record should persist across reload");
    assertEquals("EXCEL_TO_JSON", record.conversionType);
  }

  @Test void testPersistenceMultipleRecordsAcrossReload() throws IOException {
    File original1 = createTempFile("persist1.xlsx");
    File converted1 = createTempFile("persist1.json");
    File original2 = createTempFile("persist2.html");
    File converted2 = createTempFile("persist2.json");

    ConversionMetadata writer = new ConversionMetadata(tempDir);
    writer.recordConversion(original1, converted1, "EXCEL_TO_JSON");
    writer.recordConversion(original2, converted2, "HTML_TO_JSON");

    ConversionMetadata reader = new ConversionMetadata(tempDir);
    assertEquals(2, reader.getAllConversions().size(),
        "All records should persist across reload");
    assertNotNull(reader.getConversionRecord(converted1));
    assertNotNull(reader.getConversionRecord(converted2));
  }

  @Test void testReloadMethod() throws IOException {
    File original = createTempFile("reload_test.xlsx");
    File converted = createTempFile("reload_test.json");

    ConversionMetadata metadata1 = new ConversionMetadata(tempDir);
    ConversionMetadata metadata2 = new ConversionMetadata(tempDir);

    // Instance 1 records a conversion
    metadata1.recordConversion(original, converted, "EXCEL_TO_JSON");

    // Instance 2 does not see it yet (loaded at construction)
    assertNull(metadata2.getConversionRecord(converted));

    // After reload, instance 2 sees it
    metadata2.reload();
    assertNotNull(metadata2.getConversionRecord(converted),
        "Reload should pick up changes from other instances");
  }

  @Test void testMetadataIsWrittenAtomically() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("atomic_source.xlsx");
    File converted = createTempFile("atomic_conv.json");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    // After saving, metadata file should exist and no temp files should remain
    File metadataFile = new File(tempDir, ".conversions.json");
    assertTrue(metadataFile.exists(), "Metadata file should exist after save");

    // No .tmp files should remain (atomic rename should clean up)
    File[] tmpFiles = tempDir.listFiles(new java.io.FilenameFilter() {
      @Override public boolean accept(File dir, String name) {
        return name.startsWith(".conversions.json.tmp.");
      }
    });
    assertTrue(tmpFiles == null || tmpFiles.length == 0,
        "No temp files should remain after atomic write");
  }

  @Test void testMetadataFileIsValidJson() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("valid_json_src.xlsx");
    File converted = createTempFile("valid_json_conv.json");
    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    File metadataFile = new File(tempDir, ".conversions.json");
    assertTrue(metadataFile.exists());

    // Should be parseable JSON
    ObjectMapper mapper = new ObjectMapper();
    @SuppressWarnings("unchecked")
    Map<String, Object> parsed = mapper.readValue(metadataFile, Map.class);
    assertFalse(parsed.isEmpty(), "Metadata JSON should have entries");
  }

  // =========================================================================
  // 8. Concurrent Access with JVM_LOCKS
  // =========================================================================

  @Test void testConcurrentRecordConversion() throws Exception {
    final int threadCount = 8;
    final int recordsPerThread = 10;
    final ConversionMetadata metadata = new ConversionMetadata(tempDir);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicInteger errors = new AtomicInteger(0);

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    List<Future<?>> futures = new ArrayList<Future<?>>();

    for (int t = 0; t < threadCount; t++) {
      final int threadIndex = t;
      futures.add(
          executor.submit(new Runnable() {
        @Override public void run() {
          try {
            startLatch.await();
            for (int i = 0; i < recordsPerThread; i++) {
              File original =
                  new File(tempDir, "concurrent_orig_" + threadIndex + "_" + i + ".html");
              original.createNewFile();
              File converted =
                  new File(tempDir, "concurrent_conv_" + threadIndex + "_" + i + ".json");
              converted.createNewFile();
              metadata.recordConversion(original, converted, "HTML_TO_JSON");
            }
          } catch (Exception e) {
            errors.incrementAndGet();
          }
        }
      }));
    }

    // Release all threads simultaneously
    startLatch.countDown();

    for (Future<?> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }

    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

    assertEquals(0, errors.get(), "No errors should occur during concurrent writes");

    // All records should be present
    int totalExpected = threadCount * recordsPerThread;
    assertEquals(totalExpected, metadata.getAllConversions().size(),
        "All concurrent records should be stored");
  }

  @Test void testConcurrentReadAndWrite() throws Exception {
    final ConversionMetadata metadata = new ConversionMetadata(tempDir);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final AtomicInteger errors = new AtomicInteger(0);

    // Pre-populate some records
    for (int i = 0; i < 5; i++) {
      File original = createTempFile("preload_orig_" + i + ".xlsx");
      File converted = createTempFile("preload_conv_" + i + ".json");
      metadata.recordConversion(original, converted, "EXCEL_TO_JSON");
    }

    ExecutorService executor = Executors.newFixedThreadPool(4);
    List<Future<?>> futures = new ArrayList<Future<?>>();

    // Two readers
    for (int r = 0; r < 2; r++) {
      futures.add(
          executor.submit(new Runnable() {
        @Override public void run() {
          try {
            startLatch.await();
            for (int i = 0; i < 20; i++) {
              metadata.getAllConversions();
              metadata.reload();
            }
          } catch (Exception e) {
            errors.incrementAndGet();
          }
        }
      }));
    }

    // Two writers
    for (int w = 0; w < 2; w++) {
      final int writerIndex = w;
      futures.add(
          executor.submit(new Runnable() {
        @Override public void run() {
          try {
            startLatch.await();
            for (int i = 0; i < 10; i++) {
              File original =
                  new File(tempDir, "rw_orig_" + writerIndex + "_" + i + ".html");
              original.createNewFile();
              File converted =
                  new File(tempDir, "rw_conv_" + writerIndex + "_" + i + ".json");
              converted.createNewFile();
              metadata.recordConversion(original, converted, "HTML_TO_JSON");
            }
          } catch (Exception e) {
            errors.incrementAndGet();
          }
        }
      }));
    }

    startLatch.countDown();

    for (Future<?> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }

    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    assertEquals(0, errors.get(), "No errors should occur during concurrent read/write");
  }

  // =========================================================================
  // 9. ConversionRecord Inner Class Tests
  // =========================================================================

  @Test void testConversionRecordDefaultConstructor() {
    ConversionRecord record = new ConversionRecord();
    assertNull(record.originalFile);
    assertNull(record.convertedFile);
    assertNull(record.conversionType);
    assertNull(record.tableName);
    assertNull(record.tableType);
    assertNull(record.parquetCacheFile);
  }

  @Test void testConversionRecordThreeArgConstructor() {
    ConversionRecord record = new ConversionRecord("/orig.xlsx", "/conv.json", "EXCEL_TO_JSON");
    assertEquals("/orig.xlsx", record.originalFile);
    assertEquals("/conv.json", record.convertedFile);
    assertEquals("EXCEL_TO_JSON", record.conversionType);
    assertTrue(record.timestamp > 0);
    assertNull(record.parquetCacheFile);
  }

  @Test void testConversionRecordFourArgConstructor() {
    ConversionRecord record =
        new ConversionRecord("/orig.xlsx", "/conv.json", "EXCEL_TO_JSON", "/cache.parquet");
    assertEquals("/orig.xlsx", record.originalFile);
    assertEquals("/conv.json", record.convertedFile);
    assertEquals("EXCEL_TO_JSON", record.conversionType);
    assertEquals("/cache.parquet", record.parquetCacheFile);
    assertTrue(record.timestamp > 0);
  }

  @Test void testConversionRecordHttpMetadataConstructor() {
    ConversionRecord record =
        new ConversionRecord("https://example.com/data.csv", "/conv.json", "HTTP_DOWNLOAD",
        "/cache.parquet", "etag-123", 5000L, "text/csv");
    assertEquals("https://example.com/data.csv", record.originalFile);
    assertEquals("etag-123", record.etag);
    assertEquals(Long.valueOf(5000L), record.contentLength);
    assertEquals("text/csv", record.contentType);
  }

  @Test void testConversionRecordComprehensiveConstructor() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("key", "value");

    ConversionRecord record =
        new ConversionRecord("table1", "CsvScannableTable", "/source.csv", "csv",
        "/orig.csv", "/conv.json", "DIRECT",
        "/cache.parquet", "/dir/**/*.parquet", true, "PT5M",
        "etag-xyz", 1234L, "text/csv",
        config);

    assertEquals("table1", record.tableName);
    assertEquals("CsvScannableTable", record.tableType);
    assertEquals("/source.csv", record.sourceFile);
    assertEquals("csv", record.sourceType);
    assertEquals("/orig.csv", record.originalFile);
    assertEquals("/conv.json", record.convertedFile);
    assertEquals("DIRECT", record.conversionType);
    assertEquals("/cache.parquet", record.parquetCacheFile);
    assertEquals("/dir/**/*.parquet", record.viewScanPattern);
    assertTrue(record.refreshEnabled);
    assertEquals("PT5M", record.refreshInterval);
    assertEquals("etag-xyz", record.etag);
    assertEquals(Long.valueOf(1234L), record.contentLength);
    assertEquals("text/csv", record.contentType);
    assertEquals(config, record.tableConfig);
  }

  @Test void testConversionRecordGetters() {
    ConversionRecord record =
        new ConversionRecord("/orig.xlsx", "/conv.json", "EXCEL_TO_JSON", "/cache.parquet");
    record.tableName = "test_table";
    record.sourceFile = "/source.csv";

    assertEquals("/orig.xlsx", record.getOriginalPath());
    assertEquals("EXCEL_TO_JSON", record.getConversionType());
    assertEquals("test_table", record.getTableName());
    assertEquals("/source.csv", record.getSourceFile());
    assertEquals("/cache.parquet", record.getParquetCacheFile());
    assertEquals("/conv.json", record.getConvertedFile());
  }

  @Test void testConversionRecordHasChangedLocalFile() throws Exception {
    File localFile = createTempFile("local.csv");
    try (FileWriter writer = new FileWriter(localFile)) {
      writer.write("initial data");
    }

    ConversionRecord record =
        new ConversionRecord(localFile.getCanonicalPath(), "/converted.json", "DIRECT");
    // Set timestamp to before the file was last modified
    record.timestamp = localFile.lastModified() - 5000;

    assertTrue(record.hasChanged(),
        "File modified after record timestamp should be detected as changed");
  }

  @Test void testConversionRecordHasChangedLocalFileNotChanged() throws Exception {
    File localFile = createTempFile("unchanged.csv");
    try (FileWriter writer = new FileWriter(localFile)) {
      writer.write("data");
    }

    ConversionRecord record =
        new ConversionRecord(localFile.getCanonicalPath(), "/converted.json", "DIRECT");
    // Set timestamp to after the file was last modified
    record.timestamp = localFile.lastModified() + 5000;

    assertFalse(record.hasChanged(),
        "File not modified after record timestamp should not be detected as changed");
  }

  @Test void testConversionRecordHasChangedRemoteFile() {
    ConversionRecord record =
        new ConversionRecord("https://example.com/data.csv", "/converted.json", "DIRECT");

    // Remote files always return true (conservative)
    assertTrue(record.hasChanged());
  }

  @Test void testConversionRecordHasChangedNonExistentLocalFile() {
    ConversionRecord record =
        new ConversionRecord("/nonexistent/file.csv", "/converted.json", "DIRECT");
    // Non-existent file should return false (no change possible)
    assertFalse(record.hasChanged());
  }

  @Test void testConversionRecordIsIcebergFormat() {
    ConversionRecord record = new ConversionRecord();
    Map<String, Object> config = new HashMap<String, Object>();
    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", "iceberg");
    config.put("materialize", materialize);
    record.tableConfig = config;

    assertTrue(record.isIcebergFormat());
  }

  @Test void testConversionRecordIsIcebergFormatCaseInsensitive() {
    ConversionRecord record = new ConversionRecord();
    Map<String, Object> config = new HashMap<String, Object>();
    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", "ICEBERG");
    config.put("materialize", materialize);
    record.tableConfig = config;

    assertTrue(record.isIcebergFormat());
  }

  @Test void testConversionRecordIsNotIcebergFormatWithoutConfig() {
    ConversionRecord record = new ConversionRecord();
    assertFalse(record.isIcebergFormat());
  }

  @Test void testConversionRecordIsNotIcebergFormatNonMapMaterialize() {
    ConversionRecord record = new ConversionRecord();
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("materialize", "not_a_map");
    record.tableConfig = config;

    assertFalse(record.isIcebergFormat());
  }

  @Test void testConversionRecordIsNotIcebergFormatWrongFormat() {
    ConversionRecord record = new ConversionRecord();
    Map<String, Object> config = new HashMap<String, Object>();
    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", "parquet");
    config.put("materialize", materialize);
    record.tableConfig = config;

    assertFalse(record.isIcebergFormat());
  }

  // =========================================================================
  // 10. FileBaseline Tests
  // =========================================================================

  @Test void testFileBaselineHasChangedDifferentEtags() {
    FileBaseline stored = new FileBaseline(1000L, "etag-v1", 100L);
    FileBaseline current = new FileBaseline(1000L, "etag-v2", 100L);

    assertTrue(stored.hasChanged(current), "Different ETags should indicate change");
  }

  @Test void testFileBaselineSameEtags() {
    FileBaseline stored = new FileBaseline(1000L, "etag-same", 100L);
    FileBaseline current = new FileBaseline(2000L, "etag-same", 200L);

    // ETag match takes priority over size/timestamp differences
    assertFalse(stored.hasChanged(current),
        "Same ETags should indicate no change regardless of other fields");
  }

  @Test void testFileBaselineHasChangedFallbackToSize() {
    FileBaseline stored = new FileBaseline(1000L, null, 100L);
    FileBaseline current = new FileBaseline(2000L, null, 100L);

    assertTrue(stored.hasChanged(current), "Different sizes should indicate change");
  }

  @Test void testFileBaselineHasChangedFallbackToTimestamp() {
    FileBaseline stored = new FileBaseline(1000L, null, 100000L);
    FileBaseline current = new FileBaseline(1000L, null, 200000L);

    assertTrue(stored.hasChanged(current),
        "Timestamps differing by more than 1 second should indicate change");
  }

  @Test void testFileBaselineTimestampToleranceOneSecond() {
    FileBaseline stored = new FileBaseline(1000L, null, 100000L);
    FileBaseline current = new FileBaseline(1000L, null, 100500L);

    // 500ms difference is within 1-second tolerance
    assertFalse(stored.hasChanged(current),
        "Timestamps within 1-second tolerance should not indicate change");
  }

  @Test void testFileBaselineNoChangeSameMetadata() {
    FileBaseline stored = new FileBaseline(1000L, "etag-v1", 100L);
    FileBaseline current = new FileBaseline(1000L, "etag-v1", 100L);

    assertFalse(stored.hasChanged(current), "Same metadata should not indicate change");
  }

  @Test void testFileBaselineHasChangedWithNullCurrent() {
    FileBaseline stored = new FileBaseline(1000L, "etag-v1", 100L);
    assertTrue(stored.hasChanged(null), "Null current should indicate change");
  }

  @Test void testFileBaselineAllNullFields() {
    FileBaseline stored = new FileBaseline(null, null, null);
    FileBaseline current = new FileBaseline(null, null, null);

    assertFalse(stored.hasChanged(current),
        "All null fields should not indicate change");
  }

  // =========================================================================
  // 11. PartitionBaseline Tests
  // =========================================================================

  @Test void testPartitionBaselineIsEmptyWithNullFiles() {
    PartitionBaseline baseline = new PartitionBaseline();
    assertTrue(baseline.isEmpty(), "New baseline with null files should be empty");
  }

  @Test void testPartitionBaselineIsEmptyWithEmptyFiles() {
    PartitionBaseline baseline = new PartitionBaseline();
    baseline.files = new HashMap<String, FileBaseline>();
    assertTrue(baseline.isEmpty(), "Baseline with empty files map should be empty");
  }

  @Test void testPartitionBaselineIsNotEmptyWithFiles() {
    PartitionBaseline baseline = new PartitionBaseline();
    baseline.files = new HashMap<String, FileBaseline>();
    baseline.files.put("file1.parquet", new FileBaseline(100L, "etag", 123L));
    assertFalse(baseline.isEmpty(), "Baseline with files should not be empty");
  }

  // =========================================================================
  // 12. Hints (setHint / getHint)
  // =========================================================================

  @Test void testSetHintGetHintRoundTrip() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    metadata.setHint("cik", "0001234567");
    metadata.setHint("form", "10-K");

    assertEquals("0001234567", metadata.getHint("cik"));
    assertEquals("10-K", metadata.getHint("form"));
    assertNull(metadata.getHint("nonexistent"));
  }

  @Test void testSetHintWithNullValueIsIgnored() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    metadata.setHint("key", "value");
    metadata.setHint("key", null);

    // Null value should not overwrite existing value
    assertEquals("value", metadata.getHint("key"));
  }

  @Test void testSetHintOverwritesExistingValue() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    metadata.setHint("key", "old");
    metadata.setHint("key", "new");

    assertEquals("new", metadata.getHint("key"));
  }

  // =========================================================================
  // 13. Edge Cases
  // =========================================================================

  @Test void testEmptyDirectoryNoMetadataFile() {
    // When no metadata file exists, construction should not throw
    ConversionMetadata metadata = new ConversionMetadata(tempDir);
    assertTrue(metadata.getAllConversions().isEmpty());
  }

  @Test void testCorruptedMetadataFileHandledGracefully() throws IOException {
    // Write corrupted JSON to the metadata file
    File metadataFile = new File(tempDir, ".conversions.json");
    try (FileWriter writer = new FileWriter(metadataFile)) {
      writer.write("{ this is not valid json !!!");
    }

    // Construction should not throw - corrupted file should be handled gracefully
    ConversionMetadata metadata = new ConversionMetadata(tempDir);
    // After graceful handling of corruption, conversions map should be empty
    assertTrue(metadata.getAllConversions().isEmpty(),
        "Corrupted metadata should result in empty conversions");
  }

  @Test void testMetadataFileWithEmptyJsonObject() throws IOException {
    File metadataFile = new File(tempDir, ".conversions.json");
    try (FileWriter writer = new FileWriter(metadataFile)) {
      writer.write("{}");
    }

    ConversionMetadata metadata = new ConversionMetadata(tempDir);
    assertTrue(metadata.getAllConversions().isEmpty());
  }

  @Test void testBuildComprehensiveMappingWithNullBaseDirectory() {
    Map<String, String> result =
        ConversionMetadata.buildComprehensiveMapping(null, new HashMap<String, String>());
    assertTrue(result.isEmpty());
  }

  @Test void testBuildComprehensiveMappingWithNonExistentDirectory() {
    File nonExistent = new File(tempDir, "does_not_exist");
    Map<String, String> result =
        ConversionMetadata.buildComprehensiveMapping(nonExistent, new HashMap<String, String>());
    assertTrue(result.isEmpty());
  }

  @Test void testConversionRecordIsLocalFileClassification() {
    // Test the isLocalFile method indirectly through hasChanged
    // Local files use timestamp-based detection
    ConversionRecord localRecord = new ConversionRecord("/local/file.csv", "/conv.json", "DIRECT");
    // Non-existent local file returns false (no change possible)
    assertFalse(localRecord.hasChanged());

    // Remote files always return true (conservative)
    ConversionRecord httpRecord =
        new ConversionRecord("https://example.com/file.csv", "/conv.json", "DIRECT");
    assertTrue(httpRecord.hasChanged());

    ConversionRecord s3Record =
        new ConversionRecord("s3://bucket/file.csv", "/conv.json", "DIRECT");
    assertTrue(s3Record.hasChanged());

    ConversionRecord ftpRecord =
        new ConversionRecord("ftp://server/file.csv", "/conv.json", "DIRECT");
    assertTrue(ftpRecord.hasChanged());

    ConversionRecord sftpRecord =
        new ConversionRecord("sftp://server/file.csv", "/conv.json", "DIRECT");
    assertTrue(sftpRecord.hasChanged());
  }

  @Test void testConversionRecordHasChangedViaMetadataWithNullMetadata() {
    ConversionRecord record = new ConversionRecord("/orig", "/conv", "DIRECT");
    assertTrue(record.hasChangedViaMetadata(null),
        "Null metadata should return true (conservative)");
  }

  @Test void testMultipleOverwritesPreserveLatest() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("overwrite.xlsx");
    File converted = createTempFile("overwrite.json");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");
    metadata.recordConversion(original, converted, "EXCEL_TO_JSON_V2");

    ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertEquals("EXCEL_TO_JSON_V2", record.conversionType,
        "Latest conversion type should be stored");
  }

  @Test void testSaveAndLoadWithSpecialCharacters() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("file with spaces.xlsx");
    File converted = createTempFile("file with spaces.json");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    // Reload and verify
    ConversionMetadata reloaded = new ConversionMetadata(tempDir);
    ConversionRecord record = reloaded.getConversionRecord(converted);
    assertNotNull(record, "Records with special characters in path should persist");
  }

  @Test void testLockFileIsCreatedDuringSave() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("lock_test.xlsx");
    File converted = createTempFile("lock_test.json");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    File lockFile = new File(tempDir, ".conversions.json.lock");
    assertTrue(lockFile.exists(), "Lock file should be created during save operations");
  }

  @Test void testOverwriteConversionDoesNotDuplicate() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original1 = createTempFile("first_orig.xlsx");
    File original2 = createTempFile("second_orig.html");
    File converted = createTempFile("same_converted.json");

    metadata.recordConversion(original1, converted, "EXCEL_TO_JSON");
    int sizeAfterFirst = metadata.getAllConversions().size();

    metadata.recordConversion(original2, converted, "HTML_TO_JSON");
    int sizeAfterSecond = metadata.getAllConversions().size();

    assertEquals(sizeAfterFirst, sizeAfterSecond,
        "Re-recording same converted file key should overwrite, not duplicate");
  }

  // =========================================================================
  // Helper Methods
  // =========================================================================

  private File createTempFile(String name) throws IOException {
    File file = new File(tempDir, name);
    if (!file.exists()) {
      file.createNewFile();
    }
    return file;
  }

  /**
   * Invokes the private static method detectTypeFromTestFile via reflection.
   */
  private static String invokeDetectTypeFromTestFile(String filename) throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("detectTypeFromTestFile", String.class);
    method.setAccessible(true);
    return (String) method.invoke(null, filename);
  }

  /**
   * Invokes the private static method detectTypeFromExtension via reflection.
   */
  private static String invokeDetectTypeFromExtension(String extension) throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("detectTypeFromExtension", String.class);
    method.setAccessible(true);
    return (String) method.invoke(null, extension);
  }

  /**
   * Invokes the private static method generateFileExtensions via reflection.
   */
  @SuppressWarnings("unchecked")
  private static List<Map.Entry<String, String>> invokeGenerateFileExtensions() throws Exception {
    Method method = ConversionMetadata.class.getDeclaredMethod("generateFileExtensions");
    method.setAccessible(true);
    return (List<Map.Entry<String, String>>) method.invoke(null);
  }

  /**
   * Invokes the private static method extractExtension via reflection.
   */
  private static String invokeExtractExtension(String filename) throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("extractExtension", String.class);
    method.setAccessible(true);
    return (String) method.invoke(null, filename);
  }
}
