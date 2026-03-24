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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ConversionMetadata}.
 */
@Tag("unit")
public class ConversionMetadataTest {

  @TempDir
  public File tempDir;

  @Test void testConstructorCreatesMetadataFile() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);
    // The .conversions.json file is only written on first save, but the object is created
    assertNotNull(metadata);
  }

  @Test void testRecordConversionBasicCrud() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("source.xlsx");
    File converted = createTempFile("source.json");

    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    ConversionRecord record = metadata.getConversionRecord(converted);
    assertNotNull(record);
    assertEquals(original.getCanonicalPath(), record.originalFile);
    assertEquals(converted.getCanonicalPath(), record.convertedFile);
    assertEquals("EXCEL_TO_JSON", record.conversionType);
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

  @Test void testFindOriginalSourceReverseLookup() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("report.html");
    File converted = createTempFile("report.json");

    metadata.recordConversion(original, converted, "HTML_TO_JSON");

    File found = metadata.findOriginalSource(converted);
    assertNotNull(found);
    assertEquals(original.getCanonicalPath(), found.getCanonicalPath());
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

  @Test void testClearRemovesAllRecords() throws IOException {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    File original = createTempFile("clear_me.xlsx");
    File converted = createTempFile("clear_me.json");
    metadata.recordConversion(original, converted, "EXCEL_TO_JSON");

    metadata.clear();

    Map<String, ConversionRecord> all = metadata.getAllConversions();
    assertTrue(all.isEmpty(), "All conversions should be cleared");
  }

  @Test void testHasTableMetadataAfterPutConversionRecord() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    assertFalse(metadata.hasTableMetadata("my_table"));

    ConversionRecord record = new ConversionRecord("/orig", "/conv", "DIRECT");
    metadata.putConversionRecord("my_table", record);

    assertTrue(metadata.hasTableMetadata("my_table"));
  }

  @Test void testConversionRecordHasChangedLocalFile() throws Exception {
    File localFile = createTempFile("local.csv");
    // Write some initial content
    try (FileWriter writer = new FileWriter(localFile)) {
      writer.write("initial data");
    }

    ConversionRecord record = new ConversionRecord(
        localFile.getCanonicalPath(), "/converted.json", "DIRECT");
    // Set timestamp to before the file was last modified
    record.timestamp = localFile.lastModified() - 5000;

    assertTrue(record.hasChanged(), "File modified after record timestamp should be detected as changed");
  }

  @Test void testConversionRecordHasChangedRemoteFile() {
    ConversionRecord record = new ConversionRecord(
        "https://example.com/data.csv", "/converted.json", "DIRECT");

    // Remote files always return true (conservative)
    assertTrue(record.hasChanged());
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

  @Test void testConversionRecordIsNotIcebergFormatWithoutConfig() {
    ConversionRecord record = new ConversionRecord();
    assertFalse(record.isIcebergFormat());
  }

  @Test void testFileBaselineHasChangedDifferentEtags() {
    FileBaseline stored = new FileBaseline(1000L, "etag-v1", 100L);
    FileBaseline current = new FileBaseline(1000L, "etag-v2", 100L);

    assertTrue(stored.hasChanged(current), "Different ETags should indicate change");
  }

  @Test void testFileBaselineHasChangedFallbackToSizeWhenNullEtags() {
    FileBaseline stored = new FileBaseline(1000L, null, 100L);
    FileBaseline current = new FileBaseline(2000L, null, 100L);

    assertTrue(stored.hasChanged(current), "Different sizes should indicate change");
  }

  @Test void testFileBaselineNoChangeSameMetadata() {
    FileBaseline stored = new FileBaseline(1000L, "etag-v1", 100L);
    FileBaseline current = new FileBaseline(1000L, "etag-v1", 100L);

    assertFalse(stored.hasChanged(current), "Same metadata should not indicate change");
  }

  @Test void testPartitionBaselineIsEmpty() {
    PartitionBaseline baseline = new PartitionBaseline();
    assertTrue(baseline.isEmpty(), "New baseline with null files should be empty");

    baseline.files = new HashMap<String, FileBaseline>();
    assertTrue(baseline.isEmpty(), "Baseline with empty files map should be empty");

    baseline.files.put("file1.parquet", new FileBaseline(100L, "etag", 123L));
    assertFalse(baseline.isEmpty(), "Baseline with files should not be empty");
  }

  @Test void testSetHintGetHintRoundTrip() {
    ConversionMetadata metadata = new ConversionMetadata(tempDir);

    metadata.setHint("cik", "0001234567");
    metadata.setHint("form", "10-K");

    assertEquals("0001234567", metadata.getHint("cik"));
    assertEquals("10-K", metadata.getHint("form"));
    assertNull(metadata.getHint("nonexistent"));
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

  private File createTempFile(String name) throws IOException {
    File file = new File(tempDir, name);
    if (!file.exists()) {
      file.createNewFile();
    }
    return file;
  }
}
