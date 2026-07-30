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
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.adapter.file.partition.PartitionDetector;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Sources;

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
 * Deep coverage tests for {@link ConversionMetadata} targeting uncovered
 * methods and branches across ConversionRecord, FileBaseline,
 * PartitionBaseline, static utility methods, and file type detection.
 *
 * <p>Focuses on:
 * <ul>
 *   <li>ConversionRecord: isIcebergFormat, hasChanged, hasChangedViaTimestamp,
 *       hasChangedViaMetadata, updateMetadata, isLocalFile</li>
 *   <li>FileBaseline: hasChanged comparison logic (etag, size, timestamp)</li>
 *   <li>PartitionBaseline: isEmpty</li>
 *   <li>Static utilities: detectTypeFromTestFile, detectConvertibleType,
 *       detectDirectType, extractExtension, generateFileExtensions</li>
 *   <li>File type detection for all supported formats</li>
 *   <li>isLocalFile for http, https, s3, ftp, sftp, and local paths</li>
 * </ul>
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

  // ===================================================================
  // isLocalFile -- all branches via hasChanged()
  // ===================================================================


  /**
   * Source type detected for a file name, resolved through the public
   * {@link ConversionMetadata#recordTable} path.
   *
   * <p>The detection itself lives in a private method, so it is reached the way production
   * reaches it: recordTable stores the detected type on the record it keys by table name, and
   * getAllConversions exposes that record. The table is a plain mock on purpose — recordTable
   * special-cases four table class names (ParquetTranslatableTable and friends) and overrides
   * the detected type for them, so a mock keeps the file name as the only input under test.
   */
  private String detectedSourceTypeFor(String fileName) {
    // A distinct table name per call: ConversionMetadata persists to metadataDir and reloads it
    // on construction, and recordTable only fills sourceType on an existing record when it is
    // null. Reusing one name would make the second call in a test read back the first's answer.
    String tableName = "t" + nextTable.getAndIncrement();
    ConversionMetadata metadata = new ConversionMetadata(metadataDir);
    metadata.recordTable(tableName, mock(Table.class),
        Sources.of(new File(metadataDir, fileName)), null);
    return metadata.getAllConversions().get(tableName).sourceType;
  }

  private final java.util.concurrent.atomic.AtomicInteger nextTable =
      new java.util.concurrent.atomic.AtomicInteger();


  /**
   * Whether a set of paths reads as Hive-partitioned, via the public
   * {@link PartitionDetector#detectPartitionScheme} rather than the deleted
   * ConversionMetadata.isHivePartitioned. detectPartitionScheme returns null when it finds no
   * consistent scheme, which is the same answer as "not partitioned".
   */
  private static boolean hivePartitioned(List<String> filePaths) {
    PartitionDetector.PartitionInfo info = PartitionDetector.detectPartitionScheme(filePaths);
    return info != null && info.isHiveStyle();
  }

  @Test void testIsLocalFileWithHttpUrl() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "http://example.com/data.csv";
    // Remote file -> hasChanged returns true (conservative)
    assertTrue(record.hasChanged());
  }

  @Test void testIsLocalFileWithHttpsUrl() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "https://secure.example.com/data.csv";
    assertTrue(record.hasChanged());
  }

  @Test void testIsLocalFileWithS3Url() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "s3://my-bucket/data/file.parquet";
    assertTrue(record.hasChanged());
  }

  @Test void testIsLocalFileWithFtpUrl() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "ftp://files.example.com/data.csv";
    assertTrue(record.hasChanged());
  }

  @Test void testIsLocalFileWithSftpUrl() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "sftp://secure.example.com/data.csv";
    assertTrue(record.hasChanged());
  }

  @Test void testIsLocalFileWithAbsoluteUnixPath() throws IOException {
    File testFile = new File(metadataDir, "local_test.csv");
    testFile.createNewFile();

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = testFile.getAbsolutePath();
    record.timestamp = System.currentTimeMillis() + 10000;
    // Local file with future timestamp -> not changed
    assertFalse(record.hasChanged());
  }

  @Test void testIsLocalFileWithRelativePath() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "relative/path/data.csv";
    record.timestamp = System.currentTimeMillis();
    // Relative path is treated as local; file doesn't exist -> false
    assertFalse(record.hasChanged());
  }

  @Test void testIsLocalFileWithNullPath() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = null;
    // null path -> isLocalFile returns false -> remote code path -> returns true
    assertTrue(record.hasChanged());
  }

  // ===================================================================
  // hasChangedViaTimestamp -- timestamp-based local file change detection
  // ===================================================================

  @Test void testHasChangedViaTimestampFileNotExists() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "/nonexistent/path/to/file.xlsx";
    record.timestamp = System.currentTimeMillis();
    // Non-existent file returns false (no change possible)
    assertFalse(record.hasChanged());
  }

  @Test void testHasChangedViaTimestampFileUnchanged() throws IOException {
    File testFile = new File(metadataDir, "unchanged.xlsx");
    testFile.createNewFile();

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = testFile.getAbsolutePath();
    // Set stored timestamp after the file's last modified time
    record.timestamp = testFile.lastModified() + 5000;
    assertFalse(record.hasChanged());
  }

  @Test void testHasChangedViaTimestampFileChanged() throws Exception {
    File testFile = new File(metadataDir, "changed.xlsx");
    try (FileWriter fw = new FileWriter(testFile)) {
      fw.write("initial content");
    }

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = testFile.getAbsolutePath();
    // Set stored timestamp well before the file was created
    record.timestamp = 1000L;
    assertTrue(record.hasChanged());
  }

  @Test void testHasChangedViaTimestampFileModifiedAfterRecord() throws Exception {
    File testFile = new File(metadataDir, "modified_later.txt");
    try (FileWriter fw = new FileWriter(testFile)) {
      fw.write("original");
    }

    // Record timestamp at the moment the file was last modified
    long fileTime = testFile.lastModified();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = testFile.getAbsolutePath();
    record.timestamp = fileTime - 1; // Before the file's last modified
    // currentTimestamp > stored timestamp -> changed
    assertTrue(record.hasChanged());
  }

  // ===================================================================
  // hasChangedViaMetadata -- StorageProvider metadata comparison
  // ===================================================================

  @Test void testHasChangedViaMetadataNullMetadataReturnsTrue() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertTrue(record.hasChangedViaMetadata(null));
  }

  @Test void testHasChangedViaMetadataEtagMatchNoChange() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = "\"abc123\"";
    record.originalFile = "s3://bucket/file.csv";

    StorageProvider.FileMetadata currentMetadata =
        new StorageProvider.FileMetadata("s3://bucket/file.csv", 1024, 100000, "text/csv", "\"abc123\"");
    assertFalse(record.hasChangedViaMetadata(currentMetadata));
  }

  @Test void testHasChangedViaMetadataEtagMismatchChanged() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = "\"abc123\"";
    record.originalFile = "s3://bucket/file.csv";

    StorageProvider.FileMetadata currentMetadata =
        new StorageProvider.FileMetadata("s3://bucket/file.csv", 1024, 100000, "text/csv", "\"xyz789\"");
    assertTrue(record.hasChangedViaMetadata(currentMetadata));
  }

  @Test void testHasChangedViaMetadataNoEtagFallbackToSizeChanged() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = null;
    record.contentLength = 1000L;
    record.originalFile = "https://example.com/data.csv";

    StorageProvider.FileMetadata currentMetadata =
        new StorageProvider.FileMetadata("https://example.com/data.csv", 2000, 100000, "text/csv", null);
    assertTrue(record.hasChangedViaMetadata(currentMetadata));
  }

  @Test void testHasChangedViaMetadataNoEtagSameSizeFallbackToTimestamp() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = null;
    record.contentLength = 1000L;
    record.timestamp = 50000L;
    record.originalFile = "https://example.com/data.csv";

    // Same size but timestamp changed beyond tolerance
    StorageProvider.FileMetadata currentMetadata =
        new StorageProvider.FileMetadata("https://example.com/data.csv", 1000, 60000, "text/csv", null);
    assertTrue(record.hasChangedViaMetadata(currentMetadata));
  }

  @Test void testHasChangedViaMetadataTimestampWithinOneSec() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = null;
    record.contentLength = null;
    record.timestamp = 100000L;
    record.originalFile = "https://example.com/data.csv";

    // Timestamp difference of 500ms (within 1 second tolerance)
    StorageProvider.FileMetadata currentMetadata =
        new StorageProvider.FileMetadata("https://example.com/data.csv", 0, 100500, "text/csv", null);
    assertFalse(record.hasChangedViaMetadata(currentMetadata));
  }

  @Test void testHasChangedViaMetadataTimestampExactlyOneSecBoundary() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = null;
    record.contentLength = null;
    record.timestamp = 100000L;
    record.originalFile = "https://example.com/data.csv";

    // Exactly 1000ms difference -> NOT changed (tolerance is > 1000)
    StorageProvider.FileMetadata currentMetadata =
        new StorageProvider.FileMetadata("https://example.com/data.csv", 0, 101000, "text/csv", null);
    assertFalse(record.hasChangedViaMetadata(currentMetadata));
  }

  @Test void testHasChangedViaMetadataTimestampJustOverTolerance() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = null;
    record.contentLength = null;
    record.timestamp = 100000L;
    record.originalFile = "https://example.com/data.csv";

    // 1001ms difference -> changed
    StorageProvider.FileMetadata currentMetadata =
        new StorageProvider.FileMetadata("https://example.com/data.csv", 0, 101001, "text/csv", null);
    assertTrue(record.hasChangedViaMetadata(currentMetadata));
  }

  @Test void testHasChangedViaMetadataStoredEtagNullCurrentEtagPresent() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = null;
    record.contentLength = null;
    record.timestamp = 100000L;
    record.originalFile = "s3://bucket/file.csv";

    // Stored etag null, current has etag -> skip etag comparison, fall to timestamp
    StorageProvider.FileMetadata currentMetadata =
        new StorageProvider.FileMetadata("s3://bucket/file.csv", 0, 100000, "text/csv", "\"new-etag\"");
    assertFalse(record.hasChangedViaMetadata(currentMetadata));
  }

  @Test void testHasChangedViaMetadataStoredEtagPresentCurrentEtagNull() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = "\"existing-etag\"";
    record.contentLength = null;
    record.timestamp = 100000L;
    record.originalFile = "s3://bucket/file.csv";

    // Current etag null -> skip etag comparison, fall to timestamp
    StorageProvider.FileMetadata currentMetadata =
        new StorageProvider.FileMetadata("s3://bucket/file.csv", 0, 100000, "text/csv", null);
    assertFalse(record.hasChangedViaMetadata(currentMetadata));
  }

  // ===================================================================
  // updateMetadata -- update record fields from FileMetadata
  // ===================================================================

  @Test void testUpdateMetadataPopulatesAllFields() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "test.csv";

    StorageProvider.FileMetadata fileMetadata =
        new StorageProvider.FileMetadata("test.csv", 5000, 12345L, "text/csv", "new-etag");

    record.updateMetadata(fileMetadata);
    assertEquals("new-etag", record.etag);
    assertEquals(Long.valueOf(5000L), record.contentLength);
    assertEquals("text/csv", record.contentType);
    assertEquals(12345L, record.timestamp);
  }

  @Test void testUpdateMetadataWithNullDoesNotModifyRecord() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.etag = "old-etag";
    record.contentLength = 999L;
    record.contentType = "application/json";
    record.timestamp = 50000L;

    record.updateMetadata(null);
    assertEquals("old-etag", record.etag);
    assertEquals(Long.valueOf(999L), record.contentLength);
    assertEquals("application/json", record.contentType);
    assertEquals(50000L, record.timestamp);
  }

  @Test void testUpdateMetadataOverwritesPreviousValues() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.originalFile = "test.csv";
    record.etag = "old-etag";
    record.contentLength = 100L;
    record.contentType = "text/plain";
    record.timestamp = 1000L;

    StorageProvider.FileMetadata fileMetadata =
        new StorageProvider.FileMetadata("test.csv", 200, 2000L, "text/csv", "new-etag");

    record.updateMetadata(fileMetadata);
    assertEquals("new-etag", record.etag);
    assertEquals(Long.valueOf(200L), record.contentLength);
    assertEquals("text/csv", record.contentType);
    assertEquals(2000L, record.timestamp);
  }

  // ===================================================================
  // isIcebergFormat -- all branches
  // ===================================================================

  @Test void testIsIcebergFormatNullTableConfig() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = null;
    assertFalse(record.isIcebergFormat());
  }

  @Test void testIsIcebergFormatEmptyTableConfig() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<String, Object>();
    assertFalse(record.isIcebergFormat());
  }

  @Test void testIsIcebergFormatMaterializeNotAMap() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<String, Object>();
    record.tableConfig.put("materialize", "string_value");
    assertFalse(record.isIcebergFormat());
  }

  @Test void testIsIcebergFormatMaterializeIsInteger() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableConfig = new HashMap<String, Object>();
    record.tableConfig.put("materialize", 42);
    assertFalse(record.isIcebergFormat());
  }

  @Test void testIsIcebergFormatIcebergLowerCase() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", "iceberg");
    tableConfig.put("materialize", materialize);
    record.tableConfig = tableConfig;
    assertTrue(record.isIcebergFormat());
  }

  @Test void testIsIcebergFormatIcebergUpperCase() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", "ICEBERG");
    tableConfig.put("materialize", materialize);
    record.tableConfig = tableConfig;
    assertTrue(record.isIcebergFormat());
  }

  @Test void testIsIcebergFormatIcebergMixedCase() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", "Iceberg");
    tableConfig.put("materialize", materialize);
    record.tableConfig = tableConfig;
    assertTrue(record.isIcebergFormat());
  }

  @Test void testIsIcebergFormatParquetFormat() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", "parquet");
    tableConfig.put("materialize", materialize);
    record.tableConfig = tableConfig;
    assertFalse(record.isIcebergFormat());
  }

  @Test void testIsIcebergFormatNullFormat() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    Map<String, Object> tableConfig = new HashMap<String, Object>();
    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", null);
    tableConfig.put("materialize", materialize);
    record.tableConfig = tableConfig;
    assertFalse(record.isIcebergFormat());
  }

  // ===================================================================
  // FileBaseline.hasChanged -- all branches
  // ===================================================================

  @Test void testFileBaselineHasChangedNullCurrentReturnsTrue() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, "etag-abc", 12345L);
    assertTrue(fb.hasChanged(null));
  }

  @Test void testFileBaselineHasChangedEtagBothPresentMatch() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, "etag-abc", 12345L);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(9999L, "etag-abc", 99999L);
    // ETags match -> not changed (etag takes priority even if size/timestamp differ)
    assertFalse(fb.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedEtagBothPresentMismatch() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, "etag-abc", 12345L);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(1024L, "etag-xyz", 12345L);
    assertTrue(fb.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedEtagOneNullFallbackToSize() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, "etag-abc", 12345L);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(2048L, null, 12345L);
    // Stored etag non-null but current etag null -> skip etag, fallback to size
    assertTrue(fb.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedBothEtagsNullSizesDiffer() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, null, 12345L);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(2048L, null, 12345L);
    assertTrue(fb.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedBothEtagsNullSameSize() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, null, 12345L);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(1024L, null, 12345L);
    // Same size -> fall to timestamp, within tolerance -> not changed
    assertFalse(fb.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedNoEtagNoSizeTimestampChanged() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(null, null, 10000L);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(null, null, 20000L);
    // 10000ms diff > 1000ms tolerance -> changed
    assertTrue(fb.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedNoEtagNoSizeTimestampWithinTolerance() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(null, null, 10000L);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(null, null, 10500L);
    // 500ms diff <= 1000ms tolerance -> not changed
    assertFalse(fb.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedAllFieldsNull() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(null, null, null);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(null, null, null);
    // No fields to compare -> false
    assertFalse(fb.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedStoredSizeNullCurrentSizePresent() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(null, null, 10000L);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(5000L, null, 10000L);
    // stored size null -> skip size comparison, fall to timestamp (within tolerance)
    assertFalse(fb.hasChanged(current));
  }

  @Test void testFileBaselineHasChangedStoredTimestampNullCurrentPresent() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, null, null);
    ConversionMetadata.FileBaseline current =
        new ConversionMetadata.FileBaseline(1024L, null, 50000L);
    // Same size, timestamp comparison skipped (stored null) -> false
    assertFalse(fb.hasChanged(current));
  }

  // ===================================================================
  // PartitionBaseline.isEmpty -- all branches
  // ===================================================================

  @Test void testPartitionBaselineNullFiles() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = null;
    assertTrue(baseline.isEmpty());
  }

  @Test void testPartitionBaselineEmptyFiles() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<String, ConversionMetadata.FileBaseline>();
    assertTrue(baseline.isEmpty());
  }

  @Test void testPartitionBaselineWithOneFile() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    baseline.files = new HashMap<String, ConversionMetadata.FileBaseline>();
    baseline.files.put("file1.parquet",
        new ConversionMetadata.FileBaseline(100L, "etag1", System.currentTimeMillis()));
    baseline.snapshotTimestamp = System.currentTimeMillis();
    assertFalse(baseline.isEmpty());
  }

  @Test void testPartitionBaselineDefaultConstructorIsEmpty() {
    ConversionMetadata.PartitionBaseline baseline = new ConversionMetadata.PartitionBaseline();
    // Default constructor leaves files null
    assertTrue(baseline.isEmpty());
  }

  // ===================================================================
  // detectConvertibleType -- all file type branches via reflection
  // ===================================================================

  @Test void testDetectConvertibleTypeXlsx() {
    assertEquals("excel", detectedSourceTypeFor("report.xlsx"));
  }

  @Test void testDetectConvertibleTypeXls() {
    assertEquals("excel", detectedSourceTypeFor("legacy.xls"));
  }

  @Test void testDetectConvertibleTypeHtml() {
    assertEquals("html", detectedSourceTypeFor("page.html"));
  }

  @Test void testDetectConvertibleTypeHtm() {
    assertEquals("html", detectedSourceTypeFor("page.htm"));
  }

  @Test void testDetectConvertibleTypeXml() {
    assertEquals("xml", detectedSourceTypeFor("config.xml"));
  }

  @Test void testDetectConvertibleTypeMarkdown() {
    assertEquals("markdown", detectedSourceTypeFor("readme.md"));
  }

  @Test void testDetectConvertibleTypeDocx() {
    assertEquals("docx", detectedSourceTypeFor("document.docx"));
  }

  @Test void testDetectConvertibleTypePptx() {
    assertEquals("pptx", detectedSourceTypeFor("slides.pptx"));
  }

  @Test void testDetectConvertibleTypeUnknown() {
    assertEquals("unknown", detectedSourceTypeFor("file.xyz"));
  }

  @Test void testDetectConvertibleTypeCaseInsensitive() {
    assertEquals("excel", detectedSourceTypeFor("DATA.XLSX"));
    assertEquals("html", detectedSourceTypeFor("PAGE.HTML"));
  }

  // ===================================================================
  // detectDirectType -- all file type branches via reflection
  // ===================================================================

  @Test void testDetectDirectTypeCsv() {
    assertEquals("csv", detectedSourceTypeFor("data.csv"));
  }

  @Test void testDetectDirectTypeTsv() {
    assertEquals("tsv", detectedSourceTypeFor("data.tsv"));
  }

  @Test void testDetectDirectTypeJson() {
    assertEquals("json", detectedSourceTypeFor("data.json"));
  }

  @Test void testDetectDirectTypeParquet() {
    assertEquals("parquet", detectedSourceTypeFor("data.parquet"));
  }

  @Test void testDetectDirectTypeYaml() {
    assertEquals("yaml", detectedSourceTypeFor("config.yaml"));
  }

  @Test void testDetectDirectTypeYml() {
    assertEquals("yaml", detectedSourceTypeFor("config.yml"));
  }

  @Test void testDetectDirectTypeArrow() {
    assertEquals("arrow", detectedSourceTypeFor("data.arrow"));
  }

  @Test void testDetectDirectTypeUnknown() {
    assertEquals("unknown", detectedSourceTypeFor("data.binary"));
  }

  @Test void testDetectDirectTypeCaseInsensitive() {
    assertEquals("csv", detectedSourceTypeFor("DATA.CSV"));
    assertEquals("json", detectedSourceTypeFor("FILE.JSON"));
    assertEquals("parquet", detectedSourceTypeFor("TABLE.PARQUET"));
  }

  // ===================================================================
  // detectTypeFromTestFile -- integration of requiresConversion/isDirectlyUsable
  // ===================================================================

  @Test void testDetectTypeFromTestFileConvertible() {
    assertEquals("excel", detectedSourceTypeFor("test.xlsx"));
    assertEquals("html", detectedSourceTypeFor("test.html"));
  }

  @Test void testDetectTypeFromTestFileDirectlyUsable() {
    assertEquals("csv", detectedSourceTypeFor("test.csv"));
    assertEquals("json", detectedSourceTypeFor("test.json"));
    assertEquals("parquet", detectedSourceTypeFor("test.parquet"));
  }

  @Test void testDetectTypeFromTestFileCompressedCsv() {
    assertEquals("csv", detectedSourceTypeFor("test.csv.gz"));
  }

  @Test void testDetectTypeFromTestFileCompressedJson() {
    assertEquals("json", detectedSourceTypeFor("test.json.gz"));
  }

  @Test void testDetectTypeFromTestFileUnknown() {
    assertEquals("unknown", detectedSourceTypeFor("test.xyz"));
  }












  // ===================================================================
  // isRemoteFile -- via reflection on instance method
  // ===================================================================

  @Test void testIsRemoteFileHttp() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("isRemoteFile", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(cm, "http://example.com/file.csv"));
  }

  @Test void testIsRemoteFileHttps() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("isRemoteFile", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(cm, "https://example.com/file.csv"));
  }

  @Test void testIsRemoteFileS3() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("isRemoteFile", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(cm, "s3://bucket/path/file.parquet"));
  }

  @Test void testIsRemoteFileFtp() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("isRemoteFile", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(cm, "ftp://server/file.csv"));
  }

  @Test void testIsRemoteFileSftp() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("isRemoteFile", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(cm, "sftp://server/file.csv"));
  }

  @Test void testIsRemoteFileLocalPath() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("isRemoteFile", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(cm, "/local/path/file.csv"));
  }

  @Test void testIsRemoteFileNull() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("isRemoteFile", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(cm, (String) null));
  }

  // ===================================================================
  // isGlobPattern -- via reflection
  // ===================================================================

  @Test void testIsGlobPatternWithAsterisk() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("isGlobPattern", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(cm, "s3://bucket/**/*.parquet"));
  }

  @Test void testIsGlobPatternWithQuestionMark() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("isGlobPattern", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(cm, "/path/file?.csv"));
  }

  @Test void testIsGlobPatternWithBrackets() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("isGlobPattern", String.class);
    method.setAccessible(true);
    assertTrue((Boolean) method.invoke(cm, "/path/file[0-9].csv"));
  }

  @Test void testIsGlobPatternPlainPath() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("isGlobPattern", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(cm, "/path/to/plain_file.csv"));
  }

  @Test void testIsGlobPatternNull() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("isGlobPattern", String.class);
    method.setAccessible(true);
    assertFalse((Boolean) method.invoke(cm, (String) null));
  }

  // ===================================================================
  // ConversionRecord constructors and getters
  // ===================================================================

  @Test void testConversionRecordDefaultConstructor() {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    assertNull(record.tableName);
    assertNull(record.sourceFile);
    assertNull(record.conversionType);
    assertNull(record.parquetCacheFile);
    assertNull(record.etag);
    assertNull(record.contentLength);
    assertNull(record.contentType);
  }

  @Test void testConversionRecordThreeArgConstructor() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/path/original", "/path/converted", "EXCEL_TO_JSON");
    assertEquals("/path/original", record.getOriginalPath());
    assertEquals("EXCEL_TO_JSON", record.getConversionType());
    assertEquals("/path/converted", record.getConvertedFile());
    assertTrue(record.timestamp > 0);
  }

  @Test void testConversionRecordFourArgConstructor() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("/original", "/converted", "HTML_TO_JSON", "/cache.parquet");
    assertEquals("/original", record.getOriginalPath());
    assertEquals("/cache.parquet", record.getParquetCacheFile());
    assertEquals("/converted", record.getConvertedFile());
  }

  @Test void testConversionRecordHttpMetadataConstructor() {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("http://source",
            "/converted", "HTTP_DOWNLOAD", "/cache.parquet",
            "etag-123", 2048L, "application/json");
    assertEquals("etag-123", record.etag);
    assertEquals(Long.valueOf(2048L), record.contentLength);
    assertEquals("application/json", record.contentType);
    assertEquals("http://source", record.originalFile);
  }

  @Test void testConversionRecordComprehensiveConstructor() {
    Map<String, Object> tableConfig = new HashMap<String, Object>();
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
    assertEquals("etag-abc", record.etag);
    assertEquals(Long.valueOf(4096L), record.contentLength);
    assertEquals("text/csv", record.contentType);
    assertEquals(tableConfig, record.tableConfig);
    assertTrue(record.timestamp > 0);
  }

  // ===================================================================
  // FileBaseline constructors
  // ===================================================================

  @Test void testFileBaselineDefaultConstructor() {
    ConversionMetadata.FileBaseline fb = new ConversionMetadata.FileBaseline();
    assertNull(fb.size);
    assertNull(fb.etag);
    assertNull(fb.lastModified);
  }

  @Test void testFileBaselineParameterizedConstructor() {
    ConversionMetadata.FileBaseline fb =
        new ConversionMetadata.FileBaseline(1024L, "etag-abc", 12345L);
    assertEquals(Long.valueOf(1024L), fb.size);
    assertEquals("etag-abc", fb.etag);
    assertEquals(Long.valueOf(12345L), fb.lastModified);
  }

  // ===================================================================
  // ConversionMetadata constructor and basic operations
  // ===================================================================

  @Test void testConstructorWithLocalDirectory() {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    assertNotNull(cm);
    assertNotNull(cm.getAllConversions());
    assertTrue(cm.getAllConversions().isEmpty());
  }

  @Test void testConstructorWithStringLocalPath() {
    ConversionMetadata cm = new ConversionMetadata(metadataDir.getAbsolutePath());
    assertNotNull(cm);
  }

  @Test void testConstructorWithS3Path() {
    ConversionMetadata cm = new ConversionMetadata("s3://my-bucket/my-prefix");
    assertNotNull(cm);
  }

  @Test void testConstructorWithHttpsPath() {
    ConversionMetadata cm = new ConversionMetadata("https://example.com/data");
    assertNotNull(cm);
  }

  // ===================================================================
  // Hint accessors
  // ===================================================================

  @Test void testSetAndGetHint() {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    assertNull(cm.getHint("cik"));
    cm.setHint("cik", "0001234567");
    assertEquals("0001234567", cm.getHint("cik"));
  }

  @Test void testSetHintNullValueIgnored() {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    cm.setHint("key", null);
    assertNull(cm.getHint("key"));
  }

  @Test void testSetHintMultipleValues() {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    cm.setHint("cik", "0001234567");
    cm.setHint("form", "10-K");
    cm.setHint("filingDate", "2024-01-15");
    assertEquals("0001234567", cm.getHint("cik"));
    assertEquals("10-K", cm.getHint("form"));
    assertEquals("2024-01-15", cm.getHint("filingDate"));
  }

  // ===================================================================
  // recordConversion variants and persistence
  // ===================================================================

  @Test void testRecordConversionTwoArgs() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File original = new File(metadataDir, "source.xlsx");
    original.createNewFile();
    File converted = new File(metadataDir, "source.json");
    converted.createNewFile();

    cm.recordConversion(original, converted, "EXCEL_TO_JSON");

    Map<String, ConversionMetadata.ConversionRecord> records = cm.getAllConversions();
    assertFalse(records.isEmpty());
  }

  @Test void testRecordConversionWithParquetCache() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File original = new File(metadataDir, "data.html");
    original.createNewFile();
    File converted = new File(metadataDir, "data.json");
    converted.createNewFile();
    File parquetCache = new File(metadataDir, "data.parquet");
    parquetCache.createNewFile();

    cm.recordConversion(original, converted, "HTML_TO_JSON", parquetCache);
    assertFalse(cm.getAllConversions().isEmpty());
  }

  @Test void testRecordConversionWithPreBuiltRecord() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File converted = new File(metadataDir, "result.json");
    converted.createNewFile();

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord("http://example.com/data",
            converted.getAbsolutePath(), "HTTP_DOWNLOAD", null,
            "etag123", 1024L, "application/json");

    cm.recordConversion(converted, record);
    assertFalse(cm.getAllConversions().isEmpty());
  }

  // ===================================================================
  // findOriginalSource
  // ===================================================================

  @Test void testFindOriginalSourceNotFound() {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File jsonFile = new File(metadataDir, "unknown.json");
    assertNull(cm.findOriginalSource(jsonFile));
  }

  @Test void testFindOriginalSourceFound() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File original = new File(metadataDir, "original.xlsx");
    original.createNewFile();
    File converted = new File(metadataDir, "original.json");
    converted.createNewFile();

    cm.recordConversion(original, converted, "EXCEL_TO_JSON");

    File result = cm.findOriginalSource(converted);
    assertNotNull(result);
    assertEquals(original.getCanonicalPath(), result.getCanonicalPath());
  }

  @Test void testFindOriginalSourceDeletedOriginal() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File original = new File(metadataDir, "will_delete.xlsx");
    original.createNewFile();
    File converted = new File(metadataDir, "will_delete.json");
    converted.createNewFile();

    cm.recordConversion(original, converted, "EXCEL_TO_JSON");
    assertTrue(original.delete());

    // Original deleted -> returns null and cleans up record
    assertNull(cm.findOriginalSource(converted));
  }

  // ===================================================================
  // reload
  // ===================================================================

  @Test void testReloadRestoresRecordsFromDisk() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File original = new File(metadataDir, "before.xlsx");
    original.createNewFile();
    File converted = new File(metadataDir, "before.json");
    converted.createNewFile();

    cm.recordConversion(original, converted, "EXCEL_TO_JSON");
    int sizeBefore = cm.getAllConversions().size();

    cm.reload();
    assertEquals(sizeBefore, cm.getAllConversions().size());
  }

  // ===================================================================
  // clear
  // ===================================================================

  @Test void testClearRemovesAllRecords() throws IOException {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    File original = new File(metadataDir, "clear_orig.csv");
    original.createNewFile();
    File converted = new File(metadataDir, "clear_conv.json");
    converted.createNewFile();

    cm.recordConversion(original, converted, "TEST");
    assertFalse(cm.getAllConversions().isEmpty());

    cm.clear();
    assertTrue(cm.getAllConversions().isEmpty());
  }


  @Test void testIsHivePartitionedEmpty() {
    assertFalse(hivePartitioned(Collections.emptyList()));
  }

  @Test void testIsHivePartitionedSingleFile() {
    List<String> singleFile = Collections.singletonList("s3://bucket/year=2020/file.parquet");
    // The deleted isHivePartitioned required two or more paths before calling a layout
    // partitioned. PartitionDetector carries no such rule: one hive-style path is enough,
    // since the columns it declares are unambiguous on their own.
    assertTrue(hivePartitioned(singleFile));
  }

  @Test void testIsHivePartitionedNonPartitioned() {
    List<String> files = new ArrayList<String>();
    files.add("/data/file1.parquet");
    files.add("/data/file2.parquet");
    assertFalse(hivePartitioned(files));
  }

  @Test void testIsHivePartitionedTrue() {
    List<String> files = new ArrayList<String>();
    files.add("s3://bucket/table/year=2020/file1.parquet");
    files.add("s3://bucket/table/year=2021/file2.parquet");
    assertTrue(hivePartitioned(files));
  }







  // ===================================================================
  // formatRecord -- via reflection
  // ===================================================================

  @Test void testFormatRecordNull() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method =
        ConversionMetadata.class.getDeclaredMethod("formatRecord", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);
    assertEquals("null", method.invoke(cm, (ConversionMetadata.ConversionRecord) null));
  }

  @Test void testFormatRecordNormal() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method =
        ConversionMetadata.class.getDeclaredMethod("formatRecord", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "test_table";
    record.conversionType = "DIRECT";
    String formatted = (String) method.invoke(cm, record);
    assertTrue(formatted.contains("test_table"));
    assertTrue(formatted.contains("DIRECT"));
  }

  @Test void testFormatRecordTruncatesLongParquetCache() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method =
        ConversionMetadata.class.getDeclaredMethod("formatRecord", ConversionMetadata.ConversionRecord.class);
    method.setAccessible(true);

    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.tableName = "test";
    StringBuilder longPath = new StringBuilder();
    for (int i = 0; i < 300; i++) {
      longPath.append("x");
    }
    record.parquetCacheFile = longPath.toString();
    String truncated = (String) method.invoke(cm, record);
    assertTrue(truncated.contains("..."));
  }

  // ===================================================================
  // detectSourceType -- via reflection on instance method
  // ===================================================================

  @Test void testDetectSourceTypeNull() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("detectSourceType", String.class);
    method.setAccessible(true);
    assertEquals("unknown", method.invoke(cm, (String) null));
  }

  @Test void testDetectSourceTypeCsv() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("detectSourceType", String.class);
    method.setAccessible(true);
    assertEquals("csv", method.invoke(cm, "/path/to/file.csv"));
  }

  @Test void testDetectSourceTypeExcel() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("detectSourceType", String.class);
    method.setAccessible(true);
    assertEquals("excel", method.invoke(cm, "/path/to/file.xlsx"));
  }

  @Test void testDetectSourceTypeUnknown() throws Exception {
    ConversionMetadata cm = new ConversionMetadata(metadataDir);
    Method method = ConversionMetadata.class.getDeclaredMethod("detectSourceType", String.class);
    method.setAccessible(true);
    assertEquals("unknown", method.invoke(cm, "/path/to/file.xyz"));
  }
}
