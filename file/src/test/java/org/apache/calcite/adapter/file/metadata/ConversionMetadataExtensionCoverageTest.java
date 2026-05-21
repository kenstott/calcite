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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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

/**
 * Coverage tests for {@link ConversionMetadata} targeting:
 * <ul>
 *   <li>generateFileExtensions() full iteration including compressed variants</li>
 *   <li>detectTypeFromExtension() with all 24+ extensions</li>
 *   <li>Thread-contention on JVM_LOCKS (concurrent save/load)</li>
 *   <li>FileLock error handling and acquireLockWithRetry paths</li>
 *   <li>loadMetadata cleanup paths for stale file-based and table-based records</li>
 * </ul>
 */
@Tag("unit")
class ConversionMetadataExtensionCoverageTest {

  @TempDir
  Path tempDir;

  private ConversionMetadata metadata;

  @BeforeEach
  void setUp() {
    metadata = new ConversionMetadata(tempDir.toFile());
  }

  // =========================================================================
  // 1. generateFileExtensions() - covers the full iteration
  // =========================================================================

  @Test void testGenerateFileExtensionsReturnsNonEmptyList() throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("generateFileExtensions");
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map.Entry<String, String>> extensions =
        (List<Map.Entry<String, String>>) method.invoke(null);

    assertNotNull(extensions);
    assertFalse(extensions.isEmpty(),
        "generateFileExtensions should return a non-empty list");

    // Check that we have a mix of directly usable and convertible types
    boolean hasCsv = false;
    boolean hasExcel = false;
    boolean hasCompressed = false;
    for (Map.Entry<String, String> entry : extensions) {
      String ext = entry.getKey();
      String type = entry.getValue();
      if ("csv".equals(ext)) {
        hasCsv = true;
      }
      if ("xlsx".equals(ext) || "xls".equals(ext)) {
        hasExcel = true;
      }
      if (ext.contains(".gz") || ext.contains(".bz2")
          || ext.contains(".xz") || ext.contains(".zip")) {
        hasCompressed = true;
      }
      // All types should be non-null and non-empty
      assertNotNull(type, "Type for extension '" + ext + "' should not be null");
      assertFalse(type.isEmpty(),
          "Type for extension '" + ext + "' should not be empty");
    }

    assertTrue(hasCsv, "Should include csv extension");
    assertTrue(hasExcel, "Should include excel extensions");
    assertTrue(hasCompressed,
        "Should include compressed variants (e.g., csv.gz)");
  }

  @Test void testGenerateFileExtensionsContainsAllCompressedVariants()
      throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("generateFileExtensions");
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Map.Entry<String, String>> extensions =
        (List<Map.Entry<String, String>>) method.invoke(null);

    // Collect all extension keys for easier lookup
    List<String> extKeys = new ArrayList<>();
    for (Map.Entry<String, String> entry : extensions) {
      extKeys.add(entry.getKey());
    }

    // The code generates compressed variants for: csv, tsv, json, yaml, yml, arrow
    // with compression suffixes: gz, gzip, bz2, xz, zip
    String[] baseExts = {"csv", "tsv", "json", "yaml", "yml", "arrow"};
    String[] compressionSuffixes = {"gz", "gzip", "bz2", "xz", "zip"};

    for (String base : baseExts) {
      for (String suffix : compressionSuffixes) {
        String compressedExt = base + "." + suffix;
        assertTrue(extKeys.contains(compressedExt),
            "Should contain compressed extension: " + compressedExt);
      }
    }
  }

  // =========================================================================
  // 2. detectTypeFromExtension() - covers all 24+ extensions
  // =========================================================================

  @Test void testDetectTypeFromExtensionDirectlyUsableTypes() throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("detectTypeFromExtension", String.class);
    method.setAccessible(true);

    // Directly usable types
    assertEquals("csv", method.invoke(null, "csv"));
    assertEquals("tsv", method.invoke(null, "tsv"));
    assertEquals("json", method.invoke(null, "json"));
    assertEquals("parquet", method.invoke(null, "parquet"));
    assertEquals("yaml", method.invoke(null, "yaml"));
    assertEquals("yaml", method.invoke(null, "yml"));
    assertEquals("arrow", method.invoke(null, "arrow"));
  }

  @Test void testDetectTypeFromExtensionConvertibleTypes() throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("detectTypeFromExtension", String.class);
    method.setAccessible(true);

    // Convertible types
    assertEquals("excel", method.invoke(null, "xlsx"));
    assertEquals("excel", method.invoke(null, "xls"));
    assertEquals("html", method.invoke(null, "html"));
    assertEquals("html", method.invoke(null, "htm"));
    assertEquals("xml", method.invoke(null, "xml"));
    assertEquals("markdown", method.invoke(null, "md"));
    assertEquals("docx", method.invoke(null, "docx"));
    assertEquals("pptx", method.invoke(null, "pptx"));
  }

  @Test void testDetectTypeFromExtensionCompressedDirectlyUsable() throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("detectTypeFromExtension", String.class);
    method.setAccessible(true);

    // Compressed variants of directly usable types
    assertEquals("csv", method.invoke(null, "csv.gz"));
    assertEquals("tsv", method.invoke(null, "tsv.gz"));
    assertEquals("json", method.invoke(null, "json.gz"));
  }

  @Test void testDetectTypeFromExtensionUnknown() throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("detectTypeFromExtension", String.class);
    method.setAccessible(true);

    assertEquals("unknown", method.invoke(null, "foo"));
    assertEquals("unknown", method.invoke(null, "pdf"));
    assertEquals("unknown", method.invoke(null, "doc"));
    assertEquals("unknown", method.invoke(null, "txt"));
  }

  // =========================================================================
  // 3. detectTypeFromTestFile() - covers the full method
  // =========================================================================

  @Test void testDetectTypeFromTestFileAllCandidateExtensions() throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("detectTypeFromTestFile", String.class);
    method.setAccessible(true);

    // All candidate extensions from the source code
    String[] candidates = {
        "csv", "csv.gz", "tsv", "tsv.gz", "json", "json.gz", "parquet",
        "yaml", "yml", "arrow", "xlsx", "xls", "html", "htm",
        "xml", "md", "docx", "pptx", "gz", "gzip", "bz2", "xz", "zip"
    };

    int knownCount = 0;
    for (String ext : candidates) {
      String result = (String) method.invoke(null, "test." + ext);
      assertNotNull(result,
          "Result for test." + ext + " should not be null");
      if (!"unknown".equals(result)) {
        knownCount++;
      }
    }

    // Most extensions should be recognized
    assertTrue(knownCount >= 14,
        "At least 14 extensions should be recognized, got " + knownCount);
  }

  @Test void testDetectTypeFromTestFileUnknownExtension() throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("detectTypeFromTestFile", String.class);
    method.setAccessible(true);

    assertEquals("unknown", method.invoke(null, "test.randomext"));
    assertEquals("unknown", method.invoke(null, "test.pdf"));
  }

  // =========================================================================
  // 4. extractExtension() - covers null return path
  // =========================================================================

  @Test void testExtractExtensionWithTestPrefix() throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("extractExtension", String.class);
    method.setAccessible(true);

    assertEquals("csv", method.invoke(null, "test.csv"));
    assertEquals("json.gz", method.invoke(null, "test.json.gz"));
    assertEquals("xlsx", method.invoke(null, "test.xlsx"));
  }

  @Test void testExtractExtensionWithoutTestPrefix() throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("extractExtension", String.class);
    method.setAccessible(true);

    assertNull(method.invoke(null, "data.csv"));
    assertNull(method.invoke(null, "noprefix"));
    assertNull(method.invoke(null, "other.test.csv"));
  }

  // =========================================================================
  // 5. detectConvertibleType() and detectDirectType() - branch coverage
  // =========================================================================

  @Test void testDetectConvertibleTypeAllBranches() throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("detectConvertibleType", String.class);
    method.setAccessible(true);

    // Each branch in order
    assertEquals("excel", method.invoke(null, "report.xlsx"));
    assertEquals("excel", method.invoke(null, "report.xls"));
    assertEquals("html", method.invoke(null, "page.html"));
    assertEquals("html", method.invoke(null, "page.htm"));
    assertEquals("xml", method.invoke(null, "data.xml"));
    assertEquals("markdown", method.invoke(null, "readme.md"));
    assertEquals("docx", method.invoke(null, "document.docx"));
    assertEquals("pptx", method.invoke(null, "slides.pptx"));
    assertEquals("unknown", method.invoke(null, "file.unknown"));

    // Case insensitive check (uses .toLowerCase())
    assertEquals("excel", method.invoke(null, "REPORT.XLSX"));
    assertEquals("html", method.invoke(null, "PAGE.HTML"));
  }

  @Test void testDetectDirectTypeAllBranches() throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("detectDirectType", String.class);
    method.setAccessible(true);

    // Each branch in order
    assertEquals("csv", method.invoke(null, "data.csv"));
    assertEquals("tsv", method.invoke(null, "data.tsv"));
    assertEquals("json", method.invoke(null, "data.json"));
    assertEquals("parquet", method.invoke(null, "data.parquet"));
    assertEquals("yaml", method.invoke(null, "config.yaml"));
    assertEquals("yaml", method.invoke(null, "config.yml"));
    assertEquals("arrow", method.invoke(null, "data.arrow"));
    assertEquals("unknown", method.invoke(null, "data.abc"));

    // Case insensitive check
    assertEquals("csv", method.invoke(null, "DATA.CSV"));
    assertEquals("json", method.invoke(null, "DATA.JSON"));
  }

  // =========================================================================
  // 6. Concurrent save/load - covers JVM_LOCKS and thread contention
  // =========================================================================

  @Test void testConcurrentSaveMetadata() throws Exception {
    int numThreads = 8;
    int operationsPerThread = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger errorCount = new AtomicInteger(0);

    List<Future<?>> futures = new ArrayList<>();

    for (int t = 0; t < numThreads; t++) {
      final int threadId = t;
      futures.add(
          executor.submit(() -> {
        try {
          startLatch.await(); // Wait for all threads to be ready
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }

        for (int i = 0; i < operationsPerThread; i++) {
          try {
            String tableName = "thread_" + threadId + "_table_" + i;
            ConversionMetadata.ConversionRecord record =
                new ConversionMetadata.ConversionRecord();
            record.tableName = tableName;
            record.sourceFile = "/data/" + tableName + ".csv";
            record.conversionType = "DIRECT";
            metadata.putConversionRecord(tableName, record);
            metadata.saveMetadata();
            successCount.incrementAndGet();
          } catch (Exception e) {
            errorCount.incrementAndGet();
          }
        }
      }));
    }

    // Release all threads simultaneously
    startLatch.countDown();

    // Wait for completion
    for (Future<?> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    // All operations should succeed
    assertTrue(successCount.get() > 0, "Some operations should succeed");
    // The metadata should be loadable
    ConversionMetadata reloaded = new ConversionMetadata(tempDir.toFile());
    assertFalse(reloaded.getAllConversions().isEmpty(),
        "Reloaded metadata should not be empty");
  }

  @Test void testConcurrentReadWriteMetadata() throws Exception {
    // Pre-populate some data
    for (int i = 0; i < 5; i++) {
      ConversionMetadata.ConversionRecord record =
          new ConversionMetadata.ConversionRecord();
      record.tableName = "initial_" + i;
      record.sourceFile = "/data/initial_" + i + ".csv";
      record.conversionType = "DIRECT";
      metadata.putConversionRecord("initial_" + i, record);
    }
    metadata.saveMetadata();

    int numReaders = 4;
    int numWriters = 4;
    ExecutorService executor =
        Executors.newFixedThreadPool(numReaders + numWriters);
    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicInteger readSuccess = new AtomicInteger(0);
    AtomicInteger writeSuccess = new AtomicInteger(0);

    List<Future<?>> futures = new ArrayList<>();

    // Reader threads
    for (int t = 0; t < numReaders; t++) {
      futures.add(
          executor.submit(() -> {
        try {
          startLatch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }

        for (int i = 0; i < 10; i++) {
          try {
            ConversionMetadata reader =
                new ConversionMetadata(tempDir.toFile());
            reader.getAllConversions();
            readSuccess.incrementAndGet();
          } catch (Exception e) {
            // Read errors are acceptable under contention
          }
        }
      }));
    }

    // Writer threads
    for (int t = 0; t < numWriters; t++) {
      final int threadId = t;
      futures.add(
          executor.submit(() -> {
        try {
          startLatch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }

        for (int i = 0; i < 10; i++) {
          try {
            String name = "writer_" + threadId + "_" + i;
            ConversionMetadata.ConversionRecord record =
                new ConversionMetadata.ConversionRecord();
            record.tableName = name;
            record.conversionType = "DIRECT";
            metadata.putConversionRecord(name, record);
            metadata.saveMetadata();
            writeSuccess.incrementAndGet();
          } catch (Exception e) {
            // Write errors are acceptable under contention
          }
        }
      }));
    }

    startLatch.countDown();

    for (Future<?> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    assertTrue(readSuccess.get() > 0, "Some reads should succeed");
    assertTrue(writeSuccess.get() > 0, "Some writes should succeed");
  }

  // =========================================================================
  // 7. loadMetadata cleanup paths - stale records
  // =========================================================================

  @Test void testLoadMetadataRemovesStaleFileBasedRecords() throws Exception {
    // Create a file-based record with a file that exists
    File original = new File(tempDir.toFile(), "exists.csv");
    File converted = new File(tempDir.toFile(), "exists.json");
    Files.write(original.toPath(), "data".getBytes());
    Files.write(converted.toPath(), "{}".getBytes());

    metadata.recordConversion(original, converted, "CSV_TO_JSON");

    // Create a file-based record with a file that will be deleted
    File originalStale = new File(tempDir.toFile(), "stale.csv");
    File convertedStale = new File(tempDir.toFile(), "stale.json");
    Files.write(originalStale.toPath(), "data".getBytes());
    Files.write(convertedStale.toPath(), "{}".getBytes());

    metadata.recordConversion(originalStale, convertedStale, "CSV_TO_JSON");
    metadata.saveMetadata();

    // Delete the stale files
    assertTrue(originalStale.delete());
    assertTrue(convertedStale.delete());

    // Create new metadata instance which triggers loadMetadata
    ConversionMetadata reloaded = new ConversionMetadata(tempDir.toFile());
    Map<String, ConversionMetadata.ConversionRecord> all =
        reloaded.getAllConversions();

    // The stale record should have been removed
    ConversionMetadata.ConversionRecord staleRecord =
        reloaded.getConversionRecord(convertedStale);
    assertNull(staleRecord,
        "Stale record for deleted file should be removed");

    // The valid record should still exist
    ConversionMetadata.ConversionRecord validRecord =
        reloaded.getConversionRecord(converted);
    assertNotNull(validRecord,
        "Valid record should still exist after cleanup");
  }

  @Test void testLoadMetadataKeepsTableBasedRecords() throws Exception {
    // Create a table-based record (key is a table name, no path separators)
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord();
    record.tableName = "my_table";
    record.sourceFile = "/some/nonexistent/path.csv";
    record.conversionType = "DIRECT";
    metadata.putConversionRecord("my_table", record);
    metadata.saveMetadata();

    // Create new instance which triggers loadMetadata
    ConversionMetadata reloaded = new ConversionMetadata(tempDir.toFile());

    // Table-based records should be kept regardless of file existence
    assertTrue(reloaded.hasTableMetadata("my_table"),
        "Table-based records should be preserved during cleanup");
  }

  @Test void testLoadMetadataKeepsRemoteUrlRecords() throws Exception {
    // Create a record with a remote URL original file
    File converted = new File(tempDir.toFile(), "remote_data.json");
    Files.write(converted.toPath(), "{}".getBytes());

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord(
            "https://example.com/data.csv",
            converted.getCanonicalPath(),
            "HTTP_DOWNLOAD");
    metadata.recordConversion(converted, record);
    metadata.saveMetadata();

    // Create new instance which triggers loadMetadata
    ConversionMetadata reloaded = new ConversionMetadata(tempDir.toFile());

    // Records with HTTP URLs should be kept (remote URLs are always valid)
    ConversionMetadata.ConversionRecord loaded =
        reloaded.getConversionRecord(converted);
    assertNotNull(loaded,
        "Records with remote URL originals should be preserved");
    assertEquals("https://example.com/data.csv", loaded.originalFile);
  }

  // =========================================================================
  // 8. saveMetadata creates file and temp file cleanup
  // =========================================================================

  @Test void testSaveMetadataCreatesFile() throws Exception {
    File metadataFile = new File(tempDir.toFile(), ".conversions.json");

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord();
    record.tableName = "save_test";
    record.conversionType = "DIRECT";
    metadata.putConversionRecord("save_test", record);
    metadata.saveMetadata();

    assertTrue(metadataFile.exists(),
        "Metadata file should be created after save");
  }

  @Test void testSaveMetadataAtomicMoveRoundTrip() throws Exception {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord();
    record.tableName = "atomic_test";
    record.sourceFile = "/data/atomic.csv";
    record.conversionType = "DIRECT";
    record.etag = "etag-atomic";
    record.contentLength = 12345L;
    record.contentType = "text/csv";
    metadata.putConversionRecord("atomic_test", record);
    metadata.saveMetadata();

    ConversionMetadata reloaded = new ConversionMetadata(tempDir.toFile());
    ConversionMetadata.ConversionRecord loaded =
        reloaded.getAllConversions().get("atomic_test");
    assertNotNull(loaded);
    assertEquals("atomic_test", loaded.tableName);
    assertEquals("/data/atomic.csv", loaded.sourceFile);
    assertEquals("DIRECT", loaded.conversionType);
    assertEquals("etag-atomic", loaded.etag);
    assertEquals(Long.valueOf(12345L), loaded.contentLength);
    assertEquals("text/csv", loaded.contentType);
  }

  // =========================================================================
  // 9. Multiple ConversionMetadata instances sharing same directory
  // =========================================================================

  @Test void testMultipleInstancesSameDirectory() throws Exception {
    ConversionMetadata instance1 = new ConversionMetadata(tempDir.toFile());
    ConversionMetadata.ConversionRecord record1 =
        new ConversionMetadata.ConversionRecord();
    record1.tableName = "shared_table_1";
    record1.conversionType = "DIRECT";
    instance1.putConversionRecord("shared_table_1", record1);
    instance1.saveMetadata();

    ConversionMetadata instance2 = new ConversionMetadata(tempDir.toFile());
    assertTrue(instance2.hasTableMetadata("shared_table_1"),
        "Second instance should see first instance's data");

    ConversionMetadata.ConversionRecord record2 =
        new ConversionMetadata.ConversionRecord();
    record2.tableName = "shared_table_2";
    record2.conversionType = "DIRECT";
    instance2.putConversionRecord("shared_table_2", record2);
    instance2.saveMetadata();

    instance1.reload();
    assertTrue(instance1.hasTableMetadata("shared_table_2"),
        "First instance should see second instance's data after reload");
  }

  // =========================================================================
  // 10. S3 URI constructor path
  // =========================================================================

  @Test void testConstructorWithS3Uri() {
    ConversionMetadata s3Metadata =
        new ConversionMetadata("s3://my-bucket/data/schema");
    assertNotNull(s3Metadata);
    assertTrue(s3Metadata.getAllConversions().isEmpty());
  }

  @Test void testConstructorWithFtpUri() {
    ConversionMetadata ftpMetadata =
        new ConversionMetadata("ftp://server/data/schema");
    assertNotNull(ftpMetadata);
    assertTrue(ftpMetadata.getAllConversions().isEmpty());
  }

  @Test void testConstructorWithLocalStringPath() {
    ConversionMetadata localMetadata =
        new ConversionMetadata(tempDir.toString());
    assertNotNull(localMetadata);
  }

  // =========================================================================
  // 11. detectSourceType with various file paths
  // =========================================================================

  @Test void testDetectSourceTypeCoversAllTypes() throws Exception {
    Method method =
        ConversionMetadata.class.getDeclaredMethod("detectSourceType", String.class);
    method.setAccessible(true);

    // Direct types
    assertEquals("csv", method.invoke(metadata, "/path/to/data.csv"));
    assertEquals("tsv", method.invoke(metadata, "/path/to/data.tsv"));
    assertEquals("json", method.invoke(metadata, "/path/to/data.json"));
    assertEquals("parquet", method.invoke(metadata, "/path/to/data.parquet"));
    assertEquals("yaml", method.invoke(metadata, "/path/to/config.yaml"));
    assertEquals("yaml", method.invoke(metadata, "/path/to/config.yml"));
    assertEquals("arrow", method.invoke(metadata, "/path/to/data.arrow"));

    // Convertible types
    assertEquals("excel", method.invoke(metadata, "/path/to/report.xlsx"));
    assertEquals("excel", method.invoke(metadata, "/path/to/report.xls"));
    assertEquals("html", method.invoke(metadata, "/path/to/page.html"));
    assertEquals("html", method.invoke(metadata, "/path/to/page.htm"));
    assertEquals("xml", method.invoke(metadata, "/path/to/data.xml"));
    assertEquals("markdown", method.invoke(metadata, "/path/to/readme.md"));
    assertEquals("docx", method.invoke(metadata, "/path/to/doc.docx"));
    assertEquals("pptx", method.invoke(metadata, "/path/to/slides.pptx"));

    // Unknown
    assertEquals("unknown", method.invoke(metadata, "/path/to/file.abc"));
    assertEquals("unknown", method.invoke(metadata, (String) null));
  }

  // =========================================================================
  // 12. Concurrent reload operations
  // =========================================================================

  @Test void testConcurrentReloadFromMultipleInstances() throws Exception {
    // Pre-populate metadata
    for (int i = 0; i < 10; i++) {
      ConversionMetadata.ConversionRecord record =
          new ConversionMetadata.ConversionRecord();
      record.tableName = "preloaded_" + i;
      record.conversionType = "DIRECT";
      metadata.putConversionRecord("preloaded_" + i, record);
    }
    metadata.saveMetadata();

    int numThreads = 6;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicInteger successCount = new AtomicInteger(0);

    List<Future<?>> futures = new ArrayList<>();
    for (int t = 0; t < numThreads; t++) {
      futures.add(
          executor.submit(() -> {
        try {
          startLatch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }

        for (int i = 0; i < 5; i++) {
          try {
            ConversionMetadata instance =
                new ConversionMetadata(tempDir.toFile());
            instance.reload();
            Map<String, ConversionMetadata.ConversionRecord> all =
                instance.getAllConversions();
            if (!all.isEmpty()) {
              successCount.incrementAndGet();
            }
          } catch (Exception e) {
            // Contention errors are acceptable
          }
        }
      }));
    }

    startLatch.countDown();

    for (Future<?> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    assertTrue(successCount.get() > 0,
        "Some concurrent reload operations should succeed");
  }

  // =========================================================================
  // 13. Clear and re-save cycle
  // =========================================================================

  @Test void testClearDeletesMetadataFile() throws Exception {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord();
    record.tableName = "clear_me";
    record.conversionType = "DIRECT";
    metadata.putConversionRecord("clear_me", record);
    metadata.saveMetadata();

    File metadataFile = new File(tempDir.toFile(), ".conversions.json");
    assertTrue(metadataFile.exists(), "File should exist before clear");

    metadata.clear();
    assertTrue(metadata.getAllConversions().isEmpty(),
        "Conversions should be empty after clear");
    assertFalse(metadataFile.exists(),
        "Metadata file should be deleted after clear");
  }

  @Test void testClearThenSave() throws Exception {
    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord();
    record.tableName = "old_data";
    record.conversionType = "DIRECT";
    metadata.putConversionRecord("old_data", record);
    metadata.saveMetadata();

    metadata.clear();

    ConversionMetadata.ConversionRecord newRecord =
        new ConversionMetadata.ConversionRecord();
    newRecord.tableName = "new_data";
    newRecord.conversionType = "PARQUET";
    metadata.putConversionRecord("new_data", newRecord);
    metadata.saveMetadata();

    ConversionMetadata reloaded = new ConversionMetadata(tempDir.toFile());
    assertTrue(reloaded.hasTableMetadata("new_data"),
        "New data should exist after clear+save cycle");
    assertFalse(reloaded.hasTableMetadata("old_data"),
        "Old data should not exist after clear+save cycle");
  }

  // =========================================================================
  // 14. loadMetadata with corrupted file
  // =========================================================================

  @Test void testLoadMetadataWithCorruptedFile() throws Exception {
    File metadataFile = new File(tempDir.toFile(), ".conversions.json");
    try (FileWriter writer = new FileWriter(metadataFile)) {
      writer.write("this is not valid JSON {{{");
    }

    ConversionMetadata corrupted = new ConversionMetadata(tempDir.toFile());
    assertTrue(corrupted.getAllConversions().isEmpty(),
        "Corrupted metadata should result in empty conversions");
  }

  @Test void testLoadMetadataWithEmptyFile() throws Exception {
    File metadataFile = new File(tempDir.toFile(), ".conversions.json");
    Files.write(metadataFile.toPath(), new byte[0]);

    ConversionMetadata empty = new ConversionMetadata(tempDir.toFile());
    assertTrue(empty.getAllConversions().isEmpty(),
        "Empty metadata file should result in empty conversions");
  }

  // =========================================================================
  // 15. hasChanged with local file newer than timestamp
  // =========================================================================

  @Test void testHasChangedLocalFileNewerThanRecord() throws Exception {
    File sourceFile = new File(tempDir.toFile(), "changing.csv");
    Files.write(sourceFile.toPath(), "original".getBytes());

    ConversionMetadata.ConversionRecord record =
        new ConversionMetadata.ConversionRecord();
    record.originalFile = sourceFile.getAbsolutePath();
    // Set timestamp to past so file appears "newer"
    record.timestamp = 1000L;

    assertTrue(record.hasChanged(),
        "File with newer timestamp should be detected as changed");
  }
}
