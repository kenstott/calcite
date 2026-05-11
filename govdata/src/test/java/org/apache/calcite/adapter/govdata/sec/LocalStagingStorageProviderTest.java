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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link LocalStagingStorageProvider}.
 *
 * <p>Verifies that multiple per-filing parquet writes are batched into fewer
 * delegate {@code writeFile} calls, reducing R2 Class A operations.
 */
@Tag("unit")
class LocalStagingStorageProviderTest {

  @TempDir
  File stagingDir;

  @TempDir
  File uploadDir;

  private CountingStorageProvider countingDelegate;
  private LocalStagingStorageProvider staging;

  /** Simple Avro schema with cik and concept columns for test records. */
  private static final Schema FACTS_SCHEMA = SchemaBuilder.record("XbrlFact")
      .fields()
      .name("cik").type().stringType().noDefault()
      .name("concept").type().stringType().noDefault()
      .name("value").type().nullable().stringType().noDefault()
      .endRecord();

  @BeforeEach
  void setUp() {
    countingDelegate = new CountingStorageProvider(uploadDir);
    // flush threshold = 3 so tests can trigger batching with few calls
    staging = new LocalStagingStorageProvider(countingDelegate, stagingDir) {
      // Override to use threshold of 3 for fast testing
    };
  }

  @Test
  void testBatchFlushReducesDelegateWriteCalls() throws IOException {
    // Flush threshold defaults to 100 from env; we test flushAll() instead
    // to avoid needing to override the threshold.
    // Simulate 5 fact writes for different filings in the same year partition.
    String r2Base = "/r2/sec/parquet/year=2024";
    for (int i = 0; i < 5; i++) {
      String path = r2Base + "/000012345" + i + "-24-00000" + i + "_facts.parquet";
      List<GenericRecord> records = makeRecords(FACTS_SCHEMA, "000012345" + i, 2);
      staging.writeAvroParquet(path, FACTS_SCHEMA, records, "XbrlFact");
    }

    // No flush yet (threshold not reached with default 100)
    assertEquals(0, countingDelegate.writeFileCallCount.get(),
        "No delegate writes expected before flush threshold or flushAll");

    // flushAll merges all 5 staged files into one delegate write
    staging.flushAll();

    assertEquals(1, countingDelegate.writeFileCallCount.get(),
        "flushAll should produce exactly 1 delegate write for same table+partition group");
  }

  @Test
  void testDifferentTableTypesFlushSeparately() throws IOException {
    String r2Base = "/r2/sec/parquet/year=2024";
    // 3 facts writes + 3 metadata writes → 2 groups → 2 delegate writes on flushAll
    for (int i = 0; i < 3; i++) {
      String factsPath = r2Base + "/cik" + i + "_facts.parquet";
      staging.writeAvroParquet(factsPath, FACTS_SCHEMA,
          makeRecords(FACTS_SCHEMA, "cik" + i, 1), "XbrlFact");

      String metaPath = r2Base + "/cik" + i + "_metadata.parquet";
      staging.writeAvroParquet(metaPath, FACTS_SCHEMA,
          makeRecords(FACTS_SCHEMA, "cik" + i, 1), "FilingMetadata");
    }

    staging.flushAll();

    assertEquals(2, countingDelegate.writeFileCallCount.get(),
        "Expected 2 delegate writes: one for facts group, one for metadata group");
  }

  @Test
  void testDifferentYearsFlushSeparately() throws IOException {
    // Same table type but different year partitions → 2 separate groups
    for (int year = 2023; year <= 2024; year++) {
      String path = "/r2/sec/parquet/year=" + year + "/cik0001_facts.parquet";
      staging.writeAvroParquet(path, FACTS_SCHEMA,
          makeRecords(FACTS_SCHEMA, "0001", 1), "XbrlFact");
    }

    staging.flushAll();

    assertEquals(2, countingDelegate.writeFileCallCount.get(),
        "Facts for different years should flush as separate groups");
  }

  @Test
  void testMergedFileContainsAllRecords() throws IOException {
    Schema schema = SchemaBuilder.record("XbrlFact")
        .fields()
        .name("cik").type().stringType().noDefault()
        .name("concept").type().stringType().noDefault()
        .name("value").type().nullable().stringType().noDefault()
        .endRecord();

    String r2Base = "/r2/sec/parquet/year=2024";
    int totalRecords = 0;
    for (int i = 0; i < 3; i++) {
      String path = r2Base + "/cik" + i + "_facts.parquet";
      List<GenericRecord> records = makeRecords(schema, "cik" + i, 2);
      totalRecords += records.size();
      staging.writeAvroParquet(path, schema, records, "XbrlFact");
    }

    staging.flushAll();

    // Verify the merged file has all 6 records
    File uploadedFile = countingDelegate.uploadedFiles.get(0);
    int rowCount = countParquetRows(uploadedFile);
    assertEquals(totalRecords, rowCount,
        "Merged parquet file should contain all records from staged files");
  }

  @Test
  void testFlushAllOnEmptyStagingIsNoop() throws IOException {
    staging.flushAll(); // Should not throw
    assertEquals(0, countingDelegate.writeFileCallCount.get());
  }

  @Test
  void testNonParquetWriteFilePassesThrough() throws IOException {
    byte[] content = "index content".getBytes();
    staging.writeFile("/r2/cache/index.json", content);
    assertEquals(1, countingDelegate.writeFileCallCount.get(),
        "Non-parquet writeFile calls should pass directly to delegate");
  }

  @Test
  void testDelegateMethodsPassThrough() throws IOException {
    staging.exists("/some/path");
    staging.resolvePath("/base", "relative");
    staging.getStorageType();
    // No assertion needed — just verifying no exception is thrown
    // and delegate receives the calls
    assertEquals(1, countingDelegate.existsCallCount.get());
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private static List<GenericRecord> makeRecords(Schema schema, String cik, int count) {
    List<GenericRecord> records = new ArrayList<GenericRecord>();
    for (int i = 0; i < count; i++) {
      GenericRecord r = new GenericData.Record(schema);
      r.put("cik", cik);
      r.put("concept", "concept_" + i);
      r.put("value", "val_" + i);
      records.add(r);
    }
    return records;
  }

  @SuppressWarnings("deprecation")
  private static int countParquetRows(File parquetFile) throws IOException {
    int count = 0;
    org.apache.parquet.io.InputFile inputFile = HadoopInputFile.fromPath(
        new Path(parquetFile.toURI()), new Configuration());
    try (org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
      while (reader.read() != null) {
        count++;
      }
    }
    return count;
  }

  // -----------------------------------------------------------------------
  // Stub StorageProvider that counts writeFile calls and saves uploaded files
  // -----------------------------------------------------------------------

  static class CountingStorageProvider implements StorageProvider {

    final AtomicInteger writeFileCallCount = new AtomicInteger(0);
    final AtomicInteger existsCallCount = new AtomicInteger(0);
    final CopyOnWriteArrayList<File> uploadedFiles = new CopyOnWriteArrayList<File>();
    private final File uploadDir;

    CountingStorageProvider(File uploadDir) {
      this.uploadDir = uploadDir;
    }

    @Override public void writeFile(String path, byte[] content) throws IOException {
      writeFileCallCount.incrementAndGet();
      File out = new File(uploadDir, writeFileCallCount.get() + "_" + extractFilename(path));
      java.nio.file.Files.write(out.toPath(), content);
      uploadedFiles.add(out);
    }

    @Override public void writeFile(String path, InputStream content) throws IOException {
      writeFileCallCount.incrementAndGet();
      File out = new File(uploadDir, writeFileCallCount.get() + "_" + extractFilename(path));
      try (FileOutputStream fos = new FileOutputStream(out)) {
        byte[] buf = new byte[4096];
        int n;
        while ((n = content.read(buf)) != -1) {
          fos.write(buf, 0, n);
        }
      }
      uploadedFiles.add(out);
    }

    @Override public boolean exists(String path) throws IOException {
      existsCallCount.incrementAndGet();
      return false;
    }

    @Override public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }

    @Override public String getStorageType() {
      return "test";
    }

    @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
      return new ArrayList<FileEntry>();
    }

    @Override public FileMetadata getMetadata(String path) throws IOException {
      return new FileMetadata(path, 0, 0, null, null);
    }

    @Override public InputStream openInputStream(String path) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override public Reader openReader(String path) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override public boolean isDirectory(String path) throws IOException {
      return false;
    }

    private static String extractFilename(String path) {
      int lastSlash = path.lastIndexOf('/');
      return lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
    }
  }
}