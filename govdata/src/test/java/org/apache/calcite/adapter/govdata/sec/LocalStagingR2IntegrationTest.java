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
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test that verifies {@link LocalStagingStorageProvider} actually reduces
 * Class A operations (PUT requests) against the real Cloudflare R2 backend.
 *
 * <p>Requires the same env vars as {@link Worker23ProductionIntegrationTest}:
 * {@code CALCITE_TRACKER_S3_BUCKET}, {@code AWS_ACCESS_KEY_ID},
 * {@code AWS_SECRET_ACCESS_KEY}, {@code AWS_ENDPOINT_OVERRIDE}.
 *
 * <p>The test simulates 10 per-filing parquet writes (as XbrlToParquetConverter would
 * produce) for the same table type and year partition. With staging disabled each write
 * would be a separate PUT (10 Class A ops). With staging + flushAll(), all 10 are merged
 * locally and uploaded as a single PUT (1 Class A op).
 *
 * <p>Assertions:
 * <ol>
 *   <li>The counting proxy records exactly 1 {@code writeFile} call to the real R2 provider.</li>
 *   <li>The merged file exists in R2 at the expected path.</li>
 *   <li>The merged file contains all records from all staged writes.</li>
 * </ol>
 *
 * <p>Test data is written under {@code sec/parquet/test-staging/} and deleted in
 * {@link #tearDown()}.
 */
@Tag("integration")
class LocalStagingR2IntegrationTest {

  private static final Schema FACTS_SCHEMA = SchemaBuilder.record("XbrlFact")
      .fields()
      .name("cik").type().stringType().noDefault()
      .name("concept").type().stringType().noDefault()
      .name("value").type().nullable().stringType().noDefault()
      .endRecord();

  /** Number of per-filing writes to simulate — each would be a separate Class A op without staging. */
  private static final int FILING_COUNT = 10;

  /** Prefix under which test data is written — cleaned up in tearDown. */
  private static final String TEST_PREFIX = "sec/parquet/test-staging/year=2026/";

  private StorageProvider r2Provider;
  private CountingProxy countingProxy;
  private LocalStagingStorageProvider staging;
  private final List<String> uploadedPaths = new ArrayList<String>();

  @TempDir
  File stagingDir;

  @BeforeEach
  void setUp() {
    Assumptions.assumeTrue(hasCredentials(),
        "Skipping: R2 credentials not set "
            + "(CALCITE_TRACKER_S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, "
            + "AWS_ENDPOINT_OVERRIDE)");

    Map<String, Object> config = new HashMap<String, Object>();
    config.put("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
    config.put("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
    config.put("endpoint", System.getenv("AWS_ENDPOINT_OVERRIDE"));
    config.put("bucket", System.getenv("CALCITE_TRACKER_S3_BUCKET"));
    config.put("directory", "");

    r2Provider = StorageProviderFactory.createFromType("s3", config);
    countingProxy = new CountingProxy(r2Provider, uploadedPaths);
    staging = new LocalStagingStorageProvider(countingProxy, stagingDir);
  }

  @AfterEach
  void tearDown() {
    if (r2Provider == null) {
      return;
    }
    // Best-effort: delete all test files uploaded to R2
    for (String path : uploadedPaths) {
      try {
        r2Provider.delete(path);
      } catch (Exception ignored) {
        // Best-effort cleanup
      }
    }
  }

  /**
   * Core verification: N individual writeAvroParquet calls → 1 writeFile PUT to R2.
   *
   * <p>Without LocalStagingStorageProvider, XbrlToParquetConverter would generate
   * {@value #FILING_COUNT} Class A PUT operations for the same table+year group.
   * With staging, all are merged locally and uploaded in a single PUT.
   */
  @Test
  void nFilingWritesProduceSingleR2Put() throws IOException {
    String bucket = System.getenv("CALCITE_TRACKER_S3_BUCKET");
    String r2Base = bucket + "/" + TEST_PREFIX;

    int totalRecords = 0;
    for (int i = 0; i < FILING_COUNT; i++) {
      String path = r2Base + "000012345" + String.format("%02d", i) + "-26-00000" + i + "_facts.parquet";
      List<GenericRecord> records = makeRecords("cik" + i, 3);
      totalRecords += records.size();
      staging.writeAvroParquet(path, FACTS_SCHEMA, records, "XbrlFact");
    }

    // Before flush: no writes should have reached R2 (threshold default = 100, we have 10)
    assertEquals(0, countingProxy.writeCount.get(),
        FILING_COUNT + " writeAvroParquet calls should not reach R2 before flushAll()");

    // Flush: all 10 staged files merged into one → 1 PUT to R2
    staging.flushAll();

    assertEquals(1, countingProxy.writeCount.get(),
        FILING_COUNT + " staged writes for the same table+year should produce exactly 1 R2 PUT. "
            + "Actual PUT count: " + countingProxy.writeCount.get()
            + ". Class A ops reduction factor: " + FILING_COUNT + "x expected.");

    // Verify the merged file exists in R2
    assertTrue(r2Provider.exists(uploadedPaths.get(0)),
        "Merged batch file must exist in R2 at: " + uploadedPaths.get(0));

    // Verify merged file contains all records from all staged writes
    int r2RowCount = countRowsInR2(uploadedPaths.get(0));
    assertEquals(totalRecords, r2RowCount,
        "Merged R2 file must contain all " + totalRecords + " records from "
            + FILING_COUNT + " staged writes. Got: " + r2RowCount);
  }

  /**
   * Verifies that different table types (facts, metadata) flush as separate R2 objects —
   * one PUT per table type, not one per filing.
   */
  @Test
  void differentTableTypesProduceSeparateR2Puts() throws IOException {
    String bucket = System.getenv("CALCITE_TRACKER_S3_BUCKET");
    String r2Base = bucket + "/" + TEST_PREFIX;

    int filings = 5;
    for (int i = 0; i < filings; i++) {
      String factsPath = r2Base + "cik" + i + "_facts.parquet";
      staging.writeAvroParquet(factsPath, FACTS_SCHEMA, makeRecords("cik" + i, 2), "XbrlFact");

      String metaPath = r2Base + "cik" + i + "_metadata.parquet";
      staging.writeAvroParquet(metaPath, FACTS_SCHEMA, makeRecords("cik" + i, 1), "FilingMetadata");
    }

    staging.flushAll();

    assertEquals(2, countingProxy.writeCount.get(),
        filings + " filings with 2 table types should produce exactly 2 R2 PUTs "
            + "(1 per table type), not " + (filings * 2) + ". "
            + "Actual: " + countingProxy.writeCount.get());
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private static List<GenericRecord> makeRecords(String cik, int count) {
    List<GenericRecord> records = new ArrayList<GenericRecord>();
    for (int i = 0; i < count; i++) {
      GenericRecord r = new GenericData.Record(FACTS_SCHEMA);
      r.put("cik", cik);
      r.put("concept", "concept_" + i);
      r.put("value", "val_" + i);
      records.add(r);
    }
    return records;
  }

  private int countRowsInR2(String path) throws IOException {
    File temp = File.createTempFile("r2-verify-", ".parquet");
    try {
      try (InputStream is = r2Provider.openInputStream(path)) {
        java.nio.file.Files.copy(is, temp.toPath(),
            java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      }
      // Use DuckDB to count rows — avoids Avro schema compatibility issues with
      // parquet files written by DuckDB's COPY statement.
      try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
           java.sql.Statement stmt = conn.createStatement();
           java.sql.ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) FROM read_parquet('"
               + temp.getAbsolutePath().replace("'", "''") + "')")) {
        rs.next();
        return rs.getInt(1);
      } catch (java.sql.SQLException e) {
        throw new IOException("DuckDB row count failed: " + e.getMessage(), e);
      }
    } finally {
      temp.delete();
    }
  }

  private static boolean hasCredentials() {
    return isSet("CALCITE_TRACKER_S3_BUCKET")
        && isSet("AWS_ACCESS_KEY_ID")
        && isSet("AWS_SECRET_ACCESS_KEY")
        && isSet("AWS_ENDPOINT_OVERRIDE");
  }

  private static boolean isSet(String var) {
    String v = System.getenv(var);
    return v != null && !v.isEmpty();
  }

  // -----------------------------------------------------------------------
  // Counting proxy — wraps real R2 provider, records writeFile calls and paths
  // -----------------------------------------------------------------------

  private static final class CountingProxy implements StorageProvider {

    final AtomicInteger writeCount = new AtomicInteger(0);
    private final StorageProvider delegate;
    private final List<String> uploadedPaths;

    CountingProxy(StorageProvider delegate, List<String> uploadedPaths) {
      this.delegate = delegate;
      this.uploadedPaths = uploadedPaths;
    }

    @Override public void writeFile(String path, InputStream content) throws IOException {
      writeCount.incrementAndGet();
      uploadedPaths.add(path);
      delegate.writeFile(path, content);
    }

    @Override public void writeFile(String path, byte[] content) throws IOException {
      writeCount.incrementAndGet();
      uploadedPaths.add(path);
      delegate.writeFile(path, content);
    }

    @Override public boolean exists(String path) throws IOException {
      return delegate.exists(path);
    }

    @Override public boolean delete(String path) throws IOException {
      return delegate.delete(path);
    }

    @Override public InputStream openInputStream(String path) throws IOException {
      return delegate.openInputStream(path);
    }

    @Override public Reader openReader(String path) throws IOException {
      return delegate.openReader(path);
    }

    @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
      return delegate.listFiles(path, recursive);
    }

    @Override public FileMetadata getMetadata(String path) throws IOException {
      return delegate.getMetadata(path);
    }

    @Override public String resolvePath(String basePath, String relativePath) {
      return delegate.resolvePath(basePath, relativePath);
    }

    @Override public String getStorageType() {
      return "counting+" + delegate.getStorageType();
    }

    @Override public boolean isDirectory(String path) throws IOException {
      return delegate.isDirectory(path);
    }

    @Override public byte[] readRange(String path, long offset, long length) throws IOException {
      return delegate.readRange(path, offset, length);
    }

    @Override public void createDirectories(String path) throws IOException {
      delegate.createDirectories(path);
    }

    @Override public int deleteBatch(List<String> paths) throws IOException {
      return delegate.deleteBatch(paths);
    }

    @Override public void ensureLifecycleRule(String prefix, int expirationDays) throws IOException {
      delegate.ensureLifecycleRule(prefix, expirationDays);
    }

    @Override public String getStagingDirectory(String purpose) throws IOException {
      return delegate.getStagingDirectory(purpose);
    }

    @Override public void copyFile(String source, String destination) throws IOException {
      delegate.copyFile(source, destination);
    }

    @Override public boolean hasChanged(String path, FileMetadata cachedMetadata) throws IOException {
      return delegate.hasChanged(path, cachedMetadata);
    }

    @Override public void cleanupMacosMetadata(String directoryPath) throws IOException {
      delegate.cleanupMacosMetadata(directoryPath);
    }

    @Override public Map<String, String> getS3Config() {
      return delegate.getS3Config();
    }

    @Override public void writeAvroParquet(String path, org.apache.avro.Schema schema,
        List<org.apache.avro.generic.GenericRecord> records, String recordType) throws IOException {
      // writeAvroParquet on delegate is NOT intercepted — LocalStagingStorageProvider
      // intercepts this above us and converts it into a writeFile call on us.
      delegate.writeAvroParquet(path, schema, records, recordType);
    }
  }
}