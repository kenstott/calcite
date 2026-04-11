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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.partition.PipelineTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that worker-23 (SEC secondary filings: 8-K, Proxy, Insider, 13F, 13D/G)
 * does not reprocess form submissions that have already been processed.
 *
 * <p>Worker-23 processes the following form types: 8-K, 8-K/A, DEF 14A,
 * Form 3, Form 4, Form 5, 13F-HR, 13F-HR/A, SC 13D, SC 13D/A, SC 13G, SC 13G/A.
 *
 * <p>The test exercises the production optimization path:
 * <ol>
 *   <li>Parquet records are written to a local staging directory.</li>
 *   <li>Multiple staged files are compacted by DuckDB into a single batch file.</li>
 *   <li>The compacted batch file is uploaded to the delegate storage (simulating R2/S3).</li>
 *   <li>After marking a filing complete, {@link SecFilingCache#checkFiling} must
 *       return {@link ProcessingDecision.Action#SKIP} for that filing.</li>
 * </ol>
 */
@Tag("unit")
public class Worker23NoReprocessTest {

  /** Form types processed by worker-23. */
  private static final String[] WORKER_23_FORM_TYPES = {
      "8-K", "8-K/A", "DEF 14A",
      "3", "4", "5",
      "13F-HR", "13F-HR/A",
      "SC 13D", "SC 13D/A",
      "SC 13G", "SC 13G/A"
  };

  /** Minimal Avro schema reused across all test writes. */
  private static final Schema METADATA_SCHEMA = SchemaBuilder
      .record("FilingMetadata")
      .namespace("org.apache.calcite.adapter.govdata.sec")
      .fields()
      .name("cik").type().stringType().noDefault()
      .name("accession_number").type().stringType().noDefault()
      .name("form_type").type().stringType().noDefault()
      .endRecord();

  @TempDir
  File stagingDir;

  private CapturingStorageProvider capturingDelegate;
  private LocalStagingStorageProvider stagingProvider;

  @BeforeEach
  void setUp() {
    capturingDelegate = new CapturingStorageProvider();
    stagingProvider = new LocalStagingStorageProvider(capturingDelegate, stagingDir);
  }

  @AfterEach
  void tearDown() throws IOException {
    stagingProvider.flushAll();
  }

  // -----------------------------------------------------------------------
  // Staging → compact → upload mechanics
  // -----------------------------------------------------------------------

  /**
   * A single parquet file written via the staging provider is uploaded to
   * the delegate as-is (no DuckDB merge needed for one file).
   */
  @Test
  void singleFileIsUploadedDirectlyWithoutMerge() throws IOException {
    List<GenericRecord> records = buildRecords("0001234567", "0001234567-26-000001", "8-K");
    String path = "/parquet/sec/year=2026/0001234567-26-000001_metadata.parquet";

    stagingProvider.writeAvroParquet(path, METADATA_SCHEMA, records, "metadata");
    stagingProvider.flushAll();

    assertEquals(1, capturingDelegate.uploadedPaths().size(),
        "One staged file should produce exactly one upload");
    String uploadedPath = capturingDelegate.uploadedPaths().iterator().next();
    assertTrue(uploadedPath.contains("metadata"), "Uploaded path must include table type");
  }

  /**
   * Multiple parquet files for the same group (same parent dir + table type)
   * are merged by DuckDB into one batch file before upload.
   */
  @Test
  void multipleFilesForSameGroupAreMergedByDuckDb() throws IOException {
    String parentPath = "/parquet/sec/year=2026";
    String tableType = "metadata";
    int fileCount = 3;

    for (int i = 1; i <= fileCount; i++) {
      String accession = String.format("0001234567-26-%06d", i);
      List<GenericRecord> records = buildRecords("0001234567", accession, "8-K");
      String path = parentPath + "/" + accession + "_" + tableType + ".parquet";
      stagingProvider.writeAvroParquet(path, METADATA_SCHEMA, records, tableType);
    }

    stagingProvider.flushAll();

    assertEquals(1, capturingDelegate.uploadedPaths().size(),
        fileCount + " staged files for the same group should produce exactly one merged upload");
    long totalBytesUploaded = capturingDelegate.totalBytesUploaded();
    assertTrue(totalBytesUploaded > 0, "Merged parquet file must have non-zero size");
  }

  /**
   * Files for different table types are staged and uploaded in separate batches.
   */
  @Test
  void filesForDifferentTableTypesAreUploadedSeparately() throws IOException {
    String parentPath = "/parquet/sec/year=2026";
    String[] tableTypes = {"metadata", "facts"};
    Schema factsSchema = SchemaBuilder.record("XbrlFact").fields()
        .name("cik").type().stringType().noDefault()
        .name("concept").type().stringType().noDefault()
        .endRecord();

    // Write one file per table type
    GenericRecord metaRecord = new GenericData.Record(METADATA_SCHEMA);
    metaRecord.put("cik", "0001234567");
    metaRecord.put("accession_number", "0001234567-26-000001");
    metaRecord.put("form_type", "8-K");
    stagingProvider.writeAvroParquet(
        parentPath + "/0001234567-26-000001_metadata.parquet",
        METADATA_SCHEMA,
        Collections.singletonList(metaRecord),
        "metadata");

    GenericRecord factsRecord = new GenericData.Record(factsSchema);
    factsRecord.put("cik", "0001234567");
    factsRecord.put("concept", "us-gaap/Revenue");
    stagingProvider.writeAvroParquet(
        parentPath + "/0001234567-26-000001_facts.parquet",
        factsSchema,
        Collections.singletonList(factsRecord),
        "facts");

    stagingProvider.flushAll();

    assertEquals(2, capturingDelegate.uploadedPaths().size(),
        "Each table type should produce a separate upload");
    assertTrue(capturingDelegate.hasPathMatching("metadata"),
        "A metadata batch must have been uploaded");
    assertTrue(capturingDelegate.hasPathMatching("facts"),
        "A facts batch must have been uploaded");
  }

  // -----------------------------------------------------------------------
  // No-reprocess guarantee through the staging path
  // -----------------------------------------------------------------------

  /**
   * Verifies that all 12 worker-23 form types do not get reprocessed after
   * their parquet files have been staged locally, compacted by DuckDB,
   * uploaded to the delegate storage, and marked complete in SecFilingCache.
   */
  @Test
  void worker23DoesNotReprocessAfterStagingAndFlush() throws IOException {
    for (String formTypeName : WORKER_23_FORM_TYPES) {
      InMemoryPipelineTracker tracker = new InMemoryPipelineTracker();
      LocalStagingStorageProvider localStaging =
          new LocalStagingStorageProvider(capturingDelegate, stagingDir);
      SecFilingCache cache =
          new SecFilingCache(tracker, localStaging, "/parquet");

      String cik = "0001234567";
      String accession = "0001234567-26-000001";
      String filingDate = "2026-01-01";

      // Step 1: Confirm the filing needs processing.
      ProcessingDecision before =
          cache.checkFiling(cik, accession, formTypeName, filingDate, false);
      assertEquals(ProcessingDecision.Action.PROCESS, before.getAction(),
          "Form " + formTypeName + " should require processing on first check");

      // Step 2: Simulate writing parquet files through the staging provider
      //         (the path taken by XbrlToParquetConverter / worker-23).
      String parentPath = "/parquet/sec/year=2026";
      List<GenericRecord> records = buildRecords(cik, accession, formTypeName);
      localStaging.writeAvroParquet(
          parentPath + "/" + accession + "_metadata.parquet",
          METADATA_SCHEMA, records, "metadata");

      // Step 3: Flush staged files — DuckDB merges and uploads to the delegate.
      localStaging.flushAll();
      assertTrue(capturingDelegate.uploadedPaths().size() > 0,
          "At least one batch must have been uploaded to the delegate for form " + formTypeName);

      // Step 4: Mark the filing complete in SecFilingCache (as worker-23 does on success).
      FormType form = FormType.fromString(formTypeName);
      FileInventory inventory = completeInventoryFor(form);
      cache.markComplete(cik, accession, formTypeName, filingDate, false, inventory);

      // Step 5: SecFilingCache must now return SKIP — no reprocessing.
      ProcessingDecision after =
          cache.checkFiling(cik, accession, formTypeName, filingDate, false);
      assertEquals(ProcessingDecision.Action.SKIP, after.getAction(),
          "Form " + formTypeName + " must not be reprocessed after markComplete");
      assertFalse(after.shouldProcess(),
          "shouldProcess() must return false for completed form " + formTypeName);

      // Reset uploaded paths for next iteration.
      capturingDelegate.reset();
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private static List<GenericRecord> buildRecords(String cik, String accession,
      String formType) {
    GenericRecord record = new GenericData.Record(METADATA_SCHEMA);
    record.put("cik", cik);
    record.put("accession_number", accession);
    record.put("form_type", formType);
    return Collections.singletonList(record);
  }

  /**
   * Builds a complete {@link FileInventory} for the given form type
   * (vectorization disabled, so chunks are excluded).
   */
  private static FileInventory completeInventoryFor(FormType form) {
    return FileInventory.builder()
        .hasMetadata(form.expectsMetadata())
        .hasFacts(form.expectsFacts())
        .hasContexts(form.expectsContexts())
        .hasRelationships(form.expectsRelationships())
        .hasMda(form.expectsMda())
        .hasInsider(form.expectsInsider())
        .hasEarnings(form.expectsEarnings())
        // chunks omitted: vectorizationEnabled=false
        .build();
  }

  // -----------------------------------------------------------------------
  // In-memory PipelineTracker
  // -----------------------------------------------------------------------

  /**
   * In-memory {@link PipelineTracker} for unit testing.
   *
   * <p>Stores state as a map from {@code alternateName → Set<keyValues>}.
   * Overrides {@link #getCompletedTables} so {@link SecFilingCache} can
   * check completeness without touching S3 or DuckDB.
   */
  private static final class InMemoryPipelineTracker implements PipelineTracker {

    private final Map<String, Set<Map<String, String>>> processed =
        new HashMap<String, Set<Map<String, String>>>();

    private final Map<String, String> tableCompletions =
        new HashMap<String, String>();

    @Override
    public boolean isProcessed(String alternateName, String sourceTable,
        Map<String, String> keyValues) {
      Set<Map<String, String>> entries = processed.get(alternateName);
      return entries != null && entries.contains(keyValues);
    }

    @Override
    public boolean isProcessedWithTtl(String alternateName, String sourceTable,
        Map<String, String> keyValues, long ttlMillis) {
      return isProcessed(alternateName, sourceTable, keyValues);
    }

    @Override
    public void markProcessed(String alternateName, String sourceTable,
        Map<String, String> keyValues, String targetPattern) {
      processed.computeIfAbsent(alternateName,
          k -> new HashSet<Map<String, String>>()).add(new HashMap<String, String>(keyValues));
    }

    @Override
    public Set<Map<String, String>> getProcessedKeyValues(String alternateName) {
      Set<Map<String, String>> entries = processed.get(alternateName);
      return entries != null ? entries : Collections.<Map<String, String>>emptySet();
    }

    @Override
    public void invalidate(String alternateName, Map<String, String> keyValues) {
      Set<Map<String, String>> entries = processed.get(alternateName);
      if (entries != null) {
        entries.remove(keyValues);
      }
    }

    @Override
    public void invalidateAll(String alternateName) {
      processed.remove(alternateName);
    }

    @Override
    public Set<Integer> filterUnprocessed(String alternateName, String sourceTable,
        List<Map<String, String>> allCombinations) {
      Set<Integer> unprocessed = new HashSet<Integer>();
      for (int i = 0; i < allCombinations.size(); i++) {
        if (!isProcessed(alternateName, sourceTable, allCombinations.get(i))) {
          unprocessed.add(i);
        }
      }
      return unprocessed;
    }

    @Override
    public boolean isTableComplete(String pipelineName, String dimensionSignature) {
      return dimensionSignature.equals(tableCompletions.get(pipelineName));
    }

    @Override
    public void markTableComplete(String pipelineName, String dimensionSignature) {
      tableCompletions.put(pipelineName, dimensionSignature);
    }

    @Override
    public void invalidateTableCompletion(String pipelineName) {
      tableCompletions.remove(pipelineName);
    }

    @Override
    public void clearAllCompletions() {
      processed.clear();
      tableCompletions.clear();
    }

    @Override
    public Set<String> getCompletedTables(String sourceKey, String phase) {
      Set<String> tables = new HashSet<String>();
      String suffix = ":" + phase;
      Map<String, String> sourceKeyMap =
          Collections.singletonMap("source_key", sourceKey);
      for (Map.Entry<String, Set<Map<String, String>>> entry : processed.entrySet()) {
        String alternateName = entry.getKey();
        if (alternateName.endsWith(suffix)) {
          if (entry.getValue().contains(sourceKeyMap)) {
            tables.add(alternateName.substring(0,
                alternateName.length() - suffix.length()));
          }
        }
      }
      return tables;
    }
  }

  // -----------------------------------------------------------------------
  // Capturing StorageProvider (simulates R2/S3)
  // -----------------------------------------------------------------------

  /**
   * {@link StorageProvider} that captures all {@code writeFile} calls so that
   * tests can verify which batch files were uploaded and their sizes.
   *
   * <p>{@link #exists} returns {@code true} only for paths that have been
   * explicitly uploaded, enabling self-healing tests if needed.
   */
  private static final class CapturingStorageProvider implements StorageProvider {

    private final Map<String, byte[]> uploads = new ConcurrentHashMap<String, byte[]>();

    /** Paths of all files that have been uploaded via {@link #writeFile}. */
    Set<String> uploadedPaths() {
      return uploads.keySet();
    }

    /** Total bytes across all uploaded files. */
    long totalBytesUploaded() {
      long total = 0;
      for (byte[] bytes : uploads.values()) {
        total += bytes.length;
      }
      return total;
    }

    /** Returns true if any uploaded path contains the given substring. */
    boolean hasPathMatching(String substring) {
      for (String path : uploads.keySet()) {
        if (path.contains(substring)) {
          return true;
        }
      }
      return false;
    }

    /** Clears all recorded uploads (use between test iterations). */
    void reset() {
      uploads.clear();
    }

    @Override
    public void writeFile(String path, InputStream content) throws IOException {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      byte[] block = new byte[8192];
      int n;
      while ((n = content.read(block)) != -1) {
        buf.write(block, 0, n);
      }
      uploads.put(path, buf.toByteArray());
    }

    @Override
    public void writeFile(String path, byte[] content) throws IOException {
      uploads.put(path, content);
    }

    @Override
    public boolean exists(String path) throws IOException {
      return uploads.containsKey(path);
    }

    @Override
    public List<StorageProvider.FileEntry> listFiles(String path, boolean recursive)
        throws IOException {
      return Collections.emptyList();
    }

    @Override
    public StorageProvider.FileMetadata getMetadata(String path) throws IOException {
      throw new IOException("Not available in CapturingStorageProvider: " + path);
    }

    @Override
    public InputStream openInputStream(String path) throws IOException {
      throw new IOException("Not available in CapturingStorageProvider: " + path);
    }

    @Override
    public Reader openReader(String path) throws IOException {
      throw new IOException("Not available in CapturingStorageProvider: " + path);
    }

    @Override
    public boolean isDirectory(String path) throws IOException {
      return false;
    }

    @Override
    public String getStorageType() {
      return "capturing";
    }

    @Override
    public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }
}