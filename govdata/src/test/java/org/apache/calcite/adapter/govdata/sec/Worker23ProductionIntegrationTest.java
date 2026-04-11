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
import org.apache.calcite.adapter.file.partition.PipelineTrackerFactory;
import org.apache.calcite.adapter.file.partition.S3HivePipelineTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests that verify worker-23 does not reprocess previously
 * processed SEC filings when tested against the production S3 tracker backend.
 *
 * <p>These tests require the following environment variables to be set:
 * <ul>
 *   <li>{@code CALCITE_TRACKER_S3_BUCKET} — S3/R2 bucket for tracker data</li>
 *   <li>{@code AWS_ACCESS_KEY_ID} — S3-compatible access key</li>
 *   <li>{@code AWS_SECRET_ACCESS_KEY} — S3-compatible secret key</li>
 *   <li>{@code AWS_ENDPOINT_OVERRIDE} — S3-compatible endpoint (Cloudflare R2, MinIO, etc.)</li>
 * </ul>
 *
 * <p>Each test writes a synthetic accession ({@code 0099999999-26-ITXXXXXXXX}) to
 * the tracker, recreates the tracker from scratch to simulate a fresh worker-23
 * invocation, and verifies that the accession is correctly returned as SKIP.
 * Test entries are cleaned up in {@link #tearDown()} by marking them as cleared.
 *
 * <p>If any of the above env vars are absent the tests are silently skipped.
 */
@Tag("integration")
public class Worker23ProductionIntegrationTest {

  /** Form types processed by worker-23. */
  private static final String[] WORKER_23_FORM_TYPES = {
      "8-K", "8-K/A", "DEF 14A",
      "3", "4", "5",
      "13F-HR", "13F-HR/A",
      "SC 13D", "SC 13D/A",
      "SC 13G", "SC 13G/A"
  };

  /**
   * Synthetic test accession.
   * CIK 0099999999 is not a valid EDGAR filer; year=26→2026 is a safe tracker partition.
   * Format: 0099999999-YY-XXXXXX (10 chars, dash, 2-char YY, dash, 6 chars).
   */
  private static final String TEST_CIK = "0099999999";
  private static final String TEST_ACCESSION = "0099999999-26-IT000001";
  private static final String TEST_FILING_DATE = "2026-01-01";

  private Map<String, Object> trackerOperand;
  private S3HivePipelineTracker activeTracker;

  @TempDir
  File tempDir;

  @BeforeEach
  void setUp() {
    assumeTrue(hasProductionCredentials(),
        "Skipping: production S3 credentials not set "
            + "(CALCITE_TRACKER_S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, "
            + "AWS_ENDPOINT_OVERRIDE)");

    trackerOperand = buildTrackerOperand();
    activeTracker = openTracker();
  }

  @AfterEach
  void tearDown() {
    if (activeTracker == null) {
      return;
    }
    // Mark all test tables as cleared to remove the test entry from the tracker.
    String[] tableNames = {
        "metadata", "facts", "contexts", "relationships",
        "mda", "insider", "earnings", "_filing_meta"
    };
    for (String table : tableNames) {
      try {
        activeTracker.markCleared(TEST_ACCESSION, table, "staging");
      } catch (Exception ignored) {
        // Best-effort cleanup
      }
    }
    try {
      activeTracker.close();
    } catch (Exception ignored) {
      // Best-effort
    }
  }

  // -----------------------------------------------------------------------
  // Core no-reprocess guarantee via production tracker
  // -----------------------------------------------------------------------

  /**
   * The main diagnostic test:
   * <ol>
   *   <li>Create a fresh tracker (instance A) — simulates the first worker-23 run.</li>
   *   <li>Mark the test filing complete in A.</li>
   *   <li>Close A and open a new tracker instance B — simulates a subsequent worker-23 run.</li>
   *   <li>Verify that {@link SecFilingCache#checkFiling} returns SKIP in B.</li>
   * </ol>
   *
   * <p>If this test fails with PROCESS instead of SKIP, the tracker is not persisting
   * state across worker invocations — confirming the suspected reprocessing bug.
   */
  @Test
  void trackerPersistsAcrossWorkerInvocations() throws Exception {
    for (String formTypeName : WORKER_23_FORM_TYPES) {
      // --- Run 1: process and mark complete ---
      SecFilingCache cacheA = new SecFilingCache(activeTracker, noopStorage(), "/parquet");

      ProcessingDecision firstCheck =
          cacheA.checkFiling(TEST_CIK, TEST_ACCESSION, formTypeName, TEST_FILING_DATE, false);
      assertEquals(ProcessingDecision.Action.PROCESS, firstCheck.getAction(),
          "Fresh tracker must require processing for form " + formTypeName);

      FormType form = FormType.fromString(formTypeName);
      FileInventory inventory = completeInventoryFor(form);
      cacheA.markComplete(TEST_CIK, TEST_ACCESSION, formTypeName, TEST_FILING_DATE, false,
          inventory);

      // --- Run 2: fresh tracker instance (as worker-23 does on the next run) ---
      // Flush buffered states to S3 before opening the fresh tracker, simulating
      // the real worker-23 shutdown boundary (flushPendingStates is called on close).
      activeTracker.flushPendingStates();
      S3HivePipelineTracker freshTracker = openTracker();
      try {
        SecFilingCache cacheB = new SecFilingCache(freshTracker, noopStorage(), "/parquet");

        ProcessingDecision secondCheck =
            cacheB.checkFiling(TEST_CIK, TEST_ACCESSION, formTypeName, TEST_FILING_DATE, false);

        assertEquals(ProcessingDecision.Action.SKIP, secondCheck.getAction(),
            "Form " + formTypeName + " must not be reprocessed: "
                + "tracker state must survive closing and reopening the S3 tracker. "
                + "If this fails, worker-23 IS reprocessing previously completed filings.");
        assertFalse(secondCheck.shouldProcess(),
            "shouldProcess() must return false for " + formTypeName + " on second run");
      } finally {
        freshTracker.close();
      }

      // Clean up this form type's tracker entries before the next iteration.
      cleanupTestEntry();
    }
  }

  /**
   * Verifies that even the first invocation of a fresh tracker instance correctly
   * loads pre-existing state from S3 for a filing that was completed in a prior
   * tracker instance (written separately before this test).
   *
   * <p>This test writes state via a tracker instance, flushes, closes, and then
   * opens a brand-new instance that must read that state from S3 without any
   * in-memory cache.
   */
  @Test
  void freshTrackerInstanceReadsPersistedStateFromS3() throws Exception {
    // Write state via instance A and ensure it is flushed to S3.
    SecFilingCache cacheA = new SecFilingCache(activeTracker, noopStorage(), "/parquet");
    FormType form = FormType.fromString("8-K");
    FileInventory inventory = completeInventoryFor(form);
    cacheA.markComplete(TEST_CIK, TEST_ACCESSION, "8-K", TEST_FILING_DATE, false, inventory);

    // Flush all pending writes to S3 before closing.
    activeTracker.flushPendingStates();
    activeTracker.close();
    activeTracker = null;

    // Open a completely fresh tracker — no in-memory cache from the previous run.
    S3HivePipelineTracker freshTracker = openTracker();
    activeTracker = freshTracker;

    SecFilingCache cacheB = new SecFilingCache(freshTracker, noopStorage(), "/parquet");

    ProcessingDecision decision =
        cacheB.checkFiling(TEST_CIK, TEST_ACCESSION, "8-K", TEST_FILING_DATE, false);

    assertEquals(ProcessingDecision.Action.SKIP, decision.getAction(),
        "A fresh S3HivePipelineTracker instance must read previously persisted state. "
            + "If PROCESS is returned, the tracker is not loading S3 state on startup — "
            + "this is the root cause of the worker-23 reprocessing bug.");
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private S3HivePipelineTracker openTracker() {
    PipelineTracker t = PipelineTrackerFactory.createFromOperand(trackerOperand,
        tempDir.getAbsolutePath());
    if (!(t instanceof S3HivePipelineTracker)) {
      throw new IllegalStateException(
          "Expected S3HivePipelineTracker but got: " + t.getClass().getName());
    }
    return (S3HivePipelineTracker) t;
  }

  private void cleanupTestEntry() {
    String[] tableNames = {
        "metadata", "facts", "contexts", "relationships",
        "mda", "insider", "earnings", "_filing_meta"
    };
    for (String table : tableNames) {
      try {
        activeTracker.markCleared(TEST_ACCESSION, table, "staging");
      } catch (Exception ignored) {
        // Best-effort
      }
    }
    try {
      activeTracker.flushPendingStates();
    } catch (Exception ignored) {
      // Best-effort
    }
  }

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

  private static Map<String, Object> buildTrackerOperand() {
    Map<String, String> trackerConfig = new HashMap<String, String>();
    trackerConfig.put("bucket", System.getenv("CALCITE_TRACKER_S3_BUCKET"));
    trackerConfig.put("endpoint", System.getenv("AWS_ENDPOINT_OVERRIDE"));

    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
    s3Config.put("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
    s3Config.put("endpoint", System.getenv("AWS_ENDPOINT_OVERRIDE"));

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("trackerBackend", "s3");
    operand.put("trackerConfig", trackerConfig);
    operand.put("s3Config", s3Config);
    return operand;
  }

  private static boolean hasProductionCredentials() {
    return isSet("CALCITE_TRACKER_S3_BUCKET")
        && isSet("AWS_ACCESS_KEY_ID")
        && isSet("AWS_SECRET_ACCESS_KEY")
        && isSet("AWS_ENDPOINT_OVERRIDE");
  }

  private static boolean isSet(String envVar) {
    String val = System.getenv(envVar);
    return val != null && !val.isEmpty();
  }

  private static StorageProvider noopStorage() {
    return new NoopStorageProvider();
  }

  // -----------------------------------------------------------------------
  // No-op StorageProvider (disables S3 self-healing in SecFilingCache)
  // -----------------------------------------------------------------------

  private static final class NoopStorageProvider implements StorageProvider {

    @Override
    public List<StorageProvider.FileEntry> listFiles(String path, boolean recursive) {
      return Collections.emptyList();
    }

    @Override
    public StorageProvider.FileMetadata getMetadata(String path) throws IOException {
      throw new IOException("NoopStorageProvider: " + path);
    }

    @Override
    public InputStream openInputStream(String path) throws IOException {
      throw new IOException("NoopStorageProvider: " + path);
    }

    @Override
    public Reader openReader(String path) throws IOException {
      throw new IOException("NoopStorageProvider: " + path);
    }

    @Override
    public boolean exists(String path) {
      return false;
    }

    @Override
    public boolean isDirectory(String path) {
      return false;
    }

    @Override
    public String getStorageType() {
      return "noop";
    }

    @Override
    public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }
}