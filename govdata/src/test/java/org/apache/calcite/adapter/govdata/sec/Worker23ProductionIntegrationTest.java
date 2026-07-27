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

import org.apache.calcite.adapter.file.partition.PGPipelineTracker;
import org.apache.calcite.adapter.file.partition.PipelineTracker;
import org.apache.calcite.adapter.file.partition.PipelineTrackerFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests that verify worker-23 does not reprocess previously processed SEC filings.
 *
 * <p>The guarantee under test is that completion state survives the worker process: a filing
 * marked complete by one tracker instance must read back as SKIP from a brand-new instance that
 * holds no in-memory cache. Postgres is the canonical (and only) ETL state store, so this is the
 * property that keeps a re-run from redoing finished work.
 *
 * <p>These tests require the tracker database connection to be set:
 * <ul>
 *   <li>{@code GOVDATA_TRACKER_PG_URL} — e.g. {@code jdbc:postgresql://localhost:5432/govdata}</li>
 *   <li>{@code GOVDATA_TRACKER_PG_USER}</li>
 *   <li>{@code GOVDATA_TRACKER_PG_PASSWORD}</li>
 * </ul>
 *
 * <p>Writes go to a dedicated tracker schema ({@link #TEST_NAMESPACE}, created on demand) rather
 * than the production namespace, so a failed run can never leave a synthetic accession in the
 * state the real ETL reads. Test entries are cleared in {@link #tearDown()} regardless.
 *
 * <p>If any of the above env vars are absent the tests are skipped.
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

  /** Tables SecFilingCache may write for the form types above; cleared between runs. */
  private static final String[] TEST_TABLE_NAMES = {
      "metadata", "facts", "contexts", "relationships",
      "mda", "insider", "earnings", "13f", "13dg", "_filing_meta"
  };

  /**
   * Synthetic test accession.
   * CIK 0099999999 is not a valid EDGAR filer; year=26→2026 is a safe tracker partition.
   * Format: 0099999999-YY-XXXXXX (10 chars, dash, 2-char YY, dash, 6 chars).
   */
  private static final String TEST_CIK = "0099999999";
  private static final String TEST_ACCESSION = "0099999999-26-IT000001";
  private static final String TEST_FILING_DATE = "2026-01-01";

  /**
   * Tracker schema for this test. PGPipelineTracker derives its namespace from the operand's
   * {@code directory}, so pointing that at a test-only bucket name isolates these writes from
   * the production {@code govdata_parquet_v1} tables.
   */
  private static final String TEST_NAMESPACE = "s3://govdata-worker23-it";

  private Map<String, Object> trackerOperand;
  private PGPipelineTracker activeTracker;

  @TempDir
  File tempDir;

  @BeforeEach
  void setUp() {
    assumeTrue(hasTrackerCredentials(),
        "Skipping: tracker database not configured "
            + "(GOVDATA_TRACKER_PG_URL, GOVDATA_TRACKER_PG_USER, GOVDATA_TRACKER_PG_PASSWORD)");

    trackerOperand = buildTrackerOperand();
    activeTracker = openTracker();
    // A previous failed run may have left the synthetic accession behind; start from a known state.
    cleanupTestEntry();
  }

  @AfterEach
  void tearDown() {
    if (activeTracker == null) {
      return;
    }
    cleanupTestEntry();
    try {
      activeTracker.close();
    } catch (Exception ignored) {
      // Best-effort
    }
  }

  // -----------------------------------------------------------------------
  // Core no-reprocess guarantee
  // -----------------------------------------------------------------------

  /**
   * The main diagnostic test:
   * <ol>
   *   <li>Create a fresh tracker (instance A) — simulates the first worker-23 run.</li>
   *   <li>Mark the test filing complete in A.</li>
   *   <li>Open a new tracker instance B — simulates a subsequent worker-23 run.</li>
   *   <li>Verify that {@link SecFilingCache#checkFiling} returns SKIP in B.</li>
   * </ol>
   *
   * <p>If this test fails with PROCESS instead of SKIP, the tracker is not persisting state
   * across worker invocations, which is exactly the reprocessing bug.
   */
  @Test
  void trackerPersistsAcrossWorkerInvocations() throws Exception {
    for (String formTypeName : WORKER_23_FORM_TYPES) {
      // --- Run 1: process and mark complete ---
      SecFilingCache cacheA = new SecFilingCache(activeTracker);

      ProcessingDecision firstCheck =
          cacheA.checkFiling(TEST_CIK, TEST_ACCESSION, formTypeName, TEST_FILING_DATE, false);
      assertEquals(ProcessingDecision.Action.PROCESS, firstCheck.getAction(),
          "Fresh tracker must require processing for form " + formTypeName);

      FormType form = FormType.fromString(formTypeName);
      FileInventory inventory = completeInventoryFor(form);
      cacheA.markComplete(TEST_CIK, TEST_ACCESSION, formTypeName, TEST_FILING_DATE, false,
          inventory);

      // --- Run 2: fresh tracker instance (as worker-23 does on the next run) ---
      PGPipelineTracker freshTracker = openTracker();
      try {
        SecFilingCache cacheB = new SecFilingCache(freshTracker);

        ProcessingDecision secondCheck =
            cacheB.checkFiling(TEST_CIK, TEST_ACCESSION, formTypeName, TEST_FILING_DATE, false);

        assertEquals(ProcessingDecision.Action.SKIP, secondCheck.getAction(),
            "Form " + formTypeName + " must not be reprocessed: tracker state must survive "
                + "closing and reopening the tracker. If this fails, worker-23 IS reprocessing "
                + "previously completed filings.");
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
   * Verifies that a brand-new tracker instance loads pre-existing state from the database with no
   * help from an in-memory cache: state is written through instance A, A is closed, and a fresh
   * instance B must still answer SKIP.
   */
  @Test
  void freshTrackerInstanceReadsPersistedState() throws Exception {
    SecFilingCache cacheA = new SecFilingCache(activeTracker);
    FormType form = FormType.fromString("8-K");
    FileInventory inventory = completeInventoryFor(form);
    cacheA.markComplete(TEST_CIK, TEST_ACCESSION, "8-K", TEST_FILING_DATE, false, inventory);

    activeTracker.close();
    activeTracker = null;

    // Open a completely fresh tracker — no in-memory cache from the previous instance.
    PGPipelineTracker freshTracker = openTracker();
    activeTracker = freshTracker;

    SecFilingCache cacheB = new SecFilingCache(freshTracker);

    ProcessingDecision decision =
        cacheB.checkFiling(TEST_CIK, TEST_ACCESSION, "8-K", TEST_FILING_DATE, false);

    assertEquals(ProcessingDecision.Action.SKIP, decision.getAction(),
        "A fresh tracker instance must read previously persisted state. If PROCESS is returned, "
            + "the tracker is not loading state on startup — the root cause of the worker-23 "
            + "reprocessing bug.");
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private PGPipelineTracker openTracker() {
    PipelineTracker t = PipelineTrackerFactory.createFromOperand(trackerOperand,
        tempDir.getAbsolutePath());
    if (!(t instanceof PGPipelineTracker)) {
      throw new IllegalStateException(
          "Expected PGPipelineTracker but got: " + t.getClass().getName());
    }
    return (PGPipelineTracker) t;
  }

  private void cleanupTestEntry() {
    for (String table : TEST_TABLE_NAMES) {
      try {
        activeTracker.markCleared(TEST_ACCESSION, table, "staging");
      } catch (Exception ignored) {
        // Best-effort
      }
    }
    try {
      activeTracker.markCleared(TEST_ACCESSION, "_no_xbrl", "staging");
      activeTracker.markCleared(TEST_ACCESSION, "_error_count", "staging");
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
        .hasInstitutionalHoldings(form.expectsInstitutionalHoldings())
        .hasBeneficialOwnership(form.expectsBeneficialOwnership())
        // chunks omitted: vectorizationEnabled=false
        .build();
  }

  private static Map<String, Object> buildTrackerOperand() {
    Map<String, String> trackerConfig = new HashMap<String, String>();
    trackerConfig.put("jdbcUrl", System.getenv("GOVDATA_TRACKER_PG_URL"));
    trackerConfig.put("user", System.getenv("GOVDATA_TRACKER_PG_USER"));
    trackerConfig.put("password", System.getenv("GOVDATA_TRACKER_PG_PASSWORD"));

    Map<String, Object> operand = new HashMap<String, Object>();
    operand.put("trackerBackend", "pg");
    operand.put("trackerConfig", trackerConfig);
    // Drives PGPipelineTracker's schema namespace — keeps these writes out of the prod tables.
    operand.put("directory", TEST_NAMESPACE);
    return operand;
  }

  private static boolean hasTrackerCredentials() {
    return isSet("GOVDATA_TRACKER_PG_URL")
        && isSet("GOVDATA_TRACKER_PG_USER")
        && isSet("GOVDATA_TRACKER_PG_PASSWORD");
  }

  private static boolean isSet(String envVar) {
    String val = System.getenv(envVar);
    return val != null && !val.isEmpty();
  }
}
