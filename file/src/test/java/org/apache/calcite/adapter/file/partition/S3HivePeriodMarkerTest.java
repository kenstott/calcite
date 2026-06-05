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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for the fix #2 per-period completion markers against a real S3-compatible
 * store (MinIO). Proves the append-only, latest-wins store cycle end-to-end in seconds — no
 * ETL pipeline, no large dataset. Requires AWS_ENDPOINT_OVERRIDE + credentials in the env
 * (set by sourcing govdata/.env.dq); skipped otherwise.
 */
@Tag("integration")
public class S3HivePeriodMarkerTest {

  @Test
  @EnabledIfEnvironmentVariable(named = "AWS_ENDPOINT_OVERRIDE", matches = ".+")
  void markCompleteInvalidateRemarkCycle() {
    Map<String, String> config = new HashMap<String, String>();
    config.put("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
    config.put("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
    config.put("region", "us-east-1");
    String bucket = System.getenv("GOVDATA_DQ_TRACKER_BUCKET");
    if (bucket == null || bucket.isEmpty()) {
      bucket = "govdata-tracker-v1-dq";
    }
    String prefix = "s3://" + bucket + "/__fix2_period_marker_test__";
    S3HivePipelineTracker tracker =
        new S3HivePipelineTracker(prefix, System.getenv("AWS_ENDPOINT_OVERRIDE"), config);

    // Unique pipeline name so repeated runs never see each other's markers.
    String pipeline = "ittest_pipeline_" + System.nanoTime();
    Map<String, String> y2099 = Collections.singletonMap("year", "2099");
    Map<String, String> y2098 = Collections.singletonMap("year", "2098");

    // 1. A fresh period is not complete.
    assertFalse(tracker.isPeriodComplete(pipeline, y2099), "fresh period must not be complete");

    // 2. After marking complete (+flush so write-behind is visible), it IS complete...
    tracker.markPeriodComplete(pipeline, y2099);
    tracker.flushPending();
    assertTrue(tracker.isPeriodComplete(pipeline, y2099), "after markPeriodComplete must be complete");
    // ...and a DIFFERENT period is unaffected (the whole point: per-period, not per-table).
    assertFalse(tracker.isPeriodComplete(pipeline, y2098), "other period must be unaffected");

    // 3. Appending an invalidate marker wins (latest-wins) -> not complete.
    tracker.invalidatePeriod(pipeline, y2099);
    tracker.flushPending();
    assertFalse(tracker.isPeriodComplete(pipeline, y2099), "after invalidatePeriod must not be complete");

    // 4. Re-marking complete wins again (append-only, latest-wins).
    tracker.markPeriodComplete(pipeline, y2099);
    tracker.flushPending();
    assertTrue(tracker.isPeriodComplete(pipeline, y2099), "re-mark must be complete again");
  }

  /**
   * Proves the core fix #2 rule — "a period is complete ONLY when ALL its combos are done in
   * the run" — using the exact tracker primitives the pipeline's markCompletedPeriods relies
   * on (markProcessed/filterUnprocessed gate the markPeriodComplete). A period with a non-period
   * dimension (geography) fanned across combos must NOT be marked complete after a partial run.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "AWS_ENDPOINT_OVERRIDE", matches = ".+")
  void periodMarkedOnlyWhenAllCombosDone() {
    Map<String, String> config = new HashMap<String, String>();
    config.put("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
    config.put("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
    config.put("region", "us-east-1");
    String bucket = System.getenv("GOVDATA_DQ_TRACKER_BUCKET");
    if (bucket == null || bucket.isEmpty()) {
      bucket = "govdata-tracker-v1-dq";
    }
    S3HivePipelineTracker tracker = new S3HivePipelineTracker(
        "s3://" + bucket + "/__fix2_period_marker_test__",
        System.getenv("AWS_ENDPOINT_OVERRIDE"), config);

    String pipe = "ittest_rule_" + System.nanoTime();
    // Two periods, each fanned across a non-period dim (geography). This mirrors census
    // ([type, year, geography]): the per-period (year) marker must compose with the per-combo
    // tracker so finishing one geography does NOT mark the whole year complete.
    Map<String, String> ca99 = combo("2099", "CA");
    Map<String, String> ny99 = combo("2099", "NY");
    Map<String, String> ca98 = combo("2098", "CA");
    Map<String, String> ny98 = combo("2098", "NY");

    // Simulate a partial run with REAL (non-empty) results — markProcessedWithRowCount(>0),
    // exactly as the pipeline does. (markProcessed alone records 0 rows, which filterUnprocessed
    // correctly treats as retry-able/empty-result, so it would not count as done.)
    // ALL of 2099's combos processed, but only ONE of 2098's.
    tracker.markProcessedWithRowCount(pipe, pipe, ca99, "t", 100);
    tracker.markProcessedWithRowCount(pipe, pipe, ny99, "t", 100);
    tracker.markProcessedWithRowCount(pipe, pipe, ca98, "t", 100); // ny98 deliberately NOT processed
    tracker.flushPending();

    List<Map<String, String>> group99 = new ArrayList<Map<String, String>>();
    group99.add(ca99);
    group99.add(ny99);
    List<Map<String, String>> group98 = new ArrayList<Map<String, String>>();
    group98.add(ca98);
    group98.add(ny98);

    // The gate markCompletedPeriods actually uses: a period is done iff EVERY one of its combos
    // is isProcessed (the per-combo read that matches how marks were written — consistent,
    // unlike the year-scan cache filterUnprocessed consults).
    assertTrue(allProcessed(tracker, pipe, group99),
        "2099: all combos processed -> eligible to mark");
    assertFalse(allProcessed(tracker, pipe, group98),
        "2098: a combo (ny98) is unprocessed -> NOT eligible to mark");

    // Apply the rule exactly as markCompletedPeriods does: mark only the fully-done period.
    if (allProcessed(tracker, pipe, group99)) {
      tracker.markPeriodComplete(pipe, ca99);
    }
    if (allProcessed(tracker, pipe, group98)) {
      tracker.markPeriodComplete(pipe, ca98); // must NOT happen — 2098 is partial
    }
    tracker.flushPending();

    assertTrue(tracker.isPeriodComplete(pipe, ca99),
        "2099 fully done -> period complete (would be skipped next run)");
    assertFalse(tracker.isPeriodComplete(pipe, ca98),
        "2098 partial -> period NOT complete (would be reprocessed next run)");
  }

  /** Mirrors the markCompletedPeriods gate: a period's full combo set is done iff every
   * combo reads back isProcessed. */
  private static boolean allProcessed(S3HivePipelineTracker tracker, String pipe,
      List<Map<String, String>> group) {
    for (Map<String, String> combo : group) {
      if (!tracker.isProcessed(pipe, pipe, combo)) {
        return false;
      }
    }
    return true;
  }

  private static Map<String, String> combo(String year, String geography) {
    Map<String, String> m = new LinkedHashMap<String, String>();
    m.put("year", year);
    m.put("geography", geography);
    return m;
  }
}
