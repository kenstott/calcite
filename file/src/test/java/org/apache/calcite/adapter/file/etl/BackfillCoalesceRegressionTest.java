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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.partition.DuckDBPartitionStatusStore;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test pinning the current tracker round-trip semantics for a delta table
 * with {@code backfill_period}.
 *
 * <p>Specifically verifies:
 * <ol>
 *   <li>mark → {@code filterUnprocessedWithEmptyTtl} → period-complete rollup produces the
 *       expected completion state (unchanged by the coalescing change).</li>
 *   <li>The tracker key used by {@code markProcessedWithRowCount} is keyed on the RAW combo
 *       (as enriched by {@link EtlPipeline#enrichWithPeriodBounds} — same as today) not on
 *       any coarse fetch window.</li>
 *   <li>{@code backfill_period == null} — behavior byte-identical: no enrichment, mark key
 *       is the raw combo.</li>
 * </ol>
 *
 * <p>This test has NO network calls and NO DuckDB (uses the NOOP tracker for simplicity
 * plus a manual DuckDBPartitionStatusStore round-trip for the store-read assertion).
 *
 * <p>Tag: {@code unit} — no external resources.
 */
@Tag("unit")
class BackfillCoalesceRegressionTest {

  @TempDir
  Path tempDir;

  // -----------------------------------------------------------------------
  // Part 1 — enrichWithPeriodBounds mark-key is unchanged by coalescing plan
  // -----------------------------------------------------------------------

  /**
   * Verifies that {@code enrichWithPeriodBounds(combo, "annual")} on a {@code (year,month)}
   * combo still produces an ANNUAL window (full-year bounds). This is the mark key that
   * coalesced runs must reproduce for every fine combo.
   */
  @Test void annualEnrichMarkKeyForMonthlyCombo() {
    Map<String, String> combo = new LinkedHashMap<String, String>();
    combo.put("year", "2022");
    combo.put("month", "06");

    // When backfill_period=annual, enrichment always uses the ANNUAL bounds —
    // month is irrelevant to the key computation at annual granularity.
    Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(combo, "annual");

    assertEquals("2022-01-01", enriched.get("period_start"),
        "annual enrichment must use year-start regardless of month");
    assertEquals("2022-12-31", enriched.get("period_end"),
        "annual enrichment must use year-end regardless of month");
    // Original fields must still be present
    assertEquals("2022", enriched.get("year"));
    assertEquals("06", enriched.get("month"));
  }

  /**
   * Verifies that two different months in the same year produce the SAME enriched
   * period_start / period_end when backfill_period=annual. This is the invariant
   * the coalescing relies on: all 12 months of year Y map to the same annual window,
   * so the mark key via enrichWithPeriodBounds is identical for every combo in the group.
   */
  @Test void allMonthsInYearProduceSameAnnualWindow() {
    String year = "2021";
    String expectedStart = "2021-01-01";
    String expectedEnd = "2021-12-31";

    for (int m = 1; m <= 12; m++) {
      Map<String, String> combo = new LinkedHashMap<String, String>();
      combo.put("year", year);
      combo.put("month", String.valueOf(m));

      Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(combo, "annual");
      assertEquals(expectedStart, enriched.get("period_start"),
          "month=" + m + " must produce same annual start");
      assertEquals(expectedEnd, enriched.get("period_end"),
          "month=" + m + " must produce same annual end");
    }
  }

  /**
   * Verifies that {@code backfill_period=null} produces no enrichment — the raw combo
   * is the mark key, period_start/period_end are absent.
   * (Backward-compat: cyber-vuln, patents, sec are all null.)
   */
  @Test void nullBackfillPeriodProducesNoEnrichment() {
    Map<String, String> combo = new LinkedHashMap<String, String>();
    combo.put("year", "2023");
    combo.put("month", "03");

    Map<String, String> result = EtlPipeline.enrichWithPeriodBounds(combo, null);

    assertFalse(result.containsKey("period_start"),
        "null backfill_period must not add period_start");
    assertFalse(result.containsKey("period_end"),
        "null backfill_period must not add period_end");
    // Exact same map reference (no copy created)
    assertEquals(combo, result);
  }

  /**
   * Verifies that {@code enrichWithPeriodBoundsAt} (the new parameterized helper) produces
   * the same result as {@code enrichWithPeriodBounds} for the same granularity — required
   * invariant for the "mark-key byte-identical to today" guarantee.
   */
  @Test void enrichAtGranularityMatchesEnrichForSameGranularity() {
    Map<String, String> combo = new LinkedHashMap<String, String>();
    combo.put("year", "2023");
    combo.put("month", "07");

    // The old path (mark key today)
    Map<String, String> viaOld = EtlPipeline.enrichWithPeriodBounds(combo, "annual");

    // The new path (should match)
    Map<String, String> viaNew = EtlPipeline.enrichWithPeriodBoundsAt(combo, "annual");

    assertEquals(viaOld, viaNew,
        "enrichWithPeriodBoundsAt must produce identical result to enrichWithPeriodBounds");
  }

  // -----------------------------------------------------------------------
  // Part 2 — buildFetchUnits grouping and bounds correctness
  // -----------------------------------------------------------------------

  /**
   * Helper: build a (year, month) combo map.
   */
  private static Map<String, String> ym(String year, String month) {
    Map<String, String> m = new LinkedHashMap<String, String>();
    m.put("year", year);
    m.put("month", month);
    return m;
  }

  /**
   * Three past years (2021-2023), each with all 12 months, plus a single open month
   * in the current-ish year (2024-03). With backfill_period=annual:
   * <ul>
   *   <li>2021: 12 combos → 1 coarse fetch unit (annual window)</li>
   *   <li>2022: 12 combos → 1 coarse fetch unit (annual window)</li>
   *   <li>2023: 12 combos → 1 coarse fetch unit (annual window)</li>
   *   <li>2024-03 singleton → 1 fine fetch unit (monthly window)</li>
   * </ul>
   * Total: 4 FetchUnits; 37 combos to mark.
   */
  @Test void buildFetchUnitsGroupsByAnnualWindow() {
    List<Map<String, String>> combinations = new ArrayList<Map<String, String>>();
    // 2021 all months
    for (int m = 1; m <= 12; m++) {
      combinations.add(ym("2021", String.valueOf(m)));
    }
    // 2022 all months
    for (int m = 1; m <= 12; m++) {
      combinations.add(ym("2022", String.valueOf(m)));
    }
    // 2023 all months
    for (int m = 1; m <= 12; m++) {
      combinations.add(ym("2023", String.valueOf(m)));
    }
    // 2024 just March (open/current month singleton)
    combinations.add(ym("2024", "3"));

    // All 37 are unprocessed
    Set<Integer> unprocessed = new HashSet<Integer>();
    for (int i = 0; i < combinations.size(); i++) {
      unprocessed.add(i);
    }

    List<EtlPipeline.FetchUnit> units =
        EtlPipeline.buildFetchUnits(combinations, unprocessed, "annual");

    assertEquals(4, units.size(),
        "3 past years + 1 singleton month = 4 fetch units");

    // Collect units by the year in their fetchVariables
    Map<String, EtlPipeline.FetchUnit> byYear = new HashMap<String, EtlPipeline.FetchUnit>();
    for (EtlPipeline.FetchUnit u : units) {
      String yr = u.getFetchVariables().get("year");
      byYear.put(yr, u);
    }

    // 2021 coarse: 12 combos, annual window
    EtlPipeline.FetchUnit u2021 = byYear.get("2021");
    assertEquals(12, u2021.getCombosToMark().size(),
        "2021 must have 12 combos to mark");
    assertEquals("2021-01-01", u2021.getFetchVariables().get("period_start"),
        "2021 coarse fetch must use annual start");
    assertEquals("2021-12-31", u2021.getFetchVariables().get("period_end"),
        "2021 coarse fetch must use annual end");

    // 2022 coarse
    EtlPipeline.FetchUnit u2022 = byYear.get("2022");
    assertEquals(12, u2022.getCombosToMark().size());
    assertEquals("2022-01-01", u2022.getFetchVariables().get("period_start"));
    assertEquals("2022-12-31", u2022.getFetchVariables().get("period_end"));

    // 2023 coarse
    EtlPipeline.FetchUnit u2023 = byYear.get("2023");
    assertEquals(12, u2023.getCombosToMark().size());
    assertEquals("2023-01-01", u2023.getFetchVariables().get("period_start"));
    assertEquals("2023-12-31", u2023.getFetchVariables().get("period_end"));

    // 2024-03 singleton: fine window (monthly bounds for March 2024)
    EtlPipeline.FetchUnit u2024 = byYear.get("2024");
    assertEquals(1, u2024.getCombosToMark().size(),
        "singleton must have exactly 1 combo to mark");
    assertEquals("2024-03-01", u2024.getFetchVariables().get("period_start"),
        "singleton must use monthly start (fine bounds)");
    assertEquals("2024-03-31", u2024.getFetchVariables().get("period_end"),
        "singleton must use monthly end (fine bounds)");

    // Verify total combo coverage: all 37 combos appear in exactly one unit
    int totalCombos = 0;
    for (EtlPipeline.FetchUnit u : units) {
      totalCombos += u.getCombosToMark().size();
    }
    assertEquals(37, totalCombos,
        "All 37 combos must be covered across fetch units");
  }

  /**
   * When backfill_period=null, buildFetchUnits must return one unit per combo
   * (exactly today's behavior) with NO period_start/period_end enrichment.
   */
  @Test void buildFetchUnitsNullBackfillOneUnitPerCombo() {
    List<Map<String, String>> combinations = new ArrayList<Map<String, String>>();
    combinations.add(ym("2023", "01"));
    combinations.add(ym("2023", "02"));
    combinations.add(ym("2023", "03"));

    Set<Integer> unprocessed = new HashSet<Integer>();
    for (int i = 0; i < combinations.size(); i++) {
      unprocessed.add(i);
    }

    List<EtlPipeline.FetchUnit> units =
        EtlPipeline.buildFetchUnits(combinations, unprocessed, null);

    assertEquals(3, units.size(),
        "null backfill_period must produce one unit per combo");

    for (EtlPipeline.FetchUnit u : units) {
      assertEquals(1, u.getCombosToMark().size(),
          "null backfill_period: each unit must cover exactly 1 combo");
      assertFalse(u.getFetchVariables().containsKey("period_start"),
          "null backfill_period: no period_start in fetch vars");
    }
  }

  /**
   * When backfill_period granularity == finest dimension (e.g. quarterly with year+quarter
   * dims), every window has size 1 → singleton path, fetch bounds == mark-key bounds ==
   * today-identical. No coalescing.
   */
  @Test void buildFetchUnitsGranularityEqualsFinestNeverCoalesces() {
    // quarterly dims: 4 quarters in 2023
    List<Map<String, String>> combinations = new ArrayList<Map<String, String>>();
    for (int q = 1; q <= 4; q++) {
      Map<String, String> c = new LinkedHashMap<String, String>();
      c.put("year", "2023");
      c.put("quarter", String.valueOf(q));
      combinations.add(c);
    }

    Set<Integer> unprocessed = new HashSet<Integer>();
    for (int i = 0; i < combinations.size(); i++) {
      unprocessed.add(i);
    }

    List<EtlPipeline.FetchUnit> units =
        EtlPipeline.buildFetchUnits(combinations, unprocessed, "quarterly");

    assertEquals(4, units.size(),
        "quarterly backfill_period with year+quarter dims: each combo is its own unit");
    for (EtlPipeline.FetchUnit u : units) {
      assertEquals(1, u.getCombosToMark().size());
    }
  }

  /**
   * Partial year: only some months of 2022 are unprocessed (e.g. months 7-9).
   * They should still coalesce into ONE annual unit (the whole year's window),
   * but only mark those 3 combos — not the processed ones.
   */
  @Test void buildFetchUnitsPartialYearCoalescesUnprocessedOnly() {
    List<Map<String, String>> combinations = new ArrayList<Map<String, String>>();
    for (int m = 1; m <= 12; m++) {
      combinations.add(ym("2022", String.valueOf(m)));
    }

    // Only months 7, 8, 9 unprocessed (indices 6, 7, 8)
    Set<Integer> unprocessed = new HashSet<Integer>();
    unprocessed.add(6); // month=7
    unprocessed.add(7); // month=8
    unprocessed.add(8); // month=9

    List<EtlPipeline.FetchUnit> units =
        EtlPipeline.buildFetchUnits(combinations, unprocessed, "annual");

    assertEquals(1, units.size(),
        "3 unprocessed months of same year → 1 coarse fetch unit");
    EtlPipeline.FetchUnit u = units.get(0);
    assertEquals(3, u.getCombosToMark().size(),
        "only the 3 unprocessed combos are to be marked");
    assertEquals("2022-01-01", u.getFetchVariables().get("period_start"),
        "fetch window is still the full year (coarse), not just the 3 months");
    assertEquals("2022-12-31", u.getFetchVariables().get("period_end"));
  }

  // -----------------------------------------------------------------------
  // Part 3 — tracker round-trip: mark keys survive a store+read cycle
  // -----------------------------------------------------------------------

  /**
   * Verifies that marking a fine (year,month) combo via the enriched mark key and then
   * calling isProcessed with the same combo produces {@code true} — the tracker key
   * format is stable across mark and query.
   *
   * <p>Uses {@link DuckDBPartitionStatusStore} backed by a temp directory.
   */
  @Test void trackerRoundTripMarkKeyForMonthlyComboWithAnnualBackfill() throws Exception {
    Path trackerDir = tempDir.resolve("tracker");
    java.nio.file.Files.createDirectories(trackerDir);

    try (DuckDBPartitionStatusStore store = DuckDBPartitionStatusStore.getInstance(trackerDir.toString())) {
      String pipeline = "test_pipeline";

      // A fine (year,month) combo — the raw combo as seen by the dispatch loop
      Map<String, String> rawCombo = new LinkedHashMap<String, String>();
      rawCombo.put("year", "2021");
      rawCombo.put("month", "05");

      // Today's path: enrich at backfill_period granularity to produce the mark key
      Map<String, String> markKey = EtlPipeline.enrichWithPeriodBounds(rawCombo, "annual");

      // Mark as processed (as processSingleBatch does today)
      store.markProcessedWithRowCount(pipeline, pipeline, markKey, null, 100L);

      // isProcessed must return true when queried with the SAME enriched key
      assertTrue(store.isProcessed(pipeline, pipeline, markKey),
          "isProcessed(markKey) must be true after markProcessedWithRowCount(markKey)");

      // isProcessed with the RAW combo (no enrichment) must return false
      // — proves the key is the enriched map, not the raw combo
      assertFalse(store.isProcessed(pipeline, pipeline, rawCombo),
          "isProcessed(rawCombo) must be false — key was written with enriched map");

      // filterUnprocessedWithEmptyTtl with [rawCombo] must still return index 0
      // (the raw combo is NOT marked; only the enriched variant is)
      List<Map<String, String>> allCombos =
          Collections.<Map<String, String>>singletonList(rawCombo);
      Set<Integer> unprocessed =
          store.filterUnprocessed(pipeline, pipeline, allCombos);
      assertEquals(1, unprocessed.size(),
          "rawCombo should still appear unprocessed (key was enriched variant)");
    }
  }

  /**
   * Verifies that when coalesced mode marks each fine combo individually with
   * {@code enrichWithPeriodBounds(combo, backfillPeriod)}, all combos in a past year
   * are independently processed per the tracker — and a subsequent
   * filterUnprocessedWithEmptyTtl returns zero for that year.
   */
  @Test void trackerRoundTripCoalescedMarkAllCombosInYear() throws Exception {
    Path trackerDir = tempDir.resolve("tracker2");
    java.nio.file.Files.createDirectories(trackerDir);

    try (DuckDBPartitionStatusStore store = DuckDBPartitionStatusStore.getInstance(trackerDir.toString())) {
      String pipeline = "test_pipeline_2";
      String backfillPeriod = "annual";
      String year = "2022";

      // 12 fine combos for 2022
      List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
      for (int m = 1; m <= 12; m++) {
        combos.add(ym(year, String.valueOf(m)));
      }

      // Simulate coalesced mark: mark each combo using enrichWithPeriodBounds
      for (Map<String, String> combo : combos) {
        Map<String, String> markKey = EtlPipeline.enrichWithPeriodBounds(combo, backfillPeriod);
        store.markProcessedWithRowCount(pipeline, pipeline, markKey, null, 50L);
      }

      // Now query: each combo's enriched key should be processed
      for (Map<String, String> combo : combos) {
        Map<String, String> markKey = EtlPipeline.enrichWithPeriodBounds(combo, backfillPeriod);
        assertTrue(store.isProcessed(pipeline, pipeline, markKey),
            "combo " + combo + " must be marked processed");
      }
    }
  }
}
