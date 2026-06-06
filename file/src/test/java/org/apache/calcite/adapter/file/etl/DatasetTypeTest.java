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

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for Phase 4 behavioral: {@code dataset_type} and {@code backfill_period}
 * in {@link EtlPipeline}.
 *
 * <p>Tests {@link EtlPipeline#enrichWithPeriodBounds} directly (static method),
 * and exercises the {@code snapshot} gate via an instrumented pipeline.
 */
@Tag("unit")
class DatasetTypeTest {

  @TempDir
  Path tempDir;

  // ===== enrichWithPeriodBounds unit tests (static, no I/O) =====

  @Test void annualBoundsFullYear() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(vars, "annual");
    assertEquals("2024-01-01", enriched.get("period_start"));
    assertEquals("2024-12-31", enriched.get("period_end"));
  }

  @Test void quarterlyBoundsQ1() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    vars.put("quarter", "1");
    Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(vars, "quarterly");
    assertEquals("2024-01-01", enriched.get("period_start"));
    assertEquals("2024-03-31", enriched.get("period_end"));
  }

  @Test void quarterlyBoundsQ2() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    vars.put("quarter", "2");
    Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(vars, "quarterly");
    assertEquals("2024-04-01", enriched.get("period_start"));
    assertEquals("2024-06-30", enriched.get("period_end"));
  }

  @Test void quarterlyBoundsQ3() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    vars.put("quarter", "3");
    Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(vars, "quarterly");
    assertEquals("2024-07-01", enriched.get("period_start"));
    assertEquals("2024-09-30", enriched.get("period_end"));
  }

  @Test void quarterlyBoundsQ4() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    vars.put("quarter", "4");
    Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(vars, "quarterly");
    assertEquals("2024-10-01", enriched.get("period_start"));
    assertEquals("2024-12-31", enriched.get("period_end"));
  }

  @Test void monthlyBoundsFebruary2024() {
    // Feb 2024 is a leap year — 29 days
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    vars.put("month", "2");
    Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(vars, "monthly");
    assertEquals("2024-02-01", enriched.get("period_start"));
    assertEquals("2024-02-29", enriched.get("period_end"));
  }

  @Test void monthlyBoundsFebruary2023() {
    // Feb 2023 is not a leap year — 28 days
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2023");
    vars.put("month", "2");
    Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(vars, "monthly");
    assertEquals("2023-02-01", enriched.get("period_start"));
    assertEquals("2023-02-28", enriched.get("period_end"));
  }

  @Test void monthlyBoundsJanuary() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    vars.put("month", "1");
    Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(vars, "monthly");
    assertEquals("2024-01-01", enriched.get("period_start"));
    assertEquals("2024-01-31", enriched.get("period_end"));
  }

  @Test void weeklyBoundsIso() {
    // ISO week 10 of 2024 — Monday 2024-03-04, Sunday 2024-03-10
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    vars.put("week", "10");
    Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(vars, "weekly");
    assertEquals("2024-03-04", enriched.get("period_start"));
    assertEquals("2024-03-10", enriched.get("period_end"));
    // period_end must be 6 days after period_start
    LocalDate start = LocalDate.parse(enriched.get("period_start"));
    LocalDate end = LocalDate.parse(enriched.get("period_end"));
    assertEquals(6, end.toEpochDay() - start.toEpochDay());
  }

  @Test void dailyBounds() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    vars.put("month", "3");
    vars.put("day", "15");
    Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(vars, "daily");
    assertEquals("2024-03-15", enriched.get("period_start"));
    assertEquals("2024-03-15", enriched.get("period_end"));
  }

  @Test void nullBackfillPeriodReturnsOriginal() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    Map<String, String> result = EtlPipeline.enrichWithPeriodBounds(vars, null);
    // Must be the exact same map (no copy)
    assertNull(result.get("period_start"));
    assertNull(result.get("period_end"));
  }

  @Test void emptyBackfillPeriodReturnsOriginal() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    Map<String, String> result = EtlPipeline.enrichWithPeriodBounds(vars, "");
    assertNull(result.get("period_start"));
  }

  @Test void missingYearReturnsOriginal() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("quarter", "2");
    Map<String, String> result = EtlPipeline.enrichWithPeriodBounds(vars, "annual");
    assertNull(result.get("period_start"),
        "Without year, enrichment must not throw and must return original");
  }

  @Test void quarterlyMissingQuarterReturnsOriginal() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    // No "quarter" key
    Map<String, String> result = EtlPipeline.enrichWithPeriodBounds(vars, "quarterly");
    assertNull(result.get("period_start"),
        "Missing quarter: must return original without period_start");
  }

  @Test void enrichmentDoesNotMutateOriginal() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    vars.put("quarter", "1");
    Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(vars, "quarterly");
    // Original must not be modified
    assertNull(vars.get("period_start"),
        "Original variables map must not be mutated by enrichment");
    assertNotNull(enriched.get("period_start"),
        "Enriched copy must have period_start");
  }

  @Test void existingVariablesPreservedInEnrichedCopy() {
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2024");
    vars.put("quarter", "2");
    vars.put("geography", "US");
    Map<String, String> enriched = EtlPipeline.enrichWithPeriodBounds(vars, "quarterly");
    assertEquals("US", enriched.get("geography"),
        "Non-period variables must be preserved in the enriched copy");
    assertNotNull(enriched.get("period_start"));
  }

  // ===== snapshot dataset_type: only most-recent combo is processed =====

  /**
   * Tracking DataProvider that records which "year" values it was called with.
   */
  static final class RecordingDataProvider implements DataProvider {
    final List<String> fetchedYears = new ArrayList<String>();

    @Override public Iterator<Map<String, Object>> fetch(
        EtlPipelineConfig config, Map<String, String> variables) {
      String year = variables.get("year");
      if (year != null) {
        fetchedYears.add(year);
      }
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", 1);
      return Collections.<Map<String, Object>>singletonList(row).iterator();
    }
  }

  static final class NoOpDataWriter implements DataWriter {
    @Override public long write(EtlPipelineConfig config,
        Iterator<Map<String, Object>> data, Map<String, String> variables) {
      long count = 0;
      while (data.hasNext()) {
        data.next();
        count++;
      }
      return count;
    }
  }

  private EtlPipelineConfig buildConfigWithType(String datasetType) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    // Use YEAR_RANGE which emits descending (newest first) — the real use case for snapshot.
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2020)
        .end(2024)
        .build());

    EtlPipelineConfig.Builder b = EtlPipelineConfig.builder()
        .name("dataset_type_test")
        .source(HttpSourceConfig.builder()
            .url("https://example.invalid/api")
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build());
    if (datasetType != null) {
      b.datasetType(datasetType);
    }
    return b.build();
  }

  /**
   * With {@code dataset_type: snapshot}, only the SINGLE most-recent combination
   * is processed, not the full year range 2020–2024.
   *
   * <p>YEAR_RANGE emits descending (newest first), so the most-recent = 2024.
   */
  @Test void snapshotProcessesOnlyMostRecentCombination() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    RecordingDataProvider provider = new RecordingDataProvider();
    NoOpDataWriter writer = new NoOpDataWriter();

    EtlPipelineConfig config = buildConfigWithType("snapshot");

    EtlPipeline pipeline = new EtlPipeline(
        config, sp, tempDir.toString(), null, IncrementalTracker.NOOP, provider, writer);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(1, provider.fetchedYears.size(),
        "snapshot must process exactly ONE combination (the most-recent)");
    // The most-recent year in a YEAR_RANGE 2020-2024 descending is 2024
    assertEquals("2024", provider.fetchedYears.get(0),
        "snapshot must use the most-recent year");
  }

  /**
   * With {@code dataset_type: delta} (default), all combinations in the range are processed.
   * Back-compat: this is today's behavior.
   */
  @Test void deltaDefaultProcessesAllCombinations() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    RecordingDataProvider provider = new RecordingDataProvider();
    NoOpDataWriter writer = new NoOpDataWriter();

    EtlPipelineConfig config = buildConfigWithType("delta");

    EtlPipeline pipeline = new EtlPipeline(
        config, sp, tempDir.toString(), null, IncrementalTracker.NOOP, provider, writer);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // 5 years: 2020, 2021, 2022, 2023, 2024
    assertEquals(5, provider.fetchedYears.size(),
        "delta must process all 5 combinations (2020-2024)");
  }

  /**
   * Without {@code dataset_type} (null/absent in YAML), the default is {@code delta} —
   * existing behavior unchanged (back-compat).
   */
  @Test void noDatasetTypeDefaultsToDelta() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    RecordingDataProvider provider = new RecordingDataProvider();
    NoOpDataWriter writer = new NoOpDataWriter();

    EtlPipelineConfig config = buildConfigWithType(null);
    // Verify default
    assertEquals("delta", config.getDatasetType(),
        "Absent dataset_type must default to 'delta'");

    EtlPipeline pipeline = new EtlPipeline(
        config, sp, tempDir.toString(), null, IncrementalTracker.NOOP, provider, writer);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(5, provider.fetchedYears.size(),
        "default (delta) must process all combinations — back-compat");
  }

  // ===== backfill_period: period_start/period_end injected into variables =====

  /**
   * Capturing DataProvider that records the variables it receives (including
   * injected period_start/period_end).
   */
  static final class VariableCaptureProvider implements DataProvider {
    final List<Map<String, String>> capturedVars = new ArrayList<Map<String, String>>();

    @Override public Iterator<Map<String, Object>> fetch(
        EtlPipelineConfig config, Map<String, String> variables) {
      capturedVars.add(new LinkedHashMap<String, String>(variables));
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", 1);
      return Collections.<Map<String, Object>>singletonList(row).iterator();
    }
  }

  private EtlPipelineConfig buildConfigWithBackfillPeriod(String backfillPeriod) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2024)
        .end(2024) // Single year to keep the test focused
        .build());

    return EtlPipelineConfig.builder()
        .name("backfill_test")
        .source(HttpSourceConfig.builder()
            .url("https://example.invalid/api")
            .build())
        .dimensions(dims)
        .backfillPeriod(backfillPeriod)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();
  }

  /**
   * When {@code backfill_period: annual} is set, each batch's variables must
   * contain {@code period_start=YYYY-01-01} and {@code period_end=YYYY-12-31}.
   */
  @Test void annualBackfillPeriodInjectsCorrectBounds() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    VariableCaptureProvider provider = new VariableCaptureProvider();
    NoOpDataWriter writer = new NoOpDataWriter();

    EtlPipelineConfig config = buildConfigWithBackfillPeriod("annual");

    EtlPipeline pipeline = new EtlPipeline(
        config, sp, tempDir.toString(), null, IncrementalTracker.NOOP, provider, writer);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(1, provider.capturedVars.size(), "Single year=2024 combo");
    Map<String, String> vars = provider.capturedVars.get(0);
    assertEquals("2024-01-01", vars.get("period_start"),
        "annual backfill_period must inject period_start=YYYY-01-01");
    assertEquals("2024-12-31", vars.get("period_end"),
        "annual backfill_period must inject period_end=YYYY-12-31");
  }

  /**
   * When {@code backfill_period} is null (absent), no period bounds are injected —
   * back-compat guarantee.
   */
  @Test void nullBackfillPeriodDoesNotInjectBounds() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    VariableCaptureProvider provider = new VariableCaptureProvider();
    NoOpDataWriter writer = new NoOpDataWriter();

    EtlPipelineConfig config = buildConfigWithBackfillPeriod(null);

    EtlPipeline pipeline = new EtlPipeline(
        config, sp, tempDir.toString(), null, IncrementalTracker.NOOP, provider, writer);

    pipeline.execute();

    assertEquals(1, provider.capturedVars.size());
    assertNull(provider.capturedVars.get(0).get("period_start"),
        "Without backfill_period, period_start must NOT be injected");
    assertNull(provider.capturedVars.get(0).get("period_end"),
        "Without backfill_period, period_end must NOT be injected");
  }
}
