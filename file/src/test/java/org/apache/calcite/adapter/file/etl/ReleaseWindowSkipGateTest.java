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
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@code releaseWindow:} skip-gate in {@link EtlPipeline#execute()}.
 *
 * <p>Mirrors {@link FreshnessSkipGateTest}'s harness: a minimal {@link EtlPipelineConfig}, a
 * stub {@link DataProvider}/{@link DataWriter} pair that counts invocations, and
 * {@link IncrementalTracker#NOOP} (the release-window gate runs before any tracker work, so
 * NOOP is sufficient). "Today" is injected via a {@link EtlPipeline#currentDate()} override
 * rather than depending on wall-clock {@code now()}, so assertions never depend on the date the
 * test happens to run.
 */
@Tag("unit")
class ReleaseWindowSkipGateTest {

  @TempDir
  Path tempDir;

  // 2024-08-15 is a Thursday (dow=4), even year, month=8.
  private static final LocalDate THURSDAY_AUG_2024 = LocalDate.of(2024, Month.AUGUST, 15);
  // 2024-03-10 is a Sunday (dow=0), even year, month=3.
  private static final LocalDate SUNDAY_MARCH_2024 = LocalDate.of(2024, Month.MARCH, 10);
  // 2025-08-14 is a Thursday, odd year.
  private static final LocalDate THURSDAY_AUG_2025 = LocalDate.of(2025, Month.AUGUST, 14);

  /** Stub {@link DataProvider} that counts fetch calls. */
  static final class CountingDataProvider implements DataProvider {
    final AtomicInteger fetchCount = new AtomicInteger();
    final List<Map<String, Object>> rowsToReturn;

    CountingDataProvider(List<Map<String, Object>> rowsToReturn) {
      this.rowsToReturn = rowsToReturn;
    }

    @Override public Iterator<Map<String, Object>> fetch(
        EtlPipelineConfig config, Map<String, String> variables) {
      fetchCount.incrementAndGet();
      return new ArrayList<Map<String, Object>>(rowsToReturn).iterator();
    }
  }

  /** Stub {@link DataWriter} that counts write calls. */
  static final class CountingDataWriter implements DataWriter {
    final AtomicInteger writeCount = new AtomicInteger();

    @Override public long write(EtlPipelineConfig config,
        Iterator<Map<String, Object>> data, Map<String, String> variables) {
      writeCount.incrementAndGet();
      long count = 0;
      while (data.hasNext()) {
        data.next();
        count++;
      }
      return count;
    }
  }

  /** {@link EtlPipeline} subclass that injects a fixed "today" for the release-window gate. */
  static final class FixedDatePipeline extends EtlPipeline {
    private final LocalDate fixedToday;

    FixedDatePipeline(EtlPipelineConfig config, StorageProvider storageProvider,
        String baseDirectory, IncrementalTracker tracker,
        DataProvider dataProvider, DataWriter dataWriter,
        LocalDate fixedToday, boolean ignoreReleaseWindow) {
      super(config, storageProvider, null, baseDirectory, null, tracker,
          dataProvider, dataWriter, null, ignoreReleaseWindow);
      this.fixedToday = fixedToday;
    }

    @Override protected LocalDate currentDate() {
      return fixedToday;
    }
  }

  private EtlPipelineConfig buildConfig(ReleaseWindowConfig releaseWindow) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2024)
        .end(2024)
        .build());

    EtlPipelineConfig.Builder b = EtlPipelineConfig.builder()
        .name("release_window_test_pipeline")
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
    if (releaseWindow != null) {
      b.releaseWindow(releaseWindow);
    }
    return b.build();
  }

  // ===== months constraint =====

  @Test void testMonthsInWindowProceeds() throws IOException {
    ReleaseWindowConfig window = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("months", Arrays.asList(7, 8, 9, 10)));
    EtlPipelineConfig config = buildConfig(window);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    CountingDataProvider provider = new CountingDataProvider(Collections.singletonList(row));
    CountingDataWriter writer = new CountingDataWriter();

    FixedDatePipeline pipeline = new FixedDatePipeline(config, new LocalFileStorageProvider(),
        tempDir.toString(), IncrementalTracker.NOOP, provider, writer, THURSDAY_AUG_2024, false);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(provider.fetchCount.get() > 0,
        "August is within [7,8,9,10] — pipeline must proceed and fetch");
  }

  @Test void testMonthsOutOfWindowSkips() throws IOException {
    ReleaseWindowConfig window = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("months", Arrays.asList(7, 8, 9, 10)));
    EtlPipelineConfig config = buildConfig(window);

    CountingDataProvider provider = new CountingDataProvider(
        Collections.<Map<String, Object>>emptyList());
    CountingDataWriter writer = new CountingDataWriter();

    FixedDatePipeline pipeline = new FixedDatePipeline(config, new LocalFileStorageProvider(),
        tempDir.toString(), IncrementalTracker.NOOP, provider, writer, SUNDAY_MARCH_2024, false);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertEquals(0, provider.fetchCount.get(),
        "March is outside [7,8,9,10] — pipeline must skip without fetching");
    assertEquals(0, writer.writeCount.get());
    assertEquals(1, result.getSkippedBatches());
  }

  // ===== dow constraint =====

  @Test void testDowInWindowProceeds() throws IOException {
    ReleaseWindowConfig window = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("dow", Collections.singletonList(4))); // Thu
    EtlPipelineConfig config = buildConfig(window);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    CountingDataProvider provider = new CountingDataProvider(Collections.singletonList(row));
    CountingDataWriter writer = new CountingDataWriter();

    FixedDatePipeline pipeline = new FixedDatePipeline(config, new LocalFileStorageProvider(),
        tempDir.toString(), IncrementalTracker.NOOP, provider, writer, THURSDAY_AUG_2024, false);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(provider.fetchCount.get() > 0, "Thursday matches dow=[4] — must proceed");
  }

  @Test void testDowOutOfWindowSkips() throws IOException {
    // Only Sunday (dow=0) allowed; THURSDAY_AUG_2024 (dow=4) is not a run day.
    ReleaseWindowConfig window = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("dow", Collections.singletonList(0)));
    EtlPipelineConfig config = buildConfig(window);

    CountingDataProvider provider = new CountingDataProvider(
        Collections.<Map<String, Object>>emptyList());
    CountingDataWriter writer = new CountingDataWriter();

    FixedDatePipeline pipeline = new FixedDatePipeline(config, new LocalFileStorageProvider(),
        tempDir.toString(), IncrementalTracker.NOOP, provider, writer, THURSDAY_AUG_2024, false);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertEquals(0, provider.fetchCount.get(),
        "Thursday does not satisfy dow=[0] (Sunday only) — pipeline must skip");
    assertEquals(1, result.getSkippedBatches());
  }

  // ===== yearParity constraint =====

  @Test void testYearParityOddProceedsOnOddYear() throws IOException {
    ReleaseWindowConfig window = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("yearParity", "odd"));
    EtlPipelineConfig config = buildConfig(window);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    CountingDataProvider provider = new CountingDataProvider(Collections.singletonList(row));
    CountingDataWriter writer = new CountingDataWriter();

    FixedDatePipeline pipeline = new FixedDatePipeline(config, new LocalFileStorageProvider(),
        tempDir.toString(), IncrementalTracker.NOOP, provider, writer, THURSDAY_AUG_2025, false);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(provider.fetchCount.get() > 0, "2025 is odd — yearParity=odd must proceed");
  }

  @Test void testYearParityEvenSkipsOnOddYear() throws IOException {
    ReleaseWindowConfig window = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("yearParity", "even"));
    EtlPipelineConfig config = buildConfig(window);

    CountingDataProvider provider = new CountingDataProvider(
        Collections.<Map<String, Object>>emptyList());
    CountingDataWriter writer = new CountingDataWriter();

    FixedDatePipeline pipeline = new FixedDatePipeline(config, new LocalFileStorageProvider(),
        tempDir.toString(), IncrementalTracker.NOOP, provider, writer, THURSDAY_AUG_2025, false);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertEquals(0, provider.fetchCount.get(), "2025 is odd — yearParity=even must skip");
    assertEquals(1, result.getSkippedBatches());
  }

  // ===== combined constraints (months + dow both set; each independently gates) =====

  @Test void testCombinedConstraintsBothSatisfiedProceeds() throws IOException {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("months", Arrays.asList(7, 8, 9, 10));
    map.put("dow", Collections.singletonList(4)); // Thursday
    ReleaseWindowConfig window = ReleaseWindowConfig.fromMap(map);
    EtlPipelineConfig config = buildConfig(window);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    CountingDataProvider provider = new CountingDataProvider(Collections.singletonList(row));
    CountingDataWriter writer = new CountingDataWriter();

    FixedDatePipeline pipeline = new FixedDatePipeline(config, new LocalFileStorageProvider(),
        tempDir.toString(), IncrementalTracker.NOOP, provider, writer, THURSDAY_AUG_2024, false);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(provider.fetchCount.get() > 0,
        "August + Thursday satisfies both months and dow — must proceed");
  }

  @Test void testCombinedConstraintsDowAloneFailsGatesRun() throws IOException {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("months", Arrays.asList(7, 8, 9, 10)); // August satisfies this
    map.put("dow", Collections.singletonList(0)); // Sunday only — Thursday fails this
    ReleaseWindowConfig window = ReleaseWindowConfig.fromMap(map);
    EtlPipelineConfig config = buildConfig(window);

    CountingDataProvider provider = new CountingDataProvider(
        Collections.<Map<String, Object>>emptyList());
    CountingDataWriter writer = new CountingDataWriter();

    FixedDatePipeline pipeline = new FixedDatePipeline(config, new LocalFileStorageProvider(),
        tempDir.toString(), IncrementalTracker.NOOP, provider, writer, THURSDAY_AUG_2024, false);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertEquals(0, provider.fetchCount.get(),
        "dow constraint alone must gate even though months is satisfied");
  }

  // ===== no releaseWindow configured =====

  @Test void testNoReleaseWindowConfigAlwaysProceeds() throws IOException {
    EtlPipelineConfig config = buildConfig(null); // no releaseWindow: block

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    CountingDataProvider provider = new CountingDataProvider(Collections.singletonList(row));
    CountingDataWriter writer = new CountingDataWriter();

    // Even a date wildly outside any plausible window must proceed, since there is no gate.
    FixedDatePipeline pipeline = new FixedDatePipeline(config, new LocalFileStorageProvider(),
        tempDir.toString(), IncrementalTracker.NOOP, provider, writer, SUNDAY_MARCH_2024, false);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(provider.fetchCount.get() > 0,
        "No releaseWindow: configured — pipeline must always proceed (default/back-compat)");
  }

  // ===== ignoreReleaseWindow bypass =====

  @Test void testIgnoreReleaseWindowBypassesOtherwiseFailingCheck() throws IOException {
    ReleaseWindowConfig window = ReleaseWindowConfig.fromMap(
        Collections.<String, Object>singletonMap("months", Arrays.asList(7, 8, 9, 10)));
    EtlPipelineConfig config = buildConfig(window);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    CountingDataProvider provider = new CountingDataProvider(Collections.singletonList(row));
    CountingDataWriter writer = new CountingDataWriter();

    // March is outside [7,8,9,10], but ignoreReleaseWindow=true must force a proceed.
    FixedDatePipeline pipeline = new FixedDatePipeline(config, new LocalFileStorageProvider(),
        tempDir.toString(), IncrementalTracker.NOOP, provider, writer, SUNDAY_MARCH_2024, true);

    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertTrue(provider.fetchCount.get() > 0,
        "ignoreReleaseWindow=true must bypass an otherwise-failing window check");
    assertEquals(0, result.getSkippedBatches());
  }
}
