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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the freshness skip-gate in {@link EtlPipeline} (Phase 3).
 *
 * <p>Uses a stub {@link IncrementalTracker} implementation that persists freshness
 * tokens in memory, and a stub {@link DataProvider} that counts how many times it
 * was invoked, so we can assert that a skip produces zero fetches.
 */
@Tag("unit")
class FreshnessSkipGateTest {

  @TempDir
  Path tempDir;

  // ===== In-memory tracker that supports freshness tokens =====

  /**
   * Minimal in-memory {@link IncrementalTracker} that supports freshness-token
   * round-trips. All other operations delegate to the same no-op semantics as
   * {@link IncrementalTracker#NOOP} so the pipeline can run to completion.
   */
  static final class MemoryFreshnessTracker implements IncrementalTracker {

    private final Map<String, String> freshnessTokens = new ConcurrentHashMap<String, String>();

    @Override public String getFreshnessToken(String pipelineName) {
      return freshnessTokens.get(pipelineName);
    }

    @Override public void putFreshnessToken(String pipelineName, String token) {
      if (token != null) {
        freshnessTokens.put(pipelineName, token);
      }
    }

    /** Test accessor: snapshot of all currently-stored token keys. */
    Set<String> tokenKeys() {
      return new HashSet<String>(freshnessTokens.keySet());
    }

    /** Test accessor: remove a single token (used to isolate the per-unit gate). */
    void removeToken(String key) {
      freshnessTokens.remove(key);
    }

    // --- Everything else is NOOP ---

    @Override public boolean isProcessed(String alternateName, String sourceTable,
        Map<String, String> keyValues) {
      return false;
    }

    @Override public boolean isProcessedWithTtl(String alternateName, String sourceTable,
        Map<String, String> keyValues, long ttlMillis) {
      return false;
    }

    @Override public void markProcessed(String alternateName, String sourceTable,
        Map<String, String> keyValues, String targetPattern) {
      // no-op
    }

    @Override public Set<Map<String, String>> getProcessedKeyValues(String alternateName) {
      return Collections.emptySet();
    }

    @Override public void invalidate(String alternateName, Map<String, String> keyValues) {
      // no-op
    }

    @Override public void invalidateAll(String alternateName) {
      // no-op
    }

    @Override public Set<Integer> filterUnprocessed(String alternateName, String sourceTable,
        List<Map<String, String>> allCombinations) {
      Set<Integer> all = new HashSet<Integer>();
      for (int i = 0; i < allCombinations.size(); i++) {
        all.add(i);
      }
      return all;
    }

    @Override public boolean isTableComplete(String pipelineName, String dimensionSignature) {
      return false;
    }

    @Override public void markTableComplete(String pipelineName, String dimensionSignature) {
      // no-op
    }

    @Override public void invalidateTableCompletion(String pipelineName) {
      // no-op
    }

    @Override public void clearAllCompletions() {
      // no-op
    }
  }

  // ===== Freshness token round-trip (tracker only, no pipeline) =====

  @Test void testFreshnessTokenNullOnFirstRun() {
    MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();
    assertNull(tracker.getFreshnessToken("my_pipeline"),
        "First run: no token should be stored yet");
  }

  @Test void testFreshnessTokenRoundTrip() {
    MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();
    tracker.putFreshnessToken("my_pipeline", "etag-abc123");
    assertEquals("etag-abc123", tracker.getFreshnessToken("my_pipeline"));
  }

  @Test void testFreshnessTokenOverwrite() {
    MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();
    tracker.putFreshnessToken("p", "v1");
    tracker.putFreshnessToken("p", "v2");
    assertEquals("v2", tracker.getFreshnessToken("p"));
  }

  @Test void testFreshnessTokenIndependentPerPipeline() {
    MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();
    tracker.putFreshnessToken("p1", "token-a");
    tracker.putFreshnessToken("p2", "token-b");
    assertEquals("token-a", tracker.getFreshnessToken("p1"));
    assertEquals("token-b", tracker.getFreshnessToken("p2"));
    assertNull(tracker.getFreshnessToken("p3"));
  }

  @Test void testPutNullTokenIsIgnored() {
    MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();
    tracker.putFreshnessToken("p", null);
    assertNull(tracker.getFreshnessToken("p"),
        "Putting null should not store anything");
  }

  // ===== Skip-gate wired via EtlPipeline =====

  /**
   * Builds a minimal {@link EtlPipelineConfig} with a single-year dimension and
   * the given freshness config attached. The source URL is a fake URL that won't
   * be contacted because we override the data source with a stub DataProvider.
   */
  private EtlPipelineConfig buildConfig(FreshnessConfig freshness) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2024)
        .end(2024)
        .build());

    EtlPipelineConfig.Builder b = EtlPipelineConfig.builder()
        .name("freshness_test_pipeline")
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
    if (freshness != null) {
      b.freshness(freshness);
    }
    return b.build();
  }

  /**
   * Builds a config with a YEAR_RANGE period dimension (so {@code hasPeriodDimension} is true and
   * the per-unit freshness gate is active) spanning {@code [startYear, endYear]}.
   */
  private EtlPipelineConfig buildPeriodConfig(FreshnessConfig freshness, int startYear, int endYear) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(startYear)
        .end(endYear)
        .build());

    EtlPipelineConfig.Builder b = EtlPipelineConfig.builder()
        .name("freshness_test_pipeline")
        .source(HttpSourceConfig.builder()
            .url("https://example.invalid/api/{year}")
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build());
    if (freshness != null) {
      b.freshness(freshness);
    }
    return b.build();
  }

  /**
   * Stub {@link DataProvider} that counts fetch calls and can be configured to
   * return a fixed set of rows.
   */
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

  /**
   * Stub {@link DataWriter} that counts write calls.
   */
  static final class CountingDataWriter implements DataWriter {
    final AtomicInteger writeCount = new AtomicInteger();
    final List<String> writtenYears = new ArrayList<String>();

    @Override public long write(EtlPipelineConfig config,
        Iterator<Map<String, Object>> data, Map<String, String> variables) {
      writeCount.incrementAndGet();
      writtenYears.add(variables.get("year"));
      long count = 0;
      while (data.hasNext()) {
        data.next();
        count++;
      }
      return count;
    }
  }

  /**
   * {@link DataProvider} whose returned rows vary by the {@code year} fetch variable, so a test
   * can change ONE period's content between runs while leaving another period's content
   * identical — simulating a genuine upstream revision of a single period rather than a
   * first-ever fetch (previousToken == null) or a wholesale content change of every period.
   */
  static final class VaryingDataProvider implements DataProvider {
    final AtomicInteger fetchCount = new AtomicInteger();
    final Map<String, List<Map<String, Object>>> rowsByYear;

    VaryingDataProvider(Map<String, List<Map<String, Object>>> rowsByYear) {
      this.rowsByYear = rowsByYear;
    }

    @Override public Iterator<Map<String, Object>> fetch(
        EtlPipelineConfig config, Map<String, String> variables) {
      fetchCount.incrementAndGet();
      List<Map<String, Object>> rows = rowsByYear.get(variables.get("year"));
      return new ArrayList<Map<String, Object>>(rows).iterator();
    }
  }

  /**
   * Stub {@link HttpSource} subclass that serves a fixed {@link ProbeResult} without
   * hitting the network. Allows testing the skip-gate without a live server.
   *
   * <p>Because {@link HttpSource} is not an interface, we extend it and override
   * {@link #probe} to return the injected result. The {@link #fetch} path is never
   * called in the skip scenario (the pipeline returns early), so we don't need to
   * override that.
   */
  static final class StubHttpSource extends HttpSource {
    private final HttpSource.ProbeResult probeResult;

    StubHttpSource(HttpSourceConfig cfg, HttpSource.ProbeResult probeResult) {
      super(cfg);
      this.probeResult = probeResult;
    }

    @Override public HttpSource.ProbeResult probe(FreshnessConfig freshnessCfg,
        Map<String, String> variables) {
      return probeResult;
    }
  }

  /**
   * DataProvider that always uses the injected StubHttpSource for probing.
   * The probe itself doesn't go through DataProvider, but we need a way to
   * inject a stub HttpSource into the pipeline for the Phase 3b probe path.
   * We do this by overriding createDataSource via a special pipeline subclass.
   */

  /**
   * EtlPipeline subclass that injects a stub HttpSource so the freshness probe
   * returns a controlled ProbeResult without any network call.
   */
  static final class StubProbePipeline extends EtlPipeline {
    private final StubHttpSource stubSource;

    StubProbePipeline(EtlPipelineConfig config, StorageProvider storageProvider,
        String baseDirectory, IncrementalTracker tracker,
        DataProvider dataProvider, DataWriter dataWriter,
        StubHttpSource stubSource) {
      super(config, storageProvider, baseDirectory, null, tracker,
          dataProvider, dataWriter);
      this.stubSource = stubSource;
    }

    @Override protected DataSource createDataSource(EtlPipelineConfig config,
        boolean forceReprocessAll) {
      return stubSource;
    }
  }

  /**
   * When the token is unchanged (prev == cur), the pipeline must return early
   * without calling the DataProvider's fetch() at all.
   */
  @Test void testUnchangedTokenCausesSkip() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();

    // Pre-seed the tracker with a known token
    tracker.putFreshnessToken("freshness_test_pipeline", "etag-xyz");

    // The probe will return the same token → unchanged
    Map<String, String> probeHeaders = new HashMap<String, String>();
    probeHeaders.put("ETag", "etag-xyz");
    HttpSource.ProbeResult probeResult = new HttpSource.ProbeResult(probeHeaders, null);

    FreshnessConfig freshnessConfig = FreshnessConfig.fromMap(
        Collections.<String, Object>singletonMap("type", "etag"));

    CountingDataProvider provider = new CountingDataProvider(
        Collections.<Map<String, Object>>emptyList());
    CountingDataWriter writer = new CountingDataWriter();

    StubHttpSource stubSource = new StubHttpSource(
        HttpSourceConfig.builder().url("https://example.invalid/api").build(),
        probeResult);

    EtlPipelineConfig config = buildConfig(freshnessConfig);

    StubProbePipeline pipeline = new StubProbePipeline(
        config, sp, tempDir.toString(), tracker, provider, writer, stubSource);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // Pipeline should have skipped — DataProvider.fetch() must not be called
    assertEquals(0, provider.fetchCount.get(),
        "DataProvider.fetch() must not be called when token is unchanged");
    assertEquals(0, writer.writeCount.get(),
        "DataWriter.write() must not be called when token is unchanged");
    // The skip means 1 combination was skipped (year=2024)
    assertEquals(1, result.getSkippedBatches());
  }

  /**
   * When there is no previous token (first run), the pipeline must proceed with
   * the full fetch — FreshnessCheck.changed(null, anything) == true.
   */
  @Test void testFirstRunProceedsWhenNoPreviousToken() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();
    // No token seeded — first run

    Map<String, String> probeHeaders = new HashMap<String, String>();
    probeHeaders.put("ETag", "etag-new");
    HttpSource.ProbeResult probeResult = new HttpSource.ProbeResult(probeHeaders, null);

    FreshnessConfig freshnessConfig = FreshnessConfig.fromMap(
        Collections.<String, Object>singletonMap("type", "etag"));

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    CountingDataProvider provider = new CountingDataProvider(
        Collections.singletonList(row));
    CountingDataWriter writer = new CountingDataWriter();

    StubHttpSource stubSource = new StubHttpSource(
        HttpSourceConfig.builder().url("https://example.invalid/api").build(),
        probeResult);

    EtlPipelineConfig config = buildConfig(freshnessConfig);

    StubProbePipeline pipeline = new StubProbePipeline(
        config, sp, tempDir.toString(), tracker, provider, writer, stubSource);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    // Pipeline should have fetched — first run always proceeds
    assertTrue(provider.fetchCount.get() > 0,
        "DataProvider.fetch() must be called on first run (no previous token)");
  }

  /**
   * When the token changes (prev != cur), the pipeline must proceed and then
   * persist the new token so the next run can compare.
   */
  @Test void testChangedTokenProceedsAndPersistsNewToken() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();

    // Seed with an old token
    tracker.putFreshnessToken("freshness_test_pipeline", "etag-old");

    // Probe returns a different (new) token
    Map<String, String> probeHeaders = new HashMap<String, String>();
    probeHeaders.put("ETag", "etag-new");
    HttpSource.ProbeResult probeResult = new HttpSource.ProbeResult(probeHeaders, null);

    FreshnessConfig freshnessConfig = FreshnessConfig.fromMap(
        Collections.<String, Object>singletonMap("type", "etag"));

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    CountingDataProvider provider = new CountingDataProvider(
        Collections.singletonList(row));
    CountingDataWriter writer = new CountingDataWriter();

    StubHttpSource stubSource = new StubHttpSource(
        HttpSourceConfig.builder().url("https://example.invalid/api").build(),
        probeResult);

    EtlPipelineConfig config = buildConfig(freshnessConfig);

    StubProbePipeline pipeline = new StubProbePipeline(
        config, sp, tempDir.toString(), tracker, provider, writer, stubSource);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(provider.fetchCount.get() > 0,
        "DataProvider.fetch() must be called when token changed");
    // New token must be persisted after the run
    assertEquals("etag-new", tracker.getFreshnessToken("freshness_test_pipeline"),
        "New token must be persisted after a successful run");
  }

  /**
   * Per-period freshness gate (the "freshness OR period-completion OR both" composition).
   *
   * <p>The pipeline-level gate probes a single non-templated URL with an empty variable map, so it
   * is a no-op for period tables whose source URL is templated. The per-unit gate in
   * processSingleBatch probes each fetch unit's substituted URL and skips that unit when unchanged.
   *
   * <p>This test isolates the per-unit gate from the pipeline-level gate: it runs a two-year
   * pipeline once to seed per-unit tokens, then REMOVES the pipeline-level token (so the
   * pipeline-level gate proceeds, prev=null) while keeping the per-unit tokens, and asserts the
   * second run fetches nothing — proving the skip came from the per-unit gate, not the
   * pipeline-level one.
   */
  @Test void testPerPeriodFreshnessSkipsUnchangedUnits() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();

    Map<String, String> probeHeaders = new HashMap<String, String>();
    probeHeaders.put("ETag", "etag-stable");
    HttpSource.ProbeResult probeResult = new HttpSource.ProbeResult(probeHeaders, null);

    FreshnessConfig freshnessConfig = FreshnessConfig.fromMap(
        Collections.<String, Object>singletonMap("type", "etag"));

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    CountingDataProvider provider = new CountingDataProvider(Collections.singletonList(row));
    CountingDataWriter writer = new CountingDataWriter();

    StubHttpSource stubSource = new StubHttpSource(
        HttpSourceConfig.builder().url("https://example.invalid/api/{year}").build(),
        probeResult);

    // Two periods → two fetch units.
    EtlPipelineConfig config = buildPeriodConfig(freshnessConfig, 2023, 2024);

    // --- First run: no tokens. Both periods fetch; per-unit tokens get persisted. ---
    StubProbePipeline run1 = new StubProbePipeline(
        config, sp, tempDir.toString(), tracker, provider, writer, stubSource);
    run1.execute();
    assertEquals(2, provider.fetchCount.get(),
        "First run must fetch both periods (no tokens yet)");

    Set<String> keys = tracker.tokenKeys();
    List<String> perUnitKeys = new ArrayList<String>();
    for (String k : keys) {
      if (k.contains("::")) {
        perUnitKeys.add(k);
      }
    }
    assertEquals(2, perUnitKeys.size(),
        "First run must persist a per-unit freshness token for each period (got " + keys + ")");

    // --- Isolate the per-unit gate: drop the pipeline-level token, keep per-unit tokens. ---
    tracker.removeToken(config.getName());
    assertNull(tracker.getFreshnessToken(config.getName()),
        "Pipeline-level token removed so the pipeline-level gate cannot be the cause of the skip");

    // --- Second run: per-unit tokens unchanged → every unit skips via the per-unit gate. ---
    provider.fetchCount.set(0);
    writer.writeCount.set(0);
    StubProbePipeline run2 = new StubProbePipeline(
        config, sp, tempDir.toString(), tracker, provider, writer, stubSource);
    run2.execute();
    assertEquals(0, provider.fetchCount.get(),
        "Second run must skip every period via the per-unit freshness gate (no fetch)");
    assertEquals(0, writer.writeCount.get(),
        "Second run must not write — every period skipped");
  }

  /**
   * HASH-type freshness with {@code trailing_window} set must scope its post-download token
   * PER FETCH UNIT, not per pipeline — otherwise processing year=2023 then year=2024 in the same
   * run would compare 2024's hash against 2023's just-stored hash (a single pipeline-scoped
   * token), always see "different", and always write, even when neither year's content actually
   * changed since the last run.
   *
   * <p>Runs a two-year (2023, 2024) HASH+trailing_window pipeline twice with IDENTICAL fetched
   * data both times. The first run writes both years (no prior tokens). The second run must write
   * NEITHER year — proving each year's hash was compared against its OWN prior token, not a
   * cross-contaminated shared one.
   */
  @Test void testHashFreshnessScopedPerUnitWhenTrailingWindowSet() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();

    Map<String, Object> hashFreshnessMap = new HashMap<String, Object>();
    hashFreshnessMap.put("type", "hash");
    hashFreshnessMap.put("trailing_window", 2);
    FreshnessConfig freshnessConfig = FreshnessConfig.fromMap(hashFreshnessMap);

    // Probe result is irrelevant for HASH (post-download only; HttpSource#probe returns an empty
    // result for HASH and the pre-download token path always passes a null `content`), but a
    // StubHttpSource is still required so `dataSource instanceof HttpSource` gates the hash check.
    HttpSource.ProbeResult probeResult = new HttpSource.ProbeResult(new HashMap<String, String>(), null);
    StubHttpSource stubSource = new StubHttpSource(
        HttpSourceConfig.builder().url("https://example.invalid/api/{year}").build(),
        probeResult);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("value", "stable");
    CountingDataProvider provider = new CountingDataProvider(Collections.singletonList(row));
    CountingDataWriter writer = new CountingDataWriter();

    EtlPipelineConfig config = buildPeriodConfig(freshnessConfig, 2023, 2024);

    // --- First run: no per-unit tokens yet — both years must be written. ---
    StubProbePipeline run1 = new StubProbePipeline(
        config, sp, tempDir.toString(), tracker, provider, writer, stubSource);
    run1.execute();
    assertEquals(2, writer.writeCount.get(),
        "First run must write both years (no prior hash tokens)");

    Set<String> perUnitKeys = new HashSet<String>();
    for (String k : tracker.tokenKeys()) {
      if (k.contains("::")) {
        perUnitKeys.add(k);
      }
    }
    assertEquals(2, perUnitKeys.size(),
        "Each year must persist its OWN hash token (got " + tracker.tokenKeys() + ")");

    // --- Second run: identical data for both years — neither should be re-written. ---
    writer.writeCount.set(0);
    StubProbePipeline run2 = new StubProbePipeline(
        config, sp, tempDir.toString(), tracker, provider, writer, stubSource);
    run2.execute();
    assertEquals(0, writer.writeCount.get(),
        "Second run must skip writing both years — each year's hash is unchanged from its OWN "
            + "prior token, not a cross-contaminated pipeline-scoped one");
  }

  /**
   * The complement to {@link #testHashFreshnessScopedPerUnitWhenTrailingWindowSet}: that test
   * only proves "no prior token → write" and "identical content → skip". Neither proves the
   * scenario trailing_window exists for — an upstream agency REVISING a period's data (e.g. BLS
   * restating a prior year in its February benchmark) — because "no prior token" is a first-ever
   * fetch, not a changed re-fetch of a period already held.
   *
   * <p>Seeds two years with stable content (both write, tokens persist), then re-runs with 2023's
   * content changed and 2024's left identical, and asserts exactly 2023 is rewritten while 2024
   * is still skipped — proving a genuine per-unit content change is detected and overwrites only
   * the affected partition, not the whole table.
   */
  @Test void testHashFreshnessDetectsChangedYearAndLeavesOtherUnchanged() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();

    Map<String, Object> hashFreshnessMap = new HashMap<String, Object>();
    hashFreshnessMap.put("type", "hash");
    hashFreshnessMap.put("trailing_window", 2);
    FreshnessConfig freshnessConfig = FreshnessConfig.fromMap(hashFreshnessMap);

    HttpSource.ProbeResult probeResult = new HttpSource.ProbeResult(new HashMap<String, String>(), null);
    StubHttpSource stubSource = new StubHttpSource(
        HttpSourceConfig.builder().url("https://example.invalid/api/{year}").build(),
        probeResult);

    EtlPipelineConfig config = buildPeriodConfig(freshnessConfig, 2023, 2024);

    Map<String, Object> stableRow = new HashMap<String, Object>();
    stableRow.put("id", 1);
    stableRow.put("value", "stable");

    Map<String, List<Map<String, Object>>> initialRows =
        new HashMap<String, List<Map<String, Object>>>();
    initialRows.put("2023", Collections.singletonList(stableRow));
    initialRows.put("2024", Collections.singletonList(stableRow));

    // --- First run: no prior tokens — both years write, seeding a hash token each. ---
    VaryingDataProvider provider1 = new VaryingDataProvider(initialRows);
    CountingDataWriter writer1 = new CountingDataWriter();
    new StubProbePipeline(config, sp, tempDir.toString(), tracker, provider1, writer1, stubSource)
        .execute();
    assertEquals(2, writer1.writeCount.get(), "First run must write both years");

    // --- Second run: 2023's content actually changed (a revision); 2024 is untouched. ---
    Map<String, Object> revisedRow = new HashMap<String, Object>();
    revisedRow.put("id", 1);
    revisedRow.put("value", "revised");

    Map<String, List<Map<String, Object>>> revisedRows =
        new HashMap<String, List<Map<String, Object>>>();
    revisedRows.put("2023", Collections.singletonList(revisedRow));
    revisedRows.put("2024", Collections.singletonList(stableRow));

    VaryingDataProvider provider2 = new VaryingDataProvider(revisedRows);
    CountingDataWriter writer2 = new CountingDataWriter();
    new StubProbePipeline(config, sp, tempDir.toString(), tracker, provider2, writer2, stubSource)
        .execute();

    assertEquals(1, writer2.writeCount.get(),
        "Second run must write exactly ONE year — the one whose content actually changed");
    assertEquals(Collections.singletonList("2023"), writer2.writtenYears,
        "The rewritten year must be 2023 (the changed one), not 2024 (unchanged, must stay "
            + "skipped) — proves the hash-changed branch fires per-unit, not table-wide");
  }

  /**
   * When {@code freshness} is absent (null), the pipeline runs unconditionally —
   * preserving today's behavior (back-compat).
   */
  @Test void testNoFreshnessConfigRunsUnconditionally() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    CountingDataProvider provider = new CountingDataProvider(
        Collections.singletonList(row));
    CountingDataWriter writer = new CountingDataWriter();

    // No freshness config — build a plain HttpSource (not a stub)
    EtlPipelineConfig config = buildConfig(null); // null = no freshness

    // Use normal EtlPipeline (no stub source) with the custom provider/writer
    EtlPipeline pipeline = new EtlPipeline(
        config, sp, tempDir.toString(), null, tracker, provider, writer);

    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertTrue(provider.fetchCount.get() > 0,
        "Without freshness config, pipeline must always fetch");
  }
}
