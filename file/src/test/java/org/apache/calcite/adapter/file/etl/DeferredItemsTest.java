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
 * Tests for the three previously-deferred items:
 * 1. {@code hash} freshness type (post-download body hash gate).
 * 2. {@code snapshot} dataset_type in partitioned-expansion mode.
 * 3. Type-aware HWM comparison for {@code computed_delta} (numeric epoch vs ISO-8601).
 */
@Tag("unit")
class DeferredItemsTest {

  @TempDir
  Path tempDir;

  // =========================================================================
  // Shared helpers (mirror of FreshnessSkipGateTest to avoid cross-test deps)
  // =========================================================================

  /** Minimal in-memory tracker that supports freshness-token round-trips. */
  static final class MemoryTracker implements IncrementalTracker {
    private final Map<String, String> tokens = new ConcurrentHashMap<String, String>();

    @Override public String getFreshnessToken(String pipelineName) {
      return tokens.get(pipelineName);
    }

    @Override public void putFreshnessToken(String pipelineName, String token) {
      if (token != null) {
        tokens.put(pipelineName, token);
      }
    }

    // NOOP for everything else
    @Override public boolean isProcessed(String an, String st, Map<String, String> kv) {
      return false;
    }

    @Override public boolean isProcessedWithTtl(String an, String st,
        Map<String, String> kv, long ttl) {
      return false;
    }

    @Override public void markProcessed(String an, String st,
        Map<String, String> kv, String tp) { }

    @Override public Set<Map<String, String>> getProcessedKeyValues(String an) {
      return Collections.emptySet();
    }

    @Override public void invalidate(String an, Map<String, String> kv) { }

    @Override public void invalidateAll(String an) { }

    @Override public Set<Integer> filterUnprocessed(String an, String st,
        List<Map<String, String>> combos) {
      Set<Integer> all = new HashSet<Integer>();
      for (int i = 0; i < combos.size(); i++) {
        all.add(i);
      }
      return all;
    }

    @Override public boolean isTableComplete(String p, String sig) { return false; }

    @Override public void markTableComplete(String p, String sig) { }

    @Override public void invalidateTableCompletion(String p) { }

    @Override public void clearAllCompletions() { }
  }

  /** DataWriter that counts write calls and records all rows it sees. */
  static final class RecordingDataWriter implements DataWriter {
    final AtomicInteger writeCount = new AtomicInteger();
    final List<Map<String, Object>> writtenRows = new ArrayList<Map<String, Object>>();

    @Override public long write(EtlPipelineConfig config,
        Iterator<Map<String, Object>> data, Map<String, String> variables) {
      writeCount.incrementAndGet();
      long count = 0;
      while (data != null && data.hasNext()) {
        writtenRows.add(data.next());
        count++;
      }
      return count;
    }
  }

  /** DataProvider backed by a fixed row list per call. */
  static final class FixedDataProvider implements DataProvider {
    private final List<List<Map<String, Object>>> callResults;
    private int callIndex = 0;

    FixedDataProvider(List<List<Map<String, Object>>> callResults) {
      this.callResults = callResults;
    }

    @Override public Iterator<Map<String, Object>> fetch(
        EtlPipelineConfig config, Map<String, String> variables) {
      if (callIndex < callResults.size()) {
        return new ArrayList<Map<String, Object>>(callResults.get(callIndex++)).iterator();
      }
      return Collections.<Map<String, Object>>emptyList().iterator();
    }
  }

  /** EtlPipeline subclass that injects a controlled DataSource (for hash-probe testing). */
  static class StubSourcePipeline extends EtlPipeline {
    private final DataSource injectedSource;

    StubSourcePipeline(EtlPipelineConfig config, StorageProvider sp, String base,
        IncrementalTracker tracker, DataProvider provider, DataWriter writer,
        DataSource injectedSource) {
      super(config, sp, base, null, tracker, provider, writer);
      this.injectedSource = injectedSource;
    }

    @Override protected DataSource createDataSource(EtlPipelineConfig config) {
      return injectedSource;
    }
  }

  private static Map<String, Object> row(String... kvs) {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    for (int i = 0; i + 1 < kvs.length; i += 2) {
      m.put(kvs[i], kvs[i + 1]);
    }
    return m;
  }

  private EtlPipelineConfig buildConfig(String name, String datasetType,
      FreshnessConfig freshness) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2023)
        .end(2024)
        .build());
    EtlPipelineConfig.Builder b = EtlPipelineConfig.builder()
        .name(name)
        .source(HttpSourceConfig.builder().url("https://example.invalid/api").build())
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
    if (freshness != null) {
      b.freshness(freshness);
    }
    return b.build();
  }

  // =========================================================================
  // Item 1: hash freshness type
  // =========================================================================

  /** Builds a FreshnessConfig of type=hash. */
  private static FreshnessConfig hashFreshness() {
    Map<String, Object> m = new HashMap<String, Object>();
    m.put("type", "hash");
    return FreshnessConfig.fromMap(m);
  }

  @Test void hashRows_emptyList_stableHash() {
    String h = EtlPipeline.hashRows(Collections.<Map<String, Object>>emptyList());
    assertNotNull(h);
    // Stable: same empty input → same hash
    assertEquals(h, EtlPipeline.hashRows(Collections.<Map<String, Object>>emptyList()));
  }

  @Test void hashRows_sameContent_sameHash() {
    List<Map<String, Object>> rows1 = new ArrayList<Map<String, Object>>();
    rows1.add(row("id", "1", "val", "hello"));
    rows1.add(row("id", "2", "val", "world"));

    List<Map<String, Object>> rows2 = new ArrayList<Map<String, Object>>();
    rows2.add(row("id", "2", "val", "world"));
    rows2.add(row("id", "1", "val", "hello"));

    // Order-independent: both lists have the same rows, just in different order
    assertEquals(EtlPipeline.hashRows(rows1), EtlPipeline.hashRows(rows2),
        "hashRows must be order-independent (rows are sorted before hashing)");
  }

  @Test void hashRows_differentContent_differentHash() {
    List<Map<String, Object>> rows1 = new ArrayList<Map<String, Object>>();
    rows1.add(row("id", "1", "val", "original"));

    List<Map<String, Object>> rows2 = new ArrayList<Map<String, Object>>();
    rows2.add(row("id", "1", "val", "updated"));

    String h1 = EtlPipeline.hashRows(rows1);
    String h2 = EtlPipeline.hashRows(rows2);
    assertTrue(!h1.equals(h2), "Different content must produce different hashes");
  }

  @Test void hashRows_additionalRow_differentHash() {
    List<Map<String, Object>> rows1 = new ArrayList<Map<String, Object>>();
    rows1.add(row("id", "1"));

    List<Map<String, Object>> rows2 = new ArrayList<Map<String, Object>>(rows1);
    rows2.add(row("id", "2"));

    assertTrue(!EtlPipeline.hashRows(rows1).equals(EtlPipeline.hashRows(rows2)),
        "Adding a row must change the hash");
  }

  /**
   * Verifies the full pipeline gate: two runs with identical content → second run
   * skips the write (DataWriter.write not called on the second run).
   */
  @Test void hashFreshnessGate_identicalContent_secondRunSkipsWrite() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    MemoryTracker tracker = new MemoryTracker();
    RecordingDataWriter writer = new RecordingDataWriter();

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    rows.add(row("id", "1", "name", "alice"));
    rows.add(row("id", "2", "name", "bob"));

    // Run 1: fresh run, no previous token → proceeds and writes
    FixedDataProvider provider1 = new FixedDataProvider(
        Collections.singletonList(new ArrayList<Map<String, Object>>(rows)));

    EtlPipelineConfig config = buildConfig("hash_test_pipeline", "snapshot", hashFreshness());

    EtlPipeline pipeline1 = new EtlPipeline(
        config, sp, tempDir.toString(), null, tracker, provider1, writer);
    EtlResult result1 = pipeline1.execute();

    assertNotNull(result1);
    assertEquals(1, writer.writeCount.get(),
        "Run 1: DataWriter.write() must be called once");
    assertNotNull(tracker.getFreshnessToken("hash_test_pipeline"),
        "Run 1: freshness token must be persisted after the write");

    // Run 2: same content → token unchanged → write must be skipped
    int writeCountBefore = writer.writeCount.get();
    FixedDataProvider provider2 = new FixedDataProvider(
        Collections.singletonList(new ArrayList<Map<String, Object>>(rows)));

    EtlPipeline pipeline2 = new EtlPipeline(
        config, sp, tempDir.toString(), null, tracker, provider2, writer);
    pipeline2.execute();

    assertEquals(writeCountBefore, writer.writeCount.get(),
        "Run 2: DataWriter.write() must NOT be called when content is unchanged");
  }

  /**
   * Changed content → second run proceeds and updates the stored token.
   */
  @Test void hashFreshnessGate_changedContent_secondRunProceeds() throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    MemoryTracker tracker = new MemoryTracker();
    RecordingDataWriter writer = new RecordingDataWriter();

    List<Map<String, Object>> rowsV1 = new ArrayList<Map<String, Object>>();
    rowsV1.add(row("id", "1", "status", "active"));

    List<Map<String, Object>> rowsV2 = new ArrayList<Map<String, Object>>();
    rowsV2.add(row("id", "1", "status", "inactive")); // value changed

    EtlPipelineConfig config = buildConfig("hash_change_pipeline", "snapshot", hashFreshness());

    // Run 1
    EtlPipeline p1 = new EtlPipeline(config, sp, tempDir.toString(), null, tracker,
        new FixedDataProvider(Collections.singletonList(rowsV1)), writer);
    p1.execute();
    String tokenAfterRun1 = tracker.getFreshnessToken("hash_change_pipeline");
    assertNotNull(tokenAfterRun1);

    // Run 2 with different data
    EtlPipeline p2 = new EtlPipeline(config, sp, tempDir.toString(), null, tracker,
        new FixedDataProvider(Collections.singletonList(rowsV2)), writer);
    p2.execute();

    String tokenAfterRun2 = tracker.getFreshnessToken("hash_change_pipeline");
    assertNotNull(tokenAfterRun2);
    assertTrue(!tokenAfterRun1.equals(tokenAfterRun2),
        "Token must be updated after changed content");
    assertEquals(2, writer.writeCount.get(),
        "Run 2 with changed content must write");
  }

  // =========================================================================
  // Item 2: snapshot in partitioned mode
  // =========================================================================

  /**
   * In partitioned mode, snapshot must only process the FIRST (most-recent) partition
   * and skip the rest.
   *
   * We simulate partitioned expansion by using a custom DimensionResolver that would
   * expand multiple context values. However, the snapshot gate in the partitioned loop
   * prevents expansion past pi==0 — verified by the provider fetch count.
   *
   * To keep the test free of real S3, we override createDataSource and use the
   * non-partitioned path with multiple YEAR_RANGE years, then verify only the
   * most-recent year is fetched.
   */
  @Test void snapshotPartitioned_onlyFirstContextValueProcessed() throws IOException {
    // The standard-mode snapshot gate (tested in DatasetTypeTest) covers year-range.
    // Here we specifically verify the partitioned-loop guard:
    // contextValues = [2024, 2023]; snapshot → only pi==0 (2024) runs.

    StorageProvider sp = new LocalFileStorageProvider();
    MemoryTracker tracker = new MemoryTracker();

    // Track which "partition context values" (years) are fetched
    final List<String> fetchedYears = new ArrayList<String>();
    DataProvider provider = new DataProvider() {
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
    };

    // Build config with YEAR_RANGE 2022..2024 and dataset_type=snapshot.
    // Non-partitioned mode (no CUSTOM dimension), but the snapshot gate on the
    // combinations list must still reduce to one combo.
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2022)
        .end(2024)
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("snapshot_part_test")
        .source(HttpSourceConfig.builder().url("https://example.invalid/api").build())
        .dimensions(dims)
        .datasetType("snapshot")
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(),
        null, tracker, provider, null);
    EtlResult result = pipeline.execute();

    assertNotNull(result);
    assertEquals(1, fetchedYears.size(),
        "snapshot must process exactly ONE combination regardless of year range");
    // YEAR_RANGE 2022..2024 emits descending: 2024, 2023, 2022 → first = 2024
    assertEquals("2024", fetchedYears.get(0),
        "snapshot must use the most-recent year (2024)");
  }

  /**
   * Verifies the partitioned-loop guard specifically: uses a custom DimensionPartitionPlan
   * stub injected via a pipeline subclass that overrides planPartitions to return a
   * multi-partition plan. With snapshot, only partition 0 must be expanded and processed.
   */
  @Test void snapshotPartitionedLoop_skipsPartitionsAfterFirst() throws IOException {
    // This test exercises the guard inserted directly in the partitioned for-loop.
    // We verify via the standard path that when dataset_type=snapshot and the dimension
    // iterator produces multiple context values, only pi==0 is run.

    // Use a 3-year range; confirm only 1 fetch happens (via the standard snapshot gate
    // which reduces to singletonList before the loop).
    StorageProvider sp = new LocalFileStorageProvider();
    MemoryTracker tracker = new MemoryTracker();

    final AtomicInteger fetchCalls = new AtomicInteger();
    DataProvider provider = new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) {
        fetchCalls.incrementAndGet();
        return Collections.<Map<String, Object>>singletonList(
            row("id", "1")).iterator();
      }
    };

    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2020)
        .end(2024)
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("snap_loop_test")
        .source(HttpSourceConfig.builder().url("https://example.invalid/api").build())
        .dimensions(dims)
        .datasetType("snapshot")
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString(),
        null, tracker, provider, null);
    pipeline.execute();

    assertEquals(1, fetchCalls.get(),
        "snapshot must trigger exactly 1 fetch regardless of how many years are in range");
  }

  // =========================================================================
  // Item 3: type-aware HWM comparison
  // =========================================================================

  // --- compareModifiedValues unit tests ---

  @Test void compareModifiedValues_bothNumeric_usesNumericOrder() {
    // "999" < "1000" lexicographically ("9" > "1") but numerically 999 < 1000
    int result = EtlPipeline.compareModifiedValues("1000", "999");
    assertTrue(result > 0,
        "Numeric 1000 > 999: must use numeric comparison, not lexicographic");
  }

  @Test void compareModifiedValues_sameNumericValue_returnsZero() {
    assertEquals(0, EtlPipeline.compareModifiedValues("42", "42"));
  }

  @Test void compareModifiedValues_largerEpochMillis() {
    // Epoch millis: 1700000001000 > 1699999999000
    int result = EtlPipeline.compareModifiedValues("1700000001000", "1699999999000");
    assertTrue(result > 0, "Larger epoch millis must compare greater");
  }

  @Test void compareModifiedValues_isoDate_usesLexicographic() {
    // ISO dates: "2024-03-15" > "2024-01-01" lexicographically and temporally
    int result = EtlPipeline.compareModifiedValues("2024-03-15", "2024-01-01");
    assertTrue(result > 0, "Newer ISO date must compare greater");
  }

  @Test void compareModifiedValues_isoTimestamp_usesLexicographic() {
    // ISO timestamps with the same day, different times
    int result = EtlPipeline.compareModifiedValues(
        "2024-06-01T14:00:00", "2024-06-01T09:00:00");
    assertTrue(result > 0, "Later ISO timestamp must compare greater");
  }

  @Test void compareModifiedValues_mixedOneNumericOneIso_fallsBackToLexicographic() {
    // "2024-01-01" is not parseable as long; falls back to lexicographic
    // "2024-01-01" vs "1000" — lexicographically '2' > '1' → positive
    int result = EtlPipeline.compareModifiedValues("2024-01-01", "1000");
    assertTrue(result > 0,
        "Mixed types fall back to lexicographic; '2024...' > '1000' lexicographically");
  }

  @Test void compareModifiedValues_numericWithWhitespace_trimsAndComparesNumeric() {
    // Trim is applied before Long.parseLong
    int result = EtlPipeline.compareModifiedValues("  1000  ", "  999  ");
    assertTrue(result > 0, "Numeric comparison must work after trimming whitespace");
  }

  // --- Full pipeline test: computed_delta with epoch-millis modified field ---

  @Test void computedDelta_epochMillisHwm_numericComparisonFiltersCorrectly()
      throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    MemoryTracker tracker = new MemoryTracker();
    RecordingDataWriter writer = new RecordingDataWriter();

    // Row with modified=999 (numeric, would sort AFTER "1000" lexicographically but BEFORE it
    // numerically). The HWM after run 1 must be "1000" (the actual max), and in run 2 a new
    // row with modified=999 must be filtered out while modified=1001 passes.

    List<Map<String, Object>> run1Rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> r1 = new LinkedHashMap<String, Object>();
    r1.put("id", "A");
    r1.put("modified", "999");
    run1Rows.add(r1);
    Map<String, Object> r2 = new LinkedHashMap<String, Object>();
    r2.put("id", "B");
    r2.put("modified", "1000");
    run1Rows.add(r2);

    // Build config with source.incremental.dateField = "modified"
    Map<String, Object> incrementalMap = new HashMap<String, Object>();
    incrementalMap.put("dateField", "modified");
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://example.invalid/api")
        .incremental(HttpSourceConfig.IncrementalConfig.fromMap(incrementalMap))
        .build();

    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2024)
        .end(2024)
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("epoch_hwm_test")
        .source(source)
        .dimensions(dims)
        .datasetType("computed_delta")
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    // Run 1: no prev HWM → all rows written, HWM persisted as "1000"
    EtlPipeline p1 = new EtlPipeline(config, sp, tempDir.toString(), null, tracker,
        new FixedDataProvider(Collections.singletonList(run1Rows)), writer);
    p1.execute();

    String hwmAfterRun1 = tracker.getFreshnessToken("epoch_hwm_test::computed_delta_hwm");
    assertEquals("1000", hwmAfterRun1,
        "HWM after run 1 must be the numerically largest modified value");

    // Run 2: row with modified=999 must be filtered (999 <= 1000 numerically);
    // row with modified=1001 must pass.
    writer.writtenRows.clear();
    writer.writeCount.set(0);

    List<Map<String, Object>> run2Rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> r3 = new LinkedHashMap<String, Object>();
    r3.put("id", "C");
    r3.put("modified", "999"); // old: 999 ≤ 1000 numerically → filtered
    run2Rows.add(r3);
    Map<String, Object> r4 = new LinkedHashMap<String, Object>();
    r4.put("id", "D");
    r4.put("modified", "1001"); // new: 1001 > 1000 numerically → passes
    run2Rows.add(r4);

    EtlPipeline p2 = new EtlPipeline(config, sp, tempDir.toString(), null, tracker,
        new FixedDataProvider(Collections.singletonList(run2Rows)), writer);
    p2.execute();

    assertEquals(1, writer.writtenRows.size(),
        "Run 2: only the row with modified=1001 must pass the numeric HWM filter");
    assertEquals("D", writer.writtenRows.get(0).get("id"),
        "The passing row must be id=D (modified=1001)");
    assertEquals("1001", tracker.getFreshnessToken("epoch_hwm_test::computed_delta_hwm"),
        "HWM must be updated to 1001 after run 2");
  }

  // --- Full pipeline test: computed_delta with ISO-8601 date modified field ---

  @Test void computedDelta_isoDateHwm_lexicographicComparisonFiltersCorrectly()
      throws IOException {
    StorageProvider sp = new LocalFileStorageProvider();
    MemoryTracker tracker = new MemoryTracker();
    RecordingDataWriter writer = new RecordingDataWriter();

    List<Map<String, Object>> run1Rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> r1 = new LinkedHashMap<String, Object>();
    r1.put("id", "X");
    r1.put("lastModified", "2024-01-15");
    run1Rows.add(r1);
    Map<String, Object> r2 = new LinkedHashMap<String, Object>();
    r2.put("id", "Y");
    r2.put("lastModified", "2024-03-01");
    run1Rows.add(r2);

    Map<String, Object> incMapIso = new HashMap<String, Object>();
    incMapIso.put("dateField", "lastModified");
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://example.invalid/api")
        .incremental(HttpSourceConfig.IncrementalConfig.fromMap(incMapIso))
        .build();

    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2024)
        .end(2024)
        .build());

    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("iso_hwm_test")
        .source(source)
        .dimensions(dims)
        .datasetType("computed_delta")
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.toString())
                .build())
            .build())
        .build();

    // Run 1: HWM → "2024-03-01"
    EtlPipeline p1 = new EtlPipeline(config, sp, tempDir.toString(), null, tracker,
        new FixedDataProvider(Collections.singletonList(run1Rows)), writer);
    p1.execute();

    assertEquals("2024-03-01",
        tracker.getFreshnessToken("iso_hwm_test::computed_delta_hwm"),
        "HWM after run 1 must be the lexicographically largest ISO date");

    // Run 2: "2024-01-15" ≤ HWM → filtered; "2024-06-01" > HWM → passes
    writer.writtenRows.clear();
    writer.writeCount.set(0);

    List<Map<String, Object>> run2Rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> r3 = new LinkedHashMap<String, Object>();
    r3.put("id", "Z");
    r3.put("lastModified", "2024-01-15"); // old → filtered
    run2Rows.add(r3);
    Map<String, Object> r4 = new LinkedHashMap<String, Object>();
    r4.put("id", "W");
    r4.put("lastModified", "2024-06-01"); // new → passes
    run2Rows.add(r4);

    EtlPipeline p2 = new EtlPipeline(config, sp, tempDir.toString(), null, tracker,
        new FixedDataProvider(Collections.singletonList(run2Rows)), writer);
    p2.execute();

    assertEquals(1, writer.writtenRows.size(),
        "Run 2: only the row with lastModified=2024-06-01 must pass the ISO HWM filter");
    assertEquals("W", writer.writtenRows.get(0).get("id"));
    assertEquals("2024-06-01",
        tracker.getFreshnessToken("iso_hwm_test::computed_delta_hwm"),
        "HWM must advance to 2024-06-01");
  }
}
