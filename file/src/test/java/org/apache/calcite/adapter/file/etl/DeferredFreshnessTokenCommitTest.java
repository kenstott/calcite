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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The computed_delta high-water mark and the pipeline-scoped hash token tell the next run what it
 * may skip, so they reach the tracker only once the rows they cover are committed. Units of one
 * run still see the tokens recorded by the units before them.
 */
@Tag("unit")
class DeferredFreshnessTokenCommitTest {

  private static final String NAME = "deferred_token_test";
  private static final String HWM_KEY = NAME + "::computed_delta_hwm";
  private static final String HWM_2023 = HWM_KEY + "::year=2023";
  private static final String HWM_2024 = HWM_KEY + "::year=2024";

  @TempDir
  File tempDir;

  /** In-memory token store that logs every persisted token; all other tracking is inert. */
  static final class TokenTracker implements IncrementalTracker {
    final Map<String, String> tokens = new HashMap<String, String>();
    final List<String> puts = new CopyOnWriteArrayList<String>();

    @Override public String getFreshnessToken(String pipelineName) {
      return tokens.get(pipelineName);
    }

    @Override public void putFreshnessToken(String pipelineName, String token) {
      tokens.put(pipelineName, token);
      puts.add(pipelineName);
    }

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

    @Override public boolean isTableComplete(String p, String sig) {
      return false;
    }

    @Override public void markTableComplete(String p, String sig) { }

    @Override public void invalidateTableCompletion(String p) { }

    @Override public void clearAllCompletions() { }
  }

  /** Serves the stub source the HASH gate requires; rows still come from the DataProvider. */
  private static final class StubSourcePipeline extends EtlPipeline {
    StubSourcePipeline(EtlPipelineConfig config, StorageProvider storage, String baseDirectory,
        IncrementalTracker tracker, DataProvider provider) {
      super(config, storage, baseDirectory, null, tracker, provider, null);
    }

    @Override protected DataSource createDataSource(EtlPipelineConfig config,
        boolean forceReprocessAll) {
      return new HttpSource(config.getSource());
    }
  }

  @Test void computedDeltaHwmIsNotPersistedWhenTheCommitFails() throws Exception {
    TokenTracker tracker = new TokenTracker();
    EtlPipelineConfig seed = config("computed_delta", null, 2023, 2023);
    assertTrue(!run(seed, tracker, rowsModified("2023-01-01")).isFailed());
    assertEquals("2023-01-01", tracker.tokens.get(HWM_2023));

    EtlResult failed = runWithFailingCommit(config("computed_delta", null, 2024, 2024),
        tracker, rowsModified("2024-01-01"));

    assertTrue(failed == null || failed.isFailed(), "commit unexpectedly succeeded");
    assertEquals("2023-01-01", tracker.tokens.get(HWM_2023));
    assertNull(tracker.tokens.get(HWM_2024));
  }

  @Test void pipelineHashTokenIsNotPersistedWhenTheCommitFails() throws Exception {
    TokenTracker tracker = new TokenTracker();
    EtlResult seeded = run(config("snapshot", hashFreshness(), 2023, 2023), tracker,
        rowsModified("2023-01-01"));
    assertTrue(!seeded.isFailed(), String.valueOf(seeded.getFailureMessage()));
    String committed = tracker.tokens.get(NAME);

    EtlResult failed = runWithFailingCommit(config("snapshot", hashFreshness(), 2023, 2023),
        tracker, rowsModified("2024-01-01"));

    assertTrue(failed == null || failed.isFailed(), "commit unexpectedly succeeded");
    assertEquals(committed, tracker.tokens.get(NAME));
  }

  /** A unit's rows are filtered only by its own mark, never by a sibling's newer one. */
  @Test void eachUnitAdvancesOnlyItsOwnHwm() throws Exception {
    TokenTracker tracker = new TokenTracker();
    DataProvider provider = (config, variables) ->
        row(variables, "2023".equals(variables.get("year")) ? "2024-06-01" : "2025-06-01");

    EtlResult result = run(config("computed_delta", null, 2023, 2024), tracker, provider);

    assertTrue(!result.isFailed(), String.valueOf(result.getFailureMessage()));
    assertEquals(2L, result.getTotalRows());
    assertEquals("2024-06-01", tracker.tokens.get(HWM_2023));
    assertEquals("2025-06-01", tracker.tokens.get(HWM_2024));
  }

  /** A failed unit's rows older than a successful sibling's still arrive on the retry. */
  @Test void failedUnitsOlderRowsAreWrittenOnTheNextRun() throws Exception {
    TokenTracker tracker = new TokenTracker();
    EtlPipelineConfig config = config("computed_delta", null, 2023, 2024);
    DataProvider failing2023 = (c, variables) -> {
      if ("2023".equals(variables.get("year"))) {
        throw new java.io.IOException("source unavailable for 2023");
      }
      return row(variables, "2025-06-01");
    };

    EtlResult partial = run(config, tracker, failing2023);

    assertEquals(1, partial.getFailedBatches(), String.valueOf(partial.getFailureMessage()));
    assertEquals(1L, partial.getTotalRows());
    assertNull(tracker.tokens.get(HWM_2023));
    assertEquals("2025-06-01", tracker.tokens.get(HWM_2024));

    EtlResult retry = run(config, tracker, (c, variables) ->
        row(variables, "2023".equals(variables.get("year")) ? "2024-06-01" : "2025-06-01"));

    assertTrue(!retry.isFailed(), String.valueOf(retry.getFailureMessage()));
    // 2023's row is written; 2024's unchanged row is not appended a second time.
    assertEquals(1L, retry.getTotalRows());
    assertEquals("2024-06-01", tracker.tokens.get(HWM_2023));
  }

  /** A pipeline with one possible unit keys its mark by pipeline name alone. */
  @Test void singleValuedDimensionKeepsThePipelineScopedKey() {
    Map<String, DimensionConfig> dims = DimensionConfig.fromDimensionsMap(
        Collections.<String, Object>singletonMap("type", Collections.singletonList("t")));
    EtlPipelineConfig single = EtlPipelineConfig.builder()
        .name(NAME)
        .source(HttpSourceConfig.builder().url("https://example.invalid/api").build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .build();

    assertEquals(HWM_KEY,
        EtlPipeline.computedDeltaHwmKey(single, NAME, Collections.singletonMap("type", "t")));
  }

  /** With one token shared by all units, identical content in the next unit reads as unchanged. */
  @Test void laterUnitComparesAgainstTheEarlierUnitsUncommittedHash() throws Exception {
    TokenTracker tracker = new TokenTracker();
    EtlResult seeded = run(config("delta", hashFreshness(), 2023, 2023), tracker,
        rowsModified("2023-01-01"));
    assertTrue(!seeded.isFailed(), String.valueOf(seeded.getFailureMessage()));
    tracker.puts.clear();
    // Rows that hash alike for both years, so only a shared token can tell the second unit apart.
    DataProvider identical = (config, variables) -> {
      Map<String, Object> row = new HashMap<String, Object>();
      row.put("id", 2);
      return Collections.singletonList(row).iterator();
    };

    EtlResult result = run(config("delta", hashFreshness(), 2023, 2024), tracker, identical);

    assertTrue(!result.isFailed(), String.valueOf(result.getFailureMessage()));
    assertEquals(1L, result.getTotalRows());
    assertEquals(Collections.singletonList(NAME), tracker.puts);
  }

  private EtlResult run(EtlPipelineConfig config, TokenTracker tracker, DataProvider provider)
      throws Exception {
    return new StubSourcePipeline(config, new LocalFileStorageProvider(),
        tempDir.getAbsolutePath(), tracker, provider).execute();
  }

  /** Runs with the table's directories read-only, so {@code writer.commit()} cannot succeed. */
  private EtlResult runWithFailingCommit(EtlPipelineConfig config, TokenTracker tracker,
      DataProvider provider) throws Exception {
    File tableDir = new File(tempDir, NAME);
    File metadataDir = new File(tableDir, "metadata");
    assertTrue(metadataDir.isDirectory(), "no Iceberg metadata at " + metadataDir);
    Runnable restoreMetadata = EtlPipelineCommitFailureTrackerTest.denyNewFiles(metadataDir);
    Runnable restoreTable = EtlPipelineCommitFailureTrackerTest.denyNewFiles(tableDir);
    try {
      return run(config, tracker, provider);
    } finally {
      restoreTable.run();
      restoreMetadata.run();
    }
  }

  private static DataProvider rowsModified(final String modified) {
    return (config, variables) -> row(variables, modified);
  }

  private static Iterator<Map<String, Object>> row(Map<String, String> variables,
      String modified) {
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    row.put("year", Integer.parseInt(variables.get("year")));
    row.put("modified", modified);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    rows.add(row);
    return rows.iterator();
  }

  private static FreshnessConfig hashFreshness() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "hash");
    return FreshnessConfig.fromMap(map);
  }

  private EtlPipelineConfig config(String datasetType, FreshnessConfig freshness,
      int startYear, int endYear) {
    List<ColumnConfig> columns = Arrays.asList(
        ColumnConfig.builder().name("id").type("INTEGER").build(),
        ColumnConfig.builder().name("modified").type("VARCHAR").build(),
        ColumnConfig.builder().name("year").type("INTEGER").build());
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(startYear)
        .end(endYear)
        .build());
    return EtlPipelineConfig.builder()
        .name(NAME)
        .datasetType(datasetType)
        .freshness(freshness)
        .source(HttpSourceConfig.builder()
            .url("https://example.invalid/api/{year}")
            .incremental(HttpSourceConfig.IncrementalConfig.fromMap(
                Collections.<String, Object>singletonMap("dateField", "modified")))
            .build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .name(NAME)
            .targetTableId(NAME)
            .output(MaterializeOutputConfig.builder().build())
            .columns(columns)
            // Partitioning on the fetch dimension is what permits per-batch hash skipping.
            .partition(MaterializePartitionConfig.builder()
                .columns(Collections.singletonList("year"))
                .build())
            .iceberg(MaterializeConfig.IcebergConfig.builder()
                .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
                .warehousePath(tempDir.getAbsolutePath())
                .namespace("default")
                .overwritePartitions(false)
                .build())
            .build())
        .build();
  }
}
