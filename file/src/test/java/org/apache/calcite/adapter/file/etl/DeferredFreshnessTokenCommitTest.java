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
    assertEquals("2023-01-01", tracker.tokens.get(HWM_KEY));

    EtlResult failed = runWithFailingCommit(config("computed_delta", null, 2024, 2024),
        tracker, rowsModified("2024-01-01"));

    assertTrue(failed == null || failed.isFailed(), "commit unexpectedly succeeded");
    assertEquals("2023-01-01", tracker.tokens.get(HWM_KEY));
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

  /** The second unit's older rows fall behind the mark the first unit recorded in this run. */
  @Test void laterUnitFiltersAgainstTheEarlierUnitsUncommittedHwm() throws Exception {
    TokenTracker tracker = new TokenTracker();
    final List<String> fetched = new ArrayList<String>();
    // Newest date for whichever unit is fetched first, so the order of the units does not matter.
    DataProvider newestFirst = (config, variables) -> {
      fetched.add(variables.get("year"));
      return row(variables, fetched.size() == 1 ? "2025-06-01" : "2024-06-01");
    };

    EtlResult result = run(config("computed_delta", null, 2023, 2024), tracker, newestFirst);

    assertTrue(!result.isFailed(), String.valueOf(result.getFailureMessage()));
    assertEquals(2, fetched.size(), "units fetched: " + fetched);
    assertEquals(1L, result.getTotalRows());
    assertEquals("2025-06-01", tracker.tokens.get(HWM_KEY));
    assertEquals(Collections.singletonList(HWM_KEY), tracker.puts);
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
