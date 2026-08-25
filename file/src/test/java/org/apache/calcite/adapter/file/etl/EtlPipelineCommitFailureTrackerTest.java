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
import java.io.IOException;
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
 * A pipeline whose final Iceberg commit fails must not leave the per-combo tracker marked
 * {@code complete} for the combos it buffered.
 *
 * <p>Found live: {@code fec.committee_contributions} was marked {@code complete} for 3 combos
 * (~1.45M rows) despite every retry of the covering Iceberg commit throwing
 * {@code AlreadyExistsException} on a stale metadata file — none of that data ever reached the
 * table, but {@code force-reprocess.sh}'s validation trusted the tracker and reported a
 * successful reprocess that never happened. This reproduces the same mechanism (a colliding
 * {@code vN.metadata.json}) against a real local Iceberg table.
 */
@Tag("unit")
class EtlPipelineCommitFailureTrackerTest {

  @TempDir
  File tempDir;

  /** Tracker that records every {@code markProcessedWithRowCount} call; everything else is inert. */
  static final class RecordingTracker implements IncrementalTracker {
    final List<Map<String, String>> processedCalls = new CopyOnWriteArrayList<Map<String, String>>();

    @Override public void markProcessedWithRowCount(String alternateName, String sourceTable,
        Map<String, String> keyValues, String targetPattern, long rowCount) {
      processedCalls.add(new LinkedHashMap<String, String>(keyValues));
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

    @Override public boolean isTableComplete(String p, String sig) { return false; }

    @Override public void markTableComplete(String p, String sig) { }

    @Override public void invalidateTableCompletion(String p) { }

    @Override public void clearAllCompletions() { }
  }

  private static EtlPipelineConfig config(File warehouseDir, Map<String, DimensionConfig> dims) {
    List<ColumnConfig> columns = Arrays.asList(
        ColumnConfig.builder().name("id").type("INTEGER").build(),
        ColumnConfig.builder().name("year").type("INTEGER").build());
    return EtlPipelineConfig.builder()
        .name("commit_failure_test")
        .source(HttpSourceConfig.builder().url("https://example.invalid/api").build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .name("commit_failure_test")
            .targetTableId("commit_failure_test")
            .output(MaterializeOutputConfig.builder().build())
            .columns(columns)
            .iceberg(MaterializeConfig.IcebergConfig.builder()
                .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
                .warehousePath(warehouseDir.getAbsolutePath())
                .namespace("default")
                .overwritePartitions(true)
                .build())
            .build())
        .build();
  }

  private static Map<String, DimensionConfig> yearDim(int year) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(year)
        .end(year)
        .build());
    return dims;
  }

  private static DataProvider fixedRow(final int id, final int year) {
    return new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) {
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", id);
        row.put("year", year);
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        rows.add(row);
        return rows.iterator();
      }
    };
  }

  private static File findMetadataDir(File warehouseDir) {
    File candidate = new File(warehouseDir, "commit_failure_test/metadata");
    assertTrue(candidate.isDirectory(),
        "expected Iceberg metadata dir at " + candidate + " after a successful run");
    return candidate;
  }

  @Test void failedCommitLeavesNoPerComboTrackerMark() throws IOException {
    File warehouseDir = new File(tempDir, "warehouse");
    warehouseDir.mkdirs();
    StorageProvider sp = new LocalFileStorageProvider();

    // First run: real success — creates the table and its first data (year=2023).
    RecordingTracker tracker = new RecordingTracker();
    EtlPipeline first = new EtlPipeline(config(warehouseDir, yearDim(2023)), sp,
        warehouseDir.getAbsolutePath(), null, tracker, fixedRow(1, 2023), null);
    first.execute();
    assertEquals(1, tracker.processedCalls.size(), "first, successful run must mark its combo");
    tracker.processedCalls.clear();

    // Make the metadata dir read-only so the second run's commit fails outright — not the exact
    // AlreadyExistsException production hit (a local Hadoop catalog resolves a colliding
    // vN.metadata.json differently than S3 does), but the same class of failure: writer.commit()
    // throws after rows were already buffered/flushed. That is the property this test guards.
    File metadataDir = findMetadataDir(warehouseDir);
    assertTrue(metadataDir.setWritable(false, false), "must be able to make metadata dir read-only");
    assertTrue(metadataDir.getParentFile().setWritable(false, false),
        "must be able to make table dir read-only");
    try {
      // Second run: a different combo (year=2024) — its commit must fail on the read-only dir.
      EtlPipeline second = new EtlPipeline(config(warehouseDir, yearDim(2024)), sp,
          warehouseDir.getAbsolutePath(), null, tracker, fixedRow(2, 2024), null);
      EtlResult result = second.execute();
      // Whether the failure surfaces as a thrown exception (caught below) or a failed EtlResult
      // depends on the errorHandling policy in play — either is fine; what matters is the
      // assertion below. Only treat it as a real problem if the run reports success outright.
      assertTrue(result == null || result.isFailed(),
          "the commit must not report success against a read-only table directory");
    } catch (Exception expected) {
      // A thrown exception is also an acceptable failure signal — see comment above.
    } finally {
      // Restore permissions so JUnit's @TempDir cleanup can delete the tree.
      metadataDir.getParentFile().setWritable(true, false);
      metadataDir.setWritable(true, false);
    }

    assertTrue(tracker.processedCalls.isEmpty(),
        "a failed commit must leave NO per-combo tracker mark for the combo it buffered — "
            + "before the fix this recorded year=2024 as complete despite the data never "
            + "reaching the table");
  }
}
