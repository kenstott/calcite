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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A source that refuses every further call ({@link SourceExhaustedException}, e.g. a spent daily
 * quota) stops the table at the first refusal on every batch path, instead of failing each
 * remaining batch. A generic batch failure keeps the sequential consecutive-failure abort.
 */
@Tag("unit")
class EtlPipelineSourceExhaustedTest {

  private static final int BATCHES = 60;
  private static final int OK_CALLS = 5;

  @TempDir
  File tempDir;

  /** Tracker with every combo unprocessed; everything else is inert. */
  static final class InertTracker implements IncrementalTracker {
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

  private EtlPipelineConfig config(int parallel) {
    File warehouseDir = new File(tempDir, "warehouse" + parallel);
    warehouseDir.mkdirs();
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2000)
        .end(2000 + BATCHES - 1)
        .build());
    List<ColumnConfig> columns = Arrays.asList(
        ColumnConfig.builder().name("id").type("INTEGER").build(),
        ColumnConfig.builder().name("year").type("INTEGER").build());
    return EtlPipelineConfig.builder()
        .name("source_exhausted_test")
        .source(HttpSourceConfig.builder()
            .url("https://example.invalid/api").parallel(parallel).build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .name("source_exhausted_test")
            .targetTableId("source_exhausted_test")
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

  /** Returns a row for the first {@code okCalls} fetches, then throws {@code failure} each call. */
  private static DataProvider failingAfter(final int okCalls, final AtomicInteger calls,
      final IOException failure) {
    return new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) throws IOException {
        if (calls.incrementAndGet() > okCalls) {
          throw failure;
        }
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", 1);
        row.put("year", Integer.parseInt(variables.get("year")));
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        rows.add(row);
        return rows.iterator();
      }
    };
  }

  private EtlResult run(int parallel, DataProvider provider) throws IOException {
    File warehouseDir = new File(tempDir, "warehouse" + parallel);
    StorageProvider sp = new LocalFileStorageProvider();
    return new EtlPipeline(config(parallel), sp, warehouseDir.getAbsolutePath(), null,
        new InertTracker(), provider, null).execute();
  }

  private static void assertStoppedOnFirstRefusal(EtlResult result, AtomicInteger calls,
      int maxExtraInFlight) {
    assertTrue(result.isFailed(), "the table must fail, not report a clean run");
    String message = result.getFailureMessage();
    assertNotNull(message);
    assertTrue(message.contains("quota spent, resets 2026-11-03T00:00Z"),
        "the failure must carry the source's reset time: " + message);
    assertTrue(message.contains("batches not attempted"),
        "the failure must report the batches not attempted: " + message);
    assertTrue(calls.get() <= OK_CALLS + 1 + maxExtraInFlight,
        "no batch may be issued after the first refusal beyond those already in flight; calls="
            + calls.get() + " of " + BATCHES);
  }

  @Test void sequentialPathStopsOnFirstRefusal() throws IOException {
    AtomicInteger calls = new AtomicInteger();
    EtlResult result = run(1, failingAfter(OK_CALLS, calls,
        new SourceExhaustedException("quota spent, resets 2026-11-03T00:00Z", null)));
    assertEquals(OK_CALLS + 1, calls.get(), "sequential path stops at exactly the first refusal");
    assertStoppedOnFirstRefusal(result, calls, 0);
  }

  @Test void parallelPathStopsOnFirstRefusal() throws IOException {
    AtomicInteger calls = new AtomicInteger();
    EtlResult result = run(4, failingAfter(OK_CALLS, calls,
        new SourceExhaustedException("quota spent, resets 2026-11-03T00:00Z", null)));
    // At most (threads - 1) other batches can already be past the exhausted check.
    assertStoppedOnFirstRefusal(result, calls, 3);
  }

  @Test void sequentialGenericFailuresStillAbortAfterConsecutiveLimit() throws IOException {
    AtomicInteger calls = new AtomicInteger();
    EtlResult result = run(1, failingAfter(OK_CALLS, calls, new IOException("HTTP 500 upstream")));
    assertTrue(result.isFailed());
    assertTrue(result.getFailureMessage().contains("Aborting after 10 consecutive failures"),
        result.getFailureMessage());
    assertEquals(OK_CALLS + 10, calls.get());
  }
}
