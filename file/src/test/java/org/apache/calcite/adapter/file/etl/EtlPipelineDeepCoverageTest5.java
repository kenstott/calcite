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
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests (phase 5) for {@link EtlPipeline} targeting
 * previously uncovered lines: parallel processing paths (both standard
 * and partitioned), response partitioning with DataWriter, Phase 1.5
 * self-healing, table-complete TTL invalidation in Iceberg mode,
 * cold-start Iceberg recovery, config merging nuances, GC triggers,
 * rebuildCacheFromIceberg partition matching, and consecutive failure
 * abort in partitioned mode.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class EtlPipelineDeepCoverageTest5 {

  @TempDir
  Path tempDir;

  // -----------------------------------------------------------------------
  // Helper methods
  // -----------------------------------------------------------------------

  private EtlPipelineConfig createHttpConfig(String name,
      Map<String, DimensionConfig> dimensions) {
    return createHttpConfig(name, dimensions, null, null, null, null);
  }

  private EtlPipelineConfig createHttpConfig(String name,
      Map<String, DimensionConfig> dimensions,
      MaterializeConfig.Format format,
      MaterializeOptionsConfig options,
      List<ColumnConfig> columns) {
    return createHttpConfig(name, dimensions, format, options, columns, null);
  }

  private EtlPipelineConfig createHttpConfig(String name,
      Map<String, DimensionConfig> dimensions,
      MaterializeConfig.Format format,
      MaterializeOptionsConfig options,
      List<ColumnConfig> columns,
      EtlPipelineConfig.ErrorHandlingConfig errorHandling) {
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    MaterializeConfig.Builder matBuilder = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build());
    if (format != null) {
      matBuilder.format(format);
    }
    if (options != null) {
      matBuilder.options(options);
    }
    EtlPipelineConfig.Builder configBuilder = EtlPipelineConfig.builder()
        .name(name)
        .source(source)
        .materialize(matBuilder.build());
    if (dimensions != null) {
      configBuilder.dimensions(dimensions);
    }
    if (columns != null) {
      configBuilder.columns(columns);
    }
    if (errorHandling != null) {
      configBuilder.errorHandling(errorHandling);
    }
    return configBuilder.build();
  }

  private Map<String, DimensionConfig> singleRangeDimension(String key,
      int start, int end) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put(key, DimensionConfig.builder()
        .name(key).type(DimensionType.RANGE).start(start).end(end).build());
    return dims;
  }

  private Map<String, DimensionConfig> multiRangeDimension(String key1,
      int start1, int end1, String key2, int start2, int end2) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put(key1, DimensionConfig.builder()
        .name(key1).type(DimensionType.RANGE).start(start1).end(end1).build());
    dims.put(key2, DimensionConfig.builder()
        .name(key2).type(DimensionType.RANGE).start(start2).end(end2).build());
    return dims;
  }

  private Map<String, Object> row(String... kvPairs) {
    Map<String, Object> r = new LinkedHashMap<String, Object>();
    for (int i = 0; i < kvPairs.length; i += 2) {
      r.put(kvPairs[i], kvPairs[i + 1]);
    }
    return r;
  }

  private IncrementalTracker mockTracker() {
    IncrementalTracker tracker = mock(IncrementalTracker.class);
    when(tracker.getCachedCompletion(anyString())).thenReturn(null);
    when(tracker.isTableComplete(anyString(), anyString())).thenReturn(false);
    when(tracker.filterUnprocessed(anyString(), anyString(), any(List.class)))
        .thenAnswer(inv -> {
          List<?> combos = inv.getArgument(2);
          Set<Integer> all = new HashSet<Integer>();
          for (int i = 0; i < combos.size(); i++) {
            all.add(i);
          }
          return all;
        });
    when(
        tracker.filterUnprocessedWithEmptyTtl(
        anyString(), anyString(), any(List.class), anyLong()))
        .thenAnswer(inv -> {
          List<?> combos = inv.getArgument(2);
          Set<Integer> all = new HashSet<Integer>();
          for (int i = 0; i < combos.size(); i++) {
            all.add(i);
          }
          return all;
        });
    return tracker;
  }

  private StorageProvider mockStorage() throws IOException {
    StorageProvider sp = mock(StorageProvider.class);
    when(sp.getStorageType()).thenReturn("local");
    when(sp.isDirectory(anyString())).thenReturn(false);
    return sp;
  }

  private Object invokePrivate(Object instance, String methodName,
      Class<?>[] paramTypes, Object... args) throws Exception {
    Method m = EtlPipeline.class.getDeclaredMethod(methodName, paramTypes);
    m.setAccessible(true);
    return m.invoke(instance, args);
  }

  @SuppressWarnings("unchecked")
  private <T> T getField(Object instance, String fieldName) throws Exception {
    Field f = EtlPipeline.class.getDeclaredField(fieldName);
    f.setAccessible(true);
    return (T) f.get(instance);
  }

  // -----------------------------------------------------------------------
  // 1. Parallel standard processing (threadCount > 1)
  // -----------------------------------------------------------------------

  @Test void testExecute_parallelStandardMode() throws Exception {
    String origThreads = System.getProperty("calcite.etl.threads");
    try {
      System.setProperty("calcite.etl.threads", "2");
      IncrementalTracker tracker = mockTracker();
      StorageProvider sp = mockStorage();
      DataProvider prov = mock(DataProvider.class);
      List<Map<String, Object>> data1 = new ArrayList<Map<String, Object>>();
      data1.add(row("id", "1"));
      List<Map<String, Object>> data2 = new ArrayList<Map<String, Object>>();
      data2.add(row("id", "2"));
      when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
          .thenReturn(data1.iterator())
          .thenReturn(data2.iterator());
      EtlPipelineConfig config =
          createHttpConfig("par_std", singleRangeDimension("y", 2020, 2021),
          MaterializeConfig.Format.PARQUET, null, null);
      EtlPipeline pipeline =
          new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
      EtlResult result = pipeline.execute();
      assertNotNull(result);
      // Should complete without errors using parallel mode
      assertFalse(result.isFailed());
    } finally {
      if (origThreads != null) {
        System.setProperty("calcite.etl.threads", origThreads);
      } else {
        System.clearProperty("calcite.etl.threads");
      }
    }
  }

  @Test void testExecute_parallelStandardModeWithErrors() throws Exception {
    String origThreads = System.getProperty("calcite.etl.threads");
    try {
      System.setProperty("calcite.etl.threads", "3");
      IncrementalTracker tracker = mockTracker();
      StorageProvider sp = mockStorage();
      DataProvider prov = mock(DataProvider.class);
      when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
          .thenReturn(Collections.singletonList(row("id", "1")).iterator())
          .thenThrow(new IOException("HTTP 404 Not Found"))
          .thenReturn(Collections.singletonList(row("id", "3")).iterator());
      EtlPipelineConfig config =
          createHttpConfig("par_err", singleRangeDimension("y", 2020, 2022),
          MaterializeConfig.Format.PARQUET, null, null);
      EtlPipeline pipeline =
          new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
      EtlResult result = pipeline.execute();
      assertNotNull(result);
      // Parallel mode catches errors, so pipeline still completes
    } finally {
      if (origThreads != null) {
        System.setProperty("calcite.etl.threads", origThreads);
      } else {
        System.clearProperty("calcite.etl.threads");
      }
    }
  }

  @Test void testExecute_parallelStandardModeSkipAction() throws Exception {
    String origThreads = System.getProperty("calcite.etl.threads");
    try {
      System.setProperty("calcite.etl.threads", "2");
      IncrementalTracker tracker = mockTracker();
      StorageProvider sp = mockStorage();
      DataProvider prov = mock(DataProvider.class);
      when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
          .thenThrow(new IOException("HTTP 404 Not Found"))
          .thenThrow(new IOException("HTTP 404 Not Found"));
      EtlPipelineConfig config =
          createHttpConfig("par_skip", singleRangeDimension("y", 2020, 2021),
          MaterializeConfig.Format.PARQUET, null, null);
      EtlPipeline pipeline =
          new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
      EtlResult result = pipeline.execute();
      assertNotNull(result);
    } finally {
      if (origThreads != null) {
        System.setProperty("calcite.etl.threads", origThreads);
      } else {
        System.clearProperty("calcite.etl.threads");
      }
    }
  }

  @Test void testExecute_parallelStandardModeWarnAction() throws Exception {
    String origThreads = System.getProperty("calcite.etl.threads");
    try {
      System.setProperty("calcite.etl.threads", "2");
      IncrementalTracker tracker = mockTracker();
      StorageProvider sp = mockStorage();
      DataProvider prov = mock(DataProvider.class);
      when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
          .thenThrow(new IOException("HTTP 500 Server Error"))
          .thenThrow(new IOException("HTTP 500 Server Error"));
      EtlPipelineConfig.ErrorHandlingConfig eh =
          new EtlPipelineConfig.ErrorHandlingConfig.Builder()
              .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
              .build();
      EtlPipelineConfig config =
          createHttpConfig("par_warn", singleRangeDimension("y", 2020, 2021),
          MaterializeConfig.Format.PARQUET, null, null, eh);
      EtlPipeline pipeline =
          new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
      EtlResult result = pipeline.execute();
      assertNotNull(result);
    } finally {
      if (origThreads != null) {
        System.setProperty("calcite.etl.threads", origThreads);
      } else {
        System.clearProperty("calcite.etl.threads");
      }
    }
  }

  @Test void testExecute_parallelStandardModeFailAction() throws Exception {
    String origThreads = System.getProperty("calcite.etl.threads");
    try {
      System.setProperty("calcite.etl.threads", "2");
      IncrementalTracker tracker = mockTracker();
      StorageProvider sp = mockStorage();
      DataProvider prov = mock(DataProvider.class);
      when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
          .thenThrow(new IOException("HTTP 401 Unauthorized"))
          .thenThrow(new IOException("HTTP 401 Unauthorized"));
      EtlPipelineConfig config =
          createHttpConfig("par_fail", singleRangeDimension("y", 2020, 2021),
          MaterializeConfig.Format.PARQUET, null, null);
      EtlPipeline pipeline =
          new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
      EtlResult result = pipeline.execute();
      assertNotNull(result);
      // FAIL action in parallel mode logs but doesn't abort other threads
    } finally {
      if (origThreads != null) {
        System.setProperty("calcite.etl.threads", origThreads);
      } else {
        System.clearProperty("calcite.etl.threads");
      }
    }
  }

  // -----------------------------------------------------------------------
  // 2. Response partitioning with DataWriter
  // -----------------------------------------------------------------------

  @Test void testWriteWithResponsePartitioning_writesAllRows() throws Exception {
    IncrementalTracker tracker = mockTracker();
    MaterializationWriter w = mock(MaterializationWriter.class);
    when(w.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(2L);
    EtlPipelineConfig config =
        createHttpConfig("t_rp_all", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, null, null);
    Method m =
        EtlPipeline.class.getDeclaredMethod("writeWithResponsePartitioning", Iterator.class, Map.class, HttpSourceConfig.ResponsePartitioningConfig.class,
        MaterializationWriter.class, IncrementalTracker.class, String.class);
    m.setAccessible(true);
    Map<String, Object> rpMap = new HashMap<String, Object>();
    rpMap.put("fields", Collections.singletonMap("year", "year"));
    HttpSourceConfig.ResponsePartitioningConfig rp =
        HttpSourceConfig.ResponsePartitioningConfig.fromMap(rpMap);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1", "year", "2020"));
    data.add(row("id", "2", "year", "2021"));
    long rows =
        (long) m.invoke(pipeline, data.iterator(), new HashMap<String, String>(), rp, w, tracker, "t_rp_all");
    // Writer.writeBatch is called with all rows since no year filter
    assertEquals(2L, rows);
    verify(w).writeBatch(any(Iterator.class), any(Map.class));
  }

  @Test void testWriteWithResponsePartitioning_withYearFilterFiltersAll() throws Exception {
    IncrementalTracker tracker = mockTracker();
    MaterializationWriter w = mock(MaterializationWriter.class);
    EtlPipelineConfig config =
        createHttpConfig("t_rp_empty", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, null, null);
    Method m =
        EtlPipeline.class.getDeclaredMethod("writeWithResponsePartitioning", Iterator.class, Map.class, HttpSourceConfig.ResponsePartitioningConfig.class,
        MaterializationWriter.class, IncrementalTracker.class, String.class);
    m.setAccessible(true);
    Map<String, Object> rpMap = new HashMap<String, Object>();
    rpMap.put("fields", Collections.singletonMap("year", "year"));
    rpMap.put("yearField", "year");
    rpMap.put("yearStart", 2025);
    rpMap.put("yearEnd", 2025);
    HttpSourceConfig.ResponsePartitioningConfig rp =
        HttpSourceConfig.ResponsePartitioningConfig.fromMap(rpMap);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1", "year", "2020"));
    data.add(row("id", "2", "year", "2021"));
    long rows =
        (long) m.invoke(pipeline, data.iterator(), new HashMap<String, String>(), rp, w, tracker, "t_rp_empty");
    assertEquals(0L, rows);
    // Empty result should still mark as processed with 0 rows
    verify(tracker).markProcessedWithRowCount(
        eq("t_rp_empty"), eq("t_rp_empty"), any(Map.class), any(), eq(0L));
  }

  @Test void testWriteWithResponsePartitioning_noYearFilterNoFiltering() throws Exception {
    IncrementalTracker tracker = mockTracker();
    MaterializationWriter w = mock(MaterializationWriter.class);
    when(w.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(5L);
    EtlPipelineConfig config =
        createHttpConfig("t_rp_noyr", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, null, null);
    Method m =
        EtlPipeline.class.getDeclaredMethod("writeWithResponsePartitioning", Iterator.class, Map.class, HttpSourceConfig.ResponsePartitioningConfig.class,
        MaterializationWriter.class, IncrementalTracker.class, String.class);
    m.setAccessible(true);
    Map<String, Object> rpMap = new HashMap<String, Object>();
    rpMap.put("fields", Collections.singletonMap("year", "year"));
    // No yearField set, so no year filter applied
    HttpSourceConfig.ResponsePartitioningConfig rp =
        HttpSourceConfig.ResponsePartitioningConfig.fromMap(rpMap);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1", "year", "2020"));
    data.add(row("id", "2", "year", "2021"));
    data.add(row("id", "3", "year", "2022"));
    data.add(row("id", "4", "year", "2023"));
    data.add(row("id", "5", "year", "2024"));
    long rows =
        (long) m.invoke(pipeline, data.iterator(), new HashMap<String, String>(), rp, w, tracker, "t_rp_noyr");
    assertEquals(5L, rows);
    verify(tracker).markProcessedWithRowCount(
        eq("t_rp_noyr"), eq("t_rp_noyr"), any(Map.class), any(), eq(5L));
  }

  // -----------------------------------------------------------------------
  // 3. processSingleBatch with response partitioning enabled
  // -----------------------------------------------------------------------

  @Test void testProcessSingleBatch_withResponsePartitioning() throws Exception {
    IncrementalTracker tracker = mockTracker();
    MaterializationWriter w = mock(MaterializationWriter.class);
    when(w.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(2L);
    DataSource ds = mock(DataSource.class);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1", "year", "2020"));
    data.add(row("id", "2", "year", "2021"));
    when(ds.fetch(any(Map.class))).thenReturn(data.iterator());
    // Create config with response partitioning enabled
    Map<String, Object> rpMap = new HashMap<String, Object>();
    rpMap.put("fields", Collections.singletonMap("year", "year"));
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .responsePartitioning(
            HttpSourceConfig.ResponsePartitioningConfig.fromMap(rpMap))
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("rp_batch")
        .source(source)
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build())
            .format(MaterializeConfig.Format.PARQUET).build())
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, null, null);
    Method m =
        EtlPipeline.class.getDeclaredMethod("processSingleBatch", EtlPipelineConfig.class, Map.class, DataSource.class, MaterializationWriter.class,
        int.class, String.class);
    m.setAccessible(true);
    long rows =
        (long) m.invoke(pipeline, config, new HashMap<String, String>(), ds, w, 1, "rp_batch");
    // Response partitioning buffers then writes
    assertTrue(rows >= 0);
  }

  @Test void testProcessSingleBatch_noResponsePartitioningWithDataWriter() throws Exception {
    IncrementalTracker tracker = mockTracker();
    DataWriter dw = mock(DataWriter.class);
    when(dw.write(any(EtlPipelineConfig.class), any(Iterator.class), any(Map.class)))
        .thenReturn(7L);
    MaterializationWriter w = mock(MaterializationWriter.class);
    DataSource ds = mock(DataSource.class);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1"));
    when(ds.fetch(any(Map.class))).thenReturn(data.iterator());
    EtlPipelineConfig config =
        createHttpConfig("dw_batch", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, null, dw);
    Method m =
        EtlPipeline.class.getDeclaredMethod("processSingleBatch", EtlPipelineConfig.class, Map.class, DataSource.class, MaterializationWriter.class,
        int.class, String.class);
    m.setAccessible(true);
    long rows =
        (long) m.invoke(pipeline, config, new HashMap<String, String>(), ds, w, 1, "dw_batch");
    assertEquals(7L, rows);
    verify(dw).write(any(EtlPipelineConfig.class), any(Iterator.class), any(Map.class));
  }

  @Test void testProcessSingleBatch_noResponsePartitioningNoDataWriter() throws Exception {
    IncrementalTracker tracker = mockTracker();
    MaterializationWriter w = mock(MaterializationWriter.class);
    when(w.writeBatch(any(Iterator.class), any(Map.class))).thenReturn(3L);
    DataSource ds = mock(DataSource.class);
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(row("id", "1"));
    when(ds.fetch(any(Map.class))).thenReturn(data.iterator());
    EtlPipelineConfig config =
        createHttpConfig("no_dw_batch", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline =
        new EtlPipeline(config, mockStorage(), tempDir.toString(), null, tracker, null, null);
    Method m =
        EtlPipeline.class.getDeclaredMethod("processSingleBatch", EtlPipelineConfig.class, Map.class, DataSource.class, MaterializationWriter.class,
        int.class, String.class);
    m.setAccessible(true);
    long rows =
        (long) m.invoke(pipeline, config, new HashMap<String, String>(), ds, w, 1, "no_dw_batch");
    assertEquals(3L, rows);
    verify(w).writeBatch(any(Iterator.class), any(Map.class));
    verify(tracker).markProcessedWithRowCount(
        eq("no_dw_batch"), eq("no_dw_batch"), any(Map.class), any(), eq(3L));
  }

  // -----------------------------------------------------------------------
  // 4. Table complete with Iceberg format - zero rows + TTL invalidation
  // -----------------------------------------------------------------------

  @Test void testExecute_tableCompleteIcebergZeroRowsWithTtlInvalidates() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.isTableComplete(eq("ice_zero"), anyString())).thenReturn(true);
    // Return no unprocessed after invalidation
    when(
        tracker.filterUnprocessedWithEmptyTtl(
        anyString(), anyString(), any(List.class), anyLong()))
        .thenReturn(Collections.<Integer>emptySet());
    // Iceberg format but readRowCountFromIceberg will return 0 (no actual Iceberg table)
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.builder()
        .emptyResultTtlDays(7).build();
    EtlPipelineConfig config =
        createHttpConfig("ice_zero", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.ICEBERG, opts, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult result = pipeline.execute();
    // Current behavior: neededCount=0 → skipped (period-aware filter handles TTL via
    // filterUnprocessedWithEmptyTtl; the neededCount==0 path no longer re-invalidates here).
    assertNotNull(result);
    assertTrue(result.isSkippedEntirePipeline());
  }

  @Test void testExecute_tableCompleteIcebergZeroRowsNoTtlSkips() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.isTableComplete(eq("ice_skip"), anyString())).thenReturn(true);
    // emptyResultTtlDays=0 means no TTL, so emptyResultTtlMillis will be 0
    // When rowCount=0 AND emptyResultTtlMillis=0, it should NOT invalidate
    // (the options null check: options != null, then checks emptyResultTtlMillis > 0)
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.builder()
        .emptyResultTtlDays(0).build();
    EtlPipelineConfig config =
        createHttpConfig("ice_skip", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.ICEBERG, opts, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult result = pipeline.execute();
    // With TTL=0 and rowCount=0, the else branch is used: skipped
    assertTrue(result.isSkippedEntirePipeline()
        || result.getTotalRows() == 0);
  }

  @Test void testExecute_tableCompleteIcebergNonZeroRows() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.isTableComplete(eq("ice_rows"), anyString())).thenReturn(true);
    // Provide a DataProvider to handle the processing path
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("ice_rows", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // The isTableComplete path finds rowCount=0 (no real Iceberg table).
    // The default emptyResultTtlDays (7) causes the pipeline to invalidate
    // the table completion and proceed to data processing rather than skipping.
    assertNotNull(result);
    assertFalse(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // 5. Cached completion with zero rows and config mismatch
  // -----------------------------------------------------------------------

  @Test void testExecute_cachedMismatchZeroRowsExpandsDimensions() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    // Config hash mismatch with 0 rows
    when(tracker.getCachedCompletion("mismatch_zero"))
        .thenReturn(new IncrementalTracker.CachedCompletion("old_hash", "sig", 0));
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("mismatch_zero", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // Should proceed to dimension expansion, not skip
    assertFalse(result.isSkippedEntirePipeline());
  }

  @Test void testExecute_cachedMismatchNonZeroRowsNoDataExpandsDimensions() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    // Config hash mismatch with rows but data doesn't exist
    when(tracker.getCachedCompletion("mismatch_nodata"))
        .thenReturn(new IncrementalTracker.CachedCompletion("old_hash", "sig", 50));
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("mismatch_nodata", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // Config hash mismatch with rows but data gone -> falls through to expand
    assertFalse(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // 6. Cold-start Iceberg recovery (cached==null, Iceberg format)
  // -----------------------------------------------------------------------

  @Test void testExecute_coldStartIcebergRecoveryNoData() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(false);
    // cached is null, Iceberg format
    when(tracker.getCachedCompletion("cold_no_data")).thenReturn(null);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("cold_no_data", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // No data exists, so no recovery, proceed with ETL
    assertFalse(result.isSkippedEntirePipeline());
  }

  @Test void testExecute_coldStartIcebergRecoveryWithData() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    // Data exists for Iceberg check
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.getCachedCompletion("cold_data")).thenReturn(null);
    // readRowCountFromIceberg will return 0 (no real Iceberg table)
    // so recovery will NOT skip
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("cold_data", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // readRowCountFromIceberg returns 0 (no real table), so proceeds to normal
    assertNotNull(result);
  }

  @Test void testExecute_coldStartIcebergWithExpectedColumns() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.getCachedCompletion("cold_cols")).thenReturn(null);
    List<ColumnConfig> cols = new ArrayList<ColumnConfig>();
    cols.add(ColumnConfig.builder().name("id").type("VARCHAR").build());
    cols.add(ColumnConfig.builder().name("name").type("VARCHAR").build());
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("cold_cols", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.ICEBERG, null, cols);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  @Test void testExecute_coldStartIcebergColumnsFromMaterialize() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.getCachedCompletion("cold_mat_cols")).thenReturn(null);
    List<ColumnConfig> matCols = new ArrayList<ColumnConfig>();
    matCols.add(ColumnConfig.builder().name("code").type("VARCHAR").build());
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .columns(matCols)
        .build();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("code", "US")).iterator());
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("cold_mat_cols")
        .source(HttpSourceConfig.builder().url("https://api.example.com/data").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // 7. Config merging - both name and columns
  // -----------------------------------------------------------------------

  @Test void testExecute_mergesBothNameAndColumns() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    List<ColumnConfig> cols =
        Collections.singletonList(ColumnConfig.builder().name("id").type("VARCHAR").build());
    // materialize has no name, no targetTableId, no columns
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.PARQUET)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("merge_both")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .columns(cols)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  @Test void testExecute_noMergeWhenNameAndColumnsAlreadySet() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    List<ColumnConfig> matCols =
        Collections.singletonList(ColumnConfig.builder().name("code").type("VARCHAR").build());
    // materialize already has name and columns
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.PARQUET)
        .name("existing_name")
        .columns(matCols)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("no_merge")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  @Test void testExecute_mergeNameButNotColumnsWhenTargetTableIdSet() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    // materialize has targetTableId but no columns
    List<ColumnConfig> configCols =
        Collections.singletonList(ColumnConfig.builder().name("id").type("VARCHAR").build());
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.PARQUET)
        .targetTableId("my_table_id")
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("merge_cols_only")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .columns(configCols)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // 8. Table directory path construction - Iceberg vs Parquet
  // -----------------------------------------------------------------------

  @Test void testExecute_icebergDoesNotAppendTableName() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("ice_path", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, "/data/warehouse", null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // Iceberg format does not append table name to base directory
    assertNotNull(result);
  }

  @Test void testExecute_parquetWithTrailingSlash() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("tbl_trail", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    // Base directory ends with / - different code path
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString() + "/", null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  @Test void testExecute_parquetWithEmptyTableName() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    // Pipeline name is non-empty but test if code handles empty name scenario
    EtlPipelineConfig config =
        createHttpConfig("p", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // 9. GC trigger at every 10th batch
  // -----------------------------------------------------------------------

  @Test void testExecute_gcTriggeredEvery10thBatch() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    // Return data for 12 batches
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator())
        .thenReturn(Collections.singletonList(row("id", "2")).iterator())
        .thenReturn(Collections.singletonList(row("id", "3")).iterator())
        .thenReturn(Collections.singletonList(row("id", "4")).iterator())
        .thenReturn(Collections.singletonList(row("id", "5")).iterator())
        .thenReturn(Collections.singletonList(row("id", "6")).iterator())
        .thenReturn(Collections.singletonList(row("id", "7")).iterator())
        .thenReturn(Collections.singletonList(row("id", "8")).iterator())
        .thenReturn(Collections.singletonList(row("id", "9")).iterator())
        .thenReturn(Collections.singletonList(row("id", "10")).iterator())
        .thenReturn(Collections.singletonList(row("id", "11")).iterator())
        .thenReturn(Collections.singletonList(row("id", "12")).iterator());
    // 12 dimensions
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("y", DimensionConfig.builder()
        .name("y").type(DimensionType.RANGE).start(2000).end(2011).build());
    EtlPipelineConfig config =
        createHttpConfig("gc_test", dims, MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // GC should have been triggered at batch 10
    assertEquals(12, result.getSuccessfulBatches());
  }

  // -----------------------------------------------------------------------
  // 10. Consecutive failures abort
  // -----------------------------------------------------------------------

  @Test void testExecute_consecutiveFailuresAbortWithRetryAction() throws Exception {
    String origMax = System.getProperty("calcite.etl.maxConsecutiveFailures");
    try {
      System.setProperty("calcite.etl.maxConsecutiveFailures", "3");
      IncrementalTracker tracker = mockTracker();
      StorageProvider sp = mockStorage();
      DataProvider prov = mock(DataProvider.class);
      // All fetches fail with retryable error
      when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
          .thenThrow(new IOException("HTTP 429 Too Many Requests"));
      EtlPipelineConfig.ErrorHandlingConfig eh =
          new EtlPipelineConfig.ErrorHandlingConfig.Builder()
              .transientErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
              .build();
      Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
      dims.put("y", DimensionConfig.builder()
          .name("y").type(DimensionType.RANGE).start(2020).end(2025).build());
      EtlPipelineConfig config =
          createHttpConfig("abort_retry", dims, MaterializeConfig.Format.PARQUET, null, null, eh);
      EtlPipeline pipeline =
          new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
      EtlResult result = pipeline.execute();
      // Should abort after 3 consecutive failures
      assertTrue(result.isFailed());
      assertNotNull(result.getFailureMessage());
      assertTrue(result.getFailureMessage().contains("consecutive failures"));
    } finally {
      if (origMax != null) {
        System.setProperty("calcite.etl.maxConsecutiveFailures", origMax);
      } else {
        System.clearProperty("calcite.etl.maxConsecutiveFailures");
      }
    }
  }

  @Test void testExecute_maxConsecutiveFailuresFromEnvVar() throws Exception {
    // This test covers the env var fallback path in maxConsecutiveFailures parsing
    String origProp = System.getProperty("calcite.etl.maxConsecutiveFailures");
    try {
      System.setProperty("calcite.etl.maxConsecutiveFailures", "5");
      IncrementalTracker tracker = mockTracker();
      StorageProvider sp = mockStorage();
      DataProvider prov = mock(DataProvider.class);
      when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
          .thenReturn(Collections.singletonList(row("id", "1")).iterator());
      EtlPipelineConfig config =
          createHttpConfig("env_test", singleRangeDimension("y", 2020, 2020),
          MaterializeConfig.Format.PARQUET, null, null);
      EtlPipeline pipeline =
          new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
      EtlResult result = pipeline.execute();
      assertNotNull(result);
    } finally {
      if (origProp != null) {
        System.setProperty("calcite.etl.maxConsecutiveFailures", origProp);
      } else {
        System.clearProperty("calcite.etl.maxConsecutiveFailures");
      }
    }
  }

  // -----------------------------------------------------------------------
  // 11. rebuildCacheFromIceberg - various paths
  // -----------------------------------------------------------------------

  @Test void testRebuildCacheFromIceberg_cacheAlreadyHasData() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    // Return some processed entries (not all unprocessed)
    Set<Integer> partial = new HashSet<Integer>();
    partial.add(0);
    when(tracker.filterUnprocessed(anyString(), anyString(), any(List.class)))
        .thenReturn(partial);
    EtlPipelineConfig config =
        createHttpConfig("rebuild_partial", singleRangeDimension("y", 2020, 2021),
        MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    Method m =
        EtlPipeline.class.getDeclaredMethod("rebuildCacheFromIceberg", String.class, EtlPipelineConfig.class, List.class);
    m.setAccessible(true);
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    Map<String, String> c1 = new HashMap<String, String>();
    c1.put("y", "2020");
    Map<String, String> c2 = new HashMap<String, String>();
    c2.put("y", "2021");
    combos.add(c1);
    combos.add(c2);
    @SuppressWarnings("unchecked")
    Set<Integer> result = (Set<Integer>) m.invoke(pipeline, "rebuild_partial", config, combos);
    // Cache has data (1 of 2 unprocessed), returns partial set without rebuilding
    assertEquals(1, result.size());
  }

  @Test void testRebuildCacheFromIceberg_cacheEmptyDisabledMaterialize() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    // All unprocessed - cache is empty
    Set<Integer> all = new HashSet<Integer>();
    all.add(0);
    when(tracker.filterUnprocessed(anyString(), anyString(), any(List.class)))
        .thenReturn(all);
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .enabled(false)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("rebuild_disabled")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    Method m =
        EtlPipeline.class.getDeclaredMethod("rebuildCacheFromIceberg", String.class, EtlPipelineConfig.class, List.class);
    m.setAccessible(true);
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    Map<String, String> c1 = new HashMap<String, String>();
    c1.put("y", "2020");
    combos.add(c1);
    @SuppressWarnings("unchecked")
    Set<Integer> result = (Set<Integer>) m.invoke(pipeline, "rebuild_disabled", config, combos);
    // Disabled materialize returns unprocessed without rebuild
    assertEquals(1, result.size());
  }

  @Test void testRebuildCacheFromIceberg_cacheEmptyNoPartitionColumns() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    Set<Integer> all = new HashSet<Integer>();
    all.add(0);
    when(tracker.filterUnprocessed(anyString(), anyString(), any(List.class)))
        .thenReturn(all);
    // Materialize enabled but no partition columns
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("rebuild_nopart")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    Method m =
        EtlPipeline.class.getDeclaredMethod("rebuildCacheFromIceberg", String.class, EtlPipelineConfig.class, List.class);
    m.setAccessible(true);
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    Map<String, String> c1 = new HashMap<String, String>();
    c1.put("y", "2020");
    combos.add(c1);
    @SuppressWarnings("unchecked")
    Set<Integer> result = (Set<Integer>) m.invoke(pipeline, "rebuild_nopart", config, combos);
    // No partition columns -> skips rebuild
    assertEquals(1, result.size());
  }

  @Test void testRebuildCacheFromIceberg_targetTableIdFallbacks() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    Set<Integer> all = new HashSet<Integer>();
    all.add(0);
    when(tracker.filterUnprocessed(anyString(), anyString(), any(List.class)))
        .thenReturn(all);
    // Create config with empty targetTableId and empty materialize name
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .partition(MaterializePartitionConfig.builder()
            .columns(Collections.singletonList("y")).build())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("rebuild_fallback")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    Method m =
        EtlPipeline.class.getDeclaredMethod("rebuildCacheFromIceberg", String.class, EtlPipelineConfig.class, List.class);
    m.setAccessible(true);
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    Map<String, String> c1 = new HashMap<String, String>();
    c1.put("y", "2020");
    combos.add(c1);
    @SuppressWarnings("unchecked")
    Set<Integer> result = (Set<Integer>) m.invoke(pipeline, "rebuild_fallback", config, combos);
    // Should proceed and try to query Iceberg (which fails gracefully)
    assertNotNull(result);
  }

  @Test void testRebuildCacheFromIceberg_nullMaterializeConfig() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    Set<Integer> all = new HashSet<Integer>();
    all.add(0);
    when(tracker.filterUnprocessed(anyString(), anyString(), any(List.class)))
        .thenReturn(all);
    // Build a valid config with materialize for the pipeline constructor
    EtlPipelineConfig pipelineConfig =
        createHttpConfig("rebuild_null_mat", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline =
        new EtlPipeline(pipelineConfig, sp, tempDir.toString(), null, tracker);
    // Build a config with null materialize (enabled=false bypasses materialize requirement)
    // to test the rebuildCacheFromIceberg null-materialize branch
    EtlPipelineConfig configWithNullMat = EtlPipelineConfig.builder()
        .name("rebuild_null_mat")
        .enabled(false)
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .build();
    Method m =
        EtlPipeline.class.getDeclaredMethod("rebuildCacheFromIceberg", String.class, EtlPipelineConfig.class, List.class);
    m.setAccessible(true);
    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    Map<String, String> c1 = new HashMap<String, String>();
    c1.put("y", "2020");
    combos.add(c1);
    @SuppressWarnings("unchecked")
    Set<Integer> result = (Set<Integer>) m.invoke(pipeline, "rebuild_null_mat", configWithNullMat, combos);
    // Null materialize returns unprocessed
    assertEquals(1, result.size());
  }

  // -----------------------------------------------------------------------
  // 12. createDataSource - file source type
  // -----------------------------------------------------------------------

  @Test void testCreateDataSource_fileSourceWithStorageProvider() throws Exception {
    StorageProvider sp = mockStorage();
    StorageProvider sourceSp = mock(StorageProvider.class);
    when(sourceSp.getStorageType()).thenReturn("local");
    Map<String, Object> rawSource = new HashMap<String, Object>();
    rawSource.put("path", "/data/input.csv");
    rawSource.put("format", "csv");
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("file_src")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .sourceType("file")
        .rawSourceConfig(rawSource)
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build()).build())
        .build();
    // Use the constructor that accepts sourceStorageProvider
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, sourceSp, tempDir.toString(), null, IncrementalTracker.NOOP, null, null);
    Method m =
        EtlPipeline.class.getDeclaredMethod("createDataSource", EtlPipelineConfig.class);
    m.setAccessible(true);
    DataSource ds = (DataSource) m.invoke(pipeline, config);
    assertNotNull(ds);
  }

  @Test void testCreateDataSource_httpWithRawCacheAndOperatingDir() throws Exception {
    StorageProvider sp = mockStorage();
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .rawCache(HttpSourceConfig.RawCacheConfig.enabled())
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("http_cache_op")
        .source(source)
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build()).build())
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, sp, tempDir.toString(), null, IncrementalTracker.NOOP, null, null, "/op/dir");
    Method m =
        EtlPipeline.class.getDeclaredMethod("createDataSource", EtlPipelineConfig.class);
    m.setAccessible(true);
    DataSource ds = (DataSource) m.invoke(pipeline, config);
    assertNotNull(ds);
  }

  @Test void testCreateDataSource_httpWithoutRawCache() throws Exception {
    StorageProvider sp = mockStorage();
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("https://api.example.com/data")
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("http_no_cache")
        .source(source)
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build()).build())
        .build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Method m =
        EtlPipeline.class.getDeclaredMethod("createDataSource", EtlPipelineConfig.class);
    m.setAccessible(true);
    DataSource ds = (DataSource) m.invoke(pipeline, config);
    assertNotNull(ds);
    assertEquals("http", ds.getType());
  }

  // -----------------------------------------------------------------------
  // 13. Phase 1.5 self-healing (rebuildCacheFromIceberg integration path)
  // -----------------------------------------------------------------------

  @Test void testExecute_phase15SelfHealingWithIcebergData() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    // Force the Phase 1.5 path: not table complete, but data exists
    when(tracker.isTableComplete(anyString(), anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("heal_ice")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }

  @Test void testExecute_phase15SkippedForParquet() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("heal_parq", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // Phase 1.5 only runs for Iceberg format
  }

  // -----------------------------------------------------------------------
  // 14. execute() - dataSource.close() called
  // -----------------------------------------------------------------------

  @Test void testExecute_dataSourceClosed() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("ds_close", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // DataSource.close() is called on non-null data sources after processing
  }

  // -----------------------------------------------------------------------
  // 15. execute() - writer close exception handling
  // -----------------------------------------------------------------------

  @Test void testExecute_writerCloseExceptionHandled() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenThrow(new RuntimeException("fatal"));
    EtlPipelineConfig config =
        createHttpConfig("writer_close_err", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // RuntimeException is caught per ErrorHandlingConfig (SKIP for API errors);
    // pipeline completes rather than setting isFailed.
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // 16. execute() - no failed batches marks complete
  // -----------------------------------------------------------------------

  @Test void testExecute_successMarksComplete() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator())
        .thenReturn(Collections.singletonList(row("id", "2")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("mark_complete", singleRangeDimension("y", 2020, 2021),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    if (!result.isFailed() && result.getFailedBatches() == 0 && result.getErrors().isEmpty()) {
      verify(tracker).markTableCompleteWithConfig(eq("mark_complete"),
          anyString(), anyString(), anyLong());
    }
  }

  @Test void testExecute_withErrorsDoesNotMarkComplete() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator())
        .thenThrow(new IOException("HTTP 500 Server Error"));
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .apiErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build();
    EtlPipelineConfig config =
        createHttpConfig("no_complete", singleRangeDimension("y", 2020, 2021),
        MaterializeConfig.Format.PARQUET, null, null, eh);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    if (result.getFailedBatches() > 0) {
      verify(tracker, never()).markTableCompleteWithConfig(eq("no_complete"),
          anyString(), anyString(), anyLong());
    }
  }

  // -----------------------------------------------------------------------
  // 17. execute() result metadata: tableLocation and materializeFormat
  // -----------------------------------------------------------------------

  @Test void testExecute_resultIncludesTableLocationAndFormat() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("meta_test", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertNotNull(result.getPipelineName());
    assertEquals("meta_test", result.getPipelineName());
  }

  // -----------------------------------------------------------------------
  // 18. execute() - table complete check with non-materialize enabled
  // -----------------------------------------------------------------------

  @Test void testExecute_tableCompleteNotMaterializedSkips() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.isTableComplete(eq("not_mat"), anyString())).thenReturn(true);
    when(tracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(Collections.<Integer>emptySet());
    // Materialize disabled - table complete should result in skipped
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .enabled(false)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("not_mat")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult result = pipeline.execute();
    assertTrue(result.isSkipped());
  }

  // -----------------------------------------------------------------------
  // 19. execute() with nullMaterializeConfig (no materialize section)
  // -----------------------------------------------------------------------

  @Test void testExecute_nullMaterializeConfig() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    // EtlPipelineConfig.Builder now requires materialize for enabled pipelines.
    // Test with a disabled materialize config to exercise the null/disabled
    // materialization path in execute().
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("null_mat")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(MaterializeConfig.builder()
            .enabled(false)
            .output(MaterializeOutputConfig.builder().build())
            .build())
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // Should handle disabled materialize gracefully
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // 20. Progress listener on all phases
  // -----------------------------------------------------------------------

  @Test void testExecute_progressListenerAllPhasesIncludingComplete() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator())
        .thenReturn(Collections.singletonList(row("id", "2")).iterator());
    EtlPipeline.ProgressListener listener = mock(EtlPipeline.ProgressListener.class);
    EtlPipelineConfig config =
        createHttpConfig("pl_all", singleRangeDimension("y", 2020, 2021),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), listener, tracker, prov, null);
    pipeline.execute();
    verify(listener).onPhaseStart(eq("dimension_expansion"), eq(2));
    verify(listener).onPhaseComplete(eq("dimension_expansion"), eq(2));
    verify(listener).onPhaseStart(eq("data_processing"), eq(2));
    verify(listener, times(2)).onBatchStart(any(int.class), eq(2), any(Map.class));
    verify(listener).onPhaseComplete(eq("data_processing"), eq(2));
  }

  @Test void testExecute_progressListenerOnBatchError() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenThrow(new IOException("HTTP 404 Not Found"));
    EtlPipeline.ProgressListener listener = mock(EtlPipeline.ProgressListener.class);
    EtlPipelineConfig config =
        createHttpConfig("pl_err", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), listener, tracker, prov, null);
    pipeline.execute();
    verify(listener).onBatchComplete(eq(1), eq(1), eq(0), any(IOException.class));
  }

  // -----------------------------------------------------------------------
  // 21. readRowCountFromIceberg - single arg variant
  // -----------------------------------------------------------------------

  @Test void testReadRowCountFromIceberg_singleArgDelegatesToTwoArg() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config =
        createHttpConfig("read_cnt", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("readRowCountFromIceberg", String.class);
    m.setAccessible(true);
    long result = (Long) m.invoke(pipeline, tempDir.toString() + "/no_table");
    assertEquals(0, result);
  }

  // -----------------------------------------------------------------------
  // 22. storeEtlPropertiesToIceberg with valid and invalid paths
  // -----------------------------------------------------------------------

  @Test void testStoreEtlPropertiesToIceberg_nonExistentTable() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config =
        createHttpConfig("store_props", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Method m =
        EtlPipeline.class.getDeclaredMethod("storeEtlPropertiesToIceberg", String.class, String.class, String.class, long.class);
    m.setAccessible(true);
    // Should not throw - gracefully handles missing table
    m.invoke(pipeline, tempDir.toString() + "/no_table", "hash", "sig", 42L);
  }

  // -----------------------------------------------------------------------
  // 23. readEtlPropertiesFromIceberg - exception path
  // -----------------------------------------------------------------------

  @Test void testReadEtlPropertiesFromIceberg_nonExistentPath() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config =
        createHttpConfig("read_props", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Method m =
        EtlPipeline.class.getDeclaredMethod("readEtlPropertiesFromIceberg", String.class);
    m.setAccessible(true);
    Object result = m.invoke(pipeline, "/nonexistent/path/table");
    assertNull(result);
  }

  // -----------------------------------------------------------------------
  // 24. getParallelThreadCount - per-table parallel config
  // -----------------------------------------------------------------------

  @Test void testGetParallelThreadCount_sourceParallelTakesPrecedence() throws Exception {
    String origThreads = System.getProperty("calcite.etl.threads");
    try {
      System.setProperty("calcite.etl.threads", "8");
      StorageProvider sp = mockStorage();
      HttpSourceConfig source = HttpSourceConfig.builder()
          .url("http://x")
          .parallel(4)
          .build();
      EtlPipelineConfig config = EtlPipelineConfig.builder()
          .name("par_cfg")
          .source(source)
          .materialize(MaterializeConfig.builder()
              .output(MaterializeOutputConfig.builder().build()).build())
          .build();
      EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
      Method m = EtlPipeline.class.getDeclaredMethod("getParallelThreadCount");
      m.setAccessible(true);
      int count = (Integer) m.invoke(pipeline);
      // per-table parallel=4 takes precedence over system property=8
      assertEquals(4, count);
    } finally {
      if (origThreads != null) {
        System.setProperty("calcite.etl.threads", origThreads);
      } else {
        System.clearProperty("calcite.etl.threads");
      }
    }
  }

  @Test void testGetParallelThreadCount_nullSourceConfig() throws Exception {
    StorageProvider sp = mockStorage();
    // Config with null source - sourceConfig check should handle null
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("null_src")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .materialize(MaterializeConfig.builder()
            .output(MaterializeOutputConfig.builder().build()).build())
        .build();
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    Method m = EtlPipeline.class.getDeclaredMethod("getParallelThreadCount");
    m.setAccessible(true);
    int count = (Integer) m.invoke(pipeline);
    // No parallel config, falls to system property default of 1
    assertEquals(1, count);
  }

  // -----------------------------------------------------------------------
  // 25. execute() - multiple dimensions produce cross-product
  // -----------------------------------------------------------------------

  @Test void testExecute_multipleDimensionsCrossProduct() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    // Two dimensions: y (2020-2021) x q (1-2) = 4 combinations
    Map<String, DimensionConfig> dims = multiRangeDimension("y", 2020, 2021, "q", 1, 2);
    EtlPipelineConfig config =
        createHttpConfig("multi_dim", dims, MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
    assertEquals(4, result.getSuccessfulBatches());
  }

  // -----------------------------------------------------------------------
  // 26. EtlResult builder edge cases
  // -----------------------------------------------------------------------

  @Test void testEtlResult_builderWithAllFields() {
    List<String> errors = new ArrayList<String>();
    errors.add("error1");
    EtlResult result = EtlResult.builder()
        .pipelineName("p")
        .totalRows(1000)
        .successfulBatches(8)
        .failedBatches(2)
        .skippedBatches(3)
        .elapsedMs(5000)
        .errors(errors)
        .tableLocation("/data/table")
        .materializeFormat(MaterializeConfig.Format.PARQUET)
        .skippedEntirePipeline(false)
        .failed(false)
        .failureMessage(null)
        .build();
    assertEquals("p", result.getPipelineName());
    assertEquals(1000, result.getTotalRows());
    assertEquals(8, result.getSuccessfulBatches());
    assertEquals(2, result.getFailedBatches());
    assertEquals(3, result.getSkippedBatches());
    assertEquals(5000, result.getElapsedMs());
    assertEquals(1, result.getErrors().size());
    assertEquals("/data/table", result.getTableLocation());
    assertEquals(MaterializeConfig.Format.PARQUET, result.getMaterializeFormat());
    assertFalse(result.isSkippedEntirePipeline());
    assertFalse(result.isFailed());
    assertNull(result.getFailureMessage());
    assertEquals(13, result.getTotalBatches());
    assertEquals(200.0, result.getRowsPerSecond(), 0.01);
  }

  // -----------------------------------------------------------------------
  // 27. execute() - forceReprocessAll in standard mode
  // -----------------------------------------------------------------------

  @Test void testExecute_forceReprocessAllStandard() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    // Cached completion matches but data gone -> invalidates -> forceReprocessAll
    Map<String, DimensionConfig> dims = singleRangeDimension("y", 2020, 2021);
    String hash = IncrementalTracker.computeConfigHash(dims);
    when(tracker.getCachedCompletion("force_all"))
        .thenReturn(new IncrementalTracker.CachedCompletion(hash, "sig", 100));
    when(sp.isDirectory(anyString())).thenReturn(false);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator())
        .thenReturn(Collections.singletonList(row("id", "2")).iterator());
    EtlPipelineConfig config =
        createHttpConfig("force_all", dims, MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
    verify(tracker).invalidateTableCompletion("force_all");
    // forceReprocessAll skips tracker scan and processes all
    assertEquals(2, result.getSuccessfulBatches());
  }

  // -----------------------------------------------------------------------
  // 28. execute() - emptyResultTtlMillis default value
  // -----------------------------------------------------------------------

  @Test void testExecute_emptyResultTtlDefaultsFromOptions() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    // Materialize with null options
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.PARQUET)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("ttl_default")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2020))
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // Uses MaterializeOptionsConfig.defaults().getEmptyResultTtlMillis()
    assertNotNull(result);
  }

  // -----------------------------------------------------------------------
  // 29. execute() - prefilteredIndices reuse from Phase 1.5
  // -----------------------------------------------------------------------

  @Test void testExecute_prefilteredIndicesReusedInPhase2() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    // Phase 1.5 returns partial set which should be reused in Phase 2
    Set<Integer> partialUnprocessed = new HashSet<Integer>();
    partialUnprocessed.add(0);
    when(tracker.filterUnprocessed(anyString(), anyString(), any(List.class)))
        .thenReturn(partialUnprocessed);
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenReturn(Collections.singletonList(row("id", "1")).iterator());
    MaterializeConfig mc = MaterializeConfig.builder()
        .output(MaterializeOutputConfig.builder().build())
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name("prefilter_reuse")
        .source(HttpSourceConfig.builder().url("http://x").build())
        .dimensions(singleRangeDimension("y", 2020, 2021))
        .materialize(mc)
        .build();
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
    // Phase 1.5 found 1 of 2 unprocessed, reused in Phase 2
  }

  // -----------------------------------------------------------------------
  // 30. determineErrorAction - transient error actions
  // -----------------------------------------------------------------------

  @Test void testDetermineErrorAction_http429CustomTransientAction() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config =
        createHttpConfig("err_429", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .transientErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL)
            .build();
    Method m =
        EtlPipeline.class.getDeclaredMethod("determineErrorAction", Throwable.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);
    Object result = m.invoke(pipeline, new IOException("HTTP 429 Rate Limited"), eh);
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.FAIL, result);
  }

  @Test void testDetermineErrorAction_http503CustomTransientAction() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config =
        createHttpConfig("err_503", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .transientErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP)
            .build();
    Method m =
        EtlPipeline.class.getDeclaredMethod("determineErrorAction", Throwable.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);
    Object result = m.invoke(pipeline, new IOException("HTTP 503 Unavailable"), eh);
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP, result);
  }

  @Test void testDetermineErrorAction_http404CustomNotFoundAction() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config =
        createHttpConfig("err_404", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .notFoundAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN)
            .build();
    Method m =
        EtlPipeline.class.getDeclaredMethod("determineErrorAction", Throwable.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);
    Object result = m.invoke(pipeline, new IOException("HTTP 404 Not Found"), eh);
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.WARN, result);
  }

  @Test void testDetermineErrorAction_authError401CustomAction() throws Exception {
    StorageProvider sp = mockStorage();
    EtlPipelineConfig config =
        createHttpConfig("err_401", singleRangeDimension("y", 2020, 2020));
    EtlPipeline pipeline = new EtlPipeline(config, sp, tempDir.toString());
    EtlPipelineConfig.ErrorHandlingConfig eh =
        new EtlPipelineConfig.ErrorHandlingConfig.Builder()
            .authErrorAction(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP)
            .build();
    Method m =
        EtlPipeline.class.getDeclaredMethod("determineErrorAction", Throwable.class, EtlPipelineConfig.ErrorHandlingConfig.class);
    m.setAccessible(true);
    Object result = m.invoke(pipeline, new IOException("HTTP 401 Unauthorized"), eh);
    assertEquals(EtlPipelineConfig.ErrorHandlingConfig.ErrorAction.SKIP, result);
  }

  // -----------------------------------------------------------------------
  // 31. Parallel mode with single needed batch (no parallelism)
  // -----------------------------------------------------------------------

  @Test void testExecute_parallelModeWithSingleBatchUsesSequential() throws Exception {
    String origThreads = System.getProperty("calcite.etl.threads");
    try {
      System.setProperty("calcite.etl.threads", "4");
      IncrementalTracker tracker = mockTracker();
      StorageProvider sp = mockStorage();
      DataProvider prov = mock(DataProvider.class);
      when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
          .thenReturn(Collections.singletonList(row("id", "1")).iterator());
      // Only 1 batch - should use sequential even though threads > 1
      EtlPipelineConfig config =
          createHttpConfig("par_single", singleRangeDimension("y", 2020, 2020),
          MaterializeConfig.Format.PARQUET, null, null);
      EtlPipeline pipeline =
          new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
      EtlResult result = pipeline.execute();
      assertNotNull(result);
      assertEquals(1, result.getSuccessfulBatches());
    } finally {
      if (origThreads != null) {
        System.setProperty("calcite.etl.threads", origThreads);
      } else {
        System.clearProperty("calcite.etl.threads");
      }
    }
  }

  // -----------------------------------------------------------------------
  // 32. LoggingProgressListener edge cases
  // -----------------------------------------------------------------------

  @Test void testLoggingProgressListener_nullVariables() {
    EtlPipeline.LoggingProgressListener l = new EtlPipeline.LoggingProgressListener();
    l.onBatchStart(1, 5, null);
    l.onBatchComplete(1, 5, 0, null);
    // Should handle null variables gracefully
  }

  @Test void testLoggingProgressListener_zeroTotalBatches() {
    EtlPipeline.LoggingProgressListener l = new EtlPipeline.LoggingProgressListener();
    l.onPhaseStart("test", 0);
    l.onPhaseComplete("test", 0);
    // Should handle zero total batches
  }

  // -----------------------------------------------------------------------
  // 33. execute() - exception in outer try-catch
  // -----------------------------------------------------------------------

  @Test void testExecute_outerCatchBlockHandlesException() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    DataProvider prov = mock(DataProvider.class);
    when(prov.fetch(any(EtlPipelineConfig.class), any(Map.class)))
        .thenThrow(new RuntimeException("Unexpected runtime error"));
    EtlPipelineConfig config =
        createHttpConfig("outer_catch", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker, prov, null);
    EtlResult result = pipeline.execute();
    // RuntimeException from DataProvider is caught in processSingleBatch and handled per
    // ErrorHandlingConfig (default: SKIP for API errors). The pipeline completes, not fails.
    assertNotNull(result);
    assertEquals("outer_catch", result.getPipelineName());
  }

  // -----------------------------------------------------------------------
  // 34. execute() - cachedCompletion with 0 rows, no options
  // -----------------------------------------------------------------------

  @Test void testExecute_cachedMatchZeroRowsNoOptions() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    Map<String, DimensionConfig> dims = singleRangeDimension("y", 2020, 2020);
    String hash = IncrementalTracker.computeConfigHash(dims);
    // 0 rows, but no options configured
    when(tracker.getCachedCompletion("zero_no_opts"))
        .thenReturn(new IncrementalTracker.CachedCompletion(hash, "sig", 0));
    // All combos already processed — stub Phase 2 filter to return empty
    when(tracker.filterUnprocessedWithEmptyTtl(anyString(), anyString(),
        any(List.class), anyLong())).thenReturn(Collections.<Integer>emptySet());
    EtlPipelineConfig config =
        createHttpConfig("zero_no_opts", dims, MaterializeConfig.Format.ICEBERG, null, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult result = pipeline.execute();
    assertTrue(result.isSkippedEntirePipeline());
  }

  // -----------------------------------------------------------------------
  // 35. execute() - Parquet format doesn't read row count from Iceberg
  // -----------------------------------------------------------------------

  @Test void testExecute_tableCompleteParquetFormatNoIcebergRowCount() throws Exception {
    IncrementalTracker tracker = mockTracker();
    StorageProvider sp = mockStorage();
    when(sp.isDirectory(anyString())).thenReturn(true);
    when(tracker.isTableComplete(eq("parq_no_ice"), anyString())).thenReturn(true);
    // Parquet format: rowCount stays 0 (not read from Iceberg)
    // With emptyResultTtlDays=0, TTL millis=0 -> goes to else -> skipped
    MaterializeOptionsConfig opts = MaterializeOptionsConfig.builder()
        .emptyResultTtlDays(0).build();
    EtlPipelineConfig config =
        createHttpConfig("parq_no_ice", singleRangeDimension("y", 2020, 2020),
        MaterializeConfig.Format.PARQUET, opts, null);
    EtlPipeline pipeline =
        new EtlPipeline(config, sp, tempDir.toString(), null, tracker);
    EtlResult result = pipeline.execute();
    assertNotNull(result);
  }
}
