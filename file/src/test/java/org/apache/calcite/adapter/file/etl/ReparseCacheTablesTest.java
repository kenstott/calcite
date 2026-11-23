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

import org.apache.calcite.adapter.file.etl.FreshnessSkipGateTest.CountingDataProvider;
import org.apache.calcite.adapter.file.etl.FreshnessSkipGateTest.CountingDataWriter;
import org.apache.calcite.adapter.file.etl.FreshnessSkipGateTest.MemoryFreshnessTracker;
import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@code GOVDATA_REPARSE_CACHE_TABLES}: a listed table keeps its raw cache entries and
 * reads them on a force-reprocess; unlisted tables and the automatically derived
 * forceReprocessAll keep dropping or bypassing the cache.
 */
@Tag("unit")
class ReparseCacheTablesTest {
  private static final String TABLE = "reparse_test_pipeline";
  private static final String REPARSE = "GOVDATA_REPARSE_CACHE_TABLES";
  private static final String DOWNLOAD = "GOVDATA_FORCE_DOWNLOAD_TABLES";

  @TempDir
  Path tempDir;

  @AfterEach void clearProperties() {
    System.clearProperty(REPARSE);
    System.clearProperty(DOWNLOAD);
  }

  /** Stub source that counts raw cache invalidations and serves a mutable probe result. */
  static final class RecordingSource extends HttpSource {
    final AtomicInteger invalidations = new AtomicInteger();
    volatile HttpSource.ProbeResult probeResult;

    RecordingSource(HttpSourceConfig cfg, HttpSource.ProbeResult probeResult) {
      super(cfg);
      this.probeResult = probeResult;
    }

    @Override public HttpSource.ProbeResult probe(FreshnessConfig freshnessCfg,
        Map<String, String> variables) {
      return probeResult;
    }

    @Override public boolean invalidateRawCache(Map<String, String> variables) {
      invalidations.incrementAndGet();
      return true;
    }
  }

  static final class Pipeline extends EtlPipeline {
    private final HttpSource source;

    Pipeline(EtlPipelineConfig config, StorageProvider sp, String dir, IncrementalTracker tracker,
        DataProvider provider, DataWriter writer, HttpSource source) {
      super(config, sp, dir, null, tracker, provider, writer);
      this.source = source;
    }

    @Override protected DataSource createDataSource(EtlPipelineConfig config,
        boolean forceReprocessAll) {
      return source;
    }

    DataSource realDataSource(EtlPipelineConfig config, boolean forceReprocessAll) {
      return super.createDataSource(config, forceReprocessAll);
    }
  }

  private EtlPipelineConfig config(FreshnessConfig freshness) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder().name("year").type(DimensionType.YEAR_RANGE)
        .start(2023).end(2024).build());
    return EtlPipelineConfig.builder()
        .name(TABLE)
        .source(HttpSourceConfig.builder().url("https://example.invalid/api/{year}").build())
        .dimensions(dims)
        .freshness(freshness)
        .lookbackPeriods(2)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .partition(MaterializePartitionConfig.builder()
                .columns(Collections.singletonList("year")).build())
            .output(MaterializeOutputConfig.builder().location(tempDir.toString()).build())
            .build())
        .build();
  }

  private static HttpSource.ProbeResult etag(String value) {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("ETag", value);
    return new HttpSource.ProbeResult(headers, null);
  }

  private int hashRunInvalidations() throws IOException {
    FreshnessConfig hash = FreshnessConfig.fromMap(
        Collections.<String, Object>singletonMap("type", "hash"));
    RecordingSource source = new RecordingSource(
        HttpSourceConfig.builder().url("https://example.invalid/api/{year}").build(),
        new HttpSource.ProbeResult(new HashMap<String, String>(), null));
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    new Pipeline(config(hash), new LocalFileStorageProvider(), tempDir.toString(),
        new MemoryFreshnessTracker(), new CountingDataProvider(Collections.singletonList(row)),
        new CountingDataWriter(), source).execute();
    return source.invalidations.get();
  }

  /** Second run sees a changed etag for every unit, so the per-unit gate reaches its drop. */
  private int changedTokenRunInvalidations() throws IOException {
    FreshnessConfig etag = FreshnessConfig.fromMap(
        Collections.<String, Object>singletonMap("type", "etag"));
    RecordingSource source = new RecordingSource(
        HttpSourceConfig.builder().url("https://example.invalid/api/{year}").build(),
        etag("v1"));
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();
    CountingDataProvider provider = new CountingDataProvider(Collections.singletonList(row));
    CountingDataWriter writer = new CountingDataWriter();
    StorageProvider sp = new LocalFileStorageProvider();
    new Pipeline(config(etag), sp, tempDir.toString(), tracker, provider, writer, source)
        .execute();
    source.probeResult = etag("v2");
    source.invalidations.set(0);
    new Pipeline(config(etag), sp, tempDir.toString(), tracker, provider, writer, source)
        .execute();
    return source.invalidations.get();
  }

  @Test void normalRunDropsEntryAtHashGate() throws IOException {
    assertTrue(hashRunInvalidations() > 0, "hash gate must drop the raw cache entry");
  }

  @Test void listedTableKeepsEntryAtHashGate() throws IOException {
    System.setProperty(REPARSE, TABLE);
    assertEquals(0, hashRunInvalidations());
  }

  @Test void forceDownloadWinsAtHashGate() throws IOException {
    System.setProperty(REPARSE, TABLE);
    System.setProperty(DOWNLOAD, TABLE);
    assertTrue(hashRunInvalidations() > 0);
  }

  @Test void unlistedTableStillDropsAtHashGate() throws IOException {
    System.setProperty(REPARSE, "some_other_table");
    assertTrue(hashRunInvalidations() > 0);
  }

  @Test void normalRunDropsEntryAtPerUnitGate() throws IOException {
    assertTrue(changedTokenRunInvalidations() > 0, "per-unit gate must drop on a changed token");
  }

  @Test void listedTableKeepsEntryAtPerUnitGate() throws IOException {
    System.setProperty(REPARSE, TABLE);
    assertEquals(0, changedTokenRunInvalidations());
  }

  private static boolean bypass(HttpSource source) throws Exception {
    Field f = HttpSource.class.getDeclaredField("bypassRawCache");
    f.setAccessible(true);
    return f.getBoolean(source);
  }

  private boolean bypassFor(boolean forceReprocessAll) throws Exception {
    Pipeline p = new Pipeline(config(null), new LocalFileStorageProvider(), tempDir.toString(),
        new MemoryFreshnessTracker(), null, null, null);
    return bypass((HttpSource) p.realDataSource(config(null), forceReprocessAll));
  }

  @Test void listedTableReadsCacheUnderForceReprocess() throws Exception {
    System.setProperty(REPARSE, TABLE);
    assertFalse(bypassFor(true));
  }

  @Test void unlistedTableForceStillBypasses() throws Exception {
    System.setProperty(REPARSE, "some_other_table");
    assertTrue(bypassFor(true));
  }

  @Test void unsetSettingForceStillBypasses() throws Exception {
    assertTrue(bypassFor(true));
  }

  @Test void forceDownloadWinsOverReparse() throws Exception {
    System.setProperty(REPARSE, TABLE);
    System.setProperty(DOWNLOAD, TABLE);
    assertTrue(bypassFor(true));
  }
}
