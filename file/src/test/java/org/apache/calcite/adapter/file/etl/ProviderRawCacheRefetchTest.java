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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests that a freshness gate or lookback reopen reaches the source through a
 * {@link CachingDataProvider}'s raw cache, not only through {@link HttpSource}'s.
 */
@Tag("unit")
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class ProviderRawCacheRefetchTest {
  private static final String TABLE = "provider_refetch_test";
  private static final String REPARSE = "GOVDATA_REPARSE_CACHE_TABLES";

  @TempDir
  Path tempDir;

  @AfterEach void clearProperties() {
    System.clearProperty(REPARSE);
  }

  /** Provider over one in-memory "source" whose content the test changes between runs. */
  private static final class SourceProvider implements CachingDataProvider {
    private final boolean revisionKeyed;
    String content = "first";
    int downloads;
    final List<String> seen = new ArrayList<String>();

    SourceProvider(boolean revisionKeyed) {
      this.revisionKeyed = revisionKeyed;
    }

    @Override public boolean isCacheKeyedByRevision() {
      return revisionKeyed;
    }

    @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
        Map<String, String> variables, RawCache rawCache) throws IOException {
      try (InputStream in = rawCache.openStream("https://example.invalid/data", () -> {
        downloads++;
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
      });
          BufferedReader reader =
              new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        String value = reader.readLine();
        seen.add(value);
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("value", value);
        return Collections.singletonList(row).iterator();
      }
    }
  }

  private EtlPipelineConfig config(String freshnessType) {
    Map<String, Object> rawCache = new LinkedHashMap<String, Object>();
    rawCache.put("enabled", true);
    rawCache.put("sharedKey", tempDir.resolve("raw").toString());
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder().name("year").type(DimensionType.YEAR_RANGE)
        .start(2024).end(2024).build());
    return EtlPipelineConfig.builder()
        .name(TABLE)
        .source(HttpSourceConfig.builder().url("https://example.invalid/api/{year}")
            .rawCache(HttpSourceConfig.RawCacheConfig.fromMap(rawCache)).build())
        .dimensions(dims)
        .freshness(FreshnessConfig.fromMap(
            Collections.<String, Object>singletonMap("type", freshnessType)))
        .lookbackPeriods(1)
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .partition(MaterializePartitionConfig.builder()
                .columns(Collections.singletonList("year")).build())
            .output(MaterializeOutputConfig.builder()
                .location(tempDir.resolve("out").toString()).build())
            .build())
        .build();
  }

  /** Forgets what this JVM downloaded, as a later worker process would. */
  private static void startNewProcess() throws Exception {
    Field f = StorageRawCache.class.getDeclaredField("WRITTEN_THIS_RUN");
    f.setAccessible(true);
    ((Set<?>) f.get(null)).clear();
  }

  /** Runs twice, changing the source in between, and returns what the second run was handed. */
  private List<String> secondRunReads(SourceProvider provider, String freshnessType)
      throws Exception {
    EtlPipelineConfig config = config(freshnessType);
    IncrementalTracker tracker = new FreshnessSkipGateTest.MemoryFreshnessTracker();
    StorageProvider sp = new LocalFileStorageProvider();
    // The etag probe reports a new token on each run, so the per-unit gate sees a change.
    final String[] etag = {"v1"};
    HttpSource source = new HttpSource(config.getSource()) {
      @Override public HttpSource.ProbeResult probe(FreshnessConfig freshness,
          Map<String, String> variables) {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("ETag", etag[0]);
        return new HttpSource.ProbeResult(headers, null);
      }
    };
    new ReparseCacheTablesTest.Pipeline(config, sp, tempDir.toString(), tracker, provider,
        new ReleaseWindowSkipGateTest.CountingDataWriter(), source).execute();
    startNewProcess();
    provider.content = "second";
    provider.seen.clear();
    provider.downloads = 0;
    etag[0] = "v2";
    new ReparseCacheTablesTest.Pipeline(config, sp, tempDir.toString(), tracker, provider,
        new ReleaseWindowSkipGateTest.CountingDataWriter(), source).execute();
    return provider.seen;
  }

  @Test void hashGateReadsTheSourceOnceAndWritesWhatItHashed() throws Exception {
    SourceProvider provider = new SourceProvider(false);
    List<String> reads = secondRunReads(provider, "hash");
    assertEquals(java.util.Arrays.asList("second", "second"), reads);
    assertEquals(1, provider.downloads, "the write pass must read the entry the hash pass wrote");
  }

  @Test void changedTokenReachesTheSource() throws Exception {
    SourceProvider provider = new SourceProvider(false);
    assertEquals(Collections.singletonList("second"), secondRunReads(provider, "etag"));
  }

  @Test void revisionKeyedProviderKeepsItsEntry() throws Exception {
    SourceProvider provider = new SourceProvider(true);
    List<String> reads = secondRunReads(provider, "hash");
    assertEquals(Collections.singletonList("first"), reads);
    assertEquals(0, provider.downloads);
  }

  @Test void reparsedTableKeepsItsEntry() throws Exception {
    System.setProperty(REPARSE, TABLE);
    SourceProvider provider = new SourceProvider(false);
    secondRunReads(provider, "hash");
    assertEquals(0, provider.downloads);
  }
}
