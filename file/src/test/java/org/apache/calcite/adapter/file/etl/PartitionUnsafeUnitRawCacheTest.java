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

import org.apache.calcite.adapter.file.etl.FreshnessSkipGateTest.MemoryFreshnessTracker;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * For a templated table the per-unit probe decides which of the processed units read the source: a
 * unit it cannot prove unchanged re-reads it, an unchanged one replays its raw cache entry. A
 * table whose fetch dimension is outside the partition key writes every unit on every run; one
 * partitioned by that dimension skips its unchanged units.
 */
@Tag("unit")
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class PartitionUnsafeUnitRawCacheTest {
  private static final String TABLE = "unsafe_unit_pipeline";
  private static final String REPARSE = "GOVDATA_REPARSE_CACHE_TABLES";

  @TempDir
  Path tempDir;

  private HttpServer server;
  /** Version the server serves for each unit; also its ETag when {@link #sendEtag}. */
  private final Map<String, String> versions = new ConcurrentHashMap<String, String>();
  private final List<String> downloads = Collections.synchronizedList(new ArrayList<String>());
  private volatile boolean sendEtag = true;
  private final List<String> written = Collections.synchronizedList(new ArrayList<String>());
  private MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();
  private boolean partitionedByUnit;

  @BeforeEach void startServer() throws IOException {
    versions.put("a", "1");
    versions.put("b", "1");
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/data/", new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String unit = path.substring(path.lastIndexOf('/') + 1);
        String version = versions.get(unit);
        if (sendEtag) {
          exchange.getResponseHeaders().add("ETag", "\"" + unit + version + "\"");
        }
        if ("HEAD".equals(exchange.getRequestMethod())) {
          exchange.sendResponseHeaders(200, -1);
        } else {
          downloads.add(unit);
          byte[] bytes = ("[{\"v\":\"" + unit + version + "\"}]")
              .getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
          }
        }
        exchange.close();
      }
    });
    server.start();
  }

  @AfterEach void stopServer() {
    server.stop(0);
    System.clearProperty(REPARSE);
  }

  private void run() throws IOException {
    downloads.clear();
    written.clear();
    Map<String, Object> rawCache = new HashMap<String, Object>();
    rawCache.put("enabled", true);
    rawCache.put("sharedKey", tempDir.resolve("raw").toString());
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("http://127.0.0.1:" + server.getAddress().getPort() + "/data/{unit}")
        .rawCache(HttpSourceConfig.RawCacheConfig.fromMap(rawCache))
        .build();
    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("unit", DimensionConfig.builder()
        .name("unit")
        .type(DimensionType.LIST)
        .values(Arrays.asList("a", "b"))
        .build());
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name(TABLE)
        .source(source)
        .dimensions(dimensions)
        .freshness(
            FreshnessConfig.fromMap(Collections.<String, Object>singletonMap("type", "etag")))
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder().location(tempDir.toString()).build())
            .partition(MaterializePartitionConfig.builder()
                .columns(partitionedByUnit
                    ? Collections.singletonList("unit") : Collections.<String>emptyList())
                .build())
            .build())
        .build();
    DataWriter writer = new DataWriter() {
      @Override public long write(EtlPipelineConfig c, Iterator<Map<String, Object>> data,
          Map<String, String> variables) {
        long n = 0;
        while (data.hasNext()) {
          written.add((String) data.next().get("v"));
          n++;
        }
        return n;
      }
    };
    new EtlPipeline(config, new LocalFileStorageProvider(), tempDir.toString(), null, tracker,
        null, writer).execute();
  }

  private static List<String> sorted(List<String> values) {
    List<String> copy = new ArrayList<String>(values);
    Collections.sort(copy);
    return copy;
  }

  @Test void onlyTheChangedUnitReadsTheSource() throws IOException {
    run();
    versions.put("a", "2");
    run();

    assertEquals(Collections.singletonList("a"), downloads);
    assertEquals(Arrays.asList("a2", "b1"), sorted(written));
  }

  @Test void unchangedUnitsAreWrittenFromTheCache() throws IOException {
    run();
    run();

    assertEquals(Collections.<String>emptyList(), downloads);
    assertEquals(Arrays.asList("a1", "b1"), sorted(written));
  }

  @Test void unitWithoutStoredTokenReadsTheSource() throws IOException {
    run();
    versions.put("a", "2");
    tracker = new MemoryFreshnessTracker();
    run();

    assertEquals(Arrays.asList("a", "b"), sorted(downloads));
    assertEquals(Arrays.asList("a2", "b1"), sorted(written));
  }

  @Test void probeWithoutTokenRefetchesOnEveryRun() throws IOException {
    sendEtag = false;
    run();
    versions.put("b", "2");
    run();

    assertEquals(Arrays.asList("a", "b"), sorted(downloads));
    assertEquals(Arrays.asList("a1", "b2"), sorted(written));
  }

  @Test void skipSafeUnitWithoutStoredTokenReadsTheSource() throws IOException {
    partitionedByUnit = true;
    run();
    versions.put("a", "2");
    tracker = new MemoryFreshnessTracker();
    run();

    assertEquals(Arrays.asList("a", "b"), sorted(downloads));
    assertEquals(Arrays.asList("a2", "b1"), sorted(written));
  }

  @Test void skipSafeProbeWithoutTokenRefetchesOnEveryRun() throws IOException {
    partitionedByUnit = true;
    sendEtag = false;
    run();
    versions.put("b", "2");
    run();

    assertEquals(Arrays.asList("a", "b"), sorted(downloads));
    assertEquals(Arrays.asList("a1", "b2"), sorted(written));
  }

  @Test void skipSafeUnchangedUnitsAreSkipped() throws IOException {
    partitionedByUnit = true;
    run();
    versions.put("a", "2");
    run();

    assertEquals(Collections.singletonList("a"), downloads);
    assertEquals(Collections.singletonList("a2"), written);
  }

  @Test void skipSafeReparseListedTableReadsTheCache() throws IOException {
    partitionedByUnit = true;
    run();
    tracker = new MemoryFreshnessTracker();
    versions.put("a", "2");
    System.setProperty(REPARSE, TABLE);
    run();

    assertEquals(Collections.<String>emptyList(), downloads);
    assertEquals(Arrays.asList("a1", "b1"), sorted(written));
  }

  @Test void reparseListedTableReadsTheCache() throws IOException {
    run();
    versions.put("a", "2");
    System.setProperty(REPARSE, TABLE);
    run();

    assertEquals(Collections.<String>emptyList(), downloads);
    assertEquals(Arrays.asList("a1", "b1"), sorted(written));
  }
}
