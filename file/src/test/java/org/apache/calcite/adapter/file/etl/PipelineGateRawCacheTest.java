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
import org.apache.calcite.adapter.file.partition.IncrementalTracker;
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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A pipeline-level freshness gate that reports CHANGED must put the units' fetches in front of the
 * source: a raw cache entry is validated by existence alone, so reading it would rewrite the table
 * with the first fetch's bytes. The tables here declare no dimensions, so no per-unit gate runs.
 */
@Tag("unit")
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class PipelineGateRawCacheTest {
  private static final String TABLE = "gate_cache_pipeline";
  private static final String REPARSE = "GOVDATA_REPARSE_CACHE_TABLES";

  @TempDir
  Path tempDir;

  private HttpServer server;
  private final AtomicInteger downloads = new AtomicInteger();
  private volatile String body = "[{\"v\":\"first\"}]";
  private volatile int getStatus = 200;
  private volatile String etag;
  private final List<Object> written = new ArrayList<Object>();
  private final MemoryFreshnessTracker tracker = new MemoryFreshnessTracker();

  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/data", new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        if (etag != null) {
          exchange.getResponseHeaders().add("ETag", etag);
        }
        if ("HEAD".equals(exchange.getRequestMethod())) {
          exchange.sendResponseHeaders(200, -1);
        } else {
          downloads.incrementAndGet();
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(getStatus, bytes.length);
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
    HttpSourceConfig source = HttpSourceConfig.builder()
        .url("http://127.0.0.1:" + server.getAddress().getPort() + "/data")
        .rawCache(HttpSourceConfig.RawCacheConfig.fromMap(rawCacheMap()))
        .build();
    EtlPipelineConfig config = EtlPipelineConfig.builder()
        .name(TABLE)
        .source(source)
        .freshness(
            FreshnessConfig.fromMap(Collections.<String, Object>singletonMap("type", "etag")))
        .materialize(MaterializeConfig.builder()
            .format(MaterializeConfig.Format.PARQUET)
            .output(MaterializeOutputConfig.builder().location(tempDir.toString()).build())
            .build())
        .build();
    DataWriter writer = new DataWriter() {
      @Override public long write(EtlPipelineConfig c, Iterator<Map<String, Object>> data,
          Map<String, String> variables) {
        long n = 0;
        while (data.hasNext()) {
          written.add(data.next().get("v"));
          n++;
        }
        return n;
      }
    };
    IncrementalTracker t = tracker;
    new EtlPipeline(config, new LocalFileStorageProvider(), tempDir.toString(), null, t,
        null, writer).execute();
  }

  private Map<String, Object> rawCacheMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("enabled", true);
    map.put("sharedKey", tempDir.resolve("raw").toString());
    return map;
  }

  private String cachedBody() throws IOException {
    try (java.util.stream.Stream<Path> files = Files.walk(tempDir.resolve("raw"))) {
      File entry = files.map(Path::toFile).filter(File::isFile)
          .filter(f -> f.getName().startsWith("response")).findFirst().get();
      return new String(Files.readAllBytes(entry.toPath()), StandardCharsets.UTF_8);
    }
  }

  @Test void probeWithoutTokenRefetchesOnEveryRun() throws IOException {
    run();
    body = "[{\"v\":\"second\"}]";
    run();

    assertEquals(2, downloads.get());
    assertEquals(java.util.Arrays.<Object>asList("first", "second"), written);
    assertTrue(cachedBody().contains("second"), "the entry mirrors the latest fetch");
  }

  @Test void changedTokenRefetches() throws IOException {
    etag = "\"v1\"";
    run();
    etag = "\"v2\"";
    body = "[{\"v\":\"second\"}]";
    run();

    assertEquals(java.util.Arrays.<Object>asList("first", "second"), written);
  }

  @Test void unchangedTokenFetchesNothing() throws IOException {
    etag = "\"v1\"";
    run();
    run();

    assertEquals(1, downloads.get());
    assertEquals(Collections.<Object>singletonList("first"), written);
  }

  @Test void failedRefetchLeavesTheCachedEntry() throws IOException {
    run();
    getStatus = 404;
    body = "gone";
    run();

    assertEquals(2, downloads.get(), "the source was asked");
    assertEquals(Collections.<Object>singletonList("first"), written);
    assertTrue(cachedBody().contains("first"), "a failed fetch keeps the previous entry");
  }

  @Test void reparseListedTableReadsTheCache() throws IOException {
    run();
    body = "[{\"v\":\"second\"}]";
    System.setProperty(REPARSE, TABLE);
    run();

    assertEquals(1, downloads.get());
    assertEquals(java.util.Arrays.<Object>asList("first", "first"), written);
  }
}
