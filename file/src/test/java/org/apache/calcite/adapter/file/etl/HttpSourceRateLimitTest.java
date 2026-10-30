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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An API token's rate limit applies to the origin, so two sources that call the same origin share
 * one pace; a source on a different origin is not slowed by them.
 *
 * <p>Uses a local {@link HttpServer} on a random port — no network access required.
 */
class HttpSourceRateLimitTest {

  private HttpServer server;
  private String baseUrl;

  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    baseUrl = "http://localhost:" + server.getAddress().getPort();
    server.createContext("/data", new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        byte[] bytes = "[{\"a\":1}]".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(bytes);
        }
      }
    });
    server.start();
  }

  @AfterEach void stopServer() {
    server.stop(0);
  }

  private static HttpSource source(String url, double requestsPerSecond) {
    Map<String, Object> responseMap = new LinkedHashMap<String, Object>();
    responseMap.put("format", "JSON");
    Map<String, Object> rateLimit = new LinkedHashMap<String, Object>();
    rateLimit.put("requestsPerSecond", requestsPerSecond);
    return new HttpSource(HttpSourceConfig.builder()
        .url(url)
        .response(HttpSourceConfig.ResponseConfig.fromMap(responseMap))
        .rateLimit(HttpSourceConfig.RateLimitConfig.fromMap(rateLimit))
        .build());
  }

  private static void fetch(HttpSource s) throws IOException {
    s.fetch(Collections.<String, String>emptyMap());
  }

  @Test @Tag("unit") void sourcesOnOneOriginSharePace() throws Exception {
    // 10 per second is a 100 ms interval. Two sources taking turns, six requests in all, must
    // span five intervals when they share the pace; separate limiters would finish in about half.
    HttpSource a = source(baseUrl + "/data?x=1", 10.0);
    HttpSource b = source(baseUrl + "/data?x=2", 10.0);
    // The clock starts before the first request: its slot opens at once, so the sixth opens five intervals
    // later however long the first request itself takes (starting after it let a slow first request, JIT and
    // client warm-up, shorten the measured span below the threshold).
    long start = System.nanoTime();
    fetch(a);
    fetch(b);
    fetch(a);
    fetch(b);
    fetch(a);
    fetch(b);
    long elapsedMs = (System.nanoTime() - start) / 1000000L;
    assertTrue(elapsedMs >= 450, "two sources on one origin must share one pace, took " + elapsedMs + " ms");
  }

  @Test @Tag("unit") void aDifferentOriginIsNotSlowedByThem() throws Exception {
    HttpSource busy = source(baseUrl + "/data", 2.0);  // 500 ms interval
    fetch(busy);
    fetch(busy);   // the slot for this origin is now ~1 s ahead
    // The same local server under another host spelling is another origin. 127.0.0.1 is loopback on every
    // OS; 127.0.0.2 is not routed on macOS, which made this test hang there.
    HttpSource other = source("http://127.0.0.1:" + server.getAddress().getPort() + "/data", 2.0);
    long start = System.nanoTime();
    fetch(other);
    long elapsedMs = (System.nanoTime() - start) / 1000000L;
    assertTrue(elapsedMs < 300, "another origin must not wait on this one's slot, took " + elapsedMs + " ms");
  }
}
