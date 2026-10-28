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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A 429 that says the token's per-day request limit is spent is not retried, and the origin is
 * not called again for the rest of the run; an ordinary 429 is still retried.
 *
 * <p>Uses a local {@link HttpServer} on a random port — no network access required.
 */
class HttpSourceQuotaTest {

  private HttpServer server;
  private String baseUrl;
  private final AtomicInteger hits = new AtomicInteger();

  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    baseUrl = "http://localhost:" + server.getAddress().getPort();
    server.start();
  }

  @AfterEach void stopServer() {
    server.stop(0);
  }

  private void answer(String path, final int status, final String body) {
    server.createContext(path, new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        hits.incrementAndGet();
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(bytes);
        }
      }
    });
  }

  private HttpSource source(String path) {
    Map<String, Object> responseMap = new LinkedHashMap<String, Object>();
    responseMap.put("format", "JSON");
    Map<String, Object> rateLimit = new LinkedHashMap<String, Object>();
    rateLimit.put("requestsPerSecond", 1000.0);
    rateLimit.put("retryOn", java.util.Arrays.asList(429, 503));
    rateLimit.put("maxRetries", 3);
    rateLimit.put("retryBackoffMs", 1);
    return new HttpSource(HttpSourceConfig.builder()
        .url(baseUrl + path)
        .response(HttpSourceConfig.ResponseConfig.fromMap(responseMap))
        .rateLimit(HttpSourceConfig.RateLimitConfig.fromMap(rateLimit))
        .build());
  }

  @Test @Tag("unit") void aSpentDailyQuotaIsNotRetriedAndNotCalledAgain() {
    answer("/quota", 429,
        "{\"status\" : \"429\", \"message\" : \"This token has reached its temporary request "
        + "limit of 10000 per day.\"}");

    IOException first = assertThrows(IOException.class,
        () -> source("/quota").fetch(Collections.<String, String>emptyMap()));
    assertTrue(first.getMessage().contains("daily request quota exhausted"), first.getMessage());
    assertEquals(1, hits.get(), "a spent quota is sent once, with no retries");

    assertThrows(IOException.class,
        () -> source("/quota").fetch(Collections.<String, String>emptyMap()));
    assertEquals(1, hits.get(), "the origin is not called again once its quota is spent");
  }
}
