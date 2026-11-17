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

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The wait between retries of a throttled request: a positive Retry-After is honoured, a zero or
 * absent one falls back to the configured exponential backoff, and retries stop at the
 * configured attempt count.
 */
@Tag("unit")
class HttpSourceRetryWaitTest {

  @Test void zeroRetryAfterUsesConfiguredBackoff() {
    assertEquals(15000L, HttpSource.retryWaitMs(0L, 15000L, 1));
    assertEquals(30000L, HttpSource.retryWaitMs(0L, 15000L, 2));
    assertEquals(60000L, HttpSource.retryWaitMs(0L, 15000L, 3));
  }

  @Test void absentRetryAfterUsesConfiguredBackoff() {
    assertEquals(1000L, HttpSource.retryWaitMs(-1L, 1000L, 1));
    assertEquals(4000L, HttpSource.retryWaitMs(-1L, 1000L, 3));
  }

  @Test void positiveRetryAfterIsHonoured() {
    assertEquals(7000L, HttpSource.retryWaitMs(7000L, 15000L, 1));
    assertEquals(7000L, HttpSource.retryWaitMs(7000L, 15000L, 3));
  }

  @Test void retriesStopAtConfiguredAttemptsOnZeroRetryAfter() throws Exception {
    final AtomicInteger requests = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/limited", exchange -> {
      requests.incrementAndGet();
      exchange.getResponseHeaders().add("Retry-After", "0");
      byte[] body = "error code: 1015".getBytes("UTF-8");
      exchange.sendResponseHeaders(429, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    server.start();
    try {
      Map<String, Object> rateLimit = new HashMap<String, Object>();
      rateLimit.put("maxRetries", 2);
      rateLimit.put("retryBackoffMs", 1);
      HttpSourceConfig config = HttpSourceConfig.builder()
          .url("http://127.0.0.1:" + server.getAddress().getPort() + "/limited")
          .rateLimit(HttpSourceConfig.RateLimitConfig.fromMap(rateLimit))
          .build();
      HttpSource source = new HttpSource(config);
      assertThrows(IOException.class, () -> source.fetch(new HashMap<String, String>()));
      assertEquals(3, requests.get());
    } finally {
      server.stop(0);
    }
  }
}
