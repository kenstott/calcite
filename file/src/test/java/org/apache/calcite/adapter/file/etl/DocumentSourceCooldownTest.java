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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** DocumentSource honours the host-wide cool-down and Retry-After. */
@Tag("unit")
class DocumentSourceCooldownTest {
  private HttpServer server;
  private final AtomicInteger hits = new AtomicInteger();
  private volatile int firstStatuses;
  private volatile boolean forbidden;

  @BeforeEach void setUp() throws IOException {
    Files.deleteIfExists(Paths.get(System.getProperty("java.io.tmpdir"),
        "calcite-ratelimit-localhost.blocked"));
    ModelOperand.capture("http",
        Collections.<String, Object>singletonMap("rateLimitRetryFloorMs", 1));
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/", ex -> {
      int n = hits.incrementAndGet();
      byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
      if (forbidden) {
        ex.sendResponseHeaders(403, -1);
      } else if (n <= firstStatuses) {
        ex.getResponseHeaders().add("Retry-After", "0");
        ex.sendResponseHeaders(429, -1);
      } else {
        ex.sendResponseHeaders(200, body.length);
        ex.getResponseBody().write(body);
      }
      ex.close();
    });
    server.start();
  }

  @AfterEach void tearDown() throws IOException {
    server.stop(0);
    Files.deleteIfExists(Paths.get(System.getProperty("java.io.tmpdir"),
        "calcite-ratelimit-localhost.blocked"));
  }

  private DocumentSource source() {
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost:" + server.getAddress().getPort() + "/").build();
    return new DocumentSource(config, new LocalFileStorageProvider(), "/tmp");
  }

  private String url() {
    return "http://localhost:" + server.getAddress().getPort() + "/doc";
  }

  @Test void retriesAfter429ThenSucceeds() throws IOException {
    firstStatuses = 1;
    assertEquals("ok\n", source().fetchUrlContentLive(url()));
    assertEquals(2, hits.get());
  }

  @Test void a403StartsTheCooldownAndTheNextFetchIsRefused() {
    forbidden = true;
    assertThrows(IOException.class, () -> source().fetchUrlContentLive(url()));
    assertEquals(1, hits.get());
    assertTrue(Files.exists(Paths.get(System.getProperty("java.io.tmpdir"),
        "calcite-ratelimit-localhost.blocked")));
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> source().fetchUrlContentLive(url()));
    assertTrue(e.getMessage().contains("localhost"));
    assertEquals(1, hits.get());
  }

  @Test void cooldownMarkerStopsFetchBeforeAnyRequest() {
    CrossProcessRateLimiter.block("localhost", 60_000L);
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> source().fetchUrlContentLive(url()));
    assertTrue(e.getMessage().contains("localhost"));
    assertEquals(0, hits.get());
  }
}
