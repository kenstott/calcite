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
package org.apache.calcite.adapter.govdata.sec;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Focused test for {@link EdgarRateLimiter} pacing.
 *
 * <p>Verifies that successive {@code acquire()} calls are spaced by at least the
 * configured interval (default 100ms = 10 req/s), i.e. the limiter actually
 * paces requests rather than letting them through unbounded.
 */
@Tag("unit")
public class EdgarRateLimiterTest {

  private String savedTmp;

  @BeforeEach void isolateHostState() throws Exception {
    savedTmp = System.getProperty("java.io.tmpdir");
    Path dir = Files.createTempDirectory(java.nio.file.Paths.get(savedTmp), "edgar-limiter");
    System.setProperty("java.io.tmpdir", dir.toString());
  }

  @AfterEach void restore() {
    System.setProperty("java.io.tmpdir", savedTmp);
  }

  @Test void refusalFromServerStopsFetchersForCooldown() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", ex -> {
      ex.sendResponseHeaders(403, -1);
      ex.close();
    });
    server.start();
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/submissions.json";
      EdgarRateLimiter.acquire();
      HttpURLConnection conn = (HttpURLConnection) java.net.URI.create(url).toURL().openConnection();
      assertEquals(403, conn.getResponseCode());
      EdgarRateLimiter.recordStatus(conn.getResponseCode(), url);
      assertThrows(IllegalStateException.class, EdgarRateLimiter::acquire);
      assertTrue(EdgarRateLimiter.blockedUntil() > System.currentTimeMillis());
    } finally {
      server.stop(0);
    }
  }

  @Test void tooManyRequestsWaitsThenRetriesAndSucceeds() throws Exception {
    declareUserAgent();
    final AtomicInteger hits = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", ex -> {
      if (hits.incrementAndGet() == 1) {
        ex.getResponseHeaders().add("Retry-After", "1");
        ex.sendResponseHeaders(429, -1);
      } else {
        ex.sendResponseHeaders(200, -1);
      }
      ex.close();
    });
    server.start();
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/s.json";
      long start = System.currentTimeMillis();
      HttpURLConnection conn = EdgarRateLimiter.connect(url, "HEAD", 2000, 2000, 10L, 3);
      long elapsed = System.currentTimeMillis() - start;
      assertEquals(200, conn.getResponseCode());
      assertEquals(2, hits.get());
      assertTrue(elapsed >= 1000L, "Retry-After of 1s not honoured: " + elapsed + "ms");
      assertEquals(0L, EdgarRateLimiter.blockedUntil());
    } finally {
      server.stop(0);
    }
  }

  @Test void tooManyRequestsFailsByNameAfterBoundedRetries() throws Exception {
    declareUserAgent();
    final AtomicInteger hits = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", ex -> {
      hits.incrementAndGet();
      ex.getResponseHeaders().add("Retry-After", "0");
      ex.sendResponseHeaders(429, -1);
      ex.close();
    });
    server.start();
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/CIK1.json";
      java.io.IOException e = assertThrows(java.io.IOException.class,
          () -> EdgarRateLimiter.connect(url, "GET", 2000, 2000, 10L, 2));
      assertTrue(e.getMessage().contains(url), e.getMessage());
      assertEquals(3, hits.get());
    } finally {
      server.stop(0);
    }
  }

  private static void declareUserAgent() {
    java.util.Map<String, Object> operand = new java.util.HashMap<String, Object>();
    operand.put("edgarUserAgent", "govdata-etl test@example.org");
    org.apache.calcite.adapter.file.etl.ModelOperand.capture("sec", operand);
  }

  @Test void successStatusDoesNotBlock() {
    EdgarRateLimiter.recordStatus(200, "http://x/");
    EdgarRateLimiter.recordStatus(404, "http://x/");
    EdgarRateLimiter.acquire();
    assertEquals(0L, EdgarRateLimiter.blockedUntil());
  }

  @Test void concurrentFetchersStayWithinBudget() throws Exception {
    final int threads = 4;
    final int perThread = 5;
    final List<Long> stamps = java.util.Collections.synchronizedList(new ArrayList<Long>());
    final AtomicInteger errors = new AtomicInteger();
    List<Thread> ts = new ArrayList<Thread>();
    long start = System.currentTimeMillis();
    for (int t = 0; t < threads; t++) {
      Thread th = new Thread(() -> {
        try {
          for (int i = 0; i < perThread; i++) {
            EdgarRateLimiter.acquire();
            stamps.add(System.currentTimeMillis());
          }
        } catch (RuntimeException e) {
          errors.incrementAndGet();
        }
      });
      ts.add(th);
      th.start();
    }
    for (Thread th : ts) {
      th.join();
    }
    long elapsed = System.currentTimeMillis() - start;
    assertEquals(0, errors.get());
    assertEquals(threads * perThread, stamps.size());
    // 20 requests at 125ms spacing need at least 19 gaps; allow slack for the first slot.
    assertTrue(elapsed >= 19L * 100L, "20 requests finished in " + elapsed + "ms, over the 8/s budget");
  }

  @Test void acquirePacesSuccessiveCalls() {
    // Default interval is 100ms. N calls => about (N-1) gaps of >=100ms each.
    int calls = 5;
    long start = System.currentTimeMillis();
    for (int i = 0; i < calls; i++) {
      EdgarRateLimiter.acquire();
    }
    long elapsed = System.currentTimeMillis() - start;

    // Conservative lower bound: at least 3 full inter-call gaps (allow scheduler slack).
    long minExpected = 3L * 100L;
    assertTrue(elapsed >= minExpected,
        "expected >= " + minExpected + "ms pacing for " + calls + " acquires, got " + elapsed + "ms");
  }
}
