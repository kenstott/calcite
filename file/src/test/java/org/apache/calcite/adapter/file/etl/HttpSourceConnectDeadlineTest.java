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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression for kenstott/govdata-ops#236/#139/#121: {@code HttpURLConnection}'s
 * {@code setConnectTimeout}/{@code setReadTimeout} bound the socket-level connect and the
 * response read, but not the DNS resolution that happens first when {@code getResponseCode()}
 * (or an implicit connect via {@code getOutputStream()}) is called — a stalled/blackholed
 * resolver blocks the calling thread indefinitely, with zero CPU and no timeout ever firing,
 * regardless of either configured value. Three unrelated upstream hosts (FMCSA, Census, USITC)
 * produced the identical near-zero-CPU, multi-hour hang before this fix.
 *
 * <p>Tests {@code HttpSource#connectWithDeadline}/{@code connectOnlyWithDeadline} directly via
 * reflection (both private static), using a fake {@link HttpURLConnection} whose
 * {@code getResponseCode()}/{@code connect()} is fully controllable — this exercises the exact
 * mechanism (a bounded {@code Future} wrapping the call) without depending on real network or
 * DNS conditions, which can't be reliably forced to stall in a unit test.
 */
@Tag("unit")
class HttpSourceConnectDeadlineTest {

  /** A connection whose {@code getResponseCode()} blocks on a latch until released, and whose
   * {@code connect()} either returns immediately or blocks the same way — simulating an
   * indefinite DNS/connect stall that neither connectTimeout nor readTimeout would catch. */
  private static HttpURLConnection latchGatedConnection(CountDownLatch releaseGate,
      boolean blockOnConnect) throws Exception {
    URL url = new URL("http://198.51.100.1.invalid/probe");
    return new HttpURLConnection(url) {
      @Override public void connect() throws IOException {
        if (blockOnConnect) {
          await(releaseGate);
        }
      }

      @Override public void disconnect() {
        // no-op: this fake never opens a real socket
      }

      @Override public boolean usingProxy() {
        return false;
      }

      @Override public int getResponseCode() throws IOException {
        if (!blockOnConnect) {
          await(releaseGate);
        }
        return 200;
      }
    };
  }

  private static void await(CountDownLatch gate) throws IOException {
    try {
      gate.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted", e);
    }
  }

  private static Method connectWithDeadlineMethod() throws Exception {
    Method m = HttpSource.class.getDeclaredMethod("connectWithDeadline", HttpURLConnection.class);
    m.setAccessible(true);
    return m;
  }

  private static Method connectOnlyWithDeadlineMethod() throws Exception {
    Method m = HttpSource.class.getDeclaredMethod("connectOnlyWithDeadline", HttpURLConnection.class);
    m.setAccessible(true);
    return m;
  }

  @Test void connectWithDeadlineReturnsPromptlyWhenNotStalled() throws Exception {
    HttpURLConnection conn = latchGatedConnection(new CountDownLatch(0), false);
    conn.setConnectTimeout(1000);
    conn.setReadTimeout(1000);

    long start = System.nanoTime();
    Object result = connectWithDeadlineMethod().invoke(null, conn);
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    assertEquals(200, (int) (Integer) result);
    assertTrue(elapsedMs < 2000, "should return near-instantly for a connection that isn't "
        + "stalled, took " + elapsedMs + "ms");
  }

  @Test void connectWithDeadlineThrowsInsteadOfHangingForeverOnAStalledGetResponseCode()
      throws Exception {
    // Never released within the test - simulates the exact indefinite DNS/connect stall this
    // fix targets. Small connectTimeout/readTimeout so the derived deadline (connectTimeout +
    // readTimeout + the fixed DNS buffer) stays test-fast rather than the production default.
    HttpURLConnection conn = latchGatedConnection(new CountDownLatch(1), false);
    conn.setConnectTimeout(100);
    conn.setReadTimeout(100);

    long start = System.nanoTime();
    InvocationTargetException thrown = org.junit.jupiter.api.Assertions.assertThrows(
        InvocationTargetException.class, () -> connectWithDeadlineMethod().invoke(null, conn));
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    assertTrue(thrown.getCause() instanceof IOException,
        "expected IOException, got " + thrown.getCause());
    assertTrue(thrown.getCause().getMessage().contains("deadline exceeded"),
        "message should name the deadline as the cause: " + thrown.getCause().getMessage());
    // Deadline = connectTimeout(100) + readTimeout(100) + the fixed DNS buffer - well under a
    // real caller's own connect/read timeouts would otherwise let this run for indefinitely,
    // but still finite and bounded rather than never returning at all.
    assertTrue(elapsedMs < 60_000, "deadline should have fired well under 60s, took "
        + elapsedMs + "ms");
  }

  @Test void connectOnlyWithDeadlineCallsConnectAndReturnsWithoutReadingResponse()
      throws Exception {
    HttpURLConnection conn = latchGatedConnection(new CountDownLatch(0), true);
    conn.setConnectTimeout(1000);
    conn.setReadTimeout(1000);

    long start = System.nanoTime();
    Object result = connectOnlyWithDeadlineMethod().invoke(null, conn);
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    assertNull(result, "connectOnlyWithDeadline returns void (null via reflection)");
    assertTrue(elapsedMs < 2000, "should return near-instantly for a connect that isn't "
        + "stalled, took " + elapsedMs + "ms");
  }

  @Test void connectOnlyWithDeadlineThrowsInsteadOfHangingForeverOnAStalledConnect()
      throws Exception {
    // blockOnConnect=true: the pre-body-write connect() step itself stalls - this is the gap
    // that a POST/PUT source's conn.getOutputStream() would otherwise hit with no deadline at
    // all, since that call implicitly connects before connectWithDeadline's later
    // getResponseCode() call ever runs.
    HttpURLConnection conn = latchGatedConnection(new CountDownLatch(1), true);
    conn.setConnectTimeout(100);
    conn.setReadTimeout(100);

    InvocationTargetException thrown = org.junit.jupiter.api.Assertions.assertThrows(
        InvocationTargetException.class, () -> connectOnlyWithDeadlineMethod().invoke(null, conn));

    assertTrue(thrown.getCause() instanceof IOException,
        "expected IOException, got " + thrown.getCause());
    assertTrue(thrown.getCause().getMessage().contains("deadline exceeded"),
        "message should name the deadline as the cause: " + thrown.getCause().getMessage());
  }
}
