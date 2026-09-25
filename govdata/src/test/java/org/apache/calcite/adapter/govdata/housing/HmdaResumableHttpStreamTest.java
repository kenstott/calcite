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
package org.apache.calcite.adapter.govdata.housing;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks that {@link HmdaLoanLevelAggregateProvider.ResumableHttpStream} delivers exactly
 * {@code Content-Length} bytes: it resumes a connection cut short with a {@code Range} request,
 * and fails rather than hand on a partial or restarted body.
 */
@Tag("unit")
class HmdaResumableHttpStreamTest {

  private static final int TOTAL = 1_000_000;
  private static final int CUT_AT = 300_000;

  private final byte[] body = new byte[TOTAL];
  private HttpServer server;
  private final AtomicInteger requests = new AtomicInteger();

  @BeforeEach void startServer() {
    for (int i = 0; i < TOTAL; i++) {
      body[i] = (byte) (i * 31 + i / 251);
    }
  }

  @AfterEach void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  private String serve(final boolean honourRange, final int cutFirstAt) throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/f", exchange -> {
      int call = requests.incrementAndGet();
      String range = exchange.getRequestHeaders().getFirst("Range");
      int from = 0;
      if (range != null && honourRange) {
        from = Integer.parseInt(range.substring("bytes=".length(), range.length() - 1));
        exchange.getResponseHeaders().add("Content-Range",
            "bytes " + from + "-" + (TOTAL - 1) + "/" + TOTAL);
        exchange.sendResponseHeaders(206, TOTAL - from);
      } else {
        exchange.sendResponseHeaders(200, TOTAL);
      }
      OutputStream out = exchange.getResponseBody();
      int end = call == 1 ? Math.min(cutFirstAt, TOTAL) : TOTAL;
      out.write(body, from, end - from);
      out.flush();
      // Closing before Content-Length bytes are written drops the connection mid-body.
      exchange.close();
    });
    server.start();
    return "http://127.0.0.1:" + server.getAddress().getPort() + "/f";
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }

  @Test void wholeBodyIsReadWithoutResume() throws IOException {
    String url = serve(true, TOTAL);
    try (InputStream in = HmdaLoanLevelAggregateProvider.ResumableHttpStream.open(url, 1)) {
      assertArrayEquals(body, readAll(in));
    }
    assertTrue(requests.get() == 1, "no resume expected, saw " + requests.get() + " requests");
  }

  @Test void connectionCutMidBodyResumesFromTheCutOffset() throws IOException {
    String url = serve(true, CUT_AT);
    try (InputStream in = HmdaLoanLevelAggregateProvider.ResumableHttpStream.open(url, 1)) {
      byte[] got = readAll(in);
      assertTrue(got.length == TOTAL, "got " + got.length + " bytes");
      assertArrayEquals(body, got);
    }
    assertTrue(requests.get() == 2, "expected one resume request, saw " + requests.get());
  }

  @Test void serverThatIgnoresRangeFailsInsteadOfRestarting() throws IOException {
    String url = serve(false, CUT_AT);
    try (InputStream in = HmdaLoanLevelAggregateProvider.ResumableHttpStream.open(url, 1)) {
      IOException e = assertThrows(IOException.class, () -> readAll(in));
      assertTrue(e.getMessage().contains("stalled at " + CUT_AT + " of " + TOTAL),
          e.getMessage());
    }
  }

  @Test void missingContentLengthIsRejected() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/f", exchange -> {
      exchange.sendResponseHeaders(200, 0);
      OutputStream out = exchange.getResponseBody();
      out.write(Arrays.copyOf(body, 100));
      exchange.close();
    });
    server.start();
    String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/f";
    assertThrows(IOException.class,
        () -> HmdaLoanLevelAggregateProvider.ResumableHttpStream.open(url, 1));
  }
}
