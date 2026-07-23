/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the JDBC metering surface end-to-end without the govdata lake: an
 * in-memory DuckDB connection is wrapped by {@link UsageMetering} and pointed at a
 * local HTTP stub, so we can assert exactly what gets reported — and that metering
 * can be turned off.
 *
 * <p>Setup is inline per test (not in {@code @BeforeEach}) so the stub URL / sync
 * system properties are guaranteed set on the same thread immediately before the
 * connection is wrapped.
 */
@Execution(ExecutionMode.SAME_THREAD)  // shares global system properties (stub URL) — must not run in parallel
class UsageMeteringTest {

  private final BlockingQueue<String> bodies = new LinkedBlockingQueue<String>();
  private volatile long quotaRemaining = Long.MAX_VALUE;
  private volatile int quotaStatus = 200;  // HTTP status the /v1/quota stub returns

  private HttpServer startStub() throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/v1/quota", exchange -> {
      int status = quotaStatus;
      byte[] resp = status == 200
          ? ("{\"remaining_bytes\":" + quotaRemaining
              + ",\"limit_bytes\":53687091200,\"used_bytes\":0,\"tier\":\"starter\",\"period\":\"2026-07\"}")
              .getBytes(StandardCharsets.UTF_8)
          : "{\"error\":\"rejected\"}".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(status, resp.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(resp);
      }
    });
    server.createContext("/v1/metering/usage", exchange -> {
      bodies.offer(new String(readAll(exchange.getRequestBody()), StandardCharsets.UTF_8));
      byte[] resp = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, resp.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(resp);
      }
    });
    server.start();
    System.setProperty("askamerica.api.url",
        "http://127.0.0.1:" + server.getAddress().getPort());
    System.setProperty("askamerica.metering.sync", "true");
    return server;
  }

  private void stopStub(HttpServer server) {
    System.clearProperty("askamerica.api.url");
    System.clearProperty("askamerica.metering.sync");
    if (server != null) {
      server.stop(0);
    }
  }

  private Connection freshDuck() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection c = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement s = c.createStatement()) {
      s.execute("CREATE TABLE sec_filings AS "
          + "SELECT * FROM (VALUES (1,'Acme'),(2,'Beta'),(3,'Gamma')) t(cik, name)");
    }
    return c;
  }

  @Test void metersQueryAcrossTheJdbcSurface() throws Exception {
    HttpServer server = startStub();
    try {
      Connection conn = UsageMetering.wrap(freshDuck(), "aa_live_test_metered");
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery("SELECT cik, name FROM sec_filings")) {
        int rows = 0;
        while (rs.next()) {
          rows++;
        }
        assertEquals(3, rows);
      }
      conn.close();

      String body = bodies.poll(10, TimeUnit.SECONDS);
      assertNotNull(body, "expected a metering POST after the query");
      assertTrue(body.contains("\"row_count\":3"), body);
      assertTrue(body.contains("sec_filings"), body);
      assertTrue(body.matches("(?s).*\"actual_bytes\":[1-9][0-9]*.*"),
          "actual_bytes should be a positive egress estimate: " + body);
    } finally {
      stopStub(server);
    }
  }

  @Test void blocksQueryWhenQuotaExhausted() throws Exception {
    HttpServer server = startStub();
    quotaRemaining = 0;  // out of data
    try {
      Connection conn = UsageMetering.wrap(freshDuck(), "aa_live_test_over");
      Statement st = conn.createStatement();
      java.sql.SQLException ex = assertThrows(java.sql.SQLException.class,
          () -> st.executeQuery("SELECT cik, name FROM sec_filings"));
      assertTrue(ex.getMessage().toLowerCase().contains("quota"), ex.getMessage());
      assertNull(bodies.poll(1, TimeUnit.SECONDS), "a blocked query must not be metered");
      st.close();
      conn.close();
    } finally {
      stopStub(server);
    }
  }

  @Test void allowsQueryWhenQuotaAvailable() throws Exception {
    HttpServer server = startStub();
    quotaRemaining = 40L * 1024 * 1024 * 1024;  // plenty
    try {
      Connection conn = UsageMetering.wrap(freshDuck(), "aa_live_test_under");
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery("SELECT cik, name FROM sec_filings")) {
        int rows = 0;
        while (rs.next()) {
          rows++;
        }
        assertEquals(3, rows);
      }
      conn.close();
      assertNotNull(bodies.poll(10, TimeUnit.SECONDS), "an allowed query should meter");
    } finally {
      stopStub(server);
    }
  }

  @Test void blocksAndUnmetersWhenKeyRejected() throws Exception {
    // An explicit 401/403 from /v1/quota means the key is invalid, expired, or
    // revoked — the client must fail CLOSED (block), not fail open.
    HttpServer server = startStub();
    quotaStatus = 401;
    try {
      Connection conn = UsageMetering.wrap(freshDuck(), "aa_live_test_revoked");
      Statement st = conn.createStatement();
      java.sql.SQLException ex = assertThrows(java.sql.SQLException.class,
          () -> st.executeQuery("SELECT cik, name FROM sec_filings"));
      String msg = ex.getMessage().toLowerCase();
      assertTrue(msg.contains("invalid") || msg.contains("expired") || msg.contains("revoked"),
          ex.getMessage());
      assertNull(bodies.poll(1, TimeUnit.SECONDS), "a rejected query must not be metered");
      st.close();
      conn.close();
    } finally {
      stopStub(server);
    }
  }

  @Test void allowsQueryWhenQuotaServerErrors() throws Exception {
    // A transient infra error (5xx) must fail OPEN so a hiccup on our side never
    // hard-blocks a legitimate user.
    HttpServer server = startStub();
    quotaStatus = 503;
    try {
      Connection conn = UsageMetering.wrap(freshDuck(), "aa_live_test_5xx");
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery("SELECT cik, name FROM sec_filings")) {
        int rows = 0;
        while (rs.next()) {
          rows++;
        }
        assertEquals(3, rows);
      }
      conn.close();
    } finally {
      stopStub(server);
    }
  }

  @Test void bypassKeyDisablesMeteringOnlyWhenEnabled() throws Exception {
    // The bypass key is a secret; it is supplied to the runner, never committed.
    String bypass = System.getProperty("askamerica.test.bypass.key",
        System.getenv("ASKAMERICA_TEST_BYPASS_KEY"));
    Assumptions.assumeTrue(bypass != null && !bypass.isEmpty(),
        "no bypass key supplied (askamerica.test.bypass.key / ASKAMERICA_TEST_BYPASS_KEY)");

    HttpServer server = startStub();
    try {
      // Without the opt-in flag the probe token is treated as an ordinary key:
      // it goes through the quota gate and its query is metered.
      Connection normal = UsageMetering.wrap(freshDuck(), bypass);
      try (Statement st = normal.createStatement();
           ResultSet rs = st.executeQuery("SELECT cik, name FROM sec_filings")) {
        while (rs.next()) {
          // drain
        }
      }
      normal.close();
      assertNotNull(bodies.poll(10, TimeUnit.SECONDS),
          "without the flag, the probe token must NOT bypass metering");

      // With the flag set, the same token bypasses metering + quota entirely.
      System.setProperty("askamerica.selftest.enabled", "true");
      Connection bypassed = UsageMetering.wrap(freshDuck(), bypass);
      try (Statement st = bypassed.createStatement();
           ResultSet rs = st.executeQuery("SELECT cik, name FROM sec_filings")) {
        while (rs.next()) {
          // drain
        }
      }
      bypassed.close();
      assertNull(bodies.poll(1, TimeUnit.SECONDS),
          "with the flag, a self-test session must not report usage");
    } finally {
      System.clearProperty("askamerica.selftest.enabled");
      stopStub(server);
    }
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[4096];
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }
}
