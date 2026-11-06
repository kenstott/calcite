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
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Issue #954: a stats call that outlives the host's ~60 s limit used to keep its statement
 * running while holding {@link McpServer#DB_LOCK}. These tests run a deliberately slow
 * statement on an in-process DuckDB connection (the real data server was not available) and
 * check that a host cancellation, or the call's own budget, ends it and frees the lock, and
 * that the diagnostics no longer re-run the caller's SQL when the rows are already in hand.
 */
@Tag("unit")
class CallScopeTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Roughly 10^12 multiplications: far longer than any test waits, so only a cancel ends it. */
  private static final String SLOW_SQL =
      "SELECT sum(a.range * b.range) AS y, 'e' AS ent, 't' AS tm "
          + "FROM range(1000000) a, range(1000000) b";

  /** Runs the slow extraction on its own thread, bound to {@code scope}. */
  private static Thread runSlow(final CallScope scope, final Connection c,
      final AtomicReference<Throwable> outcome) {
    Thread t = new Thread(() -> {
      scope.enter();
      scope.lockAcquired();
      try {
        StatsEngine.extractColumnsWithLabels(c, SLOW_SQL, new String[] {"y"},
            new String[] {"ent", "tm"});
      } catch (Throwable e) {
        outcome.set(e);
      } finally {
        scope.end();
      }
    }, "slow-call");
    t.setDaemon(true);
    t.start();
    return t;
  }

  private static void awaitRunning(Connection c) throws Exception {
    // The statement is registered before it executes; give DuckDB a moment to be inside it.
    Thread.sleep(700);
  }

  @Test void hostCancellationStopsTheRunningStatementAndFreesTheLock() throws Exception {
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
      CallScope scope = CallScope.begin("cancel-1", "panel_fixed_effects", 0);
      AtomicReference<Throwable> outcome = new AtomicReference<>();
      final AtomicReference<Boolean> lockFreed = new AtomicReference<>(false);
      // Mirrors dispatch(): the call holds DB_LOCK for as long as its statement runs.
      Thread holder = new Thread(() -> {
        synchronized (McpServer.DB_LOCK) {
          scope.enter();
          try {
            scope.lockAcquired();
            StatsEngine.extractColumnsWithLabels(c, SLOW_SQL, new String[] {"y"},
                new String[] {"ent", "tm"});
          } catch (Throwable e) {
            outcome.set(e);
          } finally {
            scope.end();
          }
        }
        lockFreed.set(true);
      }, "lock-holder");
      holder.setDaemon(true);
      holder.start();
      awaitRunning(c);
      long t0 = System.currentTimeMillis();
      assertTrue(CallScope.cancelRequest("cancel-1", "user pressed stop"));
      holder.join(20_000);
      long took = System.currentTimeMillis() - t0;
      assertFalse(holder.isAlive(), "the cancelled call must release DB_LOCK, not run on");
      assertTrue(lockFreed.get());
      assertTrue(took < 15_000, "cancel took " + took + "ms");
      assertNotNull(outcome.get(), "a cancelled statement ends in an exception");
      assertTrue(scope.hostCancelled());
      assertTrue(scope.stopMessage().contains("cancelled by the host"), scope.stopMessage());
      synchronized (McpServer.DB_LOCK) {
        // reaching here at all is the assertion: the next call is not queued behind the first
      }
    }
  }

  @Test void callBudgetStopsTheStatementAndSaysSo() throws Exception {
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
      CallScope scope = CallScope.begin("budget-1", "panel_fixed_effects", 1000);
      AtomicReference<Throwable> outcome = new AtomicReference<>();
      Thread t = runSlow(scope, c, outcome);
      t.join(20_000);
      assertFalse(t.isAlive(), "the budget must end the statement");
      assertNotNull(outcome.get());
      assertTrue(scope.stopped());
      assertFalse(scope.hostCancelled());
      String msg = scope.stopMessage();
      assertTrue(msg.contains("did not finish within 1s"), msg);
      assertTrue(msg.contains("statement was cancelled"), msg);
    }
  }

  @Test void aCallStoppedWhileQueuedNeverStarts() {
    CallScope scope = CallScope.begin("queued-1", "panel_fixed_effects", 0);
    assertTrue(CallScope.cancelRequest("queued-1", null));
    try {
      scope.check();
      throw new AssertionError("a stopped scope must refuse to start work");
    } catch (CallScope.Stopped expected) {
      assertTrue(expected.getMessage().contains("cancelled by the host"));
    } finally {
      scope.end();
    }
  }

  @Test void cancellingAnUnknownRequestIsReportedAsUnknown() {
    assertFalse(CallScope.cancelRequest("no-such-request", null));
  }

  @Test void probeTimeoutNeverExceedsWhatTheCallHasLeft() {
    CallScope scope = CallScope.begin("probe-1", "panel_fixed_effects", 3000);
    scope.enter();
    try {
      assertTrue(McpServer.probeTimeoutSeconds() <= 3,
          "probe timeout " + McpServer.probeTimeoutSeconds());
    } finally {
      scope.end();
    }
  }

  @Test void extractionKeepsTheRowsSoDiagnosticsNeedNotRerunTheSql() throws Exception {
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
         Statement s = c.createStatement()) {
      s.execute("CREATE TABLE t AS SELECT * FROM (VALUES ('Alabama', 1, 2.0), "
          + "('Delaware', 2, 3.0), ('Ohio', 3, 5.0)) v(name, k, y)");
      StatsEngine.LabeledExtraction ex = StatsEngine.extractColumnsWithLabels(c,
          "SELECT name, k, y FROM t WHERE name <> 'Delaware'", new String[] {"y"},
          new String[] {"name"});
      assertNotNull(ex.sampleRows);
      assertEquals(2, ex.sampleRows.size());
      assertEquals("Alabama", ex.sampleRows.get(0).get("name").asText());
    }
  }

  private static ArrayNode rows(String... names) {
    ArrayNode a = MAPPER.createArrayNode();
    for (String n : names) {
      ObjectNode r = MAPPER.createObjectNode();
      r.put("name", n);
      a.add(r);
    }
    return a;
  }

  @Test void exclusionProbeUsesRowsInHandAndRunsOnlyTheRelaxedSql() {
    AtomicInteger runs = new AtomicInteger();
    AtomicReference<String> ran = new AtomicReference<>();
    String sql = "SELECT name, y FROM t WHERE name <> 'Delaware'";
    ObjectNode out = QuestionDiagnostics.forExtraction(sql,
        Collections.<String>emptyList(), null, 2, 2, 0, null, relaxed -> {
          runs.incrementAndGet();
          ran.set(relaxed);
          return rows("Alabama", "Delaware", "Ohio");
        }, rows("Alabama", "Ohio"));
    assertEquals(1, runs.get(), "only the relaxed copy may run; the original rows are in hand");
    assertTrue(!ran.get().contains("Delaware"), "the executed statement is the relaxed one");
    assertTrue(out.toString().contains("Delaware"), out.toString());
  }

  @Test void exclusionProbeThatCannotRunSaysSoInTheResult() {
    String sql = "SELECT name, y FROM t WHERE name <> 'Delaware'";
    ObjectNode out = QuestionDiagnostics.forExtraction(sql,
        Collections.<String>emptyList(), null, 2, 2, 0, null, relaxed -> {
          throw new java.sql.SQLException("the diagnostic query did not finish within its 10s "
              + "budget, so it was skipped");
        }, rows("Alabama", "Ohio"));
    assertTrue(out.toString().contains("excluded_units_unavailable"), out.toString());
    assertTrue(out.toString().contains("did not finish within its 10s budget"), out.toString());
  }
}
