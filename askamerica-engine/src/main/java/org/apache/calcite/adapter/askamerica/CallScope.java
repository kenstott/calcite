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

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * One in-flight {@code tools/call}: the statements it has running, and the two ways it can be
 * stopped from outside before it finishes on its own.
 *
 * <p>The host abandons a tool call at about 60 s ("did not respond within 60s") and says so with
 * an MCP {@code notifications/cancelled} naming the request id. Before this class the engine
 * ignored that notification, so the abandoned statement kept running while holding
 * {@link McpServer#DB_LOCK}, and the next call queued behind it until the 180 s statement
 * timeout. Now the notification cancels every statement the call has registered, which ends the
 * blocked read on the worker thread and releases the lock.
 *
 * <p>The second way is the call's own time budget (stats tools only, see
 * {@link McpServer#statsCallBudgetSeconds()}): a call that is going to outlive the host's limit
 * is stopped by the engine before the host gives up, and the caller is told so explicitly
 * rather than seeing a silent timeout.
 *
 * <p>A scope is created on the stdio reader thread when the request arrives, so a cancellation
 * that overtakes a request still waiting for a worker thread finds it, and so the budget clock
 * starts where the host's clock does.
 */
final class CallScope {
  /** Raised on the worker thread once the scope has been stopped; carries the caller-facing
   *  explanation of why. */
  static final class Stopped extends RuntimeException {
    Stopped(String message) {
      super(message);
    }
  }

  private static final ConcurrentHashMap<String, CallScope> IN_FLIGHT = new ConcurrentHashMap<>();
  private static final ThreadLocal<CallScope> CURRENT = new ThreadLocal<>();
  private static final ScheduledExecutorService TIMER =
      Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "mcp-call-budget");
        t.setDaemon(true);
        return t;
      });

  private final String key;
  private final String tool;
  private final long startedAtMillis;
  private final long budgetMillis;
  private final Set<Statement> statements = ConcurrentHashMap.newKeySet();
  private volatile ScheduledFuture<?> timer;
  private volatile boolean lockAcquired;
  private volatile boolean hostCancelled;
  private volatile String stopMessage;

  private CallScope(String key, String tool, long budgetMillis) {
    this.key = key;
    this.tool = tool;
    this.startedAtMillis = System.currentTimeMillis();
    this.budgetMillis = budgetMillis;
  }

  /** Registers the scope for request {@code key} (idempotent) and arms its budget, if any.
   *  {@code budgetMillis <= 0} means no budget. */
  static CallScope begin(String key, String tool, long budgetMillis) {
    CallScope scope = IN_FLIGHT.computeIfAbsent(key, k -> {
      CallScope s = new CallScope(k, tool, budgetMillis);
      if (budgetMillis > 0) {
        s.timer = TIMER.schedule(s::budgetExpired, budgetMillis, TimeUnit.MILLISECONDS);
      }
      return s;
    });
    return scope;
  }

  /** Binds the scope to the calling thread, so statements it opens register with it. */
  void enter() {
    CURRENT.set(this);
  }

  /** Unbinds the scope and forgets it; the budget timer is disarmed. */
  void end() {
    ScheduledFuture<?> t = timer;
    if (t != null) {
      t.cancel(false);
    }
    IN_FLIGHT.remove(key, this);
    statements.clear();
    if (CURRENT.get() == this) {
      CURRENT.remove();
    }
  }

  /** The scope bound to this thread, or null when the thread is not serving a tool call. */
  static CallScope current() {
    return CURRENT.get();
  }

  /** Handles {@code notifications/cancelled}: stops the call and cancels its statements.
   *  Returns false when no such call is in flight (it already finished). */
  static boolean cancelRequest(String key, String reason) {
    CallScope scope = IN_FLIGHT.get(key);
    if (scope == null) {
      return false;
    }
    scope.hostCancelled = true;
    scope.stop(scope.tool + " was cancelled by the host"
        + (reason == null || reason.isEmpty() ? "" : " (" + reason + ")")
        + " after " + scope.elapsedSeconds() + "s; its running statement was cancelled and the "
        + "database released.");
    return true;
  }

  void lockAcquired() {
    lockAcquired = true;
  }

  boolean hostCancelled() {
    return hostCancelled;
  }

  boolean stopped() {
    return stopMessage != null;
  }

  String stopMessage() {
    return stopMessage;
  }

  long elapsedSeconds() {
    return (System.currentTimeMillis() - startedAtMillis) / 1000;
  }

  /** Milliseconds left of the budget; {@link Long#MAX_VALUE} when the call has none. */
  long remainingMillis() {
    return budgetMillis > 0
        ? budgetMillis - (System.currentTimeMillis() - startedAtMillis) : Long.MAX_VALUE;
  }

  private void budgetExpired() {
    long seconds = budgetMillis / 1000;
    String where = lockAcquired
        ? "the running statement was cancelled and the database released"
        : "it was still queued behind another database call and never started";
    stop(tool + " did not finish within " + seconds + "s (the host abandons a tool call at "
        + "about 60s), so it was stopped: " + where + ". Narrow the SQL (filter on the "
        + "partition columns, shorten the year range, aggregate in SQL before the model) and "
        + "retry.");
  }

  /** Marks the scope stopped, then cancels every registered statement. The message is set
   *  before the statements are walked, so a statement registered concurrently either sees it
   *  in {@link #register} or is cancelled here. */
  private void stop(String message) {
    if (stopMessage != null) {
      return;
    }
    stopMessage = message;
    for (Statement s : statements) {
      cancel(s);
    }
  }

  private void cancel(Statement s) {
    try {
      s.cancel();
    } catch (SQLException | RuntimeException e) {
      McpServer.logLine("[askamerica-mcp] call " + key + " (" + tool
          + "): Statement.cancel() failed: " + e);
    }
  }

  /** Adds a statement about to run; throws {@link Stopped} if the scope was already stopped. */
  void register(Statement s) {
    statements.add(s);
    check();
  }

  void unregister(Statement s) {
    statements.remove(s);
  }

  /** Throws {@link Stopped} if the scope has been stopped. */
  void check() {
    String m = stopMessage;
    if (m != null) {
      throw new Stopped(m);
    }
  }

  /** {@link #check()} on the current thread's scope; a no-op off a tool call. */
  static void checkCurrent() {
    CallScope s = CURRENT.get();
    if (s != null) {
      s.check();
    }
  }

  /** Registers {@code s} with the current thread's scope, if there is one. */
  static void registerCurrent(Statement s) {
    CallScope c = CURRENT.get();
    if (c != null) {
      c.register(s);
    }
  }

  static void unregisterCurrent(Statement s) {
    CallScope c = CURRENT.get();
    if (c != null) {
      c.unregister(s);
    }
  }
}
