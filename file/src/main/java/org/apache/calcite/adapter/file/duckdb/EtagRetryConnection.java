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
package org.apache.calcite.adapter.file.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wraps a DuckDB {@link Connection} so a live Iceberg commit racing a read is transient instead
 * of fatal.
 *
 * <p>{@code cftc_trades}-scale tables commit a new Iceberg snapshot roughly every 7-10 seconds
 * under the daily worker (observed directly: row count climbed ~55M rows across one test hour).
 * Two distinct DuckDB failure modes surface when a read's internal, multi-step resolution
 * straddles one of those commits, on both the primary (MinIO, written continuously by the ETL
 * pool) and the R2 mirror (updated in periodic pointer-last sync passes):
 *
 * <h3>1. httpfs ETag drift</h3>
 * {@code version-hint.text} (Iceberg's current-snapshot pointer) is deliberately excluded from
 * {@code cache_httpfs}'s content cache and read fresh on every access &mdash; see
 * {@link DuckDBJdbcSchemaFactory#addIcebergCacheExclusions}. But {@code cache_httpfs} tracks a
 * separate metadata/file-handle layer that is NOT covered by that exclusion: it remembers the
 * ETag it last saw for a path, and when a concurrent commit legitimately advances the pointer
 * between two queries on the same connection, that layer surfaces the change as a hard failure
 * instead of transparently re-reading:
 * <pre>
 *   HTTP Error: ETag on reading file ".../version-hint.text" was initially "X" and now it
 *   returned "Y", this likely means the remote file has changed.
 * </pre>
 *
 * <h3>2. View-on-view type-binding drift</h3>
 * A SQL view defined over an Iceberg-backed table (e.g. {@code credit_default_swaps FROM
 * cftc_trades WHERE asset_class = 'CREDITS'}) resolves and binds column types for both the outer
 * and inner {@code iceberg_scan} lazily, on first query (see {@code DuckDBPendingViews}: {@code
 * CREATE VIEW} itself is pure DDL, no binding). That resolution is not one atomic step; a commit
 * landing mid-resolution can make the outer view's newly-bound expectation disagree with what a
 * moment-later inner re-resolution reports, even though neither reflects a real Iceberg schema
 * change (verified: the table's metadata.json carries a single, unevolved schema-id and every
 * physical data file sampled old-to-new agrees on the column's type):
 * <pre>
 *   Binder Error: Contents of view were altered: types don't match!
 *   Expected [..., VARCHAR, ...], but found [..., VARCHAR[], ...] instead
 * </pre>
 * This is distinct from the known cross-session staleness {@code
 * DuckDBJdbcSchema.refreshStaleIcebergViewIfNeeded} guards against (a view surviving unchanged
 * across a real upstream schema change) — that check compares column *count* only, would not
 * catch a same-count type drift, and only runs for the base table's own conversion record, not
 * for SQL views layered on top of it.
 *
 * <p>Neither failure means the table is broken or the writer is misbehaving. On either signature,
 * clear the relevant {@code cache_httpfs} state and retry the statement once against a FRESH
 * {@link Statement}/{@link PreparedStatement} &mdash; DuckDB's JDBC driver leaves the original
 * unusable after either error ("Statement was closed" on re-invoke), so re-running the same
 * execute call on {@code real} is not an option. A second failure is a real error and propagates
 * normally.
 */
final class EtagRetryConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(EtagRetryConnection.class);

  /** Matches DuckDB httpfs's ETag-drift message and captures the file path it was reading. */
  private static final Pattern ETAG_DRIFT = Pattern.compile(
      "ETag on reading file \"([^\"]+)\".*likely means the remote file has changed");

  /** Matches DuckDB's view-on-view type-binding drift message (see class javadoc, mode 2). No file path is embedded, so recovery purges the whole cache_httpfs cache rather than one path. */
  private static final Pattern VIEW_TYPE_DRIFT = Pattern.compile(
      "Contents of view were altered: types don't match");

  private static final int MAX_ATTEMPTS = 2;

  private EtagRetryConnection() {
  }

  /** Wraps {@code conn} so every {@link Statement}/{@link PreparedStatement} it creates retries once on a live-commit race. */
  static Connection wrap(Connection conn) {
    return (Connection) Proxy.newProxyInstance(
        EtagRetryConnection.class.getClassLoader(),
        new Class<?>[] {Connection.class},
        new ConnectionHandler(conn));
  }

  /**
   * Classifies a caught exception as one of the two known transient live-commit races.
   *
   * @return the {@code cache_httpfs} path to evict (ETag drift), {@code ""} to evict the whole
   *     cache (view-type drift — no path is embedded in that message), or {@code null} if this
   *     is not a recognized transient race and should propagate without a retry.
   */
  private static String transientRaceTarget(Throwable t) {
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      String msg = cur.getMessage();
      if (msg == null) {
        continue;
      }
      Matcher etag = ETAG_DRIFT.matcher(msg);
      if (etag.find()) {
        return etag.group(1);
      }
      if (VIEW_TYPE_DRIFT.matcher(msg).find()) {
        return "";
      }
    }
    return null;
  }

  /** Evicts {@code path} (or the whole cache, if {@code path} is empty) from cache_httpfs so the next read resolves live. Failures are non-fatal: worst case the retry hits the same drift error and surfaces normally. */
  private static void purge(Connection realConn, String path) {
    String sql = path.isEmpty()
        ? "SELECT cache_httpfs_clear_cache()"
        : "SELECT cache_httpfs_clear_cache_for_file('" + path.replace("'", "''") + "')";
    try (Statement st = realConn.createStatement()) {
      st.execute(sql);
      LOGGER.info("cache_httpfs: evicted {} after live-commit race; retrying query",
          path.isEmpty() ? "whole cache" : "'" + path + "'");
    } catch (SQLException e) {
      LOGGER.debug("{} failed: {}", sql, e.getMessage());
    }
  }

  private static final class ConnectionHandler implements InvocationHandler {
    private final Connection real;

    ConnectionHandler(Connection real) {
      this.real = real;
    }

    @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        Object result = method.invoke(real, args);
        if (result instanceof Statement) {
          // Records exactly how this statement was created (createStatement()/prepareStatement()
          // and its args) so a retry can recreate an equivalent fresh one on the same connection.
          Class<?> iface = result instanceof PreparedStatement ? PreparedStatement.class : Statement.class;
          return Proxy.newProxyInstance(
              EtagRetryConnection.class.getClassLoader(),
              new Class<?>[] {iface},
              new StatementHandler((Statement) result, real, method, args));
        }
        return result;
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }

  /**
   * Applies to both {@link Statement} and {@link PreparedStatement}. Only execute* calls retry;
   * everything else passes through to the current underlying statement untouched.
   *
   * <p>For a {@link PreparedStatement}, parameter-setter calls (setString, setInt, ...) are
   * recorded in call order so they can be replayed against a freshly re-prepared statement before
   * retrying a no-arg {@code execute()}/{@code executeQuery()}/{@code executeUpdate()}.
   */
  private static final class StatementHandler implements InvocationHandler {
    private final Connection realConn;
    private final Method creator;   // Connection.createStatement / prepareStatement
    private final Object[] creatorArgs;
    private final boolean isPrepared;
    private Statement real;
    private final List<Object[]> paramCalls = new ArrayList<>();  // [Method, args] in call order, prepared only

    StatementHandler(Statement real, Connection realConn, Method creator, Object[] creatorArgs) {
      this.real = real;
      this.realConn = realConn;
      this.creator = creator;
      this.creatorArgs = creatorArgs;
      this.isPrepared = real instanceof PreparedStatement;
    }

    @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      boolean isExecute = "execute".equals(name) || "executeQuery".equals(name)
          || "executeUpdate".equals(name) || "executeLargeUpdate".equals(name);

      if (!isExecute) {
        try {
          Object result = method.invoke(real, args);
          // Record parameter binds (setString, setInt, setNull, setObject, clearParameters, ...)
          // so a re-prepared statement can be brought to the same bound state on retry.
          if (isPrepared && isParamSetter(method)) {
            if ("clearParameters".equals(name)) {
              paramCalls.clear();
            } else {
              paramCalls.add(new Object[] {method, args});
            }
          }
          return result;
        } catch (InvocationTargetException e) {
          throw e.getCause();
        }
      }

      SQLException last = null;
      for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        try {
          return method.invoke(real, args);
        } catch (InvocationTargetException e) {
          if (!(e.getCause() instanceof SQLException)) {
            throw e.getCause();
          }
          last = (SQLException) e.getCause();
          String target = transientRaceTarget(last);
          if (target == null || attempt == MAX_ATTEMPTS) {
            throw last;
          }
          purge(realConn, target);
          if (!recreate()) {
            // Couldn't rebuild a working statement to retry against — surface the original error.
            throw last;
          }
        }
      }
      throw last;
    }

    /** True for PreparedStatement parameter-binding methods (setXxx besides setFetchSize/setMaxRows/etc., which are set* but not binds). */
    private boolean isParamSetter(Method m) {
      String n = m.getName();
      if (!n.startsWith("set") && !"clearParameters".equals(n)) {
        return false;
      }
      Class<?>[] p = m.getParameterTypes();
      // Binds are setXxx(int parameterIndex, ...); statement-config setters (setFetchSize(int),
      // setQueryTimeout(int), setMaxRows(int), ...) take no leading parameter-index arg pattern
      // shared with binds beyond their single int — the reliable signal is >= 2 params with the
      // first being the 1-based parameter index.
      return "clearParameters".equals(n) || (p.length >= 2 && p[0] == int.class);
    }

    /** Rebuilds {@code real} as a fresh statement (re-prepared and re-bound, for a PreparedStatement) after ETag eviction. Returns false if recreation itself fails. */
    private boolean recreate() {
      try {
        try {
          real.close();
        } catch (SQLException ignored) {
          // Best-effort; the original may already be unusable, which is exactly why we're here.
        }
        Statement fresh = (Statement) creator.invoke(realConn, creatorArgs);
        if (isPrepared) {
          PreparedStatement freshPs = (PreparedStatement) fresh;
          for (Object[] call : paramCalls) {
            ((Method) call[0]).invoke(freshPs, (Object[]) call[1]);
          }
        }
        real = fresh;
        return true;
      } catch (ReflectiveOperationException e) {
        LOGGER.debug("Could not recreate statement after ETag eviction: {}", e.getMessage());
        return false;
      }
    }
  }
}
