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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Deferred DuckDB view registration with retry-until-convergence.
 *
 * <p>Views defined in schema YAMLs may reference tables or other views that
 * do not yet exist when the schema is first initialized (e.g. cross-schema
 * references, or view-on-view chains). This class collects all pending view
 * definitions during schema initialization and flushes them lazily on the
 * first query, by which point all schemas and their base tables are registered.
 *
 * <p>The flush loop retries failed views until either all succeed or a full
 * round produces no progress (indicating a genuine error or circular dependency).
 */
public final class DuckDBPendingViews {

  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBPendingViews.class);

  /** Pending views keyed by canonical DuckDB database file path. */
  private static final ConcurrentHashMap<String, CopyOnWriteArrayList<PendingView>> PENDING =
      new ConcurrentHashMap<>();

  /** Databases whose views have already been flushed. */
  private static final Set<String> FLUSHED = ConcurrentHashMap.newKeySet();

  /**
   * Tracks SQL view names (from YAML views: section) per database path.
   * Key: dbPath, Value: set of "duckdbSchema.viewName" strings.
   * These are excluded from JDBC metadata (getTables) but remain queryable.
   */
  private static final ConcurrentHashMap<String, Set<String>> SQL_VIEW_NAMES =
      new ConcurrentHashMap<>();

  private DuckDBPendingViews() {}

  /** A single deferred view definition. */
  static final class PendingView {
    final String duckdbSchema;
    final String viewName;
    final String viewSql;
    SQLException lastError;  // most recent create/validate failure, for end-of-flush reporting

    PendingView(String duckdbSchema, String viewName, String viewSql) {
      this.duckdbSchema = duckdbSchema;
      this.viewName = viewName;
      this.viewSql = viewSql;
    }
  }

  /**
   * Enqueues a view for deferred creation against the given database file.
   * Called during schema initialization instead of creating the view immediately.
   */
  static void enqueue(String dbPath, String duckdbSchema, String viewName, String viewSql) {
    PENDING.computeIfAbsent(dbPath, k -> new CopyOnWriteArrayList<>())
        .add(new PendingView(duckdbSchema, viewName, viewSql));
  }

  /**
   * Records a SQL view name (from YAML views: section) for a database path.
   * These views are excluded from JDBC metadata (getTables) but remain queryable.
   */
  static void trackSqlView(String dbPath, String duckdbSchema, String viewName) {
    SQL_VIEW_NAMES.computeIfAbsent(dbPath, k -> ConcurrentHashMap.newKeySet())
        .add(duckdbSchema + "." + viewName);
  }

  /**
   * Returns true if the given name is a SQL view (from YAML views: section)
   * rather than a data table (iceberg_scan/parquet_scan wrapper).
   */
  static boolean isSqlView(String dbPath, String duckdbSchema, String viewName) {
    Set<String> names = SQL_VIEW_NAMES.get(dbPath);
    return names != null && names.contains(duckdbSchema + "." + viewName);
  }

  /**
   * Returns true if this database path has pending views not yet flushed.
   */
  static boolean hasPending(String dbPath) {
    return !FLUSHED.contains(dbPath) && PENDING.containsKey(dbPath);
  }

  /**
   * Flushes all pending views for the given database file using retry-until-convergence.
   *
   * <p>Each retry round creates as many views as possible. Views that fail due to a
   * missing dependency (table or view not yet created) are retried in the next round.
   * The loop terminates when all views succeed or when a full round makes no progress
   * (circular dependency or genuine SQL error).
   *
   * <p>Safe to call from multiple threads — idempotent after first flush.
   */
  static void flush(String dbPath, Connection conn) {
    if (FLUSHED.contains(dbPath)) {
      return;
    }
    synchronized (dbPath.intern()) {
      if (FLUSHED.contains(dbPath)) {
        return;
      }
      try {
        List<PendingView> pending =
            new ArrayList<>(PENDING.getOrDefault(dbPath, new CopyOnWriteArrayList<>()));

        LOGGER.info("Flushing {} deferred SQL views for database '{}'", pending.size(), dbPath);

        // Fixpoint: each pass creates as many views as possible; ANY view that fails (its
        // referenced table/view may simply not be created yet this pass) is retried in the next
        // pass. Loop until a full pass resolves nothing new — at which point the remainder is
        // genuinely unresolvable (a missing table/view, a missing column, or a circular reference),
        // which is a view-design problem, not something more retries can fix.
        while (!pending.isEmpty()) {
          List<PendingView> failed = new ArrayList<>();
          for (PendingView pv : pending) {
            SQLException err = tryCreateView(conn, pv);
            if (err == null) {
              LOGGER.info("✅ Created deferred view: {}.{}", pv.duckdbSchema, pv.viewName);
            } else {
              pv.lastError = err;
              failed.add(pv);
            }
          }
          if (failed.size() == pending.size()) {
            for (PendingView pv : failed) {
              LOGGER.error("Cannot create view {}.{} — {}. SQL: {}",
                  pv.duckdbSchema, pv.viewName, classifyError(pv.lastError),
                  pv.viewSql.length() > 200 ? pv.viewSql.substring(0, 200) + "..." : pv.viewSql);
            }
            break;
          }
          pending = failed;
        }

        LOGGER.info("Deferred view flush complete for database '{}'", dbPath);
      } finally {
        FLUSHED.add(dbPath);
        PENDING.remove(dbPath);
      }
    }
  }

  /**
   * Resets state for a database path — used when a database is recreated.
   * This allows views to be re-enqueued and re-flushed on the next access.
   */
  static void reset(String dbPath) {
    FLUSHED.remove(dbPath);
    PENDING.remove(dbPath);
    SQL_VIEW_NAMES.remove(dbPath);
  }

  /**
   * Creates and validates one deferred view. Returns null on success; otherwise returns the error
   * and leaves nothing half-created. DuckDB defers name resolution at CREATE time, so a view with
   * an unresolved reference is created but fails the validating SELECT — it is dropped here so it
   * never appears in JDBC metadata and so the next pass can retry it cleanly.
   */
  private static SQLException tryCreateView(Connection conn, PendingView pv) {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(String.format("CREATE VIEW IF NOT EXISTS \"%s\".\"%s\" AS %s",
          pv.duckdbSchema, pv.viewName, pv.viewSql));
    } catch (SQLException createEx) {
      return createEx;
    }
    try (Statement validateStmt = conn.createStatement()) {
      validateStmt.execute(String.format("SELECT * FROM \"%s\".\"%s\" LIMIT 0",
          pv.duckdbSchema, pv.viewName));
      return null;
    } catch (SQLException validateEx) {
      try (Statement dropStmt = conn.createStatement()) {
        dropStmt.execute(String.format("DROP VIEW IF EXISTS \"%s\".\"%s\"",
            pv.duckdbSchema, pv.viewName));
      } catch (SQLException ignored) {
        // best-effort drop
      }
      return validateEx;
    }
  }

  /** Human-readable cause for a view that never resolved, from the underlying DuckDB error. */
  private static String classifyError(SQLException e) {
    String msg = e == null ? null : e.getMessage();
    if (msg == null) {
      return "unresolvable dependency or circular reference";
    }
    String lower = msg.toLowerCase();
    if (lower.contains("binder error") || lower.contains("referenced column")
        || (lower.contains("column") && lower.contains("not found"))) {
      return "references a missing column: " + firstLine(msg);
    }
    if (lower.contains("does not exist")
        && (lower.contains("table") || lower.contains("view") || lower.contains("catalog"))) {
      return "references a missing table/view (or a circular view reference): " + firstLine(msg);
    }
    return firstLine(msg);
  }

  private static String firstLine(String msg) {
    if (msg == null) {
      return "";
    }
    int nl = msg.indexOf('\n');
    return nl >= 0 ? msg.substring(0, nl) : msg;
  }
}
