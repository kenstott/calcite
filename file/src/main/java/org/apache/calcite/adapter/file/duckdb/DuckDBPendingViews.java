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
 * Deferred DuckDB view registration.
 *
 * <p>Views defined in schema YAMLs may reference tables or other views that
 * do not yet exist when the schema is first initialized (e.g. cross-schema
 * references, or view-on-view chains). This class collects all pending view
 * definitions during schema initialization and flushes them lazily on the
 * first query, by which point all schemas and their base tables are registered.
 *
 * <p>Views are CREATEd but never proactively validated. DuckDB defers name resolution at
 * CREATE time, so {@code CREATE VIEW} is pure local DDL that never contacts the object store
 * and succeeds for any well-formed statement — including view-on-view chains, in any order.
 * Proving a view actually resolves requires querying it, and for an {@code iceberg_scan} view
 * that is an object-store round trip (~29ms against a local MinIO, more over a WAN).
 *
 * <p>Validating every deferred view was the single largest component of connect time: N round
 * trips to answer a question about tables the caller had not asked for. It is not done at all
 * now, in either the metadata path or the query path — relocating it merely moved the cost
 * from connect into whichever path ran first.
 *
 * <p>Consequence: a view whose references cannot be resolved stays in the catalog and fails
 * when queried, carrying DuckDB's own error, instead of being dropped at connect and silently
 * missing from metadata. A view that vanishes is harder to diagnose than one that explains
 * itself on use.
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
   * Creates all pending views for the given database file, in a single pass.
   *
   * <p>One pass suffices: DuckDB resolves names lazily, so {@code CREATE VIEW} succeeds even
   * when the view references a table or another view that does not exist yet. The previous
   * retry-until-convergence loop existed only because creation was fused with a validating
   * SELECT, which genuinely does depend on creation order; with validation gone there is
   * nothing left for a second pass to resolve.
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

        LOGGER.info("Creating {} deferred SQL views for database '{}'", pending.size(), dbPath);

        for (PendingView pv : pending) {
          SQLException err = createView(conn, pv);
          if (err == null) {
            LOGGER.debug("Created deferred view: {}.{}", pv.duckdbSchema, pv.viewName);
          } else {
            // A CREATE failure is a malformed statement, not an unresolved reference — retrying
            // cannot help.
            pv.lastError = err;
            LOGGER.error("Cannot create view {}.{} — {}. SQL: {}",
                pv.duckdbSchema, pv.viewName, classifyError(err),
                pv.viewSql.length() > 200 ? pv.viewSql.substring(0, 200) + "..." : pv.viewSql);
          }
        }

        LOGGER.info("Deferred view creation complete for database '{}'", dbPath);
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
   * Creates one deferred view. Returns null on success, else the error. Local DDL only — DuckDB
   * defers name resolution, so this succeeds even when the view's references do not exist yet.
   */
  private static SQLException createView(Connection conn, PendingView pv) {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(String.format("CREATE VIEW IF NOT EXISTS \"%s\".\"%s\" AS %s",
          pv.duckdbSchema, pv.viewName, pv.viewSql));
      return null;
    // fallback-guard: allow Javadoc states 'Returns null on success, else the error' — the exception is returned as a value to the caller, not swallowed at all.
    } catch (SQLException createEx) {
      return createEx;
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
