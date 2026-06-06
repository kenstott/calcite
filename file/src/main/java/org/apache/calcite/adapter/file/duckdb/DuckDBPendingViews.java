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

        while (!pending.isEmpty()) {
          List<PendingView> failed = new ArrayList<>();
          for (PendingView pv : pending) {
            try {
              String sql =
                  String.format("CREATE VIEW IF NOT EXISTS \"%s\".\"%s\" AS %s",
                  pv.duckdbSchema, pv.viewName, pv.viewSql);
              try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
              }
              // Validate by actually executing the view (SELECT … LIMIT 0).
              // DESCRIBE passes even when cross-schema refs are unavailable (DuckDB defers
              // resolution), but a real execution will fail.  Drop unexecutable views so
              // they never appear in JDBC metadata.
              try (Statement validateStmt = conn.createStatement()) {
                validateStmt.execute(
                    String.format(
                    "SELECT * FROM \"%s\".\"%s\" LIMIT 0", pv.duckdbSchema, pv.viewName));
              } catch (SQLException validateEx) {
                try (Statement dropStmt = conn.createStatement()) {
                  dropStmt.execute(
                      String.format(
                      "DROP VIEW IF EXISTS \"%s\".\"%s\"", pv.duckdbSchema, pv.viewName));
                } catch (SQLException ignored) {
                  // best-effort drop
                }
                LOGGER.info("Dropped inaccessible view {}.{} (cross-schema ref unavailable): {}",
                    pv.duckdbSchema, pv.viewName, firstLine(validateEx.getMessage()));
                continue;
              }
              LOGGER.info("✅ Created deferred view: {}.{}", pv.duckdbSchema, pv.viewName);
            } catch (SQLException e) {
              if (isDependencyError(e)) {
                LOGGER.debug("Deferring view {}.{} — dependency not yet available: {}",
                    pv.duckdbSchema, pv.viewName, firstLine(e.getMessage()));
                failed.add(pv);
              } else {
                LOGGER.error("Failed to create view {}.{}: {}",
                    pv.duckdbSchema, pv.viewName, e.getMessage());
                LOGGER.error("View SQL: {}",
                    pv.viewSql.length() > 300 ? pv.viewSql.substring(0, 300) + "..." : pv.viewSql);
              }
            }
          }
          if (failed.size() == pending.size()) {
            // No progress — circular dependency or unresolvable errors
            for (PendingView pv : failed) {
              LOGGER.error("Cannot create view {}.{} — unresolvable dependency or circular reference. SQL: {}",
                  pv.duckdbSchema, pv.viewName,
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

  private static boolean isDependencyError(SQLException e) {
    String msg = e.getMessage();
    if (msg == null) {
      return false;
    }
    return msg.contains("does not exist")
        || msg.contains("Table with name")
        || msg.contains("View with name")
        || msg.contains("Schema with name")
        || msg.contains("Catalog Error");
  }

  private static String firstLine(String msg) {
    if (msg == null) {
      return "";
    }
    int nl = msg.indexOf('\n');
    return nl >= 0 ? msg.substring(0, nl) : msg;
  }
}
