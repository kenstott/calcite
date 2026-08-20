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
 * <p>{@code CREATE VIEW} is NOT pure order-independent DDL in DuckDB: binding the view body to
 * determine its output row type happens at CREATE time, so a view whose SQL references another
 * deferred view that hasn't been created yet in this pass fails immediately with DuckDB's own
 * {@code Catalog Error}, rather than succeeding and resolving lazily on first query. (A prior
 * version of this class assumed lazy resolution and dropped the retry loop for a cold-start
 * perf win — confirmed wrong empirically: a real view-on-view chain, econ.trade_balance_summary
 * over econ.trade_statistics, failed permanently under the single-pass version whenever
 * trade_statistics happened to land later in the pending list.) The fix costs nothing extra on
 * the common no-forward-reference case — a pass with zero failures is one pass, same as before.
 *
 * <p>Each retry pass attempts every still-failing view; anything that fails due to a missing
 * dependency is retried in the next pass. The loop stops once a full pass makes no progress —
 * at that point the remaining failures are genuinely unresolvable (bad SQL, a missing column,
 * or a true circular reference), not an ordering artifact, and are logged as errors. Views are
 * still never proactively validated with a SELECT — creation itself is the only round trip, so
 * this stays cheap even with retries: DuckDB's CREATE VIEW binding is local catalog metadata,
 * not an object-store call, and only fails/retries on the minority of views with forward
 * references, which is why this can restore correctness without reintroducing the actual
 * expensive part the original perf pass removed (a validating SELECT per view).
 *
 * <p>Consequence: a view whose references genuinely cannot be resolved (after convergence) stays
 * out of the catalog and its error is logged, instead of leaving a broken view registered that
 * would fail cryptically when queried later.
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
   * Creates all pending views for the given database file, retrying until convergence.
   *
   * <p>Each pass attempts every view that hasn't succeeded yet. A view that fails because a
   * dependency (table or another deferred view) doesn't exist in the catalog yet is retried in
   * the next pass, once whatever it depends on has (hopefully) been created. The loop stops when
   * a full pass creates nothing new — the remainder is then a genuine SQL error, missing column,
   * or circular reference, not an ordering artifact, and gets logged as such.
   *
   * <p>Safe to call from multiple threads — idempotent after first flush.
   */
  /**
   * Every view already in the catalog, as {@code schema.name}. One local metadata query, versus
   * one object-store-binding CREATE per view — see the rationale in {@link #flush}.
   */
  private static Set<String> existingViews(Connection conn) {
    Set<String> out = ConcurrentHashMap.newKeySet();
    try (java.sql.Statement stmt = conn.createStatement();
         java.sql.ResultSet rs = stmt.executeQuery(
             "SELECT table_schema, table_name FROM information_schema.tables "
             + "WHERE table_type = 'VIEW'")) {
      while (rs.next()) {
        out.add(qualified(rs.getString(1), rs.getString(2)));
      }
    } catch (SQLException e) {
      // An empty set means "create everything", i.e. exactly the previous behaviour — correct,
      // just slower. Never skip a view on the strength of a failed lookup.
      LOGGER.warn("Could not list existing views; will attempt every deferred CREATE: {}",
          e.getMessage());
      return ConcurrentHashMap.newKeySet();
    }
    return out;
  }

  /** Case-insensitive {@code schema.name} key; DuckDB identifiers here are lower-cased. */
  private static String qualified(String schema, String name) {
    return (schema == null ? "" : schema.toLowerCase(java.util.Locale.ROOT))
        + "." + (name == null ? "" : name.toLowerCase(java.util.Locale.ROOT));
  }

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

        // Skip views the catalog already holds — normally every one of them, because the
        // JAR-bundled seed ships this same view DDL.
        //
        // This is not a micro-optimization. CREATE VIEW IF NOT EXISTS does NOT short-circuit
        // on existence in DuckDB: it binds the view body FIRST and only then honours IF NOT
        // EXISTS (verified — creating an existing view whose body names a missing table
        // raises "Table with name ... does not exist" rather than succeeding). These bodies
        // call iceberg_scan(), so every redundant CREATE is an object-store round trip, and
        // the whole batch runs on the first query that touches any view. Measured against a
        // seeded catalog that already had all 490 views, the flush blocked the first query
        // for minutes doing nothing but re-binding what was already there.
        //
        // That also invalidates this class's original premise that CREATE VIEW is "pure
        // local DDL that never contacts the object store" — it is not, so the only way to
        // keep the flush cheap is to not issue the statement at all.
        Set<String> existing = existingViews(conn);
        int skipped = 0;
        List<PendingView> toCreate = new ArrayList<>(pending.size());
        for (PendingView pv : pending) {
          if (existing.contains(qualified(pv.duckdbSchema, pv.viewName))) {
            skipped++;
          } else {
            toCreate.add(pv);
          }
        }
        pending = toCreate;

        LOGGER.info("Creating {} deferred SQL views for database '{}' ({} already in catalog)",
            pending.size(), dbPath, skipped);

        // Fixpoint: each pass creates as many views as possible; any view that fails because
        // its referenced table/view hasn't been created yet THIS pass is retried next pass.
        // Loop until a full pass resolves nothing new — the remainder is then a genuine error
        // (bad SQL, missing column, or circular reference), not an ordering artifact.
        while (!pending.isEmpty()) {
          List<PendingView> failed = new ArrayList<>();
          for (PendingView pv : pending) {
            SQLException err = createView(conn, pv);
            if (err == null) {
              LOGGER.debug("Created deferred view: {}.{}", pv.duckdbSchema, pv.viewName);
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
   * Creates one deferred view. Returns null on success, else the error. Local catalog metadata
   * only, no object-store round trip — but DuckDB binds the view body at CREATE time, so this
   * fails if a referenced table or view doesn't exist in the catalog yet; see the retry loop
   * in {@link #flush}.
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
