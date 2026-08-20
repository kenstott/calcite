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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Deferred DuckDB view registration, resolved on demand, one view at a time.
 *
 * <p>Views defined in schema YAMLs may reference tables or other views that do not yet exist
 * when the schema is first initialized (e.g. cross-schema references, or view-on-view chains).
 * This class collects all pending view definitions during schema initialization and creates each
 * one lazily, the first time something actually asks for it by name — not the whole backlog on
 * the first query to touch any view.
 *
 * <p>{@code CREATE VIEW} is NOT pure local DDL in DuckDB: binding the view body to determine its
 * output row type happens at CREATE time and, for a body that calls {@code iceberg_scan()}, that
 * bind is an object-store round trip. Two consequences follow directly from that fact:
 *
 * <ul>
 *   <li>A view whose SQL references another deferred view that hasn't been created yet fails
 *       immediately with DuckDB's own {@code Catalog Error} rather than resolving lazily later.
 *       {@link #createOnDemand} recovers from this by parsing the missing identifier out of the
 *       error, resolving that dependency first (recursively, with cycle detection for a genuine
 *       circular reference), then retrying — so a forward reference costs one extra round trip
 *       instead of failing permanently.
 *   <li>Because binding can mean a real network call, it can also genuinely hang if the
 *       underlying object is unreachable or the store is unresponsive — with no timeout, that
 *       blocks whichever caller happened to ask for it first, indefinitely. Every create attempt
 *       here therefore runs under {@link Statement#setQueryTimeout}, which DuckDB's JDBC driver
 *       honors by interrupting the native call from a background canceller — so a broken
 *       reference fails loudly in bounded time instead of hanging the caller forever.
 * </ul>
 *
 * <p>The common case — a view already present in the catalog, which is how every production
 * connection starts (the JAR-bundled seed ships this same view DDL already created) — costs one
 * cheap local existence check and nothing else: {@code CREATE VIEW} is never issued for a view
 * that's already there, so a fully up-to-date seed makes this class a no-op in practice. A gap in
 * the seed (a view added since it was last built, or one that failed to build) is what actually
 * exercises the create-and-maybe-retry path above.
 */
public final class DuckDBPendingViews {

  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBPendingViews.class);

  /** Bounded wait for a single deferred view's CREATE to resolve before giving up on it. */
  private static final int CREATE_VIEW_TIMEOUT_SECONDS =
      Integer.getInteger("calcite.duckdb.deferredView.timeoutSeconds", 15);

  /** DuckDB's own wording for "the view body names something that isn't in the catalog yet". */
  private static final Pattern MISSING_REFERENCE =
      Pattern.compile("(?:Table|View) with name \"?([\\w.]+)\"?\\s+does not exist",
          Pattern.CASE_INSENSITIVE);

  /** Pending views keyed by canonical DuckDB database file path. */
  private static final ConcurrentHashMap<String, CopyOnWriteArrayList<PendingView>> PENDING =
      new ConcurrentHashMap<>();

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
    SQLException lastError;  // most recent create failure, for end-of-attempt reporting

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
   * Names still pending for one DuckDB schema — no catalog access, no creation, just what's
   * queued in memory. Lets {@code getTableNames()} report a deferred view's name without paying
   * for (or risking hanging on) its creation.
   */
  static Set<String> pendingViewNames(String dbPath, String duckdbSchema) {
    CopyOnWriteArrayList<PendingView> pendingList = PENDING.get(dbPath);
    if (pendingList == null) {
      return java.util.Collections.emptySet();
    }
    Set<String> names = new java.util.LinkedHashSet<>();
    for (PendingView pv : pendingList) {
      if (pv.duckdbSchema.equalsIgnoreCase(duckdbSchema)) {
        names.add(pv.viewName);
      }
    }
    return names;
  }

  /** Case-insensitive {@code schema.name} key; DuckDB identifiers here are lower-cased. */
  private static String qualified(String schema, String name) {
    return (schema == null ? "" : schema.toLowerCase(java.util.Locale.ROOT))
        + "." + (name == null ? "" : name.toLowerCase(java.util.Locale.ROOT));
  }

  /**
   * Ensures exactly one deferred view exists, creating it (and, if needed, its dependency chain)
   * on demand. No-op if the name was never deferred, or was already resolved — including the
   * common case where the seed already shipped it, which this confirms with a single cheap
   * existence check rather than an object-store-binding {@code CREATE}.
   */
  static void createOnDemand(String dbPath, Connection conn, String duckdbSchema,
      String viewName) {
    createOnDemand(dbPath, conn, duckdbSchema, viewName, new java.util.HashSet<>());
  }

  private static void createOnDemand(String dbPath, Connection conn, String duckdbSchema,
      String viewName, Set<String> inFlight) {
    CopyOnWriteArrayList<PendingView> pendingList = PENDING.get(dbPath);
    if (pendingList == null) {
      return;
    }
    String key = qualified(duckdbSchema, viewName);
    if (!inFlight.add(key)) {
      // Already being resolved higher up this same call chain: a genuine circular reference,
      // not an ordering artifact. Let the original caller's CREATE fail on it naturally.
      LOGGER.error("Cannot create view {} — circular view reference", key);
      return;
    }
    try {
      synchronized ((dbPath + '|' + key).intern()) {
        PendingView pv = findExact(pendingList, duckdbSchema, viewName);
        if (pv == null) {
          return; // not pending: never deferred, or already resolved (by us or another caller)
        }
        if (existsInCatalog(conn, duckdbSchema, viewName)) {
          pendingList.remove(pv);
          return;
        }
        SQLException err = createViewWithTimeout(conn, pv);
        if (err != null) {
          String missing = extractMissingReference(err);
          PendingView dependency =
              missing == null ? null : findByReference(pendingList, duckdbSchema, missing);
          if (dependency != null) {
            createOnDemand(dbPath, conn, dependency.duckdbSchema, dependency.viewName, inFlight);
            err = createViewWithTimeout(conn, pv);
          }
        }
        if (err == null) {
          LOGGER.debug("Created deferred view: {}", key);
        } else {
          pv.lastError = err;
          LOGGER.error("Cannot create view {} — {}. SQL: {}", key, classifyError(err),
              pv.viewSql.length() > 200 ? pv.viewSql.substring(0, 200) + "..." : pv.viewSql);
        }
        pendingList.remove(pv);
      }
    } finally {
      inFlight.remove(key);
    }
  }

  /**
   * Retries every still-pending view for one database file against current data — the explicit,
   * operator-triggered "(re)build the catalog now" operation, as opposed to the lazy per-view
   * path {@link #createOnDemand} normally takes. A view that previously failed because of a data
   * problem that's since been fixed gets one more attempt; anything already resolved is a
   * no-op. Still one view at a time, still bounded by {@link #CREATE_VIEW_TIMEOUT_SECONDS} each
   * — one bad view can't stall the rest of the rebuild.
   */
  public static void buildAll(String dbPath, Connection conn) {
    CopyOnWriteArrayList<PendingView> pendingList = PENDING.get(dbPath);
    if (pendingList == null) {
      return;
    }
    for (PendingView pv : new java.util.ArrayList<>(pendingList)) {
      createOnDemand(dbPath, conn, pv.duckdbSchema, pv.viewName);
    }
  }

  private static PendingView findExact(List<PendingView> pendingList, String schema,
      String name) {
    for (PendingView pv : pendingList) {
      if (pv.duckdbSchema.equalsIgnoreCase(schema) && pv.viewName.equalsIgnoreCase(name)) {
        return pv;
      }
    }
    return null;
  }

  /**
   * Resolves a missing-reference identifier pulled from a DuckDB error (schema-qualified or
   * bare) against this database's pending views. A bare name prefers a match in the requesting
   * view's own schema — the one real forward-reference chain on record (econ.trade_balance_summary
   * over econ.trade_statistics) is same-schema — falling back to any schema.
   */
  private static PendingView findByReference(List<PendingView> pendingList,
      String requesterSchema, String reference) {
    int dot = reference.indexOf('.');
    if (dot >= 0) {
      String wantSchema = reference.substring(0, dot);
      String wantName = reference.substring(dot + 1);
      return findExact(pendingList, wantSchema, wantName);
    }
    PendingView anySchema = null;
    for (PendingView pv : pendingList) {
      if (pv.viewName.equalsIgnoreCase(reference)) {
        if (pv.duckdbSchema.equalsIgnoreCase(requesterSchema)) {
          return pv;
        }
        if (anySchema == null) {
          anySchema = pv;
        }
      }
    }
    return anySchema;
  }

  private static String extractMissingReference(SQLException e) {
    if (e == null || e.getMessage() == null) {
      return null;
    }
    Matcher m = MISSING_REFERENCE.matcher(e.getMessage());
    return m.find() ? m.group(1) : null;
  }

  /** One local metadata lookup — versus one object-store-binding CREATE — see the class javadoc. */
  private static boolean existsInCatalog(Connection conn, String schema, String name) {
    try (java.sql.PreparedStatement ps = conn.prepareStatement(
        "SELECT 1 FROM information_schema.tables "
        + "WHERE table_schema = ? AND table_name = ? AND table_type = 'VIEW'")) {
      ps.setString(1, schema);
      ps.setString(2, name);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    } catch (SQLException e) {
      // Unknown means "try to create it" — exactly the previous behaviour, just for one view
      // instead of the whole backlog.
      LOGGER.warn("Could not check existing view {}.{}; will attempt CREATE: {}",
          schema, name, e.getMessage());
      return false;
    }
  }

  /**
   * Creates one deferred view, bounded by {@link #CREATE_VIEW_TIMEOUT_SECONDS}. DuckDB's JDBC
   * driver honors {@link Statement#setQueryTimeout} by interrupting the native call from a
   * background canceller, so a bind that would otherwise hang on an unreachable/unresponsive
   * object store fails loudly instead — see the class javadoc.
   */
  private static SQLException createViewWithTimeout(Connection conn, PendingView pv) {
    try (Statement stmt = conn.createStatement()) {
      stmt.setQueryTimeout(CREATE_VIEW_TIMEOUT_SECONDS);
      stmt.execute(String.format("CREATE VIEW IF NOT EXISTS \"%s\".\"%s\" AS %s",
          pv.duckdbSchema, pv.viewName, pv.viewSql));
      return null;
    // fallback-guard: the exception is returned as a value to the caller, not swallowed.
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
    if (lower.contains("interrupt") || lower.contains("timeout") || lower.contains("timed out")) {
      return "timed out after " + CREATE_VIEW_TIMEOUT_SECONDS
          + "s — underlying data source unreachable or unresponsive: " + firstLine(msg);
    }
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
