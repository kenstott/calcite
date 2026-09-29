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

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.iceberg.IcebergTable;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.schema.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * for SQL views layered on top of it. It also shares a subtler gap with this class: the
 * "expected" count it compares against comes from {@link IcebergTable#getRowType}, which itself
 * prefers a cached, previously-published {@code IcebergSchemaCache} entry over a live read — so
 * if a table's schema has genuinely grown since that entry was published, both sides of the
 * self-heal's comparison agree on the same stale count and no heal fires, even though DuckDB's
 * own runtime {@code iceberg_scan} (unrelated to that Java-side cache) sees the true, larger
 * live schema and throws exactly this error. {@link #refreshDriftedViews} repairs both gaps
 * reactively, on the actual failure, rather than trying to keep every cache continuously
 * coherent: it walks the failing statement's table/view references, recreates any base Iceberg
 * table's DuckDB view fresh (forcing DuckDB to re-resolve {@code iceberg_scan} against live
 * data) and drops that table's {@code IcebergSchemaCache}/{@link IcebergTable} cache too, then —
 * bottom-up — recreates any SQL view built on top of it from its own recorded definition, so an
 * outer view rebinds against the now-current dependency instead of the stale one.
 *
 * <h3>3. Transient network failure</h3>
 * A WAN object-store endpoint (e.g. R2, reached over the public internet, vs. a LAN-local mirror)
 * occasionally drops a connection mid-request. httpfs's own {@code http_retries}/{@code
 * http_retry_wait_ms}/{@code http_retry_backoff} settings (set in {@link DuckDBJdbcSchemaFactory})
 * absorb most of these, but a burst that outlasts httpfs's own retry budget still surfaces as one
 * of several messages, e.g.:
 * <pre>
 *   IO Error: Could not establish connection error for HTTP GET to 'https://...'
 *   IO Error: SSL connection failed error for HTTP GET to 'https://...'
 * </pre>
 * Unlike modes 1/2, this carries no cache-consistency implication — nothing needs evicting — so
 * recovery is a brief backoff ({@link #NETWORK_RETRY_BACKOFF_MS}) followed by a plain retry.
 *
 * <h3>4. Pointer-ahead-of-metadata race (mirror read-after-write lag)</h3>
 * {@code version-hint.text} names a version whose {@code v{N}.metadata.json} the reader's own
 * object-store endpoint does not (yet) serve, surfacing as:
 * <pre>
 *   Invalid Configuration Error: Iceberg metadata file not found for table version '{N}' using
 *   'none' compression and format(s): 'v%s%s.metadata.json,%s%s.metadata.json'
 * </pre>
 * Both this repo's writer ({@code S3FileIOTableOperations#commit}) and the R2 mirror sync
 * ({@code sync-to-r2.sh}'s pointer-last closure copy) already order writes so the pointer is
 * never advanced ahead of the metadata it names on the SAME endpoint that wrote it — but a
 * different endpoint's own read-after-write consistency window (observed live against R2,
 * 2026-09-29, reported by peer session entity-resolution: a query against {@code fec.candidates}
 * failed this way once, then succeeded on the next attempt with no retry logic involved) is a
 * separate, unaddressed gap — an object that a PUT already reports as durably written is not
 * guaranteed instantly GET-visible everywhere. Like mode 3, nothing is stale here (the pointer
 * and the metadata it names are BOTH already correct) — a plain retry after
 * {@link #NETWORK_RETRY_BACKOFF_MS} resolves it once the window closes, no cache_httpfs purge
 * needed.
 *
 * <p>None of the four failures mean the table is broken or the writer is misbehaving. On any
 * signature, clear the relevant {@code cache_httpfs} state (modes 1/2 only) and retry the
 * statement once against a FRESH {@link Statement}/{@link PreparedStatement} &mdash; DuckDB's JDBC
 * driver leaves the original unusable after any of these errors ("Statement was closed" on
 * re-invoke), so re-running the same execute call on {@code real} is not an option. A second
 * failure is a real error and propagates normally.
 */
final class EtagRetryConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(EtagRetryConnection.class);

  /** Matches DuckDB httpfs's ETag-drift message and captures the file path it was reading. */
  private static final Pattern ETAG_DRIFT = Pattern.compile(
      "ETag on reading file \"([^\"]+)\".*likely means the remote file has changed");

  /** Matches DuckDB's view-on-view type-binding drift message (see class javadoc, mode 2). No file path is embedded, so recovery purges the whole cache_httpfs cache rather than one path. */
  private static final Pattern VIEW_TYPE_DRIFT = Pattern.compile(
      "Contents of view were altered: types don't match");

  /**
   * Matches a transient network failure from DuckDB's httpfs client (WAN endpoints like R2 see
   * occasional TCP resets/connect failures under load — httpfs's own {@code http_retries} setting
   * covers most of these, but a burst that outlasts it still surfaces here as a hard SQLException).
   * No file path or cache state is implicated, so recovery is a plain retry with a short backoff
   * and no {@code cache_httpfs} purge.
   */
  private static final Pattern NETWORK_TRANSIENT = Pattern.compile(
      "Could not establish connection|Connection (reset|timed out)|Connection refused"
          + "|SSL connection failed|SSL_ERROR|Broken pipe");

  /**
   * Matches DuckDB's "pointer names a version whose metadata.json isn't visible yet" message
   * (see class javadoc, mode 4) — a mirror read-after-write consistency gap, not a stale cache
   * or a broken table. Recovery is the same as {@link #NETWORK_TRANSIENT}: backoff and retry,
   * no cache_httpfs purge (there is nothing stale to evict).
   */
  private static final Pattern METADATA_NOT_YET_VISIBLE = Pattern.compile(
      "Iceberg metadata file not found for table version");

  /** Sentinel {@code transientRaceTarget()} return value for {@link #NETWORK_TRANSIENT}: retry, but skip the cache_httpfs purge that ETAG_DRIFT/VIEW_TYPE_DRIFT need. */
  private static final String NETWORK_RETRY = " network";

  private static final int MAX_ATTEMPTS = 2;

  /** Backoff before a {@link #NETWORK_RETRY} retry; the other two race classes retry immediately since they resolve deterministically once the stale cache entry is evicted. */
  private static final long NETWORK_RETRY_BACKOFF_MS = 500;

  private EtagRetryConnection() {
  }

  /**
   * Matches a {@code "schema"."name"} or bare {@code name} reference after FROM/JOIN, used to
   * find which table(s)/view(s) a failing statement touched. Group 2 is present only for the
   * qualified form; a bare reference (group 2 null) means "resolve via the current schema" —
   * exactly how YAML {@code views:} SQL references sibling tables in the same schema (e.g.
   * {@code FROM acs_poverty p JOIN acs_age a ...}, relying on DuckDB's search_path rather than
   * spelling out the schema on every reference).
   */
  private static final Pattern TABLE_REF = Pattern.compile(
      "(?:FROM|JOIN)\\s+\"?([A-Za-z_][A-Za-z0-9_]*)\"?(?:\\s*\\.\\s*\"?([A-Za-z_][A-Za-z0-9_]*)\"?)?",
      Pattern.CASE_INSENSITIVE);

  /** Bound on view-dependency recursion depth in {@link #refreshOne} — real schemas nest at most 2-3 views deep; this is purely a runaway guard. */
  private static final int MAX_REPAIR_DEPTH = 5;

  /** Wraps {@code conn} so every {@link Statement}/{@link PreparedStatement} it creates retries once on a live-commit race. {@code fileSchema} (nullable) is used only to repair view-type drift (mode 2) — resolving a failing view back to its base Iceberg table(s) so both DuckDB's catalog and the Java-side schema cache can be refreshed before the retry. */
  static Connection wrap(Connection conn, FileSchema fileSchema) {
    return (Connection) Proxy.newProxyInstance(
        EtagRetryConnection.class.getClassLoader(),
        new Class<?>[] {Connection.class},
        new ConnectionHandler(conn, fileSchema));
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
      if (NETWORK_TRANSIENT.matcher(msg).find()) {
        return NETWORK_RETRY;
      }
      if (METADATA_NOT_YET_VISIBLE.matcher(msg).find()) {
        return NETWORK_RETRY;
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

  /**
   * On a view-type-drift failure, recreates every {@code "schema"."table"} reference found in
   * the failing SQL, bottom-up: a base Iceberg table's DuckDB view is recreated fresh (forcing
   * DuckDB to re-resolve {@code iceberg_scan} against live data) and its Java-side schema cache
   * dropped; a SQL view built on top of one is recreated from its own recorded definition only
   * after its dependencies have been healed, so it rebinds against the now-current dependency
   * rather than the stale one. No-op if {@code fileSchema} or {@code sql} is unavailable — the
   * cache_httpfs purge already issued by the caller is then the only recovery attempted.
   */
  private static void refreshDriftedViews(Connection conn, FileSchema fileSchema, String sql) {
    if (fileSchema == null || sql == null) {
      return;
    }
    // Top-level: the runner always fully qualifies (SELECT ... FROM "schema"."table"), so a bare
    // reference here has no schema to default to — null defaultSchema skips it rather than guess.
    walkTableRefs(conn, fileSchema, sql, null, new HashSet<>(), 0);
  }

  /** Finds every FROM/JOIN table/view reference in {@code sql} and heals each via {@link #refreshOne}. A bare (unqualified) reference resolves against {@code defaultSchema} — the schema the SQL itself lives in — or is skipped if that is null. */
  private static void walkTableRefs(Connection conn, FileSchema fileSchema, String sql,
      String defaultSchema, Set<String> visited, int depth) {
    Matcher m = TABLE_REF.matcher(sql);
    while (m.find()) {
      String first = m.group(1);
      String second = m.group(2);
      if (second != null) {
        refreshOne(conn, fileSchema, first, second, visited, depth);
      } else if (defaultSchema != null) {
        refreshOne(conn, fileSchema, defaultSchema, first, visited, depth);
      }
    }
  }

  /** Heals one {@code duckdbSchema.name} reference: recreates it as a base Iceberg view if it is one, else treats it as a SQL view and heals its own dependencies first. */
  private static void refreshOne(Connection conn, FileSchema fileSchema, String duckdbSchema,
      String name, Set<String> visited, int depth) {
    if (depth > MAX_REPAIR_DEPTH || !visited.add(duckdbSchema + "." + name)) {
      return;
    }
    ConversionMetadata meta = fileSchema.getConversionMetadata();
    ConversionMetadata.ConversionRecord record = meta == null ? null
        : meta.getAllConversions().get(name);
    if (record == null && meta != null) {
      record = meta.getAllConversions().get(name.toLowerCase());
    }
    if (record != null && "ICEBERG_PARQUET".equals(record.conversionType)
        && record.sourceFile != null && !record.sourceFile.endsWith(".parquet")) {
      refreshBaseIcebergView(conn, fileSchema, duckdbSchema, name, record.sourceFile);
      return;
    }
    refreshDependentView(conn, fileSchema, duckdbSchema, name, visited, depth);
  }

  /** Recreates a base table's own Iceberg wrapper view fresh, and drops the Java-side schema cache backing it so Calcite's row-type view of the table also reflects live reality. */
  private static void refreshBaseIcebergView(Connection conn, FileSchema fileSchema,
      String duckdbSchema, String name, String sourceFile) {
    Table fsTable = fileSchema.tables().get(name);
    if (fsTable == null) {
      fsTable = fileSchema.tables().get(name.toLowerCase());
    }
    if (fsTable instanceof IcebergTable) {
      ((IcebergTable) fsTable).invalidateCachedSchema();
    }
    String viewSql = String.format(
        "CREATE OR REPLACE VIEW \"%s\".\"%s\" AS SELECT * FROM iceberg_scan('%s', allow_moved_paths=true)",
        duckdbSchema, name, sourceFile);
    try (Statement st = conn.createStatement()) {
      st.execute(viewSql);
      LOGGER.info("View-drift repair: recreated base view \"{}\".\"{}\"", duckdbSchema, name);
    } catch (SQLException e) {
      LOGGER.debug("View-drift repair: could not recreate base view \"{}\".\"{}\": {}",
          duckdbSchema, name, e.getMessage());
    }
  }

  /** Heals {@code name}'s own table/view dependencies first, then recreates {@code name} itself from its recorded DuckDB definition — a no-op if {@code name} is not a known view either. */
  private static void refreshDependentView(Connection conn, FileSchema fileSchema,
      String duckdbSchema, String name, Set<String> visited, int depth) {
    String createSql = fetchViewCreateSql(conn, duckdbSchema, name);
    if (createSql == null) {
      return;
    }
    // Unqualified references inside name's own definition resolve within name's own schema.
    walkTableRefs(conn, fileSchema, createSql, duckdbSchema, visited, depth + 1);
    try (Statement st = conn.createStatement()) {
      st.execute(toCreateOrReplace(createSql));
      LOGGER.info("View-drift repair: recreated dependent view \"{}\".\"{}\"", duckdbSchema, name);
    } catch (SQLException e) {
      LOGGER.debug("View-drift repair: could not recreate view \"{}\".\"{}\": {}",
          duckdbSchema, name, e.getMessage());
    }
  }

  /** The recorded {@code CREATE VIEW ...} DDL for a view from DuckDB's own catalog, or null if {@code name} is not a view (e.g. it doesn't exist, or is a base table with no view wrapper by this name). */
  private static String fetchViewCreateSql(Connection conn, String duckdbSchema, String name) {
    String sql = "SELECT sql FROM duckdb_views() WHERE schema_name = ? AND view_name = ?";
    try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, duckdbSchema);
      ps.setString(2, name);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getString(1);
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("View-drift repair: could not fetch definition for \"{}\".\"{}\": {}",
          duckdbSchema, name, e.getMessage());
    }
    return null;
  }

  /** Rewrites a recorded {@code CREATE VIEW [IF NOT EXISTS]} statement to {@code CREATE OR REPLACE VIEW} so re-issuing it rebinds against the current catalog state instead of no-op'ing. */
  private static String toCreateOrReplace(String createViewSql) {
    return createViewSql
        .replaceFirst("(?i)^CREATE\\s+VIEW\\s+IF\\s+NOT\\s+EXISTS", "CREATE OR REPLACE VIEW")
        .replaceFirst("(?i)^CREATE\\s+VIEW\\s+", "CREATE OR REPLACE VIEW ");
  }

  private static final class ConnectionHandler implements InvocationHandler {
    private final Connection real;
    private final FileSchema fileSchema;

    ConnectionHandler(Connection real, FileSchema fileSchema) {
      this.real = real;
      this.fileSchema = fileSchema;
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
              new StatementHandler((Statement) result, real, method, args, fileSchema));
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
    private final FileSchema fileSchema;
    private Statement real;
    private final List<Object[]> paramCalls = new ArrayList<>();  // [Method, args] in call order, prepared only

    StatementHandler(Statement real, Connection realConn, Method creator, Object[] creatorArgs,
        FileSchema fileSchema) {
      this.real = real;
      this.realConn = realConn;
      this.creator = creator;
      this.creatorArgs = creatorArgs;
      this.isPrepared = real instanceof PreparedStatement;
      this.fileSchema = fileSchema;
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
          if (NETWORK_RETRY.equals(target)) {
            LOGGER.info("Transient network error from httpfs; retrying query in {}ms: {}",
                NETWORK_RETRY_BACKOFF_MS, last.getMessage());
            try {
              Thread.sleep(NETWORK_RETRY_BACKOFF_MS);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw last;
            }
          } else {
            purge(realConn, target);
            if (target.isEmpty()) {
              // View-type drift: a cache_httpfs purge alone does not fix a DuckDB catalog view
              // whose binding is genuinely stale (or a Java-side schema cache that agrees with
              // it) — recreate whatever the failing statement referenced, bottom-up.
              refreshDriftedViews(realConn, fileSchema, currentSql(args));
            }
          }
          if (!recreate()) {
            // Couldn't rebuild a working statement to retry against — surface the original error.
            throw last;
          }
        }
      }
      throw last;
    }

    /** The SQL text of the statement currently being (re)tried: {@code args[0]} when this call passed one explicitly (a plain {@code execute(sql)}/{@code executeQuery(sql)} call — legal even on a PreparedStatement-typed reference, since it resolves to the inherited {@link Statement} method), else the SQL originally given to {@code prepareStatement} for a true no-arg PreparedStatement execute. Null if neither is available. */
    private String currentSql(Object[] args) {
      if (args != null && args.length > 0 && args[0] instanceof String) {
        return (String) args[0];
      }
      if (creatorArgs != null && creatorArgs.length > 0 && creatorArgs[0] instanceof String) {
        return (String) creatorArgs[0];
      }
      return null;
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
