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
package org.apache.calcite.adapter.file.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Durable per-column cardinality (NDV) store, kept in the same PostgreSQL database as the
 * pipeline tracker.
 *
 * <p>Why this exists: {@link HLLSketchCache} is process-local, so estimates computed in one JVM
 * are invisible to the next one. That made the planner's cardinality rules dead weight for a
 * long-lived query server, which starts with an empty cache and never fills it. Persisting NDVs
 * next to the tracker means the ETL run that writes a table also publishes its statistics, and
 * any later reader loads them at schema-open for the price of one query.
 *
 * <p>Why NDV rather than serialized sketches: {@link HyperLogLogSketch#fromEstimate(long)} wraps
 * a cardinality computed elsewhere, and the estimates here come from DuckDB's
 * {@code approx_count_distinct} — computed over the freshly-written table in a single pass. A
 * scalar per column is all the planner consumes. Storing raw buckets would only matter for
 * merging sketches across tables, which nothing here does.
 *
 * <p>Statistics are advisory. Every failure path logs and returns rather than propagating: a
 * missing or stale estimate makes the planner fall back to its default heuristics, which is
 * exactly the behaviour that existed before this class. It must never fail an ETL run.
 */
public final class PGColumnStatisticsStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(PGColumnStatisticsStore.class);

  private final String jdbcUrl;
  private final String user;
  private final String password;
  private final String namespace;

  public PGColumnStatisticsStore(String jdbcUrl, String user, String password, String namespace) {
    this.jdbcUrl = jdbcUrl;
    this.user = user;
    this.password = password;
    this.namespace = namespace;
  }

  /**
   * Builds a store from the same configuration the pipeline tracker resolves, so statistics land
   * in the same database and the same bucket-derived namespace without a second set of settings
   * to keep in sync. Returns null when no PG tracker is configured — statistics are optional and
   * a deployment without Postgres simply does not collect them.
   *
   * @param trackerConfig the {@code trackerConfig} operand map, may be null
   */
  public static PGColumnStatisticsStore fromTrackerConfig(Map<String, String> trackerConfig) {
    Map<String, String> cfg = trackerConfig != null
        ? trackerConfig : java.util.Collections.<String, String>emptyMap();
    String jdbcUrl = cfg.get("jdbcUrl");
    if (jdbcUrl == null) {
      jdbcUrl = System.getenv("CALCITE_TRACKER_PG_URL");
    }
    if (jdbcUrl == null) {
      return null;
    }
    String user = cfg.get("user");
    if (user == null) {
      user = System.getenv("CALCITE_TRACKER_PG_USER");
    }
    String password = cfg.get("password");
    if (password == null) {
      password = System.getenv("CALCITE_TRACKER_PG_PASSWORD");
    }
    return new PGColumnStatisticsStore(jdbcUrl, user, password, cfg.get("namespace"));
  }

  /**
   * Builds a store from a schema operand, reading the same {@code trackerBackend} /
   * {@code trackerConfig} keys {@code PipelineTrackerFactory.createFromOperand} reads. Returns
   * null unless a PG tracker is configured, so a deployment on another backend silently collects
   * no statistics rather than half-configuring a second one.
   */
  @SuppressWarnings("unchecked")
  public static PGColumnStatisticsStore fromOperand(Map<String, Object> operand) {
    if (operand == null) {
      return null;
    }
    Object backend = operand.get("trackerBackend");
    if (!(backend instanceof String) || !"pg".equalsIgnoreCase((String) backend)) {
      return null;
    }
    Object trackerConfig = operand.get("trackerConfig");
    Map<String, String> cfg = trackerConfig instanceof Map
        ? (Map<String, String>) trackerConfig : null;
    return fromTrackerConfig(cfg);
  }

  private Connection open() throws SQLException {
    Connection c = user != null
        ? DriverManager.getConnection(jdbcUrl, user, password)
        : DriverManager.getConnection(jdbcUrl);
    c.setAutoCommit(true);
    if (namespace != null && !namespace.isEmpty()) {
      try (Statement st = c.createStatement()) {
        // Same namespacing rule the tracker uses, so dq and prod statistics never mix.
        st.execute("CREATE SCHEMA IF NOT EXISTS \"" + namespace + "\"");
        st.execute("SET search_path TO \"" + namespace + "\"");
      }
    }
    try (Statement st = c.createStatement()) {
      st.execute(
          "CREATE TABLE IF NOT EXISTS column_statistics ("
          + "  schema_name VARCHAR NOT NULL,"
          + "  table_name VARCHAR NOT NULL,"
          + "  column_name VARCHAR NOT NULL,"
          + "  ndv BIGINT NOT NULL,"
          + "  row_count BIGINT NOT NULL,"
          + "  computed_at BIGINT NOT NULL,"
          + "  PRIMARY KEY (schema_name, table_name, column_name)"
          + ")");
    }
    return c;
  }

  /**
   * Replaces the stored statistics for one table. Delete-then-insert rather than upsert so a
   * column dropped from the schema does not leave a stale estimate behind for a name the table no
   * longer has.
   */
  public void put(String schema, String table, long rowCount, Map<String, Long> ndvByColumn) {
    if (ndvByColumn == null || ndvByColumn.isEmpty()) {
      return;
    }
    String s = schema.toLowerCase(Locale.ROOT);
    String t = table.toLowerCase(Locale.ROOT);
    try (Connection c = open()) {
      try (PreparedStatement del = c.prepareStatement(
          "DELETE FROM column_statistics WHERE schema_name = ? AND table_name = ?")) {
        del.setString(1, s);
        del.setString(2, t);
        del.executeUpdate();
      }
      try (PreparedStatement ins = c.prepareStatement(
          "INSERT INTO column_statistics"
          + " (schema_name, table_name, column_name, ndv, row_count, computed_at)"
          + " VALUES (?, ?, ?, ?, ?, ?)")) {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> e : ndvByColumn.entrySet()) {
          ins.setString(1, s);
          ins.setString(2, t);
          ins.setString(3, e.getKey().toLowerCase(Locale.ROOT));
          ins.setLong(4, e.getValue() == null ? -1L : e.getValue());
          ins.setLong(5, rowCount);
          ins.setLong(6, now);
          ins.addBatch();
        }
        ins.executeBatch();
      }
      LOGGER.info("Published {} column statistics for {}.{} (rowCount={})",
          ndvByColumn.size(), s, t, rowCount);
    } catch (Exception e) {
      // Advisory data — never fail the run that produced the table.
      LOGGER.warn("Could not publish column statistics for {}.{}: {}", s, t, e.getMessage());
    }
  }

  /**
   * Loads every stored estimate for one schema into the process-local {@link HLLSketchCache} the
   * planner rules consult. One query per schema, run at schema-open.
   *
   * @return number of columns loaded
   */
  public int loadInto(String schema, HLLSketchCache cache) {
    String s = schema.toLowerCase(Locale.ROOT);
    int n = 0;
    try (Connection c = open();
         PreparedStatement ps = c.prepareStatement(
             "SELECT table_name, column_name, ndv FROM column_statistics WHERE schema_name = ?")) {
      ps.setString(1, s);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          long ndv = rs.getLong(3);
          if (ndv < 0) {
            continue;
          }
          cache.putSketch(s, rs.getString(1), rs.getString(2),
              HyperLogLogSketch.fromEstimate(ndv));
          n++;
        }
      }
      LOGGER.info("Loaded {} column statistics for schema '{}' into the HLL cache", n, s);
    } catch (Exception e) {
      LOGGER.warn("Could not load column statistics for schema '{}': {}", s, e.getMessage());
    }
    return n;
  }

  /**
   * Computes NDV for every column of an already-materialized table in a single pass, using
   * DuckDB's {@code approx_count_distinct}.
   *
   * <p>One pass for all columns is what makes publishing statistics affordable at write time:
   * measured over a 22.8M-row Iceberg table, ten columns resolve in ~0.6s, because DuckDB
   * maintains all the sketches concurrently while scanning once. Per-column queries would
   * multiply that by the column count for no additional information.
   *
   * @param conn      a DuckDB connection that can already see {@code fromClause}
   * @param fromClause the scan expression for the table (e.g. an {@code iceberg_scan(...)} call)
   * @param columns   columns to profile
   * @return NDV by column name, empty if the profile could not be taken
   */
  public static Map<String, Long> computeNdv(Connection conn, String fromClause,
      List<String> columns) {
    Map<String, Long> out = new LinkedHashMap<String, Long>();
    if (columns == null || columns.isEmpty()) {
      return out;
    }
    StringBuilder sql = new StringBuilder("SELECT ");
    List<String> ordered = new ArrayList<String>(columns);
    for (int i = 0; i < ordered.size(); i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append("approx_count_distinct(\"").append(ordered.get(i)).append("\")");
    }
    sql.append(" FROM ").append(fromClause);
    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(sql.toString())) {
      if (rs.next()) {
        for (int i = 0; i < ordered.size(); i++) {
          out.put(ordered.get(i), rs.getLong(i + 1));
        }
      }
    } catch (Exception e) {
      // A single unhashable column type fails the whole SELECT. Rather than fall back to N
      // per-column queries (which reintroduces the cost this design avoids), skip the table —
      // the planner simply keeps its defaults for it.
      LOGGER.warn("Could not profile columns for statistics: {}", e.getMessage());
      out.clear();
    }
    return out;
  }
}
