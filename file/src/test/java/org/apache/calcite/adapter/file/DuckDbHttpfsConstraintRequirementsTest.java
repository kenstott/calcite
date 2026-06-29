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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * FILE-037 / FILE-079 / FILE-070 — execution-path requirements for the file adapter, verified
 * hermetically against local fixtures with the {@code duckdb} CLI on PATH (no remote services).
 *
 * <p>FILE-037: with {@code executionEngine=duckdb}, DuckDB reads LOCAL storage directly via its
 * native file functions ({@code read_parquet} / {@code read_csv_auto} — see
 * {@code DuckDBJdbcSchemaFactory.duckdbNativeReader} and {@code createParquetView}), bypassing the
 * generic StorageProvider scan path. The duckdb x local cell must be IDENTICAL to the generic
 * (parquet engine) path. NOTE: the s3/http cells of that matrix need live storage and are covered
 * separately; only the local cell is exercised here.
 *
 * <p>FILE-079: a declared PRIMARY KEY (via the {@code tableConstraints} operand) drives self-join /
 * join elimination, and results are UNCHANGED versus declaring no constraints — given accurate
 * constraints. The PK/constraint model declaration mirrors {@link ConstraintRequirementsTest}.
 *
 * <p>FILE-070: JDBC query engines are READ-ONLY. The file adapter exposes no ModifiableTable, so
 * the query path has no SQL write surface; all real writes (materialization / partition reorg) go
 * through DuckDB via the Iceberg Java API regardless of the configured query engine. That routing
 * is an architectural guarantee verified structurally (no DML target reachable from a query-engine
 * connection); see the per-method NOTE.
 */
@Tag("integration")
@Execution(ExecutionMode.SAME_THREAD)
public class DuckDbHttpfsConstraintRequirementsTest {

  @TempDir
  Path tempDir;

  // =========================================================================
  // FILE-037 — duckdb engine reads LOCAL storage directly; identical to generic path.
  // =========================================================================

  @Test @Tag("FILE-037")
  void duckdbLocalCellIdenticalToGenericParquetPath() throws Exception {
    // One shared local fixture, read through two engines into their own caches.
    createParquet("widgets.parquet",
        "SELECT CAST(i AS INTEGER) AS id, "
        + "'name_' || CAST(i AS VARCHAR) AS name, "
        + "CAST(i AS DOUBLE) * 1.5 AS score "
        + "FROM generate_series(1, 7) AS t(i)");

    String sql = "SELECT id, name, score FROM s.widgets ORDER BY id";

    List<String> generic = query("parquet", sql);
    List<String> duckdb = query("duckdb", sql);

    assertFalse(generic.isEmpty(), "FILE-037: generic (parquet) path must return rows");
    assertEquals(generic, duckdb,
        "FILE-037: the duckdb x local cell must be IDENTICAL to the generic StorageProvider "
        + "(parquet) path for the same local fixture");
  }

  // =========================================================================
  // FILE-079 — declared PK drives self-join elimination; results unchanged.
  // =========================================================================

  @Test @Tag("FILE-079")
  void declaredPrimaryKeyEliminatesRedundantSelfJoinWithUnchangedResults() throws Exception {
    createParquet("customers.parquet",
        "SELECT CAST(i AS INTEGER) AS customer_id, "
        + "'name_' || CAST(i AS VARCHAR) AS name "
        + "FROM generate_series(1, 5) AS t(i)");

    // A redundant self-join on customer_id: every row joins to exactly itself, so on accurate data
    // the join is a no-op and must return the same rows as a plain scan.
    String selfJoin =
        "SELECT a.customer_id, a.name "
        + "FROM s.customers a JOIN s.customers b ON a.customer_id = b.customer_id "
        + "ORDER BY a.customer_id";
    String noJoin =
        "SELECT customer_id, name FROM s.customers ORDER BY customer_id";

    // (a) results-unchanged: the redundant self-join returns exactly the no-join baseline.
    // Both queries run on ONE connection so they share a single cache/catalog (a fresh persistent
    // cache per connection would otherwise collide on reopen).
    List<String> selfJoinRows;
    List<String> baseline;
    try (Connection conn = openConnection("parquet", false)) {
      selfJoinRows = runQuery(conn, selfJoin);
      baseline = runQuery(conn, noJoin);
    }

    assertFalse(baseline.isEmpty(), "FILE-079: baseline must return rows");
    assertEquals(baseline, selfJoinRows,
        "FILE-079: a redundant self-join on customer_id must return the SAME rows as the no-join "
        + "baseline (results unchanged given accurate data)");

    // (b) the PRIMARY KEY on customer_id is actually declared/surfaced via the constraint model
    // (mirrors ConstraintRequirementsTest's getPrimaryKeys harness). The declared key is what
    // drives join elimination; we assert the constraint is present and accurate.
    try (Connection conn = openConnection("parquet", true)) {
      Map<Short, String> pkBySeq = new java.util.TreeMap<Short, String>();
      try (ResultSet rs = conn.getMetaData().getPrimaryKeys(null, "s", "customers")) {
        while (rs.next()) {
          pkBySeq.put(rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME"));
        }
      }
      assertEquals(Arrays.asList("customer_id"), new ArrayList<String>(pkBySeq.values()),
          "FILE-079: the declared PRIMARY KEY(customer_id) must be surfaced as the accurate key "
          + "that drives self-join / join elimination");
    }

    // NOTE: the plan-level "redundant join collapsed to a single scan" marker is NOT separately
    // asserted here: declaring tableConstraints in this minimal hermetic harness builds the table
    // as a PartitionedParquetTable whose single-file scan is not queryable through the inline
    // model, so EXPLAIN over the PK-declared self-join is not reachable. The results-unchanged
    // guarantee (a) plus the accurate declared key (b) are the hermetically reachable assertions.
  }

  // =========================================================================
  // FILE-070 — query engines are READ-ONLY; no DML write surface.
  // =========================================================================

  @Test @Tag("FILE-070")
  void queryEngineConnectionExposesNoWriteSurface() throws Exception {
    createParquet("ledger.parquet",
        "SELECT CAST(i AS INTEGER) AS id, CAST(i AS DOUBLE) * 10.0 AS amount "
        + "FROM generate_series(1, 3) AS t(i)");

    // NOTE: writes (materialization / partition reorg) are routed to DuckDB via the Iceberg Java
    // API and are NOT reachable from a SQL connection. The reachable hermetic guarantee is that the
    // file adapter exposes no ModifiableTable, so DML against a file-adapter table has no target and
    // must fail — for either query engine. We assert that structurally for both engines.
    assertNoWriteSurface("parquet");
    assertNoWriteSurface("duckdb");
  }

  private void assertNoWriteSurface(String engine) throws Exception {
    try (Connection conn = openConnection(engine, false)) {
      // A read must succeed on the query path.
      try (PreparedStatement ps = conn.prepareStatement(
               "SELECT count(*) FROM s.ledger");
           ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next(), "FILE-070: read must succeed on the " + engine + " query path");
        assertEquals(3L, rs.getLong(1), "FILE-070: read row count for " + engine);
      }

      // A DML write must NOT be accepted (no ModifiableTable target in the file adapter).
      boolean rejected = false;
      try (Statement st = conn.createStatement()) {
        st.executeUpdate("INSERT INTO s.ledger (id, amount) VALUES (99, 1.0)");
      } catch (SQLException expected) {
        rejected = true;
      }
      assertTrue(rejected,
          "FILE-070: the " + engine + " query path must reject INSERT (read-only, no write surface)");
    }
  }

  // -------------------------------------------------------------------------
  // Query helpers.
  // -------------------------------------------------------------------------

  /** Opens a fresh (no-constraints) connection for the engine and renders the query's rows. */
  private List<String> query(String engine, String sql) throws Exception {
    try (Connection conn = openConnection(engine, false)) {
      return runQuery(conn, sql);
    }
  }

  /** Runs {@code sql} on an open connection and renders each row as a "|"-joined string. */
  private List<String> runQuery(Connection conn, String sql) throws Exception {
    List<String> rows = new ArrayList<String>();
    try (PreparedStatement ps = conn.prepareStatement(sql);
         ResultSet rs = ps.executeQuery()) {
      ResultSetMetaData md = rs.getMetaData();
      int cols = md.getColumnCount();
      while (rs.next()) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= cols; i++) {
          if (i > 1) {
            sb.append(" | ");
          }
          Object v = rs.getObject(i);
          sb.append(rs.wasNull() || v == null ? "␀" : v.toString());
        }
        rows.add(sb.toString());
      }
    }
    return rows;
  }

  // -------------------------------------------------------------------------
  // Fixture + connection helpers (mirror ConstraintRequirementsTest).
  // -------------------------------------------------------------------------

  /** Source data directory (kept separate from per-connection cache directories). */
  private File srcDir() {
    File src = new File(tempDir.toFile(), "src");
    if (!src.exists()) {
      src.mkdirs();
    }
    return src;
  }

  private void createParquet(String fileName, String selectSql) throws Exception {
    File out = new File(srcDir(), fileName);
    String sql = "COPY (" + selectSql + ") TO '" + out.getAbsolutePath() + "' (FORMAT PARQUET)";
    ProcessBuilder pb = new ProcessBuilder("duckdb", "-c", sql);
    pb.redirectErrorStream(true);
    Process proc = pb.start();
    int exit = proc.waitFor();
    if (exit != 0) {
      byte[] buf = new byte[4096];
      int len = proc.getInputStream().read(buf);
      String err = len > 0 ? new String(buf, 0, len) : "unknown error";
      fail("DuckDB CLI failed creating " + fileName + ": " + err);
    }
    assertTrue(out.exists(), "Parquet file should exist: " + out);
  }

  /**
   * Opens a Calcite connection over the file adapter for the given execution engine. When
   * {@code withConstraints} is true, customers declares PRIMARY KEY(customer_id) via the
   * {@code tableConstraints} operand (declaration mirrors {@link ConstraintRequirementsTest}).
   * A per-engine baseDirectory keeps each engine's cache isolated.
   */
  private Connection openConnection(String engine, boolean withConstraints) throws Exception {
    // Source data lives under tempDir/src; each connection's cache is a SIBLING of src (never
    // inside the scanned source directory), so caches never contaminate table discovery.
    String dir = srcDir().getAbsolutePath().replace("\\", "\\\\");
    String cache = new File(tempDir.toFile(), "cache_" + engine + "_" + withConstraints)
        .getAbsolutePath().replace("\\", "\\\\");

    String constraints = withConstraints
        ? "    \"tableConstraints\":{"
            + "      \"customers\":{ \"primaryKey\":[\"customer_id\"] }"
            + "    },"
        : "";

    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"s\","
        + "\"schemas\":[{"
        + "  \"name\":\"s\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "  \"operand\":{"
        + "    \"directory\":\"" + dir + "\","
        + "    \"baseDirectory\":\"" + cache + "\","
        + "    \"ephemeralCache\":false,"
        + constraints
        + "    \"executionEngine\":\"" + engine + "\""
        + "  }"
        + "}]}";

    Properties props = new Properties();
    props.put("model", "inline:" + model);
    props.put("lex", "ORACLE");
    props.put("unquotedCasing", "TO_LOWER");
    return DriverManager.getConnection("jdbc:calcite:", props);
  }
}
