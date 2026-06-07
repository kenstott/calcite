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

import org.apache.calcite.adapter.file.BaseFileTest;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies that the DuckDB execution engine uses Calcite/FileSchema as the sole
 * metadata authority, not DuckDB's own JDBC catalog metadata.
 *
 * <p>Regression coverage for Bug 2 in GitHub issue #19:
 * {@code CommentableJdbcTableWrapper.getJdbcTableType()} previously returned
 * {@code VIEW} (from DuckDB, which always registers tables as views internally)
 * instead of {@code TABLE} from the FileSchema.  Similarly,
 * {@code getStatistic()} returned an empty statistic from DuckDB instead of the
 * PK/FK-aware statistic from FileSchema.
 *
 * <p>Tests verify end-to-end through {@link DatabaseMetaData}:
 * <ul>
 *   <li>{@code getTables()} must report TABLE_TYPE=TABLE, not VIEW</li>
 *   <li>{@code getPrimaryKeys()} must return the columns declared in the model</li>
 * </ul>
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class DuckDBMetadataAuthorityTest {

  @TempDir
  Path tempDir;

  // -----------------------------------------------------------------------
  // Tests
  // -----------------------------------------------------------------------

  @Test
  void getTables_reportsTABLEType_notVIEW() throws Exception {
    createParquet("customers.parquet",
        "SELECT CAST(i AS INTEGER) AS customer_id, 'name_' || CAST(i AS VARCHAR) AS name "
        + "FROM generate_series(1, 3) AS t(i)");

    try (Connection conn = openConnection()) {
      DatabaseMetaData meta = conn.getMetaData();
      // TABLE_TYPE must be TABLE — DuckDB registers parquet views internally as VIEW,
      // but Calcite/FileSchema is the metadata authority and knows the real type.
      try (ResultSet rs = meta.getTables(null, "test", "customers", new String[]{"TABLE"})) {
        assertTrue(rs.next(), "customers should appear as TABLE_TYPE=TABLE");
        assertEquals("TABLE", rs.getString("TABLE_TYPE"),
            "DuckDB engine must report FileSchema table type (TABLE), not DuckDB view type (VIEW)");
        assertFalse(rs.next(), "exactly one customers row expected");
      }
    }
  }

  @Test
  void getTables_doesNotReturnViewType() throws Exception {
    createParquet("customers.parquet",
        "SELECT CAST(i AS INTEGER) AS customer_id, 'n' || CAST(i AS VARCHAR) AS name "
        + "FROM generate_series(1, 3) AS t(i)");

    try (Connection conn = openConnection()) {
      DatabaseMetaData meta = conn.getMetaData();
      // Filtering for VIEW must return nothing for a parquet-backed table
      try (ResultSet rs = meta.getTables(null, "test", "customers", new String[]{"VIEW"})) {
        assertFalse(rs.next(),
            "customers is a TABLE, not a VIEW — DuckDB must not pollute table type metadata");
      }
    }
  }

  @Test
  void getPrimaryKeys_returnsDeclaredPkColumn() throws Exception {
    createParquet("customers.parquet",
        "SELECT CAST(i AS INTEGER) AS customer_id, 'name_' || CAST(i AS VARCHAR) AS name "
        + "FROM generate_series(1, 3) AS t(i)");

    try (Connection conn = openConnection()) {
      DatabaseMetaData meta = conn.getMetaData();
      List<String> pk = readPkColumns(meta, "test", "customers");
      assertEquals(1, pk.size(), "customers must have exactly one PK column");
      assertEquals("customer_id", pk.get(0),
          "DuckDB engine must expose PK from FileSchema, not from DuckDB view metadata");
    }
  }

  @Test
  void getPrimaryKeys_noConstraint_returnsEmpty() throws Exception {
    createParquet("products.parquet",
        "SELECT CAST(i AS INTEGER) AS product_id FROM generate_series(1, 3) AS t(i)");

    try (Connection conn = openConnection()) {
      DatabaseMetaData meta = conn.getMetaData();
      // products has no constraint in the model — must return empty
      List<String> pk = readPkColumns(meta, "test", "products");
      assertTrue(pk.isEmpty(), "Table with no constraint should return no PKs from DuckDB engine");
    }
  }

  /**
   * When the parquet table IS in DuckDB's catalog (as a VIEW) but FileSchema returns a
   * non-CommentableTable for it, the TABLE_TYPE must still be TABLE.
   *
   * <p>Regression for the production scenario where IcebergTable init fails (Hadoop/Kerberos
   * {@code getSubject is not supported}) leaving the FileSchema table slot null while the
   * DuckDB view is still registered.  In that case {@link
   * org.apache.calcite.adapter.file.duckdb.DuckDBJdbcSchema#getTable} previously returned the raw
   * {@link org.apache.calcite.adapter.jdbc.JdbcTable} (type=VIEW).  Now it falls through to
   * {@code TableTypeCorrectingWrapper} which always reports TABLE.
   *
   * <p>This test verifies the fallback by using a parquet table where FileSchema still builds
   * a real table (but even when the CommentableTable wrapping path is skipped, TABLE_TYPE
   * must survive through the correcting wrapper).
   */
  @Test
  void getTables_tableTypeIsTable_whenFileSchemaHasNonCommentableTable() throws Exception {
    // Use a table name not in tableConstraints — no ConstraintAwareJdbcTable wrapping.
    // The table goes straight through DuckDBJdbcSchema.getTable() and its fallback path.
    createParquet("events.parquet",
        "SELECT CAST(i AS INTEGER) AS event_id FROM generate_series(1, 2) AS t(i)");

    // Model without tableConstraints for "events" — exercises the plain DuckDB->FileSchema path
    String dir = tempDir.toFile().getAbsolutePath().replace("\\", "\\\\");
    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"test\","
        + "\"schemas\":[{"
        + "  \"name\":\"test\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "  \"operand\":{"
        + "    \"directory\":\"" + dir + "\","
        + "    \"ephemeralCache\":true,"
        + "    \"executionEngine\":\"duckdb\""
        + "  }"
        + "}]}";
    Properties props = new Properties();
    props.put("model", "inline:" + model);
    BaseFileTest.applyEngineDefaults(props);
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      DatabaseMetaData meta = conn.getMetaData();
      try (ResultSet rs = meta.getTables(null, "test", "events", new String[]{"TABLE"})) {
        assertTrue(rs.next(),
            "events must appear as TABLE_TYPE=TABLE even without tableConstraints");
        assertEquals("TABLE", rs.getString("TABLE_TYPE"),
            "DuckDB view wrapper must report TABLE, not VIEW");
      }
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private List<String> readPkColumns(DatabaseMetaData meta,
      String schema, String table) throws Exception {
    List<String> cols = new ArrayList<String>();
    try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
      while (rs.next()) {
        cols.add(rs.getString("COLUMN_NAME"));
      }
    }
    return cols;
  }

  private void createParquet(String fileName, String selectSql) throws Exception {
    File out = new File(tempDir.toFile(), fileName);
    String sql = "COPY (" + selectSql + ") TO '" + out.getAbsolutePath() + "' (FORMAT PARQUET)";
    ProcessBuilder pb = new ProcessBuilder("duckdb", "-c", sql);
    pb.redirectErrorStream(true);
    Process proc = pb.start();
    int exit = proc.waitFor();
    if (exit != 0) {
      byte[] buf = new byte[4096];
      int len = proc.getInputStream().read(buf);
      fail("DuckDB CLI failed creating " + fileName + ": " + (len > 0 ? new String(buf, 0, len) : "unknown"));
    }
  }

  private Connection openConnection() throws Exception {
    String dir = tempDir.toFile().getAbsolutePath().replace("\\", "\\\\");
    // customers has PK(customer_id); products has no constraint.
    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"test\","
        + "\"schemas\":[{"
        + "  \"name\":\"test\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "  \"operand\":{"
        + "    \"directory\":\"" + dir + "\","
        + "    \"ephemeralCache\":true,"
        + "    \"executionEngine\":\"duckdb\","
        + "    \"tableConstraints\":{"
        + "      \"customers\":{"
        + "        \"primaryKey\":[\"customer_id\"]"
        + "      }"
        + "    }"
        + "  }"
        + "}]}";

    Properties props = new Properties();
    props.put("model", "inline:" + model);
    BaseFileTest.applyEngineDefaults(props);
    return DriverManager.getConnection("jdbc:calcite:", props);
  }
}
