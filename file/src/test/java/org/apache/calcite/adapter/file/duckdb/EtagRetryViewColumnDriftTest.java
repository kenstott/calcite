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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A view bound over more columns than its table now has is view drift, healed by recreating it.
 *
 * <p>govdata's health.cdc_county_injury_mortality view was bound at 13 columns and its live
 * Iceberg table had 12. On DuckDB 1.4 every read failed with the message below, which the retry
 * did not recognize, so the view was never recreated. DuckDB 1.5 rebinds such a view to its
 * source's current columns, so the read no longer fails; the message stays recognized for any
 * engine still on 1.4.
 */
@Tag("unit")
class EtagRetryViewColumnDriftTest {

  /** DuckDB 1.4.4's message for that read, as the bundle reported it. */
  private static final String DUCKDB_14_MESSAGE = "Binder Error: table \"unnamed_subquery\" has 12"
      + " columns available but 13 columns specified";

  @TempDir Path tempDir;

  @Test void duckDb14sColumnCountMessageIsViewDrift() {
    assertEquals("", EtagRetryConnection.transientRaceTarget(new SQLException(DUCKDB_14_MESSAGE)),
        "recognized as view drift, which recreates the view and retries");
  }

  @Test void aViewOverASourceThatLostAColumnStillReads() throws Exception {
    Path file = tempDir.resolve("t.parquet");
    try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
         Statement s = duck.createStatement()) {
      s.execute("COPY (SELECT 1 AS a, 2 AS b, 3 AS c) TO '" + file + "' (FORMAT PARQUET)");
      s.execute("CREATE VIEW v AS SELECT * FROM read_parquet('" + file + "')");
      s.execute("COPY (SELECT 1 AS a, 2 AS b) TO '" + file + "' (FORMAT PARQUET)");
      try (ResultSet rs = s.executeQuery("SELECT a FROM v")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }
  }

  @Test void aMissingColumnIsNotViewDrift() throws Exception {
    try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
         Statement s = duck.createStatement()) {
      SQLException e = assertThrows(SQLException.class,
          () -> s.executeQuery("SELECT no_such_column FROM (SELECT 1 AS a)"));
      assertNull(EtagRetryConnection.transientRaceTarget(e), e.getMessage());
    }
  }

  /** DuckDB's message for a view whose source renamed a column, as the 0.118.0 data-server
   *  bundle reported it for econ.employment_statistics after periodName became period_name
   *  (govdata-ops #1104, 2026-11-08). */
  private static final String RENAMED_COLUMN_MESSAGE = "Binder Error: Contents of view were "
      + "altered: names don't match! Expected [series, year, period, periodName, value], but "
      + "found [series, year, period, period_name, value] instead";

  @Test void aRenamedColumnMessageIsViewDrift() {
    assertEquals("", EtagRetryConnection.transientRaceTarget(
        new SQLException(RENAMED_COLUMN_MESSAGE)),
        "recognized as view drift, which recreates the view and retries");
  }

  /** What the current DuckDB does when a view's source renames a column: the read either works
   *  or fails with a message the retry recognizes, so the view is recreated. */
  @Test void aViewOverASourceThatRenamedAColumnReadsOrIsRecognized() throws Exception {
    Path file = tempDir.resolve("r.parquet");
    try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
         Statement s = duck.createStatement()) {
      s.execute("COPY (SELECT 1 AS a, 'Jan' AS periodName) TO '" + file + "' (FORMAT PARQUET)");
      s.execute("CREATE VIEW v AS SELECT * FROM read_parquet('" + file + "')");
      s.execute("COPY (SELECT 1 AS a, 'Jan' AS period_name) TO '" + file + "' (FORMAT PARQUET)");
      try (ResultSet rs = s.executeQuery("SELECT a FROM v")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      } catch (SQLException e) {
        assertEquals("", EtagRetryConnection.transientRaceTarget(e),
            "a rename the current DuckDB rejects must be recognized as view drift: "
                + e.getMessage());
      }
    }
  }
}
