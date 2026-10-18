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
 * Iceberg table had 12; on DuckDB 1.4 every read failed with {@code table "unnamed_subquery" has
 * 12 columns available but 13 columns specified}, which the retry did not recognize, so the
 * view was never recreated.
 */
@Tag("unit")
class EtagRetryViewColumnDriftTest {

  @TempDir Path tempDir;

  @Test void aViewBoundOverMoreColumnsThanItsTableHasIsViewDrift() throws Exception {
    Path file = tempDir.resolve("t.parquet");
    String scan = "SELECT * FROM read_parquet('" + file + "')";
    try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
         Statement s = duck.createStatement()) {
      s.execute("COPY (SELECT 1 AS a, 2 AS b, 3 AS c) TO '" + file + "' (FORMAT PARQUET)");
      s.execute("CREATE VIEW v AS " + scan);
      // The table loses a column after the view was bound.
      s.execute("COPY (SELECT 1 AS a, 2 AS b) TO '" + file + "' (FORMAT PARQUET)");

      SQLException drift = assertThrows(SQLException.class, () -> read(duck));
      assertTrue(drift.getMessage().contains("2 columns available but 3 columns specified"),
          drift.getMessage());
      assertEquals("", EtagRetryConnection.transientRaceTarget(drift),
          "recognized as view drift, which recreates the view and retries");

      // What the heal does for a base view: recreate it over the live table.
      try (Statement heal = duck.createStatement()) {
        heal.execute("CREATE OR REPLACE VIEW v AS " + scan);
      }
      assertEquals(1, read(duck));
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

  private static int read(Connection duck) throws SQLException {
    try (Statement s = duck.createStatement(); ResultSet rs = s.executeQuery("SELECT a FROM v")) {
      rs.next();
      return rs.getInt(1);
    }
  }
}
