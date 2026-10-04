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
package org.apache.calcite.adapter.govdata.ref;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code sweep(..., onlySources)} must touch only the named {@code schema.table} sources, in both
 * the row-concat and the document-blob loop.
 *
 * <p>Each registered source below has a {@code table_completion} row, so it needs sweeping, but
 * the sweep's base path holds no data. A source the filter lets through therefore fails on its
 * {@code iceberg_scan}; a source the filter excludes is never scanned, so the sweep returns
 * cleanly and marks nothing. That makes "no exception" the proof a source was skipped and the
 * source's schema directory in the exception's SQL (DuckDB truncates the rest of the path) the
 * proof of which source was attempted.
 *
 * <p>Writes are confined to an isolated PG schema dropped in {@link #afterAll}. Requires {@code
 * CALCITE_TRACKER_PG_URL}; skipped otherwise.
 */
@Tag("integration")
class ChunkOrganizerOnlySourcesTest {

  private static final String NS = "vc_only_sources_test";
  private static final String NO_DATA_BASE = "/nonexistent/only-sources-test";
  private static final String ROW_CONCAT = "disasters.disaster_declarations";
  private static final String BLOB = "patents.patent_abstracts";
  private static Connection conn;

  @BeforeAll
  static void openConnectionAndSchema() throws Exception {
    String url = System.getenv("CALCITE_TRACKER_PG_URL");
    Assumptions.assumeTrue(url != null,
        "CALCITE_TRACKER_PG_URL not set -- skipping live PG integration test");
    String user = System.getenv("CALCITE_TRACKER_PG_USER");
    String password = System.getenv("CALCITE_TRACKER_PG_PASSWORD");
    conn = user != null ? DriverManager.getConnection(url, user, password)
        : DriverManager.getConnection(url);
    conn.setAutoCommit(false);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("DROP SCHEMA IF EXISTS \"" + NS + "\" CASCADE");
      stmt.execute("CREATE SCHEMA \"" + NS + "\"");
      stmt.execute("SET search_path TO \"" + NS + "\"");
      stmt.execute("CREATE TABLE table_completion ("
          + "pipeline_name VARCHAR PRIMARY KEY, completed_at BIGINT NOT NULL)");
    }
    ChunkOrganizer.ensureVcSchema(conn);
    conn.commit();
  }

  @AfterAll
  static void dropSchemaAndClose() throws Exception {
    if (conn == null) {
      return;
    }
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("DROP SCHEMA IF EXISTS \"" + NS + "\" CASCADE");
    }
    conn.commit();
    conn.close();
  }

  @BeforeEach
  void bothSourcesNeedSweeping() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM vc_sync_state");
      stmt.execute("DELETE FROM table_completion");
    }
    for (String table : new String[] {"disaster_declarations", "patent_abstracts"}) {
      try (PreparedStatement ps = conn.prepareStatement(
          "INSERT INTO table_completion (pipeline_name, completed_at) VALUES (?, 1)")) {
        ps.setString(1, table);
        ps.executeUpdate();
      }
    }
    conn.commit();
  }

  private static Set<String> only(String... sources) {
    Set<String> set = new HashSet<String>();
    Collections.addAll(set, sources);
    return set;
  }

  private static void sweepOnly(Set<String> onlySources) throws Exception {
    try (Connection duckdb = ChunkOrganizer.openDuckDbStandalone()) {
      ChunkOrganizer.sweep(duckdb, conn, NO_DATA_BASE, 0, false, onlySources);
    }
  }

  private static long markedSwept() throws Exception {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT count(*) FROM vc_sync_state")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  /** Control: with no filter the same setup does try to scan, so the tests below prove something. */
  @Test void unfilteredSweepAttemptsEverySourceThatNeedsSweeping() {
    Exception e = assertThrows(Exception.class, () -> {
      try (Connection duckdb = ChunkOrganizer.openDuckDbStandalone()) {
        ChunkOrganizer.sweep(duckdb, conn, NO_DATA_BASE, 0, false, null);
      }
    });
    assertTrue(String.valueOf(e.getMessage()).contains("/disasters/"),
        "row-concat source is swept first and has no data: " + e.getMessage());
  }

  @Test void aFilterNamingNeitherSourceScansNothingInEitherLoop() throws Exception {
    sweepOnly(only("cyber_threat.owasp_top10"));
    assertEquals(0, markedSwept());
  }

  @Test void anEmptyFilterScansNothing() throws Exception {
    sweepOnly(new HashSet<String>());
    assertEquals(0, markedSwept());
  }

  @Test void aFilterNamingARowConcatSourceScansOnlyThatOne() {
    Exception e = assertThrows(Exception.class, () -> sweepOnly(only(ROW_CONCAT)));
    assertTrue(String.valueOf(e.getMessage()).contains("/disasters/"),
        "named row-concat source must be attempted: " + e.getMessage());
  }

  /** The row-concat loop runs first, so reaching the blob source's scan proves it was skipped. */
  @Test void aFilterNamingADocumentBlobSourceSkipsTheRowConcatLoop() {
    Exception e = assertThrows(Exception.class, () -> sweepOnly(only(BLOB)));
    assertTrue(String.valueOf(e.getMessage()).contains("/patents/"),
        "named document-blob source must be attempted: " + e.getMessage());
    assertTrue(!String.valueOf(e.getMessage()).contains("/disasters/"),
        "unnamed row-concat source must not be attempted: " + e.getMessage());
  }
}
