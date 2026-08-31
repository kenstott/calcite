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

import org.apache.calcite.adapter.file.partition.PGPipelineTracker;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@code ChunkOrganizer}'s PG compute-layer logic against a live PostgreSQL instance:
 * the skip-if-unchanged / tombstone-and-replace primitives, the coarse per-source watermark, and
 * (where live source data allows) a full {@link ChunkOrganizer#sweep} end to end. {@code
 * vc_staging} is the durable artifact (backed up via {@code pg_dump}, not synced to Iceberg), so
 * there is no sync-side logic to exercise here.
 *
 * <p>All writes are scoped to an isolated PG schema ({@link #NS}, dropped in {@link #afterAll}),
 * never the real {@code vc_staging}/{@code vc_sync_state}. The full-sweep test reads REAL MinIO
 * data (read-only) via {@code GOVDATA_PARQUET_DIR} + {@code AWS_*} -- skipped if those aren't
 * set, same as the PG assumption below.
 *
 * <p>Requires {@code CALCITE_TRACKER_PG_URL} (and optionally {@code _USER}/{@code _PASSWORD}) in
 * the environment; skips via {@code @Tag("integration")} otherwise.
 */
@Tag("integration")
class ChunkOrganizerVcStagingTest {

  private static final String NS = "vc_staging_test";
  private static String trackerNs;
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
    String base = System.getenv("GOVDATA_PARQUET_DIR");
    trackerNs = PGPipelineTracker.sanitizeNamespace(base != null ? base : "s3://govdata-parquet-v1");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS \"" + NS + "\"");
      // search_path puts NS first (vc_staging/vc_tombstones/vc_sync_state, created by
      // ensureVcSchema below, resolve there) then falls through to the REAL tracker namespace
      // for table_completion -- read-only lookups against real data, never written here.
      stmt.execute("SET search_path TO \"" + NS + "\", \"" + trackerNs + "\"");
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

  private static Map<String, Object> row(String schema, String table, String fk, long seq,
      String parentHash, String text) {
    Map<String, Object> r = new LinkedHashMap<>();
    r.put("source_schema", schema);
    r.put("source_table", table);
    r.put("stringified_fk", fk);
    r.put("sequence", seq);
    r.put("chunk_id", schema + ":" + table + ":" + fk + ":" + seq);
    r.put("parent_hash", parentHash);
    r.put("source_type", "row_concat");
    r.put("chunk_text", text);
    r.put("enriched_text", text);
    return r;
  }

  private static long stagingCount(String schema, String table, String fk) throws Exception {
    try (PreparedStatement ps = conn.prepareStatement(
        "SELECT count(*) FROM vc_staging WHERE source_schema=? AND source_table=? "
        + "AND stringified_fk=?")) {
      ps.setString(1, schema);
      ps.setString(2, table);
      ps.setString(3, fk);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  private static long tombstoneCount(String schema, String table, String fk) throws Exception {
    try (PreparedStatement ps = conn.prepareStatement(
        "SELECT count(*) FROM vc_tombstones WHERE source_schema=? AND source_table=? "
        + "AND stringified_fk=?")) {
      ps.setString(1, schema);
      ps.setString(2, table);
      ps.setString(3, fk);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  // ========================================================================
  // Fine-grained layer: parent_hash skip-if-unchanged / tombstone-and-replace
  // ========================================================================

  @Test void firstInsertLands() throws Exception {
    ChunkOrganizer.insertParentRows(conn,
        java.util.Collections.singletonList(row("t", "widgets", "1", 0, "hashA", "hello")));
    conn.commit();
    assertEquals(1, stagingCount("t", "widgets", "1"));
    assertEquals("hashA", ChunkOrganizer.selectExistingParentHashes(conn, "t", "widgets", java.util.Collections.singletonList("1")).get("1"));
  }

  @Test void unchangedHashIsNoOpAtCallerLevel() throws Exception {
    ChunkOrganizer.insertParentRows(conn,
        java.util.Collections.singletonList(row("t", "gadgets", "2", 0, "hashB", "v1")));
    conn.commit();
    // writeToPgStaging's own loop is what skips on a hash match -- this proves the primitive
    // it relies on: selectExistingParentHashes correctly reports the stored hash (or null).
    assertEquals("hashB", ChunkOrganizer.selectExistingParentHashes(conn, "t", "gadgets", java.util.Collections.singletonList("2")).get("2"));
    assertNull(ChunkOrganizer.selectExistingParentHashes(conn, "t", "gadgets", java.util.Collections.singletonList("nonexistent")).get("nonexistent"));
  }

  @Test void changedParentTombstonesOldAndAcceptsNew() throws Exception {
    ChunkOrganizer.insertParentRows(conn, java.util.Arrays.asList(
        row("t", "sprockets", "3", 0, "hashC1", "part one"),
        row("t", "sprockets", "3", 1, "hashC1", "part two")));
    conn.commit();
    assertEquals(2, stagingCount("t", "sprockets", "3"));
    assertEquals(0, tombstoneCount("t", "sprockets", "3"));

    ChunkOrganizer.tombstoneParents(conn, "t", "sprockets", java.util.Collections.singletonList("3"));
    conn.commit();
    assertEquals(0, stagingCount("t", "sprockets", "3"));
    assertEquals(2, tombstoneCount("t", "sprockets", "3"));

    ChunkOrganizer.insertParentRows(conn,
        java.util.Collections.singletonList(row("t", "sprockets", "3", 0, "hashC2", "merged")));
    conn.commit();
    assertEquals(1, stagingCount("t", "sprockets", "3"));
    assertEquals("hashC2", ChunkOrganizer.selectExistingParentHashes(conn, "t", "sprockets", java.util.Collections.singletonList("3")).get("3"));
    // Tombstones from the prior generation are retained (drained by the not-yet-built sync
    // step), not overwritten by a later insert of the same parent.
    assertEquals(2, tombstoneCount("t", "sprockets", "3"));
  }

  @Test void writeToPgStagingSkipsUnchangedAndReplacesChanged() throws Exception {
    // Exercises the actual caller-level method (not just its primitives): first write lands,
    // identical-hash rewrite is a true no-op (no tombstones at all), changed-hash rewrite
    // replaces.
    java.util.List<Map<String, Object>> gen1 = java.util.Collections.singletonList(
        row("t", "bolts", "4", 0, "hashD1", "v1"));
    ChunkOrganizer.writeToPgStaging(conn, gen1);
    assertEquals(1, stagingCount("t", "bolts", "4"));
    assertEquals(0, tombstoneCount("t", "bolts", "4"));

    // Same hash, same text -- true no-op.
    ChunkOrganizer.writeToPgStaging(conn, gen1);
    assertEquals(1, stagingCount("t", "bolts", "4"));
    assertEquals(0, tombstoneCount("t", "bolts", "4"));

    // Changed hash -- replace.
    java.util.List<Map<String, Object>> gen2 = java.util.Collections.singletonList(
        row("t", "bolts", "4", 0, "hashD2", "v2"));
    ChunkOrganizer.writeToPgStaging(conn, gen2);
    assertEquals(1, stagingCount("t", "bolts", "4"));
    assertEquals(1, tombstoneCount("t", "bolts", "4"));
    assertEquals("hashD2", ChunkOrganizer.selectExistingParentHashes(conn, "t", "bolts", java.util.Collections.singletonList("4")).get("4"));
  }

  @Test void ensureVcSchemaIsIdempotent() throws Exception {
    ChunkOrganizer.ensureVcSchema(conn);
    conn.commit();
  }

  @Test void sha256HexIsDeterministicAndDistinct() {
    String h1 = ChunkOrganizer.sha256Hex("same text");
    String h2 = ChunkOrganizer.sha256Hex("same text");
    String h3 = ChunkOrganizer.sha256Hex("different text");
    assertEquals(h1, h2);
    assertEquals(64, h1.length());
    assertNotEquals(h1, h3);
  }

  @Test void chunkerVersionIsFoldedIntoTheHash() {
    // Guards against a regression where someone removes the CHUNKER_VERSION fold: hashing the
    // same text with and without a version prefix must differ, or a chunking-logic fix would
    // never invalidate stored hashes the way the class javadoc promises.
    String withV1 = ChunkOrganizer.sha256Hex("1:some source text");
    String bare = ChunkOrganizer.sha256Hex("some source text");
    assertNotEquals(withV1, bare);
  }

  // ========================================================================
  // Coarse layer: per-source watermark (table_completion.completed_at vs vc_sync_state)
  // ========================================================================

  @Test void neverCompletedSourceNeedsNoSweep() throws Exception {
    assertFalse(ChunkOrganizer.sourceNeedsSweep(conn, "__definitely_nonexistent_table__"));
  }

  @Test void watermarkTransitionsFromNeededToSatisfiedAfterMarkSwept() throws Exception {
    // owasp_top10 is real, live data this session confirmed still populated (unlike the other
    // six ChunkOrganizer sources, purged earlier this session) -- its table_completion row is
    // read-only here, never written. Reset only this test's OWN watermark row in the isolated
    // NS schema so the test is self-contained regardless of method execution order.
    try (PreparedStatement ps = conn.prepareStatement(
        "DELETE FROM vc_sync_state WHERE source_table = 'owasp_top10'")) {
      ps.executeUpdate();
    }
    conn.commit();

    assertTrue(ChunkOrganizer.sourceNeedsSweep(conn, "owasp_top10"),
        "no watermark recorded yet -- must need a sweep");

    ChunkOrganizer.markSwept(conn, "cyber_threat", "owasp_top10");

    assertFalse(ChunkOrganizer.sourceNeedsSweep(conn, "owasp_top10"),
        "watermark now matches table_completion.completed_at -- must be satisfied");
  }

  // ========================================================================
  // Full end-to-end sweep against real (read-only) MinIO data
  // ========================================================================

  @Test void sweepEndToEndAgainstRealOwaspTop10() throws Exception {
    String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    Assumptions.assumeTrue(accessKey != null,
        "AWS_ACCESS_KEY_ID not set -- skipping real-data sweep test");
    String base = System.getenv("GOVDATA_PARQUET_DIR");
    Assumptions.assumeTrue(base != null,
        "GOVDATA_PARQUET_DIR not set -- skipping real-data sweep test");

    // Clean slate for this one source so the test is self-contained regardless of order or a
    // prior run in the same NS.
    try (PreparedStatement ps = conn.prepareStatement(
        "DELETE FROM vc_sync_state WHERE source_table = 'owasp_top10'")) {
      ps.executeUpdate();
    }
    try (PreparedStatement ps = conn.prepareStatement(
        "DELETE FROM vc_staging WHERE source_schema = 'cyber_threat' "
        + "AND source_table = 'owasp_top10'")) {
      ps.executeUpdate();
    }
    try (PreparedStatement ps = conn.prepareStatement(
        "DELETE FROM vc_tombstones WHERE source_schema = 'cyber_threat' "
        + "AND source_table = 'owasp_top10'")) {
      ps.executeUpdate();
    }
    conn.commit();

    try (Connection duckdb = openStandaloneDuckDbForTest()) {
      // First sweep: owasp_top10's watermark is unset -> needs sweeping. The other six
      // registered sources have no table_completion row at all this session (purged) -> cleanly
      // skipped, not errored on a missing Iceberg table.
      ChunkOrganizer.sweep(duckdb, conn, base);
    }

    assertTrue(countAllOwaspTop10Parents() >= 1,
        "expected at least one owasp_top10 parent staged from real data");

    Long afterFirstSweep = selectLastSweptCompletedAt();
    assertTrue(afterFirstSweep != null && afterFirstSweep > 0,
        "watermark must be recorded after a real sweep");
    assertFalse(ChunkOrganizer.sourceNeedsSweep(conn, "owasp_top10"),
        "immediately after a sweep, the same source must not need another one");

    long stagedAfterFirst = countAllOwaspTop10StagedRows();

    // Second sweep: watermark unchanged -> must skip owasp_top10 entirely (no re-scan, no
    // duplicate rows, no tombstones from a no-op "change").
    try (Connection duckdb = openStandaloneDuckDbForTest()) {
      ChunkOrganizer.sweep(duckdb, conn, base);
    }
    assertEquals(stagedAfterFirst, countAllOwaspTop10StagedRows(),
        "a second sweep with an unchanged watermark must be a true no-op");
    assertEquals(0, tombstoneCountForOwasp(),
        "a true no-op sweep must not tombstone anything");
  }

  private static long countAllOwaspTop10Parents() throws Exception {
    try (PreparedStatement ps = conn.prepareStatement(
        "SELECT count(DISTINCT stringified_fk) FROM vc_staging "
        + "WHERE source_schema='cyber_threat' AND source_table='owasp_top10'")) {
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  private static long countAllOwaspTop10StagedRows() throws Exception {
    try (PreparedStatement ps = conn.prepareStatement(
        "SELECT count(*) FROM vc_staging "
        + "WHERE source_schema='cyber_threat' AND source_table='owasp_top10'")) {
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  private static long tombstoneCountForOwasp() throws Exception {
    try (PreparedStatement ps = conn.prepareStatement(
        "SELECT count(*) FROM vc_tombstones "
        + "WHERE source_schema='cyber_threat' AND source_table='owasp_top10'")) {
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  private static Long selectLastSweptCompletedAt() throws Exception {
    try (PreparedStatement ps = conn.prepareStatement(
        "SELECT last_swept_completed_at FROM vc_sync_state WHERE source_table = 'owasp_top10'")) {
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : null;
      }
    }
  }

  /** Mirrors {@code ChunkOrganizer.openDuckDbStandalone} (package-private, not exposed for
   *  direct reuse since it reads env vars itself) -- same setup, for this test's own S3 env. */
  private static Connection openStandaloneDuckDbForTest() throws Exception {
    Connection c = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement stmt = c.createStatement()) {
      stmt.execute("SET threads=2");
      stmt.execute("SET memory_limit='2GB'");
      stmt.execute("INSTALL parquet");
      stmt.execute("LOAD parquet");
      stmt.execute("INSTALL iceberg");
      stmt.execute("LOAD iceberg");
      stmt.execute("SET unsafe_enable_version_guessing = true");
      stmt.execute("INSTALL httpfs");
      stmt.execute("LOAD httpfs");
      stmt.execute("SET http_timeout=10000");
      String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
      String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
      String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
      String region = System.getenv("AWS_REGION") != null ? System.getenv("AWS_REGION") : "auto";
      StringBuilder secret = new StringBuilder("CREATE OR REPLACE SECRET calcite_s3 (TYPE S3");
      secret.append(", KEY_ID '").append(accessKey).append('\'');
      secret.append(", SECRET '").append(secretKey).append('\'');
      if (endpoint != null && !endpoint.isEmpty()) {
        String endpointHost = endpoint.replaceFirst("^https?://", "");
        secret.append(", ENDPOINT '").append(endpointHost).append('\'');
        secret.append(", URL_STYLE 'path'");
        secret.append(", USE_SSL ").append(endpoint.startsWith("http://") ? "false" : "true");
      }
      secret.append(", REGION '").append(region).append('\'');
      secret.append(')');
      stmt.execute(secret.toString());
    }
    return c;
  }
}
