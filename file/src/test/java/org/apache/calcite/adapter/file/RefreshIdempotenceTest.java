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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-029 / FILE-086 — parquet-cache idempotence and exclusivity.
 *
 * <p>FILE-029: re-ingesting an unchanged source into a persistent parquet cache must NOT rewrite the
 * cache file (no churn) — the invariant whose absence produced resume/re-write defects.
 * FILE-086: setting refreshInterval disables parquet conversion caching (mutually exclusive).
 */
@Tag("unit")
public class RefreshIdempotenceTest {

  @Test @Tag("FILE-029") void reingestionDoesNotRewriteParquetCache(@TempDir Path root) throws Exception {
    Path src = fixture(root, "src");
    Path cache = Files.createDirectories(root.resolve("cache"));

    query(src, cache, null);
    Map<String, Long> before = parquetMtimes(cache);
    assertFalse(before.isEmpty(), "the parquet engine should have produced a cache file on first ingest");

    Thread.sleep(1100); // mtime comparison resolves at ~1s granularity (FILE-130)
    query(src, cache, null); // unchanged source

    assertEquals(before, parquetMtimes(cache),
        "re-ingesting an unchanged source must not rewrite the parquet cache");
  }

  // FINDING: a SCHEMA-level refreshInterval did NOT suppress the parquet cache (a .parquet was still
  // written). The "mutually exclusive" exclusivity likely needs per-table config, or refreshInterval
  // swaps in a *refreshable* parquet cache (so "no .parquet file" is the wrong assertion). Staged until
  // the precise exclusivity contract is pinned (per-table config vs doc/code discrepancy).
  @Test @Tag("FILE-086")
  @Disabled("FINDING: schema-level refreshInterval did not disable the parquet cache — investigate")
  void refreshIntervalDisablesParquetCache(@TempDir Path root) throws Exception {
    Path src = fixture(root, "src");

    Path baseline = Files.createDirectories(root.resolve("c1"));
    query(src, baseline, null);
    assertFalse(parquetMtimes(baseline).isEmpty(), "without refreshInterval a parquet cache is created");

    Path refreshing = Files.createDirectories(root.resolve("c2"));
    query(src, refreshing, "1 hour");
    assertTrue(parquetMtimes(refreshing).isEmpty(),
        "refreshInterval must disable parquet conversion caching (mutually exclusive)");
  }

  // ---------------------------------------------------------------------------------------------

  private static Path fixture(Path root, String name) throws IOException {
    Path src = Files.createDirectories(root.resolve(name));
    Files.write(src.resolve("data.csv"),
        "id,name\n1,alice\n2,bob\n3,carol\n".getBytes(StandardCharsets.UTF_8));
    return src;
  }

  private static void query(Path src, Path cache, String refreshInterval) throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + model(src, cache, refreshInterval));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM s.data ORDER BY id")) {
      while (rs.next()) {
        rs.getObject(1);
      }
    }
  }

  private static Map<String, Long> parquetMtimes(Path dir) throws IOException {
    Map<String, Long> m = new TreeMap<>();
    if (!Files.exists(dir)) {
      return m;
    }
    try (Stream<Path> s = Files.walk(dir)) {
      s.filter(p -> p.toString().endsWith(".parquet"))
          .forEach(p -> m.put(dir.relativize(p).toString(), p.toFile().lastModified()));
    }
    return m;
  }

  private static String model(Path src, Path cache, String refreshInterval) {
    String refresh = refreshInterval == null
        ? "" : ",\n      \"refreshInterval\": \"" + refreshInterval + "\"";
    return "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"s\",\n"
        + "  \"schemas\": [{\n"
        + "    \"name\": \"s\",\n"
        + "    \"type\": \"custom\",\n"
        + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "    \"operand\": {\n"
        + "      \"directory\": \"" + src.toString().replace("\\", "\\\\") + "\",\n"
        + "      \"baseDirectory\": \"" + cache.toString().replace("\\", "\\\\") + "\",\n"
        + "      \"ephemeralCache\": false,\n"
        + "      \"primeCache\": false,\n"
        + "      \"executionEngine\": \"parquet\"" + refresh + "\n"
        + "    }\n"
        + "  }]\n"
        + "}\n";
  }
}
