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

import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-152 / FILE-154 — two file-adapter execution requirements recoded to exact assertions.
 *
 * <p>FILE-152 (pure, {@code @Tag("unit")}) pins the HyperLogLog precision-vs-accuracy bounds table
 * using deterministic keys ("k-"+i) so the error rates are reproducible (no {@code Math.random}).
 * StatisticsRequirementsTest already covers precision/buckets/merge basics, so this only adds the
 * measured-error-bounds table plus the 1 byte/bucket memory pin via {@code getBuckets().length}.
 *
 * <p>FILE-154 (needs duckdb on PATH, {@code @Tag("integration")}) builds the same CSV+JSON fixture
 * twice — once with {@code executionEngine=parquet}, once with {@code executionEngine=duckdb} — and
 * asserts COUNT(*), filter, row-level scan, and GROUP BY results are element-for-element identical
 * across the two engines AND equal a hand-computed golden, through the full JDBC path.
 *
 * <p>The class mixes a pure unit method with a duckdb integration method; methods are tagged
 * individually so {@code -PincludeTags="FILE-152,FILE-154"} selects both.
 */
public class HllAccuracyEngineParityRequirementsTest {

  // ---------------------------------------------------------------------------
  // FILE-152 — HyperLogLog measured accuracy bounds (pure, deterministic keys)
  // ---------------------------------------------------------------------------

  /** Build a sketch over {@code count} deterministic distinct keys "k-0".."k-(count-1)". */
  private static HyperLogLogSketch sketchOverDistinctKeys(int precision, int count) {
    HyperLogLogSketch hll = new HyperLogLogSketch(precision);
    for (int i = 0; i < count; i++) {
      hll.addString("k-" + i);
    }
    return hll;
  }

  private static double errorRate(HyperLogLogSketch hll, int actual) {
    long estimate = hll.getEstimate();
    return Math.abs(estimate - actual) / (double) actual;
  }

  private static void assertWithin(int precision, int actual, double bound) {
    HyperLogLogSketch hll = sketchOverDistinctKeys(precision, actual);
    double err = errorRate(hll, actual);
    assertTrue(err < bound,
        String.format("precision %d, %d distinct: error %.4f must be < %.4f (estimate=%d)",
            precision, actual, err, bound, hll.getEstimate()));
  }

  @Test @Tag("unit") @Tag("FILE-152")
  void hllPrecisionVsAccuracyBoundsTable() {
    // precision 12: 100 distinct within 20%, 10k within 5%, 100k within 5%.
    assertWithin(12, 100, 0.20);
    assertWithin(12, 10_000, 0.05);
    assertWithin(12, 100_000, 0.05);

    // precision 14: large dataset within 2%.
    // NOTE: exemplar used 1,000,000; deterministic "k-"+i keys at precision 14 land well under 2%
    // and run in a few seconds, so the full 1,000,000 target is kept.
    assertWithin(14, 1_000_000, 0.02);

    // Every precision 8/10/12/14 keeps error < 10% over 50k distinct.
    for (int precision : new int[] {8, 10, 12, 14}) {
      assertWithin(precision, 50_000, 0.10);
    }
  }

  @Test @Tag("unit") @Tag("FILE-152")
  void hllMemoryIsOneBytePerBucket() {
    // memory == 2^precision bytes (1 byte/bucket), measured via getBuckets().length.
    for (int precision : new int[] {8, 10, 12, 14}) {
      HyperLogLogSketch hll = new HyperLogLogSketch(precision);
      assertEquals(1 << precision, hll.getBuckets().length,
          "precision " + precision + " must allocate exactly 2^precision bucket bytes");
    }
  }

  // ---------------------------------------------------------------------------
  // FILE-154 — Parquet vs DuckDB engine parity through the full JDBC path
  // ---------------------------------------------------------------------------

  private static final String[] PARITY_ENGINES = {"parquet", "duckdb"};

  @Test @Tag("integration") @Tag("FILE-154")
  void parquetAndDuckDbAgreeAndMatchGolden(@TempDir Path root) throws Exception {
    Path src = Files.createDirectories(root.resolve("src"));
    // 6-row CSV fixture: region in {east,west}, amount integers chosen for a clean GROUP BY golden.
    Files.write(src.resolve("sales.csv"),
        ("id,region,amount\n"
            + "1,east,10\n"
            + "2,west,20\n"
            + "3,east,30\n"
            + "4,west,40\n"
            + "5,east,50\n"
            + "6,west,60\n").getBytes(StandardCharsets.UTF_8));
    // 3-row JSON fixture.
    Files.write(src.resolve("people.json"),
        ("[{\"id\":1,\"name\":\"alice\"},"
            + "{\"id\":2,\"name\":\"bob\"},"
            + "{\"id\":3,\"name\":\"carol\"}]").getBytes(StandardCharsets.UTF_8));

    // Queries exercised through each engine: COUNT(*), filter, row scan, GROUP BY.
    String countCsv = "SELECT COUNT(*) FROM s.sales";
    // NOTE: filter threshold is an inline literal, not a "?" bind parameter. DuckDB pushes the
    // predicate into its own JDBC engine where an untyped bind "?" infers VARCHAR and fails the
    // amount(>DECIMAL) comparison; a literal keeps the comparison type known. Still prepared below.
    String filterCsv = "SELECT id, region, amount FROM s.sales WHERE amount > 25 ORDER BY id";
    String scanJson = "SELECT id, name FROM s.people ORDER BY id";
    String groupCsv =
        "SELECT region, COUNT(*), SUM(amount) FROM s.sales GROUP BY region ORDER BY region";

    // Hand-computed goldens (engine-independent truth).
    String goldenCount = "6";
    String goldenFilter = "3 | east | 30\n4 | west | 40\n5 | east | 50\n6 | west | 60\n";
    String goldenScan = "1 | alice\n2 | bob\n3 | carol\n";
    // east: rows 1,3,5 -> count 3, sum 90; west: rows 2,4,6 -> count 3, sum 120.
    // NOTE: CSV "amount" is inferred as DECIMAL, so SUM renders with scale (90.000000000).
    String goldenGroup = "east | 3 | 90.000000000\nwest | 3 | 120.000000000\n";

    String countParquet = null;
    String filterParquet = null;
    String scanParquet = null;
    String groupParquet = null;
    String firstEngine = null;

    for (String engine : PARITY_ENGINES) {
      Path cache = Files.createDirectories(root.resolve("cache_" + engine));

      String count = render(query(src, cache, engine, countCsv));
      String filter = render(query(src, cache, engine, filterCsv));
      String scan = render(query(src, cache, engine, scanJson));
      String group = render(query(src, cache, engine, groupCsv));

      // Each engine must equal the known golden.
      assertEquals(goldenCount + "\n", count, engine + " COUNT(*) golden");
      assertEquals(goldenFilter, filter, engine + " filter golden");
      assertEquals(goldenScan, scan, engine + " json scan golden");
      assertEquals(goldenGroup, group, engine + " GROUP BY golden");

      // And the two engines must agree element-for-element.
      if (firstEngine == null) {
        firstEngine = engine;
        countParquet = count;
        filterParquet = filter;
        scanParquet = scan;
        groupParquet = group;
      } else {
        assertEquals(countParquet, count, engine + " COUNT(*) must match " + firstEngine);
        assertEquals(filterParquet, filter, engine + " filter must match " + firstEngine);
        assertEquals(scanParquet, scan, engine + " json scan must match " + firstEngine);
        assertEquals(groupParquet, group, engine + " GROUP BY must match " + firstEngine);
      }
    }
  }

  /** Run {@code sql} through one engine via a prepared statement, returning rendered rows. */
  private static List<String[]> query(Path src, Path cache, String engine, String sql)
      throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + model(src, cache, engine));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    List<String[]> rows = new ArrayList<>();
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         PreparedStatement ps = conn.prepareStatement(sql)) {
      try (ResultSet rs = ps.executeQuery()) {
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();
        while (rs.next()) {
          String[] row = new String[cols];
          for (int i = 1; i <= cols; i++) {
            Object v = rs.getObject(i);
            row[i - 1] = rs.wasNull() || v == null ? "␀" : v.toString();
          }
          rows.add(row);
        }
      }
    }
    return rows;
  }

  private static String model(Path src, Path cache, String engine) {
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
        + "      \"executionEngine\": \"" + engine + "\"\n"
        + "    }\n"
        + "  }]\n"
        + "}\n";
  }

  private static String render(List<String[]> rows) {
    StringBuilder sb = new StringBuilder();
    for (String[] row : rows) {
      sb.append(String.join(" | ", row)).append('\n');
    }
    return sb.toString();
  }
}
