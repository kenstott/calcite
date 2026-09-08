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
package org.apache.calcite.adapter.file.similarity;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers {@link SemanticSearch}'s local codes copy and its two-stage query, against parquet
 * fixtures written the same way {@code vss-local.py} writes real codes (sign-bit words packed by
 * {@link SemanticSearch#packBits}, int8 rerank vectors scaled by the producer's 0.35 divisor).
 *
 * <p>Uses no S3 and no embedding model: {@code calcite.vss.codes} points at local files and
 * {@code searchVector} takes an already-embedded query, which is why that method is
 * package-visible.
 */
// SemanticSearch caches one DuckDB connection and its local-copy state in statics, and these
// tests steer it through system properties -- both process-wide. The build passes
// junit.jupiter.execution.parallel.mode.default=concurrent (build.gradle.kts), which overrides
// this module's same_thread setting and would otherwise interleave these methods over that shared
// state: a reset from one test closes the connection another is mid-query on, and a property set
// by one decides what another loads.
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class SemanticSearchLocalCodesTest {

  private static final int DIM = 384;
  private static final double I8_SCALE = 0.35;

  @TempDir Path tmp;

  private String priorCodes;
  private String priorLocalDb;

  @BeforeEach void saveProps() {
    priorCodes = System.getProperty("calcite.vss.codes");
    priorLocalDb = System.getProperty("calcite.vss.localDb");
    SemanticSearch.resetForTesting();
  }

  @AfterEach void restoreProps() {
    SemanticSearch.resetForTesting();
    restore("calcite.vss.codes", priorCodes);
    restore("calcite.vss.localDb", priorLocalDb);
  }

  private static void restore(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  /** Deterministic pseudo-random unit vector, so a failure is reproducible. */
  private static double[] vector(Random rnd) {
    double[] v = new double[DIM];
    double norm = 0;
    for (int i = 0; i < DIM; i++) {
      v[i] = rnd.nextGaussian();
      norm += v[i] * v[i];
    }
    norm = Math.sqrt(norm);
    for (int i = 0; i < DIM; i++) {
      v[i] /= norm;
    }
    return v;
  }

  /** Writes {@code n} rows of codes to a parquet file, ids prefixed with {@code idPrefix}, and
   *  returns the vectors it encoded so a test can search for one of them. */
  private List<double[]> writeCodes(Path file, String idPrefix, int n, long seed)
      throws Exception {
    Random rnd = new Random(seed);
    List<double[]> vectors = new ArrayList<double[]>(n);
    StringBuilder values = new StringBuilder();
    for (int r = 0; r < n; r++) {
      double[] v = vector(rnd);
      vectors.add(v);
      long[] w = SemanticSearch.packBits(v);
      if (r > 0) {
        values.append(',');
      }
      values.append("('").append(idPrefix).append(r).append("'");
      for (int i = 0; i < w.length; i++) {
        values.append(',').append(Long.toUnsignedString(w[i])).append("::UBIGINT");
      }
      values.append(",[");
      for (int i = 0; i < DIM; i++) {
        if (i > 0) {
          values.append(',');
        }
        long q = Math.round(v[i] / I8_SCALE * 127.0);
        values.append(Math.max(-127, Math.min(127, q)));
      }
      values.append("]::TINYINT[").append(DIM).append("])");
    }
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t(chunk_id VARCHAR, w0 UBIGINT, w1 UBIGINT, w2 UBIGINT,"
          + " w3 UBIGINT, w4 UBIGINT, w5 UBIGINT, rerank_i8 TINYINT[" + DIM + "])");
      st.execute("INSERT INTO t VALUES " + values);
      st.execute("COPY t TO '" + file.toAbsolutePath() + "' (FORMAT parquet)");
    }
    return vectors;
  }

  private static long localRowCount(String db) throws Exception {
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:" + db);
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery("SELECT count(*) FROM vss_codes")) {
      return rs.next() ? rs.getLong(1) : -1;
    }
  }

  private static String codesArg(Path... files) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < files.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append('\'').append(files[i].toAbsolutePath()).append('\'');
    }
    return sb.append(']').toString();
  }

  /** The query vector is one of the encoded rows, so that row must come back first: this is the
   *  end-to-end check that stage 1's Hamming prefilter and stage 2's cosine rerank still agree
   *  after being split into two passes over the codes. */
  @Test void findsTheExactRowItSearchesFor() throws Exception {
    Path codes = tmp.resolve("codes-a.parquet");
    List<double[]> vectors = writeCodes(codes, "a", 300, 42L);
    String db = tmp.resolve("local.duckdb").toAbsolutePath().toString();
    System.setProperty("calcite.vss.codes", codesArg(codes));
    System.setProperty("calcite.vss.localDb", db);

    List<Object[]> hits = SemanticSearch.searchVector(vectors.get(7), 5);

    assertEquals(5, hits.size(), "should return exactly k hits");
    assertEquals("a7", hits.get(0)[0], "the searched-for row must rank first");
    assertTrue((Double) hits.get(0)[1] > 0.999, "self-match should score ~1.0");
    for (int i = 1; i < hits.size(); i++) {
      assertTrue((Double) hits.get(i - 1)[1] >= (Double) hits.get(i)[1],
          "hits must be ordered by descending score");
    }
  }

  /** The local copy is the point of the property: it must actually be built and hold every row. */
  @Test void materializesTheCodesLocally() throws Exception {
    Path codes = tmp.resolve("codes-a.parquet");
    List<double[]> vectors = writeCodes(codes, "a", 120, 7L);
    String db = tmp.resolve("local.duckdb").toAbsolutePath().toString();
    System.setProperty("calcite.vss.codes", codesArg(codes));
    System.setProperty("calcite.vss.localDb", db);

    SemanticSearch.searchVector(vectors.get(0), 3);
    SemanticSearch.resetForTesting();

    assertTrue(new File(db).isFile(), "the local codes database should have been created");
    assertEquals(120L, localRowCount(db), "every code row should be loaded locally");
  }

  /** A backlog run only ever appends new files, so a second file must load incrementally and the
   *  rows already present must not be duplicated. */
  @Test void addsNewCodesFilesWithoutDuplicating() throws Exception {
    Path a = tmp.resolve("codes-a.parquet");
    Path b = tmp.resolve("codes-b.parquet");
    List<double[]> va = writeCodes(a, "a", 100, 1L);
    writeCodes(b, "b", 60, 2L);
    String db = tmp.resolve("local.duckdb").toAbsolutePath().toString();
    System.setProperty("calcite.vss.localDb", db);

    System.setProperty("calcite.vss.codes", codesArg(a));
    SemanticSearch.searchVector(va.get(0), 3);
    SemanticSearch.resetForTesting();
    assertEquals(100L, localRowCount(db), "first load should hold only the first file's rows");

    System.setProperty("calcite.vss.codes", codesArg(a, b));
    SemanticSearch.searchVector(va.get(0), 3);
    SemanticSearch.resetForTesting();
    assertEquals(160L, localRowCount(db), "the second file's rows should be appended exactly once");
  }

  /** Compaction replaces several files with one merged file that re-states their rows. Appending
   *  that merged file alone would double every row it absorbed, so a non-additive change to the
   *  file set has to reload from scratch. */
  @Test void reloadsFromScratchWhenCompactionRetiresFiles() throws Exception {
    Path a = tmp.resolve("codes-a.parquet");
    Path b = tmp.resolve("codes-b.parquet");
    Path merged = tmp.resolve("codes-compact-1.parquet");
    List<double[]> va = writeCodes(a, "a", 100, 1L);
    writeCodes(b, "b", 60, 2L);
    String db = tmp.resolve("local.duckdb").toAbsolutePath().toString();
    System.setProperty("calcite.vss.localDb", db);

    System.setProperty("calcite.vss.codes", codesArg(a, b));
    SemanticSearch.searchVector(va.get(0), 3);
    SemanticSearch.resetForTesting();
    assertEquals(160L, localRowCount(db));

    // What compaction leaves behind: one file carrying both originals' rows, originals deleted.
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = c.createStatement()) {
      st.execute("COPY (SELECT * FROM read_parquet(['" + a.toAbsolutePath() + "','"
          + b.toAbsolutePath() + "'])) TO '" + merged.toAbsolutePath() + "' (FORMAT parquet)");
    }
    assertTrue(a.toFile().delete() && b.toFile().delete(), "fixture cleanup should succeed");

    System.setProperty("calcite.vss.codes", codesArg(merged));
    SemanticSearch.searchVector(va.get(0), 3);
    SemanticSearch.resetForTesting();
    assertEquals(160L, localRowCount(db), "a compacted set must reload, not double up");
  }

  /** The stage-1 width bounds what stage 2 can rerank, so pinning it to one candidate must yield
   *  exactly one hit however large k is. Proves {@code calcite.vss.prefilter} is still honoured
   *  now that the default is computed from the corpus rather than fixed. */
  @Test void explicitPrefilterWidthCapsTheCandidates() throws Exception {
    Path codes = tmp.resolve("codes-a.parquet");
    List<double[]> vectors = writeCodes(codes, "a", 200, 9L);
    System.setProperty("calcite.vss.codes", codesArg(codes));
    System.setProperty("calcite.vss.localDb", tmp.resolve("local.duckdb").toString());
    String priorPrefilter = System.getProperty("calcite.vss.prefilter");
    System.setProperty("calcite.vss.prefilter", "1");
    try {
      List<Object[]> hits = SemanticSearch.searchVector(vectors.get(3), 5);
      assertEquals(1, hits.size(), "a one-candidate prefilter can yield only one hit");
      assertEquals("a3", hits.get(0)[0], "and it should be the nearest one");
    } finally {
      restore("calcite.vss.prefilter", priorPrefilter);
    }
  }

  /** With the width unpinned, a corpus far smaller than the floor must not be trimmed at all:
   *  every row stays a candidate, so the result matches what exhaustive rerank would give. */
  @Test void defaultWidthKeepsSmallCorporaWhole() throws Exception {
    Path codes = tmp.resolve("codes-a.parquet");
    List<double[]> vectors = writeCodes(codes, "a", 250, 11L);
    System.clearProperty("calcite.vss.prefilter");
    System.setProperty("calcite.vss.codes", codesArg(codes));
    System.setProperty("calcite.vss.localDb", tmp.resolve("local.duckdb").toString());

    List<Object[]> hits = SemanticSearch.searchVector(vectors.get(17), 250);

    assertEquals(250, hits.size(), "every row should survive the prefilter on a small corpus");
    assertEquals("a17", hits.get(0)[0], "the searched-for row must still rank first");
  }

  /** Without the property nothing local is built, and search still works by reading the files
   *  directly -- the fallback that keeps standalone and test use unchanged. */
  @Test void worksWithoutALocalDatabase() throws Exception {
    Path codes = tmp.resolve("codes-a.parquet");
    List<double[]> vectors = writeCodes(codes, "a", 80, 3L);
    System.clearProperty("calcite.vss.localDb");
    System.setProperty("calcite.vss.codes", codesArg(codes));

    List<Object[]> hits = SemanticSearch.searchVector(vectors.get(11), 3);

    assertEquals("a11", hits.get(0)[0], "remote-read path must return the same top hit");
    assertFalse(new File(tmp.resolve("local.duckdb").toString()).exists(),
        "no local database should be created when the property is unset");
  }
}
