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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Path B semantic search: two-stage brute-force over the quantized-code dataset in the lake
 * (48-byte binary sign codes + int8 rerank vectors produced by vss-local.py), exposed as the
 * table function {@code SEMANTIC_SEARCH(query_text, k)}.
 *
 * <p>Stage 1 is a {@code bit_count(xor(...))} Hamming prefilter over the packed 6×uint64 code;
 * stage 2 reranks the survivors by cosine against the int8 vectors. The query is embedded once
 * with the same model as the producer ({@link EmbeddingService}, arctic-embed-xs 384-d) and
 * packed to the identical bit layout, so query and corpus codes are directly comparable. No
 * HNSW, no server — DuckDB streams the codes from object storage (cache_httpfs makes repeat
 * scans fast across sessions).
 *
 * <p>Usage: {@code SELECT * FROM TABLE(SEMANTIC_SEARCH('material weakness in ICFR', 10))}, then
 * join {@code chunk_id} back to {@code vectorized_chunks} for text/metadata.
 *
 * <p>No launcher config is required: S3 access is handed in by the file adapter via
 * {@link #configure} (it reuses the same credentials/endpoint the adapter already resolved), and
 * the codes location defaults to {@link #DEFAULT_CODES}. All of the following are optional
 * overrides (system properties — file/ code must not read the environment):
 * <ul>
 *   <li>{@code calcite.vss.codes} — override the codes parquet glob</li>
 *   <li>{@code calcite.vss.prefilter} — stage-1 Hamming candidate count (default 1000)</li>
 *   <li>{@code calcite.vss.s3.endpoint|region|accessKey|secretKey|useSsl} — S3 fallback for
 *       standalone use when {@link #configure} was not called</li>
 * </ul>
 */
public final class SemanticSearch {

  private static final Logger LOGGER = LoggerFactory.getLogger(SemanticSearch.class);

  private static final int DIM = 384;
  private static final int WORDS = DIM / 64;   // 6 uint64 words per code

  private static volatile Connection duck;

  // Default codes location (the producer's standard output). Overridable via
  // -Dcalcite.vss.codes for non-standard buckets; not required in the normal deployment.
  private static final String DEFAULT_CODES =
      "s3://govdata-parquet-v1/sec/vectorized_chunk_codes/*.parquet";

  // S3 access captured from the file adapter's own resolved config (see configure()), so the
  // query reuses the same credentials/endpoint the adapter already set up — no launcher flags.
  private static volatile String s3Endpoint;   // host:port, no scheme
  private static volatile String s3Region;
  private static volatile String s3AccessKey;
  private static volatile String s3SecretKey;
  private static volatile boolean s3UseSsl;
  private static volatile boolean s3Configured;

  private SemanticSearch() {
  }

  /**
   * Hand SEMANTIC_SEARCH the file adapter's already-resolved S3 config so it needs no separate
   * credentials. Called by {@code DuckDBJdbcSchemaFactory} at schema setup (right where it
   * builds its own {@code duckdb_s3_secret}). System properties {@code calcite.vss.s3.*} remain
   * a fallback for standalone use.
   */
  public static void configure(String endpoint, String region, String accessKey,
      String secretKey, boolean useSsl) {
    s3Endpoint = endpoint;
    s3Region = region;
    s3AccessKey = accessKey;
    s3SecretKey = secretKey;
    s3UseSsl = useSsl;
    s3Configured = accessKey != null && secretKey != null;
  }

  /**
   * Sign-bit code, byte-for-byte identical to the producer's
   * {@code numpy.packbits(v > 0).view(uint64)}: bits are MSB-first within each byte (packbits),
   * and the 8 bytes of each word are little-endian (the x86 uint64 view). Getting this layout
   * wrong silently corrupts every Hamming distance, so it mirrors the producer exactly.
   */
  static long[] packBits(double[] v) {
    long[] w = new long[WORDS];
    for (int e = 0; e < DIM; e++) {
      if (v[e] > 0) {
        int word = e >>> 6;             // e / 64
        int bytePos = (e >>> 3) & 7;    // (e / 8) % 8  — little-endian byte within the word
        int bitInByte = 7 - (e & 7);    // MSB-first within the byte (numpy packbits)
        w[word] |= 1L << (bytePos * 8 + bitInByte);
      }
    }
    return w;
  }

  /** Table function entry point registered as {@code SEMANTIC_SEARCH(query, k)}. */
  public static ScannableTable SEMANTIC_SEARCH(String query, int k) {
    return new CodesResultTable(run(query, k));
  }

  /** The fixed (chunk_id, score) result of one semantic search, as a scannable table. */
  private static final class CodesResultTable extends AbstractTable implements ScannableTable {
    private final List<Object[]> rows;

    CodesResultTable(List<Object[]> rows) {
      this.rows = rows;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("chunk_id", SqlTypeName.VARCHAR)
          .add("score", SqlTypeName.DOUBLE)
          .build();
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(rows);
    }

    @Override public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }
  }

  private static List<Object[]> run(String query, int k) {
    double[] v = EmbeddingService.get().embed(query);
    if (v.length != DIM) {
      throw new IllegalStateException(
          "embedding dim " + v.length + " != " + DIM + " — model/producer mismatch");
    }
    return searchVector(v, k);
  }

  /** Two-stage search over the codes for an already-embedded query vector. Package-visible so
   *  the query path (packing + Hamming prefilter + int8 rerank) can be exercised without
   *  standing up the embedding server. */
  static List<Object[]> searchVector(double[] v, int k) {
    try {
      long[] w = packBits(v);
      String codes = System.getProperty("calcite.vss.codes", DEFAULT_CODES);
      int prefilter = Integer.getInteger("calcite.vss.prefilter", 1000);

      StringBuilder ham = new StringBuilder();
      for (int i = 0; i < WORDS; i++) {
        if (i > 0) {
          ham.append('+');
        }
        // ::INT so the 6 popcounts (each 0..64) don't overflow TINYINT when summed.
        ham.append("bit_count(xor(w").append(i).append(',')
            .append(Long.toUnsignedString(w[i])).append("::UBIGINT))::INT");
      }
      StringBuilder qv = new StringBuilder(DIM * 10);
      qv.append('[');
      for (int i = 0; i < DIM; i++) {
        if (i > 0) {
          qv.append(',');
        }
        qv.append(v[i]);
      }
      qv.append(']');

      String sql = "WITH prefilter AS ("
          + "SELECT chunk_id, rerank_i8, (" + ham + ") AS hd "
          + "FROM read_parquet('" + codes + "') ORDER BY hd LIMIT " + prefilter + ") "
          + "SELECT chunk_id, "
          + "list_cosine_similarity(" + qv + "::DOUBLE[], rerank_i8::DOUBLE[]) AS score "
          + "FROM prefilter ORDER BY score DESC LIMIT " + Math.max(1, k);

      List<Object[]> out = new ArrayList<>();
      try (Statement st = connection().createStatement();
           ResultSet rs = st.executeQuery(sql)) {
        while (rs.next()) {
          out.add(new Object[]{rs.getString(1), rs.getDouble(2)});
        }
      }
      LOGGER.debug("SEMANTIC_SEARCH returned {} rows (prefilter {})", out.size(), prefilter);
      return out;
    } catch (Exception e) {
      throw new RuntimeException("SEMANTIC_SEARCH failed: " + e.getMessage(), e);
    }
  }

  /**
   * Lazily-created, cached DuckDB connection for reading the codes from object storage. S3 access
   * comes from the adapter-supplied config (see {@link #configure}); {@code calcite.vss.s3.*}
   * system properties are a fallback so the query can also run standalone.
   */
  private static Connection connection() throws SQLException {
    Connection c = duck;
    if (c != null && !c.isClosed()) {
      return c;
    }
    synchronized (SemanticSearch.class) {
      if (duck != null && !duck.isClosed()) {
        return duck;
      }
      String region = firstNonEmpty(s3Region, System.getProperty("calcite.vss.s3.region"),
          "us-east-1");
      String endpoint = firstNonEmpty(s3Endpoint, System.getProperty("calcite.vss.s3.endpoint"));
      String key = firstNonEmpty(s3AccessKey, System.getProperty("calcite.vss.s3.accessKey"));
      String secret = firstNonEmpty(s3SecretKey, System.getProperty("calcite.vss.s3.secretKey"));
      boolean useSsl = s3Configured ? s3UseSsl
          : Boolean.parseBoolean(System.getProperty("calcite.vss.s3.useSsl", "false"));

      Connection nc = DriverManager.getConnection("jdbc:duckdb:");
      try (Statement st = nc.createStatement()) {
        st.execute("INSTALL httpfs; LOAD httpfs");
        st.execute("SET s3_region='" + esc(region) + "'");
        if (endpoint != null) {
          st.execute("SET s3_endpoint='" + esc(endpoint) + "'");
        }
        if (key != null) {
          st.execute("SET s3_access_key_id='" + esc(key) + "'");
        }
        if (secret != null) {
          st.execute("SET s3_secret_access_key='" + esc(secret) + "'");
        }
        st.execute("SET s3_url_style='path'");
        st.execute("SET s3_use_ssl=" + useSsl);
      }
      duck = nc;
      return nc;
    }
  }

  private static String firstNonEmpty(String... vals) {
    for (String v : vals) {
      if (v != null && !v.isEmpty()) {
        return v;
      }
    }
    return null;
  }

  private static String esc(String s) {
    return s.replace("'", "''");
  }
}
