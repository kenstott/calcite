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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Path B semantic search: two-stage brute-force over the quantized-code dataset in the lake
 * (48-byte binary sign codes + int8 rerank vectors produced by vss-local.py), exposed as the
 * table function {@code SEMANTIC_SEARCH(query_text, k)}.
 *
 * <p>Stage 1 is a {@code bit_count(xor(...))} Hamming prefilter over the packed 6×uint64 code;
 * stage 2 reranks the survivors by cosine against the int8 vectors. The two stages are separate
 * scans on purpose: {@code rerank_i8} is ~85% of the dataset's bytes but is only ever needed for
 * the handful of prefilter survivors, so stage 1 projects the sign words alone and the columnar
 * read skips the rerank vectors entirely. The query is embedded once with the same model as the
 * producer ({@link EmbeddingService}, arctic-embed-xs 384-d) and packed to the identical bit
 * layout, so query and corpus codes are directly comparable. No HNSW, no server.
 *
 * <p>The codes are searched LOCALLY, not over the network. They are immutable once written, so
 * when {@code calcite.vss.localDb} names a DuckDB file they are loaded into it on first touch and
 * every query afterwards reads that copy; later runs top it up with whatever files have appeared
 * since. Searching object storage directly would re-read the entire dataset on every single
 * query — tolerable on a LAN, and a fresh multi-hundred-megabyte transfer per question for a
 * client reading from a remote bucket. Without the property the codes are read remotely per
 * query, which keeps standalone and test use working.
 *
 * <p>Usage: {@code SELECT * FROM TABLE(SEMANTIC_SEARCH('material weakness in ICFR', 10))}, then
 * join {@code chunk_id} back to {@code vectorized_chunks} for text/metadata.
 *
 * <p>No launcher config is required: S3 access is handed in by the file adapter via
 * {@link #configure} (it reuses the same credentials/endpoint the adapter already resolved), and
 * the codes locations default to every schema in {@link #DEFAULT_SOURCE_SCHEMAS} — each source
 * schema's codes live under its own bucket-rooted prefix (see {@link #defaultCodesGlobs}), so
 * search spans all of them, not just {@code sec}. All of the following are optional overrides
 * (system properties — file/ code must not read the environment):
 * <ul>
 *   <li>{@code calcite.vss.localDb} — path to the DuckDB file holding the local copy of the
 *       codes. Set it to search locally; leave it unset to read object storage per query</li>
 *   <li>{@code calcite.vss.codes} — override the {@code read_parquet(...)} argument entirely:
 *       either a single quoted glob (one schema) or a bracketed list of quoted globs (several)</li>
 *   <li>{@code calcite.vss.prefilter} — pin the stage-1 Hamming candidate count. Left unset it
 *       scales with the corpus (see {@link #prefilterWidth}), which is what keeps recall stable
 *       as the corpus grows</li>
 *   <li>{@code calcite.vss.s3.endpoint|region|accessKey|secretKey|useSsl} — S3 fallback for
 *       standalone use when {@link #configure} was not called</li>
 * </ul>
 */
public final class SemanticSearch {

  private static final Logger LOGGER = LoggerFactory.getLogger(SemanticSearch.class);

  private static final int DIM = 384;
  private static final int WORDS = DIM / 64;   // 6 uint64 words per code

  // Prefilter sizing -- see prefilterWidth(). The divisor pins the candidates-to-corpus ratio at
  // the measured 50,000-in-3.19M operating point (80% recall@10); the floor keeps small corpora
  // from being trimmed for nothing.
  //
  // The ceiling exists only to bound stage-2 rerank, and it is set from the measured cost of
  // widening rather than picked defensively: on a 3.19M corpus, query time ran 0.386s at 50,000
  // candidates and 1.168s at 1,000,000 -- about 0.82us per additional candidate, against a scan
  // that dominates until the width is enormous. At a million it therefore binds no earlier than
  // a 64M-row corpus and costs under a second of rerank when it does. A tighter ceiling is worse
  // than it looks: it silently reintroduces the fixed-width problem this sizing exists to fix,
  // just at a higher row count, and recall decays with no signal when it does.
  private static final int FRACTION_DIVISOR = 64;
  private static final int MIN_PREFILTER = 50_000;
  private static final int MAX_PREFILTER = 1_000_000;

  /** Cached {@code count(*)} of the codes, so prefilter sizing costs one query per process. */
  private static volatile long corpusSize = -1;

  private static volatile Connection duck;

  // The local copy of the codes and the file list it was built from. Searching object storage on
  // every call re-reads the whole dataset per query -- the codes are immutable once written, so
  // they are loaded once into a persistent DuckDB under calcite.vss.localDb and searched there.
  private static final String LOCAL_TABLE = "vss_codes";
  private static final String LOCAL_SOURCE_TABLE = "vss_codes_source";

  /** True once {@link #LOCAL_TABLE} is populated and searches should read it instead of S3. */
  private static volatile boolean localReady;

  // Source schemas the routine embed step produces codes for. This is the consumer half of
  // vss-local.py's CODES_SOURCE_SCHEMAS and must list exactly what that one lists: a schema
  // missing here is silently unsearchable no matter how many codes exist for it, because the
  // globs built below are the only places SEMANTIC_SEARCH ever looks. Scanning a partition that
  // turns out to be empty is harmless by comparison -- the prefilter simply finds nothing there.
  private static final List<String> DEFAULT_SOURCE_SCHEMAS =
      Collections.unmodifiableList(Arrays.asList("sec", "ref", "fedregister", "cyber_threat",
          "transport", "patents", "disasters", "geo", "officials", "health"));

  // Each source schema's codes live under its OWN bucket-rooted prefix -- e.g.
  // sec/vectorized_chunk_codes/source_schema=sec vs. ref/vectorized_chunk_codes/
  // source_schema=ref (see vss-local.py's codes_dataset_for()) -- not a shared parent
  // directory, so one glob cannot span multiple schemas.
  private static List<String> defaultCodesGlobs() {
    List<String> globs = new ArrayList<String>(DEFAULT_SOURCE_SCHEMAS.size() * 2);
    for (String schema : DEFAULT_SOURCE_SCHEMAS) {
      String dataset = "s3://govdata-parquet-v1/" + schema + "/vectorized_chunk_codes/"
          + "source_schema=" + schema;
      // Both shapes a codes dataset holds: the flat tail of recent embed flushes, and the
      // centroid-partitioned set compaction folds them into (vss-local.py's cmd_compact).
      // Reading only the first would silently lose everything already compacted.
      globs.add(dataset + "/*.parquet");
      globs.add(dataset + "/ivf/**/*.parquet");
    }
    return globs;
  }

  /** The globs actually in force: the {@code calcite.vss.codes} override parsed back into
   *  individual patterns when set, else {@link #defaultCodesGlobs}. The override is documented as
   *  a whole {@code read_parquet} argument, so it arrives either as one quoted glob or as a
   *  bracketed list of them; both forms are unwrapped here because the local load needs to see
   *  one file set per pattern, not an opaque argument string. */
  private static List<String> codesGlobs() {
    String override = System.getProperty("calcite.vss.codes");
    if (override == null || override.trim().isEmpty()) {
      return defaultCodesGlobs();
    }
    String s = override.trim();
    if (s.startsWith("[")) {
      s = s.substring(1, s.endsWith("]") ? s.length() - 1 : s.length());
    }
    List<String> globs = new ArrayList<String>();
    for (String part : s.split(",")) {
      String p = part.trim();
      if (p.length() >= 2 && p.charAt(0) == '\'' && p.charAt(p.length() - 1) == '\'') {
        p = p.substring(1, p.length() - 1);
      }
      if (!p.isEmpty()) {
        globs.add(p);
      }
    }
    return globs.isEmpty() ? defaultCodesGlobs() : globs;
  }

  /** A {@code read_parquet(...)} call over every glob in play.
   *
   * <p>{@code union_by_name} is required, not cosmetic: compaction encodes {@code centroid} into
   * the directory path, so a partitioned file does not carry that column while a flat tail file
   * does. Matching positionally across the two shapes fails on the differing column sets. */
  private static String readCodes(List<String> globs) {
    StringBuilder sb = new StringBuilder("read_parquet([");
    for (int i = 0; i < globs.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append('\'').append(globs.get(i)).append('\'');
    }
    return sb.append("], union_by_name=true)").toString();
  }

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

  /** Drops the cached connection so the next search re-reads {@code calcite.vss.*} and reloads
   *  the local codes. Exists for tests, which need several configurations in one JVM; the
   *  connection is otherwise deliberately process-lifetime, since reopening it would discard the
   *  local copy's warm state on every query. */
  static void resetForTesting() {
    synchronized (SemanticSearch.class) {
      Connection c = duck;
      duck = null;
      localReady = false;
      corpusSize = -1;
      if (c != null) {
        try {
          c.close();
        } catch (SQLException e) {
          LOGGER.debug("closing the cached DuckDB connection failed: {}", e.getMessage());
        }
      }
    }
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

  /**
   * How many Hamming candidates stage 2 reranks, as a share of the corpus rather than a constant.
   *
   * <p>Recall depends on the ratio of candidates to corpus, not on the candidate count, so a fixed
   * width silently returns fewer and fewer true neighbours as the corpus grows — and says nothing
   * about it, because the query still returns a full set of plausible-looking hits. Measured on
   * 3.19M codes with out-of-corpus queries (cosine 0.72-0.76 to their nearest stored chunk, which
   * is what a text query looks like), recall@10 against exhaustive cosine ran 32% at 1,000
   * candidates, 62% at 20,000 and 80% at 50,000 — while query time stayed between 0.18s and 0.26s
   * throughout, because the cost is the scan, not the width. {@link #FRACTION_DIVISOR} fixes the
   * share at that 50,000-in-3.19M operating point so recall holds as the corpus grows.
   *
   * <p>The floor matters for small corpora (a width below the corpus size would discard neighbours
   * for no reason) and the ceiling bounds stage-2 work on a very large one.
   * {@code calcite.vss.prefilter} still pins an explicit width when one is wanted.
   */
  private static int prefilterWidth(Connection c, String src) {
    Integer explicit = Integer.getInteger("calcite.vss.prefilter");
    if (explicit != null) {
      return Math.max(1, explicit.intValue());
    }
    long n = corpusSize;
    if (n < 0) {
      n = 0;
      try (Statement st = c.createStatement();
           ResultSet rs = st.executeQuery("SELECT count(*) FROM " + src)) {
        if (rs.next()) {
          n = rs.getLong(1);
        }
      } catch (SQLException e) {
        LOGGER.debug("corpus size unavailable, using the floor prefilter: {}", e.getMessage());
      }
      corpusSize = n;
    }
    long scaled = n / FRACTION_DIVISOR;
    return (int) Math.max(MIN_PREFILTER, Math.min(MAX_PREFILTER, scaled));
  }

  /** Two-stage search over the codes for an already-embedded query vector. Package-visible so
   *  the query path (packing + Hamming prefilter + int8 rerank) can be exercised without
   *  standing up the embedding server. */
  static List<Object[]> searchVector(double[] v, int k) {
    try {
      long[] w = packBits(v);
      Connection c = connection();
      String src = localReady ? LOCAL_TABLE : readCodes(codesGlobs());
      int prefilter = prefilterWidth(c, src);

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

      // Two passes over the codes, deliberately. rerank_i8 is 384 bytes a row and ~85% of the
      // dataset's bytes, but only the `prefilter` survivors are ever scored -- so stage 1
      // projects the packed sign words and chunk_id ONLY, letting the columnar read skip the
      // rerank vectors entirely, and stage 2 fetches them back for the survivors alone.
      // Carrying rerank_i8 through the stage-1 top-N instead (one pass, which reads and sorts
      // the whole column) measured 3.9s against 0.19s+1.2s remote, and 0.46s against 0.19s
      // local, on a 3.2M-row corpus.
      String sql = "WITH prefilter AS ("
          + "SELECT chunk_id, (" + ham + ") AS hd "
          + "FROM " + src + " ORDER BY hd LIMIT " + prefilter + ") "
          + "SELECT c.chunk_id, "
          + "list_cosine_similarity(" + qv + "::DOUBLE[], c.rerank_i8::DOUBLE[]) AS score "
          + "FROM " + src + " c SEMI JOIN prefilter p ON p.chunk_id = c.chunk_id "
          + "ORDER BY score DESC LIMIT " + Math.max(1, k);

      List<Object[]> out = new ArrayList<>();
      try (Statement st = c.createStatement();
           ResultSet rs = st.executeQuery(sql)) {
        while (rs.next()) {
          out.add(new Object[]{rs.getString(1), rs.getDouble(2)});
        }
      }
      LOGGER.debug("SEMANTIC_SEARCH returned {} rows (prefilter {}, source {})",
          out.size(), prefilter, localReady ? "local" : "remote");
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

      // A file-backed DuckDB when calcite.vss.localDb names one, so the loaded codes survive the
      // process; in-memory otherwise, which keeps standalone and test use working unchanged.
      String localDb = System.getProperty("calcite.vss.localDb", "").trim();
      Connection nc = DriverManager.getConnection(
          localDb.isEmpty() ? "jdbc:duckdb:" : "jdbc:duckdb:" + localDb);
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
      if (!localDb.isEmpty()) {
        loadLocalCodes(nc);
      }
      duck = nc;
      return nc;
    }
  }

  /**
   * Brings the local copy of the codes level with object storage, and flips {@link #localReady}
   * once it can be searched.
   *
   * <p>Codes files are immutable once written — a backlog run appends new ones, and compaction
   * replaces a set of them with one merged file — so the file list fully describes what the local
   * copy holds. A purely additive change loads incrementally. Anything else means compaction has
   * retired files the local copy still carries, and that is a full reload rather than an append:
   * the merged file re-states every row it absorbed, so adding it alone would duplicate them.
   *
   * <p>Failure here is not fatal. The local copy is an optimisation over reading object storage
   * directly, so a load that cannot complete leaves {@link #localReady} false and searches fall
   * back to scanning S3 — slower, and still correct.
   */
  private static void loadLocalCodes(Connection c) {
    List<String> globs = codesGlobs();
    try (Statement st = c.createStatement()) {
      st.execute("CREATE TABLE IF NOT EXISTS " + LOCAL_SOURCE_TABLE
          + "(file VARCHAR PRIMARY KEY)");
      StringBuilder remote = new StringBuilder();
      for (int i = 0; i < globs.size(); i++) {
        if (i > 0) {
          remote.append(" UNION ALL ");
        }
        remote.append("SELECT file FROM glob('").append(esc(globs.get(i))).append("')");
      }
      st.execute("CREATE OR REPLACE TEMP TABLE _vss_remote AS " + remote);

      boolean haveTable;
      try (ResultSet rs = st.executeQuery("SELECT count(*) FROM duckdb_tables() WHERE table_name="
          + "'" + LOCAL_TABLE + "'")) {
        haveTable = rs.next() && rs.getLong(1) > 0;
      }
      long retired;
      try (ResultSet rs = st.executeQuery("SELECT count(*) FROM " + LOCAL_SOURCE_TABLE
          + " s WHERE s.file NOT IN (SELECT file FROM _vss_remote)")) {
        retired = rs.next() ? rs.getLong(1) : 0L;
      }

      if (!haveTable || retired > 0) {
        st.execute("DROP TABLE IF EXISTS " + LOCAL_TABLE);
        st.execute("DELETE FROM " + LOCAL_SOURCE_TABLE);
        st.execute("CREATE TABLE " + LOCAL_TABLE + " AS SELECT chunk_id, w0, w1, w2, w3, w4, w5,"
            + " rerank_i8 FROM " + readCodes(globs));
        st.execute("INSERT INTO " + LOCAL_SOURCE_TABLE + " SELECT file FROM _vss_remote");
        LOGGER.info("SEMANTIC_SEARCH loaded the codes locally ({} files)", globs.size());
      } else {
        List<String> missing = new ArrayList<String>();
        try (ResultSet rs = st.executeQuery("SELECT file FROM _vss_remote WHERE file NOT IN "
            + "(SELECT file FROM " + LOCAL_SOURCE_TABLE + ")")) {
          while (rs.next()) {
            missing.add(rs.getString(1));
          }
        }
        if (!missing.isEmpty()) {
          st.execute("INSERT INTO " + LOCAL_TABLE + " SELECT chunk_id, w0, w1, w2, w3, w4, w5,"
              + " rerank_i8 FROM " + readCodes(missing));
          st.execute("INSERT INTO " + LOCAL_SOURCE_TABLE + " SELECT file FROM _vss_remote "
              + "WHERE file NOT IN (SELECT file FROM " + LOCAL_SOURCE_TABLE + ")");
          LOGGER.info("SEMANTIC_SEARCH added {} new codes files locally", missing.size());
        }
      }
      localReady = true;
    } catch (Exception e) {
      localReady = false;
      LOGGER.warn("SEMANTIC_SEARCH could not load the codes locally; falling back to reading "
          + "object storage on every query: {}", e.getMessage());
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
