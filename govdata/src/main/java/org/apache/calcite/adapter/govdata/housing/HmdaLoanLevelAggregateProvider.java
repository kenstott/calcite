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
package org.apache.calcite.adapter.govdata.housing;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code housing.hmda_lending_by_tract} — real census-tract-grain HMDA lending
 * activity, aggregated from CFPB's loan-level "Nationwide Data Subset" export.
 *
 * <p>{@code housing.hmda_loans} (the sibling table) uses the HMDA Data Browser's {@code
 * aggregations} endpoint, which accepts at most two breakdown dimensions per request — its own
 * comment already documents that a {@code counties=} filter is accepted and echoed back by that
 * endpoint but never actually applied server-side (confirmed live 2026-09-11: a real,
 * near-empty-population county returned the entire state's total). Tract/county grain is
 * structurally unreachable through that endpoint; it requires the loan-level data instead.
 *
 * <p>This provider downloads the year's full loan-level CSV from the {@code nationwide/csv}
 * endpoint — confirmed live to require an explicit {@code actions_taken} filter alongside {@code
 * years}, or the API silently returns a small, undocumented partial subset instead of an error
 * (verified: omitting it returned ~261K rows for 2023; passing all 8 action codes returned the
 * real ~4.2GB/~1.11M-row file) — through the pipeline's {@link RawCache} rather than a direct
 * connection. This matters here specifically, not just as a general courtesy: {@code freshness:
 * type: hash} on this table's YAML makes {@code EtlPipeline} call this provider's {@code fetch}
 * <em>twice</em> per batch — once to hash the content, once to write — relying on the raw cache to
 * make the second call free (its own comment: "the raw HTTP response is cached after the first
 * pull, so this re-parses from the warm cache"). Bypassing the cache with a direct download, as an
 * earlier version of this class did, made the second call re-download the full multi-GB file from
 * CFPB's server (confirmed live: a 23-minute, 4.2GB download, twice, for one batch). Routing
 * through {@code rawCache.openStream} fixes that call for free and also means a later reprocess of
 * an already-fetched year does not need to hit CFPB's server again either.
 *
 * <p>The cached stream is copied to a local temp file (DuckDB's {@code read_csv_auto} needs a real
 * file path), aggregated to tract grain via an embedded DuckDB query — streamed off disk, never
 * loaded into JVM heap as a row list, matching the same temp-file-plus-DuckDB pattern {@code
 * IcebergMaterializationWriter.transformRowsWithDuckDb} already uses for batch expression
 * evaluation — and the temp file is deleted immediately after. The aggregated result (one row per
 * census tract) is small enough to hold in memory and return directly.
 */
public class HmdaLoanLevelAggregateProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(HmdaLoanLevelAggregateProvider.class);
  private static final int MAX_ATTEMPTS = 3;

  /**
   * One row per 11-digit census tract. The census tract FIPS encodes its own county, so
   * {@code county_fips} comes from the tract rather than from the loan record's {@code county_code}
   * (which the source reports as the literal {@code NA} when absent). {@code state_code} is the
   * state on records whose reported county agrees with the tract's county; it is null when no
   * record of the tract carries a matching county. Records whose {@code census_tract} is not an
   * 11-digit code (the source's {@code NA} for a property with no tract) belong to no tract and
   * are excluded.
   */
  private static final String AGGREGATE_SQL =
      "SELECT "
      + "  MODE(state_code) FILTER (WHERE county_code = LEFT(census_tract, 5) "
      + "    AND state_code != 'NA') AS state_code, "
      + "  LEFT(census_tract, 5) AS county_fips, "
      + "  census_tract, "
      + "  COUNT(*) AS application_count, "
      + "  COUNT(*) FILTER (WHERE action_taken = '1') AS origination_count, "
      + "  COUNT(*) FILTER (WHERE action_taken = '3') AS denial_count, "
      + "  SUM(TRY_CAST(loan_amount AS DOUBLE)) FILTER (WHERE action_taken = '1') "
      + "    AS origination_total_amount, "
      + "  MEDIAN(TRY_CAST(loan_amount AS DOUBLE)) FILTER (WHERE action_taken = '1') "
      + "    AS origination_median_amount, "
      + "  AVG(TRY_CAST(interest_rate AS DOUBLE)) FILTER (WHERE action_taken = '1') "
      + "    AS avg_interest_rate, "
      + "  MAX(TRY_CAST(tract_population AS DOUBLE)) AS tract_population, "
      + "  MAX(TRY_CAST(tract_minority_population_percent AS DOUBLE)) "
      + "    AS tract_minority_population_percent, "
      + "  MAX(TRY_CAST(tract_to_msa_income_percentage AS DOUBLE)) "
      + "    AS tract_to_msa_income_percentage "
      + "FROM read_csv_auto(?, ALL_VARCHAR=TRUE) "
      + "WHERE regexp_full_match(census_tract, '[0-9]{11}') "
      + "GROUP BY census_tract";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    // effective_year is the HMDA data year (publish year - dataLag); the pipeline's own
    // `year` is the publish year and is not a year FFIEC has a file for until mid-following-year.
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      throw new IOException("HMDA loan-level aggregate: no effective_year in dimension variables "
          + variables);
    }
    String url = "https://ffiec.cfpb.gov/v2/data-browser-api/view/nationwide/csv?years=" + year
        + "&actions_taken=1,2,3,4,5,6,7,8";

    File tempCsv = File.createTempFile("hmda-" + year + "-", ".csv");
    try {
      LOGGER.info("HMDA loan-level aggregate {}: fetching {} (through raw cache)", year, url);
      copyThroughCache(rawCache, url, tempCsv);
      LOGGER.info("HMDA loan-level aggregate {}: {} MB, aggregating to tract grain",
          year, tempCsv.length() / (1024 * 1024));
      List<Map<String, Object>> rows = aggregate(tempCsv, year);
      LOGGER.info("HMDA loan-level aggregate {}: {} tracts", year, rows.size());
      return rows.iterator();
    } catch (SQLException e) {
      throw new IOException("HMDA loan-level aggregate " + year + ": DuckDB aggregation failed", e);
    } finally {
      if (!tempCsv.delete()) {
        LOGGER.warn("HMDA loan-level aggregate {}: failed to delete temp file {}", year, tempCsv);
      }
    }
  }

  /**
   * Reads {@code url} through {@code rawCache} (cache hit serves from storage with no network
   * call; a miss downloads, stores, and serves) and copies the bytes to {@code destFile}. Retries
   * the whole call on failure — a multi-GB download run over many minutes is more exposed to a
   * transient network blip than a small one, and a failed attempt leaves no cache entry to
   * conflict with a clean retry.
   */
  private void copyThroughCache(RawCache rawCache, String url, File destFile) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try (InputStream in = rawCache.openStream(url, () -> openConnectionStream(url));
           FileOutputStream out = new FileOutputStream(destFile)) {
        byte[] buf = new byte[1 << 20];
        int n;
        while ((n = in.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
        return;
      } catch (IOException e) {
        last = e;
        LOGGER.warn("HMDA loan-level aggregate: attempt {}/{} failed for {}: {}",
            attempt, MAX_ATTEMPTS, url, e.getMessage());
      }
    }
    throw last != null ? last : new IOException("HMDA loan-level aggregate: failed to fetch " + url);
  }

  private static InputStream openConnectionStream(String url) throws IOException {
    return ResumableHttpStream.open(url);
  }

  /**
   * Streams a file of known length over HTTP and, when the connection drops or stalls part-way,
   * reopens it with a {@code Range} request from the byte where it stopped rather than restarting.
   * On JDK 21 {@code HttpURLConnection} reports a connection dropped mid-body as an ordinary
   * end-of-stream, so the byte count against {@code Content-Length} is what tells a whole file
   * from a partial one — and {@code RawCache} commits whatever a stream delivers before it ends
   * cleanly. The stream only ends once exactly {@code Content-Length} bytes have been delivered;
   * a server that will not honour {@code Range}, or that keeps failing without progress, raises
   * an {@link IOException} instead.
   */
  static final class ResumableHttpStream extends InputStream {
    private static final int MAX_STALLED_RESUMES = 5;
    private static final int CONNECT_TIMEOUT_MS = 60_000;
    private static final int IDLE_TIMEOUT_MS = 600_000;
    private static final long RESUME_BACKOFF_MS = 5_000L;

    private final String url;
    private final long expectedBytes;
    private final long backoffMs;
    private InputStream current;
    private long position;
    private int stalledResumes;

    private ResumableHttpStream(String url, long expectedBytes, InputStream current,
        long backoffMs) {
      this.url = url;
      this.expectedBytes = expectedBytes;
      this.current = current;
      this.backoffMs = backoffMs;
    }

    static ResumableHttpStream open(String url) throws IOException {
      return open(url, RESUME_BACKOFF_MS);
    }

    static ResumableHttpStream open(String url, long backoffMs) throws IOException {
      HttpURLConnection conn = connect(url, 0);
      long expected = conn.getContentLengthLong();
      if (expected < 0) {
        conn.disconnect();
        throw new IOException("HMDA download from " + url + " returned no Content-Length; "
            + "cannot verify the file arrived whole");
      }
      return new ResumableHttpStream(url, expected, conn.getInputStream(), backoffMs);
    }

    private static HttpURLConnection connect(String url, long from) throws IOException {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(IDLE_TIMEOUT_MS);
      conn.setInstanceFollowRedirects(true);
      conn.setRequestProperty("User-Agent", "Apache-Calcite-GovData/1.0");
      if (from > 0) {
        conn.setRequestProperty("Range", "bytes=" + from + "-");
      }
      return conn;
    }

    @Override public int read() throws IOException {
      byte[] one = new byte[1];
      int n = read(one, 0, 1);
      return n == -1 ? -1 : one[0] & 0xff;
    }

    @Override public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      while (position < expectedBytes) {
        int want = (int) Math.min(len, expectedBytes - position);
        int n;
        try {
          n = current.read(b, off, want);
        } catch (IOException e) {
          resume(e);
          continue;
        }
        if (n < 0) {
          resume(new IOException("HMDA download ended after " + position + " of " + expectedBytes
              + " bytes"));
          continue;
        }
        position += n;
        stalledResumes = 0;
        return n;
      }
      return -1;
    }

    private void resume(IOException cause) throws IOException {
      closeQuietly();
      while (true) {
        if (++stalledResumes > MAX_STALLED_RESUMES) {
          throw new IOException("HMDA download stalled at " + position + " of " + expectedBytes
              + " bytes after " + MAX_STALLED_RESUMES + " resume attempts", cause);
        }
        LOGGER.warn("HMDA download interrupted at {} of {} bytes ({}); resuming (attempt {}/{})",
            position, expectedBytes, cause.getMessage(), stalledResumes, MAX_STALLED_RESUMES);
        try {
          Thread.sleep(backoffMs * stalledResumes);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("HMDA download interrupted while resuming");
        }
        try {
          current = openFrom(position);
          return;
        } catch (IOException e) {
          cause = e;
        }
      }
    }

    private InputStream openFrom(long from) throws IOException {
      HttpURLConnection conn = connect(url, from);
      int code = conn.getResponseCode();
      String range = conn.getHeaderField("Content-Range");
      String expectedRange = "bytes " + from + "-" + (expectedBytes - 1) + "/" + expectedBytes;
      if (code != HttpURLConnection.HTTP_PARTIAL || !expectedRange.equals(range)) {
        conn.disconnect();
        throw new IOException("HMDA resume from byte " + from + " rejected: HTTP " + code
            + ", Content-Range " + range + " (wanted " + expectedRange + ")");
      }
      return conn.getInputStream();
    }

    @Override public void close() throws IOException {
      if (current != null) {
        current.close();
      }
    }

    private void closeQuietly() {
      try {
        current.close();
      } catch (IOException e) {
        LOGGER.debug("HMDA download: error closing interrupted connection: {}", e.getMessage());
      }
    }
  }

  List<Map<String, Object>> aggregate(File csvFile, String year) throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement init = conn.createStatement()) {
      init.execute("PRAGMA memory_limit='4GB'");
      java.sql.PreparedStatement ps = conn.prepareStatement(AGGREGATE_SQL);
      ps.setString(1, csvFile.getAbsolutePath());
      try (ResultSet rs = ps.executeQuery()) {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        while (rs.next()) {
          Map<String, Object> row = new LinkedHashMap<String, Object>();
          for (int i = 1; i <= cols; i++) {
            row.put(meta.getColumnLabel(i), rs.getObject(i));
          }
          row.put("year", Integer.valueOf(year));
          rows.add(row);
        }
      }
    }
    return rows;
  }
}
