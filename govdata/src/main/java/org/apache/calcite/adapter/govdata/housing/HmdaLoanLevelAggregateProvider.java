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
import java.net.HttpURLConnection;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
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
 * state/county/tract) is small enough to hold in memory and return directly.
 */
public class HmdaLoanLevelAggregateProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(HmdaLoanLevelAggregateProvider.class);
  private static final int MAX_ATTEMPTS = 3;

  private static final String AGGREGATE_SQL =
      "SELECT "
      + "  state_code, "
      + "  county_code AS county_fips, "
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
      + "WHERE census_tract IS NOT NULL AND census_tract != '' "
      + "  AND state_code IS NOT NULL AND state_code != '' "
      + "GROUP BY state_code, county_code, census_tract";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String year = variables.get("year");
    if (year == null || year.isEmpty()) {
      LOGGER.warn("HMDA loan-level aggregate: no year in dimension variables {}", variables);
      return Collections.emptyIterator();
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
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(60_000);
    conn.setReadTimeout(600_000);
    conn.setInstanceFollowRedirects(true);
    conn.setRequestProperty("User-Agent", "Apache-Calcite-GovData/1.0");
    return conn.getInputStream();
  }

  private List<Map<String, Object>> aggregate(File csvFile, String year) throws SQLException {
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
