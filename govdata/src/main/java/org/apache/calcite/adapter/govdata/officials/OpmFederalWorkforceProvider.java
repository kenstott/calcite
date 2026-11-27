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
package org.apache.calcite.adapter.govdata.officials;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.govdata.housing.HmdaLoanLevelAggregateProvider.ResumableHttpStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
 * Federal civilian headcount by month, agency, sub-agency and duty-station state, from OPM's
 * Federal Workforce Data (data.opm.gov, keyless public API).
 *
 * <p>The list endpoint {@code /api/v1/files/employment?current=true&year=Y} names the current
 * month files; each is a ~50 MB parquet of the EHRI Status snapshot (one row per employee-record
 * cell, {@code count} = 1, ~2M rows) as of the last day of the month. For every month of the
 * pipeline's {@code year} dimension the file is fetched through the raw cache, copied to a temp
 * file and aggregated by an embedded DuckDB query to ~8.6k rows, so the 2M-row cube is never held
 * in heap. A year for which OPM lists no file yields no rows.
 *
 * <p>OPM redacts the duty-station state and country on sensitive records under its data release
 * policy; the literal {@code REDACTED} is kept as published, not mapped to null.
 */
public class OpmFederalWorkforceProvider implements CachingDataProvider {

  /** The URL names the release version, which is looked up uncached on every fetch. */
  @Override public boolean isCacheKeyedByRevision() {
    return true;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(OpmFederalWorkforceProvider.class);
  private static final String BASE = "https://data.opm.gov/api/v1/files/employment";
  private static final int MAX_ATTEMPTS = 3;
  private static final String LABEL = "OPM federal workforce";
  private static final long BACKOFF_MS = 2000L;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * OPM text cells can carry stray whitespace (a leading tab on one "UNITED STATES" cell split a
   * duty-station cell in two), so every text column is trimmed before grouping.
   */
  private static String t(String col) {
    return "regexp_replace(" + col + ", '^\\s+|\\s+$', '', 'g')";
  }

  private static final String AGGREGATE_SQL =
      "SELECT "
      + "  CAST(snapshot_yyyymm AS VARCHAR) AS snapshot_month, "
      + "  " + t("department_code") + " AS department_code, "
      + "  " + t("department") + " AS department, "
      + "  " + t("agency_code") + " AS agency_code, "
      + "  " + t("agency") + " AS agency, "
      + "  " + t("agency_subelement_code") + " AS agency_subelement_code, "
      + "  " + t("agency_subelement") + " AS agency_subelement, "
      + "  " + t("duty_station_state_abbreviation") + " AS state_abbr, "
      + "  " + t("duty_station_state") + " AS state_name, "
      + "  " + t("duty_station_country") + " AS country_name, "
      + "  CAST(SUM(CAST(\"count\" AS BIGINT)) AS BIGINT) AS headcount "
      + "FROM read_parquet(?) "
      + "GROUP BY ALL";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String year = variables.get("year");
    if (year == null || year.isEmpty()) {
      throw new IOException("OPM federal workforce: no year in dimension variables " + variables);
    }
    List<String[]> files = listFiles(year);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (String[] f : files) {
      String month = f[0];
      String version = f[1];
      String url = BASE + "/" + year + "/" + month + "/" + version + "/download";
      File temp = org.apache.calcite.adapter.file.StagingDirectory.newTempFile(
          "opm-" + year + month + "-", ".parquet");
      try {
        LOGGER.info("OPM federal workforce {}-{} v{}: fetching {} (through raw cache)",
            year, month, version, url);
        copyThroughCache(rawCache, url, temp);
        int before = rows.size();
        aggregate(temp, year, rows);
        LOGGER.info("OPM federal workforce {}-{}: {} aggregate rows", year, month,
            rows.size() - before);
      } catch (SQLException e) {
        throw new IOException("OPM federal workforce " + year + "-" + month
            + ": DuckDB aggregation failed", e);
      } finally {
        if (!temp.delete()) {
          LOGGER.warn("OPM federal workforce: failed to delete temp file {}", temp);
        }
      }
    }
    LOGGER.info("OPM federal workforce {}: {} months, {} rows", year, files.size(), rows.size());
    return rows.iterator();
  }

  /** Returns {month, version} for each current-version file OPM lists for the year. */
  private static List<String[]> listFiles(String year) throws IOException {
    String url = BASE + "?current=true&year=" + year;
    byte[] body = readAll(url);
    List<String[]> out = new ArrayList<String[]>();
    if (body.length == 0) {
      return out;
    }
    JsonNode arr = MAPPER.readTree(body);
    if (!arr.isArray()) {
      throw new IOException("OPM federal workforce: list endpoint " + url
          + " did not return a JSON array");
    }
    for (JsonNode n : arr) {
      if (!n.hasNonNull("month") || !n.hasNonNull("version") || !n.hasNonNull("year")) {
        throw new IOException("OPM federal workforce: list entry missing month/version/year: " + n);
      }
      if (!year.equals(n.get("year").asText())) {
        continue;
      }
      out.add(new String[] {n.get("month").asText(), n.get("version").asText()});
    }
    return out;
  }

  private static byte[] readAll(String url) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try (InputStream in = ResumableHttpStream.open(LABEL, url, BACKOFF_MS)) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
        return out.toByteArray();
      } catch (IOException e) {
        last = e;
        LOGGER.warn("OPM federal workforce: attempt {}/{} failed for {}: {}", attempt,
            MAX_ATTEMPTS, url, e.getMessage());
      }
    }
    throw last;
  }

  private void copyThroughCache(RawCache rawCache, String url, File dest) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try (InputStream in = rawCache.openStream(url,
               () -> ResumableHttpStream.open(LABEL, url, BACKOFF_MS));
           FileOutputStream out = new FileOutputStream(dest)) {
        byte[] buf = new byte[1 << 20];
        int n;
        while ((n = in.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
        return;
      } catch (IOException e) {
        last = e;
        LOGGER.warn("OPM federal workforce: attempt {}/{} failed for {}: {}", attempt,
            MAX_ATTEMPTS, url, e.getMessage());
      }
    }
    throw last;
  }

  static void aggregate(File parquet, String year, List<Map<String, Object>> rows)
      throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement init = conn.createStatement()) {
      init.execute("PRAGMA memory_limit='2GB'");
      try (PreparedStatement ps = conn.prepareStatement(AGGREGATE_SQL)) {
        ps.setString(1, parquet.getAbsolutePath());
        try (ResultSet rs = ps.executeQuery()) {
          ResultSetMetaData meta = rs.getMetaData();
          int cols = meta.getColumnCount();
          while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            for (int i = 1; i <= cols; i++) {
              row.put(meta.getColumnLabel(i), rs.getObject(i));
            }
            row.put("type", "federal_workforce_by_agency");
            row.put("year", Integer.valueOf(year));
            rows.add(row);
          }
        }
      }
    }
  }
}
