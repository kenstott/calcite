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

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.NetworkOutageWait;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Fetches the CFPB Mortgage Performance Trends downloads (National Mortgage Database) and
 * maps them into {@code cfpb_mortgage_performance} rows (geography x month).
 *
 * <p>The CFPB publishes six wide CSVs per quarter (state, metro area and county, each for
 * "30-89 days late" and "90+ days late"), named {@code <Level>MortgagesPercent-<bucket>-thru-
 * YYYY-MM.csv}. Each file carries the whole history from 2008-01, one column per month. The
 * latest quarter is discovered from the download page; the two buckets of a level are joined
 * on geography code and unpivoted to one row per (geo_level, geo_fips, month).
 */
public class CfpbMortgagePerformanceDataProvider implements DataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CfpbMortgagePerformanceDataProvider.class);

  private static final String PAGE_URL =
      "https://www.consumerfinance.gov/data-research/mortgage-performance-trends/"
          + "download-the-data/";
  private static final String FILE_BASE =
      "https://files.consumerfinance.gov/data/mortgage-performance/downloads/";
  private static final String USER_AGENT =
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          + "(KHTML, like Gecko) Chrome/120.0 Safari/537.36";
  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 180_000;
  private static final int MAX_RETRIES = 3;

  private static final Pattern MONTH_HEADER = Pattern.compile("^(\\d{4})-(\\d{2})$");
  private static final Pattern LATEST_FILE = Pattern.compile(
      "CountyMortgagesPercent-30-89DaysLate-thru-(\\d{4}-\\d{2})\\.csv");

  /** Geography levels: file prefix, geo_level value, key columns before the first month. */
  private static final String[] LEVEL_PREFIX = {"State", "MetroArea", "County"};
  private static final String[] LEVEL_NAME = {"state", "metro", "county"};

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String page = new String(download(PAGE_URL), StandardCharsets.UTF_8);
    String thru = latestPeriod(page);
    LOGGER.info("CfpbMortgagePerformanceDataProvider: fetching {} data thru {}",
        config.getName(), thru);
    // key "<level>|<fips>|<month>" -> row
    Map<String, Map<String, Object>> rows = new LinkedHashMap<String, Map<String, Object>>();
    for (int lv = 0; lv < LEVEL_PREFIX.length; lv++) {
      String late = file(LEVEL_PREFIX[lv], "30-89DaysLate", thru);
      String severe = file(LEVEL_PREFIX[lv], "90-plusDaysLate", thru);
      merge(rows, LEVEL_NAME[lv], late, "pct_30_89_delinquent", lv);
      merge(rows, LEVEL_NAME[lv], severe, "pct_90_plus_delinquent", lv);
    }
    for (Map<String, Object> r : rows.values()) {
      if (r.get("pct_30_89_delinquent") == null || r.get("pct_90_plus_delinquent") == null) {
        throw new IOException("cfpb mortgage performance: " + r.get("geo_level") + " "
            + r.get("geo_fips") + " " + r.get("month")
            + " is missing one of the two delinquency buckets");
      }
    }
    if (rows.isEmpty()) {
      throw new IOException("cfpb mortgage performance: parsed to zero rows");
    }
    LOGGER.info("CfpbMortgagePerformanceDataProvider: {} rows", rows.size());
    return new ArrayList<Map<String, Object>>(rows.values()).iterator();
  }

  private String file(String prefix, String bucket, String thru) throws IOException {
    String url = FILE_BASE + prefix + "MortgagesPercent-" + bucket + "-thru-" + thru + ".csv";
    return new String(download(url), StandardCharsets.UTF_8);
  }

  /** Most recent quarter for which the page links the county 30-89 file. */
  static String latestPeriod(String page) throws IOException {
    Matcher m = LATEST_FILE.matcher(page);
    String best = null;
    while (m.find()) {
      if (best == null || m.group(1).compareTo(best) > 0) {
        best = m.group(1);
      }
    }
    if (best == null) {
      throw new IOException("cfpb mortgage performance: no county download link on " + PAGE_URL);
    }
    return best;
  }

  /**
   * Unpivots one wide CSV into {@code rows}, setting {@code column} on each (geo, month) row.
   * Layout by level: state {@code RegionType,Name,FIPSCode,months...}; metro
   * {@code RegionType,Name,CBSACode,months...}; county
   * {@code RegionType,State,Name,FIPSCode,months...}. National rows are kept as
   * {@code geo_level=national}; the metro files also carry one {@code NonMetroArea} row per
   * state (code like {@code 01-non}), kept as {@code geo_level=nonmetro}.
   */
  static void merge(Map<String, Map<String, Object>> rows, String level, String csv,
      String column, int levelIdx) throws IOException {
    String[] lines = csv.split("\r?\n");
    List<String> header = parseLine(lines[0]);
    boolean county = "county".equals(level);
    int firstMonth = county ? 4 : 3;
    int nameIdx = county ? 2 : 1;
    int codeIdx = firstMonth - 1;
    List<String> months = new ArrayList<String>();
    for (int c = firstMonth; c < header.size(); c++) {
      Matcher m = MONTH_HEADER.matcher(header.get(c));
      if (!m.matches()) {
        throw new IOException("cfpb mortgage performance: unrecognised month header '"
            + header.get(c) + "' in " + LEVEL_PREFIX[levelIdx] + " " + column);
      }
      months.add(header.get(c));
    }
    if (months.isEmpty()) {
      throw new IOException("cfpb mortgage performance: no month columns in "
          + LEVEL_PREFIX[levelIdx] + " " + column);
    }
    for (int i = 1; i < lines.length; i++) {
      if (lines[i].trim().isEmpty()) {
        continue;
      }
      List<String> f = parseLine(lines[i]);
      if (f.size() != header.size()) {
        throw new IOException("cfpb mortgage performance: line " + (i + 1) + " has "
            + f.size() + " fields, header has " + header.size());
      }
      String regionType = f.get(0);
      String geoLevel;
      String fips;
      if ("National".equals(regionType)) {
        geoLevel = "national";
        fips = "US";
      } else if ("metro".equals(level) && "NonMetroArea".equals(regionType)) {
        geoLevel = "nonmetro";
        fips = f.get(codeIdx).trim();
      } else if (regionType.equals(LEVEL_PREFIX[levelIdx])) {
        geoLevel = level;
        fips = f.get(codeIdx).replace("'", "").trim();
      } else {
        throw new IOException("cfpb mortgage performance: line " + (i + 1)
            + " unexpected RegionType '" + regionType + "'");
      }
      if (fips.isEmpty()) {
        throw new IOException("cfpb mortgage performance: line " + (i + 1) + " has no code");
      }
      for (int k = 0; k < months.size(); k++) {
        String v = f.get(firstMonth + k).trim();
        if (v.isEmpty()) {
          continue;
        }
        double value;
        try {
          value = Double.parseDouble(v);
        } catch (NumberFormatException e) {
          throw new IOException("cfpb mortgage performance: line " + (i + 1) + " month "
              + months.get(k) + " non-numeric '" + v + "'", e);
        }
        String month = months.get(k);
        String key = geoLevel + "|" + fips + "|" + month;
        Map<String, Object> row = rows.get(key);
        if (row == null) {
          row = new LinkedHashMap<String, Object>();
          row.put("type", "cfpb_mortgage_performance");
          row.put("geo_level", geoLevel);
          row.put("geo_fips", fips);
          row.put("geo_name", f.get(nameIdx));
          row.put("state_abbr", county ? f.get(1) : null);
          row.put("data_year", Integer.valueOf(month.substring(0, 4)));
          row.put("month", month);
          row.put("pct_30_89_delinquent", null);
          row.put("pct_90_plus_delinquent", null);
          rows.put(key, row);
        }
        row.put(column, value);
      }
    }
  }

  /** Splits one CSV line, honouring double-quoted fields containing commas. */
  static List<String> parseLine(String line) {
    List<String> out = new ArrayList<String>();
    StringBuilder sb = new StringBuilder();
    boolean quoted = false;
    for (int i = 0; i < line.length(); i++) {
      char ch = line.charAt(i);
      if (quoted) {
        if (ch == '"') {
          if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
            sb.append('"');
            i++;
          } else {
            quoted = false;
          }
        } else {
          sb.append(ch);
        }
      } else if (ch == '"') {
        quoted = true;
      } else if (ch == ',') {
        out.add(sb.toString());
        sb.setLength(0);
      } else {
        sb.append(ch);
      }
    }
    out.add(sb.toString());
    return out;
  }

  private byte[] download(String url) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", USER_AGENT);
      conn.setRequestProperty("Accept", "*/*");
      try {
        NetworkOutageWait.connect(conn);
        int status = conn.getResponseCode();
        if (status == HttpURLConnection.HTTP_OK) {
          try (InputStream in = conn.getInputStream()) {
            ByteArrayOutputStream out = new ByteArrayOutputStream(1 << 20);
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) != -1) {
              out.write(buf, 0, n);
            }
            return out.toByteArray();
          }
        }
        if (status != 429 && status < 500) {
          throw new IOException("HTTP " + status + " from " + url);
        }
        last = new IOException("HTTP " + status + " from " + url);
      } finally {
        conn.disconnect();
      }
      try {
        Thread.sleep(1000L * attempt);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted during CFPB retry backoff", e);
      }
    }
    throw last;
  }
}
