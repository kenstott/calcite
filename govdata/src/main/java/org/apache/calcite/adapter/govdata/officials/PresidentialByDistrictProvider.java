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
import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * DataProvider for {@code officials.presidential_results_by_district}: presidential vote by
 * congressional district, as calculated by The Downballot (the-downballot.com) and published as
 * public Google Sheets CSV exports.
 *
 * <p>Two sheets are read. The "2026 lines" detail tab carries the 2024 (and, where recomputed,
 * 2020) presidential vote re-aggregated onto the district lines adopted for the 2026 midterms,
 * including the 2025-26 mid-decade redraws. The "2024 lines" sheet carries the 2024 vote on the
 * lines used in the 2024 election. The parse is strict: the header rows must match exactly and any
 * unrecognised district code or number fails the fetch.
 */
public class PresidentialByDistrictProvider implements CachingDataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PresidentialByDistrictProvider.class);

  static final String TABLE = "presidential_results_by_district";
  static final String LINES_2026_URL = "https://docs.google.com/spreadsheets/d/"
      + "1eZfaFI-c-PFOoKx1-zZA2MP0_dxRq_LVK0re3BOQqy0/export?format=csv&gid=1491069057";
  static final String LINES_2024_URL = "https://docs.google.com/spreadsheets/d/"
      + "1ZHx5E0-5vuXxcZShBgsAl_vwAntkkanGqYQp0owNjoQ/export?format=csv&gid=0";
  private static final Pattern CODE = Pattern.compile("([A-Z]{2})-(AL|\\d{2})");
  private static final String USER_AGENT = "Apache-Calcite-GovData/1.0";
  private static final int MAX_ATTEMPTS = 3;
  private static final int EXPECTED_DISTRICTS = 435;

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    rows.addAll(parse2026(read(LINES_2026_URL, rawCache, "Downballot 2026-lines results")));
    rows.addAll(parse2024(read(LINES_2024_URL, rawCache, "Downballot 2024-lines results")));
    LOGGER.info("{}: {} rows", TABLE, Integer.valueOf(rows.size()));
    return rows.iterator();
  }

  private static String read(final String url, RawCache rawCache, final String label)
      throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try (InputStream in = rawCache.openStream(url,
              () -> open(url, label));
           ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        byte[] buf = new byte[1 << 16];
        int n;
        while ((n = in.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
      } catch (IOException e) {
        last = e;
        LOGGER.warn("{}: attempt {}/{} failed for {}: {}", TABLE, Integer.valueOf(attempt),
            Integer.valueOf(MAX_ATTEMPTS), url, e.getMessage());
      }
    }
    throw last;
  }

  /**
   * Opens a Google Sheets CSV export. The export is chunked (no Content-Length), so completeness
   * is verified by the strict header and district-count checks in the parsers, not by length.
   */
  private static InputStream open(String url, String label) throws IOException {
    HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
    c.setRequestProperty("User-Agent", USER_AGENT);
    c.setConnectTimeout(30_000);
    c.setReadTimeout(120_000);
    c.setInstanceFollowRedirects(true);
    int code = c.getResponseCode();
    if (code != 200) {
      throw new IOException(label + ": HTTP " + code + " from " + url);
    }
    return c.getInputStream();
  }

  private static List<List<String>> records(String csv) throws IOException {
    BufferedReader r = new BufferedReader(new StringReader(csv));
    List<List<String>> out = new ArrayList<List<String>>();
    String rec;
    while ((rec = CsvRecordReader.readRecord(r)) != null) {
      if (!rec.trim().isEmpty()) {
        out.add(CsvRecordReader.splitFields(rec, ','));
      }
    }
    return out;
  }

  private static boolean cell(List<String> f, int i, String want) {
    return f.size() > i && want.equals(f.get(i).trim());
  }

  /** The 2026-lines detail tab: 2024 votes in columns E-G, 2020 votes in columns L-N. */
  static List<Map<String, Object>> parse2026(String csv) throws IOException {
    List<List<String>> recs = records(csv);
    int h = -1;
    for (int i = 0; i < recs.size(); i++) {
      if (cell(recs.get(i), 0, "District") && cell(recs.get(i), 4, "2024")
          && cell(recs.get(i), 11, "2020")) {
        h = i;
        break;
      }
    }
    if (h < 0 || h + 1 >= recs.size()) {
      throw new IOException(TABLE + ": 2026-lines header row (District ... 2024 ... 2020) not found");
    }
    List<String> c = recs.get(h + 1);
    if (!cell(c, 4, "Harris") || !cell(c, 5, "Trump") || !cell(c, 6, "Total")
        || !cell(c, 11, "Biden") || !cell(c, 12, "Trump") || !cell(c, 13, "Total")) {
      throw new IOException(TABLE + ": 2026-lines candidate header changed: " + c);
    }
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    int districts = 0;
    int line = h + 1;
    for (int i = h + 2; i < recs.size(); i++) {
      List<String> f = recs.get(i);
      line = i + 1;
      String code = f.get(0).trim();
      if (!CODE.matcher(code).matches()) {
        continue;
      }
      districts++;
      rows.add(row("2026", 2024, code, "Kamala Harris", "Donald Trump", f, 4, line));
      boolean any2020 = !f.get(11).trim().isEmpty() || !f.get(12).trim().isEmpty()
          || !f.get(13).trim().isEmpty();
      if (any2020) {
        rows.add(row("2026", 2020, code, "Joe Biden", "Donald Trump", f, 11, line));
      }
    }
    if (districts != EXPECTED_DISTRICTS) {
      throw new IOException(TABLE + ": 2026-lines sheet has " + districts + " districts, expected "
          + EXPECTED_DISTRICTS);
    }
    return rows;
  }

  /** The 2024-lines sheet: Harris, Trump, Other, Total in columns B-E. */
  static List<Map<String, Object>> parse2024(String csv) throws IOException {
    List<List<String>> recs = records(csv);
    List<String> hdr = recs.get(0);
    if (!cell(hdr, 0, "CD") || !cell(hdr, 1, "Harris") || !cell(hdr, 2, "Trump")
        || !cell(hdr, 3, "Other") || !cell(hdr, 4, "Total")) {
      throw new IOException(TABLE + ": 2024-lines header changed: " + hdr);
    }
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int i = 1; i < recs.size(); i++) {
      List<String> f = recs.get(i);
      String code = f.get(0).trim();
      if (!CODE.matcher(code).matches()) {
        continue;
      }
      long d = num(f.get(1), "Harris", i + 1);
      long r = num(f.get(2), "Trump", i + 1);
      long o = num(f.get(3), "Other", i + 1);
      long t = num(f.get(4), "Total", i + 1);
      // The source's hand-entered Other column is off by one vote from Total in ten NC and TN
      // rows (+-1); a larger gap fails the fetch. other_votes is derived from Total, not from it.
      if (Math.abs(d + r + o - t) > 1) {
        throw new IOException(TABLE + " line " + (i + 1) + ": Harris+Trump+Other " + (d + r + o)
            + " != Total " + t);
      }
      rows.add(build("2024", 2024, code, "Kamala Harris", "Donald Trump", d, r, t));
    }
    if (rows.size() != EXPECTED_DISTRICTS) {
      throw new IOException(TABLE + ": 2024-lines sheet has " + rows.size()
          + " districts, expected " + EXPECTED_DISTRICTS);
    }
    return rows;
  }

  private static Map<String, Object> row(String vintage, int year, String code, String dem,
      String rep, List<String> f, int col, int line) throws IOException {
    long d = num(f.get(col), "dem", line);
    long r = num(f.get(col + 1), "rep", line);
    long t = num(f.get(col + 2), "total", line);
    return build(vintage, year, code, dem, rep, d, r, t);
  }

  private static Map<String, Object> build(String vintage, int year, String code, String dem,
      String rep, long d, long r, long t) throws IOException {
    if (t <= 0 || d + r > t) {
      throw new IOException(TABLE + " " + code + " " + year + ": votes " + d + "+" + r
          + " inconsistent with total " + t);
    }
    java.util.regex.Matcher m = CODE.matcher(code);
    m.matches();
    Map<String, Object> o = new LinkedHashMap<String, Object>();
    o.put("district_lines", vintage);
    o.put("election_year", Integer.valueOf(year));
    o.put("district_code", code);
    o.put("state_postal", m.group(1));
    o.put("district", m.group(2));
    o.put("dem_candidate", dem);
    o.put("rep_candidate", rep);
    o.put("dem_votes", Long.valueOf(d));
    o.put("rep_votes", Long.valueOf(r));
    o.put("other_votes", Long.valueOf(t - d - r));
    o.put("total_votes", Long.valueOf(t));
    o.put("dem_pct", Double.valueOf(pct(d, t)));
    o.put("rep_pct", Double.valueOf(pct(r, t)));
    o.put("margin_pct", Double.valueOf(pct(d - r, t)));
    return o;
  }

  private static double pct(long n, long t) {
    return Math.round(10000.0 * n / t) / 100.0;
  }

  private static long num(String v, String col, int line) throws IOException {
    String s = v.trim().replace(",", "");
    try {
      long n = Long.parseLong(s);
      if (n < 0) {
        throw new IOException(TABLE + " line " + line + ": " + col + " '" + v + "' is negative");
      }
      return n;
    } catch (NumberFormatException e) {
      throw new IOException(TABLE + " line " + line + ": " + col + " '" + v
          + "' is not an integer", e);
    }
  }
}
