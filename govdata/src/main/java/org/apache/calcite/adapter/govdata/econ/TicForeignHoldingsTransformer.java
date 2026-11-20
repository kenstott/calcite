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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds the Treasury International Capital (TIC) "Major Foreign Holders of Treasury
 * Securities" series, one row per (month, holder). Two keyless Treasury files are combined:
 * the configured source URL is the current-vintage table (slt_table5.txt, the latest 13 months
 * as last revised), and the history file (mfhhis01.txt, one block per calendar year) supplies
 * every earlier month. A month present in both comes from the current table. Only Series XIII
 * (December 2011 onward, the basis that includes TIC Form SLT custodian data) is emitted:
 * earlier series use a different survey basis and are not comparable. A block or row that does
 * not match the layout fails the run.
 */
public class TicForeignHoldingsTransformer implements StreamingResponseTransformer {

  static final String HISTORY_URL =
      "https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/mfhhis01.txt";
  static final int FIRST_YEAR = 2012;

  private static final int TIMEOUT_MS = 120_000;
  private static final int MAX_BYTES = 16 * 1024 * 1024;
  private static final List<String> MONTHS = Arrays.asList("Jan", "Feb", "Mar", "Apr", "May",
      "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec");
  private static final Pattern FOOTNOTE = Pattern.compile("\\s+\\d+/$");
  private static final Pattern ISO_MONTH = Pattern.compile("^(\\d{4})-(\\d{2})$");

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        "treasury_foreign_holdings is a StreamingResponseTransformer; call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String latest = get(context.getUrl());
    String history = get(HISTORY_URL);
    return parse(history, latest).iterator();
  }

  /** Merges the history and current-vintage files; the current table wins on overlapping months. */
  static List<Map<String, Object>> parse(String history, String latest) {
    Map<String, Map<String, Object>> rows = new LinkedHashMap<String, Map<String, Object>>();
    for (Map<String, Object> r : parseHistory(history)) {
      rows.put(key(r), r);
    }
    for (Map<String, Object> r : parseLatest(latest)) {
      rows.put(key(r), r);
    }
    if (rows.isEmpty()) {
      throw new IllegalStateException("treasury_foreign_holdings: no rows parsed");
    }
    return new ArrayList<Map<String, Object>>(rows.values());
  }

  private static String key(Map<String, Object> r) {
    return r.get("year") + "-" + r.get("month") + "|" + r.get("holder");
  }

  /** History file: tab-delimited, one block per year with a month row then a "Country" year row. */
  static List<Map<String, Object>> parseHistory(String text) {
    String[] lines = text.split("\r?\n", -1);
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    int blocks = 0;
    for (int i = 0; i < lines.length - 1; i++) {
      List<String> monthCells = cells(lines[i]);
      List<String> yearCells = cells(lines[i + 1]);
      if (monthCells.size() < 2 || !monthCells.get(0).isEmpty()
          || !MONTHS.contains(monthCells.get(1)) || !"Country".equals(yearCells.get(0))) {
        continue;
      }
      int cols = 0;
      for (int c = 1; c < monthCells.size() && !monthCells.get(c).isEmpty(); c++) {
        cols++;
      }
      int year = Integer.parseInt(yearCells.get(1));
      for (int c = 1; c <= cols; c++) {
        if (Integer.parseInt(yearCells.get(c)) != year
            || !MONTHS.contains(monthCells.get(c))) {
          throw new IllegalStateException("treasury_foreign_holdings: header of the " + year
              + " block has an unexpected column " + monthCells.get(c) + " " + yearCells.get(c));
        }
      }
      blocks++;
      if (year < FIRST_YEAR) {
        continue;
      }
      boolean official = false;
      for (int j = i + 2; j < lines.length; j++) {
        List<String> row = cells(lines[j]);
        String label = row.get(0).trim();
        if (label.isEmpty() || label.startsWith("---")) {
          continue;
        }
        if (label.startsWith("Of which")) {
          official = true;
          continue;
        }
        String holder = FOOTNOTE.matcher(label).replaceAll("");
        for (int c = 1; c <= cols; c++) {
          String cell = c < row.size() ? row.get(c).trim() : "";
          if (cell.isEmpty()) {
            throw new IllegalStateException("treasury_foreign_holdings: " + year + " row "
                + label + " has no value for " + monthCells.get(c));
          }
          out.add(row(year, MONTHS.indexOf(monthCells.get(c)) + 1, holder, official, cell));
        }
        if (official && holder.endsWith("T-Bonds & Notes")) {
          break;
        }
      }
    }
    if (blocks == 0) {
      throw new IllegalStateException("treasury_foreign_holdings: no year blocks in history file");
    }
    return out;
  }

  /** Current-vintage file: a "Country" header of yyyy-MM columns followed by one row per holder. */
  static List<Map<String, Object>> parseLatest(String text) {
    String[] lines = text.split("\r?\n", -1);
    int header = -1;
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].startsWith("Country\t")) {
        header = i;
        break;
      }
    }
    if (header < 0) {
      throw new IllegalStateException("treasury_foreign_holdings: no Country header in latest file");
    }
    List<String> head = cells(lines[header]);
    int[] years = new int[head.size()];
    int[] months = new int[head.size()];
    for (int c = 1; c < head.size(); c++) {
      Matcher m = ISO_MONTH.matcher(head.get(c).trim());
      if (!m.matches()) {
        throw new IllegalStateException("treasury_foreign_holdings: bad month column '"
            + head.get(c) + "' in latest file");
      }
      years[c] = Integer.parseInt(m.group(1));
      months[c] = Integer.parseInt(m.group(2));
    }
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    for (int j = header + 1; j < lines.length; j++) {
      List<String> row = cells(lines[j]);
      String label = row.get(0).trim();
      if (label.isEmpty() || label.startsWith("Notes")) {
        break;
      }
      boolean official = label.startsWith("Of Which");
      String holder = label.replaceFirst("^Of Which:\\s*", "");
      for (int c = 1; c < head.size(); c++) {
        String cell = c < row.size() ? row.get(c).trim() : "";
        if (cell.isEmpty()) {
          throw new IllegalStateException("treasury_foreign_holdings: latest row " + label
              + " has no value for " + head.get(c));
        }
        out.add(row(years[c], months[c], holder, official, cell));
      }
    }
    return out;
  }

  private static Map<String, Object> row(int year, int month, String holder, boolean official,
      String cell) {
    Map<String, Object> r = new LinkedHashMap<String, Object>();
    String kind;
    String name = holder;
    if (official) {
      if (holder.equals("Foreign Official") || holder.equals("For. Official")) {
        kind = "FOREIGN_OFFICIAL_TOTAL";
        name = "Foreign Official";
      } else if (holder.endsWith("Treasury Bills")) {
        kind = "FOREIGN_OFFICIAL_BILLS";
        name = "Foreign Official Treasury Bills";
      } else if (holder.endsWith("T-Bonds & Notes")) {
        kind = "FOREIGN_OFFICIAL_BONDS_NOTES";
        name = "Foreign Official T-Bonds & Notes";
      } else {
        throw new IllegalStateException("treasury_foreign_holdings: unknown official line '"
            + holder + "'");
      }
    } else if (holder.equals("All Other")) {
      kind = "ALL_OTHER";
    } else if (holder.equals("Grand Total")) {
      kind = "GRAND_TOTAL";
    } else {
      kind = "COUNTRY";
    }
    if (!cell.matches("[0-9]+(\\.[0-9]+)?")) {
      throw new IllegalStateException("treasury_foreign_holdings: non-numeric value '" + cell
          + "' for " + holder + " " + year + "-" + month);
    }
    r.put("year", Integer.valueOf(year));
    r.put("month", Integer.valueOf(month));
    r.put("holder", name);
    r.put("holder_kind", kind);
    r.put("holdings_usd_billions", Double.valueOf(cell));
    return r;
  }

  /** Splits a tab-delimited line, removing the double quotes around "Country, Name" cells. */
  private static List<String> cells(String line) {
    String[] raw = line.split("\t", -1);
    List<String> out = new ArrayList<String>(raw.length);
    for (String s : raw) {
      String t = s;
      if (t.length() >= 2 && t.startsWith("\"") && t.endsWith("\"")) {
        t = t.substring(1, t.length() - 1);
      }
      out.add(t);
    }
    return out;
  }

  private static String get(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    try {
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      NetworkOutageWait.connect(conn);
      int status = conn.getResponseCode();
      if (status != HttpURLConnection.HTTP_OK) {
        throw new IOException("HTTP " + status + " from " + url);
      }
      try (InputStream in = conn.getInputStream()) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
          if (out.size() + n > MAX_BYTES) {
            throw new IOException("response from " + url + " exceeds " + MAX_BYTES + " bytes");
          }
          out.write(buf, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
      }
    } finally {
      conn.disconnect();
    }
  }
}
