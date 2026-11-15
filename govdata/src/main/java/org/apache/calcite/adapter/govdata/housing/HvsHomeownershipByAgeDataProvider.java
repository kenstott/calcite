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

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Fetches the Census Housing Vacancy Survey historical Table 12
 * ({@code histtab12.xlsx}: household estimates by age of householder, 1982 to present) and
 * maps it into {@code hvs_homeownership_by_age} rows (year x age group).
 *
 * <p>The sheet stacks several blocks, each headed by an "Age of Householder" row whose
 * year cells sit in every other column (Total, Owner pair per year) and carry glued
 * footnote markers ({@code 19821}, {@code 19892}) or revision tags ({@code 1993r1},
 * {@code 2002r2}). The block layout is discovered from those header rows, not fixed
 * offsets. Where Census publishes both an original and a revised column for a year
 * (1993, 2002), the revised column supersedes the original.
 */
public class HvsHomeownershipByAgeDataProvider implements DataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(HvsHomeownershipByAgeDataProvider.class);

  private static final String URL = "https://www.census.gov/housing/hvs/data/histtab12.xlsx";
  private static final String REFERER = "https://www.census.gov/housing/hvs/data/histtabs.html";
  private static final String USER_AGENT =
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          + "(KHTML, like Gecko) Chrome/120.0 Safari/537.36";
  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 180_000;
  private static final int MAX_RETRIES = 3;

  /** Year header: four digits, then an optional footnote digit, revision tag or asterisk. */
  private static final Pattern YEAR_HEADER =
      Pattern.compile("^(\\d{4})(r\\d+|\\d|\\*)?$");

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    LOGGER.info("HvsHomeownershipByAgeDataProvider: fetching {} from {}", config.getName(), URL);
    byte[] body = download(URL);
    // key "<year>|<age_group>" -> row; revised columns replace originals
    Map<String, Map<String, Object>> rows = new LinkedHashMap<String, Map<String, Object>>();
    Map<String, Boolean> revised = new LinkedHashMap<String, Boolean>();
    try (ByteArrayInputStream is = new ByteArrayInputStream(body);
         Workbook wb = WorkbookFactory.create(is)) {
      Sheet sheet = wb.getSheetAt(0);
      int last = sheet.getLastRowNum();
      List<Integer> yearCols = null;
      List<Integer> years = null;
      List<Boolean> isRevised = null;
      for (int i = 0; i <= last; i++) {
        Row r = sheet.getRow(i);
        if (r == null) {
          continue;
        }
        String label = text(r.getCell(0));
        if (label == null) {
          continue;
        }
        if ("Footnotes:".equals(label)) {
          break;
        }
        if ("Age of Householder".equals(label)) {
          yearCols = new ArrayList<Integer>();
          years = new ArrayList<Integer>();
          isRevised = new ArrayList<Boolean>();
          for (int c = 1; c < r.getLastCellNum(); c++) {
            String h = text(r.getCell(c));
            if (h == null) {
              continue;
            }
            Matcher m = YEAR_HEADER.matcher(h);
            if (!m.matches()) {
              throw new IOException("hvs table 12: row " + (i + 1) + " col " + c
                  + " unrecognised year header '" + h + "'");
            }
            yearCols.add(c);
            years.add(Integer.valueOf(m.group(1)));
            isRevised.add(m.group(2) != null && m.group(2).startsWith("r"));
          }
          if (yearCols.isEmpty()) {
            throw new IOException("hvs table 12: header row " + (i + 1) + " has no years");
          }
          Row sub = sheet.getRow(i + 1);
          for (int c : yearCols) {
            if (sub == null || !"Total".equals(text(sub.getCell(c)))
                || !"Owner".equals(text(sub.getCell(c + 1)))) {
              throw new IOException("hvs table 12: row " + (i + 2) + " col " + c
                  + " is not a Total/Owner pair");
            }
          }
          continue;
        }
        if (yearCols == null || "Total".equals(label) || label.startsWith("Table")
            || label.startsWith("(Numbers")) {
          continue;
        }
        String group = ageGroup(label, i);
        if (group == null) {
          continue;
        }
        for (int k = 0; k < yearCols.size(); k++) {
          Double total = number(r.getCell(yearCols.get(k)), i);
          Double owner = number(r.getCell(yearCols.get(k) + 1), i);
          if (total == null && owner == null) {
            continue;
          }
          if (total == null || owner == null || total <= 0) {
            throw new IOException("hvs table 12: row " + (i + 1) + " year " + years.get(k)
                + " has total=" + total + " owner=" + owner);
          }
          String key = years.get(k) + "|" + group;
          Boolean prior = revised.get(key);
          if (prior != null && prior && !isRevised.get(k)) {
            continue;
          }
          Map<String, Object> row = new LinkedHashMap<String, Object>();
          row.put("type", "hvs_homeownership_by_age");
          row.put("data_year", years.get(k));
          row.put("age_group", group);
          row.put("age_group_kind", kind(group));
          row.put("households_total_thousands", total);
          row.put("households_owner_thousands", owner);
          row.put("households_renter_thousands", total - owner);
          row.put("homeownership_rate_pct", 100.0 * owner / total);
          row.put("is_revised", isRevised.get(k));
          rows.put(key, row);
          revised.put(key, isRevised.get(k));
        }
      }
    }
    if (rows.isEmpty()) {
      throw new IOException("hvs table 12: workbook parsed to zero rows");
    }
    LOGGER.info("HvsHomeownershipByAgeDataProvider: {} rows", rows.size());
    return new ArrayList<Map<String, Object>>(rows.values()).iterator();
  }

  /** Canonical age-group key for a Table 12 row label; null for rows that are not rates. */
  private static String ageGroup(String rawLabel, int rowIdx) throws IOException {
    String l = rawLabel.replaceAll("^[.\\u2026]+", "").replaceAll("[.\\u2026]+$", "").trim();
    if (l.startsWith("United States, total")) {
      return "total";
    }
    if (l.startsWith("No householder")) {
      return null;
    }
    if (l.startsWith("Less than 25")) {
      return "under_25";
    }
    if (l.startsWith("Less than 35")) {
      return "under_35";
    }
    if (l.startsWith("75 years and over")) {
      return "75_plus";
    }
    if (l.startsWith("65 years and over")) {
      return "65_plus";
    }
    Matcher m = Pattern.compile("^(\\d{2}) to (\\d{2}) years").matcher(l);
    if (m.find()) {
      return m.group(1) + "_" + m.group(2);
    }
    throw new IOException("hvs table 12: row " + (rowIdx + 1) + " unrecognised label '"
        + rawLabel + "'");
  }

  private static String kind(String group) {
    if ("total".equals(group)) {
      return "total";
    }
    return "under_35".equals(group) || "35_44".equals(group) || "45_54".equals(group)
        || "55_64".equals(group) || "65_plus".equals(group) ? "broad" : "five_year";
  }

  private static String text(Cell c) {
    if (c == null) {
      return null;
    }
    String s;
    if (c.getCellType() == CellType.NUMERIC) {
      double d = c.getNumericCellValue();
      s = d == Math.floor(d) ? String.valueOf((long) d) : String.valueOf(d);
    } else if (c.getCellType() == CellType.STRING) {
      s = c.getStringCellValue();
    } else {
      return null;
    }
    s = s.trim();
    return s.isEmpty() ? null : s;
  }

  private static Double number(Cell c, int rowIdx) throws IOException {
    if (c == null || c.getCellType() == CellType.BLANK) {
      return null;
    }
    if (c.getCellType() == CellType.NUMERIC) {
      return c.getNumericCellValue();
    }
    // Census stores some totals as formulas with a cached result (e.g. SUM(N84:N95)+1);
    // the cached value is the published figure.
    if (c.getCellType() == CellType.FORMULA
        && c.getCachedFormulaResultType() == CellType.NUMERIC) {
      return c.getNumericCellValue();
    }
    throw new IOException("hvs table 12: row " + (rowIdx + 1) + " non-numeric cell '"
        + c + "'");
  }

  private byte[] download(String url) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", USER_AGENT);
      conn.setRequestProperty("Referer", REFERER);
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
        throw new IOException("interrupted during HVS retry backoff", e);
      }
    }
    throw last;
  }
}
