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

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Fetches the HUD "A Picture of Subsidized Households" county-level workbook and maps it into
 * {@code hud_subsidized_county} rows (county × HUD program). HUD publishes one {@code .xlsx} per
 * data year at a stable path; from 2022 the files carry 2020-census county geography
 * ({@code COUNTY_<year>_2020census.xlsx}), so this provider is scoped to 2022+ and always requests
 * that flavor (matching {@code geo.counties}). Each row is one (county, program) summary with the
 * full demographic profile — units, occupancy, income, rent/spending, and percent
 * minority / female-headed / elderly / disabled — that the ArcGIS tract snapshot
 * ({@code hud_subsidized_housing}) does not carry.
 *
 * <p>HUD encodes suppressed / not-applicable numeric cells as {@code -1} or {@code "NA"}; both map
 * to null. The workbook is ~5 MB and parsed in one POI pass; rows are buffered (~25k) like the
 * geo TIGER provider. Mirrors {@link org.apache.calcite.adapter.govdata.geo geo}'s dataProvider
 * pattern: the built-in HttpSource is skipped because this returns non-null.
 */
public class HudPictureCountyDataProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(HudPictureCountyDataProvider.class);

  private static final String BASE_URL =
      "https://www.huduser.gov/portal/datasets/pictures/files/";
  private static final String REFERER =
      "https://www.huduser.gov/portal/datasets/assthsg.html";
  private static final String USER_AGENT = "Mozilla/5.0 (compatible; GovData/1.0)";
  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 180_000;
  private static final int MAX_RETRIES = 3;

  /** HUD workbook header -> our column name. Order defines the emitted row shape. */
  private static final String[][] COLUMN_MAP = {
      {"code", "county_fips", "str"},
      {"state", "state_abbr", "str"},
      {"name", "county_name", "str"},
      {"program", "program_code", "str"},
      {"program_label", "program", "str"},
      {"total_units", "total_units", "int"},
      {"pct_occupied", "pct_occupied", "dbl"},
      {"people_total", "people_total", "int"},
      {"people_per_unit", "people_per_unit", "dbl"},
      {"hh_income", "avg_hh_income", "int"},
      {"rent_per_month", "avg_tenant_rent", "int"},
      {"spending_per_month", "avg_hud_spending", "int"},
      {"pct_minority", "pct_minority", "dbl"},
      {"pct_female_head", "pct_female_head", "dbl"},
      {"pct_age62plus", "pct_elderly", "dbl"},
      {"pct_disabled_all", "pct_disabled", "dbl"},
      {"months_waiting", "months_waiting", "int"},
  };

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.isEmpty()) {
      throw new IOException("hud_subsidized_county requires a 'year' (or 'effective_year') dimension");
    }
    String url = BASE_URL + "COUNTY_" + year + "_2020census.xlsx";
    LOGGER.info("HudPictureCountyDataProvider: fetching {} from {}", config.getName(), url);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    byte[] body = download(url);
    try (java.io.ByteArrayInputStream is = new java.io.ByteArrayInputStream(body);
         Workbook wb = WorkbookFactory.create(is)) {
      Sheet sheet = wb.getSheetAt(0);
      Row header = sheet.getRow(0);
      if (header == null) {
        throw new IOException("hud_subsidized_county: empty workbook for year " + year);
      }
      Map<String, Integer> col = new HashMap<String, Integer>();
      for (Cell c : header) {
        String h = c.getStringCellValue();
        if (h != null) {
          col.put(h.trim(), c.getColumnIndex());
        }
      }
      Integer yearInt = Integer.valueOf(year);
      for (int i = 1; i <= sheet.getLastRowNum(); i++) {
        Row r = sheet.getRow(i);
        if (r == null) {
          continue;
        }
        String code = str(cell(r, col.get("code")));
        if (code == null) {
          continue;
        }
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("type", "hud_subsidized_county");
        row.put("year", yearInt);
        row.put("state_fips", code.length() >= 2 ? code.substring(0, 2) : null);
        for (String[] m : COLUMN_MAP) {
          Cell c = cell(r, col.get(m[0]));
          Object v;
          if ("int".equals(m[2])) {
            v = toInt(c);
          } else if ("dbl".equals(m[2])) {
            v = toDbl(c);
          } else {
            v = str(c);
          }
          row.put(m[1], v);
        }
        rows.add(row);
      }
    }
    LOGGER.info("HudPictureCountyDataProvider: {} rows for year {}", rows.size(), year);
    return rows.iterator();
  }

  private static Cell cell(Row row, Integer idx) {
    return idx == null ? null : row.getCell(idx);
  }

  /** Downloads bytes with UA + Referer (HUD rejects requests lacking them); retries 429/5xx. */
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
        int status = conn.getResponseCode();
        if (status == HttpURLConnection.HTTP_OK) {
          try (InputStream in = conn.getInputStream()) {
            return readAll(in);
          }
        }
        if (status != 429 && status < 500) {
          throw new IOException("HTTP " + status + " from " + url);
        }
        last = new IOException("HTTP " + status + " from " + url);
      } finally {
        conn.disconnect();
      }
      sleepBackoff(attempt);
    }
    throw last != null ? last : new IOException("GET failed: " + url);
  }

  private static byte[] readAll(InputStream in) throws IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(1 << 20);
    byte[] buf = new byte[8192];
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }

  private static void sleepBackoff(int attempt) throws IOException {
    try {
      Thread.sleep(1000L * attempt);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted during HUD retry backoff", e);
    }
  }

  // --- cell decoding (HUD -1 / "NA" are suppression sentinels -> null) ---------

  private static String str(Cell c) {
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
    if (s == null) {
      return null;
    }
    s = s.trim();
    return s.isEmpty() || "NA".equalsIgnoreCase(s) ? null : s;
  }

  private static Double toDbl(Cell c) {
    Double d = numeric(c);
    return d == null || d == -1.0 ? null : d;
  }

  private static Integer toInt(Cell c) {
    Double d = numeric(c);
    if (d == null || d == -1.0) {
      return null;
    }
    return Integer.valueOf((int) Math.round(d));
  }

  private static Double numeric(Cell c) {
    if (c == null) {
      return null;
    }
    if (c.getCellType() == CellType.NUMERIC) {
      return c.getNumericCellValue();
    }
    if (c.getCellType() == CellType.STRING) {
      String s = c.getStringCellValue().trim();
      if (s.isEmpty() || "NA".equalsIgnoreCase(s)) {
        return null;
      }
      try {
        return Double.valueOf(s);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
