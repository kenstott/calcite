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

/**
 * Fetches FHFA's annual all-transactions county House Price Index workbook
 * ({@code hpi_at_county.xlsx}) and maps it into {@code county_house_price_index} rows
 * (county × year). The workbook carries a multi-line banner followed by the header row
 * {@code State | County | FIPS code | Year | Annual Change (%) | HPI | HPI with 1990 base |
 * HPI with 2000 base}; the header row is located by its "FIPS code" cell rather than a fixed
 * index so a changed banner length does not shift the parse.
 *
 * <p>FHFA writes a lone period ({@code "."}) for county-years whose sample is too small to
 * publish an index; that sentinel and genuinely blank cells map to null. Any other
 * non-numeric text in a numeric column is a source change and fails the fetch.
 */
public class FhfaCountyHpiDataProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(FhfaCountyHpiDataProvider.class);

  private static final String URL = "https://www.fhfa.gov/hpi/download/annual/hpi_at_county.xlsx";
  private static final String REFERER = "https://www.fhfa.gov/data/hpi/datasets";
  private static final String USER_AGENT =
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          + "(KHTML, like Gecko) Chrome/120.0 Safari/537.36";
  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 180_000;
  private static final int MAX_RETRIES = 3;
  private static final int HEADER_SCAN_ROWS = 30;

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    LOGGER.info("FhfaCountyHpiDataProvider: fetching {} from {}", config.getName(), URL);
    byte[] body = download(URL);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    try (ByteArrayInputStream is = new ByteArrayInputStream(body);
         Workbook wb = WorkbookFactory.create(is)) {
      Sheet sheet = wb.getSheetAt(0);
      int headerRow = findHeaderRow(sheet);
      for (int i = headerRow + 1; i <= sheet.getLastRowNum(); i++) {
        Row r = sheet.getRow(i);
        if (r == null) {
          continue;
        }
        String fips = text(r.getCell(2));
        if (fips == null) {
          continue;
        }
        if (fips.length() != 5) {
          throw new IOException("fhfa county hpi: row " + (i + 1) + " FIPS '" + fips
              + "' is not 5 digits");
        }
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("type", "county_house_price_index");
        row.put("state_fips", fips.substring(0, 2));
        row.put("county_fips", fips);
        row.put("state_abbr", text(r.getCell(0)));
        row.put("county_name", text(r.getCell(1)));
        Double yr = number(r.getCell(3), i);
        if (yr == null) {
          throw new IOException("fhfa county hpi: row " + (i + 1) + " has no year");
        }
        row.put("data_year", Integer.valueOf((int) Math.round(yr)));
        row.put("annual_change_pct", number(r.getCell(4), i));
        row.put("hpi", number(r.getCell(5), i));
        row.put("hpi_1990_base", number(r.getCell(6), i));
        row.put("hpi_2000_base", number(r.getCell(7), i));
        rows.add(row);
      }
    }
    if (rows.isEmpty()) {
      throw new IOException("fhfa county hpi: workbook parsed to zero rows");
    }
    LOGGER.info("FhfaCountyHpiDataProvider: {} rows", rows.size());
    return rows.iterator();
  }

  private static int findHeaderRow(Sheet sheet) throws IOException {
    int limit = Math.min(sheet.getLastRowNum(), HEADER_SCAN_ROWS);
    for (int i = 0; i <= limit; i++) {
      Row r = sheet.getRow(i);
      if (r == null) {
        continue;
      }
      String h = text(r.getCell(2));
      if (h != null && "FIPS code".equalsIgnoreCase(h)) {
        return i;
      }
    }
    throw new IOException("fhfa county hpi: header row ('FIPS code') not found in first "
        + HEADER_SCAN_ROWS + " rows");
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

  /** Numeric cell value; blank and FHFA's "." not-published sentinel are null. */
  private static Double number(Cell c, int rowIdx) throws IOException {
    if (c == null || c.getCellType() == CellType.BLANK) {
      return null;
    }
    if (c.getCellType() == CellType.NUMERIC) {
      return c.getNumericCellValue();
    }
    if (c.getCellType() == CellType.STRING) {
      String s = c.getStringCellValue().trim();
      if (s.isEmpty() || ".".equals(s)) {
        return null;
      }
      try {
        return Double.valueOf(s);
      } catch (NumberFormatException e) {
        throw new IOException("fhfa county hpi: row " + (rowIdx + 1)
            + " non-numeric value '" + s + "'", e);
      }
    }
    throw new IOException("fhfa county hpi: row " + (rowIdx + 1) + " unexpected cell type "
        + c.getCellType());
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
        throw new IOException("interrupted during FHFA retry backoff", e);
      }
    }
    throw last;
  }
}
