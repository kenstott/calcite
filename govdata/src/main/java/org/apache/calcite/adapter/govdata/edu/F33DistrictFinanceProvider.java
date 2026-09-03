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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code f33_district_finance} — Census Bureau Annual Survey of School System
 * Finances ("F-33"), individual-unit file: one row per public school district (LEA) per fiscal
 * year, carrying real dollar revenue/expenditure figures (ccd_districts, by contrast, has no
 * finance data at all — see that table's comment).
 *
 * <p>The Census Bureau publishes one comma-delimited file per fiscal year at a predictable URL,
 * {@code https://www2.census.gov/programs-surveys/school-finances/tables/{year}/
 * secondary-education-finance/elsec{yy}t.txt} where {@code {year}} is the 4-digit fiscal year and
 * {@code {yy}} its last two digits (e.g. FY2023 -&gt; {@code .../tables/2023/.../elsec23t.txt}).
 * Verified live 2026-08-21 back to FY2000 and forward through FY2024 (the latest published at
 * that date); FY2025 was not yet published, consistent with this survey's ~2-year lag (see
 * {@code dataLag: 2} on the schema's year dimension). A DataProvider (rather than a URL-templated
 * {@code source:} + responseTransformer) is used here because the URL needs both the 4-digit
 * fiscal year (path segment) and its 2-digit form (filename segment), which the framework's
 * simple {@code {year}} URL substitution cannot produce on its own.
 *
 * <p>The file's {@code NCESID} column is the Census Bureau's own copy of the NCES LEA ID (7-digit
 * string, 2-digit state FIPS + 5-digit in-state agency number) and is used here as {@code leaid},
 * joining directly to {@code ccd_districts.leaid}. Confirmed live against the FY2023 file
 * (14,088 rows): NCESID is unique per row (0 duplicates), always 7 digits, and none of
 * TOTALREV/TOTALEXP/TCURSPND/PPCSTOT/ENROLL carried a negative value in that file (unlike CCD,
 * which uses negative sentinel codes for missing/not-applicable) — this provider does not attempt
 * to detect or null out sentinel codes, since none were observed. All revenue/expenditure amounts
 * except the per-pupil columns are in thousands of dollars, per the source's own documentation
 * (school{yy}doc.docx); per-pupil columns (PPCSTOT/PPITOTAL/PPSTOTAL) are the source's own
 * per-pupil dollar figures, not derived here.
 */
public class F33DistrictFinanceProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(F33DistrictFinanceProvider.class);

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.isEmpty()) {
      LOGGER.warn("f33_district_finance: no year in dimension variables {}", variables);
      return Collections.emptyIterator();
    }
    int yearInt;
    try {
      yearInt = Integer.parseInt(year.trim());
    } catch (NumberFormatException e) {
      LOGGER.warn("f33_district_finance: non-numeric year '{}'", year);
      return Collections.emptyIterator();
    }
    String yy = String.format(java.util.Locale.ROOT, "%02d", yearInt % 100);
    String url = "https://www2.census.gov/programs-surveys/school-finances/tables/" + yearInt
        + "/secondary-education-finance/elsec" + yy + "t.txt";

    byte[] bytes;
    try {
      bytes = downloadBytes(url);
    } catch (IOException e) {
      if (e.getMessage() != null && e.getMessage().startsWith("HTTP 404")) {
        LOGGER.info("f33_district_finance: no F-33 file published yet for FY{} ({}) — skipping",
            yearInt, url);
        return Collections.emptyIterator();
      }
      throw e;
    }

    List<Map<String, Object>> rows = parseRows(bytes, yearInt);
    LOGGER.info("f33_district_finance: {} district rows for FY{}", rows.size(), yearInt);
    return rows.iterator();
  }

  private List<Map<String, Object>> parseRows(byte[] csvBytes, int year) throws IOException {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(csvBytes), StandardCharsets.ISO_8859_1))) {
      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        return rows;
      }
      // The delimiter is not stable across fiscal years despite the .txt name: FY2019's file is
      // tab-separated where its neighbours are comma-separated. Reading it with a fixed comma
      // yields one field per line, so every column but the first parses as absent. Decide from
      // whichever character actually separates the header's column names.
      final char delimiter = countChar(headerLine, '\t') > countChar(headerLine, ',') ? '\t' : ',';
      List<String> headers = CsvRecordReader.splitFields(headerLine, delimiter);

      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }
        List<String> values = CsvRecordReader.splitFields(line, delimiter);
        Map<String, Object> row = mapRow(headers, values, year);
        if (row != null) {
          rows.add(row);
        }
      }
    }
    return rows;
  }

  private Map<String, Object> mapRow(List<String> headers, List<String> values, int year) {
    String leaid = col(headers, values, "NCESID");
    if (leaid == null || leaid.isEmpty()) {
      return null;
    }

    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("leaid", leaid);
    row.put("year", Integer.valueOf(year));
    putString(row, "pid6", col(headers, values, "PID6"));
    putString(row, "unit_type", col(headers, values, "UNIT_TYPE"));
    String countyFips = col(headers, values, "CONUM");
    String stateFips = col(headers, values, "FIPST");
    if (stateFips == null && countyFips != null && countyFips.length() >= 2) {
      // FY2021 and earlier publish a narrower header (IDCENSUS, NAME, CONUM, ...) with no FIPST
      // column at all. A county FIPS is by construction the 2-digit state FIPS followed by the
      // 3-digit county code, so CONUM carries the state identity those years otherwise lack.
      // IDCENSUS is NOT usable for this: it leads with the Census Bureau's own alphabetical state
      // code, which diverges from FIPS above Arkansas (Census 05 = California, FIPS 06).
      stateFips = countyFips.substring(0, 2);
    }
    putString(row, "state_fips", stateFips);
    putString(row, "county_fips", countyFips);
    putString(row, "district_name", col(headers, values, "NAME"));
    putString(row, "cbsa", col(headers, values, "CBSA"));
    putString(row, "school_level", col(headers, values, "SCHLEV"));

    putLong(row, "enrollment", col(headers, values, "ENROLL"));

    putDouble(row, "total_revenue_thousand", col(headers, values, "TOTALREV"));
    putDouble(row, "federal_revenue_thousand", col(headers, values, "TFEDREV"));
    putDouble(row, "state_revenue_thousand", col(headers, values, "TSTREV"));
    putDouble(row, "local_revenue_thousand", col(headers, values, "TLOCREV"));

    putDouble(row, "total_expenditure_thousand", col(headers, values, "TOTALEXP"));
    putDouble(row, "current_expenditure_thousand", col(headers, values, "TCURSPND"));
    putDouble(row, "instruction_expenditure_thousand", col(headers, values, "TCURINST"));
    putDouble(row, "support_services_expenditure_thousand", col(headers, values, "TCURSSVC"));
    putDouble(row, "capital_outlay_expenditure_thousand", col(headers, values, "TCAPOUT"));
    putDouble(row, "interest_on_debt_thousand", col(headers, values, "TINTRST"));
    putDouble(row, "long_term_debt_outstanding_thousand", col(headers, values, "DEBTOUT"));

    putDouble(row, "per_pupil_current_expenditure", col(headers, values, "PPCSTOT"));
    putDouble(row, "per_pupil_instruction_expenditure", col(headers, values, "PPITOTAL"));
    putDouble(row, "per_pupil_support_services_expenditure", col(headers, values, "PPSTOTAL"));

    return row;
  }

  private static int countChar(String s, char c) {
    int n = 0;
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) == c) {
        n++;
      }
    }
    return n;
  }

  private String col(List<String> headers, List<String> values, String name) {
    for (int i = 0; i < headers.size(); i++) {
      if (headers.get(i).equalsIgnoreCase(name)) {
        if (i < values.size()) {
          String v = values.get(i).trim();
          return v.isEmpty() ? null : v;
        }
        return null;
      }
    }
    return null;
  }

  private void putString(Map<String, Object> row, String key, String value) {
    row.put(key, value);
  }

  private void putLong(Map<String, Object> row, String key, String value) {
    if (value == null) {
      row.put(key, null);
      return;
    }
    try {
      row.put(key, Long.valueOf(Long.parseLong(value)));
    } catch (NumberFormatException e) {
      row.put(key, null);
    }
  }

  private void putDouble(Map<String, Object> row, String key, String value) {
    if (value == null) {
      row.put(key, null);
      return;
    }
    try {
      row.put(key, Double.valueOf(Double.parseDouble(value)));
    } catch (NumberFormatException e) {
      row.put(key, null);
    }
  }

  private byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != HttpURLConnection.HTTP_OK) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = conn.getInputStream();
         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
      return baos.toByteArray();
    }
  }
}
