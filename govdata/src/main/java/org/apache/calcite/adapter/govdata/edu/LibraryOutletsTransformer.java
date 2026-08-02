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
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
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
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Discovers and parses IMLS's annual Public Libraries Survey (PLS) "outlet" CSV — one row per
 * physical library outlet (central, branch, or bookmobile stop), the only PLS grain that carries
 * square footage.
 *
 * <p>Three-step process, all driven by this single transformer since IMLS lists every fiscal
 * year's download on one static landing page (no per-year URL) and the ZIP path segment is a
 * publish-date stamp with no relation to the survey year (confirmed live: FY2024's ZIP is under
 * {@code /sites/default/files/2026-06/...}, FY2023's under {@code .../2025-08/...}): (1) the
 * framework fetches the landing page ({@code imls.gov/research-evaluation/surveys/public-libraries-survey-pls})
 * and hands its HTML to {@link #transform}; this method Jsoup-selects the anchor whose href
 * matches {@code pls_fy{year}_csv.zip}; (2) downloads that ZIP; (3) inside it, picks the entry
 * whose name contains "outlet" (case-insensitive — the ZIP always also contains an
 * administrative-entity-level CSV with no square-footage column, plus a README), and parses it
 * with {@link CsvRecordReader} (RFC4180-aware; a naive comma split misaligns rows here too, since
 * address fields carry quoted embedded commas).
 *
 * <p>Verified live against the real FY2024 file (downloaded 2026-08-02,
 * {@code pls_fy24_outlet_pud24i.csv}, 17,615 records): (STABR, FSCSKEY, FSCS_SEQ) is unique
 * across every row (0 duplicates) and is used as the outlet identity instead of the source's own
 * LIBID field, which is NOT reliably unique/self-consistent in the raw data (confirmed: 1,049
 * LIBID values are shared across two different FSCSKEY library systems in the FY2024 file — an
 * upstream data-quality artifact, not a parsing bug). SQ_FEET carries NCES sentinel codes
 * (-1/-3/-4) for missing/not-applicable/not-reported, which this transformer nulls out rather
 * than passing through as if they were square-foot counts. county_fips is derived from the
 * first 5 digits of CENTRACT (an 11-digit Census tract FIPS: 2 state + 3 county + 6 tract),
 * since the file carries no county FIPS column directly.
 */
public class LibraryOutletsTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(LibraryOutletsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String IMLS_BASE = "https://www.imls.gov";

  @Override public String transform(String response, RequestContext context) {
    String yearStr = context.getDimensionValues().get("year");
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Library Outlets: Empty landing page response for year={}", yearStr);
      return "[]";
    }

    try {
      String zipUrl = findZipUrl(response, yearStr);
      if (zipUrl == null) {
        LOGGER.warn("Library Outlets: no pls_fy{}_csv.zip link found on {}", yearStr,
            context.getUrl());
        return "[]";
      }

      byte[] zipBytes = downloadBytes(zipUrl);
      byte[] outletCsvBytes = extractOutletFile(zipBytes);
      if (outletCsvBytes == null) {
        LOGGER.warn("Library Outlets: no 'outlet' .csv entry found in {} for year={}",
            zipUrl, yearStr);
        return "[]";
      }

      return parseCsv(outletCsvBytes, yearStr);

    } catch (Exception e) {
      throw new RuntimeException("Library Outlets: Failed to parse response for "
          + context.getUrl() + " (year=" + yearStr + ")", e);
    }
  }

  private String findZipUrl(String landingHtml, String yearStr) {
    Document doc = Jsoup.parse(landingHtml);
    Elements links = doc.select("a[href$=pls_fy" + yearStr + "_csv.zip]");
    if (links.isEmpty()) {
      return null;
    }
    String href = links.first().attr("href");
    return href.startsWith("http") ? href : IMLS_BASE + href;
  }

  private byte[] extractOutletFile(byte[] zipBytes) throws IOException {
    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName().toLowerCase(java.util.Locale.ROOT);
        if (!name.endsWith(".csv") || !name.contains("outlet")) {
          continue;
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[65536];
        int len;
        while ((len = zis.read(buf)) > 0) {
          baos.write(buf, 0, len);
        }
        return baos.toByteArray();
      }
    }
    return null;
  }

  private String parseCsv(byte[] csvBytes, String yearStr) throws IOException {
    Integer year = parseIntOrNull(yearStr);
    ArrayNode out = MAPPER.createArrayNode();

    // PLS CSVs are not valid UTF-8 (Windows-1252-era exports); ISO-8859-1 never throws and
    // preserves every byte, which is what we need for a handful of accented facility names.
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(csvBytes), StandardCharsets.ISO_8859_1))) {
      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        return "[]";
      }
      List<String> headers = CsvRecordReader.splitFields(headerLine, ',');

      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }
        List<String> values = CsvRecordReader.splitFields(line, ',');
        ObjectNode row = MAPPER.createObjectNode();
        mapRow(headers, values, year, row);
        out.add(row);
      }
    }
    LOGGER.debug("Library Outlets: Parsed {} rows for year={}", out.size(), yearStr);
    return out.toString();
  }

  private void mapRow(List<String> headers, List<String> values, Integer year, ObjectNode row) {
    String state = col(headers, values, "STABR");
    String fscsKey = col(headers, values, "FSCSKEY");
    String fscsSeq = col(headers, values, "FSCS_SEQ");
    String libId = col(headers, values, "LIBID");
    String libName = col(headers, values, "LIBNAME");
    String outletType = col(headers, values, "C_OUT_TY");
    String address = col(headers, values, "ADDRESS");
    String city = col(headers, values, "CITY");
    String zip = col(headers, values, "ZIP");
    String county = col(headers, values, "CNTY");
    String centract = col(headers, values, "CENTRACT");
    String sqFeet = col(headers, values, "SQ_FEET");
    String latitude = col(headers, values, "LATITUDE");
    String longitude = col(headers, values, "LONGITUD");

    row.put("year", year);
    put(row, "state", state);
    put(row, "fscs_key", fscsKey);
    put(row, "fscs_seq", fscsSeq);
    put(row, "library_id", libId);
    put(row, "library_name", libName);
    put(row, "outlet_type", outletType);
    put(row, "address", address);
    put(row, "city", city);
    put(row, "zip_code", zip);
    put(row, "county_name", county);

    String countyFips = null;
    if (centract != null && centract.length() >= 5 && isDigits(centract)) {
      countyFips = centract.substring(0, 5);
    }
    put(row, "county_fips", countyFips);

    Long sqFeetLong = parseLongOrNull(sqFeet);
    if (sqFeetLong != null && sqFeetLong >= 0) {
      row.put("square_feet", sqFeetLong);
    } else {
      row.putNull("square_feet");
    }

    putDouble(row, "latitude", latitude);
    putDouble(row, "longitude", longitude);
  }

  private boolean isDigits(String s) {
    for (int i = 0; i < s.length(); i++) {
      if (!Character.isDigit(s.charAt(i))) {
        return false;
      }
    }
    return true;
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

  private void put(ObjectNode row, String key, String value) {
    if (value == null) {
      row.putNull(key);
    } else {
      row.put(key, value);
    }
  }

  private void putDouble(ObjectNode row, String key, String value) {
    if (value == null) {
      row.putNull(key);
      return;
    }
    try {
      row.put(key, Double.parseDouble(value));
    } catch (NumberFormatException e) {
      row.putNull(key);
    }
  }

  private Long parseLongOrNull(String s) {
    if (s == null) {
      return null;
    }
    try {
      return Long.parseLong(s.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private Integer parseIntOrNull(String s) {
    if (s == null) {
      return null;
    }
    try {
      return Integer.parseInt(s.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != 200) {
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
