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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Discovers and parses HRSA's county-level Area Health Resources File (AHRF) release —
 * primary-care-physician supply and population counts for every U.S. county.
 *
 * <p>Two-step process, driven by this single transformer since the release's download
 * filename embeds a two-year range (e.g. {@code AHRF_2024-2025_CSV.zip}) with an
 * inconsistent separator across releases — space, underscore, or none — so no single
 * {@code {year}}-templatable URL pattern exists (confirmed live against
 * {@code data.hrsa.gov/data/download} 2026-08-02: the current file sits alongside
 * older releases named {@code "AHRF 2023-2024 CSV.zip"} and {@code
 * "AHRF_CSV_2022-2023.zip"}): (1) the framework fetches HRSA's download-listing page
 * and hands its HTML body to {@link #transform}; this method scans it for AHRF's
 * county-level CSV zip link (excluding the sibling State-and-National "SN" file and the
 * SAS/technical-documentation distributions) with the highest embedded release year;
 * (2) downloads and unzips that file, which contains eight topical CSVs, all row-aligned
 * one-to-one by 5-digit county FIPS ({@code fips_st_cnty}) — this transformer reads three
 * of them: {@code *geo.csv} (county/state names, rural-urban classification), {@code
 * *hp.csv} (health-profession counts), and {@code *pop.csv} (population estimates).
 *
 * <p>AHRF is a wide file: each variable is stored as a family of year-suffixed columns
 * (e.g. {@code phys_nf_prim_care_pc_exc_rsdt_23}, {@code phys_nf_prim_care_pc_exc_rsdt_22},
 * ...) rather than one row per year, and each annual release adds a new newest-year column
 * while keeping the older ones. Rather than hardcoding a suffix that would silently go
 * stale on the next release, this transformer scans the header for the highest-numbered
 * suffix present for each variable family at load time and records which data year it
 * picked ({@code physician_data_year}, {@code population_data_year}) so downstream readers
 * know exactly what the counts reflect.
 *
 * <p>Verified live against the real 2024-2025 release (downloaded 2026-08-02,
 * {@code AHRF2025hp.csv} / {@code AHRF2025pop.csv} / {@code AHRF2025geo.csv}, 3,235
 * county rows each, identical row order and fips_st_cnty key across all three files):
 * the newest available primary-care-physician column is {@code
 * phys_nf_prim_care_pc_exc_rsdt_23} ("Phys,Primary Care, Patient Care" / "Non-Fed;Excl
 * Hsp Res &amp; 75+ Yrs", non-federal MD+DO physicians in patient care excluding hospital
 * residents and physicians 75+, sourced from the AMA Physician Masterfile per HRSA's own
 * Technical Documentation workbook) and the newest population column is {@code
 * popn_est_24} (2024 Census county population estimate). AHRF ships raw counts only — no
 * physicians-per-100k rate column exists in the source — so this transformer computes the
 * rate directly from the matched count and population columns.
 */
public class AhrfPhysicianSupplyTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AhrfPhysicianSupplyTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern HREF_PATTERN = Pattern.compile("href=\"([^\"]+)\"", Pattern.CASE_INSENSITIVE);
  private static final Pattern YEAR_PATTERN = Pattern.compile("(19|20)\\d{2}");

  private static final String PRIMARY_CARE_PREFIX = "phys_nf_prim_care_pc_exc_rsdt_";
  private static final String MD_ACTIVE_PREFIX = "md_nf_activ_";
  private static final String DO_ACTIVE_PREFIX = "do_nf_activ_";
  private static final String POPULATION_PREFIX = "popn_est_";
  private static final String RURAL_URBAN_PREFIX = "rural_urban_contnm_";

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("AHRF: Empty download-page response from {}", context.getUrl());
      return "[]";
    }

    try {
      String zipUrl = findCountyCsvZipUrl(response);
      if (zipUrl == null) {
        LOGGER.warn("AHRF: no county-level CSV zip link found on {}", context.getUrl());
        return "[]";
      }

      byte[] zipBytes = downloadBytes(zipUrl);
      return parseZip(zipBytes);

    } catch (Exception e) {
      throw new RuntimeException("AHRF: Failed to parse response for " + context.getUrl(), e);
    }
  }

  /**
   * Scans the HRSA download-listing HTML for AHRF's county-level CSV distribution and
   * returns the absolute URL of the one with the highest embedded release year. Excludes
   * the State-and-National ("SN") sibling file and the SAS/technical-documentation
   * distributions, which never match on the CSV + not-SN filter.
   */
  private String findCountyCsvZipUrl(String html) {
    Matcher m = HREF_PATTERN.matcher(html);
    String bestHref = null;
    int bestYear = -1;
    while (m.find()) {
      String href = m.group(1);
      String normalized = href.toUpperCase(Locale.ROOT).replace("%20", "_");
      if (!normalized.contains("DATADOWNLOAD") || !normalized.contains("AHRF")
          || !normalized.contains("CSV") || !normalized.endsWith(".ZIP")) {
        continue;
      }
      if (normalized.contains("AHRF_SN") || normalized.contains("_SN_")) {
        continue; // State-and-National file, not the county-level file we want
      }
      int year = maxYear(href);
      if (year > bestYear) {
        bestYear = year;
        bestHref = href;
      }
    }
    if (bestHref == null) {
      return null;
    }
    String decoded = bestHref.replace("&amp;", "&");
    return decoded.startsWith("http") ? decoded : "https://data.hrsa.gov" + decoded;
  }

  private int maxYear(String s) {
    Matcher m = YEAR_PATTERN.matcher(s);
    int best = -1;
    while (m.find()) {
      int y = Integer.parseInt(m.group());
      if (y > best) {
        best = y;
      }
    }
    return best;
  }

  private String parseZip(byte[] zipBytes) throws IOException {
    byte[] geoCsv = null;
    byte[] hpCsv = null;
    byte[] popCsv = null;

    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName().toLowerCase(Locale.ROOT);
        if (name.endsWith("geo.csv")) {
          geoCsv = readAll(zis);
        } else if (name.endsWith("hp.csv")) {
          hpCsv = readAll(zis);
        } else if (name.endsWith("pop.csv")) {
          popCsv = readAll(zis);
        }
      }
    }

    if (geoCsv == null || hpCsv == null || popCsv == null) {
      LOGGER.warn("AHRF: zip missing one of the expected CSVs (geo={}, hp={}, pop={})",
          geoCsv != null, hpCsv != null, popCsv != null);
      return "[]";
    }

    CsvTable geo = readCsv(geoCsv);
    CsvTable hp = readCsv(hpCsv);
    CsvTable pop = readCsv(popCsv);

    int geoFipsIdx = requireIndex(geo.headers, "fips_st_cnty");
    int geoStateFipsIdx = requireIndex(geo.headers, "fips_st");
    int geoStateAbbrIdx = requireIndex(geo.headers, "st_name_abbrev");
    int geoStateNameIdx = requireIndex(geo.headers, "st_name");
    int geoCountyNameIdx = requireIndex(geo.headers, "cnty_name");
    String ruralUrbanCol = findLatestYearColumn(geo.headers, RURAL_URBAN_PREFIX);
    int ruralUrbanIdx = ruralUrbanCol == null ? -1 : geo.headers.indexOf(ruralUrbanCol);

    int hpFipsIdx = requireIndex(hp.headers, "fips_st_cnty");
    String primaryCareCol = findLatestYearColumn(hp.headers, PRIMARY_CARE_PREFIX);
    if (primaryCareCol == null) {
      throw new IllegalStateException("AHRF: no " + PRIMARY_CARE_PREFIX
          + "YY column found in the hp CSV header — HRSA may have renamed the field");
    }
    int primaryCareIdx = hp.headers.indexOf(primaryCareCol);
    int physicianDataYear = 2000 + Integer.parseInt(primaryCareCol.substring(PRIMARY_CARE_PREFIX.length()));

    // Total non-federal active physicians (all specialties) is a two-column sum (MD + DO).
    // Reuse the primary-care metric's data year rather than re-deriving independently, since
    // both column families are refreshed together each release; if a future release drops
    // either specific column the corresponding count is simply left null (col() returns -1).
    int mdActiveIdx = hp.headers.indexOf(MD_ACTIVE_PREFIX + (physicianDataYear - 2000));
    int doActiveIdx = hp.headers.indexOf(DO_ACTIVE_PREFIX + (physicianDataYear - 2000));

    int popFipsIdx = requireIndex(pop.headers, "fips_st_cnty");
    String populationCol = findLatestYearColumn(pop.headers, POPULATION_PREFIX);
    if (populationCol == null) {
      throw new IllegalStateException("AHRF: no " + POPULATION_PREFIX
          + "YY column found in the pop CSV header — HRSA may have renamed the field");
    }
    int populationIdx = pop.headers.indexOf(populationCol);
    int populationDataYear = 2000 + Integer.parseInt(populationCol.substring(POPULATION_PREFIX.length()));

    Map<String, List<String>> hpByFips = indexByColumn(hp, hpFipsIdx);
    Map<String, List<String>> popByFips = indexByColumn(pop, popFipsIdx);

    ArrayNode out = MAPPER.createArrayNode();
    for (List<String> geoRow : geo.rows) {
      String fips = value(geoRow, geoFipsIdx);
      if (fips == null) {
        continue;
      }
      List<String> hpRow = hpByFips.get(fips);
      List<String> popRow = popByFips.get(fips);

      ObjectNode row = MAPPER.createObjectNode();
      put(row, "county_fips", fips);
      put(row, "state_fips", value(geoRow, geoStateFipsIdx));
      put(row, "state_abbr", value(geoRow, geoStateAbbrIdx));
      put(row, "state_name", value(geoRow, geoStateNameIdx));
      put(row, "county_name", value(geoRow, geoCountyNameIdx));
      put(row, "rural_urban_code", ruralUrbanIdx < 0 ? null : value(geoRow, ruralUrbanIdx));

      Long populationEstimate = popRow == null ? null : parseLong(value(popRow, populationIdx));
      putLong(row, "population_estimate", populationEstimate);
      row.put("population_data_year", populationDataYear);

      Long primaryCarePhysicians = hpRow == null ? null : parseLong(value(hpRow, primaryCareIdx));
      putLong(row, "primary_care_physicians", primaryCarePhysicians);
      row.put("physician_data_year", physicianDataYear);
      putDouble(row, "primary_care_physicians_per_100k", rate(primaryCarePhysicians, populationEstimate));

      Long mdActive = (hpRow == null || mdActiveIdx < 0) ? null : parseLong(value(hpRow, mdActiveIdx));
      Long doActive = (hpRow == null || doActiveIdx < 0) ? null : parseLong(value(hpRow, doActiveIdx));
      Long totalActive = sumNullable(mdActive, doActive);
      putLong(row, "total_active_physicians", totalActive);
      putDouble(row, "total_active_physicians_per_100k", rate(totalActive, populationEstimate));

      out.add(row);
    }

    LOGGER.debug("AHRF: Parsed {} county rows (physician_data_year={}, population_data_year={})",
        out.size(), physicianDataYear, populationDataYear);
    return out.toString();
  }

  /**
   * Finds the header matching {@code prefix + "YY"} (a 2-digit year suffix) with the
   * highest YY, or {@code null} if no such column exists.
   */
  private String findLatestYearColumn(List<String> headers, String prefix) {
    String best = null;
    int bestYear = -1;
    for (String h : headers) {
      if (h.length() == prefix.length() + 2 && h.startsWith(prefix)) {
        String suffix = h.substring(prefix.length());
        if (Character.isDigit(suffix.charAt(0)) && Character.isDigit(suffix.charAt(1))) {
          int yy = Integer.parseInt(suffix);
          if (yy > bestYear) {
            bestYear = yy;
            best = h;
          }
        }
      }
    }
    return best;
  }

  private int requireIndex(List<String> headers, String name) {
    int idx = headers.indexOf(name);
    if (idx < 0) {
      throw new IllegalStateException("AHRF: expected column '" + name + "' not found in CSV header");
    }
    return idx;
  }

  private Map<String, List<String>> indexByColumn(CsvTable table, int keyIdx) {
    Map<String, List<String>> map = new HashMap<>(table.rows.size() * 2);
    for (List<String> row : table.rows) {
      String key = value(row, keyIdx);
      if (key != null) {
        map.put(key, row);
      }
    }
    return map;
  }

  private String value(List<String> row, int idx) {
    if (idx < 0 || idx >= row.size()) {
      return null;
    }
    String v = row.get(idx).trim();
    return v.isEmpty() ? null : v;
  }

  private Long parseLong(String s) {
    if (s == null) {
      return null;
    }
    try {
      return Long.parseLong(s);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private Long sumNullable(Long a, Long b) {
    if (a == null && b == null) {
      return null;
    }
    return (a == null ? 0L : a) + (b == null ? 0L : b);
  }

  private Double rate(Long count, Long population) {
    if (count == null || population == null || population == 0L) {
      return null;
    }
    return (count * 100000.0) / population;
  }

  private void put(ObjectNode row, String key, String value) {
    if (value == null) {
      row.putNull(key);
    } else {
      row.put(key, value);
    }
  }

  private void putLong(ObjectNode row, String key, Long value) {
    if (value == null) {
      row.putNull(key);
    } else {
      row.put(key, value.longValue());
    }
  }

  private void putDouble(ObjectNode row, String key, Double value) {
    if (value == null) {
      row.putNull(key);
    } else {
      row.put(key, value.doubleValue());
    }
  }

  private CsvTable readCsv(byte[] csvBytes) throws IOException {
    List<String> headers;
    List<List<String>> rows = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(csvBytes), StandardCharsets.UTF_8))) {
      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        return new CsvTable(Collections.<String>emptyList(), rows);
      }
      headers = CsvRecordReader.splitFields(headerLine, ',');

      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }
        rows.add(CsvRecordReader.splitFields(line, ','));
      }
    }
    return new CsvTable(headers, rows);
  }

  private byte[] readAll(InputStream is) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[65536];
    int len;
    while ((len = is.read(buf)) > 0) {
      baos.write(buf, 0, len);
    }
    return baos.toByteArray();
  }

  private byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(180000);
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

  /** Minimal header + row holder for a parsed CSV file. */
  private static final class CsvTable {
    final List<String> headers;
    final List<List<String>> rows;

    CsvTable(List<String> headers, List<List<String>> rows) {
      this.headers = headers;
      this.rows = rows;
    }
  }
}
