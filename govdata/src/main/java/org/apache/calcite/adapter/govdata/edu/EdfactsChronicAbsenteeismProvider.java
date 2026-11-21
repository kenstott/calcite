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

import org.apache.calcite.adapter.file.etl.CompleteBody;
import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.NetworkOutageWait;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * DataProvider for {@code edfacts_chronic_absenteeism} — annual chronic absenteeism from the U.S.
 * Department of Education EDFacts collection (file specification FS195, data group DG814, and the
 * derived percent file DG814PCT), published through ED Data Express.
 *
 * <p>Each school year is one zip on
 * {@code https://eddataexpress.ed.gov/resources/reports-and-files/chronic-absenteeism-data}
 * whose file name embeds a publication date, so the URL cannot be templated. The provider reads
 * that page once per JVM and picks the link whose text is
 * {@code School Year yyyy-yyyy Chronic Absenteeism Zip File}. A year with no link is an error.
 *
 * <p>The zip holds state (SEA), district (LEA) and school (SCH) student-count files plus state and
 * district percent files. The school file (about 160 MB uncompressed) is skipped; rows are state,
 * district and the national total. Counts come from the FS195/DG814 files, one row per subgroup
 * and sex. The DG814PCT files carry the all-students denominator (October membership), numerator
 * and rate, joined onto the all-students row. The percent numerator can differ slightly from the
 * count file because of population definition and privacy perturbation, so both are kept.
 * Source codes {@code MISSING} and {@code S} (suppressed) in a value column become null.
 *
 * <p>The {@code year} dimension is the school year's starting calendar year (2022-23 is 2022).
 */
public class EdfactsChronicAbsenteeismProvider implements DataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(EdfactsChronicAbsenteeismProvider.class);

  private static final String PAGE_URL =
      "https://eddataexpress.ed.gov/resources/reports-and-files/chronic-absenteeism-data";

  private static final String BASE = "https://eddataexpress.ed.gov";

  private static final String USER_AGENT =
      "Mozilla/5.0 (X11; Linux x86_64) GovData/1.0";

  private static final Pattern ZIP_LINK = Pattern.compile(
      "href=\"([^\"]+\\.zip)\"[^>]*>\\s*School Year (\\d{4})-\\d{4} Chronic Absenteeism Zip File");

  /** Subgroup code to label; an unlisted code is an error rather than a guess. */
  private static final Map<String, String> SUBGROUPS = new HashMap<String, String>();

  static {
    SUBGROUPS.put("NOCODES", "All students");
    SUBGROUPS.put("ALLSEA", "All students");
    SUBGROUPS.put("ALLLEA", "All students");
    SUBGROUPS.put("AM7", "American Indian or Alaska Native");
    SUBGROUPS.put("AS7", "Asian");
    SUBGROUPS.put("BL7", "Black or African American");
    SUBGROUPS.put("HI7", "Hispanic/Latino");
    SUBGROUPS.put("MU7", "Two or more races");
    SUBGROUPS.put("PI7", "Native Hawaiian or Other Pacific Islander");
    SUBGROUPS.put("WH7", "White");
    SUBGROUPS.put("ECODIS", "Economically disadvantaged");
    SUBGROUPS.put("HOM", "Homeless enrolled");
    SUBGROUPS.put("LEP", "English learner");
    SUBGROUPS.put("WDIS", "Children with disabilities (IDEA)");
    SUBGROUPS.put("DISAB504STAT", "Section 504 status");
  }

  private static Map<Integer, String> zipUrls;

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.trim().isEmpty()) {
      throw new IOException("edfacts_chronic_absenteeism: no year in dimension variables "
          + variables);
    }
    int yearInt = Integer.parseInt(year.trim());
    String url = resolveUrl(yearInt);
    byte[] zip = download(url);
    Map<String, Map<String, Object>> rows = new LinkedHashMap<String, Map<String, Object>>();
    int files = 0;
    try (ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(zip))) {
      ZipEntry entry;
      while ((entry = zin.getNextEntry()) != null) {
        String name = entry.getName().toUpperCase(Locale.ROOT);
        boolean count = name.contains("_FS195_DG814_");
        boolean pct = name.contains("_DG814PCT_");
        if ((!count && !pct) || !name.endsWith(".CSV") || name.contains("_SCH_")) {
          continue;
        }
        String geo = name.contains("_SEA_") ? "state" : name.contains("_LEA_") ? "district" : null;
        if (geo == null) {
          throw new IOException("edfacts_chronic_absenteeism: unrecognised file " + name
              + " in " + url);
        }
        byte[] csv = readEntry(zin);
        if (count) {
          readCounts(csv, geo, yearInt, rows);
        } else {
          readPercents(csv, geo, yearInt, rows);
        }
        files++;
      }
    }
    if (files != 4) {
      throw new IOException("edfacts_chronic_absenteeism: expected 4 SEA/LEA files in " + url
          + " but read " + files);
    }
    LOGGER.info("edfacts_chronic_absenteeism: {} rows for school year {} from {}", rows.size(),
        yearInt, url);
    return new ArrayList<Map<String, Object>>(rows.values()).iterator();
  }

  private static synchronized String resolveUrl(int year) throws IOException {
    if (zipUrls == null) {
      String html = new String(download(PAGE_URL), StandardCharsets.UTF_8);
      Map<Integer, String> urls = new HashMap<Integer, String>();
      Matcher m = ZIP_LINK.matcher(html);
      while (m.find()) {
        String href = m.group(1);
        urls.put(Integer.valueOf(Integer.parseInt(m.group(2))),
            href.startsWith("http") ? href : BASE + href);
      }
      if (urls.isEmpty()) {
        throw new IOException("edfacts_chronic_absenteeism: " + PAGE_URL
            + " lists no chronic absenteeism zip files");
      }
      zipUrls = urls;
    }
    String url = zipUrls.get(Integer.valueOf(year));
    if (url == null) {
      throw new IOException("edfacts_chronic_absenteeism: no chronic absenteeism zip for school "
          + "year " + year + " on " + PAGE_URL + " (found years " + zipUrls.keySet() + ")");
    }
    return url;
  }

  private static void readCounts(byte[] csv, String geo, int year,
      Map<String, Map<String, Object>> rows) throws IOException {
    forEachRecord(csv, geo, (h, v) -> {
      String sub = require(h, v, "subgroup");
      String sex = sex(col(h, v, "characteristics"), sub);
      String leaid = "state".equals(geo) ? null : require(h, v, "leaid");
      Map<String, Object> row = row(rows, geo, year, h, v, leaid, sub, sex);
      row.put("students_chronically_absent", number(col(h, v, "numeric_value")));
    });
  }

  private static void readPercents(byte[] csv, String geo, int year,
      Map<String, Map<String, Object>> rows) throws IOException {
    forEachRecord(csv, geo, (h, v) -> {
      String sub = require(h, v, "subgroup");
      String leaid = "state".equals(geo) ? null : require(h, v, "leaid");
      String state = require(h, v, "state_name");
      String g = "NOCODES".equals(sub) ? "national" : geo;
      Map<String, Object> row = row(rows, g, year, h, v, leaid, sub, "all");
      if ("national".equals(g) && !"UNITED STATES".equals(state)) {
        throw new IOException("edfacts_chronic_absenteeism: NOCODES row for " + state);
      }
      row.put("enrolled", number(col(h, v, "denominator")));
      row.put("percent_numerator", number(col(h, v, "numerator")));
      String rate = col(h, v, "numeric_value");
      row.put("rate_pct", rate == null ? null : Double.valueOf(Double.parseDouble(rate)));
    });
  }

  private static Map<String, Object> row(Map<String, Map<String, Object>> rows, String geo,
      int year, List<String> h, List<String> v, String leaid, String sub, String sex)
      throws IOException {
    String label = SUBGROUPS.get(sub);
    if (label == null) {
      throw new IOException("edfacts_chronic_absenteeism: unknown subgroup code " + sub);
    }
    String state = require(h, v, "state_name");
    String key = geo + "|" + state + "|" + leaid + "|" + sub + "|" + sex;
    Map<String, Object> row = rows.get(key);
    if (row == null) {
      row = new LinkedHashMap<String, Object>();
      row.put("year", Integer.valueOf(year));
      row.put("geo_level", geo);
      row.put("state_name", state);
      row.put("leaid", leaid);
      row.put("lea_name", col(h, v, "lea_name"));
      row.put("subgroup_code", sub);
      row.put("subgroup", label);
      row.put("sex", sex);
      row.put("students_chronically_absent", null);
      row.put("enrolled", null);
      row.put("percent_numerator", null);
      row.put("rate_pct", null);
      rows.put(key, row);
    }
    return row;
  }

  /** Sex from the CHARACTERISTICS column: blank = all, F, M, or the source's MISSING. */
  private static String sex(String characteristics, String sub) throws IOException {
    if (characteristics == null) {
      return "all";
    }
    switch (characteristics) {
    case "F":
      return "female";
    case "M":
      return "male";
    case "MISSING":
      return "missing";
    default:
      throw new IOException("edfacts_chronic_absenteeism: unknown CHARACTERISTICS value "
          + characteristics + " for subgroup " + sub);
    }
  }

  /** Parses a numeric cell; the source's MISSING and S (suppressed) markers become null. */
  private static Long number(String value) {
    if (value == null || "MISSING".equals(value) || "S".equals(value)) {
      return null;
    }
    return Long.valueOf(Long.parseLong(value));
  }

  /** Callback over one CSV record. */
  private interface Record {
    void accept(List<String> headers, List<String> values) throws IOException;
  }

  private static void forEachRecord(byte[] csv, String geo, Record fn) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(csv), StandardCharsets.UTF_8))) {
      List<String> headers = null;
      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.startsWith("﻿")) {
          line = line.substring(1);
        }
        if (headers == null) {
          headers = new ArrayList<String>();
          for (String h : CsvRecordReader.splitFields(line, ',')) {
            headers.add(h.trim().toLowerCase(Locale.ROOT));
          }
          continue;
        }
        if (line.trim().isEmpty()) {
          continue;
        }
        fn.accept(headers, CsvRecordReader.splitFields(line, ','));
      }
      if (headers == null) {
        throw new IOException("edfacts_chronic_absenteeism: empty " + geo + " file");
      }
    }
  }

  private static String require(List<String> h, List<String> v, String name) throws IOException {
    String s = col(h, v, name);
    if (s == null) {
      throw new IOException("edfacts_chronic_absenteeism: row without " + name + ": " + v);
    }
    return s;
  }

  private static String col(List<String> headers, List<String> values, String name) {
    int i = headers.indexOf(name);
    if (i < 0 || i >= values.size()) {
      return null;
    }
    String v = values.get(i).trim();
    return v.isEmpty() ? null : v;
  }

  private static byte[] readEntry(InputStream in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[65536];
    int len;
    while ((len = in.read(buf)) > 0) {
      baos.write(buf, 0, len);
    }
    return baos.toByteArray();
  }

  private static byte[] download(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(180000);
    conn.setRequestProperty("User-Agent", USER_AGENT);
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != HttpURLConnection.HTTP_OK) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = CompleteBody.of(conn)) {
      return readEntry(is);
    }
  }
}
