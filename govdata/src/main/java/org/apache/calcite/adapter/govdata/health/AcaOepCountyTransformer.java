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
// storage-provider-guard:ignore-file - the zip is a local temp artifact (ZipFile needs random
// access); it is deleted in the finally block and never reaches storage.
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.SkippedBatchException;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Turns one CMS "OEP County-Level Public Use File" (one plan year) into
 * {@code health.aca_oep_county_plan_selections} rows: one row per state and county.
 *
 * <p>The CSV is read by header name, so the column order may differ between plan years; a
 * missing required header fails the batch instead of shifting to a guessed position. The
 * file's trailing {@code Total} row (the HealthCare.gov-states total) is not a county and is
 * dropped. CMS suppresses small cells with {@code *} and complementary cells with {@code +};
 * both are the source's own "not published" markers and load as NULL. Any other
 * non-numeric cell fails the batch.
 */
public class AcaOepCountyTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AcaOepCountyTransformer.class);

  private static final String COL_STATE = "State_Abrvtn";
  private static final String COL_FIPS = "County_FIPS_Cd";

  /** Source header to output column; value type is encoded by the {@link #MONEY} set. */
  private static final Map<String, String> COUNTS = new LinkedHashMap<String, String>();
  private static final Map<String, String> MONEY = new LinkedHashMap<String, String>();

  static {
    COUNTS.put("Cnsmr", "plan_selections");
    COUNTS.put("New_Cnsmr", "new_consumers");
    COUNTS.put("Tot_Renrl", "returning_consumers");
    COUNTS.put("Actv_Renrl", "active_reenrolled");
    COUNTS.put("Auto_Renrl", "auto_reenrolled");
    COUNTS.put("APTC_Cnsmr", "aptc_consumers");
    COUNTS.put("CSR_Cnsmr", "csr_consumers");
    COUNTS.put("Age_0_17", "age_0_17");
    COUNTS.put("Age_18_25", "age_18_25");
    COUNTS.put("Age_26_34", "age_26_34");
    COUNTS.put("Age_35_44", "age_35_44");
    COUNTS.put("Age_45_54", "age_45_54");
    COUNTS.put("Age_55_64", "age_55_64");
    COUNTS.put("Age_GE65", "age_65_plus");
    COUNTS.put("Male", "male");
    COUNTS.put("Female", "female");
    COUNTS.put("Ctstrphc", "metal_catastrophic");
    COUNTS.put("Brnz", "metal_bronze");
    COUNTS.put("Slvr", "metal_silver");
    COUNTS.put("Gld", "metal_gold");
    COUNTS.put("Pltnm", "metal_platinum");
    COUNTS.put("FPL_LT100", "fpl_lt100");
    COUNTS.put("FPL_100_138", "fpl_100_138");
    COUNTS.put("FPL_100_150", "fpl_100_150");
    COUNTS.put("FPL_150_200", "fpl_150_200");
    COUNTS.put("FPL_200_250", "fpl_200_250");
    COUNTS.put("FPL_250_300", "fpl_250_300");
    COUNTS.put("FPL_300_400", "fpl_300_400");
    COUNTS.put("FPL_GT400", "fpl_gt400");
    COUNTS.put("FPL_OTHR", "fpl_other");
    COUNTS.put("Dntl_Cnsmr", "dental_selections");
    MONEY.put("Avg_Prm", "avg_premium");
    MONEY.put("Avg_Prm_Aftr_APTC", "avg_premium_after_aptc");
    MONEY.put("APTC_Cnsmr_Avg_APTC", "avg_aptc");
  }

  private static final HttpClient CLIENT = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(30))
      .build();

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String yearText = context.getDimensionValues().get("year");
    if (yearText == null) {
      throw new IllegalStateException("ACA OEP county: year dimension is required");
    }
    int planYear = Integer.parseInt(yearText.trim());
    String url = context.getUrl();
    Path zip = org.apache.calcite.adapter.file.StagingDirectory
        .newTempFile("aca-oep-", ".zip").toPath();
    try {
      download(url, zip);
      try (ZipFile zf = new ZipFile(zip.toFile())) {
        ZipEntry entry = findCsv(zf, url);
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(zf.getInputStream(entry), StandardCharsets.UTF_8))) {
          List<Map<String, Object>> rows = parse(reader, planYear, url);
          LOGGER.debug("ACA OEP county {}: {} county rows for plan year {}", url, rows.size(),
              planYear);
          return rows.iterator();
        }
      }
    } finally {
      Files.deleteIfExists(zip);
    }
  }

  /** Parses the CSV text of one plan year; package-private for tests. */
  static List<Map<String, Object>> parse(BufferedReader reader, int planYear, String url)
      throws IOException {
    String headerLine = reader.readLine();
    if (headerLine == null) {
      throw new IllegalStateException("ACA OEP county: empty file " + url);
    }
    if (headerLine.startsWith("﻿")) {
      headerLine = headerLine.substring(1);
    }
    List<String> names = splitCsv(headerLine);
    Map<String, Integer> h = new HashMap<String, Integer>();
    for (int i = 0; i < names.size(); i++) {
      h.put(names.get(i).trim(), i);
    }
    List<String> required = new ArrayList<String>();
    required.add(COL_STATE);
    required.add(COL_FIPS);
    required.addAll(COUNTS.keySet());
    required.addAll(MONEY.keySet());
    for (String col : required) {
      if (!h.containsKey(col)) {
        throw new IllegalStateException(
            "ACA OEP county: header has no '" + col + "' column in " + url + "; found "
                + h.keySet());
      }
    }
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.trim().isEmpty()) {
        continue;
      }
      List<String> cells = splitCsv(line);
      String state = cell(cells, h.get(COL_STATE));
      String fips = cell(cells, h.get(COL_FIPS));
      if (state == null || fips == null) {
        throw new IllegalStateException("ACA OEP county: row without state/county: " + line);
      }
      if ("Total".equalsIgnoreCase(state)) {
        continue;
      }
      if (fips.length() != 5 || !fips.chars().allMatch(Character::isDigit)) {
        throw new IllegalStateException("ACA OEP county: bad county FIPS '" + fips + "' in " + url);
      }
      Map<String, Object> r = new LinkedHashMap<String, Object>();
      r.put("plan_year", planYear);
      r.put("state_abbr", state);
      r.put("county_fips", fips);
      for (Map.Entry<String, String> e : COUNTS.entrySet()) {
        r.put(e.getValue(), count(cell(cells, h.get(e.getKey())), e.getKey()));
      }
      for (Map.Entry<String, String> e : MONEY.entrySet()) {
        r.put(e.getValue(), money(cell(cells, h.get(e.getKey())), e.getKey()));
      }
      out.add(r);
    }
    if (out.isEmpty()) {
      throw new IllegalStateException("ACA OEP county: no county rows in " + url);
    }
    return out;
  }

  /** Parses "3,613"; {@code *} and {@code +} are CMS suppression markers and give null. */
  static Integer count(String s, String column) {
    if (s == null || isSuppressed(s)) {
      return null;
    }
    try {
      return Integer.valueOf(s.replace(",", "").trim());
    } catch (NumberFormatException e) {
      throw new IllegalStateException("ACA OEP county: non-numeric '" + s + "' in " + column, e);
    }
  }

  /** Parses "$1,672 " (CMS prints whole dollars); suppression markers give null. */
  static Double money(String s, String column) {
    if (s == null || isSuppressed(s)) {
      return null;
    }
    try {
      return Double.valueOf(s.replace("$", "").replace(",", "").trim());
    } catch (NumberFormatException e) {
      throw new IllegalStateException("ACA OEP county: non-numeric '" + s + "' in " + column, e);
    }
  }

  private static boolean isSuppressed(String s) {
    String t = s.trim();
    return t.equals("*") || t.equals("+");
  }

  private static String cell(List<String> cells, int i) {
    if (i >= cells.size()) {
      return null;
    }
    String t = cells.get(i).trim();
    return t.isEmpty() ? null : t;
  }

  /** RFC 4180 split of one line: commas inside double quotes are data, "" is a quote. */
  static List<String> splitCsv(String line) {
    List<String> out = new ArrayList<String>();
    StringBuilder cur = new StringBuilder();
    boolean quoted = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (quoted) {
        if (c == '"') {
          if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
            cur.append('"');
            i++;
          } else {
            quoted = false;
          }
        } else {
          cur.append(c);
        }
      } else if (c == '"') {
        quoted = true;
      } else if (c == ',') {
        out.add(cur.toString());
        cur.setLength(0);
      } else {
        cur.append(c);
      }
    }
    if (quoted) {
      throw new IllegalStateException("ACA OEP county: unterminated quote in row: " + line);
    }
    out.add(cur.toString());
    return out;
  }

  private static ZipEntry findCsv(ZipFile zf, String url) throws IOException {
    ZipEntry found = null;
    for (Enumeration<? extends ZipEntry> e = zf.entries(); e.hasMoreElements();) {
      ZipEntry entry = e.nextElement();
      if (entry.getName().toLowerCase(Locale.ROOT).endsWith(".csv")) {
        if (found != null) {
          throw new IOException("ACA OEP county: more than one csv in " + url);
        }
        found = entry;
      }
    }
    if (found == null) {
      throw new IOException("ACA OEP county: no csv in " + url);
    }
    return found;
  }

  private static void download(String url, Path target) throws IOException {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMinutes(5))
        .GET()
        .build();
    HttpResponse<InputStream> response;
    try {
      response = CLIENT.send(request, HttpResponse.BodyHandlers.ofInputStream());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted fetching ACA OEP county file: " + url, e);
    }
    int code = response.statusCode();
    try (InputStream body = response.body()) {
      if (code == 404) {
        throw new SkippedBatchException("ACA OEP county file not yet published (HTTP 404): " + url);
      }
      if (code != 200) {
        throw new IOException("ACA OEP county download HTTP " + code + ": " + url);
      }
      Files.copy(body, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }
}
