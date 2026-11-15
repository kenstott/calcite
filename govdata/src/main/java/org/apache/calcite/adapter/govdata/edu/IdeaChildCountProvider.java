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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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

/**
 * DataProvider for {@code district_special_ed_counts} — IDEA Section 618 Part B Child Count by
 * LEA and disability category (U.S. Department of Education, OSEP), one row per LEA per school
 * year. Replaces the {@code spec_ed_students} field the Urban Institute CCD directory stopped
 * populating from 2022.
 *
 * <p>Each school year is a separate data.ed.gov resource whose URL carries a random UUID, so the
 * URL cannot be templated. The provider reads the Department's DCAT catalog
 * ({@code https://data.ed.gov/data.json}) once per JVM and picks the distribution whose file
 * name is {@code bchildcountdisabilitycategorylea{yyyy}-{yy}.csv} (2022-23 onward) or
 * {@code bchildcountandedenvironmentlea{yyyy}-{yy}.csv} (2020-21, 2021-22), so a newly
 * published school year is found without a code change. A year with no catalog entry is an error,
 * not an empty result.
 *
 * <p>The {@code year} dimension is the school year's starting calendar year (2022-23 is 2022),
 * the same convention as {@code ccd_districts.year}. File layout differs between years and is
 * handled by header name rather than position: the header row is the one starting with
 * "School Year" (after 0 or 4 metadata rows), column names vary in letter case, and the 2024-25
 * file has no "Missing" columns. NCES LEA IDs lose their leading zero in the 2021-22 file and
 * are left-padded to 7 digits. Suppression codes -8 (small cell) and -9 (questionable quality)
 * become null. Verified live 2026-11-15: no blank or duplicate NCES LEA IDs in any of the five
 * files, and the 2021-22 Texas total (635,327) equals the CCD value for 2021.
 */
public class IdeaChildCountProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdeaChildCountProvider.class);

  private static final String CATALOG_URL = "https://data.ed.gov/data.json";

  private static final Pattern FILE_NAME =
      Pattern.compile("^bchildcount(?:disabilitycategory|andedenvironment)lea(\\d{4})-\\d{2}\\.csv$");

  /** Disability categories in the order the source lists them. */
  private static final String[][] CATEGORIES = {
      {"all_disabilities", "all disabilities"},
      {"autism", "autism"},
      {"deaf_blindness", "deaf-blindness"},
      {"developmental_delay", "developmental delay"},
      {"emotional_disturbance", "emotional disturbance"},
      {"hearing_impairment", "hearing impairment"},
      {"intellectual_disability", "intellectual disability"},
      {"multiple_disabilities", "multiple disabilities"},
      {"orthopedic_impairment", "orthopedic impairment"},
      {"other_health_impairment", "other health impairment"},
      {"specific_learning_disability", "specific learning disability"},
      {"speech_language_impairment", "speech or language impairment"},
      {"traumatic_brain_injury", "traumatic brain injury"},
      {"visual_impairment", "visual impairment"},
      {"missing", "missing"},
  };

  private static Map<Integer, String> catalogUrls;

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.trim().isEmpty()) {
      throw new IOException("district_special_ed_counts: no year in dimension variables "
          + variables);
    }
    int yearInt = Integer.parseInt(year.trim());

    String url = resolveUrl(yearInt);
    byte[] bytes = downloadBytes(url);
    List<Map<String, Object>> rows = parseRows(bytes, yearInt, url);
    LOGGER.info("district_special_ed_counts: {} LEA rows for school year {}-{} from {}",
        rows.size(), yearInt, (yearInt + 1) % 100, url);
    return rows.iterator();
  }

  private static synchronized String resolveUrl(int year) throws IOException {
    if (catalogUrls == null) {
      catalogUrls = loadCatalog();
    }
    String url = catalogUrls.get(Integer.valueOf(year));
    if (url == null) {
      throw new IOException("district_special_ed_counts: no IDEA Part B Child Count LEA file for "
          + "school year " + year + " in " + CATALOG_URL + " (found years " + catalogUrls.keySet()
          + ")");
    }
    return url;
  }

  private static Map<Integer, String> loadCatalog() throws IOException {
    JsonNode root = new ObjectMapper().readTree(downloadBytes(CATALOG_URL));
    Map<Integer, String> urls = new HashMap<Integer, String>();
    for (JsonNode dataset : root.path("dataset")) {
      for (JsonNode dist : dataset.path("distribution")) {
        String u = dist.path("downloadURL").asText("");
        String name = u.substring(u.lastIndexOf('/') + 1).toLowerCase(Locale.ROOT);
        Matcher m = FILE_NAME.matcher(name);
        if (m.matches()) {
          urls.put(Integer.valueOf(Integer.parseInt(m.group(1))), u);
        }
      }
    }
    if (urls.isEmpty()) {
      throw new IOException("district_special_ed_counts: " + CATALOG_URL
          + " lists no bchildcount*lea CSV distributions");
    }
    return urls;
  }

  private List<Map<String, Object>> parseRows(byte[] csvBytes, int year, String url)
      throws IOException {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(csvBytes), StandardCharsets.UTF_8))) {
      List<String> headers = null;
      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.startsWith("﻿")) {
          line = line.substring(1);
        }
        if (headers == null) {
          // Metadata rows (extraction date, suppression legend) precede the header in most years.
          if (line.startsWith("School Year,")) {
            headers = new ArrayList<String>();
            for (String h : CsvRecordReader.splitFields(line, ',')) {
              headers.add(h.trim().toLowerCase(Locale.ROOT));
            }
          }
          continue;
        }
        if (line.trim().isEmpty()) {
          continue;
        }
        rows.add(mapRow(headers, CsvRecordReader.splitFields(line, ','), year));
      }
      if (headers == null) {
        throw new IOException("district_special_ed_counts: no 'School Year' header row in "
            + url);
      }
    }
    return rows;
  }

  private Map<String, Object> mapRow(List<String> headers, List<String> values, int year)
      throws IOException {
    String leaid = col(headers, values, "nces lea id");
    if (leaid == null) {
      throw new IOException("district_special_ed_counts: row without NCES LEA ID for " + year
          + ": " + values);
    }
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("leaid", leaid.length() < 7 ? String.format(Locale.ROOT, "%7s", leaid).replace(' ', '0')
        : leaid);
    row.put("year", Integer.valueOf(year));
    String fips = col(headers, values, "state fips code");
    row.put("state_fips", fips == null ? null
        : (fips.length() < 2 ? "0" + fips : fips));
    row.put("state_name", col(headers, values, "state name"));
    row.put("lea_name", col(headers, values, "lea name"));
    row.put("state_lea_id", col(headers, values, "state lea id"));
    row.put("total_children", count(col(headers, values,
        "combined early childhood and school age total")));
    for (String[] cat : CATEGORIES) {
      row.put("early_childhood_" + cat[0], count(col(headers, values,
          "early childhood " + cat[1])));
    }
    for (String[] cat : CATEGORIES) {
      row.put("school_age_" + cat[0], count(col(headers, values, "school age " + cat[1])));
    }
    return row;
  }

  /** Parses a count; the source's -8 / -9 suppression codes and blanks become null. */
  private static Long count(String value) {
    if (value == null) {
      return null;
    }
    long v = Long.parseLong(value);
    return v < 0 ? null : Long.valueOf(v);
  }

  private static String col(List<String> headers, List<String> values, String name) {
    int i = headers.indexOf(name);
    if (i < 0 || i >= values.size()) {
      return null;
    }
    String v = values.get(i).trim();
    return v.isEmpty() ? null : v;
  }

  private static byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(180000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != HttpURLConnection.HTTP_OK) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = CompleteBody.of(conn);
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
