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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code ref.holidays} — multi-country public holidays from the free
 * <a href="https://date.nager.at">Nager.Date</a> API (no API key required).
 *
 * <p>Invoked once per {@code year} dimension batch. For that year it:
 * <ol>
 *   <li>Fetches the list of supported countries ({@code /api/v3/AvailableCountries}, ~110)</li>
 *   <li>For each country, fetches {@code /api/v3/PublicHolidays/{year}/{countryCode}}</li>
 *   <li>Emits one row per (country, holiday), keyed for FK joins to {@code ref.countries}
 *       (via {@code country_code} = ISO 3166-1 alpha-2) and {@code ref.calendar}
 *       (via {@code holiday_date} = ISO {@code YYYY-MM-DD})</li>
 * </ol>
 *
 * <p>A country that returns no holidays for the year is legitimate and simply contributes no
 * rows; a non-200 response for a single country is logged and that country is skipped for the
 * year (mirrors the per-item degradation in {@link FigiDataProvider}).
 */
public class HolidaysDataProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(HolidaysDataProvider.class);

  private static final String COUNTRIES_URL = "https://date.nager.at/api/v3/AvailableCountries";
  private static final String HOLIDAYS_URL = "https://date.nager.at/api/v3/PublicHolidays/%d/%s";
  /** Politeness delay between per-country requests against the free public endpoint. */
  private static final long REQUEST_DELAY_MS = 150;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public HolidaysDataProvider() {
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String yearStr = variables.get("effective_year");
    if (yearStr == null || yearStr.isEmpty()) {
      yearStr = variables.get("year");
    }
    if (yearStr == null || yearStr.isEmpty()) {
      throw new IOException("HolidaysDataProvider: no year in dimension variables " + variables);
    }
    int year = Integer.parseInt(yearStr.trim());

    Map<String, String> countries = fetchCountries();
    LOGGER.info("HolidaysDataProvider: {} countries for year {}", countries.size(), year);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (Map.Entry<String, String> country : countries.entrySet()) {
      String code = country.getKey();
      rows.addAll(fetchCountryHolidays(year, code, country.getValue()));
      sleep();
    }
    LOGGER.info("HolidaysDataProvider: {} holiday rows across {} countries for year {}",
        rows.size(), countries.size(), year);
    return rows.iterator();
  }

  /** ISO alpha-2 country code → English country name for every Nager-supported country. */
  private Map<String, String> fetchCountries() throws IOException {
    String body = get(COUNTRIES_URL);
    JsonNode root = MAPPER.readTree(body);
    Map<String, String> countries = new HashMap<String, String>();
    if (root.isArray()) {
      for (JsonNode node : root) {
        String code = textOrNull(node, "countryCode");
        if (code != null && !code.isEmpty()) {
          countries.put(code, textOrNull(node, "name"));
        }
      }
    }
    if (countries.isEmpty()) {
      throw new IOException("HolidaysDataProvider: AvailableCountries returned no countries");
    }
    return countries;
  }

  private List<Map<String, Object>> fetchCountryHolidays(int year, String code, String name) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    String url = String.format(HOLIDAYS_URL, year, code);
    String body;
    try {
      body = get(url);
    } catch (IOException e) {
      LOGGER.warn("HolidaysDataProvider: {} {} failed: {}", code, year, e.getMessage());
      return rows;
    }
    try {
      JsonNode root = MAPPER.readTree(body);
      if (!root.isArray()) {
        return rows;
      }
      for (JsonNode h : root) {
        Map<String, Object> r = new HashMap<String, Object>();
        r.put("country_code", code);
        r.put("country_name", name);
        r.put("holiday_date", textOrNull(h, "date"));       // YYYY-MM-DD → FK ref.calendar.iso_date
        r.put("year", year);
        r.put("holiday_name", textOrNull(h, "name"));       // English name
        r.put("local_name", textOrNull(h, "localName"));
        r.put("is_global", boolOrNull(h, "global"));
        r.put("is_fixed", boolOrNull(h, "fixed"));
        r.put("counties", joinArray(h.get("counties")));
        r.put("launch_year", intOrNull(h, "launchYear"));
        r.put("holiday_types", joinArray(h.get("types")));
        rows.add(r);
      }
    } catch (IOException e) {
      LOGGER.warn("HolidaysDataProvider: parse failed for {} {}: {}", code, year, e.getMessage());
    }
    return rows;
  }

  private String get(String urlStr) throws IOException {
    URL url = URI.create(urlStr).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");
    conn.setRequestProperty("User-Agent", "govdata-etl kennethstott@gmail.com");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);
    int responseCode = conn.getResponseCode();
    if (responseCode != 200) {
      throw new IOException("HTTP " + responseCode + " from " + urlStr);
    }
    try (InputStream in = conn.getInputStream()) {
      byte[] buf = new byte[65536];
      StringBuilder sb = new StringBuilder();
      int n;
      while ((n = in.read(buf)) != -1) {
        sb.append(new String(buf, 0, n, StandardCharsets.UTF_8));
      }
      return sb.toString();
    }
  }

  private void sleep() {
    try {
      Thread.sleep(REQUEST_DELAY_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static String joinArray(JsonNode array) {
    if (array == null || !array.isArray() || array.size() == 0) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (JsonNode node : array) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append(node.asText());
    }
    return sb.toString();
  }

  private static String textOrNull(JsonNode node, String field) {
    JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? null : value.asText();
  }

  private static Boolean boolOrNull(JsonNode node, String field) {
    JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? null : value.asBoolean();
  }

  private static Integer intOrNull(JsonNode node, String field) {
    JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? null : value.asInt();
  }
}
