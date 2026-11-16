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

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns DHS Study in the States' SEVIS Data Mapping Tool feed into long-format rows: one row per
 * (reporting month, country of citizenship, breakdown, category) with the count of active F-1/M-1
 * student SEVIS records.
 *
 * <p>The tool page ({@code studyinthestates.dhs.gov/sevis-data-mapping-tool}) carries a
 * {@code data-json="/sites/default/files/YYYY-MM/MONTH_YYYY.json"} attribute whose target changes
 * every month, so the page is fetched first and the JSON path read from it. The JSON (verified
 * live 2026-11-16) holds {@code reporting_periods} (month labels, "April 2014" onward) and one
 * object per label of parallel arrays indexed by country: {@code codes}, {@code continent},
 * {@code region}, {@code coc}, {@code active_students}, {@code male}/{@code female}, one array
 * per education level, one per U.S. state/territory, and from late 2014 {@code stem_*} arrays
 * for the STEM-field subset. {@code codes} is not unique (Bassas da India and Tromelin Island
 * share TF; Gaza Strip and West Bank share PS), so the country name is the identity.
 *
 * <p>The country list in the first two published months carries two extra trailing
 * {@code region}/{@code coc} elements, and the September 2015 numeric arrays carry one extra
 * trailing element; the country count is therefore taken from {@code codes}, every array must be
 * at least that long, and the trailing surplus is not read. Zero counts are not emitted.
 */
public class SevisStudentCountsTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern DATA_JSON = Pattern.compile("data-json=\"([^\"]+\\.json)\"");
  private static final Pattern PERIOD = Pattern.compile("^([A-Za-z]+) (\\d{4})$");

  private static final String[] EDUCATION_LEVELS = {
      "associate", "bachelors", "masters", "doctorate", "language_training", "high_school",
      "primary", "secondary", "flight_school", "other_vocational_school", "other"};

  @Override
  public String transform(String response, RequestContext context) {
    String pageUrl = context.getUrl();
    try {
      Matcher m = DATA_JSON.matcher(response);
      if (!m.find()) {
        throw new IllegalStateException("no data-json attribute on " + pageUrl
            + " — page layout may have changed");
      }
      String jsonUrl = URI.create(pageUrl).resolve(m.group(1)).toString();
      JsonNode root = MAPPER.readTree(download(jsonUrl));
      JsonNode periods = root.get("reporting_periods");
      if (periods == null || !periods.isArray() || periods.size() == 0) {
        throw new IllegalStateException(jsonUrl + ": reporting_periods missing or empty");
      }
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode p : periods) {
        String label = p.asText();
        JsonNode period = root.get(label);
        if (period == null) {
          throw new IllegalStateException(jsonUrl + ": no data object for period " + label);
        }
        parsePeriod(label, period, out);
      }
      return out.toString();
    } catch (Exception e) {
      throw new RuntimeException("SEVIS data mapping tool: failed to parse from " + pageUrl, e);
    }
  }

  private static void parsePeriod(String label, JsonNode period, ArrayNode out) {
    LocalDate month = parseMonth(label);
    JsonNode codes = require(period, "codes", label);
    int n = codes.size();
    JsonNode continent = sized(period, "continent", n, label);
    JsonNode region = sized(period, "region", n, label);
    JsonNode coc = sized(period, "coc", n, label);
    List<String> stateCodes = new ArrayList<>();
    Iterator<String> names = period.fieldNames();
    while (names.hasNext()) {
      String name = names.next();
      if (name.length() == 2 && Character.isUpperCase(name.charAt(0))
          && Character.isUpperCase(name.charAt(1))) {
        stateCodes.add(name);
      }
    }
    if (stateCodes.isEmpty()) {
      throw new IllegalStateException(label + ": no state arrays found");
    }
    for (int i = 0; i < n; i++) {
      Header h = new Header(month, coc.get(i).asText().trim(), codes.get(i).asText().trim(),
          continent.get(i).asText().trim(), region.get(i).asText().trim());
      emit(out, h, "total", "active", sized(period, "active_students", n, label), i);
      emit(out, h, "sex", "female", sized(period, "female", n, label), i);
      emit(out, h, "sex", "male", sized(period, "male", n, label), i);
      for (String level : EDUCATION_LEVELS) {
        emit(out, h, "education_level", level, sized(period, level, n, label), i);
      }
      for (String st : stateCodes) {
        emit(out, h, "state", st, sized(period, st, n, label), i);
      }
      if (period.has("stem_male")) {
        emit(out, h, "stem_sex", "female", sized(period, "stem_female", n, label), i);
        emit(out, h, "stem_sex", "male", sized(period, "stem_male", n, label), i);
        for (String level : EDUCATION_LEVELS) {
          emit(out, h, "stem_education_level", level, sized(period, "stem_" + level, n, label), i);
        }
      }
    }
  }

  private static JsonNode require(JsonNode period, String field, String label) {
    JsonNode node = period.get(field);
    if (node == null || !node.isArray()) {
      throw new IllegalStateException(label + ": array '" + field + "' missing");
    }
    return node;
  }

  private static JsonNode sized(JsonNode period, String field, int n, String label) {
    JsonNode node = require(period, field, label);
    if (node.size() < n) {
      throw new IllegalStateException(label + ": array '" + field + "' has " + node.size()
          + " elements, fewer than the " + n + " countries");
    }
    return node;
  }

  private static void emit(ArrayNode out, Header h, String breakdown, String category,
      JsonNode values, int i) {
    JsonNode v = values.get(i);
    if (!v.isIntegralNumber()) {
      throw new IllegalStateException(h.month + " " + h.country + " " + breakdown + "/" + category
          + ": non-integer count " + v);
    }
    long count = v.asLong();
    if (count == 0) {
      return;
    }
    ObjectNode row = MAPPER.createObjectNode();
    row.put("reporting_month", h.month.toString());
    row.put("country_name", h.country);
    row.put("country_code", h.code);
    row.put("continent", h.continent);
    row.put("region", h.region);
    row.put("breakdown", breakdown);
    row.put("category", category);
    row.put("students", count);
    out.add(row);
  }

  static LocalDate parseMonth(String label) {
    Matcher m = PERIOD.matcher(label);
    if (!m.matches()) {
      throw new IllegalStateException("unrecognised reporting period label '" + label + "'");
    }
    Month month = Month.valueOf(m.group(1).toUpperCase(Locale.ROOT));
    return LocalDate.of(Integer.parseInt(m.group(2)), month, 1);
  }

  private static byte[] download(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            + "Chrome/128.0.0.0 Safari/537.36");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (InputStream is = conn.getInputStream()) {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
    }
    return baos.toByteArray();
  }

  /** Per-country identity columns shared by every row emitted for that country and month. */
  private static final class Header {
    final LocalDate month;
    final String country;
    final String code;
    final String continent;
    final String region;

    Header(LocalDate month, String country, String code, String continent, String region) {
      this.month = month;
      this.country = country;
      this.code = code;
      this.continent = continent;
      this.region = region;
    }
  }
}
