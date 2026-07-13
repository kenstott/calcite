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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Streaming transformer for {@code income_limits_county} (HUD Income Limits at HUD-area grain).
 *
 * <p>The HUD USER API exposes county/metro income limits only per area, so for one
 * {@code (year, state)} fetch unit this transformer first reads the state's HUD area list
 * ({@code fmr/listCounties/{state}} — the source URL) and then streams {@code il/data/{area}} for
 * each area, yielding one row per HUD area. Keeping the fetch unit at {@code (year, state)} (rather
 * than fanning out a per-area dimension) means each state overwrites exactly its own partition and
 * the ~3k-area universe never becomes 3k partitions.
 *
 * <p>{@code fips_code} is the HUD 10-digit area code ({@code state+county+subdivision}); the first
 * 5 digits are the county FIPS. Areas whose {@code il/data} lookup returns no record (HTTP 404) are
 * skipped as honest absences; transient 429/5xx responses are retried and then propagated.
 */
public class HudCountyIncomeLimitsStreamingTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(HudCountyIncomeLimitsStreamingTransformer.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String API_MARKER = "/hudapi/public/";
  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 60_000;
  private static final int MAX_RETRIES = 3;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String year = context.getDimensionValues().get("year");
    String state = context.getDimensionValues().get("state");
    if (year == null || state == null) {
      throw new IllegalStateException("income_limits_county requires 'year' and 'state' dimensions");
    }
    String auth = context.getHeaders() != null ? context.getHeaders().get("Authorization") : null;
    if (auth == null || auth.trim().isEmpty() || auth.trim().equalsIgnoreCase("Bearer")) {
      throw new IllegalStateException("income_limits_county requires a HUD_TOKEN Authorization header");
    }
    String listUrl = context.getUrl();
    int marker = listUrl.indexOf(API_MARKER);
    if (marker < 0) {
      throw new IOException("income_limits_county: unexpected HUD URL (no " + API_MARKER + "): " + listUrl);
    }
    String dataBase = listUrl.substring(0, marker + API_MARKER.length()) + "il/data/";

    List<String> areaCodes = fetchAreaCodes(listUrl, auth, state, year);
    LOGGER.info("income_limits_county[{},{}]: {} HUD areas", year, state, areaCodes.size());
    return new CountyIlIterator(areaCodes, dataBase, auth, year, state);
  }

  /** Reads the state's HUD area list (a bare JSON array of {@code {fips_code,...}}). */
  private List<String> fetchAreaCodes(String listUrl, String auth, String state, String year)
      throws IOException {
    String body = get(listUrl, auth);
    if (body == null) {
      throw new IOException("income_limits_county[" + year + "," + state + "]: no area list from " + listUrl);
    }
    JsonNode root = MAPPER.readTree(body);
    if (!root.isArray()) {
      throw new IOException("income_limits_county[" + year + "," + state
          + "]: expected JSON array from listCounties, got: "
          + body.substring(0, Math.min(200, body.length())));
    }
    List<String> codes = new ArrayList<String>();
    for (JsonNode n : root) {
      JsonNode f = n.path("fips_code");
      if (!f.isMissingNode() && !f.isNull() && !f.asText().isEmpty()) {
        codes.add(f.asText());
      }
    }
    return codes;
  }

  /** Performs an authenticated GET, retrying 429/5xx; returns the body, or null on 404. */
  private String get(String url, String auth) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      conn.setRequestProperty("Authorization", auth);
      int status;
      try {
        status = conn.getResponseCode();
        if (status == HttpURLConnection.HTTP_NOT_FOUND) {
          return null;
        }
        if (status == HttpURLConnection.HTTP_OK) {
          return readBody(conn.getInputStream());
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

  private static String readBody(InputStream in) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      char[] buf = new char[8192];
      int n;
      while ((n = r.read(buf)) != -1) {
        sb.append(buf, 0, n);
      }
    }
    return sb.toString();
  }

  private static void sleepBackoff(int attempt) throws IOException {
    try {
      Thread.sleep(500L * attempt);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted during HUD retry backoff", e);
    }
  }

  /** Lazily fetches {@code il/data/{area}} for each area code, one row per area. */
  private final class CountyIlIterator implements Iterator<Map<String, Object>> {
    private final List<String> areaCodes;
    private final String dataBase;
    private final String auth;
    private final String year;
    private final String state;
    private int pos;
    private Map<String, Object> nextRow;

    CountyIlIterator(List<String> areaCodes, String dataBase, String auth, String year, String state) {
      this.areaCodes = areaCodes;
      this.dataBase = dataBase;
      this.auth = auth;
      this.year = year;
      this.state = state;
      advance();
    }

    private void advance() {
      nextRow = null;
      while (pos < areaCodes.size()) {
        String fips = areaCodes.get(pos++);
        try {
          String body = get(dataBase + fips + "?year=" + year, auth);
          if (body == null) {
            continue;
          }
          JsonNode data = MAPPER.readTree(body).path("data");
          if (data.isMissingNode() || data.isNull()) {
            continue;
          }
          nextRow = mapRow(fips, data);
          return;
        } catch (IOException e) {
          throw new RuntimeException("income_limits_county[" + year + "," + state
              + "]: il/data fetch failed for " + fips, e);
        }
      }
    }

    private Map<String, Object> mapRow(String fips, JsonNode data) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("state_fips", fips.length() >= 2 ? fips.substring(0, 2) : null);
      row.put("county_fips", fips.length() >= 5 ? fips.substring(0, 5) : null);
      row.put("hud_area_code", fips);
      row.put("county_name", text(data, "county_name"));
      row.put("metro_status", text(data, "metro_status"));
      row.put("metro_name", text(data, "metro_name"));
      row.put("area_name", text(data, "area_name"));
      row.put("median_income", intg(data.path("median_income")));
      row.put("il30_4person", intg(data.path("extremely_low").path("il30_p4")));
      row.put("il50_4person", intg(data.path("very_low").path("il50_p4")));
      row.put("il80_4person", intg(data.path("low").path("il80_p4")));
      return row;
    }

    @Override public boolean hasNext() {
      return nextRow != null;
    }

    @Override public Map<String, Object> next() {
      if (nextRow == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = nextRow;
      advance();
      return row;
    }
  }

  private static String text(JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    return v.isMissingNode() || v.isNull() || v.asText().isEmpty() ? null : v.asText();
  }

  private static Integer intg(JsonNode v) {
    if (v.isMissingNode() || v.isNull()) {
      return null;
    }
    if (v.isNumber()) {
      return v.asInt();
    }
    String s = v.asText().trim();
    if (s.isEmpty()) {
      return null;
    }
    try {
      return (int) Double.parseDouble(s);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
