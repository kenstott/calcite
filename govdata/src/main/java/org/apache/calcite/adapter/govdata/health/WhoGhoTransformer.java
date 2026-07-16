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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Streaming transformer for {@code health.who_gho_indicators} (WHO Global Health Observatory).
 *
 * <p>Each fan-out request pulls one GHO indicator via the OData endpoint
 * ({@code https://ghoapi.azureedge.net/api/{indicator}}), which returns
 * {@code {"@odata.context":..,"value":[{obs},..]}} with one observation per country × dimension ×
 * year. This class implements {@link StreamingResponseTransformer}: {@code HttpSource} calls
 * {@link #fetchAndTransform} directly, which opens the URL and streams the {@code value} array one
 * record at a time — heap stays O(1) per row.
 *
 * <p>Rows are mapped to the clean {@code who_gho_indicators} columns. {@code SpatialDim} is ISO
 * alpha-3 for COUNTRY rows ({@code country_code}, the {@code ref.countries} FK); WHO REGION /
 * income-group rows ({@code SpatialDimType != COUNTRY}) are flagged {@code is_aggregate}. The
 * {@code Dim1} value is routed to {@code sex} when {@code Dim1Type} is a sex dimension, else to the
 * generic {@code dimension} column. Each row carries its own {@code year} (from {@code TimeDim}) so
 * the writer can partition on {@code [type, indicator, year]}. The indicator label is resolved from
 * the bundled catalog {@code /who/who-gho-indicators.json}.
 *
 * <p>NOTE: GHO field names follow the documented OData schema; verify against a live response
 * before relying on this table.
 */
public class WhoGhoTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(WhoGhoTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonFactory JSON_FACTORY = MAPPER.getFactory();

  private static final int CONNECT_TIMEOUT_MS = 60_000;
  private static final int READ_TIMEOUT_MS = 900_000;
  private static final int MAX_RETRIES = 4;

  /** code -> label, loaded once from the bundled fan-out catalog. */
  private static volatile Map<String, String> indicatorNames;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    if (url == null || url.isEmpty()) {
      throw new IllegalStateException("WhoGhoTransformer: no URL in context");
    }
    String indicator = context.getDimensionValues().get("indicator");
    if (indicator == null || indicator.isEmpty()) {
      indicator = indicatorFromUrl(url);
    }
    HttpURLConnection conn = openWithRetry(url);
    InputStream in = conn.getInputStream();
    JsonParser parser = JSON_FACTORY.createParser(in);

    if (parser.nextToken() != JsonToken.START_OBJECT) {
      close(parser, conn);
      throw new IOException("WHO GHO: expected top-level object for " + url);
    }
    JsonToken t;
    while ((t = parser.nextToken()) != null && t != JsonToken.END_OBJECT) {
      if (t != JsonToken.FIELD_NAME) {
        continue;
      }
      String field = parser.currentName();
      if ("value".equals(field)) {
        if (parser.nextToken() == JsonToken.START_ARRAY) {
          return new RecordIterator(parser, conn, indicator, indicatorLabel(indicator), url);
        }
        parser.skipChildren();
      } else {
        parser.nextToken();
        if (parser.currentToken().isStructStart()) {
          parser.skipChildren();
        }
      }
    }
    close(parser, conn);
    LOGGER.debug("WHO GHO: no value array for {}", url);
    return Collections.<Map<String, Object>>emptyList().iterator();
  }

  private static String indicatorFromUrl(String url) {
    int slash = url.lastIndexOf('/');
    return slash >= 0 ? url.substring(slash + 1) : url;
  }

  private static String indicatorLabel(String code) {
    Map<String, String> names = indicatorNames;
    if (names == null) {
      names = loadIndicatorNames();
      indicatorNames = names;
    }
    return names.get(code);
  }

  private static Map<String, String> loadIndicatorNames() {
    Map<String, String> names = new HashMap<String, String>();
    try (InputStream in =
        WhoGhoTransformer.class.getResourceAsStream("/who/who-gho-indicators.json")) {
      if (in == null) {
        return names;
      }
      JsonNode root = MAPPER.readTree(in);
      for (JsonNode n : root.path("indicators")) {
        String code = n.path("code").asText(null);
        if (code != null) {
          names.put(code, n.path("name").asText(null));
        }
      }
    } catch (IOException e) {
      LOGGER.warn("WHO GHO: could not load indicator catalog: {}", e.getMessage());
    }
    return names;
  }

  private static HttpURLConnection openWithRetry(String url) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      conn.setRequestProperty("Accept", "application/json");
      int status;
      try {
        status = conn.getResponseCode();
      } catch (IOException e) {
        conn.disconnect();
        last = e;
        sleep(attempt);
        continue;
      }
      if (status == HttpURLConnection.HTTP_OK) {
        return conn;
      }
      conn.disconnect();
      if (status == 429 || status == 503) {
        LOGGER.warn("WHO GHO: HTTP {} (attempt {}/{}) for {}", status, attempt, MAX_RETRIES, url);
        sleep(attempt);
        continue;
      }
      throw new IOException("HTTP " + status + " from " + url);
    }
    throw last != null ? last : new IOException("WHO GHO: retries exhausted for " + url);
  }

  private static void sleep(int attempt) {
    try {
      Thread.sleep(1000L * attempt);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void close(JsonParser parser, HttpURLConnection conn) {
    try {
      parser.close();
    } catch (IOException ignored) {
      // best-effort
    }
    conn.disconnect();
  }

  private static Double toDouble(Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof Number) {
      return ((Number) v).doubleValue();
    }
    String s = v.toString().trim();
    if (s.isEmpty()) {
      return null;
    }
    try {
      return Double.parseDouble(s);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Integer toInt(Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof Number) {
      return ((Number) v).intValue();
    }
    String s = v.toString().trim();
    if (s.length() < 4) {
      return null;
    }
    try {
      return Integer.parseInt(s.substring(0, 4));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String toStr(Object v) {
    if (v == null) {
      return null;
    }
    String s = v.toString().trim();
    return s.isEmpty() ? null : s;
  }

  /** Lazy iterator over the {@code value} array; maps each GHO observation to a clean row. */
  private static final class RecordIterator implements Iterator<Map<String, Object>> {
    private final JsonParser parser;
    private final HttpURLConnection conn;
    private final String indicator;
    private final String indicatorName;
    private final String url;
    private Map<String, Object> mapped;
    private boolean closed;
    private long rowCount;

    RecordIterator(JsonParser parser, HttpURLConnection conn, String indicator,
        String indicatorName, String url) {
      this.parser = parser;
      this.conn = conn;
      this.indicator = indicator;
      this.indicatorName = indicatorName;
      this.url = url;
      advance();
    }

    @SuppressWarnings("unchecked")
    private void advance() {
      try {
        JsonToken t = parser.nextToken();
        while (t == JsonToken.START_OBJECT) {
          Map<String, Object> rec = MAPPER.readValue(parser, Map.class);
          Map<String, Object> row = map(rec);
          if (row != null) {
            mapped = row;
            return;
          }
          t = parser.nextToken();
        }
        mapped = null;
        finish();
      } catch (IOException e) {
        finish();
        throw new RuntimeException("WHO GHO: read failed for " + url, e);
      }
    }

    private Map<String, Object> map(Map<String, Object> rec) {
      Integer year = toInt(rec.get("TimeDim"));
      if (year == null) {
        return null; // no parseable year → cannot partition; skip
      }
      String spatialType = toStr(rec.get("SpatialDimType"));
      String dim1Type = toStr(rec.get("Dim1Type"));
      String dim1 = toStr(rec.get("Dim1"));
      boolean sexDim = dim1Type != null && dim1Type.toUpperCase().contains("SEX");

      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("indicator_code", indicator);
      row.put("indicator_name", indicatorName);
      row.put("country_code", toStr(rec.get("SpatialDim")));
      row.put("spatial_type", spatialType);
      row.put("sex", sexDim ? dim1 : null);
      row.put("dimension", sexDim ? null : dim1);
      row.put("value_numeric", toDouble(rec.get("NumericValue")));
      row.put("value_display", toStr(rec.get("Value")));
      row.put("unit", null);
      row.put("year", year);
      row.put("is_aggregate", !"COUNTRY".equalsIgnoreCase(spatialType));
      return row;
    }

    private void finish() {
      if (closed) {
        return;
      }
      closed = true;
      close(parser, conn);
      LOGGER.debug("WHO GHO[{}]: streamed {} rows from {}", indicator, rowCount, url);
    }

    @Override public boolean hasNext() {
      return mapped != null;
    }

    @Override public Map<String, Object> next() {
      if (mapped == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = mapped;
      rowCount++;
      advance();
      return row;
    }
  }
}
