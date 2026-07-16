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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Streaming transformer for {@code econ.comtrade_flows} (UN Comtrade bilateral merchandise trade).
 *
 * <p>The Comtrade v1 data API returns a JSON object {@code {"elapsedTime":..,"count":N,"data":[
 * {record},..]}} where each record is one reporter × partner × HS-6 commodity × flow observation
 * for the requested {@code (reporterCode, period, flowCode)}. A single big-reporter call can return
 * hundreds of thousands of records, so this class implements {@link StreamingResponseTransformer}:
 * {@code HttpSource} calls {@link #fetchAndTransform} directly, which opens the URL (applying the
 * {@code Ocp-Apim-Subscription-Key} header from the request context), locates the {@code data}
 * array, and streams one record at a time — heap stays O(1) per row regardless of response size.
 *
 * <p>Each source record is mapped to the clean {@code comtrade_flows} columns: the reporter/partner
 * ISO alpha-3 codes ({@code reporterISO}/{@code partnerISO}) are passed through for the
 * {@code ref.countries} FK; the World partner ({@code partnerCode = 0}) is emitted as
 * {@code partner_code = 'WLD'} with {@code is_aggregate = true}; {@code cmdCode} is split into
 * {@code hs2}/{@code hs6}; and {@code primaryValue}/{@code netWgt}/{@code qty} are coerced to
 * doubles.
 *
 * <p>NOTE: Comtrade field names follow the documented v1 schema; verify against a live keyed
 * response before relying on this table (requires {@code COMTRADE_API_KEY}).
 */
public class ComtradeTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ComtradeTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonFactory JSON_FACTORY = MAPPER.getFactory();

  private static final int CONNECT_TIMEOUT_MS = 60_000;
  private static final int READ_TIMEOUT_MS = 900_000;
  private static final int MAX_RETRIES = 4;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    if (url == null || url.isEmpty()) {
      throw new IllegalStateException("ComtradeTransformer: no URL in context");
    }
    HttpURLConnection conn = openWithRetry(url, context.getHeaders());
    InputStream in = conn.getInputStream();
    JsonParser parser = JSON_FACTORY.createParser(in);

    if (parser.nextToken() != JsonToken.START_OBJECT) {
      close(parser, conn);
      throw new IOException("Comtrade: expected top-level object for " + url);
    }
    // Walk top-level fields until the "data" array; capture any error message on the way.
    String errorMessage = null;
    JsonToken t;
    while ((t = parser.nextToken()) != null && t != JsonToken.END_OBJECT) {
      if (t != JsonToken.FIELD_NAME) {
        continue;
      }
      String field = parser.currentName();
      if ("data".equals(field)) {
        JsonToken arr = parser.nextToken();
        if (arr == JsonToken.START_ARRAY) {
          return new RecordIterator(parser, conn, url);
        }
        // data present but null/empty object — no rows.
        if (arr == JsonToken.VALUE_NULL) {
          close(parser, conn);
          return Collections.<Map<String, Object>>emptyList().iterator();
        }
        parser.skipChildren();
      } else {
        parser.nextToken();
        if (("statusCode".equals(field) || "message".equals(field) || "error".equals(field))
            && parser.currentToken().isScalarValue()) {
          String v = parser.getValueAsString();
          if (v != null && !v.isEmpty() && !"200".equals(v)) {
            errorMessage = field + "=" + v;
          }
        } else if (parser.currentToken().isStructStart()) {
          parser.skipChildren();
        }
      }
    }
    close(parser, conn);
    String dims = context.getDimensionValues().isEmpty()
        ? "" : " [dimensions: " + context.getDimensionValues() + "]";
    if (errorMessage != null) {
      throw new RuntimeException("Comtrade API error: " + errorMessage + dims);
    }
    LOGGER.debug("Comtrade: no data array for {}{}", url, dims);
    return Collections.<Map<String, Object>>emptyList().iterator();
  }

  private static HttpURLConnection openWithRetry(String url, Map<String, String> headers)
      throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      conn.setRequestProperty("Accept", "application/json");
      if (headers != null) {
        for (Map.Entry<String, String> h : headers.entrySet()) {
          conn.setRequestProperty(h.getKey(), h.getValue());
        }
      }
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
      if (status == 429 || status == 500 || status == 503) {
        LOGGER.warn("Comtrade: HTTP {} (attempt {}/{}) for {}", status, attempt, MAX_RETRIES, url);
        sleep(attempt);
        continue;
      }
      throw new IOException("HTTP " + status + " from " + url);
    }
    throw last != null ? last : new IOException("Comtrade: retries exhausted for " + url);
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

  private static String toStr(Object v) {
    if (v == null) {
      return null;
    }
    String s = v.toString().trim();
    return s.isEmpty() ? null : s;
  }

  /** Lazy iterator over the {@code data} array; maps each Comtrade record to a clean row. */
  private static final class RecordIterator implements Iterator<Map<String, Object>> {
    private final JsonParser parser;
    private final HttpURLConnection conn;
    private final String url;
    private Map<String, Object> pending;
    private boolean closed;
    private long rowCount;

    RecordIterator(JsonParser parser, HttpURLConnection conn, String url) {
      this.parser = parser;
      this.conn = conn;
      this.url = url;
      advance();
    }

    @SuppressWarnings("unchecked")
    private void advance() {
      try {
        JsonToken t = parser.nextToken();
        if (t == JsonToken.START_OBJECT) {
          Map<String, Object> rec = MAPPER.readValue(parser, Map.class);
          pending = map(rec);
        } else {
          pending = null;
          finish();
        }
      } catch (IOException e) {
        finish();
        throw new RuntimeException("Comtrade: read failed for " + url, e);
      }
    }

    private Map<String, Object> map(Map<String, Object> rec) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("reporter_code", toStr(rec.get("reporterISO")));

      String partnerCode = toStr(rec.get("partnerCode"));
      String partnerIso = toStr(rec.get("partnerISO"));
      boolean world = "0".equals(partnerCode);
      row.put("partner_code", world ? "WLD" : partnerIso);

      String flowDesc = toStr(rec.get("flowDesc"));
      if (flowDesc == null) {
        flowDesc = flowFromCode(toStr(rec.get("flowCode")));
      }
      row.put("flow_desc", flowDesc);

      String cmd = toStr(rec.get("cmdCode"));
      row.put("hs6", cmd);
      row.put("hs2", cmd != null && cmd.length() >= 2 ? cmd.substring(0, 2) : null);
      row.put("hs_description", toStr(rec.get("cmdDesc")));
      row.put("hs_revision", toStr(rec.get("classificationCode")));

      row.put("trade_value_usd", toDouble(rec.get("primaryValue")));
      row.put("net_weight_kg", toDouble(rec.get("netWgt")));
      row.put("quantity", toDouble(rec.get("qty")));
      row.put("quantity_unit", toStr(rec.get("qtyUnitAbbr")));

      Object agg = rec.get("isAggregate");
      boolean isAggregate = world
          || (agg instanceof Boolean && (Boolean) agg)
          || partnerIso == null;
      row.put("is_aggregate", isAggregate);
      return row;
    }

    private static String flowFromCode(String code) {
      if (code == null) {
        return null;
      }
      if ("M".equals(code)) {
        return "Import";
      }
      if ("X".equals(code)) {
        return "Export";
      }
      if ("RM".equals(code)) {
        return "Re-import";
      }
      if ("RX".equals(code)) {
        return "Re-export";
      }
      return code;
    }

    private void finish() {
      if (closed) {
        return;
      }
      closed = true;
      close(parser, conn);
      LOGGER.debug("Comtrade: streamed {} records from {}", rowCount, url);
    }

    @Override public boolean hasNext() {
      return pending != null;
    }

    @Override public Map<String, Object> next() {
      if (pending == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = pending;
      rowCount++;
      advance();
      return row;
    }
  }
}
