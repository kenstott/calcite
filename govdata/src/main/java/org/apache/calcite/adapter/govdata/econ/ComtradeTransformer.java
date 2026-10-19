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
import org.apache.calcite.adapter.file.etl.RetryableHttp;
import org.apache.calcite.adapter.file.etl.SecretRedaction;
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


  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    if (url == null || url.isEmpty()) {
      throw new IllegalStateException("ComtradeTransformer: no URL in context");
    }
    if (context.getRateLimit() == null) {
      throw new IllegalStateException(
          "ComtradeTransformer: source declares no rateLimit, so maxRetries is unknown for "
              + SecretRedaction.redact(url));
    }
    Fetch fetch = new Fetch(context, url);
    if (!fetch.open()) {
      return Collections.<Map<String, Object>>emptyList().iterator();
    }
    return new RecordIterator(fetch);
  }

  /**
   * One Comtrade request: opens the URL and positions a parser inside the {@code data} array. A
   * connection reset or truncated body is a transport fault on an idempotent GET, so the whole
   * fetch (open + body read) is re-run, up to the source's {@code rateLimit.maxRetries} in total,
   * for as long as no row has been handed to the caller (a retry after that would duplicate rows).
   */
  private static final class Fetch {
    private final RequestContext context;
    private final String url;
    private final String safeUrl;
    private final Map<String, String> reqHeaders;
    private final int maxRetries;
    private int attempt;
    JsonParser parser;
    HttpURLConnection conn;

    Fetch(RequestContext context, String url) {
      this.context = context;
      this.url = url;
      this.safeUrl = SecretRedaction.redact(url);
      this.reqHeaders = new LinkedHashMap<String, String>(context.getHeaders());
      this.reqHeaders.put("Accept", "application/json");
      this.maxRetries = context.getRateLimit().getMaxRetries();
    }

    /**
     * Opens the request, retrying transport faults while locating the {@code data} array.
     *
     * @return true with {@link #parser} inside the array; false when the response has no rows
     */
    boolean open() throws IOException {
      while (true) {
        try {
          return openOnce();
        } catch (IOException e) {
          retryOrThrow(e);
        }
      }
    }

    /** Prepares a re-run after a mid-body fault, or throws once retries are exhausted. */
    void retryOrThrow(IOException cause) throws IOException {
      closeQuietly();
      if (attempt >= maxRetries) {
        throw new IOException("Comtrade: read failed for " + safeUrl + " after "
            + (attempt + 1) + " attempts", cause);
      }
      LOGGER.warn("Comtrade: {} while reading response (attempt {}/{}), retrying whole fetch of {}",
          cause.toString(), attempt + 1, maxRetries + 1, safeUrl);
      RetryableHttp.backoffBeforeRetry(attempt, context.getRateLimit());
      attempt++;
    }

    void closeQuietly() {
      if (conn != null) {
        close(parser, conn);
        conn = null;
        parser = null;
      }
    }

    private boolean openOnce() throws IOException {
      conn = RetryableHttp.openWithRetry(url, reqHeaders, context.getRateLimit(), false);
      parser = JSON_FACTORY.createParser(conn.getInputStream());
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        closeQuietly();
        throw new IllegalStateException("Comtrade: expected top-level object for " + safeUrl);
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
            return true;
          }
          // data present but null/empty object — no rows.
          if (arr == JsonToken.VALUE_NULL) {
            closeQuietly();
            return false;
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
      closeQuietly();
      String dims = context.getDimensionValues().isEmpty()
          ? "" : " [dimensions: " + context.getDimensionValues() + "]";
      if (errorMessage != null) {
        throw new RuntimeException("Comtrade API error: " + errorMessage + dims);
      }
      LOGGER.debug("Comtrade: no data array for {}{}", safeUrl, dims);
      return false;
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
    // fallback-guard: allow per-field value parser while mapping a single row's columns; null marks that one value unparseable
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
    private final Fetch fetch;
    private Map<String, Object> pending;
    private boolean closed;
    private long rowCount;

    RecordIterator(Fetch fetch) {
      this.fetch = fetch;
      advance();
    }

    @SuppressWarnings("unchecked")
    private void advance() {
      while (true) {
        try {
          JsonToken t = fetch.parser.nextToken();
          if (t == JsonToken.START_OBJECT) {
            Map<String, Object> rec = MAPPER.readValue(fetch.parser, Map.class);
            pending = map(rec);
          } else {
            pending = null;
            finish();
          }
          return;
        } catch (IOException e) {
          if (rowCount > 0) {
            // Rows already reached the caller: a re-fetch would duplicate them.
            finish();
            throw new RuntimeException("Comtrade: read failed for "
                + SecretRedaction.redact(fetch.url) + " after " + rowCount + " rows", e);
          }
          try {
            fetch.retryOrThrow(e);
            if (!fetch.open()) {
              pending = null;
              closed = true;
              return;
            }
          } catch (IOException exhausted) {
            closed = true;
            throw new RuntimeException(exhausted.getMessage(), exhausted.getCause());
          }
        }
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
      fetch.closeQuietly();
      LOGGER.debug("Comtrade: streamed {} records from {}", rowCount,
          SecretRedaction.redact(fetch.url));
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
