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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * True end-to-end streaming transformer for the large Census International Trade timeseries
 * tables ({@code trade_by_state}, {@code trade_exports}, {@code trade_imports}).
 *
 * <p>The Census data API returns a single JSON array-of-arrays — a header row followed by data
 * rows: {@code [["STATE","CTY_CODE",...],["06","1220",...],...]}. A {@code statehs} month is
 * ~2.1M rows / ~172 MB. The legacy {@code transform(String)} path materialised the whole
 * response String, rebuilt it as an equally large objects String, then
 * {@code HttpSource.parseJsonArrayStreaming} parsed that into a full {@code List<Map>} — three
 * copies of the payload resident at once, which OOMed worker-econ.
 *
 * <p>Implementing {@link StreamingResponseTransformer} makes {@code HttpSource} call
 * {@link #fetchAndTransform} directly: this class opens the Census URL itself and returns a lazy
 * iterator that parses one data row at a time straight off the socket, keyed by header name.
 * The Iceberg writer drains it in fixed-size chunks, so heap stays O(chunk) regardless of month
 * size. Rows are emitted keyed by the raw Census header (e.g. {@code STATE}, {@code ALL_VAL_MO});
 * the schema {@code expression:} columns map them, exactly as before.
 *
 * <p>Census suppression sentinels are coerced to null (parity with
 * {@code org.apache.calcite.adapter.govdata.geo.CensusResponseTransformer}). A "no results"
 * response yields zero rows (not an error) so empty months don't fail the batch.
 */
public class CensusTradeStreamingTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CensusTradeStreamingTransformer.class);
  private static final JsonFactory JSON_FACTORY = new ObjectMapper().getFactory();

  private static final int CONNECT_TIMEOUT_MS = 60_000;
  private static final int READ_TIMEOUT_MS = 900_000;
  private static final int MAX_RETRIES = 4;

  /** Census suppression/unavailability sentinels — coerced to null before write. */
  private static final Set<String> SENTINELS = Collections.unmodifiableSet(
      new HashSet<String>(java.util.Arrays.asList(
          "-666666666", "-333333333", "-222222222", "-999999999")));

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    if (url == null || url.isEmpty()) {
      throw new IllegalStateException("CensusTradeStreamingTransformer: no URL in context");
    }
    HttpURLConnection conn = openWithRetry(url);
    InputStream in = conn.getInputStream();
    JsonParser parser = JSON_FACTORY.createParser(in);

    // Classify the response shape from its leading tokens (streaming — no full parse).
    JsonToken t1 = parser.nextToken();
    if (t1 == JsonToken.START_OBJECT) {
      // Error/wrapper object, e.g. {"error": "..."}. Small — safe to read fully.
      String err = readErrorObject(parser);
      close(parser, conn);
      return handleNonData(err, context);
    }
    if (t1 != JsonToken.START_ARRAY) {
      close(parser, conn);
      throw new IOException("Census trade: unexpected top-level token " + t1 + " for " + url);
    }
    JsonToken t2 = parser.nextToken();
    if (t2 == JsonToken.END_ARRAY) {
      close(parser, conn);
      return Collections.<Map<String, Object>>emptyList().iterator();
    }
    if (t2 == JsonToken.VALUE_STRING) {
      // Array-of-strings error shape, e.g. ["error: unknown variable 'X'"].
      String err = parser.getText();
      close(parser, conn);
      return handleNonData(err, context);
    }
    if (t2 != JsonToken.START_ARRAY) {
      close(parser, conn);
      throw new IOException("Census trade: expected header row array, got " + t2 + " for " + url);
    }

    // t2 is the header row's START_ARRAY. Read the (small) header, then stream data rows.
    String[] headers = readRow(parser);
    return new RowIterator(parser, conn, headers, context.getUrl());
  }

  /** Reads one inner array (row) of string cells into an array. Parser is at the row START_ARRAY. */
  private static String[] readRow(JsonParser parser) throws IOException {
    List<String> cells = new ArrayList<String>();
    JsonToken t;
    while ((t = parser.nextToken()) != JsonToken.END_ARRAY && t != null) {
      cells.add(parser.getValueAsString());
    }
    return cells.toArray(new String[0]);
  }

  private static String readErrorObject(JsonParser parser) throws IOException {
    String message = null;
    JsonToken t;
    while ((t = parser.nextToken()) != JsonToken.END_OBJECT && t != null) {
      if (t == JsonToken.FIELD_NAME && "error".equals(parser.currentName())) {
        parser.nextToken();
        message = parser.getValueAsString();
      } else if (t == JsonToken.START_OBJECT || t == JsonToken.START_ARRAY) {
        parser.skipChildren();
      }
    }
    return message;
  }

  /**
   * Applies Census error semantics: recoverable "no results" style messages yield an empty
   * iterator; genuine errors (unknown variable, rate limit) throw so the batch fails loudly.
   */
  private Iterator<Map<String, Object>> handleNonData(String message, RequestContext context) {
    String dims = context.getDimensionValues().isEmpty()
        ? "" : " [dimensions: " + context.getDimensionValues() + "]";
    String lower = message == null ? "" : message.toLowerCase();
    if (message == null
        || lower.contains("did not return any results")
        || lower.contains("no data")
        || (lower.contains("invalid") && lower.contains("geography"))) {
      LOGGER.debug("Census trade: no results for {}{} - {}", context.getUrl(), dims, message);
      return Collections.<Map<String, Object>>emptyList().iterator();
    }
    throw new RuntimeException("Census API error: " + message + dims);
  }

  private static HttpURLConnection openWithRetry(String url) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
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
        LOGGER.warn("Census trade: HTTP {} (attempt {}/{}) for {}", status, attempt, MAX_RETRIES, url);
        sleep(attempt);
        continue;
      }
      throw new IOException("HTTP " + status + " from " + url);
    }
    throw last != null ? last : new IOException("Census trade: retries exhausted for " + url);
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

  /** Lazy iterator: parses one data row (inner array) per {@code next()}; closes on exhaustion. */
  private static final class RowIterator implements Iterator<Map<String, Object>> {
    private final JsonParser parser;
    private final HttpURLConnection conn;
    private final String[] headers;
    private final String url;
    private String[] pending;
    private boolean closed;
    private long rowCount;

    RowIterator(JsonParser parser, HttpURLConnection conn, String[] headers, String url) {
      this.parser = parser;
      this.conn = conn;
      this.headers = headers;
      this.url = url;
      advance();
    }

    private void advance() {
      try {
        JsonToken t;
        while ((t = parser.nextToken()) != null) {
          if (t == JsonToken.START_ARRAY) {
            pending = readRow(parser);
            return;
          }
          if (t == JsonToken.END_ARRAY) {
            break; // end of the outer table
          }
          if (t.isStructStart()) {
            parser.skipChildren(); // defensive: unexpected element
          }
        }
        pending = null;
        finish();
      } catch (IOException e) {
        finish();
        throw new RuntimeException("Census trade: read failed for " + url, e);
      }
    }

    private void finish() {
      if (closed) {
        return;
      }
      closed = true;
      close(parser, conn);
      LOGGER.debug("Census trade: streamed {} data rows from {}", rowCount, url);
    }

    @Override public boolean hasNext() {
      return pending != null;
    }

    @Override public Map<String, Object> next() {
      if (pending == null) {
        throw new NoSuchElementException();
      }
      String[] cells = pending;
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      int n = Math.min(headers.length, cells.length);
      for (int i = 0; i < n; i++) {
        String v = cells[i];
        row.put(headers[i], v != null && SENTINELS.contains(v) ? null : v);
      }
      rowCount++;
      advance();
      return row;
    }
  }
}
