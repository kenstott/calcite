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

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;

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

  private static final int GATEWAY_TIMEOUT = 524;
  private static final int NO_CONTENT = 204;
  private static final int FULL_CODE_LENGTH = 6;
  private static final Pattern UNFILTERED_HS6 =
      Pattern.compile("[?&]COMM_LVL=HS6(&|$)");

  /** A non-200 Census response, carrying the status so callers can react to it. */
  static final class CensusHttpException extends IOException {
    private final int status;

    CensusHttpException(int status, String url) {
      super("HTTP " + status + " from " + url);
      this.status = status;
    }

    int status() {
      return status;
    }
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    if (url == null || url.isEmpty()) {
      throw new IllegalStateException("CensusTradeStreamingTransformer: no URL in context");
    }
    String commodityParam = commodityParam(url);
    if (commodityParam != null) {
      return new PrefixSplitIterator(url, commodityParam, context);
    }
    return fetchOne(url, context, false, false);
  }

  /**
   * Returns the commodity filter parameter ({@code I_COMMODITY} or {@code E_COMMODITY}) when the
   * URL is a national HS6 request with no commodity filter of its own, else null. Such a month is
   * one very large origin query that Census answers with a gateway timeout (HTTP 524) when its
   * backend is slow, so it is fetched as commodity-prefix sub-requests instead.
   */
  private static String commodityParam(String url) {
    if (!UNFILTERED_HS6.matcher(url).find() || url.contains("_COMMODITY=")) {
      return null;
    }
    if (url.contains("/intltrade/imports/hs?")) {
      return "I_COMMODITY";
    }
    if (url.contains("/intltrade/exports/hs?")) {
      return "E_COMMODITY";
    }
    return null;
  }

  private Iterator<Map<String, Object>> fetchOne(String url, RequestContext context,
      boolean prefixed, boolean failFastOnGatewayTimeout) throws IOException {
    Path spool = spoolResponse(url, prefixed, failFastOnGatewayTimeout);
    JsonParser parser;
    try {
      parser = JSON_FACTORY.createParser(Files.newInputStream(spool));
    } catch (IOException | RuntimeException e) {
      deleteQuietly(spool);
      throw e;
    }

    // Classify the response shape from its leading tokens (streaming — no full parse).
    JsonToken t1 = parser.nextToken();
    if (t1 == JsonToken.START_OBJECT) {
      // Error/wrapper object, e.g. {"error": "..."}. Small — safe to read fully.
      String err = readErrorObject(parser);
      close(parser, spool);
      return handleNonData(err, context);
    }
    if (t1 != JsonToken.START_ARRAY) {
      close(parser, spool);
      throw new IOException("Census trade: unexpected top-level token " + t1 + " for " + url);
    }
    JsonToken t2 = parser.nextToken();
    if (t2 == JsonToken.END_ARRAY) {
      close(parser, spool);
      return Collections.<Map<String, Object>>emptyList().iterator();
    }
    if (t2 == JsonToken.VALUE_STRING) {
      // Array-of-strings error shape, e.g. ["error: unknown variable 'X'"].
      String err = parser.getText();
      close(parser, spool);
      return handleNonData(err, context);
    }
    if (t2 != JsonToken.START_ARRAY) {
      close(parser, spool);
      throw new IOException("Census trade: expected header row array, got " + t2 + " for " + url);
    }

    // t2 is the header row's START_ARRAY. Read the (small) header, then stream data rows.
    String[] headers = readRow(parser);
    return new RowIterator(parser, spool, headers, context.getUrl());
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

  /**
   * Fetches the whole response body to a temp file before any row is parsed. A connection reset
   * mid-body (seen on 2026-10-20: two trade periods lost to "Connection reset") then fails this
   * fetch, not a half-consumed row stream, so the whole request is repeated, up to
   * {@link #MAX_RETRIES} attempts. The body is on disk, not in memory: a statehs month is ~170 MB.
   */
  private static Path spoolResponse(String url, boolean prefixed,
      boolean failFastOnGatewayTimeout)
      throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      Path spool = Files.createTempFile("census-trade-", ".json");
      HttpURLConnection conn = null;
      try {
        conn = openWithRetry(url);
        long copied;
        try (InputStream in = conn.getInputStream()) {
          copied = Files.copy(in, spool, StandardCopyOption.REPLACE_EXISTING);
        }
        // HttpURLConnection ends a body that the server cut short at the declared length without
        // raising, so a short copy is checked explicitly; a chunked body (-1) has no length to check.
        long declared = conn.getContentLengthLong();
        if (declared >= 0 && copied != declared) {
          throw new java.net.SocketException("truncated body: " + copied + " of " + declared
              + " bytes");
        }
        return spool;
      } catch (IOException e) {
        deleteQuietly(spool);
        if (prefixed && (isStatus(e, NO_CONTENT)
            || failFastOnGatewayTimeout && isStatus(e, GATEWAY_TIMEOUT))) {
          throw e;
        }
        last = e;
        LOGGER.warn("Census trade: body fetch failed (attempt {}/{}) for {}: {}", attempt,
            MAX_RETRIES, url, e.toString());
        sleep(attempt);
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
    }
    throw new IOException("Census trade: read failed after " + MAX_RETRIES + " attempts for " + url,
        last);
  }

  private static boolean isStatus(IOException e, int status) {
    return e instanceof CensusHttpException && ((CensusHttpException) e).status() == status;
  }

  private static void deleteQuietly(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      LOGGER.warn("Census trade: could not delete spool file {}: {}", path, e.toString());
    }
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
        NetworkOutageWait.connect(conn);
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
      throw new CensusHttpException(status, url);
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

  private static void close(JsonParser parser, Path spool) {
    try {
      parser.close();
    } catch (IOException ignored) {
      // best-effort
    }
    deleteQuietly(spool);
  }

  /**
   * Fetches an unfiltered HS6 month as commodity-prefix sub-requests, starting at the 2-digit HS
   * chapters. A prefix Census answers with a gateway timeout is replaced by its ten child
   * prefixes (down to the full 6-digit code length, where the timeout is raised as a failure);
   * a prefix with no commodities (HTTP 204) contributes no rows. Prefixes are disjoint, so the
   * union of the sub-responses is exactly the unfiltered month. One sub-response is resident at
   * a time and rows stream through unchanged.
   */
  private final class PrefixSplitIterator implements Iterator<Map<String, Object>> {
    private final String baseUrl;
    private final String commodityParam;
    private final RequestContext context;
    private final Deque<String> prefixes = new ArrayDeque<String>();
    private Iterator<Map<String, Object>> current = Collections.emptyIterator();

    PrefixSplitIterator(String baseUrl, String commodityParam, RequestContext context) {
      this.baseUrl = baseUrl;
      this.commodityParam = commodityParam;
      this.context = context;
      for (int chapter = 1; chapter <= 99; chapter++) {
        prefixes.add(String.format("%02d", chapter));
      }
    }

    @Override public boolean hasNext() {
      while (!current.hasNext()) {
        if (prefixes.isEmpty()) {
          return false;
        }
        current = fetchPrefix(prefixes.pollFirst());
      }
      return true;
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return current.next();
    }

    private Iterator<Map<String, Object>> fetchPrefix(String prefix) {
      String url = baseUrl + "&" + commodityParam + "=" + prefix + "*";
      try {
        return fetchOne(url, context, true, prefix.length() < FULL_CODE_LENGTH);
      } catch (CensusHttpException e) {
        if (e.status() == NO_CONTENT) {
          return Collections.<Map<String, Object>>emptyList().iterator();
        }
        if (e.status() == GATEWAY_TIMEOUT && prefix.length() < FULL_CODE_LENGTH) {
          LOGGER.warn("Census trade: HTTP 524 for commodity prefix {}, splitting into {}0-{}9",
              prefix, prefix, prefix);
          for (int digit = 9; digit >= 0; digit--) {
            prefixes.addFirst(prefix + digit);
          }
          return Collections.<Map<String, Object>>emptyList().iterator();
        }
        throw new RuntimeException("Census trade: prefix " + prefix + " failed", e);
      } catch (IOException e) {
        throw new RuntimeException("Census trade: prefix " + prefix + " failed", e);
      }
    }
  }

  /** Lazy iterator: parses one data row (inner array) per {@code next()}; closes on exhaustion. */
  private static final class RowIterator implements Iterator<Map<String, Object>> {
    private final JsonParser parser;
    private final Path spool;
    private final String[] headers;
    private final String url;
    private String[] pending;
    private boolean closed;
    private long rowCount;

    RowIterator(JsonParser parser, Path spool, String[] headers, String url) {
      this.parser = parser;
      this.spool = spool;
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
      close(parser, spool);
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
