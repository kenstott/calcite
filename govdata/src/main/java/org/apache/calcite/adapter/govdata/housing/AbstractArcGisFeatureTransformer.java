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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Base for HUD Open Data ArcGIS FeatureServer transformers. The configured source URL is a
 * {@code .../FeatureServer/{layer}/query} endpoint returning {@code {"features":[{"attributes":{…}}]}};
 * ArcGIS caps a page at {@code maxRecordCount} (2000) and flags {@code exceededTransferLimit} when
 * more rows remain, so this base pages with {@code &resultOffset=N} until the layer is exhausted.
 *
 * <p>The source URL must already carry the query parameters ({@code where}, {@code outFields},
 * {@code returnGeometry=false}, {@code f=json}, {@code resultRecordCount}); only {@code resultOffset}
 * is appended per page. Rows are produced lazily one page at a time (contract of
 * {@link StreamingResponseTransformer}), so memory stays O(page) regardless of layer size.
 */
abstract class AbstractArcGisFeatureTransformer implements StreamingResponseTransformer {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 120_000;
  private static final int MAX_RETRIES = 3;
  private static final int PAGE_SIZE = 2000;
  private static final int MAX_PAGES = 1000;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Maps one feature's {@code attributes} node to a schema-shaped row, or null to skip it. */
  protected abstract Map<String, Object> mapAttributes(JsonNode attrs);

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String baseUrl = context.getUrl();
    if (baseUrl == null || baseUrl.isEmpty()) {
      throw new IOException(getClass().getSimpleName() + ": no source URL in request context");
    }
    return new PageIterator(baseUrl);
  }

  /** Fetches one page of features at the given offset; sets {@link #lastExceeded}. */
  private final class PageIterator implements Iterator<Map<String, Object>> {
    private final String baseUrl;
    private Iterator<JsonNode> page = Collections.emptyIterator();
    private int offset;
    private int pagesFetched;
    private boolean exhausted;
    private boolean lastExceeded;
    private Map<String, Object> nextRow;

    PageIterator(String baseUrl) throws IOException {
      this.baseUrl = baseUrl;
      advance();
    }

    private void advance() throws IOException {
      nextRow = null;
      while (true) {
        while (page.hasNext()) {
          JsonNode attrs = page.next().path("attributes");
          if (!attrs.isObject()) {
            continue;
          }
          Map<String, Object> row = mapAttributes(attrs);
          if (row != null) {
            nextRow = row;
            return;
          }
        }
        if (exhausted) {
          return;
        }
        List<JsonNode> feats = fetchPage(offset);
        if (feats.isEmpty()) {
          exhausted = true;
          return;
        }
        offset += feats.size();
        exhausted = !lastExceeded;
        page = feats.iterator();
      }
    }

    private List<JsonNode> fetchPage(int off) throws IOException {
      if (++pagesFetched > MAX_PAGES) {
        throw new IOException(getClass().getSimpleName() + ": exceeded " + MAX_PAGES
            + " pages (offset " + off + ") — pagination did not terminate");
      }
      String body = get(baseUrl + "&resultOffset=" + off);
      JsonNode root = MAPPER.readTree(body);
      JsonNode error = root.path("error");
      if (error.isObject()) {
        throw new IOException(getClass().getSimpleName() + ": ArcGIS error "
            + error.path("code").asText() + " " + error.path("message").asText());
      }
      lastExceeded = root.path("exceededTransferLimit").asBoolean(false);
      JsonNode features = root.path("features");
      if (!features.isArray() || features.size() == 0) {
        return Collections.emptyList();
      }
      List<JsonNode> out = new ArrayList<JsonNode>(features.size());
      for (JsonNode f : features) {
        out.add(f);
      }
      if (out.size() >= PAGE_SIZE && !lastExceeded) {
        // full page with no explicit continuation flag: keep paging defensively
        lastExceeded = true;
      }
      logger.debug("{}: page at offset {} -> {} features (more={})",
          getClass().getSimpleName(), off, out.size(), lastExceeded);
      return out;
    }

    @Override public boolean hasNext() {
      return nextRow != null;
    }

    @Override public Map<String, Object> next() {
      if (nextRow == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = nextRow;
      try {
        advance();
      } catch (IOException e) {
        throw new RuntimeException(getClass().getSimpleName() + ": paging failed", e);
      }
      return row;
    }
  }

  /** Performs a GET, retrying 429/5xx with backoff; returns the body. */
  private String get(String url) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      conn.setRequestProperty("Accept", "application/json");
      try {
        int status = conn.getResponseCode();
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
      throw new IOException("interrupted during ArcGIS retry backoff", e);
    }
  }

  // --- shared attribute helpers ---------------------------------------------

  protected static String text(JsonNode attrs, String field) {
    JsonNode v = attrs.path(field);
    return v.isMissingNode() || v.isNull() || v.asText().isEmpty() ? null : v.asText();
  }

  protected static Integer intg(JsonNode v) {
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

  protected static Double dbl(JsonNode v) {
    if (v.isMissingNode() || v.isNull()) {
      return null;
    }
    if (v.isNumber()) {
      return v.asDouble();
    }
    String s = v.asText().trim();
    if (s.isEmpty()) {
      return null;
    }
    try {
      return Double.parseDouble(s);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
