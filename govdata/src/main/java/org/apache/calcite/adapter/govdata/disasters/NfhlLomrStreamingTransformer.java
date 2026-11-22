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
package org.apache.calcite.adapter.govdata.disasters;

import org.apache.calcite.adapter.file.etl.CompleteBody;
import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Streaming transformer for {@code nfhl_lomr} (FEMA National Flood Hazard Layer, layer 1 "LOMRs").
 *
 * <p>Pages the NFHL MapServer {@code /1/query} endpoint with {@code resultOffset} (the layer holds
 * about 10k Letter of Map Revision polygons; the server caps a page at 2000). Each feature's Esri
 * ring geometry is converted to simplified WGS84 WKT plus a centroid via
 * {@link EsriGeometryConverter}. {@code EFF_DATE} is epoch-millis. The first five characters of
 * {@code DFIRM_ID} are the state+county FIPS code.
 */
public class NfhlLomrStreamingTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NfhlLomrStreamingTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ISO_LOCAL_DATE;

  private static final int INITIAL_PAGE_SIZE = 1000;
  // A failed page is retried with half as many features so a size-related truncation is not
  // repeated verbatim.
  private static final int MIN_PAGE_SIZE = 50;
  private static final int CONNECT_TIMEOUT_MS = 60_000;
  // Per-page read timeout: a stuck connection must fail fast enough for the retry to help.
  private static final int READ_TIMEOUT_MS = 90_000;
  // fetchPage() manages its own HttpURLConnection (the StreamingResponseTransformer contract
  // bypasses HttpSource's shared retry path), so it retries on its own. After the last attempt
  // the failure propagates; a partial result is never returned as complete.
  private static final int MAX_PAGE_RETRIES = 4;
  private static final long RETRY_BACKOFF_MS = 3_000L;

  private static final String OUT_FIELDS = String.join(",",
      "OBJECTID", "DFIRM_ID", "VERSION_ID", "LOMR_ID", "EFF_DATE", "CASE_NO", "SCALE", "STATUS",
      "SOURCE_CIT");

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    return new LomrIterator(context.getUrl());
  }

  /** Lazily pages the FeatureServer via resultOffset, one page of features at a time. */
  private static final class LomrIterator implements Iterator<Map<String, Object>> {
    private final String baseUrl;
    private int offset;
    private int pageSize = INITIAL_PAGE_SIZE;
    private JsonNode features;
    private int featureIdx;
    private boolean exhausted;

    LomrIterator(String baseUrl) throws IOException {
      this.baseUrl = baseUrl;
      fetchPage();
    }

    private void fetchPage() throws IOException {
      IOException last = null;
      for (int attempt = 0; attempt <= MAX_PAGE_RETRIES; attempt++) {
        if (attempt > 0) {
          LOGGER.warn("nfhl_lomr: retrying page at offset {} (attempt {}/{}): {}",
              offset, attempt + 1, MAX_PAGE_RETRIES + 1, last.getMessage());
          try {
            Thread.sleep(RETRY_BACKOFF_MS * attempt);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while retrying NFHL page fetch", ie);
          }
        }
        try {
          fetchPageOnce();
          return;
        } catch (IOException e) {
          last = e;
          pageSize = Math.max(MIN_PAGE_SIZE, pageSize / 2);
        }
      }
      throw last;
    }

    private void fetchPageOnce() throws IOException {
      String url = baseUrl
          + (baseUrl.contains("?") ? "&" : "?")
          + "where=" + enc("1=1")
          + "&outFields=" + enc(OUT_FIELDS)
          + "&returnGeometry=true&outSR=4326&f=json"
          + "&orderByFields=" + enc("OBJECTID")
          + "&resultRecordCount=" + pageSize
          + "&resultOffset=" + offset;
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      try {
        NetworkOutageWait.connect(conn);
        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
          throw new IOException("HTTP " + conn.getResponseCode() + " from NFHL query");
        }
        JsonNode root;
        try (BufferedReader r = new BufferedReader(
            new InputStreamReader(CompleteBody.of(conn), StandardCharsets.UTF_8))) {
          root = MAPPER.readTree(r);
        }
        if (root.path("error").isObject()) {
          throw new IOException("NFHL query error: " + root.path("error").toString());
        }
        features = root.path("features");
        featureIdx = 0;
        if (!features.isArray() || features.size() == 0) {
          exhausted = true;
        }
      } finally {
        conn.disconnect();
      }
    }

    @Override public boolean hasNext() {
      return !exhausted && features.isArray() && featureIdx < features.size();
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      JsonNode feature = features.get(featureIdx++);
      Map<String, Object> row = mapFeature(feature);
      if (featureIdx >= features.size()) {
        // Page consumed; a full page implies there may be more.
        if (features.size() < pageSize) {
          exhausted = true;
        } else {
          offset += pageSize;
          try {
            fetchPage();
          } catch (IOException e) {
            throw new RuntimeException("nfhl_lomr: page fetch failed at offset "
                + offset, e);
          }
        }
      }
      return row;
    }
  }

  private static Map<String, Object> mapFeature(JsonNode feature) {
    JsonNode a = feature.path("attributes");
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("objectid", a.path("OBJECTID").asLong());
    String dfirm = text(a, "DFIRM_ID");
    row.put("dfirm_id", dfirm);
    String county = dfirm != null && dfirm.length() >= 5 ? dfirm.substring(0, 5) : null;
    row.put("state_fips", county != null ? county.substring(0, 2) : null);
    row.put("county_fips", county);
    row.put("lomr_id", text(a, "LOMR_ID"));
    row.put("case_number", text(a, "CASE_NO"));
    Long eff = epochMillis(a, "EFF_DATE");
    row.put("effective_date", eff != null ? isoDate(eff) : null);
    row.put("effective_year", eff != null ? yearOf(eff) : null);
    row.put("status", text(a, "STATUS"));
    row.put("scale", text(a, "SCALE"));
    row.put("version_id", text(a, "VERSION_ID"));
    row.put("source_citation", text(a, "SOURCE_CIT"));
    EsriGeometryConverter.Result geom = EsriGeometryConverter.convert(feature.path("geometry"));
    if (geom != null) {
      row.put("centroid_lat", geom.centroidLat);
      row.put("centroid_lon", geom.centroidLon);
      row.put("geometry_wkt", geom.wkt);
    } else {
      row.put("centroid_lat", null);
      row.put("centroid_lon", null);
      row.put("geometry_wkt", null);
    }
    return row;
  }

  private static String enc(String v) {
    try {
      return URLEncoder.encode(v, "UTF-8");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node.path(field);
    if (v.isMissingNode() || v.isNull()) {
      return null;
    }
    String s = v.asText().trim();
    return s.isEmpty() ? null : s;
  }

  private static Long epochMillis(JsonNode node, String field) {
    JsonNode v = node.path(field);
    return v.isMissingNode() || v.isNull() || !v.canConvertToLong() ? null : v.asLong();
  }

  private static Integer yearOf(long epochMs) {
    return Instant.ofEpochMilli(epochMs).atZone(ZoneOffset.UTC).getYear();
  }

  private static String isoDate(long epochMs) {
    return Instant.ofEpochMilli(epochMs).atZone(ZoneOffset.UTC).toLocalDate().format(ISO_DATE);
  }
}
