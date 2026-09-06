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
package org.apache.calcite.adapter.govdata.lands;

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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Streaming transformer for {@code padus_federal_fee_lands} (USGS GAP Analysis Project's
 * authoritative federal-fee-land layer, {@code Federal_Fee_Managers_Authoritative_PADUS}).
 *
 * <p>The layer holds ~5,361 polygon features, under the service's {@code maxRecordCount} of
 * 1000 per page, so this transformer pages the FeatureServer {@code /query} endpoint with
 * {@code resultOffset}, fetching one bounded page at a time and yielding rows lazily (memory
 * stays O(page)) — the same pattern as {@code disasters.wildfire_perimeters}'s
 * {@code WfigsPerimeterStreamingTransformer}. Each feature's Esri ring geometry is converted to
 * simplified WKT via {@link PadusGeometryConverter}. The query requests {@code outSR=4326} so
 * geometry arrives already in WGS84 (no Web Mercator area-correction needed, unlike
 * {@code NpsUnitBoundaryTransformer}).
 *
 * <p>This layer carries no {@code county_fips} column — assigning per-county ownership shares
 * requires a spatial join (e.g. DuckDB {@code ST_Intersects} against {@code geo.counties}) done
 * downstream as a view, not at ingest time.
 */
public class PadusFederalFeeLandsStreamingTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PadusFederalFeeLandsStreamingTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // Small page size: a handful of federal administrative units (e.g. BLM's Alaska Arctic
  // District Office, ~20M acres) carry tens of MB of raw ring coordinates each — a checkerboard
  // land pattern with many disjoint parts, not redundant vertices, so no maxAllowableOffset
  // tolerance collapses the part count. A large page can combine several such features into a
  // 100+MB response that stalls past readTimeout; keeping pages small bounds the worst case to
  // one or two oversized features per request instead of dozens.
  private static final int PAGE_SIZE = 100;
  private static final int CONNECT_TIMEOUT_MS = 60_000;
  // Generous: a single oversized feature can take 10-15s even after server-side generalization.
  private static final int READ_TIMEOUT_MS = 600_000;
  // Degrees (~100m at the equator) — matches PadusGeometryConverter's own client-side
  // TopologyPreservingSimplifier tolerance, so asking ArcGIS to generalize server-side loses no
  // precision beyond what would be discarded locally anyway, while cutting transfer size
  // substantially for the largest features (confirmed live: one ~47MB feature drops to ~19MB).
  private static final String MAX_ALLOWABLE_OFFSET = "0.001";

  private static final String OUT_FIELDS = String.join(",",
      "OBJECTID", "Own_Type", "Own_Name", "Mang_Type", "Mang_Name", "Des_Tp", "Unit_Nm",
      "State_Nm", "GIS_Acres");

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    return new FeatureIterator(context.getUrl());
  }

  /** Lazily pages the FeatureServer via resultOffset, one page of features at a time. */
  private static final class FeatureIterator implements Iterator<Map<String, Object>> {
    private final String baseUrl;
    private int offset;
    private JsonNode features;
    private int featureIdx;
    private boolean exhausted;

    FeatureIterator(String baseUrl) throws IOException {
      this.baseUrl = baseUrl;
      fetchPage();
    }

    private void fetchPage() throws IOException {
      String url = baseUrl
          + (baseUrl.contains("?") ? "&" : "?")
          + "where=" + enc("1=1")
          + "&outFields=" + enc(OUT_FIELDS)
          + "&returnGeometry=true&outSR=4326&f=json"
          + "&orderByFields=" + enc("OBJECTID")
          + "&maxAllowableOffset=" + MAX_ALLOWABLE_OFFSET
          + "&resultRecordCount=" + PAGE_SIZE
          + "&resultOffset=" + offset;
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      try {
        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
          throw new IOException("HTTP " + conn.getResponseCode() + " from PAD-US query");
        }
        JsonNode root;
        try (BufferedReader r = new BufferedReader(
            new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
          root = MAPPER.readTree(r);
        }
        if (root.path("error").isObject()) {
          throw new IOException("PAD-US query error: " + root.path("error").toString());
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
        if (features.size() < PAGE_SIZE) {
          exhausted = true;
        } else {
          offset += PAGE_SIZE;
          try {
            fetchPage();
          } catch (IOException e) {
            throw new RuntimeException("padus_federal_fee_lands: page fetch failed at offset "
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

    String objectId = text(a, "OBJECTID");
    row.put("padus_id", objectId != null ? "PADUS-" + objectId : null);
    row.put("own_type", text(a, "Own_Type"));
    row.put("own_name", text(a, "Own_Name"));
    row.put("mang_type", text(a, "Mang_Type"));
    row.put("mang_name", text(a, "Mang_Name"));
    row.put("designation_type", text(a, "Des_Tp"));
    row.put("unit_name", text(a, "Unit_Nm"));
    row.put("state_abbr", text(a, "State_Nm"));
    row.put("acres", dbl(a, "GIS_Acres"));
    row.put("geometry_wkt", PadusGeometryConverter.convert(feature.path("geometry")));

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

  private static Double dbl(JsonNode node, String field) {
    JsonNode v = node.path(field);
    return v.isMissingNode() || v.isNull() ? null : v.asDouble();
  }
}
