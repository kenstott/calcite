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
 * Streaming transformer for {@code wildfire_perimeters} (NIFC WFIGS Interagency Perimeters).
 *
 * <p>The layer holds ~39k polygon features — well over the ArcGIS transfer cap — so this
 * transformer pages the FeatureServer {@code /query} endpoint with {@code resultOffset}, fetching
 * one bounded page at a time and yielding rows lazily (memory stays O(page)). Each feature's Esri
 * ring geometry is converted to simplified WKT plus a centroid via {@link EsriGeometryConverter}
 * (the project's JTS stack). ArcGIS {@code f=json} date fields are epoch-millis; {@code fire_year}
 * and the date columns derive from {@code attr_FireDiscoveryDateTime}.
 */
public class WfigsPerimeterStreamingTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(WfigsPerimeterStreamingTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ISO_LOCAL_DATE;

  private static final int PAGE_SIZE = 2000;
  private static final int CONNECT_TIMEOUT_MS = 60_000;
  private static final int READ_TIMEOUT_MS = 300_000;

  private static final String OUT_FIELDS = String.join(",",
      "OBJECTID", "poly_IRWINID", "attr_UniqueFireIdentifier", "attr_IncidentName",
      "attr_FireDiscoveryDateTime", "attr_ContainmentDateTime", "attr_FireCause",
      "attr_IncidentTypeCategory", "attr_IncidentSize", "attr_FinalAcres",
      "attr_POOState", "attr_POOFips", "poly_GISAcres");

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    return new PerimeterIterator(context.getUrl());
  }

  /** Lazily pages the FeatureServer via resultOffset, one page of features at a time. */
  private static final class PerimeterIterator implements Iterator<Map<String, Object>> {
    private final String baseUrl;
    private int offset;
    private JsonNode features;
    private int featureIdx;
    private boolean exhausted;

    PerimeterIterator(String baseUrl) throws IOException {
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
          + "&resultRecordCount=" + PAGE_SIZE
          + "&resultOffset=" + offset;
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      try {
        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
          throw new IOException("HTTP " + conn.getResponseCode() + " from WFIGS query");
        }
        JsonNode root;
        try (BufferedReader r = new BufferedReader(
            new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
          root = MAPPER.readTree(r);
        }
        if (root.path("error").isObject()) {
          throw new IOException("WFIGS query error: " + root.path("error").toString());
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
            throw new RuntimeException("wildfire_perimeters: page fetch failed at offset "
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

    String irwin = text(a, "poly_IRWINID");
    String objectId = text(a, "OBJECTID");
    // Surrogate only when IRWIN is absent, so the PK is always non-null and unique.
    row.put("incident_id", irwin != null ? irwin : (objectId != null ? "WFIGS-" + objectId : null));
    row.put("incident_name", text(a, "attr_IncidentName"));
    row.put("unique_fire_identifier", text(a, "attr_UniqueFireIdentifier"));

    Long discovery = epochMillis(a, "attr_FireDiscoveryDateTime");
    row.put("fire_year", discovery != null ? yearOf(discovery) : null);
    row.put("discovery_date", discovery != null ? isoDate(discovery) : null);
    Long containment = epochMillis(a, "attr_ContainmentDateTime");
    row.put("containment_date", containment != null ? isoDate(containment) : null);

    String pooFips = digits(text(a, "attr_POOFips"));
    String countyFips = pooFips != null && pooFips.length() >= 5 ? pooFips.substring(0, 5) : null;
    row.put("state_fips", countyFips != null ? countyFips.substring(0, 2) : null);
    row.put("county_fips", countyFips);
    row.put("poo_state", text(a, "attr_POOState"));

    row.put("fire_cause", text(a, "attr_FireCause"));
    row.put("incident_type_category", text(a, "attr_IncidentTypeCategory"));
    row.put("gis_acres", dbl(a, "poly_GISAcres"));
    row.put("final_acres", dbl(a, "attr_FinalAcres"));

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

  private static Double dbl(JsonNode node, String field) {
    JsonNode v = node.path(field);
    return v.isMissingNode() || v.isNull() ? null : v.asDouble();
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

  /** Strips non-digit characters (POOFips may carry stray formatting). */
  private static String digits(String v) {
    if (v == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder(v.length());
    for (int i = 0; i < v.length(); i++) {
      char c = v.charAt(i);
      if (c >= '0' && c <= '9') {
        sb.append(c);
      }
    }
    return sb.length() == 0 ? null : sb.toString();
  }
}
