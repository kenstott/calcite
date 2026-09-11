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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Streaming transformer for {@code disasters.fema_nri_earthquake_by_county} — FEMA National Risk
 * Index (NRI) county-level earthquake hazard, exposure, and expected-annual-loss data.
 *
 * <p>Hosted on FEMA's own ArcGIS Online tenant ({@code services.arcgis.com/XG15cJAlne2vxtgt}) as
 * the {@code NRI_Counties_Prod_v1181_view} FeatureServer layer — the canonical {@code
 * hazards.fema.gov/nri} URLs 301-redirect to a landing page, not a data endpoint (confirmed live
 * 2026-09-11). Same "individually/agency-hosted ArcGIS mirror, not the .fema.gov domain itself"
 * pattern already accepted for {@code housing.opportunity_zones} — re-verify the item id if this
 * ever 404s.
 *
 * <p>The layer carries all 18 NRI hazard types (~367 fields); this transformer selects only the
 * overall composite risk/loss fields plus the full {@code ERQK_*} (earthquake) block via
 * {@code outFields}, matching what ops#191 asked for. The other 17 hazard blocks (wildfire,
 * hurricane, flood, etc.) are real fields on the same layer, not ingested here — a follow-on
 * sourcing item, not a gap in this table.
 *
 * <p>ArcGIS caps a page at {@code resultRecordCount} (2000 here) and reports {@code
 * exceededTransferLimit} when more rows remain; this transformer pages with {@code
 * resultOffset=N} until the layer (3,142 counties) is exhausted, one page at a time — memory
 * stays O(page), never the whole 3,142-row response.
 */
public class FemaNriEarthquakeByCountyTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FemaNriEarthquakeByCountyTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 120_000;
  private static final int MAX_RETRIES = 3;
  private static final int PAGE_SIZE = 2000;
  private static final int MAX_PAGES = 10;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String baseUrl = context.getUrl();
    if (baseUrl == null || baseUrl.isEmpty()) {
      throw new IOException("FemaNriEarthquakeByCountyTransformer: no source URL in context");
    }
    return new PageIterator(baseUrl);
  }

  private static final class PageIterator implements Iterator<Map<String, Object>> {
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
          if (attrs.isObject()) {
            nextRow = mapAttributes(attrs);
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
        throw new IOException("FEMA NRI: exceeded " + MAX_PAGES + " pages (offset " + off
            + ") — pagination did not terminate");
      }
      JsonNode root = MAPPER.readTree(get(baseUrl + "&resultOffset=" + off));
      JsonNode error = root.path("error");
      if (error.isObject()) {
        throw new IOException("FEMA NRI: ArcGIS error " + error.path("code").asText() + " "
            + error.path("message").asText());
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
      LOGGER.debug("FEMA NRI: page at offset {} -> {} features (more={})", off, out.size(),
          lastExceeded);
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
        throw new RuntimeException("FEMA NRI: paging failed", e);
      }
      return row;
    }
  }

  private static Map<String, Object> mapAttributes(JsonNode attrs) {
    String stcofips = text(attrs, "STCOFIPS");
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("county_fips", stcofips);
    row.put("state_fips", stcofips != null && stcofips.length() == 5
        ? stcofips.substring(0, 2) : null);
    row.put("state_name", text(attrs, "STATE"));
    row.put("state_abbr", text(attrs, "STATEABBRV"));
    row.put("county_name", text(attrs, "COUNTY"));
    row.put("population", intg(attrs.path("POPULATION")));
    row.put("building_value_usd", dbl(attrs.path("BUILDVALUE")));
    row.put("agriculture_value_usd", dbl(attrs.path("AGRIVALUE")));
    row.put("area_sq_mi", dbl(attrs.path("AREA")));
    row.put("risk_score", dbl(attrs.path("RISK_SCORE")));
    row.put("risk_rating", text(attrs, "RISK_RATNG"));
    row.put("risk_national_pctl", dbl(attrs.path("RISK_NPCTL")));
    row.put("risk_state_pctl", dbl(attrs.path("RISK_SPCTL")));
    row.put("eal_score", dbl(attrs.path("EAL_SCORE")));
    row.put("eal_rating", text(attrs, "EAL_RATNG"));
    row.put("earthquake_events", dbl(attrs.path("ERQK_EVNTS")));
    row.put("earthquake_annualized_frequency", dbl(attrs.path("ERQK_AFREQ")));
    row.put("earthquake_exposure_building_value", dbl(attrs.path("ERQK_EXPB")));
    row.put("earthquake_exposure_population", dbl(attrs.path("ERQK_EXPP")));
    row.put("earthquake_exposure_population_equivalence", dbl(attrs.path("ERQK_EXPPE")));
    row.put("earthquake_exposure_total", dbl(attrs.path("ERQK_EXPT")));
    row.put("earthquake_historic_loss_ratio_buildings", dbl(attrs.path("ERQK_HLRB")));
    row.put("earthquake_historic_loss_ratio_population", dbl(attrs.path("ERQK_HLRP")));
    row.put("earthquake_historic_loss_ratio_rating", text(attrs, "ERQK_HLRR"));
    row.put("earthquake_eal_building_value", dbl(attrs.path("ERQK_EALB")));
    row.put("earthquake_eal_population", dbl(attrs.path("ERQK_EALP")));
    row.put("earthquake_eal_population_equivalence", dbl(attrs.path("ERQK_EALPE")));
    row.put("earthquake_eal_total", dbl(attrs.path("ERQK_EALT")));
    row.put("earthquake_eal_score", dbl(attrs.path("ERQK_EALS")));
    row.put("earthquake_eal_rating", text(attrs, "ERQK_EALR"));
    row.put("earthquake_risk_score", dbl(attrs.path("ERQK_RISKS")));
    row.put("earthquake_risk_rating", text(attrs, "ERQK_RISKR"));
    return row;
  }

  private static String text(JsonNode attrs, String field) {
    JsonNode v = attrs.path(field);
    return v.isMissingNode() || v.isNull() || v.asText().isEmpty() ? null : v.asText();
  }

  private static Integer intg(JsonNode v) {
    if (v.isMissingNode() || v.isNull()) {
      return null;
    }
    return Integer.valueOf(v.asInt());
  }

  private static Double dbl(JsonNode v) {
    if (v.isMissingNode() || v.isNull()) {
      return null;
    }
    return Double.valueOf(v.asDouble());
  }

  /** Performs a GET, retrying 429/5xx with backoff; returns the body. */
  private static String get(String url) throws IOException {
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
      throw new IOException("interrupted during FEMA NRI retry backoff", e);
    }
  }
}
