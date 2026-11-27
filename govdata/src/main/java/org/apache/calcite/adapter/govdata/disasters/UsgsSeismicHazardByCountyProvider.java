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
import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.NetworkOutageWait;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code disasters.usgs_seismic_hazard_by_county}: USGS ground-shaking hazard
 * and ASCE 7-22 seismic design values, one row per county.
 *
 * <p>The county list and the point each county is evaluated at come from the Census Bureau's 2020
 * centers of population (the table's {@code source.url}). Each point is sent twice to the USGS
 * seismic design web service: once for site class BC (the reference rock condition, which also
 * returns the uniform-hazard 2%-in-50-years peak ground acceleration) and once for the ASCE 7-22
 * default site class (design values and Seismic Design Category when the soil is unknown).
 *
 * <p>The table is a whole-table snapshot, so any county that cannot be fetched or parsed fails
 * the run; a partial county list is never written.
 */
public class UsgsSeismicHazardByCountyProvider implements DataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UsgsSeismicHazardByCountyProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static final String SERVICE_URL =
      "https://earthquake.usgs.gov/ws/building-codes/asce7-22/calculate";
  static final String SITE_CLASS_REFERENCE = "BC";
  static final String SITE_CLASS_DEFAULT = "Default";
  static final String METHOD = "USGS ASCE7-22 web service at the 2020 Census county center of "
      + "population; risk category II";

  private static final String CENTROID_HEADER =
      "STATEFP,COUNTYFP,COUNAME,STNAME,POPULATION,LATITUDE,LONGITUDE";
  private static final String USER_AGENT = "GovData/1.0";
  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 120_000;
  private static final int MAX_ATTEMPTS = 5;
  private static final long BACKOFF_MS = 2_000L;
  private static final long PAUSE_BETWEEN_REQUESTS_MS = 100L;

  /** One county and the point at which its hazard is evaluated. */
  static final class County {
    final String stateFips;
    final String countyFips;
    final String name;
    final String stateName;
    final int population;
    final double latitude;
    final double longitude;

    County(String stateFips, String countyFips, String name, String stateName, int population,
        double latitude, double longitude) {
      this.stateFips = stateFips;
      this.countyFips = countyFips;
      this.name = name;
      this.stateName = stateName;
      this.population = population;
      this.latitude = latitude;
      this.longitude = longitude;
    }
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    List<County> counties = parseCentroids(get(config.getSource().getUrl()));
    LOGGER.info("usgs_seismic_hazard_by_county: {} counties to evaluate", counties.size());
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>(counties.size());
    for (County county : counties) {
      String reference = get(serviceUrl(county, SITE_CLASS_REFERENCE));
      pause(PAUSE_BETWEEN_REQUESTS_MS);
      String siteDefault = get(serviceUrl(county, SITE_CLASS_DEFAULT));
      pause(PAUSE_BETWEEN_REQUESTS_MS);
      rows.add(buildRow(county, reference, siteDefault));
      if (rows.size() % 250 == 0) {
        LOGGER.info("usgs_seismic_hazard_by_county: {} of {} counties", rows.size(),
            counties.size());
      }
    }
    return rows.iterator();
  }

  static String serviceUrl(County county, String siteClass) {
    return SERVICE_URL + "?latitude=" + county.latitude + "&longitude=" + county.longitude
        + "&riskCategory=II&siteClass=" + siteClass + "&title=" + county.countyFips;
  }

  static List<County> parseCentroids(String csv) throws IOException {
    String[] lines = csv.split("\r?\n");
    String header = lines[0].startsWith("﻿") ? lines[0].substring(1) : lines[0];
    if (!CENTROID_HEADER.equals(header.trim())) {
      throw new IOException("Census centers of population: unexpected header '" + header + "'");
    }
    List<County> out = new ArrayList<County>(lines.length);
    for (int i = 1; i < lines.length; i++) {
      if (lines[i].trim().isEmpty()) {
        continue;
      }
      String[] f = lines[i].split(",", -1);
      if (f.length != 7) {
        throw new IOException("Census centers of population: expected 7 fields, got "
            + f.length + " in '" + lines[i] + "'");
      }
      out.add(
          new County(f[0].trim(), f[0].trim() + f[1].trim(), f[2].trim(), f[3].trim(),
              Integer.parseInt(f[4].trim()), Double.parseDouble(f[5].trim()),
              Double.parseDouble(f[6].trim())));
    }
    if (out.isEmpty()) {
      throw new IOException("Census centers of population: no county rows");
    }
    return out;
  }

  static Map<String, Object> buildRow(County county, String referenceJson, String defaultJson)
      throws IOException {
    JsonNode reference = response(county, referenceJson);
    JsonNode siteDefault = response(county, defaultJson);
    JsonNode ref = reference.path("data");
    JsonNode def = siteDefault.path("data");

    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("state_fips", county.stateFips);
    row.put("county_fips", county.countyFips);
    row.put("county_name", county.name);
    row.put("state_name", county.stateName);
    row.put("population_2020", county.population);
    row.put("latitude", county.latitude);
    row.put("longitude", county.longitude);
    row.put("pga_2pct50yr_g", number(county, ref.path("underlyingData"), "pgauh"));
    row.put("ss_g", number(county, def, "ss"));
    row.put("s1_g", number(county, def, "s1"));
    row.put("sds_g", number(county, def, "sds"));
    row.put("sd1_g", number(county, def, "sd1"));
    row.put("pgam_g", number(county, def, "pgam"));
    row.put("seismic_design_category", string(county, def, "sdc"));
    row.put("sds_site_class_bc_g", number(county, ref, "sds"));
    row.put("sd1_site_class_bc_g", number(county, ref, "sd1"));
    row.put("model_version", string(county, siteDefault.path("metadata"), "modelVersion"));
    row.put("method", METHOD);
    return row;
  }

  private static JsonNode response(County county, String json) throws IOException {
    JsonNode root = MAPPER.readTree(json);
    String status = root.path("request").path("status").asText();
    if (!"success".equals(status)) {
      throw new IOException("USGS design service, county " + county.countyFips
          + ": request status '" + status + "'");
    }
    return root.path("response");
  }

  private static Double number(County county, JsonNode node, String field) throws IOException {
    JsonNode v = node.path(field);
    if (!v.isNumber()) {
      throw new IOException("USGS design service, county " + county.countyFips
          + ": no numeric '" + field + "' (got " + v + ")");
    }
    return v.asDouble();
  }

  private static String string(County county, JsonNode node, String field) throws IOException {
    JsonNode v = node.path(field);
    if (!v.isTextual() || v.asText().isEmpty()) {
      throw new IOException("USGS design service, county " + county.countyFips
          + ": no text '" + field + "' (got " + v + ")");
    }
    return v.asText();
  }

  /** GET with backoff on connection errors, 429 and 5xx; any other status fails at once. */
  private static String get(String url) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", USER_AGENT);
      try {
        NetworkOutageWait.connect(conn);
        int status = conn.getResponseCode();
        if (status == HttpURLConnection.HTTP_OK) {
          return readBody(CompleteBody.of(conn));
        }
        IOException failure = new IOException("HTTP " + status + " from " + url);
        if (status != 429 && status < 500) {
          throw new PermanentHttpException(failure.getMessage());
        }
        last = failure;
      } catch (PermanentHttpException e) {
        throw e;
      } catch (IOException e) {
        last = e;
      } finally {
        conn.disconnect();
      }
      LOGGER.warn("Attempt {}/{} failed: {}", attempt, MAX_ATTEMPTS, last.getMessage());
      if (attempt < MAX_ATTEMPTS) {
        pause(BACKOFF_MS * attempt);
      }
    }
    throw new IOException("Gave up after " + MAX_ATTEMPTS + " attempts: " + url, last);
  }

  /** An HTTP status that retrying cannot change. */
  private static final class PermanentHttpException extends IOException {
    PermanentHttpException(String message) {
      super(message);
    }
  }

  private static String readBody(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }

  private static void pause(long millis) throws IOException {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while fetching USGS design values", e);
    }
  }
}
