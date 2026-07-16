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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Emits the HMDA fair-lending demographic breakdown for {@code hmda_applicant_demographics}.
 *
 * <p>The CFPB HMDA Data Browser aggregations endpoint permits at most two breakdown dimensions per
 * request, so {@code hmda_loans} spends its budget on {@code action_taken × loan_purpose}. This
 * companion table recovers the applicant demographics by issuing three requests per {@code (year,
 * state)} fetch unit — {@code actions_taken × races}, {@code × ethnicities}, {@code × sexes} — and
 * unions them into a long-format table keyed by {@code (demographic_dimension, demographic_value)}.
 * The base URL (already carrying {@code years}, {@code states}, and {@code actions_taken=1..8}) is
 * taken from the request context; each dimension appends its own second filter.
 */
public class HmdaApplicantDemographicsTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(HmdaApplicantDemographicsTransformer.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 60_000;
  private static final int MAX_RETRIES = 3;

  private static final Map<String, String> ACTION_LABELS;

  /** (demographic_dimension, API filter field, comma-joined value list). */
  private static final String[][] DIMENSIONS = {
      {"race", "races",
          "American Indian or Alaska Native,Asian,Black or African American,"
              + "Native Hawaiian or Other Pacific Islander,White,2 or more minority races,"
              + "Joint,Free Form Text Only,Race Not Available"},
      {"ethnicity", "ethnicities",
          "Hispanic or Latino,Not Hispanic or Latino,Joint,Ethnicity Not Available,"
              + "Free Form Text Only"},
      {"sex", "sexes", "Male,Female,Joint,Sex Not Available"},
  };

  static {
    Map<String, String> a = new HashMap<String, String>();
    a.put("1", "Loan originated");
    a.put("2", "Application approved but not accepted");
    a.put("3", "Application denied");
    a.put("4", "Application withdrawn by applicant");
    a.put("5", "File closed for incompleteness");
    a.put("6", "Purchased loan");
    a.put("7", "Preapproval request denied");
    a.put("8", "Preapproval request approved but not accepted");
    ACTION_LABELS = Collections.unmodifiableMap(a);
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String baseUrl = context.getUrl();
    if (baseUrl == null || baseUrl.isEmpty()) {
      throw new IOException("hmda_applicant_demographics: no source URL in request context");
    }
    String stateAbbr = context.getDimensionValues() != null
        ? context.getDimensionValues().get("state") : null;

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (String[] dim : DIMENSIONS) {
      String dimension = dim[0];
      String field = dim[1];
      String url = baseUrl + "&" + field + "=" + encode(dim[2]);
      String body = get(url);
      if (body == null) {
        continue;
      }
      JsonNode root = MAPPER.readTree(body);
      JsonNode aggs = root.path("aggregations");
      if (!aggs.isArray()) {
        JsonNode errorType = root.path("errorType");
        if (!errorType.isMissingNode() && !errorType.isNull()) {
          throw new IOException("HMDA API error (" + dimension + "): "
              + root.path("message").asText(errorType.asText()));
        }
        continue;
      }
      String abbr = stateAbbr != null ? stateAbbr : text(root.path("parameters"), "state");
      for (JsonNode agg : aggs) {
        String action = text(agg, "actions_taken");
        String value = text(agg, field);
        if (action == null || value == null) {
          continue;
        }
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("state_abbr", abbr);
        row.put("action_taken", action);
        row.put("action_label", ACTION_LABELS.get(action));
        row.put("demographic_dimension", dimension);
        row.put("demographic_value", value);
        JsonNode count = agg.path("count");
        row.put("loan_count", count.isNumber() ? Long.valueOf(count.asLong()) : null);
        JsonNode sum = agg.path("sum");
        row.put("loan_amount_total", sum.isNumber() ? Double.valueOf(sum.asDouble()) : null);
        rows.add(row);
      }
    }
    LOGGER.debug("hmda_applicant_demographics[{}]: {} rows", stateAbbr, rows.size());
    return rows.iterator();
  }

  private static String encode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8").replace("+", "%20");
    } catch (java.io.UnsupportedEncodingException e) {
      throw new IllegalStateException("UTF-8 unavailable", e);
    }
  }

  /** Performs a GET, retrying 429/5xx with backoff; returns the body, or null on 404. */
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
        if (status == HttpURLConnection.HTTP_NOT_FOUND) {
          return null;
        }
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
      throw new IOException("interrupted during HMDA retry backoff", e);
    }
  }

  private static String text(JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    return v.isMissingNode() || v.isNull() || v.asText().isEmpty() ? null : v.asText();
  }
}
