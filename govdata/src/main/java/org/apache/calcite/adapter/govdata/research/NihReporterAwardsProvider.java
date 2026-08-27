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
package org.apache.calcite.adapter.govdata.research;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DataProvider for {@code nih_award_projects} — award-level NIH grant microdata from RePORTER's
 * {@code POST /v2/projects/search} API, one row per (fiscal year, awarding IC) dimension combo.
 *
 * <p>RePORTER caps every query at {@code offset <= 14999} (confirmed live: requesting a higher
 * offset returns a plain error string, not a paginated empty page) — a single fiscal year runs
 * ~85,000 awards, well over that cap. Slicing by {@code agencies} (NIH Institute/Center) keeps
 * every (year, IC) slice safely under the cap: the largest IC (NCI) was ~13,200 awards in a
 * single year when this was checked live, versus the ~15,000 ceiling. This is a real semantic
 * subdivision of the data (and IC is a useful column in its own right), not an arbitrary
 * truncation — every IC's own full page range is walked to exhaustion, so the (year, IC) union
 * is the true, complete population for that year, not a sampled prefix of it.
 */
public class NihReporterAwardsProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(NihReporterAwardsProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ENDPOINT = "https://api.reporter.nih.gov/v2/projects/search";
  private static final int PAGE_SIZE = 500;
  // RePORTER's own documented ceiling: offset + limit must not exceed this.
  private static final int MAX_OFFSET = 14999;

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String year = variables.get("year");
    String ic = variables.get("agency_ic");
    if (year == null || year.isEmpty() || ic == null || ic.isEmpty()) {
      LOGGER.warn("nih_award_projects: missing year/agency_ic in dimension variables {}", variables);
      return java.util.Collections.emptyIterator();
    }

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    // RePORTER's default sort has no documented tiebreaker, so a row can land on both sides
    // of a page boundary across two offset calls. appl_id is unique per application, so
    // dedup on it rather than trusting the API's pagination windows not to overlap.
    Set<Long> seenApplIds = new HashSet<Long>();
    int offset = 0;
    while (offset <= MAX_OFFSET) {
      String body = "{\"criteria\":{\"fiscal_years\":[" + year.trim() + "],\"agencies\":[\""
          + ic.trim() + "\"]},\"limit\":" + PAGE_SIZE + ",\"offset\":" + offset + "}";
      JsonNode root = postJson(ENDPOINT, body);
      JsonNode results = root.path("results");
      if (!results.isArray() || results.size() == 0) {
        break;
      }
      for (JsonNode r : results) {
        Long applId = longOrNull(r, "appl_id");
        if (applId == null || seenApplIds.add(applId)) {
          rows.add(toRow(r, year));
        }
      }
      if (results.size() < PAGE_SIZE) {
        break;
      }
      offset += PAGE_SIZE;
    }
    LOGGER.info("nih_award_projects: {} awards for fy={} ic={}", rows.size(), year, ic);
    return rows.iterator();
  }

  private Map<String, Object> toRow(JsonNode r, String year) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("appl_id", longOrNull(r, "appl_id"));
    row.put("fiscal_year", year);
    row.put("project_num", text(r, "project_num"));
    row.put("agency_ic", text(r.path("agency_ic_admin"), "abbreviation"));
    row.put("activity_code", text(r, "activity_code"));
    row.put("award_amount", num(r, "award_amount"));
    row.put("is_active", r.path("is_active").isBoolean() ? r.path("is_active").asBoolean() : null);
    row.put("contact_pi_name", text(r, "contact_pi_name"));
    row.put("org_name", text(r.path("organization"), "org_name"));
    row.put("org_city", text(r.path("organization"), "org_city"));
    row.put("org_state", text(r.path("organization"), "org_state"));
    row.put("org_country", text(r.path("organization"), "org_country"));
    return row;
  }

  private JsonNode postJson(String url, String jsonBody) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);
    conn.setDoOutput(true);
    try (OutputStream os = conn.getOutputStream()) {
      os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
    }
    int status = conn.getResponseCode();
    InputStream in = status >= 200 && status < 300 ? conn.getInputStream() : conn.getErrorStream();
    try {
      JsonNode node = MAPPER.readTree(in);
      if (status < 200 || status >= 300) {
        throw new IOException("NIH RePORTER HTTP " + status + ": " + node);
      }
      return node;
    } finally {
      in.close();
    }
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    String s = v.asText();
    return (s == null || s.trim().isEmpty()) ? null : s;
  }

  private static Long longOrNull(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return (v == null || v.isNull()) ? null : v.asLong();
  }

  private static Double num(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return (v == null || v.isNull()) ? null : v.asDouble();
  }
}
