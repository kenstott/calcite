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

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.RawCache;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code nih_publications} — long-format (appl_id, pmid) publication links from
 * RePORTER's {@code POST /v2/publications/search} API, one row per funded-project/publication
 * pair per fiscal year and awarding IC dimension combo.
 *
 * <p>RePORTER's publications endpoint takes a list of {@code appl_ids} (or {@code
 * core_project_nums}/{@code pmids}) as its criteria rather than a year/agency filter directly, so
 * this provider first re-walks {@code /v2/projects/search} for the same (year, agency_ic) combo
 * {@link NihReporterAwardsProvider} uses — fetching only {@code appl_id}/{@code project_num}, not
 * the full award record — to get the appl_id population for that slice, then batches those
 * appl_ids (500 per call, matching the projects-search page size) into {@code
 * /v2/publications/search} calls.
 *
 * <p>The publications response's {@code applid} is not necessarily one of the appl_ids just
 * queried — confirmed live, a batch of FY2023/FIC appl_ids surfaced a linked FY2025/NHGRI appl_id
 * in the same response (a renewal-year continuation of the same underlying project, credited on
 * the same publication). Rows are kept only when {@code applid} is a member of the batch queried,
 * so every emitted row's {@code fiscal_year}/slice is guaranteed accurate; the cross-linked
 * appl_id is captured correctly when its own (year, agency_ic) slice is walked instead.
 *
 * <p>The publications response carries no publication year (confirmed live) — only {@code
 * (coreproject, pmid, applid)} triples plus {@code meta.total} for pagination. This table
 * therefore does not attempt to derive a publication year; {@code fiscal_year} here is the
 * *funding* fiscal year of the (year, agency_ic) slice being walked, not when the paper appeared.
 */
public class NihReporterPublicationsProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(NihReporterPublicationsProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String PROJECTS_ENDPOINT = "https://api.reporter.nih.gov/v2/projects/search";
  private static final String PUBLICATIONS_ENDPOINT = "https://api.reporter.nih.gov/v2/publications/search";
  private static final int PAGE_SIZE = 500;
  // RePORTER's own documented ceiling: offset + limit must not exceed this.
  private static final int MAX_OFFSET = 14999;

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String year = variables.get("year");
    String ic = variables.get("agency_ic");
    if (year == null || year.isEmpty() || ic == null || ic.isEmpty()) {
      LOGGER.warn("nih_publications: missing year/agency_ic in dimension variables {}", variables);
      return java.util.Collections.emptyIterator();
    }

    List<Long> applIds = fetchApplIds(year, ic, rawCache);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < applIds.size(); i += PAGE_SIZE) {
      List<Long> batch = applIds.subList(i, Math.min(i + PAGE_SIZE, applIds.size()));
      rows.addAll(fetchPublicationsForBatch(batch, year, ic, rawCache, i / PAGE_SIZE));
    }
    LOGGER.info("nih_publications: {} publication links for fy={} ic={} ({} appl_ids)",
        rows.size(), year, ic, applIds.size());
    return rows.iterator();
  }

  /** Walks /v2/projects/search for this (year, ic) slice, collecting only appl_id. */
  private List<Long> fetchApplIds(String year, String ic, RawCache rawCache) throws IOException {
    List<Long> applIds = new ArrayList<Long>();
    int offset = 0;
    while (offset <= MAX_OFFSET) {
      String body = "{\"criteria\":{\"fiscal_years\":[" + year.trim() + "],\"agencies\":[\""
          + ic.trim() + "\"]},\"limit\":" + PAGE_SIZE + ",\"offset\":" + offset + "}";
      JsonNode root = postJson(PROJECTS_ENDPOINT, body, rawCache, "applids-offset-" + offset);
      JsonNode results = root.path("results");
      if (!results.isArray() || results.size() == 0) {
        break;
      }
      for (JsonNode r : results) {
        Long applId = longOrNull(r, "appl_id");
        if (applId != null) {
          applIds.add(applId);
        }
      }
      if (results.size() < PAGE_SIZE) {
        break;
      }
      offset += PAGE_SIZE;
    }
    return applIds;
  }

  /**
   * Fetches every publication linked to one batch of up to 500 appl_ids, paging to exhaustion.
   *
   * <p>Confirmed live: the publications response's {@code applid} field is not necessarily one of
   * the appl_ids in the query batch — a project spanning renewal years gets a new appl_id each
   * year, and RePORTER's publications endpoint returns every appl_id ever linked to a matching
   * publication, including appl_ids from *other* fiscal years/ICs entirely (e.g. querying a FY2023
   * FIC appl_id surfaced a linked FY2025 NHGRI appl_id in the same response). Stamping {@code
   * fiscal_year=year} on such a row would tag it with the wrong funding year, so rows are kept
   * only when {@code applid} is actually a member of the batch just queried — cross-linked
   * appl_ids from other slices are captured correctly when their own (year, agency_ic) combo is
   * walked instead.
   */
  private List<Map<String, Object>> fetchPublicationsForBatch(List<Long> applIdBatch, String year,
      String ic, RawCache rawCache, int batchIndex) throws IOException {
    java.util.Set<Long> batchSet = new java.util.HashSet<Long>(applIdBatch);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    StringBuilder idsJson = new StringBuilder();
    for (int i = 0; i < applIdBatch.size(); i++) {
      if (i > 0) {
        idsJson.append(',');
      }
      idsJson.append(applIdBatch.get(i));
    }
    int offset = 0;
    while (offset <= MAX_OFFSET) {
      String body = "{\"criteria\":{\"appl_ids\":[" + idsJson + "]},\"offset\":" + offset
          + ",\"limit\":" + PAGE_SIZE + "}";
      JsonNode root = postJson(PUBLICATIONS_ENDPOINT, body, rawCache,
          "pubs-batch" + batchIndex + "-offset-" + offset);
      JsonNode results = root.path("results");
      if (!results.isArray() || results.size() == 0) {
        break;
      }
      for (JsonNode r : results) {
        Long applId = longOrNull(r, "applid");
        if (applId != null && batchSet.contains(applId)) {
          rows.add(toRow(r, year));
        }
      }
      int total = root.path("meta").path("total").asInt(0);
      offset += PAGE_SIZE;
      if (offset >= total || results.size() < PAGE_SIZE) {
        break;
      }
    }
    return rows;
  }

  private Map<String, Object> toRow(JsonNode r, String year) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("appl_id", longOrNull(r, "applid"));
    row.put("fiscal_year", year);
    row.put("core_project_num", text(r, "coreproject"));
    row.put("pmid", longOrNull(r, "pmid"));
    return row;
  }

  /**
   * One page of results, read through the raw cache.
   *
   * <p>Keyed as endpoint + page label + body, matching {@link NihReporterAwardsProvider}'s
   * convention — the label disambiguates the projects-search appl_id-collection pages from the
   * publications-search pages sharing the same (year, ic) dimension combo.
   */
  private JsonNode postJson(String url, String jsonBody, RawCache rawCache, String pageLabel)
      throws IOException {
    String key = url + "/" + pageLabel + "?" + jsonBody;
    try (InputStream in = rawCache.openStream(key, () -> rawPost(url, jsonBody))) {
      return MAPPER.readTree(in);
    }
  }

  /** Issues the POST, failing on a non-2xx rather than returning the error body as content. */
  private InputStream rawPost(String url, String jsonBody) throws IOException {
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
    if (status < 200 || status >= 300) {
      StringBuilder err = new StringBuilder();
      InputStream es = conn.getErrorStream();
      if (es != null) {
        try (java.io.BufferedReader r = new java.io.BufferedReader(
            new java.io.InputStreamReader(es, StandardCharsets.UTF_8))) {
          String line;
          while ((line = r.readLine()) != null) {
            err.append(line);
          }
        }
      }
      throw new IOException("NIH RePORTER HTTP " + status + ": " + err);
    }
    return conn.getInputStream();
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
}
