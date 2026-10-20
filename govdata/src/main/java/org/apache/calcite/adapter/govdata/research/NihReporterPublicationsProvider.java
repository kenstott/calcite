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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * <p>The publications response carries no publication year — only {@code (coreproject, pmid,
 * applid)} triples plus {@code meta.total} for pagination. {@code fiscal_year} here is the
 * *funding* fiscal year of the (year, agency_ic) slice being walked, not when the paper appeared;
 * the real publication year comes from NIH iCite ({@code /api/pubs?pmids=...&fl=pmid,year}) and is
 * emitted as {@code pub_year}. A pmid iCite does not index has a null {@code pub_year}.
 */
public class NihReporterPublicationsProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(NihReporterPublicationsProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String PROJECTS_ENDPOINT = "https://api.reporter.nih.gov/v2/projects/search";
  private static final String PUBLICATIONS_ENDPOINT = "https://api.reporter.nih.gov/v2/publications/search";
  /** Source keys in the schema YAML: invalid appl_ids tolerated per slice before the run is
   *  treated as an outage, and retries of a failing single-appl_id request before it is declared
   *  invalid. Read from the pipeline's own source block: ModelOperand is not captured until after
   *  the ETL phase, so it reads back empty while this provider runs. */
  static final String MAX_INVALID_KEY = "maxInvalidRecordsPerSlice";
  static final String MAX_RETRIES_KEY = "maxRetries";
  static final String STATUS_VALID = "valid";
  static final String STATUS_INVALID = "invalid_source_record";
  // The publications endpoint rejects offset + limit above 10000.
  private static final int MAX_REACHABLE_ROWS = 10000;
  private static final String ICITE_ENDPOINT = "https://icite.od.nih.gov/api/pubs";
  private static final int PAGE_SIZE = 500;
  // iCite is a GET with pmids in the query string; 400 ids stays well under its URL-size limit
  // (1000 ids returns HTTP 413).
  private static final int ICITE_BATCH = 400;
  // Projects endpoint: offset + limit must not exceed 14999.
  private static final int MAX_OFFSET = 14999;
  // Publications endpoint: offset + limit must not exceed 10000 (stricter than projects).
  // Confirmed live via curl: offset=9500 succeeds, offset=10000 returns HTTP 400.
  private static final int MAX_PUBLICATIONS_OFFSET = 9500;

  private static final long RETRY_BACKOFF_MS = 2000L;

  private final String publicationsEndpoint;
  private final long retryBackoffMs;

  /** Production constructor: the live RePORTER publications endpoint. */
  public NihReporterPublicationsProvider() {
    this(PUBLICATIONS_ENDPOINT, RETRY_BACKOFF_MS);
  }

  /** Test seam: points the publications calls at another endpoint, with its own backoff. */
  NihReporterPublicationsProvider(String publicationsEndpoint, long retryBackoffMs) {
    this.publicationsEndpoint = publicationsEndpoint;
    this.retryBackoffMs = retryBackoffMs;
  }

  /** HTTP failure from the RePORTER API, carrying the status code. */
  static final class NihHttpException extends IOException {
    final int status;

    NihHttpException(int status, String body) {
      super("NIH RePORTER HTTP " + status + ": " + body);
      this.status = status;
    }
  }

  /** A request's result set is larger than the endpoint's offset cap can page through. */
  static final class ResultWindowExceededException extends IOException {
    ResultWindowExceededException(int total) {
      super("publications/search meta.total " + total + " exceeds the " + MAX_REACHABLE_ROWS
          + "-row offset window");
    }
  }

  /** The two required source settings, read once per fetch. */
  static final class Limits {
    final int maxRetries;
    final int maxInvalid;

    Limits(int maxRetries, int maxInvalid) {
      this.maxRetries = maxRetries;
      this.maxInvalid = maxInvalid;
    }

    static Limits from(EtlPipelineConfig config) {
      Map<String, Object> source = config.getRawSourceConfig();
      return new Limits(require(source, MAX_RETRIES_KEY), require(source, MAX_INVALID_KEY));
    }

    private static int require(Map<String, Object> source, String key) {
      Object v = source == null ? null : source.get(key);
      if (!(v instanceof Number)) {
        throw new IllegalStateException(
            "nih_publications: source." + key + " is required in the schema YAML, got " + v);
      }
      return ((Number) v).intValue();
    }
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String year = variables.get("year");
    String ic = variables.get("agency_ic");
    if (year == null || year.isEmpty() || ic == null || ic.isEmpty()) {
      LOGGER.warn("nih_publications: missing year/agency_ic in dimension variables {}", variables);
      return java.util.Collections.emptyIterator();
    }

    Limits limits = Limits.from(config);
    List<Long> applIds = fetchApplIds(year, ic, rawCache);
    // Offset paging over an unsorted result set can return the same link on more than one page,
    // so (appl_id, pmid) is deduplicated to keep it unique within the slice.
    java.util.Map<String, Map<String, Object>> byLink =
        new LinkedHashMap<String, Map<String, Object>>();
    Map<Long, String> invalid = new LinkedHashMap<Long, String>();
    for (int i = 0; i < applIds.size(); i += PAGE_SIZE) {
      List<Long> batch = applIds.subList(i, Math.min(i + PAGE_SIZE, applIds.size()));
      for (Map<String, Object> row : fetchPublicationsForBatch(batch, year, rawCache,
          i / PAGE_SIZE, invalid, limits)) {
        byLink.putIfAbsent(row.get("appl_id") + ":" + row.get("pmid"), row);
      }
    }
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>(byLink.values());
    attachPublicationYears(rows, rawCache, limits);
    LOGGER.info("nih_publications: {} publication links for fy={} ic={} ({} appl_ids, "
        + "{} invalid_source_record)", rows.size(), year, ic, applIds.size(), invalid.size());
    return rows.iterator();
  }

  /** Sets {@code pub_year} on every row from iCite, one batched lookup per {@link #ICITE_BATCH} pmids. */
  private void attachPublicationYears(List<Map<String, Object>> rows, RawCache rawCache,
      Limits limits) throws IOException {
    Set<Long> pmidSet = new LinkedHashSet<Long>();
    for (Map<String, Object> row : rows) {
      Long pmid = (Long) row.get("pmid");
      if (pmid != null) {
        pmidSet.add(pmid);
      }
    }
    List<Long> pmids = new ArrayList<Long>(pmidSet);
    Map<Long, Integer> years = new HashMap<Long, Integer>();
    for (int i = 0; i < pmids.size(); i += ICITE_BATCH) {
      List<Long> batch = pmids.subList(i, Math.min(i + ICITE_BATCH, pmids.size()));
      StringBuilder ids = new StringBuilder();
      for (Long pmid : batch) {
        if (ids.length() > 0) {
          ids.append(',');
        }
        ids.append(pmid);
      }
      String url = ICITE_ENDPOINT + "?fl=pmid,year&pmids=" + ids;
      try (InputStream in = rawCache.openStream(url, () -> rawGetWithRetry(url, limits.maxRetries))) {
        for (JsonNode d : MAPPER.readTree(in).path("data")) {
          Long pmid = longOrNull(d, "pmid");
          JsonNode y = d.get("year");
          if (pmid != null && y != null && !y.isNull()) {
            years.put(pmid, y.asInt());
          }
        }
      }
    }
    for (Map<String, Object> row : rows) {
      row.put("pub_year", years.get((Long) row.get("pmid")));
    }
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
  List<Map<String, Object>> fetchPublicationsForBatch(List<Long> applIdBatch, String year,
      RawCache rawCache, int batchIndex, Map<Long, String> invalid, Limits limits)
      throws IOException {
    return fetchPublicationsForBatchPart(applIdBatch, year, rawCache,
        String.valueOf(batchIndex), invalid, limits);
  }

  /**
   * Fetches one id list. A multi-id list that fails with HTTP 5xx, or whose result set exceeds the
   * endpoint's offset window, is split in half and each half recursed. A single id that still
   * returns 5xx after the configured retries is an invalid source record: one marker row is
   * emitted and the id is recorded in {@code invalid}. A single id whose result set exceeds the
   * window throws.
   */
  private List<Map<String, Object>> fetchPublicationsForBatchPart(List<Long> ids, String year,
      RawCache rawCache, String label, Map<Long, String> invalid, Limits limits)
      throws IOException {
    if (ids.size() == 1) {
      return fetchSingleId(ids.get(0), year, rawCache, label, invalid, limits);
    }
    try {
      return fetchPublicationsPages(ids, year, rawCache, "batch" + label);
    } catch (IOException e) {
      if (!(e instanceof ResultWindowExceededException) && !isHttp5xx(e)) {
        throw e;
      }
      int mid = ids.size() / 2;
      LOGGER.warn("nih_publications: {} for batch{} of {} appl_ids (fy={}); splitting into {} + {}",
          e.getMessage(), label, ids.size(), year, mid, ids.size() - mid);
      List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
      rows.addAll(fetchPublicationsForBatchPart(ids.subList(0, mid), year, rawCache,
          label + "a", invalid, limits));
      rows.addAll(fetchPublicationsForBatchPart(ids.subList(mid, ids.size()), year, rawCache,
          label + "b", invalid, limits));
      return rows;
    }
  }

  private List<Map<String, Object>> fetchSingleId(Long applId, String year, RawCache rawCache,
      String label, Map<Long, String> invalid, Limits limits) throws IOException {
    int maxRetries = limits.maxRetries;
    List<Long> single = java.util.Collections.singletonList(applId);
    for (int attempt = 1;; attempt++) {
      try {
        return fetchPublicationsPages(single, year, rawCache, "batch" + label);
      } catch (ResultWindowExceededException e) {
        // One grant does not have >10000 publications: for these ids the endpoint ignores the
        // appl_id filter and answers with an unrelated set of projects' papers (observed for
        // intramural appl_ids, e.g. 11333657 -> ZIDBC011317), so the response says nothing
        // about this appl_id. Retrying returns the same thing; it is an invalid source record.
        String issue = "NIH publications API ignored the appl_id filter: " + e.getMessage()
            + " for this appl_id alone";
        return invalidMarker(applId, year, issue, invalid, limits, e);
      } catch (IOException e) {
        if (!isHttp5xx(e)) {
          throw e;
        }
        if (attempt <= maxRetries) {
          LOGGER.warn("nih_publications: appl_id {} alone failed attempt {} of {} ({}); retrying",
              applId, attempt, maxRetries + 1, e.getMessage());
          sleepBackoff(attempt);
          continue;
        }
        String issue = "NIH publications API returned HTTP " + httpStatus(e)
            + " for this appl_id alone after " + attempt + " attempts";
        return invalidMarker(applId, year, issue, invalid, limits, e);
      }
    }
  }

  /** Records {@code applId} as invalid and returns its single marker row; fails past the cap. */
  private List<Map<String, Object>> invalidMarker(Long applId, String year, String issue,
      Map<Long, String> invalid, Limits limits, IOException cause) throws IOException {
    LOGGER.warn("nih_publications: appl_id {} fy={} is INVALID: {}", applId, year, issue);
    invalid.put(applId, issue);
    int max = limits.maxInvalid;
    if (invalid.size() > max) {
      List<Long> first = new ArrayList<Long>(invalid.keySet());
      throw new IOException("nih_publications: " + invalid.size() + " invalid appl_ids in one "
          + "slice exceeds maxInvalidRecordsPerSlice=" + max + " (first: "
          + first.subList(0, Math.min(5, first.size())) + "); treating as an API outage, "
          + "not committing invalid markers", cause);
    }
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = toRow(applId, null, null, year);
    row.put("record_status", STATUS_INVALID);
    row.put("record_issue", issue);
    rows.add(row);
    return rows;
  }

  private void sleepBackoff(int attempt) throws IOException {
    try {
      Thread.sleep(retryBackoffMs * attempt);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while backing off a NIH retry", ie);
    }
  }

  private static boolean isHttp5xx(Throwable e) {
    return httpStatus(e) >= 500;
  }

  private static int httpStatus(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof NihHttpException) {
        return ((NihHttpException) t).status;
      }
    }
    return -1;
  }

  private List<Map<String, Object>> fetchPublicationsPages(List<Long> applIdBatch, String year,
      RawCache rawCache, String batchLabel) throws IOException {
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
    while (offset <= MAX_PUBLICATIONS_OFFSET) {
      String body = "{\"criteria\":{\"appl_ids\":[" + idsJson + "]},\"offset\":" + offset
          + ",\"limit\":" + PAGE_SIZE + "}";
      JsonNode root = postJson(publicationsEndpoint, body, rawCache,
          "pubs-" + batchLabel + "-offset-" + offset);
      JsonNode results = root.path("results");
      int total = root.path("meta").path("total").asInt(0);
      if (total > MAX_REACHABLE_ROWS) {
        throw new ResultWindowExceededException(total);
      }
      if (!results.isArray() || results.size() == 0) {
        break;
      }
      for (JsonNode r : results) {
        Long applId = longOrNull(r, "applid");
        if (applId != null && batchSet.contains(applId)) {
          Map<String, Object> row = toRow(applId, text(r, "coreproject"), longOrNull(r, "pmid"),
              year);
          row.put("record_status", STATUS_VALID);
          row.put("record_issue", null);
          rows.add(row);
        }
      }
      offset += PAGE_SIZE;
      if (offset >= total || results.size() < PAGE_SIZE) {
        break;
      }
    }
    return rows;
  }

  private Map<String, Object> toRow(Long applId, String coreProjectNum, Long pmid, String year) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("appl_id", applId);
    row.put("fiscal_year", year);
    row.put("core_project_num", coreProjectNum);
    row.put("pmid", pmid);
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
      throw new NihHttpException(status, err.toString());
    }
    return conn.getInputStream();
  }

  /** An iCite non-2xx; {@code status} decides whether the GET is worth repeating. */
  private static final class IciteHttpException extends IOException {
    final int status;

    IciteHttpException(int status, String url) {
      super("NIH iCite HTTP " + status + " for " + url);
      this.status = status;
    }
  }

  /**
   * {@link #rawGet}, repeated up to {@code maxRetries} times when iCite answers 429 or 5xx or the
   * connection fails; any other status is a real answer and fails at once.
   */
  private InputStream rawGetWithRetry(String url, int maxRetries) throws IOException {
    for (int attempt = 1; ; attempt++) {
      try {
        return rawGet(url);
      } catch (IciteHttpException e) {
        if (attempt > maxRetries || !(e.status == 429 || e.status >= 500)) {
          throw e;
        }
        LOGGER.warn("nih_publications: iCite HTTP {} (attempt {}/{}), retrying", e.status, attempt,
            maxRetries + 1);
      } catch (java.net.SocketException | java.net.SocketTimeoutException e) {
        if (attempt > maxRetries) {
          throw e;
        }
        LOGGER.warn("nih_publications: iCite {} (attempt {}/{}), retrying", e, attempt,
            maxRetries + 1);
      }
      sleepBackoff(attempt);
    }
  }

  /** Issues the GET, failing on a non-2xx rather than returning the error body as content. */
  private InputStream rawGet(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);
    int status = conn.getResponseCode();
    if (status < 200 || status >= 300) {
      throw new IciteHttpException(status, url);
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
