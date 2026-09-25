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
package org.apache.calcite.adapter.govdata.law;

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.etl.RetryableHttp;
import org.apache.calcite.adapter.file.etl.StorageAwareDataProvider;
import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.GovDataException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * DataProvider for the lobbying tables, sourced from the Senate Office of Public Records' LDA.gov
 * REST API ({@code https://lda.gov/api/v1/}).
 *
 * <p>One class serves every lobbying table. Which endpoint a table reads and how its rows are cut
 * from a response is fixed by {@link #SPECS}; the URL, headers (the API key), query parameters and
 * rate limit come from the table's YAML {@code source:} block.
 *
 * <p>Three kinds of table:
 * <ul>
 *   <li><b>windowed</b> — filings and contribution reports, fetched one calendar quarter at a time
 *       by <em>posted</em> date. The pipeline's {@code backfill_period: quarterly} supplies
 *       {@code period_start}/{@code period_end}. A filing keeps the date it was posted, so a past
 *       window never changes; a later amendment is a new filing in a later window.</li>
 *   <li><b>paged</b> — the registrant, client and lobbyist registries: the whole list, one
 *       snapshot per {@code refresh_month}.</li>
 *   <li><b>constant</b> — small code lists, one unpaged request.</li>
 * </ul>
 *
 * <p>Streaming: pages are read one at a time by following the response's own {@code next} link,
 * and rows are handed to the pipeline lazily, so at most one page (25 records) is in memory.
 *
 * <p>Several tables read the same pages (a filing yields filings, activities, lobbyists, ...), so
 * every page is cached, gzipped, in the raw cache, keyed by endpoint, request parameters, window
 * and page number. A closed window's pages are permanent; an open window's are kept for one day.
 * The cache also makes a crawl resumable after a failure.
 *
 * <p>No silent failures: a request that still fails after the shared retry policy (which honours
 * {@code Retry-After}) throws, a response without the expected fields throws, and a crawl that
 * returns fewer records than the API's own {@code count}, or the same record twice, throws after
 * its cached pages are discarded.
 */
public class LdaApiProvider implements StorageAwareDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(LdaApiProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** LDA.gov posts and filters in Eastern time, so a window's "today" is Eastern too. */
  private static final ZoneId LDA_ZONE = ZoneId.of("America/New_York");

  static final String FILINGS = "lobbying_filings";
  static final String ACTIVITIES = "lobbying_activities";
  static final String ACTIVITY_LOBBYISTS = "lobbying_activity_lobbyists";
  static final String ACTIVITY_GOVERNMENT_ENTITIES = "lobbying_activity_government_entities";
  static final String FOREIGN_ENTITIES = "lobbying_foreign_entities";
  static final String AFFILIATED_ORGANIZATIONS = "lobbying_affiliated_organizations";
  static final String CONVICTION_DISCLOSURES = "lobbying_conviction_disclosures";
  static final String CONTRIBUTION_REPORTS = "lobbying_contribution_reports";
  static final String CONTRIBUTION_ITEMS = "lobbying_contribution_items";
  static final String CONTRIBUTION_PACS = "lobbying_contribution_pacs";
  static final String LOBBYISTS = "lobbyists";
  static final String REGISTRANTS = "lobbying_registrants";
  static final String CLIENTS = "lobbying_clients";
  static final String ISSUE_CODES = "lobbying_issue_codes";
  static final String GOVERNMENT_ENTITY_CODES = "lobbying_government_entity_codes";
  static final String FILING_TYPES = "lobbying_filing_types";
  static final String CONTRIBUTION_ITEM_TYPES = "lobbying_contribution_item_types";

  private enum Kind { WINDOWED, PAGED, CONSTANT }

  /** Cuts the rows of one table out of one record of a response. */
  private interface Extractor {
    void extract(JsonNode record, List<Map<String, Object>> out);
  }

  private static final class Spec {
    final String endpoint;
    final Kind kind;
    /** Field that uniquely identifies a record, used to detect a page served twice. */
    final String idField;
    final Extractor extractor;

    Spec(String endpoint, Kind kind, String idField, Extractor extractor) {
      this.endpoint = endpoint;
      this.kind = kind;
      this.idField = idField;
      this.extractor = extractor;
    }
  }

  private static final Map<String, Spec> SPECS = new HashMap<String, Spec>();

  static {
    SPECS.put(FILINGS, new Spec("filings", Kind.WINDOWED, "filing_uuid",
        LdaApiProvider::filings));
    SPECS.put(ACTIVITIES, new Spec("filings", Kind.WINDOWED, "filing_uuid",
        LdaApiProvider::activities));
    SPECS.put(ACTIVITY_LOBBYISTS, new Spec("filings", Kind.WINDOWED, "filing_uuid",
        LdaApiProvider::activityLobbyists));
    SPECS.put(ACTIVITY_GOVERNMENT_ENTITIES, new Spec("filings", Kind.WINDOWED, "filing_uuid",
        LdaApiProvider::activityGovernmentEntities));
    SPECS.put(FOREIGN_ENTITIES, new Spec("filings", Kind.WINDOWED, "filing_uuid",
        LdaApiProvider::foreignEntities));
    SPECS.put(AFFILIATED_ORGANIZATIONS, new Spec("filings", Kind.WINDOWED, "filing_uuid",
        LdaApiProvider::affiliatedOrganizations));
    SPECS.put(CONVICTION_DISCLOSURES, new Spec("filings", Kind.WINDOWED, "filing_uuid",
        LdaApiProvider::convictionDisclosures));
    SPECS.put(CONTRIBUTION_REPORTS, new Spec("contributions", Kind.WINDOWED, "filing_uuid",
        LdaApiProvider::contributionReports));
    SPECS.put(CONTRIBUTION_ITEMS, new Spec("contributions", Kind.WINDOWED, "filing_uuid",
        LdaApiProvider::contributionItems));
    SPECS.put(CONTRIBUTION_PACS, new Spec("contributions", Kind.WINDOWED, "filing_uuid",
        LdaApiProvider::contributionPacs));
    SPECS.put(LOBBYISTS, new Spec("lobbyists", Kind.PAGED, "id", LdaApiProvider::lobbyists));
    SPECS.put(REGISTRANTS, new Spec("registrants", Kind.PAGED, "id",
        LdaApiProvider::registrants));
    SPECS.put(CLIENTS, new Spec("clients", Kind.PAGED, "id", LdaApiProvider::clients));
    SPECS.put(ISSUE_CODES, new Spec("constants/issues", Kind.CONSTANT, null,
        (r, out) -> codeRow(out, "issue_code", text(r, "value"), "issue_name", text(r, "name"))));
    SPECS.put(GOVERNMENT_ENTITY_CODES, new Spec("constants/governmententities", Kind.CONSTANT,
        null, (r, out) -> {
          Map<String, Object> row = new LinkedHashMap<String, Object>();
          row.put("government_entity_id", integer(r, "id"));
          row.put("government_entity_name", text(r, "name"));
          out.add(row);
        }));
    SPECS.put(FILING_TYPES, new Spec("constants/filingtypes", Kind.CONSTANT, null,
        (r, out) -> codeRow(out, "filing_type", text(r, "value"), "filing_type_name",
            text(r, "name"))));
    SPECS.put(CONTRIBUTION_ITEM_TYPES, new Spec("constants/itemtypes", Kind.CONSTANT, null,
        (r, out) -> codeRow(out, "contribution_type", text(r, "value"),
            "contribution_type_name", text(r, "name"))));
  }

  private StorageProvider storageProvider;
  private String cacheBaseDir;

  @Override public void setStorageProvider(StorageProvider sp, String cacheDir) {
    this.storageProvider = sp;
    this.cacheBaseDir = cacheDir;
  }

  private StorageProvider storageProvider() {
    if (storageProvider == null) {
      storageProvider = StorageProviderFactory.createForGovDataCache();
      cacheBaseDir = StorageProviderFactory.getGovDataCacheDir();
    }
    return storageProvider;
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    HttpSourceConfig source = config.getSource();
    Map<String, String> params = new LinkedHashMap<String, String>();
    if (source.getParameters() != null) {
      for (Map.Entry<String, String> e : source.getParameters().entrySet()) {
        params.put(e.getKey(), VariableResolver.substitute(e.getValue(), variables));
      }
    }
    Map<String, String> headers = new LinkedHashMap<String, String>();
    headers.put("Accept", "application/json");
    if (source.getHeaders() != null) {
      for (Map.Entry<String, String> e : source.getHeaders().entrySet()) {
        headers.put(e.getKey(), VariableResolver.substitute(e.getValue(), variables));
      }
    }
    // dq_record_limit is this provider's own knob, not an API parameter.
    String limit = params.remove("dq_record_limit");
    int recordLimit = 0;
    if (limit != null && isDqSampleMode()) {
      try {
        recordLimit = Integer.parseInt(limit.trim());
      } catch (NumberFormatException e) {
        throw new GovDataException("LdaApiProvider: dq_record_limit must be an integer but is '"
            + limit + "'", e);
      }
    }
    StorageProvider sp = storageProvider();
    return open(config.getName(), VariableResolver.substitute(source.getUrl(), variables),
        params, headers, source.getRateLimit(), variables, sp, cacheBaseDir, recordLimit);
  }

  /** True when DQ sample mode is active; GOVDATA_DQ is an allowed global run flag. */
  private static boolean isDqSampleMode() {
    String v = System.getProperty("GOVDATA_DQ");
    if (v == null) {
      v = System.getenv("GOVDATA_DQ");
    }
    return "true".equalsIgnoreCase(v);
  }

  /** The tables this provider serves. */
  static Set<String> tables() {
    return SPECS.keySet();
  }

  /** The rows {@code table} takes from one record of a response; for tests. */
  static List<Map<String, Object>> rowsOf(String table, JsonNode record) {
    Spec spec = SPECS.get(table);
    if (spec == null) {
      throw new GovDataException("LdaApiProvider does not serve table '" + table + "'");
    }
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    spec.extractor.extract(record, out);
    return out;
  }

  /**
   * Starts a crawl for {@code table}. Package-private so tests can drive it against a stub server
   * without building a full pipeline config.
   */
  static Iterator<Map<String, Object>> open(String table, String baseUrl,
      Map<String, String> params, Map<String, String> headers,
      HttpSourceConfig.RateLimitConfig rateLimit, Map<String, String> variables,
      StorageProvider sp, String cacheBaseDir) throws IOException {
    return open(table, baseUrl, params, headers, rateLimit, variables, sp, cacheBaseDir, 0);
  }

  /**
   * As above; when {@code recordLimit} is positive only the first that many records of a paged
   * listing are read (DQ sampling). The limit is on records — whole filings or reports — so every
   * table read from the same window is cut from the same records and stays consistent with the
   * others, which a per-table row cap would not.
   */
  static Iterator<Map<String, Object>> open(String table, String baseUrl,
      Map<String, String> params, Map<String, String> headers,
      HttpSourceConfig.RateLimitConfig rateLimit, Map<String, String> variables,
      StorageProvider sp, String cacheBaseDir, int recordLimit) throws IOException {
    Spec spec = SPECS.get(table);
    if (spec == null) {
      throw new GovDataException("LdaApiProvider does not serve table '" + table + "'");
    }
    requireResolved("url", baseUrl);
    for (Map.Entry<String, String> e : params.entrySet()) {
      requireResolved("parameter " + e.getKey(), e.getValue());
    }
    for (Map.Entry<String, String> e : headers.entrySet()) {
      requireResolved("header " + e.getKey(), e.getValue());
    }
    if (spec.kind == Kind.CONSTANT) {
      return new ConstantIterator(spec, fetchNode(baseUrl, headers, rateLimit));
    }
    Map<String, String> query = new LinkedHashMap<String, String>(params);
    String windowKey;
    boolean closed;
    if (spec.kind == Kind.WINDOWED) {
      String start = required(variables, "period_start");
      String end = required(variables, "period_end");
      // The API's "before" filter is exclusive and "after" is inclusive (measured against the
      // posted timestamp in Eastern time), so a window that must include its last day ends the
      // day after; consecutive windows then tile the year with nothing lost at a quarter end.
      query.put("filing_dt_posted_after", start);
      query.put("filing_dt_posted_before", LocalDate.parse(end).plusDays(1).toString());
      LocalDate today = LocalDate.now(LDA_ZONE);
      closed = LocalDate.parse(end).isBefore(today);
      windowKey = start + "_" + end + (closed ? "" : "__open-" + today);
      if (!closed) {
        purgeStaleOpenWindows(sp, cacheBaseDir, spec, query, start + "_" + end, windowKey);
      }
    } else {
      windowKey = "refresh=" + required(variables, "refresh_month");
      closed = false;
    }
    String cacheDir = sp.resolvePath(cacheBaseDir,
        "lda/" + spec.endpoint + "/" + paramsKey(query) + "/" + windowKey);
    return new PageIterator(table, spec, buildUrl(baseUrl, query), headers, rateLimit, sp,
        cacheDir, closed, recordLimit);
  }

  /**
   * The variable resolver leaves an unset {@code ${VAR}} in place. LDA.gov ignores an
   * Authorization header it cannot use and serves the request anonymously at a much lower rate
   * limit, so a missing API key would not fail; it would quietly slow every crawl. Refuse it.
   */
  private static void requireResolved(String what, String value) {
    if (value != null && value.contains("${")) {
      throw new GovDataException("LdaApiProvider: " + what + " still contains an unresolved "
          + "variable (" + value + "); set the environment variable it names");
    }
  }

  private static String required(Map<String, String> variables, String name) {
    String v = variables.get(name);
    if (v == null || v.isEmpty()) {
      throw new GovDataException("LdaApiProvider: dimension/variable '" + name
          + "' is required but was not supplied (a windowed table needs backfill_period: "
          + "quarterly with year and quarter dimensions)");
    }
    return v;
  }

  /** A short stable key for the request parameters, so differently-parameterised crawls of one
   *  endpoint never share cached pages. */
  private static String paramsKey(Map<String, String> query) {
    Map<String, String> sorted = new java.util.TreeMap<String, String>(query);
    sorted.remove("filing_dt_posted_after");
    sorted.remove("filing_dt_posted_before");
    return Integer.toHexString(sorted.toString().hashCode());
  }

  private static String buildUrl(String baseUrl, Map<String, String> query) {
    if (query.isEmpty()) {
      return baseUrl;
    }
    StringBuilder sb = new StringBuilder(baseUrl).append(baseUrl.contains("?") ? '&' : '?');
    boolean first = true;
    for (Map.Entry<String, String> e : query.entrySet()) {
      if (!first) {
        sb.append('&');
      }
      first = false;
      sb.append(encode(e.getKey())).append('=').append(encode(e.getValue()));
    }
    return sb.toString();
  }

  private static String encode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("UTF-8 is always supported", e);
    }
  }

  /** An open window is cached per day; drop the pages cached on earlier days. */
  private static void purgeStaleOpenWindows(StorageProvider sp, String cacheBaseDir, Spec spec,
      Map<String, String> query, String window, String todayKey) throws IOException {
    String dir = sp.resolvePath(cacheBaseDir, "lda/" + spec.endpoint + "/" + paramsKey(query));
    List<StorageProvider.FileEntry> files;
    try {
      files = sp.listFiles(dir, true);
    } catch (IOException e) {
      if (sp.exists(dir)) {
        throw e;
      }
      return;   // nothing has been cached for this endpoint yet
    }
    List<String> stale = new ArrayList<String>();
    for (StorageProvider.FileEntry f : files) {
      String path = f.getPath();
      if (!f.isDirectory() && path.contains("/" + window + "__open-")
          && !path.contains("/" + todayKey + "/")) {
        stale.add(path);
      }
    }
    if (!stale.isEmpty()) {
      LOGGER.info("lda: dropping {} cached pages of earlier days for open window {}",
          stale.size(), window);
      sp.deleteBatch(stale);
    }
  }

  private static JsonNode fetchNode(String url, Map<String, String> headers,
      HttpSourceConfig.RateLimitConfig rateLimit) throws IOException {
    HttpURLConnection conn = RetryableHttp.openWithRetry(url, headers, rateLimit, true);
    try (InputStream in = conn.getInputStream()) {
      return MAPPER.readTree(in);
    } finally {
      conn.disconnect();
    }
  }

  /** Reads the rows of an unpaged code list. */
  private static final class ConstantIterator implements Iterator<Map<String, Object>> {
    private final Iterator<Map<String, Object>> rows;

    ConstantIterator(Spec spec, JsonNode root) {
      if (!root.isArray()) {
        throw new GovDataException("Expected a JSON array of codes but got " + root.getNodeType());
      }
      List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
      for (JsonNode r : root) {
        spec.extractor.extract(r, out);
      }
      this.rows = out.iterator();
    }

    @Override public boolean hasNext() {
      return rows.hasNext();
    }

    @Override public Map<String, Object> next() {
      return rows.next();
    }
  }

  /** Walks a paged listing lazily, one page at a time, checking it for completeness. */
  private static final class PageIterator implements Iterator<Map<String, Object>> {
    private final String table;
    private final Spec spec;
    private final String firstUrl;
    private final Map<String, String> headers;
    private final HttpSourceConfig.RateLimitConfig rateLimit;
    private final StorageProvider sp;
    private final String cacheDir;
    private final boolean closedWindow;
    private final int recordLimit;
    private final Deque<Map<String, Object>> pending = new ArrayDeque<Map<String, Object>>();
    private final Set<String> seen = new HashSet<String>();
    private final List<String> cachedPages = new ArrayList<String>();
    private int page;
    private String nextUrl;
    private boolean exhausted;
    private boolean verified;
    private long expected = -1;
    private long read;
    private int pageSize = 1;

    PageIterator(String table, Spec spec, String firstUrl, Map<String, String> headers,
        HttpSourceConfig.RateLimitConfig rateLimit, StorageProvider sp, String cacheDir,
        boolean closedWindow, int recordLimit) {
      this.table = table;
      this.spec = spec;
      this.firstUrl = firstUrl;
      this.headers = headers;
      this.rateLimit = rateLimit;
      this.sp = sp;
      this.cacheDir = cacheDir;
      this.closedWindow = closedWindow;
      this.recordLimit = recordLimit;
    }

    @Override public boolean hasNext() {
      try {
        while (pending.isEmpty()) {
          if (exhausted) {
            verify();
            return false;
          }
          loadNextPage();
        }
        return true;
      } catch (IOException e) {
        throw new GovDataException(table + ": failed reading " + spec.endpoint
            + " page " + page, e);
      }
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return pending.poll();
    }

    private void loadNextPage() throws IOException {
      page++;
      JsonNode root = readPage(page);
      JsonNode results = root.get("results");
      if (results == null || !results.isArray()) {
        throw new GovDataException(table + ": page " + page + " has no 'results' array");
      }
      if (page == 1) {
        JsonNode count = root.get("count");
        if (count == null || !count.isIntegralNumber()) {
          throw new GovDataException(table + ": first page has no integer 'count'");
        }
        expected = count.asLong();
        pageSize = Math.max(1, results.size());
      } else if (page > (expected + pageSize - 1) / pageSize + 100) {
        // Only records posted after the crawl began can add pages, and never this many.
        discardCache();
        throw new GovDataException(table + ": paging did not terminate by page " + page
            + " although the API reported " + expected + " records");
      }
      for (JsonNode record : results) {
        if (recordLimit > 0 && read >= recordLimit) {
          break;
        }
        JsonNode id = record.get(spec.idField);
        if (id == null || id.isNull()) {
          throw new GovDataException(table + ": a record on page " + page + " has no "
              + spec.idField);
        }
        if (!seen.add(id.asText())) {
          discardCache();
          throw new GovDataException(table + ": " + spec.idField + " " + id.asText()
              + " was returned twice; paging is not stable, cached pages discarded");
        }
        read++;
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        spec.extractor.extract(record, rows);
        pending.addAll(rows);
      }
      if (recordLimit > 0 && read >= recordLimit) {
        // A deliberate DQ sample: not the whole listing, so the completeness check does not apply.
        exhausted = true;
        verified = true;
        return;
      }
      JsonNode next = root.get("next");
      if (next == null || next.isNull()) {
        exhausted = true;
      } else {
        nextUrl = next.asText();
      }
    }

    private void verify() {
      if (verified) {
        return;
      }
      verified = true;
      // A closed window cannot change, so the crawl must return exactly what the API counted. An
      // open window or a registry can only grow while it is read (ordering is oldest first, so
      // new records land after everything already read), so it must return at least the count.
      boolean bad = closedWindow ? read != expected : read < expected;
      if (bad) {
        discardCache();
        throw new GovDataException(table + ": read " + read + " records but the API counted "
            + expected + (closedWindow ? " in a closed window" : "")
            + "; cached pages discarded so the next run refetches");
      }
      LOGGER.info("{}: read {} records in {} pages (API count {})", table, read, page, expected);
    }

    private void discardCache() {
      try {
        if (!cachedPages.isEmpty()) {
          sp.deleteBatch(cachedPages);
        }
      } catch (IOException e) {
        throw new GovDataException(table + ": could not discard cached pages", e);
      }
    }

    /** Page {@code n}, from the cache when present, otherwise from the API (and cached). */
    private JsonNode readPage(int n) throws IOException {
      String path = sp.resolvePath(cacheDir, String.format(Locale.ROOT, "page-%05d.json.gz", n));
      cachedPages.add(path);
      if (sp.exists(path)) {
        try (InputStream in = new GZIPInputStream(sp.openInputStream(path))) {
          return MAPPER.readTree(in);
        }
      }
      String url = n == 1 ? firstUrl : nextUrl;
      HttpURLConnection conn = RetryableHttp.openWithRetry(url, headers, rateLimit, true);
      byte[] body;
      try (InputStream in = conn.getInputStream()) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        byte[] chunk = new byte[16384];
        int len;
        while ((len = in.read(chunk)) != -1) {
          buf.write(chunk, 0, len);
        }
        body = buf.toByteArray();
      } finally {
        conn.disconnect();
      }
      JsonNode root = MAPPER.readTree(body);
      ByteArrayOutputStream gz = new ByteArrayOutputStream();
      try (GZIPOutputStream out = new GZIPOutputStream(gz)) {
        out.write(body);
      }
      sp.writeFile(path, gz.toByteArray());
      return root;
    }
  }

  // ---------------------------------------------------------------------------------------
  // JSON access. A JSON null or an empty string is an absent value; anything of the wrong type
  // is an error, never coerced.
  // ---------------------------------------------------------------------------------------

  private static String text(JsonNode n, String field) {
    JsonNode v = n.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    if (v.isContainerNode()) {
      throw new GovDataException("Expected a scalar for '" + field + "' but got "
          + v.getNodeType());
    }
    String s = v.asText().trim();
    return s.isEmpty() ? null : s;
  }

  private static String requiredText(JsonNode n, String field) {
    String s = text(n, field);
    if (s == null) {
      throw new GovDataException("Record has no '" + field + "'");
    }
    return s;
  }

  private static Integer integer(JsonNode n, String field) {
    JsonNode v = n.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    if (!v.isIntegralNumber() || !v.canConvertToInt()) {
      throw new GovDataException("Expected an integer for '" + field + "' but got " + v);
    }
    return Integer.valueOf(v.asInt());
  }

  private static Long longValue(JsonNode n, String field) {
    JsonNode v = n.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    if (!v.isIntegralNumber()) {
      throw new GovDataException("Expected an integer for '" + field + "' but got " + v);
    }
    return Long.valueOf(v.asLong());
  }

  /** Money and percentages arrive as strings such as "1000.00". */
  private static Double decimal(JsonNode n, String field) {
    String s = text(n, field);
    if (s == null) {
      return null;
    }
    try {
      return Double.valueOf(s);
    } catch (NumberFormatException e) {
      throw new GovDataException("Expected a number for '" + field + "' but got '" + s + "'", e);
    }
  }

  private static Boolean bool(JsonNode n, String field) {
    JsonNode v = n.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    if (!v.isBoolean()) {
      throw new GovDataException("Expected a boolean for '" + field + "' but got " + v);
    }
    return Boolean.valueOf(v.asBoolean());
  }

  private static JsonNode array(JsonNode n, String field) {
    JsonNode v = n.get(field);
    if (v == null || v.isNull()) {
      return MAPPER.createArrayNode();
    }
    if (!v.isArray()) {
      throw new GovDataException("Expected an array for '" + field + "' but got "
          + v.getNodeType());
    }
    return v;
  }

  private static JsonNode object(JsonNode n, String field) {
    JsonNode v = n.get(field);
    if (v == null || !v.isObject()) {
      throw new GovDataException("Record has no '" + field + "' object");
    }
    return v;
  }

  private static Map<String, Object> row(String uuid) {
    Map<String, Object> r = new LinkedHashMap<String, Object>();
    r.put("filing_uuid", uuid);
    return r;
  }

  private static void codeRow(List<Map<String, Object>> out, String codeKey, String code,
      String nameKey, String name) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put(codeKey, code);
    row.put(nameKey, name);
    out.add(row);
  }

  /** Adds the person columns of a lobbyist object under {@code prefix}. */
  private static void person(Map<String, Object> row, String prefix, JsonNode p) {
    row.put(prefix + "id", longValue(p, "id"));
    row.put(prefix + "prefix", text(p, "prefix_display"));
    row.put(prefix + "first_name", text(p, "first_name"));
    row.put(prefix + "nickname", text(p, "nickname"));
    row.put(prefix + "middle_name", text(p, "middle_name"));
    row.put(prefix + "last_name", text(p, "last_name"));
    row.put(prefix + "suffix", text(p, "suffix_display"));
  }

  // ---------------------------------------------------------------------------------------
  // Filings (LD-1 registrations and LD-2 quarterly reports)
  // ---------------------------------------------------------------------------------------

  private static void filings(JsonNode f, List<Map<String, Object>> out) {
    JsonNode registrant = object(f, "registrant");
    JsonNode client = object(f, "client");
    Map<String, Object> r = row(requiredText(f, "filing_uuid"));
    r.put("filing_type", text(f, "filing_type"));
    r.put("filing_type_name", text(f, "filing_type_display"));
    r.put("filing_year", integer(f, "filing_year"));
    r.put("filing_period", text(f, "filing_period"));
    r.put("filing_document_url", text(f, "filing_document_url"));
    r.put("income", decimal(f, "income"));
    r.put("expenses", decimal(f, "expenses"));
    r.put("expenses_method", text(f, "expenses_method"));
    r.put("expenses_method_name", text(f, "expenses_method_display"));
    r.put("dt_posted", text(f, "dt_posted"));
    r.put("termination_date", text(f, "termination_date"));
    r.put("registrant_id", longValue(registrant, "id"));
    r.put("registrant_name", text(registrant, "name"));
    r.put("registrant_house_id", integer(registrant, "house_registrant_id"));
    r.put("registrant_description", text(registrant, "description"));
    r.put("registrant_address_1", text(f, "registrant_address_1"));
    r.put("registrant_address_2", text(f, "registrant_address_2"));
    r.put("registrant_city", text(f, "registrant_city"));
    r.put("registrant_state", text(f, "registrant_state"));
    r.put("registrant_zip", text(f, "registrant_zip"));
    r.put("registrant_country", text(f, "registrant_country"));
    r.put("registrant_ppb_country", text(f, "registrant_ppb_country"));
    r.put("registrant_different_address", bool(f, "registrant_different_address"));
    r.put("client_id", longValue(client, "id"));
    r.put("client_name", text(client, "name"));
    r.put("client_general_description", text(client, "general_description"));
    r.put("client_state", text(client, "state"));
    r.put("client_country", text(client, "country"));
    r.put("client_ppb_state", text(client, "ppb_state"));
    r.put("client_ppb_country", text(client, "ppb_country"));
    r.put("client_government_entity", bool(client, "client_government_entity"));
    r.put("client_self_select", bool(client, "client_self_select"));
    r.put("client_effective_date", text(client, "effective_date"));
    r.put("activity_count", Integer.valueOf(array(f, "lobbying_activities").size()));
    r.put("foreign_entity_count", Integer.valueOf(array(f, "foreign_entities").size()));
    r.put("affiliated_organization_count",
        Integer.valueOf(array(f, "affiliated_organizations").size()));
    r.put("conviction_disclosure_count",
        Integer.valueOf(array(f, "conviction_disclosures").size()));
    out.add(r);
  }

  private static void activities(JsonNode f, List<Map<String, Object>> out) {
    String uuid = requiredText(f, "filing_uuid");
    int seq = 0;
    for (JsonNode a : array(f, "lobbying_activities")) {
      seq++;
      Map<String, Object> r = row(uuid);
      r.put("activity_seq", Integer.valueOf(seq));
      r.put("general_issue_code", text(a, "general_issue_code"));
      r.put("general_issue_name", text(a, "general_issue_code_display"));
      r.put("description", text(a, "description"));
      r.put("foreign_entity_issues", text(a, "foreign_entity_issues"));
      r.put("lobbyist_count", Integer.valueOf(array(a, "lobbyists").size()));
      r.put("government_entity_count", Integer.valueOf(array(a, "government_entities").size()));
      out.add(r);
    }
  }

  private static void activityLobbyists(JsonNode f, List<Map<String, Object>> out) {
    String uuid = requiredText(f, "filing_uuid");
    int seq = 0;
    for (JsonNode a : array(f, "lobbying_activities")) {
      seq++;
      for (JsonNode l : array(a, "lobbyists")) {
        Map<String, Object> r = row(uuid);
        r.put("activity_seq", Integer.valueOf(seq));
        person(r, "lobbyist_", object(l, "lobbyist"));
        r.put("covered_position", text(l, "covered_position"));
        r.put("is_new", bool(l, "new"));
        out.add(r);
      }
    }
  }

  private static void activityGovernmentEntities(JsonNode f, List<Map<String, Object>> out) {
    String uuid = requiredText(f, "filing_uuid");
    int seq = 0;
    for (JsonNode a : array(f, "lobbying_activities")) {
      seq++;
      for (JsonNode g : array(a, "government_entities")) {
        Map<String, Object> r = row(uuid);
        r.put("activity_seq", Integer.valueOf(seq));
        r.put("government_entity_id", integer(g, "id"));
        r.put("government_entity_name", text(g, "name"));
        out.add(r);
      }
    }
  }

  private static void foreignEntities(JsonNode f, List<Map<String, Object>> out) {
    String uuid = requiredText(f, "filing_uuid");
    int seq = 0;
    for (JsonNode e : array(f, "foreign_entities")) {
      seq++;
      Map<String, Object> r = row(uuid);
      r.put("entity_seq", Integer.valueOf(seq));
      r.put("name", text(e, "name"));
      r.put("contribution", decimal(e, "contribution"));
      r.put("ownership_percentage", decimal(e, "ownership_percentage"));
      r.put("address", text(e, "address"));
      r.put("city", text(e, "city"));
      r.put("state", text(e, "state"));
      r.put("country", text(e, "country"));
      r.put("country_name", text(e, "country_display"));
      r.put("ppb_city", text(e, "ppb_city"));
      r.put("ppb_state", text(e, "ppb_state"));
      r.put("ppb_country", text(e, "ppb_country"));
      out.add(r);
    }
  }

  private static void affiliatedOrganizations(JsonNode f, List<Map<String, Object>> out) {
    String uuid = requiredText(f, "filing_uuid");
    int seq = 0;
    for (JsonNode o : array(f, "affiliated_organizations")) {
      seq++;
      Map<String, Object> r = row(uuid);
      r.put("organization_seq", Integer.valueOf(seq));
      r.put("name", text(o, "name"));
      r.put("url", text(o, "url"));
      r.put("address_1", text(o, "address_1"));
      r.put("address_2", text(o, "address_2"));
      r.put("city", text(o, "city"));
      r.put("state", text(o, "state"));
      r.put("zip", text(o, "zip"));
      r.put("country", text(o, "country"));
      r.put("ppb_city", text(o, "ppb_city"));
      r.put("ppb_state", text(o, "ppb_state"));
      r.put("ppb_country", text(o, "ppb_country"));
      out.add(r);
    }
  }

  private static void convictionDisclosures(JsonNode f, List<Map<String, Object>> out) {
    String uuid = requiredText(f, "filing_uuid");
    int seq = 0;
    for (JsonNode c : array(f, "conviction_disclosures")) {
      seq++;
      Map<String, Object> r = row(uuid);
      r.put("disclosure_seq", Integer.valueOf(seq));
      person(r, "lobbyist_", object(c, "lobbyist"));
      r.put("conviction_date", text(c, "date"));
      r.put("description", text(c, "description"));
      out.add(r);
    }
  }

  // ---------------------------------------------------------------------------------------
  // Contribution reports (LD-203)
  // ---------------------------------------------------------------------------------------

  private static void contributionReports(JsonNode c, List<Map<String, Object>> out) {
    JsonNode registrant = object(c, "registrant");
    Map<String, Object> r = row(requiredText(c, "filing_uuid"));
    r.put("filing_type", text(c, "filing_type"));
    r.put("filing_type_name", text(c, "filing_type_display"));
    r.put("filing_year", integer(c, "filing_year"));
    r.put("filing_period", text(c, "filing_period"));
    r.put("filing_document_url", text(c, "filing_document_url"));
    r.put("filer_type", text(c, "filer_type"));
    r.put("dt_posted", text(c, "dt_posted"));
    r.put("comments", text(c, "comments"));
    // The report's own address is the filer's; for an individual lobbyist that is a home
    // address, so only the state and country are kept.
    r.put("filer_state", text(c, "state"));
    r.put("filer_country", text(c, "country"));
    r.put("registrant_id", longValue(registrant, "id"));
    r.put("registrant_name", text(registrant, "name"));
    r.put("registrant_house_id", integer(registrant, "house_registrant_id"));
    r.put("registrant_description", text(registrant, "description"));
    JsonNode lobbyist = c.get("lobbyist");
    if (lobbyist != null && lobbyist.isObject()) {
      person(r, "lobbyist_", lobbyist);
    } else {
      person(r, "lobbyist_", MAPPER.createObjectNode());
    }
    r.put("no_contributions", bool(c, "no_contributions"));
    r.put("item_count", Integer.valueOf(array(c, "contribution_items").size()));
    r.put("pac_count", Integer.valueOf(array(c, "pacs").size()));
    out.add(r);
  }

  private static void contributionItems(JsonNode c, List<Map<String, Object>> out) {
    String uuid = requiredText(c, "filing_uuid");
    int seq = 0;
    for (JsonNode i : array(c, "contribution_items")) {
      seq++;
      Map<String, Object> r = row(uuid);
      r.put("item_seq", Integer.valueOf(seq));
      r.put("contribution_type", text(i, "contribution_type"));
      r.put("contribution_type_name", text(i, "contribution_type_display"));
      r.put("contributor_name", text(i, "contributor_name"));
      r.put("payee_name", text(i, "payee_name"));
      r.put("honoree_name", text(i, "honoree_name"));
      r.put("amount", decimal(i, "amount"));
      r.put("contribution_date", text(i, "date"));
      out.add(r);
    }
  }

  private static void contributionPacs(JsonNode c, List<Map<String, Object>> out) {
    String uuid = requiredText(c, "filing_uuid");
    int seq = 0;
    for (JsonNode p : array(c, "pacs")) {
      if (!p.isTextual()) {
        throw new GovDataException("Expected a PAC name string but got " + p.getNodeType());
      }
      seq++;
      Map<String, Object> r = row(uuid);
      r.put("pac_seq", Integer.valueOf(seq));
      r.put("pac_name", p.asText().trim());
      out.add(r);
    }
  }

  // ---------------------------------------------------------------------------------------
  // Registries. Contact names and telephone numbers are deliberately not carried.
  // ---------------------------------------------------------------------------------------

  private static void lobbyists(JsonNode l, List<Map<String, Object>> out) {
    JsonNode registrant = object(l, "registrant");
    Map<String, Object> r = new LinkedHashMap<String, Object>();
    r.put("lobbyist_id", longValue(l, "id"));
    r.put("prefix", text(l, "prefix_display"));
    r.put("first_name", text(l, "first_name"));
    r.put("nickname", text(l, "nickname"));
    r.put("middle_name", text(l, "middle_name"));
    r.put("last_name", text(l, "last_name"));
    r.put("suffix", text(l, "suffix_display"));
    r.put("registrant_id", longValue(registrant, "id"));
    r.put("registrant_name", text(registrant, "name"));
    r.put("registrant_house_id", integer(registrant, "house_registrant_id"));
    r.put("registrant_city", text(registrant, "city"));
    r.put("registrant_state", text(registrant, "state"));
    r.put("registrant_country", text(registrant, "country"));
    out.add(r);
  }

  private static void registrants(JsonNode g, List<Map<String, Object>> out) {
    Map<String, Object> r = new LinkedHashMap<String, Object>();
    r.put("registrant_id", longValue(g, "id"));
    r.put("house_registrant_id", integer(g, "house_registrant_id"));
    r.put("name", text(g, "name"));
    r.put("description", text(g, "description"));
    r.put("address_1", text(g, "address_1"));
    r.put("address_2", text(g, "address_2"));
    r.put("address_3", text(g, "address_3"));
    r.put("address_4", text(g, "address_4"));
    r.put("city", text(g, "city"));
    r.put("state", text(g, "state"));
    r.put("zip", text(g, "zip"));
    r.put("country", text(g, "country"));
    r.put("ppb_country", text(g, "ppb_country"));
    r.put("dt_updated", text(g, "dt_updated"));
    out.add(r);
  }

  private static void clients(JsonNode c, List<Map<String, Object>> out) {
    JsonNode registrant = object(c, "registrant");
    Map<String, Object> r = new LinkedHashMap<String, Object>();
    r.put("client_id", longValue(c, "id"));
    r.put("name", text(c, "name"));
    r.put("general_description", text(c, "general_description"));
    r.put("government_entity", bool(c, "client_government_entity"));
    r.put("self_select", bool(c, "client_self_select"));
    r.put("state", text(c, "state"));
    r.put("country", text(c, "country"));
    r.put("ppb_state", text(c, "ppb_state"));
    r.put("ppb_country", text(c, "ppb_country"));
    r.put("effective_date", text(c, "effective_date"));
    r.put("registrant_id", longValue(registrant, "id"));
    r.put("registrant_name", text(registrant, "name"));
    out.add(r);
  }
}
