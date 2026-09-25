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

import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.etl.RetryableHttp;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.govdata.GovDataException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link LdaApiProvider} against a local HTTP server that serves real LDA.gov records
 * (trimmed from responses fetched on 2026-09-25, with contact names, telephone numbers, home
 * addresses and the name of whoever posted a filing removed) as a paged API.
 *
 * <p>The server pages the way LDA.gov does: an absolute {@code next} link, a {@code count}, and
 * {@code results}. Every request it receives is recorded so the tests can check the query, the
 * window boundaries and the API-key header the provider sent.
 */
@Tag("unit")
class LdaApiProviderTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpSourceConfig.RateLimitConfig FAST = rateLimit();

  @TempDir File cacheDir;

  private HttpServer server;
  private String base;
  private final List<String> requests = new CopyOnWriteArrayList<String>();
  private final List<String> authHeaders = new CopyOnWriteArrayList<String>();
  private final Map<String, Page> routes = new HashMap<String, Page>();

  /** Produces the JSON body for one page of an endpoint. */
  private interface Page {
    String body(int page);
  }

  private static HttpSourceConfig.RateLimitConfig rateLimit() {
    Map<String, Object> m = new HashMap<String, Object>();
    m.put("requestsPerSecond", Integer.valueOf(1000));
    m.put("maxRetries", Integer.valueOf(1));
    return HttpSourceConfig.RateLimitConfig.fromMap(m);
  }

  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", exchange -> {
      String path = exchange.getRequestURI().getPath();
      String query = exchange.getRequestURI().getRawQuery();
      requests.add(path + (query == null ? "" : "?" + query));
      authHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));
      Page route = routes.get(path);
      byte[] body;
      int status = 200;
      if (route == null) {
        body = "{\"detail\":\"not found\"}".getBytes(StandardCharsets.UTF_8);
        status = 404;
      } else {
        int page = 1;
        if (query != null) {
          for (String kv : query.split("&")) {
            if (kv.startsWith("page=")) {
              page = Integer.parseInt(kv.substring(5));
            }
          }
        }
        body = route.body(page).getBytes(StandardCharsets.UTF_8);
      }
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(status, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    });
    server.start();
    base = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach void stopServer() {
    server.stop(0);
  }

  // ---- fixtures and the paged stub -------------------------------------------------------

  private static ArrayNode fixtureResults(String name) throws IOException {
    try (InputStream in = LdaApiProviderTest.class.getResourceAsStream("/law/lda/" + name)) {
      assertTrue(in != null, "missing fixture " + name);
      JsonNode root = MAPPER.readTree(in);
      return root.isArray() ? (ArrayNode) root : (ArrayNode) root.get("results");
    }
  }

  /** Serves {@code records} as a paged listing of {@code pageSize} per page, reporting {@code
   *  count} as the API's own total. */
  private void route(String path, final List<JsonNode> records, final int pageSize,
      final long count) {
    routes.put(path, page -> {
      ObjectNode root = MAPPER.createObjectNode();
      root.put("count", count);
      int from = (page - 1) * pageSize;
      int to = Math.min(records.size(), from + pageSize);
      boolean more = to < records.size();
      if (more) {
        root.put("next", base + path + "?page=" + (page + 1));
      } else {
        root.putNull("next");
      }
      root.putNull("previous");
      ArrayNode results = root.putArray("results");
      for (int i = from; i < to; i++) {
        results.add(records.get(i));
      }
      return root.toString();
    });
  }

  private void routeFixture(String path, String fixture, int pageSize) throws IOException {
    List<JsonNode> records = new ArrayList<JsonNode>();
    for (JsonNode n : fixtureResults(fixture)) {
      records.add(n);
    }
    route(path, records, pageSize, records.size());
  }

  private Iterator<Map<String, Object>> crawl(String table, String path,
      Map<String, String> variables) throws IOException {
    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("page_size", "25");
    params.put("ordering", "dt_posted");
    Map<String, String> headers = new LinkedHashMap<String, String>();
    headers.put("Authorization", "Token test-key");
    return LdaApiProvider.open(table, base + path, params, headers, FAST, variables,
        new LocalFileStorageProvider(), cacheDir.getAbsolutePath());
  }

  private static Map<String, String> window(String start, String end) {
    Map<String, String> v = new HashMap<String, String>();
    v.put("period_start", start);
    v.put("period_end", end);
    return v;
  }

  private static List<Map<String, Object>> drain(Iterator<Map<String, Object>> it) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    while (it.hasNext()) {
      rows.add(it.next());
    }
    return rows;
  }

  private List<Map<String, Object>> rows(String table, String path, Map<String, String> vars)
      throws IOException {
    return drain(crawl(table, path, vars));
  }

  private static final Map<String, String> Q2_2025 = window("2025-04-01", "2025-06-30");

  // ---- filings ----------------------------------------------------------------------------

  @Test void filingRowsCarryRegistrantClientAndCounts() throws Exception {
    routeFixture("/api/v1/filings/", "filings_page.json", 2);
    List<Map<String, Object>> rows = rows(LdaApiProvider.FILINGS, "/api/v1/filings/", Q2_2025);
    assertEquals(4, rows.size());

    Map<String, Object> ld2 = rows.get(0);
    assertTrue(((String) ld2.get("filing_uuid")).startsWith("8149a0b7"));
    assertEquals("Q2", ld2.get("filing_type"));
    assertEquals(2025, ld2.get("filing_year"));
    assertEquals("second_quarter", ld2.get("filing_period"));
    assertEquals(5500.0, ld2.get("income"));
    assertNull(ld2.get("expenses"));
    assertEquals(401108124L, ld2.get("registrant_id"));
    assertEquals(64967L, ld2.get("client_id"));
    assertEquals("MYHY", ld2.get("client_name"));
    assertEquals(1, ld2.get("activity_count"));
    assertEquals(0, ld2.get("foreign_entity_count"));
    assertTrue(((String) ld2.get("dt_posted")).startsWith("2025-06-03T11:04:32"));

    assertEquals("RR", rows.get(1).get("filing_type"));
    assertEquals(2, rows.get(1).get("foreign_entity_count"));
    assertEquals(1, rows.get(2).get("affiliated_organization_count"));
    assertEquals(3, rows.get(2).get("activity_count"));
    assertEquals(1, rows.get(3).get("conviction_disclosure_count"));
  }

  @Test void personalContactFieldsAreNeverCarried() throws Exception {
    routeFixture("/api/v1/filings/", "filings_page.json", 2);
    Map<String, Object> row = rows(LdaApiProvider.FILINGS, "/api/v1/filings/", Q2_2025).get(0);
    for (String key : row.keySet()) {
      assertFalse(key.contains("contact") || key.contains("telephone") || key.contains("posted_by"),
          "unexpected personal column " + key);
    }
  }

  @Test void activitiesAreNumberedWithinTheirFiling() throws Exception {
    routeFixture("/api/v1/filings/", "filings_page.json", 2);
    List<Map<String, Object>> rows = rows(LdaApiProvider.ACTIVITIES, "/api/v1/filings/", Q2_2025);
    assertEquals(6, rows.size());
    Map<String, Object> first = rows.get(0);
    assertEquals(1, first.get("activity_seq"));
    assertEquals("DEF", first.get("general_issue_code"));
    assertEquals("Defense", first.get("general_issue_name"));
    assertTrue(((String) first.get("description")).startsWith("hydration supplements"));
    assertEquals(4, first.get("government_entity_count"));
    // The three-activity filing numbers them 1, 2, 3.
    assertEquals(1, rows.get(2).get("activity_seq"));
    assertEquals(3, rows.get(4).get("activity_seq"));
  }

  @Test void lobbyersAndTheirCoveredPositionsAreOneRowPerActivityAndPerson() throws Exception {
    routeFixture("/api/v1/filings/", "filings_page.json", 2);
    List<Map<String, Object>> rows =
        rows(LdaApiProvider.ACTIVITY_LOBBYISTS, "/api/v1/filings/", Q2_2025);
    assertEquals(6, rows.size());
    Map<String, Object> first = rows.get(0);
    assertEquals(149015L, first.get("lobbyist_id"));
    assertEquals("HOLLINGER", first.get("lobbyist_last_name"));
    assertEquals("Principal", first.get("covered_position"));
    assertEquals(Boolean.FALSE, first.get("is_new"));
  }

  @Test void governmentEntitiesForeignEntitiesAffiliatesAndConvictions() throws Exception {
    routeFixture("/api/v1/filings/", "filings_page.json", 2);
    List<Map<String, Object>> gov =
        rows(LdaApiProvider.ACTIVITY_GOVERNMENT_ENTITIES, "/api/v1/filings/", Q2_2025);
    assertEquals(4, gov.size());
    assertEquals("Army, Dept of (Other)", gov.get(0).get("government_entity_name"));

    List<Map<String, Object>> foreign =
        rows(LdaApiProvider.FOREIGN_ENTITIES, "/api/v1/filings/", Q2_2025);
    assertEquals(2, foreign.size());
    assertEquals("AIRBUS SE", foreign.get(0).get("name"));
    assertEquals(37.5, foreign.get(0).get("ownership_percentage"));
    assertEquals("NL", foreign.get(0).get("country"));
    assertEquals("Netherlands", foreign.get(0).get("country_name"));

    List<Map<String, Object>> affiliated =
        rows(LdaApiProvider.AFFILIATED_ORGANIZATIONS, "/api/v1/filings/", Q2_2025);
    assertEquals(1, affiliated.size());
    assertEquals("WASHINGTON HEALTH STRATEGIES GROUP", affiliated.get(0).get("name"));
    assertEquals("DC", affiliated.get(0).get("state"));

    List<Map<String, Object>> convictions =
        rows(LdaApiProvider.CONVICTION_DISCLOSURES, "/api/v1/filings/", Q2_2025);
    assertEquals(1, convictions.size());
    assertEquals("BURKMAN", convictions.get(0).get("lobbyist_last_name"));
    assertEquals("2022-11-30", convictions.get(0).get("conviction_date"));
  }

  // ---- request shape ------------------------------------------------------------------------

  @Test void windowIsHalfOpenSoTheLastDayOfAQuarterIsIncluded() throws Exception {
    routeFixture("/api/v1/filings/", "filings_page.json", 2);
    rows(LdaApiProvider.FILINGS, "/api/v1/filings/", Q2_2025);
    String first = requests.get(0);
    // The API's "before" is exclusive, so a window ending 2025-06-30 asks for before 2025-07-01.
    assertTrue(first.contains("filing_dt_posted_after=2025-04-01"), first);
    assertTrue(first.contains("filing_dt_posted_before=2025-07-01"), first);
    assertTrue(first.contains("ordering=dt_posted"), first);
    assertTrue(first.contains("page_size=25"), first);
  }

  @Test void theApiKeyHeaderIsSentOnEveryRequest() throws Exception {
    routeFixture("/api/v1/filings/", "filings_page.json", 2);
    rows(LdaApiProvider.FILINGS, "/api/v1/filings/", Q2_2025);
    assertEquals(2, authHeaders.size());
    for (String h : authHeaders) {
      assertEquals("Token test-key", h);
    }
  }

  @Test void everyPageIsReadByFollowingTheNextLink() throws Exception {
    routeFixture("/api/v1/filings/", "filings_page.json", 2);
    assertEquals(4, rows(LdaApiProvider.FILINGS, "/api/v1/filings/", Q2_2025).size());
    assertEquals(2, requests.size(), "4 filings at 2 per page is 2 requests");
    assertTrue(requests.get(1).contains("page=2"), requests.get(1));
  }

  @Test void aDqRecordLimitStopsAfterThatManyRecordsAndSkipsTheCountCheck() throws Exception {
    List<JsonNode> records = new ArrayList<JsonNode>();
    for (JsonNode n : fixtureResults("filings_page.json")) {
      records.add(n);
    }
    route("/api/v1/filings/", records, 2, 400);   // the API counts far more than is read
    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("page_size", "25");
    Iterator<Map<String, Object>> it = LdaApiProvider.open(LdaApiProvider.FILINGS,
        base + "/api/v1/filings/", params, new LinkedHashMap<String, String>(), FAST, Q2_2025,
        new LocalFileStorageProvider(), cacheDir.getAbsolutePath(), 3);
    List<Map<String, Object>> rows = drain(it);
    assertEquals(3, rows.size(), "three filings, cut mid-page");
    assertEquals(2, requests.size(), "the fourth filing's page is not needed beyond page 2");
  }

  @Test void aDqRecordLimitCutsEveryTableFromTheSameFilings() throws Exception {
    routeFixture("/api/v1/filings/", "filings_page.json", 2);
    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("page_size", "25");
    // Two filings: the LD-2 (1 activity) and the registration with foreign entities (1 activity).
    List<Map<String, Object>> activities = drain(LdaApiProvider.open(LdaApiProvider.ACTIVITIES,
        base + "/api/v1/filings/", params, new LinkedHashMap<String, String>(), FAST, Q2_2025,
        new LocalFileStorageProvider(), cacheDir.getAbsolutePath(), 2));
    List<Map<String, Object>> foreign = drain(LdaApiProvider.open(LdaApiProvider.FOREIGN_ENTITIES,
        base + "/api/v1/filings/", params, new LinkedHashMap<String, String>(), FAST, Q2_2025,
        new LocalFileStorageProvider(), cacheDir.getAbsolutePath(), 2));
    assertEquals(2, activities.size());
    assertEquals(2, foreign.size(), "the second filing's two foreign entities are in the sample");
    assertEquals(activities.get(1).get("filing_uuid"), foreign.get(0).get("filing_uuid"));
  }

  @Test void pagesAreReadLazilyNotAllUpFront() throws Exception {
    List<JsonNode> many = new ArrayList<JsonNode>();
    for (JsonNode n : fixtureResults("filings_page.json")) {
      many.add(n);
    }
    route("/api/v1/filings/", many, 1, many.size());
    Iterator<Map<String, Object>> it = crawl(LdaApiProvider.FILINGS, "/api/v1/filings/", Q2_2025);
    assertTrue(it.hasNext());
    it.next();
    assertEquals(1, requests.size(), "only the first of four pages should have been requested");
  }

  // ---- caching --------------------------------------------------------------------------------

  @Test void aSecondTableReadingTheSamePagesDoesNotCallTheApiAgain() throws Exception {
    routeFixture("/api/v1/filings/", "filings_page.json", 2);
    rows(LdaApiProvider.FILINGS, "/api/v1/filings/", Q2_2025);
    int afterFirst = requests.size();
    List<Map<String, Object>> activities =
        rows(LdaApiProvider.ACTIVITIES, "/api/v1/filings/", Q2_2025);
    assertEquals(6, activities.size());
    assertEquals(afterFirst, requests.size(), "the pages must come from the cache");
  }

  @Test void pagesCachedOnAnEarlierDayForAnOpenWindowAreDropped() throws Exception {
    routeFixture("/api/v1/filings/", "filings_page.json", 2);
    String today = LocalDate.now(ZoneId.of("America/New_York")).toString();
    Map<String, String> open = window(today, today);
    rows(LdaApiProvider.FILINGS, "/api/v1/filings/", open);
    File[] dirs = findWindowDirs(cacheDir, today + "_" + today + "__open-");
    assertEquals(1, dirs.length);
    // Pretend the same open window was cached yesterday.
    File stale = new File(dirs[0].getParentFile(), today + "_" + today + "__open-2020-01-01");
    assertTrue(stale.mkdirs());
    Files.write(new File(stale, "page-00001.json.gz").toPath(), new byte[] {1, 2, 3});
    rows(LdaApiProvider.FILINGS, "/api/v1/filings/", open);
    assertFalse(new File(stale, "page-00001.json.gz").exists(), "stale day's pages must be gone");
    assertEquals(1, findWindowDirs(cacheDir, today + "_" + today + "__open-" + today).length);
  }

  private static File[] findWindowDirs(File root, String prefix) {
    List<File> found = new ArrayList<File>();
    collect(root, prefix, found);
    return found.toArray(new File[0]);
  }

  private static void collect(File dir, String prefix, List<File> out) {
    File[] kids = dir.listFiles();
    if (kids == null) {
      return;
    }
    for (File k : kids) {
      if (k.isDirectory()) {
        if (k.getName().startsWith(prefix)) {
          out.add(k);
        }
        collect(k, prefix, out);
      }
    }
  }

  // ---- completeness checks -------------------------------------------------------------------

  @Test void aClosedWindowThatReturnsFewerRecordsThanCountedFails() throws Exception {
    List<JsonNode> records = new ArrayList<JsonNode>();
    for (JsonNode n : fixtureResults("filings_page.json")) {
      records.add(n);
    }
    route("/api/v1/filings/", records, 2, 5);   // the API claims 5 but serves 4
    Iterator<Map<String, Object>> it = crawl(LdaApiProvider.FILINGS, "/api/v1/filings/", Q2_2025);
    GovDataException e = assertThrows(GovDataException.class, () -> drain(it));
    assertTrue(e.getMessage().contains("counted 5"), e.getMessage());
    // The failed crawl's pages were discarded, so the next attempt refetches them.
    int before = requests.size();
    assertThrows(GovDataException.class,
        () -> drain(crawl(LdaApiProvider.FILINGS, "/api/v1/filings/", Q2_2025)));
    assertTrue(requests.size() > before, "cached pages of a failed crawl must not be reused");
  }

  @Test void anOpenWindowMayReturnMoreThanCountedBecauseNewFilingsArePosted() throws Exception {
    List<JsonNode> records = new ArrayList<JsonNode>();
    for (JsonNode n : fixtureResults("filings_page.json")) {
      records.add(n);
    }
    route("/api/v1/filings/", records, 2, 3);   // counted 3 when the crawl began; 4 by the end
    String today = LocalDate.now(ZoneId.of("America/New_York")).toString();
    assertEquals(4, rows(LdaApiProvider.FILINGS, "/api/v1/filings/", window(today, today)).size());
  }

  @Test void aRecordServedTwiceMeansPagingIsUnstableAndFails() throws Exception {
    List<JsonNode> records = new ArrayList<JsonNode>();
    JsonNode a = fixtureResults("filings_page.json").get(0);
    records.add(a);
    records.add(a);
    route("/api/v1/filings/", records, 1, 2);
    GovDataException e = assertThrows(GovDataException.class,
        () -> rows(LdaApiProvider.FILINGS, "/api/v1/filings/", Q2_2025));
    assertTrue(e.getMessage().contains("returned twice"), e.getMessage());
  }

  @Test void aResponseWithoutResultsFails() throws Exception {
    routes.put("/api/v1/filings/", page -> "{\"count\": 1, \"next\": null}");
    GovDataException e = assertThrows(GovDataException.class,
        () -> rows(LdaApiProvider.FILINGS, "/api/v1/filings/", Q2_2025));
    assertTrue(e.getMessage().contains("results"), e.getMessage());
  }

  @Test void aFieldOfTheWrongTypeFailsInsteadOfBeingCoerced() throws Exception {
    ObjectNode bad = (ObjectNode) fixtureResults("filings_page.json").get(0).deepCopy();
    bad.put("filing_year", "twenty twenty-five");
    route("/api/v1/filings/", Collections.<JsonNode>singletonList(bad), 25, 1);
    assertThrows(GovDataException.class,
        () -> rows(LdaApiProvider.FILINGS, "/api/v1/filings/", Q2_2025));
  }

  @Test void aMissingApiKeyFailsInsteadOfSilentlyRunningAnonymously() {
    Map<String, String> params = new LinkedHashMap<String, String>();
    Map<String, String> headers = new LinkedHashMap<String, String>();
    headers.put("Authorization", "Token ${LDA_API_KEY}");
    GovDataException e = assertThrows(GovDataException.class,
        () -> LdaApiProvider.open(LdaApiProvider.FILINGS, base + "/api/v1/filings/", params,
            headers, FAST, Q2_2025, new LocalFileStorageProvider(), cacheDir.getAbsolutePath()));
    assertTrue(e.getMessage().contains("LDA_API_KEY"), e.getMessage());
    assertEquals(0, requests.size(), "nothing may be requested without a usable key");
  }

  @Test void aWindowedTableNeedsItsPeriodBounds() {
    GovDataException e = assertThrows(GovDataException.class,
        () -> crawl(LdaApiProvider.FILINGS, "/api/v1/filings/", new HashMap<String, String>()));
    assertTrue(e.getMessage().contains("period_start"), e.getMessage());
  }

  @Test void anHttpErrorThatSurvivesTheRetriesFails() {
    // No route registered: the server answers 404, which is not retryable.
    GovDataException e = assertThrows(GovDataException.class,
        () -> rows(LdaApiProvider.FILINGS, "/api/v1/filings/", Q2_2025));
    assertTrue(e.getCause() instanceof RetryableHttp.HttpStatusException, String.valueOf(e));
    assertEquals(404, ((RetryableHttp.HttpStatusException) e.getCause()).getStatus());
  }

  // ---- contribution reports -----------------------------------------------------------------

  @Test void contributionReportsKeepOnlyTheFilersStateAndCountry() throws Exception {
    routeFixture("/api/v1/contributions/", "contributions_page.json", 2);
    List<Map<String, Object>> rows =
        rows(LdaApiProvider.CONTRIBUTION_REPORTS, "/api/v1/contributions/", window("2025-01-01",
            "2025-03-31"));
    assertEquals(3, rows.size());
    Map<String, Object> individual = rows.get(0);
    assertEquals("lobbyist", individual.get("filer_type"));
    assertEquals("CANFIELD", individual.get("lobbyist_last_name"));
    assertEquals(Boolean.FALSE, individual.get("no_contributions"));
    assertEquals(2, individual.get("item_count"));
    for (String key : individual.keySet()) {
      assertFalse(key.startsWith("address") || key.equals("zip") || key.equals("filer_city")
          || key.contains("contact"), "unexpected address or contact column " + key);
    }
    Map<String, Object> organization = rows.get(1);
    assertEquals("organization", organization.get("filer_type"));
    assertNull(organization.get("lobbyist_id"));
    assertEquals(1, organization.get("pac_count"));
    assertEquals(Boolean.TRUE, rows.get(2).get("no_contributions"));
  }

  @Test void contributionItemsAndPacsAreRowsOfTheirOwn() throws Exception {
    routeFixture("/api/v1/contributions/", "contributions_page.json", 2);
    Map<String, String> q1 = window("2025-01-01", "2025-03-31");
    List<Map<String, Object>> items =
        rows(LdaApiProvider.CONTRIBUTION_ITEMS, "/api/v1/contributions/", q1);
    assertEquals(4, items.size());
    assertEquals("feca", items.get(0).get("contribution_type"));
    assertEquals("FECA", items.get(0).get("contribution_type_name"));
    assertEquals("FRENCH HILL FOR ARKANSAS", items.get(0).get("payee_name"));
    assertEquals(1000.0, items.get(0).get("amount"));
    assertEquals("2025-02-04", items.get(0).get("contribution_date"));
    assertEquals(2, items.get(1).get("item_seq"));

    List<Map<String, Object>> pacs =
        rows(LdaApiProvider.CONTRIBUTION_PACS, "/api/v1/contributions/", q1);
    assertEquals(2, pacs.size());
    assertEquals("Kidney Care Council PAC", pacs.get(0).get("pac_name"));
    assertEquals("Recording Industry Association of America", pacs.get(1).get("pac_name"));
  }

  // ---- registries and code lists -------------------------------------------------------------

  @Test void lobbyistRegistryRowsCarryTheEmployerButNoContactDetails() throws Exception {
    routeFixture("/api/v1/lobbyists/", "lobbyists_page.json", 25);
    Map<String, String> refresh = new HashMap<String, String>();
    refresh.put("refresh_month", "09");
    List<Map<String, Object>> rows = rows(LdaApiProvider.LOBBYISTS, "/api/v1/lobbyists/", refresh);
    assertEquals(2, rows.size());
    assertEquals(1L, rows.get(0).get("lobbyist_id"));
    assertEquals("ALICE", rows.get(0).get("first_name"));
    assertEquals("DUCQ", rows.get(0).get("last_name"));
    assertEquals(51968L, rows.get(0).get("registrant_id"));
    assertEquals("MAYBERRY & ASSOCIATES, LLC", rows.get(0).get("registrant_name"));
    assertFalse(rows.get(0).containsKey("contact_name"));
  }

  @Test void registrantAndClientRegistries() throws Exception {
    routeFixture("/api/v1/registrants/", "registrants_page.json", 25);
    routeFixture("/api/v1/clients/", "clients_page.json", 25);
    Map<String, String> refresh = new HashMap<String, String>();
    refresh.put("refresh_month", "09");
    List<Map<String, Object>> registrants =
        rows(LdaApiProvider.REGISTRANTS, "/api/v1/registrants/", refresh);
    assertEquals(3, registrants.size());
    assertEquals(401111131L, registrants.get(0).get("registrant_id"));
    assertEquals("AEVEX CORP.", registrants.get(0).get("name"));
    assertFalse(registrants.get(0).containsKey("contact_name"));
    assertFalse(registrants.get(0).containsKey("contact_telephone"));

    List<Map<String, Object>> clients = rows(LdaApiProvider.CLIENTS, "/api/v1/clients/", refresh);
    assertEquals(3, clients.size());
    assertEquals(213852L, clients.get(0).get("client_id"));
    assertEquals("TURNTIDE TECHNOLOGIES", clients.get(0).get("name"));
    assertTrue(clients.get(0).get("registrant_id") instanceof Long);
  }

  @Test void aRegistryNeedsItsRefreshMonth() {
    GovDataException e = assertThrows(GovDataException.class,
        () -> crawl(LdaApiProvider.LOBBYISTS, "/api/v1/lobbyists/", new HashMap<String, String>()));
    assertTrue(e.getMessage().contains("refresh_month"), e.getMessage());
  }

  @Test void codeListsAreOneUnpagedRequestEach() throws Exception {
    String[][] lists = {
        {LdaApiProvider.ISSUE_CODES, "constants_issues.json", "issue_code", "issue_name"},
        {LdaApiProvider.FILING_TYPES, "constants_filing_types.json", "filing_type",
            "filing_type_name"},
        {LdaApiProvider.CONTRIBUTION_ITEM_TYPES, "constants_item_types.json", "contribution_type",
            "contribution_type_name"},
    };
    for (String[] l : lists) {
      String path = "/api/v1/c/" + l[0] + "/";
      String body = fixtureResults(l[1]).toString();
      routes.put(path, page -> body);
      List<Map<String, Object>> rows = rows(l[0], path, new HashMap<String, String>());
      assertEquals(fixtureResults(l[1]).size(), rows.size(), l[0]);
      assertTrue(rows.get(0).get(l[2]) != null && rows.get(0).get(l[3]) != null, l[0]);
    }
    String path = "/api/v1/c/gov/";
    String body = fixtureResults("constants_government_entities.json").toString();
    routes.put(path, page -> body);
    List<Map<String, Object>> gov = rows(LdaApiProvider.GOVERNMENT_ENTITY_CODES, path,
        new HashMap<String, String>());
    assertTrue(gov.get(0).get("government_entity_id") instanceof Integer);
    assertTrue(gov.get(0).get("government_entity_name") != null);
    assertEquals(4, requests.size(), "one request per code list");
  }

  @Test void everyTableTheSchemaNeedsHasASpec() {
    assertEquals(17, LdaApiProvider.tables().size());
  }
}
