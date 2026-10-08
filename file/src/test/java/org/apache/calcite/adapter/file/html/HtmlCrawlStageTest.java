/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.html;

import org.apache.calcite.adapter.file.converters.HtmlCrawlStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A schema's {@code crawl} operand against a site laid out like a wiki: navigation, a content
 * region, a navigation box inside the content, footnote markers, and a table with spans.
 */
@Tag("unit")
public class HtmlCrawlStageTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String START = "<html><body>"
      + "<nav><a href='/wiki/Main_Page'>Main page</a>"
      + "<a href='/wiki/Special:Random'>Random</a></nav>"
      + "<div id='mw-content-text'><div class='mw-parser-output'>"
      + "<p>See <a href='/wiki/Linked_Article'>the linked article</a>, "
      + "<a href='/wiki/Linked_Article#History'>its history</a>, "
      + "<a href='/wiki/File:Chart.png'>a chart</a>, "
      + "<a href='/wiki/Start#Notes'>a note</a> and "
      + "<a href='/data/figures.csv?download=1'>the figures</a>.</p>"
      + "<table class='wikitable'><caption>Population by region</caption>"
      + "<tr><th rowspan='2'>Region</th><th colspan='2'>Population</th></tr>"
      + "<tr><th>2020</th><th>2021</th></tr>"
      + "<tr><th rowspan='2'>North<sup class='reference'>[1]</sup></th><td>10</td><td>11</td></tr>"
      + "<tr><td>12</td><td>13</td></tr>"
      + "<tr><td>South</td><td>20</td><td>21</td></tr>"
      + "<tr><th>Region</th><th>2020</th><th>2021</th></tr>"
      + "</table>"
      + "<table><tr><td>layout</td><td>only</td></tr></table>"
      + "<table class='navbox'><tr><td><a href='/wiki/Navbox_Only'>related</a></td></tr></table>"
      + "<div class='reflist'><a href='/wiki/Cited_Page'>cited</a></div>"
      + "</div></div>"
      + "<footer><a href='/wiki/Footer_Page'>About</a></footer>"
      + "</body></html>";

  private static final String LINKED = "<html><body>"
      + "<div id='mw-content-text'><div class='mw-parser-output'>"
      + "<div class='mw-heading'><h2>Results</h2></div>"
      + "<p>Text with <a href='/wiki/Second_Level'>a further link</a>.</p>"
      + "<table class='wikitable'><tr><th>Team</th><th>Points</th></tr>"
      + "<tr><td>Reds</td><td>3</td></tr><tr><td>Blues</td><td>1</td></tr></table>"
      + "</div></div></body></html>";

  @TempDir File directory;
  private HttpServer server;
  private String base;
  private final List<String> requested = new CopyOnWriteArrayList<>();
  private final List<String> agents = new CopyOnWriteArrayList<>();

  @BeforeEach void serve() throws IOException {
    final Map<String, String> pages = new HashMap<>();
    pages.put("/wiki/Start", START);
    pages.put("/wiki/Linked_Article", LINKED);
    pages.put("/data/figures.csv", "id,amount\n1,5\n2,7\n");
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/", (HttpExchange exchange) -> {
      String path = exchange.getRequestURI().getPath();
      boolean head = "HEAD".equals(exchange.getRequestMethod());
      if (!head) {
        requested.add(path);
      }
      agents.add(String.valueOf(exchange.getRequestHeaders().getFirst("User-Agent")));
      String body = pages.get(path);
      byte[] bytes = (body == null ? "" : body).getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type",
          path.endsWith(".csv") ? "text/csv" : "text/html; charset=utf-8");
      exchange.sendResponseHeaders(body == null ? 404 : 200, head ? -1 : bytes.length);
      if (!head) {
        exchange.getResponseBody().write(bytes);
      }
      exchange.close();
    });
    server.start();
    base = "http://localhost:" + server.getAddress().getPort();
  }

  @AfterEach void stop() {
    server.stop(0);
  }

  private Map<String, Object> crawl() {
    Map<String, Object> crawl = new HashMap<>();
    crawl.put("startUrls", Collections.singletonList(base + "/wiki/Start"));
    crawl.put("maxDepth", 1);
    crawl.put("requestDelay", "0 seconds");
    crawl.put("contentSelector", "#mw-content-text .mw-parser-output");
    crawl.put("removeSelectors", Arrays.asList(".navbox", ".reflist", "sup.reference"));
    crawl.put("linkExcludePatterns", Arrays.asList("/wiki/[A-Za-z_]+:", "[?&]action="));
    crawl.put("tableSelector", "table.wikitable");
    crawl.put("userAgent", "ExampleBot/1.0 (ops@example.test)");
    return crawl;
  }

  private List<String> files() {
    List<String> names = new ArrayList<>();
    for (File file : directory.listFiles()) {
      if (!file.getName().startsWith(".")) {
        names.add(file.getName());
      }
    }
    Collections.sort(names);
    return names;
  }

  @Test void onlyLinksInTheContentAreFollowedAndWhatThePagesHoldLandsAsFiles() throws Exception {
    HtmlCrawlStage.run("wiki", crawl(), directory.getPath(), "local", "SMART_CASING");

    // Navigation, the footer, the navigation box, the reference list and the File: page are
    // never requested; a link to a place on a page is that page, once.
    assertEquals(
        new TreeSet<>(Arrays.asList("/wiki/Start", "/wiki/Linked_Article", "/data/figures.csv")),
        new TreeSet<>(requested));
    assertEquals(1, Collections.frequency(requested, "/wiki/Linked_Article"));
    assertEquals(Collections.singleton("ExampleBot/1.0 (ops@example.test)"),
        new TreeSet<>(agents));
    assertEquals(
        Arrays.asList("figures.csv", "linked_article__results.json",
            "start__population_by_region.json"),
        files());

    // A cell that spans rows or columns is in every row and column it covers; the footnote
    // marker is gone; the header repeated at the foot is no row.
    JsonNode rows = MAPPER.readTree(new File(directory, "start__population_by_region.json"));
    assertEquals(3, rows.size());
    List<String> columns = new ArrayList<>();
    rows.get(0).fieldNames().forEachRemaining(columns::add);
    assertEquals(3, columns.size());
    assertEquals("North", rows.get(0).get(columns.get(0)).asText());
    assertEquals("North", rows.get(1).get(columns.get(0)).asText());
    assertEquals(13, rows.get(1).get(columns.get(2)).asInt());
    assertEquals("South", rows.get(2).get(columns.get(0)).asText());
    assertTrue(columns.get(1).toLowerCase().contains("2020"), columns.toString());
    assertEquals("id,amount\n1,5\n2,7\n",
        new String(Files.readAllBytes(new File(directory, "figures.csv").toPath()),
            StandardCharsets.UTF_8));
  }

  @Test void aCrawlWithinItsCacheLifetimeRequestsNothingAndAChangedOperandCrawlsAgain()
      throws Exception {
    Map<String, Object> crawl = crawl();
    HtmlCrawlStage.run("wiki", crawl, directory.getPath(), "local", "SMART_CASING");
    requested.clear();
    HtmlCrawlStage.run("wiki", crawl, directory.getPath(), "local", "SMART_CASING");
    assertEquals(Collections.emptyList(), requested);

    // Not following links any more: the linked article's table is no longer the crawl's.
    crawl.put("maxDepth", 0);
    HtmlCrawlStage.run("wiki", crawl, directory.getPath(), "local", "SMART_CASING");
    assertEquals(Arrays.asList("figures.csv", "start__population_by_region.json"), files());
  }

  @Test void aStartUrlThatCannotBeReadFailsTheCrawl() {
    Map<String, Object> crawl = crawl();
    crawl.put("startUrls", Collections.singletonList(base + "/wiki/Missing"));
    assertThrows(IOException.class,
        () -> HtmlCrawlStage.run("wiki", crawl, directory.getPath(), "local", "SMART_CASING"));
    crawl.remove("startUrls");
    assertThrows(IllegalArgumentException.class,
        () -> HtmlCrawlStage.run("wiki", crawl, directory.getPath(), "local", "SMART_CASING"));
    assertThrows(IllegalArgumentException.class,
        () -> HtmlCrawlStage.run("wiki", crawl(), "s3://bucket/x", "s3", "SMART_CASING"));
  }

  @Test void aSchemaThatDeclaresACrawlHasItsTablesAndDataFilesAsTables() throws Exception {
    String model = "{\"version\":\"1.0\",\"defaultSchema\":\"wiki\",\"schemas\":[{"
        + "\"name\":\"wiki\",\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{\"directory\":" + MAPPER.writeValueAsString(directory.getPath())
        + ",\"ephemeralCache\":true,\"crawl\":" + MAPPER.writeValueAsString(crawl()) + "}}]}";
    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {
      List<String> tables = new ArrayList<>();
      try (ResultSet rs = connection.getMetaData().getTables(null, "wiki", "%", null)) {
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }
      }
      Collections.sort(tables);
      assertEquals(
          Arrays.asList("figures", "linked_article__results", "start__population_by_region"),
          tables);
      try (ResultSet rs = statement.executeQuery(
          "select \"team\", \"points\" from \"wiki\".\"linked_article__results\" "
              + "order by \"points\" desc")) {
        assertTrue(rs.next());
        assertEquals("Reds", rs.getString(1));
        assertEquals(3, rs.getInt(2));
        assertTrue(rs.next());
        assertFalse(rs.next());
      }
    }
  }
}
