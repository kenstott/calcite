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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A crawl's {@code generateTablesFromHtmlLists} option (REQ-2186): the lists of a page's
 * content land as tables, named for the heading above them; lists outside the content do not.
 */
@Tag("unit")
public class HtmlListCrawlStageTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String PAGE = "<html><body>"
      + "<nav><ul><li><a href='/wiki/Home'>Home</a></li><li><a href='/wiki/About'>About</a></li>"
      + "<li><a href='/wiki/Help'>Help</a></li></ul></nav>"
      + "<div id='content'>"
      + "<h2>Member states</h2>"
      + "<ol><li><a href='/wiki/Austria'>Austria</a></li><li><a href='/wiki/Belgium'>Belgium</a>"
      + "</li><li><a href='https://other.test/Bulgaria'>Bulgaria</a></li></ol>"
      + "<h2>Colours</h2>"
      + "<ul><li>Red</li><li>Green</li><li>Blue</li></ul>"
      + "<p>And again:</p>"
      + "<ul><li>Cyan</li><li>Magenta</li><li>Yellow</li></ul>"
      + "<h3>Pairs</h3>"
      + "<ul><li>Left</li><li>Right</li></ul>"
      + "<div class='navbox'><ul><li>x</li><li>y</li><li>z</li></ul></div>"
      + "<aside><ul><li>See</li><li>also</li><li>this</li></ul></aside>"
      + "<header><ul><li>Top</li><li>of</li><li>content</li></ul></header>"
      + "</div>"
      + "<footer><ul><li>Terms</li><li>Privacy</li><li>Contact</li></ul></footer>"
      + "</body></html>";

  @TempDir File directory;
  @TempDir File crawlerWorkDir;
  private HttpServer server;
  private String base;

  @BeforeEach void serve() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/", (HttpExchange exchange) -> {
      boolean found = "/wiki/Start".equals(exchange.getRequestURI().getPath());
      boolean head = "HEAD".equals(exchange.getRequestMethod());
      byte[] bytes = (found ? PAGE : "").getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
      exchange.sendResponseHeaders(found ? 200 : 404, head ? -1 : bytes.length);
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

  private Map<String, Object> crawl(boolean lists) {
    Map<String, Object> crawl = new HashMap<>();
    crawl.put("startUrls", Collections.singletonList(base + "/wiki/Start"));
    crawl.put("maxDepth", 0);
    crawl.put("requestDelay", "0 seconds");
    crawl.put("contentSelector", "#content");
    crawl.put("removeSelectors", Collections.singletonList(".navbox"));
    if (lists) {
      crawl.put("generateTablesFromHtmlLists", true);
    }
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

  private JsonNode read(String name) throws IOException {
    return MAPPER.readTree(new File(directory, name));
  }

  @Test void withTheOptionOffNoListIsATable() throws Exception {
    HtmlCrawlStage.run("lists", crawl(false), directory.getPath(), "local", "SMART_CASING",
        crawlerWorkDir);
    assertEquals(Collections.emptyList(), files());
  }

  @Test void theContentsListsAreTablesNamedForTheHeadingAboveThem() throws Exception {
    HtmlCrawlStage.run("lists", crawl(true), directory.getPath(), "local", "SMART_CASING",
        crawlerWorkDir);

    // The navigation, footer and navigation-box lists are outside the content; the aside and
    // header lists inside it are never content; the two-item list is too short; two lists
    // under one heading are numbered.
    assertEquals(
        Arrays.asList("start__colours.json", "start__colours_2.json",
            "start__member_states.json"),
        files());

    JsonNode members = read("start__member_states.json");
    assertEquals(3, members.size());
    List<String> columns = new ArrayList<>();
    members.get(0).fieldNames().forEachRemaining(columns::add);
    assertEquals(Arrays.asList("value", "uri", "level", "position"), columns);
    assertEquals("Austria", members.get(0).get("value").asText());
    assertEquals(base + "/wiki/Austria", members.get(0).get("uri").asText());
    assertEquals("https://other.test/Bulgaria", members.get(2).get("uri").asText());
    assertEquals(1, members.get(2).get("level").asInt());
    assertEquals(3, members.get(2).get("position").asInt());

    JsonNode colours = read("start__colours.json");
    List<String> plain = new ArrayList<>();
    colours.get(0).fieldNames().forEachRemaining(plain::add);
    assertEquals(Arrays.asList("value", "level"), plain);
    assertEquals("Red", colours.get(0).get("value").asText());
    assertEquals("Cyan", read("start__colours_2.json").get(0).get("value").asText());
  }
}
