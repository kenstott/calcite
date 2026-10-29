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

import org.apache.calcite.adapter.file.converters.CrawlerConfiguration;
import org.apache.calcite.adapter.file.converters.HtmlToJsonConverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A file schema's own HTML files with {@code generateTablesFromHtmlLists} (REQ-2186): their
 * content's lists are tables, read by the same rule and named the same way as a crawled page's.
 */
@Tag("unit")
public class HtmlListLocalFileTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String PAGE = "<html><body>"
      + "<header><ul><li>Logo</li><li>Search</li><li>Sign in</li></ul></header>"
      + "<div class='skin'><ul><li>Skin</li><li>Menu</li><li>Items</li></ul></div>"
      + "<main>"
      + "<h2>Results</h2>"
      + "<table><tr><th>Team</th><th>Points</th></tr><tr><td>Reds</td><td>3</td></tr></table>"
      + "<h2>Member states</h2>"
      + "<ol><li><a href='austria.html'>Austria</a></li><li>Belgium</li>"
      + "<li><a href='https://other.test/Bulgaria'>Bulgaria</a></li></ol>"
      + "<aside><ul><li>See</li><li>also</li><li>this</li></ul></aside>"
      + "</main>"
      + "<footer><ul><li>Terms</li><li>Privacy</li><li>Contact</li></ul></footer>"
      + "</body></html>";

  @TempDir File directory;
  @TempDir File output;
  @TempDir File baseDirectory;
  private File page;

  @BeforeEach void write() throws IOException {
    page = new File(directory, "page.html");
    Files.write(page.toPath(), PAGE.getBytes(StandardCharsets.UTF_8));
  }

  private static CrawlerConfiguration pages(boolean lists) {
    Map<String, Object> operand = new HashMap<>();
    operand.put("contentSelector", "main");
    if (lists) {
      operand.put("generateTablesFromHtmlLists", true);
    }
    return CrawlerConfiguration.pagesOf(operand);
  }

  private static List<String> names(List<File> files) {
    List<String> names = new ArrayList<>();
    for (File file : files) {
      names.add(file.getName());
    }
    Collections.sort(names);
    return names;
  }

  @Test void withTheOptionOffAPageGivesItsTablesOnly() throws IOException {
    List<File> files = HtmlToJsonConverter.convert(page, output, "UNCHANGED", "SMART_CASING",
        null, baseDirectory, null, pages(false));
    assertEquals(Collections.singletonList("page__results.json"), names(files));
  }

  @Test void withTheOptionOnTheContentsListsAreTablesToo() throws IOException {
    List<File> files = HtmlToJsonConverter.convert(page, output, "UNCHANGED", "SMART_CASING",
        null, baseDirectory, null, pages(true));
    // The list outside the content and those in the header, footer and aside give no table.
    assertEquals(Arrays.asList("page__member_states.json", "page__results.json"), names(files));

    JsonNode rows = MAPPER.readTree(new File(output, "page__member_states.json"));
    assertEquals(3, rows.size());
    List<String> columns = new ArrayList<>();
    rows.get(0).fieldNames().forEachRemaining(columns::add);
    assertEquals(Arrays.asList("value", "uri", "level", "position"), columns);
    // A link is absolute against the page's own address: its file.
    assertEquals(new File(directory, "austria.html").toURI().toString(),
        rows.get(0).get("uri").asText());
    assertEquals("", rows.get(1).get("uri").asText());
    assertEquals("https://other.test/Bulgaria", rows.get(2).get("uri").asText());
  }

  @Test void aListNamedAsATableOfThePageIsNumbered() throws IOException {
    Files.write(page.toPath(), ("<html><body><h2>Results</h2>"
        + "<table><tr><th>Team</th></tr><tr><td>Reds</td></tr></table>"
        + "<ul><li>a</li><li>b</li><li>c</li></ul></body></html>")
        .getBytes(StandardCharsets.UTF_8));
    List<File> files = HtmlToJsonConverter.convert(page, output, "UNCHANGED", "SMART_CASING",
        null, baseDirectory, null, CrawlerConfiguration.pagesOf(
            Collections.<String, Object>singletonMap("generateTablesFromHtmlLists", true)));
    assertEquals(Arrays.asList("page__results.json", "page__results_2.json"), names(files));
  }

  @Test void aSchemaWithTheOptionHasItsPagesListsAsTables() throws Exception {
    String model = "{\"version\":\"1.0\",\"defaultSchema\":\"docs\",\"schemas\":[{"
        + "\"name\":\"docs\",\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\":{\"directory\":" + MAPPER.writeValueAsString(directory.getPath())
        + ",\"baseDirectory\":" + MAPPER.writeValueAsString(baseDirectory.getPath())
        + ",\"ephemeralCache\":true,\"generateTablesFromHtmlLists\":true"
        + ",\"contentSelector\":\"main\"}}]}";
    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {
      List<String> tables = new ArrayList<>();
      try (ResultSet rs = connection.getMetaData().getTables(null, "docs", "%", null)) {
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }
      }
      assertTrue(tables.contains("page__member_states"), tables.toString());
      try (ResultSet rs = statement.executeQuery(
          "select \"value\", \"level\", \"position\" from \"docs\".\"page__member_states\" "
              + "order by \"position\"")) {
        assertTrue(rs.next());
        assertEquals("Austria", rs.getString(1));
        assertEquals(1, rs.getInt(2));
        assertEquals(1, rs.getInt(3));
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertEquals("Bulgaria", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }
}
