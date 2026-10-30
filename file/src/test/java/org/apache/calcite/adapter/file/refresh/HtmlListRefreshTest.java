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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.adapter.file.converters.CrawlerConfiguration;
import org.apache.calcite.adapter.file.converters.SourceFileConversion;
import org.apache.calcite.util.Sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A local HTML page's list table after the page changes (REQ-2186): the watcher and a
 * refreshable table re-run the schema's own conversion, so the list's rows are the page's new
 * ones and the table names and columns keep the schema's casing.
 */
@Tag("unit")
public class HtmlListRefreshTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir File directory;
  @TempDir File baseDirectory;
  private File page;
  private File conversions;
  private SourceFileConversion conversion;
  private final String schema = "html_list_refresh_" + System.nanoTime();

  private static String page(String... items) {
    StringBuilder list = new StringBuilder();
    for (String item : items) {
      list.append("<li><a href='/wiki/").append(item).append("'>").append(item).append("</a></li>");
    }
    return "<html><body><nav><ul><li>a</li><li>b</li><li>c</li></ul></nav>"
        + "<h2>Results</h2>"
        + "<table><tr><th>Team Name</th><th>Points</th></tr><tr><td>Reds</td><td>3</td></tr>"
        + "</table>"
        + "<h2>Member states</h2><ol>" + list + "</ol></body></html>";
  }

  @BeforeEach void convertOnce() throws IOException {
    File sub = new File(directory, "eu");
    sub.mkdirs();
    page = new File(sub, "page.html");
    Files.write(page.toPath(),
        page("Austria", "Belgium", "Bulgaria").getBytes(StandardCharsets.UTF_8));
    page.setLastModified(System.currentTimeMillis() - 60_000L);
    conversions = new File(baseDirectory, "conversions");
    conversions.mkdirs();
    Map<String, Object> operand = new HashMap<>();
    operand.put("generateTablesFromHtmlLists", true);
    // As FileSchema makes it: the data directory as the root, conversions/ of the cache directory.
    conversion = new SourceFileConversion(directory.getPath(), conversions, baseDirectory,
        "UPPER", "UPPER", CrawlerConfiguration.pagesOf(operand));
    conversion.convertIfNeeded(page);
  }

  @AfterEach void unwatch() {
    ConversionFileWatcher.getInstance().unwatchFile(schema, page);
  }

  private List<String> files() {
    List<String> names = new ArrayList<>();
    for (File file : conversions.listFiles()) {
      if (file.getName().endsWith(".json")) {
        names.add(file.getName());
      }
    }
    Collections.sort(names);
    return names;
  }

  private List<String> listValues() throws IOException {
    List<String> values = new ArrayList<>();
    JsonNode rows = MAPPER.readTree(new File(conversions, "eu_page__member_states.json"));
    for (JsonNode row : rows) {
      if (!row.has("value")) {
        throw new AssertionError("no value column: " + rows);
      }
      values.add(row.get("value").asText());
    }
    return values;
  }

  private List<String> tableColumns() throws IOException {
    List<String> columns = new ArrayList<>();
    MAPPER.readTree(new File(conversions, "eu_page__RESULTS.json")).get(0).fieldNames()
        .forEachRemaining(columns::add);
    return columns;
  }

  private void change() throws IOException {
    Files.write(page.toPath(),
        page("Croatia", "Cyprus", "Czechia", "Denmark").getBytes(StandardCharsets.UTF_8));
    // Strictly after the conversion recorded the page, as an edit made later is.
    page.setLastModified(System.currentTimeMillis() + 2_000L);
  }

  @Test void theWatcherRewritesTheListTableAndKeepsTheCasing() throws Exception {
    List<String> before = files();
    assertEquals(Arrays.asList("eu_page__RESULTS.json", "eu_page__member_states.json"), before);
    List<String> columns = tableColumns();
    assertEquals(Arrays.asList("Austria", "Belgium", "Bulgaria"), listValues());

    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    watcher.watchFile(schema, page, Duration.ofMinutes(60), conversion);
    change();
    Method checkFile =
        ConversionFileWatcher.class.getDeclaredMethod("checkFile", String.class, File.class);
    checkFile.setAccessible(true);
    checkFile.invoke(watcher, schema, page);

    assertEquals(before, files());
    assertEquals(columns, tableColumns());
    assertEquals(Arrays.asList("Croatia", "Cyprus", "Czechia", "Denmark"), listValues());
  }

  @Test void aRefreshableTableRewritesTheListTableAndKeepsTheCasing() throws Exception {
    List<String> before = files();
    List<String> columns = tableColumns();
    RefreshableJsonTable table = new RefreshableJsonTable(
        Sources.of(new File(conversions, "eu_page__member_states.json")), "member_states",
        Duration.ofMinutes(60), conversion);
    change();
    table.doRefresh();

    assertEquals(before, files());
    assertEquals(columns, tableColumns());
    assertEquals(Arrays.asList("Croatia", "Cyprus", "Czechia", "Denmark"), listValues());
  }
}
