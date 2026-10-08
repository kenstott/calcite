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
package org.apache.calcite.adapter.file.converters;

import org.apache.calcite.adapter.file.util.SmartCasing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The crawl a file schema declares with its {@code crawl} operand.
 *
 * <p>The pages reached from the start URLs are read, and what they hold lands in the schema's
 * directory as ordinary files: each HTML table as a JSON file, each linked data file (CSV, TSV,
 * Excel, JSON, Parquet) as itself. The schema then discovers them like any other file of the
 * directory, so a crawled table is a table of the schema with nothing else to know about it.
 *
 * <p>A crawl is kept for the configured HTML cache lifetime: a schema made again within it,
 * with the same operand, reads the files already there and requests nothing.
 */
public final class HtmlCrawlStage {
  private static final Logger LOGGER = LoggerFactory.getLogger(HtmlCrawlStage.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** What the crawl left in the directory: skipped by the schema's scan, as every dot file is. */
  static final String MANIFEST = ".crawl-manifest.json";

  private static final int MAX_NAME = 60;

  private HtmlCrawlStage() {
  }

  /**
   * Runs the crawl {@code operand} declares into {@code directoryPath}.
   *
   * @return the files the crawl holds in the directory, by the URL each came from
   */
  public static Map<String, List<String>> run(String schemaName, Map<String, Object> operand,
      String directoryPath, String storageType, String columnNameCasing) throws IOException {
    if (!"local".equals(storageType)) {
      throw new IllegalArgumentException("Schema '" + schemaName + "' declares a crawl with "
          + "storageType '" + storageType + "': a crawl lands its files in a local directory");
    }
    List<String> startUrls = startUrls(schemaName, operand);
    Map<String, Object> options = new HashMap<>(operand);
    // The operand is the request to crawl: how far it goes is maxDepth's to say.
    options.put("enabled", true);
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);

    File directory = new File(directoryPath);
    if (!directory.isDirectory() && !directory.mkdirs()) {
      throw new IOException("Cannot create the crawl directory " + directory);
    }
    File manifestFile = new File(directory, MANIFEST);
    String fingerprint = fingerprint(operand, columnNameCasing);
    Map<String, List<String>> kept = fresh(manifestFile, fingerprint, config, directory);
    if (kept != null) {
      LOGGER.info("Schema '{}': the crawl in {} is within its cache lifetime; nothing requested",
          schemaName, directory);
      return kept;
    }

    Set<String> before = manifestFile.isFile() ? filesOf(manifestFile) : new HashSet<String>();
    Map<String, List<String>> written = new LinkedHashMap<>();
    Set<String> names = new HashSet<>();
    HtmlCrawler crawler = new HtmlCrawler(config);
    try {
      HtmlCrawler.CrawlResult result = crawler.crawl(startUrls);
      for (String startUrl : startUrls) {
        if (result.getFailedUrls().contains(startUrl)) {
          throw new IOException("Schema '" + schemaName + "': the crawl could not read its "
              + "start URL " + startUrl);
        }
      }
      if (config.isGenerateTablesFromHtml()) {
        for (String url : result.getVisitedUrls()) {
          HtmlLinkCache.ExtractedLinks page = crawler.getLinkCache().peek(url);
          if (page == null || page.getContent() == null) {
            continue;
          }
          List<String> files =
              writeTables(url, page.getContent(), directory, config, columnNameCasing, names);
          if (!files.isEmpty()) {
            written.put(url, files);
          }
        }
      }
      for (Map.Entry<String, File> dataFile : result.getDataFiles().entrySet()) {
        String name = unique(dataFileName(dataFile.getKey()), names);
        Files.copy(dataFile.getValue().toPath(), new File(directory, name).toPath(),
            StandardCopyOption.REPLACE_EXISTING);
        List<String> files = new ArrayList<>();
        files.add(name);
        written.put(dataFile.getKey(), files);
      }
    } finally {
      crawler.cleanup();
    }

    // A file the last crawl left and this one did not make again is no longer on the site.
    for (String old : before) {
      if (!names.contains(old)) {
        Files.deleteIfExists(new File(directory, old).toPath());
      }
    }
    writeManifest(manifestFile, fingerprint, written);
    LOGGER.info("Schema '{}': the crawl of {} wrote {} file(s) from {} source(s) into {}",
        schemaName, startUrls, names.size(), written.size(), directory);
    return written;
  }

  private static List<String> startUrls(String schemaName, Map<String, Object> operand) {
    Object value = operand.get("startUrls");
    List<String> urls = new ArrayList<>();
    if (value instanceof Iterable) {
      for (Object url : (Iterable<?>) value) {
        urls.add(url.toString());
      }
    } else if (value != null) {
      urls.add(value.toString());
    }
    if (urls.isEmpty()) {
      throw new IllegalArgumentException("Schema '" + schemaName + "' declares a crawl with no "
          + "startUrls");
    }
    return urls;
  }

  // -- the manifest -------------------------------------------------------------------------

  private static String fingerprint(Map<String, Object> operand, String columnNameCasing) {
    try {
      Map<String, Object> described = new TreeMap<>(operand);
      described.put("#columnNameCasing", columnNameCasing);
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(MAPPER.writeValueAsBytes(described));
      StringBuilder hex = new StringBuilder();
      for (int i = 0; i < 8; i++) {
        hex.append(String.format(Locale.ROOT, "%02x", hash[i]));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException | IOException e) {
      throw new IllegalStateException("Cannot describe the crawl operand", e);
    }
  }

  /**
   * The files of the last crawl, when it was this operand's, is within the HTML cache lifetime
   * and every file it wrote is still there; null when the crawl has to run.
   */
  private static Map<String, List<String>> fresh(File manifestFile, String fingerprint,
      CrawlerConfiguration config, File directory) throws IOException {
    if (!manifestFile.isFile()) {
      return null;
    }
    JsonNode manifest = MAPPER.readTree(manifestFile);
    long age = System.currentTimeMillis() - manifest.get("crawledAt").asLong();
    if (!fingerprint.equals(manifest.get("fingerprint").asText())
        || age > config.getHtmlCacheTTL().toMillis()) {
      return null;
    }
    Map<String, List<String>> files = new LinkedHashMap<>();
    java.util.Iterator<Map.Entry<String, JsonNode>> sources = manifest.get("sources").fields();
    while (sources.hasNext()) {
      Map.Entry<String, JsonNode> source = sources.next();
      List<String> names = new ArrayList<>();
      for (JsonNode name : source.getValue()) {
        if (!new File(directory, name.asText()).isFile()) {
          return null;
        }
        names.add(name.asText());
      }
      files.put(source.getKey(), names);
    }
    return files;
  }

  private static Set<String> filesOf(File manifestFile) throws IOException {
    Set<String> names = new HashSet<>();
    for (JsonNode source : MAPPER.readTree(manifestFile).get("sources")) {
      for (JsonNode name : source) {
        names.add(name.asText());
      }
    }
    return names;
  }

  private static void writeManifest(File manifestFile, String fingerprint,
      Map<String, List<String>> written) throws IOException {
    ObjectNode manifest = MAPPER.createObjectNode();
    manifest.put("fingerprint", fingerprint);
    manifest.put("crawledAt", System.currentTimeMillis());
    manifest.set("sources", MAPPER.valueToTree(written));
    atomicWrite(manifestFile, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(manifest));
  }

  private static void atomicWrite(File target, byte[] content) throws IOException {
    File temp = new File(target.getParentFile(), "." + target.getName() + ".tmp");
    Files.write(temp.toPath(), content);
    Files.move(temp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.ATOMIC_MOVE);
  }

  // -- names --------------------------------------------------------------------------------

  /** {@code text} as part of a file name: lower case, runs of anything else as one underscore. */
  static String slug(String text) {
    String slug = text.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_")
        .replaceAll("^_+|_+$", "");
    return slug.length() > MAX_NAME ? slug.substring(0, MAX_NAME).replaceAll("_+$", "") : slug;
  }

  /** The last segment of the URL's path, decoded; the host when the path has none. */
  private static String lastSegment(String url) {
    try {
      URI uri = new URI(url);
      String path = uri.getRawPath() == null ? "" : uri.getRawPath().replaceAll("/+$", "");
      String segment = path.substring(path.lastIndexOf('/') + 1);
      if (segment.isEmpty()) {
        return uri.getHost() == null ? url : uri.getHost();
      }
      return URLDecoder.decode(segment, StandardCharsets.UTF_8.name());
    } catch (URISyntaxException | IOException e) {
      throw new IllegalArgumentException("Not a URL the crawl can name a file for: " + url, e);
    }
  }

  private static String dataFileName(String url) {
    String segment = lastSegment(url);
    int dot = segment.lastIndexOf('.');
    if (dot < 0) {
      throw new IllegalArgumentException("The data file at " + url + " has no extension to "
          + "say what kind of file it is");
    }
    return slug(segment.substring(0, dot)) + segment.substring(dot).toLowerCase(Locale.ROOT);
  }

  /** {@code name}, or the first of name_2, name_3... the crawl has not used; and marks it used. */
  private static String unique(String name, Set<String> used) {
    int dot = name.lastIndexOf('.');
    String stem = name.substring(0, dot);
    String candidate = name;
    for (int n = 2; !used.add(candidate); n++) {
      candidate = stem + "_" + n + name.substring(dot);
    }
    return candidate;
  }

  // -- tables -------------------------------------------------------------------------------

  private static List<String> writeTables(String url, Element content, File directory,
      CrawlerConfiguration config, String columnNameCasing, Set<String> names) throws IOException {
    List<String> files = new ArrayList<>();
    String page = slug(lastSegment(url));
    // In document order, so a table is named for the last heading before it.
    String heading = null;
    int unnamed = 0;
    for (Element element : content.select("h1, h2, h3, h4, h5, h6, table")) {
      if (!"table".equals(element.tagName())) {
        heading = element.text().trim();
        continue;
      }
      if (!element.is(config.getTableSelector())) {
        continue;
      }
      List<Map<String, String>> rows = rows(element, columnNameCasing);
      if (rows.size() < config.getHtmlTableMinRows() || rows.size() > config.getHtmlTableMaxRows()) {
        continue;
      }
      Element caption = element.selectFirst("caption");
      String title = caption != null && !caption.text().trim().isEmpty()
          ? caption.text().trim() : heading;
      String table = title == null || slug(title).isEmpty() ? "table_" + (++unnamed) : slug(title);
      String name = unique(page + "__" + table + ".json", names);
      ArrayNode json = MAPPER.createArrayNode();
      for (Map<String, String> row : rows) {
        ObjectNode object = MAPPER.createObjectNode();
        for (Map.Entry<String, String> cell : row.entrySet()) {
          ConverterUtils.setJsonValueWithTypeInference(object, cell.getKey(), cell.getValue());
        }
        json.add(object);
      }
      atomicWrite(new File(directory, name),
          MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(json));
      files.add(name);
    }
    return files;
  }

  /** One cell of a table's grid: its text, and whether it is a header cell. */
  private static final class Cell {
    final String text;
    final boolean header;

    Cell(String text, boolean header) {
      this.text = text;
      this.header = header;
    }
  }

  /**
   * The table's rows by column name. A cell that spans rows or columns is in every position it
   * covers, so a row reads whole on its own. The header is the table's leading rows of header
   * cells, read down each column; a later row of header cells alone repeats it and is no row.
   */
  static List<Map<String, String>> rows(Element table, String columnNameCasing) {
    List<List<Cell>> grid = grid(table);
    int width = 0;
    for (List<Cell> row : grid) {
      width = Math.max(width, row.size());
    }
    int headerRows = 0;
    while (headerRows < grid.size() && allHeader(grid.get(headerRows))) {
      headerRows++;
    }
    List<String> columns = new ArrayList<>();
    Set<String> taken = new HashSet<>();
    for (int c = 0; c < width; c++) {
      StringBuilder label = new StringBuilder();
      String last = null;
      for (int r = 0; r < headerRows; r++) {
        List<Cell> row = grid.get(r);
        String text = c < row.size() ? row.get(c).text : "";
        // A header that spans rows is one label, not one per row it covers.
        if (!text.isEmpty() && !text.equals(last)) {
          label.append(label.length() == 0 ? "" : " ").append(text);
        }
        last = text;
      }
      String column = label.length() == 0
          ? "col" + c : SmartCasing.applyCasing(label.toString(), columnNameCasing);
      String candidate = column;
      for (int n = 2; !taken.add(candidate); n++) {
        candidate = column + "_" + n;
      }
      columns.add(candidate);
    }
    List<Map<String, String>> rows = new ArrayList<>();
    for (int r = headerRows; r < grid.size(); r++) {
      List<Cell> row = grid.get(r);
      if (row.isEmpty() || allHeader(row)) {
        continue;
      }
      Map<String, String> values = new LinkedHashMap<>();
      for (int c = 0; c < width; c++) {
        values.put(columns.get(c), c < row.size() ? row.get(c).text : "");
      }
      rows.add(values);
    }
    return rows;
  }

  private static boolean allHeader(List<Cell> row) {
    if (row.isEmpty()) {
      return false;
    }
    for (Cell cell : row) {
      if (!cell.header) {
        return false;
      }
    }
    return true;
  }

  private static List<List<Cell>> grid(Element table) {
    List<List<Cell>> grid = new ArrayList<>();
    // Cells carried down from a row above: column -> (the cell, rows it still covers).
    Map<Integer, Cell> carried = new HashMap<>();
    Map<Integer, Integer> remaining = new HashMap<>();
    for (Element tr : table.select("tr")) {
      if (tr.closest("table") != table) {
        continue; // a row of a table nested in a cell
      }
      List<Cell> row = new ArrayList<>();
      List<Element> cells = new ArrayList<>();
      for (Element child : tr.children()) {
        if ("th".equals(child.tagName()) || "td".equals(child.tagName())) {
          cells.add(child);
        }
      }
      int next = 0;
      int column = 0;
      while (next < cells.size() || remaining.containsKey(column)) {
        if (remaining.containsKey(column)) {
          row.add(carried.get(column));
          int left = remaining.get(column) - 1;
          if (left == 0) {
            remaining.remove(column);
            carried.remove(column);
          } else {
            remaining.put(column, left);
          }
          column++;
          continue;
        }
        Element element = cells.get(next++);
        Cell cell = new Cell(text(element), "th".equals(element.tagName()));
        int colspan = span(element, "colspan");
        int rowspan = span(element, "rowspan");
        for (int i = 0; i < colspan; i++) {
          row.add(cell);
          if (rowspan > 1) {
            carried.put(column, cell);
            remaining.put(column, rowspan - 1);
          }
          column++;
        }
      }
      grid.add(row);
    }
    return grid;
  }

  private static int span(Element cell, String attribute) {
    String value = cell.attr(attribute).trim();
    if (value.isEmpty()) {
      return 1;
    }
    if (!value.matches("\\d+")) {
      throw new IllegalArgumentException("A table cell's " + attribute + " is not a number: "
          + value);
    }
    return Math.max(1, Integer.parseInt(value));
  }

  private static String text(Element cell) {
    Element clone = cell.clone();
    clone.select("br").append(" ");
    return clone.text().replaceAll("\\s+", " ").trim();
  }
}
