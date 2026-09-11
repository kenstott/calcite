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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataProvider for {@code econ.usgs_mineral_commodity_stats} — USGS Mineral Commodity Summaries
 * (MCS) per-commodity statistics (production, imports, exports, net import reliance, price,
 * reserves, and more), long-format.
 *
 * <p>Each annual MCS release is a ScienceBase catalog item whose id is not derivable from the
 * year (confirmed live 2026-09-11: item ids for 2022-2026 share no pattern). Discovery lists the
 * children of the stable MCS series parent item {@code 5c8c03e4e4b0938824529f7d}
 * (ScienceBase {@code catalog/items?parentId=...}), filters titles matching
 * {@code "Mineral Commodity Summaries <year> Data Release"} (that parent also holds unrelated
 * geospatial-compilation items, which the title filter excludes), then reads each matching
 * item's file list for one named {@code *_Commodities_Data.csv} — the single unified long-format
 * table USGS began publishing with the MCS 2026 release (columns: MCS chapter, Section,
 * Commodity, Country, Statistics, Statistics_detail, Unit, Year, Value, Notes,
 * Is critical mineral 2025, Other notes).
 *
 * <p>Releases before 2026 (confirmed live: 2022-2025) instead ship an 85-file-per-release ZIP of
 * per-commodity wide-format CSVs with no unified file — a release with no
 * {@code *_Commodities_Data.csv} is skipped with a logged reason rather than guessed at; ingesting
 * that older shape is a separate follow-on, not attempted here.
 *
 * <p>No {@code year} dimension: like {@code pesticide_use_by_county}, one run discovers every
 * currently-published release and partitions by each row's own {@code year} (the MCS release
 * year, stamped from the matched item's title) — there is no per-year URL to template. The actual
 * calendar year a statistic describes is carried separately as {@code data_year}
 * (nullable — MCS also reports multi-year aggregate periods like {@code "2021-24"}, preserved
 * verbatim in {@code data_year_text} when not a single 4-digit year).
 *
 * <p>The CSV is Windows-1252 encoded (confirmed live: em-dashes in the Section column decode as
 * {@code 0x97}, not valid UTF-8) — read explicitly as {@code windows-1252}, never the platform
 * default charset.
 */
public class UsgsMineralCommodityStatsProvider implements CachingDataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UsgsMineralCommodityStatsProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Charset MCS_CHARSET = Charset.forName("windows-1252");
  private static final String DEFAULT_UA = "Apache-Calcite-GovData/1.0";

  private static final Pattern RELEASE_TITLE =
      Pattern.compile("Mineral Commodity Summaries (\\d{4}) Data Release",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern SINGLE_YEAR = Pattern.compile("^\\d{4}$");

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String listingUrl = config.getSource() != null ? config.getSource().getUrl() : null;
    if (listingUrl == null || listingUrl.isEmpty()) {
      throw new IOException("USGS MCS: source.url (ScienceBase parent listing) is required");
    }
    String userAgent = headerOrDefault(config, "User-Agent", DEFAULT_UA);

    List<ReleaseFile> releases = resolveReleases(listingUrl, userAgent);
    if (releases.isEmpty()) {
      throw new IOException("USGS MCS: no MCS Data Release items with a unified "
          + "Commodities_Data.csv resolved from " + listingUrl);
    }
    LOGGER.info("USGS MCS: resolved {} release(s) with a unified Commodities_Data.csv",
        releases.size());

    return new MultiReleaseRowIterator(releases, userAgent, rawCache);
  }

  // ---------------------------------------------------------------------
  // Discovery: parent listing -> matching releases -> unified CSV URL
  // ---------------------------------------------------------------------

  private static final class ReleaseFile {
    final int releaseYear;
    final String csvUrl;

    ReleaseFile(int releaseYear, String csvUrl) {
      this.releaseYear = releaseYear;
      this.csvUrl = csvUrl;
    }
  }

  private List<ReleaseFile> resolveReleases(String listingUrl, String userAgent)
      throws IOException {
    JsonNode items = MAPPER.readTree(getText(listingUrl, userAgent)).path("items");
    List<ReleaseFile> releases = new ArrayList<ReleaseFile>();
    if (!items.isArray()) {
      return releases;
    }
    for (JsonNode item : items) {
      String title = item.path("title").asText("");
      Matcher m = RELEASE_TITLE.matcher(title);
      if (!m.find()) {
        continue;
      }
      int releaseYear = Integer.parseInt(m.group(1));
      String itemId = item.path("id").asText(null);
      if (itemId == null || itemId.isEmpty()) {
        continue;
      }
      String csvUrl = findCommoditiesDataCsv(itemId, userAgent);
      if (csvUrl == null) {
        LOGGER.info("USGS MCS {}: no unified Commodities_Data.csv on item {} "
            + "(pre-2026-shape release, not ingested by this provider) — skipping",
            releaseYear, itemId);
        continue;
      }
      releases.add(new ReleaseFile(releaseYear, csvUrl));
    }
    return releases;
  }

  private String findCommoditiesDataCsv(String itemId, String userAgent) throws IOException {
    String apiUrl = "https://www.sciencebase.gov/catalog/item/" + itemId
        + "?format=json&fields=files";
    JsonNode files = MAPPER.readTree(getText(apiUrl, userAgent)).path("files");
    if (!files.isArray()) {
      return null;
    }
    for (JsonNode f : files) {
      String name = f.path("name").asText("");
      if (name.toLowerCase(Locale.ROOT).endsWith("_commodities_data.csv")) {
        String downloadUri = f.path("downloadUri").asText(null);
        if (downloadUri != null) {
          return downloadUri;
        }
      }
    }
    return null;
  }

  // ---------------------------------------------------------------------
  // Row streaming: one release's CSV at a time, one row at a time
  // ---------------------------------------------------------------------

  private static final String[] REQUIRED_HEADERS = {
      "MCS chapter", "Section", "Commodity", "Country", "Statistics", "Statistics_detail",
      "Unit", "Year", "Value", "Notes", "Is critical mineral 2025", "Other notes"
  };

  private static final class MultiReleaseRowIterator implements Iterator<Map<String, Object>> {
    private final List<ReleaseFile> releases;
    private final String userAgent;
    private final RawCache rawCache;
    private int releaseIndex;
    private int currentReleaseYear;
    private BufferedReader reader;
    private Map<String, Integer> headerIndex;
    private Map<String, Object> nextRow;
    private boolean done;

    MultiReleaseRowIterator(List<ReleaseFile> releases, String userAgent, RawCache rawCache) {
      this.releases = releases;
      this.userAgent = userAgent;
      this.rawCache = rawCache;
    }

    private void advance() throws IOException {
      while (nextRow == null && !done) {
        if (reader == null) {
          if (!openNextRelease()) {
            done = true;
            return;
          }
        }
        String record = CsvRecordReader.readRecord(reader);
        if (record == null) {
          reader.close();
          reader = null;
          continue;
        }
        if (record.trim().isEmpty()) {
          continue;
        }
        nextRow = toRow(CsvRecordReader.splitFields(record, ','), headerIndex, currentReleaseYear);
      }
    }

    private boolean openNextRelease() throws IOException {
      if (releaseIndex >= releases.size()) {
        return false;
      }
      ReleaseFile release = releases.get(releaseIndex++);
      currentReleaseYear = release.releaseYear;
      LOGGER.info("USGS MCS {}: streaming {}", currentReleaseYear, release.csvUrl);
      final String csvUrl = release.csvUrl;
      final String ua = userAgent;
      reader = new BufferedReader(new InputStreamReader(
          rawCache.openStream(csvUrl, () -> openWithUserAgent(csvUrl, ua)), MCS_CHARSET));
      String header = CsvRecordReader.readRecord(reader);
      if (header == null) {
        throw new IOException("USGS MCS " + currentReleaseYear + ": empty file " + csvUrl);
      }
      if (header.startsWith("﻿")) {
        header = header.substring(1);
      }
      headerIndex = headerIndex(CsvRecordReader.splitFields(header, ','), csvUrl);
      return true;
    }

    @Override public boolean hasNext() {
      try {
        advance();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return nextRow != null;
    }

    @Override public Map<String, Object> next() {
      try {
        advance();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (nextRow == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = nextRow;
      nextRow = null;
      return row;
    }
  }

  private static Map<String, Integer> headerIndex(List<String> cols, String url)
      throws IOException {
    Map<String, Integer> pos = new LinkedHashMap<String, Integer>();
    for (int i = 0; i < cols.size(); i++) {
      pos.put(cols.get(i).trim().toLowerCase(Locale.ROOT), Integer.valueOf(i));
    }
    for (String required : REQUIRED_HEADERS) {
      if (!pos.containsKey(required.toLowerCase(Locale.ROOT))) {
        throw new IOException("USGS MCS: expected column '" + required + "' not found in "
            + url + " — header=" + cols);
      }
    }
    return pos;
  }

  private static Map<String, Object> toRow(List<String> fields, Map<String, Integer> headerIndex,
      int releaseYear) {
    String yearText = field(fields, headerIndex, "year");
    String valueText = field(fields, headerIndex, "value");

    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("mcs_chapter", field(fields, headerIndex, "mcs chapter"));
    row.put("section", field(fields, headerIndex, "section"));
    row.put("commodity", field(fields, headerIndex, "commodity"));
    row.put("country", field(fields, headerIndex, "country"));
    row.put("statistic", field(fields, headerIndex, "statistics"));
    row.put("statistic_detail", field(fields, headerIndex, "statistics_detail"));
    row.put("unit", field(fields, headerIndex, "unit"));
    row.put("data_year", parseSingleYear(yearText));
    row.put("data_year_text", yearText);
    row.put("value_num", parseValue(valueText));
    row.put("value_text", valueText);
    row.put("notes", field(fields, headerIndex, "notes"));
    row.put("is_critical_mineral_2025", parseYesNo(field(fields, headerIndex,
        "is critical mineral 2025")));
    row.put("other_notes", field(fields, headerIndex, "other notes"));
    row.put("year", Integer.valueOf(releaseYear));
    return row;
  }

  private static String field(List<String> fields, Map<String, Integer> headerIndex,
      String name) {
    Integer at = headerIndex.get(name.toLowerCase(Locale.ROOT));
    if (at == null || at.intValue() >= fields.size()) {
      return null;
    }
    String raw = fields.get(at.intValue());
    if (raw == null) {
      return null;
    }
    String trimmed = raw.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private static Integer parseSingleYear(String yearText) {
    if (yearText == null || !SINGLE_YEAR.matcher(yearText).matches()) {
      return null;
    }
    return Integer.valueOf(yearText);
  }

  private static Double parseValue(String valueText) {
    if (valueText == null) {
      return null;
    }
    String cleaned = valueText.replace(",", "").trim();
    try {
      return Double.valueOf(Double.parseDouble(cleaned));
    } catch (NumberFormatException e) {
      // Non-numeric annotations (e.g. "W" withheld, "NA") are preserved in value_text,
      // not fabricated as a number here.
      return null;
    }
  }

  private static Boolean parseYesNo(String v) {
    if (v == null) {
      return null;
    }
    if ("yes".equalsIgnoreCase(v)) {
      return Boolean.TRUE;
    }
    if ("no".equalsIgnoreCase(v)) {
      return Boolean.FALSE;
    }
    return null;
  }

  // ---------------------------------------------------------------------
  // HTTP helpers
  // ---------------------------------------------------------------------

  private static InputStream openWithUserAgent(String url, String userAgent) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestProperty("User-Agent", userAgent);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setInstanceFollowRedirects(true);
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("USGS MCS: HTTP " + code + " for " + url);
    }
    return conn.getInputStream();
  }

  private static String headerOrDefault(EtlPipelineConfig config, String name, String dflt) {
    Map<String, String> headers = config.getSource() != null
        ? config.getSource().getHeaders() : null;
    if (headers != null) {
      String v = headers.get(name);
      if (v != null && !v.isEmpty()) {
        return v;
      }
    }
    return dflt;
  }

  private static String getText(String url, String userAgent) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestProperty("User-Agent", userAgent);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);
    conn.setInstanceFollowRedirects(true);
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("USGS MCS: HTTP " + code + " for " + url);
    }
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (InputStream in = conn.getInputStream()) {
      byte[] chunk = new byte[65536];
      int n;
      while ((n = in.read(chunk)) != -1) {
        buf.write(chunk, 0, n);
      }
    }
    return new String(buf.toByteArray(), StandardCharsets.UTF_8);
  }
}
