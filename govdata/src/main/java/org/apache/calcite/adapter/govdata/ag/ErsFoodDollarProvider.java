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
package org.apache.calcite.adapter.govdata.ag;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.CompleteBody;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RawCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * DataProvider for the USDA ERS "Food Dollar Series" machine-readable bulk file.
 *
 * <p>The landing page links a zip whose Drupal media id rotates each November release
 * ({@code /media/NNNN/food-dollar-nominal-data-in-machine-readable-format.zip}); this
 * provider scrapes the landing page for that link, reads the single linearized CSV
 * inside, and streams one output row per CSV row. The file carries every year
 * (2007-present), so the table does a single fetch and partitions by each row's
 * {@code year}. The source writes a missing value as the literal {@code NULL}.
 */
public class ErsFoodDollarProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(ErsFoodDollarProvider.class);

  private static final Pattern ZIP_LINK =
      Pattern.compile("href=\"(/media/[0-9]+/food-dollar-nominal-data-in-machine-readable-format\\.zip)",
          Pattern.CASE_INSENSITIVE);

  private static final String HOST = "https://www.ers.usda.gov";
  private static final String DEFAULT_UA = "Mozilla/5.0 (compatible; govdata-etl/1.0)";

  /** CSV header name -> output column. */
  private static final String[][] COLUMNS = {
      {"Year", "year"},
      {"Type", "product_type"},
      {"Channel", "channel"},
      {"Aggregation", "aggregation"},
      {"Account", "account"},
      {"AccountName", "account_name"},
      {"Series", "series"},
      {"Category", "category"},
      {"CategoryName", "category_name"},
      {"ValueType", "value_type"},
      {"ValueTypeDesc", "value_type_desc"},
      {"Unit", "unit"},
      {"Value", "value"},
  };

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String landingUrl = config.getSource() != null ? config.getSource().getUrl() : null;
    if (landingUrl == null || landingUrl.isEmpty()) {
      throw new IOException("ERS Food Dollar: source.url (landing page) is required");
    }
    String userAgent = DEFAULT_UA;
    Map<String, String> headers = config.getSource().getHeaders();
    if (headers != null && headers.get("User-Agent") != null) {
      userAgent = headers.get("User-Agent");
    }

    final String zipUrl = resolveZipUrl(landingUrl, userAgent);
    LOGGER.info("ERS Food Dollar: current release zip resolved to {}", zipUrl);

    final BufferedReader reader = openCsvReader(zipUrl, userAgent, rawCache);
    final int[] index = readHeader(reader, zipUrl);

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> nextRow;
      private boolean done;

      private void advance() {
        if (nextRow != null || done) {
          return;
        }
        try {
          String line;
          while ((line = reader.readLine()) != null) {
            if (line.isEmpty()) {
              continue;
            }
            nextRow = toRow(ErsFarmIncomeProvider.parseCsvLine(line), index);
            return;
          }
          done = true;
          reader.close();
        } catch (IOException e) {
          throw new RuntimeException("ERS Food Dollar: failed streaming CSV from " + zipUrl, e);
        }
      }

      @Override public boolean hasNext() {
        advance();
        return nextRow != null;
      }

      @Override public Map<String, Object> next() {
        advance();
        if (nextRow == null) {
          throw new NoSuchElementException();
        }
        Map<String, Object> row = nextRow;
        nextRow = null;
        return row;
      }
    };
  }

  /** Scrapes the landing page (never cached, so a new release is discovered) for the zip link. */
  private String resolveZipUrl(String landingUrl, String userAgent) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (InputStream in = CompleteBody.of(open(landingUrl, userAgent))) {
      byte[] chunk = new byte[8192];
      int n;
      while ((n = in.read(chunk)) != -1) {
        buf.write(chunk, 0, n);
      }
    }
    Matcher m = ZIP_LINK.matcher(new String(buf.toByteArray(), StandardCharsets.UTF_8));
    if (!m.find()) {
      throw new IOException("ERS Food Dollar: no machine-readable zip link found on " + landingUrl);
    }
    return HOST + m.group(1);
  }

  /** Reads the zip through the raw cache keyed on its own URL: a new release is a new URL. */
  private BufferedReader openCsvReader(String zipUrl, String userAgent, RawCache rawCache)
      throws IOException {
    ZipInputStream zis = new ZipInputStream(
        rawCache.openStream(zipUrl, () -> open(zipUrl, userAgent).getInputStream()));
    ZipEntry entry;
    while ((entry = zis.getNextEntry()) != null) {
      if (entry.getName().toLowerCase().endsWith(".csv")) {
        return new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8));
      }
    }
    zis.close();
    throw new IOException("ERS Food Dollar: no .csv entry inside " + zipUrl);
  }

  private int[] readHeader(BufferedReader reader, String zipUrl) throws IOException {
    String header = reader.readLine();
    if (header == null) {
      throw new IOException("ERS Food Dollar: empty CSV in " + zipUrl);
    }
    if (header.startsWith("﻿")) {
      header = header.substring(1);
    }
    String[] cols = ErsFarmIncomeProvider.parseCsvLine(header);
    Map<String, Integer> pos = new HashMap<String, Integer>();
    for (int i = 0; i < cols.length; i++) {
      pos.put(cols[i].trim(), i);
    }
    int[] index = new int[COLUMNS.length];
    for (int i = 0; i < COLUMNS.length; i++) {
      Integer at = pos.get(COLUMNS[i][0]);
      if (at == null) {
        throw new IOException("ERS Food Dollar: expected column '" + COLUMNS[i][0]
            + "' not found in CSV header of " + zipUrl + " - header=" + header);
      }
      index[i] = at.intValue();
    }
    return index;
  }

  private Map<String, Object> toRow(String[] fields, int[] index) throws IOException {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    for (int i = 0; i < COLUMNS.length; i++) {
      String col = COLUMNS[i][1];
      if (index[i] >= fields.length) {
        throw new IOException("ERS Food Dollar: short row, " + fields.length + " fields: "
            + String.join(",", fields));
      }
      String raw = fields[index[i]].trim();
      if (raw.isEmpty()) {
        raw = null;
      }
      if ("year".equals(col) || "category".equals(col) || "value_type".equals(col)) {
        row.put(col, raw == null ? null : Integer.valueOf(Integer.parseInt(raw)));
      } else if ("value".equals(col)) {
        // The source's own missing-value token is the literal NULL; anything else must parse.
        row.put(col, raw == null || "NULL".equals(raw) ? null : Double.valueOf(Double.parseDouble(raw)));
      } else {
        row.put(col, raw);
      }
    }
    return row;
  }

  private HttpURLConnection open(String url, String userAgent) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestProperty("User-Agent", userAgent);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setInstanceFollowRedirects(true);
    NetworkOutageWait.connect(conn);
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("ERS Food Dollar: HTTP " + code + " for " + url);
    }
    return conn;
  }
}
