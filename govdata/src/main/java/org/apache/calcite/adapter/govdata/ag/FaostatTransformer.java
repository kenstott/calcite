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

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.RetryableHttp;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Streaming transformer for {@code ag.faostat_production} (UN FAO FAOSTAT).
 *
 * <p>Each fan-out request pulls one FAOSTAT domain's normalized bulk file
 * ({@code .../production/{domain}_All_Data_(Normalized).zip}) — a ZIP whose main entry is a CSV
 * carrying all countries and years for that domain (alongside small Flags/Symbols sidecars). This
 * class implements {@link StreamingResponseTransformer}: {@code HttpSource} calls
 * {@link #fetchAndTransform} directly, which opens the URL, positions the {@link ZipInputStream} on
 * the normalized data entry, and streams one CSV record at a time via {@link CsvRecordReader} —
 * heap stays O(1) per row.
 *
 * <p>Rows map to the clean {@code faostat_production} columns. {@code Area Code (M49)} is emitted as
 * {@code country_code}, normalized to the unpadded numeric form that matches
 * {@code ref.countries.iso_numeric} (e.g. {@code '004} → {@code 4}); FAO regional/economic-group
 * areas ({@code Area Code >= 5000}) are flagged {@code is_aggregate}. Each row carries its own
 * {@code year} (from {@code Year}) so the writer can partition on {@code [type, domain, year]}.
 *
 * <p>NOTE: FAOSTAT normalized column names and domain codes follow the documented layout; verify
 * against a live download before relying on this table.
 */
public class FaostatTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FaostatTransformer.class);

  private static final int AGGREGATE_AREA_CODE_FLOOR = 5000;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    if (url == null || url.isEmpty()) {
      throw new IllegalStateException("FaostatTransformer: no URL in context");
    }
    String domain = context.getDimensionValues().get("domain");
    LOGGER.info("faostat[{}]: streaming {}", domain, url);

    Map<String, String> reqProps = new LinkedHashMap<String, String>();
    reqProps.put("User-Agent", "Mozilla/5.0 (compatible; govdata-etl/1.0)");
    if (context.getHeaders() != null) {
      reqProps.putAll(context.getHeaders());
    }
    HttpURLConnection conn;
    try {
      // Shared retryable open (Retry-After + backoff + declared rateLimit), following redirects.
      conn = RetryableHttp.openWithRetry(url, reqProps, context.getRateLimit(), true);
    } catch (RetryableHttp.HttpStatusException e) {
      if (e.getStatus() == HttpURLConnection.HTTP_NOT_FOUND) {
        LOGGER.warn("faostat[{}]: HTTP 404 (no bulk file) for {}", domain, url);
        return Collections.<Map<String, Object>>emptyList().iterator();
      }
      throw e;
    }

    ZipInputStream zis = new ZipInputStream(conn.getInputStream());
    if (!positionOnDataEntry(zis, domain)) {
      zis.close();
      conn.disconnect();
      throw new IOException("faostat[" + domain + "]: no normalized data entry in " + url);
    }
    BufferedReader reader = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8));
    String header = CsvRecordReader.readRecord(reader);
    if (header == null) {
      reader.close();
      conn.disconnect();
      throw new IOException("faostat[" + domain + "]: empty data entry in " + url);
    }
    Map<String, Integer> idx = headerIndex(CsvRecordReader.splitFields(header, ','));
    return new FaoRowIterator(reader, conn, idx, domain, url);
  }

  /**
   * Advances the ZIP to the normalized data entry (the main CSV, not the Flags/Symbols sidecars).
   * Prefers a {@code *(Normalized).csv} entry; falls back to the first {@code .csv} that is not a
   * flag/symbol/note sidecar.
   */
  private static boolean positionOnDataEntry(ZipInputStream zis, String domain) throws IOException {
    ZipEntry entry;
    while ((entry = zis.getNextEntry()) != null) {
      String name = entry.getName().toLowerCase();
      if (!name.endsWith(".csv")) {
        continue;
      }
      if (name.contains("normalized")) {
        return true;
      }
      if (!name.contains("flag") && !name.contains("symbol") && !name.contains("note")
          && !name.contains("areacode") && !name.contains("itemcode")) {
        return true;
      }
    }
    return false;
  }

  private static Map<String, Integer> headerIndex(List<String> header) {
    Map<String, Integer> idx = new HashMap<String, Integer>();
    for (int i = 0; i < header.size(); i++) {
      // Strip a UTF-8 BOM and surrounding quotes FAO sometimes prepends to the first header cell.
      String key = header.get(i).replace("﻿", "").trim();
      idx.put(key, i);
    }
    return idx;
  }

  /** Lazy row iterator over the normalized CSV inside the ZIP; closes on exhaustion. */
  private static final class FaoRowIterator implements Iterator<Map<String, Object>> {
    private final BufferedReader reader;
    private final HttpURLConnection conn;
    private final Map<String, Integer> idx;
    private final String domain;
    private final String url;
    private Map<String, Object> mapped;
    private boolean done;

    FaoRowIterator(BufferedReader reader, HttpURLConnection conn, Map<String, Integer> idx,
        String domain, String url) {
      this.reader = reader;
      this.conn = conn;
      this.idx = idx;
      this.domain = domain;
      this.url = url;
      advance();
    }

    private void advance() {
      try {
        String record = CsvRecordReader.readRecord(reader);
        while (record != null) {
          Map<String, Object> row = mapRow(CsvRecordReader.splitFields(record, ','));
          if (row != null) {
            mapped = row;
            return;
          }
          record = CsvRecordReader.readRecord(reader);
        }
        mapped = null;
        close();
      } catch (IOException e) {
        close();
        throw new RuntimeException("faostat[" + domain + "]: read failed", e);
      }
    }

    private Map<String, Object> mapRow(List<String> cols) {
      Integer year = year(cols);
      if (year == null) {
        return null; // no parseable year → cannot partition; skip
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("country_code", m49(pick(cols, "Area Code (M49)", "Area Code (M49) ")));
      row.put("area_name", pick(cols, "Area"));
      row.put("item_code", pick(cols, "Item Code"));
      row.put("item_name", pick(cols, "Item"));
      row.put("element_code", pick(cols, "Element Code"));
      row.put("element_name", pick(cols, "Element"));
      row.put("year", year);
      row.put("value", dbl(pick(cols, "Value")));
      row.put("unit", pick(cols, "Unit"));
      row.put("flag", pick(cols, "Flag"));
      Integer areaCode = intOrNull(pick(cols, "Area Code"));
      row.put("is_aggregate", areaCode != null && areaCode >= AGGREGATE_AREA_CODE_FLOOR);
      return row;
    }

    /** Normalizes a FAO M49 cell (e.g. {@code '004}) to the unpadded numeric string {@code 4}. */
    private static String m49(String raw) {
      if (raw == null) {
        return null;
      }
      String digits = raw.replaceAll("[^0-9]", "");
      if (digits.isEmpty()) {
        return null;
      }
      try {
        return String.valueOf(Integer.parseInt(digits));
      } catch (NumberFormatException e) {
        return null;
      }
    }

    private Integer year(List<String> cols) {
      String y = pick(cols, "Year");
      if (y == null || y.length() < 4) {
        return null;
      }
      try {
        return Integer.parseInt(y.substring(0, 4));
      } catch (NumberFormatException e) {
        return null;
      }
    }

    private String pick(List<String> cols, String... names) {
      for (String name : names) {
        Integer i = idx.get(name);
        if (i != null && i < cols.size()) {
          String v = cols.get(i).trim();
          if (!v.isEmpty()) {
            return v;
          }
        }
      }
      return null;
    }

    private static Integer intOrNull(String v) {
      if (v == null) {
        return null;
      }
      try {
        return Integer.parseInt(v.trim());
      } catch (NumberFormatException e) {
        return null;
      }
    }

    private static Double dbl(String v) {
      if (v == null) {
        return null;
      }
      try {
        return Double.parseDouble(v);
      } catch (NumberFormatException e) {
        return null;
      }
    }

    private void close() {
      if (done) {
        return;
      }
      done = true;
      try {
        reader.close();
      } catch (IOException ignored) {
        // best-effort
      }
      conn.disconnect();
    }

    @Override public boolean hasNext() {
      return mapped != null;
    }

    @Override public Map<String, Object> next() {
      if (mapped == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = mapped;
      advance();
      return row;
    }
  }
}
