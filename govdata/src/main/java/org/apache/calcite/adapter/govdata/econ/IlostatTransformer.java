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

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.RetryableHttp;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.zip.GZIPInputStream;

/**
 * Streaming transformer for {@code econ.ilostat_indicators} (UN ILO ILOSTAT labor indicators).
 *
 * <p>Each fan-out request pulls one indicator's full-history bulk file
 * ({@code .../WEB_bulk_download/indicator/{indicator}.csv.gz}) — a gzip CSV carrying all countries
 * and years. This class implements {@link StreamingResponseTransformer}: {@code HttpSource} calls
 * {@link #fetchAndTransform} directly, which opens the URL, gunzips, and streams one CSV record at
 * a time via {@link CsvRecordReader} (RFC4180-correct), keeping heap O(1) per row.
 *
 * <p>Rows are mapped to the clean {@code ilostat_indicators} columns. {@code ref_area} is ISO
 * alpha-3 for countries ({@code country_code}, the {@code ref.countries} FK); ILO regional/
 * income-group areas (codes containing a digit, e.g. {@code X01}) are flagged {@code is_aggregate}.
 * Each row carries its own {@code year} (from ILO {@code time}) so the writer can partition on
 * {@code [type, indicator, year]}. The indicator label is resolved from the bundled catalog
 * {@code /ilo/ilostat-indicators.json}.
 *
 * <p>NOTE: ILOSTAT bulk column names and indicator ids follow the documented layout; verify
 * against a live download before relying on this table.
 */
public class IlostatTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(IlostatTransformer.class);

  /** code -> label, loaded once from the bundled fan-out catalog. */
  private static volatile Map<String, String> indicatorNames;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    if (url == null || url.isEmpty()) {
      throw new IllegalStateException("IlostatTransformer: no URL in context");
    }
    String indicator = context.getDimensionValues().get("indicator");
    if (indicator == null || indicator.isEmpty()) {
      indicator = indicatorFromUrl(url);
    }
    LOGGER.info("ilostat[{}]: streaming {}", indicator, url);

    HttpURLConnection conn;
    try {
      // Shared retryable open (Retry-After + backoff + declared rateLimit); default UA is GovData/1.0.
      conn = RetryableHttp.openWithRetry(url, null, context.getRateLimit(), false);
    } catch (RetryableHttp.HttpStatusException e) {
      if (e.getStatus() == HttpURLConnection.HTTP_NOT_FOUND) {
        // No bulk file for this indicator id — honest absence, not a fabricated row.
        LOGGER.warn("ilostat[{}]: HTTP 404 (no bulk file) for {}", indicator, url);
        return Collections.<Map<String, Object>>emptyList().iterator();
      }
      throw e;
    }
    InputStream raw = conn.getInputStream();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(raw), StandardCharsets.UTF_8));

    String header = CsvRecordReader.readRecord(reader);
    if (header == null) {
      reader.close();
      conn.disconnect();
      throw new IOException("ilostat[" + indicator + "]: empty file " + url);
    }
    Map<String, Integer> idx = headerIndex(CsvRecordReader.splitFields(header, ','));
    return new IloRowIterator(reader, conn, idx, indicator,
        indicatorLabel(indicator), url);
  }

  private static String indicatorFromUrl(String url) {
    String s = url;
    int slash = s.lastIndexOf('/');
    if (slash >= 0) {
      s = s.substring(slash + 1);
    }
    int dot = s.indexOf('.');
    return dot >= 0 ? s.substring(0, dot) : s;
  }

  private static Map<String, Integer> headerIndex(List<String> header) {
    Map<String, Integer> idx = new HashMap<String, Integer>();
    for (int i = 0; i < header.size(); i++) {
      idx.put(header.get(i).trim(), i);
    }
    return idx;
  }

  private static String indicatorLabel(String code) {
    Map<String, String> names = indicatorNames;
    if (names == null) {
      names = loadIndicatorNames();
      indicatorNames = names;
    }
    return names.get(code);
  }

  private static Map<String, String> loadIndicatorNames() {
    Map<String, String> names = new HashMap<String, String>();
    try (InputStream in =
        IlostatTransformer.class.getResourceAsStream("/ilo/ilostat-indicators.json")) {
      if (in == null) {
        return names;
      }
      JsonNode root = new ObjectMapper().readTree(in);
      JsonNode arr = root.path("indicators");
      for (JsonNode n : arr) {
        String code = n.path("code").asText(null);
        String name = n.path("name").asText(null);
        if (code != null) {
          names.put(code, name);
        }
      }
    } catch (IOException e) {
      LOGGER.warn("ilostat: could not load indicator catalog: {}", e.getMessage());
    }
    return names;
  }

  /** Lazy row iterator over the gzip CSV; closes the stream on exhaustion. */
  private static final class IloRowIterator implements Iterator<Map<String, Object>> {
    private final BufferedReader reader;
    private final HttpURLConnection conn;
    private final Map<String, Integer> idx;
    private final String indicator;
    private final String indicatorName;
    private final String url;
    private Map<String, Object> mapped;
    private boolean done;

    IloRowIterator(BufferedReader reader, HttpURLConnection conn, Map<String, Integer> idx,
        String indicator, String indicatorName, String url) {
      this.reader = reader;
      this.conn = conn;
      this.idx = idx;
      this.indicator = indicator;
      this.indicatorName = indicatorName;
      this.url = url;
      advance();
    }

    private void advance() {
      try {
        String record = CsvRecordReader.readRecord(reader);
        while (record != null) {
          List<String> cols = CsvRecordReader.splitFields(record, ',');
          Map<String, Object> row = mapRow(cols);
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
        throw new RuntimeException("ilostat[" + indicator + "]: read failed", e);
      }
    }

    private Map<String, Object> mapRow(List<String> cols) {
      Integer yi = year(cols);
      if (yi == null) {
        return null; // no parseable year → cannot partition; skip
      }
      String refArea = pick(cols, "ref_area", "REF_AREA");
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("indicator_code", indicator);
      row.put("indicator_name", indicatorName);
      row.put("country_code", refArea);
      row.put("sex", pick(cols, "sex.label", "sex", "SEX"));
      row.put("classif1", pick(cols, "classif1.label", "classif1", "CLASSIF1"));
      row.put("classif2", pick(cols, "classif2.label", "classif2", "CLASSIF2"));
      row.put("year", yi);
      row.put("value", dbl(pick(cols, "obs_value", "OBS_VALUE")));
      row.put("unit", null);
      row.put("obs_status", pick(cols, "obs_status", "OBS_STATUS"));
      row.put("is_aggregate", refArea == null || !refArea.matches("[A-Za-z]{3}"));
      return row;
    }

    private Integer year(List<String> cols) {
      String time = pick(cols, "time", "TIME");
      if (time == null || time.length() < 4) {
        return null;
      }
      try {
        return Integer.parseInt(time.substring(0, 4));
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
