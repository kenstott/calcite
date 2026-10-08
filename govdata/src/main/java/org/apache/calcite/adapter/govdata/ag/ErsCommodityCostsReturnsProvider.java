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
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataProvider for USDA ERS "Commodity Costs and Returns" per-commodity CSV files.
 *
 * <p>Unlike {@link ErsFarmIncomeProvider} (one cumulative file for all data), ERS
 * publishes this product as one CSV per commodity (corn, wheat, milk, ...), each
 * carrying that commodity's full 1996-present history by USDA Production Region.
 * The landing page link for each commodity is a Drupal media path whose numeric id
 * and {@code ?v=} cache-busting query rotate at each twice-yearly release, so the
 * fixed URL in {@code source.url} is the landing page; this provider scrapes it
 * once per fetch call for the href matching the requested commodity's filename
 * slug (e.g. {@code cow-calf.csv} for the {@code cow-calf} dimension value).
 *
 * <p>Each fetch is driven by the {@code commodity} dimension (see {@code
 * dimensions.commodity} in ag-schema.yaml) and returns that one commodity's
 * entire file; the materialize partition ({@code type, commodity}) is replaced
 * wholesale each run — same shape as the {@code faostat_production} domain fetch.
 */
public class ErsCommodityCostsReturnsProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(ErsCommodityCostsReturnsProvider.class);

  private static final String HOST = "https://www.ers.usda.gov";
  private static final String DEFAULT_UA = "Mozilla/5.0 (compatible; govdata-etl/1.0)";

  /** Source CSV header -> output column. Numeric coercion handled per-column below. */
  private static final String[][] COLUMNS = {
      {"Commodity", "commodity_name"},
      {"Category", "category"},
      {"Item", "item"},
      {"Units", "units"},
      {"Size", "size"},
      {"Region", "region"},
      {"Country", "country"},
      {"Year", "year"},
      {"Value", "value"},
      {"Survey base year", "survey_base_year"},
  };

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String landingUrl = config.getSource() != null ? config.getSource().getUrl() : null;
    if (landingUrl == null || landingUrl.isEmpty()) {
      throw new IOException("ERS CCR: source.url (landing page) is required");
    }
    String commodity = variables.get("commodity");
    if (commodity == null || commodity.isEmpty()) {
      throw new IOException("ERS CCR: no commodity dimension value supplied");
    }
    String userAgent = headerOrDefault(config, "User-Agent", DEFAULT_UA);

    String csvUrl = resolveCommodityCsvUrl(landingUrl, userAgent, commodity);
    LOGGER.info("ERS CCR: commodity {} -> {}", commodity, csvUrl);

    final BufferedReader reader = openCsvReader(csvUrl, userAgent, rawCache);
    final int[] index = readHeader(reader, csvUrl);
    final String commodityDim = commodity;

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
            String[] fields = parseCsvLine(line);
            nextRow = toRow(fields, index, commodityDim);
            return;
          }
          done = true;
          reader.close();
        } catch (IOException e) {
          throw new RuntimeException("ERS CCR: failed streaming CSV from " + csvUrl, e);
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

  private String headerOrDefault(EtlPipelineConfig config, String name, String dflt) {
    Map<String, String> headers = config.getSource() != null ? config.getSource().getHeaders() : null;
    if (headers != null) {
      String v = headers.get(name);
      if (v != null && !v.isEmpty()) {
        return v;
      }
    }
    return dflt;
  }

  /**
   * Scrapes the landing page and returns the absolute URL of the CSV link whose filename
   * matches {@code <commodity>.csv} (case-insensitive), e.g. {@code cow-calf} ->
   * {@code .../cow-calf.csv?v=...}. The numeric media id and version query are not assumed
   * stable across releases; only the filename slug is matched.
   */
  private String resolveCommodityCsvUrl(String landingUrl, String userAgent, String commodity)
      throws IOException {
    HttpURLConnection conn = open(landingUrl, userAgent);
    String html;
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (InputStream in = conn.getInputStream()) {
      byte[] chunk = new byte[8192];
      int n;
      while ((n = in.read(chunk)) != -1) {
        buf.write(chunk, 0, n);
      }
    }
    html = new String(buf.toByteArray(), StandardCharsets.UTF_8);
    Pattern commodityCsv = Pattern.compile(
        "href=\"([^\"]*/" + Pattern.quote(commodity) + "\\.csv[^\"]*)\"", Pattern.CASE_INSENSITIVE);
    Matcher m = commodityCsv.matcher(html);
    if (!m.find()) {
      throw new IOException("ERS CCR: no " + commodity + ".csv link found on landing page " + landingUrl);
    }
    String href = m.group(1);
    return href.startsWith("http") ? href : HOST + href;
  }

  /** Downloads the CSV through the raw cache, keyed on the resolved commodity URL. */
  private BufferedReader openCsvReader(String csvUrl, String userAgent, RawCache rawCache)
      throws IOException {
    InputStream in = rawCache.openStream(csvUrl, () -> open(csvUrl, userAgent).getInputStream());
    return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
  }

  private int[] readHeader(BufferedReader reader, String csvUrl) throws IOException {
    String header = reader.readLine();
    if (header == null) {
      throw new IOException("ERS CCR: empty CSV in " + csvUrl);
    }
    String[] cols = parseCsvLine(header);
    Map<String, Integer> pos = new HashMap<String, Integer>();
    for (int i = 0; i < cols.length; i++) {
      pos.put(cols[i].trim(), i);
    }
    int[] index = new int[COLUMNS.length];
    for (int i = 0; i < COLUMNS.length; i++) {
      Integer at = pos.get(COLUMNS[i][0]);
      if (at == null) {
        throw new IOException("ERS CCR: expected column '" + COLUMNS[i][0]
            + "' not found in CSV header of " + csvUrl + " — header=" + header);
      }
      index[i] = at.intValue();
    }
    return index;
  }

  private Map<String, Object> toRow(String[] fields, int[] index, String commodityDim) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("commodity", commodityDim);
    for (int i = 0; i < COLUMNS.length; i++) {
      String col = COLUMNS[i][1];
      int at = index[i];
      String raw = at < fields.length ? fields[at] : null;
      if (raw != null) {
        raw = raw.trim();
        if (raw.isEmpty()) {
          raw = null;
        }
      }
      if ("year".equals(col)) {
        row.put(col, raw == null ? null : Integer.valueOf(Integer.parseInt(raw)));
      } else if ("value".equals(col)) {
        row.put(col, raw == null ? null : Double.valueOf(Double.parseDouble(raw)));
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
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("ERS CCR: HTTP " + code + " for " + url);
    }
    return conn;
  }

  /** Minimal RFC4180 line parser: comma-separated, double-quote quoting, "" escape. */
  static String[] parseCsvLine(String line) {
    List<String> out = new ArrayList<String>();
    StringBuilder field = new StringBuilder();
    boolean inQuotes = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
            field.append('"');
            i++;
          } else {
            inQuotes = false;
          }
        } else {
          field.append(c);
        }
      } else if (c == '"') {
        inQuotes = true;
      } else if (c == ',') {
        out.add(field.toString());
        field.setLength(0);
      } else {
        field.append(c);
      }
    }
    out.add(field.toString());
    return out.toArray(new String[0]);
  }
}
