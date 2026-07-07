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

import org.apache.calcite.adapter.file.etl.DataProvider;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * DataProvider for the USDA ERS "Farm Income and Wealth Statistics" bulk CSV.
 *
 * <p>The release URL rotates each publication (a Drupal media id + human-dated
 * filename), so the fixed URL in {@code source.url} is the <em>landing page</em>;
 * this provider scrapes it for the newest {@code /media/.../<...>release.zip}
 * link, downloads that zip, and streams its single CSV. The CSV is <b>latin-1</b>
 * (windows-1252) — the native UTF-8 CSV path would mis-decode it, so a
 * {@link DataProvider} owning the decode is required.
 *
 * <p>One cumulative file carries all years (1910–present), so the ag table does a
 * single fetch (dimension {@code type} only) and partitions by each row's
 * {@code year}. Rows are produced lazily (O(1) memory over the ~460k-row file).
 */
public class ErsFarmIncomeProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(ErsFarmIncomeProvider.class);

  /** First /media/.../*release.zip on the landing page is the current release. */
  private static final Pattern RELEASE_LINK =
      Pattern.compile("href=\"(/media/[0-9]+/[^\"]*release\\.zip)\"", Pattern.CASE_INSENSITIVE);

  private static final String HOST = "https://www.ers.usda.gov";
  private static final String DEFAULT_UA = "Mozilla/5.0 (compatible; govdata-etl/1.0)";

  /** Header name -> output column. Numeric coercion handled per-column below. */
  private static final String[][] COLUMNS = {
      {"Year", "year"},
      {"State", "state"},
      {"artificialKey", "artificial_key"},
      {"VariableDescriptionTotal", "variable_description_total"},
      {"VariableDescriptionPart1", "category"},
      {"VariableDescriptionPart2", "subcategory"},
      {"Amount", "amount"},
      {"unit_desc", "unit_desc"},
      {"PublicationDate", "publication_date"},
      {"Source", "source"},
      {"ChainType_GDP_Deflator", "gdp_deflator"},
  };

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String landingUrl = config.getSource() != null ? config.getSource().getUrl() : null;
    if (landingUrl == null || landingUrl.isEmpty()) {
      throw new IOException("ERS: source.url (landing page) is required");
    }
    String userAgent = headerOrDefault(config, "User-Agent", DEFAULT_UA);

    String zipUrl = resolveReleaseZipUrl(landingUrl, userAgent);
    LOGGER.info("ERS: current release zip resolved to {}", zipUrl);

    final BufferedReader reader = openCsvReader(zipUrl, userAgent);
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
            String[] fields = parseCsvLine(line);
            nextRow = toRow(fields, index);
            return;
          }
          done = true;
          reader.close();
        } catch (IOException e) {
          throw new RuntimeException("ERS: failed streaming CSV from " + zipUrl, e);
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

  /** Scrapes the landing page HTML and returns the absolute URL of the newest release zip. */
  private String resolveReleaseZipUrl(String landingUrl, String userAgent) throws IOException {
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
    Matcher m = RELEASE_LINK.matcher(html);
    if (!m.find()) {
      throw new IOException("ERS: no /media/*release.zip link found on landing page " + landingUrl);
    }
    String href = m.group(1);
    return href.startsWith("http") ? href : HOST + href;
  }

  /** Downloads the zip and returns a latin-1 reader over its first .csv entry. */
  private BufferedReader openCsvReader(String zipUrl, String userAgent) throws IOException {
    HttpURLConnection conn = open(zipUrl, userAgent);
    ZipInputStream zis = new ZipInputStream(conn.getInputStream());
    ZipEntry entry;
    while ((entry = zis.getNextEntry()) != null) {
      if (entry.getName().toLowerCase().endsWith(".csv")) {
        return new BufferedReader(new InputStreamReader(zis, StandardCharsets.ISO_8859_1));
      }
    }
    zis.close();
    throw new IOException("ERS: no .csv entry inside " + zipUrl);
  }

  private int[] readHeader(BufferedReader reader, String zipUrl) throws IOException {
    String header = reader.readLine();
    if (header == null) {
      throw new IOException("ERS: empty CSV in " + zipUrl);
    }
    String[] cols = parseCsvLine(header);
    Map<String, Integer> pos = new java.util.HashMap<String, Integer>();
    for (int i = 0; i < cols.length; i++) {
      pos.put(cols[i].trim(), i);
    }
    int[] index = new int[COLUMNS.length];
    for (int i = 0; i < COLUMNS.length; i++) {
      Integer at = pos.get(COLUMNS[i][0]);
      if (at == null) {
        throw new IOException("ERS: expected column '" + COLUMNS[i][0]
            + "' not found in CSV header of " + zipUrl + " — header=" + header);
      }
      index[i] = at.intValue();
    }
    return index;
  }

  private Map<String, Object> toRow(String[] fields, int[] index) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
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
      } else if ("amount".equals(col) || "gdp_deflator".equals(col)) {
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
      throw new IOException("ERS: HTTP " + code + " for " + url);
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
