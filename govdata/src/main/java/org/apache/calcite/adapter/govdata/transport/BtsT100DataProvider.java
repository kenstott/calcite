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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * DataProvider for {@code t100_segments} (BTS T-100 Domestic Segment, All Carriers).
 *
 * <p>T-100 has no stable direct-download URL; the TranStats download is an
 * ASP.NET postback to {@code DL_SelectFields.aspx?gnoyr_VQ=GEE}. This provider
 * reproduces the two-step handshake verified live 2026-07-13:
 * <ol>
 *   <li>GET the field-selection page to obtain the session cookies
 *       ({@code ASP.NET_SessionId} + F5 cookies) and the page-specific
 *       {@code __VIEWSTATE} / {@code __VIEWSTATEGENERATOR} / {@code __EVENTVALIDATION}
 *       hidden fields.</li>
 *   <li>POST those back with the selected-field checkboxes and
 *       {@code cboYear=<effective_year>}, {@code cboPeriod=All},
 *       {@code cboGeography=All}, {@code chkDownloadZip=on} — the response body is
 *       the ZIP directly.</li>
 * </ol>
 * The ZIP holds {@code T_T100D_SEGMENT_ALL_CARRIER.csv}, parsed row-by-row into
 * one record per (carrier, origin, dest, aircraft, month). The {@code year}
 * partition comes from the {@code effective_year} dimension, so it is not emitted.
 *
 * <p>The base URL ({@code gnoyr_VQ=GEE}) is read from the table's {@code source.url}
 * so the table id stays declared in the model.
 */
public class BtsT100DataProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(BtsT100DataProvider.class);

  private static final String DEFAULT_URL =
      "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=GEE&QO_fu146_anzr=Nv4%20Pn44vr45";
  private static final String USER_AGENT =
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) GovData/1.0";

  /** T-100 fields requested (each posted as {@code NAME=on}); CSV columns returned mirror these. */
  private static final String[] FIELDS = {
      "DEPARTURES_SCHEDULED", "DEPARTURES_PERFORMED", "SEATS", "PASSENGERS", "FREIGHT", "MAIL",
      "DISTANCE", "UNIQUE_CARRIER", "UNIQUE_CARRIER_NAME", "ORIGIN", "ORIGIN_CITY_NAME",
      "ORIGIN_STATE_ABR", "DEST", "DEST_CITY_NAME", "DEST_STATE_ABR", "AIRCRAFT_TYPE",
      "YEAR", "MONTH", "CLASS"
  };

  /** Source CSV header -> emitted column name. */
  private static final Map<String, String> COLUMN_MAP = buildColumnMap();
  /** Emitted columns that are numeric (parsed to Double). */
  private static final Map<String, Boolean> NUMERIC = buildNumeric();

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.isEmpty()) {
      LOGGER.warn("t100_segments: no year in dimension variables {}", variables);
      return Collections.emptyIterator();
    }

    String baseUrl = DEFAULT_URL;
    if (config.getSource() != null && config.getSource().getUrl() != null
        && !config.getSource().getUrl().isEmpty()) {
      baseUrl = config.getSource().getUrl();
    }

    // Step 1 — GET the form; capture cookies + hidden fields.
    GetResult form = httpGet(baseUrl);
    String viewState = scrape(form.body, "__VIEWSTATE");
    String viewStateGen = scrape(form.body, "__VIEWSTATEGENERATOR");
    String eventValidation = scrape(form.body, "__EVENTVALIDATION");
    if (viewState == null || eventValidation == null) {
      throw new IOException("t100_segments: could not scrape ASP.NET hidden fields from "
          + baseUrl + " (page-specific __VIEWSTATE/__EVENTVALIDATION missing)");
    }

    // Step 2 — POST the postback; body is the ZIP.
    String body = buildPostBody(viewState, viewStateGen, eventValidation, year);
    byte[] zip = httpPostForZip(baseUrl, form.cookies, body);

    List<Map<String, Object>> rows = parseZip(zip, year);
    LOGGER.info("t100_segments: {} segment rows for year {}", rows.size(), year);
    return rows.iterator();
  }

  private String buildPostBody(String viewState, String viewStateGen, String eventValidation,
      String year) throws IOException {
    StringBuilder sb = new StringBuilder();
    add(sb, "__EVENTTARGET", "");
    add(sb, "__EVENTARGUMENT", "");
    add(sb, "__VIEWSTATE", viewState);
    if (viewStateGen != null) {
      add(sb, "__VIEWSTATEGENERATOR", viewStateGen);
    }
    add(sb, "__EVENTVALIDATION", eventValidation);
    add(sb, "affiliate", "dot-bts");
    for (String field : FIELDS) {
      add(sb, field, "on");
    }
    add(sb, "cboGeography", "All");
    add(sb, "cboYear", year);
    add(sb, "cboPeriod", "All");
    add(sb, "chkDownloadZip", "on");
    add(sb, "btnDownload", "Download");
    return sb.toString();
  }

  private static void add(StringBuilder sb, String name, String value) throws IOException {
    if (sb.length() > 0) {
      sb.append('&');
    }
    sb.append(URLEncoder.encode(name, "UTF-8")).append('=')
        .append(URLEncoder.encode(value, "UTF-8"));
  }

  private GetResult httpGet(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", USER_AGENT);
    conn.setRequestProperty("Accept", "text/html,application/xhtml+xml,*/*");
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("t100_segments: HTTP " + status + " on GET " + url);
    }
    StringBuilder cookies = new StringBuilder();
    List<String> setCookies = conn.getHeaderFields().get("Set-Cookie");
    if (setCookies != null) {
      for (String sc : setCookies) {
        int semi = sc.indexOf(';');
        String pair = semi > 0 ? sc.substring(0, semi) : sc;
        if (cookies.length() > 0) {
          cookies.append("; ");
        }
        cookies.append(pair);
      }
    }
    String body;
    try (InputStream in = conn.getInputStream()) {
      body = readAll(in);
    }
    return new GetResult(body, cookies.toString());
  }

  private byte[] httpPostForZip(String url, String cookies, String body) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setRequestProperty("User-Agent", USER_AGENT);
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    conn.setRequestProperty("Referer", url);
    if (cookies != null && !cookies.isEmpty()) {
      conn.setRequestProperty("Cookie", cookies);
    }
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    conn.setRequestProperty("Content-Length", String.valueOf(bytes.length));
    try (OutputStream out = conn.getOutputStream()) {
      out.write(bytes);
      out.flush();
    }
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("t100_segments: HTTP " + status + " on POST " + url);
    }
    String contentType = conn.getContentType();
    try (InputStream in = conn.getInputStream()) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[65536];
      int n;
      while ((n = in.read(buf)) > 0) {
        baos.write(buf, 0, n);
      }
      byte[] out = baos.toByteArray();
      // A missing/expired session returns the DOT homepage HTML rather than a zip.
      if (contentType != null && contentType.contains("text/html")) {
        throw new IOException("t100_segments: expected application/zip but got HTML "
            + "(session/viewstate handshake failed)");
      }
      return out;
    }
  }

  private List<Map<String, Object>> parseZip(byte[] zip, String year) throws IOException {
    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zip))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        if (entry.getName().toLowerCase().endsWith(".csv")) {
          return parseCsv(zis);
        }
      }
    }
    throw new IOException("t100_segments: no CSV entry in the downloaded zip for year " + year);
  }

  private List<Map<String, Object>> parseCsv(InputStream in) throws IOException {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    String headerRecord = CsvRecordReader.readRecord(reader);
    if (headerRecord == null) {
      return rows;
    }
    List<String> headers = CsvRecordReader.splitFields(headerRecord, ',');
    String record;
    while ((record = CsvRecordReader.readRecord(reader)) != null) {
      List<String> cols = CsvRecordReader.splitFields(record, ',');
      if (cols.isEmpty()) {
        continue;
      }
      Map<String, Object> row = new HashMap<String, Object>();
      for (int i = 0; i < headers.size() && i < cols.size(); i++) {
        String outCol = COLUMN_MAP.get(headers.get(i).trim());
        if (outCol == null) {
          continue;
        }
        String raw = cols.get(i);
        if (raw != null) {
          raw = raw.trim();
        }
        if (raw == null || raw.isEmpty()) {
          row.put(outCol, null);
        } else if (Boolean.TRUE.equals(NUMERIC.get(outCol))) {
          row.put(outCol, parseDouble(raw));
        } else {
          row.put(outCol, raw);
        }
      }
      rows.add(row);
    }
    return rows;
  }

  private static Double parseDouble(String s) {
    try {
      return Double.valueOf(s);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String readAll(InputStream in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[65536];
    int n;
    while ((n = in.read(buf)) > 0) {
      baos.write(buf, 0, n);
    }
    return new String(baos.toByteArray(), StandardCharsets.UTF_8);
  }

  private static String scrape(String html, String field) {
    Pattern p = Pattern.compile(
        "id=\"" + Pattern.quote(field) + "\"[^>]*value=\"([^\"]*)\"");
    Matcher m = p.matcher(html);
    if (m.find()) {
      return m.group(1);
    }
    // Attribute order can vary; try name-anchored as a fallback.
    p = Pattern.compile("name=\"" + Pattern.quote(field) + "\"[^>]*value=\"([^\"]*)\"");
    m = p.matcher(html);
    return m.find() ? m.group(1) : null;
  }

  private static Map<String, String> buildColumnMap() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("DEPARTURES_SCHEDULED", "departures_scheduled");
    m.put("DEPARTURES_PERFORMED", "departures_performed");
    m.put("SEATS", "seats");
    m.put("PASSENGERS", "passengers");
    m.put("FREIGHT", "freight");
    m.put("MAIL", "mail");
    m.put("DISTANCE", "distance");
    m.put("UNIQUE_CARRIER", "carrier");
    m.put("UNIQUE_CARRIER_NAME", "carrier_name");
    m.put("ORIGIN", "origin");
    m.put("ORIGIN_CITY_NAME", "origin_city_name");
    m.put("ORIGIN_STATE_ABR", "origin_state");
    m.put("DEST", "dest");
    m.put("DEST_CITY_NAME", "dest_city_name");
    m.put("DEST_STATE_ABR", "dest_state");
    m.put("AIRCRAFT_TYPE", "aircraft_type");
    m.put("MONTH", "month");
    m.put("CLASS", "service_class");
    return Collections.unmodifiableMap(m);
  }

  private static Map<String, Boolean> buildNumeric() {
    Map<String, Boolean> m = new HashMap<String, Boolean>();
    m.put("departures_scheduled", Boolean.TRUE);
    m.put("departures_performed", Boolean.TRUE);
    m.put("seats", Boolean.TRUE);
    m.put("passengers", Boolean.TRUE);
    m.put("freight", Boolean.TRUE);
    m.put("mail", Boolean.TRUE);
    m.put("distance", Boolean.TRUE);
    m.put("month", Boolean.TRUE);
    return Collections.unmodifiableMap(m);
  }

  /** GET response: HTML body + collapsed cookie header. */
  private static final class GetResult {
    final String body;
    final String cookies;

    GetResult(String body, String cookies) {
      this.body = body;
      this.cookies = cookies;
    }
  }
}
