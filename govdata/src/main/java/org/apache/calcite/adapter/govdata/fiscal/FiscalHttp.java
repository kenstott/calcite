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
package org.apache.calcite.adapter.govdata.fiscal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * Shared HTTP fetch and value-coercion helpers for the {@code fiscal} schema
 * DataProviders. All fiscal sources are free/keyless bulk files or aggregate
 * APIs; these helpers centralize the connection setup (browser-like User-Agent,
 * generous timeouts, redirect following, fail-loud on non-2xx) and the numeric
 * parsing that every provider repeats.
 */
final class FiscalHttp {

  /** IRS/SBA hosts serve non-browser clients erratically; present a browser UA. */
  static final String USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) govdata-etl/1.0";

  private FiscalHttp() {
  }

  /** Opens a GET connection with browser headers; throws on any non-2xx status. */
  static HttpURLConnection openGet(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setInstanceFollowRedirects(true);
    conn.setRequestProperty("User-Agent", USER_AGENT);
    conn.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/json,text/csv,*/*");
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("HTTP " + code + " for GET " + url);
    }
    return conn;
  }

  /** Opens a POST connection with a JSON body; throws on any non-2xx status. */
  static HttpURLConnection openPostJson(String url, String jsonBody) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setInstanceFollowRedirects(true);
    conn.setRequestProperty("User-Agent", USER_AGENT);
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setRequestProperty("Accept", "application/json");
    byte[] bytes = jsonBody.getBytes(StandardCharsets.UTF_8);
    conn.setRequestProperty("Content-Length", String.valueOf(bytes.length));
    OutputStream out = conn.getOutputStream();
    try {
      out.write(bytes);
      out.flush();
    } finally {
      out.close();
    }
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("HTTP " + code + " for POST " + url + " body=" + jsonBody);
    }
    return conn;
  }

  /** Reads an input stream fully into a UTF-8 string. */
  static String readAll(InputStream in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[65536];
    int n;
    while ((n = in.read(buf)) > 0) {
      baos.write(buf, 0, n);
    }
    return new String(baos.toByteArray(), StandardCharsets.UTF_8);
  }

  /** Reads an input stream fully into a byte array. */
  static byte[] readBytes(InputStream in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[65536];
    int n;
    while ((n = in.read(buf)) > 0) {
      baos.write(buf, 0, n);
    }
    return baos.toByteArray();
  }

  /**
   * Fetches a WAF-blocked file via the Wayback Machine (which is not WAF'd).
   * Resolves the closest archived snapshot of {@code originalUrl} through the
   * availability API, then downloads the raw ({@code id_}) capture. Fails loud if
   * no snapshot exists. Transparently gunzips a gzip-wrapped capture.
   */
  static byte[] fetchViaWayback(String originalUrl) throws IOException {
    String availUrl = "http://archive.org/wayback/available?url="
        + java.net.URLEncoder.encode(originalUrl, "UTF-8");
    String json;
    InputStream in = openGet(availUrl).getInputStream();
    try {
      json = readAll(in);
    } finally {
      in.close();
    }
    com.fasterxml.jackson.databind.JsonNode root =
        new com.fasterxml.jackson.databind.ObjectMapper().readTree(json);
    com.fasterxml.jackson.databind.JsonNode closest =
        root.path("archived_snapshots").path("closest");
    if (!closest.path("available").asBoolean(false)) {
      throw new IOException("Wayback: no archived snapshot for " + originalUrl);
    }
    String ts = closest.path("timestamp").asText(null);
    if (ts == null) {
      throw new IOException("Wayback: snapshot without timestamp for " + originalUrl);
    }
    // Raw-capture form: /web/{TS}id_/<original-url>.
    String rawUrl = "http://web.archive.org/web/" + ts + "id_/" + originalUrl;
    byte[] bytes;
    InputStream raw = openGet(rawUrl).getInputStream();
    try {
      bytes = readBytes(raw);
    } finally {
      raw.close();
    }
    // The archive sometimes serves the capture gzip-encoded (magic 1f 8b).
    if (bytes.length > 1 && (bytes[0] & 0xff) == 0x1f && (bytes[1] & 0xff) == 0x8b) {
      java.util.zip.GZIPInputStream gz =
          new java.util.zip.GZIPInputStream(new java.io.ByteArrayInputStream(bytes));
      try {
        bytes = readBytes(gz);
      } finally {
        gz.close();
      }
    }
    return bytes;
  }

  /** Last two digits of a 4-digit year (e.g. "2022" -> "22"). */
  static String twoDigitYear(String year) {
    String y = year.trim();
    return y.length() >= 2 ? y.substring(y.length() - 2) : y;
  }

  /** Left-pads a numeric-ish string to {@code width} with leading zeros. */
  static String pad(String s, int width) {
    if (s == null) {
      return null;
    }
    String t = s.trim();
    if (t.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder(t);
    while (sb.length() < width) {
      sb.insert(0, '0');
    }
    return sb.toString();
  }

  /** Trims to null; empty becomes null. */
  static String str(String s) {
    if (s == null) {
      return null;
    }
    String t = s.trim();
    return t.isEmpty() ? null : t;
  }

  /** Parses a long; blank or unparseable becomes null (never throws). */
  static Long toLong(String s) {
    String t = str(s);
    if (t == null) {
      return null;
    }
    try {
      // Some IRS cells arrive as floats ("123.0000").
      int dot = t.indexOf('.');
      if (dot >= 0) {
        return Long.valueOf((long) Double.parseDouble(t));
      }
      return Long.valueOf(Long.parseLong(t));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** Parses an int; blank or unparseable becomes null (never throws). */
  static Integer toInt(String s) {
    Long l = toLong(s);
    return l == null ? null : Integer.valueOf(l.intValue());
  }

  /** Parses a double; blank or unparseable becomes null (never throws). */
  static Double toDouble(String s) {
    String t = str(s);
    if (t == null) {
      return null;
    }
    try {
      return Double.valueOf(Double.parseDouble(t));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** Builds a case-insensitive header name -> column index map. */
  static java.util.Map<String, Integer> headerIndex(java.util.List<String> headers) {
    java.util.Map<String, Integer> m = new java.util.HashMap<String, Integer>();
    for (int i = 0; i < headers.size(); i++) {
      m.put(headers.get(i).trim().toLowerCase(java.util.Locale.ROOT), Integer.valueOf(i));
    }
    return m;
  }

  /** Case-insensitive column-index lookup; null if the header is absent. */
  static Integer col(java.util.Map<String, Integer> idx, String name) {
    return idx.get(name.toLowerCase(java.util.Locale.ROOT));
  }

  /** Resolves a required column index; throws (fail loud) if the header is absent. */
  static int required(java.util.Map<String, Integer> idx, String name, String source) throws IOException {
    Integer i = col(idx, name);
    if (i == null) {
      throw new IOException("Expected column '" + name + "' not found in header of " + source
          + " — headers=" + idx.keySet());
    }
    return i.intValue();
  }

  /** Reads a cell by index, tolerating short rows and a null index (missing optional column). */
  static String cell(java.util.List<String> cols, Integer i) {
    if (i == null) {
      return null;
    }
    int at = i.intValue();
    if (at < 0 || at >= cols.size()) {
      return null;
    }
    return cols.get(at);
  }
}
