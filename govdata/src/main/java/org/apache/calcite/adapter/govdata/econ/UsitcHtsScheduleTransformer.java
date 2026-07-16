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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Year;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Streaming transformer for {@code econ.usitc_tariff_schedule} (USITC statutory Harmonized
 * Tariff Schedule rate lines).
 *
 * <p>The public HTS export ({@code hts.usitc.gov/reststop/exportList?format=JSON}) returns a
 * top-level JSON <em>array</em> of tariff-line objects for the current HTS revision — verified
 * live 2026-07-16, e.g.
 * <pre>{@code
 * {"htsno":"0101.30.00.00","indent":"1","description":"Asses","superior":null,
 *  "units":["No."],"general":"6.8%","special":"Free (A+,AU,...)","other":"15%","footnotes":[...]}
 * }</pre>
 * The full export is ~19k lines, so this class implements {@link StreamingResponseTransformer}:
 * {@code HttpSource} calls {@link #fetchAndTransform} directly, which opens the URL and streams
 * one array element at a time — heap stays O(1) per row.
 *
 * <p>The export carries no {@code year} or {@code hs6} field. Each row is stamped with the current
 * HTS revision year (UTC calendar year — the export always serves the current revision) so the
 * writer can partition on {@code [type, year]}, and {@code hs6} is derived as the first 6 digits of
 * the HTS number. Only 8-digit-or-longer tariff lines are emitted; shorter heading/chapter rows
 * carry no tariff line. {@code general_ave} (ad-valorem equivalent) and {@code effective_date} are
 * not present in the export and are left {@code null} rather than fabricated.
 */
public class UsitcHtsScheduleTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsitcHtsScheduleTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonFactory JSON_FACTORY = MAPPER.getFactory();

  private static final int CONNECT_TIMEOUT_MS = 60_000;
  private static final int READ_TIMEOUT_MS = 900_000;
  private static final int MAX_RETRIES = 4;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    if (url == null || url.isEmpty()) {
      throw new IllegalStateException("UsitcHtsScheduleTransformer: no URL in context");
    }
    // The export serves the current revision; stamp rows with the current UTC year.
    int revisionYear = Year.now(ZoneOffset.UTC).getValue();
    LOGGER.info("usitc_tariff_schedule: streaming HTS revision {} from {}", revisionYear, url);

    HttpURLConnection conn = openWithRetry(url, context.getHeaders());
    InputStream in = conn.getInputStream();
    JsonParser parser = JSON_FACTORY.createParser(in);

    JsonToken first = parser.nextToken();
    if (first != JsonToken.START_ARRAY) {
      close(parser, conn);
      throw new IOException("usitc_tariff_schedule: expected top-level JSON array for " + url
          + " but got " + first);
    }
    return new HtsRowIterator(parser, conn, url, revisionYear);
  }

  private static HttpURLConnection openWithRetry(String url, Map<String, String> headers)
      throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
      conn.setReadTimeout(READ_TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      conn.setRequestProperty("Accept", "application/json");
      if (headers != null) {
        for (Map.Entry<String, String> h : headers.entrySet()) {
          conn.setRequestProperty(h.getKey(), h.getValue());
        }
      }
      int status;
      try {
        status = conn.getResponseCode();
      } catch (IOException e) {
        conn.disconnect();
        last = e;
        sleep(attempt);
        continue;
      }
      if (status == HttpURLConnection.HTTP_OK) {
        return conn;
      }
      conn.disconnect();
      if (status == 429 || status == 500 || status == 503) {
        LOGGER.warn("usitc_tariff_schedule: HTTP {} (attempt {}/{}) for {}",
            status, attempt, MAX_RETRIES, url);
        sleep(attempt);
        continue;
      }
      throw new IOException("HTTP " + status + " from " + url);
    }
    throw last != null ? last : new IOException(
        "usitc_tariff_schedule: retries exhausted for " + url);
  }

  private static void sleep(int attempt) {
    try {
      Thread.sleep(1000L * attempt);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void close(JsonParser parser, HttpURLConnection conn) {
    try {
      parser.close();
    } catch (IOException ignored) {
      // best-effort
    }
    conn.disconnect();
  }

  private static String toStr(Object v) {
    if (v == null) {
      return null;
    }
    String s = v.toString().trim();
    return s.isEmpty() ? null : s;
  }

  /** Digits-only form of an HTS number (e.g. {@code "0101.30.00.00" -> "0101300000"}). */
  private static String digits(String htsno) {
    if (htsno == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder(htsno.length());
    for (int i = 0; i < htsno.length(); i++) {
      char c = htsno.charAt(i);
      if (c >= '0' && c <= '9') {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /** Lazy iterator over the HTS export array; maps each 8-digit tariff line to a clean row. */
  private static final class HtsRowIterator implements Iterator<Map<String, Object>> {
    private final JsonParser parser;
    private final HttpURLConnection conn;
    private final String url;
    private final int revisionYear;
    private Map<String, Object> pending;
    private boolean closed;
    private long rowCount;

    HtsRowIterator(JsonParser parser, HttpURLConnection conn, String url, int revisionYear) {
      this.parser = parser;
      this.conn = conn;
      this.url = url;
      this.revisionYear = revisionYear;
      advance();
    }

    @SuppressWarnings("unchecked")
    private void advance() {
      try {
        JsonToken t;
        while ((t = parser.nextToken()) != null && t != JsonToken.END_ARRAY) {
          if (t != JsonToken.START_OBJECT) {
            continue;
          }
          Map<String, Object> rec = MAPPER.readValue(parser, Map.class);
          Map<String, Object> row = map(rec);
          if (row != null) {
            pending = row;
            return;
          }
        }
        pending = null;
        finish();
      } catch (IOException e) {
        finish();
        throw new RuntimeException("usitc_tariff_schedule: read failed for " + url, e);
      }
    }

    private Map<String, Object> map(Map<String, Object> rec) {
      String htsno = toStr(rec.get("htsno"));
      String d = digits(htsno);
      if (d == null || d.length() < 8) {
        // Chapter/heading/subheading structural rows carry no 8-digit tariff line — skip.
        return null;
      }
      String hts8 = d.substring(0, 8);
      String hs6 = d.substring(0, 6);

      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("hts8", hts8);
      row.put("hs6", hs6);
      row.put("description", toStr(rec.get("description")));
      row.put("unit_of_quantity", units(rec.get("units")));
      row.put("general_rate", toStr(rec.get("general")));
      // AVE is not published in the export; leave null rather than fabricate (rule #6).
      row.put("general_ave", null);
      row.put("special_rate", toStr(rec.get("special")));
      row.put("column2_rate", toStr(rec.get("other")));
      // effective_date is not carried by the export — null, not fabricated.
      row.put("effective_date", null);
      row.put("year", revisionYear);
      return row;
    }

    @SuppressWarnings("unchecked")
    private static String units(Object v) {
      if (!(v instanceof List)) {
        return toStr(v);
      }
      List<Object> list = (List<Object>) v;
      StringBuilder sb = new StringBuilder();
      for (Object o : list) {
        String s = toStr(o);
        if (s == null) {
          continue;
        }
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append(s);
      }
      return sb.length() == 0 ? null : sb.toString();
    }

    private void finish() {
      if (closed) {
        return;
      }
      closed = true;
      close(parser, conn);
      LOGGER.debug("usitc_tariff_schedule: streamed {} tariff lines from {}", rowCount, url);
    }

    @Override public boolean hasNext() {
      return pending != null;
    }

    @Override public Map<String, Object> next() {
      if (pending == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = pending;
      rowCount++;
      advance();
      return row;
    }
  }
}
