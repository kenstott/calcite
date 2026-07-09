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
package org.apache.calcite.adapter.govdata.disasters;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Streaming transformer for {@code storm_events} (NOAA NCEI Storm Events Database).
 *
 * <p>The per-year detail file name carries a processing-date suffix that changes on every NCEI
 * republish ({@code StormEvents_details-ftp_v1.0_d<year>_c<procdate>.csv.gz}), so this transformer
 * first fetches the directory listing at {@link RequestContext#getUrl()} and resolves the current
 * file for the requested {@code year} dimension (highest processing date wins), then streams the
 * gzip CSV record-by-record via {@link CsvRecordReader} (RFC4180-correct: NCEI narratives contain
 * embedded commas and newlines). Memory stays O(1) per row — no whole-file buffering.
 *
 * <p>Damage values use NCEI's {@code K}/{@code M}/{@code B} magnitude notation and are converted to
 * USD doubles. County FIPS is emitted only for county-type ({@code CZ_TYPE = 'C'}) rows.
 */
public class StormEventsStreamingTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(StormEventsStreamingTransformer.class);

  private static final int CONNECT_TIMEOUT_MS = 60_000;
  private static final int READ_TIMEOUT_MS = 1_800_000;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String year = context.getDimensionValues().get("year");
    if (year == null || year.isEmpty()) {
      throw new IllegalStateException("storm_events requires a 'year' dimension");
    }
    String dirUrl = context.getUrl();
    String fileName = resolveFileName(dirUrl, year);
    if (fileName == null) {
      // No detail file published for this year yet (e.g. current year early in Q1). Honest
      // absence — emit zero rows and log; do not fabricate.
      LOGGER.warn("storm_events[{}]: no detail file found in {}", year, dirUrl);
      return Collections.<Map<String, Object>>emptyList().iterator();
    }
    String fileUrl = (dirUrl.endsWith("/") ? dirUrl : dirUrl + "/") + fileName;
    LOGGER.info("storm_events[{}]: streaming {}", year, fileUrl);

    HttpURLConnection conn = (HttpURLConnection) URI.create(fileUrl).toURL().openConnection();
    conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
    conn.setReadTimeout(READ_TIMEOUT_MS);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != HttpURLConnection.HTTP_OK) {
      conn.disconnect();
      throw new IOException("HTTP " + status + " from " + fileUrl);
    }
    InputStream raw = conn.getInputStream();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(raw), StandardCharsets.UTF_8));

    String headerRecord = CsvRecordReader.readRecord(reader);
    if (headerRecord == null) {
      reader.close();
      conn.disconnect();
      throw new IOException("storm_events[" + year + "]: empty file " + fileUrl);
    }
    Map<String, Integer> idx = headerIndex(CsvRecordReader.splitFields(headerRecord, ','));
    return new StormRowIterator(reader, conn, idx, year);
  }

  /** Fetches the NCEI directory listing and returns the newest detail file for the year. */
  private String resolveFileName(String dirUrl, String year) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(dirUrl).toURL().openConnection();
    conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
    conn.setReadTimeout(CONNECT_TIMEOUT_MS);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    try {
      if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
        throw new IOException("HTTP " + conn.getResponseCode() + " listing " + dirUrl);
      }
      Pattern p = Pattern.compile(
          "StormEvents_details-ftp_v1\\.0_d" + Pattern.quote(year) + "_c(\\d+)\\.csv\\.gz");
      String best = null;
      long bestProc = -1L;
      try (BufferedReader r = new BufferedReader(
          new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = r.readLine()) != null) {
          Matcher m = p.matcher(line);
          while (m.find()) {
            long proc = Long.parseLong(m.group(1));
            if (proc > bestProc) {
              bestProc = proc;
              best = m.group();
            }
          }
        }
      }
      return best;
    } finally {
      conn.disconnect();
    }
  }

  private static Map<String, Integer> headerIndex(List<String> header) {
    Map<String, Integer> idx = new HashMap<String, Integer>();
    for (int i = 0; i < header.size(); i++) {
      idx.put(header.get(i).trim(), i);
    }
    return idx;
  }

  /** Lazy row iterator over the gzip CSV; closes the stream on exhaustion. */
  private static final class StormRowIterator implements Iterator<Map<String, Object>> {
    private final BufferedReader reader;
    private final HttpURLConnection conn;
    private final Map<String, Integer> idx;
    private final String year;
    private List<String> pending;
    private boolean done;

    StormRowIterator(BufferedReader reader, HttpURLConnection conn,
        Map<String, Integer> idx, String year) {
      this.reader = reader;
      this.conn = conn;
      this.idx = idx;
      this.year = year;
      advance();
    }

    private void advance() {
      try {
        String record = CsvRecordReader.readRecord(reader);
        if (record == null) {
          pending = null;
          close();
        } else {
          pending = CsvRecordReader.splitFields(record, ',');
        }
      } catch (IOException e) {
        close();
        throw new RuntimeException("storm_events[" + year + "]: read failed", e);
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
      return pending != null;
    }

    @Override public Map<String, Object> next() {
      if (pending == null) {
        throw new NoSuchElementException();
      }
      List<String> cols = pending;
      Map<String, Object> row = mapRow(cols);
      advance();
      return row;
    }

    private Map<String, Object> mapRow(List<String> cols) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("event_id", str(cols, "EVENT_ID"));
      row.put("episode_id", str(cols, "EPISODE_ID"));
      row.put("begin_date", isoDate(str(cols, "BEGIN_YEARMONTH"), str(cols, "BEGIN_DAY")));
      row.put("end_date", isoDate(str(cols, "END_YEARMONTH"), str(cols, "END_DAY")));

      String stateFips = pad(str(cols, "STATE_FIPS"), 2);
      row.put("state_fips", stateFips);
      String czType = str(cols, "CZ_TYPE");
      String czFips = pad(str(cols, "CZ_FIPS"), 3);
      row.put("county_fips",
          "C".equals(czType) && stateFips != null && czFips != null ? stateFips + czFips : null);
      row.put("state_name", str(cols, "STATE"));
      row.put("event_type", str(cols, "EVENT_TYPE"));
      row.put("cz_type", czType);
      row.put("magnitude", dbl(str(cols, "MAGNITUDE")));
      row.put("magnitude_type", str(cols, "MAGNITUDE_TYPE"));
      row.put("injuries_direct", intg(str(cols, "INJURIES_DIRECT")));
      row.put("injuries_indirect", intg(str(cols, "INJURIES_INDIRECT")));
      row.put("deaths_direct", intg(str(cols, "DEATHS_DIRECT")));
      row.put("deaths_indirect", intg(str(cols, "DEATHS_INDIRECT")));
      row.put("damage_property", damage(str(cols, "DAMAGE_PROPERTY")));
      row.put("damage_crops", damage(str(cols, "DAMAGE_CROPS")));
      row.put("begin_lat", dbl(str(cols, "BEGIN_LAT")));
      row.put("begin_lon", dbl(str(cols, "BEGIN_LON")));
      return row;
    }

    private String str(List<String> cols, String name) {
      Integer i = idx.get(name);
      if (i == null || i >= cols.size()) {
        return null;
      }
      String v = cols.get(i).trim();
      return v.isEmpty() ? null : v;
    }
  }

  /** Builds {@code YYYY-MM-DD} from an NCEI {@code YYYYMM} + day-of-month. */
  private static String isoDate(String yearMonth, String day) {
    if (yearMonth == null || yearMonth.length() != 6 || day == null) {
      return null;
    }
    return yearMonth.substring(0, 4) + "-" + yearMonth.substring(4, 6) + "-" + pad(day, 2);
  }

  private static String pad(String v, int width) {
    if (v == null) {
      return null;
    }
    String s = v.trim();
    if (s.isEmpty()) {
      return null;
    }
    while (s.length() < width) {
      s = "0" + s;
    }
    return s.length() > width ? s.substring(s.length() - width) : s;
  }

  private static Integer intg(String v) {
    if (v == null) {
      return null;
    }
    try {
      return (int) Double.parseDouble(v);
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

  /** Converts NCEI K/M/B damage notation (e.g. {@code 55.00K}, {@code 1.5M}) to USD. */
  private static Double damage(String v) {
    if (v == null) {
      return null;
    }
    String s = v.trim();
    if (s.isEmpty()) {
      return null;
    }
    double mult = 1.0;
    char last = Character.toUpperCase(s.charAt(s.length() - 1));
    if (last == 'K') {
      mult = 1_000.0;
    } else if (last == 'M') {
      mult = 1_000_000.0;
    } else if (last == 'B') {
      mult = 1_000_000_000.0;
    }
    if (mult != 1.0) {
      s = s.substring(0, s.length() - 1);
    }
    try {
      return Double.parseDouble(s) * mult;
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
