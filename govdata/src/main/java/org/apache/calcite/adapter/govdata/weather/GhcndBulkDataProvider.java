/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.weather;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;

/**
 * DataProvider for NOAA GHCN-Daily bulk annual CSV files.
 *
 * <p>Downloads the per-year compressed CSV from NCEI
 * ({@code https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/YYYY.csv.gz}),
 * decompresses it on the fly, filters to US stations with passing quality flags,
 * performs long-to-wide pivot, and emits one row per (station_id, date).
 *
 * <p>File format (no header line):
 * <pre>
 * ID,DATE,ELEMENT,DATA_VALUE,M_FLAG,Q_FLAG,S_FLAG,OBS_TIME
 * USW00094728,20250101,TMAX,56,,,W,0800
 * </pre>
 *
 * <p>Units: TMAX/TMIN/TAVG in tenths of °C, PRCP in tenths of mm,
 * AWND in tenths of m/s. SNOW/SNWD are in mm (no conversion needed).
 *
 * <p>The NCEI file is sorted by (date, station_id), not by station_id first.
 * To enable the single-pass station accumulator, all relevant filtered lines are
 * loaded into memory, sorted by station_id, then streamed into the iterator.
 */
public class GhcndBulkDataProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(GhcndBulkDataProvider.class);

  private static final String BULK_URL_TEMPLATE =
      "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz";
  private static final String STATIONS_URL =
      "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt";

  /** JVM-lifetime cache of station→state_fips mapping (loaded once, reused across years). */
  private static final Map<String, String> STATION_STATE_CACHE =
      new ConcurrentHashMap<String, String>();
  private static volatile boolean stationsLoaded = false;

  /** State abbreviation to 2-digit FIPS code. */
  private static final Map<String, String> ABBR_TO_FIPS = new HashMap<String, String>();

  static {
    ABBR_TO_FIPS.put("AL", "01"); ABBR_TO_FIPS.put("AK", "02"); ABBR_TO_FIPS.put("AZ", "04");
    ABBR_TO_FIPS.put("AR", "05"); ABBR_TO_FIPS.put("CA", "06"); ABBR_TO_FIPS.put("CO", "08");
    ABBR_TO_FIPS.put("CT", "09"); ABBR_TO_FIPS.put("DE", "10"); ABBR_TO_FIPS.put("DC", "11");
    ABBR_TO_FIPS.put("FL", "12"); ABBR_TO_FIPS.put("GA", "13"); ABBR_TO_FIPS.put("HI", "15");
    ABBR_TO_FIPS.put("ID", "16"); ABBR_TO_FIPS.put("IL", "17"); ABBR_TO_FIPS.put("IN", "18");
    ABBR_TO_FIPS.put("IA", "19"); ABBR_TO_FIPS.put("KS", "20"); ABBR_TO_FIPS.put("KY", "21");
    ABBR_TO_FIPS.put("LA", "22"); ABBR_TO_FIPS.put("ME", "23"); ABBR_TO_FIPS.put("MD", "24");
    ABBR_TO_FIPS.put("MA", "25"); ABBR_TO_FIPS.put("MI", "26"); ABBR_TO_FIPS.put("MN", "27");
    ABBR_TO_FIPS.put("MS", "28"); ABBR_TO_FIPS.put("MO", "29"); ABBR_TO_FIPS.put("MT", "30");
    ABBR_TO_FIPS.put("NE", "31"); ABBR_TO_FIPS.put("NV", "32"); ABBR_TO_FIPS.put("NH", "33");
    ABBR_TO_FIPS.put("NJ", "34"); ABBR_TO_FIPS.put("NM", "35"); ABBR_TO_FIPS.put("NY", "36");
    ABBR_TO_FIPS.put("NC", "37"); ABBR_TO_FIPS.put("ND", "38"); ABBR_TO_FIPS.put("OH", "39");
    ABBR_TO_FIPS.put("OK", "40"); ABBR_TO_FIPS.put("OR", "41"); ABBR_TO_FIPS.put("PA", "42");
    ABBR_TO_FIPS.put("RI", "44"); ABBR_TO_FIPS.put("SC", "45"); ABBR_TO_FIPS.put("SD", "46");
    ABBR_TO_FIPS.put("TN", "47"); ABBR_TO_FIPS.put("TX", "48"); ABBR_TO_FIPS.put("UT", "49");
    ABBR_TO_FIPS.put("VT", "50"); ABBR_TO_FIPS.put("VA", "51"); ABBR_TO_FIPS.put("WA", "53");
    ABBR_TO_FIPS.put("WV", "54"); ABBR_TO_FIPS.put("WI", "55"); ABBR_TO_FIPS.put("WY", "56");
  }

  @Override public Iterator<Map<String, Object>> fetch(
      EtlPipelineConfig config, Map<String, String> variables) throws IOException {

    String year = variables.get("year");
    if (year == null) {
      throw new IOException("GhcndBulkDataProvider requires 'year' dimension");
    }

    ensureStationsLoaded();

    String cacheDir = resolveCacheDir();
    File gzFile = new File(cacheDir, "ghcnd_bulk_" + year + ".csv.gz");
    downloadIfAbsent(gzFile, BULK_URL_TEMPLATE.replace("{year}", year));

    List<String> sortedLines = loadAndSortFilteredLines(gzFile, year);
    return new BulkIterator(sortedLines, year);
  }

  // ---------------------------------------------------------------------------
  // Station metadata loading
  // ---------------------------------------------------------------------------

  private void ensureStationsLoaded() throws IOException {
    if (stationsLoaded) {
      return;
    }
    synchronized (STATION_STATE_CACHE) {
      if (stationsLoaded) {
        return;
      }
      String cacheDir = resolveCacheDir();
      File stationsFile = new File(cacheDir, "ghcnd-stations.txt");
      downloadIfAbsent(stationsFile, STATIONS_URL);
      loadStations(stationsFile);
      stationsLoaded = true;
    }
  }

  private void loadStations(File file) throws IOException {
    int loaded = 0;
    int skipped = 0;
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.length() < 40) {
          continue;
        }
        String stationId = line.substring(0, 11).trim();
        String stateAbbr = line.substring(38, 40).trim();
        if (stationId.isEmpty() || stateAbbr.isEmpty()) {
          skipped++;
          continue;
        }
        String fips = ABBR_TO_FIPS.get(stateAbbr);
        if (fips == null) {
          skipped++;
          continue;
        }
        STATION_STATE_CACHE.put(stationId, fips);
        loaded++;
      }
    }
    LOGGER.info("GHCND Bulk: loaded {} US station→state mappings ({} skipped)", loaded, skipped);
  }

  // ---------------------------------------------------------------------------
  // HTTP download with local cache
  // ---------------------------------------------------------------------------

  private String resolveCacheDir() {
    String localCache = System.getenv("ETL_LOCAL_RAW_CACHE");
    if (localCache != null && !localCache.isEmpty()) {
      return localCache + "/ghcnd_bulk";
    }
    return System.getProperty("java.io.tmpdir") + "/govdata_ghcnd_bulk";
  }

  private void downloadIfAbsent(File dest, String url) throws IOException {
    if (dest.exists() && dest.length() > 0) {
      LOGGER.info("GHCND Bulk: using cached file {} ({} MB)",
          dest.getName(), dest.length() / (1024 * 1024));
      return;
    }
    dest.getParentFile().mkdirs();
    LOGGER.info("GHCND Bulk: downloading {} -> {}", url, dest.getAbsolutePath());

    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(600000);
    conn.setRequestProperty("User-Agent", "(calcite-govdata, kennethstott@gmail.com)");

    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("HTTP " + code + " for " + url);
    }

    File tmp = new File(dest.getParentFile(), dest.getName() + ".tmp");
    try (InputStream is = new BufferedInputStream(conn.getInputStream(), 65536);
         FileOutputStream fos = new FileOutputStream(tmp)) {
      byte[] buf = new byte[65536];
      int len;
      long totalBytes = 0;
      while ((len = is.read(buf)) != -1) {
        fos.write(buf, 0, len);
        totalBytes += len;
      }
      fos.flush();
      LOGGER.info("GHCND Bulk: downloaded {} MB to {}", totalBytes / (1024 * 1024), dest.getName());
    } finally {
      conn.disconnect();
    }

    if (!tmp.renameTo(dest)) {
      tmp.delete();
      throw new IOException("Failed to rename temp file to " + dest.getAbsolutePath());
    }
  }

  // ---------------------------------------------------------------------------
  // Sort-before-stream
  // ---------------------------------------------------------------------------

  private List<String> loadAndSortFilteredLines(File gzFile, String year) throws IOException {
    List<String> lines = new ArrayList<String>();
    try (InputStream fis = new FileInputStream(gzFile);
         InputStream gzis = new GZIPInputStream(new BufferedInputStream(fis, 65536));
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(gzis, StandardCharsets.UTF_8), 65536)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (isRelevantLine(line)) {
          lines.add(line);
        }
      }
    }
    LOGGER.info("GHCND Bulk year={}: loaded {} relevant lines, sorting by station_id...",
        year, lines.size());
    // US GHCND station IDs are all 11 chars — lexicographic sort gives (station_id, date, element)
    Collections.sort(lines);
    LOGGER.info("GHCND Bulk year={}: sort complete", year);
    return lines;
  }

  private static boolean isRelevantLine(String line) {
    String[] f = line.split(",", 8);
    if (f.length < 6) {
      return false;
    }
    if (f[0].length() < 2 || !f[0].startsWith("US")) {
      return false;
    }
    if (!BulkIterator.isRelevantElement(f[2])) {
      return false;
    }
    if (!f[5].isEmpty()) {
      return false; // non-empty q_flag = failed quality check
    }
    if (f[3].isEmpty() || "-9999".equals(f[3])) {
      return false;
    }
    return true;
  }

  // ---------------------------------------------------------------------------
  // Streaming iterator
  // ---------------------------------------------------------------------------

  private static final class BulkIterator implements Iterator<Map<String, Object>> {

    private final Iterator<String> lineIterator;
    private final String year;

    private String currentStation = null;
    private Map<String, StationDayRecord> currentDays = new LinkedHashMap<String, StationDayRecord>();

    private final Queue<Map<String, Object>> outputQueue =
        new ArrayDeque<Map<String, Object>>();
    private boolean eof = false;
    private long linesRead = 0;
    private long rowsEmitted = 0;

    BulkIterator(List<String> sortedLines, String year) {
      this.year = year;
      this.lineIterator = sortedLines.iterator();
    }

    @Override public boolean hasNext() {
      while (outputQueue.isEmpty() && !eof) {
        fillQueue();
      }
      return !outputQueue.isEmpty();
    }

    @Override public Map<String, Object> next() {
      Map<String, Object> row = outputQueue.poll();
      if (row == null) {
        throw new java.util.NoSuchElementException();
      }
      return row;
    }

    private void fillQueue() {
      // Read until we emit at least one record into the queue (station boundary)
      while (lineIterator.hasNext()) {
        linesRead++;
        processLine(lineIterator.next());
        if (!outputQueue.isEmpty()) {
          return;
        }
      }
      // EOF: emit final station's records
      eof = true;
      if (currentStation != null) {
        emitCurrentStation();
      }
      LOGGER.info("GHCND Bulk year={}: read {} lines, emitted {} wide rows",
          year, linesRead, rowsEmitted);
    }

    private void processLine(String line) {
      // Format: ID,DATE,ELEMENT,DATA_VALUE,M_FLAG,Q_FLAG,S_FLAG,OBS_TIME
      // Split at most 8 fields; Q_FLAG is index 5
      String[] f = line.split(",", 8);
      if (f.length < 6) {
        return;
      }

      String stationId = f[0];
      if (stationId.length() < 2 || !stationId.startsWith("US")) {
        return;
      }

      String element = f[2];
      if (!isRelevantElement(element)) {
        return;
      }

      String qFlag = f[5];
      if (!qFlag.isEmpty()) {
        return; // Failed quality check — skip
      }

      String rawDate = f[1]; // YYYYMMDD
      if (rawDate.length() != 8) {
        return;
      }

      String valueStr = f[3];
      if (valueStr.isEmpty() || valueStr.equals("-9999")) {
        return;
      }
      double rawValue;
      try {
        rawValue = Double.parseDouble(valueStr);
      } catch (NumberFormatException e) {
        return;
      }

      // When station changes, emit previous station's accumulated records
      if (!stationId.equals(currentStation)) {
        if (currentStation != null) {
          emitCurrentStation();
        }
        currentStation = stationId;
        currentDays = new LinkedHashMap<String, StationDayRecord>();
      }

      StationDayRecord rec = currentDays.get(rawDate);
      if (rec == null) {
        rec = new StationDayRecord();
        currentDays.put(rawDate, rec);
      }

      String mFlag = f[4];
      applyElement(rec, element, rawValue, mFlag);
    }

    private void emitCurrentStation() {
      String stateFips = STATION_STATE_CACHE.get(currentStation);
      if (stateFips == null) {
        currentDays = new LinkedHashMap<String, StationDayRecord>();
        return;
      }

      for (Map.Entry<String, StationDayRecord> entry : currentDays.entrySet()) {
        String rawDate = entry.getKey(); // YYYYMMDD
        StationDayRecord rec = entry.getValue();

        // Convert date from YYYYMMDD to YYYY-MM-DD
        String isoDate = rawDate.substring(0, 4) + "-"
            + rawDate.substring(4, 6) + "-"
            + rawDate.substring(6, 8);
        int yearInt;
        try {
          yearInt = Integer.parseInt(rawDate.substring(0, 4));
        } catch (NumberFormatException e) {
          continue;
        }
        String month = rawDate.substring(4, 6);

        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("station_id", currentStation);
        row.put("state_fips", stateFips);
        row.put("date", isoDate);
        row.put("year", yearInt);
        row.put("month", month);
        row.put("tmax_c", rec.tmaxC);
        row.put("tmin_c", rec.tminC);
        row.put("tavg_c", rec.tavgC);
        row.put("prcp_mm", rec.prcpMm);
        row.put("snow_mm", rec.snowMm);
        row.put("snwd_mm", rec.snwdMm);
        row.put("awnd_ms", rec.awndMs);
        row.put("tmax_flag", rec.tmaxFlag);
        row.put("tmin_flag", rec.tminFlag);
        row.put("prcp_flag", rec.prcpFlag);
        outputQueue.add(row);
        rowsEmitted++;
      }
      currentDays = new LinkedHashMap<String, StationDayRecord>();
    }

    static boolean isRelevantElement(String element) {
      return "TMAX".equals(element) || "TMIN".equals(element) || "TAVG".equals(element)
          || "PRCP".equals(element) || "SNOW".equals(element) || "SNWD".equals(element)
          || "AWND".equals(element);
    }

    private static void applyElement(StationDayRecord rec, String element,
        double raw, String mFlag) {
      // TMAX/TMIN/TAVG: tenths of °C → °C
      // PRCP: tenths of mm → mm
      // AWND: tenths of m/s → m/s
      // SNOW/SNWD: mm (no conversion)
      if ("TMAX".equals(element)) {
        rec.tmaxC = raw / 10.0;
        rec.tmaxFlag = mFlag.isEmpty() ? null : mFlag;
      } else if ("TMIN".equals(element)) {
        rec.tminC = raw / 10.0;
        rec.tminFlag = mFlag.isEmpty() ? null : mFlag;
      } else if ("TAVG".equals(element)) {
        rec.tavgC = raw / 10.0;
      } else if ("PRCP".equals(element)) {
        rec.prcpMm = raw / 10.0;
        rec.prcpFlag = mFlag.isEmpty() ? null : mFlag;
      } else if ("SNOW".equals(element)) {
        rec.snowMm = raw;
      } else if ("SNWD".equals(element)) {
        rec.snwdMm = raw;
      } else if ("AWND".equals(element)) {
        rec.awndMs = raw / 10.0;
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Per-station-day accumulator
  // ---------------------------------------------------------------------------

  private static final class StationDayRecord {
    Double tmaxC;
    Double tminC;
    Double tavgC;
    Double prcpMm;
    Double snowMm;
    Double snwdMm;
    Double awndMs;
    String tmaxFlag;
    String tminFlag;
    String prcpFlag;
  }

  /**
   * Returns a provider that always returns an empty iterator (for testing).
   */
  public static DataProvider empty() {
    return (config, variables) -> Collections.<Map<String, Object>>emptyList().iterator();
  }
}
