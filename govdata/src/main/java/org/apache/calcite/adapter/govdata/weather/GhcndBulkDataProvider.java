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
package org.apache.calcite.adapter.govdata.weather;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;
import org.apache.calcite.adapter.govdata.ZipDownloadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

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
 * <p>The NCEI file is sorted by (date, station_id), not by station_id first. The
 * single-pass station accumulator ({@link BulkIterator}) needs each station's rows
 * contiguous, so the file is regrouped by station_id. To keep heap bounded regardless of
 * year size, that regroup is done by DuckDB (filter + ORDER BY station, plus DQ striding)
 * with the sorted result written to a temp file that is then streamed line-by-line — no
 * full-file buffer in the JVM.
 */
public class GhcndBulkDataProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(GhcndBulkDataProvider.class);

  private volatile StorageProvider storageProvider;

  private StorageProvider storageProvider() {
    if (storageProvider == null) {
      synchronized (this) {
        if (storageProvider == null) {
          storageProvider = StorageProviderFactory.createForGovDataCache();
        }
      }
    }
    return storageProvider;
  }

  private String cacheDir() {
    return storageProvider().resolvePath(StorageProviderFactory.getGovDataCacheDir(), "ghcnd_bulk");
  }

  private String cachePath(String filename) {
    return storageProvider().resolvePath(cacheDir(), filename);
  }

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

    String gzPath = cachePath("ghcnd_bulk_" + year + ".csv.gz");
    downloadIfAbsent(gzPath, BULK_URL_TEMPLATE.replace("{year}", year));

    return new BulkIterator(streamSortedFilteredLines(gzPath, year), year);
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
      String stationsPath = cachePath("ghcnd-stations.txt");
      downloadIfAbsent(stationsPath, STATIONS_URL);
      try (InputStream in = storageProvider().openInputStream(stationsPath);
           BufferedReader reader = new BufferedReader(
               new InputStreamReader(in, StandardCharsets.UTF_8))) {
        loadStations(reader);
      }
      stationsLoaded = true;
    }
  }

  private void loadStations(BufferedReader reader) throws IOException {
    int loaded = 0;
    int skipped = 0;
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.length() < 40) continue;
      String stationId = line.substring(0, 11).trim();
      String stateAbbr = line.substring(38, 40).trim();
      if (stationId.isEmpty() || stateAbbr.isEmpty()) { skipped++; continue; }
      String fips = ABBR_TO_FIPS.get(stateAbbr);
      if (fips == null) { skipped++; continue; }
      STATION_STATE_CACHE.put(stationId, fips);
      loaded++;
    }
    LOGGER.info("GHCND Bulk: loaded {} US station→state mappings ({} skipped)", loaded, skipped);
  }

  private void downloadIfAbsent(String storagePath, String url) throws IOException {
    try {
      if (storageProvider().exists(storagePath)) {
        StorageProvider.FileMetadata meta = storageProvider().getMetadata(storagePath);
        if (meta != null && meta.getSize() > 0) {
          LOGGER.info("GHCND Bulk: using cached {}", storagePath);
          return;
        }
      }
    } catch (IOException e) {
      // not cached — fall through to download
    }
    LOGGER.info("GHCND Bulk: downloading {} -> {}", url, storagePath);
    File tempFile = File.createTempFile("ghcnd-", ".tmp");
    try {
      ZipDownloadUtils.downloadToFile(url, null, tempFile);
      try (InputStream in = new java.io.FileInputStream(tempFile)) {
        storageProvider().writeFile(storagePath, in);
      }
    } finally {
      tempFile.delete();
    }
  }

  // ---------------------------------------------------------------------------
  // Sort-before-stream
  // ---------------------------------------------------------------------------

  /**
   * Filters + regroups the GHCND by-year file by station_id using DuckDB and streams the sorted
   * result line-by-line from a temp file. DuckDB reads the gzip directly (decompress + filter +
   * {@code ORDER BY station_id}, plus DQ striding), spilling the sort to disk as needed, so JVM
   * heap stays flat regardless of year size. Each reconstructed line is
   * {@code station,date,element,value,m_flag,} — the field layout {@link BulkIterator} expects.
   */
  private Iterator<String> streamSortedFilteredLines(String gzPath, String year) throws IOException {
    File tempFile = File.createTempFile("ghcnd-sorted-" + year + "-", ".tsv");
    String strideClause = "";
    if (isDqSampleMode()) {
      // Keep ~DQ_TARGET_STATIONS stations spread across the sorted station range: every Kth
      // distinct station (srank is 1-based dense rank → srank-1 is the 0-based station index),
      // K = distinct/target (>=1). Keep all when there are fewer than the target.
      strideClause = " WHERE ndist <= " + DQ_TARGET_STATIONS
          + " OR ((srank - 1) % GREATEST(CAST(ndist / " + DQ_TARGET_STATIONS + " AS BIGINT), 1)) = 0";
    }
    String sql = "COPY (WITH src AS ("
        + "SELECT column0 AS station, column1 AS dt, column2 AS el, column3 AS val, "
        + "COALESCE(column4, '') AS mf "
        + "FROM read_csv('" + gzPath + "', header=false, all_varchar=true, ignore_errors=true) "
        + "WHERE column0 LIKE 'US%' "
        + "AND column2 IN ('TMAX','TMIN','TAVG','PRCP','SNOW','SNWD','AWND') "
        + "AND COALESCE(column5, '') = '' "
        + "AND column3 IS NOT NULL AND column3 <> '' AND column3 <> '-9999'"
        + "), ranked AS ("
        + "SELECT station, dt, el, val, mf, "
        + "DENSE_RANK() OVER (ORDER BY station) AS srank, "
        + "(SELECT COUNT(DISTINCT station) FROM src) AS ndist FROM src) "
        + "SELECT concat_ws(',', station, dt, el, val, mf, '') AS line FROM ranked"
        + strideClause
        + " ORDER BY station) TO '" + tempFile.getAbsolutePath()
        + "' (FORMAT CSV, DELIMITER E'\\t', HEADER false)";

    long t0 = System.currentTimeMillis();
    try {
      Connection conn = AbstractGovDataDownloader.getDuckDBConnection(storageProvider());
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("SET preserve_insertion_order=false");
        stmt.execute("SET memory_limit='2GB'");
        stmt.execute("SET temp_directory='" + System.getProperty("java.io.tmpdir") + "'");
        stmt.execute(sql);
      } finally {
        conn.close();
      }
    } catch (SQLException e) {
      tempFile.delete();
      throw new IOException("GHCND DuckDB filter+sort failed for year=" + year + ": "
          + e.getMessage(), e);
    }
    LOGGER.info("GHCND Bulk year={}: DuckDB filter+sort{} complete in {}ms (streaming from {})",
        year, isDqSampleMode() ? "+DQ-stride" : "", System.currentTimeMillis() - t0,
        tempFile.getName());
    return new TempFileLineIterator(tempFile, year);
  }

  /**
   * Streams lines from a temp file, deleting it once exhausted (or on read failure). Keeps the
   * GHCND pipeline heap-bounded: only one line is held in memory at a time.
   */
  private static final class TempFileLineIterator implements Iterator<String> {
    private final File tempFile;
    private final String year;
    private final BufferedReader reader;
    private String nextLine;
    private boolean closed;

    TempFileLineIterator(File tempFile, String year) throws IOException {
      this.tempFile = tempFile;
      this.year = year;
      this.reader = new BufferedReader(new InputStreamReader(
          new java.io.FileInputStream(tempFile), StandardCharsets.UTF_8), 65536);
      advance();
    }

    private void advance() {
      try {
        String line = reader.readLine();
        if (line == null) {
          nextLine = null;
          close();
        } else {
          nextLine = line;
        }
      } catch (IOException e) {
        close();
        throw new RuntimeException(
            "GHCND: failed reading sorted temp file for year=" + year, e);
      }
    }

    private void close() {
      if (closed) {
        return;
      }
      closed = true;
      try {
        reader.close();
      } catch (IOException ignore) {
        // best-effort close
      }
      tempFile.delete();
    }

    @Override public boolean hasNext() {
      return nextLine != null;
    }

    @Override public String next() {
      if (nextLine == null) {
        throw new java.util.NoSuchElementException();
      }
      String current = nextLine;
      advance();
      return current;
    }
  }

  /** True in DQ sample mode (GOVDATA_DQ=true as system property or env). */
  private static boolean isDqSampleMode() {
    String v = System.getProperty("GOVDATA_DQ");
    if (v == null) {
      v = System.getenv("GOVDATA_DQ");
    }
    return "true".equalsIgnoreCase(v);
  }

  /** Target number of stations to retain in a DQ sample (spread across the ID range). */
  private static final int DQ_TARGET_STATIONS = 150;

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

    BulkIterator(Iterator<String> lineIterator, String year) {
      this.year = year;
      this.lineIterator = lineIterator;
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
