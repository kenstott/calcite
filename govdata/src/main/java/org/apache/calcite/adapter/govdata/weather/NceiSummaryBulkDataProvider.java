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

import org.apache.calcite.adapter.file.StagingDirectory;
import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.ZipDownloadUtils;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.zip.GZIPInputStream;

/**
 * DataProvider for the NCEI Global Summary of the Month (GSOM) and Global Summary of the Year
 * (GSOY) bulk archives, the same station summaries NOAA CDO serves behind a 10,000-calls-per-day
 * token quota.
 *
 * <p>Each archive ({@code https://www.ncei.noaa.gov/data/gsom/archive/gsom-latest.tar.gz},
 * {@code .../gsoy/archive/gsoy-latest.tar.gz}) holds one CSV per station in a wide layout:
 * <pre>
 * "STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","PRCP","PRCP_ATTRIBUTES","TAVG",...
 * </pre>
 * The archive is downloaded once per calendar month into the raw cache and streamed from there
 * for every year; it is never held in memory. For the requested year each station's wide rows are
 * unpivoted to one row per station, period and datatype, in the layout the CDO responses had
 * ({@code station_id} with the {@code GHCND:} prefix, {@code date} as
 * {@code YYYY-MM-01T00:00:00} for GSOM or {@code YYYY-01-01T00:00:00} for GSOY).
 *
 * <p>The archives carry no state, so {@code state_fips} comes from
 * {@link GhcndStationStates}; stations that file does not assign to a state (for example the
 * Canadian stations CDO returned for border states) are not emitted.
 */
public class NceiSummaryBulkDataProvider implements DataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NceiSummaryBulkDataProvider.class);

  private static final String MONTHLY_TYPE = "cdo_monthly";
  private static final String ANNUAL_TYPE = "cdo_annual";
  private static final String MONTHLY_URL =
      "https://www.ncei.noaa.gov/data/gsom/archive/gsom-latest.tar.gz";
  private static final String ANNUAL_URL =
      "https://www.ncei.noaa.gov/data/gsoy/archive/gsoy-latest.tar.gz";

  private static final String ATTRIBUTES_SUFFIX = "_ATTRIBUTES";
  private static final String STATION_ID_PREFIX = "GHCND:";
  /** Columns before the first datatype: STATION, DATE, LATITUDE, LONGITUDE, ELEVATION, NAME. */
  private static final int FIRST_DATATYPE_COLUMN = 6;

  private static final Object DOWNLOAD_LOCK = new Object();

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

  @Override public Iterator<Map<String, Object>> fetch(
      EtlPipelineConfig config, Map<String, String> variables) throws IOException {
    // effective_year is the data year once the dimension's publication lag is applied
    String year = variables.get("effective_year");
    if (year == null) {
      throw new IOException("NceiSummaryBulkDataProvider requires 'effective_year'");
    }
    String type = variables.get("type");
    boolean monthly;
    String url;
    if (MONTHLY_TYPE.equals(type)) {
      monthly = true;
      url = MONTHLY_URL;
    } else if (ANNUAL_TYPE.equals(type)) {
      monthly = false;
      url = ANNUAL_URL;
    } else {
      throw new IOException("NceiSummaryBulkDataProvider: unsupported type '" + type
          + "' (expected " + MONTHLY_TYPE + " or " + ANNUAL_TYPE + ")");
    }

    HttpSourceConfig.RateLimitConfig rateLimit = config.getSource().getRateLimit();
    String archivePath = ensureArchive(url, monthly ? "gsom" : "gsoy");
    InputStream in = storageProvider().openInputStream(archivePath);
    TarArchiveInputStream tar;
    try {
      tar = new TarArchiveInputStream(
          new GZIPInputStream(new BufferedInputStream(in, 1 << 16), 1 << 16));
    } catch (IOException | RuntimeException e) {
      in.close();
      throw e;
    }
    return new ArchiveIterator(tar, year, monthly, rateLimit);
  }

  /** Downloads the archive into the raw cache once per calendar month and returns its path. */
  private String ensureArchive(String url, String dataset) throws IOException {
    StorageProvider sp = storageProvider();
    String weatherCache = sp.resolvePath(StorageProviderFactory.getGovDataCacheDir(), "weather");
    String dir = sp.resolvePath(weatherCache, "ncei_bulk");
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    String path = sp.resolvePath(dir,
        String.format("%s_%04d-%02d.tar.gz", dataset, now.getYear(), now.getMonthValue()));
    synchronized (DOWNLOAD_LOCK) {
      if (sp.exists(path) && sp.getMetadata(path).getSize() > 0) {
        LOGGER.info("NCEI bulk: using cached {}", path);
        return path;
      }
      LOGGER.info("NCEI bulk: downloading {} -> {}", url, path);
      File temp = StagingDirectory.newTempFile("ncei-" + dataset + "-", ".tar.gz");
      try {
        ZipDownloadUtils.downloadToFile(url, null, temp);
        // temp is the local staging copy of the download, uploaded to the cache and deleted
        try (InputStream src = new FileInputStream(temp)) { // storage-provider-guard:ignore
          sp.writeFile(path, src);
        }
      } finally {
        temp.delete();
      }
    }
    return path;
  }

  /** Splits one CSV line honouring double-quoted fields (which may contain commas). */
  static List<String> parseCsvLine(String line) {
    List<String> fields = new ArrayList<String>();
    StringBuilder cur = new StringBuilder();
    boolean quoted = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (quoted) {
        if (c == '"') {
          if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
            cur.append('"');
            i++;
          } else {
            quoted = false;
          }
        } else {
          cur.append(c);
        }
      } else if (c == '"') {
        quoted = true;
      } else if (c == ',') {
        fields.add(cur.toString());
        cur.setLength(0);
      } else {
        cur.append(c);
      }
    }
    fields.add(cur.toString());
    return fields;
  }

  /**
   * Streams the archive entry by entry, reading each station CSV line by line and queuing the
   * unpivoted rows of the requested year; heap holds one station's rows at a time.
   */
  private static final class ArchiveIterator implements Iterator<Map<String, Object>> {
    private final TarArchiveInputStream tar;
    private final String year;
    private final boolean monthly;
    private final HttpSourceConfig.RateLimitConfig rateLimit;
    private final Queue<Map<String, Object>> queue = new ArrayDeque<Map<String, Object>>();
    private boolean done;
    private long stations;
    private long skippedNoState;
    private long rows;

    ArchiveIterator(TarArchiveInputStream tar, String year, boolean monthly,
        HttpSourceConfig.RateLimitConfig rateLimit) {
      this.tar = tar;
      this.year = year;
      this.monthly = monthly;
      this.rateLimit = rateLimit;
    }

    @Override public boolean hasNext() {
      try {
        while (queue.isEmpty() && !done) {
          readNextStation();
        }
      } catch (IOException e) {
        closeQuietly();
        throw new RuntimeException("NCEI bulk: failed reading archive for year=" + year, e);
      }
      return !queue.isEmpty();
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return queue.poll();
    }

    private void readNextStation() throws IOException {
      TarArchiveEntry entry = tar.getNextEntry();
      if (entry == null) {
        done = true;
        LOGGER.info("NCEI bulk year={}: {} station files, {} without a US state, {} rows emitted",
            year, stations, skippedNoState, rows);
        tar.close();
        return;
      }
      if (entry.isDirectory() || !entry.getName().endsWith(".csv")) {
        return;
      }
      stations++;
      // A reader per entry: read-ahead text buffered from a station file that is abandoned early
      // must not reach the next file. It is not closed, as that would close the archive stream.
      BufferedReader entryReader =
          new BufferedReader(new InputStreamReader(tar, StandardCharsets.UTF_8));
      String headerLine = entryReader.readLine();
      if (headerLine == null) {
        return;
      }
      List<String> header = parseCsvLine(headerLine);
      Map<String, Integer> attrIndex = new LinkedHashMap<String, Integer>();
      for (int i = FIRST_DATATYPE_COLUMN; i < header.size(); i++) {
        String name = header.get(i);
        if (name.endsWith(ATTRIBUTES_SUFFIX)) {
          attrIndex.put(name.substring(0, name.length() - ATTRIBUTES_SUFFIX.length()), i);
        }
      }
      String stateFips = null;
      boolean stateResolved = false;
      String line;
      while ((line = entryReader.readLine()) != null) {
        List<String> f = parseCsvLine(line);
        if (f.size() < 2 || !f.get(1).startsWith(year)) {
          continue;
        }
        String station = f.get(0);
        if (!stateResolved) {
          stateFips = GhcndStationStates.stateFips(station, rateLimit);
          stateResolved = true;
          if (stateFips == null) {
            skippedNoState++;
          }
        }
        if (stateFips == null) {
          return;
        }
        emitRows(f, header, attrIndex, station, stateFips);
      }
    }

    private void emitRows(List<String> f, List<String> header, Map<String, Integer> attrIndex,
        String station, String stateFips) {
      String period = f.get(1);
      int yearInt = Integer.parseInt(period.substring(0, 4));
      Integer month = null;
      String date;
      if (monthly) {
        month = Integer.valueOf(period.substring(5, 7));
        date = period.substring(0, 7) + "-01T00:00:00";
      } else {
        date = period.substring(0, 4) + "-01-01T00:00:00";
      }
      for (int i = FIRST_DATATYPE_COLUMN; i < header.size() && i < f.size(); i++) {
        String name = header.get(i);
        if (name.endsWith(ATTRIBUTES_SUFFIX)) {
          continue;
        }
        String raw = f.get(i).trim();
        if (raw.isEmpty()) {
          continue;
        }
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("state_fips", stateFips);
        row.put("station_id", STATION_ID_PREFIX + station);
        row.put("year", yearInt);
        if (monthly) {
          row.put("month", month);
        }
        row.put("datatype", name);
        row.put("value", Double.valueOf(raw));
        Integer ai = attrIndex.get(name);
        row.put("attributes", ai != null && ai < f.size() ? f.get(ai) : null);
        row.put("date", date);
        queue.add(row);
        rows++;
      }
    }

    private void closeQuietly() {
      try {
        tar.close();
      } catch (IOException ignore) {
        // already failing on a read error
      }
      done = true;
    }
  }
}
