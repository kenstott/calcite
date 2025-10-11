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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Downloads and manages U.S. Census TIGER/Line geographic boundary files.
 *
 * <p>TIGER/Line Shapefiles are free, publicly available geographic boundary
 * files from the U.S. Census Bureau. No registration required.
 *
 * <p>Available datasets:
 * <ul>
 *   <li>States and equivalent entities</li>
 *   <li>Counties and equivalent entities</li>
 *   <li>Places (cities, towns, CDPs)</li>
 *   <li>ZIP Code Tabulation Areas (ZCTAs)</li>
 *   <li>Congressional districts</li>
 *   <li>School districts</li>
 * </ul>
 *
 * <p>Data is available from: https://www2.census.gov/geo/tiger/
 */
public class TigerDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TigerDataDownloader.class);

  private static final String TIGER_BASE_URL = "https://www2.census.gov/geo/tiger";

  /**
   * TIGER/Line Shapefile Support:
   * - 2010: Supported (different URL structure with /2010/ subdirectory and file suffixes)
   * - 2011+: Supported (standard TIGER{year} structure)
   * - Pre-2010: Not currently supported (uses incompatible formats and directory structures)
   */

  /**
   * Get the TIGER directory path for a specific year.
   * TIGER data URL structure varies by year.
   */
  private String getTigerYearPath(int year) {
    return "TIGER" + year;
  }

  /**
   * Get the subdirectory path for TIGER 2010 data.
   * 2010 data has an additional /2010/ subdirectory.
   */
  private String getTiger2010Subdir(int year) {
    return (year == 2010) ? "/2010" : "";
  }

  /**
   * Get the file suffix for TIGER data based on year.
   * 2010 uses different suffixes (e.g., state10, county10, place10).
   */
  private String getTigerFileSuffix(int year, String type) {
    if (year == 2010) {
      return type + "10";
    }
    return type;
  }

  private final File cacheDir;
  private final List<Integer> dataYears;
  private final boolean autoDownload;
  private final StorageProvider storageProvider;
  private final GeoCacheManifest cacheManifest;

  /**
   * Constructor with year list and StorageProvider (matching ECON pattern).
   */
  public TigerDataDownloader(File cacheDir, List<Integer> dataYears, boolean autoDownload,
      StorageProvider storageProvider) {
    this(cacheDir, dataYears, autoDownload, storageProvider, null);
  }

  /**
   * Constructor with year list, StorageProvider, and cacheManifest.
   */
  public TigerDataDownloader(File cacheDir, List<Integer> dataYears, boolean autoDownload,
      StorageProvider storageProvider, GeoCacheManifest cacheManifest) {
    this.cacheDir = cacheDir;
    this.dataYears = dataYears;
    this.autoDownload = autoDownload;
    this.storageProvider = storageProvider;
    this.cacheManifest = cacheManifest;

    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }

    LOGGER.info("TIGER data downloader initialized for years {} in directory: {}",
        dataYears, cacheDir);
  }

  /**
   * Constructor with year list (backward compatibility).
   */
  public TigerDataDownloader(File cacheDir, List<Integer> dataYears, boolean autoDownload) {
    this(cacheDir, dataYears, autoDownload, null);
  }

  /**
   * Backward compatibility constructor with single year.
   */
  public TigerDataDownloader(File cacheDir, int dataYear, boolean autoDownload) {
    this(cacheDir, Arrays.asList(dataYear), autoDownload, null);
  }

  /**
   * Download all TIGER data for the specified year range (matching ECON pattern).
   */
  public void downloadAll(int startYear, int endYear) throws IOException {
    // Download all datasets year by year to match expected directory structure
    for (int year = startYear; year <= endYear; year++) {
      // Download states
      downloadStatesForYear(year);

      // Download counties
      downloadCountiesForYear(year);

      // Download places for key states (CA, TX, NY, FL)
      downloadPlacesForYear(year, "06"); // California
      downloadPlacesForYear(year, "48"); // Texas
      downloadPlacesForYear(year, "36"); // New York
      downloadPlacesForYear(year, "12"); // Florida

      // Download ZCTAs
      downloadZctasForYear(year);

      // Download census tracts for key states
      downloadCensusTractsForYear(year);

      // Download block groups for key states
      downloadBlockGroupsForYear(year);

      // Download CBSAs
      downloadCbsasForYear(year);

      // Download congressional districts
      try {
        downloadCongressionalDistrictsForYear(year);
      } catch (Exception e) {
        LOGGER.warn("Failed to download congressional districts for year {}: {}", year, e.getMessage());
      }

      // Download school districts
      try {
        downloadSchoolDistrictsForYear(year);
      } catch (Exception e) {
        LOGGER.warn("Failed to download school districts for year {}: {}", year, e.getMessage());
      }
    }

    LOGGER.info("TIGER data download completed for years {} to {}", startYear, endYear);
  }

  /**
   * Download state boundary shapefiles for all configured years.
   */
  public void downloadStates() throws IOException {
    for (int year : dataYears) {
      downloadStatesForYear(year);
    }
  }

  /**
   * Download state boundary shapefile for the first configured year.
   * For backward compatibility with tests.
   */
  public File downloadStatesFirstYear() throws IOException {
    if (dataYears.isEmpty()) {
      return null;
    }
    return downloadStatesForYear(dataYears.get(0));
  }

  /**
   * Download state boundary shapefile for a specific year.
   */
  public File downloadStatesForYear(int year) throws IOException {
    String fileSuffix = getTigerFileSuffix(year, "state");
    String filename = String.format("tl_%d_us_%s.zip", year, fileSuffix);
    String url = String.format("%s/%s/STATE%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);

    // Create year-partitioned directory structure
    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "states");
    File zipFile = new File(targetDir, filename);

    if (zipFile.exists()) {
      LOGGER.info("States shapefile already exists for year {}: {}", year, zipFile);
      return targetDir;
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. States shapefile not found for year {}: {}", year, zipFile);
      return null;
    }

    LOGGER.info("Downloading states shapefile for year {} from: {}", year, url);
    targetDir.mkdirs();

    try {
      downloadFile(url, zipFile);
      extractZipFile(zipFile, targetDir);
      return targetDir;
    } catch (IOException e) {
      if (e.getMessage().contains("404")) {
        LOGGER.warn("TIGER data not available for year {} at URL: {} - skipping", year, url);
        return null;
      }
      throw e;
    }
  }

  /**
   * Download county boundary shapefiles for all configured years.
   */
  public void downloadCounties() throws IOException {
    for (int year : dataYears) {
      downloadCountiesForYear(year);
    }
  }

  /**
   * Download county boundary shapefile for the first configured year.
   * For backward compatibility with tests.
   */
  public File downloadCountiesFirstYear() throws IOException {
    if (dataYears.isEmpty()) {
      return null;
    }
    return downloadCountiesForYear(dataYears.get(0));
  }

  /**
   * Download county boundary shapefile for a specific year.
   */
  public File downloadCountiesForYear(int year) throws IOException {
    String fileSuffix = getTigerFileSuffix(year, "county");
    String filename = String.format("tl_%d_us_%s.zip", year, fileSuffix);
    String url = String.format("%s/%s/COUNTY%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);

    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "counties");
    File zipFile = new File(targetDir, filename);

    if (zipFile.exists()) {
      LOGGER.info("Counties shapefile already exists for year {}: {}", year, zipFile);
      return targetDir;
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Counties shapefile not found for year {}: {}", year, zipFile);
      return null;
    }

    LOGGER.info("Downloading counties shapefile for year {} from: {}", year, url);
    targetDir.mkdirs();

    try {
      downloadFile(url, zipFile);
      extractZipFile(zipFile, targetDir);
      return targetDir;
    } catch (IOException e) {
      if (e.getMessage().contains("404")) {
        LOGGER.warn("TIGER data not available for year {} at URL: {} - skipping", year, url);
        return null;
      }
      throw e;
    }
  }

  /**
   * Download places (cities, towns) boundary shapefiles for all configured years.
   */
  public void downloadPlaces() throws IOException {
    // Download places for all states (simplified for now)
    for (int year : dataYears) {
      downloadAllPlacesForYear(year);
    }
  }

  /**
   * Download places (cities, towns) boundary shapefile.
   * Note: Places are downloaded by state FIPS code.
   */
  public File downloadPlacesForYear(int year, String stateFips) throws IOException {
    String fileSuffix = getTigerFileSuffix(year, "place");
    String filename = String.format("tl_%d_%s_%s.zip", year, stateFips, fileSuffix);
    String url = String.format("%s/%s/PLACE%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);

    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "places/" + stateFips);
    File zipFile = new File(targetDir, filename);

    if (zipFile.exists()) {
      LOGGER.info("Places shapefile already exists for state {}: {}", stateFips, zipFile);
      return targetDir;
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Places shapefile not found for state {}: {}",
          stateFips, zipFile);
      return null;
    }

    LOGGER.info("Downloading places shapefile for state {} from: {}", stateFips, url);
    targetDir.mkdirs();
    downloadFile(url, zipFile);
    extractZipFile(zipFile, targetDir);

    return targetDir;
  }

  /**
   * Download all places for all states for a specific year.
   */
  private void downloadAllPlacesForYear(int year) throws IOException {
    // Download places for all 50 states + DC + territories
    String[] stateFipsCodes = {
        "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
        "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
        "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
        "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
        "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
        "56", "60", "66", "69", "72", "78" // Territories
    };

    for (String stateFips : stateFipsCodes) {
      try {
        downloadPlacesForYear(year, stateFips);
      } catch (Exception e) {
        LOGGER.warn("Failed to download places for state {} year {}: {}", stateFips, year, e.getMessage());
      }
    }
  }

  /**
   * Download ZIP Code Tabulation Areas (ZCTAs) shapefiles for all configured years.
   */
  public void downloadZctas() throws IOException {
    for (int year : dataYears) {
      downloadZctasForYear(year);
    }
  }

  /**
   * Download ZIP Code Tabulation Areas (ZCTAs) shapefile for a specific year.
   */
  public File downloadZctasForYear(int year) throws IOException {
    // ZCTA5 (5-digit ZCTAs) were used in 2010, ZCTA520 is used in later years
    String zctaType = (year == 2010) ? "ZCTA5" : "ZCTA520";
    String fileSuffix = (year == 2010) ? "zcta510" : "zcta520";
    String filename = String.format("tl_%d_us_%s.zip", year, fileSuffix);
    String url = String.format("%s/%s/%s%s/%s", TIGER_BASE_URL, getTigerYearPath(year), zctaType, getTiger2010Subdir(year), filename);

    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "zctas");
    File zipFile = new File(targetDir, filename);

    if (zipFile.exists()) {
      LOGGER.info("ZCTAs shapefile already exists: {}", zipFile);
      return targetDir;
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. ZCTAs shapefile not found: {}", zipFile);
      return null;
    }

    LOGGER.info("Downloading ZCTAs shapefile from: {}", url);
    LOGGER.warn("Note: ZCTA file is large (~200MB), this may take a while...");
    targetDir.mkdirs();

    try {
      downloadFile(url, zipFile);
      extractZipFile(zipFile, targetDir);
      return targetDir;
    } catch (IOException e) {
      if (e.getMessage().contains("404")) {
        LOGGER.warn("TIGER ZCTA data not available for year {} at URL: {} - skipping", year, url);
        return null;
      }
      throw e;
    }
  }

  /**
   * Download congressional districts shapefiles for all configured years.
   */
  public void downloadCongressionalDistricts() throws IOException {
    for (int year : dataYears) {
      downloadCongressionalDistrictsForYear(year);
    }
  }

  /**
   * Download congressional districts shapefile for a specific year.
   * Congressional districts are provided as state-level files, we need to download all states.
   */
  public File downloadCongressionalDistrictsForYear(int year) throws IOException {
    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "congressional_districts");

    // Check if we already have CD shapefile
    if (targetDir.exists() && targetDir.listFiles((dir, name) -> name.endsWith(".shp")).length > 0) {
      LOGGER.info("Congressional districts shapefiles already exist for year {}", year);
      return targetDir;
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Congressional districts not found for year {}", year);
      return null;
    }

    // Calculate correct Congress number: ((year - 1789) / 2) + 1
    // Each Congress serves 2 years starting in odd years
    // 116th Congress: 2019-2020, 117th: 2021-2022, 118th: 2023-2024, etc.
    int congressNum = ((year - 1789) / 2) + 1;

    // Congressional districts are provided as a nationwide file (not per-state)
    String filename = String.format("tl_%d_us_cd%d.zip", year, congressNum);

    // 2010 has a different directory structure with congress subdirectory
    String url;
    if (year == 2010) {
      url = String.format("%s/%s/CD/%d/%s", TIGER_BASE_URL, getTigerYearPath(year), congressNum, filename);
    } else {
      url = String.format("%s/%s/CD/%s", TIGER_BASE_URL, getTigerYearPath(year), filename);
    }

    File zipFile = new File(targetDir, filename);

    targetDir.mkdirs();
    if (!zipFile.exists()) {
      try {
        LOGGER.info("Downloading CD shapefile for year {} (Congress {})", year, congressNum);
        downloadFile(url, zipFile);
        extractZipFile(zipFile, targetDir);
      } catch (IOException e) {
        if (e.getMessage().contains("404")) {
          LOGGER.warn("Congressional districts data not available for year {} (Congress {}) - skipping", year, congressNum);
          return null;
        }
        LOGGER.warn("Failed to download CD for year {}: {}", year, e.getMessage());
        throw e;
      }
    }

    return targetDir;
  }

  /**
   * Download a file from a URL.
   */
  private void downloadFile(String urlString, File outputFile) throws IOException {
    URI uri = URI.create(urlString);
    URL url = uri.toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(10000);
    conn.setReadTimeout(60000);

    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      throw new IOException("Failed to download file. HTTP response code: " + responseCode);
    }

    long contentLength = conn.getContentLengthLong();
    LOGGER.info("Downloading {} ({} MB)", outputFile.getName(), contentLength / (1024 * 1024));

    try (BufferedInputStream in = new BufferedInputStream(conn.getInputStream());
         FileOutputStream out = new FileOutputStream(outputFile)) {

      byte[] buffer = new byte[8192];
      int bytesRead;
      long totalBytesRead = 0;
      long lastLogTime = System.currentTimeMillis();

      while ((bytesRead = in.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
        totalBytesRead += bytesRead;

        // Log progress every 5 seconds
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastLogTime > 5000) {
          int percentComplete = (int) ((totalBytesRead * 100) / contentLength);
          LOGGER.info("Download progress: {}% ({} MB / {} MB)",
              percentComplete,
              totalBytesRead / (1024 * 1024),
              contentLength / (1024 * 1024));
          lastLogTime = currentTime;
        }
      }
    }

    LOGGER.info("Download complete: {}", outputFile);
  }

  /**
   * Extract a ZIP file to a directory.
   */
  private void extractZipFile(File zipFile, File outputDir) throws IOException {
    LOGGER.info("Extracting ZIP file: {}", zipFile);

    try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFile.toPath()))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        File outputFile = new File(outputDir, entry.getName());

        if (entry.isDirectory()) {
          outputFile.mkdirs();
        } else {
          outputFile.getParentFile().mkdirs();

          try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = zis.read(buffer)) != -1) {
              fos.write(buffer, 0, bytesRead);
            }
          }

          LOGGER.debug("Extracted: {}", outputFile.getName());
        }

        zis.closeEntry();
      }
    }

    LOGGER.info("Extraction complete to: {}", outputDir);
  }

  /**
   * Download census tracts shapefiles for all configured years.
   */
  public void downloadCensusTracts() throws IOException {
    for (int year : dataYears) {
      downloadCensusTractsForYear(year);
    }
  }

  /**
   * Download census tracts shapefile for a specific year.
   * Census tracts are organized by state, so we'll download for selected states.
   */
  public File downloadCensusTractsForYear(int year) throws IOException {
    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "census_tracts");

    // Check if we already have census tract data
    if (targetDir.exists() && targetDir.listFiles() != null && targetDir.listFiles().length > 0) {
      LOGGER.info("Census tracts already downloaded for year {}: {}", year, targetDir);
      return targetDir;
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Census tracts not found for year {}: {}", year, targetDir);
      return null;
    }

    targetDir.mkdirs();

    // Download census tracts for selected states only
    // Using same states as we do for places: CA(06), TX(48), NY(36), FL(12)
    String[] stateFips = {"06", "48", "36", "12"};

    for (String fips : stateFips) {
      String fileSuffix = getTigerFileSuffix(year, "tract");
      String filename = String.format("tl_%d_%s_%s.zip", year, fips, fileSuffix);
      String url = String.format("%s/%s/TRACT%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);

      File stateDir = new File(targetDir, fips);
      File zipFile = new File(stateDir, filename);

      if (zipFile.exists()) {
        LOGGER.info("Census tracts shapefile already exists for state {}: {}", fips, zipFile);
        continue;
      }

      LOGGER.info("Downloading census tracts shapefile for state {} year {} from: {}", fips, year, url);
      stateDir.mkdirs();

      try {
        downloadFile(url, zipFile);
        extractZipFile(zipFile, stateDir);
      } catch (IOException e) {
        if (e.getMessage().contains("404")) {
          LOGGER.warn("TIGER census tract data not available for state {} year {} at URL: {} - skipping", fips, year, url);
        } else {
          LOGGER.warn("Failed to download census tracts for state {}: {}", fips, e.getMessage());
        }
        // Continue with other states even if one fails
      }
    }

    return targetDir;
  }

  /**
   * Download block groups shapefiles for all configured years.
   */
  public void downloadBlockGroups() throws IOException {
    for (int year : dataYears) {
      downloadBlockGroupsForYear(year);
    }
  }

  /**
   * Download block groups shapefile for a specific year.
   * Block groups are organized by state, so we'll download for selected states.
   */
  public File downloadBlockGroupsForYear(int year) throws IOException {
    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "block_groups");

    // Check if we already have block group data
    if (targetDir.exists() && targetDir.listFiles() != null && targetDir.listFiles().length > 0) {
      LOGGER.info("Block groups already downloaded for year {}: {}", year, targetDir);
      return targetDir;
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Block groups not found for year {}: {}", year, targetDir);
      return null;
    }

    targetDir.mkdirs();

    // Download block groups for selected states only
    // Using same states as we do for places: CA(06), TX(48), NY(36), FL(12)
    String[] stateFips = {"06", "48", "36", "12"};

    for (String fips : stateFips) {
      String fileSuffix = getTigerFileSuffix(year, "bg");
      String filename = String.format("tl_%d_%s_%s.zip", year, fips, fileSuffix);
      String url = String.format("%s/%s/BG%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);

      File stateDir = new File(targetDir, fips);
      File zipFile = new File(stateDir, filename);

      if (zipFile.exists()) {
        LOGGER.info("Block groups shapefile already exists for state {}: {}", fips, zipFile);
        continue;
      }

      LOGGER.info("Downloading block groups shapefile for state {} year {} from: {}", fips, year, url);
      stateDir.mkdirs();

      try {
        downloadFile(url, zipFile);
        extractZipFile(zipFile, stateDir);
      } catch (IOException e) {
        if (e.getMessage().contains("404")) {
          LOGGER.warn("TIGER block group data not available for state {} year {} at URL: {} - skipping", fips, year, url);
        } else {
          LOGGER.warn("Failed to download block groups for state {}: {}", fips, e.getMessage());
        }
        // Continue with other states even if one fails
      }
    }

    return targetDir;
  }

  /**
   * Download Core Based Statistical Areas (CBSAs) shapefiles for all configured years.
   */
  public void downloadCbsas() throws IOException {
    for (int year : dataYears) {
      downloadCbsasForYear(year);
    }
  }

  /**
   * Download Core Based Statistical Areas (CBSAs) shapefile for a specific year.
   */
  public File downloadCbsasForYear(int year) throws IOException {
    // 2010 has special naming: cbsa10 instead of cbsa
    String cbsaSuffix = (year == 2010) ? "10" : "";
    String filename = String.format("tl_%d_us_cbsa%s.zip", year, cbsaSuffix);
    String url = String.format("%s/%s/CBSA%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);

    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "cbsa");
    File zipFile = new File(targetDir, filename);

    if (zipFile.exists()) {
      LOGGER.info("CBSA shapefile already exists for year {}: {}", year, zipFile);
      return targetDir;
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. CBSA shapefile not found for year {}: {}", year, zipFile);
      return null;
    }

    LOGGER.info("Downloading CBSA shapefile for year {} from: {}", year, url);
    targetDir.mkdirs();
    downloadFile(url, zipFile);
    extractZipFile(zipFile, targetDir);

    return targetDir;
  }

  /**
   * Get the cache directory.
   */
  public File getCacheDir() {
    return cacheDir;
  }

  /**
   * Check if auto-download is enabled.
   */
  public boolean isAutoDownload() {
    return autoDownload;
  }

  /**
   * Download school districts shapefiles for all configured years.
   */
  public void downloadSchoolDistricts() throws IOException {
    for (int year : dataYears) {
      downloadSchoolDistrictsForYear(year);
    }
  }

  /**
   * Download school districts shapefile for a specific year.
   */
  public File downloadSchoolDistrictsForYear(int year) throws IOException {
    return downloadSchoolDistrictsForYear(year, null);
  }

  /**
   * Download school districts shapefile for a specific year.
   * @param year The year to download
   * @param parquetPath Optional parquet path - if provided and file exists, skip download
   */
  public File downloadSchoolDistrictsForYear(int year, String parquetPath) throws IOException {
    // Check if parquet already exists (skip raw download if converted file exists)
    if (parquetPath != null && shouldSkipDownload("school_districts", year, parquetPath)) {
      LOGGER.info("Skipping school districts download for year {} - parquet already exists", year);
      File yearDir = new File(cacheDir, String.format("year=%d", year));
      return new File(yearDir, "school_districts");
    }

    // School districts are organized by state, so we'll download for selected states
    File yearDir = new File(cacheDir, String.format("year=%d", year));
    File targetDir = new File(yearDir, "school_districts");

    // Check if we already have school district shapefiles (not just empty directories)
    if (targetDir.exists()) {
      boolean hasShapefiles = false;
      File[] stateDirs = targetDir.listFiles(File::isDirectory);
      if (stateDirs != null) {
        for (File stateDir : stateDirs) {
          File[] shpFiles = stateDir.listFiles((dir, name) -> name.endsWith(".shp"));
          if (shpFiles != null && shpFiles.length > 0) {
            hasShapefiles = true;
            break;
          }
        }
      }
      if (hasShapefiles) {
        LOGGER.info("School districts already downloaded for year {}: {}", year, targetDir);
        return targetDir;
      }
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. School districts not found for year {}: {}", year, targetDir);
      return null;
    }

    targetDir.mkdirs();

    // Download school districts for selected states only
    String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL

    for (String fips : stateFips) {
      // Try different school district types
      String[] districtTypes = {"unsd", "elsd", "scsd"}; // Unified, Elementary, Secondary

      for (String type : districtTypes) {
        // 2010 has different naming: subdirectory "2010" and type suffix "10" (e.g., unsd10)
        String typeSuffix = (year == 2010) ? type + "10" : type;
        String filename = String.format("tl_%d_%s_%s.zip", year, fips, typeSuffix);
        String urlPath = type.toUpperCase();
        // 2010 has additional subdirectory level
        String url = (year == 2010)
            ? String.format("%s/%s/%s/2010/%s", TIGER_BASE_URL, getTigerYearPath(year), urlPath, filename)
            : String.format("%s/%s/%s/%s", TIGER_BASE_URL, getTigerYearPath(year), urlPath, filename);

        File stateDir = new File(targetDir, fips);
        File zipFile = new File(stateDir, filename);

        if (zipFile.exists()) {
          LOGGER.debug("School district shapefile already exists for state {} type {}: {}", fips, type, zipFile);
          continue;
        }

        LOGGER.info("Downloading school district shapefile for state {} type {} year {} from: {}", fips, type, year, url);
        stateDir.mkdirs();

        try {
          downloadFile(url, zipFile);
          extractZipFile(zipFile, stateDir);
        } catch (IOException e) {
          LOGGER.debug("Failed to download school districts for state {} type {}: {}", fips, type, e.getMessage());
          // Continue with other types/states even if one fails
        }
      }
    }

    // Check if any shapefiles were successfully downloaded
    boolean hasShapefiles = false;
    File[] stateDirs = targetDir.listFiles(File::isDirectory);
    if (stateDirs != null) {
      for (File stateDir : stateDirs) {
        File[] shpFiles = stateDir.listFiles((dir, name) -> name.endsWith(".shp"));
        if (shpFiles != null && shpFiles.length > 0) {
          hasShapefiles = true;
          break;
        }
      }
    }

    if (!hasShapefiles) {
      LOGGER.warn("No school district shapefiles were successfully downloaded for year {}", year);
      return null;
    }

    return targetDir;
  }

  /**
   * Helper to check if we should skip downloading raw file (manifest says already converted).
   * Does NOT check parquet existence - that happens later during conversion.
   */
  private boolean shouldSkipDownload(String dataType, int year, String parquetPath) {
    // Only check manifest - if it says converted, we can skip download
    // We don't check parquet existence here because we want the raw file regardless
    // (for potential re-processing, debugging, etc.)
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("type", dataType);
      if (cacheManifest.isParquetConverted(dataType, year, params)) {
        LOGGER.debug("Manifest indicates parquet already converted, skipping download");
        return true;
      }
    }

    return false;
  }

  /**
   * Convert TIGER shapefiles to Parquet format (matching ECON pattern).
   * This is a placeholder that should invoke ShapefileToParquetConverter.
   */
  public void convertToParquet(File sourceDir, String targetFilePath) throws IOException {
    // Extract year from path (pattern: year=YYYY)
    int year = extractYearFromPath(targetFilePath);

    // Extract data type from filename
    String fileName = targetFilePath.substring(targetFilePath.lastIndexOf("/") + 1);
    String dataType = fileName.replace(".parquet", "");

    // Check manifest first (avoids S3 check)
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("type", dataType);
      if (cacheManifest.isParquetConverted(dataType, year, params)) {
        LOGGER.debug("Parquet already converted per manifest: {}", targetFilePath);
        return;
      }
    }

    // Defensive check if file already exists (for backfill/legacy data)
    if (storageProvider != null && storageProvider.exists(targetFilePath)) {
      LOGGER.debug("Target parquet file already exists, skipping: {}", targetFilePath);
      // Update manifest since file exists but wasn't tracked
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("type", dataType);
        cacheManifest.markParquetConverted(dataType, year, params, targetFilePath);
        cacheManifest.save(cacheDir.getAbsolutePath());
      }
      return;
    }

    if (storageProvider == null) {
      LOGGER.error("StorageProvider is null, cannot convert to Parquet");
      return;
    }

    LOGGER.info("Converting TIGER data from {} to parquet: {}", sourceDir, targetFilePath);

    // Use ShapefileToParquetConverter for actual conversion
    ShapefileToParquetConverter converter = new ShapefileToParquetConverter(storageProvider);

    LOGGER.info("Calling converter for table type: {} with source: {}", dataType, sourceDir);

    try {
      // Convert based on the table type
      converter.convertSingleShapefileType(sourceDir, targetFilePath, dataType);

      // Mark parquet conversion complete in manifest
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("type", dataType);
        cacheManifest.markParquetConverted(dataType, year, params, targetFilePath);
        cacheManifest.save(cacheDir.getAbsolutePath());
      }
    } catch (Exception e) {
      LOGGER.error("Error converting {} to Parquet", dataType, e);
    }
  }

  /**
   * Extract year from path containing year=YYYY pattern.
   */
  private int extractYearFromPath(String path) {
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("year=(\\d{4})");
    java.util.regex.Matcher matcher = pattern.matcher(path);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group(1));
    }
    throw new IllegalArgumentException("Could not extract year from path: " + path);
  }

  /**
   * Check if a shapefile exists in the cache.
   */
  public boolean isShapefileAvailable(String category) {
    File dir = new File(cacheDir, category);
    if (!dir.exists()) {
      return false;
    }

    // Check for .shp file
    File[] shpFiles = dir.listFiles((d, name) -> name.endsWith(".shp"));
    return shpFiles != null && shpFiles.length > 0;
  }
}
