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
import org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry;

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

  private final String cacheDir;
  private final List<Integer> dataYears;
  private final boolean autoDownload;
  private final StorageProvider storageProvider;
  private final GeoCacheManifest cacheManifest;
  private final String operatingDirectory;

  /**
   * Constructor with year list and StorageProvider (matching ECON pattern).
   */
  public TigerDataDownloader(String cacheDir, List<Integer> dataYears, boolean autoDownload,
      StorageProvider storageProvider) {
    this(cacheDir, dataYears, autoDownload, storageProvider, null);
  }

  /**
   * Constructor with year list, StorageProvider, and cacheManifest.
   */
  public TigerDataDownloader(String cacheDir, List<Integer> dataYears, boolean autoDownload,
      StorageProvider storageProvider, GeoCacheManifest cacheManifest) {
    this(cacheDir, cacheDir, dataYears, autoDownload, storageProvider, cacheManifest);
  }

  /**
   * Constructor with separate cache and operating directories (standardized naming).
   */
  public TigerDataDownloader(String cacheDir, String operatingDirectory, List<Integer> dataYears,
      boolean autoDownload, StorageProvider storageProvider, GeoCacheManifest cacheManifest) {
    this.cacheDir = cacheDir;
    this.operatingDirectory = operatingDirectory;
    this.dataYears = dataYears;
    this.autoDownload = autoDownload;
    this.storageProvider = storageProvider;
    this.cacheManifest = cacheManifest;

    LOGGER.info("TIGER data downloader initialized for years {} in directory: {}",
        dataYears, cacheDir);
  }

  /**
   * Backward compatibility constructor - delegates to String-based constructor.
   */
  public TigerDataDownloader(File cacheDir, List<Integer> dataYears, boolean autoDownload,
      StorageProvider storageProvider) {
    this(cacheDir.getAbsolutePath(), dataYears, autoDownload, storageProvider, null);
  }

  /**
   * Backward compatibility constructor - delegates to String-based constructor.
   */
  public TigerDataDownloader(File cacheDir, List<Integer> dataYears, boolean autoDownload,
      StorageProvider storageProvider, GeoCacheManifest cacheManifest) {
    this(cacheDir.getAbsolutePath(), cacheDir.getAbsolutePath(), dataYears, autoDownload, storageProvider, cacheManifest);
  }

  /**
   * Backward compatibility constructor - delegates to String-based constructor.
   */
  public TigerDataDownloader(File cacheDir, String operatingDirectory, List<Integer> dataYears,
      boolean autoDownload, StorageProvider storageProvider, GeoCacheManifest cacheManifest) {
    this(cacheDir.getAbsolutePath(), operatingDirectory, dataYears, autoDownload, storageProvider, cacheManifest);
  }

  /**
   * Constructor with year list (backward compatibility).
   */
  public TigerDataDownloader(File cacheDir, List<Integer> dataYears, boolean autoDownload) {
    this(cacheDir.getAbsolutePath(), dataYears, autoDownload, null);
  }

  /**
   * Backward compatibility constructor with single year.
   */
  public TigerDataDownloader(File cacheDir, int dataYear, boolean autoDownload) {
    this(cacheDir.getAbsolutePath(), Arrays.asList(dataYear), autoDownload, null);
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

    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDir, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "states");
    String zipCachePath = storageProvider.resolvePath(cachePath, filename);

    // Check manifest first for cached raw shapefiles
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      if (cacheManifest.isCached("states", year, params)) {
        LOGGER.debug("States shapefile cached per manifest for year {}", year);
        return downloadCacheToTemp(cachePath, "states_" + year);
      }
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. States shapefile not found for year {}: {}", year, zipCachePath);
      return null;
    }

    LOGGER.info("Downloading states shapefile for year {} from: {}", year, url);

    try {
      // Download and extract to temp directory
      File tempDir = Files.createTempDirectory("tiger-states-" + year + "-").toFile();
      File zipFile = new File(tempDir, filename);

      downloadFile(url, zipFile);
      extractZipFile(zipFile, tempDir);

      // Upload extracted files to cache storage
      uploadDirectoryToStorage(tempDir, cachePath);

      // Mark as cached in manifest
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        cacheManifest.markCached("states", year, params, cachePath, zipFile.length());
        cacheManifest.save(this.operatingDirectory);
      }

      // Keep temp directory for immediate use by converter
      // Note: Caller is responsible for cleanup
      return tempDir;
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

    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDir, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "counties");
    String zipCachePath = storageProvider.resolvePath(cachePath, filename);

    // Check manifest first for cached raw shapefiles
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      if (cacheManifest.isCached("counties", year, params)) {
        LOGGER.debug("Counties shapefile cached per manifest for year {}", year);
        return downloadCacheToTemp(cachePath, "counties_" + year);
      }
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Counties shapefile not found for year {}: {}", year, zipCachePath);
      return null;
    }

    LOGGER.info("Downloading counties shapefile for year {} from: {}", year, url);

    try {
      // Download and extract to temp directory
      File tempDir = Files.createTempDirectory("tiger-counties-" + year + "-").toFile();
      File zipFile = new File(tempDir, filename);

      downloadFile(url, zipFile);
      extractZipFile(zipFile, tempDir);

      // Upload extracted files to cache storage
      uploadDirectoryToStorage(tempDir, cachePath);

      // Mark as cached in manifest
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        cacheManifest.markCached("counties", year, params, cachePath, zipFile.length());
        cacheManifest.save(this.operatingDirectory);
      }

      // Keep temp directory for immediate use by converter
      return tempDir;
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

    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDir, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "places");
    cachePath = storageProvider.resolvePath(cachePath, stateFips);
    String zipCachePath = storageProvider.resolvePath(cachePath, filename);

    // Check manifest first for cached raw shapefiles
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("state", stateFips);
      if (cacheManifest.isCached("places", year, params)) {
        LOGGER.debug("Places shapefile cached per manifest for state {} year {}", stateFips, year);
        return downloadCacheToTemp(cachePath, "places_" + stateFips + "_" + year);
      }
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Places shapefile not found for state {}: {}", stateFips, zipCachePath);
      return null;
    }

    LOGGER.info("Downloading places shapefile for state {} from: {}", stateFips, url);

    // Download and extract to temp directory
    File tempDir = Files.createTempDirectory("tiger-places-" + stateFips + "-" + year + "-").toFile();
    File zipFile = new File(tempDir, filename);

    downloadFile(url, zipFile);
    extractZipFile(zipFile, tempDir);

    // Upload extracted files to cache storage
    uploadDirectoryToStorage(tempDir, cachePath);

    // Mark as cached in manifest
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("state", stateFips);
      cacheManifest.markCached("places", year, params, cachePath, zipFile.length());
      cacheManifest.save(this.operatingDirectory);
    }

    // Keep temp directory for immediate use
    return tempDir;
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

    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDir, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "zctas");
    String zipCachePath = storageProvider.resolvePath(cachePath, filename);

    // Check if already exists in cache via StorageProvider
    if (storageProvider.exists(zipCachePath)) {
      LOGGER.info("ZCTAs shapefile already exists in cache: {}", zipCachePath);
      return downloadCacheToTemp(cachePath, "zctas_" + year);
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. ZCTAs shapefile not found: {}", zipCachePath);
      return null;
    }

    LOGGER.info("Downloading ZCTAs shapefile from: {}", url);
    LOGGER.warn("Note: ZCTA file is large (~200MB), this may take a while...");

    try {
      // Download and extract to temp directory
      File tempDir = Files.createTempDirectory("tiger-zctas-" + year + "-").toFile();
      File zipFile = new File(tempDir, filename);

      downloadFile(url, zipFile);
      extractZipFile(zipFile, tempDir);

      // Upload extracted files to cache storage
      uploadDirectoryToStorage(tempDir, cachePath);

      // Keep temp directory for immediate use
      return tempDir;
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
    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDir, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "congressional_districts");

    // Calculate correct Congress number: ((year - 1789) / 2) + 1
    int congressNum = ((year - 1789) / 2) + 1;
    String filename = String.format("tl_%d_us_cd%d.zip", year, congressNum);
    String zipCachePath = storageProvider.resolvePath(cachePath, filename);

    // Check if already exists in cache via StorageProvider
    if (storageProvider.exists(zipCachePath)) {
      LOGGER.info("Congressional districts shapefile already exists in cache for year {}: {}", year, zipCachePath);
      return downloadCacheToTemp(cachePath, "congressional_districts_" + year);
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Congressional districts not found for year {}", year);
      return null;
    }

    // 2010 has a different directory structure with congress subdirectory
    String url;
    if (year == 2010) {
      url = String.format("%s/%s/CD/%d/%s", TIGER_BASE_URL, getTigerYearPath(year), congressNum, filename);
    } else {
      url = String.format("%s/%s/CD/%s", TIGER_BASE_URL, getTigerYearPath(year), filename);
    }

    try {
      LOGGER.info("Downloading CD shapefile for year {} (Congress {})", year, congressNum);

      // Download and extract to temp directory
      File tempDir = Files.createTempDirectory("tiger-cd-" + year + "-").toFile();
      File zipFile = new File(tempDir, filename);

      downloadFile(url, zipFile);
      extractZipFile(zipFile, tempDir);

      // Upload extracted files to cache storage
      uploadDirectoryToStorage(tempDir, cachePath);

      // Keep temp directory for immediate use
      return tempDir;
    } catch (IOException e) {
      if (e.getMessage().contains("404")) {
        LOGGER.warn("Congressional districts data not available for year {} (Congress {}) - skipping", year, congressNum);
        return null;
      }
      LOGGER.warn("Failed to download CD for year {}: {}", year, e.getMessage());
      throw e;
    }
  }

  /**
   * Upload all files from a local directory to storage (recursively).
   */
  private void uploadDirectoryToStorage(File sourceDir, String targetPath) throws IOException {
    File[] files = sourceDir.listFiles();
    if (files == null) {
      return;
    }

    for (File file : files) {
      if (file.isDirectory()) {
        // Recursively upload subdirectory
        String subPath = storageProvider.resolvePath(targetPath, file.getName());
        uploadDirectoryToStorage(file, subPath);
      } else {
        // Skip ZIP files (only upload extracted shapefiles)
        if (file.getName().endsWith(".zip")) {
          LOGGER.debug("Skipping ZIP file upload: {}", file.getName());
          continue;
        }
        // Upload file
        String filePath = storageProvider.resolvePath(targetPath, file.getName());
        LOGGER.debug("Uploading {} to {}", file.getName(), filePath);
        byte[] data = Files.readAllBytes(file.toPath());
        storageProvider.writeFile(filePath, data);
      }
    }
  }

  /**
   * Download files from cache storage to a temp directory for reading.
   * Used when shapefiles exist in remote storage but need to be read locally.
   */
  private File downloadCacheToTemp(String cachePath, String tempPrefix) throws IOException {
    // For local filesystem cache, just return the path directly
    if (cacheDir != null && !cacheDir.startsWith("s3://")) {
      // Local path - can use directly
      File localPath = new File(cachePath);
      if (localPath.exists()) {
        return localPath;
      }
    }

    // For remote storage, download files to temp directory
    File tempDir = Files.createTempDirectory(tempPrefix + "-").toFile();

    // List all files in the cache path recursively and download them
    java.util.List<FileEntry> files = storageProvider.listFiles(cachePath, true);
    if (files.isEmpty()) {
      LOGGER.warn("No files found in cache path: {}", cachePath);
      return tempDir;
    }

    for (FileEntry fileEntry : files) {
      // Skip directories
      if (fileEntry.isDirectory()) {
        continue;
      }

      String filePath = fileEntry.getPath();
      // Extract relative path from the cache path
      String relativePath = filePath.substring(cachePath.length());
      if (relativePath.startsWith("/")) {
        relativePath = relativePath.substring(1);
      }

      File targetFile = new File(tempDir, relativePath);
      targetFile.getParentFile().mkdirs();

      // Download file from storage using InputStream
      try (java.io.InputStream in = storageProvider.openInputStream(filePath);
           java.io.OutputStream out = new java.io.FileOutputStream(targetFile)) {
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = in.read(buffer)) != -1) {
          out.write(buffer, 0, bytesRead);
        }
      }

      LOGGER.debug("Downloaded {} to {}", filePath, targetFile);
    }

    LOGGER.info("Downloaded {} files from cache {} to temp directory {}", files.size(), cachePath, tempDir);
    return tempDir;
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
    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDir, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "census_tracts");

    // Check if any census tracts exist in cache for this year (check first state)
    String testStateFips = "06";
    String testFileSuffix = getTigerFileSuffix(year, "tract");
    String testFilename = String.format("tl_%d_%s_%s.zip", year, testStateFips, testFileSuffix);
    String testStatePath = storageProvider.resolvePath(cachePath, testStateFips);
    String testZipPath = storageProvider.resolvePath(testStatePath, testFilename);

    // If cached, download to temp
    if (storageProvider.exists(testZipPath)) {
      LOGGER.info("Census tracts already cached for year {}", year);
      return downloadCacheToTemp(cachePath, "census_tracts_" + year);
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Census tracts not found for year {}", year);
      return null;
    }

    // Download census tracts for selected states only
    String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
    File tempDir = Files.createTempDirectory("tiger-census_tracts-" + year + "-").toFile();

    for (String fips : stateFips) {
      String fileSuffix = getTigerFileSuffix(year, "tract");
      String filename = String.format("tl_%d_%s_%s.zip", year, fips, fileSuffix);
      String url = String.format("%s/%s/TRACT%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);

      File stateDir = new File(tempDir, fips);
      File zipFile = new File(stateDir, filename);

      LOGGER.info("Downloading census tracts shapefile for state {} year {} from: {}", fips, year, url);
      stateDir.mkdirs();

      try {
        downloadFile(url, zipFile);
        extractZipFile(zipFile, stateDir);
      } catch (IOException e) {
        if (e.getMessage().contains("404")) {
          LOGGER.warn("TIGER census tract data not available for state {} year {} - skipping", fips, year);
        } else {
          LOGGER.warn("Failed to download census tracts for state {}: {}", fips, e.getMessage());
        }
      }
    }

    // Upload extracted files to cache storage
    uploadDirectoryToStorage(tempDir, cachePath);

    // Keep temp directory for immediate use
    return tempDir;
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
    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDir, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "block_groups");

    // Check if any block groups exist in cache for this year (check first state)
    String testStateFips = "06";
    String testFileSuffix = getTigerFileSuffix(year, "bg");
    String testFilename = String.format("tl_%d_%s_%s.zip", year, testStateFips, testFileSuffix);
    String testStatePath = storageProvider.resolvePath(cachePath, testStateFips);
    String testZipPath = storageProvider.resolvePath(testStatePath, testFilename);

    // If cached, download to temp
    if (storageProvider.exists(testZipPath)) {
      LOGGER.info("Block groups already cached for year {}", year);
      return downloadCacheToTemp(cachePath, "block_groups_" + year);
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Block groups not found for year {}", year);
      return null;
    }

    // Download block groups for selected states only
    String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
    File tempDir = Files.createTempDirectory("tiger-block_groups-" + year + "-").toFile();

    for (String fips : stateFips) {
      String fileSuffix = getTigerFileSuffix(year, "bg");
      String filename = String.format("tl_%d_%s_%s.zip", year, fips, fileSuffix);
      String url = String.format("%s/%s/BG%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);

      File stateDir = new File(tempDir, fips);
      File zipFile = new File(stateDir, filename);

      LOGGER.info("Downloading block groups shapefile for state {} year {} from: {}", fips, year, url);
      stateDir.mkdirs();

      try {
        downloadFile(url, zipFile);
        extractZipFile(zipFile, stateDir);
      } catch (IOException e) {
        if (e.getMessage().contains("404")) {
          LOGGER.warn("TIGER block group data not available for state {} year {} - skipping", fips, year);
        } else {
          LOGGER.warn("Failed to download block groups for state {}: {}", fips, e.getMessage());
        }
      }
    }

    // Upload extracted files to cache storage
    uploadDirectoryToStorage(tempDir, cachePath);

    // Keep temp directory for immediate use
    return tempDir;
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

    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDir, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "cbsa");
    String zipCachePath = storageProvider.resolvePath(cachePath, filename);

    // Check if already exists in cache via StorageProvider
    if (storageProvider.exists(zipCachePath)) {
      LOGGER.info("CBSA shapefile already exists in cache for year {}: {}", year, zipCachePath);
      return downloadCacheToTemp(cachePath, "cbsa_" + year);
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. CBSA shapefile not found for year {}: {}", year, zipCachePath);
      return null;
    }

    LOGGER.info("Downloading CBSA shapefile for year {} from: {}", year, url);

    // Download and extract to temp directory
    File tempDir = Files.createTempDirectory("tiger-cbsa-" + year + "-").toFile();
    File zipFile = new File(tempDir, filename);

    downloadFile(url, zipFile);
    extractZipFile(zipFile, tempDir);

    // Upload extracted files to cache storage
    uploadDirectoryToStorage(tempDir, cachePath);

    // Keep temp directory for immediate use
    return tempDir;
  }

  /**
   * Get the cache directory.
   */
  public String getCacheDir() {
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
      // Return temp directory placeholder (no actual download needed)
      return Files.createTempDirectory("tiger-school_districts-" + year + "-").toFile();
    }

    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDir, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "school_districts");

    // Check if any school districts exist in cache for this year
    String testStateFips = "06";
    String testType = "unsd";
    String testTypeSuffix = (year == 2010) ? testType + "10" : testType;
    String testFilename = String.format("tl_%d_%s_%s.zip", year, testStateFips, testTypeSuffix);
    String testStatePath = storageProvider.resolvePath(cachePath, testStateFips);
    String testZipPath = storageProvider.resolvePath(testStatePath, testFilename);

    // If cached, download to temp
    if (storageProvider.exists(testZipPath)) {
      LOGGER.info("School districts already cached for year {}", year);
      return downloadCacheToTemp(cachePath, "school_districts_" + year);
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. School districts not found for year {}", year);
      return null;
    }

    // Download school districts for selected states only
    String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
    File tempDir = Files.createTempDirectory("tiger-school_districts-" + year + "-").toFile();
    boolean hasAnyDownloads = false;

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

        File stateDir = new File(tempDir, fips);
        File zipFile = new File(stateDir, filename);

        LOGGER.info("Downloading school district shapefile for state {} type {} year {} from: {}", fips, type, year, url);
        stateDir.mkdirs();

        try {
          downloadFile(url, zipFile);
          extractZipFile(zipFile, stateDir);
          hasAnyDownloads = true;
        } catch (IOException e) {
          LOGGER.debug("Failed to download school districts for state {} type {}: {}", fips, type, e.getMessage());
        }
      }
    }

    if (!hasAnyDownloads) {
      LOGGER.warn("No school district shapefiles were successfully downloaded for year {}", year);
      return null;
    }

    // Upload extracted files to cache storage
    uploadDirectoryToStorage(tempDir, cachePath);

    // Keep temp directory for immediate use
    return tempDir;
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
        cacheManifest.save(this.operatingDirectory);
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
        cacheManifest.save(this.operatingDirectory);
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
    // For local filesystem cache
    if (cacheDir != null && !cacheDir.startsWith("s3://")) {
      File dir = new File(cacheDir, category);
      if (!dir.exists()) {
        return false;
      }
      File[] shpFiles = dir.listFiles((d, name) -> name.endsWith(".shp"));
      return shpFiles != null && shpFiles.length > 0;
    }

    // For remote storage, check if any .shp files exist in the category path
    String categoryPath = storageProvider.resolvePath(cacheDir, category);
    try {
      java.util.List<FileEntry> files = storageProvider.listFiles(categoryPath, true);
      return files.stream().anyMatch(f -> !f.isDirectory() && f.getPath().endsWith(".shp"));
    } catch (IOException e) {
      LOGGER.warn("Failed to check shapefile availability: {}", e.getMessage());
      return false;
    }
  }
}
