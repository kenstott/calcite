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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry;
import org.apache.calcite.adapter.govdata.CacheKey;
import org.apache.calcite.adapter.govdata.DuckDBCacheStore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
public class TigerDataDownloader extends AbstractGeoDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TigerDataDownloader.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  /** Fallback URL if schema config not available. */
  private static final String TIGER_BASE_URL = "https://www2.census.gov/geo/tiger";

  /** Cached schema configuration for download URLs. */
  private static JsonNode geoSchemaConfig = null;

  /** TIGER dataset types for download/conversion. */
  private static final String[] TIGER_TABLES = {
      "states", "counties", "places", "zctas", "census_tracts",
      "block_groups", "cbsa", "congressional_districts", "school_districts"
  };

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

  // ===== SCHEMA-DRIVEN URL CONFIGURATION =====

  /**
   * Load geo-schema.json configuration lazily.
   * Configuration is cached after first load.
   */
  private static synchronized JsonNode loadGeoSchemaConfig() {
    if (geoSchemaConfig != null) {
      return geoSchemaConfig;
    }

    try (InputStream is = TigerDataDownloader.class.getResourceAsStream("/geo/geo-schema.json")) {
      if (is == null) {
        LOGGER.warn("Could not find /geo/geo-schema.json - using hardcoded URLs");
        return null;
      }
      geoSchemaConfig = JSON_MAPPER.readTree(is);
      LOGGER.debug("Loaded geo-schema.json configuration");
      return geoSchemaConfig;
    } catch (Exception e) {
      LOGGER.warn("Failed to load geo-schema.json: {} - using hardcoded URLs", e.getMessage());
      return null;
    }
  }

  /**
   * Get download configuration for a table from geo-schema.json.
   *
   * @param tableName Table name (e.g., "states", "counties")
   * @return JsonNode with download config, or null if not found
   */
  private static JsonNode getTableDownloadConfig(String tableName) {
    JsonNode schema = loadGeoSchemaConfig();
    if (schema == null) {
      return null;
    }

    JsonNode tables = schema.get("partitionedTables");
    if (tables == null || !tables.isArray()) {
      return null;
    }

    for (JsonNode table : tables) {
      JsonNode nameNode = table.get("name");
      if (nameNode != null && tableName.equals(nameNode.asText())) {
        return table.get("download");
      }
    }

    return null;
  }

  /**
   * Build download URL from schema configuration with variable substitution.
   * Falls back to hardcoded URL if schema config not available.
   *
   * @param tableName Table name (e.g., "states", "counties")
   * @param year Year for the data
   * @param vars Additional variables for substitution (e.g., state_fips)
   * @return Download URL
   */
  private String buildDownloadUrlFromSchema(String tableName, int year, java.util.Map<String, String> vars) {
    JsonNode downloadConfig = getTableDownloadConfig(tableName);

    if (downloadConfig == null) {
      LOGGER.debug("No download config for table {}, using hardcoded URL", tableName);
      return null; // Caller will use hardcoded URL
    }

    JsonNode baseUrlNode = downloadConfig.get("baseUrl");
    JsonNode filePatternNode = downloadConfig.get("filePattern");

    if (baseUrlNode == null || filePatternNode == null) {
      LOGGER.debug("Incomplete download config for table {}, using hardcoded URL", tableName);
      return null;
    }

    String baseUrl = baseUrlNode.asText();
    String filePattern = filePatternNode.asText();

    // Substitute variables
    String url = baseUrl + filePattern;
    url = url.replace("{year}", String.valueOf(year));

    // Substitute additional variables
    if (vars != null) {
      for (java.util.Map.Entry<String, String> entry : vars.entrySet()) {
        url = url.replace("{" + entry.getKey() + "}", entry.getValue());
      }
    }

    // Handle 2010 special case - TIGER2010 has different directory structure
    if (year == 2010) {
      // For 2010, need to add /2010/ subdirectory for some paths
      // Also need to handle different file suffixes (e.g., state10 instead of state)
      // The schema baseUrl should handle most of this, but we may need adjustments
      url = url.replace("TIGER{year}", "TIGER2010");
    }

    LOGGER.debug("Built URL from schema for {}: {}", tableName, url);
    return url;
  }

  /** Local list of data years for backward compatibility methods. */
  private final List<Integer> dataYears;
  /** Auto-download flag for backward compatibility methods. */
  private final boolean autoDownload;

  /**
   * Create a List of Integer years from a range.
   * Unlike parent's yearRange() which returns List<String>, this returns List<Integer>.
   */
  private static List<Integer> intYearRange(int startYear, int endYear) {
    java.util.List<Integer> years = new java.util.ArrayList<>();
    for (int year = startYear; year <= endYear; year++) {
      years.add(year);
    }
    return years;
  }

  // ===== CONSTRUCTORS =====

  /**
   * Primary constructor matching BEA/ECON pattern with all required parameters.
   *
   * @param cacheDir Cache directory for raw shapefile downloads
   * @param operatingDirectory Operating directory for metadata (.aperio/geo/)
   * @param parquetDir Parquet output directory
   * @param cacheStorageProvider Provider for cache file operations
   * @param storageProvider Provider for output file operations
   * @param sharedManifest Shared cache manifest
   * @param startYear First year for downloads
   * @param endYear Last year for downloads
   */
  public TigerDataDownloader(String cacheDir, String operatingDirectory, String parquetDir,
      StorageProvider cacheStorageProvider, StorageProvider storageProvider,
      GeoCacheManifest sharedManifest, int startYear, int endYear) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider,
        sharedManifest, startYear, endYear, null);
    this.dataYears = intYearRange(startYear, endYear);
    this.autoDownload = true;

    LOGGER.info("TIGER data downloader initialized for years {}-{} in directory: {}",
        startYear, endYear, cacheDir);
  }

  /**
   * Constructor with year list and StorageProvider (legacy pattern).
   */
  public TigerDataDownloader(String cacheDir, List<Integer> dataYears, boolean autoDownload,
      StorageProvider storageProvider) {
    this(cacheDir, dataYears, autoDownload, storageProvider, null);
  }

  /**
   * Constructor with year list, StorageProvider, and cacheManifest (legacy pattern).
   */
  public TigerDataDownloader(String cacheDir, List<Integer> dataYears, boolean autoDownload,
      StorageProvider storageProvider, GeoCacheManifest cacheManifest) {
    this(cacheDir, cacheDir, dataYears, autoDownload, storageProvider, cacheManifest);
  }

  /**
   * Constructor with separate cache and operating directories (legacy pattern).
   */
  public TigerDataDownloader(String cacheDir, String operatingDirectory, List<Integer> dataYears,
      boolean autoDownload, StorageProvider storageProvider, GeoCacheManifest cacheManifest) {
    super(cacheDir, operatingDirectory, cacheDir,
        storageProvider != null ? storageProvider : org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(cacheDir),
        storageProvider != null ? storageProvider : org.apache.calcite.adapter.file.storage.StorageProviderFactory.createFromUrl(cacheDir),
        cacheManifest,
        dataYears.isEmpty() ? 2020 : dataYears.get(0),
        dataYears.isEmpty() ? 2020 : dataYears.get(dataYears.size() - 1),
        null);
    this.dataYears = dataYears;
    this.autoDownload = autoDownload;

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

  // ===== ACCESSOR METHODS =====

  /**
   * Get the cache manifest as GeoCacheManifest for convenience methods.
   * The parent class stores it as AbstractCacheManifest, but GEO adapters use GeoCacheManifest.
   *
   * @return Cache manifest cast to GeoCacheManifest
   */
  private GeoCacheManifest getGeoManifest() {
    return (GeoCacheManifest) cacheManifest;
  }

  /**
   * Get the cache manifest for this downloader.
   * Used by GeoSchemaFactory for metadata-driven downloads.
   *
   * @return Cache manifest instance
   */
  public GeoCacheManifest getCacheManifest() {
    return getGeoManifest();
  }

  /**
   * Get the operating directory for this downloader.
   * Used by GeoSchemaFactory for metadata-driven downloads.
   *
   * @return Operating directory path
   */
  public String getOperatingDirectory() {
    return operatingDirectory;
  }

  /**
   * Download all TIGER data for the specified year range (matching ECON pattern).
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   * @throws IOException If download or file I/O fails
   */
  @Override public void downloadAll(int startYear, int endYear) throws IOException {
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
   * Convert all downloaded TIGER shapefiles to Parquet format (matching ECON pattern).
   * Uses iterateTableOperationsOptimized for efficient cache checking.
   *
   * @param startYear First year to convert
   * @param endYear Last year to convert
   * @throws IOException If conversion or file I/O fails
   */
  @Override public void convertAll(int startYear, int endYear) throws IOException {
    LOGGER.info("TIGER conversion phase: {} years ({}-{}), {} tables",
        endYear - startYear + 1, startYear, endYear, TIGER_TABLES.length);

    // Convert each table type
    for (String tableName : TIGER_TABLES) {
      convertTableForYears(tableName, startYear, endYear);
    }

    LOGGER.info("TIGER data conversion completed for years {} to {}", startYear, endYear);
  }

  /**
   * Convert a specific table type for all years in range.
   *
   * @param tableName The table name (e.g., "states", "counties")
   * @param startYear First year to convert
   * @param endYear Last year to convert
   */
  private void convertTableForYears(String tableName, int startYear, int endYear) throws IOException {
    LOGGER.debug("Converting {} for years {}-{}", tableName, startYear, endYear);

    for (int year = startYear; year <= endYear; year++) {
      // Build paths
      String yearPath = String.format("year=%d", year);

      // Determine if this is a per-state table or national
      boolean perState = isPerStateTable(tableName);

      if (perState) {
        // Per-state tables need to iterate over states
        String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
        for (String state : stateFips) {
          convertSingleDataset(tableName, year, state);
        }
      } else {
        // National tables - single conversion per year
        convertSingleDataset(tableName, year, null);
      }
    }
  }

  /**
   * Convert a single shapefile dataset to Parquet.
   *
   * @param tableName Table type
   * @param year Year
   * @param stateFips State FIPS code (null for national datasets)
   */
  private void convertSingleDataset(String tableName, int year, String stateFips) throws IOException {
    // Build cache path where shapefile is stored
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDirectory, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, tableName);
    if (stateFips != null) {
      cachePath = storageProvider.resolvePath(cachePath, stateFips);
    }

    // Build parquet target path
    String outputPath =
        storageProvider.resolvePath(parquetDirectory, "source=census/type=boundary/" + yearPath + "/" + tableName + ".parquet");

    // Build cache key params
    java.util.Map<String, String> params = new java.util.HashMap<>();
    params.put("year", String.valueOf(year));
    if (stateFips != null) {
      params.put("state", stateFips);
    }

    // Check if already converted
    if (isMaterializedOrExists(tableName, year, params, cachePath, outputPath)) {
      LOGGER.debug("Parquet already exists or converted: {} year={} state={}",
          tableName, year, stateFips);
      return;
    }

    // Find shapefile in cache
    File cacheFile = new File(cachePath);
    if (!cacheFile.exists() || !cacheFile.isDirectory()) {
      LOGGER.debug("No cached shapefile found for {} year={} state={}", tableName, year, stateFips);
      return;
    }

    // Convert using existing method
    LOGGER.info("Converting {} to Parquet: year={} state={}", tableName, year, stateFips);
    convertToParquet(cacheFile, outputPath);
  }

  /**
   * Determine if a table is per-state or national.
   */
  private boolean isPerStateTable(String tableName) {
    switch (tableName) {
      case "places":
      case "census_tracts":
      case "block_groups":
      case "school_districts":
        return true;
      default:
        return false;
    }
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

    // Try schema-driven URL first, fall back to hardcoded
    String url = buildDownloadUrlFromSchema("states", year, null);
    if (url == null) {
      url = String.format("%s/%s/STATE%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);
    }

    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDirectory, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "states");
    String zipCachePath = storageProvider.resolvePath(cachePath, filename);

    // Note: outputPath check moved to GeoSchemaFactory for better control flow
    // GeoSchemaFactory will call isMaterializedOrExists() before calling this method

    // Check manifest for cached raw shapefiles
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey("states", params);
      if (cacheManifest.isCached(cacheKey)) {
        LOGGER.debug("States shapefile cached per manifest for year {}", year);
        return downloadCacheToTemp(cachePath, "states_" + year);
      }
    }

    // Defensive fallback: check if shapefiles exist in cache even without manifest entry
    try {
      java.util.List<FileEntry> files = storageProvider.listFiles(cachePath, false);
      boolean hasShapefile = files.stream().anyMatch(f -> !f.isDirectory() && f.getPath().endsWith(".shp"));
      if (hasShapefile) {
        LOGGER.info("⚡ States shapefile exists in cache, updating manifest: year={}", year);
        if (cacheManifest != null) {
          java.util.Map<String, String> params = new java.util.HashMap<>();
          // Estimate file size from shapefile
          long totalSize = files.stream().mapToLong(FileEntry::getSize).sum();
          getGeoManifest().markCached("states", year, params, cachePath, totalSize);
          cacheManifest.save(this.operatingDirectory);
        }
        return downloadCacheToTemp(cachePath, "states_" + year);
      }
    } catch (IOException e) {
      LOGGER.debug("Could not check for existing shapefiles: {}", e.getMessage());
      // Fall through to download
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
        getGeoManifest().markCached("states", year, params, cachePath, zipFile.length());
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

    // Try schema-driven URL first, fall back to hardcoded
    String url = buildDownloadUrlFromSchema("counties", year, null);
    if (url == null) {
      url = String.format("%s/%s/COUNTY%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);
    }

    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDirectory, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "counties");
    String zipCachePath = storageProvider.resolvePath(cachePath, filename);

    // Check manifest first - if parquet already converted, no need to download raw files
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey("counties", params);
      if (cacheManifest.isMaterialized(cacheKey)) {
        LOGGER.debug("Counties parquet already converted per manifest for year {} - skipping raw download", year);
        return null; // No need to download raw files if parquet exists
      }
    }

    // Check manifest for cached raw shapefiles
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey("counties", params);
      if (cacheManifest.isCached(cacheKey)) {
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
        getGeoManifest().markCached("counties", year, params, cachePath, zipFile.length());
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

    // Try schema-driven URL first
    java.util.Map<String, String> vars = new java.util.HashMap<>();
    vars.put("state_fips", stateFips);
    String url = buildDownloadUrlFromSchema("places", year, vars);
    if (url == null) {
      url = String.format("%s/%s/PLACE%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);
    }

    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDirectory, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "places");
    cachePath = storageProvider.resolvePath(cachePath, stateFips);
    String zipCachePath = storageProvider.resolvePath(cachePath, filename);

    // Check manifest first - if parquet already converted, no need to download raw files
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("state", stateFips);
      params.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey("places", params);
      if (cacheManifest.isMaterialized(cacheKey)) {
        LOGGER.debug("Places parquet already converted per manifest for state {} year {} - skipping raw download", stateFips, year);
        return null; // No need to download raw files if parquet exists
      }
    }

    // Check manifest for cached raw shapefiles
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("state", stateFips);
      params.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey("places", params);
      if (cacheManifest.isCached(cacheKey)) {
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
      getGeoManifest().markCached("places", year, params, cachePath, zipFile.length());
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

    // Try schema-driven URL first, fall back to hardcoded
    String url = buildDownloadUrlFromSchema("zctas", year, null);
    if (url == null) {
      url = String.format("%s/%s/%s%s/%s", TIGER_BASE_URL, getTigerYearPath(year), zctaType, getTiger2010Subdir(year), filename);
    }

    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDirectory, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "zctas");
    String zipCachePath = storageProvider.resolvePath(cachePath, filename);

    // Check manifest first - if parquet already converted, no need to download raw files
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey("zctas", params);
      if (cacheManifest.isMaterialized(cacheKey)) {
        LOGGER.debug("ZCTAs parquet already converted per manifest for year {} - skipping raw download", year);
        return null; // No need to download raw files if parquet exists
      }
    }

    // Check manifest for cached raw shapefiles
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey("zctas", params);
      if (cacheManifest.isCached(cacheKey)) {
        LOGGER.debug("ZCTAs shapefile cached per manifest for year {}", year);
        return downloadCacheToTemp(cachePath, "zctas_" + year);
      }
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

      // Mark as cached in manifest
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        getGeoManifest().markCached("zctas", year, params, cachePath, zipFile.length());
        cacheManifest.save(this.operatingDirectory);
      }

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
    String cachePath = storageProvider.resolvePath(cacheDirectory, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "congressional_districts");

    // Calculate correct Congress number: ((year - 1789) / 2) + 1
    int congressNum = ((year - 1789) / 2) + 1;
    String filename = String.format("tl_%d_us_cd%d.zip", year, congressNum);
    String zipCachePath = storageProvider.resolvePath(cachePath, filename);

    // Check manifest first - if parquet already converted, no need to download raw files
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey("congressional_districts", params);
      if (cacheManifest.isMaterialized(cacheKey)) {
        LOGGER.debug("Congressional districts parquet already converted per manifest for year {} - skipping raw download", year);
        return null; // No need to download raw files if parquet exists
      }
    }

    // Check manifest for cached raw shapefiles
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey("congressional_districts", params);
      if (cacheManifest.isCached(cacheKey)) {
        LOGGER.debug("Congressional districts shapefile cached per manifest for year {}", year);
        return downloadCacheToTemp(cachePath, "congressional_districts_" + year);
      }
    }

    if (!autoDownload) {
      LOGGER.info("Auto-download disabled. Congressional districts not found for year {}", year);
      return null;
    }

    // Try schema-driven URL first
    java.util.Map<String, String> vars = new java.util.HashMap<>();
    vars.put("congress", String.valueOf(congressNum));
    String url = buildDownloadUrlFromSchema("congressional_districts", year, vars);

    // Fall back to hardcoded URL if schema not available
    if (url == null) {
      // 2010 has a different directory structure with congress subdirectory
      if (year == 2010) {
        url = String.format("%s/%s/CD/%d/%s", TIGER_BASE_URL, getTigerYearPath(year), congressNum, filename);
      } else {
        url = String.format("%s/%s/CD/%s", TIGER_BASE_URL, getTigerYearPath(year), filename);
      }
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

      // Mark as cached in manifest
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        getGeoManifest().markCached("congressional_districts", year, params, cachePath, zipFile.length());
        cacheManifest.save(this.operatingDirectory);
      }

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
    if (cacheDirectory != null && !cacheDirectory.startsWith("s3://")) {
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
    // Download census tracts for selected states only
    String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
    File tempDir = Files.createTempDirectory("tiger-census_tracts-" + year + "-").toFile();
    boolean hasAnyDownloads = false;

    for (String fips : stateFips) {
      // Build target path in cache storage for this state
      String yearPath = String.format("year=%d", year);
      String cachePath = storageProvider.resolvePath(cacheDirectory, yearPath);
      cachePath = storageProvider.resolvePath(cachePath, "census_tracts");
      cachePath = storageProvider.resolvePath(cachePath, fips);

      // Check manifest first - if parquet already converted for this state, skip
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("state", fips);
        params.put("year", String.valueOf(year));
        CacheKey cacheKey = new CacheKey("census_tracts", params);
        if (cacheManifest.isMaterialized(cacheKey)) {
          LOGGER.debug("Census tracts parquet already converted per manifest for state {} year {} - skipping raw download", fips, year);
          continue;
        }
      }

      // Check manifest for cached raw shapefiles for this state
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("state", fips);
        params.put("year", String.valueOf(year));
        CacheKey cacheKey = new CacheKey("census_tracts", params);
        if (cacheManifest.isCached(cacheKey)) {
          LOGGER.debug("Census tracts shapefile cached per manifest for state {} year {}", fips, year);
          // Download this state's data to temp dir
          File stateTemp = downloadCacheToTemp(cachePath, "census_tracts_" + fips + "_" + year);
          if (stateTemp != null) {
            // Copy to main temp dir under state subdirectory
            File stateTempTarget = new File(tempDir, fips);
            stateTempTarget.mkdirs();
            for (File file : stateTemp.listFiles()) {
              Files.copy(file.toPath(), new File(stateTempTarget, file.getName()).toPath());
            }
            hasAnyDownloads = true;
          }
          continue;
        }
      }

      if (!autoDownload) {
        LOGGER.info("Auto-download disabled. Census tracts not found for state {} year {}", fips, year);
        continue;
      }

      String fileSuffix = getTigerFileSuffix(year, "tract");
      String filename = String.format("tl_%d_%s_%s.zip", year, fips, fileSuffix);

      // Try schema-driven URL first
      java.util.Map<String, String> vars = new java.util.HashMap<>();
      vars.put("state_fips", fips);
      String url = buildDownloadUrlFromSchema("census_tracts", year, vars);
      if (url == null) {
        url = String.format("%s/%s/TRACT%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);
      }

      File stateDir = new File(tempDir, fips);
      File zipFile = new File(stateDir, filename);

      LOGGER.info("Downloading census tracts shapefile for state {} year {} from: {}", fips, year, url);
      stateDir.mkdirs();

      try {
        downloadFile(url, zipFile);
        extractZipFile(zipFile, stateDir);
        hasAnyDownloads = true;

        // Upload extracted files to cache storage for this state
        uploadDirectoryToStorage(stateDir, cachePath);

        // Mark as cached in manifest with state parameter
        if (cacheManifest != null) {
          java.util.Map<String, String> params = new java.util.HashMap<>();
          params.put("state", fips);
          getGeoManifest().markCached("census_tracts", year, params, cachePath, zipFile.length());
          cacheManifest.save(this.operatingDirectory);
        }
      } catch (IOException e) {
        if (e.getMessage().contains("404")) {
          LOGGER.warn("TIGER census tract data not available for state {} year {} - skipping", fips, year);
        } else {
          LOGGER.warn("Failed to download census tracts for state {}: {}", fips, e.getMessage());
        }
      }
    }

    if (!hasAnyDownloads) {
      LOGGER.warn("No census tract shapefiles were successfully downloaded or cached for year {}", year);
      return null;
    }

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
    // Download block groups for selected states only
    String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
    File tempDir = Files.createTempDirectory("tiger-block_groups-" + year + "-").toFile();
    boolean hasAnyDownloads = false;

    for (String fips : stateFips) {
      // Build target path in cache storage for this state
      String yearPath = String.format("year=%d", year);
      String cachePath = storageProvider.resolvePath(cacheDirectory, yearPath);
      cachePath = storageProvider.resolvePath(cachePath, "block_groups");
      cachePath = storageProvider.resolvePath(cachePath, fips);

      // Check manifest first - if parquet already converted for this state, skip
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("state", fips);
        params.put("year", String.valueOf(year));
        CacheKey cacheKey = new CacheKey("block_groups", params);
        if (cacheManifest.isMaterialized(cacheKey)) {
          LOGGER.debug("Block groups parquet already converted per manifest for state {} year {} - skipping raw download", fips, year);
          continue;
        }
      }

      // Check manifest for cached raw shapefiles for this state
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("state", fips);
        params.put("year", String.valueOf(year));
        CacheKey cacheKey = new CacheKey("block_groups", params);
        if (cacheManifest.isCached(cacheKey)) {
          LOGGER.debug("Block groups shapefile cached per manifest for state {} year {}", fips, year);
          // Download this state's data to temp dir
          File stateTemp = downloadCacheToTemp(cachePath, "block_groups_" + fips + "_" + year);
          if (stateTemp != null) {
            // Copy to main temp dir under state subdirectory
            File stateTempTarget = new File(tempDir, fips);
            stateTempTarget.mkdirs();
            for (File file : stateTemp.listFiles()) {
              Files.copy(file.toPath(), new File(stateTempTarget, file.getName()).toPath());
            }
            hasAnyDownloads = true;
          }
          continue;
        }
      }

      if (!autoDownload) {
        LOGGER.info("Auto-download disabled. Block groups not found for state {} year {}", fips, year);
        continue;
      }

      String fileSuffix = getTigerFileSuffix(year, "bg");
      String filename = String.format("tl_%d_%s_%s.zip", year, fips, fileSuffix);

      // Try schema-driven URL first
      java.util.Map<String, String> vars = new java.util.HashMap<>();
      vars.put("state_fips", fips);
      String url = buildDownloadUrlFromSchema("block_groups", year, vars);
      if (url == null) {
        url = String.format("%s/%s/BG%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);
      }

      File stateDir = new File(tempDir, fips);
      File zipFile = new File(stateDir, filename);

      LOGGER.info("Downloading block groups shapefile for state {} year {} from: {}", fips, year, url);
      stateDir.mkdirs();

      try {
        downloadFile(url, zipFile);
        extractZipFile(zipFile, stateDir);
        hasAnyDownloads = true;

        // Upload extracted files to cache storage for this state
        uploadDirectoryToStorage(stateDir, cachePath);

        // Mark as cached in manifest with state parameter
        if (cacheManifest != null) {
          java.util.Map<String, String> params = new java.util.HashMap<>();
          params.put("state", fips);
          getGeoManifest().markCached("block_groups", year, params, cachePath, zipFile.length());
          cacheManifest.save(this.operatingDirectory);
        }
      } catch (IOException e) {
        if (e.getMessage().contains("404")) {
          LOGGER.warn("TIGER block group data not available for state {} year {} - skipping", fips, year);
        } else {
          LOGGER.warn("Failed to download block groups for state {}: {}", fips, e.getMessage());
        }
      }
    }

    if (!hasAnyDownloads) {
      LOGGER.warn("No block group shapefiles were successfully downloaded or cached for year {}", year);
      return null;
    }

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

    // Try schema-driven URL first, fall back to hardcoded
    String url = buildDownloadUrlFromSchema("cbsa", year, null);
    if (url == null) {
      url = String.format("%s/%s/CBSA%s/%s", TIGER_BASE_URL, getTigerYearPath(year), getTiger2010Subdir(year), filename);
    }

    // Build target path in cache storage
    String yearPath = String.format("year=%d", year);
    String cachePath = storageProvider.resolvePath(cacheDirectory, yearPath);
    cachePath = storageProvider.resolvePath(cachePath, "cbsa");
    String zipCachePath = storageProvider.resolvePath(cachePath, filename);

    // Check manifest first - if parquet already converted, no need to download raw files
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey("cbsa", params);
      if (cacheManifest.isMaterialized(cacheKey)) {
        LOGGER.debug("CBSA parquet already converted per manifest for year {} - skipping raw download", year);
        return null; // No need to download raw files if parquet exists
      }
    }

    // Check manifest for cached raw shapefiles
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey("cbsa", params);
      if (cacheManifest.isCached(cacheKey)) {
        LOGGER.debug("CBSA shapefile cached per manifest for year {}", year);
        return downloadCacheToTemp(cachePath, "cbsa_" + year);
      }
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

    // Mark as cached in manifest
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      getGeoManifest().markCached("cbsa", year, params, cachePath, zipFile.length());
      cacheManifest.save(this.operatingDirectory);
    }

    // Keep temp directory for immediate use
    return tempDir;
  }

  /**
   * Get the cache directory.
   */
  public String getCacheDir() {
    return cacheDirectory;
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
   * @param outputPath Optional parquet path - if provided and file exists, skip download
   */
  public File downloadSchoolDistrictsForYear(int year, String outputPath) throws IOException {
    // Download school districts for selected states only
    String[] stateFips = {"06", "48", "36", "12"}; // CA, TX, NY, FL
    File tempDir = Files.createTempDirectory("tiger-school_districts-" + year + "-").toFile();
    boolean hasAnyDownloads = false;

    for (String fips : stateFips) {
      // Build target path in cache storage for this state
      String yearPath = String.format("year=%d", year);
      String cachePath = storageProvider.resolvePath(cacheDirectory, yearPath);
      cachePath = storageProvider.resolvePath(cachePath, "school_districts");
      cachePath = storageProvider.resolvePath(cachePath, fips);

      // Check manifest first - if parquet already converted for this state, skip
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("state", fips);
        params.put("year", String.valueOf(year));
        CacheKey cacheKey = new CacheKey("school_districts", params);
        if (cacheManifest.isMaterialized(cacheKey)) {
          LOGGER.debug("School districts parquet already converted per manifest for state {} year {} - skipping raw download", fips, year);
          // Still need to copy to temp dir if we have cached data
          File stateTemp = downloadCacheToTemp(cachePath, "school_districts_" + fips + "_" + year);
          if (stateTemp != null) {
            File stateTempTarget = new File(tempDir, fips);
            stateTempTarget.mkdirs();
            for (File file : stateTemp.listFiles()) {
              Files.copy(file.toPath(), new File(stateTempTarget, file.getName()).toPath());
            }
            hasAnyDownloads = true;
          }
          continue;
        }
      }

      // Check manifest for cached raw shapefiles for this state
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("state", fips);
        params.put("year", String.valueOf(year));
        CacheKey cacheKey = new CacheKey("school_districts", params);
        if (cacheManifest.isCached(cacheKey)) {
          LOGGER.debug("School districts shapefile cached per manifest for state {} year {}", fips, year);
          // Download this state's data to temp dir
          File stateTemp = downloadCacheToTemp(cachePath, "school_districts_" + fips + "_" + year);
          if (stateTemp != null) {
            // Copy to main temp dir under state subdirectory
            File stateTempTarget = new File(tempDir, fips);
            stateTempTarget.mkdirs();
            for (File file : stateTemp.listFiles()) {
              Files.copy(file.toPath(), new File(stateTempTarget, file.getName()).toPath());
            }
            hasAnyDownloads = true;
          }
          continue;
        }
      }

      if (!autoDownload) {
        LOGGER.info("Auto-download disabled. School districts not found for state {} year {}", fips, year);
        continue;
      }

      // Try different school district types
      String[] districtTypes = {"unsd", "elsd", "scsd"}; // Unified, Elementary, Secondary
      boolean stateHasDownloads = false;

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
          stateHasDownloads = true;
          hasAnyDownloads = true;
        } catch (IOException e) {
          LOGGER.debug("Failed to download school districts for state {} type {}: {}", fips, type, e.getMessage());
        }
      }

      // If this state had any successful downloads, upload to cache and mark in manifest
      if (stateHasDownloads) {
        File stateDir = new File(tempDir, fips);
        // Upload extracted files to cache storage for this state
        uploadDirectoryToStorage(stateDir, cachePath);

        // Mark as cached in manifest with state parameter
        if (cacheManifest != null) {
          java.util.Map<String, String> params = new java.util.HashMap<>();
          params.put("state", fips);
          getGeoManifest().markCached("school_districts", year, params, cachePath, 0);
          cacheManifest.save(this.operatingDirectory);
        }
      }
    }

    if (!hasAnyDownloads) {
      LOGGER.warn("No school district shapefiles were successfully downloaded or cached for year {}", year);
      return null;
    }

    // Keep temp directory for immediate use
    return tempDir;
  }

  /**
   * Helper to check if we should skip downloading raw file (manifest says already converted).
   * Does NOT check parquet existence - that happens later during conversion.
   */
  private boolean shouldSkipDownload(String dataType, int year, String outputPath) {
    // Only check manifest - if it says converted, we can skip download
    // We don't check parquet existence here because we want the raw file regardless
    // (for potential re-processing, debugging, etc.)
    if (cacheManifest != null) {
      java.util.Map<String, String> params = new java.util.HashMap<>();
      params.put("type", dataType);
      params.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey(dataType, params);
      if (cacheManifest.isMaterialized(cacheKey)) {
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
      params.put("year", String.valueOf(year));
      CacheKey cacheKey = new CacheKey(dataType, params);
      if (cacheManifest.isMaterialized(cacheKey)) {
        LOGGER.debug("Parquet already converted per manifest: {}", targetFilePath);
        return;
      }
    }

    // Defensive check if file already exists (for backfill/legacy data)
    if (storageProvider != null && storageProvider.exists(targetFilePath)) {
      LOGGER.debug("Target output file already exists, skipping: {}", targetFilePath);
      // Update manifest since file exists but wasn't tracked
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("year", String.valueOf(year));
        CacheKey cacheKey = new CacheKey(dataType, params);
        cacheManifest.markMaterialized(cacheKey, targetFilePath);
        cacheManifest.save(this.operatingDirectory);
      }
      return;
    }

    if (storageProvider == null) {
      LOGGER.error("StorageProvider is null, cannot convert to Parquet");
      return;
    }

    LOGGER.info("Converting TIGER data from {} to parquet: {}", sourceDir, targetFilePath);

    // Try DuckDB spatial conversion first (5-8x faster)
    // Find the ZIP file in the source directory
    File zipFile = findZipFileInDirectory(sourceDir);
    if (zipFile != null && zipFile.exists()) {
      try {
        LOGGER.info("Using DuckDB spatial for conversion: {}", dataType);
        convertToParquetViaDuckDB(zipFile.getAbsolutePath(), targetFilePath, dataType, year);
        return; // Success!
      } catch (Exception e) {
        LOGGER.warn("DuckDB spatial conversion failed, falling back to Java parser: {}",
            e.getMessage());
        LOGGER.debug("DuckDB error details: ", e);
        // Fall through to Java-based conversion
      }
    } else {
      LOGGER.warn("No ZIP file found in {}, using Java parser (requires extracted shapefiles)",
          sourceDir);
    }

    // Fallback: Use ShapefileToParquetConverter (Java-based)
    ShapefileToParquetConverter converter = new ShapefileToParquetConverter(storageProvider);

    LOGGER.info("Calling Java converter for table type: {} with source: {}", dataType, sourceDir);

    try {
      // Convert based on the table type
      converter.convertSingleShapefileType(sourceDir, targetFilePath, dataType);

      // Mark materialization complete in manifest
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("year", String.valueOf(year));
        CacheKey cacheKey = new CacheKey(dataType, params);
        cacheManifest.markMaterialized(cacheKey, targetFilePath);
        cacheManifest.save(this.operatingDirectory);
      }
    } catch (Exception e) {
      LOGGER.error("Error converting {} to Parquet", dataType, e);
    }
  }

  /**
   * Convert TIGER shapefile to Parquet using DuckDB spatial extension.
   * This method provides 5-8x performance improvement over Java-based parsing.
   *
   * <p>Uses DuckDB's ST_Read() with /vsizip/ virtual file system to read shapefiles
   * directly from ZIP archives without extraction, then writes to Parquet in a single operation.
   *
   * @param zipFilePath Path to the downloaded TIGER ZIP file
   * @param targetFilePath Target Output file path
   * @param dataType Table type (e.g., "states", "counties")
   * @param year Year of the data
   * @throws Exception if DuckDB conversion fails
   */
  public void convertToParquetViaDuckDB(String zipFilePath, String targetFilePath,
      String dataType, int year) throws Exception {

    LOGGER.info("Converting TIGER shapefile to Parquet via DuckDB: {} -> {}",
        zipFilePath, targetFilePath);

    // Determine shapefile name from data type and year
    String shapefileName = determineShapefileName(dataType, year);

    // Get field mappings from GeoConceptualMapper
    org.apache.calcite.adapter.govdata.geo.GeoConceptualMapper mapper =
        org.apache.calcite.adapter.govdata.geo.GeoConceptualMapper.getInstance();
    java.util.Map<String, Object> dimensions = new java.util.HashMap<>();
    dimensions.put("year", year);
    dimensions.put("dataSource", "tiger");

    // Build SQL query to read shapefile and convert to Parquet
    // ST_Read uses GDAL's /vsizip/ to read directly from ZIP without extraction
    String sql = buildDuckDBConversionSQL(zipFilePath, shapefileName, targetFilePath, dataType, dimensions, mapper);

    // Execute conversion via DuckDB
    try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
      // Load spatial extension
      try (java.sql.Statement stmt = conn.createStatement()) {
        stmt.execute("INSTALL spatial");
        stmt.execute("LOAD spatial");
        LOGGER.debug("Loaded DuckDB spatial extension");
      }

      // Execute conversion
      long startMs = System.currentTimeMillis();
      try (java.sql.Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
        long elapsedMs = System.currentTimeMillis() - startMs;
        LOGGER.info("DuckDB conversion completed in {}ms: {}", elapsedMs, targetFilePath);
      }

      // Mark materialization complete in manifest
      if (cacheManifest != null) {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("year", String.valueOf(year));
        CacheKey cacheKey = new CacheKey(dataType, params);
        cacheManifest.markMaterialized(cacheKey, targetFilePath);
        cacheManifest.save(this.operatingDirectory);
      }
    }
  }

  /**
   * Build DuckDB SQL query for shapefile to Materialization.
   * Uses field mappings from GeoConceptualMapper to extract correct fields for the year.
   */
  private String buildDuckDBConversionSQL(String zipFilePath, String shapefileName,
      String targetFilePath, String dataType,
      java.util.Map<String, Object> dimensions,
      org.apache.calcite.adapter.govdata.geo.GeoConceptualMapper mapper) {

    // Get field mappings for this table
    java.util.Map<String, org.apache.calcite.adapter.govdata.AbstractConceptualMapper.VariableMapping> mappings =
        mapper.getVariablesForTable(dataType, dimensions);

    // Build SELECT clause with field name mapping
    StringBuilder selectClause = new StringBuilder();
    for (org.apache.calcite.adapter.govdata.AbstractConceptualMapper.VariableMapping mapping : mappings.values()) {
      if (selectClause.length() > 0) {
        selectClause.append(", ");
      }

      String sourceField = mapping.getVariable();
      String targetField = mapping.getConceptualName();

      // Handle geometry field specially - convert to WKT
      if ("geometry".equals(targetField) || "geom".equals(sourceField)) {
        selectClause.append("ST_AsText(geom) as geometry");
      } else if (!sourceField.equals(targetField)) {
        selectClause.append(sourceField).append(" as ").append(targetField);
      } else {
        selectClause.append(sourceField);
      }
    }

    // Build full SQL query
    return String.format(
        "COPY (" +
            "  SELECT %s" +
            "  FROM ST_Read('/vsizip/%s/%s')" +
            ") TO '%s' (FORMAT PARQUET)",
        selectClause, zipFilePath, shapefileName, targetFilePath);
  }

  /**
   * Determine the shapefile name within the ZIP archive based on data type and year.
   * TIGER naming conventions: tl_{year}_{geography}.shp
   */
  private String determineShapefileName(String dataType, int year) {
    switch (dataType) {
      case "states":
        return String.format("tl_%d_us_state.shp", year);
      case "counties":
        return String.format("tl_%d_us_county.shp", year);
      case "places":
        return String.format("tl_%d_{state_fips}_place.shp", year);
      case "zctas":
        return String.format("tl_%d_us_zcta5%s.shp", year, year >= 2020 ? "20" : "10");
      case "census_tracts":
        return String.format("tl_%d_{state_fips}_tract.shp", year);
      case "block_groups":
        return String.format("tl_%d_{state_fips}_bg.shp", year);
      case "cbsa":
        return String.format("tl_%d_us_cbsa.shp", year);
      case "congressional_districts":
        int congress = year >= 2023 ? 118 : (year >= 2021 ? 117 : (year >= 2019 ? 116 : 115));
        return String.format("tl_%d_us_cd%d.shp", year, congress);
      case "school_districts":
        return String.format("tl_%d_{state_fips}_unsd.shp", year);
      default:
        throw new IllegalArgumentException("Unknown TIGER data type: " + dataType);
    }
  }

  /**
   * Find the ZIP file in a directory (for DuckDB spatial conversion).
   * TIGER downloads are typically stored as ZIP files in the cache.
   *
   * @param sourceDir Directory to search
   * @return First ZIP file found, or null if none
   */
  private File findZipFileInDirectory(File sourceDir) {
    if (sourceDir == null || !sourceDir.exists() || !sourceDir.isDirectory()) {
      return null;
    }

    File[] zipFiles = sourceDir.listFiles((dir, name) -> name.endsWith(".zip"));
    if (zipFiles != null && zipFiles.length > 0) {
      return zipFiles[0]; // Return first ZIP file found
    }

    return null;
  }

  /**
   * Check if a shapefile exists in the cache.
   */
  public boolean isShapefileAvailable(String category) {
    // For local filesystem cache
    if (cacheDirectory != null && !cacheDirectory.startsWith("s3://")) {
      File dir = new File(cacheDirectory, category);
      if (!dir.exists()) {
        return false;
      }
      File[] shpFiles = dir.listFiles((d, name) -> name.endsWith(".shp"));
      return shpFiles != null && shpFiles.length > 0;
    }

    // For remote storage, check if any .shp files exist in the category path
    String categoryPath = storageProvider.resolvePath(cacheDirectory, category);
    try {
      java.util.List<FileEntry> files = storageProvider.listFiles(categoryPath, true);
      return files.stream().anyMatch(f -> !f.isDirectory() && f.getPath().endsWith(".shp"));
    } catch (IOException e) {
      LOGGER.warn("Failed to check shapefile availability: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Check if output file has been converted, with self-healing fallback to file existence.
   * Uses centralized self-healing in DuckDBCacheStore to avoid code duplication.
   *
   * <p>For GEO data, we compare parquet timestamps against shapefile timestamps instead of JSON.
   *
   * @param dataType Type of data (e.g., "states", "counties")
   * @param year Year of data
   * @param params Additional parameters for cache key
   * @param shapefileCachePath Path to shapefile directory in cache
   * @param outputPath Full path to output file
   * @return true if parquet exists and is newer than shapefiles, false if conversion needed
   */
  public boolean isMaterializedOrExists(String dataType, int year,
      java.util.Map<String, String> params, String shapefileCachePath, String outputPath) {

    if (cacheManifest == null) {
      return false;
    }

    java.util.Map<String, String> allParams = new java.util.HashMap<>(params != null ? params : new java.util.HashMap<>());
    allParams.put("year", String.valueOf(year));
    CacheKey cacheKey = new CacheKey(dataType, allParams);

    // Create FileChecker that handles shapefile directory lookup for raw file timestamp
    DuckDBCacheStore.FileChecker fileChecker = path -> {
      try {
        if (path.equals(outputPath)) {
          // Output file - use storageProvider
          if (storageProvider.exists(path)) {
            return storageProvider.getMetadata(path).getLastModified();
          }
        } else {
          // Shapefile directory - find .shp file and get its timestamp
          java.util.List<FileEntry> files = storageProvider.listFiles(path, false);
          java.util.Optional<FileEntry> shpFile = files.stream()
              .filter(f -> !f.isDirectory() && f.getPath().endsWith(".shp"))
              .findFirst();
          if (shpFile.isPresent()) {
            return shpFile.get().getLastModified();
          }
          // No .shp file found - return -1 to indicate raw file doesn't exist
          return -1;
        }
      } catch (IOException e) {
        LOGGER.debug("Error checking file existence for {}: {}", path, e.getMessage());
      }
      return -1;
    };

    // Use centralized self-healing
    return ((GeoCacheManifest) cacheManifest).isMaterializedWithSelfHealing(
        cacheKey, outputPath, shapefileCachePath, fileChecker);
  }

  /**
   * Check if a multi-state dataset needs processing (for census_tracts, block_groups, school_districts).
   * Returns true if ANY state needs processing OR if output file doesn't exist/is outdated.
   *
   * @param dataType Data type identifier
   * @param year Year of the data
   * @param states Array of state FIPS codes to check
   * @param shapefileCacheBasePath Base cache path where state-specific shapefiles would be stored
   * @param outputPath Path to the combined output file
   * @return true if processing is needed, false if parquet is up-to-date
   */
  public boolean needsProcessingMultiState(String dataType, int year, String[] states,
      String shapefileCacheBasePath, String outputPath) {

    // 1. Check if parquet exists and get its timestamp
    try {
      if (!storageProvider.exists(outputPath)) {
        LOGGER.debug("Parquet does not exist, processing needed: {}", outputPath);
        return true; // Parquet doesn't exist - needs processing
      }

      long parquetModTime = storageProvider.getMetadata(outputPath).getLastModified();

      // 2. Check manifest for each state
      if (cacheManifest != null) {
        boolean anyStateMissingInManifest = false;
        for (String stateFips : states) {
          java.util.Map<String, String> params = new java.util.HashMap<>();
          params.put("state", stateFips);
          params.put("year", String.valueOf(year));
          CacheKey cacheKey = new CacheKey(dataType, params);
          if (!cacheManifest.isMaterialized(cacheKey)) {
            anyStateMissingInManifest = true;
            LOGGER.debug("{} needs processing for state {} year {} (not in manifest)",
                dataType, stateFips, year);
            break;
          }
        }

        if (!anyStateMissingInManifest) {
          // All states are in manifest - no processing needed
          return false;
        }
      }

      // 3. Defensive fallback: check if any state's shapefile is newer than parquet
      //    This handles the case where manifest is missing/incomplete but shapefiles exist
      boolean anyShapefileNewer = false;
      for (String stateFips : states) {
        String stateCachePath = storageProvider.resolvePath(shapefileCacheBasePath, "state=" + stateFips);
        try {
          java.util.List<FileEntry> files = storageProvider.listFiles(stateCachePath, false);
          java.util.Optional<FileEntry> shpFile = files.stream()
              .filter(f -> !f.isDirectory() && f.getPath().endsWith(".shp"))
              .findFirst();

          if (shpFile.isPresent() && shpFile.get().getLastModified() > parquetModTime) {
            LOGGER.info("Shapefile for state {} is newer than parquet, reconversion needed: {} (year={})",
                stateFips, dataType, year);
            anyShapefileNewer = true;
            break;
          }
        } catch (IOException e) {
          LOGGER.debug("Could not check shapefile timestamp for state {}: {}", stateFips, e.getMessage());
          // Continue checking other states
        }
      }

      if (anyShapefileNewer) {
        return true; // At least one shapefile is newer - needs reconversion
      }

      // 4. Parquet exists, is up-to-date, but manifest is incomplete - update manifest
      if (cacheManifest != null) {
        LOGGER.info("⚡ Parquet exists and is up-to-date, updating cache manifest: {} (year={})",
            dataType, year);
        for (String stateFips : states) {
          java.util.Map<String, String> params = new java.util.HashMap<>();
          params.put("state", stateFips);
          params.put("year", String.valueOf(year));
          CacheKey cacheKey = new CacheKey(dataType, params);
          cacheManifest.markMaterialized(cacheKey, outputPath);
        }
        cacheManifest.save(this.operatingDirectory);
      }

      return false; // Parquet is up-to-date, no processing needed

    } catch (IOException e) {
      LOGGER.warn("Failed to check output file for {} year {}: {}", dataType, year, e.getMessage());
      return true; // Error checking - process to be safe
    }
  }
}
