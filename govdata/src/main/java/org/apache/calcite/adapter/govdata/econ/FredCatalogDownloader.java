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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.AbstractGovDataDownloader;
import org.apache.calcite.adapter.govdata.CacheKey;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Downloads and processes FRED economic data series catalog.
 *
 * <p>Provides comprehensive access to FRED's 841,000+ economic data series
 * through multiple API endpoints:
 * <ul>
 *   <li>Series search by text query</li>
 *   <li>Category-based browsing</li>
 *   <li>Recently updated series tracking</li>
 *   <li>Series metadata and descriptions</li>
 * </ul>
 *
 * <p>Implements rate limiting (120 requests/minute) and caching to avoid
 * overwhelming the FRED API while providing comprehensive catalog data.
 */
public class FredCatalogDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(FredCatalogDownloader.class);

  private static final String FRED_API_BASE = FredCatalogConfig.getApiBaseUrl();
  private static final int FRED_API_DELAY_MS = FredCatalogConfig.getApiDelayMs();
  private static final int MAX_RESULTS_PER_REQUEST = FredCatalogConfig.getMaxResultsPerRequest();
  private static final List<Integer> MAJOR_CATEGORIES = FredCatalogConfig.getMajorCategoryIds();

  private final String fredApiKey;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final String cacheDir;
  private final String parquetDir;
  private final StorageProvider storageProvider;
  private final CacheManifest cacheManifest;

  private long lastRequestTime = 0;
  private final Map<String, List<Map<String, Object>>> partitionedSeries = new HashMap<>();

  public FredCatalogDownloader(String fredApiKey, String cacheDir, String parquetDir,
                               StorageProvider storageProvider,
                               CacheManifest cacheManifest) {
    this.fredApiKey = fredApiKey;
    this.cacheDir = cacheDir;
    this.parquetDir = parquetDir;
    this.storageProvider = storageProvider;
    this.cacheManifest = cacheManifest;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();
    this.objectMapper = new ObjectMapper();

    // Directory creation handled automatically by StorageProvider when writing files
  }

  /**
   * Download FRED catalog with cache checking.
   *
   * @param fredMinPopularity Minimum popularity threshold
   * @param fredCatalogForceRefresh Force refresh flag
   */
  public void downloadCatalogIfNeeded(int fredMinPopularity, boolean fredCatalogForceRefresh)
      throws IOException {
    // Check if catalog already downloaded via cache manifest
    List<String> catalogSeries = cacheManifest.getCachedCatalogSeries(fredMinPopularity);
    boolean catalogCached = (catalogSeries != null && !catalogSeries.isEmpty());

    if (fredCatalogForceRefresh) {
      cacheManifest.invalidateCatalogSeriesCache(fredMinPopularity);
      LOGGER.info("Force refresh enabled - invalidated catalog series cache and redownloading");
      catalogCached = false;
    }

    if (!catalogCached) {
      LOGGER.info("Downloading FRED reference_fred_series catalog");
      downloadCatalog();
      LOGGER.info("Completed FRED reference_fred_series catalog download");
    } else {
      LOGGER.info("FRED catalog already cached ({} series), skipping download", catalogSeries.size());
    }
  }

  /**
   * Download comprehensive FRED series catalog directly into partitioned files.
   * Creates individual JSON/Parquet files for each category/frequency/source combination.
   */
  public void downloadCatalog() throws IOException {
    LOGGER.info("Starting FRED catalog download with direct partitioning");

    // Use category-based approach with direct partitioning
    LOGGER.info("Downloading FRED series catalog via category browsing with direct partitioning...");
    LOGGER.info("This creates individual files per category/frequency/source combination");
    downloadAllSeriesDirectlyPartitioned();

    LOGGER.info("Downloaded and partitioned FRED catalog into {} unique partitions", partitionedSeries.size());

    // Convert all partitions to Parquet
    convertPartitionsToParquet();
  }

  /**
   * Download all FRED series using category-based approach with direct partitioning.
   * Creates individual cache files for each category/frequency/source combination.
   */
  private void downloadAllSeriesDirectlyPartitioned()
      throws IOException {
    Set<String> seenSeriesIds = new HashSet<>();
    long startTime = System.currentTimeMillis();
    int requestCount = 0;
    int totalSeriesCount = 0;

    LOGGER.info("Beginning comprehensive FRED catalog download with direct partitioning");
    LOGGER.info("Target: Download all 841,000+ FRED series into individual partition files");
    LOGGER.info("Will create separate files for each category/frequency/source combination");

    // Start with root categories and recursively browse their children
    for (int categoryId : MAJOR_CATEGORIES) {
      try {
        LOGGER.info("Browsing category {} for series", categoryId);
        List<Map<String, Object>> categorySeries = downloadCategoryBrowse(categoryId);
        requestCount++;

        // Process and partition series immediately
        for (Map<String, Object> series : categorySeries) {
          String seriesId = (String) series.get("id");
          if (!seenSeriesIds.contains(seriesId)) {
            enrichSeriesWithSource(series);
            addSeriesToPartition(series);
            seenSeriesIds.add(seriesId);
            totalSeriesCount++;
          }
        }

        // Also get child categories
        List<Integer> childCategories = downloadChildCategories(categoryId);
        for (int childId : childCategories) {
          try {
            List<Map<String, Object>> childSeries = downloadCategoryBrowse(childId);
            requestCount++;

            for (Map<String, Object> series : childSeries) {
              String seriesId = (String) series.get("id");
              if (!seenSeriesIds.contains(seriesId)) {
                enrichSeriesWithSource(series);
                addSeriesToPartition(series);
                seenSeriesIds.add(seriesId);
                totalSeriesCount++;
              }
            }

            // Progress logging and periodic flushing every 10,000 series
            if (totalSeriesCount % 10000 == 0 && totalSeriesCount > 0) {
              long elapsedMinutes = (System.currentTimeMillis() - startTime) / 60000;
              double seriesPerMinute = totalSeriesCount / Math.max(elapsedMinutes, 1.0);
              double estimatedTotalMinutes = 841000.0 / Math.max(seriesPerMinute, 1.0);
              LOGGER.info("Progress: {} unique series found, {} partitions, {} requests made, {} series/min, elapsed: {} min, estimated total: {} min",
                  totalSeriesCount, partitionedSeries.size(), requestCount, String.format("%.0f", seriesPerMinute),
                  elapsedMinutes, String.format("%.0f", estimatedTotalMinutes));

              // Periodically flush partitions to disk to manage memory
              flushPartitionsToDisk();
            }
          } catch (Exception e) {
            LOGGER.warn("Error downloading series for child category {}: {}", childId, e.getMessage());
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error downloading series for category {}: {}", categoryId, e.getMessage());
      }
    }

    // Also add recently updated series to catch any we might have missed
    try {
      LOGGER.info("Adding recently updated series");
      List<Map<String, Object>> recentUpdates = downloadSeriesUpdates();
      requestCount++;

      for (Map<String, Object> series : recentUpdates) {
        String seriesId = (String) series.get("id");
        if (!seenSeriesIds.contains(seriesId)) {
          enrichSeriesWithSource(series);
          addSeriesToPartition(series);
          seenSeriesIds.add(seriesId);
          totalSeriesCount++;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Error downloading recent updates: {}", e.getMessage());
    }

    // Final flush of remaining partitions
    flushPartitionsToDisk();

    long totalMinutes = (System.currentTimeMillis() - startTime) / 60000;
    LOGGER.info("Completed FRED catalog download: {} unique series in {} partitions, {} requests, {} minutes",
        totalSeriesCount, partitionedSeries.size(), requestCount, totalMinutes);
  }

  /**
   * Enrich series data with source and status information.
   * Uses comprehensive patterns to detect data sources from series IDs, titles, and notes.
   * Also determines if the series is active or discontinued.
   */
  private void enrichSeriesWithSource(Map<String, Object> series) {
    String sourceName = detectSourceFromSeries(series);
    series.put("source_name", sourceName);

    String status = detectSeriesStatus(series);
    series.put("series_status", status);
  }

  /**
   * Detect data source from series metadata using comprehensive pattern matching.
   */
  private String detectSourceFromSeries(Map<String, Object> series) {
    String seriesId = (String) series.get("id");
    String title = series.get("title") != null ? series.get("title").toString() : "";
    String notes = series.get("notes") != null ? series.get("notes").toString() : "";

    String allText = (seriesId + " " + title + " " + notes).toLowerCase();

    Map<String, FredCatalogConfig.SourceDetector> detectors = FredCatalogConfig.getSourceDetectors();

    for (FredCatalogConfig.SourceDetector detector : detectors.values()) {
        // Check text patterns
        for (String pattern : detector.getTextPatterns()) {
            if (allText.contains(pattern)) {
                return detector.getName();
    }
    }

        // Check series ID patterns
        if (seriesId != null) {
            String lowerSeriesId = seriesId.toLowerCase();
            for (String pattern : detector.getSeriesIdPatterns()) {
                if (lowerSeriesId.contains(pattern)) {
                    return detector.getName();
    }
    }
    }
    }

    // Return default source
    return detectors.get("defaultSource").getName();
  }

  /**
   * Determine if a FRED series is active or discontinued based on its metadata.
   *
   * @param series The series metadata map
   * @return "active" if the series is currently active, "discontinued" if discontinued
   */
  public String detectSeriesStatus(Map<String, Object> series) {
    FredCatalogConfig.StatusDetection statusConfig = FredCatalogConfig.getStatusDetection();
    FredCatalogConfig.StatusDetection.DiscontinuedIndicators discontinued =
        statusConfig.getDiscontinuedIndicators();

    String observationEnd = series.get("observation_end") != null ?
        series.get("observation_end").toString() : "";
    String lastUpdated = series.get("last_updated") != null ?
        series.get("last_updated").toString() : "";
    String notes = series.get("notes") != null ?
        series.get("notes").toString().toLowerCase() : "";
    String title = series.get("title") != null ?
        series.get("title").toString().toLowerCase() : "";

    // Check for explicit discontinuation indicators
    for (String pattern : discontinued.getTextPatterns()) {
        if (notes.contains(pattern)) {
      return "discontinued";
    }
    }

    for (String pattern : discontinued.getTitlePatterns()) {
        if (title.contains(pattern)) {
      return "discontinued";
    }
    }

    // Check observation end date
    if (!observationEnd.isEmpty()) {
      try {
        LocalDate endDate = LocalDate.parse(observationEnd);
            LocalDate cutoffDate = LocalDate.parse(discontinued.getCutoffDate());

        if (endDate.isBefore(cutoffDate)) {
          return "discontinued";
        }
      } catch (Exception e) {
        LOGGER.debug("Failed to parse observation_end date: {}", observationEnd);
      }
    }

    // Check last updated date
    if (!lastUpdated.isEmpty()) {
      try {
        String dateOnly = lastUpdated.split(" ")[0];
        LocalDate updateDate = LocalDate.parse(dateOnly);
            LocalDate staleDate = LocalDate.now().minusYears(discontinued.getStaleDataYears());

            if (updateDate.isBefore(staleDate)) {
          return "discontinued";
        }
      } catch (Exception e) {
        LOGGER.debug("Failed to parse last_updated date: {}", lastUpdated);
      }
    }
    return "active";
  }

  /**
   * Download child categories for a given parent category.
   */
  private List<Integer> downloadChildCategories(int parentCategoryId)
      throws IOException, InterruptedException {
    List<Integer> childCategories = new ArrayList<>();

    String url =
        String.format("%s/category/children?category_id=%d&api_key=%s&file_type=json", FRED_API_BASE, parentCategoryId, fredApiKey);

    JsonNode response = makeApiRequest(url);
    JsonNode categoriesArray = response.get("categories");

    if (categoriesArray != null) {
      for (JsonNode category : categoriesArray) {
        childCategories.add(category.get("id").asInt());
      }
    }

    return childCategories;
  }

  /**
   * Get category name for a given category ID.
   */
  private String getCategoryName(int categoryId) {
    try {
      String url =
          String.format("%s/category?category_id=%d&api_key=%s&file_type=json", FRED_API_BASE, categoryId, fredApiKey);

      JsonNode response = makeApiRequest(url);
      JsonNode categories = response.get("categories");

      if (categories != null && !categories.isEmpty()) {
        String name = categories.get(0).get("name").asText();
        // Normalize category name for use as partition key
        return normalizeCategoryName(name);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to get category name for ID {}: {}", categoryId, e.getMessage());
    }
    return "unknown";
  }

  /**
   * Normalize category name for use as a partition key.
   * Removes special characters and converts to lowercase with underscores.
   */
  private String normalizeCategoryName(String name) {
    return name.toLowerCase()
        .replaceAll("[^a-z0-9]+", "_")  // Replace non-alphanumeric with underscore
        .replaceAll("^_+|_+$", "")      // Remove leading/trailing underscores
        .replaceAll("_+", "_");         // Collapse multiple underscores
  }

  /**
   * Browse series by category using fred/category/series endpoint.
   */
  private List<Map<String, Object>> downloadCategoryBrowse(int categoryId)
      throws IOException, InterruptedException {
    List<Map<String, Object>> allResults = new ArrayList<>();
    int offset = 0;
    boolean hasMoreResults = true;

    // First get the category name
    String categoryName = getCategoryName(categoryId);

    while (hasMoreResults) {
      String url =
          String.format("%s/category/series?category_id=%d&api_key=%s&file_type=json&limit=%d&offset=%d", FRED_API_BASE, categoryId, fredApiKey, MAX_RESULTS_PER_REQUEST, offset);

      JsonNode response = makeApiRequest(url);
      JsonNode seriesArray = response.get("seriess");

      if (seriesArray != null && !seriesArray.isEmpty()) {
        for (JsonNode series : seriesArray) {
          Map<String, Object> seriesMap =
              objectMapper.convertValue(series, new TypeReference<Map<String, Object>>() {});
          // Add category information
          seriesMap.put("category_id", categoryId);
          seriesMap.put("category_name", categoryName);
          allResults.add(seriesMap);
        }

        offset += seriesArray.size();
        hasMoreResults = seriesArray.size() == MAX_RESULTS_PER_REQUEST;
      } else {
        hasMoreResults = false;
      }
    }

    return allResults;
  }

  /**
   * Download recently updated series using fred/series/updates endpoint.
   */
  private List<Map<String, Object>> downloadSeriesUpdates()
      throws IOException, InterruptedException {
    List<Map<String, Object>> allResults = new ArrayList<>();
    int offset = 0;
    boolean hasMoreResults = true;

    // Get updates from last 13 days (FRED API allows "within last two weeks" = < 14 days)
    LocalDate startDate = LocalDate.now().minusDays(13);
    String startDateStr = startDate.atStartOfDay().format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));

    while (hasMoreResults) {
      String url =
          String.format("%s/series/updates?api_key=%s&file_type=json&limit=%d&offset=%d&start_time=%s", FRED_API_BASE, fredApiKey, MAX_RESULTS_PER_REQUEST, offset, startDateStr);

      JsonNode response = makeApiRequest(url);
      JsonNode seriesArray = response.get("seriess");

      if (seriesArray != null && !seriesArray.isEmpty()) {
        for (JsonNode series : seriesArray) {
          Map<String, Object> seriesMap =
              objectMapper.convertValue(series, new TypeReference<Map<String, Object>>() {});
          allResults.add(seriesMap);
        }

        offset += seriesArray.size();
        hasMoreResults = seriesArray.size() == MAX_RESULTS_PER_REQUEST;
      } else {
        hasMoreResults = false;
      }
    }

    return allResults;
  }

  /**
   * Make rate-limited API request to FRED.
   */
  private JsonNode makeApiRequest(String url) throws IOException, InterruptedException {
    // Rate limiting
    long currentTime = System.currentTimeMillis();
    long timeSinceLastRequest = currentTime - lastRequestTime;
    if (timeSinceLastRequest < FRED_API_DELAY_MS) {
      Thread.sleep(FRED_API_DELAY_MS - timeSinceLastRequest);
    }

    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .GET()
        .build();

    try {
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      lastRequestTime = System.currentTimeMillis();

      if (response.statusCode() != 200) {
        throw new IOException("FRED API request failed with status: " + response.statusCode() +
            ", body: " + response.body());
      }

      return objectMapper.readTree(response.body());
    } catch (Exception e) {
      LOGGER.error("API request failed for URL: {}", url, e);
      throw e;
    }
  }

  /**
   * Add a series to the appropriate partition based on category/frequency/source/status.
   */
  private void addSeriesToPartition(Map<String, Object> series) {
    String categoryName = series.get("category_name") != null ?
        normalizePartitionValue(series.get("category_name").toString()) : "uncategorized";
    String frequency = series.get("frequency_short") != null ?
        normalizePartitionValue(series.get("frequency_short").toString()) : "unknown";
    String sourceName = series.get("source_name") != null ?
        normalizePartitionValue(series.get("source_name").toString()) : "unknown";
    String seriesStatus = series.get("series_status") != null ?
        normalizePartitionValue(series.get("series_status").toString()) : "unknown";

    String partitionKey = categoryName + "|" + frequency + "|" + sourceName + "|" + seriesStatus;
    partitionedSeries.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(series);
  }

  /**
   * Flush current partitions to individual JSON cache files and clear memory.
   * Uses StorageProvider for S3 compatibility.
   */
  private void flushPartitionsToDisk() throws IOException {
    for (Map.Entry<String, List<Map<String, Object>>> entry : partitionedSeries.entrySet()) {
      String[] parts = entry.getKey().split("\\|");
      String categoryName = parts[0];
      String frequency = parts[1];
      String sourceName = parts[2];
      String seriesStatus = parts[3];
      List<Map<String, Object>> partitionData = entry.getValue();

      // Build cache file path using StorageProvider
      String relativePath = "type=reference" +
          "/category=" + categoryName +
          "/frequency=" + frequency +
          "/source=" + sourceName +
          "/status=" + seriesStatus +
          "/reference_fred_series.json";
      String jsonFile = storageProvider.resolvePath(cacheDir, relativePath);

      // If file already exists, merge with existing data
      List<Map<String, Object>> existingData = new ArrayList<>();
      if (storageProvider.exists(jsonFile)) {
        try (InputStream inputStream = storageProvider.openInputStream(jsonFile);
             InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
          existingData = objectMapper.readValue(reader, new TypeReference<List<Map<String, Object>>>() {});
        }
      }

      existingData.addAll(partitionData);

      // Write merged data using StorageProvider
      String jsonContent = objectMapper.writeValueAsString(existingData);
      storageProvider.writeFile(jsonFile, jsonContent.getBytes(StandardCharsets.UTF_8));

      LOGGER.debug("Flushed {} series to partition cache: {}", partitionData.size(), jsonFile);
    }

    // Clear memory
    partitionedSeries.clear();
  }

  /**
   * Convert all in-memory partitions to Parquet format.
   */
  private void convertPartitionsToParquet() throws IOException {
    LOGGER.info("Converting {} partitions to Parquet format", partitionedSeries.size());

    for (Map.Entry<String, List<Map<String, Object>>> entry : partitionedSeries.entrySet()) {
      String[] parts = entry.getKey().split("\\|");
      String categoryName = parts[0];
      String frequency = parts[1];
      String sourceName = parts[2];
      String seriesStatus = parts[3];

      // Create corresponding cache file path
      String catalogCacheDir = cacheDir + "/type=reference" +
          "/category=" + categoryName +
          "/frequency=" + frequency +
          "/source=" + sourceName +
          "/status=" + seriesStatus;
      String jsonFile = catalogCacheDir + "/reference_fred_series.json";

      convertSinglePartitionToParquet(jsonFile);
    }
  }

  /**
   * Convert a single partition JSON file to Parquet format.
   * Uses StorageProvider for S3 compatibility.
   */
  private void convertSinglePartitionToParquet(String jsonFile) throws IOException {
    // Skip macOS metadata files
    String fileName = jsonFile.substring(jsonFile.lastIndexOf('/') + 1);
    if (fileName.startsWith(".") || fileName.startsWith("._")) {
      LOGGER.debug("Skipping macOS metadata file: {}", jsonFile);
      return;
    }

    // Extract partition information from file path
    // Remove the resolved base path to get relative path
    String relativePath = jsonFile;
    if (jsonFile.startsWith(cacheDir)) {
      relativePath = jsonFile.substring(cacheDir.length());
      if (relativePath.startsWith("/")) {
        relativePath = relativePath.substring(1);
      }
    }

    // Parse partition keys from path: type=reference/category=X/frequency=Y/source=Z/status=W/
    String[] pathParts = relativePath.split("/");
    if (pathParts.length < 5) {
      LOGGER.warn("Invalid partition path structure: {}", jsonFile);
      return;
    }

    String categoryName = pathParts[1].substring("category=".length());
    String frequency = pathParts[2].substring("frequency=".length());
    String sourceName = pathParts[3].substring("source=".length());
    String seriesStatus = pathParts[4].substring("status=".length());

    // Read JSON data with error handling for corrupted files
    List<Map<String, Object>> seriesList;
    try (InputStream inputStream = storageProvider.openInputStream(jsonFile);
         InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      seriesList = objectMapper.readValue(reader, new TypeReference<List<Map<String, Object>>>() {});
    } catch (Exception e) {
      LOGGER.warn("Skipping corrupted or invalid JSON file {}: {}", jsonFile, e.getMessage());
      return;
    }

    if (seriesList.isEmpty()) {
      LOGGER.debug("No series data found in partition file: {}", jsonFile);
      return;
    }

    // Build parquet path
    String parquetFile = parquetDir + "/type=reference" +
        "/category=" + categoryName +
        "/frequency=" + frequency +
        "/source=" + sourceName +
        "/status=" + seriesStatus +
        "/reference_fred_series.parquet";

    // Check if parquet file already converted using manifest (avoids expensive S3 exists check)
    Map<String, String> partitionParams = new HashMap<>();
    partitionParams.put("category", categoryName);
    partitionParams.put("frequency", frequency);
    partitionParams.put("source", sourceName);
    partitionParams.put("status", seriesStatus);
    partitionParams.put("year", "0");
    CacheKey cacheKey = new CacheKey("catalog", partitionParams);

    if (cacheManifest.isParquetConverted(cacheKey)) {
      LOGGER.debug("Parquet file already converted (manifest), skipping partition {}/{}/{}/{}", categoryName, frequency, sourceName, seriesStatus);
      return;
    }

    LOGGER.debug("Converting partition {}/{}/{}/{} with {} series", categoryName, frequency, sourceName, seriesStatus, seriesList.size());

    // Transform data to match expected schema
    List<Map<String, Object>> transformedSeries = new ArrayList<>();
    for (Map<String, Object> series : seriesList) {
      Map<String, Object> transformed = new HashMap<>();

      // Core fields
      transformed.put("series_id", series.get("id"));
      transformed.put("title", series.get("title"));
      transformed.put("observation_start", series.get("observation_start"));
      transformed.put("observation_end", series.get("observation_end"));
      transformed.put("frequency", series.get("frequency"));
      transformed.put("frequency_short", series.get("frequency_short"));
      transformed.put("units", series.get("units"));
      transformed.put("units_short", series.get("units_short"));
      transformed.put("seasonal_adjustment", series.get("seasonal_adjustment"));
      transformed.put("seasonal_adjustment_short", series.get("seasonal_adjustment_short"));
      transformed.put("last_updated", series.get("last_updated"));
      transformed.put("popularity", series.get("popularity"));
      transformed.put("group_popularity", series.get("group_popularity"));
      transformed.put("notes", series.get("notes"));

      // Category information
      transformed.put("category_id", series.get("category_id"));
      transformed.put("category_name", series.get("category_name"));

      // Source information
      transformed.put("source_id", series.get("source_id"));
      transformed.put("source_name", series.get("source_name"));

      // Status information
      transformed.put("series_status", series.get("series_status"));

      transformedSeries.add(transformed);
    }

    // Write Parquet file using StorageProvider (parquetFile already constructed above)
    writeParquetWithStorageProvider(parquetFile, transformedSeries);

    // Mark as converted in manifest to avoid expensive S3 exists checks on subsequent runs
    // Note: Manifest will be saved centrally by EconSchemaFactory after all conversions complete
    cacheManifest.markParquetConverted(cacheKey, parquetFile);

    LOGGER.debug("Created Parquet file for partition {}/{}/{}/{}: {} series",
        categoryName, frequency, sourceName, seriesStatus, transformedSeries.size());

    // FileSchema's conversion registry automatically tracks this conversion
  }

  /**
   * Normalize a value for use as a partition key.
   * Removes special characters and converts to lowercase with underscores.
   */
  private String normalizePartitionValue(String value) {
    return value.toLowerCase()
        .replaceAll("[^a-z0-9]+", "_")  // Replace non-alphanumeric with underscore
        .replaceAll("^_+|_+$", "")      // Remove leading/trailing underscores
        .replaceAll("_+", "_");         // Collapse multiple underscores
  }


  /**
   * Write Parquet file using StorageProvider.
   */
  private void writeParquetWithStorageProvider(String parquetFile, List<Map<String, Object>> transformedSeries) throws IOException {
    // Load column metadata and write parquet
    List<PartitionedTableConfig.TableColumn> columns =
        AbstractEconDataDownloader.loadTableColumns("reference_fred_series");
    convertInMemoryToParquetViaDuckDB(columns, transformedSeries, parquetFile);
  }

  /**
   * Converts in-memory records to Parquet using DuckDB.
   * Simplified version for FredCatalogDownloader (doesn't extend AbstractGovDataDownloader).
   */
  private void convertInMemoryToParquetViaDuckDB(
      List<PartitionedTableConfig.TableColumn> columns,
      List<Map<String, Object>> records,
      String fullParquetPath) throws IOException {

    // Write records to temporary JSON file in cache
    String tempJsonPath = fullParquetPath.replace(".parquet", "_temp.json");
    String fullTempJsonPath =
        storageProvider.resolvePath(cacheDir, tempJsonPath.substring(tempJsonPath.lastIndexOf('/') + 1));

    try {
      // Write JSON
      writeJsonRecords(fullTempJsonPath, records);

      // Convert using DuckDB
      String sql =
          AbstractGovDataDownloader.buildDuckDBConversionSql(columns, null, fullTempJsonPath, fullParquetPath);

      LOGGER.debug("DuckDB conversion SQL:\n{}", sql);

      // Execute using in-memory DuckDB connection with extensions pre-loaded
      try (Connection conn = org.apache.calcite.adapter.govdata.AbstractGovDataDownloader.getDuckDBConnection();
           Statement stmt = conn.createStatement()) {

        // Execute the COPY statement (extensions already loaded via getDuckDBConnection)
        stmt.execute(sql);

        LOGGER.info("Successfully converted {} to Parquet using DuckDB", "reference_fred_series");

      } catch (SQLException e) {
        String errorMsg =
            String.format("DuckDB conversion failed for table '%s': %s", "reference_fred_series", e.getMessage());
        LOGGER.error(errorMsg, e);
        throw new IOException(errorMsg, e);
      }

      // Clean up temp file
      storageProvider.delete(fullTempJsonPath);

    } catch (IOException | RuntimeException e) {
      // Clean up temp file on error
      try {
        storageProvider.delete(fullTempJsonPath);
      } catch (IOException cleanupError) {
        LOGGER.warn("Failed to clean up temp JSON file: {}", fullTempJsonPath, cleanupError);
      }
      throw e instanceof IOException ? (IOException) e : new IOException(e);
    }
  }


  /**
   * Writes in-memory records to a JSON file.
   */
  private void writeJsonRecords(String fullJsonPath, List<Map<String, Object>> records) throws IOException {
    // StorageProvider.writeFile automatically creates parent directories if needed

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    objectMapper.writeValue(baos, records);
    storageProvider.writeFile(fullJsonPath, baos.toByteArray());

    LOGGER.debug("Wrote {} records to temporary JSON: {}", records.size(), fullJsonPath);
  }

}
