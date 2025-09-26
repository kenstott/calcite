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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

  private static final String FRED_API_BASE = "https://api.stlouisfed.org/fred";
  private static final int FRED_RATE_LIMIT_REQUESTS_PER_MINUTE = 120;
  private static final int FRED_API_DELAY_MS = 60000 / FRED_RATE_LIMIT_REQUESTS_PER_MINUTE; // ~500ms
  private static final int MAX_RESULTS_PER_REQUEST = 1000;

  // Common search terms to get comprehensive series coverage
  private static final List<String> COMPREHENSIVE_SEARCH_TERMS = Arrays.asList(
      "gdp", "unemployment", "inflation", "interest", "employment", "cpi", "ppi",
      "wages", "income", "productivity", "trade", "housing", "retail", "manufacturing",
      "services", "energy", "commodity", "stock", "bond", "treasury", "federal",
      "state", "regional", "international", "currency", "exchange", "population",
      "labor", "consumer", "producer", "industrial", "construction", "agriculture"
  );

  // Major FRED categories to browse systematically
  private static final List<Integer> MAJOR_CATEGORIES = Arrays.asList(
      1,    // National Accounts
      10,   // Population, Employment, & Labor Markets
      32992, // Money, Banking, & Finance
      32455, // International Data
      3,    // Production & Business Activity
      32991, // Prices
      32263  // Academic Data
  );

  private final String fredApiKey;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final String cacheDir;
  private final String parquetDir;

  private long lastRequestTime = 0;
  private final Set<String> processedSeriesIds = new HashSet<>();

  public FredCatalogDownloader(String fredApiKey, String cacheDir, String parquetDir) {
    this.fredApiKey = fredApiKey;
    this.cacheDir = cacheDir;
    this.parquetDir = parquetDir;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build();
    this.objectMapper = new ObjectMapper();

    // Ensure directories exist
    try {
      Files.createDirectories(Paths.get(cacheDir));
      Files.createDirectories(Paths.get(parquetDir));
    } catch (IOException e) {
      throw new RuntimeException("Failed to create directories", e);
    }
  }

  /**
   * Download comprehensive FRED series catalog using paginated series listing.
   * Uses the /fred/series endpoint to get all 841,000+ series with proper pagination.
   */
  public void downloadCatalog() throws IOException, InterruptedException {
    LOGGER.info("Starting FRED catalog download");

    String catalogCacheFile = cacheDir + "/fred_catalog_complete.json";

    // Check if we have recent cached data (less than 24 hours old)
    File cacheFile = new File(catalogCacheFile);
    if (cacheFile.exists() &&
        (System.currentTimeMillis() - cacheFile.lastModified()) < TimeUnit.HOURS.toMillis(24)) {
      LOGGER.info("Using cached FRED catalog from {}", catalogCacheFile);
      convertCatalogToParquet(catalogCacheFile);
      return;
    }

    // Use comprehensive series listing with proper pagination
    LOGGER.info("Downloading complete FRED series catalog via paginated series listing...");
    List<Map<String, Object>> allSeries = downloadAllSeriesPaginated();

    LOGGER.info("Downloaded {} total series from FRED catalog", allSeries.size());

    // Save complete results to cache
    objectMapper.writeValue(new File(catalogCacheFile), allSeries);

    // Convert to Parquet
    convertCatalogToParquet(catalogCacheFile);
  }

  /**
   * Download all FRED series using comprehensive pagination.
   * Uses the /fred/series endpoint without search terms to get complete coverage.
   */
  private List<Map<String, Object>> downloadAllSeriesPaginated()
      throws IOException, InterruptedException {
    List<Map<String, Object>> allSeries = new ArrayList<>();
    int offset = 0;
    boolean hasMoreResults = true;
    long startTime = System.currentTimeMillis();
    int requestCount = 0;

    LOGGER.info("Beginning comprehensive FRED catalog download (estimated 841,000+ series)");
    LOGGER.info("Rate limit: {} requests/minute, estimated time: ~7 hours", FRED_RATE_LIMIT_REQUESTS_PER_MINUTE);

    while (hasMoreResults) {
      try {
        // Use the comprehensive series endpoint without search constraints
        String url = String.format("%s/series?api_key=%s&file_type=json&limit=%d&offset=%d&sort_order=series_id",
            FRED_API_BASE, fredApiKey, MAX_RESULTS_PER_REQUEST, offset);

        JsonNode response = makeApiRequest(url);
        JsonNode seriesArray = response.get("seriess");
        requestCount++;

        if (seriesArray != null && seriesArray.size() > 0) {
          for (JsonNode series : seriesArray) {
            Map<String, Object> seriesMap = objectMapper.convertValue(series,
                new TypeReference<Map<String, Object>>() {});
            allSeries.add(seriesMap);
          }

          offset += seriesArray.size();
          hasMoreResults = seriesArray.size() == MAX_RESULTS_PER_REQUEST;

          // Progress logging every 10,000 series
          if (allSeries.size() % 10000 == 0) {
            long elapsedMinutes = (System.currentTimeMillis() - startTime) / 60000;
            double seriesPerMinute = allSeries.size() / Math.max(elapsedMinutes, 1.0);
            LOGGER.info("Progress: {} series downloaded, {} requests made, {} series/min, elapsed: {} min",
                allSeries.size(), requestCount, String.format("%.0f", seriesPerMinute), elapsedMinutes);
          }
        } else {
          hasMoreResults = false;
        }
      } catch (Exception e) {
        LOGGER.error("Error downloading series at offset {}: {}", offset, e.getMessage());
        // On error, wait longer before retrying
        Thread.sleep(FRED_API_DELAY_MS * 2);
        throw e;
      }
    }

    long totalMinutes = (System.currentTimeMillis() - startTime) / 60000;
    LOGGER.info("Completed FRED catalog download: {} series, {} requests, {} minutes",
        allSeries.size(), requestCount, totalMinutes);

    return allSeries;
  }

  /**
   * Browse series by category using fred/category/series endpoint.
   */
  private List<Map<String, Object>> downloadCategoryBrowse(int categoryId)
      throws IOException, InterruptedException {
    List<Map<String, Object>> allResults = new ArrayList<>();
    int offset = 0;
    boolean hasMoreResults = true;

    while (hasMoreResults) {
      String url = String.format("%s/category/series?category_id=%d&api_key=%s&file_type=json&limit=%d&offset=%d",
          FRED_API_BASE, categoryId, fredApiKey, MAX_RESULTS_PER_REQUEST, offset);

      JsonNode response = makeApiRequest(url);
      JsonNode seriesArray = response.get("seriess");

      if (seriesArray != null && seriesArray.size() > 0) {
        for (JsonNode series : seriesArray) {
          Map<String, Object> seriesMap = objectMapper.convertValue(series,
              new TypeReference<Map<String, Object>>() {});
          // Add category information
          seriesMap.put("category_id", categoryId);
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

    // Get updates from last 30 days
    LocalDate startDate = LocalDate.now().minusDays(30);
    String startDateStr = startDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

    while (hasMoreResults) {
      String url = String.format("%s/series/updates?api_key=%s&file_type=json&limit=%d&offset=%d&start_time=%s",
          FRED_API_BASE, fredApiKey, MAX_RESULTS_PER_REQUEST, offset, startDateStr);

      JsonNode response = makeApiRequest(url);
      JsonNode seriesArray = response.get("seriess");

      if (seriesArray != null && seriesArray.size() > 0) {
        for (JsonNode series : seriesArray) {
          Map<String, Object> seriesMap = objectMapper.convertValue(series,
              new TypeReference<Map<String, Object>>() {});
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
      HttpResponse<String> response = httpClient.send(request,
          HttpResponse.BodyHandlers.ofString());
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
   * Convert cached JSON catalog to partitioned Parquet format.
   */
  private void convertCatalogToParquet(String jsonFile) throws IOException {
    LOGGER.info("Converting FRED catalog to Parquet format");

    // Read JSON data
    List<Map<String, Object>> seriesList = objectMapper.readValue(
        new File(jsonFile), new TypeReference<List<Map<String, Object>>>() {});

    if (seriesList.isEmpty()) {
      LOGGER.warn("No series data found in catalog file");
      return;
    }

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

      // Category information (if available)
      transformed.put("category_id", series.get("category_id"));
      transformed.put("category_name", series.get("category_name"));

      // Source information (if available)
      transformed.put("source_id", series.get("source_id"));
      transformed.put("source_name", series.get("source_name"));

      transformedSeries.add(transformed);
    }

    // Create year-partitioned directory structure
    String currentYear = String.valueOf(LocalDate.now().getYear());
    String catalogParquetDir = parquetDir + "/type=catalog/year=" + currentYear;

    Path catalogDir = Paths.get(catalogParquetDir);
    Files.createDirectories(catalogDir);

    // Write to temporary JSON file for conversion
    String tempJsonFile = catalogParquetDir + "/fred_data_series_catalog_temp.json";
    objectMapper.writeValue(new File(tempJsonFile), transformedSeries);

    // Convert JSON to Parquet using duckdb (similar to other downloaders)
    String parquetFile = catalogParquetDir + "/fred_data_series_catalog.parquet";
    convertJsonToParquet(tempJsonFile, parquetFile);

    // Clean up temp file
    Files.deleteIfExists(Paths.get(tempJsonFile));

    LOGGER.info("FRED catalog converted to Parquet: {} ({} series)",
        parquetFile, transformedSeries.size());
  }

  /**
   * Convert JSON file to Parquet using external process (similar to other downloaders).
   */
  private void convertJsonToParquet(String jsonFile, String parquetFile) throws IOException {
    // Use duckdb command if available, otherwise use Java-based conversion
    try {
      ProcessBuilder pb = new ProcessBuilder(
          "duckdb", "-c",
          String.format("COPY (SELECT * FROM read_json('%s')) TO '%s'", jsonFile, parquetFile)
      );

      Process process = pb.start();
      int exitCode = process.waitFor();

      if (exitCode == 0) {
        LOGGER.debug("Successfully converted {} to Parquet using DuckDB", jsonFile);
        return;
      }
    } catch (Exception e) {
      LOGGER.debug("DuckDB conversion failed, using fallback method: {}", e.getMessage());
    }

    // Fallback: copy JSON file as-is (Calcite can read JSON too)
    Files.copy(Paths.get(jsonFile), Paths.get(parquetFile.replace(".parquet", ".json")));
    LOGGER.info("Saved catalog as JSON file: {}", parquetFile.replace(".parquet", ".json"));
  }
}