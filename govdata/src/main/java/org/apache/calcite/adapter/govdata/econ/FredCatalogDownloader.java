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

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
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
  private static final List<String> COMPREHENSIVE_SEARCH_TERMS =
      Arrays.asList("gdp", "unemployment", "inflation", "interest", "employment", "cpi", "ppi",
      "wages", "income", "productivity", "trade", "housing", "retail", "manufacturing",
      "services", "energy", "commodity", "stock", "bond", "treasury", "federal",
      "state", "regional", "international", "currency", "exchange", "population",
      "labor", "consumer", "producer", "industrial", "construction", "agriculture");

  // Major FRED categories to browse systematically
  private static final List<Integer> MAJOR_CATEGORIES =
      Arrays.asList(1,    // National Accounts
      10,   // Population, Employment, & Labor Markets
      32992, // Money, Banking, & Finance
      32455, // International Data
      3,    // Production & Business Activity
      32991, // Prices
      32263);  // Academic Data

  private final String fredApiKey;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final String cacheDir;
  private final String parquetDir;
  private final org.apache.calcite.adapter.file.storage.StorageProvider storageProvider;
  private final CacheManifest cacheManifest;

  private long lastRequestTime = 0;
  private final Set<String> processedSeriesIds = new HashSet<>();
  private final Map<String, List<Map<String, Object>>> partitionedSeries = new HashMap<>();

  public FredCatalogDownloader(String fredApiKey, String cacheDir, String parquetDir,
                               org.apache.calcite.adapter.file.storage.StorageProvider storageProvider,
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
      throws IOException, InterruptedException {
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
  public void downloadCatalog() throws IOException, InterruptedException {
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
      throws IOException, InterruptedException {
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

    // Combine all text for pattern matching
    String allText = (seriesId + " " + title + " " + notes).toLowerCase();

    // Bureau of Labor Statistics patterns
    if (containsAny(allText, "bureau of labor", "bls", "current employment", "unemployment rate",
        "labor force", "employment situation", "job openings", "layoffs", "quits")) {
      return "Bureau of Labor Statistics";
    }
    if (seriesId != null && containsAny(seriesId.toLowerCase(), "unrate", "lns", "ces", "cps", "jts", "ppi", "cpi")) {
      return "Bureau of Labor Statistics";
    }

    // Bureau of Economic Analysis patterns
    if (containsAny(allText, "bureau of economic analysis", "bea", "gross domestic product",
        "personal income", "consumer spending", "business investment", "international trade")) {
      return "Bureau of Economic Analysis";
    }
    if (seriesId != null && containsAny(seriesId.toLowerCase(), "gdp", "pce", "bea", "nipa", "ita", "sagdp")) {
      return "Bureau of Economic Analysis";
    }

    // Federal Reserve Board patterns
    if (containsAny(allText, "board of governors", "federal reserve board", "fed", "monetary policy",
        "interest rate", "federal funds", "discount rate", "reserve requirements")) {
      return "Board of Governors";
    }
    if (seriesId != null && containsAny(seriesId.toLowerCase(), "dgs", "dff", "fedfunds", "bogz", "h15", "h6")) {
      return "Board of Governors";
    }

    // U.S. Census Bureau patterns
    if (containsAny(allText, "u.s. census bureau", "census bureau", "census", "housing starts",
        "building permits", "construction spending", "retail sales", "manufacturing")) {
      return "U.S. Census Bureau";
    }
    if (seriesId != null && containsAny(seriesId.toLowerCase(), "houst", "permit", "census", "rsxfs", "ttlcons")) {
      return "U.S. Census Bureau";
    }

    // Treasury Department patterns
    if (containsAny(allText, "u.s. treasury", "treasury department", "treasury yields", "government debt",
        "federal debt", "treasury securities", "bills", "notes", "bonds")) {
      return "U.S. Treasury";
    }
    if (seriesId != null && containsAny(seriesId.toLowerCase(), "gs", "tb", "treasury", "debt")) {
      return "U.S. Treasury";
    }

    // Energy Information Administration patterns
    if (containsAny(allText, "energy information administration", "eia", "crude oil", "natural gas",
        "petroleum", "energy consumption", "electricity", "coal", "renewable energy")) {
      return "Energy Information Administration";
    }
    if (seriesId != null && containsAny(seriesId.toLowerCase(), "dcoilwtico", "eia", "energy")) {
      return "Energy Information Administration";
    }

    // World Bank patterns
    if (containsAny(allText, "world bank", "world development indicators", "international development",
        "global economy", "developing countries", "world bank group")) {
      return "World Bank";
    }

    // OECD patterns
    if (containsAny(allText, "oecd", "organisation for economic", "organization for economic",
        "developed countries", "international economic", "oecd countries")) {
      return "OECD";
    }

    // International Monetary Fund patterns
    if (containsAny(allText, "international monetary fund", "imf", "international finance",
        "exchange rates", "balance of payments", "international reserves")) {
      return "International Monetary Fund";
    }

    // University of Michigan patterns
    if (containsAny(allText, "university of michigan", "consumer sentiment", "consumer expectations",
        "thomson reuters", "surveys of consumers")) {
      return "University of Michigan";
    }
    if (seriesId != null && containsAny(seriesId.toLowerCase(), "umcsi", "umcsent")) {
      return "University of Michigan";
    }

    // Conference Board patterns
    if (containsAny(allText, "conference board", "leading indicators", "coincident indicators",
        "consumer confidence", "help wanted")) {
      return "Conference Board";
    }

    // Institute for Supply Management patterns
    if (containsAny(allText, "institute for supply management", "ism", "purchasing managers",
        "manufacturing index", "non-manufacturing index", "pmi")) {
      return "Institute for Supply Management";
    }

    // Chicago Board of Trade patterns
    if (containsAny(allText, "chicago board of trade", "cbot", "commodity prices", "futures")) {
      return "Chicago Board of Trade";
    }

    // National Association of Realtors patterns
    if (containsAny(allText, "national association of realtors", "nar", "existing home sales",
        "pending home sales", "home price index")) {
      return "National Association of Realtors";
    }

    // Default to Federal Reserve if no specific source detected
    return "Federal Reserve";
  }

  /**
   * Check if text contains any of the given patterns.
   */
  private boolean containsAny(String text, String... patterns) {
    for (String pattern : patterns) {
      if (text.contains(pattern)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Determine if a FRED series is active or discontinued based on its metadata.
   *
   * @param series The series metadata map
   * @return "active" if the series is currently active, "discontinued" if discontinued
   */
  public String detectSeriesStatus(Map<String, Object> series) {
    String observationEnd = series.get("observation_end") != null ?
        series.get("observation_end").toString() : "";
    String lastUpdated = series.get("last_updated") != null ?
        series.get("last_updated").toString() : "";
    String notes = series.get("notes") != null ?
        series.get("notes").toString().toLowerCase() : "";
    String title = series.get("title") != null ?
        series.get("title").toString().toLowerCase() : "";

    // Check for explicit discontinuation indicators in notes or title
    if (containsAny(notes, "discontinued", "no longer", "ceased", "terminated",
        "ended", "superseded", "replaced by", "not published", "not available")) {
      return "discontinued";
    }

    if (containsAny(title, "discontinued", "obsolete", "historical only")) {
      return "discontinued";
    }

    // Check observation end date - if it's before 2020, likely discontinued
    if (!observationEnd.isEmpty()) {
      try {
        LocalDate endDate = LocalDate.parse(observationEnd);
        LocalDate cutoffDate = LocalDate.of(2020, 1, 1);

        if (endDate.isBefore(cutoffDate)) {
          return "discontinued";
        }
      } catch (Exception e) {
        // If date parsing fails, continue with other checks
        LOGGER.debug("Failed to parse observation_end date: {}", observationEnd);
      }
    }

    // Check last updated date - if very old, might be discontinued
    if (!lastUpdated.isEmpty()) {
      try {
        // FRED last_updated format is typically "2024-01-15 08:31:05-06"
        String dateOnly = lastUpdated.split(" ")[0];
        LocalDate updateDate = LocalDate.parse(dateOnly);
        LocalDate twoYearsAgo = LocalDate.now().minusYears(2);

        if (updateDate.isBefore(twoYearsAgo)) {
          return "discontinued";
        }
      } catch (Exception e) {
        // If date parsing fails, continue
        LOGGER.debug("Failed to parse last_updated date: {}", lastUpdated);
      }
    }

    // If observation_end is "9999-12-31" or similar, it's typically active
    if (observationEnd.contains("9999")) {
      return "active";
    }

    // Default to active if no clear discontinuation indicators
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

      if (categories != null && categories.size() > 0) {
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
   * Download all FRED series using comprehensive pagination.
   * Uses the /fred/series/search endpoint with a broad search to get complete coverage.
   */
  private List<Map<String, Object>> downloadAllSeriesPaginated()
      throws IOException, InterruptedException {
    List<Map<String, Object>> allSeries = new ArrayList<>();
    Set<String> seenSeriesIds = new HashSet<>();
    long startTime = System.currentTimeMillis();
    int requestCount = 0;

    LOGGER.info("Beginning comprehensive FRED catalog download using search API");
    LOGGER.info("Target: Download all 841,000+ FRED series for complete catalog coverage");

    // Use comprehensive search patterns to get complete coverage
    // Start with empty search, then use single letters and common terms
    List<String> searchPatterns = new ArrayList<>();
    searchPatterns.add(""); // Empty search gets popular series

    // Add single letters to catch all series starting with each letter
    for (char c = 'a'; c <= 'z'; c++) {
      searchPatterns.add(String.valueOf(c));
    }
    for (char c = '0'; c <= '9'; c++) {
      searchPatterns.add(String.valueOf(c));
    }

    // Add common economic terms for better coverage
    searchPatterns.addAll(
        Arrays.asList(
        "GDP", "CPI", "unemployment", "inflation", "interest",
        "trade", "housing", "manufacturing", "retail", "employment",
        "income", "price", "index", "rate", "growth", "production",
        "sales", "inventory", "debt", "credit", "loan", "mortgage"));

    for (String searchText : searchPatterns) {
      int offset = 0;
      boolean hasMoreResults = true;

      while (hasMoreResults) { // Download all available series
        try {
          // Use the search endpoint which supports pagination
          String url =
              String.format("%s/series/search?search_text=%s&api_key=%s&file_type=json&limit=%d&offset=%d", FRED_API_BASE, URLEncoder.encode(searchText, StandardCharsets.UTF_8.toString()),
              fredApiKey, MAX_RESULTS_PER_REQUEST, offset);

          JsonNode response = makeApiRequest(url);
          JsonNode seriesArray = response.get("seriess");
          requestCount++;

          if (seriesArray != null && seriesArray.size() > 0) {
            for (JsonNode series : seriesArray) {
              String seriesId = series.get("id").asText();
              if (!seenSeriesIds.contains(seriesId)) {
                seenSeriesIds.add(seriesId);
                Map<String, Object> seriesMap =
                    objectMapper.convertValue(series, new TypeReference<Map<String, Object>>() {});
                allSeries.add(seriesMap);
              }
            }

            offset += seriesArray.size();
            hasMoreResults = seriesArray.size() == MAX_RESULTS_PER_REQUEST;

            // Progress logging every 10,000 series
            if (allSeries.size() % 10000 == 0 && allSeries.size() > 0) {
              long elapsedMinutes = (System.currentTimeMillis() - startTime) / 60000;
              double seriesPerMinute = allSeries.size() / Math.max(elapsedMinutes, 1.0);
              double estimatedTotalMinutes = 841000.0 / Math.max(seriesPerMinute, 1.0);
              LOGGER.info("Progress: {} unique series found, {} requests made, {} series/min, elapsed: {} min, estimated total: {} min",
                  allSeries.size(), requestCount, String.format("%.0f", seriesPerMinute),
                  elapsedMinutes, String.format("%.0f", estimatedTotalMinutes));
            }
          } else {
            hasMoreResults = false;
          }
        } catch (Exception e) {
          LOGGER.error("Error downloading series for search '{}' at offset {}: {}",
              searchText, offset, e.getMessage());
          // Continue with next search pattern
          break;
        }
      }
    }

    long totalMinutes = (System.currentTimeMillis() - startTime) / 60000;
    LOGGER.info("Completed FRED catalog download: {} unique series, {} requests, {} minutes",
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

    // First get the category name
    String categoryName = getCategoryName(categoryId);

    while (hasMoreResults) {
      String url =
          String.format("%s/category/series?category_id=%d&api_key=%s&file_type=json&limit=%d&offset=%d", FRED_API_BASE, categoryId, fredApiKey, MAX_RESULTS_PER_REQUEST, offset);

      JsonNode response = makeApiRequest(url);
      JsonNode seriesArray = response.get("seriess");

      if (seriesArray != null && seriesArray.size() > 0) {
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

      if (seriesArray != null && seriesArray.size() > 0) {
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
        try (java.io.InputStream inputStream = storageProvider.openInputStream(jsonFile);
             java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
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
   * Convert existing partition cache files to Parquet format.
   * Uses StorageProvider for S3 compatibility.
   */
  private void convertExistingPartitionsToParquet() throws IOException {
    LOGGER.info("Converting existing partition cache files to Parquet format");

    String catalogPath = storageProvider.resolvePath(cacheDir, "type=reference");

    try {
      List<org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry> files =
          storageProvider.listFiles(catalogPath, true);

      if (files == null || files.isEmpty()) {
        LOGGER.warn("No existing partition cache files found");
        return;
      }

      // Filter for JSON catalog files and skip macOS metadata
      for (org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry file : files) {
        if (!file.isDirectory() &&
            file.getName().equals("reference_fred_series.json") &&
            !file.getName().startsWith(".")) {
          try {
            convertSinglePartitionToParquet(file.getPath());
          } catch (IOException e) {
            LOGGER.error("Failed to convert partition {}: {}", file.getPath(), e.getMessage());
          }
        }
      }
    } catch (IOException e) {
      LOGGER.error("Error listing partition cache directory: {}", e.getMessage());
    }
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
    try (java.io.InputStream inputStream = storageProvider.openInputStream(jsonFile);
         java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
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

    if (cacheManifest.isParquetConverted("catalog", 0, partitionParams)) {
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
    cacheManifest.markParquetConverted("catalog", 0, partitionParams, parquetFile);

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
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractEconDataDownloader.loadTableColumns("reference_fred_series");
    convertInMemoryToParquetViaDuckDB("reference_fred_series", columns, transformedSeries, parquetFile);
  }

  /**
   * Converts in-memory records to Parquet using DuckDB.
   * Simplified version for FredCatalogDownloader (doesn't extend AbstractGovDataDownloader).
   */
  private void convertInMemoryToParquetViaDuckDB(
      String tableName,
      List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns,
      List<Map<String, Object>> records,
      String fullParquetPath) throws IOException {

    // Write records to temporary JSON file in cache
    String tempJsonPath = fullParquetPath.replace(".parquet", "_temp.json");
    String fullTempJsonPath = storageProvider.resolvePath(cacheDir,
        tempJsonPath.substring(tempJsonPath.lastIndexOf('/') + 1));

    try {
      // Write JSON
      writeJsonRecords(fullTempJsonPath, records);

      // Convert using DuckDB
      String sql = org.apache.calcite.adapter.govdata.AbstractGovDataDownloader.buildDuckDBConversionSql(
          columns, null, fullTempJsonPath, fullParquetPath);

      LOGGER.debug("DuckDB conversion SQL:\n{}", sql);

      // Execute using in-memory DuckDB connection
      try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
           java.sql.Statement stmt = conn.createStatement()) {

        // Load DuckDB extensions (quackformers, spatial, etc.)
        loadDuckDBExtensions(conn);

        // Execute the COPY statement
        stmt.execute(sql);

        LOGGER.info("Successfully converted {} to Parquet using DuckDB", tableName);

      } catch (java.sql.SQLException e) {
        String errorMsg = String.format(
            "DuckDB conversion failed for table '%s': %s", tableName, e.getMessage());
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
   * Loads DuckDB extensions for data conversion (quackformers, spatial, etc.).
   */
  private void loadDuckDBExtensions(java.sql.Connection conn) {
    String[][] extensions = {
        {"quackformers", "FROM community"},
        {"spatial", ""},
        {"h3", "FROM community"},
        {"excel", ""},
        {"fts", ""}
    };

    for (String[] ext : extensions) {
      try {
        LOGGER.debug("Loading DuckDB extension: {}", ext[0]);
        conn.createStatement().execute("INSTALL " + ext[0] + " " + ext[1]);
        conn.createStatement().execute("LOAD " + ext[0]);
        LOGGER.debug("Successfully loaded extension: {}", ext[0]);
      } catch (java.sql.SQLException e) {
        LOGGER.warn("Failed to load extension '{}': {}", ext[0], e.getMessage());
      }
    }
  }

  /**
   * Writes in-memory records to a JSON file.
   */
  private void writeJsonRecords(String fullJsonPath, List<Map<String, Object>> records) throws IOException {
    // StorageProvider.writeFile automatically creates parent directories if needed

    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    objectMapper.writeValue(baos, records);
    storageProvider.writeFile(fullJsonPath, baos.toByteArray());

    LOGGER.debug("Wrote {} records to temporary JSON: {}", records.size(), fullJsonPath);
  }

}
