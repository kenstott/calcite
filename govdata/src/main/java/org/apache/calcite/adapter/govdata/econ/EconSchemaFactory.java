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

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;
import org.apache.calcite.model.JsonTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema factory for U.S. economic data sources.
 *
 * <p>Provides access to economic data from:
 * <ul>
 *   <li>Bureau of Labor Statistics (BLS) - Employment, inflation, wages</li>
 *   <li>Federal Reserve (FRED) - Interest rates, GDP, economic indicators</li>
 *   <li>U.S. Treasury - Treasury yields, auction results, debt statistics</li>
 *   <li>Bureau of Economic Analysis (BEA) - GDP components, trade data</li>
 * </ul>
 *
 * <p>Example configuration:
 * <pre>
 * {
 *   "schemas": [{
 *     "name": "ECON",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.econ.EconSchemaFactory",
 *     "operand": {
 *       "blsApiKey": "${BLS_API_KEY}",
 *       "fredApiKey": "${FRED_API_KEY}",
 *       "updateFrequency": "daily",
 *       "historicalDepth": "10 years",
 *       "enabledSources": ["bls", "fred", "treasury"],
 *       "cacheDirectory": "${ECON_CACHE_DIR:/tmp/econ-cache}"
 *     }
 *   }]
 * }
 * </pre>
 */
public class EconSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(EconSchemaFactory.class);

  private Map<String, Map<String, Object>> tableConstraints;
  private List<String> customFredSeries;
  private java.util.Set<String> enabledBlsTables;

/**
   * Build the operand configuration for ECON schema without creating the FileSchema.
   * This method is called by GovDataSchemaFactory to collect table definitions.
   */
  public Map<String, Object> buildOperand(Map<String, Object> operand, org.apache.calcite.adapter.govdata.GovDataSchemaFactory parent) {
    LOGGER.debug("Building ECON schema operand configuration");

    // Access shared services from parent
    StorageProvider storageProvider = parent.getStorageProvider();
    StorageProvider cacheStorageProvider = parent.getCacheStorageProvider();

    // Use directory from operand if provided (contains full S3 URI or local path)
    // Otherwise fall back to environment variables
    String baseDirectory = (String) operand.get("directory");
    String cacheDirectory = (String) operand.get("cacheDirectory");

    if (baseDirectory == null) {
      // Fallback to environment variables
      String govdataParquetDir = getGovDataParquetDir(operand);
      if (govdataParquetDir == null || govdataParquetDir.isEmpty()) {
        throw new IllegalStateException("GOVDATA_PARQUET_DIR environment variable must be set");
      }
      baseDirectory = govdataParquetDir;
    }

    if (cacheDirectory == null) {
      String govdataCacheDir = getGovDataCacheDir(operand);
      if (govdataCacheDir == null || govdataCacheDir.isEmpty()) {
        throw new IllegalStateException("GOVDATA_CACHE_DIR environment variable must be set");
      }
      cacheDirectory = govdataCacheDir;
    }

    // ECON data directories
    // Add source=econ prefix ONCE here when reading from operand
    // All downstream code uses these variables with relative paths (e.g., "type=indicators/year=2020")
    // Final paths: baseDirectory/source=econ + type=indicators/year=2020 = baseDirectory/source=econ/type=indicators/year=2020
    String econRawDir = storageProvider.resolvePath(cacheDirectory, "source=econ");
    String econParquetDir = storageProvider.resolvePath(baseDirectory, "source=econ");

    // Use unified govdata directory structure (matching GEO pattern)
    LOGGER.debug("Using unified govdata directories - cache: {}, parquet: {}",
        econRawDir, econParquetDir);

    // Get year range from unified environment variables
    Integer startYear = getConfiguredStartYear(operand);
    Integer endYear = getConfiguredEndYear(operand);

    LOGGER.debug("Economic data configuration:");
    LOGGER.debug("  Cache directory: {}", econRawDir);
    LOGGER.debug("  Parquet directory: {}", econParquetDir);
    LOGGER.debug("  Year range: {} - {}", startYear, endYear);

    // Get API keys: try operand first (allows model.json to specify key directly),
    // then fall back to environment variable (supports ${ENV_VAR} syntax in model.json)
    // If key comes from operand, set it as system property so metadata-driven downloaders can access it
    String blsApiKey = (String) operand.get("blsApiKey");
    if (blsApiKey == null) {
      blsApiKey = System.getenv("BLS_API_KEY");
    } else {
      // Set as system property for metadata-driven downloaders to access
      System.setProperty("BLS_API_KEY", blsApiKey);
    }

    String fredApiKey = (String) operand.get("fredApiKey");
    if (fredApiKey == null) {
      fredApiKey = System.getenv("FRED_API_KEY");
    } else {
      // Set as system property for metadata-driven downloaders to access
      System.setProperty("FRED_API_KEY", fredApiKey);
    }

    String beaApiKey = (String) operand.get("beaApiKey");
    if (beaApiKey == null) {
      beaApiKey = System.getenv("BEA_API_KEY");
    } else {
      // Set as system property for metadata-driven downloaders to access
      System.setProperty("BEA_API_KEY", beaApiKey);
    }

    // Get enabled sources
    @SuppressWarnings("unchecked")
    List<String> enabledSources = (List<String>) operand.get("enabledSources");
    if (enabledSources == null) {
      enabledSources = java.util.Arrays.asList("bls", "fred", "treasury", "bea", "worldbank");
    }

    LOGGER.debug("  Enabled sources: {}", enabledSources);
    LOGGER.debug("  BLS API key: {}", blsApiKey != null ? "configured" : "not configured");
    LOGGER.debug("  FRED API key: {}", fredApiKey != null ? "configured" : "not configured");
    LOGGER.debug("  BEA API key: {}", beaApiKey != null ? "configured" : "not configured");

    // Parse custom FRED series configuration
    @SuppressWarnings("unchecked")
    List<String> customFredSeries = (List<String>) operand.get("customFredSeries");

    // Store configuration for table definition generation
    this.customFredSeries = customFredSeries;

    // Parse BLS table filtering configuration (needed for both download and schema filtering)
    this.enabledBlsTables = parseBlsTableFilter(operand);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("  Custom FRED series: {}", customFredSeries != null ? customFredSeries.size() + " series configured" : "none");
    }

    // Parse FRED minimum popularity threshold for catalog-based series filtering
    Integer fredMinPopularity = (Integer) operand.get("fredMinPopularity");
    if (fredMinPopularity == null) {
      fredMinPopularity = 50;  // Default: only moderately popular series
    }
    LOGGER.debug("  FRED minimum popularity threshold: {}", fredMinPopularity);

    // Parse FRED catalog cache force refresh flag (default false)
    Boolean fredCatalogForceRefresh = (Boolean) operand.get("fredCatalogForceRefresh");
    if (fredCatalogForceRefresh == null) {
      fredCatalogForceRefresh = false;
    }

    // Check auto-download setting (default true like GEO)
    Boolean autoDownload = (Boolean) operand.get("autoDownload");
    if (autoDownload == null) {
      autoDownload = true;
    }

    // Create mutable operand for modifications
    Map<String, Object> mutableOperand = new java.util.HashMap<>(operand);

    // Download data if auto-download is enabled
    if (autoDownload) {
      LOGGER.debug("Auto-download enabled for ECON data");
      try {
        downloadEconData(mutableOperand, econRawDir, econParquetDir,
            blsApiKey, fredApiKey, beaApiKey, enabledSources, startYear, endYear, storageProvider,
            customFredSeries, cacheStorageProvider, fredMinPopularity, fredCatalogForceRefresh);
      } catch (Exception e) {
        LOGGER.error("Error downloading ECON data", e);
        // Continue even if download fails - existing data may be available
      }
    }

    // Set the directory for FileSchemaFactory to use
    mutableOperand.put("directory", econParquetDir);

    // Separate table definitions into partitioned tables and views
    List<Map<String, Object>> allTables = loadTableDefinitions();

    // Rewrite FK schema names if actual schema name is different from declared name
    String actualSchemaName = (String) operand.get("actualSchemaName");
    if (actualSchemaName != null) {
      rewriteForeignKeySchemaNames(allTables, actualSchemaName);
    }

    List<Map<String, Object>> partitionedTables = new ArrayList<>();
    List<Map<String, Object>> regularTables = new ArrayList<>();

    for (Map<String, Object> table : allTables) {
      String tableType = (String) table.get("type");
      if ("view".equals(tableType)) {
        // This is a view definition - add to regular tables
        regularTables.add(table);
        LOGGER.debug("Adding view to tables array: {}", table.get("name"));
      } else {
        // This is a partitioned table - add to partitioned tables
        partitionedTables.add(table);
      }
    }

    mutableOperand.put("partitionedTables", partitionedTables);
    if (!regularTables.isEmpty()) {
      mutableOperand.put("views", regularTables);
      LOGGER.info("Added {} views to 'views' operand for FileSchema", regularTables.size());
    }

    // Pass through executionEngine if specified (critical for DuckDB vs PARQUET)
    if (operand.containsKey("executionEngine")) {
      mutableOperand.put("executionEngine", operand.get("executionEngine"));
    }
    Map<String, Map<String, Object>> econConstraints = loadTableConstraints();

    // Rewrite FK schema names in constraints if actual schema name is different from declared name
    if (actualSchemaName != null) {
      rewriteConstraintForeignKeySchemaNames(econConstraints, actualSchemaName);
    }

    if (tableConstraints != null) {
      econConstraints.putAll(tableConstraints);
    }

    // Add constraint metadata to operand so FileSchemaFactory can pass it to FileSchema
    mutableOperand.put("tableConstraints", econConstraints);

    // NOTE: We cannot create and register the RawToParquetConverter here because:
    // 1. EconSchemaFactory only builds operand configuration, doesn't create FileSchema
    // 2. The downloaders are created inside downloadEconData() which already finished
    // 3. GovDataSchemaFactory creates the FileSchema after this method returns
    //
    // SOLUTION: GovDataSchemaFactory will need to create the converter and downloaders
    // after schema creation, then cast FileSchema and call registerRawToParquetConverter()
    // This requires downloaders to be recreated in GovDataSchemaFactory.buildEconOperand()

    // Add schema-level comment from JSON metadata
    String schemaComment = loadSchemaComment();
    if (schemaComment != null) {
      mutableOperand.put("comment", schemaComment);
    }

    return mutableOperand;
  }

  @Override public String getSchemaResourceName() {
    return "/econ-schema.json";
  }


  /**
   * Download economic data from various sources following GEO pattern.
   */
  private void downloadEconData(Map<String, Object> operand, String cacheDir, String parquetDir,
      String blsApiKey, String fredApiKey, String beaApiKey, List<String> enabledSources,
      int startYear, int endYear, StorageProvider storageProvider,
      List<String> customFredSeries, StorageProvider cacheStorageProvider, int fredMinPopularity,
      boolean fredCatalogForceRefresh) throws IOException {

    // Operating directory for metadata (.aperio/econ/)
    // This is passed from GovDataSchemaFactory which establishes it centrally
    // The .aperio directory is ALWAYS on local filesystem (working directory), even if parquet data is on S3
    String econOperatingDirectory = (String) operand.get("operatingDirectory");
    if (econOperatingDirectory == null) {
      throw new IllegalStateException("Operating directory must be established by GovDataSchemaFactory");
    }
    LOGGER.debug("Received operating directory from parent: {}", econOperatingDirectory);

    // Directory creation handled automatically by StorageProvider when writing files
    // No need to create cache or parquet directories explicitly

    // Load or create cache manifest for tracking parquet conversions from operating directory
    CacheManifest cacheManifest = CacheManifest.load(econOperatingDirectory);
    LOGGER.debug("Loaded ECON cache manifest from {}", econOperatingDirectory);

    // Handle catalog cache invalidation if force refresh requested
    if (fredCatalogForceRefresh) {
      cacheManifest.invalidateCatalogSeriesCache(fredMinPopularity);
      LOGGER.info("Force refresh enabled - invalidated catalog series cache for threshold {}", fredMinPopularity);
    }

    // cacheStorageProvider is passed as parameter (created by GovDataSchemaFactory)
    LOGGER.debug("Using shared cache storage provider for cache directory: {}", cacheDir);

    // Download FRED series catalog FIRST as a prerequisite for fred_indicators population
    // This must happen before FRED indicators download since we need the catalog to determine series list
    LOGGER.debug("FRED catalog download check: fredApiKey={}, isEmpty={}",
        fredApiKey != null ? "configured" : "null",
        fredApiKey != null ? fredApiKey.isEmpty() : "N/A");
    if (fredApiKey != null && !fredApiKey.isEmpty()) {
      try {
        LOGGER.info("Downloading FRED catalog as prerequisite for indicators");
        // Note: FredCatalogDownloader will be refactored in Phase 6 to use cacheStorageProvider
        // For now it still uses storageProvider for both cache and parquet operations
        FredCatalogDownloader catalogDownloader = new FredCatalogDownloader(fredApiKey, cacheDir, parquetDir, storageProvider, cacheManifest);
        catalogDownloader.downloadCatalog();
      } catch (Exception e) {
        LOGGER.error("Error downloading FRED catalog", e);
      }
    } else {
      LOGGER.warn("Skipping FRED catalog download - API key not available");
    }

    // Download BLS data if enabled
    if (enabledSources.contains("bls") && blsApiKey != null && !blsApiKey.isEmpty()) {
      try {
        BlsDataDownloader blsDownloader = new BlsDataDownloader(blsApiKey, cacheDir, econOperatingDirectory, parquetDir, cacheStorageProvider, storageProvider, cacheManifest);

        // Download all BLS data for the year range (using enabledBlsTables parsed in buildOperand)
        blsDownloader.downloadAll(startYear, endYear, enabledBlsTables);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          String cacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=indicators/year=" + year);

          // Convert employment statistics
          String employmentParquetPath = storageProvider.resolvePath(parquetDir, "type=employment_statistics/frequency=monthly/year=" + year + "/employment_statistics.parquet");
          String employmentRawPath = cacheStorageProvider.resolvePath(cacheDir, "type=employment_statistics/frequency=monthly/year=" + year + "/employment_statistics.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "employment_statistics", year, employmentRawPath, employmentParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            variables.put("frequency", "monthly");
            blsDownloader.convertCachedJsonToParquet("employment_statistics", variables);
            cacheManifest.markParquetConverted("employment_statistics", year, null, employmentParquetPath);
          }

          // Convert inflation metrics
          String inflationParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/frequency=monthly/year=" + year + "/inflation_metrics.parquet");
          String inflationRawPath = cacheStorageProvider.resolvePath(cacheDir, "type=indicators/frequency=monthly/year=" + year + "/inflation_metrics.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "inflation_metrics", year, inflationRawPath, inflationParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            variables.put("frequency", "monthly");
            blsDownloader.convertCachedJsonToParquet("inflation_metrics", variables);
            cacheManifest.markParquetConverted("inflation_metrics", year, null, inflationParquetPath);
          }

          // Convert regional CPI
          String regionalCpiCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=cpi_regional/year=" + year);
          String regionalCpiParquetPath = storageProvider.resolvePath(parquetDir, "type=cpi_regional/year=" + year + "/regional_cpi.parquet");
          String regionalCpiRawPath = cacheStorageProvider.resolvePath(regionalCpiCacheYearPath, "regional_cpi.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "regional_cpi", year, regionalCpiRawPath, regionalCpiParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            blsDownloader.convertCachedJsonToParquet("regional_cpi", variables);
            cacheManifest.markParquetConverted("regional_cpi", year, null, regionalCpiParquetPath);
          }

          // Convert metro CPI
          String metroCpiCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=metro_cpi/frequency=month/year=" + year);
          String metroCpiParquetPath = storageProvider.resolvePath(parquetDir, "type=metro_cpi/frequency=month/year=" + year + "/metro_cpi.parquet");
          String metroCpiRawPath = cacheStorageProvider.resolvePath(metroCpiCacheYearPath, "metro_cpi.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "metro_cpi", year, metroCpiRawPath, metroCpiParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            variables.put("frequency", "month");
            blsDownloader.convertCachedJsonToParquet("metro_cpi", variables);
            cacheManifest.markParquetConverted("metro_cpi", year, null, metroCpiParquetPath);
          }

          // Convert state industry employment
          String stateIndustryCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=state_industry/year=" + year);
          String stateIndustryParquetPath = storageProvider.resolvePath(parquetDir, "type=state_industry/year=" + year + "/state_industry.parquet");
          String stateIndustryRawPath = cacheStorageProvider.resolvePath(stateIndustryCacheYearPath, "state_industry.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "state_industry", year, stateIndustryRawPath, stateIndustryParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            blsDownloader.convertCachedJsonToParquet("state_industry", variables);
            cacheManifest.markParquetConverted("state_industry", year, null, stateIndustryParquetPath);
          }

          // Convert state wages
          String stateWagesCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=state_wages/year=" + year);
          String stateWagesParquetPath = storageProvider.resolvePath(parquetDir, "type=state_wages/year=" + year + "/state_wages.parquet");
          String stateWagesRawPath = cacheStorageProvider.resolvePath(stateWagesCacheYearPath, "state_wages.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "state_wages", year, stateWagesRawPath, stateWagesParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            blsDownloader.convertCachedJsonToParquet("state_wages", variables);
            cacheManifest.markParquetConverted("state_wages", year, null, stateWagesParquetPath);
          }

          // Convert metro industry employment
          String metroIndustryCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=metro_industry/year=" + year);
          String metroIndustryParquetPath = storageProvider.resolvePath(parquetDir, "type=metro_industry/year=" + year + "/metro_industry.parquet");
          String metroIndustryRawPath = cacheStorageProvider.resolvePath(metroIndustryCacheYearPath, "metro_industry.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "metro_industry", year, metroIndustryRawPath, metroIndustryParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            blsDownloader.convertCachedJsonToParquet("metro_industry", variables);
            cacheManifest.markParquetConverted("metro_industry", year, null, metroIndustryParquetPath);
          }

          // Convert metro wages
          String metroWagesCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=metro_wages/year=" + year);
          String metroWagesParquetPath = storageProvider.resolvePath(parquetDir, "type=metro_wages/year=" + year + "/metro_wages.parquet");
          String metroWagesRawPath = cacheStorageProvider.resolvePath(metroWagesCacheYearPath, "metro_wages.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "metro_wages", year, metroWagesRawPath, metroWagesParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            blsDownloader.convertCachedJsonToParquet("metro_wages", variables);
            cacheManifest.markParquetConverted("metro_wages", year, null, metroWagesParquetPath);
          }

          // Convert JOLTS regional
          String joltsRegionalCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=jolts_regional/frequency=monthly/year=" + year);
          String joltsRegionalParquetPath = storageProvider.resolvePath(parquetDir, "type=jolts_regional/frequency=monthly/year=" + year + "/jolts_regional.parquet");
          String joltsRegionalRawPath = cacheStorageProvider.resolvePath(joltsRegionalCacheYearPath, "jolts_regional.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "jolts_regional", year, joltsRegionalRawPath, joltsRegionalParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            variables.put("frequency", "monthly");
            blsDownloader.convertCachedJsonToParquet("jolts_regional", variables);
            cacheManifest.markParquetConverted("jolts_regional", year, null, joltsRegionalParquetPath);
          }

          // Convert county wages
          String countyWagesCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=county_wages/year=" + year);
          String countyWagesParquetPath = storageProvider.resolvePath(parquetDir, "type=county_wages/year=" + year + "/county_wages.parquet");
          String countyWagesRawPath = cacheStorageProvider.resolvePath(countyWagesCacheYearPath, "county_wages.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "county_wages", year, countyWagesRawPath, countyWagesParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            blsDownloader.convertCachedJsonToParquet("county_wages", variables);
            cacheManifest.markParquetConverted("county_wages", year, null, countyWagesParquetPath);
          }

          // Convert JOLTS state
          String joltsStateCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=jolts_state/year=" + year);
          String joltsStateParquetPath = storageProvider.resolvePath(parquetDir, "type=jolts_state/year=" + year + "/jolts_state.parquet");
          String joltsStateRawPath = cacheStorageProvider.resolvePath(joltsStateCacheYearPath, "jolts_state.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "jolts_state", year, joltsStateRawPath, joltsStateParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            blsDownloader.convertCachedJsonToParquet("jolts_state", variables);
            cacheManifest.markParquetConverted("jolts_state", year, null, joltsStateParquetPath);
          }

          // Convert wage growth
          String wageParquetPath = storageProvider.resolvePath(parquetDir, "type=wage_growth/frequency=quarterly/year=" + year + "/wage_growth.parquet");
          String wageRawPath = cacheStorageProvider.resolvePath(cacheDir, "type=wage_growth/frequency=quarterly/year=" + year + "/wage_growth.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "wage_growth", year, wageRawPath, wageParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            variables.put("frequency", "quarterly");
            blsDownloader.convertCachedJsonToParquet("wage_growth", variables);
            cacheManifest.markParquetConverted("wage_growth", year, null, wageParquetPath);
          }
        }

        // Convert reference tables (not partitioned by year, use 0 as sentinel)
        String joltsIndustriesCachePath = cacheStorageProvider.resolvePath(cacheDir, "type=reference");
        String joltsIndustriesParquetPath = storageProvider.resolvePath(parquetDir, "type=reference/jolts_industries.parquet");
        String joltsIndustriesRawPath = cacheStorageProvider.resolvePath(joltsIndustriesCachePath, "jolts_industries.json");
        if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "jolts_industries", 0, joltsIndustriesRawPath, joltsIndustriesParquetPath)) {
          Map<String, String> variables = new HashMap<>();
          blsDownloader.convertCachedJsonToParquet("jolts_industries", variables);
          cacheManifest.markParquetConverted("jolts_industries", 0, null, joltsIndustriesParquetPath);
        }

        String joltsDataelementsCachePath = cacheStorageProvider.resolvePath(cacheDir, "type=reference");
        String joltsDataelementsParquetPath = storageProvider.resolvePath(parquetDir, "type=reference/jolts_dataelements.parquet");
        String joltsDataelementsRawPath = cacheStorageProvider.resolvePath(joltsDataelementsCachePath, "jolts_dataelements.json");
        if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "jolts_dataelements", 0, joltsDataelementsRawPath, joltsDataelementsParquetPath)) {
          Map<String, String> variables = new HashMap<>();
          blsDownloader.convertCachedJsonToParquet("jolts_dataelements", variables);
          cacheManifest.markParquetConverted("jolts_dataelements", 0, null, joltsDataelementsParquetPath);
        }

        LOGGER.debug("BLS data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading BLS data", e);
      }
    }

    // Download FRED indicators data from catalog-based series list
    if (enabledSources.contains("fred") && fredApiKey != null && !fredApiKey.isEmpty()) {
      try {
        LOGGER.info("Building FRED indicators series list from catalog (active series with popularity >= {})", fredMinPopularity);

        // Try to get from cache first (expensive catalog parsing operation)
        List<String> catalogSeries = cacheManifest.getCachedCatalogSeries(fredMinPopularity);

        if (catalogSeries == null) {
          // Cache miss or expired - extract from catalog files
          LOGGER.info("Catalog series cache miss/expired - extracting from catalog files");
          catalogSeries = extractActivePopularSeriesFromCatalog(cacheStorageProvider, cacheDir, fredMinPopularity);

          // Cache the results with configurable TTL (default 365 days for annual refresh)
          Integer catalogCacheTtlDays = (Integer) operand.get("fredCatalogCacheTtlDays");
          if (catalogCacheTtlDays == null) {
            catalogCacheTtlDays = 365;  // Default: 1 year
          }
          cacheManifest.cacheCatalogSeries(fredMinPopularity, catalogSeries, catalogCacheTtlDays);

          LOGGER.info("Cached {} series IDs for {} days", catalogSeries.size(), catalogCacheTtlDays);
        }

        LOGGER.info("Found {} active popular series in FRED catalog", catalogSeries.size());

        // Merge with customFredSeries if provided
        java.util.Set<String> allSeriesIds = new java.util.LinkedHashSet<>(catalogSeries);
        if (customFredSeries != null && !customFredSeries.isEmpty()) {
          allSeriesIds.addAll(customFredSeries);
          LOGGER.info("Added {} custom FRED series, total series count: {}", customFredSeries.size(), allSeriesIds.size());
        }

        // Download and convert each series with series partitioning using metadata-driven approach
        FredDataDownloader fredDownloader = new FredDataDownloader(cacheDir, econOperatingDirectory, parquetDir, cacheStorageProvider, storageProvider, cacheManifest);

        // Download all series for the year range using metadata-driven executeDownload()
        fredDownloader.downloadAll(startYear, endYear, new java.util.ArrayList<>(allSeriesIds));

        // Convert downloaded JSON to Parquet for each series/year combination
        for (String seriesId : allSeriesIds) {
          for (int year = startYear; year <= endYear; year++) {
            // Build paths for this series/year
            String seriesPartition = "type=fred_indicators/series=" + seriesId + "/year=" + year;
            String parquetPath = storageProvider.resolvePath(parquetDir, seriesPartition + "/fred_indicators.parquet");
            String rawPath = cacheStorageProvider.resolvePath(cacheDir, seriesPartition + "/fred_indicators.json");

            // Check if conversion needed
            Map<String, String> params = new HashMap<>();
            params.put("series", seriesId);

            if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "fred_indicators", year, rawPath, parquetPath)) {
              try {
                Map<String, String> variables = new HashMap<>();
                variables.put("year", String.valueOf(year));
                variables.put("series_id", seriesId);
                fredDownloader.convertCachedJsonToParquet("fred_indicators", variables);
                cacheManifest.markParquetConverted("fred_indicators", year, params, parquetPath);
              } catch (Exception e) {
                LOGGER.error("Error converting FRED series {} for year {}: {}", seriesId, year, e.getMessage());
                // Continue with next series
              }
            }
          }
        }

        LOGGER.debug("FRED indicators data download and conversion completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading FRED indicators data", e);
      }
    }

    // Download Treasury data if enabled (no API key required)
    if (enabledSources.contains("treasury")) {
      try {
        TreasuryDataDownloader treasuryDownloader = new TreasuryDataDownloader(cacheDir, econOperatingDirectory, parquetDir, cacheStorageProvider, storageProvider, cacheManifest);

        // Download all Treasury data for the year range
        treasuryDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          String cacheTimeseriesYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=timeseries/year=" + year);

          String yieldsParquetPath = storageProvider.resolvePath(parquetDir, "type=timeseries/frequency=daily/year=" + year + "/treasury_yields.parquet");
          String yieldsRawPath = cacheStorageProvider.resolvePath(cacheTimeseriesYearPath, "treasury_yields.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "treasury_yields", year, yieldsRawPath, yieldsParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            variables.put("frequency", "daily");
            treasuryDownloader.convertCachedJsonToParquet("treasury_yields", variables);
            cacheManifest.markParquetConverted("treasury_yields", year, null, yieldsParquetPath);
          }

          // Convert federal debt data to parquet
          String debtParquetPath = storageProvider.resolvePath(parquetDir, "type=timeseries/frequency=daily/year=" + year + "/federal_debt.parquet");
          String debtRawPath = cacheStorageProvider.resolvePath(cacheTimeseriesYearPath, "federal_debt.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "federal_debt", year, debtRawPath, debtParquetPath)) {
            treasuryDownloader.convertFederalDebtToParquet(cacheTimeseriesYearPath, debtParquetPath);
            cacheManifest.markParquetConverted("federal_debt", year, null, debtParquetPath);
          }
        }

        LOGGER.debug("Treasury data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading Treasury data", e);
      }
    }

    // Download BEA data if enabled
    if (enabledSources.contains("bea") && beaApiKey != null && !beaApiKey.isEmpty()) {
      try {
        BeaDataDownloader beaDownloader = new BeaDataDownloader(cacheDir, econOperatingDirectory, parquetDir, beaApiKey, cacheStorageProvider, storageProvider, cacheManifest);

        // Download all BEA data for the year range
        beaDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year using StorageProvider
        for (int year = startYear; year <= endYear; year++) {
          String cacheIndicatorsYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=indicators/year=" + year);

          // Convert GDP components
          String gdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/gdp_components.parquet");
          String gdpRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "gdp_components.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "gdp_components", year, gdpRawPath, gdpParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            beaDownloader.convertCachedJsonToParquet("gdp_components", variables);
            cacheManifest.markParquetConverted("gdp_components", year, null, gdpParquetPath);
          }

          // Create gdp_statistics table (GDP growth rates from BEA)
          String gdpStatsParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/gdp_statistics.parquet");
          String gdpStatsRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "gdp_statistics.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "gdp_statistics", year, gdpStatsRawPath, gdpStatsParquetPath)) {
            beaDownloader.convertGdpStatisticsToParquet(cacheIndicatorsYearPath, gdpStatsParquetPath);
            cacheManifest.markParquetConverted("gdp_statistics", year, null, gdpStatsParquetPath);
          }

          // Convert regional_income into frequency-partitioned parquet (annual-only dataset)
          String regionalIncomeFreq = "annual";
          java.util.Map<String, String> riParams = new java.util.HashMap<>();
          riParams.put("frequency", regionalIncomeFreq);

          // Raw JSON and Parquet must use identical regional_income paths
          String riSourceDir = cacheStorageProvider.resolvePath(cacheDir, "type=regional_income/frequency=" + regionalIncomeFreq + "/year=" + year);
          String regionalIncomeRawPath = cacheStorageProvider.resolvePath(riSourceDir, "regional_income.json");
          String regionalIncomeParquetPath = storageProvider.resolvePath(parquetDir, "type=regional_income/frequency=" + regionalIncomeFreq + "/year=" + year + "/regional_income.parquet");

          boolean riConverted = cacheManifest.isParquetConverted("regional_income", year, riParams);
          if (!riConverted) {
            boolean upToDate = false;
            try {
              if (storageProvider.exists(regionalIncomeParquetPath)) {
                long parquetMod = storageProvider.getMetadata(regionalIncomeParquetPath).getLastModified();
                if (cacheStorageProvider.exists(regionalIncomeRawPath)) {
                  long rawMod = cacheStorageProvider.getMetadata(regionalIncomeRawPath).getLastModified();
                  upToDate = parquetMod > rawMod;
                } else {
                  upToDate = true;
                }
              }
            } catch (Exception ignore) {
              upToDate = false;
            }

            if (upToDate) {
              cacheManifest.markParquetConverted("regional_income", year, riParams, regionalIncomeParquetPath);
            } else {
              beaDownloader.convertRegionalIncomeToParquet(riSourceDir, regionalIncomeParquetPath);
              cacheManifest.markParquetConverted("regional_income", year, riParams, regionalIncomeParquetPath);
            }
          }

          // Convert State GDP data
          String stateGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=state_gdp/frequency=annual/year=" + year + "/state_gdp.parquet");
          String stateGdpRawPath = cacheStorageProvider.resolvePath(cacheDir, "type=state_gdp/frequency=annual/year=" + year + "/state_gdp.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "state_gdp", year, stateGdpRawPath, stateGdpParquetPath)) {
            beaDownloader.convertStateGdpToParquet(stateGdpRawPath, stateGdpParquetPath);
            cacheManifest.markParquetConverted("state_gdp", year, null, stateGdpParquetPath);
          }

          // Convert BEA trade statistics into frequency-partitioned parquet detected from the raw JSON
          String tradeRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "trade_statistics.json");
          java.util.Set<String> tradeFreqs;
          try {
            tradeFreqs = beaDownloader.detectTradeFrequencies(cacheIndicatorsYearPath);
          } catch (Exception e) {
            LOGGER.warn("Unable to detect trade_statistics frequencies for year {}: {}", year, e.getMessage());
            tradeFreqs = java.util.Collections.emptySet();
          }

          if (tradeFreqs.isEmpty()) {
            LOGGER.warn("No trade_statistics frequency codes found in {} — skipping trade_statistics parquet conversion for year {}", tradeRawPath, year);
          }

          for (String freqRaw : tradeFreqs) {
            String freq = freqRaw == null ? "" : freqRaw.trim();
            if (freq.isEmpty()) continue;

            java.util.Map<String, String> tradeParams = new java.util.HashMap<>();
            tradeParams.put("frequency", freq);

            String tradeParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/frequency=" + freq + "/year=" + year + "/trade_statistics.parquet");

            boolean converted = cacheManifest.isParquetConverted("trade_statistics", year, tradeParams);
            if (!converted) {
              boolean upToDate = false;
              try {
                if (storageProvider.exists(tradeParquetPath)) {
                  long parquetMod = storageProvider.getMetadata(tradeParquetPath).getLastModified();
                  if (cacheStorageProvider.exists(tradeRawPath)) {
                    long rawMod = cacheStorageProvider.getMetadata(tradeRawPath).getLastModified();
                    upToDate = parquetMod > rawMod;
                  } else {
                    upToDate = true;
                  }
                }
              } catch (Exception ignore) {
                upToDate = false;
              }

              if (upToDate) {
                cacheManifest.markParquetConverted("trade_statistics", year, tradeParams, tradeParquetPath);
              } else {
                beaDownloader.convertTradeStatisticsToParquetForFrequency(cacheIndicatorsYearPath, tradeParquetPath, freq);
                cacheManifest.markParquetConverted("trade_statistics", year, tradeParams, tradeParquetPath);
              }
            }
          }

          // Convert ITA data into frequency-partitioned parquet detected from the raw JSON
          String itaRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "ita_data.json");
          java.util.Set<String> detectedFreqs;
          try {
            detectedFreqs = beaDownloader.detectItaFrequencies(cacheIndicatorsYearPath);
          } catch (Exception e) {
            LOGGER.warn("Unable to detect ITA frequencies for year {}: {}", year, e.getMessage());
            detectedFreqs = java.util.Collections.emptySet();
          }

          if (detectedFreqs.isEmpty()) {
            LOGGER.warn("No ITA frequency codes found in {} — skipping ITA parquet conversion for year {}", itaRawPath, year);
          }

          for (String freqRaw : detectedFreqs) {
            String freq = freqRaw == null ? "" : freqRaw.trim();
            if (freq.isEmpty()) {
              continue;
            }

            java.util.Map<String, String> itaParams = new java.util.HashMap<>();
            itaParams.put("frequency", freq);

            String itaParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/frequency=" + freq + "/year=" + year + "/ita_data.parquet");

            // Check manifest first with params; if not present, defensively check file timestamps
            boolean converted = cacheManifest.isParquetConverted("ita_data", year, itaParams);
            if (!converted) {
              boolean upToDate = false;
              try {
                if (storageProvider.exists(itaParquetPath)) {
                  long parquetMod = storageProvider.getMetadata(itaParquetPath).getLastModified();
                  if (cacheStorageProvider.exists(itaRawPath)) {
                    long rawMod = cacheStorageProvider.getMetadata(itaRawPath).getLastModified();
                    upToDate = parquetMod > rawMod;
                  } else {
                    // Raw missing but parquet exists: consider up-to-date
                    upToDate = true;
                  }
                }
              } catch (Exception ignore) {
                upToDate = false;
              }

              if (upToDate) {
                cacheManifest.markParquetConverted("ita_data", year, itaParams, itaParquetPath);
              } else {
                beaDownloader.convertItaDataToParquetForFrequency(cacheIndicatorsYearPath, itaParquetPath, freq);
                cacheManifest.markParquetConverted("ita_data", year, itaParams, itaParquetPath);
              }
            }
          }

          // Convert industry_gdp into frequency-partitioned parquet detected from the raw JSON
          String industryGdpRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "industry_gdp.json");
          java.util.Set<String> industryFreqs;
          try {
            industryFreqs = beaDownloader.detectIndustryGdpFrequencies(cacheIndicatorsYearPath);
          } catch (Exception e) {
            LOGGER.warn("Unable to detect industry_gdp frequencies for year {}: {}", year, e.getMessage());
            industryFreqs = java.util.Collections.emptySet();
          }

          if (industryFreqs.isEmpty()) {
            LOGGER.warn("No industry_gdp frequency codes found in {} — skipping industry_gdp parquet conversion for year {}", industryGdpRawPath, year);
          }

          for (String freqRaw : industryFreqs) {
            String freq = freqRaw == null ? "" : freqRaw.trim();
            if (freq.isEmpty()) continue;

            java.util.Map<String, String> params = new java.util.HashMap<>();
            params.put("frequency", freq);

            String industryGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/frequency=" + freq + "/year=" + year + "/industry_gdp.parquet");

            boolean converted = cacheManifest.isParquetConverted("industry_gdp", year, params);
            if (!converted) {
              boolean upToDate = false;
              try {
                if (storageProvider.exists(industryGdpParquetPath)) {
                  long parquetMod = storageProvider.getMetadata(industryGdpParquetPath).getLastModified();
                  if (cacheStorageProvider.exists(industryGdpRawPath)) {
                    long rawMod = cacheStorageProvider.getMetadata(industryGdpRawPath).getLastModified();
                    upToDate = parquetMod > rawMod;
                  } else {
                    upToDate = true;
                  }
                }
              } catch (Exception ignore) {
                upToDate = false;
              }

              if (upToDate) {
                cacheManifest.markParquetConverted("industry_gdp", year, params, industryGdpParquetPath);
              } else {
                beaDownloader.convertIndustryGdpToParquetForFrequency(cacheIndicatorsYearPath, industryGdpParquetPath, freq);
                cacheManifest.markParquetConverted("industry_gdp", year, params, industryGdpParquetPath);
              }
            }
          }
        }

        LOGGER.debug("BEA data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading BEA data", e);
      }
    }

    // Phase 3: Optional BLS data sources (jolts_regional, metro_cpi)
    // These are downloaded separately to allow fine-grained control
    if (enabledSources.contains("bls_jolts_regional") && blsApiKey != null && !blsApiKey.isEmpty()) {
      try {
        LOGGER.info("Phase 3: Downloading optional BLS jolts_regional data");
        BlsDataDownloader blsDownloader = new BlsDataDownloader(blsApiKey, cacheDir, econOperatingDirectory, parquetDir, cacheStorageProvider, storageProvider, cacheManifest);
        blsDownloader.downloadJoltsRegional(startYear, endYear);
        LOGGER.debug("Phase 3: jolts_regional download completed");

        // Convert jolts_regional to parquet for each year
        for (int year = startYear; year <= endYear; year++) {
          String joltsRegionalCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=jolts_regional/frequency=monthly/year=" + year);
          String joltsRegionalParquetPath = storageProvider.resolvePath(parquetDir, "type=jolts_regional/frequency=monthly/year=" + year + "/jolts_regional.parquet");
          String joltsRegionalRawPath = cacheStorageProvider.resolvePath(joltsRegionalCacheYearPath, "jolts_regional.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "jolts_regional", year, joltsRegionalRawPath, joltsRegionalParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            variables.put("frequency", "monthly");
            blsDownloader.convertCachedJsonToParquet("jolts_regional", variables);
            cacheManifest.markParquetConverted("jolts_regional", year, null, joltsRegionalParquetPath);
          }
        }
        LOGGER.debug("Phase 3: jolts_regional conversion completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading Phase 3 jolts_regional data", e);
      }
    }

    if (enabledSources.contains("bls_metro_cpi") && blsApiKey != null && !blsApiKey.isEmpty()) {
      try {
        LOGGER.info("Phase 3: Downloading optional BLS metro_cpi data");
        BlsDataDownloader blsDownloader = new BlsDataDownloader(blsApiKey, cacheDir, econOperatingDirectory, parquetDir, cacheStorageProvider, storageProvider, cacheManifest);
        blsDownloader.downloadMetroCpi(startYear, endYear);
        LOGGER.debug("Phase 3: metro_cpi download completed");

        // Convert metro CPI to parquet for each year
        for (int year = startYear; year <= endYear; year++) {
          String metroCpiCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=metro_cpi/year=" + year);
          String metroCpiParquetPath = storageProvider.resolvePath(parquetDir, "type=metro_cpi/year=" + year + "/metro_cpi.parquet");
          String metroCpiRawPath = cacheStorageProvider.resolvePath(metroCpiCacheYearPath, "metro_cpi.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "metro_cpi", year, metroCpiRawPath, metroCpiParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            blsDownloader.convertCachedJsonToParquet("metro_cpi", variables);
            cacheManifest.markParquetConverted("metro_cpi", year, null, metroCpiParquetPath);
          }
        }
        LOGGER.debug("Phase 3: metro_cpi conversion completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading Phase 3 metro_cpi data", e);
      }
    }

    // Phase 3: Optional BEA data sources (regional_income, state_gdp)
    if (enabledSources.contains("bea_regional_income") && beaApiKey != null && !beaApiKey.isEmpty()) {
      try {
        LOGGER.info("Phase 3: Downloading optional BEA regional_income data");
        BeaDataDownloader beaDownloader = new BeaDataDownloader(cacheDir, econOperatingDirectory, parquetDir, beaApiKey, cacheStorageProvider, storageProvider, cacheManifest);
        for (int year = startYear; year <= endYear; year++) {
          beaDownloader.downloadRegionalIncomeForYear(year);
        }
        LOGGER.debug("Phase 3: regional_income download completed");

        // Convert regional_income to parquet for each year under frequency=annual partition
        for (int year = startYear; year <= endYear; year++) {
          String freq = "annual";
          java.util.Map<String, String> riParams = new java.util.HashMap<>();
          riParams.put("frequency", freq);

          // Source dir for raw JSON and target parquet must share the same regional_income path
          String riSourceDir = cacheStorageProvider.resolvePath(cacheDir, "type=regional_income/frequency=" + freq + "/year=" + year);
          String regionalIncomeRawPath = cacheStorageProvider.resolvePath(riSourceDir, "regional_income.json");
          String regionalIncomeParquetPath = storageProvider.resolvePath(parquetDir, "type=regional_income/frequency=" + freq + "/year=" + year + "/regional_income.parquet");

          boolean converted = cacheManifest.isParquetConverted("regional_income", year, riParams);
          if (!converted) {
            boolean upToDate = false;
            try {
              if (storageProvider.exists(regionalIncomeParquetPath)) {
                long parquetMod = storageProvider.getMetadata(regionalIncomeParquetPath).getLastModified();
                if (cacheStorageProvider.exists(regionalIncomeRawPath)) {
                  long rawMod = cacheStorageProvider.getMetadata(regionalIncomeRawPath).getLastModified();
                  upToDate = parquetMod > rawMod;
                } else {
                  upToDate = true;
                }
              }
            } catch (Exception ignore) {
              upToDate = false;
            }

            if (upToDate) {
              cacheManifest.markParquetConverted("regional_income", year, riParams, regionalIncomeParquetPath);
            } else {
              beaDownloader.convertRegionalIncomeToParquet(riSourceDir, regionalIncomeParquetPath);
              cacheManifest.markParquetConverted("regional_income", year, riParams, regionalIncomeParquetPath);
            }
          }
        }
        LOGGER.debug("Phase 3: regional_income conversion completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading Phase 3 regional_income data", e);
      }
    }

    if (enabledSources.contains("bea_state_gdp") && beaApiKey != null && !beaApiKey.isEmpty()) {
      try {
        LOGGER.info("Phase 3: Downloading optional BEA state_gdp data");
        BeaDataDownloader beaDownloader = new BeaDataDownloader(cacheDir, econOperatingDirectory, parquetDir, beaApiKey, cacheStorageProvider, storageProvider, cacheManifest);
        for (int year = startYear; year <= endYear; year++) {
          beaDownloader.downloadStateGdpForYear(year);
        }
        LOGGER.debug("Phase 3: state_gdp download completed");

        // Convert State GDP data to parquet for each year
        for (int year = startYear; year <= endYear; year++) {
          String stateGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=state_gdp/frequency=annual/year=" + year + "/state_gdp.parquet");
          String stateGdpRawPath = cacheStorageProvider.resolvePath(cacheDir, "type=state_gdp/frequency=annual/year=" + year + "/state_gdp.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "state_gdp", year, stateGdpRawPath, stateGdpParquetPath)) {
            beaDownloader.convertStateGdpToParquet(stateGdpRawPath, stateGdpParquetPath);
            cacheManifest.markParquetConverted("state_gdp", year, null, stateGdpParquetPath);
          }
        }
        LOGGER.debug("Phase 3: state_gdp conversion completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading Phase 3 state_gdp data", e);
      }
    }

    // Download World Bank data if enabled (no API key required)
    if (enabledSources.contains("worldbank")) {
      try {
        WorldBankDataDownloader worldBankDownloader = new WorldBankDataDownloader(cacheDir, econOperatingDirectory, parquetDir, cacheStorageProvider, storageProvider, cacheManifest);

        // Download all World Bank data for the year range
        worldBankDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          String worldParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/world_indicators.parquet");
          String cacheWorldBankYearPath = cacheStorageProvider.resolvePath(cacheDir, "type=indicators/year=" + year);
          String worldRawPath = cacheStorageProvider.resolvePath(cacheWorldBankYearPath, "world_indicators.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "world_indicators", year, worldRawPath, worldParquetPath)) {
            Map<String, String> variables = new HashMap<>();
            variables.put("year", String.valueOf(year));
            worldBankDownloader.convertCachedJsonToParquet("world_indicators", variables);
            cacheManifest.markParquetConverted("world_indicators", year, null, worldParquetPath);
          }
        }

        LOGGER.debug("World Bank data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading World Bank data", e);
      }
    }

    // Save cache manifest after all downloads to operating directory
    if (cacheManifest != null) {
      cacheManifest.save(econOperatingDirectory);
    }
  }

  /**
   * Override loadTableDefinitions to add custom FRED table definitions dynamically.
   * This combines static table definitions from econ-schema.json with dynamically
   * generated table definitions for custom FRED series.
   */
  @Override public List<Map<String, Object>> loadTableDefinitions() {
    // Start with base table definitions from econ-schema.json
    List<Map<String, Object>> baseTables = GovDataSubSchemaFactory.super.loadTableDefinitions();
    List<Map<String, Object>> tables = new ArrayList<>();

    LOGGER.info("[DEBUG] Loaded {} base table definitions from econ-schema.json", baseTables.size());

    // Filter BLS tables based on enabledBlsTables configuration
    for (Map<String, Object> table : baseTables) {
      String tableName = (String) table.get("name");

      // Check if this is a BLS table
      boolean isBlsTable = tableName.equals(BlsDataDownloader.TABLE_EMPLOYMENT_STATISTICS)
          || tableName.equals(BlsDataDownloader.TABLE_INFLATION_METRICS)
          || tableName.equals(BlsDataDownloader.TABLE_REGIONAL_CPI)
          || tableName.equals(BlsDataDownloader.TABLE_METRO_CPI)
          || tableName.equals(BlsDataDownloader.TABLE_STATE_INDUSTRY)
          || tableName.equals(BlsDataDownloader.TABLE_STATE_WAGES)
          || tableName.equals(BlsDataDownloader.TABLE_COUNTY_WAGES)
          || tableName.equals(BlsDataDownloader.TABLE_COUNTY_QCEW)
          || tableName.equals(BlsDataDownloader.TABLE_METRO_INDUSTRY)
          || tableName.equals(BlsDataDownloader.TABLE_METRO_WAGES)
          || tableName.equals(BlsDataDownloader.TABLE_JOLTS_REGIONAL)
          || tableName.equals(BlsDataDownloader.TABLE_WAGE_GROWTH)
          || tableName.equals(BlsDataDownloader.TABLE_REGIONAL_EMPLOYMENT);

      if (isBlsTable) {
        // Check if table is enabled (null means all tables enabled)
        if (enabledBlsTables == null || enabledBlsTables.contains(tableName)) {
          tables.add(table);
          LOGGER.debug("[DEBUG] Including BLS table: {} with pattern: {}", tableName, table.get("pattern"));
        } else {
          LOGGER.info("Filtering out BLS table '{}' from schema (not in enabled tables)", tableName);
        }
      } else {
        // Non-BLS table, always include
        tables.add(table);
        LOGGER.debug("[DEBUG] Including non-BLS table: {} with pattern: {}", tableName, table.get("pattern"));
      }
    }

    LOGGER.info("[DEBUG] Total ECON table definitions: {}", tables.size());
    return tables;
  }

  /**
   * Check if parquet file has been converted, with defensive fallback to file existence and timestamp check.
   * This prevents unnecessary reconversion when the manifest is deleted but parquet files still exist.
   *
   * @param cacheManifest Cache manifest to check
   * @param storageProvider Storage provider for file existence checks
   * @param cacheStorageProvider Storage provider for raw file checks
   * @param dataType Type of data (e.g., "gdp_components")
   * @param year Year of data
   * @param rawFilePath Full path to raw source file (JSON/CSV)
   * @param parquetPath Full path to parquet file
   * @return true if parquet exists and is newer than raw file, false if conversion needed
   */
  private boolean isParquetConvertedOrExists(CacheManifest cacheManifest,
      StorageProvider storageProvider, StorageProvider cacheStorageProvider,
      String dataType, int year, String rawFilePath, String parquetPath) {

    // 1. Check manifest first - trust it as source of truth
    if (cacheManifest.isParquetConverted(dataType, year, null)) {
      return true;
    }

    // 2. Defensive check: if a parquet file exists but not in manifest, verify it's up-to-date
    try {
      if (storageProvider.exists(parquetPath)) {
        // Get timestamps for both files
        long parquetModTime = storageProvider.getMetadata(parquetPath).getLastModified();

        // Check if a raw file exists and compare timestamps
        if (cacheStorageProvider.exists(rawFilePath)) {
          long rawModTime = cacheStorageProvider.getMetadata(rawFilePath).getLastModified();

          if (parquetModTime > rawModTime) {
            // Parquet is newer than raw file - update manifest and skip conversion
            LOGGER.info("⚡ Parquet exists and is up-to-date, updating cache manifest: {} (year={})",
                dataType, year);
            cacheManifest.markParquetConverted(dataType, year, null, parquetPath);
            return true;
          } else {
            // Raw file is newer - needs reconversion
            LOGGER.info("Raw file is newer than parquet, reconversion needed: {} (year={})",
                dataType, year);
            return false;
          }
        } else {
          // Raw file doesn't exist but parquet does - consider it up-to-date
          LOGGER.info("⚡ Parquet exists (raw file not found), updating cache manifest: {} (year={})",
              dataType, year);
          cacheManifest.markParquetConverted(dataType, year, null, parquetPath);
          return true;
        }
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to check parquet/raw file timestamps for {} year {}: {}",
          dataType, year, e.getMessage());
      // Fall through to return false - reconvert to be safe
    }

    // 3. Parquet doesn't exist or check failed - needs conversion
    return false;
  }

  /**
   * Parse BLS table filtering configuration from operand.
   * Supports either includeTables (whitelist) or excludeTables (blacklist), but not both.
   *
   * @param operand Schema configuration operand
   * @return Set of enabled table names, or null to download all tables
   */
  private java.util.Set<String> parseBlsTableFilter(Map<String, Object> operand) {
    @SuppressWarnings("unchecked")
    Map<String, Object> blsConfig = (Map<String, Object>) operand.get("blsConfig");
    if (blsConfig == null) {
      return null; // No filtering, download all tables
    }

    @SuppressWarnings("unchecked")
    List<String> includeTables = (List<String>) blsConfig.get("includeTables");
    @SuppressWarnings("unchecked")
    List<String> excludeTables = (List<String>) blsConfig.get("excludeTables");

    // Validate mutual exclusivity
    if (includeTables != null && excludeTables != null) {
      throw new IllegalArgumentException(
          "Cannot specify both 'includeTables' and 'excludeTables' in blsConfig. " +
          "Use includeTables for whitelist or excludeTables for blacklist, but not both.");
    }

    if (includeTables != null) {
      LOGGER.info("BLS table filter: including {} tables: {}", includeTables.size(), includeTables);
      return new java.util.HashSet<>(includeTables);
    }

    if (excludeTables != null) {
      // Return all tables except excluded ones
      java.util.Set<String> allTables =
          new java.util.HashSet<>(
              java.util.Arrays.asList(BlsDataDownloader.TABLE_EMPLOYMENT_STATISTICS,
          BlsDataDownloader.TABLE_INFLATION_METRICS,
          BlsDataDownloader.TABLE_REGIONAL_CPI,
          BlsDataDownloader.TABLE_METRO_CPI,
          BlsDataDownloader.TABLE_STATE_INDUSTRY,
          BlsDataDownloader.TABLE_STATE_WAGES,
          BlsDataDownloader.TABLE_METRO_INDUSTRY,
          BlsDataDownloader.TABLE_METRO_WAGES,
          BlsDataDownloader.TABLE_JOLTS_REGIONAL,
          BlsDataDownloader.TABLE_WAGE_GROWTH,
          BlsDataDownloader.TABLE_REGIONAL_EMPLOYMENT));
      allTables.removeAll(excludeTables);
      LOGGER.info("BLS table filter: excluding {} tables, downloading {} tables",
          excludeTables.size(), allTables.size());
      return allTables;
    }

    return null; // No filtering
  }

  /**
   * Extract series IDs from FRED catalog JSON files filtered by active status and popularity.
   * Reads all catalog JSON files under status=active partitions and filters by popularity threshold.
   *
   * @param cacheStorageProvider Storage provider for reading catalog cache files
   * @param cacheDir Cache directory containing catalog JSON files
   * @param minPopularity Minimum popularity threshold (series with popularity >= this value are included)
   * @return List of series IDs that are active and meet the popularity threshold
   */
  private List<String> extractActivePopularSeriesFromCatalog(
      StorageProvider cacheStorageProvider, String cacheDir, int minPopularity) {
    List<String> seriesIds = new ArrayList<>();

    try {
      // Find all catalog JSON files under status=active partitions
      // Pattern: type=catalog/category=*/frequency=*/source=*/status=active/fred_data_series_catalog.json
      String catalogBasePath = cacheStorageProvider.resolvePath(cacheDir, "type=catalog");

      if (!cacheStorageProvider.isDirectory(catalogBasePath)) {
        LOGGER.warn("FRED catalog cache not found at: {}", catalogBasePath);
        return seriesIds;
      }

      // Recursively find all fred_data_series_catalog.json files under status=active directories
      List<String> catalogFiles = findCatalogFilesInActivePartitions(cacheStorageProvider, catalogBasePath);

      LOGGER.info("Found {} catalog JSON files to process", catalogFiles.size());

      // Process each catalog file
      com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
      int totalSeriesProcessed = 0;
      int seriesPassingFilter = 0;

      for (String catalogFile : catalogFiles) {
        try {
          // Read JSON file as array of series objects using InputStream
          List<Map<String, Object>> seriesList;
          try (java.io.InputStream inputStream = cacheStorageProvider.openInputStream(catalogFile);
               java.io.InputStreamReader reader = new java.io.InputStreamReader(inputStream, java.nio.charset.StandardCharsets.UTF_8)) {
            com.fasterxml.jackson.core.type.TypeReference<List<Map<String, Object>>> typeRef =
                new com.fasterxml.jackson.core.type.TypeReference<List<Map<String, Object>>>() {};
            seriesList = objectMapper.readValue(reader, typeRef);
          }

          // Filter by popularity and extract series IDs
          for (Map<String, Object> series : seriesList) {
            totalSeriesProcessed++;

            // Extract series_id (may be under "id" or "series_id" key)
            String seriesId = (String) series.get("id");
            if (seriesId == null) {
              seriesId = (String) series.get("series_id");
            }

            // Extract popularity (default to 0 if not present)
            Object popularityObj = series.get("popularity");
            int popularity = 0;
            if (popularityObj != null) {
              if (popularityObj instanceof Integer) {
                popularity = (Integer) popularityObj;
              } else if (popularityObj instanceof Number) {
                popularity = ((Number) popularityObj).intValue();
              }
            }

            // Filter by popularity threshold
            if (seriesId != null && popularity >= minPopularity) {
              seriesIds.add(seriesId);
              seriesPassingFilter++;
            }
          }

        } catch (Exception e) {
          LOGGER.warn("Error processing catalog file {}: {}", catalogFile, e.getMessage());
        }
      }

      LOGGER.info("Extracted {} series IDs from catalog (processed {} total series, {} passed filter with popularity >= {})",
          seriesIds.size(), totalSeriesProcessed, seriesPassingFilter, minPopularity);

    } catch (Exception e) {
      LOGGER.error("Error extracting series from FRED catalog", e);
    }

    return seriesIds;
  }

  /**
   * Recursively find all fred_data_series_catalog.json files under status=active partitions.
   *
   * @param storageProvider Storage provider for directory traversal
   * @param basePath Base path to search from (type=catalog directory)
   * @return List of full paths to catalog JSON files in active partitions
   */
  private List<String> findCatalogFilesInActivePartitions(StorageProvider storageProvider, String basePath) {
    List<String> catalogFiles = new ArrayList<>();

    try {
      // Use a simple recursive search for directories containing "status=active"
      // and collect all fred_data_series_catalog.json files within them
      findCatalogFilesRecursive(storageProvider, basePath, catalogFiles);
    } catch (Exception e) {
      LOGGER.error("Error finding catalog files in active partitions", e);
    }

    return catalogFiles;
  }

  /**
   * Recursive helper to find catalog JSON files under status=active directories.
   */
  private void findCatalogFilesRecursive(StorageProvider storageProvider, String currentPath,
      List<String> catalogFiles) throws IOException {

    List<StorageProvider.FileEntry> entries = storageProvider.listFiles(currentPath, false);

    for (StorageProvider.FileEntry entry : entries) {
      String fullPath = entry.getPath();

      if (entry.isDirectory()) {
        // Extract directory name from path, handling trailing slash
        String pathForParsing = fullPath.endsWith("/") ?
            fullPath.substring(0, fullPath.length() - 1) : fullPath;
        String entryName = pathForParsing.substring(pathForParsing.lastIndexOf('/') + 1);

        // If this is a status=active directory, look for catalog JSON file
        if (entryName.equals("status=active")) {
          String catalogFile = storageProvider.resolvePath(fullPath, "fred_data_series_catalog.json");
          if (storageProvider.exists(catalogFile)) {
            catalogFiles.add(catalogFile);
            LOGGER.debug("Found catalog file: {}", catalogFile);
          }
        } else {
          // Recurse into subdirectories
          findCatalogFilesRecursive(storageProvider, fullPath, catalogFiles);
        }
      }
    }
  }

  @Override public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    // EconSchemaFactory doesn't use tableDefinitions
    LOGGER.debug("Received constraint metadata for {} tables",
        tableConstraints != null ? tableConstraints.size() : 0);
  }
}
