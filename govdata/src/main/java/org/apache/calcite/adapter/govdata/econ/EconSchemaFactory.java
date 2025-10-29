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
  private Map<String, Object> fredSeriesGroups;
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
    // Don't add /econ suffix - downloaders already include source=econ in their paths
    String econRawDir = cacheDirectory;
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

    // Extract API keys from environment variables or system properties
    String blsApiKey = System.getenv("BLS_API_KEY");
    if (blsApiKey == null) {
      blsApiKey = System.getProperty("BLS_API_KEY");
    }

    String fredApiKey = System.getenv("FRED_API_KEY");
    if (fredApiKey == null) {
      fredApiKey = System.getProperty("FRED_API_KEY");
    }

    String beaApiKey = System.getenv("BEA_API_KEY");
    if (beaApiKey == null) {
      beaApiKey = System.getProperty("BEA_API_KEY");
    }

    // Check for operand overrides (model can override environment)
    if (operand.get("blsApiKey") != null) {
      blsApiKey = (String) operand.get("blsApiKey");
    }
    if (operand.get("fredApiKey") != null) {
      fredApiKey = (String) operand.get("fredApiKey");
    }
    if (operand.get("beaApiKey") != null) {
      beaApiKey = (String) operand.get("beaApiKey");
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

    @SuppressWarnings("unchecked")
    Map<String, Object> fredSeriesGroups = (Map<String, Object>) operand.get("fredSeriesGroups");

    // Store configuration for table definition generation
    this.customFredSeries = customFredSeries;
    this.fredSeriesGroups = fredSeriesGroups;

    // Parse BLS table filtering configuration (needed for both download and schema filtering)
    this.enabledBlsTables = parseBlsTableFilter(operand);

    String defaultPartitionStrategy = (String) operand.get("defaultPartitionStrategy");
    if (defaultPartitionStrategy == null) {
      defaultPartitionStrategy = "AUTO";
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("  Custom FRED series: {}", customFredSeries != null ? customFredSeries.size() + " series configured" : "none");
      LOGGER.debug("  FRED series groups: {}", fredSeriesGroups != null ? fredSeriesGroups.size() + " groups configured" : "none");
    }
    LOGGER.debug("  Default partition strategy: {}", defaultPartitionStrategy);

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
            customFredSeries, fredSeriesGroups, defaultPartitionStrategy, cacheStorageProvider);
      } catch (Exception e) {
        LOGGER.error("Error downloading ECON data", e);
        // Continue even if download fails - existing data may be available
      }
    }

    // Set the directory for FileSchemaFactory to use
    mutableOperand.put("directory", econParquetDir);

    // Separate table definitions into partitioned tables and views
    List<Map<String, Object>> allTables = loadTableDefinitions();
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
      List<String> customFredSeries, Map<String, Object> fredSeriesGroups, String defaultPartitionStrategy,
      StorageProvider cacheStorageProvider) throws IOException {

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

    // cacheStorageProvider is passed as parameter (created by GovDataSchemaFactory)
    LOGGER.debug("Using shared cache storage provider for cache directory: {}", cacheDir);

    // Download BLS data if enabled
    if (enabledSources.contains("bls") && blsApiKey != null && !blsApiKey.isEmpty()) {
      try {
        BlsDataDownloader blsDownloader = new BlsDataDownloader(blsApiKey, cacheDir, econOperatingDirectory, parquetDir, cacheStorageProvider, storageProvider, cacheManifest);

        // Download all BLS data for the year range (using enabledBlsTables parsed in buildOperand)
        blsDownloader.downloadAll(startYear, endYear, enabledBlsTables);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          String cacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);

          // Convert employment statistics
          String employmentParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/employment_statistics.parquet");
          String employmentRawPath = cacheStorageProvider.resolvePath(cacheYearPath, "employment_statistics.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "employment_statistics", year, employmentRawPath, employmentParquetPath)) {
            blsDownloader.convertToParquet(cacheYearPath, employmentParquetPath);
            cacheManifest.markParquetConverted("employment_statistics", year, null, employmentParquetPath);
          }

          // Convert inflation metrics
          String inflationParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/inflation_metrics.parquet");
          String inflationRawPath = cacheStorageProvider.resolvePath(cacheYearPath, "inflation_metrics.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "inflation_metrics", year, inflationRawPath, inflationParquetPath)) {
            blsDownloader.convertToParquet(cacheYearPath, inflationParquetPath);
            cacheManifest.markParquetConverted("inflation_metrics", year, null, inflationParquetPath);
          }

          // Convert regional CPI
          String regionalCpiCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=cpi_regional/year=" + year);
          String regionalCpiParquetPath = storageProvider.resolvePath(parquetDir, "type=cpi_regional/year=" + year + "/regional_cpi.parquet");
          String regionalCpiRawPath = cacheStorageProvider.resolvePath(regionalCpiCacheYearPath, "regional_cpi.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "regional_cpi", year, regionalCpiRawPath, regionalCpiParquetPath)) {
            blsDownloader.convertToParquet(regionalCpiCacheYearPath, regionalCpiParquetPath);
            cacheManifest.markParquetConverted("regional_cpi", year, null, regionalCpiParquetPath);
          }

          // Convert metro CPI
          String metroCpiCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=cpi_metro/year=" + year);
          String metroCpiParquetPath = storageProvider.resolvePath(parquetDir, "type=cpi_metro/year=" + year + "/metro_cpi.parquet");
          String metroCpiRawPath = cacheStorageProvider.resolvePath(metroCpiCacheYearPath, "metro_cpi.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "metro_cpi", year, metroCpiRawPath, metroCpiParquetPath)) {
            blsDownloader.convertToParquet(metroCpiCacheYearPath, metroCpiParquetPath);
            cacheManifest.markParquetConverted("metro_cpi", year, null, metroCpiParquetPath);
          }

          // Convert state industry employment
          String stateIndustryCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=state_industry/year=" + year);
          String stateIndustryParquetPath = storageProvider.resolvePath(parquetDir, "type=state_industry/year=" + year + "/state_industry.parquet");
          String stateIndustryRawPath = cacheStorageProvider.resolvePath(stateIndustryCacheYearPath, "state_industry.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "state_industry", year, stateIndustryRawPath, stateIndustryParquetPath)) {
            blsDownloader.convertToParquet(stateIndustryCacheYearPath, stateIndustryParquetPath);
            cacheManifest.markParquetConverted("state_industry", year, null, stateIndustryParquetPath);
          }

          // Convert state wages
          String stateWagesCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=state_wages/year=" + year);
          String stateWagesParquetPath = storageProvider.resolvePath(parquetDir, "type=state_wages/year=" + year + "/state_wages.parquet");
          String stateWagesRawPath = cacheStorageProvider.resolvePath(stateWagesCacheYearPath, "state_wages.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "state_wages", year, stateWagesRawPath, stateWagesParquetPath)) {
            blsDownloader.convertToParquet(stateWagesCacheYearPath, stateWagesParquetPath);
            cacheManifest.markParquetConverted("state_wages", year, null, stateWagesParquetPath);
          }

          // Convert metro industry employment
          String metroIndustryCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=metro_industry/year=" + year);
          String metroIndustryParquetPath = storageProvider.resolvePath(parquetDir, "type=metro_industry/year=" + year + "/metro_industry.parquet");
          String metroIndustryRawPath = cacheStorageProvider.resolvePath(metroIndustryCacheYearPath, "metro_industry.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "metro_industry", year, metroIndustryRawPath, metroIndustryParquetPath)) {
            blsDownloader.convertToParquet(metroIndustryCacheYearPath, metroIndustryParquetPath);
            cacheManifest.markParquetConverted("metro_industry", year, null, metroIndustryParquetPath);
          }

          // Convert metro wages
          String metroWagesCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=metro_wages/year=" + year);
          String metroWagesParquetPath = storageProvider.resolvePath(parquetDir, "type=metro_wages/year=" + year + "/metro_wages.parquet");
          String metroWagesRawPath = cacheStorageProvider.resolvePath(metroWagesCacheYearPath, "metro_wages.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "metro_wages", year, metroWagesRawPath, metroWagesParquetPath)) {
            blsDownloader.convertToParquet(metroWagesCacheYearPath, metroWagesParquetPath);
            cacheManifest.markParquetConverted("metro_wages", year, null, metroWagesParquetPath);
          }

          // Convert JOLTS regional
          String joltsRegionalCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=jolts_regional/year=" + year);
          String joltsRegionalParquetPath = storageProvider.resolvePath(parquetDir, "type=jolts_regional/year=" + year + "/jolts_regional.parquet");
          String joltsRegionalRawPath = cacheStorageProvider.resolvePath(joltsRegionalCacheYearPath, "jolts_regional.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "jolts_regional", year, joltsRegionalRawPath, joltsRegionalParquetPath)) {
            blsDownloader.convertToParquet(joltsRegionalCacheYearPath, joltsRegionalParquetPath);
            cacheManifest.markParquetConverted("jolts_regional", year, null, joltsRegionalParquetPath);
          }

          // Convert county wages
          String countyWagesCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=county_wages/year=" + year);
          String countyWagesParquetPath = storageProvider.resolvePath(parquetDir, "type=county_wages/year=" + year + "/county_wages.parquet");
          String countyWagesRawPath = cacheStorageProvider.resolvePath(countyWagesCacheYearPath, "county_wages.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "county_wages", year, countyWagesRawPath, countyWagesParquetPath)) {
            blsDownloader.convertToParquet(countyWagesCacheYearPath, countyWagesParquetPath);
            cacheManifest.markParquetConverted("county_wages", year, null, countyWagesParquetPath);
          }

          // Convert JOLTS state
          String joltsStateCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=jolts_state/year=" + year);
          String joltsStateParquetPath = storageProvider.resolvePath(parquetDir, "type=jolts_state/year=" + year + "/jolts_state.parquet");
          String joltsStateRawPath = cacheStorageProvider.resolvePath(joltsStateCacheYearPath, "jolts_state.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "jolts_state", year, joltsStateRawPath, joltsStateParquetPath)) {
            blsDownloader.convertToParquet(joltsStateCacheYearPath, joltsStateParquetPath);
            cacheManifest.markParquetConverted("jolts_state", year, null, joltsStateParquetPath);
          }

          // Convert wage growth
          String wageParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/wage_growth.parquet");
          String wageRawPath = cacheStorageProvider.resolvePath(cacheYearPath, "wage_growth.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "wage_growth", year, wageRawPath, wageParquetPath)) {
            blsDownloader.convertToParquet(cacheYearPath, wageParquetPath);
            cacheManifest.markParquetConverted("wage_growth", year, null, wageParquetPath);
          }

          // NOTE: regional_employment conversion removed - downloadRegionalEmployment() now creates
          // Parquet files directly with state-level partitioning (year + state_fips).
          // No intermediate JSON files are created, and no conversion step is needed.
        }

        // Convert reference tables (not partitioned by year, use 0 as sentinel)
        String joltsIndustriesCachePath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=reference");
        String joltsIndustriesParquetPath = storageProvider.resolvePath(parquetDir, "type=reference/jolts_industries.parquet");
        String joltsIndustriesRawPath = cacheStorageProvider.resolvePath(joltsIndustriesCachePath, "jolts_industries.json");
        if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "jolts_industries", 0, joltsIndustriesRawPath, joltsIndustriesParquetPath)) {
          blsDownloader.convertToParquet(joltsIndustriesCachePath, joltsIndustriesParquetPath);
          cacheManifest.markParquetConverted("jolts_industries", 0, null, joltsIndustriesParquetPath);
        }

        String joltsDataelementsCachePath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=reference");
        String joltsDataelementsParquetPath = storageProvider.resolvePath(parquetDir, "type=reference/jolts_dataelements.parquet");
        String joltsDataelementsRawPath = cacheStorageProvider.resolvePath(joltsDataelementsCachePath, "jolts_dataelements.json");
        if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "jolts_dataelements", 0, joltsDataelementsRawPath, joltsDataelementsParquetPath)) {
          blsDownloader.convertToParquet(joltsDataelementsCachePath, joltsDataelementsParquetPath);
          cacheManifest.markParquetConverted("jolts_dataelements", 0, null, joltsDataelementsParquetPath);
        }

        LOGGER.debug("BLS data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading BLS data", e);
      }
    }

    // Download FRED data if enabled
    if (enabledSources.contains("fred") && fredApiKey != null && !fredApiKey.isEmpty()) {
      try {
        FredDataDownloader fredDownloader = new FredDataDownloader(cacheDir, econOperatingDirectory, fredApiKey, cacheStorageProvider, storageProvider, cacheManifest);

        // Download all FRED data for the year range
        fredDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          String fredParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/fred_indicators.parquet");
          String cacheFredYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
          String fredRawPath = cacheStorageProvider.resolvePath(cacheFredYearPath, "fred_indicators.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "fred_indicators", year, fredRawPath, fredParquetPath)) {
            fredDownloader.convertToParquet(cacheFredYearPath, fredParquetPath);
            cacheManifest.markParquetConverted("fred_indicators", year, null, fredParquetPath);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error downloading FRED data", e);
      }
    }

    // Download custom FRED series if configured
    if (enabledSources.contains("fred") && fredApiKey != null && !fredApiKey.isEmpty()
        && (customFredSeries != null || fredSeriesGroups != null)) {
      try {
        downloadCustomFredSeries(cacheDir, econOperatingDirectory, parquetDir, fredApiKey, storageProvider,
            customFredSeries, fredSeriesGroups, defaultPartitionStrategy, startYear, endYear, cacheManifest, cacheStorageProvider);
      } catch (Exception e) {
        LOGGER.error("Error downloading custom FRED series", e);
      }
    }

    // Download Treasury data if enabled (no API key required)
    if (enabledSources.contains("treasury")) {
      try {
        TreasuryDataDownloader treasuryDownloader = new TreasuryDataDownloader(cacheDir, econOperatingDirectory, cacheStorageProvider, storageProvider, cacheManifest);

        // Download all Treasury data for the year range
        treasuryDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          String cacheTimeseriesYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=timeseries/year=" + year);

          String yieldsParquetPath = storageProvider.resolvePath(parquetDir, "type=timeseries/frequency=daily/year=" + year + "/treasury_yields.parquet");
          String yieldsRawPath = cacheStorageProvider.resolvePath(cacheTimeseriesYearPath, "treasury_yields.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "treasury_yields", year, yieldsRawPath, yieldsParquetPath)) {
            treasuryDownloader.convertToParquet(cacheTimeseriesYearPath, yieldsParquetPath);
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
          String cacheIndicatorsYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);

          // Convert GDP components
          String gdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/gdp_components.parquet");
          String gdpRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "gdp_components.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "gdp_components", year, gdpRawPath, gdpParquetPath)) {
            beaDownloader.convertToParquet(cacheIndicatorsYearPath, gdpParquetPath);
            cacheManifest.markParquetConverted("gdp_components", year, null, gdpParquetPath);
          }

          // Create gdp_statistics table (GDP growth rates from BEA)
          String gdpStatsParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/gdp_statistics.parquet");
          String gdpStatsRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "gdp_statistics.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "gdp_statistics", year, gdpStatsRawPath, gdpStatsParquetPath)) {
            beaDownloader.convertGdpStatisticsToParquet(cacheIndicatorsYearPath, gdpStatsParquetPath);
            cacheManifest.markParquetConverted("gdp_statistics", year, null, gdpStatsParquetPath);
          }

          // Convert regional income
          String regionalIncomeParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/regional_income.parquet");
          String regionalIncomeRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "regional_income.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "regional_income", year, regionalIncomeRawPath, regionalIncomeParquetPath)) {
            beaDownloader.convertRegionalIncomeToParquet(cacheIndicatorsYearPath, regionalIncomeParquetPath);
            cacheManifest.markParquetConverted("regional_income", year, null, regionalIncomeParquetPath);
          }

          // Convert State GDP data
          String stateGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/state_gdp.parquet");
          String stateGdpRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "state_gdp.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "state_gdp", year, stateGdpRawPath, stateGdpParquetPath)) {
            beaDownloader.convertStateGdpToParquet(cacheIndicatorsYearPath, stateGdpParquetPath);
            cacheManifest.markParquetConverted("state_gdp", year, null, stateGdpParquetPath);
          }

          // Convert BEA trade statistics
          String tradeParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/trade_statistics.parquet");
          String tradeRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "trade_statistics.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "trade_statistics", year, tradeRawPath, tradeParquetPath)) {
            beaDownloader.convertTradeStatisticsToParquet(cacheIndicatorsYearPath, tradeParquetPath);
            cacheManifest.markParquetConverted("trade_statistics", year, null, tradeParquetPath);
          }

          // Convert ITA data
          String itaParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/ita_data.parquet");
          String itaRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "ita_data.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "ita_data", year, itaRawPath, itaParquetPath)) {
            beaDownloader.convertItaDataToParquet(cacheIndicatorsYearPath, itaParquetPath);
            cacheManifest.markParquetConverted("ita_data", year, null, itaParquetPath);
          }

          // Convert industry GDP data
          String industryGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/industry_gdp.parquet");
          String industryGdpRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "industry_gdp.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "industry_gdp", year, industryGdpRawPath, industryGdpParquetPath)) {
            beaDownloader.convertIndustryGdpToParquet(cacheIndicatorsYearPath, industryGdpParquetPath);
            cacheManifest.markParquetConverted("industry_gdp", year, null, industryGdpParquetPath);
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
          String joltsRegionalCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=jolts_regional/year=" + year);
          String joltsRegionalParquetPath = storageProvider.resolvePath(parquetDir, "type=jolts_regional/year=" + year + "/jolts_regional.parquet");
          String joltsRegionalRawPath = cacheStorageProvider.resolvePath(joltsRegionalCacheYearPath, "jolts_regional.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "jolts_regional", year, joltsRegionalRawPath, joltsRegionalParquetPath)) {
            blsDownloader.convertToParquet(joltsRegionalCacheYearPath, joltsRegionalParquetPath);
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
          String metroCpiCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=cpi_metro/year=" + year);
          String metroCpiParquetPath = storageProvider.resolvePath(parquetDir, "type=cpi_metro/year=" + year + "/metro_cpi.parquet");
          String metroCpiRawPath = cacheStorageProvider.resolvePath(metroCpiCacheYearPath, "metro_cpi.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "metro_cpi", year, metroCpiRawPath, metroCpiParquetPath)) {
            blsDownloader.convertToParquet(metroCpiCacheYearPath, metroCpiParquetPath);
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

        // Convert regional income to parquet for each year
        for (int year = startYear; year <= endYear; year++) {
          String cacheIndicatorsYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
          String regionalIncomeParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/regional_income.parquet");
          String regionalIncomeRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "regional_income.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "regional_income", year, regionalIncomeRawPath, regionalIncomeParquetPath)) {
            beaDownloader.convertRegionalIncomeToParquet(cacheIndicatorsYearPath, regionalIncomeParquetPath);
            cacheManifest.markParquetConverted("regional_income", year, null, regionalIncomeParquetPath);
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
          String cacheIndicatorsYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
          String stateGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/state_gdp.parquet");
          String stateGdpRawPath = cacheStorageProvider.resolvePath(cacheIndicatorsYearPath, "state_gdp.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "state_gdp", year, stateGdpRawPath, stateGdpParquetPath)) {
            beaDownloader.convertStateGdpToParquet(cacheIndicatorsYearPath, stateGdpParquetPath);
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
        WorldBankDataDownloader worldBankDownloader = new WorldBankDataDownloader(cacheDir, econOperatingDirectory, cacheStorageProvider, storageProvider, cacheManifest);

        // Download all World Bank data for the year range
        worldBankDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          String worldParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/world_indicators.parquet");
          String cacheWorldBankYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
          String worldRawPath = cacheStorageProvider.resolvePath(cacheWorldBankYearPath, "world_indicators.json");
          if (!isParquetConvertedOrExists(cacheManifest, storageProvider, cacheStorageProvider, "world_indicators", year, worldRawPath, worldParquetPath)) {
            worldBankDownloader.convertToParquet(cacheWorldBankYearPath, worldParquetPath);
            cacheManifest.markParquetConverted("world_indicators", year, null, worldParquetPath);
          }
        }

        LOGGER.debug("World Bank data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading World Bank data", e);
      }
    }

    // Download FRED series catalog if FRED API key is available
    LOGGER.debug("FRED catalog download check: fredApiKey={}, isEmpty={}",
        fredApiKey != null ? "configured" : "null",
        fredApiKey != null ? fredApiKey.isEmpty() : "N/A");
    if (fredApiKey != null && !fredApiKey.isEmpty()) {
      try {
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

    // Save cache manifest after all downloads to operating directory
    if (cacheManifest != null) {
      cacheManifest.save(econOperatingDirectory);
    }
  }

  /**
   * Download custom FRED series with smart partitioning support.
   */
  private void downloadCustomFredSeries(String cacheDir, String operatingDirectory, String parquetDir, String fredApiKey,
      StorageProvider storageProvider, List<String> customFredSeries, Map<String, Object> fredSeriesGroups,
      String defaultPartitionStrategy, int startYear, int endYear, CacheManifest sharedManifest,
      StorageProvider cacheStorageProvider) throws IOException {

    // Initialize partition analyzer
    FredSeriesPartitionAnalyzer analyzer = new FredSeriesPartitionAnalyzer();

    // Collect all custom series for volume analysis
    List<String> allCustomSeries = new ArrayList<>();
    if (customFredSeries != null) {
      allCustomSeries.addAll(customFredSeries);
    }

    // Parse series groups from configuration
    List<FredSeriesGroup> parsedGroups = new ArrayList<>();
    if (fredSeriesGroups != null) {
      for (Map.Entry<String, Object> entry : fredSeriesGroups.entrySet()) {
        String groupName = entry.getKey();
        @SuppressWarnings("unchecked")
        Map<String, Object> groupConfig = (Map<String, Object>) entry.getValue();

        @SuppressWarnings("unchecked")
        List<String> seriesList = (List<String>) groupConfig.get("series");

        String strategyStr = (String) groupConfig.get("partitionStrategy");
        if (strategyStr == null) {
          strategyStr = defaultPartitionStrategy;
        }

        FredSeriesGroup.PartitionStrategy strategy;
        try {
          strategy = FredSeriesGroup.PartitionStrategy.valueOf(strategyStr.toUpperCase());
        } catch (IllegalArgumentException e) {
          LOGGER.warn("Invalid partition strategy '{}' for group '{}'. Valid strategies are: NONE, AUTO, MANUAL. " +
              "To partition by specific fields like year and maturity, use partitionStrategy='MANUAL' with " +
              "partitionFields=['year','maturity']. Using default: {}",
              strategyStr, groupName, defaultPartitionStrategy);
          strategy = FredSeriesGroup.PartitionStrategy.valueOf(defaultPartitionStrategy.toUpperCase());
        }

        @SuppressWarnings("unchecked")
        List<String> partitionFields = (List<String>) groupConfig.get("partitionFields");

        FredSeriesGroup group = new FredSeriesGroup(groupName, seriesList, strategy, partitionFields);
        parsedGroups.add(group);

        // Add to all series for analysis
        if (seriesList != null) {
          allCustomSeries.addAll(seriesList);
        }

        LOGGER.debug("Parsed FRED series group: {}", group);
      }
    }

    // cacheStorageProvider is now passed as parameter (created by GovDataSchemaFactory)

    // Download data for each series group - use shared manifest passed from caller
    FredDataDownloader fredDownloader = new FredDataDownloader(cacheDir, operatingDirectory, fredApiKey, cacheStorageProvider, storageProvider, sharedManifest);

    for (FredSeriesGroup group : parsedGroups) {
      LOGGER.debug("Processing FRED series group: {}", group.getGroupName());

      // Analyze partitioning strategy for this group
      FredSeriesPartitionAnalyzer.PartitionAnalysis analysis = analyzer.analyzeGroup(group, allCustomSeries);
      LOGGER.debug("Partition analysis for group '{}': {}", group.getGroupName(), analysis);

      // Download series data with partitioning
      String groupTableName = group.getTableName();
      String groupParquetDir = parquetDir;

      // Directory creation handled automatically by StorageProvider when writing files

      // Download each series in the group
      for (String seriesId : group.getSeries()) {
        try {
          // Download raw series data
          fredDownloader.downloadSeries(seriesId, startYear, endYear);

          // Convert to partitioned parquet using analysis results
          if (analysis.getStrategy() == FredSeriesGroup.PartitionStrategy.NONE) {
            // No partitioning - single file
            String parquetPath = storageProvider.resolvePath(groupParquetDir, groupTableName + ".parquet");
            fredDownloader.convertSeriesToParquet(seriesId, parquetPath, null, startYear, endYear);
          } else {
            // Apply partitioning strategy
            List<String> partitionFields = analysis.getPartitionFields();
            for (int year = startYear; year <= endYear; year++) {
              String partitionPath = buildPartitionPath(groupParquetDir, groupTableName, partitionFields, year, seriesId);
              fredDownloader.convertSeriesToParquet(seriesId, partitionPath, partitionFields, year, year);
            }
          }

        } catch (Exception e) {
          LOGGER.error("Failed to download FRED series: {} in group: {}", seriesId, group.getGroupName(), e);
        }
      }
    }

    // Handle ungrouped custom series
    if (customFredSeries != null && !customFredSeries.isEmpty()) {
      LOGGER.debug("Processing {} ungrouped custom FRED series", customFredSeries.size());

      // Create default group for ungrouped series
      FredSeriesGroup defaultGroup =
          new FredSeriesGroup("custom_series", customFredSeries, FredSeriesGroup.PartitionStrategy.valueOf(defaultPartitionStrategy.toUpperCase()), null);

      FredSeriesPartitionAnalyzer.PartitionAnalysis analysis = analyzer.analyzeGroup(defaultGroup, allCustomSeries);
      LOGGER.debug("Partition analysis for ungrouped series: {}", analysis);

      // Download ungrouped series
      for (String seriesId : customFredSeries) {
        // Skip if already processed as part of a group
        boolean inGroup = false;
        for (FredSeriesGroup group : parsedGroups) {
          if (group.matchesSeries(seriesId)) {
            inGroup = true;
            break;
          }
        }

        if (!inGroup) {
          try {
            // Download raw series data
            fredDownloader.downloadSeries(seriesId, startYear, endYear);

            // Determine table name based on series ID
            String tableName = getTableNameForSeries(seriesId);

            // Apply partitioning strategy
            if (analysis.getStrategy() == FredSeriesGroup.PartitionStrategy.NONE) {
              // No partitioning
              String parquetPath = storageProvider.resolvePath(parquetDir, "type=custom/" + tableName + ".parquet");
              fredDownloader.convertSeriesToParquet(seriesId, parquetPath, null, startYear, endYear);
              LOGGER.debug("Created non-partitioned parquet for series {} at: {}", seriesId, parquetPath);
            } else {
              // Apply partitioning
              List<String> partitionFields = analysis.getPartitionFields();
              for (int year = startYear; year <= endYear; year++) {
                String partitionPath = buildPartitionPath(parquetDir, tableName, partitionFields, year, seriesId);
                fredDownloader.convertSeriesToParquet(seriesId, partitionPath, partitionFields, year, year);
              }
              LOGGER.debug("Created partitioned parquet for series {} as table: {}", seriesId, tableName);
            }

          } catch (Exception e) {
            LOGGER.error("Failed to download ungrouped FRED series: {}", seriesId, e);
          }
        }
      }
    }
  }

  /**
   * Get table name for a FRED series ID based on its type/category.
   */
  private String getTableNameForSeries(String seriesId) {
    // Map series to appropriate table names based on test expectations
    if (seriesId.startsWith("DGS") || seriesId.contains("TREASURY") || seriesId.contains("BOND")) {
      return "fred_treasuries";
    } else if (seriesId.equals("UNRATE") || seriesId.equals("PAYEMS") || seriesId.equals("CIVPART") ||
               seriesId.contains("EMPLOY") || seriesId.contains("UNEMPLOYMENT")) {
      return "fred_employment_indicators";
    } else {
      // Default naming pattern
      return "fred_" + seriesId.toLowerCase();
    }
  }

  /**
   * Build Hive-style partition path based on partition fields.
   */
  private String buildPartitionPath(String baseDir, String tableName, List<String> partitionFields,
      int year, String seriesId) {
    StringBuilder pathBuilder = new StringBuilder(baseDir);
    pathBuilder.append("/type=custom");

    if (partitionFields != null) {
      for (String field : partitionFields) {
        switch (field.toLowerCase()) {
          case "year":
            pathBuilder.append("/year=").append(year);
            break;
          case "series_id":
            pathBuilder.append("/series_id=").append(seriesId);
            break;
          case "maturity":
            // Extract maturity from treasury series ID (e.g., DGS10 = 10 year, DGS30 = 30 year)
            String maturity = extractMaturityFromSeriesId(seriesId);
            if (maturity != null) {
              pathBuilder.append("/maturity=").append(maturity);
            }
            break;
          case "frequency":
            // Determine frequency from series characteristics
            String frequency = determineSeriesFrequency(seriesId);
            pathBuilder.append("/frequency=").append(frequency);
            break;
          default:
            LOGGER.warn("Unknown partition field: {}", field);
        }
      }
    }

    pathBuilder.append("/").append(tableName).append(".parquet");
    return pathBuilder.toString();
  }

  /**
   * Determine series frequency from FRED series characteristics.
   */
  private String determineSeriesFrequency(String seriesId) {
    // This would typically require API call to get series metadata
    // For now, use pattern-based heuristics
    if (seriesId.startsWith("DGS") || seriesId.matches(".*RATE.*")) {
      return "D"; // Daily
    } else if (seriesId.matches("GDP.*") || seriesId.matches(".*QUAR.*")) {
      return "Q"; // Quarterly
    } else {
      return "M"; // Monthly (default)
    }
  }

  /**
   * Extract maturity information from Treasury series IDs.
   */
  private String extractMaturityFromSeriesId(String seriesId) {
    if (seriesId.startsWith("DGS")) {
      // Extract number after DGS (e.g., DGS10 -> 10Y)
      String maturity = seriesId.substring(3);
      if (maturity.matches("\\d+")) {
        return maturity + "Y";
      }
    }
    return null;
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

    // Add custom FRED table definitions if operand is available
    List<Map<String, Object>> customTables = generateCustomFredTableDefinitions();
    if (!customTables.isEmpty()) {
      tables.addAll(customTables);
      LOGGER.info("[DEBUG] Added {} custom FRED table definitions", customTables.size());
      for (Map<String, Object> table : customTables) {
        LOGGER.info("[DEBUG] Custom FRED table added: {} with pattern: {}", table.get("name"), table.get("pattern"));
      }
    } else {
      LOGGER.warn("[DEBUG] No custom FRED table definitions generated!");
    }

    LOGGER.info("[DEBUG] Total ECON table definitions: {} (base: {}, custom: {})",
        tables.size(), tables.size() - customTables.size(), customTables.size());
    return tables;
  }

  /**
   * Generate table definitions for custom FRED series based on configuration.
   * This is called during schema setup to discover custom tables dynamically.
   */
  private List<Map<String, Object>> generateCustomFredTableDefinitions() {
    List<Map<String, Object>> customTables = new ArrayList<>();

    LOGGER.info("[DEBUG] generateCustomFredTableDefinitions called - fredSeriesGroups: {}, customFredSeries: {}",
        fredSeriesGroups != null ? fredSeriesGroups.size() : "null",
        customFredSeries != null ? customFredSeries.size() : "null");

    // Log the actual groups if present
    if (fredSeriesGroups != null) {
      LOGGER.info("[DEBUG] FRED series groups present: {}", fredSeriesGroups.keySet());
    }

    if (fredSeriesGroups != null) {
      LOGGER.info("[DEBUG] Generating table definitions from {} FRED series groups", fredSeriesGroups.size());

      for (Map.Entry<String, Object> entry : fredSeriesGroups.entrySet()) {
        String groupName = entry.getKey();
        @SuppressWarnings("unchecked")
        Map<String, Object> groupConfig = (Map<String, Object>) entry.getValue();

        // Get table name from group configuration
        String tableName = (String) groupConfig.get("tableName");
        if (tableName == null) {
          // Generate table name from group name
          tableName = "fred_" + groupName.toLowerCase().replaceAll("[^a-z0-9_]", "_");
        }

        Map<String, Object> tableDefinition = new HashMap<>();
        tableDefinition.put("name", tableName);

        // Build pattern based on partition fields from the group configuration
        @SuppressWarnings("unchecked")
        List<String> partitionFields = (List<String>) groupConfig.get("partitionFields");
        String pattern;

        if (partitionFields != null && !partitionFields.isEmpty()) {
          // Build pattern from partition fields
          StringBuilder patternBuilder = new StringBuilder("type=custom");
          for (String field : partitionFields) {
            patternBuilder.append("/").append(field).append("=*");
          }
          patternBuilder.append("/").append(tableName).append(".parquet");
          pattern = patternBuilder.toString();
        } else {
          // Default partitioning by year only
          pattern = "type=custom/year=*/" + tableName + ".parquet";
        }
        tableDefinition.put("pattern", pattern);

        String comment = (String) groupConfig.get("comment");
        if (comment == null) {
          comment = "Custom FRED series group '" + groupName + "' with partitioned time-series data.";
        }
        tableDefinition.put("comment", comment);

        customTables.add(tableDefinition);
        LOGGER.info("[DEBUG] Generated table definition for FRED group '{}' -> table '{}' with pattern '{}'",
            groupName, tableName, pattern);
      }
    }

    // Handle ungrouped custom series
    if (customFredSeries != null && !customFredSeries.isEmpty()) {
      LOGGER.debug("Generating table definitions from {} ungrouped custom FRED series", customFredSeries.size());

      // Group series by expected table name
      Map<String, List<String>> seriesByTable = new HashMap<>();
      for (String seriesId : customFredSeries) {
        String tableName = getTableNameForSeries(seriesId);
        seriesByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(seriesId);
      }

      for (Map.Entry<String, List<String>> tableEntry : seriesByTable.entrySet()) {
        String tableName = tableEntry.getKey();
        List<String> seriesIds = tableEntry.getValue();

        Map<String, Object> tableDefinition = new HashMap<>();
        tableDefinition.put("name", tableName);
        tableDefinition.put("pattern", "type=custom/year=*/" + tableName + ".parquet");
        tableDefinition.put("comment", "Custom FRED series table containing " + seriesIds.size() +
            " series: " + String.join(", ", seriesIds) + ". Partitioned by year for time-series analysis.");

        customTables.add(tableDefinition);
        LOGGER.debug("Generated table definition for ungrouped series -> table '{}' with {} series",
            tableName, seriesIds.size());
      }
    }

    LOGGER.debug("Generated {} custom FRED table definitions", customTables.size());
    return customTables;
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

    // 2. Defensive check: if parquet file exists but not in manifest, verify it's up-to-date
    try {
      if (storageProvider.exists(parquetPath)) {
        // Get timestamps for both files
        long parquetModTime = storageProvider.getMetadata(parquetPath).getLastModified();

        // Check if raw file exists and compare timestamps
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

  @Override public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    // EconSchemaFactory doesn't use tableDefinitions
    LOGGER.debug("Received constraint metadata for {} tables",
        tableConstraints != null ? tableConstraints.size() : 0);
  }
}
