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
    mutableOperand.put("partitionedTables", loadTableDefinitions());

    // Pass through executionEngine if specified (critical for DuckDB vs PARQUET)
    if (operand.containsKey("executionEngine")) {
      mutableOperand.put("executionEngine", operand.get("executionEngine"));
    }
    Map<String, Map<String, Object>> econConstraints = loadTableConstraints();
    if (tableConstraints != null) {
      econConstraints.putAll(tableConstraints);
    }

    mutableOperand.put("comment", "U.S. and international economic data including employment statistics, inflation metrics (CPI/PPI), "
        + "Federal Reserve interest rates, GDP components, Treasury yields, federal debt, and global economic indicators. "
        + "Data sources include Bureau of Labor Statistics (BLS), U.S. Treasury, World Bank, "
        + "Federal Reserve Economic Data (FRED), and Bureau of Economic Analysis (BEA). "
        + "Enables macroeconomic analysis, market research, and correlation with financial data.");

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
        BlsDataDownloader blsDownloader = new BlsDataDownloader(blsApiKey, cacheDir, econOperatingDirectory, cacheStorageProvider, storageProvider, cacheManifest);

        // Download all BLS data for the year range
        blsDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          // Convert employment statistics
          String employmentParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/employment_statistics.parquet");
          String cacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
          blsDownloader.convertToParquet(cacheYearPath, employmentParquetPath);

          // Convert inflation metrics
          String inflationParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/inflation_metrics.parquet");
          blsDownloader.convertToParquet(cacheYearPath, inflationParquetPath);

          // Convert wage growth
          String wageParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/wage_growth.parquet");
          blsDownloader.convertToParquet(cacheYearPath, wageParquetPath);

          // Convert regional employment
          String regionalParquetPath = storageProvider.resolvePath(parquetDir, "type=regional/year=" + year + "/regional_employment.parquet");
          String regionalCacheYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=regional/year=" + year);
          blsDownloader.convertToParquet(regionalCacheYearPath, regionalParquetPath);
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
          fredDownloader.convertToParquet(cacheFredYearPath, fredParquetPath);
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
          String yieldsParquetPath = storageProvider.resolvePath(parquetDir, "type=timeseries/year=" + year + "/treasury_yields.parquet");
          String cacheTimeseriesYearPath = cacheStorageProvider.resolvePath(cacheDir, "source=econ/type=timeseries/year=" + year);
          treasuryDownloader.convertToParquet(cacheTimeseriesYearPath, yieldsParquetPath);

          // Convert federal debt data to parquet
          String debtParquetPath = storageProvider.resolvePath(parquetDir, "type=timeseries/year=" + year + "/federal_debt.parquet");
          treasuryDownloader.convertFederalDebtToParquet(cacheTimeseriesYearPath, debtParquetPath);
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

          // Convert GDP components using StorageProvider
          String gdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/gdp_components.parquet");
          beaDownloader.convertToParquet(cacheIndicatorsYearPath, gdpParquetPath);

          // Create gdp_statistics table (GDP growth rates from BEA) using StorageProvider
          String gdpStatsParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/gdp_statistics.parquet");
          beaDownloader.convertGdpStatisticsToParquet(cacheIndicatorsYearPath, gdpStatsParquetPath);

          // Convert regional income using StorageProvider
          String regionalIncomeParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/regional_income.parquet");
          beaDownloader.convertRegionalIncomeToParquet(cacheIndicatorsYearPath, regionalIncomeParquetPath);

          // Convert State GDP data using StorageProvider
          String stateGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/state_gdp.parquet");
          beaDownloader.convertStateGdpToParquet(cacheIndicatorsYearPath, stateGdpParquetPath);

          // Convert BEA trade statistics, ITA data, and industry GDP data using StorageProvider
          String tradeParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/trade_statistics.parquet");
          beaDownloader.convertTradeStatisticsToParquet(cacheIndicatorsYearPath, tradeParquetPath);

          String itaParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/ita_data.parquet");
          beaDownloader.convertItaDataToParquet(cacheIndicatorsYearPath, itaParquetPath);

          String industryGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/industry_gdp.parquet");
          beaDownloader.convertIndustryGdpToParquet(cacheIndicatorsYearPath, industryGdpParquetPath);
        }

        LOGGER.debug("BEA data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading BEA data", e);
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
          worldBankDownloader.convertToParquet(cacheWorldBankYearPath, worldParquetPath);
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
            fredDownloader.convertSeriesToParquet(seriesId, parquetPath, null);
          } else {
            // Apply partitioning strategy
            List<String> partitionFields = analysis.getPartitionFields();
            for (int year = startYear; year <= endYear; year++) {
              String partitionPath = buildPartitionPath(groupParquetDir, groupTableName, partitionFields, year, seriesId);
              fredDownloader.convertSeriesToParquet(seriesId, partitionPath, partitionFields);
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
              fredDownloader.convertSeriesToParquet(seriesId, parquetPath, null);
              LOGGER.debug("Created non-partitioned parquet for series {} at: {}", seriesId, parquetPath);
            } else {
              // Apply partitioning
              List<String> partitionFields = analysis.getPartitionFields();
              for (int year = startYear; year <= endYear; year++) {
                String partitionPath = buildPartitionPath(parquetDir, tableName, partitionFields, year, seriesId);
                fredDownloader.convertSeriesToParquet(seriesId, partitionPath, partitionFields);
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
    List<Map<String, Object>> tables = new ArrayList<>(GovDataSubSchemaFactory.super.loadTableDefinitions());

    LOGGER.info("[DEBUG] Loaded {} base table definitions from econ-schema.json", tables.size());

    // Log the base tables
    for (Map<String, Object> table : tables) {
      LOGGER.debug("[DEBUG] Base table: {} with pattern: {}", table.get("name"), table.get("pattern"));
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

  @Override public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    // EconSchemaFactory doesn't use tableDefinitions
    LOGGER.debug("Received constraint metadata for {} tables",
        tableConstraints != null ? tableConstraints.size() : 0);
  }
}
