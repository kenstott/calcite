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

import java.io.File;
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
  public Map<String, Object> buildOperand(Map<String, Object> operand, StorageProvider storageProvider) {
    LOGGER.debug("Building ECON schema operand configuration");

    // Read environment variables at runtime (not static initialization)
    String govdataCacheDir = getGovDataCacheDir();
    String govdataParquetDir = getGovDataParquetDir();

    // Check required environment variables
    if (govdataCacheDir == null || govdataCacheDir.isEmpty()) {
      throw new IllegalStateException("GOVDATA_CACHE_DIR environment variable must be set");
    }
    if (govdataParquetDir == null || govdataParquetDir.isEmpty()) {
      throw new IllegalStateException("GOVDATA_PARQUET_DIR environment variable must be set");
    }

    // ECON data directories
    String econRawDir = govdataCacheDir + "/econ";
    String econParquetDir = govdataParquetDir + "/source=econ";

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
            customFredSeries, fredSeriesGroups, defaultPartitionStrategy);
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

    return mutableOperand;
  }

  @Override
  public String getSchemaResourceName() {
    return "/econ-schema.json";
  }


  /**
   * Download economic data from various sources following GEO pattern.
   */
  private void downloadEconData(Map<String, Object> operand, String cacheDir, String parquetDir,
      String blsApiKey, String fredApiKey, String beaApiKey, List<String> enabledSources,
      int startYear, int endYear, StorageProvider storageProvider,
      List<String> customFredSeries, Map<String, Object> fredSeriesGroups, String defaultPartitionStrategy) throws IOException {

    LOGGER.debug("Downloading economic data to: {}", cacheDir);

    // Create cache and parquet directories
    File cacheDirFile = new File(cacheDir);
    File parquetDirFile = new File(parquetDir);
    if (!cacheDirFile.exists()) {
      cacheDirFile.mkdirs();
    }
    if (!parquetDirFile.exists()) {
      parquetDirFile.mkdirs();
    }

    // Create hive-partitioned subdirectories
    File timeSeriesDir = new File(parquetDir, "type=timeseries");
    File indicatorsDir = new File(parquetDir, "type=indicators");
    File regionalDir = new File(parquetDir, "type=regional");
    timeSeriesDir.mkdirs();
    indicatorsDir.mkdirs();
    regionalDir.mkdirs();


    // Download BLS data if enabled
    if (enabledSources.contains("bls") && blsApiKey != null && !blsApiKey.isEmpty()) {
      try {
        LOGGER.info("Downloading BLS data for years {}-{}", startYear, endYear);
        BlsDataDownloader blsDownloader = new BlsDataDownloader(blsApiKey, cacheDir, storageProvider);

        // Download all BLS data for the year range
        blsDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          // Convert employment statistics
          String employmentParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/employment_statistics.parquet");
          String cacheYearPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
          blsDownloader.convertToParquet(new File(cacheYearPath), employmentParquetPath);

          // Convert inflation metrics
          String inflationParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/inflation_metrics.parquet");
          blsDownloader.convertToParquet(new File(cacheYearPath), inflationParquetPath);

          // Convert wage growth
          String wageParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/wage_growth.parquet");
          blsDownloader.convertToParquet(new File(cacheYearPath), wageParquetPath);

          // Convert regional employment
          String regionalParquetPath = storageProvider.resolvePath(parquetDir, "type=regional/year=" + year + "/regional_employment.parquet");
          blsDownloader.convertToParquet(new File(cacheYearPath), regionalParquetPath);
        }

        LOGGER.debug("BLS data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading BLS data", e);
      }
    }

    // Download FRED data if enabled
    if (enabledSources.contains("fred") && fredApiKey != null && !fredApiKey.isEmpty()) {
      try {
        LOGGER.info("Downloading FRED data for years {}-{}", startYear, endYear);
        FredDataDownloader fredDownloader = new FredDataDownloader(cacheDir, fredApiKey, storageProvider);

        // Download all FRED data for the year range
        fredDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          String fredParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/fred_indicators.parquet");
          String cacheFredYearPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
          fredDownloader.convertToParquet(new File(cacheFredYearPath), fredParquetPath);
        }

        LOGGER.debug("FRED data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading FRED data", e);
      }
    }

    // Download custom FRED series if configured
    if (enabledSources.contains("fred") && fredApiKey != null && !fredApiKey.isEmpty()
        && (customFredSeries != null || fredSeriesGroups != null)) {
      try {
        LOGGER.debug("Processing custom FRED series configuration");
        downloadCustomFredSeries(cacheDir, parquetDir, fredApiKey, storageProvider,
            customFredSeries, fredSeriesGroups, defaultPartitionStrategy, startYear, endYear);
      } catch (Exception e) {
        LOGGER.error("Error downloading custom FRED series", e);
      }
    }

    // Download Treasury data if enabled (no API key required)
    if (enabledSources.contains("treasury")) {
      try {
        LOGGER.info("Downloading Treasury data for years {}-{}", startYear, endYear);
        TreasuryDataDownloader treasuryDownloader = new TreasuryDataDownloader(cacheDir, storageProvider);

        // Download all Treasury data for the year range
        treasuryDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          String yieldsParquetPath = storageProvider.resolvePath(parquetDir, "type=timeseries/year=" + year + "/treasury_yields.parquet");
          String cacheTimeseriesYearPath = storageProvider.resolvePath(cacheDir, "source=econ/type=timeseries/year=" + year);
          treasuryDownloader.convertToParquet(new File(cacheTimeseriesYearPath), yieldsParquetPath);

          // Convert federal debt data to parquet
          String debtParquetPath = storageProvider.resolvePath(parquetDir, "type=timeseries/year=" + year + "/federal_debt.parquet");
          treasuryDownloader.convertFederalDebtToParquet(new File(cacheTimeseriesYearPath), debtParquetPath);
        }

        LOGGER.debug("Treasury data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading Treasury data", e);
      }
    }

    // Download BEA data if enabled
    if (enabledSources.contains("bea") && beaApiKey != null && !beaApiKey.isEmpty()) {
      try {
        LOGGER.info("Downloading BEA data for years {}-{}", startYear, endYear);
        BeaDataDownloader beaDownloader = new BeaDataDownloader(cacheDir, beaApiKey, storageProvider);

        // Download all BEA data for the year range
        beaDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year using StorageProvider
        for (int year = startYear; year <= endYear; year++) {
          String cacheIndicatorsYearPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);

          // Convert GDP components using StorageProvider
          String gdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/gdp_components.parquet");
          beaDownloader.convertToParquet(new File(cacheIndicatorsYearPath), gdpParquetPath);

          // Create gdp_statistics table (GDP growth rates from BEA) using StorageProvider
          String gdpStatsParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/gdp_statistics.parquet");
          beaDownloader.convertGdpStatisticsToParquet(new File(cacheIndicatorsYearPath), gdpStatsParquetPath);

          // Convert regional income using StorageProvider
          String regionalIncomeParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/regional_income.parquet");
          beaDownloader.convertRegionalIncomeToParquet(new File(cacheIndicatorsYearPath), regionalIncomeParquetPath);

          // Convert State GDP data using StorageProvider
          String stateGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/state_gdp.parquet");
          beaDownloader.convertStateGdpToParquet(new File(cacheIndicatorsYearPath), stateGdpParquetPath);

          // Convert BEA trade statistics, ITA data, and industry GDP data using StorageProvider
          String tradeParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/trade_statistics.parquet");
          beaDownloader.convertTradeStatisticsToParquet(new File(cacheIndicatorsYearPath), tradeParquetPath);

          String itaParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/ita_data.parquet");
          beaDownloader.convertItaDataToParquet(new File(cacheIndicatorsYearPath), itaParquetPath);

          String industryGdpParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/industry_gdp.parquet");
          beaDownloader.convertIndustryGdpToParquet(new File(cacheIndicatorsYearPath), industryGdpParquetPath);
        }

        LOGGER.debug("BEA data download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading BEA data", e);
      }
    }

    // Download World Bank data if enabled (no API key required)
    if (enabledSources.contains("worldbank")) {
      try {
        LOGGER.info("Downloading World Bank data for years {}-{}", startYear, endYear);
        WorldBankDataDownloader worldBankDownloader = new WorldBankDataDownloader(cacheDir, storageProvider);

        // Download all World Bank data for the year range
        worldBankDownloader.downloadAll(startYear, endYear);

        // Convert to parquet files for each year
        for (int year = startYear; year <= endYear; year++) {
          String worldParquetPath = storageProvider.resolvePath(parquetDir, "type=indicators/year=" + year + "/world_indicators.parquet");
          String cacheWorldBankYearPath = storageProvider.resolvePath(cacheDir, "source=econ/type=indicators/year=" + year);
          worldBankDownloader.convertToParquet(new File(cacheWorldBankYearPath), worldParquetPath);
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
        LOGGER.info("Downloading FRED series catalog");
        FredCatalogDownloader catalogDownloader = new FredCatalogDownloader(fredApiKey, cacheDir, parquetDir, storageProvider);
        catalogDownloader.downloadCatalog();
        LOGGER.debug("FRED catalog download completed");
      } catch (Exception e) {
        LOGGER.error("Error downloading FRED catalog", e);
      }
    } else {
      LOGGER.warn("Skipping FRED catalog download - API key not available");
    }

    LOGGER.info("ECON data download completed");
  }

  /**
   * Download custom FRED series with smart partitioning support.
   */
  private void downloadCustomFredSeries(String cacheDir, String parquetDir, String fredApiKey,
      StorageProvider storageProvider, List<String> customFredSeries, Map<String, Object> fredSeriesGroups,
      String defaultPartitionStrategy, int startYear, int endYear) throws IOException {

    LOGGER.debug("Downloading custom FRED series with smart partitioning");

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
          LOGGER.warn("Invalid partition strategy '{}' for group '{}', using default: {}",
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

    // Download data for each series group
    FredDataDownloader fredDownloader = new FredDataDownloader(cacheDir, fredApiKey, storageProvider);

    for (FredSeriesGroup group : parsedGroups) {
      LOGGER.debug("Processing FRED series group: {}", group.getGroupName());

      // Analyze partitioning strategy for this group
      FredSeriesPartitionAnalyzer.PartitionAnalysis analysis = analyzer.analyzeGroup(group, allCustomSeries);
      LOGGER.debug("Partition analysis for group '{}': {}", group.getGroupName(), analysis);

      // Download series data with partitioning
      String groupTableName = group.getTableName();
      String groupCacheDir = storageProvider.resolvePath(cacheDir, "fred_custom/" + groupTableName);
      String groupParquetDir = parquetDir;

      // Create directories
      new File(groupCacheDir).mkdirs();
      new File(groupParquetDir).mkdirs();

      // Download each series in the group
      for (String seriesId : group.getSeries()) {
        try {
          LOGGER.debug("Downloading FRED series: {} for group: {}", seriesId, group.getGroupName());

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
      FredSeriesGroup defaultGroup = new FredSeriesGroup("custom_series", customFredSeries,
          FredSeriesGroup.PartitionStrategy.valueOf(defaultPartitionStrategy.toUpperCase()), null);

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
            LOGGER.debug("Downloading ungrouped FRED series: {}", seriesId);

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

    LOGGER.debug("Custom FRED series download completed");
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
  @Override
  public List<Map<String, Object>> loadTableDefinitions() {
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
