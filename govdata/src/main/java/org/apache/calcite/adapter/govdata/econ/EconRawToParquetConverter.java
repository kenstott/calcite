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

import org.apache.calcite.adapter.file.converters.RawToParquetConverter;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom raw-to-parquet converter for ECON adapter that understands economic data structures.
 *
 * <p>This converter handles domain-specific transformations for economic data from:
 * <ul>
 *   <li><b>FRED</b> - Federal Reserve Economic Data (unwraps API responses, flattens time series)</li>
 *   <li><b>BLS</b> - Bureau of Labor Statistics (transforms employment/inflation data)</li>
 *   <li><b>Treasury</b> - U.S. Treasury (processes yield curves, federal debt)</li>
 *   <li><b>BEA</b> - Bureau of Economic Analysis (GDP components, regional data)</li>
 *   <li><b>World Bank</b> - International economic indicators</li>
 * </ul>
 *
 * <h3>Path-Based Routing</h3>
 * <p>The converter uses file path patterns to route conversion to the appropriate downloader:
 * <pre>
 * Pattern                         → Handler
 * ----------------------------------------
 * source=econ/type=indicators     → BLS or FRED (based on filename)
 * source=econ/type=timeseries     → Treasury
 * source=econ/type=custom_fred    → FRED (custom series)
 * source=econ/type=regional       → BLS or BEA (regional data)
 * source=econ/type=world_*        → World Bank
 * source=econ/type=gdp_*          → BEA (GDP data)
 * source=econ/type=regional_income → BEA (regional income)
 * source=econ/type=trade_*        → BEA (trade statistics)
 * source=econ/type=ita_*          → BEA (ITA data)
 * source=econ/type=industry_*     → BEA (industry GDP)
 * </pre>
 *
 * <h3>Integration with FileSchema</h3>
 * <p>This converter is registered with FileSchema during ECON schema initialization:
 * <pre>
 * FileSchema fileSchema = new FileSchema(...);
 * EconRawToParquetConverter econConverter = new EconRawToParquetConverter(
 *     blsDownloader, fredDownloader, treasuryDownloader, beaDownloader, worldBankDownloader);
 * fileSchema.registerRawToParquetConverter(econConverter);
 * </pre>
 *
 * @see RawToParquetConverter
 * @see EconSchemaFactory
 */
public class EconRawToParquetConverter implements RawToParquetConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(EconRawToParquetConverter.class);

  private final BlsDataDownloader blsDownloader;
  private final FredDataDownloader fredDownloader;

  /**
   * Constructs ECON converter with references to all economic data downloaders.
   *
   * @param blsDownloader  BLS data downloader
   * @param fredDownloader FRED data downloader
   */
  public EconRawToParquetConverter(BlsDataDownloader blsDownloader,
                                   FredDataDownloader fredDownloader) {
    this.blsDownloader = blsDownloader;
    this.fredDownloader = fredDownloader;
  }

  public BlsDataDownloader blsDownloader() {
    return blsDownloader;
  }

  public FredDataDownloader fredDownloader() {
    return fredDownloader;
  }

  @Override public boolean canConvert(String rawFilePath, ConversionMetadata metadata) {
    // Check if this is an ECON data file by looking for source=econ in path
    boolean canConvert = rawFilePath.contains("source=econ/");
    if (canConvert) {
      LOGGER.debug("EconRawToParquetConverter can handle: {}", rawFilePath);
    }
    return canConvert;
  }

  @Override public boolean convertToParquet(String rawFilePath, String targetParquetPath,
      StorageProvider storageProvider) throws IOException {

    LOGGER.info("Starting ECON data conversion for file: {}",
        rawFilePath.substring(rawFilePath.lastIndexOf('/') + 1));
    LOGGER.debug("Full raw path: {}", rawFilePath);
    LOGGER.debug("Target parquet path: {}", targetParquetPath);

    // Extract year from path
    String year = extractYearFrom(rawFilePath);
    if (year == null) {
        LOGGER.error("Failed to extract year from path structure: {}", rawFilePath);
      return false;
    }
    LOGGER.debug("Extracted year: {}", year);

    // Extract govdata path components
    String govdataRelativePath = extractGovDataPath(rawFilePath);
    String correctedParquetPath = govdataRelativePath.replace(".json", ".parquet");
    LOGGER.debug("Resolved parquet output path: {}", correctedParquetPath);

    // Route based on data type - verification is handled by the downloader's cache manifest
    // Post-migration: This converter will be removed when file adapter handles conversion via materialize config
    try {
        // FRED indicators
      if (rawFilePath.contains("type=fred_indicators") && rawFilePath.contains("fred_indicators")) {
        LOGGER.info("Processing FRED indicators data for year {}", year);
        Map<String, String> variables = new HashMap<>();
        variables.put("year", year);
        fredDownloader.convertCachedJsonToParquet("fred_indicators", variables);
        LOGGER.info("Completed FRED indicators conversion for year {}", year);
        return true;
      }

        // BLS employment statistics
      if (rawFilePath.contains("type=employment_statistics") && rawFilePath.contains("employment_statistics")) {
        LOGGER.info("Processing BLS employment statistics for year {}", year);
        Map<String, String> variables = new HashMap<>();
        variables.put("year", year);
        variables.put("frequency", "monthly");
        blsDownloader.convertCachedJsonToParquet("employment_statistics", variables);
        LOGGER.info("Completed BLS employment conversion for year {}", year);
        return true;
      }

        // BLS inflation metrics
      if (rawFilePath.contains("type=inflation_metrics") && rawFilePath.contains("inflation_metrics")) {
        LOGGER.info("Processing BLS inflation metrics for year {}", year);
        Map<String, String> variables = new HashMap<>();
        variables.put("year", year);
        variables.put("frequency", "monthly");
        blsDownloader.convertCachedJsonToParquet("inflation_metrics", variables);
        LOGGER.info("Completed BLS inflation conversion for year {}", year);
        return true;
      }

        // BLS wage growth
      if (rawFilePath.contains("type=wage_growth") && rawFilePath.contains("wage_growth")) {
        LOGGER.info("Processing BLS wage growth data for year {} (quarterly)", year);
        Map<String, String> variables = new HashMap<>();
        variables.put("year", year);
        variables.put("frequency", "quarterly");
        blsDownloader.convertCachedJsonToParquet("wage_growth", variables);
        LOGGER.info("Completed BLS wage growth conversion for year {}", year);
        return true;
      }

        // BLS JOLTS regional
      if (rawFilePath.contains("type=jolts_regional")) {
        LOGGER.info("Processing BLS JOLTS regional data for year {}", year);
        Map<String, String> variables = new HashMap<>();
        variables.put("year", year);
        variables.put("frequency", "monthly");
        blsDownloader.convertCachedJsonToParquet("jolts_regional", variables);
        LOGGER.info("Completed BLS JOLTS conversion for year {}", year);
        return true;
      }

        // BLS metro CPI
      if (rawFilePath.contains("type=metro_cpi") || rawFilePath.contains("metro_cpi")) {
        LOGGER.info("Processing BLS metro area CPI data for year {}", year);
        Map<String, String> variables = new HashMap<>();
        variables.put("year", year);
        blsDownloader.convertCachedJsonToParquet("metro_cpi", variables);
        LOGGER.info("Completed BLS metro CPI conversion for year {}", year);
        return true;
      }

        // BLS metro wages
      if (rawFilePath.contains("type=metro_wages") || rawFilePath.contains("metro_wages")) {
        LOGGER.info("Processing BLS metro area wage data for year {}", year);
        Map<String, String> variables = new HashMap<>();
        variables.put("year", year);
        blsDownloader.convertCachedJsonToParquet("metro_wages", variables);
        LOGGER.info("Completed BLS metro wages conversion for year {}", year);
        return true;
      }

        // Unhandled data type
      LOGGER.warn("No converter configured for data type in path: {}", rawFilePath);
      LOGGER.debug("Unhandled path pattern - add converter for this data source");
      return false;

    } catch (Exception e) {
      LOGGER.error("Conversion failed for {} - Error: {}",
          rawFilePath.substring(rawFilePath.lastIndexOf('/') + 1),
          e.getMessage(), e);
      throw new IOException("ECON conversion failed: " + e.getMessage(), e);
    }
  }

  /**
   * Extracts the year value from a path containing year=YYYY.
   * Example: .../year=2020/... returns "2020"
   *
   * @param path The path containing year=YYYY
   * @return The year value (e.g., "2020"), or null if not found
   */
  private String extractYearFrom(String path) {
    int yearIndex = path.indexOf("year=");
    if (yearIndex >= 0) {
      int start = yearIndex + 5; // Skip "year="
      int end = path.indexOf("/", start);
      if (end < 0) {
        end = path.length();
      }
      return path.substring(start, end);
    }
    return null;
  }

  /**
   * Extracts the govdata-specific path components from a raw file path.
   * Transforms: .../source=econ/type=indicators/year=2020/fred_indicators.json
   * To: type=indicators/year=2020/fred_indicators.json
   *
   * @param rawFilePath The full raw file path
   * @return The relative path starting from type=
   */
  private String extractGovDataPath(String rawFilePath) {
    // Find "type=" in the path and extract everything from there
    int typeIndex = rawFilePath.indexOf("type=");
    if (typeIndex >= 0) {
      return rawFilePath.substring(typeIndex);
    }
    // Fallback: return the filename
    return new File(rawFilePath).getName();
  }
}
