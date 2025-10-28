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
  private final TreasuryDataDownloader treasuryDownloader;
  private final BeaDataDownloader beaDownloader;
  private final WorldBankDataDownloader worldBankDownloader;

  /**
   * Constructs ECON converter with references to all economic data downloaders.
   *
   * @param blsDownloader BLS data downloader
   * @param fredDownloader FRED data downloader
   * @param treasuryDownloader Treasury data downloader
   * @param beaDownloader BEA data downloader
   * @param worldBankDownloader World Bank data downloader
   */
  public EconRawToParquetConverter(
      BlsDataDownloader blsDownloader,
      FredDataDownloader fredDownloader,
      TreasuryDataDownloader treasuryDownloader,
      BeaDataDownloader beaDownloader,
      WorldBankDataDownloader worldBankDownloader) {
    this.blsDownloader = blsDownloader;
    this.fredDownloader = fredDownloader;
    this.treasuryDownloader = treasuryDownloader;
    this.beaDownloader = beaDownloader;
    this.worldBankDownloader = worldBankDownloader;
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

    LOGGER.info("=== ECON CONVERSION START ===");
    LOGGER.info("CONVERT: Raw file path from FileSchema: {}", rawFilePath);
    LOGGER.info("CONVERT: Target parquet path from FileSchema: {}", targetParquetPath);

    // Extract year from path (e.g., "year=2020")
    String year = extractYearFrom(rawFilePath);
    if (year == null) {
      LOGGER.error("CONVERT: Could not extract year from path: {}", rawFilePath);
      return false;
    }

    // Extract govdata path components from rawFilePath to construct correct parquet path
    // rawFilePath contains: .../source=econ/type=indicators/year=2020/fred_indicators.json
    // We need: type=indicators/year=2020/fred_indicators.parquet
    String govdataRelativePath = extractGovDataPath(rawFilePath);
    LOGGER.info("CONVERT: Extracted govdata relative path: {}", govdataRelativePath);

    // Construct correct parquet path for govdata (relative path for storageProvider)
    String correctedParquetPath = govdataRelativePath.replace(".json", ".parquet");
    LOGGER.info("CONVERT: Corrected parquet path for GOVDATA_PARQUET_DIR: {}", correctedParquetPath);

    // Route based on path patterns and construct CORRECT raw file paths
    try {
      // FRED indicators - raw files are at source=econ/type=fred_indicators/frequency=monthly/year=YYYY/
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("fred_indicators")) {
        LOGGER.info("CONVERT: Routing to FRED downloader for fred_indicators");
        String correctRawPath = "source=econ/type=fred_indicators/frequency=monthly/year=" + year;
        LOGGER.info("CONVERT: Corrected raw path: {}", correctRawPath);
        fredDownloader.convertToParquet(correctRawPath, correctedParquetPath);
        LOGGER.info("CONVERT: ✅ FRED conversion completed successfully");
        if (storageProvider.exists(correctedParquetPath)) {
          LOGGER.info("CONVERT: ✅ File confirmed to exist at storage location");
        } else {
          LOGGER.error("CONVERT: ❌ ERROR - File does NOT exist at storage location after conversion!");
        }
        return true;
      }

      // BLS employment statistics - raw files are at source=econ/type=employment_statistics/frequency=monthly/year=YYYY/
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("employment_statistics")) {
        LOGGER.info("CONVERT: Routing to BLS downloader for employment_statistics");
        String correctRawPath = "source=econ/type=employment_statistics/frequency=monthly/year=" + year;
        LOGGER.info("CONVERT: Corrected raw path: {}", correctRawPath);
        blsDownloader.convertToParquet(correctRawPath, correctedParquetPath);
        LOGGER.info("CONVERT: ✅ BLS employment conversion completed");
        if (storageProvider.exists(correctedParquetPath)) {
          LOGGER.info("CONVERT: ✅ File confirmed at: {}", correctedParquetPath);
        } else {
          LOGGER.error("CONVERT: ❌ File NOT found at: {}", correctedParquetPath);
        }
        return true;
      }

      // BLS inflation metrics - raw files are at source=econ/type=inflation_metrics/frequency=monthly/year=YYYY/
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("inflation_metrics")) {
        LOGGER.info("CONVERT: Routing to BLS downloader for inflation_metrics");
        String correctRawPath = "source=econ/type=inflation_metrics/frequency=monthly/year=" + year;
        LOGGER.info("CONVERT: Corrected raw path: {}", correctRawPath);
        blsDownloader.convertToParquet(correctRawPath, correctedParquetPath);
        LOGGER.info("CONVERT: ✅ BLS inflation conversion completed");
        if (storageProvider.exists(correctedParquetPath)) {
          LOGGER.info("CONVERT: ✅ File confirmed at: {}", correctedParquetPath);
        } else {
          LOGGER.error("CONVERT: ❌ File NOT found at: {}", correctedParquetPath);
        }
        return true;
      }

      // BLS wage growth - raw files are at source=econ/type=wage_growth/frequency=monthly/year=YYYY/
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("wage_growth")) {
        LOGGER.info("CONVERT: Routing to BLS downloader for wage_growth");
        String correctRawPath = "source=econ/type=wage_growth/frequency=monthly/year=" + year;
        LOGGER.info("CONVERT: Corrected raw path: {}", correctRawPath);
        blsDownloader.convertToParquet(correctRawPath, correctedParquetPath);
        LOGGER.info("CONVERT: ✅ BLS wage growth conversion completed");
        if (storageProvider.exists(correctedParquetPath)) {
          LOGGER.info("CONVERT: ✅ File confirmed at: {}", correctedParquetPath);
        } else {
          LOGGER.error("CONVERT: ❌ File NOT found at: {}", correctedParquetPath);
        }
        return true;
      }

      // TODO: Implement correct path mapping for remaining data sources
      // For now, only Phase 1 tables (fred_indicators, employment_statistics, inflation_metrics, wage_growth) are fully implemented

      LOGGER.warn("CONVERT: Path mapping not yet implemented for: {}", rawFilePath);
      LOGGER.warn("CONVERT: This conversion will be skipped. Add path mapping for this data source.");

      // If we get here, we recognized it as ECON data but don't have a specific handler
      LOGGER.warn("EconRawToParquetConverter recognized ECON data but no specific handler for: {}", rawFilePath);
      return false;

    } catch (Exception e) {
      LOGGER.error("EconRawToParquetConverter failed to convert {}: {}", rawFilePath, e.getMessage(), e);
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
