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

    LOGGER.info("EconRawToParquetConverter processing: {} -> {}", rawFilePath, targetParquetPath);

    // Extract the source directory from raw file path
    // Raw file paths are like: /path/to/cache/source=econ/type=indicators/year=2020/employment_statistics.json
    File rawFile = new File(rawFilePath);
    File sourceDir = rawFile.getParentFile(); // Get the year directory

    // Get source directory path as String
    String sourceDirPath = sourceDir.getAbsolutePath();

    // Route based on path patterns
    try {
      // FRED indicators
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("fred_indicators")) {
        fredDownloader.convertToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // BLS employment statistics
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("employment_statistics")) {
        blsDownloader.convertToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // BLS inflation metrics
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("inflation_metrics")) {
        blsDownloader.convertToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // BLS wage growth
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("wage_growth")) {
        blsDownloader.convertToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // BLS regional employment
      if (rawFilePath.contains("type=regional") && rawFilePath.contains("regional_employment")) {
        blsDownloader.convertToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // Treasury yields
      if (rawFilePath.contains("type=timeseries") && rawFilePath.contains("treasury_yields")) {
        treasuryDownloader.convertToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // Treasury federal debt
      if (rawFilePath.contains("type=timeseries") && rawFilePath.contains("federal_debt")) {
        treasuryDownloader.convertFederalDebtToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // BEA GDP components
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("gdp_components")) {
        beaDownloader.convertToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // BEA GDP statistics
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("gdp_statistics")) {
        beaDownloader.convertGdpStatisticsToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // BEA regional income
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("regional_income")) {
        beaDownloader.convertRegionalIncomeToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // BEA state GDP
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("state_gdp")) {
        beaDownloader.convertStateGdpToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // BEA trade statistics
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("trade_statistics")) {
        beaDownloader.convertTradeStatisticsToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // BEA ITA data
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("ita_data")) {
        beaDownloader.convertItaDataToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // BEA industry GDP
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("industry_gdp")) {
        beaDownloader.convertIndustryGdpToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // World Bank indicators
      if (rawFilePath.contains("type=indicators") && rawFilePath.contains("world_indicators")) {
        worldBankDownloader.convertToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // Custom FRED series
      if (rawFilePath.contains("type=custom_fred") || rawFilePath.contains("type=custom")) {
        // Extract series ID from path if possible
        // For now, delegate to FRED downloader's generic conversion
        fredDownloader.convertToParquet(sourceDirPath, targetParquetPath);
        return true;
      }

      // If we get here, we recognized it as ECON data but don't have a specific handler
      LOGGER.warn("EconRawToParquetConverter recognized ECON data but no specific handler for: {}", rawFilePath);
      return false;

    } catch (Exception e) {
      LOGGER.error("EconRawToParquetConverter failed to convert {}: {}", rawFilePath, e.getMessage(), e);
      throw new IOException("ECON conversion failed: " + e.getMessage(), e);
    }
  }
}
