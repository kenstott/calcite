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
import org.apache.calcite.adapter.govdata.OperationType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads and converts U.S. Treasury data to Parquet format.
 * Supports daily treasury yields and federal debt statistics.
 *
 * <p>Uses the Treasury Fiscal Data API which requires no authentication.
 * All endpoint configurations are defined in econ-schema.json.
 */
public class TreasuryDataDownloader extends AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TreasuryDataDownloader.class);

  public TreasuryDataDownloader(String cacheDir, StorageProvider cacheStorageProvider,
      StorageProvider storageProvider, int startYear, int endYear) {
    this(cacheDir, cacheDir, cacheDir, cacheStorageProvider, storageProvider, null, startYear, endYear);
  }

  public TreasuryDataDownloader(String cacheDir, String operatingDirectory, String parquetDir,
      StorageProvider cacheStorageProvider, StorageProvider storageProvider,
      CacheManifest sharedManifest, int startYear, int endYear) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider,
        sharedManifest, startYear, endYear);
  }

  @Override protected String getTableName() {
    return "treasury_yields";
  }

  /**
   * Downloads daily treasury yield curve data using metadata-driven pattern.
   * Uses IterationDimension pattern for declarative multi-dimensional iteration.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   */
  public void downloadTreasuryYields(int startYear, int endYear) {
    LOGGER.info("Treasury yields download: {} years of daily yield curve data",
        endYear - startYear + 1);

    String tableName = "treasury_yields";

    // Use optimized iteration with DuckDB cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          if ("year".equals(dimensionName)) {
            return yearRange(startYear, endYear);
          }
          return null; // No additional dimensions beyond year
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Use executeDownload which reads URL pattern from schema
          DownloadResult result = executeDownload(tableName, vars);

          cacheManifest.markCached(cacheKey, jsonPath, result.fileSize,
              getCacheExpiryForYear(year), getCachePolicyForYear(year));
        },
        OperationType.DOWNLOAD);
  }

  /**
   * Downloads federal debt statistics using metadata-driven pattern.
   * Uses IterationDimension pattern for declarative multi-dimensional iteration.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   */
  public void downloadFederalDebt(int startYear, int endYear) {
    LOGGER.info("Federal debt download: {} years of daily debt statistics",
        endYear - startYear + 1);

    String tableName = "federal_debt";

    // Use optimized iteration with DuckDB cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          if ("year".equals(dimensionName)) {
            return yearRange(startYear, endYear);
          }
          return null; // No additional dimensions beyond year
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          int year = Integer.parseInt(vars.get("year"));

          // Use executeDownload which reads URL pattern from schema
          DownloadResult result = executeDownload(tableName, vars);

          cacheManifest.markCached(cacheKey, jsonPath, result.fileSize,
              getCacheExpiryForYear(year), getCachePolicyForYear(year));
        },
        OperationType.DOWNLOAD);
  }

  /**
   * Converts downloaded treasury yields JSON to Parquet format.
   *
   * @param startYear First year to convert
   * @param endYear   Last year to convert
   */
  public void convertTreasuryYields(int startYear, int endYear) {
    LOGGER.info("Treasury yields conversion: {} years to parquet", endYear - startYear + 1);

    String tableName = "treasury_yields";

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          if ("year".equals(dimensionName)) {
            return yearRange(startYear, endYear);
          }
          return null;
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          // Execute conversion
          convertCachedJsonToParquet(tableName, vars);

          // Mark as converted
          cacheManifest.markParquetConverted(cacheKey, parquetPath);
        },
        OperationType.CONVERSION);
  }

  /**
   * Converts downloaded federal debt JSON to Parquet format.
   *
   * @param startYear First year to convert
   * @param endYear   Last year to convert
   */
  public void convertFederalDebt(int startYear, int endYear) {
    LOGGER.info("Federal debt conversion: {} years to parquet", endYear - startYear + 1);

    String tableName = "federal_debt";

    iterateTableOperationsOptimized(
        tableName,
        (dimensionName) -> {
          if ("year".equals(dimensionName)) {
            return yearRange(startYear, endYear);
          }
          return null;
        },
        (cacheKey, vars, jsonPath, parquetPath, prefetchHelper) -> {
          // Execute conversion
          convertCachedJsonToParquet(tableName, vars);

          // Mark as converted
          cacheManifest.markParquetConverted(cacheKey, parquetPath);
        },
        OperationType.CONVERSION);
  }
}
