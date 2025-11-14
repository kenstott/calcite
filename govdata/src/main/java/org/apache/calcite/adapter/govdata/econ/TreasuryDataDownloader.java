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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Downloads and converts U.S. Treasury data to Parquet format.
 * Supports daily treasury yields and federal debt statistics.
 *
 * <p>Uses the Treasury Fiscal Data API which requires no authentication.
 */
public class TreasuryDataDownloader extends AbstractEconDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TreasuryDataDownloader.class);
  private static final String TREASURY_API_BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/";

  public TreasuryDataDownloader(String cacheDir, StorageProvider cacheStorageProvider, StorageProvider storageProvider) {
    this(cacheDir, cacheDir, cacheDir, cacheStorageProvider, storageProvider, null);
  }

  public TreasuryDataDownloader(String cacheDir, String operatingDirectory, String parquetDir, StorageProvider cacheStorageProvider, StorageProvider storageProvider, CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider, sharedManifest);
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
  public void downloadTreasuryYields(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading treasury yields for years {}-{}", startYear, endYear);

    String tableName = "treasury_yields";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    // Use optimized iteration with DuckDB cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        startYear,
        endYear,
        (dimensionName) -> null, // No additional dimensions beyond year
        (cacheKey, vars, jsonPath, parquetPath) -> {
          int year = Integer.parseInt(vars.get("year"));

          String startDate = year + "-01-01";
          String endDate = year + "-12-31";

          String url = TREASURY_API_BASE + "v2/accounting/od/avg_interest_rates"
              + "?filter=record_date:gte:" + startDate
              + ",record_date:lte:" + endDate
              + "&sort=-record_date&page[size]=10000";

          HttpRequest request = HttpRequest.newBuilder()
              .uri(URI.create(url))
              .timeout(Duration.ofSeconds(30))
              .build();

          HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

          if (response.statusCode() != 200) {
            throw new IOException("Treasury API request failed with status: " + response.statusCode());
          }

          // Write data to cache (StorageProvider handles directory creation)
          String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
          cacheStorageProvider.writeFile(fullJsonPath, response.body().getBytes(StandardCharsets.UTF_8));

          long fileSize = cacheStorageProvider.getMetadata(fullJsonPath).getSize();
          cacheManifest.markCached(cacheKey, fullJsonPath, fileSize,
              getCacheExpiryForYear(year), getCachePolicyForYear(year));
        },
        "download");
  }

  /**
   * Downloads federal debt statistics using metadata-driven pattern.
   * Uses IterationDimension pattern for declarative multi-dimensional iteration.
   *
   * @param startYear First year to download
   * @param endYear Last year to download
   */
  public void downloadFederalDebt(int startYear, int endYear) throws IOException, InterruptedException {
    LOGGER.info("Downloading federal debt for years {}-{}", startYear, endYear);

    String tableName = "federal_debt";
    Map<String, Object> metadata = loadTableMetadata(tableName);
    String pattern = (String) metadata.get("pattern");

    // Use optimized iteration with DuckDB cache filtering (10-20x faster)
    iterateTableOperationsOptimized(
        tableName,
        startYear,
        endYear,
        (dimensionName) -> null, // No additional dimensions beyond year
        (cacheKey, vars, jsonPath, parquetPath) -> {
          int year = Integer.parseInt(vars.get("year"));

          String startDate = year + "-01-01";
          String endDate = year + "-12-31";

          String url = TREASURY_API_BASE + "v2/accounting/od/debt_to_penny"
              + "?filter=record_date:gte:" + startDate
              + ",record_date:lte:" + endDate
              + "&page[size]=10000";

          HttpRequest request = HttpRequest.newBuilder()
              .uri(URI.create(url))
              .timeout(Duration.ofSeconds(30))
              .build();

          HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

          if (response.statusCode() != 200) {
            throw new IOException("Treasury API request failed with status: " + response.statusCode());
          }

          // Write data to cache (StorageProvider handles directory creation)
          String fullJsonPath = cacheStorageProvider.resolvePath(cacheDirectory, jsonPath);
          cacheStorageProvider.writeFile(fullJsonPath, response.body().getBytes(StandardCharsets.UTF_8));

          long fileSize = cacheStorageProvider.getMetadata(fullJsonPath).getSize();
          cacheManifest.markCached(cacheKey, fullJsonPath, fileSize,
              getCacheExpiryForYear(year), getCachePolicyForYear(year));
        },
        "download");
  }

}
