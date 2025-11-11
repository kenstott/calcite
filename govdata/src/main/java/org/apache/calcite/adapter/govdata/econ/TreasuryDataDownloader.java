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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
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

  public TreasuryDataDownloader(String cacheDir, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    this(cacheDir, cacheDir, cacheDir, cacheStorageProvider, storageProvider, null);
  }

  public TreasuryDataDownloader(String cacheDir, String operatingDirectory, String parquetDir, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider, CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider, sharedManifest);
  }

  @Override protected String getTableName() {
    return "treasury_yields";
  }

  /**
   * Downloads daily treasury yield curve data.
   * @return The storage path (local or S3) where the data was saved
   */
  public String downloadTreasuryYields(int startYear, int endYear) throws IOException, InterruptedException {
    // Download for each year separately to match FileSchema partitioning expectations
    String lastPath = null;
    for (int year = startYear; year <= endYear; year++) {
      String relativePath = buildPartitionPath("timeseries", DataFrequency.DAILY, year) + "/treasury_yields.json";

      Map<String, String> cacheParams = new HashMap<>();

      // Check cache using base class helper
      if (isCachedOrExists("treasury_yields", year, cacheParams)) {
        LOGGER.info("Found cached treasury yields for year {} - skipping download", year);
        lastPath = relativePath;
        continue;
      }

      // Fetch data from Treasury API for this year
      LOGGER.info("Downloading treasury yields for year {}", year);

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
        LOGGER.warn("Treasury API request failed for year {} with status: {}", year, response.statusCode());
        continue;
      }

      // Save to cache using base class helper
      saveToCache("treasury_yields", year, cacheParams, relativePath, response.body());
      lastPath = relativePath;
    }

    return lastPath;
  }

  /**
   * Downloads federal debt statistics.
   * @return The storage path (local or S3) where the data was saved
   */
  public String downloadFederalDebt(int startYear, int endYear) throws IOException, InterruptedException {
    // Download for each year separately to match FileSchema partitioning expectations
    String lastPath = null;
    for (int year = startYear; year <= endYear; year++) {
      String relativePath = buildPartitionPath("timeseries", DataFrequency.DAILY, year) + "/federal_debt.json";

      Map<String, String> cacheParams = new HashMap<>();

      // Check cache using base class helper
      if (isCachedOrExists("federal_debt", year, cacheParams)) {
        LOGGER.info("Found cached federal debt for year {} - skipping download", year);
        lastPath = relativePath;
        continue;
      }

      // Fetch debt to the penny data for this year
      LOGGER.info("Downloading federal debt data for year {}", year);

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
        LOGGER.warn("Treasury API request failed for year {} with status: {}", year, response.statusCode());
        continue;
      }

      // Save to cache using base class helper
      saveToCache("federal_debt", year, cacheParams, relativePath, response.body());
      lastPath = relativePath;
    }

    return lastPath;
  }

}
