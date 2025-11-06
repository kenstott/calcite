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

/**
 * Downloads and converts Federal Reserve Economic Data (FRED) to Parquet format.
 * Provides access to thousands of economic time series including interest rates,
 * GDP, monetary aggregates, and economic indicators.
 *
 * <p>Requires a FRED API key from https://fred.stlouisfed.org/docs/api/api_key.html
 */
public class FredDataDownloader extends AbstractEconDataDownloader {

  public FredDataDownloader(String cacheDir, String operatingDirectory, String parquetDir, org.apache.calcite.adapter.file.storage.StorageProvider cacheStorageProvider, org.apache.calcite.adapter.file.storage.StorageProvider storageProvider, CacheManifest sharedManifest) {
    super(cacheDir, operatingDirectory, parquetDir, cacheStorageProvider, storageProvider, sharedManifest);
  }

  @Override protected String getTableName() {
    return "fred_indicators";
  }

}
