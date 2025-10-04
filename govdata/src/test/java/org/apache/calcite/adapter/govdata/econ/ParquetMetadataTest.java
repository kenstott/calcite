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
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that Parquet metadata is correctly written by data downloaders.
 */
@Tag("unit")
public class ParquetMetadataTest {

  @TempDir
  File tempDir;

  private StorageProvider createStorageProvider() {
    return StorageProviderFactory.createFromUrl("file://" + tempDir.getAbsolutePath());
  }

  @Test void testTradeStatisticsMetadata() throws Exception {
    // Create a minimal trade statistics dataset
    BeaDataDownloader downloader = new BeaDataDownloader("test_api_key", tempDir.getAbsolutePath(), createStorageProvider());

    List<BeaDataDownloader.TradeStatistic> tradeStats = new ArrayList<>();
    BeaDataDownloader.TradeStatistic trade = new BeaDataDownloader.TradeStatistic();
    trade.tableId = "T40205B";
    trade.lineNumber = 1;
    trade.lineDescription = "Exports of goods";
    trade.seriesCode = "A001";
    trade.value = 1000.0;
    trade.units = "Billions of dollars";
    trade.frequency = "Annual";
    trade.tradeType = "Export";
    trade.category = "Goods";
    trade.tradeBalance = 500.0;
    trade.year = 2023;
    tradeStats.add(trade);

    // Write the Parquet file
    File outputFile = new File(tempDir, "trade_statistics.parquet");
    downloader.writeTradeStatisticsParquet(tradeStats, outputFile.getAbsolutePath());

    // Read back the metadata
    Configuration conf = new Configuration();
    Path path = new Path(outputFile.getAbsolutePath());
    HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);

    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      ParquetMetadata metadata = reader.getFooter();
      FileMetaData fileMetadata = metadata.getFileMetaData();
      Map<String, String> keyValueMetadata = fileMetadata.getKeyValueMetaData();

      // Verify table comment is present
      assertTrue(keyValueMetadata.containsKey("parquet.meta.table.comment"),
          "Table comment should be present in metadata");
      String tableComment = keyValueMetadata.get("parquet.meta.table.comment");
      assertNotNull(tableComment, "Table comment should not be null");
      assertTrue(tableComment.contains("BEA NIPA Table T40205B"),
          "Table comment should mention BEA NIPA Table");
      assertTrue(tableComment.contains("trade"),
          "Table comment should mention trade");

      // Verify column comments are present
      assertTrue(keyValueMetadata.containsKey("parquet.meta.column.table_id.comment"),
          "Column comment for table_id should be present");
      assertEquals("BEA NIPA table identifier",
          keyValueMetadata.get("parquet.meta.column.table_id.comment"));

      assertTrue(keyValueMetadata.containsKey("parquet.meta.column.trade_balance.comment"),
          "Column comment for trade_balance should be present");
      assertEquals("Trade balance (exports minus imports)",
          keyValueMetadata.get("parquet.meta.column.trade_balance.comment"));

      // Verify generic comment is also set
      assertTrue(keyValueMetadata.containsKey("parquet.meta.comment"),
          "Generic comment should be present");
      assertEquals(tableComment, keyValueMetadata.get("parquet.meta.comment"),
          "Generic comment should match table comment");
    }
  }

  @Test void testRegionalIncomeMetadata() throws Exception {
    BeaDataDownloader downloader = new BeaDataDownloader("test_api_key", tempDir.getAbsolutePath(), createStorageProvider());

    List<BeaDataDownloader.RegionalIncome> incomeData = new ArrayList<>();
    BeaDataDownloader.RegionalIncome income = new BeaDataDownloader.RegionalIncome();
    income.geoFips = "01000";
    income.geoName = "Alabama";
    income.metric = "personal_income";
    income.lineCode = "1";
    income.lineDescription = "Personal income";
    income.year = 2023;
    income.value = 250000.0;
    income.units = "Millions of dollars";
    incomeData.add(income);

    File outputFile = new File(tempDir, "regional_income.parquet");
    downloader.writeRegionalIncomeParquet(incomeData, outputFile.getAbsolutePath());

    // Read back the metadata
    Configuration conf = new Configuration();
    Path path = new Path(outputFile.getAbsolutePath());
    HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);

    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      ParquetMetadata metadata = reader.getFooter();
      FileMetaData fileMetadata = metadata.getFileMetaData();
      Map<String, String> keyValueMetadata = fileMetadata.getKeyValueMetaData();

      // Verify table comment
      String tableComment = keyValueMetadata.get("parquet.meta.table.comment");
      assertNotNull(tableComment, "Table comment should not be null");
      assertTrue(tableComment.contains("BEA Regional Economic Accounts"),
          "Table comment should mention BEA Regional Economic Accounts");

      // Verify column comments
      assertEquals("FIPS code for geographic area",
          keyValueMetadata.get("parquet.meta.column.geo_fips.comment"));
      assertEquals("Geographic area name",
          keyValueMetadata.get("parquet.meta.column.geo_name.comment"));
      assertEquals("Metric type (personal_income, per_capita_income, population)",
          keyValueMetadata.get("parquet.meta.column.metric.comment"));
    }
  }

  @Test void testStateGdpMetadata() throws Exception {
    BeaDataDownloader downloader = new BeaDataDownloader("test_api_key", tempDir.getAbsolutePath(), createStorageProvider());

    List<BeaDataDownloader.StateGdp> gdpData = new ArrayList<>();
    BeaDataDownloader.StateGdp gdp = new BeaDataDownloader.StateGdp();
    gdp.geoFips = "01000";
    gdp.geoName = "Alabama";
    gdp.lineCode = "1";
    gdp.lineDescription = "All industry total";
    gdp.year = 2023;
    gdp.value = 250000.0;
    gdp.units = "Millions of dollars";
    gdpData.add(gdp);

    File outputFile = new File(tempDir, "state_gdp.parquet");
    downloader.writeStateGdpParquet(gdpData, outputFile.getAbsolutePath());

    // Read back the metadata
    Configuration conf = new Configuration();
    Path path = new Path(outputFile.getAbsolutePath());
    HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);

    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      ParquetMetadata metadata = reader.getFooter();
      FileMetaData fileMetadata = metadata.getFileMetaData();
      Map<String, String> keyValueMetadata = fileMetadata.getKeyValueMetaData();

      // Verify table comment
      String tableComment = keyValueMetadata.get("parquet.meta.table.comment");
      assertNotNull(tableComment, "Table comment should not be null");
      assertTrue(tableComment.contains("SAGDP2N"),
          "Table comment should mention SAGDP2N table");

      // Verify column comments
      assertEquals("FIPS code for state",
          keyValueMetadata.get("parquet.meta.column.geo_fips.comment"));
      assertEquals("State name",
          keyValueMetadata.get("parquet.meta.column.geo_name.comment"));
      assertEquals("GDP metric type",
          keyValueMetadata.get("parquet.meta.column.metric.comment"));
      assertEquals("GDP value",
          keyValueMetadata.get("parquet.meta.column.value.comment"));
      assertEquals("Units (millions of dollars, per capita, etc.)",
          keyValueMetadata.get("parquet.meta.column.units.comment"));
    }
  }
}
