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

import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Debug test for BEA regional income download issues.
 */
@Tag("integration")
public class RegionalIncomeDebugTest {

  @BeforeAll
  static void setUp() {
    TestEnvironmentLoader.ensureLoaded();
  }

  @Test public void testRegionalIncomeDownload() throws Exception {
    // Use a temp directory for testing
    Path tempDir = Files.createTempDirectory("regional-test");
    String cacheDir = tempDir.toString();

    String apiKey = System.getenv("BEA_API_KEY");
    if (apiKey == null) {
      apiKey = System.getProperty("BEA_API_KEY");
    }

    if (apiKey == null) {
      fail("BEA_API_KEY not found in environment or system properties");
    }

    System.out.println("Testing with BEA API key: " + apiKey.substring(0, 4) + "...");

    BeaDataDownloader downloader =
        new BeaDataDownloader(cacheDir,
        cacheDir,
            StorageProviderFactory.createFromUrl(cacheDir),
        StorageProviderFactory.createFromUrl(cacheDir));

    // Extract line codes list from schema
    List<String> lineCodesList = extractIterationList("regional_income", "lineCodesList");
    assumeTrue(!lineCodesList.isEmpty(), "lineCodesList not found in schema");

    // Test download for 2023 using metadata-driven method
    System.out.println("Downloading regional income for 2023...");
    downloader.downloadRegionalIncomeMetadata(2023, 2023);

    // Check if file was created (new path structure: type=regional_income/frequency=A/year=2023/)
    File jsonFile = new File(cacheDir, "type=regional_income/frequency=A/year=2023/regional_income.json");
    assertTrue(jsonFile.exists(), "Regional income JSON file should exist at: " + jsonFile.getAbsolutePath());

    // Check file size
    long fileSize = jsonFile.length();
    System.out.println("JSON file size: " + fileSize + " bytes");

    // Read and check content
    String content = Files.readString(jsonFile.toPath());
    System.out.println("First 500 chars of content: " + content.substring(0, Math.min(500, content.length())));

    // Check if it has actual data
    assertTrue(fileSize > 100, "File should have substantial data (not just empty array)");

    // Test parquet conversion using metadata-driven method
    System.out.println("Converting to parquet...");
    downloader.convertRegionalIncomeMetadata(2023, 2023);

    // Check parquet file was created (new path)
    File parquetFile = new File(cacheDir, "type=regional_income/frequency=A/year=2023/regional_income.parquet");
    assertTrue(parquetFile.exists(), "Parquet file should be created at: " + parquetFile.getAbsolutePath());
    System.out.println("Parquet file size: " + parquetFile.length() + " bytes");
  }

  @SuppressWarnings("unchecked")
  private List<String> extractIterationList(String tableName, String listKey) {
    try {
      InputStream schemaStream = getClass().getResourceAsStream("/econ-schema.json");
      if (schemaStream == null) {
        return Collections.emptyList();
      }
      ObjectMapper mapper = new ObjectMapper();
      JsonNode root = mapper.readTree(schemaStream);
      JsonNode tables = root.get("tables");
      if (tables != null && tables.isArray()) {
        for (JsonNode table : tables) {
          if (tableName.equals(table.get("name").asText())) {
            JsonNode download = table.get("download");
            if (download != null) {
              JsonNode listNode = download.get(listKey);
              if (listNode != null && listNode.isArray()) {
                List<String> result = new ArrayList<>();
                for (JsonNode item : listNode) {
                  result.add(item.asText());
                }
                return result;
              }
            }
          }
        }
      }
      return Collections.emptyList();
    } catch (Exception e) {
      return Collections.emptyList();
    }
  }
}
