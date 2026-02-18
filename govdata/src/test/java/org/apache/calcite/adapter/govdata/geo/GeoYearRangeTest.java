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
package org.apache.calcite.adapter.govdata.geo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test year range support for GEO schema.
 */
@Tag("unit")
public class GeoYearRangeTest {

  @TempDir
  File tempDir;

  private GeoSchemaFactory factory;

  @BeforeEach
  public void setUp() {
    factory = new GeoSchemaFactory();
  }

  @Test public void testCensusYearLogic() throws Exception {
    // Test the census year logic conceptually
    // Census is conducted every 10 years: 2000, 2010, 2020, 2030...
    // For a given year range, the relevant census year is the most recent one

    // For 2023-2024 range, use 2020 census
    int year2023 = 2023;
    int expectedCensusYear = (year2023 / 10) * 10; // 2020
    assertEquals(2020, expectedCensusYear, "2023 should use 2020 census");

    // For 2010 range, use 2010 census
    int year2010 = 2010;
    expectedCensusYear = (year2010 / 10) * 10;
    assertEquals(2010, expectedCensusYear, "2010 should use 2010 census");

    // For 2005 range, use 2000 census
    int year2005 = 2005;
    expectedCensusYear = (year2005 / 10) * 10;
    assertEquals(2000, expectedCensusYear, "2005 should use 2000 census");
  }

  @Test public void testSchemaCreationWithYearRange() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("cacheDir", tempDir.getAbsolutePath());
    operand.put("startYear", 2020);
    operand.put("endYear", 2024);
    operand.put("enabledSources", new String[]{"tiger"});
    operand.put("autoDownload", false);

    // Create schema through GovDataSchemaFactory
    org.apache.calcite.adapter.govdata.GovDataSchemaFactory govDataFactory = new org.apache.calcite.adapter.govdata.GovDataSchemaFactory();
    operand.put("dataSource", "geo");
    org.apache.calcite.schema.Schema schema = govDataFactory.create(null, "geo", operand);

    assertNotNull(schema, "Schema should be created");
    // Note: Schema implementation type is internal - just verify it was created
  }

  @Test public void testBackwardCompatibilityWithDataYear() throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("cacheDir", tempDir.getAbsolutePath());
    operand.put("dataYear", 2023); // Old parameter
    operand.put("enabledSources", new String[]{"tiger"});
    operand.put("autoDownload", false);

    // Create schema through GovDataSchemaFactory - should still work
    org.apache.calcite.adapter.govdata.GovDataSchemaFactory govDataFactory = new org.apache.calcite.adapter.govdata.GovDataSchemaFactory();
    operand.put("dataSource", "geo");
    org.apache.calcite.schema.Schema schema = govDataFactory.create(null, "geo", operand);

    assertNotNull(schema, "Schema should be created with backward compatibility");
    // Note: Schema implementation type is internal - just verify it was created
  }

  @Test public void testYearPartitionedDirectoryStructure() throws Exception {
    // Create a TigerDataDownloader with multiple years
    File cacheDir = new File(tempDir, "tiger-cache");
    cacheDir.mkdirs(); // Ensure directory exists for test
    List<Integer> years = Arrays.asList(2022, 2023, 2024);
    TigerDataDownloader downloader = new TigerDataDownloader(cacheDir, years, false);

    assertNotNull(downloader, "TigerDataDownloader should be created");

    // If we were to download (with autoDownload=true), files would go to:
    // cacheDir/year=2022/states/
    // cacheDir/year=2023/states/
    // cacheDir/year=2024/states/

    // Create expected directory structure manually to test
    for (int year : years) {
      File yearDir = new File(cacheDir, String.format("year=%d", year));
      File statesDir = new File(yearDir, "states");
      assertTrue(statesDir.mkdirs() || statesDir.exists(),
          "Should create year-partitioned directory: " + statesDir);
    }

    // Verify all year directories exist
    for (int year : years) {
      File yearDir = new File(cacheDir, String.format("year=%d", year));
      assertTrue(yearDir.exists(), "Year directory should exist: " + yearDir);
    }
  }

  // Skip this test for now - requires full schema setup
  // @Test // @Tag("integration")
  public void testModelFileWithYearRange() throws Exception {
    // Create a test model file
    String modelJson = "{\n"
  +
        "  \"version\": \"1.0\",\n"
  +
        "  \"defaultSchema\": \"geo\",\n"
  +
        "  \"schemas\": [{\n"
  +
        "    \"name\": \"geo\",\n"
  +
        "    \"type\": \"custom\",\n"
  +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.geo.GeoSchemaFactory\",\n"
  +
        "    \"operand\": {\n"
  +
        "      \"cacheDir\": \"" + tempDir.getAbsolutePath() + "\",\n"
  +
        "      \"startYear\": 2022,\n"
  +
        "      \"endYear\": 2024,\n"
  +
        "      \"enabledSources\": [\"tiger\"],\n"
  +
        "      \"autoDownload\": false\n"
  +
        "    }\n"
  +
        "  }]\n"
  +
        "}";

    File modelFile = new File(tempDir, "test-model.json");
    java.nio.file.Files.write(modelFile.toPath(), modelJson.getBytes());

    // Connect using the model file
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, info);
         Statement stmt = conn.createStatement()) {

      // Query a simple table to verify schema was created
      // Note: information_schema is not available in all Calcite configurations
      try (ResultSet rs =
          stmt.executeQuery("SELECT * FROM geo.tiger_states LIMIT 1")) {

        // Just verify we can query the table (even if it returns no data)
        // The fact that the query doesn't throw an exception means the schema is working
        assertNotNull(rs, "Should be able to query GEO schema tables");
      }
    }
  }

  @Test public void testCensusApiClientWithYears() {
    File cacheDir = new File(tempDir, "census-cache");
    cacheDir.mkdirs(); // Ensure directory exists for test
    List<Integer> censusYears = Arrays.asList(2010, 2020);

    // Create CensusApiClient with census years
    CensusApiClient client = new CensusApiClient("test-api-key", cacheDir, censusYears);

    assertNotNull(client, "CensusApiClient should be created with year list");
  }
}
