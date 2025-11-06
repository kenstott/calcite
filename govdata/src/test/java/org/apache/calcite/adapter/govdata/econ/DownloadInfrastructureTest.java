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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for schema-driven download infrastructure in AbstractGovDataDownloader.
 *
 * <p>Tests URL building, expression evaluation, and parameter handling for
 * metadata-driven downloads.
 */
@Tag("unit")
class DownloadInfrastructureTest {

  private TestEconDownloader downloader;

  /**
   * Test downloader that extends AbstractEconDataDownloader and exposes
   * protected methods for testing.
   */
  private static class TestEconDownloader extends AbstractEconDataDownloader {
    TestEconDownloader() {
      super("/tmp/test-cache", "/tmp/test-operating", "/tmp/test-parquet",
          new LocalFileStorageProvider(),
          new LocalFileStorageProvider(),
          null); // no manifest needed for these tests
    }

    @Override
    protected long getMinRequestIntervalMs() {
      return 0; // No rate limit for tests
    }

    @Override
    protected int getMaxRetries() {
      return 1;
    }

    @Override
    protected long getRetryDelayMs() {
      return 0;
    }

    // Public wrappers for testing protected methods
    @Override
    public String evaluateExpression(String expression, Map<String, String> variables) {
      return super.evaluateExpression(expression, variables);
    }

    @Override
    public String buildDownloadUrl(Map<String, Object> downloadConfig,
        Map<String, String> variables,
        String iterationValue,
        int paginationOffset) {
      return super.buildDownloadUrl(downloadConfig, variables, iterationValue, paginationOffset);
    }
  }

  @BeforeEach
  void setUp() {
    downloader = new TestEconDownloader();
  }

  // ===== Expression Evaluation Tests =====

  @Test
  void testEvaluateExpression_StartOfYear() {
    Map<String, String> variables = new HashMap<>();
    variables.put("year", "2020");

    String result = downloader.evaluateExpression("startOfYear({year})", variables);

    assertEquals("2020-01-01", result, "startOfYear should return first day of year");
  }

  @Test
  void testEvaluateExpression_EndOfYear() {
    Map<String, String> variables = new HashMap<>();
    variables.put("year", "2020");

    String result = downloader.evaluateExpression("endOfYear({year})", variables);

    assertEquals("2020-12-31", result, "endOfYear should return last day of year");
  }

  @Test
  void testEvaluateExpression_StartOfMonth() {
    Map<String, String> variables = new HashMap<>();
    variables.put("year", "2020");
    variables.put("month", "3");

    String result = downloader.evaluateExpression("startOfMonth({year},{month})", variables);

    assertEquals("2020-03-01", result, "startOfMonth should return first day of month");
  }

  @Test
  void testEvaluateExpression_EndOfMonth_Regular() {
    Map<String, String> variables = new HashMap<>();
    variables.put("year", "2020");
    variables.put("month", "4");

    String result = downloader.evaluateExpression("endOfMonth({year},{month})", variables);

    assertEquals("2020-04-30", result, "endOfMonth should return last day (30) for April");
  }

  @Test
  void testEvaluateExpression_EndOfMonth_LeapYear() {
    Map<String, String> variables = new HashMap<>();
    variables.put("year", "2020");
    variables.put("month", "2");

    String result = downloader.evaluateExpression("endOfMonth({year},{month})", variables);

    assertEquals("2020-02-29", result, "endOfMonth should return 29 for Feb in leap year");
  }

  @Test
  void testEvaluateExpression_EndOfMonth_NonLeapYear() {
    Map<String, String> variables = new HashMap<>();
    variables.put("year", "2021");
    variables.put("month", "2");

    String result = downloader.evaluateExpression("endOfMonth({year},{month})", variables);

    assertEquals("2021-02-28", result, "endOfMonth should return 28 for Feb in non-leap year");
  }

  @Test
  void testEvaluateExpression_NoFunction() {
    Map<String, String> variables = new HashMap<>();
    variables.put("year", "2020");

    String result = downloader.evaluateExpression("{year}-Q1", variables);

    assertEquals("2020-Q1", result, "Should perform simple variable substitution");
  }

  // ===== buildDownloadUrl Tests =====

  @Test
  void testBuildDownloadUrl_ConstantParams() {
    Map<String, Object> downloadConfig = new HashMap<>();
    downloadConfig.put("baseUrl", "https://api.example.com/data");

    Map<String, Object> queryParams = new HashMap<>();
    Map<String, Object> param1 = new HashMap<>();
    param1.put("type", "constant");
    param1.put("value", "json");
    queryParams.put("format", param1);

    Map<String, Object> param2 = new HashMap<>();
    param2.put("type", "constant");
    param2.put("value", 1000);
    queryParams.put("limit", param2);

    downloadConfig.put("queryParams", queryParams);

    Map<String, String> variables = new HashMap<>();

    String url = downloader.buildDownloadUrl(downloadConfig, variables, null, 0);

    assertTrue(url.contains("format=json"), "URL should contain format parameter");
    assertTrue(url.contains("limit=1000"), "URL should contain limit parameter");
    assertTrue(url.startsWith("https://api.example.com/data?"), "URL should have correct base");
  }

  @Test
  void testBuildDownloadUrl_ExpressionParams() {
    Map<String, Object> downloadConfig = new HashMap<>();
    downloadConfig.put("baseUrl", "https://api.example.com/data");

    Map<String, Object> queryParams = new HashMap<>();
    Map<String, Object> startParam = new HashMap<>();
    startParam.put("type", "expression");
    startParam.put("value", "startOfYear({year})");
    queryParams.put("start_date", startParam);

    Map<String, Object> endParam = new HashMap<>();
    endParam.put("type", "expression");
    endParam.put("value", "endOfYear({year})");
    queryParams.put("end_date", endParam);

    downloadConfig.put("queryParams", queryParams);

    Map<String, String> variables = new HashMap<>();
    variables.put("year", "2020");

    String url = downloader.buildDownloadUrl(downloadConfig, variables, null, 0);

    assertTrue(url.contains("start_date=2020-01-01"), "URL should contain evaluated start date");
    assertTrue(url.contains("end_date=2020-12-31"), "URL should contain evaluated end date");
  }

  @Test
  void testBuildDownloadUrl_IterationParam() {
    Map<String, Object> downloadConfig = new HashMap<>();
    downloadConfig.put("baseUrl", "https://api.example.com/data");

    Map<String, Object> queryParams = new HashMap<>();
    Map<String, Object> seriesParam = new HashMap<>();
    seriesParam.put("type", "iteration");
    seriesParam.put("source", "seriesList");
    queryParams.put("series_id", seriesParam);

    downloadConfig.put("queryParams", queryParams);

    Map<String, String> variables = new HashMap<>();

    String url = downloader.buildDownloadUrl(downloadConfig, variables, "DFF", 0);

    assertTrue(url.contains("series_id=DFF"), "URL should contain iteration value");
  }

  @Test
  void testBuildDownloadUrl_PaginationParam() {
    Map<String, Object> downloadConfig = new HashMap<>();
    downloadConfig.put("baseUrl", "https://api.example.com/data");

    Map<String, Object> queryParams = new HashMap<>();
    Map<String, Object> offsetParam = new HashMap<>();
    offsetParam.put("type", "pagination");
    queryParams.put("offset", offsetParam);

    downloadConfig.put("queryParams", queryParams);

    Map<String, String> variables = new HashMap<>();

    String url = downloader.buildDownloadUrl(downloadConfig, variables, null, 5000);

    assertTrue(url.contains("offset=5000"), "URL should contain pagination offset");
  }

  @Test
  void testBuildDownloadUrl_MissingIterationValue() {
    Map<String, Object> downloadConfig = new HashMap<>();
    downloadConfig.put("baseUrl", "https://api.example.com/data");

    Map<String, Object> queryParams = new HashMap<>();
    Map<String, Object> seriesParam = new HashMap<>();
    seriesParam.put("type", "iteration");
    queryParams.put("series_id", seriesParam);

    downloadConfig.put("queryParams", queryParams);

    Map<String, String> variables = new HashMap<>();

    // Should throw when iteration param needed but value is null
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      downloader.buildDownloadUrl(downloadConfig, variables, null, 0);
    });

    assertTrue(exception.getMessage().contains("requires iteration value"),
        "Exception should indicate missing iteration value");
  }

  @Test
  void testBuildDownloadUrl_AuthParam_EnvVarSet() {
    // Set test environment variable
    Map<String, Object> downloadConfig = new HashMap<>();
    downloadConfig.put("baseUrl", "https://api.example.com/data");

    // Add authentication config
    Map<String, Object> authConfig = new HashMap<>();
    authConfig.put("type", "query_param");
    authConfig.put("paramName", "api_key");
    authConfig.put("envVar", "TEST_API_KEY");
    downloadConfig.put("authentication", authConfig);

    Map<String, Object> queryParams = new HashMap<>();
    Map<String, Object> authParam = new HashMap<>();
    authParam.put("type", "auth");
    queryParams.put("api_key", authParam);

    downloadConfig.put("queryParams", queryParams);

    Map<String, String> variables = new HashMap<>();

    // This test will fail if TEST_API_KEY is not set
    // In real usage, the env var should be set by the test environment
    // For this test, we'll expect an exception if env var is not set
    try {
      String url = downloader.buildDownloadUrl(downloadConfig, variables, null, 0);
      // If we get here, env var was set - verify it's in URL
      assertTrue(url.contains("api_key="), "URL should contain api_key parameter");
    } catch (IllegalArgumentException e) {
      // Expected if TEST_API_KEY not set
      assertTrue(e.getMessage().contains("TEST_API_KEY"),
          "Exception should mention missing env var");
    }
  }

  @Test
  void testBuildDownloadUrl_MissingBaseUrl() {
    Map<String, Object> downloadConfig = new HashMap<>();
    // Missing baseUrl
    downloadConfig.put("queryParams", new HashMap<>());

    Map<String, String> variables = new HashMap<>();

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      downloader.buildDownloadUrl(downloadConfig, variables, null, 0);
    });

    assertTrue(exception.getMessage().contains("baseUrl"),
        "Exception should indicate missing baseUrl");
  }

  @Test
  void testBuildDownloadUrl_MixedParams() {
    Map<String, Object> downloadConfig = new HashMap<>();
    downloadConfig.put("baseUrl", "https://api.example.com/observations");

    Map<String, Object> queryParams = new HashMap<>();

    // Constant param
    Map<String, Object> formatParam = new HashMap<>();
    formatParam.put("type", "constant");
    formatParam.put("value", "json");
    queryParams.put("file_type", formatParam);

    // Expression param
    Map<String, Object> startParam = new HashMap<>();
    startParam.put("type", "expression");
    startParam.put("value", "startOfYear({year})");
    queryParams.put("observation_start", startParam);

    // Iteration param
    Map<String, Object> seriesParam = new HashMap<>();
    seriesParam.put("type", "iteration");
    queryParams.put("series_id", seriesParam);

    // Pagination param
    Map<String, Object> offsetParam = new HashMap<>();
    offsetParam.put("type", "pagination");
    queryParams.put("offset", offsetParam);

    downloadConfig.put("queryParams", queryParams);

    Map<String, String> variables = new HashMap<>();
    variables.put("year", "2021");

    String url = downloader.buildDownloadUrl(downloadConfig, variables, "UNRATE", 1000);

    assertNotNull(url, "URL should be built");
    assertTrue(url.startsWith("https://api.example.com/observations?"),
        "URL should have correct base");
    assertTrue(url.contains("file_type=json"), "URL should contain constant param");
    assertTrue(url.contains("observation_start=2021-01-01"),
        "URL should contain evaluated expression");
    assertTrue(url.contains("series_id=UNRATE"), "URL should contain iteration value");
    assertTrue(url.contains("offset=1000"), "URL should contain pagination offset");
  }

  @Test
  void testBuildDownloadUrl_UrlEncoding() {
    Map<String, Object> downloadConfig = new HashMap<>();
    downloadConfig.put("baseUrl", "https://api.example.com/data");

    Map<String, Object> queryParams = new HashMap<>();
    Map<String, Object> param = new HashMap<>();
    param.put("type", "constant");
    param.put("value", "value with spaces");
    queryParams.put("query", param);

    downloadConfig.put("queryParams", queryParams);

    Map<String, String> variables = new HashMap<>();

    String url = downloader.buildDownloadUrl(downloadConfig, variables, null, 0);

    // URL should encode spaces as +
    assertTrue(url.contains("query=value+with+spaces") || url.contains("query=value%20with%20spaces"),
        "URL should encode spaces in query parameters");
  }
}
