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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Queries Apple (CIK 0000320193) filing_metadata from Cloudflare R2 via the govdata
 * Calcite adapter with autoDownload=false and executionEngine=parquet.
 */
@Tag("integration")
public class AaplRevenueR2IntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(AaplRevenueR2IntegrationTest.class);

  private static final String R2_ACCESS_KEY = "d4dc1320014b34fef4be78f0b126e63b";
  private static final String R2_SECRET_KEY =
      "7c2a89d9f0f976116a69654026c9c47a33e8c3af2293f83d8204b1cf92a2e577";
  private static final String R2_ENDPOINT =
      "https://21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com";
  private static final String R2_PARQUET_DIR = "s3://govdata-parquet-v1";
  private static final String APPLE_CIK = "0000320193";

  private Connection createConnection() throws Exception {
    String modelJson =
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"sec\","
        + "  \"schemas\": [{"
        + "    \"name\": \"sec\","
        + "    \"type\": \"custom\","
        + "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "    \"operand\": {"
        + "      \"dataSource\": \"sec\","
        + "      \"executionEngine\": \"duckdb\","
        + "      \"autoDownload\": false,"
        + "      \"ciks\": [\"" + APPLE_CIK + "\"],"
        + "      \"filingTypes\": [\"10-K\", \"10-Q\", \"8-K\"],"
        + "      \"startYear\": 2020,"
        + "      \"endYear\": 2025,"
        + "      \"directory\": \"" + R2_PARQUET_DIR + "\","
        + "      \"storageType\": \"s3\","
        + "      \"s3Config\": {"
        + "        \"accessKeyId\": \"" + R2_ACCESS_KEY + "\","
        + "        \"secretAccessKey\": \"" + R2_SECRET_KEY + "\","
        + "        \"endpoint\": \"" + R2_ENDPOINT + "\""
        + "      },"
        + "      \"storageConfig\": {"
        + "        \"accessKeyId\": \"" + R2_ACCESS_KEY + "\","
        + "        \"secretAccessKey\": \"" + R2_SECRET_KEY + "\","
        + "        \"endpoint\": \"" + R2_ENDPOINT + "\""
        + "      }"
        + "    }"
        + "  }]"
        + "}";

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", "inline:" + modelJson);
    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  @Test
  void testAppleFilings() throws Exception {
    LOGGER.info("=== Apple filings via govdata Calcite adapter (R2, parquet engine) ===");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      String sql =
          "SELECT cik, filing_type, ticker, filing_date, period_of_report"
          + " FROM \"sec\".\"filing_metadata\""
          + " WHERE cik = '" + APPLE_CIK + "'"
          + " ORDER BY filing_date";

      int rowCount = 0;
      try (ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          LOGGER.info("filing_type={} ticker={} filing_date={} period_of_report={}",
              rs.getString("filing_type"), rs.getString("ticker"),
              rs.getString("filing_date"), rs.getString("period_of_report"));
          rowCount++;
        }
      }
      LOGGER.info("Total Apple filings: {}", rowCount);
      assertTrue(rowCount > 0, "Expected Apple filings from R2");
    }
  }

  /**
   * Verifies DQ-patched fields for AAPL 2023 10-K/10-Q filings through the full
   * Calcite govdata model stack: filing_type normalized, ticker=AAPL, sic_code
   * populated, fiscal_year_end in --MM-DD format, period_of_report in YYYY-MM-DD.
   */
  @Test
  void testApple2023DqFields() throws Exception {
    LOGGER.info("=== Apple 2023 DQ-patched fields via Calcite govdata model ===");

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {

      String sql =
          "SELECT filing_type, ticker, sic_code, fiscal_year_end, period_of_report"
          + " FROM \"sec\".\"filing_metadata\""
          + " WHERE cik = '" + APPLE_CIK + "'"
          + " AND filing_date >= '2023-01-01' AND filing_date < '2024-01-01'"
          + " AND filing_type IN ('10-K', '10-Q')"
          + " ORDER BY period_of_report";

      LOGGER.info("Executing: {}", sql);

      List<String> filingTypes = new ArrayList<>();
      List<String> tickers = new ArrayList<>();
      List<String> sicCodes = new ArrayList<>();
      List<String> fiscalYearEnds = new ArrayList<>();
      List<String> periodsOfReport = new ArrayList<>();

      try (ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          String ft = rs.getString("filing_type");
          String tk = rs.getString("ticker");
          String sic = rs.getString("sic_code");
          String fye = rs.getString("fiscal_year_end");
          String por = rs.getString("period_of_report");
          LOGGER.info("filing_type={} ticker={} sic_code={} fiscal_year_end={} period_of_report={}",
              ft, tk, sic, fye, por);
          if (ft != null) filingTypes.add(ft);
          if (tk != null) tickers.add(tk);
          if (sic != null && !sic.isEmpty()) sicCodes.add(sic);
          if (fye != null) fiscalYearEnds.add(fye);
          if (por != null) periodsOfReport.add(por);
        }
      }

      LOGGER.info("2023 10-K/10-Q rows: {}", filingTypes.size());

      assertFalse(filingTypes.isEmpty(),
          "Expected 10-K or 10-Q filings for AAPL in 2023");

      for (String ft : filingTypes) {
        assertTrue(ft.equals("10-K") || ft.equals("10-Q"),
            "filing_type must be normalized, got: " + ft);
      }

      assertTrue(tickers.stream().anyMatch(t -> "AAPL".equalsIgnoreCase(t)),
          "At least one row must have ticker=AAPL; got: " + tickers);

      assertFalse(sicCodes.isEmpty(),
          "sic_code must be populated; got empty");

      assertTrue(fiscalYearEnds.stream().anyMatch(f -> f.startsWith("--")),
          "fiscal_year_end must be in --MM-DD format; got: " + fiscalYearEnds);

      assertTrue(periodsOfReport.stream().anyMatch(p -> p.matches("\\d{4}-\\d{2}-\\d{2}")),
          "period_of_report must be ISO YYYY-MM-DD; got: " + periodsOfReport);
    }
  }
}
