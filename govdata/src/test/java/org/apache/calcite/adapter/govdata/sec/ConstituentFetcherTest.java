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

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for constituent fetchers (DJIA, Russell, SP500, etc.).
 */
@Tag("integration")
public class ConstituentFetcherTest {

  static {
    // Enable fallback for tests since Wikipedia scraping is fragile
    System.setProperty("sec.fallback.enabled", "true");
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ConstituentFetcherTest.class);

  @Test
  void testDJIAConstituents() {
    LOGGER.info("Testing DJIA constituents...");
    List<String> djia = CikRegistry.resolveCiks("_DJIA_CONSTITUENTS");

    assertNotNull(djia, "DJIA list should not be null");
    LOGGER.info("DJIA count: {} (expected: 30)", djia.size());

    // DJIA should have exactly 30 companies
    assertTrue(djia.size() >= 28, "DJIA should have at least 28 companies, got: " + djia.size());
    assertTrue(djia.size() <= 32, "DJIA should have at most 32 companies, got: " + djia.size());

    // Check for some known DJIA companies
    assertTrue(djia.contains("0000320193"), "DJIA should contain Apple (0000320193)");
    assertTrue(djia.contains("0000789019"), "DJIA should contain Microsoft (0000789019)");

    LOGGER.info("DJIA test PASSED with {} companies", djia.size());
  }

  @Test
  void testRussell1000Constituents() {
    LOGGER.info("Testing Russell 1000 constituents...");
    List<String> russell1000 = CikRegistry.resolveCiks("_RUSSELL1000_CONSTITUENTS");

    assertNotNull(russell1000, "Russell 1000 list should not be null");
    LOGGER.info("Russell 1000 count: {} (expected: ~1000)", russell1000.size());

    // Russell 1000 should have roughly 1000 companies
    assertTrue(russell1000.size() >= 500,
        "Russell 1000 should have at least 500 companies, got: " + russell1000.size());
    assertTrue(russell1000.size() <= 1200,
        "Russell 1000 should have at most 1200 companies, got: " + russell1000.size());

    // Should contain major companies
    assertTrue(russell1000.contains("0000320193"), "Russell 1000 should contain Apple");
    assertTrue(russell1000.contains("0000789019"), "Russell 1000 should contain Microsoft");

    LOGGER.info("Russell 1000 test PASSED with {} companies", russell1000.size());
  }

  @Test
  void testSP500Constituents() {
    LOGGER.info("Testing S&P 500 constituents...");
    List<String> sp500 = CikRegistry.resolveCiks("_SP500_CONSTITUENTS");

    assertNotNull(sp500, "S&P 500 list should not be null");
    LOGGER.info("S&P 500 count: {} (expected: ~500)", sp500.size());

    // S&P 500 should have roughly 500 companies
    assertTrue(sp500.size() >= 400,
        "S&P 500 should have at least 400 companies, got: " + sp500.size());
    assertTrue(sp500.size() <= 550,
        "S&P 500 should have at most 550 companies, got: " + sp500.size());

    // Should contain major companies
    assertTrue(sp500.contains("0000320193"), "S&P 500 should contain Apple");
    assertTrue(sp500.contains("0000789019"), "S&P 500 should contain Microsoft");
    assertTrue(sp500.contains("0001018724"), "S&P 500 should contain Amazon");

    LOGGER.info("S&P 500 test PASSED with {} companies", sp500.size());
  }

  @Test
  void testAllEdgarFilers() {
    LOGGER.info("Testing All EDGAR Filers...");
    List<String> all = CikRegistry.resolveCiks("_ALL_EDGAR_FILERS");

    assertNotNull(all, "All EDGAR Filers list should not be null");
    LOGGER.info("All EDGAR Filers count: {} (expected: 7000+)", all.size());

    // Should have a substantial number of filers
    assertTrue(all.size() >= 5000,
        "All EDGAR Filers should have at least 5000 companies, got: " + all.size());

    LOGGER.info("All EDGAR Filers test PASSED with {} companies", all.size());
  }

  @Test
  void testCikFormat() {
    LOGGER.info("Verifying CIK format across all fetchers...");

    String[] markers = {
        "_DJIA_CONSTITUENTS",
        "_SP500_CONSTITUENTS",
        "_RUSSELL1000_CONSTITUENTS"
    };

    for (String marker : markers) {
      List<String> ciks = CikRegistry.resolveCiks(marker);
      if (!ciks.isEmpty()) {
        for (String cik : ciks) {
          assertEquals(10, cik.length(),
              marker + ": CIK should be 10 digits: " + cik);
          assertTrue(cik.matches("\\d{10}"),
              marker + ": CIK should be all digits: " + cik);
        }
        LOGGER.info("{}: All {} CIKs have valid format", marker, ciks.size());
      }
    }
  }
}
