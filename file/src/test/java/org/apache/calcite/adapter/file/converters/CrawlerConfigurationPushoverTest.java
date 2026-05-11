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
package org.apache.calcite.adapter.file.converters;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CrawlerConfiguration} to push converters coverage past 75%.
 */
@Tag("unit")
public class CrawlerConfigurationPushoverTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(CrawlerConfigurationPushoverTest.class);

  @Test
  @DisplayName("Default configuration has expected defaults")
  void testDefaultConfiguration() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    assertFalse(config.isEnabled());
    assertEquals(0, config.getMaxDepth());
    assertNull(config.getLinkPattern());
    assertNotNull(config.getAllowedFileExtensions());
    assertTrue(config.getAllowedFileExtensions().contains("csv"));
    assertTrue(config.getAllowedFileExtensions().contains("xlsx"));
    assertTrue(config.getAllowedFileExtensions().contains("json"));
    assertTrue(config.getAllowedFileExtensions().contains("parquet"));
    assertEquals(Duration.ofSeconds(1), config.getRequestDelay());
    assertEquals(100, config.getMaxPages());
    assertFalse(config.isFollowExternalLinks());
    assertEquals(10L * 1024 * 1024, config.getMaxHtmlSize());
    assertEquals(100L * 1024 * 1024, config.getMaxDataFileSize());
    assertTrue(config.isGenerateTablesFromHtml());
    assertEquals(1, config.getHtmlTableMinRows());
    assertEquals(Integer.MAX_VALUE, config.getHtmlTableMaxRows());
    assertTrue(config.isHonorHttpCacheHeaders());
    assertTrue(config.isEnforceContentLengthHeader());
    assertNull(config.getRefreshInterval());
    assertNull(config.getDataFilePattern());
    assertNull(config.getDataFileExcludePattern());
  }

  @Test
  @DisplayName("fromMap with enabled=true sets enabled")
  void testFromMapEnabled() {
    Map<String, Object> options = new HashMap<>();
    options.put("enabled", "true");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertTrue(config.isEnabled());
  }

  @Test
  @DisplayName("fromMap sets maxDepth")
  void testFromMapMaxDepth() {
    Map<String, Object> options = new HashMap<>();
    options.put("maxDepth", "3");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(3, config.getMaxDepth());
  }

  @Test
  @DisplayName("fromMap sets linkPattern")
  void testFromMapLinkPattern() {
    Map<String, Object> options = new HashMap<>();
    options.put("linkPattern", ".*\\.html$");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertNotNull(config.getLinkPattern());
  }

  @Test
  @DisplayName("fromMap sets maxPages")
  void testFromMapMaxPages() {
    Map<String, Object> options = new HashMap<>();
    options.put("maxPages", "50");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(50, config.getMaxPages());
  }

  @Test
  @DisplayName("fromMap sets followExternalLinks")
  void testFromMapFollowExternalLinks() {
    Map<String, Object> options = new HashMap<>();
    options.put("followExternalLinks", "true");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertTrue(config.isFollowExternalLinks());
  }

  @Test
  @DisplayName("fromMap parses maxHtmlSize with MB suffix")
  void testFromMapMaxHtmlSizeMB() {
    Map<String, Object> options = new HashMap<>();
    options.put("maxHtmlSize", "5MB");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(5L * 1024 * 1024, config.getMaxHtmlSize());
  }

  @Test
  @DisplayName("fromMap parses maxDataFileSize with KB suffix")
  void testFromMapMaxDataFileSizeKB() {
    Map<String, Object> options = new HashMap<>();
    options.put("maxDataFileSize", "500KB");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(500L * 1024, config.getMaxDataFileSize());
  }

  @Test
  @DisplayName("fromMap parses dataFileCacheTTL with minutes")
  void testFromMapDataFileCacheTTL() {
    Map<String, Object> options = new HashMap<>();
    options.put("dataFileCacheTTL", "30 minutes");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(Duration.ofMinutes(30), config.getDataFileCacheTTL());
  }

  @Test
  @DisplayName("fromMap parses refreshInterval with hours")
  void testFromMapRefreshInterval() {
    Map<String, Object> options = new HashMap<>();
    options.put("refreshInterval", "2 hours");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(Duration.ofHours(2), config.getRefreshInterval());
  }

  @Test
  @DisplayName("fromMap sets allowedDomains from string")
  void testFromMapAllowedDomainsString() {
    Map<String, Object> options = new HashMap<>();
    options.put("allowedDomains", "example.com");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertTrue(config.getAllowedDomains().contains("example.com"));
  }

  @Test
  @DisplayName("fromMap sets allowedDomains from list")
  void testFromMapAllowedDomainsList() {
    Map<String, Object> options = new HashMap<>();
    List<String> domains = new ArrayList<>(Arrays.asList("example.com", "test.org"));
    options.put("allowedDomains", domains);
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertTrue(config.getAllowedDomains().contains("example.com"));
    assertTrue(config.getAllowedDomains().contains("test.org"));
  }

  @Test
  @DisplayName("fromMap sets dataFilePattern")
  void testFromMapDataFilePattern() {
    Map<String, Object> options = new HashMap<>();
    options.put("dataFilePattern", ".*\\.csv$");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertNotNull(config.getDataFilePattern());
  }

  @Test
  @DisplayName("fromMap sets dataFileExcludePattern")
  void testFromMapDataFileExcludePattern() {
    Map<String, Object> options = new HashMap<>();
    options.put("dataFileExcludePattern", ".*temp.*");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertNotNull(config.getDataFileExcludePattern());
  }

  @Test
  @DisplayName("fromMap sets generateTablesFromHtml")
  void testFromMapGenerateTablesFromHtml() {
    Map<String, Object> options = new HashMap<>();
    options.put("generateTablesFromHtml", "false");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertFalse(config.isGenerateTablesFromHtml());
  }

  @Test
  @DisplayName("fromMap sets htmlTableMinRows and htmlTableMaxRows")
  void testFromMapHtmlTableRowLimits() {
    Map<String, Object> options = new HashMap<>();
    options.put("htmlTableMinRows", "5");
    options.put("htmlTableMaxRows", "1000");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(5, config.getHtmlTableMinRows());
    assertEquals(1000, config.getHtmlTableMaxRows());
  }

  // --- Setter tests ---

  @Test
  @DisplayName("setters update configuration values")
  void testSetters() {
    CrawlerConfiguration config = new CrawlerConfiguration();

    config.setEnabled(true);
    assertTrue(config.isEnabled());

    config.setMaxDepth(5);
    assertEquals(5, config.getMaxDepth());

    config.setLinkPattern(Pattern.compile(".*"));
    assertNotNull(config.getLinkPattern());

    config.setRequestDelay(Duration.ofSeconds(2));
    assertEquals(Duration.ofSeconds(2), config.getRequestDelay());

    config.setMaxPages(200);
    assertEquals(200, config.getMaxPages());

    config.setFollowExternalLinks(true);
    assertTrue(config.isFollowExternalLinks());

    config.setHonorHttpCacheHeaders(false);
    assertFalse(config.isHonorHttpCacheHeaders());

    config.setEnforceContentLengthHeader(false);
    assertFalse(config.isEnforceContentLengthHeader());

    config.setHtmlCacheTTL(Duration.ofMinutes(15));
    assertEquals(Duration.ofMinutes(15), config.getHtmlCacheTTL());
  }

  @Test
  @DisplayName("addAllowedFileExtension adds to set")
  void testAddAllowedFileExtension() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.addAllowedFileExtension("PDF");
    assertTrue(config.getAllowedFileExtensions().contains("pdf"),
        "Extension should be stored lowercase");
  }

  @Test
  @DisplayName("addAllowedDomain adds to set")
  void testAddAllowedDomain() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.addAllowedDomain("EXAMPLE.COM");
    assertTrue(config.getAllowedDomains().contains("example.com"),
        "Domain should be stored lowercase");
  }

  @Test
  @DisplayName("getSizeLimitForExtension returns configured limit")
  void testGetSizeLimitForExtension() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    Long csvLimit = config.getSizeLimitForExtension("csv");
    assertNotNull(csvLimit);
    assertEquals(50L * 1024 * 1024, csvLimit.longValue());
  }

  @Test
  @DisplayName("getSizeLimitForExtension returns default for unknown extension")
  void testGetSizeLimitForUnknownExtension() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    Long unknownLimit = config.getSizeLimitForExtension("xyz");
    assertEquals(config.getMaxDataFileSize(), unknownLimit.longValue());
  }

  @Test
  @DisplayName("setSizeLimitForExtension updates limit")
  void testSetSizeLimitForExtension() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setSizeLimitForExtension("custom", 42L * 1024);
    assertEquals(42L * 1024, config.getSizeLimitForExtension("custom").longValue());
  }

  // --- Duration parsing via fromMap ---

  @Test
  @DisplayName("fromMap parses seconds duration")
  void testFromMapDurationSeconds() {
    Map<String, Object> options = new HashMap<>();
    options.put("refreshInterval", "30 seconds");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(Duration.ofSeconds(30), config.getRefreshInterval());
  }

  @Test
  @DisplayName("fromMap parses days duration")
  void testFromMapDurationDays() {
    Map<String, Object> options = new HashMap<>();
    options.put("refreshInterval", "1 days");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(Duration.ofDays(1), config.getRefreshInterval());
  }

  // --- Size parsing via fromMap ---

  @Test
  @DisplayName("fromMap parses GB size suffix")
  void testFromMapSizeGB() {
    Map<String, Object> options = new HashMap<>();
    options.put("maxDataFileSize", "1GB");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(1L * 1024 * 1024 * 1024, config.getMaxDataFileSize());
  }

  @Test
  @DisplayName("fromMap parses plain number as bytes")
  void testFromMapSizePlainNumber() {
    Map<String, Object> options = new HashMap<>();
    options.put("maxHtmlSize", "1024");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(1024L, config.getMaxHtmlSize());
  }
}
