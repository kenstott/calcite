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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link CrawlerConfiguration}.
 */
@Tag("unit")
class CrawlerConfigurationTest {

  @Test void testDefaultValues() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    assertFalse(config.isEnabled());
    assertEquals(0, config.getMaxDepth());
    assertNull(config.getLinkPattern());
    assertEquals(100, config.getMaxPages());
    assertFalse(config.isFollowExternalLinks());
    assertEquals(Duration.ofSeconds(1), config.getRequestDelay());
    assertEquals(10L * 1024 * 1024, config.getMaxHtmlSize());
    assertEquals(100L * 1024 * 1024, config.getMaxDataFileSize());
    assertTrue(config.isHonorHttpCacheHeaders());
    assertTrue(config.isEnforceContentLengthHeader());
    assertTrue(config.isGenerateTablesFromHtml());
    assertEquals(1, config.getHtmlTableMinRows());
    assertEquals(Integer.MAX_VALUE, config.getHtmlTableMaxRows());
    assertNull(config.getRefreshInterval());
    assertNull(config.getDataFilePattern());
    assertNull(config.getDataFileExcludePattern());
  }

  @Test void testDefaultAllowedFileExtensions() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    assertTrue(config.getAllowedFileExtensions().contains("csv"));
    assertTrue(config.getAllowedFileExtensions().contains("xlsx"));
    assertTrue(config.getAllowedFileExtensions().contains("xls"));
    assertTrue(config.getAllowedFileExtensions().contains("json"));
    assertTrue(config.getAllowedFileExtensions().contains("tsv"));
    assertTrue(config.getAllowedFileExtensions().contains("parquet"));
    assertTrue(config.getAllowedFileExtensions().contains("docx"));
    assertTrue(config.getAllowedFileExtensions().contains("pptx"));
  }

  @Test void testDefaultExtensionSizeLimits() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    assertEquals(10L * 1024 * 1024, (long) config.getSizeLimitForExtension("html"));
    assertEquals(50L * 1024 * 1024, (long) config.getSizeLimitForExtension("csv"));
    assertEquals(100L * 1024 * 1024, (long) config.getSizeLimitForExtension("xlsx"));
    assertEquals(200L * 1024 * 1024, (long) config.getSizeLimitForExtension("parquet"));
  }

  @Test void testSizeLimitForUnknownExtension() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    // Unknown extension returns maxDataFileSize
    assertEquals(config.getMaxDataFileSize(), (long) config.getSizeLimitForExtension("unknown"));
  }

  @Test void testSettersAndGetters() {
    CrawlerConfiguration config = new CrawlerConfiguration();

    config.setEnabled(true);
    assertTrue(config.isEnabled());

    config.setMaxDepth(5);
    assertEquals(5, config.getMaxDepth());

    config.setMaxPages(50);
    assertEquals(50, config.getMaxPages());

    config.setFollowExternalLinks(true);
    assertTrue(config.isFollowExternalLinks());

    config.setMaxHtmlSize(5 * 1024 * 1024);
    assertEquals(5 * 1024 * 1024, config.getMaxHtmlSize());

    config.setMaxDataFileSize(200 * 1024 * 1024);
    assertEquals(200 * 1024 * 1024, config.getMaxDataFileSize());

    config.setHonorHttpCacheHeaders(false);
    assertFalse(config.isHonorHttpCacheHeaders());

    config.setEnforceContentLengthHeader(false);
    assertFalse(config.isEnforceContentLengthHeader());

    config.setGenerateTablesFromHtml(false);
    assertFalse(config.isGenerateTablesFromHtml());

    config.setHtmlTableMinRows(5);
    assertEquals(5, config.getHtmlTableMinRows());

    config.setHtmlTableMaxRows(1000);
    assertEquals(1000, config.getHtmlTableMaxRows());
  }

  @Test void testSetLinkPattern() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    Pattern pattern = Pattern.compile(".*\\.html$");
    config.setLinkPattern(pattern);
    assertEquals(pattern, config.getLinkPattern());
  }

  @Test void testSetRequestDelay() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setRequestDelay(Duration.ofMillis(500));
    assertEquals(Duration.ofMillis(500), config.getRequestDelay());
  }

  @Test void testCacheTTLSettings() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    assertEquals(Duration.ofHours(1), config.getDataFileCacheTTL());
    assertEquals(Duration.ofMinutes(30), config.getHtmlCacheTTL());

    config.setDataFileCacheTTL(Duration.ofMinutes(15));
    assertEquals(Duration.ofMinutes(15), config.getDataFileCacheTTL());

    config.setHtmlCacheTTL(Duration.ofMinutes(5));
    assertEquals(Duration.ofMinutes(5), config.getHtmlCacheTTL());
  }

  @Test void testRefreshInterval() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    assertNull(config.getRefreshInterval());

    config.setRefreshInterval(Duration.ofHours(2));
    assertEquals(Duration.ofHours(2), config.getRefreshInterval());

    config.setRefreshInterval(null);
    assertNull(config.getRefreshInterval());
  }

  @Test void testAddAllowedDomain() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.addAllowedDomain("Example.COM");
    assertTrue(config.getAllowedDomains().contains("example.com"));
  }

  @Test void testAddAllowedFileExtension() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.addAllowedFileExtension("MD");
    assertTrue(config.getAllowedFileExtensions().contains("md"));
  }

  @Test void testSetSizeLimitForExtension() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setSizeLimitForExtension("PDF", 50L * 1024 * 1024);
    assertEquals(50L * 1024 * 1024, (long) config.getSizeLimitForExtension("pdf"));
  }

  @Test void testDataFilePatterns() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    Pattern include = Pattern.compile(".*data.*");
    Pattern exclude = Pattern.compile(".*temp.*");

    config.setDataFilePattern(include);
    config.setDataFileExcludePattern(exclude);

    assertEquals(include, config.getDataFilePattern());
    assertEquals(exclude, config.getDataFileExcludePattern());
  }

  // --- fromMap ---

  @Test void testFromMapEmpty() {
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(new HashMap<String, Object>());
    assertFalse(config.isEnabled());
    assertEquals(0, config.getMaxDepth());
  }

  @Test void testFromMapEnabled() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("enabled", "true");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertTrue(config.isEnabled());
  }

  @Test void testFromMapMaxDepth() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("maxDepth", "3");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(3, config.getMaxDepth());
  }

  @Test void testFromMapLinkPattern() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("linkPattern", ".*\\.html$");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertNotNull(config.getLinkPattern());
  }

  @Test void testFromMapMaxPages() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("maxPages", "200");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(200, config.getMaxPages());
  }

  @Test void testFromMapFollowExternalLinks() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("followExternalLinks", "true");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertTrue(config.isFollowExternalLinks());
  }

  @Test void testFromMapMaxHtmlSizeKB() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("maxHtmlSize", "512KB");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(512 * 1024, config.getMaxHtmlSize());
  }

  @Test void testFromMapMaxHtmlSizeMB() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("maxHtmlSize", "5MB");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(5L * 1024 * 1024, config.getMaxHtmlSize());
  }

  @Test void testFromMapMaxDataFileSize() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("maxDataFileSize", "1GB");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(1L * 1024 * 1024 * 1024, config.getMaxDataFileSize());
  }

  @Test void testFromMapDataFileCacheTTL() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("dataFileCacheTTL", "30 minutes");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(Duration.ofMinutes(30), config.getDataFileCacheTTL());
  }

  @Test void testFromMapRefreshInterval() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("refreshInterval", "2 hours");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(Duration.ofHours(2), config.getRefreshInterval());
  }

  @Test void testFromMapAllowedDomainsSingle() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("allowedDomains", "example.com");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertTrue(config.getAllowedDomains().contains("example.com"));
  }

  @Test void testFromMapAllowedDomainsList() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("allowedDomains", Arrays.asList("example.com", "test.com"));
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertTrue(config.getAllowedDomains().contains("example.com"));
    assertTrue(config.getAllowedDomains().contains("test.com"));
  }

  @Test void testFromMapDataFilePattern() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("dataFilePattern", ".*\\.csv$");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertNotNull(config.getDataFilePattern());
  }

  @Test void testFromMapDataFileExcludePattern() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("dataFileExcludePattern", ".*temp.*");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertNotNull(config.getDataFileExcludePattern());
  }

  @Test void testFromMapGenerateTablesFromHtml() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("generateTablesFromHtml", "false");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertFalse(config.isGenerateTablesFromHtml());
  }

  @Test void testFromMapHtmlTableMinRows() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("htmlTableMinRows", "5");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(5, config.getHtmlTableMinRows());
  }

  @Test void testFromMapHtmlTableMaxRows() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("htmlTableMaxRows", "500");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(500, config.getHtmlTableMaxRows());
  }

  @Test void testFromMapMaxHtmlSizeRawBytes() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("maxHtmlSize", "1048576");
    CrawlerConfiguration config = CrawlerConfiguration.fromMap(options);
    assertEquals(1048576L, config.getMaxHtmlSize());
  }
}
