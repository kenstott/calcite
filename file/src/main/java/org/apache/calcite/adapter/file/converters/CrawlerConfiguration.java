/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file.converters;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Configuration for HTML crawler behavior.
 */
public class CrawlerConfiguration {
  private boolean enabled = false;
  private int maxDepth = 0; // Default: no crawling beyond initial page
  private @Nullable Pattern linkPattern; // Regex for allowed links
  private Set<String> allowedFileExtensions = new HashSet<>();
  private Set<String> allowedDomains = new HashSet<>();
  private Duration requestDelay = Duration.ofSeconds(1);
  private int maxPages = 100;
  private boolean followExternalLinks = false;

  /** What a request without a configured agent names itself as. */
  public static final String DEFAULT_USER_AGENT =
      "Mozilla/5.0 (compatible; Apache Calcite/1.0; +https://calcite.apache.org)";

  // Which part of a page is read. A page's navigation, footers and reference lists hold links
  // of the same form as the ones in its text, so a URL pattern cannot tell them apart: only
  // where a link sits can. These apply before any link or table is read.
  private @Nullable String contentSelector; // CSS selector of the region read; null = the body
  private List<String> removeSelectors = new ArrayList<>(); // elements dropped from the page first
  // What a followed link is, by what the link element itself carries (its classes and
  // attributes), which a site's own markup states and a URL's wording only suggests.
  private @Nullable String linkSelector; // CSS selector a link must match to be followed; null = any
  private List<Pattern> linkExcludePatterns = new ArrayList<>(); // a link any of these finds is not followed
  private String tableSelector = "table"; // CSS selector of the HTML tables that become tables
  private String userAgent = DEFAULT_USER_AGENT;

  // Data file pattern configuration
  private @Nullable Pattern dataFilePattern; // Regex pattern for data files to include
  private @Nullable Pattern dataFileExcludePattern; // Regex pattern for data files to exclude

  // HTML table configuration
  private boolean generateTablesFromHtml = true; // Whether to extract HTML tables
  private int htmlTableMinRows = 1; // Minimum rows for HTML table extraction
  private int htmlTableMaxRows = Integer.MAX_VALUE; // Maximum rows to extract from HTML tables

  // Content size limits
  private long maxHtmlSize = 10L * 1024 * 1024; // 10MB for HTML pages
  private long maxDataFileSize = 100L * 1024 * 1024; // 100MB for data files
  private Map<String, Long> extensionSizeLimits = new HashMap<>();
  private boolean enforceContentLengthHeader = true;

  // Caching settings
  private Duration dataFileCacheTTL = Duration.ofHours(1);
  private Duration htmlCacheTTL = Duration.ofMinutes(30);
  private boolean honorHttpCacheHeaders = true;

  // Refresh settings
  private @Nullable Duration refreshInterval;

  public CrawlerConfiguration() {
    // Initialize default extension size limits
    extensionSizeLimits.put("html", 10L * 1024 * 1024);    // 10MB
    extensionSizeLimits.put("csv", 50L * 1024 * 1024);     // 50MB
    extensionSizeLimits.put("xlsx", 100L * 1024 * 1024);   // 100MB
    extensionSizeLimits.put("xls", 100L * 1024 * 1024);    // 100MB
    extensionSizeLimits.put("docx", 50L * 1024 * 1024);    // 50MB
    extensionSizeLimits.put("pptx", 100L * 1024 * 1024);   // 100MB
    extensionSizeLimits.put("pdf", 25L * 1024 * 1024);     // 25MB
    extensionSizeLimits.put("json", 20L * 1024 * 1024);    // 20MB
    extensionSizeLimits.put("parquet", 200L * 1024 * 1024); // 200MB

    // Default allowed file extensions for data files
    allowedFileExtensions.add("csv");
    allowedFileExtensions.add("xlsx");
    allowedFileExtensions.add("xls");
    allowedFileExtensions.add("json");
    allowedFileExtensions.add("tsv");
    allowedFileExtensions.add("parquet");
    allowedFileExtensions.add("docx");
    allowedFileExtensions.add("pptx");
  }

  /**
   * Creates a configuration from a map of options.
   */
  public static CrawlerConfiguration fromMap(Map<String, Object> options) {
    CrawlerConfiguration config = new CrawlerConfiguration();

    if (options.containsKey("enabled")) {
      config.setEnabled(Boolean.parseBoolean(options.get("enabled").toString()));
    }

    if (options.containsKey("maxDepth")) {
      config.setMaxDepth(Integer.parseInt(options.get("maxDepth").toString()));
    }

    if (options.containsKey("linkPattern")) {
      config.setLinkPattern(Pattern.compile(options.get("linkPattern").toString()));
    }

    if (options.containsKey("maxPages")) {
      config.setMaxPages(Integer.parseInt(options.get("maxPages").toString()));
    }

    if (options.containsKey("followExternalLinks")) {
      config.setFollowExternalLinks(Boolean.parseBoolean(options.get("followExternalLinks").toString()));
    }

    if (options.containsKey("maxHtmlSize")) {
      config.setMaxHtmlSize(parseSize(options.get("maxHtmlSize").toString()));
    }

    if (options.containsKey("maxDataFileSize")) {
      config.setMaxDataFileSize(parseSize(options.get("maxDataFileSize").toString()));
    }

    if (options.containsKey("dataFileCacheTTL")) {
      config.setDataFileCacheTTL(parseDuration(options.get("dataFileCacheTTL").toString()));
    }

    if (options.containsKey("refreshInterval")) {
      config.setRefreshInterval(parseDuration(options.get("refreshInterval").toString()));
    }

    if (options.containsKey("allowedDomains")) {
      Object domains = options.get("allowedDomains");
      if (domains instanceof String) {
        config.addAllowedDomain((String) domains);
      } else if (domains instanceof Iterable) {
        for (Object domain : (Iterable<?>) domains) {
          config.addAllowedDomain(domain.toString());
        }
      }
    }

    if (options.containsKey("dataFilePattern")) {
      config.setDataFilePattern(Pattern.compile(options.get("dataFilePattern").toString()));
    }

    if (options.containsKey("dataFileExcludePattern")) {
      config.setDataFileExcludePattern(Pattern.compile(options.get("dataFileExcludePattern").toString()));
    }

    if (options.containsKey("generateTablesFromHtml")) {
      config.setGenerateTablesFromHtml(Boolean.parseBoolean(options.get("generateTablesFromHtml").toString()));
    }

    if (options.containsKey("htmlTableMinRows")) {
      config.setHtmlTableMinRows(Integer.parseInt(options.get("htmlTableMinRows").toString()));
    }

    if (options.containsKey("htmlTableMaxRows")) {
      config.setHtmlTableMaxRows(Integer.parseInt(options.get("htmlTableMaxRows").toString()));
    }

    if (options.containsKey("contentSelector")) {
      config.setContentSelector(options.get("contentSelector").toString());
    }

    if (options.containsKey("removeSelectors")) {
      config.setRemoveSelectors(strings(options.get("removeSelectors")));
    }

    if (options.containsKey("linkSelector")) {
      config.setLinkSelector(options.get("linkSelector").toString());
    }

    if (options.containsKey("linkExcludePatterns")) {
      List<Pattern> patterns = new ArrayList<>();
      for (String pattern : strings(options.get("linkExcludePatterns"))) {
        patterns.add(Pattern.compile(pattern));
      }
      config.setLinkExcludePatterns(patterns);
    }

    if (options.containsKey("tableSelector")) {
      config.setTableSelector(options.get("tableSelector").toString());
    }

    if (options.containsKey("userAgent")) {
      config.setUserAgent(options.get("userAgent").toString());
    }

    if (options.containsKey("requestDelay")) {
      config.setRequestDelay(parseDuration(options.get("requestDelay").toString()));
    }

    if (options.containsKey("htmlCacheTTL")) {
      config.setHtmlCacheTTL(parseDuration(options.get("htmlCacheTTL").toString()));
    }

    if (options.containsKey("allowedFileExtensions")) {
      config.setAllowedFileExtensions(new HashSet<>(strings(options.get("allowedFileExtensions"))));
    }

    return config;
  }

  /** One string or a list of them, as a list. */
  private static List<String> strings(Object value) {
    List<String> list = new ArrayList<>();
    if (value instanceof Iterable) {
      for (Object item : (Iterable<?>) value) {
        list.add(item.toString());
      }
    } else {
      list.add(value.toString());
    }
    return list;
  }

  private static long parseSize(String sizeStr) {
    sizeStr = sizeStr.trim().toUpperCase();
    if (sizeStr.endsWith("KB")) {
      return Long.parseLong(sizeStr.substring(0, sizeStr.length() - 2)) * 1024;
    } else if (sizeStr.endsWith("MB")) {
      return Long.parseLong(sizeStr.substring(0, sizeStr.length() - 2)) * 1024 * 1024;
    } else if (sizeStr.endsWith("GB")) {
      return Long.parseLong(sizeStr.substring(0, sizeStr.length() - 2)) * 1024 * 1024 * 1024;
    }
    return Long.parseLong(sizeStr);
  }

  private static Duration parseDuration(String durationStr) {
    durationStr = durationStr.trim().toLowerCase();
    String[] parts = durationStr.split("\\s+");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid duration format: " + durationStr);
    }

    long value = Long.parseLong(parts[0]);
    String unit = parts[1];

    if (unit.startsWith("second")) {
      return Duration.ofSeconds(value);
    } else if (unit.startsWith("minute")) {
      return Duration.ofMinutes(value);
    } else if (unit.startsWith("hour")) {
      return Duration.ofHours(value);
    } else if (unit.startsWith("day")) {
      return Duration.ofDays(value);
    }

    throw new IllegalArgumentException("Unknown duration unit: " + unit);
  }

  // Getters and setters

  public @Nullable String getContentSelector() {
    return contentSelector;
  }

  public void setContentSelector(@Nullable String contentSelector) {
    this.contentSelector = contentSelector;
  }

  public List<String> getRemoveSelectors() {
    return removeSelectors;
  }

  public void setRemoveSelectors(List<String> removeSelectors) {
    this.removeSelectors = removeSelectors;
  }

  public @Nullable String getLinkSelector() {
    return linkSelector;
  }

  public void setLinkSelector(@Nullable String linkSelector) {
    this.linkSelector = linkSelector;
  }

  public List<Pattern> getLinkExcludePatterns() {
    return linkExcludePatterns;
  }

  public void setLinkExcludePatterns(List<Pattern> linkExcludePatterns) {
    this.linkExcludePatterns = linkExcludePatterns;
  }

  /** Whether a configured exclusion is found anywhere in {@code url}. */
  public boolean isLinkExcluded(String url) {
    for (Pattern pattern : linkExcludePatterns) {
      if (pattern.matcher(url).find()) {
        return true;
      }
    }
    return false;
  }

  public String getTableSelector() {
    return tableSelector;
  }

  public void setTableSelector(String tableSelector) {
    this.tableSelector = tableSelector;
  }

  public String getUserAgent() {
    return userAgent;
  }

  public void setUserAgent(String userAgent) {
    this.userAgent = userAgent;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public int getMaxDepth() {
    return maxDepth;
  }

  public void setMaxDepth(int maxDepth) {
    this.maxDepth = maxDepth;
  }

  public @Nullable Pattern getLinkPattern() {
    return linkPattern;
  }

  public void setLinkPattern(@Nullable Pattern linkPattern) {
    this.linkPattern = linkPattern;
  }

  public Set<String> getAllowedFileExtensions() {
    return allowedFileExtensions;
  }

  public void setAllowedFileExtensions(Set<String> allowedFileExtensions) {
    this.allowedFileExtensions = allowedFileExtensions;
  }

  public void addAllowedFileExtension(String extension) {
    this.allowedFileExtensions.add(extension.toLowerCase());
  }

  public Set<String> getAllowedDomains() {
    return allowedDomains;
  }

  public void addAllowedDomain(String domain) {
    this.allowedDomains.add(domain.toLowerCase());
  }

  public Duration getRequestDelay() {
    return requestDelay;
  }

  public void setRequestDelay(Duration requestDelay) {
    this.requestDelay = requestDelay;
  }

  public int getMaxPages() {
    return maxPages;
  }

  public void setMaxPages(int maxPages) {
    this.maxPages = maxPages;
  }

  public boolean isFollowExternalLinks() {
    return followExternalLinks;
  }

  public void setFollowExternalLinks(boolean followExternalLinks) {
    this.followExternalLinks = followExternalLinks;
  }

  public long getMaxHtmlSize() {
    return maxHtmlSize;
  }

  public void setMaxHtmlSize(long maxHtmlSize) {
    this.maxHtmlSize = maxHtmlSize;
  }

  public long getMaxDataFileSize() {
    return maxDataFileSize;
  }

  public void setMaxDataFileSize(long maxDataFileSize) {
    this.maxDataFileSize = maxDataFileSize;
  }

  public Long getSizeLimitForExtension(String extension) {
    return extensionSizeLimits.getOrDefault(extension.toLowerCase(), maxDataFileSize);
  }

  public void setSizeLimitForExtension(String extension, long sizeLimit) {
    this.extensionSizeLimits.put(extension.toLowerCase(), sizeLimit);
  }

  public Duration getDataFileCacheTTL() {
    return dataFileCacheTTL;
  }

  public void setDataFileCacheTTL(Duration dataFileCacheTTL) {
    this.dataFileCacheTTL = dataFileCacheTTL;
  }

  public Duration getHtmlCacheTTL() {
    return htmlCacheTTL;
  }

  public void setHtmlCacheTTL(Duration htmlCacheTTL) {
    this.htmlCacheTTL = htmlCacheTTL;
  }

  public boolean isHonorHttpCacheHeaders() {
    return honorHttpCacheHeaders;
  }

  public void setHonorHttpCacheHeaders(boolean honorHttpCacheHeaders) {
    this.honorHttpCacheHeaders = honorHttpCacheHeaders;
  }

  public @Nullable Duration getRefreshInterval() {
    return refreshInterval;
  }

  public void setRefreshInterval(@Nullable Duration refreshInterval) {
    this.refreshInterval = refreshInterval;
  }

  public boolean isEnforceContentLengthHeader() {
    return enforceContentLengthHeader;
  }

  public void setEnforceContentLengthHeader(boolean enforceContentLengthHeader) {
    this.enforceContentLengthHeader = enforceContentLengthHeader;
  }

  public @Nullable Pattern getDataFilePattern() {
    return dataFilePattern;
  }

  public void setDataFilePattern(@Nullable Pattern dataFilePattern) {
    this.dataFilePattern = dataFilePattern;
  }

  public @Nullable Pattern getDataFileExcludePattern() {
    return dataFileExcludePattern;
  }

  public void setDataFileExcludePattern(@Nullable Pattern dataFileExcludePattern) {
    this.dataFileExcludePattern = dataFileExcludePattern;
  }

  public boolean isGenerateTablesFromHtml() {
    return generateTablesFromHtml;
  }

  public void setGenerateTablesFromHtml(boolean generateTablesFromHtml) {
    this.generateTablesFromHtml = generateTablesFromHtml;
  }

  public int getHtmlTableMinRows() {
    return htmlTableMinRows;
  }

  public void setHtmlTableMinRows(int htmlTableMinRows) {
    this.htmlTableMinRows = htmlTableMinRows;
  }

  public int getHtmlTableMaxRows() {
    return htmlTableMaxRows;
  }

  public void setHtmlTableMaxRows(int htmlTableMaxRows) {
    this.htmlTableMaxRows = htmlTableMaxRows;
  }
}
