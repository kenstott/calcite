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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.etl.FilingIndexProvider;
import org.apache.calcite.adapter.file.etl.ProcessedDocumentTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Caches EDGAR quarterly full-index files to avoid per-CIK API calls on reruns.
 *
 * <p>EDGAR publishes quarterly full-index files at
 * {@code https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{n}/company.idx}
 * containing every filing for a quarter. One ~20MB download replaces thousands
 * of per-CIK API calls.
 *
 * <p>The index has accession numbers but NOT {@code primaryDocument} filenames,
 * so it acts as a <b>pre-filter</b>: if all accessions for a CIK are already
 * tracked, the per-CIK API call can be skipped entirely.
 */
public class EdgarFullIndexCache implements FilingIndexProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(EdgarFullIndexCache.class);

  private static final String FULL_INDEX_URL =
      "https://www.sec.gov/Archives/edgar/full-index/%d/QTR%d/company.idx";
  private static final String USER_AGENT =
      "Apache Calcite SEC Adapter (apache-calcite@apache.org)";

  /** Map from normalized CIK to list of index entries across all loaded quarters. */
  private final Map<String, List<IndexEntry>> entriesByCik =
      new HashMap<String, List<IndexEntry>>();

  /** Tracks which (year, quarter) combinations have a current (still-growing) quarter. */
  private final int currentYear;
  private final int currentQuarter;

  /**
   * Loads quarterly full-index files for the given year range.
   *
   * <p>For each completed quarter, checks S3 cache first, downloads from EDGAR if missing,
   * then parses the pipe-delimited index into an in-memory map.
   *
   * @param storageProvider Storage provider for S3 cache
   * @param cacheBasePath   Base path for cached index files (e.g. s3://bucket/sec/full-index)
   * @param startYear       First year to load
   * @param endYear         Last year to load
   */
  public EdgarFullIndexCache(StorageProvider storageProvider, String cacheBasePath,
      int startYear, int endYear) {
    LocalDate now = LocalDate.now();
    this.currentYear = now.getYear();
    this.currentQuarter = (now.getMonthValue() - 1) / 3 + 1;

    int totalFilings = 0;
    int quartersLoaded = 0;

    for (int year = startYear; year <= endYear; year++) {
      for (int quarter = 1; quarter <= 4; quarter++) {
        if (isFutureQuarter(year, quarter)) {
          continue;
        }

        boolean isCurrentQtr = isCurrentQuarter(year, quarter);
        try {
          String indexContent = isCurrentQtr
              ? downloadCurrentQuarterIndex(storageProvider, cacheBasePath, year, quarter)
              : loadOrDownloadIndex(storageProvider, cacheBasePath, year, quarter);
          if (indexContent != null) {
            int parsed = parseIndex(indexContent, year, quarter);
            totalFilings += parsed;
            quartersLoaded++;
          }
        } catch (IOException e) {
          LOGGER.warn("Failed to load index for {}Q{}: {}", year, quarter, e.getMessage());
        }
      }
    }

    LOGGER.info("Loaded {} filings from {} quarterly indexes", totalFilings, quartersLoaded);
  }

  @Override public CacheDecision checkCik(String cik, int year, List<String> filingTypes,
      ProcessedDocumentTracker tracker) {
    String normalizedCik = normalizeCik(cik);

    List<IndexEntry> entries = entriesByCik.get(normalizedCik);
    if (entries == null || entries.isEmpty()) {
      return CacheDecision.SKIP;
    }

    // Filter to requested year
    List<IndexEntry> yearEntries = new ArrayList<IndexEntry>();
    for (IndexEntry entry : entries) {
      if (entry.year == year) {
        yearEntries.add(entry);
      }
    }

    if (yearEntries.isEmpty()) {
      return CacheDecision.SKIP;
    }

    // Filter by filing types if specified
    if (filingTypes != null && !filingTypes.isEmpty()) {
      List<IndexEntry> filtered = new ArrayList<IndexEntry>();
      for (IndexEntry entry : yearEntries) {
        if (matchesFilingType(entry.formType, filingTypes)) {
          filtered.add(entry);
        }
      }
      yearEntries = filtered;
    }

    if (yearEntries.isEmpty()) {
      return CacheDecision.SKIP;
    }

    // Check tracker for all matching entries in a single bulk query
    if (tracker == null) {
      return CacheDecision.PROCESS;
    }

    List<String> accessions = new ArrayList<String>(yearEntries.size());
    List<String> formTypesList = new ArrayList<String>(yearEntries.size());
    for (IndexEntry entry : yearEntries) {
      accessions.add(entry.accession);
      formTypesList.add(entry.formType);
    }

    return tracker.areAllProcessed(normalizedCik, accessions, formTypesList)
        ? CacheDecision.SKIP : CacheDecision.PROCESS;
  }

  @Override public Set<String> getActiveCiks(int year, List<String> filingTypes) {
    Set<String> result = new HashSet<String>();
    for (Map.Entry<String, List<IndexEntry>> entry : entriesByCik.entrySet()) {
      for (IndexEntry ie : entry.getValue()) {
        if (ie.year == year && matchesFilingType(ie.formType, filingTypes)) {
          result.add(entry.getKey());
          break;
        }
      }
    }
    return result;
  }

  /** Check whether a form type matches the requested filing types. */
  private static boolean matchesFilingType(String formType, List<String> filingTypes) {
    if (filingTypes == null || filingTypes.isEmpty()) {
      return true;
    }
    for (String type : filingTypes) {
      if (formType.equalsIgnoreCase(type)
          || formType.startsWith(type + "/")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Downloads current quarter's index fresh each run (not cached — still growing).
   * Falls back to S3 cache if download fails.
   */
  private String downloadCurrentQuarterIndex(StorageProvider storageProvider,
      String cacheBasePath, int year, int quarter) throws IOException {
    String url = String.format(Locale.ROOT, FULL_INDEX_URL, year, quarter);
    LOGGER.info("Downloading current quarter index (fresh): {}", url);

    String content = downloadIndex(url);
    if (content != null) {
      // Write to S3 so it's available if next download fails
      String cachePath = storageProvider.resolvePath(cacheBasePath,
          String.format(Locale.ROOT, "full-index/%d/QTR%d/company.idx", year, quarter));
      storageProvider.writeFile(cachePath, content.getBytes(StandardCharsets.UTF_8));
      return content;
    }

    // Fall back to cached version if download fails
    String cachePath = storageProvider.resolvePath(cacheBasePath,
        String.format(Locale.ROOT, "full-index/%d/QTR%d/company.idx", year, quarter));
    if (storageProvider.exists(cachePath)) {
      LOGGER.info("Current quarter download failed, using cached index: {}", cachePath);
      return readFromStorage(storageProvider, cachePath);
    }
    return null;
  }

  /**
   * Loads an index from S3 cache, or downloads from EDGAR and caches it.
   */
  private String loadOrDownloadIndex(StorageProvider storageProvider, String cacheBasePath,
      int year, int quarter) throws IOException {
    String cachePath = storageProvider.resolvePath(cacheBasePath,
        String.format(Locale.ROOT, "full-index/%d/QTR%d/company.idx", year, quarter));

    // Check S3 cache first
    if (storageProvider.exists(cachePath)) {
      LOGGER.debug("Loading cached index: {}", cachePath);
      return readFromStorage(storageProvider, cachePath);
    }

    // Download from EDGAR
    String url = String.format(Locale.ROOT, FULL_INDEX_URL, year, quarter);
    LOGGER.info("Downloading EDGAR full-index: {}", url);

    String content = downloadIndex(url);
    if (content == null) {
      return null;
    }

    // Cache to S3
    storageProvider.writeFile(cachePath, content.getBytes(StandardCharsets.UTF_8));
    LOGGER.debug("Cached index to: {}", cachePath);

    return content;
  }

  private String readFromStorage(StorageProvider storageProvider, String path) throws IOException {
    try (InputStream is = storageProvider.openInputStream(path);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(is, StandardCharsets.UTF_8))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line).append('\n');
      }
      return sb.toString();
    }
  }

  private String downloadIndex(String url) throws IOException {
    // SEC rate limit: 10 requests/sec
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("User-Agent", USER_AGENT);
    conn.setRequestProperty("Accept", "text/plain");
    conn.setConnectTimeout(60000);
    conn.setReadTimeout(120000);

    int responseCode = conn.getResponseCode();
    if (responseCode == 404) {
      LOGGER.debug("Index not found (404): {}", url);
      return null;
    }
    if (responseCode != 200) {
      throw new IOException("HTTP " + responseCode + " from " + url);
    }

    try (InputStream is = conn.getInputStream();
         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buf = new byte[8192];
      int n;
      while ((n = is.read(buf)) != -1) {
        baos.write(buf, 0, n);
      }
      return baos.toString(StandardCharsets.UTF_8.name());
    }
  }

  /**
   * Parses a company.idx file into the in-memory map.
   *
   * <p>The file uses fixed-width columns:
   * <pre>
   * Company Name (col 0-61) | Form Type (col 62-73) | CIK (col 74-85) |
   * Date Filed (col 86-97) | File Name (col 98+)
   * </pre>
   *
   * <p>Header lines and the dashes separator are skipped. The filename field
   * contains a path like {@code edgar/data/320193/0000320193-25-000006.txt}
   * from which we extract the accession number.
   *
   * @return number of entries parsed
   */
  private int parseIndex(String content, int year, int quarter) {
    // Fixed-width column positions from the header line:
    // Company Name(0) Form Type(62) CIK(74) Date Filed(86) File Name(98)
    final int formTypeStart = 62;
    final int cikStart = 74;
    final int dateFiledStart = 86;
    final int fileNameStart = 98;

    int count = 0;
    boolean pastHeader = false;
    String[] lines = content.split("\n");

    for (String line : lines) {
      // Skip lines that are too short to contain data
      if (line.length() < fileNameStart + 10) {
        // Check if this is the dashes separator — marks end of header
        if (line.startsWith("---")) {
          pastHeader = true;
        }
        continue;
      }

      if (!pastHeader) {
        if (line.startsWith("---")) {
          pastHeader = true;
        }
        continue;
      }

      String companyName = line.substring(0, formTypeStart).trim();
      String formType = line.substring(formTypeStart, cikStart).trim();
      String cikField = line.substring(cikStart, dateFiledStart).trim();
      String filingDate = line.substring(dateFiledStart, fileNameStart).trim();
      String filename = line.substring(fileNameStart).trim();

      if (cikField.isEmpty() || formType.isEmpty()) {
        continue;
      }

      // Extract accession from filename: edgar/data/320193/0000320193-25-000006.txt
      String accession = extractAccession(filename);
      if (accession == null) {
        continue;
      }

      // Normalize CIK to 10-digit zero-padded
      String normalizedCik = normalizeCik(cikField);

      IndexEntry entry = new IndexEntry(companyName, formType, normalizedCik,
          filingDate, accession, year, quarter);

      List<IndexEntry> list = entriesByCik.get(normalizedCik);
      if (list == null) {
        list = new ArrayList<IndexEntry>();
        entriesByCik.put(normalizedCik, list);
      }
      list.add(entry);
      count++;
    }

    return count;
  }

  /**
   * Extracts accession number from index filename field.
   * Input: {@code edgar/data/320193/0000320193-25-000006.txt}
   * Output: {@code 0000320193-25-000006}
   */
  private static String extractAccession(String filename) {
    if (filename == null || filename.isEmpty()) {
      return null;
    }
    // Get the basename
    int lastSlash = filename.lastIndexOf('/');
    String basename = lastSlash >= 0 ? filename.substring(lastSlash + 1) : filename;
    // Remove .txt extension
    if (basename.endsWith(".txt")) {
      return basename.substring(0, basename.length() - 4);
    }
    return null;
  }

  private static String normalizeCik(String cik) {
    if (cik == null) {
      return "0000000000";
    }
    String stripped = cik.replaceFirst("^0+", "");
    if (stripped.isEmpty()) {
      stripped = "0";
    }
    // Pad to 10 digits
    while (stripped.length() < 10) {
      stripped = "0" + stripped;
    }
    return stripped;
  }

  private boolean isCurrentQuarter(int year, int quarter) {
    return year == currentYear && quarter == currentQuarter;
  }

  private boolean isFutureQuarter(int year, int quarter) {
    if (year > currentYear) {
      return true;
    }
    return year == currentYear && quarter > currentQuarter;
  }

  /** Parsed entry from a company.idx line. */
  static class IndexEntry {
    final String companyName;
    final String formType;
    final String cik;
    final String filingDate;
    final String accession;
    final int year;
    final int quarter;

    IndexEntry(String companyName, String formType, String cik,
        String filingDate, String accession, int year, int quarter) {
      this.companyName = companyName;
      this.formType = formType;
      this.cik = cik;
      this.filingDate = filingDate;
      this.accession = accession;
      this.year = year;
      this.quarter = quarter;
    }
  }
}
