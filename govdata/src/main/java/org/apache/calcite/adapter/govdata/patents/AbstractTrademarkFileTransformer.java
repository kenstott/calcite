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
package org.apache.calcite.adapter.govdata.patents;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Faithful, per-file landing of one USPTO trademark bulk CSV (TRCFECO2) into its own table — no
 * cross-file join in ETL (the join lives in the {@code trademark_applications} view instead).
 *
 * <p>Each snapshot year {@code Y} of TRCFECO2 is a relational set of CSVs keyed by {@code serial_no}.
 * A concrete subclass names one file via {@link #fileName()} (e.g. {@code owner}); this base:
 * <ol>
 *   <li>downloads {@code case_file.csv.zip} (cached) and collects the {@code serial_no}s whose
 *       {@code filing_dt} falls in year {@code Y} — the set of applications this snapshot contributes;</li>
 *   <li>downloads the subclass's file (cached) and streams EVERY column of it verbatim, scoping rows
 *       to that year: {@code case_file} by {@code filing_dt} year, every child file by {@code serial_no}
 *       membership;</li>
 *   <li>adds an integer {@code year} partition column.</li>
 * </ol>
 * Reuses {@link AbstractPatentsTransformer} for credentials, publish-gating, zip extraction and caching.
 */
public abstract class AbstractTrademarkFileTransformer extends AbstractPatentsTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTrademarkFileTransformer.class);

  /** USPTO Open Data Portal TRCFECO2 (trademark case-file economics) bulk download base. */
  private static final String TM_API_BASE =
      "https://api.uspto.gov/api/v1/datasets/products/files/TRCFECO2/";

  /** CSV file basename (no extension) this transformer lands, e.g. {@code "case_file"}, {@code "owner"}. */
  protected abstract String fileName();

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String yearStr = getEffectiveYear(context);
    if (yearStr == null || yearStr.isEmpty()) {
      LOGGER.warn("Trademark {}: missing year dimension", fileName());
      return Collections.emptyIterator();
    }
    final String apiKey = usptoApiKey();
    if (apiKey == null || apiKey.isEmpty()) {
      LOGGER.warn("Trademark {}: USPTO_API_KEY not set — skipping. "
          + "Register at https://data.uspto.gov/apis/getting-started", fileName());
      return Collections.emptyIterator();
    }
    final String tmBase = TM_API_BASE + yearStr + "/";
    // Catalog gate: only snapshots USPTO has actually published (2011, 2021, 2022, 2023, ...).
    if (!isPublished(tmBase + "case_file.csv.zip")) {
      LOGGER.info("Trademark {}: snapshot {} not in ODP catalog yet — skipping (auto-ingests once "
          + "USPTO publishes it).", fileName(), yearStr);
      return Collections.emptyIterator();
    }

    // Every file scopes to the applications filed in this snapshot year, identified from case_file.
    final String caseFile = downloadTmCsv(tmBase + "case_file.csv.zip",
        "tm_case_file_" + yearStr + ".csv", apiKey);
    final boolean isCaseFile = "case_file".equals(fileName());
    final Set<String> serialNos = isCaseFile ? null : readSerialNosForYear(caseFile, yearStr);

    final String targetPath = isCaseFile ? caseFile
        : downloadTmCsv(tmBase + fileName() + ".csv.zip", "tm_" + fileName() + "_" + yearStr + ".csv", apiKey);

    return streamAllColumns(targetPath, yearStr, serialNos, isCaseFile);
  }

  /** Downloads+extracts a TRCFECO2 file to the patents cache, reusing a valid cached copy. */
  private String downloadTmCsv(String url, String cacheFileName, String apiKey) throws IOException {
    String dest = cacheFile(cacheFileName);
    if (isCacheValid(dest)) {
      LOGGER.debug("Trademark cache hit: {}", cacheFileName);
      return dest;
    }
    LOGGER.info("Trademark downloading: {}", url);
    Map<String, String> headers = new LinkedHashMap<String, String>();
    headers.put("X-Api-Key", apiKey);
    extractZipEntryToFile(url, headers, dest, ".csv");
    return dest;
  }

  /** serial_nos whose {@code filing_dt} year equals {@code yearStr} (applications filed that year). */
  private Set<String> readSerialNosForYear(String caseFilePath, String yearStr) throws IOException {
    Set<String> result = new HashSet<String>();
    BufferedReader reader = openCsv(caseFilePath);
    try {
      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        return result;
      }
      Map<String, Integer> hdr = indexHeader(headerLine);
      Integer serialIdx = hdr.get("serial_no");
      Integer filingIdx = hdr.get("filing_dt");
      if (serialIdx == null || filingIdx == null) {
        return result;
      }
      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] parts = line.split(",", -1);
        String fd = field(parts, filingIdx);
        if (fd != null && fd.length() >= 4 && yearStr.equals(fd.substring(0, 4))) {
          String s = field(parts, serialIdx);
          if (s != null) {
            result.add(s);
          }
        }
      }
    } finally {
      closeQuietly(reader);
    }
    return result;
  }

  /** Streams every column of {@code path} verbatim as strings + an int {@code year}, scoped to the year. */
  private Iterator<Map<String, Object>> streamAllColumns(final String path, final String yearStr,
      final Set<String> serialNos, final boolean isCaseFile) throws IOException {
    final BufferedReader reader = openCsv(path);
    final String headerLine = CsvRecordReader.readRecord(reader);
    if (headerLine == null) {
      closeQuietly(reader);
      return Collections.emptyIterator();
    }
    final String[] cols = splitHeader(headerLine);
    final Map<String, Integer> hdr = indexHeader(headerLine);
    final Integer serialIdx = hdr.get("serial_no");
    final Integer filingIdx = hdr.get("filing_dt");
    final int yearVal = Integer.parseInt(yearStr);

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> pending;
      { advance(); }

      private void advance() {
        pending = null;
        try {
          String line;
          while ((line = CsvRecordReader.readRecord(reader)) != null) {
            if (line.isEmpty()) {
              continue;
            }
            String[] parts = line.split(",", -1);
            if (isCaseFile) {
              String fd = filingIdx != null ? field(parts, filingIdx) : null;
              if (fd == null || fd.length() < 4 || !yearStr.equals(fd.substring(0, 4))) {
                continue;
              }
            } else {
              String s = serialIdx != null ? field(parts, serialIdx) : null;
              if (s == null || !serialNos.contains(s)) {
                continue;
              }
            }
            Map<String, Object> row = new HashMap<String, Object>();
            for (int i = 0; i < cols.length; i++) {
              row.put(cols[i], i < parts.length ? field(parts, i) : null);
            }
            row.put("year", Integer.valueOf(yearVal));
            pending = row;
            return;
          }
          closeQuietly(reader);
        } catch (IOException e) {
          closeQuietly(reader);
          throw new UncheckedIOException("Trademark file read failed: " + path, e);
        }
      }

      @Override public boolean hasNext() {
        return pending != null;
      }

      @Override public Map<String, Object> next() {
        Map<String, Object> row = pending;
        advance();
        return row;
      }
    };
  }

  // ── small CSV helpers ──────────────────────────────────────────────────────

  private BufferedReader openCsv(String path) throws IOException {
    return new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(path), StandardCharsets.UTF_8));
  }

  private static Map<String, Integer> indexHeader(String headerLine) {
    String[] raw = headerLine.split(",", -1);
    Map<String, Integer> hdr = new HashMap<String, Integer>();
    for (int i = 0; i < raw.length; i++) {
      hdr.put(raw[i].trim().replace("\"", "").toLowerCase(), Integer.valueOf(i));
    }
    return hdr;
  }

  private static String[] splitHeader(String headerLine) {
    String[] raw = headerLine.split(",", -1);
    String[] cols = new String[raw.length];
    for (int i = 0; i < raw.length; i++) {
      cols[i] = raw[i].trim().replace("\"", "").toLowerCase();
    }
    return cols;
  }

  private static String field(String[] parts, int idx) {
    if (idx < 0 || idx >= parts.length) {
      return null;
    }
    String v = parts[idx].trim().replace("\"", "");
    return v.isEmpty() ? null : v;
  }

  private static void closeQuietly(BufferedReader reader) {
    try {
      reader.close();
    } catch (IOException e) {
      LOGGER.warn("Failed to close trademark CSV reader: {}", e.getMessage());
    }
  }
}
