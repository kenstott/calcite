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
 * Transforms USPTO trademark bulk CSV into trademark_applications rows.
 *
 * <p>Downloads and caches 4 CSV files from the USPTO trademark bulk dataset,
 * all joined on serial_no:
 * <ul>
 *   <li>case_file.csv — core application attributes (79 columns)</li>
 *   <li>owner.csv — applicant identity</li>
 *   <li>intl_class.csv — international class codes</li>
 *   <li>statement.csv — goods and services text</li>
 * </ul>
 *
 * <p>Eager setup: collects serial_nos for the target year, loads auxiliary lookups.
 * Returns a lazy iterator streaming case_file.csv pass 2, emitting one row per application.
 * No intermediate StringWriter — memory is O(lookup_size + chunk_size).
 */
public class TrademarkApplicationsTransformer extends AbstractPatentsTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TrademarkApplicationsTransformer.class);

  /**
   * USPTO Open Data Portal API endpoint for trademark casefile economics bulk downloads.
   * Requires X-Api-Key header. Register at https://data.uspto.gov/apis/getting-started
   * Only snapshots for years 2011, 2021, 2022, 2023 are currently published.
   * Each snapshot {year} contains applications filed in {year-1}.
   */
  private static final String TM_API_BASE =
      "https://api.uspto.gov/api/v1/datasets/products/files/TRCFECO2/";

  private static final String USPTO_API_KEY_ENV = "USPTO_API_KEY";

  @Override
  public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    // Use the framework's effective_year (dimension year − dataLag) as the data year, exactly
    // like the other patents tables (e.g. grants). The TRCFECO2 snapshot {Y} contains
    // applications filed in year {Y}, so the snapshot to download IS the effective year — no
    // extra hand-rolled offset (the old "+1" duplicated, and inverted, the dataLag concept).
    final String yearStr = getEffectiveYear(context);
    if (yearStr == null || yearStr.isEmpty()) {
      LOGGER.warn("TrademarkApplications: missing year dimension");
      return Collections.emptyIterator();
    }

    final String snapshotYear = yearStr;
    String apiKeyRaw = System.getenv(USPTO_API_KEY_ENV);
    if (apiKeyRaw == null || apiKeyRaw.isEmpty()) {
      apiKeyRaw = System.getProperty(USPTO_API_KEY_ENV);
    }
    final String apiKey = apiKeyRaw;
    if (apiKey == null || apiKey.isEmpty()) {
      LOGGER.warn("TrademarkApplications: {} not set — skipping trademark download. "
          + "Register at https://data.uspto.gov/apis/getting-started", USPTO_API_KEY_ENV);
      return Collections.emptyIterator();
    }
    final String tmBase = TM_API_BASE + snapshotYear + "/";
    // Catalog gate: only attempt snapshots USPTO has actually published. The ODP product catalog
    // lists each available file's download URI; if this snapshot year is not in it (e.g. /2024/,
    // /2025/ which do not yet exist), skip with no download — no 404, no wasted request against the
    // 20/file/year cap. When USPTO publishes a newer snapshot it appears in the catalog and is
    // picked up automatically — no schema/dataLag change needed.
    if (!isPublished(tmBase + "case_file.csv.zip")) {
      LOGGER.info("TrademarkApplications: snapshot {} is not in the ODP catalog yet — skipping with "
          + "no download (will be ingested automatically once USPTO publishes it).", snapshotYear);
      return Collections.emptyIterator();
    }
    final String caseFile;
    final String ownerFile;
    final String intlClassFile;
    final String statementFile;
    try {
      caseFile = downloadAndCacheCsv(tmBase + "case_file.csv.zip",
          "tm_case_file_" + snapshotYear + ".csv", apiKey);
      ownerFile = downloadAndCacheCsv(tmBase + "owner.csv.zip",
          "tm_owner_" + snapshotYear + ".csv", apiKey);
      intlClassFile = downloadAndCacheCsv(tmBase + "intl_class.csv.zip",
          "tm_intl_class_" + snapshotYear + ".csv", apiKey);
      statementFile = downloadAndCacheCsv(tmBase + "statement.csv.zip",
          "tm_statement_" + snapshotYear + ".csv", apiKey);
    } catch (IOException e) {
      LOGGER.warn("TrademarkApplications: download failed for snapshot year {} — {}. "
          + "Data may not be published yet (available years: 2011, 2021, 2022, 2023).",
          snapshotYear, e.getMessage());
      return Collections.emptyIterator();
    }

    final Set<String> serialNos =
        readCsvYearColumnKeys(caseFile, "filing_dt", yearStr, "serial_no");
    LOGGER.info("TrademarkApplications: {} serial_nos for year {}", serialNos.size(), yearStr);

    final Map<String, Map<String, String>> owners = readCsvAsLookupForKeys(
        ownerFile, "serial_no", serialNos,
        "own_name", "own_type_cd", "own_addr_state_cd", "own_addr_country_cd");
    final Map<String, Set<String>> intlClasses =
        readCsvMultiValueForKeys(intlClassFile, "serial_no", serialNos, "intl_class_cd");
    final Map<String, String> statements =
        readCsvSingleValueForKeys(statementFile, "serial_no", serialNos, "statement_text");

    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(caseFile), StandardCharsets.UTF_8));
    String headerLine = CsvRecordReader.readRecord(reader);
    if (headerLine == null) {
      reader.close();
      return Collections.emptyIterator();
    }
    String[] rawHeaders = headerLine.split(",", -1);
    final Map<String, Integer> hdr = new HashMap<>();
    for (int i = 0; i < rawHeaders.length; i++) {
      hdr.put(rawHeaders[i].trim().replace("\"", "").toLowerCase(), i);
    }
    final int[] count = {0};

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
            String dateVal = csvField(parts, hdr, "filing_dt");
            if (dateVal == null || dateVal.length() < 4
                || !yearStr.equals(dateVal.substring(0, 4))) {
              continue;
            }
            String serialNo = csvField(parts, hdr, "serial_no");
            Map<String, String> owner = serialNo != null ? owners.get(serialNo) : null;
            Set<String> classes = serialNo != null ? intlClasses.get(serialNo) : null;
            String stmt = serialNo != null ? statements.get(serialNo) : null;
            if (stmt != null && stmt.length() > 2000) {
              stmt = stmt.substring(0, 2000);
            }

            Map<String, Object> row = new HashMap<>();
            row.put("serial_no", strVal(serialNo));
            row.put("registration_no", strVal(normalizeRegistrationNo(csvField(parts, hdr, "registration_no"))));
            row.put("application_year", intVal(yearStr));
            row.put("filing_dt", strVal(dateVal));
            row.put("registration_dt", strVal(csvField(parts, hdr, "registration_dt")));
            row.put("abandon_dt", strVal(csvField(parts, hdr, "abandon_dt")));
            row.put("mark_id_char", strVal(csvField(parts, hdr, "mark_id_char")));
            row.put("mark_draw_cd", strVal(csvField(parts, hdr, "mark_draw_cd")));
            row.put("cfh_status_cd", strVal(csvField(parts, hdr, "cfh_status_cd")));
            row.put("cfh_status_dt", strVal(csvField(parts, hdr, "cfh_status_dt")));
            row.put("publication_dt", strVal(csvField(parts, hdr, "publication_dt")));
            row.put("renewal_dt", strVal(csvField(parts, hdr, "renewal_dt")));
            row.put("std_char_claim_in", strVal(csvField(parts, hdr, "std_char_claim_in")));
            row.put("applicant_name",
                strVal(owner != null ? owner.get("own_name") : null));
            row.put("applicant_type",
                strVal(owner != null ? owner.get("own_type_cd") : null));
            row.put("applicant_state",
                strVal(owner != null ? owner.get("own_addr_state_cd") : null));
            row.put("applicant_country",
                strVal(owner != null ? owner.get("own_addr_country_cd") : null));
            row.put("goods_services_class",
                strVal(classes != null && !classes.isEmpty()
                    ? String.join(",", classes) : null));
            row.put("goods_services_description", strVal(stmt));
            count[0]++;
            pending = row;
            return;
          }
          reader.close();
          LOGGER.info("TrademarkApplications: {} records for year {}", count[0], yearStr);
        } catch (IOException e) {
          try { reader.close(); } catch (IOException closeEx) { LOGGER.warn("Failed to close trademark reader: {}", closeEx.getMessage()); }
          throw new UncheckedIOException("TrademarkApplicationsTransformer read failed", e);
        }
      }

      @Override public boolean hasNext() { return pending != null; }

      @Override public Map<String, Object> next() {
        Map<String, Object> row = pending;
        advance();
        return row;
      }
    };
  }

  /**
   * USPTO uses an all-zeros registration_no ({@code "0000000"}) as the placeholder for
   * applications that have not yet been registered. Normalize to null so downstream
   * consumers and DQ checks see honest cardinality on this column.
   */
  private String normalizeRegistrationNo(String v) {
    if (v == null) {
      return null;
    }
    for (int i = 0; i < v.length(); i++) {
      if (v.charAt(i) != '0') {
        return v;
      }
    }
    return null;
  }

  private String csvField(String[] parts, Map<String, Integer> hdr, String column) {
    Integer idx = hdr.get(column.toLowerCase());
    if (idx == null || idx >= parts.length) {
      return null;
    }
    String v = parts[idx].trim().replace("\"", "");
    return v.isEmpty() ? null : v;
  }

  private String downloadAndCacheCsv(String url, String cacheFileName, String apiKey)
      throws IOException {
    String dest = cacheFile(cacheFileName);
    if (isCacheValid(dest)) {
      LOGGER.debug("Trademark cache hit: {}", cacheFileName);
      return dest;
    }
    // Release-freshness gate: reuse a prior copy when the upstream snapshot has not changed, so an
    // immutable annual trademark release is never re-downloaded (it is what tripped the 429 cap).
    String token = currentReleaseToken(url);
    String reuse = cachedCopyForToken(url, token);
    if (reuse != null) {
      LOGGER.info("Trademark freshness: {} unchanged (release {}) — reusing cache, no download",
          url, token);
      return reuse;
    }
    LOGGER.info("Trademark downloading: {} (release {})", url, token == null ? "unknown" : token);
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("X-Api-Key", apiKey);
    extractZipEntryToFile(url, headers, dest, ".csv");
    recordRelease(url, token, dest);
    return dest;
  }

  private Set<String> readCsvYearColumnKeys(
      String path, String dateColumn, String yearStr, String keyColumn) throws IOException {
    Set<String> result = new HashSet<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(path), StandardCharsets.UTF_8));
    try {
      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        return result;
      }
      String[] rawHeaders = headerLine.split(",", -1);
      int dateIdx = -1;
      int keyIdx = -1;
      for (int i = 0; i < rawHeaders.length; i++) {
        String h = rawHeaders[i].trim().replace("\"", "").toLowerCase();
        if (h.equalsIgnoreCase(dateColumn)) {
          dateIdx = i;
        }
        if (h.equalsIgnoreCase(keyColumn)) {
          keyIdx = i;
        }
      }
      if (keyIdx < 0) {
        return result;
      }
      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] parts = line.split(",", -1);
        if (dateIdx >= 0 && dateIdx < parts.length) {
          String dateVal = parts[dateIdx].trim().replace("\"", "");
          if (dateVal.length() < 4 || !yearStr.equals(dateVal.substring(0, 4))) {
            continue;
          }
        }
        if (keyIdx < parts.length) {
          String key = parts[keyIdx].trim().replace("\"", "");
          if (!key.isEmpty()) {
            result.add(key);
          }
        }
      }
    } finally {
      reader.close();
    }
    return result;
  }

  private Map<String, Map<String, String>> readCsvAsLookupForKeys(
      String path, String keyColumn, Set<String> keysToRetain,
      String... retainColumns) throws IOException {
    Map<String, Map<String, String>> result = new HashMap<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(path), StandardCharsets.UTF_8));
    try {
      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        return result;
      }
      String[] rawHeaders = headerLine.split(",", -1);
      int keyIdx = -1;
      for (int i = 0; i < rawHeaders.length; i++) {
        if (rawHeaders[i].trim().replace("\"", "").equalsIgnoreCase(keyColumn)) {
          keyIdx = i;
          break;
        }
      }
      if (keyIdx < 0) {
        return result;
      }
      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] parts = line.split(",", -1);
        if (keyIdx >= parts.length) {
          continue;
        }
        String key = parts[keyIdx].trim().replace("\"", "");
        if (key.isEmpty() || !keysToRetain.contains(key)) {
          continue;
        }
        Map<String, String> row = new HashMap<>();
        for (String col : retainColumns) {
          for (int i = 0; i < rawHeaders.length; i++) {
            if (rawHeaders[i].trim().replace("\"", "").equalsIgnoreCase(col)) {
              row.put(col, i < parts.length ? parts[i].trim().replace("\"", "") : "");
              break;
            }
          }
        }
        result.put(key, row);
      }
    } finally {
      reader.close();
    }
    return result;
  }

  private Map<String, Set<String>> readCsvMultiValueForKeys(
      String path, String keyColumn, Set<String> keysToRetain,
      String valueColumn) throws IOException {
    Map<String, Set<String>> result = new HashMap<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(path), StandardCharsets.UTF_8));
    try {
      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        return result;
      }
      String[] rawHeaders = headerLine.split(",", -1);
      int keyIdx = -1;
      int valIdx = -1;
      for (int i = 0; i < rawHeaders.length; i++) {
        String h = rawHeaders[i].trim().replace("\"", "");
        if (h.equalsIgnoreCase(keyColumn)) {
          keyIdx = i;
        }
        if (h.equalsIgnoreCase(valueColumn)) {
          valIdx = i;
        }
      }
      if (keyIdx < 0 || valIdx < 0) {
        return result;
      }
      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] parts = line.split(",", -1);
        if (keyIdx >= parts.length || valIdx >= parts.length) {
          continue;
        }
        String key = parts[keyIdx].trim().replace("\"", "");
        if (key.isEmpty() || !keysToRetain.contains(key)) {
          continue;
        }
        String val = parts[valIdx].trim().replace("\"", "");
        if (!val.isEmpty()) {
          Set<String> set = result.get(key);
          if (set == null) {
            set = new HashSet<>();
            result.put(key, set);
          }
          set.add(val);
        }
      }
    } finally {
      reader.close();
    }
    return result;
  }

  private Map<String, String> readCsvSingleValueForKeys(
      String path, String keyColumn, Set<String> keysToRetain,
      String valueColumn) throws IOException {
    Map<String, String> result = new HashMap<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(path), StandardCharsets.UTF_8));
    try {
      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        return result;
      }
      String[] rawHeaders = headerLine.split(",", -1);
      int keyIdx = -1;
      int valIdx = -1;
      for (int i = 0; i < rawHeaders.length; i++) {
        String h = rawHeaders[i].trim().replace("\"", "");
        if (h.equalsIgnoreCase(keyColumn)) {
          keyIdx = i;
        }
        if (h.equalsIgnoreCase(valueColumn)) {
          valIdx = i;
        }
      }
      if (keyIdx < 0 || valIdx < 0) {
        return result;
      }
      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] parts = line.split(",", -1);
        if (keyIdx >= parts.length || valIdx >= parts.length) {
          continue;
        }
        String key = parts[keyIdx].trim().replace("\"", "");
        if (key.isEmpty() || !keysToRetain.contains(key) || result.containsKey(key)) {
          continue;
        }
        String val = parts[valIdx].trim().replace("\"", "");
        if (!val.isEmpty()) {
          result.put(key, val);
        }
      }
    } finally {
      reader.close();
    }
    return result;
  }
}
