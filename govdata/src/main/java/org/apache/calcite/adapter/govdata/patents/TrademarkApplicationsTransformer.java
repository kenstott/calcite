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

import org.apache.calcite.adapter.file.etl.RequestContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

  private static final String TM_BASE =
      "https://bulkdata.uspto.gov/data/trademark/casefile/economics/2024/";

  @Override
  public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String yearStr = getYear(context);
    if (yearStr == null || yearStr.isEmpty()) {
      LOGGER.warn("TrademarkApplications: missing year dimension");
      return Collections.emptyIterator();
    }

    final File caseFile = downloadAndCacheCsv(TM_BASE + "case_file.zip", "tm_case_file.csv");
    File ownerFile = downloadAndCacheCsv(TM_BASE + "owner.zip", "tm_owner.csv");
    File intlClassFile = downloadAndCacheCsv(TM_BASE + "intl_class.zip", "tm_intl_class.csv");
    File statementFile = downloadAndCacheCsv(TM_BASE + "statement.zip", "tm_statement.csv");

    final Set<String> serialNos =
        readCsvYearColumnKeys(caseFile, "filing_dt", yearStr, "serial_no");
    LOGGER.info("TrademarkApplications: {} serial_nos for year {}", serialNos.size(), yearStr);

    final Map<String, Map<String, String>> owners = readCsvAsLookupForKeys(
        ownerFile, "serial_no", serialNos, "party_name", "party_type", "state", "country");
    final Map<String, Set<String>> intlClasses =
        readCsvMultiValueForKeys(intlClassFile, "serial_no", serialNos, "international_code");
    final Map<String, String> statements =
        readCsvSingleValueForKeys(statementFile, "serial_no", serialNos, "statement_text");

    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(Files.newInputStream(caseFile.toPath()), StandardCharsets.UTF_8));
    String headerLine = reader.readLine();
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
          while ((line = reader.readLine()) != null) {
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
            row.put("registration_no", strVal(csvField(parts, hdr, "registration_no")));
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
                strVal(owner != null ? owner.get("party_name") : null));
            row.put("applicant_type",
                strVal(owner != null ? owner.get("party_type") : null));
            row.put("applicant_state",
                strVal(owner != null ? owner.get("state") : null));
            row.put("applicant_country",
                strVal(owner != null ? owner.get("country") : null));
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
          try { reader.close(); } catch (IOException ignored) { }
          throw new RuntimeException("TrademarkApplicationsTransformer read failed", e);
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

  private String csvField(String[] parts, Map<String, Integer> hdr, String column) {
    Integer idx = hdr.get(column.toLowerCase());
    if (idx == null || idx >= parts.length) {
      return null;
    }
    String v = parts[idx].trim().replace("\"", "");
    return v.isEmpty() ? null : v;
  }

  private File downloadAndCacheCsv(String url, String cacheFileName) throws IOException {
    File dest = cacheFile(cacheFileName);
    if (isCacheValid(dest)) {
      LOGGER.debug("Trademark cache hit: {}", cacheFileName);
      return dest;
    }
    LOGGER.info("Trademark downloading: {}", url);
    extractZipEntryToFile(url, dest, ".csv");
    return dest;
  }

  private Set<String> readCsvYearColumnKeys(
      File csvFile, String dateColumn, String yearStr, String keyColumn) throws IOException {
    Set<String> result = new HashSet<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(Files.newInputStream(csvFile.toPath()), StandardCharsets.UTF_8));
    try {
      String headerLine = reader.readLine();
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
      while ((line = reader.readLine()) != null) {
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
      File csvFile, String keyColumn, Set<String> keysToRetain,
      String... retainColumns) throws IOException {
    Map<String, Map<String, String>> result = new HashMap<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(Files.newInputStream(csvFile.toPath()), StandardCharsets.UTF_8));
    try {
      String headerLine = reader.readLine();
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
      while ((line = reader.readLine()) != null) {
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
      File csvFile, String keyColumn, Set<String> keysToRetain,
      String valueColumn) throws IOException {
    Map<String, Set<String>> result = new HashMap<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(Files.newInputStream(csvFile.toPath()), StandardCharsets.UTF_8));
    try {
      String headerLine = reader.readLine();
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
      while ((line = reader.readLine()) != null) {
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
      File csvFile, String keyColumn, Set<String> keysToRetain,
      String valueColumn) throws IOException {
    Map<String, String> result = new HashMap<>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(Files.newInputStream(csvFile.toPath()), StandardCharsets.UTF_8));
    try {
      String headerLine = reader.readLine();
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
      while ((line = reader.readLine()) != null) {
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
