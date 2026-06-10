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
package org.apache.calcite.adapter.govdata.weather;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Shared parsing helpers for the EPA AirData pre-generated bulk CSV files
 * (https://aqs.epa.gov/aqsweb/airdata/), consumed by {@link EpaAirDataDailyTransformer}
 * and {@link EpaAirDataAnnualTransformer}.
 *
 * <p>The bulk files are national, keyless CSVs (one row per monitor observation). These
 * helpers normalize the quoted, space-separated headers (e.g. {@code "1st Max Value"}) to a
 * stable alphanumeric key, coerce numeric strings to typed values, and restrict rows to the
 * 50 states + DC (the bulk files also carry territory rows such as Puerto Rico (72) that the
 * weather schema's {@code geo.counties} joins do not cover).
 */
final class EpaAirDataSupport {

  private EpaAirDataSupport() {
  }

  /** 50 states + DC FIPS codes — mirrors the schema's {@code all_state_fips} dimension. */
  static final Set<String> US_STATE_FIPS = new HashSet<String>(Arrays.asList(
      "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15", "16", "17",
      "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31",
      "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "44", "45", "46",
      "47", "48", "49", "50", "51", "53", "54", "55", "56"));

  /** The 5 AQI pollutant parameter codes the weather schema tracks (PM2.5, O3, SO2, NO2, CO). */
  static final Set<String> AQI_PARAMS = new HashSet<String>(Arrays.asList(
      "88101", "44201", "42401", "42602", "42101"));

  /**
   * Re-keys a streamed CSV row by an alphanumeric-only, lowercased form of each header so
   * lookups are insensitive to spacing/casing/underscores (handles both {@code "State Code"}
   * and {@code "state_code"} keyings).
   */
  static Map<String, String> normalize(Map<String, Object> row) {
    Map<String, String> out = new LinkedHashMap<String, String>();
    for (Map.Entry<String, Object> e : row.entrySet()) {
      if (e.getKey() == null) {
        continue;
      }
      String key = e.getKey().toLowerCase().replaceAll("[^a-z0-9]", "");
      out.put(key, e.getValue() == null ? null : e.getValue().toString());
    }
    return out;
  }

  /** Left-pads a numeric FIPS fragment with zeros to {@code width}; returns null for blanks. */
  static String padFips(String value, int width) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    while (trimmed.length() < width) {
      trimmed = "0" + trimmed;
    }
    return trimmed;
  }

  /** Parses a CSV cell to Double, or null if blank/non-numeric. */
  static Double parseDouble(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    try {
      return Double.valueOf(trimmed);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** Parses a CSV cell to Integer, tolerating decimal-formatted integers, or null if blank. */
  static Integer parseInt(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    try {
      return Integer.valueOf(trimmed);
    } catch (NumberFormatException e) {
      // Some count columns arrive as "350.0"; fall back to a double parse.
      Double d = parseDouble(trimmed);
      return d == null ? null : Integer.valueOf(d.intValue());
    }
  }

  /** Extracts the 4-digit year from an ISO {@code YYYY-MM-DD} date, or null. */
  static Integer yearOf(String isoDate) {
    if (isoDate == null || isoDate.length() < 4) {
      return null;
    }
    return parseInt(isoDate.substring(0, 4));
  }
}
