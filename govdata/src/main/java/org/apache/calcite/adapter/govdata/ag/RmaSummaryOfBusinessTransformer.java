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
package org.apache.calcite.adapter.govdata.ag;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipInputStream;

/**
 * Streaming transformer for the USDA RMA "Summary of Business — State/County/Crop
 * with Coverage Level (1989 Forward)" bulk files.
 *
 * <p>Each {@code sobcov_{year}.zip} holds one pipe-delimited, header-less text file
 * with 28 fixed fields (documented in RMA's record layout), one row per State ×
 * County × Crop × Insurance Plan × Coverage Category × Delivery Type × Coverage
 * Level. Text fields are space-padded to fixed width and are trimmed here; a
 * 5-digit {@code county_fips} is derived (state + county) for the geo FK.
 *
 * <p>Implemented as a {@link StreamingResponseTransformer} because the source is a
 * multi-megabyte binary zip (149,928 rows for 2022) that must not be buffered as a
 * String; rows are produced lazily.
 */
public class RmaSummaryOfBusinessTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(RmaSummaryOfBusinessTransformer.class);

  /** Field name (in pipe order) and kind: S=string(trim), I=integer, L=long, D=double. */
  private static final String[][] FIELDS = {
      {"year", "I"},
      {"state_fips", "S"},
      {"state_abbr", "S"},
      {"county_code", "S"},
      {"county_name", "S"},
      {"commodity_code", "S"},
      {"commodity_name", "S"},
      {"insurance_plan_code", "S"},
      {"insurance_plan_abbr", "S"},
      {"coverage_category", "S"},
      {"delivery_type", "S"},
      {"coverage_level", "D"},
      {"policies_sold_count", "L"},
      {"policies_earning_prem_count", "L"},
      {"policies_indemnified_count", "L"},
      {"units_earning_prem_count", "L"},
      {"units_indemnified_count", "L"},
      {"quantity_type", "S"},
      {"net_reported_quantity", "D"},
      {"endorsed_companion_acres", "D"},
      {"liability_amount", "D"},
      {"total_premium_amount", "D"},
      {"subsidy_amount", "D"},
      {"state_private_subsidy", "D"},
      {"additional_subsidy", "D"},
      {"efa_premium_discount", "D"},
      {"indemnity_amount", "D"},
      {"loss_ratio", "D"},
  };

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String url = context.getUrl();
    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(openFirstZipEntry(url), StandardCharsets.UTF_8));
    return new Iterator<Map<String, Object>>() {
      private final ArrayDeque<Map<String, Object>> pending = new ArrayDeque<Map<String, Object>>();
      private boolean closed;

      private void fill() {
        try {
          String line;
          while (pending.isEmpty() && (line = reader.readLine()) != null) {
            if (!line.isEmpty()) {
              pending.add(parseRow(line, url));
            }
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed streaming RMA SOB file: " + url, e);
        }
        if (pending.isEmpty() && !closed) {
          closed = true;
          try {
            reader.close();
          } catch (IOException ignored) {
            // best-effort
          }
        }
      }

      @Override public boolean hasNext() {
        fill();
        return !pending.isEmpty();
      }

      @Override public Map<String, Object> next() {
        fill();
        if (pending.isEmpty()) {
          throw new NoSuchElementException();
        }
        return pending.poll();
      }
    };
  }

  private Map<String, Object> parseRow(String line, String url) {
    // -1 keeps trailing empty fields so column positions never shift.
    String[] parts = line.split("\\|", -1);
    if (parts.length < FIELDS.length) {
      throw new RuntimeException("RMA row has " + parts.length + " fields, expected "
          + FIELDS.length + " in " + url + " — layout mismatch: " + line);
    }
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    String stateFips = null;
    String countyCode = null;
    for (int i = 0; i < FIELDS.length; i++) {
      String name = FIELDS[i][0];
      String kind = FIELDS[i][1];
      String raw = parts[i] == null ? null : parts[i].trim();
      Object value;
      if (raw == null || raw.isEmpty()) {
        value = null;
      } else if ("S".equals(kind)) {
        value = raw;
      } else if ("I".equals(kind)) {
        value = Integer.valueOf(parseLongStrict(raw, name, url).intValue());
      } else if ("L".equals(kind)) {
        value = parseLongStrict(raw, name, url);
      } else {
        value = parseDoubleStrict(raw, name, url);
      }
      row.put(name, value);
      if ("state_fips".equals(name)) {
        stateFips = raw;
      } else if ("county_code".equals(name)) {
        countyCode = raw;
      }
    }
    row.put("county_fips", countyFips(stateFips, countyCode));
    return row;
  }

  /** 5-digit county FIPS (state 2 + county 3, zero-padded); null if either part missing. */
  private String countyFips(String stateFips, String countyCode) {
    if (stateFips == null || countyCode == null || stateFips.isEmpty() || countyCode.isEmpty()) {
      return null;
    }
    String st = stateFips;
    String co = countyCode;
    while (st.length() < 2) {
      st = "0" + st;
    }
    while (co.length() < 3) {
      co = "0" + co;
    }
    return st + co;
  }

  private Long parseLongStrict(String raw, String field, String url) {
    try {
      return Long.valueOf(raw);
    } catch (NumberFormatException e) {
      throw new RuntimeException("RMA field '" + field + "' not an integer: '" + raw
          + "' in " + url, e);
    }
  }

  private Double parseDoubleStrict(String raw, String field, String url) {
    try {
      return Double.valueOf(raw);
    } catch (NumberFormatException e) {
      throw new RuntimeException("RMA field '" + field + "' not a number: '" + raw
          + "' in " + url, e);
    }
  }

  /** Downloads the zip and returns a stream positioned at the first (only) entry. */
  private static InputStream openFirstZipEntry(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setInstanceFollowRedirects(true);
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("RMA SOB download HTTP " + code + ": " + url);
    }
    ZipInputStream zis = new ZipInputStream(conn.getInputStream());
    if (zis.getNextEntry() == null) {
      zis.close();
      throw new IOException("RMA SOB zip is empty: " + url);
    }
    LOGGER.debug("RMA: streaming SOB file from {}", url);
    return zis;
  }
}
