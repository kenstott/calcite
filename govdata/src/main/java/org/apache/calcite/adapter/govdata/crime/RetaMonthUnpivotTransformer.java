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
package org.apache.calcite.adapter.govdata.crime;

import org.apache.calcite.adapter.file.etl.RowContext;
import org.apache.calcite.adapter.file.etl.RowTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Unpivots one wide FBI Return A master-file record (one row per agency-year, with all
 * twelve months inline) into twelve rows — one per month.
 *
 * <p>The raw fixed-width record carries ~1552 fields: 40 agency-header fields plus 126
 * fields for each month, named with a {@code jan_}…{@code dec_} prefix (e.g.
 * {@code jan_actual_murder}). This transformer emits one row per month containing every
 * header field plus that month's fields with the prefix stripped ({@code actual_murder}),
 * and a {@code month} field (1–12). The four-digit {@code year} is taken from the batch
 * dimension (the source file's own year field is two digits).
 *
 * <p>This is a lossless structural reshape: collapsing the twelve identical month
 * column-groups into a month dimension turns a 1552-column table into a ~167-column one
 * without dropping any data. Offense/measure unpivoting is left to SQL views.
 *
 * <p>Monthly count fields (unfounded, actual, total cleared, cleared-under-18) carry
 * negative adjustments as a signed overpunch: the last digit is replaced by a letter
 * ({@code J}=-1 … {@code R}=-9, {@code }}=-0), so {@code 0000N} is -5. Those fields are
 * decoded to a plain signed integer string here so they load as longs.
 *
 * <p>A record whose agency identifier (ORI) is not seven letters/digits is dropped with a
 * WARN naming the state code and the escaped ORI field. The upstream file occasionally carries
 * a record with an embedded non-ASCII character (a zero-width space) in the ORI field; the
 * agency identifier is unrecoverable from such a record and, decoded as single-byte text, every
 * later field in it is displaced, so none of its values can be attributed to an agency.
 */
public class RetaMonthUnpivotTransformer implements RowTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(RetaMonthUnpivotTransformer.class);

  /** A valid ORI as laid out in the master file: two-letter state code plus five characters. */
  private static final Pattern VALID_ORI = Pattern.compile("[A-Z0-9]{7}");

  private static final String[] MONTHS = {
      "jan", "feb", "mar", "apr", "may", "jun",
      "jul", "aug", "sep", "oct", "nov", "dec"
  };

  /** Month prefix (e.g. "jan") -> 1-based month number. */
  private static final Map<String, Integer> MONTH_INDEX = new HashMap<String, Integer>();

  static {
    for (int i = 0; i < MONTHS.length; i++) {
      MONTH_INDEX.put(MONTHS[i], i + 1);
    }
  }

  /** Prefixes of the monthly count fields that use signed-overpunch encoding. */
  private static final String[] COUNT_PREFIXES = {
      "unfounded_", "actual_", "total_cleared_", "cleared_18_"
  };

  /** Default constructor required for reflection-based instantiation. */
  public RetaMonthUnpivotTransformer() {
  }

  @Override public List<Map<String, Object>> transform(Map<String, Object> row,
      RowContext context) {
    Object ori = row.get("ori");
    if (ori == null || !VALID_ORI.matcher(ori.toString()).matches()) {
      LOGGER.warn("Dropping RETA record with invalid ORI (state={}, ori={}, year={}): "
          + "agency identifier is unrecoverable and the record's fields are not attributable",
          row.get("state"), escape(ori), context.getDimensionValues().get("year"));
      return new ArrayList<Map<String, Object>>();
    }
    // One target map per month, plus a shared header map for fields common to all months.
    Map<String, Object> header = new LinkedHashMap<String, Object>();
    List<Map<String, Object>> monthRows = new ArrayList<Map<String, Object>>(MONTHS.length);
    for (int i = 0; i < MONTHS.length; i++) {
      monthRows.add(new LinkedHashMap<String, Object>());
    }

    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String key = entry.getKey();
      Integer month = monthOf(key);
      if (month != null) {
        String field = key.substring(4);
        Object value = entry.getValue();
        if (isCountField(field)) {
          value = decodeOverpunch(value);
        }
        monthRows.get(month - 1).put(field, value);
      } else {
        header.put(key, entry.getValue());
      }
    }

    // The four-digit reporting year comes from the batch dimension; the file's own
    // header year field is two digits.
    Object year = context.getDimensionValues().get("year");

    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>(MONTHS.length);
    for (int i = 0; i < MONTHS.length; i++) {
      Map<String, Object> result = new LinkedHashMap<String, Object>(header);
      result.putAll(monthRows.get(i));
      result.put("year", year);
      result.put("month", i + 1);
      out.add(result);
    }
    return out;
  }

  private static String escape(Object value) {
    if (value == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    for (char c : value.toString().toCharArray()) {
      if (c >= 0x20 && c < 0x7f) {
        sb.append(c);
      } else {
        sb.append(String.format("\\u%04x", (int) c));
      }
    }
    return sb.toString();
  }

  /**
   * Returns the 1-based month number when {@code key} is a {@code mon_}-prefixed monthly
   * field (e.g. "jan_actual_murder"), or null for header fields.
   */
  private static Integer monthOf(String key) {
    if (key.length() < 4 || key.charAt(3) != '_') {
      return null;
    }
    return MONTH_INDEX.get(key.substring(0, 3));
  }

  private static boolean isCountField(String field) {
    for (String prefix : COUNT_PREFIXES) {
      if (field.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Decodes a signed-overpunch count ({@code 0000N} to {@code -5}). A value whose last
   * character is not an overpunch letter is returned unchanged.
   */
  static Object decodeOverpunch(Object value) {
    if (!(value instanceof String)) {
      return value;
    }
    String text = (String) value;
    if (text.length() < 2) {
      return value;
    }
    char last = text.charAt(text.length() - 1);
    int digit;
    if (last == '}') {
      digit = 0;
    } else if (last >= 'J' && last <= 'R') {
      digit = last - 'J' + 1;
    } else {
      return value;
    }
    String leading = text.substring(0, text.length() - 1);
    for (int i = 0; i < leading.length(); i++) {
      if (!Character.isDigit(leading.charAt(i))) {
        return value;
      }
    }
    return "-" + leading + digit;
  }
}
