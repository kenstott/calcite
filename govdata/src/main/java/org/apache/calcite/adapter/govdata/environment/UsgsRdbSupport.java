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
package org.apache.calcite.adapter.govdata.environment;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Shared helpers for parsing USGS NWIS RDB (tab-delimited "rdb" format) responses.
 *
 * <p>RDB structure: leading {@code #} comment lines, then a tab-delimited column-name
 * header row, then a "types" row (field widths like {@code 5s 15s 20d 14n}), then
 * tab-delimited data rows. The {@code dv} (daily values) service concatenates one such
 * block <b>per site</b>, each with its own header (the value column is named
 * {@code <tsid>_<param>_<stat>}); the {@code site} service returns a single block.
 */
final class UsgsRdbSupport {

  private UsgsRdbSupport() {
  }

  /** True for a comment line or the types/widths row (e.g. {@code 5s\t15s\t20d}). */
  static boolean isCommentOrTypesRow(String line) {
    if (line.isEmpty() || line.charAt(0) == '#') {
      return true;
    }
    // Types row: every tab-field matches digits followed by a single s/d/n letter.
    String[] fields = line.split("\t", -1);
    for (String f : fields) {
      if (!f.matches("\\d+[sdn]")) {
        return false;
      }
    }
    return fields.length > 0;
  }

  static String[] splitTabs(String line) {
    return line.split("\t", -1);
  }

  static String trimOrNull(String s) {
    if (s == null) {
      return null;
    }
    String t = s.trim();
    return t.isEmpty() ? null : t;
  }

  /** Puts a trimmed string, or null when blank. */
  static void putText(ObjectNode row, String col, String value) {
    String v = trimOrNull(value);
    if (v == null) {
      row.putNull(col);
    } else {
      row.put(col, v);
    }
  }

  /** Puts a double parsed from the field, or null when blank/non-numeric. */
  static void putDouble(ObjectNode row, String col, String value) {
    String v = trimOrNull(value);
    if (v == null) {
      row.putNull(col);
      return;
    }
    try {
      row.put(col, Double.parseDouble(v));
    } catch (NumberFormatException e) {
      row.putNull(col);
    }
  }
}
