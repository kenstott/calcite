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
package org.apache.calcite.adapter.govdata.cftc;

import org.apache.calcite.adapter.file.etl.RowContext;
import org.apache.calcite.adapter.file.etl.RowTransformer;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Derives the {@code trade_date} (a DATE) from the source {@code "Event timestamp"} field.
 *
 * <p>The DATE writer parses only a clean {@code YYYY-MM-DD} string, not a full timestamp
 * ({@code "2024-09-01 23:17:26"}), so {@code trade_date} cannot be mapped directly from the
 * timestamp source the way {@code event_timestamp} (a TIMESTAMP) is. This transformer writes
 * a {@code trade_date} field holding the date portion (first 10 chars); the schema then maps
 * {@code trade_date}'s {@code source} to that field. {@code event_timestamp} keeps the full
 * timestamp.
 *
 * <p>One-to-one: each input row is returned enriched with the {@code trade_date} field.
 */
public class CftcTradeDateTransformer implements RowTransformer {

  private static final String SOURCE_FIELD = "Event timestamp";
  private static final String TARGET_FIELD = "trade_date";

  @Override public List<Map<String, Object>> transform(Map<String, Object> row,
      RowContext context) {
    Object ts = row.get(SOURCE_FIELD);
    if (ts == null) {
      // CSV headers preserve original casing; fall back to a case-insensitive match.
      for (Map.Entry<String, Object> entry : row.entrySet()) {
        if (SOURCE_FIELD.equalsIgnoreCase(entry.getKey())) {
          ts = entry.getValue();
          break;
        }
      }
    }
    if (ts != null) {
      String s = ts.toString();
      if (s.length() >= 10) {
        row.put(TARGET_FIELD, s.substring(0, 10));
      }
    }
    return Collections.singletonList(row);
  }
}
