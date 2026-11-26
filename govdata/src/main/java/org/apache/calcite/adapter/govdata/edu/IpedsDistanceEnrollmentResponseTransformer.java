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
package org.apache.calcite.adapter.govdata.edu;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * IPEDS 12-Month Enrollment by distance education status (EFFY{year}_DIST, published from
 * 2019-20), one row per (unitid, effydlev).
 */
public class IpedsDistanceEnrollmentResponseTransformer extends IpedsBulkCsvTransformer {

  private static final Map<String, String> COLUMN_MAP;
  private static final Map<Integer, String> LEVEL_LABELS;

  static {
    Map<String, String> m = new HashMap<String, String>();
    for (String column : new String[] {
        "UNITID", "EFFYDLEV", "EFYDETOT", "EFYDEEXC", "EFYDESOM", "EFYDENON"}) {
      m.put(column, column.toLowerCase(Locale.ROOT));
    }
    COLUMN_MAP = Collections.unmodifiableMap(m);

    Map<Integer, String> labels = new HashMap<Integer, String>();
    labels.put(1, "All students total");
    labels.put(2, "Undergraduate total");
    labels.put(3, "Undergraduate, Degree/certificate-seeking total");
    labels.put(11, "Undergraduate, Non-degree/certificate-seeking");
    labels.put(12, "Graduate");
    LEVEL_LABELS = Collections.unmodifiableMap(labels);
  }

  @Override String csvBaseName(String effectiveYear) {
    return "effy" + effectiveYear + "_dist";
  }

  @Override Map<String, String> columnMap() {
    return COLUMN_MAP;
  }

  @Override void derive(Map<String, Object> row) {
    Integer level = (Integer) row.get("effydlev");
    String label = level == null ? null : LEVEL_LABELS.get(level);
    if (label == null) {
      throw new IllegalStateException("Unknown EFFYDLEV code " + level + " for unitid "
          + row.get("unitid"));
    }
    row.put("effydlev_label", label);
  }
}
