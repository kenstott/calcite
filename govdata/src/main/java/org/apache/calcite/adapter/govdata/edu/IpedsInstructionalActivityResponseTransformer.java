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
 * IPEDS 12-Month Instructional Activity (EFIA{year}): credit and clock hours delivered between
 * 1 July and 30 June and the full-time-equivalent enrollment derived from them, one row per
 * institution.
 */
public class IpedsInstructionalActivityResponseTransformer extends IpedsBulkCsvTransformer {

  private static final Map<String, String> COLUMN_MAP;

  static {
    Map<String, String> m = new HashMap<String, String>();
    for (String column : new String[] {
        "UNITID", "CDACTUA", "CNACTUA", "CDACTGA", "EFTEUG", "EFTEGD", "FTEUG", "FTEGD",
        "FTEDPP", "ACTTYPE"}) {
      m.put(column, column.toLowerCase(Locale.ROOT));
    }
    COLUMN_MAP = Collections.unmodifiableMap(m);
  }

  @Override String csvBaseName(String effectiveYear) {
    return "efia" + effectiveYear;
  }

  @Override Map<String, String> columnMap() {
    return COLUMN_MAP;
  }

  @Override void derive(Map<String, Object> row) {
    // The file is one row per institution; acttype is kept as the published code.
  }
}
