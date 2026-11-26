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
 * IPEDS 12-Month Enrollment of high school students taking college courses for credit
 * (EFFY{year}_HS, published from 2022-23), one row per institution.
 */
public class IpedsDualEnrollmentResponseTransformer extends IpedsBulkCsvTransformer {

  private static final Map<String, String> COLUMN_MAP;

  static {
    Map<String, String> m = new HashMap<String, String>();
    for (String column : new String[] {
        "UNITID", "EFYTOTLT", "EFYTOTLM", "EFYTOTLW", "EFYAIANT", "EFYAIANM", "EFYAIANW",
        "EFYASIAT", "EFYASIAM", "EFYASIAW", "EFYBKAAT", "EFYBKAAM", "EFYBKAAW",
        "EFYHISPT", "EFYHISPM", "EFYHISPW", "EFYNHPIT", "EFYNHPIM", "EFYNHPIW",
        "EFYWHITT", "EFYWHITM", "EFYWHITW", "EFY2MORT", "EFY2MORM", "EFY2MORW",
        "EFYUNKNT", "EFYUNKNM", "EFYUNKNW", "EFYNRALT", "EFYNRALM", "EFYNRALW"}) {
      m.put(column, column.toLowerCase(Locale.ROOT));
    }
    COLUMN_MAP = Collections.unmodifiableMap(m);
  }

  @Override String csvBaseName(String effectiveYear) {
    return "effy" + effectiveYear + "_hs";
  }

  @Override Map<String, String> columnMap() {
    return COLUMN_MAP;
  }

  @Override void derive(Map<String, Object> row) {
    // The file is one row per institution with no coded columns.
  }
}
