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
 * IPEDS 12-Month Enrollment (EFFY{year}): unduplicated headcount of students enrolled for credit
 * between 1 July and 30 June, one row per (unitid, effyalev). Files before 2019-20 carry only
 * {@code EFFYLEV} (all, undergraduate, graduate), which is translated to the matching
 * {@code EFFYALEV} code so one level column spans every vintage.
 */
public class IpedsTwelveMonthEnrollmentResponseTransformer extends IpedsBulkCsvTransformer {

  private static final Map<String, String> COLUMN_MAP;
  private static final Map<Integer, Integer> COARSE_LEVELS;
  private static final Map<Integer, String> LEVEL_LABELS;

  static {
    Map<String, String> m = new HashMap<String, String>();
    for (String column : new String[] {
        "UNITID", "EFFYALEV", "EFFYLEV", "EFYTOTLT", "EFYTOTLM", "EFYTOTLW", "EFYAIANT",
        "EFYAIANM", "EFYAIANW", "EFYASIAT", "EFYASIAM", "EFYASIAW", "EFYBKAAT",
        "EFYBKAAM", "EFYBKAAW", "EFYHISPT", "EFYHISPM", "EFYHISPW", "EFYNHPIT",
        "EFYNHPIM", "EFYNHPIW", "EFYWHITT", "EFYWHITM", "EFYWHITW", "EFY2MORT",
        "EFY2MORM", "EFY2MORW", "EFYUNKNT", "EFYUNKNM", "EFYUNKNW", "EFYNRALT",
        "EFYNRALM", "EFYNRALW", "EFYGUUN", "EFYGUAN", "EFYGUTOT", "EFYGUKN"}) {
      m.put(column, column.toLowerCase(Locale.ROOT));
    }
    COLUMN_MAP = Collections.unmodifiableMap(m);

    Map<Integer, Integer> coarse = new HashMap<Integer, Integer>();
    coarse.put(1, 1);
    coarse.put(2, 2);
    coarse.put(4, 12);
    COARSE_LEVELS = Collections.unmodifiableMap(coarse);

    Map<Integer, String> labels = new HashMap<Integer, String>();
    labels.put(1, "All students total");
    labels.put(2, "Undergraduate total");
    labels.put(3, "Undergraduate, Degree/certificate-seeking total");
    labels.put(4, "Undergraduate, Degree/certificate-seeking, First-time");
    labels.put(5, "Undergraduate, Other degree/certificate-seeking");
    labels.put(19, "Undergraduate, Other degree/certificate-seeking, Transfer-ins");
    labels.put(20, "Undergraduate, Other degree/certificate-seeking, Continuing");
    labels.put(11, "Undergraduate, Non-degree/certificate-seeking");
    labels.put(12, "Graduate");
    labels.put(21, "Full-time students total");
    labels.put(22, "Full-time, Undergraduate total");
    labels.put(23, "Full-time, Undergraduate, Degree/certificate-seeking total");
    labels.put(24, "Full-time, Undergraduate, Degree/certificate-seeking, First-time");
    labels.put(25, "Full-time, Undergraduate, Other degree/certificate-seeking");
    labels.put(39, "Full-time, Undergraduate, Other degree/certificate-seeking, Transfer-ins");
    labels.put(40, "Full-time, Undergraduate, Other degree/certificate-seeking, Continuing");
    labels.put(31, "Full-time, Undergraduate, Non-degree/certificate-seeking");
    labels.put(32, "Full-time, Graduate");
    labels.put(41, "Part-time students total");
    labels.put(42, "Part-time, Undergraduate total");
    labels.put(43, "Part-time, Undergraduate, Degree/certificate-seeking total");
    labels.put(44, "Part-time, Undergraduate, Degree/certificate-seeking, First-time");
    labels.put(45, "Part-time, Undergraduate, Other degree/certificate-seeking");
    labels.put(59, "Part-time, Undergraduate, Other degree/certificate-seeking, Transfer-ins");
    labels.put(60, "Part-time, Undergraduate, Other degree/certificate-seeking, Continuing");
    labels.put(51, "Part-time, Undergraduate, Non-degree/certificate-seeking");
    labels.put(52, "Part-time, Graduate");
    LEVEL_LABELS = Collections.unmodifiableMap(labels);
  }

  @Override String csvBaseName(String effectiveYear) {
    return "effy" + effectiveYear;
  }

  @Override Map<String, String> columnMap() {
    return COLUMN_MAP;
  }

  @Override void derive(Map<String, Object> row) {
    Integer detail = (Integer) row.remove("effyalev");
    Integer coarse = (Integer) row.remove("effylev");
    if (detail == null) {
      detail = coarse == null ? null : COARSE_LEVELS.get(coarse);
    }
    String label = detail == null ? null : LEVEL_LABELS.get(detail);
    if (label == null) {
      throw new IllegalStateException("EFFY row with unknown level (EFFYALEV " + detail
          + ", EFFYLEV " + coarse + ") for unitid " + row.get("unitid"));
    }
    row.put("effyalev", detail);
    row.put("effyalev_label", label);
    row.put("attendance", detail >= 41 ? "part_time" : detail >= 21 ? "full_time" : "all");
    row.put("level_of_study", detail == 1 || detail == 21 || detail == 41 ? "all"
        : detail == 12 || detail == 32 || detail == 52 ? "graduate" : "undergraduate");
  }
}
