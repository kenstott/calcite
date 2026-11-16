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
import java.util.Map;

/**
 * IPEDS Fall Enrollment (EF{year}A): enrollment by level of student, attendance status,
 * race/ethnicity (including U.S. nonresident) and gender, one row per (unitid, efalevel).
 * {@code attendance} and {@code level_of_study} are derived from the file's SECTION and
 * LSTUDY columns; {@code efalevel_label} from the survey data dictionary.
 */
public class IpedsFallEnrollmentResponseTransformer extends IpedsBulkCsvTransformer {

  private static final Map<String, String> COLUMN_MAP;
  private static final Map<Integer, String> LEVEL_LABELS;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("UNITID", "unitid");
    m.put("EFALEVEL", "efalevel");
    m.put("LINE", "line");
    m.put("SECTION", "section");
    m.put("LSTUDY", "lstudy");
    m.put("EFTOTLT", "eftotlt");
    m.put("EFTOTLM", "eftotlm");
    m.put("EFTOTLW", "eftotlw");
    m.put("EFAIANT", "efaiant");
    m.put("EFAIANM", "efaianm");
    m.put("EFAIANW", "efaianw");
    m.put("EFASIAT", "efasiat");
    m.put("EFASIAM", "efasiam");
    m.put("EFASIAW", "efasiaw");
    m.put("EFBKAAT", "efbkaat");
    m.put("EFBKAAM", "efbkaam");
    m.put("EFBKAAW", "efbkaaw");
    m.put("EFHISPT", "efhispt");
    m.put("EFHISPM", "efhispm");
    m.put("EFHISPW", "efhispw");
    m.put("EFNHPIT", "efnhpit");
    m.put("EFNHPIM", "efnhpim");
    m.put("EFNHPIW", "efnhpiw");
    m.put("EFWHITT", "efwhitt");
    m.put("EFWHITM", "efwhitm");
    m.put("EFWHITW", "efwhitw");
    m.put("EF2MORT", "ef2mort");
    m.put("EF2MORM", "ef2morm");
    m.put("EF2MORW", "ef2morw");
    m.put("EFUNKNT", "efunknt");
    m.put("EFUNKNM", "efunknm");
    m.put("EFUNKNW", "efunknw");
    m.put("EFNRALT", "efnralt");
    m.put("EFNRALM", "efnralm");
    m.put("EFNRALW", "efnralw");
    COLUMN_MAP = Collections.unmodifiableMap(m);

    Map<Integer, String> l = new HashMap<Integer, String>();
    l.put(1, "All students total");
    l.put(2, "All students, Undergraduate total");
    l.put(3, "All students, Undergraduate, Degree/certificate-seeking total");
    l.put(4, "All students, Undergraduate, Degree/certificate-seeking, First-time");
    l.put(5, "All students, Undergraduate, Other degree/certificate-seeking");
    l.put(11, "All students, Undergraduate, Non-degree/certificate-seeking");
    l.put(12, "All students, Graduate");
    l.put(19, "All students, Undergraduate, Other degree/certificate-seeking, Transfer-ins");
    l.put(20, "All students, Undergraduate, Other degree/certificate-seeking, Continuing");
    l.put(21, "Full-time students total");
    l.put(22, "Full-time students, Undergraduate total");
    l.put(23, "Full-time students, Undergraduate, Degree/certificate-seeking total");
    l.put(24, "Full-time students, Undergraduate, Degree/certificate-seeking, First-time");
    l.put(25, "Full-time students, Undergraduate, Other degree/certificate-seeking");
    l.put(31, "Full-time students, Undergraduate, Non-degree/certificate-seeking");
    l.put(32, "Full-time students, Graduate");
    l.put(39, "Full-time students, Undergraduate, Other degree/certificate-seeking, Transfer-ins");
    l.put(40, "Full-time students, Undergraduate, Other degree/certificate-seeking, Continuing");
    l.put(41, "Part-time students total");
    l.put(42, "Part-time students, Undergraduate total");
    l.put(43, "Part-time students, Undergraduate, Degree/certificate-seeking total");
    l.put(44, "Part-time students, Undergraduate, Degree/certificate-seeking, First-time");
    l.put(45, "Part-time students, Undergraduate, Other degree/certificate-seeking");
    l.put(51, "Part-time students, Undergraduate, Non-degree/certificate-seeking");
    l.put(52, "Part-time students, Graduate");
    l.put(59, "Part-time students, Undergraduate, Other degree/certificate-seeking, Transfer-ins");
    l.put(60, "Part-time students, Undergraduate, Other degree/certificate-seeking, Continuing");
    LEVEL_LABELS = Collections.unmodifiableMap(l);
  }

  @Override String csvBaseName(String effectiveYear) {
    return "ef" + effectiveYear + "a";
  }

  @Override Map<String, String> columnMap() {
    return COLUMN_MAP;
  }

  @Override void derive(Map<String, Object> row) {
    Integer level = (Integer) row.remove("efalevel");
    Integer section = (Integer) row.remove("section");
    Integer lstudy = (Integer) row.remove("lstudy");
    row.remove("line");
    if (level == null || section == null || lstudy == null) {
      throw new IllegalStateException("EF row missing EFALEVEL/SECTION/LSTUDY for unitid "
          + row.get("unitid"));
    }
    String label = LEVEL_LABELS.get(level);
    if (label == null) {
      throw new IllegalStateException("Unknown EFALEVEL code " + level);
    }
    row.put("efalevel", level);
    row.put("efalevel_label", label);
    row.put("attendance", section == 1 ? "full_time" : section == 2 ? "part_time" : "all");
    row.put("level_of_study",
        lstudy == 1 ? "undergraduate" : lstudy == 3 ? "graduate" : "all");
  }
}
