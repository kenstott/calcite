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
package org.apache.calcite.adapter.govdata.officials;

import org.apache.calcite.adapter.file.etl.RowContext;
import org.apache.calcite.adapter.file.etl.RowTransformer;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps a row of Voteview's {@code HSall_members.csv} (UCLA, "Member Ideology") into the
 * {@code member_ideology} table. Column names in the file already match the table's, so this
 * copies the declared columns, turns blanks into nulls, and strips the trailing {@code .0}
 * the file writes on whole-number fields (e.g. {@code born = 1736.0}, {@code party_code =
 * 5000.0}) so they cast cleanly to INTEGER.
 */
public class VoteviewMemberTransformer implements RowTransformer {

  private static final String[] STRING_FIELDS = {
      "chamber", "state_abbrev", "bioname", "bioguide_id",
  };

  private static final String[] INTEGER_FIELDS = {
      "congress", "icpsr", "state_icpsr", "district_code", "party_code", "occupancy",
      "last_means", "born", "died", "nominate_number_of_votes", "nominate_number_of_errors",
  };

  private static final String[] DOUBLE_FIELDS = {
      "nominate_dim1", "nominate_dim2", "nominate_log_likelihood",
      "nominate_geo_mean_probability", "nokken_poole_dim1", "nokken_poole_dim2",
  };

  @Override public List<Map<String, Object>> transform(Map<String, Object> row,
      RowContext context) {
    Map<String, Object> out = new LinkedHashMap<String, Object>();
    for (String f : STRING_FIELDS) {
      out.put(f, text(row.get(f)));
    }
    for (String f : INTEGER_FIELDS) {
      String v = text(row.get(f));
      if (v != null && v.endsWith(".0")) {
        v = v.substring(0, v.length() - 2);
      }
      out.put(f, v);
    }
    for (String f : DOUBLE_FIELDS) {
      out.put(f, text(row.get(f)));
    }
    return Collections.<Map<String, Object>>singletonList(out);
  }

  private static String text(Object value) {
    if (value == null) {
      return null;
    }
    String s = value.toString().trim();
    return s.isEmpty() ? null : s;
  }
}
