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
 * Maps a streamed row from the Federal Judicial Center's Biographical Directory of Article III
 * Federal Judges CSV ({@code https://www.fjc.gov/sites/default/files/history/judges.csv}) into
 * the {@code federal_judges} table.
 *
 * <p>Verified against a live download (2026-08-01): the CSV is 201 columns wide, with headers
 * like {@code "Court Type (1)"} through {@code "Court Type (6)"} for judges with multiple
 * appointments (District -> Circuit, retired-then-recalled, etc.), plus separate
 * "Other Federal Judicial Service", "School"/"Degree", and free-text career columns not mapped
 * here. This transformer picks a curated 12-of-~33 fields per appointment group — the
 * appointment-identity and confirmation-timeline columns — matching the schema YAML's
 * {@code court_type_1}..{@code termination_reason_6} columns.
 *
 * <p>The CSV reader hands each row through as {@code Map<String, Object>} keyed by the raw
 * header text (e.g. {@code "Court Type (1)"}); this class re-derives the same normalized key
 * ({@code lowercase, non-alphanumeric stripped}, e.g. {@code "courttype1"}) that
 * {@code EpaAirDataSupport.normalize} uses elsewhere in govdata, so header punctuation/casing
 * variance doesn't break the lookup.
 */
public class FederalJudgesTransformer implements RowTransformer {

  private static final String[] GLOBAL_FIELDS = {
      "nid", "jid",
  };

  private static final String[][] GLOBAL_NAME_FIELDS = {
      {"last_name", "Last Name"},
      {"first_name", "First Name"},
      {"middle_name", "Middle Name"},
      {"suffix", "Suffix"},
      {"birth_year", "Birth Year"},
      {"gender", "Gender"},
      {"race_or_ethnicity", "Race or Ethnicity"},
  };

  /** Output column suffix -> source header prefix, applied for appointment groups 1-6. */
  private static final String[][] GROUP_FIELDS = {
      {"court_type", "Court Type"},
      {"court_name", "Court Name"},
      {"appointment_title", "Appointment Title"},
      {"appointing_president", "Appointing President"},
      {"party_of_appointing_president", "Party of Appointing President"},
      {"aba_rating", "ABA Rating"},
      {"nomination_date", "Nomination Date"},
      {"confirmation_date", "Confirmation Date"},
      {"commission_date", "Commission Date"},
      {"senior_status_date", "Senior Status Date"},
      {"termination_date", "Termination Date"},
      {"termination_reason", "Termination"},
  };

  @Override public List<Map<String, Object>> transform(Map<String, Object> row,
      RowContext context) {
    Map<String, String> normalized = normalize(row);

    Map<String, Object> out = new LinkedHashMap<String, Object>();
    for (String field : GLOBAL_FIELDS) {
      out.put(field, normalized.get(normalizeKey(field)));
    }
    for (String[] mapping : GLOBAL_NAME_FIELDS) {
      out.put(mapping[0], normalized.get(normalizeKey(mapping[1])));
    }
    for (int group = 1; group <= 6; group++) {
      for (String[] mapping : GROUP_FIELDS) {
        String outColumn = mapping[0] + "_" + group;
        String sourceHeader = mapping[1] + " (" + group + ")";
        out.put(outColumn, normalized.get(normalizeKey(sourceHeader)));
      }
    }

    return Collections.<Map<String, Object>>singletonList(out);
  }

  private static Map<String, String> normalize(Map<String, Object> row) {
    Map<String, String> out = new LinkedHashMap<String, String>();
    for (Map.Entry<String, Object> e : row.entrySet()) {
      if (e.getKey() == null) {
        continue;
      }
      String value = e.getValue() == null ? null : e.getValue().toString();
      out.put(normalizeKey(e.getKey()), value == null || value.trim().isEmpty() ? null : value);
    }
    return out;
  }

  private static String normalizeKey(String key) {
    return key.toLowerCase(java.util.Locale.ROOT).replaceAll("[^a-z0-9]", "");
  }
}
