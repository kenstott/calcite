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
package org.apache.calcite.adapter.govdata.law;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.govdata.GovDataException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code scotus_dockets}: one row per docket of a decided case, from its docket
 * page on supremecourt.gov. The {@code year} dimension is the Court term; see
 * {@link ScotusDocketFetcher} for which dockets are read.
 *
 * <p>The header fields are the docket's own; {@code granted_date}, {@code argued_date} and
 * {@code judgment_issued_date} are the dates of the docket entries of those names, null where the
 * docket has none.
 */
public class ScotusDocketsProvider implements DataProvider {

  static final String TABLE = "scotus_dockets";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    if (!TABLE.equals(config.getName())) {
      throw new GovDataException("ScotusDocketsProvider does not serve table '"
          + config.getName() + "'");
    }
    String year = variables.get("year");
    if (year == null || year.isEmpty()) {
      throw new IOException(TABLE + ": the 'year' (Court term) dimension is required");
    }
    final Iterator<ScotusDocketPage.Docket> dockets =
        ScotusDocketFetcher.forTerm(Integer.parseInt(year));
    return new Iterator<Map<String, Object>>() {
      @Override public boolean hasNext() {
        return dockets.hasNext();
      }

      @Override public Map<String, Object> next() {
        return row(dockets.next());
      }
    };
  }

  static Map<String, Object> row(ScotusDocketPage.Docket d) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("docket_number", d.number);
    row.put("title", d.title);
    row.put("docketed", d.docketed);
    put(row, "linked_with", d.linkedWith);
    put(row, "lower_court", d.lowerCourt);
    put(row, "lower_court_case_numbers", d.lowerCourtCaseNumbers);
    put(row, "lower_court_decision_date", d.lowerCourtDecisionDate);
    put(row, "lower_court_decision_note", d.lowerCourtDecisionNote);
    put(row, "discretionary_court_decision_date", d.discretionaryCourtDecisionDate);
    put(row, "rehearing_denied", d.rehearingDenied);
    put(row, "questions_presented_url", d.questionsPresentedUrl);
    put(row, "granted_date", d.grantedDate());
    put(row, "argued_date", d.arguedDate());
    put(row, "judgment_issued_date", d.judgmentIssuedDate());
    row.put("entry_count", Integer.valueOf(d.entries.size()));
    List<String> documents = new ArrayList<String>();
    for (ScotusDocketPage.Entry e : d.entries) {
      for (ScotusDocketPage.Filing f : e.documents) {
        documents.add(f.url);
      }
    }
    row.put("document_count", Integer.valueOf(documents.size()));
    return row;
  }

  private static void put(Map<String, Object> row, String key, String value) {
    if (value != null) {
      row.put(key, value);
    }
  }
}
