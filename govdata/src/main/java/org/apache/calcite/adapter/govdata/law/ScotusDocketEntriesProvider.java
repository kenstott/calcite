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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * DataProvider for {@code scotus_docket_entries}: one row per entry in the proceedings and orders
 * of each docket of a decided case. Reads the same pages as {@link ScotusDocketsProvider}, one
 * docket at a time, and emits that docket's entries before fetching the next.
 */
public class ScotusDocketEntriesProvider implements DataProvider {

  static final String TABLE = "scotus_docket_entries";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    if (!TABLE.equals(config.getName())) {
      throw new GovDataException("ScotusDocketEntriesProvider does not serve table '"
          + config.getName() + "'");
    }
    String year = variables.get("year");
    if (year == null || year.isEmpty()) {
      throw new IOException(TABLE + ": the 'year' (Court term) dimension is required");
    }
    final Iterator<ScotusDocketPage.Docket> dockets =
        ScotusDocketFetcher.forTerm(Integer.parseInt(year));
    return new Iterator<Map<String, Object>>() {
      private final Deque<Map<String, Object>> rows = new ArrayDeque<Map<String, Object>>();

      @Override public boolean hasNext() {
        while (rows.isEmpty() && dockets.hasNext()) {
          rows.addAll(rows(dockets.next()));
        }
        return !rows.isEmpty();
      }

      @Override public Map<String, Object> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return rows.removeFirst();
      }
    };
  }

  static List<Map<String, Object>> rows(ScotusDocketPage.Docket docket) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (ScotusDocketPage.Entry e : docket.entries) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("docket_number", docket.number);
      row.put("sequence", Integer.valueOf(e.sequence));
      row.put("entry_date", e.date);
      row.put("entry_text", e.text);
      if (!e.documents.isEmpty()) {
        List<String> labels = new ArrayList<String>();
        List<String> urls = new ArrayList<String>();
        for (ScotusDocketPage.Filing f : e.documents) {
          labels.add(f.label);
          urls.add(f.url);
        }
        row.put("document_labels", labels);
        row.put("document_urls", urls);
      }
      rows.add(row);
    }
    return rows;
  }
}
