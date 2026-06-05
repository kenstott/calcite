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
package org.apache.calcite.adapter.govdata.patents;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Faithful recreation of PatentsView g_assignee_disambiguated.tsv as patent_assignees rows —
 * one row per patent assignee, keyed by patent_id, no joins and no year. location_id is kept
 * as the foreign key into patent_locations; join there for geography and to patent_grants for
 * a grant year. Loaded once as a snapshot.
 */
public class PatentAssigneesTransformer extends AbstractPatentsTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PatentAssigneesTransformer.class);

  @Override
  public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String q = quarterToken(context);
    final String assigneeFile = downloadAndCacheTsv(
        ODP_PVGPATDIS_BASE + "g_assignee_disambiguated.tsv.zip",
        cacheFile("g_assignee_disambiguated_" + q + ".tsv"));

    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(assigneeFile),
            StandardCharsets.UTF_8));
    String headerLine = CsvRecordReader.readRecord(reader);
    if (headerLine == null) {
      reader.close();
      return Collections.emptyIterator();
    }
    final Map<String, Integer> hdr = buildHeaderMap(splitTsv(headerLine));
    final int[] count = {0};

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> pending;
      { advance(); }

      private void advance() {
        pending = null;
        try {
          String line;
          while ((line = CsvRecordReader.readRecord(reader)) != null) {
            if (line.isEmpty()) {
              continue;
            }
            String[] parts = splitTsv(line);
            String patentId = getField(parts, hdr, "patent_id");
            if (patentId == null || patentId.isEmpty()) {
              continue;
            }

            Map<String, Object> row = new HashMap<>();
            row.put("patent_id", strVal(patentId));
            row.put("assignee_sequence",
                intVal(getField(parts, hdr, "assignee_sequence")));
            row.put("assignee_id", strVal(getField(parts, hdr, "assignee_id")));
            row.put("assignee_organization",
                strVal(getField(parts, hdr, "disambig_assignee_organization")));
            row.put("assignee_name_first",
                strVal(getField(parts, hdr, "disambig_assignee_individual_name_first")));
            row.put("assignee_name_last",
                strVal(getField(parts, hdr, "disambig_assignee_individual_name_last")));
            row.put("assignee_type", strVal(getField(parts, hdr, "assignee_type")));
            row.put("location_id", strVal(getField(parts, hdr, "location_id")));
            count[0]++;
            pending = row;
            return;
          }
          reader.close();
          LOGGER.info("PatentAssignees: {} records (snapshot)", count[0]);
        } catch (IOException e) {
          try { reader.close(); } catch (IOException closeEx) { LOGGER.debug("close failed", closeEx); }
          throw new UncheckedIOException("PatentAssigneesTransformer read failed", e);
        }
      }

      @Override public boolean hasNext() { return pending != null; }

      @Override public Map<String, Object> next() {
        Map<String, Object> row = pending;
        advance();
        return row;
      }
    };
  }
}
