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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Transforms PatentsView g_cpc_current.tsv into patent_cpc_classes rows.
 *
 * <p>Downloads and caches two full-dump files (g_patent.tsv + g_cpc_current.tsv).
 * Pass 1 (eager): collects patent_ids for the requested year from g_patent.tsv.
 * Returns a lazy iterator streaming g_cpc_current.tsv, filtering to that year's patents.
 * No intermediate StringWriter — memory is O(patentIds + chunk_size).
 */
public class PatentCpcClassesTransformer extends AbstractPatentsTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PatentCpcClassesTransformer.class);

  private static final String BASE_URL =
      "https://s3.amazonaws.com/data.patentsview.org/download/";

  @Override
  public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String yearStr = getYear(context);
    if (yearStr == null || yearStr.isEmpty()) {
      LOGGER.warn("PatentCpcClasses: missing year dimension");
      return Collections.emptyIterator();
    }

    String patentFile = downloadAndCacheTsv(
        BASE_URL + "g_patent.tsv.zip", cacheFile("g_patent.tsv"));
    final String cpcFile = downloadAndCacheTsv(
        BASE_URL + "g_cpc_current.tsv.zip", cacheFile("g_cpc_current.tsv"));

    final Set<String> patentIds = readPatentIdsForYear(patentFile, yearStr);
    LOGGER.info("PatentCpcClasses: {} patent IDs for year {}", patentIds.size(), yearStr);

    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(cpcFile), StandardCharsets.UTF_8));
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
            if (patentId == null || !patentIds.contains(patentId)) {
              continue;
            }
            String cpcGroup = getField(parts, hdr, "cpc_group");
            Map<String, Object> row = new HashMap<>();
            row.put("patent_id", strVal(patentId));
            row.put("grant_year", intVal(yearStr));
            row.put("cpc_sequence", intVal(getField(parts, hdr, "cpc_sequence")));
            row.put("cpc_type", strVal(getField(parts, hdr, "cpc_type")));
            row.put("cpc_group", strVal(cpcGroup));
            row.put("cpc_section",
                (cpcGroup != null && cpcGroup.length() >= 1)
                    ? String.valueOf(cpcGroup.charAt(0)) : null);
            row.put("cpc_class",
                (cpcGroup != null && cpcGroup.length() >= 3)
                    ? cpcGroup.substring(0, 3) : null);
            row.put("cpc_subclass",
                (cpcGroup != null && cpcGroup.length() >= 4)
                    ? cpcGroup.substring(3, 4) : null);
            count[0]++;
            pending = row;
            return;
          }
          reader.close();
          LOGGER.info("PatentCpcClasses: {} records for year {}", count[0], yearStr);
        } catch (IOException e) {
          try { reader.close(); } catch (IOException closeEx) { LOGGER.debug("close failed", closeEx); }
          throw new RuntimeException("PatentCpcClassesTransformer read failed", e);
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
