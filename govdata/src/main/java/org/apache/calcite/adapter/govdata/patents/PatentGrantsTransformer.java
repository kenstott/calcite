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
 * Faithful recreation of PatentsView g_patent.tsv as patent_grants rows — one row per
 * granted patent, no joins. g_patent carries patent_date, so the table self-partitions by
 * grant_year: a single streaming pass emits only rows whose grant year matches the requested
 * year dimension. The abstract, filing date, and figure counts live in their own faithful
 * tables (patent_abstracts, patent_applications, patent_figures), joined at query time.
 */
public class PatentGrantsTransformer extends AbstractPatentsTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PatentGrantsTransformer.class);

  @Override
  public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String yearStr = getEffectiveYear(context);
    if (yearStr == null || yearStr.isEmpty()) {
      LOGGER.warn("PatentGrants: missing year dimension");
      return Collections.emptyIterator();
    }

    final String q = quarterToken(context);
    final String patentFile = downloadAndCacheTsv(
        ODP_PVGPATDIS_BASE + "g_patent.tsv.zip", cacheFile("g_patent_" + q + ".tsv"));

    final boolean includeDesign =
        "true".equalsIgnoreCase(System.getenv("PATENTS_INCLUDE_DESIGN"));

    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(patentFile),
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
            String patentDate = getField(parts, hdr, "patent_date");
            // Self-partition by grant year: keep only this year's rows.
            if (patentDate == null || !patentDate.startsWith(yearStr)) {
              continue;
            }
            String patentId = getField(parts, hdr, "patent_id");
            if (patentId == null || patentId.isEmpty()) {
              continue;
            }
            String patentType = getField(parts, hdr, "patent_type");
            if (!includeDesign && "design".equalsIgnoreCase(patentType)) {
              continue;
            }

            Map<String, Object> row = new HashMap<>();
            row.put("patent_id", strVal(patentId));
            row.put("grant_year", intVal(yearStr));
            row.put("patent_date", strVal(patentDate));
            row.put("patent_type", strVal(patentType));
            row.put("patent_title", strVal(getField(parts, hdr, "patent_title")));
            row.put("num_claims", intVal(getField(parts, hdr, "num_claims")));
            row.put("wipo_kind", strVal(getField(parts, hdr, "wipo_kind")));
            row.put("withdrawn", boolVal(getField(parts, hdr, "withdrawn")));
            count[0]++;
            pending = row;
            return;
          }
          reader.close();
          LOGGER.info("PatentGrants: {} records for year {}", count[0], yearStr);
        } catch (IOException e) {
          try { reader.close(); } catch (IOException closeEx) { LOGGER.debug("close failed", closeEx); }
          throw new UncheckedIOException("PatentGrantsTransformer read failed", e);
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
