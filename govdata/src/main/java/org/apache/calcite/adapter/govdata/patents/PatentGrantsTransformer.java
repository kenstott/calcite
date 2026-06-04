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
import java.util.Set;

/**
 * Transforms PatentsView g_patent.tsv bulk download into patent_grants rows.
 *
 * <p>Downloads and caches 4 full-dump files (g_patent, g_application, g_patent_abstract,
 * g_figures). Pass 1 (eager): collects patent_ids for the year and loads auxiliary lookups.
 * Returns a lazy iterator streaming g_patent.tsv pass 2, emitting one row per patent.
 * No intermediate StringWriter — memory is O(lookup_size + chunk_size).
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
    String applicationFile = downloadAndCacheTsv(
        ODP_PVGPATDIS_BASE + "g_application.tsv.zip", cacheFile("g_application_" + q + ".tsv"));
    String abstractFile = downloadAndCacheTsv(
        ODP_PVGPATDIS_BASE + "g_patent_abstract.tsv.zip",
        cacheFile("g_patent_abstract_" + q + ".tsv"));
    String figuresFile = downloadAndCacheTsv(
        ODP_PVGPATDIS_BASE + "g_figures.tsv.zip", cacheFile("g_figures_" + q + ".tsv"));

    final Set<String> patentIds = readPatentIdsForYear(patentFile, yearStr);
    LOGGER.info("PatentGrants: {} patent IDs for year {}", patentIds.size(), yearStr);

    final Map<String, Map<String, String>> applications = readTsvAsLookupForKeys(
        applicationFile, "patent_id", patentIds, "filing_date", "patent_application_type");
    final Map<String, Map<String, String>> abstracts = readTsvAsLookupForKeys(
        abstractFile, "patent_id", patentIds, "patent_abstract");
    final Map<String, Map<String, String>> figures = readTsvAsLookupForKeys(
        figuresFile, "patent_id", patentIds, "num_figures", "num_sheets");

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
            String patentId = getField(parts, hdr, "patent_id");
            if (patentId == null || !patentIds.contains(patentId)) {
              continue;
            }
            String patentType = getField(parts, hdr, "patent_type");
            if (!includeDesign && "design".equalsIgnoreCase(patentType)) {
              continue;
            }
            Map<String, String> app = applications.get(patentId);
            Map<String, String> abs = abstracts.get(patentId);
            Map<String, String> fig = figures.get(patentId);

            Map<String, Object> row = new HashMap<>();
            row.put("patent_id", strVal(patentId));
            row.put("grant_year", intVal(yearStr));
            row.put("patent_date", strVal(getField(parts, hdr, "patent_date")));
            row.put("patent_type", strVal(patentType));
            row.put("patent_title", strVal(getField(parts, hdr, "patent_title")));
            row.put("num_claims", intVal(getField(parts, hdr, "num_claims")));
            row.put("wipo_kind", strVal(getField(parts, hdr, "wipo_kind")));
            row.put("withdrawn", boolVal(getField(parts, hdr, "withdrawn")));
            row.put("filing_date", strVal(app != null ? app.get("filing_date") : null));
            row.put("patent_application_type",
                strVal(app != null ? app.get("patent_application_type") : null));
            row.put("patent_abstract",
                strVal(abs != null ? abs.get("patent_abstract") : null));
            row.put("num_figures", intVal(fig != null ? fig.get("num_figures") : null));
            row.put("num_sheets", intVal(fig != null ? fig.get("num_sheets") : null));
            row.put("num_us_citations", null);
            row.put("num_foreign_citations", null);
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
