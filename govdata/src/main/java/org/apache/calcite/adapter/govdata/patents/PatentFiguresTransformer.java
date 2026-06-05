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
 * Faithful recreation of PatentsView g_figures.tsv as patent_figures rows — one row per patent
 * figure/sheet count record, keyed by patent_id, no joins and no year. Loaded once as a
 * snapshot; join to patent_grants on patent_id for a grant year.
 */
public class PatentFiguresTransformer extends AbstractPatentsTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PatentFiguresTransformer.class);

  @Override
  public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String q = quarterToken(context);
    final String figuresFile = downloadAndCacheTsv(
        ODP_PVGPATDIS_BASE + "g_figures.tsv.zip", cacheFile("g_figures_" + q + ".tsv"));

    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(figuresFile),
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
            row.put("num_figures", intVal(getField(parts, hdr, "num_figures")));
            row.put("num_sheets", intVal(getField(parts, hdr, "num_sheets")));
            count[0]++;
            pending = row;
            return;
          }
          reader.close();
          LOGGER.info("PatentFigures: {} records (snapshot)", count[0]);
        } catch (IOException e) {
          try { reader.close(); } catch (IOException closeEx) { LOGGER.debug("close failed", closeEx); }
          throw new UncheckedIOException("PatentFiguresTransformer read failed", e);
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
