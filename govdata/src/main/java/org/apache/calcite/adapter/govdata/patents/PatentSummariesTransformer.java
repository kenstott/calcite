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
import java.util.regex.Pattern;

/**
 * Transforms PatentsView per-year brief summary ZIP into patent_summaries rows.
 *
 * <p>Source: {@code g_brf_sum_text_{year}.tsv.zip} from the PatentsView
 * brief-summary-text/ subdirectory. Downloads and caches the file for the requested year,
 * then returns a lazy iterator emitting one row per patent.
 * No intermediate StringWriter — memory is O(chunk_size).
 */
public class PatentSummariesTransformer extends AbstractPatentsTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PatentSummariesTransformer.class);

  private static final Pattern PATENT_ID_PATTERN = Pattern.compile("[A-Za-z]{0,2}\\d+");

  @Override
  public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String yearStr = getYear(context);
    if (yearStr == null || yearStr.isEmpty()) {
      LOGGER.warn("PatentSummaries: missing year dimension");
      return Collections.emptyIterator();
    }

    String url = context.getUrl();
    String dest = cacheFile("g_brf_sum_text_" + yearStr + ".tsv");
    if (!isCacheValid(dest)) {
      LOGGER.info("PatentSummaries downloading year {}: {}", yearStr, url);
      downloadAndCacheTsv(url, dest);
    } else {
      LOGGER.debug("PatentSummaries cache hit for year {}", yearStr);
    }

    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(storageProvider().openInputStream(dest), StandardCharsets.UTF_8));
    String headerLine = reader.readLine();
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
          while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) {
              continue;
            }
            String[] parts = splitTsv(line);
            if (parts.length < 2) {
              continue;
            }
            String patentId = getField(parts, hdr, "patent_id");
            if (patentId == null || patentId.isEmpty()
                || !PATENT_ID_PATTERN.matcher(patentId).matches()) {
              continue;
            }
            Map<String, Object> row = new HashMap<>();
            row.put("patent_id", strVal(patentId));
            row.put("grant_year", intVal(yearStr));
            row.put("summary_text", strVal(getField(parts, hdr, "summary_text")));
            count[0]++;
            pending = row;
            return;
          }
          reader.close();
          LOGGER.info("PatentSummaries: {} records for year {}", count[0], yearStr);
        } catch (IOException e) {
          try { reader.close(); } catch (IOException closeEx) { LOGGER.debug("close failed", closeEx); }
          throw new RuntimeException("PatentSummariesTransformer read failed", e);
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
