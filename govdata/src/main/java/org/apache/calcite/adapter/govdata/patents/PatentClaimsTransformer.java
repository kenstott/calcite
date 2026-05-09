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
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Transforms PatentsView per-year claims ZIP into patent_claims rows.
 *
 * <p>Source: {@code g_claims_{year}.tsv.zip} from the PatentsView claims/ subdirectory.
 * Downloads and caches the file for the requested year, then returns a lazy iterator
 * that emits one row per claim. No intermediate StringWriter — memory is O(chunk_size).
 */
public class PatentClaimsTransformer extends AbstractPatentsTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PatentClaimsTransformer.class);

  @Override
  public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String yearStr = getYear(context);
    if (yearStr == null || yearStr.isEmpty()) {
      LOGGER.warn("PatentClaims: missing year dimension");
      return Collections.emptyIterator();
    }

    String url = context.getUrl();
    File dest = cacheFile("g_claims_" + yearStr + ".tsv");
    if (!isCacheValid(dest)) {
      LOGGER.info("PatentClaims downloading year {}: {}", yearStr, url);
      downloadAndCacheTsv(url, dest);
    } else {
      LOGGER.debug("PatentClaims cache hit for year {}", yearStr);
    }

    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(Files.newInputStream(dest.toPath()), StandardCharsets.UTF_8));
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
            Map<String, Object> row = new HashMap<>();
            row.put("patent_id", strVal(getField(parts, hdr, "patent_id")));
            row.put("grant_year", intVal(yearStr));
            row.put("claim_sequence", intVal(getField(parts, hdr, "claim_sequence")));
            row.put("claim_number", intVal(getField(parts, hdr, "claim_number")));
            row.put("claim_text", strVal(getField(parts, hdr, "claim_text")));
            row.put("dependent", intVal(getField(parts, hdr, "dependent")));
            row.put("exemplary", boolVal(getField(parts, hdr, "exemplary")));
            count[0]++;
            pending = row;
            return;
          }
          reader.close();
          LOGGER.info("PatentClaims: {} records for year {}", count[0], yearStr);
        } catch (IOException e) {
          try { reader.close(); } catch (IOException ignored) { }
          throw new RuntimeException("PatentClaimsTransformer read failed", e);
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
