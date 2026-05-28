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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transforms NCES IPEDS Completions bulk CSV responses.
 *
 * <p>NCES distributes Completions data as ZIP archives (C{year}_A.zip) containing
 * wide-format CSV files. Each row represents one (unitid, cipcode, majornum, award_level)
 * combination with race/ethnicity and sex counts as separate columns. This transformer:
 * <ol>
 *   <li>Parses the CSV (reuses {@link IpedsFinancialsResponseTransformer#parseCsv})</li>
 *   <li>Skips X-prefixed imputation flag columns</li>
 *   <li>Maps NCES uppercase column names to canonical schema column names</li>
 *   <li>Injects {@code year} from the URL dimension context</li>
 * </ol>
 */
public class IpedsCompletionsResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IpedsCompletionsResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Map<String, String> COLUMN_MAP;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("UNITID",  "unitid");
    m.put("CIPCODE", "cipcode");
    m.put("MAJORNUM", "majornum");
    m.put("AWLEVEL", "award_level");
    m.put("CTOTALT", "ctotalt");
    m.put("CTOTALM", "ctotalm");
    m.put("CTOTALW", "ctotalw");
    m.put("CAIANT",  "caiant");
    m.put("CAIANM",  "caianm");
    m.put("CAIANW",  "caianw");
    m.put("CASIAT",  "casiat");
    m.put("CASIAM",  "casiam");
    m.put("CASIAW",  "casiaw");
    m.put("CBKAAT",  "cbkaat");
    m.put("CBKAAM",  "cbkaam");
    m.put("CBKAAW",  "cbkaaw");
    m.put("CHISPT",  "chispt");
    m.put("CHISPM",  "chispm");
    m.put("CHISPW",  "chispw");
    m.put("CNHPIT",  "cnhpit");
    m.put("CNHPIM",  "cnhpim");
    m.put("CNHPIW",  "cnhpiw");
    m.put("CWHITT",  "cwhitt");
    m.put("CWHITM",  "cwhitm");
    m.put("CWHITW",  "cwhitw");
    m.put("C2MORT",  "c2mort");
    m.put("C2MORM",  "c2morm");
    m.put("C2MORW",  "c2morw");
    m.put("CUNKNT",  "cunknt");
    m.put("CUNKNM",  "cunknm");
    m.put("CUNKNW",  "cunknw");
    m.put("CNRALT",  "cnralt");
    m.put("CNRALM",  "cnralm");
    m.put("CNRALW",  "cnralw");
    COLUMN_MAP = Collections.unmodifiableMap(m);
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("IPEDS Completions: empty response for {}", context.getUrl());
      return "[]";
    }

    String yearStr = context.getDimensionValues().get("year");

    try {
      List<String[]> rows = IpedsFinancialsResponseTransformer.parseCsv(response);
      if (rows.size() < 2) {
        LOGGER.debug("IPEDS Completions: no data rows for year={}", yearStr);
        return "[]";
      }

      String[] header = rows.get(0);
      for (int i = 0; i < header.length; i++) {
        String h = header[i].trim().toUpperCase();
        if (i == 0 && !h.isEmpty() && h.charAt(0) == '﻿') {
          h = h.substring(1);
        }
        header[i] = h;
      }

      ArrayNode out = MAPPER.createArrayNode();
      for (int r = 1; r < rows.size(); r++) {
        String[] values = rows.get(r);
        ObjectNode row = MAPPER.createObjectNode();

        if (yearStr != null) {
          try {
            row.put("year", Integer.parseInt(yearStr));
          } catch (NumberFormatException e) {
            LOGGER.warn("IPEDS Completions: non-integer year '{}' for {}", yearStr,
                context.getUrl());
          }
        }

        for (int c = 0; c < header.length && c < values.length; c++) {
          String col = header[c];

          if (col.startsWith("X")) {
            continue;
          }

          String canonical = COLUMN_MAP.get(col);
          if (canonical == null) {
            continue;
          }

          String rawValue = values[c].trim();
          if (rawValue.isEmpty()) {
            row.putNull(canonical);
            continue;
          }

          if ("cipcode".equals(canonical)) {
            row.put(canonical, rawValue);
          } else {
            try {
              row.put(canonical, Integer.parseInt(rawValue));
            } catch (NumberFormatException e) {
              row.putNull(canonical);
            }
          }
        }

        if (row.has("unitid") && !row.path("unitid").isNull()) {
          out.add(row);
        }
      }

      LOGGER.debug("IPEDS Completions: {} rows for year={}", out.size(), yearStr);
      return out.toString();

    } catch (Exception e) {
      LOGGER.error("IPEDS Completions: transform failed for {}: {}", context.getUrl(),
          e.getMessage());
      return "[]";
    }
  }
}
