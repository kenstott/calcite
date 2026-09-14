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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms BLS API /v2/timeseries/data/ JSON responses into TSV-compatible format
 * matching the bulk file schema (series_id, year, period, value, footnote_codes).
 *
 * Input: BLS API JSON with Results.series[*].data[*] structure
 * Output: JSON array of flat rows with series_id, year, period, value, footnote_codes
 */
public class OewsApiTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(OewsApiTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("OEWS API: empty response");
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode rows = MAPPER.createArrayNode();

      JsonNode results = root.path("Results");
      JsonNode series = results.path("series");

      if (!series.isArray()) {
        LOGGER.warn("OEWS API: no series in response");
        return "[]";
      }

      for (JsonNode seriesEntry : series) {
        String seriesId = seriesEntry.path("seriesID").asText();
        if (seriesId == null || seriesId.isEmpty()) {
          continue;
        }

        JsonNode data = seriesEntry.path("data");
        if (!data.isArray()) {
          continue;
        }

        for (JsonNode dataPoint : data) {
          ObjectNode row = MAPPER.createObjectNode();
          row.put("series_id", seriesId);
          row.put("year", dataPoint.path("year").asText());
          row.put("period", dataPoint.path("period").asText());
          row.put("value", dataPoint.path("value").asText());

          // Flatten footnotes array into comma-separated string
          JsonNode footnotes = dataPoint.path("footnotes");
          if (footnotes.isArray() && footnotes.size() > 0) {
            StringBuilder codes = new StringBuilder();
            for (JsonNode fn : footnotes) {
              if (codes.length() > 0) codes.append(",");
              codes.append(fn.path("code").asText());
            }
            row.put("footnote_codes", codes.toString());
          } else {
            row.putNull("footnote_codes");
          }

          rows.add(row);
        }
      }

      String result = rows.toString();
      LOGGER.info("OEWS API: transformed {} rows", rows.size());
      return result;

    } catch (Exception e) {
      LOGGER.error("OEWS API: failed to transform response", e);
      throw new RuntimeException("OEWS API transformation failed: " + e.getMessage(), e);
    }
  }
}
