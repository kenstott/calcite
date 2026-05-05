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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Transforms Nation's Report Card (NAEP) API responses.
 *
 * <p>The NAEP API returns {@code {"result": [...], "jurisdiction": {...}}}
 * where each element in {@code result} is a flat record with jurisdiction,
 * subject, grade, variable_type, subgroup_name, avg_score, etc.
 * The {@code year} and {@code subject} dimensions from the URL path are injected
 * into every row since the response does not always include them.
 */
public class NaepScoresResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NaepScoresResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("NAEP: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      // NAEP API uses "result" (singular) as data key
      JsonNode result = root.isArray() ? root : root.path("result");
      if (!result.isArray()) {
        LOGGER.debug("NAEP: no result array in response for {}", context.getUrl());
        return "[]";
      }

      String year = context.getDimensionValues().get("year");
      String subject = context.getDimensionValues().get("subject");
      String grade = context.getDimensionValues().get("grade");

      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode record : result) {
        if (!record.isObject()) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();

        // Copy all fields from the record
        Iterator<Map.Entry<String, JsonNode>> fields = record.fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> entry = fields.next();
          row.set(entry.getKey(), entry.getValue());
        }

        // Inject dimension values if not already present
        if (year != null && !row.has("year")) {
          try {
            row.put("year", Integer.parseInt(year));
          } catch (NumberFormatException e) {
            LOGGER.warn("NAEP: non-integer year dimension '{}', row will have no year", year);
          }
        }
        if (subject != null && !row.has("subject")) {
          row.put("subject", subject);
        }
        if (grade != null && !row.has("grade")) {
          try {
            row.put("grade", Integer.parseInt(grade));
          } catch (NumberFormatException e) {
            LOGGER.warn("NAEP: non-integer grade dimension '{}', row will have no grade", grade);
          }
        }

        // Normalise jurisdiction field — NAEP uses "statecode" or "fips"
        if (!row.has("jurisdiction") && row.has("statecode")) {
          row.set("jurisdiction", row.get("statecode"));
        }
        if (!row.has("jurisdiction_name") && row.has("statename")) {
          row.set("jurisdiction_name", row.get("statename"));
        }
        if (!row.has("jurisdiction") || row.path("jurisdiction").isNull()) {
          LOGGER.warn("NAEP: jurisdiction is null for year={} subject={} grade={}",
              year, subject, grade);
        }

        out.add(row);
      }

      LOGGER.debug("NAEP: extracted {} records for year={}, subject={}, grade={}",
          out.size(), year, subject, grade);
      return out.toString();

    } catch (Exception e) {
      LOGGER.error("NAEP: transform failed for {}: {}", context.getUrl(), e.getMessage());
      return "[]";
    }
  }
}
