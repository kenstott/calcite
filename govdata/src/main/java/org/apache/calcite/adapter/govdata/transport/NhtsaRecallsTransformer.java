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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms the DOT Socrata "Recalls Data" ({@code 6axg-epim}) SODA JSON into
 * {@code vehicle_recalls} rows.
 *
 * <p>Input is a top-level JSON array of flat recall objects; the only nested
 * field is {@code recall_link} ({@code {url, description}}), from which the
 * detail page URL is lifted to {@code recall_url}. SODA omits null-valued keys,
 * so every field is read defensively. Date fields arrive as ISO8601 timestamps
 * (e.g. {@code 2026-07-07T00:00:00.000}) and are trimmed to a {@code YYYY-MM-DD}
 * date literal for the {@code date}-typed column.
 */
public class NhtsaRecallsTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NhtsaRecallsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("vehicle_recalls: empty response from Socrata");
      return "[]";
    }
    try {
      JsonNode root = MAPPER.readTree(response);
      if (!root.isArray()) {
        LOGGER.warn("vehicle_recalls: expected a JSON array, got {}", root.getNodeType());
        return "[]";
      }
      ArrayNode result = MAPPER.createArrayNode();
      for (JsonNode rec : root) {
        ObjectNode row = MAPPER.createObjectNode();
        putText(row, "nhtsa_id", rec, "nhtsa_id");
        putDate(row, "report_received_date", rec, "report_received_date");
        putText(row, "manufacturer", rec, "manufacturer");
        putText(row, "subject", rec, "subject");
        putText(row, "component", rec, "component");
        putText(row, "mfr_campaign_number", rec, "mfr_campaign_number");
        putText(row, "recall_type", rec, "recall_type");
        putLong(row, "potentially_affected", rec, "potentially_affected");
        putText(row, "recall_description", rec, "defect_summary");
        putText(row, "consequence_summary", rec, "consequence_summary");
        putText(row, "corrective_action", rec, "corrective_action");
        putText(row, "park_outside_advisory", rec, "fire_risk_when_parked");
        putText(row, "do_not_drive_advisory", rec, "do_not_drive");
        JsonNode link = rec.path("recall_link");
        JsonNode url = link.path("url");
        if (url.isMissingNode() || url.isNull() || url.asText().isEmpty()) {
          row.putNull("recall_url");
        } else {
          row.put("recall_url", url.asText());
        }
        result.add(row);
      }
      LOGGER.debug("vehicle_recalls: transformed {} recalls", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      LOGGER.error("vehicle_recalls: failed to transform Socrata response: {}", e.getMessage(), e);
      throw new RuntimeException("vehicle_recalls transform failed", e);
    }
  }

  private static void putText(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull() || v.asText().isEmpty()) {
      row.putNull(col);
    } else {
      row.put(col, v.asText());
    }
  }

  private static void putDate(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull() || v.asText().isEmpty()) {
      row.putNull(col);
      return;
    }
    String s = v.asText();
    int t = s.indexOf('T');
    row.put(col, t > 0 ? s.substring(0, t) : s);
  }

  private static void putLong(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
      return;
    }
    if (v.isNumber()) {
      row.put(col, v.asLong());
      return;
    }
    String s = v.asText().trim();
    if (s.isEmpty()) {
      row.putNull(col);
      return;
    }
    try {
      row.put(col, (long) Double.parseDouble(s));
    } catch (NumberFormatException e) {
      row.putNull(col);
    }
  }
}
