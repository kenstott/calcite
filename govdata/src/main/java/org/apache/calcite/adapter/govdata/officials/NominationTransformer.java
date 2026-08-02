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
package org.apache.calcite.adapter.govdata.officials;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms Congress.gov API {@code /v3/nomination/{congress}} list-endpoint responses into
 * flat rows.
 *
 * <p>Verified against a live keyed response (2026-08-01): {@code {"nominations": [{citation,
 * congress, number, partNumber, organization, description, receivedDate, nominationType:
 * {isCivilian}, latestAction: {actionDate, text}, updateDate}, ...], "pagination": {...}}}.
 * {@code description} is present for individually-named nominees but absent for
 * {@code isList=true} batch/class nominations (career-service promotion lists) — that flag
 * itself is only visible on the per-nomination detail endpoint, not fetched here. {@code
 * isMilitary} and {@code isPrivileged} were never observed populated in live samples (only
 * {@code isCivilian: true} appeared), so both are passed through as-null-if-absent rather than
 * defaulted to false.
 */
public class NominationTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NominationTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Nomination: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode nominations = root.path("nominations");
      if (!nominations.isArray()) {
        LOGGER.warn("Nomination: No nominations array in response for {}", context.getUrl());
        return "[]";
      }

      ArrayNode result = MAPPER.createArrayNode();
      for (JsonNode nomination : nominations) {
        ObjectNode row = MAPPER.createObjectNode();
        row.put("citation", getTextOrNull(nomination, "citation"));
        putIntOrNull(row, "congress", nomination, "congress");
        putIntOrNull(row, "number", nomination, "number");
        row.put("part_number", getTextOrNull(nomination, "partNumber"));
        row.put("organization", getTextOrNull(nomination, "organization"));
        row.put("description", getTextOrNull(nomination, "description"));

        JsonNode nominationType = nomination.path("nominationType");
        putBooleanOrNull(row, "is_civilian", nominationType, "isCivilian");
        putBooleanOrNull(row, "is_privileged", nomination, "isPrivileged");

        row.put("received_date", getTextOrNull(nomination, "receivedDate"));

        JsonNode latestAction = nomination.path("latestAction");
        row.put("latest_action_date", getTextOrNull(latestAction, "actionDate"));
        row.put("latest_action_text", getTextOrNull(latestAction, "text"));

        row.put("update_date", getTextOrNull(nomination, "updateDate"));
        result.add(row);
      }

      LOGGER.debug("Nomination: Transformed {} nominations", result.size());
      return result.toString();

    } catch (Exception e) {
      throw new RuntimeException("Nomination: Failed to parse response for "
          + context.getUrl(), e);
    }
  }

  private static String getTextOrNull(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    return value.asText();
  }

  private static void putIntOrNull(ObjectNode row, String key, JsonNode source, String field) {
    JsonNode value = source.get(field);
    if (value != null && value.isNumber()) {
      row.put(key, value.intValue());
    } else {
      row.putNull(key);
    }
  }

  private static void putBooleanOrNull(ObjectNode row, String key, JsonNode source,
      String field) {
    JsonNode value = source.get(field);
    if (value != null && value.isBoolean()) {
      row.put(key, value.booleanValue());
    } else {
      row.putNull(key);
    }
  }
}
