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
 * Transforms Congress.gov API {@code /v3/member} list-endpoint responses into flat rows.
 *
 * <p>Input (per page): {@code {"members": [{bioguideId, state, partyName, district, name,
 * terms: [{chamber, startYear, endYear}, ...], url, depiction: {imageUrl, attribution}}, ...],
 * "pagination": {count, next}}}. The {@code terms} container is read defensively as either a
 * plain JSON array or an XML-translation-style {@code {"item": [...]}} wrapper, since this was
 * written against the published API documentation rather than a live (keyed) response — verify
 * the actual shape against a real response before relying on this in production.
 *
 * <p>Output: one row per member with {@code terms_json} holding the member's chamber service
 * history as a serialized JSON array. {@code current_member} is always null — that field is
 * only available at the item/detail level ({@code /v3/member/{bioguideId}}), not on this list
 * endpoint.
 */
public class CongressMemberTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CongressMemberTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Congress Member: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode members = root.path("members");
      if (!members.isArray()) {
        LOGGER.warn("Congress Member: No members array in response for {}", context.getUrl());
        return "[]";
      }

      ArrayNode result = MAPPER.createArrayNode();
      for (JsonNode member : members) {
        ObjectNode row = MAPPER.createObjectNode();
        row.put("bioguide_id", getTextOrNull(member, "bioguideId"));
        row.put("name_last_first", getTextOrNull(member, "name"));
        row.put("state_name", getTextOrNull(member, "state"));
        row.put("party_name", getTextOrNull(member, "partyName"));
        putIntOrNull(row, "district", member, "district");
        row.putNull("current_member");
        row.put("terms_json", extractTermsJson(member));
        row.put("image_url", member.path("depiction").path("imageUrl").asText(null));
        row.put("member_url", getTextOrNull(member, "url"));
        result.add(row);
      }

      LOGGER.debug("Congress Member: Transformed {} members", result.size());
      return result.toString();

    } catch (Exception e) {
      throw new RuntimeException("Congress Member: Failed to parse response for "
          + context.getUrl(), e);
    }
  }

  private String extractTermsJson(JsonNode member) {
    JsonNode terms = member.path("terms");
    JsonNode items = terms.isArray() ? terms : terms.path("item");
    if (!items.isArray()) {
      return "[]";
    }

    ArrayNode flatTerms = MAPPER.createArrayNode();
    for (JsonNode term : items) {
      ObjectNode flat = MAPPER.createObjectNode();
      flat.put("chamber", getTextOrNull(term, "chamber"));
      putIntOrNull(flat, "start_year", term, "startYear");
      putIntOrNull(flat, "end_year", term, "endYear");
      flatTerms.add(flat);
    }
    return flatTerms.toString();
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
    } else if (value != null && value.isTextual() && !value.asText().isEmpty()) {
      try {
        row.put(key, Integer.parseInt(value.asText().trim()));
      } catch (NumberFormatException e) {
        row.putNull(key);
      }
    } else {
      row.putNull(key);
    }
  }
}
