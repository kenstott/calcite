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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Transforms the RxNorm allconcepts bulk endpoint response.
 *
 * <p>URL: rxnav.nlm.nih.gov/REST/allconcepts.json?tty=IN+BN+SCD
 * Response: { "minConceptGroup": { "minConcept": [ { "rxcui": "...", "name": "...", "tty": "..." },
 * ... ] } }
 * Each element maps to one rxnorm_drugs row.
 */
public class RxNormDrugsResponseTransformer implements ResponseTransformer {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode concepts = root.path("minConceptGroup").path("minConcept");
      ArrayNode out = MAPPER.createArrayNode();

      if (!concepts.isArray()) {
        return "[]";
      }

      for (JsonNode concept : concepts) {
        ObjectNode row = MAPPER.createObjectNode();
        put(row, "rxcui", text(concept, "rxcui"));
        put(row, "name", text(concept, "name"));
        put(row, "tty", text(concept, "tty"));
        put(row, "type", "rxnorm_drugs");
        out.add(row);
      }

      return out.toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to transform RxNorm response", e);
    }
  }

  private static String text(JsonNode node, String field) {
    JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? null : value.asText(null);
  }

  private static void put(ObjectNode row, String key, String value) {
    if (value == null) {
      row.putNull(key);
    } else {
      row.put(key, value);
    }
  }
}
