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
package org.apache.calcite.adapter.govdata.cyber.threat;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Transforms MITRE ATT&amp;CK STIX2 bundle JSON into flat {@code attack_techniques} rows.
 *
 * <p>Two-pass processing:
 * <ol>
 *   <li>Index all {@code attack-pattern} objects: stix_id → technique_id</li>
 *   <li>Index {@code subtechnique-of} relationships: child stix_id → parent technique_id</li>
 * </ol>
 * Then emits one row per non-revoked, non-deprecated attack-pattern.
 */
public class MitreAttackResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MitreAttackResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.trim().isEmpty()) {
      LOGGER.warn("MitreAttack: empty response");
      return "[]";
    }

    JsonNode bundle;
    try {
      bundle = MAPPER.readTree(response);
    } catch (Exception e) {
      LOGGER.error("MitreAttack: failed to parse STIX bundle: {}", e.getMessage());
      return "[]";
    }

    JsonNode objects = bundle.path("objects");
    if (!objects.isArray()) {
      LOGGER.warn("MitreAttack: bundle.objects is missing or not an array");
      return "[]";
    }

    // Pass 1: stix_id → technique_id for all attack-patterns
    Map<String, String> stixIdToTechId = new HashMap<String, String>();
    for (JsonNode obj : objects) {
      if (!"attack-pattern".equals(obj.path("type").asText())) {
        continue;
      }
      String stixId = obj.path("id").asText(null);
      String techId = extractTechniqueId(obj);
      if (stixId != null && techId != null) {
        stixIdToTechId.put(stixId, techId);
      }
    }

    // Pass 2: child stix_id → parent technique_id from subtechnique-of relationships
    Map<String, String> childToParentTechId = new HashMap<String, String>();
    for (JsonNode obj : objects) {
      if (!"relationship".equals(obj.path("type").asText())) {
        continue;
      }
      if (!"subtechnique-of".equals(obj.path("relationship_type").asText())) {
        continue;
      }
      String childStixId = obj.path("source_ref").asText(null);
      String parentStixId = obj.path("target_ref").asText(null);
      if (childStixId != null && parentStixId != null) {
        String parentTechId = stixIdToTechId.get(parentStixId);
        if (parentTechId != null) {
          childToParentTechId.put(childStixId, parentTechId);
        }
      }
    }

    // Pass 3: emit rows
    ArrayNode rows = MAPPER.createArrayNode();
    for (JsonNode obj : objects) {
      if (!"attack-pattern".equals(obj.path("type").asText())) {
        continue;
      }
      // Skip revoked and deprecated entries
      if (obj.path("revoked").asBoolean(false)
          || obj.path("x_mitre_deprecated").asBoolean(false)) {
        continue;
      }
      String stixId = obj.path("id").asText(null);
      String techId = extractTechniqueId(obj);
      if (techId == null) {
        continue;
      }

      ObjectNode row = MAPPER.createObjectNode();
      row.put("technique_id", techId);
      row.put("stix_id", stixId);
      row.put("name", obj.path("name").asText(null));
      row.put("description", textTruncate(obj.path("description").asText(null), 2000));
      row.put("tactic_short_names", joinKillChainPhases(obj.path("kill_chain_phases")));
      row.put("platforms", joinStringArray(obj.path("x_mitre_platforms")));
      row.put("data_sources", joinStringArray(obj.path("x_mitre_data_sources")));
      row.put("is_subtechnique", obj.path("x_mitre_is_subtechnique").asBoolean(false));
      row.put("parent_technique_id",
          stixId != null ? childToParentTechId.get(stixId) : null);
      row.put("domain", firstDomain(obj.path("x_mitre_domains")));
      row.put("detection", textTruncate(obj.path("x_mitre_detection").asText(null), 2000));
      row.put("mitre_version", obj.path("x_mitre_version").asText(null));
      rows.add(row);
    }

    LOGGER.info("MitreAttack: emitting {} technique rows", rows.size());
    return rows.toString();
  }

  private String extractTechniqueId(JsonNode obj) {
    JsonNode refs = obj.path("external_references");
    if (!refs.isArray()) {
      return null;
    }
    for (JsonNode ref : refs) {
      if ("mitre-attack".equals(ref.path("source_name").asText())) {
        String extId = ref.path("external_id").asText(null);
        if (extId != null && extId.startsWith("T")) {
          return extId;
        }
      }
    }
    return null;
  }

  private String joinKillChainPhases(JsonNode phases) {
    if (!phases.isArray() || phases.size() == 0) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (JsonNode phase : phases) {
      String name = phase.path("phase_name").asText(null);
      if (name != null) {
        if (sb.length() > 0) {
          sb.append("|");
        }
        sb.append(name);
      }
    }
    return sb.length() > 0 ? sb.toString() : null;
  }

  private String joinStringArray(JsonNode arr) {
    if (!arr.isArray() || arr.size() == 0) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (JsonNode item : arr) {
      String val = item.asText(null);
      if (val != null) {
        if (sb.length() > 0) {
          sb.append("|");
        }
        sb.append(val);
      }
    }
    return sb.length() > 0 ? sb.toString() : null;
  }

  private String firstDomain(JsonNode domains) {
    if (!domains.isArray() || domains.size() == 0) {
      return null;
    }
    return domains.get(0).asText(null);
  }

  private String textTruncate(String text, int maxLen) {
    if (text == null || text.isEmpty()) {
      return null;
    }
    return text.length() > maxLen ? text.substring(0, maxLen) : text;
  }
}
