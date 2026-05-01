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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

/**
 * Transforms the ATT&CK → NIST 800-53 mappings JSON from
 * center-for-threat-informed-defense/mappings-explorer into flat
 * {@code attack_to_nist_mappings} rows.
 *
 * <p>Input structure:
 * <pre>
 * {
 *   "metadata": { "attack_version": "16.1", "mapping_framework_version": "rev5", ... },
 *   "mapping_objects": [
 *     {
 *       "capability_id": "CM-03",
 *       "capability_group": "CM",
 *       "capability_description": "Configuration Change Control",
 *       "mapping_type": "mitigates",
 *       "attack_object_id": "T1666",
 *       "attack_object_name": "Modify Cloud Resource Hierarchy",
 *       "comments": "...",
 *       "status": "complete"
 *     },
 *     ...
 *   ]
 * }
 * </pre>
 *
 * <p>Records with {@code status == "non_mappable"} or null {@code capability_id} are skipped.
 */
public class AttackToNistResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AttackToNistResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.trim().isEmpty()) {
      LOGGER.warn("AttackToNist: empty response from {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      JsonNode metadata = root.path("metadata");
      String attackVersion = textOrNull(metadata, "attack_version");
      String frameworkVersion = textOrNull(metadata, "mapping_framework_version");
      String sourceVersion = (attackVersion != null ? "ATT&CK-" + attackVersion : "")
          + (frameworkVersion != null ? "/NIST-" + frameworkVersion : "");

      JsonNode mappingObjects = root.path("mapping_objects");
      if (!mappingObjects.isArray()) {
        LOGGER.warn("AttackToNist: mapping_objects not an array");
        return "[]";
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream(2 * 1024 * 1024);
      JsonGenerator gen = MAPPER.getFactory().createGenerator(baos);
      gen.writeStartArray();

      int count = 0;
      int skipped = 0;
      for (JsonNode obj : mappingObjects) {
        String capabilityId = textOrNull(obj, "capability_id");
        String status = textOrNull(obj, "status");
        if (capabilityId == null || "non_mappable".equals(status)) {
          skipped++;
          continue;
        }

        ObjectNode row = MAPPER.createObjectNode();
        row.put("technique_id", textOrNull(obj, "attack_object_id"));
        row.put("attack_object_name", textOrNull(obj, "attack_object_name"));
        row.put("nist_control_id", capabilityId);
        row.put("capability_group", textOrNull(obj, "capability_group"));
        row.put("capability_description", textOrNull(obj, "capability_description"));
        row.put("mapping_type", textOrNull(obj, "mapping_type"));
        row.put("comments", textOrNull(obj, "comments"));
        row.put("status", status);
        row.put("source_version", sourceVersion);
        row.put("source", "mappings-explorer");

        gen.writeTree(row);
        count++;
      }

      gen.writeEndArray();
      gen.close();

      LOGGER.info("AttackToNist: returning {} mappings ({} non-mappable skipped)", count, skipped);
      return baos.toString("UTF-8");

    } catch (Exception e) {
      LOGGER.error("AttackToNist: failed to parse response: {}", e.getMessage());
      throw new RuntimeException("Failed to parse ATT&CK→NIST mappings: " + e.getMessage(), e);
    }
  }

  private static String textOrNull(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull() || v.isMissingNode()) {
      return null;
    }
    String t = v.asText();
    return t.isEmpty() ? null : t;
  }
}
