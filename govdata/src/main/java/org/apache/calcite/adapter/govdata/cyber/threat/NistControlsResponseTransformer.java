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
 * Transforms the NIST SP 800-53 Rev 5 OSCAL JSON catalog into flat
 * {@code nist_controls} rows — one row per base control and one per enhancement.
 *
 * <p>Source: OSCAL content from usnistgov/oscal-content on GitHub.
 * Structure: {@code catalog → groups[] (families) → controls[] (base controls)
 * → controls[] (enhancements)}.
 *
 * <p>Control ID is taken from {@code props[name=label, no class].value}
 * (e.g., {@code "AC-1"}), falling back to uppercasing the {@code id} field.
 * Description is assembled by recursively collecting {@code prose} from all
 * {@code statement} and {@code item} parts.
 */
public class NistControlsResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NistControlsResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.trim().isEmpty()) {
      LOGGER.warn("NistControls: empty response from {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode catalog = root.path("catalog");
      JsonNode groups = catalog.path("groups");

      if (!groups.isArray()) {
        LOGGER.warn("NistControls: catalog.groups not an array");
        return "[]";
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream(4 * 1024 * 1024);
      JsonGenerator gen = MAPPER.getFactory().createGenerator(baos);
      gen.writeStartArray();

      int[] count = {0};
      for (JsonNode group : groups) {
        String familyCode = textOrNull(group, "id");
        String familyName = textOrNull(group, "title");
        if (familyCode != null) {
          familyCode = familyCode.toUpperCase();
        }

        JsonNode controls = group.path("controls");
        if (controls.isArray()) {
          for (JsonNode control : controls) {
            writeControl(control, familyCode, familyName, null, gen, count);
            // Enhancements are nested in control.controls[]
            JsonNode enhancements = control.path("controls");
            if (enhancements.isArray()) {
              String parentId = extractLabel(control);
              for (JsonNode enh : enhancements) {
                writeControl(enh, familyCode, familyName, parentId, gen, count);
              }
            }
          }
        }
      }

      gen.writeEndArray();
      gen.close();

      LOGGER.info("NistControls: returning {} control rows", count[0]);
      return baos.toString("UTF-8");

    } catch (Exception e) {
      LOGGER.error("NistControls: failed to parse OSCAL JSON: {}", e.getMessage());
      throw new RuntimeException("Failed to parse NIST 800-53 OSCAL: " + e.getMessage(), e);
    }
  }

  private void writeControl(JsonNode control, String familyCode, String familyName,
      String enhancementOf, JsonGenerator gen, int[] count) throws Exception {
    String label = extractLabel(control);
    if (label == null) {
      return;
    }

    ObjectNode row = MAPPER.createObjectNode();
    row.put("control_id", label);
    row.put("family_code", familyCode);
    row.put("family_name", familyName);
    row.put("title", textOrNull(control, "title"));
    row.put("description", collectStatementProse(control.path("parts")));
    row.put("enhancement_of", enhancementOf);
    row.put("framework", "NIST SP 800-53");
    row.put("version", "Rev 5");
    row.put("source", "nist-oscal");

    gen.writeTree(row);
    count[0]++;
  }

  /**
   * Extracts the human-readable label (e.g., "AC-1") from props.
   * Prefers the label prop without the "zero-padded" class; falls back to uppercasing id.
   */
  private static String extractLabel(JsonNode control) {
    JsonNode props = control.path("props");
    if (props.isArray()) {
      // Prefer label without zero-padded class
      for (JsonNode prop : props) {
        if ("label".equals(textOrNull(prop, "name"))) {
          String cls = textOrNull(prop, "class");
          if (!"zero-padded".equals(cls)) {
            String val = textOrNull(prop, "value");
            if (val != null) {
              return val;
            }
          }
        }
      }
      // Fall back to any label
      for (JsonNode prop : props) {
        if ("label".equals(textOrNull(prop, "name"))) {
          String val = textOrNull(prop, "value");
          if (val != null) {
            return val;
          }
        }
      }
    }
    // Last resort: uppercase the id
    String id = textOrNull(control, "id");
    if (id == null) {
      return null;
    }
    int dash = id.indexOf('-');
    if (dash < 0) {
      return id.toUpperCase();
    }
    return id.substring(0, dash).toUpperCase() + id.substring(dash).toUpperCase();
  }

  /**
   * Recursively collects prose text from parts with name "statement" or "item".
   * Returns a single string with sentences joined by space, truncated to 4000 chars.
   */
  private static String collectStatementProse(JsonNode parts) {
    if (!parts.isArray() || parts.size() == 0) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    collectProse(parts, sb, 4000);
    String result = sb.toString().trim();
    return result.isEmpty() ? null : result;
  }

  private static void collectProse(JsonNode parts, StringBuilder sb, int maxLen) {
    for (JsonNode part : parts) {
      if (sb.length() >= maxLen) {
        return;
      }
      String name = textOrNull(part, "name");
      if ("statement".equals(name) || "item".equals(name)) {
        String prose = textOrNull(part, "prose");
        if (prose != null) {
          if (sb.length() > 0) {
            sb.append(" ");
          }
          sb.append(prose);
        }
        JsonNode subParts = part.path("parts");
        if (subParts.isArray()) {
          collectProse(subParts, sb, maxLen);
        }
      }
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
