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
 * Transforms the NIST Cybersecurity Framework (CSF) 2.0 OSCAL JSON catalog into flat
 * {@code nist_csf_functions} rows — one row per subcategory (most granular level).
 *
 * <p>Source: OSCAL content from usnistgov/oscal-content on GitHub.
 * 3-level hierarchy in OSCAL:
 * <ul>
 *   <li>groups[] → Functions (GV, ID, PR, DE, RS, RC)</li>
 *   <li>function.controls[] → Categories (e.g., GV.OC)</li>
 *   <li>category.controls[] → Subcategories (e.g., GV.OC-01)</li>
 * </ul>
 *
 * <p>Subcategory description comes from {@code parts[name=statement].prose}.
 * Category label comes from {@code props[name=label].value}.
 */
public class NistCsfResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NistCsfResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.trim().isEmpty()) {
      LOGGER.warn("NistCsf: empty response from {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode groups = root.path("catalog").path("groups");

      if (!groups.isArray()) {
        LOGGER.warn("NistCsf: catalog.groups not found");
        return "[]";
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream(512 * 1024);
      JsonGenerator gen = MAPPER.getFactory().createGenerator(baos);
      gen.writeStartArray();

      int count = 0;
      for (JsonNode function : groups) {
        String functionId = textOrNull(function, "id");
        String functionName = textOrNull(function, "title");
        if (functionId != null) {
          functionId = functionId.toUpperCase();
        }

        JsonNode categories = function.path("controls");
        if (!categories.isArray()) {
          continue;
        }

        for (JsonNode category : categories) {
          String categoryId = textOrNull(category, "id");
          String categoryLabel = extractLabel(category);

          JsonNode subcategories = category.path("controls");
          if (!subcategories.isArray()) {
            continue;
          }

          for (JsonNode sub : subcategories) {
            String subId = textOrNull(sub, "id");
            String subDescription = extractStatementProse(sub.path("parts"));

            ObjectNode row = MAPPER.createObjectNode();
            row.put("function_id", functionId);
            row.put("function_name", functionName);
            row.put("category_id", categoryId != null ? categoryId.toUpperCase() : null);
            row.put("category_name", categoryLabel);
            row.put("subcategory_id", subId != null ? subId.toUpperCase() : null);
            row.put("subcategory_description", subDescription);
            row.put("framework_version", "2.0");
            row.put("source", "nist-csf");

            gen.writeTree(row);
            count++;
          }
        }
      }

      gen.writeEndArray();
      gen.close();

      LOGGER.info("NistCsf: returning {} subcategory rows", count);
      return baos.toString("UTF-8");

    } catch (Exception e) {
      LOGGER.error("NistCsf: failed to parse OSCAL JSON: {}", e.getMessage());
      throw new RuntimeException("Failed to parse NIST CSF OSCAL: " + e.getMessage(), e);
    }
  }

  private static String extractLabel(JsonNode node) {
    JsonNode props = node.path("props");
    if (props.isArray()) {
      for (JsonNode prop : props) {
        if ("label".equals(textOrNull(prop, "name"))) {
          return textOrNull(prop, "value");
        }
      }
    }
    return textOrNull(node, "title");
  }

  private static String extractStatementProse(JsonNode parts) {
    if (!parts.isArray()) {
      return null;
    }
    for (JsonNode part : parts) {
      if ("statement".equals(textOrNull(part, "name"))) {
        String prose = textOrNull(part, "prose");
        if (prose != null) {
          return prose;
        }
      }
    }
    return null;
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
