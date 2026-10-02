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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transforms BEA Regional dataset responses, turning suppressed cells into explicit markers.
 *
 * <p>The Regional API never sends a marker string in {@code DataValue}. A suppressed or
 * unavailable cell arrives as {@code DataValue "0"} with BEA's marker in {@code NoteRef}
 * (for example {@code "(D)"} or {@code "(NA) 9 *"}), so reading {@code DataValue} alone turns
 * every suppressed cell into a real-looking zero. This transformer sets {@code DataValue} to
 * null on such rows and copies the marker verbatim into {@code ValueFlag}; a genuine zero
 * (no marker in {@code NoteRef}) is left untouched.
 */
public class BeaRegionalResponseTransformer extends BeaResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern MARKER = Pattern.compile("\\((?:D|L|NA|NM|T)\\)");

  @Override public String transform(String response, RequestContext context) {
    String data = super.transform(response, context);
    try {
      JsonNode rows = MAPPER.readTree(data);
      if (!rows.isArray()) {
        return data;
      }
      for (JsonNode row : rows) {
        Matcher marker = MARKER.matcher(row.path("NoteRef").asText(""));
        if (marker.find()) {
          ((ObjectNode) row).putNull("DataValue");
          ((ObjectNode) row).put("ValueFlag", marker.group());
        }
      }
      return rows.toString();
    } catch (java.io.IOException e) {
      throw new RuntimeException("Failed to parse BEA Regional response: " + e.getMessage(), e);
    }
  }
}
