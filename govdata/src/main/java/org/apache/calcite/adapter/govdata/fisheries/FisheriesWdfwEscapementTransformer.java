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
package org.apache.calcite.adapter.govdata.fisheries;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/**
 * Types the WDFW Salmonid Population Indicators (SPI) escapement dataset served by data.wa.gov
 * (Socrata resource fgyz-n3uk), which returns every value as a string.
 *
 * <p>Series rows that carry no estimate (no {@code year} or no {@code abundance_qty}) are series
 * metadata with nothing to count, so they are not emitted. A row that does carry both must have
 * numeric values: anything else fails the load.
 */
public class FisheriesWdfwEscapementTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      throw new IllegalStateException("WDFW escapement: empty response for " + context.getUrl());
    }
    JsonNode rows;
    try {
      rows = MAPPER.readTree(response);
    } catch (IOException e) {
      throw new IllegalStateException("WDFW escapement: unparseable response for "
          + context.getUrl(), e);
    }
    if (!rows.isArray()) {
      throw new IllegalStateException("WDFW escapement: expected a JSON array from "
          + context.getUrl() + " (starts: "
          + response.substring(0, Math.min(200, response.length())) + ")");
    }

    ArrayNode result = MAPPER.createArrayNode();
    for (JsonNode r : rows) {
      if (!r.hasNonNull("year") || !r.hasNonNull("abundance_qty")) {
        continue;
      }
      ObjectNode row = MAPPER.createObjectNode();
      row.put("stock_number", requiredLong(r, "stock_number"));
      row.put("population_name", requiredText(r, "population_name"));
      putNullable(row, "sub_population_name", r.path("sub_population_name").asText(""));
      row.put("data_series_num", requiredLong(r, "data_series_num"));
      row.put("species", requiredText(r, "species"));
      row.put("year", requiredLong(r, "year"));
      row.put("abundance_qty", requiredDouble(r, "abundance_qty"));
      row.put("data_type", requiredText(r, "data_type"));
      row.put("production_type", requiredText(r, "production_type"));
      row.put("calculation_type", requiredText(r, "calculation_type"));
      row.put("escapement_methodology", requiredText(r, "escapement_methodology"));
      putNullable(row, "report_types", r.path("report_types").asText(""));
      putNullable(row, "last_update", r.path("last_update").asText(""));
      row.put("state_abbr", "WA");
      result.add(row);
    }
    if (result.size() == 0) {
      throw new IllegalStateException("WDFW escapement: no estimate rows in "
          + context.getUrl());
    }
    try {
      return MAPPER.writeValueAsString(result);
    } catch (IOException e) {
      throw new IllegalStateException("WDFW escapement: cannot serialize rows", e);
    }
  }

  private static String requiredText(JsonNode r, String field) {
    String v = r.path(field).asText("").trim();
    if (v.isEmpty()) {
      throw new IllegalStateException("WDFW escapement: row has no " + field + ": " + r);
    }
    return v;
  }

  private static long requiredLong(JsonNode r, String field) {
    String v = requiredText(r, field);
    try {
      return Long.parseLong(v);
    } catch (NumberFormatException e) {
      throw new IllegalStateException("WDFW escapement: non-integer " + field + " '" + v + "'", e);
    }
  }

  private static double requiredDouble(JsonNode r, String field) {
    String v = requiredText(r, field);
    try {
      return Double.parseDouble(v);
    } catch (NumberFormatException e) {
      throw new IllegalStateException("WDFW escapement: non-numeric " + field + " '" + v + "'", e);
    }
  }

  private static void putNullable(ObjectNode row, String name, String value) {
    if (value == null || value.trim().isEmpty()) {
      row.putNull(name);
    } else {
      row.put(name, value.trim());
    }
  }
}
