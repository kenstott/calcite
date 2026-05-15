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
package org.apache.calcite.adapter.govdata.lands;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.StringWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms the ONRR fiscal-year revenue bulk CSV into {@code onrr_revenues} rows.
 *
 * <p>The ETL framework parses the CSV into a JSON array (column headers as keys) before
 * invoking this transformer. The source is a single all-years file; no year-dimension
 * filtering is applied — all rows are emitted.
 *
 * <p>Source: {@code https://revenuedata.onrr.gov/downloads/fiscal_year_revenue.csv}
 * (domain moved from {@code revenuedata.doi.gov} in 2024).
 *
 * <p>Input: pre-parsed JSON array from ONRR CSV
 * <pre>
 * [
 *   {
 *     "Fiscal Year": "2022",
 *     "Land Class": "Federal",
 *     "Land Category": "Onshore",
 *     "State": "Wyoming",
 *     "County": "Sublette",
 *     "FIPS Code": "56035",
 *     "Offshore Region": "",
 *     "Revenue Type": "Royalties",
 *     "Mineral Lease Type": "Oil & Gas",
 *     "Commodity": "Oil",
 *     "Product": "Crude Oil",
 *     "Revenue": "28927814.00"
 *   }
 * ]
 * </pre>
 *
 * <p>Output: JSON array string with columns matching the {@code onrr_revenues} schema.
 */
public class OnrrRevenueTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(OnrrRevenueTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("onrr_revenues: empty response from ONRR");
      return "[]";
    }

    try (JsonParser parser = MAPPER.getFactory().createParser(response)) {
      JsonToken first = parser.nextToken();
      if (first != JsonToken.START_ARRAY) {
        LOGGER.warn("onrr_revenues: expected JSON array, got token: {}", first);
        return "[]";
      }
      StringWriter sw = new StringWriter(1 << 16);
      int count = 0;
      try (JsonGenerator gen = MAPPER.getFactory().createGenerator(sw)) {
        gen.writeStartArray();
        while (parser.nextToken() == JsonToken.START_OBJECT) {
          JsonNode record = MAPPER.readTree(parser);
          ObjectNode row = MAPPER.createObjectNode();
          row.put("fiscal_year", intFromText(textOrNull(record, "Fiscal Year")));
          row.put("land_class", textOrNull(record, "Land Class"));
          row.put("land_category", textOrNull(record, "Land Category"));
          row.put("state_name", emptyToNull(textOrNull(record, "State")));
          row.put("county_name", emptyToNull(textOrNull(record, "County")));
          row.put("county_fips", padFips(emptyToNull(textOrNull(record, "FIPS Code"))));
          row.put("offshore_region", emptyToNull(textOrNull(record, "Offshore Region")));
          row.put("revenue_type", textOrNull(record, "Revenue Type"));
          row.put("mineral_lease_type", emptyToNull(textOrNull(record, "Mineral Lease Type")));
          row.put("commodity", textOrNull(record, "Commodity"));
          row.put("product", emptyToNull(textOrNull(record, "Product")));
          row.put("revenue", doubleOrNull(record, "Revenue"));
          MAPPER.writeTree(gen, row);
          count++;
        }
        gen.writeEndArray();
      }
      LOGGER.debug("onrr_revenues: transformed {} records", count);
      return sw.toString();
    } catch (Exception e) {
      LOGGER.error("onrr_revenues: failed to transform ONRR response: {}", e.getMessage(), e);
      throw new RuntimeException("onrr_revenues transform failed", e);
    }
  }

  private String padFips(String fips) {
    if (fips == null) {
      return null;
    }
    // FIPS codes are 5-digit strings; source omits leading zero for single-digit state codes
    String digits = fips.trim();
    while (digits.length() < 5) {
      digits = "0" + digits;
    }
    return digits;
  }

  private String textOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    return val.isNull() || val.isMissingNode() ? null : val.asText(null);
  }

  private String emptyToNull(String value) {
    return (value == null || value.trim().isEmpty()) ? null : value;
  }

  private Integer intFromText(String text) {
    if (text == null || text.trim().isEmpty()) {
      return null;
    }
    try {
      return Integer.parseInt(text.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private Double doubleOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    if (val.isNumber()) {
      return val.asDouble();
    }
    try {
      return Double.parseDouble(val.asText().replaceAll(",", ""));
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
