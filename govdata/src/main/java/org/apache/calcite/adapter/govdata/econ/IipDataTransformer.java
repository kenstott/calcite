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
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transformer for BEA International Investment Position (IIP) data.
 *
 * <p>The IIP dataset reports the value and composition of U.S. external assets
 * and liabilities: the stock (position) of U.S. holdings abroad and foreign
 * holdings in the United States, plus the annual change in those positions
 * broken out by driver (financial-account transactions, price changes,
 * exchange-rate changes, and other volume/valuation effects). It complements
 * the flow-oriented ITA (International Transactions) dataset carried in
 * {@code ita_data} and the direct-investment detail in the {@code fdi_*} tables.
 *
 * <p>Each returned record is keyed by {@code TypeOfInvestment} (the asset/
 * liability category, e.g. {@code CurrAndDepAssets}) and {@code Component}
 * (the measure, e.g. {@code Pos} = position, {@code ChgPosPrice} = change in
 * position attributable to price changes). {@code TimeSeriesId} and
 * {@code TimeSeriesDescription} carry the fully-qualified series identity, and
 * {@code CL_UNIT}/{@code UNIT_MULT} carry the units (USD, millions).
 *
 * <h3>Canonical Output Schema</h3>
 * <pre>
 * - type_of_investment:      STRING - Asset/liability category code
 * - component:               STRING - Measure code (Pos, ChgPos*, ...)
 * - time_series_id:          STRING - BEA series identifier
 * - time_series_description: STRING - Human-readable series description
 * - time_period:             STRING - Data period (YYYY or YYYYQ#)
 * - data_value:              STRING - Numeric value (null for suppressed/NA)
 * - cl_unit:                 STRING - Classification unit (e.g. 'USD')
 * - unit_mult:               STRING - Unit multiplier (e.g. '6' for millions)
 * - frequency:               STRING - Data frequency (A=Annual, Q=Quarterly)
 * - note_ref:                STRING - Reference to explanatory notes
 * </pre>
 *
 * @see BeaResponseTransformer
 * @see ResponseTransformer
 */
public class IipDataTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(IipDataTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("IIP: Empty response received for {}", context.getUrl());
      return "[]";
    }

    // Delegate to BeaResponseTransformer for BEA envelope extraction and
    // no-data/error handling (returns "[]" when BEA reports no data).
    BeaResponseTransformer beaTransformer = new BeaResponseTransformer();
    String extractedData = beaTransformer.transform(response, context);

    if ("[]".equals(extractedData)) {
      return extractedData;
    }

    try {
      JsonNode dataArray = MAPPER.readTree(extractedData);
      if (!dataArray.isArray()) {
        LOGGER.warn("IIP: Expected array after BEA extraction, got {}", dataArray.getNodeType());
        return extractedData;
      }

      ArrayNode transformedData = MAPPER.createArrayNode();
      for (JsonNode record : dataArray) {
        transformedData.add(transformRecord(record));
      }

      LOGGER.debug("IIP: Transformed {} records for {}", transformedData.size(),
          context.getDimensionValues());
      return transformedData.toString();

    } catch (Exception e) {
      LOGGER.error("IIP: Failed to transform response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to transform IIP response: " + e.getMessage(), e);
    }
  }

  private ObjectNode transformRecord(JsonNode record) {
    ObjectNode result = MAPPER.createObjectNode();
    result.put("type_of_investment", getTextValue(record, "TypeOfInvestment"));
    result.put("component", getTextValue(record, "Component"));
    result.put("time_series_id", getTextValue(record, "TimeSeriesId"));
    result.put("time_series_description", getTextValue(record, "TimeSeriesDescription"));
    result.put("time_period", getTextValue(record, "TimePeriod"));
    result.put("data_value", normalizeDataValue(getTextValue(record, "DataValue")));
    result.put("cl_unit", getTextValue(record, "CL_UNIT"));
    result.put("unit_mult", getTextValue(record, "UNIT_MULT"));
    result.put("frequency", getTextValue(record, "Frequency"));
    result.put("note_ref", getTextValue(record, "NoteRef"));
    return result;
  }

  /**
   * Normalizes BEA data values, mapping suppression/unavailable markers to null
   * so downstream DOUBLE casts see a real number or nothing.
   */
  private String normalizeDataValue(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()
        || "(D)".equals(trimmed)     // suppressed to avoid disclosure
        || "(*)".equals(trimmed)     // value between -0.5 and 0.5
        || "(NA)".equals(trimmed)
        || "(NM)".equals(trimmed)
        || "n.a.".equalsIgnoreCase(trimmed)
        || "---".equals(trimmed)) {
      return null;
    }
    return trimmed.replace(",", "");
  }

  private String getTextValue(JsonNode node, String fieldName) {
    JsonNode field = node.get(fieldName);
    if (field == null || field.isNull()) {
      return null;
    }
    return field.asText();
  }
}
