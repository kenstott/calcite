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
 * Transformer for BEA Direct Investment and Multinational Enterprises (MNE)
 * data, covering both foreign direct investment (FDI) statistics.
 *
 * <p>This transformer serves two request profiles that share an identical
 * output schema and are materialized into two partitioned tables:
 * <ul>
 *   <li><b>Direct Investment (DI)</b> &mdash; {@code fdi_direct_investment}:
 *       position, income, financial transactions, equity, debt, and reinvested
 *       earnings. Requested with {@code SeriesID=all} and the default ownership
 *       parameters.</li>
 *   <li><b>Activities of Multinational Enterprises (AMNE)</b> &mdash;
 *       {@code fdi_activities}: employment, sales, total assets, value added,
 *       net income, capital expenditures, R&amp;D, and affiliate trade in goods.
 *       Requested with {@code SeriesID=all}, {@code OwnershipLevel=0}
 *       (majority-owned), and {@code NonbankAffiliatesOnly=0}.</li>
 * </ul>
 *
 * <p>The MNE API returns rows keyed by {@code SeriesID}/{@code SeriesName}
 * (the statistic), {@code Row}/{@code RowCode} (the country or industry,
 * depending on the requested {@code Classification}), and
 * {@code Column}/{@code ColumnCode} (the statistic sub-breakdown). Units are
 * carried per-row in {@code TableScale} because AMNE mixes dollar figures
 * ("Millions of Dollars") with headcounts ("Thousands of Employees").
 *
 * <h3>Canonical Output Schema</h3>
 * <pre>
 * - series_id:     STRING - BEA MNE series identifier
 * - series_name:   STRING - Human-readable statistic name
 * - row_code:      STRING - Country/industry code (matches Classification)
 * - row_label:     STRING - Country/industry name
 * - column_code:   STRING - Sub-breakdown code
 * - column_label:  STRING - Sub-breakdown name
 * - data_value:    STRING - Numeric value (null for suppressed/NA values)
 * - table_scale:   STRING - Units (e.g. 'Millions of Dollars')
 * - time_period:   STRING - Data year (YYYY)
 * </pre>
 *
 * @see BeaResponseTransformer
 * @see ResponseTransformer
 */
public class FdiDataTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FdiDataTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("FDI: Empty response received for {}", context.getUrl());
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
        LOGGER.warn("FDI: Expected array after BEA extraction, got {}", dataArray.getNodeType());
        return extractedData;
      }

      ArrayNode transformedData = MAPPER.createArrayNode();
      for (JsonNode record : dataArray) {
        transformedData.add(transformRecord(record));
      }

      LOGGER.debug("FDI: Transformed {} records for {}", transformedData.size(),
          context.getDimensionValues());
      return transformedData.toString();

    } catch (Exception e) {
      LOGGER.error("FDI: Failed to transform response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to transform FDI response: " + e.getMessage(), e);
    }
  }

  private ObjectNode transformRecord(JsonNode record) {
    ObjectNode result = MAPPER.createObjectNode();
    result.put("series_id", getTextValue(record, "SeriesID"));
    result.put("series_name", getTextValue(record, "SeriesName"));
    result.put("row_code", getTextValue(record, "RowCode"));
    result.put("row_label", getTextValue(record, "Row"));
    result.put("column_code", getTextValue(record, "ColumnCode"));
    result.put("column_label", getTextValue(record, "Column"));
    result.put("data_value", normalizeDataValue(getTextValue(record, "DataValueUnformatted")));
    result.put("table_scale", getTextValue(record, "TableScale"));
    result.put("time_period", getTextValue(record, "Year"));
    return result;
  }

  /**
   * Normalizes MNE data values, mapping BEA suppression/unavailable markers to
   * null so downstream DOUBLE casts see a real number or nothing.
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
        || "n.s.".equalsIgnoreCase(trimmed)
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
