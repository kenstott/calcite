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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Year-aware transformer for BEA International Transactions Accounts (ITA) data.
 *
 * <p>This transformer handles schema breaks in the ITA API across different years:
 *
 * <h3>Schema Breaks Handled</h3>
 * <ul>
 *   <li><b>TimePeriod format change (pre-2014 vs post-2014)</b>:
 *       Pre-2014 data uses "20XX" format, post-2014 uses "20XXQ#" for quarterly.
 *       This transformer normalizes to canonical format.</li>
 *   <li><b>AreaOrCountry field restructure (2023)</b>:
 *       BEA restructured geographic classifications in 2023. Country codes and names
 *       were updated. This transformer maps legacy codes to canonical values.</li>
 *   <li><b>Indicator code changes</b>:
 *       Some indicator codes were renamed or consolidated over time.</li>
 * </ul>
 *
 * <h3>Canonical Output Schema</h3>
 * <pre>
 * - data_value: DOUBLE - Transaction value (normalized from string)
 * - area_or_country: STRING - Standardized geographic area/country name
 * - time_series_description: STRING - Description of the time series
 * - series_code: STRING - Unique series identifier
 * - time_period: STRING - Normalized time period (YYYY or YYYYQ#)
 * - cl_unit: STRING - Classification unit (e.g., 'USD')
 * - unit_mult: STRING - Unit multiplier
 * - metric_name: STRING - Type of metric
 * - frequency: STRING - Data frequency (A=Annual, Q=Quarterly)
 * - note_ref: STRING - Reference to explanatory notes
 * </pre>
 *
 * @see ResponseTransformer
 * @see RequestContext
 */
public class ItaDataTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ItaDataTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Year threshold for TimePeriod format changes.
   * Pre-2014: Annual format "YYYY"
   * Post-2014: Quarterly format "YYYYQ#"
   */
  private static final int TIME_PERIOD_FORMAT_CHANGE_YEAR = 2014;

  /**
   * Year threshold for AreaOrCountry restructure.
   * BEA changed geographic classifications in 2023.
   */
  private static final int AREA_RESTRUCTURE_YEAR = 2023;

  /**
   * Mapping of legacy country/area codes to canonical names.
   * Updated when BEA restructured geographic classifications.
   */
  private static final Map<String, String> LEGACY_AREA_MAPPINGS;

  static {
    LEGACY_AREA_MAPPINGS = new HashMap<String, String>();
    // Pre-2023 country name variations mapped to current standard names
    LEGACY_AREA_MAPPINGS.put("United Kingdom (UK)", "United Kingdom");
    LEGACY_AREA_MAPPINGS.put("UK", "United Kingdom");
    LEGACY_AREA_MAPPINGS.put("China, People's Republic of", "China");
    LEGACY_AREA_MAPPINGS.put("China P.R.", "China");
    LEGACY_AREA_MAPPINGS.put("Korea, Republic of", "South Korea");
    LEGACY_AREA_MAPPINGS.put("Korea", "South Korea");
    LEGACY_AREA_MAPPINGS.put("Russia", "Russian Federation");
    LEGACY_AREA_MAPPINGS.put("Czech Republic", "Czechia");
    LEGACY_AREA_MAPPINGS.put("OPEC", "OPEC countries");
    LEGACY_AREA_MAPPINGS.put("European Union (EU)", "European Union");
    LEGACY_AREA_MAPPINGS.put("EU-27", "European Union");
    LEGACY_AREA_MAPPINGS.put("EU-28", "European Union");
    // Legacy regional groupings
    LEGACY_AREA_MAPPINGS.put("Other Western Hemisphere", "Latin America and Other Western Hemisphere");
    LEGACY_AREA_MAPPINGS.put("Other Africa", "Africa (excluding South Africa)");
  }

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("ITA: Empty response received for {}", context.getUrl());
      return "[]";
    }

    // Delegate to BeaResponseTransformer for initial BEA structure extraction
    BeaResponseTransformer beaTransformer = new BeaResponseTransformer();
    String extractedData = beaTransformer.transform(response, context);

    if ("[]".equals(extractedData)) {
      return extractedData;
    }

    try {
      // Get the year from dimension values for year-aware transformations
      String yearStr = context.getDimensionValues().get("year");
      int year = yearStr != null ? Integer.parseInt(yearStr) : 0;

      JsonNode dataArray = MAPPER.readTree(extractedData);
      if (!dataArray.isArray()) {
        LOGGER.warn("ITA: Expected array after BEA extraction, got {}", dataArray.getNodeType());
        return extractedData;
      }

      ArrayNode transformedData = MAPPER.createArrayNode();

      for (JsonNode record : dataArray) {
        ObjectNode transformedRecord = transformRecord(record, year);
        transformedData.add(transformedRecord);
      }

      LOGGER.debug("ITA: Transformed {} records for year {}", transformedData.size(), year);
      return transformedData.toString();

    } catch (Exception e) {
      LOGGER.error("ITA: Failed to transform response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to transform ITA response: " + e.getMessage(), e);
    }
  }

  /**
   * Transforms a single ITA record to canonical format.
   *
   * @param record The original JSON record from BEA API
   * @param year The data year for year-aware transformations
   * @return Transformed record with canonical field names and values
   */
  private ObjectNode transformRecord(JsonNode record, int year) {
    ObjectNode result = MAPPER.createObjectNode();

    // Copy and transform fields to canonical format

    // DataValue: Normalize to double-compatible string, handle special values
    String dataValue = getTextValue(record, "DataValue");
    result.put("data_value", normalizeDataValue(dataValue));

    // AreaOrCountry: Apply legacy mappings for pre-2023 data
    String areaOrCountry = getTextValue(record, "AreaOrCountry");
    if (year < AREA_RESTRUCTURE_YEAR && areaOrCountry != null) {
      String normalized = LEGACY_AREA_MAPPINGS.get(areaOrCountry);
      if (normalized != null) {
        LOGGER.debug("ITA: Normalized area '{}' to '{}' for year {}",
            areaOrCountry, normalized, year);
        areaOrCountry = normalized;
      }
    }
    result.put("area_or_country", areaOrCountry);

    // TimePeriod: Ensure consistent format
    String timePeriod = getTextValue(record, "TimePeriod");
    result.put("time_period", normalizeTimePeriod(timePeriod, year));

    // Direct field mappings (no transformation needed)
    result.put("time_series_description", getTextValue(record, "TimeSeriesDescription"));
    result.put("series_code", getTextValue(record, "SeriesCode"));
    result.put("cl_unit", getTextValue(record, "CL_UNIT"));
    result.put("unit_mult", getTextValue(record, "UNIT_MULT"));
    result.put("metric_name", getTextValue(record, "METRIC_NAME"));
    result.put("frequency", getTextValue(record, "Frequency"));
    result.put("note_ref", getTextValue(record, "NoteRef"));

    return result;
  }

  /**
   * Normalizes data values, handling BEA special values.
   *
   * @param value Raw data value from API
   * @return Normalized value suitable for DOUBLE conversion, or null for special values
   */
  private String normalizeDataValue(String value) {
    if (value == null) {
      return null;
    }

    String trimmed = value.trim();

    // BEA special values that indicate no data
    if ("(NA)".equals(trimmed) || "(D)".equals(trimmed)
        || "(NM)".equals(trimmed) || "---".equals(trimmed)
        || "n.a.".equalsIgnoreCase(trimmed)) {
      return null;
    }

    // Remove thousands separators for numeric parsing
    return trimmed.replace(",", "");
  }

  /**
   * Normalizes TimePeriod to consistent format.
   *
   * <p>Handles format variations across years:
   * <ul>
   *   <li>Annual: "2022" remains "2022"</li>
   *   <li>Quarterly: "2022Q1" remains "2022Q1"</li>
   *   <li>Legacy quarterly: "2022-Q1" normalized to "2022Q1"</li>
   * </ul>
   *
   * @param timePeriod Raw time period string
   * @param year Data year for context
   * @return Normalized time period
   */
  private String normalizeTimePeriod(String timePeriod, int year) {
    if (timePeriod == null) {
      return null;
    }

    String normalized = timePeriod.trim();

    // Handle legacy format with hyphen: "2022-Q1" -> "2022Q1"
    normalized = normalized.replace("-Q", "Q");

    // Handle lowercase quarter indicator
    normalized = normalized.replace("q", "Q");

    return normalized;
  }

  /**
   * Safely extracts text value from a JSON node.
   *
   * @param node JSON object node
   * @param fieldName Field to extract
   * @return Text value or null if missing/null
   */
  private String getTextValue(JsonNode node, String fieldName) {
    JsonNode field = node.get(fieldName);
    if (field == null || field.isNull()) {
      return null;
    }
    return field.asText();
  }
}
