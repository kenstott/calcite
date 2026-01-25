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
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Year-aware transformer for BEA GDP by Industry data.
 *
 * <p>This transformer handles schema breaks in the GDPbyIndustry API across different years:
 *
 * <h3>Schema Breaks Handled</h3>
 * <ul>
 *   <li><b>NAICS rebasing in 2022</b>:
 *       BEA rebased industry classifications in 2022 to align with NAICS 2022.
 *       Some industry codes changed or were consolidated. This transformer maps
 *       legacy codes to the current standard.</li>
 *   <li><b>DataValue format variations</b>:
 *       Values can appear in multiple formats: plain numbers, comma-separated,
 *       scientific notation (1.23E+09), or special values like "(NA)", "(D)".
 *       This transformer normalizes all to standard numeric format.</li>
 *   <li><b>Industry code dimension changes</b>:
 *       Pre-2017 data uses some different industry aggregation codes.
 *       This transformer provides mapping to current codes.</li>
 * </ul>
 *
 * <h3>Canonical Output Schema</h3>
 * <pre>
 * - table_id: STRING - BEA table identifier
 * - quarter: STRING - Quarter identifier (Q1-Q4) or null for annual
 * - industry_code: STRING - Standardized NAICS industry code
 * - industry_description: STRING - Full industry description
 * - value: DOUBLE - GDP value (normalized from various formats)
 * - units: STRING - Units of measurement
 * - note_ref: STRING - Reference to footnotes
 * </pre>
 *
 * @see ResponseTransformer
 * @see RequestContext
 */
public class IndustryGdpTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndustryGdpTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Year threshold for NAICS rebasing.
   * 2022 saw significant NAICS updates affecting industry classifications.
   */
  private static final int NAICS_REBASE_YEAR = 2022;

  /**
   * Year threshold for older industry code format.
   * Pre-2017 data may use different aggregation codes.
   */
  private static final int LEGACY_INDUSTRY_CODE_YEAR = 2017;

  /**
   * Pattern to detect scientific notation in data values.
   */
  private static final Pattern SCIENTIFIC_NOTATION = Pattern.compile("[eE][+-]?\\d+");

  /**
   * Pattern to validate numeric data (with optional commas and decimal).
   */
  private static final Pattern NUMERIC_PATTERN = Pattern.compile("^-?[\\d,]+\\.?\\d*$");

  /**
   * Mapping of legacy NAICS codes to current codes (post-2022 rebasing).
   * Keys are old codes, values are canonical current codes.
   */
  private static final Map<String, String> NAICS_REBASING_MAP;

  /**
   * Mapping of pre-2017 industry aggregation codes to current codes.
   */
  private static final Map<String, String> LEGACY_INDUSTRY_CODES;

  static {
    // NAICS 2022 rebasing mappings
    NAICS_REBASING_MAP = new HashMap<String, String>();
    // Information sector restructuring
    NAICS_REBASING_MAP.put("51", "51");  // Information stayed same but subcategories changed
    NAICS_REBASING_MAP.put("511", "511"); // Publishing
    NAICS_REBASING_MAP.put("512", "512"); // Motion picture (restructured)
    NAICS_REBASING_MAP.put("515", "515"); // Broadcasting
    NAICS_REBASING_MAP.put("517", "517"); // Telecommunications (major restructure)
    NAICS_REBASING_MAP.put("518", "518"); // Data processing (expanded to include cloud)
    NAICS_REBASING_MAP.put("519", "519"); // Other information services

    // Retail trade restructuring
    NAICS_REBASING_MAP.put("4541", "4541"); // Electronic shopping
    NAICS_REBASING_MAP.put("45411", "454110"); // Electronic shopping and mail-order houses

    // Professional services updates
    NAICS_REBASING_MAP.put("5415", "5415"); // Computer systems design

    // Legacy industry codes from pre-2017
    LEGACY_INDUSTRY_CODES = new HashMap<String, String>();
    // Manufacturing aggregations
    LEGACY_INDUSTRY_CODES.put("MFGDUR", "31G");  // Durable goods manufacturing
    LEGACY_INDUSTRY_CODES.put("MFGNDUR", "31G"); // Non-durable -> use broader manufacturing
    LEGACY_INDUSTRY_CODES.put("MFG", "31G");     // Total manufacturing

    // Government sector codes
    LEGACY_INDUSTRY_CODES.put("GOV", "GSLG");    // Total government
    LEGACY_INDUSTRY_CODES.put("GFED", "GFE");    // Federal government
    LEGACY_INDUSTRY_CODES.put("GSL", "GSLG");    // State and local government

    // Finance and insurance
    LEGACY_INDUSTRY_CODES.put("FIN", "52");      // Finance and insurance
    LEGACY_INDUSTRY_CODES.put("FIRE", "52");     // Legacy FIRE sector -> Finance

    // Real estate
    LEGACY_INDUSTRY_CODES.put("RE", "53");       // Real estate
    LEGACY_INDUSTRY_CODES.put("REAL", "53");     // Real estate alternate code
  }

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("IndustryGDP: Empty response received for {}", context.getUrl());
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
        LOGGER.warn("IndustryGDP: Expected array after BEA extraction, got {}",
            dataArray.getNodeType());
        return extractedData;
      }

      ArrayNode transformedData = MAPPER.createArrayNode();

      for (JsonNode record : dataArray) {
        ObjectNode transformedRecord = transformRecord(record, year);
        transformedData.add(transformedRecord);
      }

      LOGGER.debug("IndustryGDP: Transformed {} records for year {}",
          transformedData.size(), year);
      return transformedData.toString();

    } catch (Exception e) {
      LOGGER.error("IndustryGDP: Failed to transform response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to transform IndustryGDP response: " + e.getMessage(), e);
    }
  }

  /**
   * Transforms a single GDPbyIndustry record to canonical format.
   *
   * @param record The original JSON record from BEA API
   * @param year The data year for year-aware transformations
   * @return Transformed record with canonical field names and values
   */
  private ObjectNode transformRecord(JsonNode record, int year) {
    ObjectNode result = MAPPER.createObjectNode();

    // TableID: Direct mapping
    result.put("table_id", getTextValue(record, "TableID"));

    // Quarter: Direct mapping (may be null for annual data)
    result.put("quarter", getTextValue(record, "Quarter"));

    // Industry code: Apply legacy mappings based on year
    String industryCode = getTextValue(record, "Industry");
    industryCode = normalizeIndustryCode(industryCode, year);
    result.put("industry_code", industryCode);

    // Industry description: Direct mapping
    result.put("industry_description", getTextValue(record, "IndusDesc"));

    // DataValue: Normalize from various formats to double-compatible string
    String dataValue = getTextValue(record, "DataValue");
    result.put("value", normalizeDataValue(dataValue));

    // Units: Direct mapping
    result.put("units", getTextValue(record, "CL_UNIT"));

    // NoteRef: Direct mapping
    result.put("note_ref", getTextValue(record, "NoteRef"));

    return result;
  }

  /**
   * Normalizes industry codes across different NAICS versions and BEA conventions.
   *
   * @param code Raw industry code from API
   * @param year Data year for determining which mappings to apply
   * @return Normalized industry code
   */
  private String normalizeIndustryCode(String code, int year) {
    if (code == null) {
      return null;
    }

    String normalized = code.trim().toUpperCase();

    // Apply legacy mappings for pre-2017 data
    if (year < LEGACY_INDUSTRY_CODE_YEAR) {
      String mapped = LEGACY_INDUSTRY_CODES.get(normalized);
      if (mapped != null) {
        LOGGER.debug("IndustryGDP: Mapped legacy code '{}' to '{}' for year {}",
            code, mapped, year);
        return mapped;
      }
    }

    // Apply NAICS rebasing mappings for data straddling the 2022 change
    // Only apply if the code exists in the mapping and year is before rebasing
    if (year < NAICS_REBASE_YEAR) {
      String rebased = NAICS_REBASING_MAP.get(normalized);
      if (rebased != null && !rebased.equals(normalized)) {
        LOGGER.debug("IndustryGDP: Rebased code '{}' to '{}' for year {}",
            code, rebased, year);
        return rebased;
      }
    }

    return code;  // Return original if no mapping needed
  }

  /**
   * Normalizes data values from various BEA formats to double-compatible strings.
   *
   * <p>Handles:
   * <ul>
   *   <li>Plain numbers: "12345" -> "12345"</li>
   *   <li>Comma-separated: "12,345.67" -> "12345.67"</li>
   *   <li>Scientific notation: "1.23E+09" -> "1230000000"</li>
   *   <li>Special values: "(NA)", "(D)", "(NM)", "---" -> null</li>
   * </ul>
   *
   * @param value Raw data value from API
   * @return Normalized value suitable for DOUBLE conversion, or null
   */
  private String normalizeDataValue(String value) {
    if (value == null) {
      return null;
    }

    String trimmed = value.trim();

    // Handle BEA special values indicating no data
    if ("(NA)".equals(trimmed) || "(D)".equals(trimmed)
        || "(NM)".equals(trimmed) || "---".equals(trimmed)
        || "n.a.".equalsIgnoreCase(trimmed) || trimmed.isEmpty()) {
      return null;
    }

    // Handle scientific notation by converting to standard decimal
    if (SCIENTIFIC_NOTATION.matcher(trimmed).find()) {
      try {
        double parsed = Double.parseDouble(trimmed);
        // Return as string to avoid precision loss in JSON
        return String.valueOf(parsed);
      } catch (NumberFormatException e) {
        LOGGER.warn("IndustryGDP: Could not parse scientific notation: {}", trimmed);
        return null;
      }
    }

    // Remove thousands separators
    String withoutCommas = trimmed.replace(",", "");

    // Validate it looks like a number
    if (!NUMERIC_PATTERN.matcher(withoutCommas).matches()) {
      LOGGER.warn("IndustryGDP: Unexpected data value format: {}", trimmed);
      return null;
    }

    return withoutCommas;
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
