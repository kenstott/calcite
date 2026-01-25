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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Year-aware transformer for BLS Job Openings and Labor Turnover Survey (JOLTS) state data.
 *
 * <p>This transformer handles schema breaks in the JOLTS data across different years:
 *
 * <h3>Schema Breaks Handled</h3>
 * <ul>
 *   <li><b>Series ID format changes (pre/post 2010)</b>:
 *       Pre-2010: Series IDs used shorter format JTS{state}{industry}{element}
 *       Post-2010: Extended format with additional padding and seasonal codes
 *       This transformer normalizes extraction of state FIPS, industry, and data element.</li>
 *   <li><b>Industry classification encoding changes</b>:
 *       Pre-2010 used SIC-based classifications, post-2010 uses NAICS.
 *       Industry codes in series IDs changed format. This transformer provides
 *       consistent industry code extraction.</li>
 *   <li><b>State FIPS extraction across formats</b>:
 *       The position of state FIPS code within series_id varies by era.
 *       This transformer correctly extracts state FIPS for all periods.</li>
 *   <li><b>Seasonal adjustment indicator changes</b>:
 *       The seasonal adjustment flag moved position in series IDs over time.</li>
 * </ul>
 *
 * <h3>JOLTS Series ID Format</h3>
 * <pre>
 * Current format (post-2010): JTS{SS}{IIIIIII}{AAA}{S}
 *   JTS = JOLTS prefix
 *   SS = State FIPS code (2 digits)
 *   IIIIIII = Industry code (7 digits, NAICS-based)
 *   AAA = Data element (JOR=Job Openings Rate, HIR=Hires, etc.)
 *   S = Seasonal adjustment (S=Seasonally adjusted, U=Unadjusted)
 *
 * Legacy format (pre-2010): JTS{SS}{IIII}{AAA}
 *   JTS = JOLTS prefix
 *   SS = State FIPS code (2 digits)
 *   IIII = Industry code (4 digits, SIC-based)
 *   AAA = Data element
 * </pre>
 *
 * <h3>Canonical Output Schema</h3>
 * <pre>
 * - series_id: STRING - Original BLS series identifier
 * - state_fips: STRING - Extracted state FIPS code (2 digits)
 * - industry_code: STRING - Normalized industry code
 * - data_element: STRING - Data element code (JOR, HIR, TSR, QUR, LDR)
 * - metric_type: STRING - Human-readable metric name
 * - year: INTEGER - Data year
 * - period: STRING - BLS period (M01-M12)
 * - value: DOUBLE - Metric value
 * - seasonal_adj: STRING - Seasonal adjustment indicator
 * - footnotes: STRING - Any footnote codes
 * </pre>
 *
 * @see ResponseTransformer
 * @see RequestContext
 */
public class JoltsStateTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(JoltsStateTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Year threshold for series ID format change.
   * 2010 marked transition to extended NAICS-based series IDs.
   */
  private static final int FORMAT_CHANGE_YEAR = 2010;

  /**
   * Pattern for current (post-2010) JOLTS series ID format.
   * Groups: 1=state(2), 2=industry(7), 3=element(3), 4=seasonal(1, optional)
   */
  private static final Pattern CURRENT_SERIES_PATTERN =
      Pattern.compile("^JTS(\\d{2})(\\d{7})([A-Z]{2,3})([SU])?$");

  /**
   * Pattern for legacy (pre-2010) JOLTS series ID format.
   * Groups: 1=state(2), 2=industry(4-6), 3=element(3)
   */
  private static final Pattern LEGACY_SERIES_PATTERN =
      Pattern.compile("^JTS(\\d{2})(\\d{4,6})([A-Z]{2,3})$");

  /**
   * Mapping of data element codes to human-readable names.
   */
  private static final Map<String, String> DATA_ELEMENT_NAMES;

  /**
   * Mapping of legacy SIC-based industry codes to NAICS equivalents.
   */
  private static final Map<String, String> LEGACY_INDUSTRY_MAP;

  static {
    // Data element code descriptions
    DATA_ELEMENT_NAMES = new HashMap<String, String>();
    DATA_ELEMENT_NAMES.put("JOR", "JobOpenings");
    DATA_ELEMENT_NAMES.put("JOL", "JobOpeningsLevel");
    DATA_ELEMENT_NAMES.put("HIR", "Hires");
    DATA_ELEMENT_NAMES.put("HI", "Hires");  // Legacy short code
    DATA_ELEMENT_NAMES.put("TSR", "TotalSeparations");
    DATA_ELEMENT_NAMES.put("TS", "TotalSeparations");  // Legacy
    DATA_ELEMENT_NAMES.put("QUR", "Quits");
    DATA_ELEMENT_NAMES.put("QU", "Quits");  // Legacy
    DATA_ELEMENT_NAMES.put("LDR", "LayoffsDischarges");
    DATA_ELEMENT_NAMES.put("LD", "LayoffsDischarges");  // Legacy
    DATA_ELEMENT_NAMES.put("OSR", "OtherSeparations");

    // Legacy SIC to NAICS industry code mapping
    LEGACY_INDUSTRY_MAP = new HashMap<String, String>();
    // Total nonfarm
    LEGACY_INDUSTRY_MAP.put("0000", "000000");
    LEGACY_INDUSTRY_MAP.put("00000", "0000000");

    // Mining and logging
    LEGACY_INDUSTRY_MAP.put("1000", "1000000");

    // Construction
    LEGACY_INDUSTRY_MAP.put("2000", "2000000");

    // Manufacturing
    LEGACY_INDUSTRY_MAP.put("3000", "3000000");
    LEGACY_INDUSTRY_MAP.put("3100", "3100000");  // Durable goods
    LEGACY_INDUSTRY_MAP.put("3200", "3200000");  // Nondurable goods

    // Trade, Transportation, Utilities
    LEGACY_INDUSTRY_MAP.put("4000", "4000000");
    LEGACY_INDUSTRY_MAP.put("4200", "4200000");  // Retail trade

    // Information
    LEGACY_INDUSTRY_MAP.put("5000", "5000000");

    // Financial activities
    LEGACY_INDUSTRY_MAP.put("5500", "5500000");

    // Professional and business services
    LEGACY_INDUSTRY_MAP.put("6000", "6000000");

    // Education and health services
    LEGACY_INDUSTRY_MAP.put("6500", "6500000");

    // Leisure and hospitality
    LEGACY_INDUSTRY_MAP.put("7000", "7000000");

    // Other services
    LEGACY_INDUSTRY_MAP.put("8000", "8000000");

    // Government
    LEGACY_INDUSTRY_MAP.put("9000", "9000000");
    LEGACY_INDUSTRY_MAP.put("9100", "9100000");  // Federal
    LEGACY_INDUSTRY_MAP.put("9200", "9200000");  // State and local
  }

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("JOLTS: Empty response received for {}", context.getUrl());
      return "[]";
    }

    try {
      // Get the year from dimension values for year-aware transformations
      String yearStr = context.getDimensionValues().get("year");
      int year = yearStr != null ? Integer.parseInt(yearStr) : 0;

      // JOLTS data from FTP is already JSON array (after TSV parsing)
      // or it could be raw TSV that needs parsing
      JsonNode dataArray = MAPPER.readTree(response);

      if (!dataArray.isArray()) {
        LOGGER.warn("JOLTS: Expected array, got {}", dataArray.getNodeType());
        return response;
      }

      ArrayNode transformedData = MAPPER.createArrayNode();

      for (JsonNode record : dataArray) {
        ObjectNode transformedRecord = transformRecord(record, year);
        if (transformedRecord != null) {
          transformedData.add(transformedRecord);
        }
      }

      LOGGER.debug("JOLTS: Transformed {} records for year {}", transformedData.size(), year);
      return transformedData.toString();

    } catch (Exception e) {
      LOGGER.error("JOLTS: Failed to transform response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to transform JOLTS response: " + e.getMessage(), e);
    }
  }

  /**
   * Transforms a single JOLTS record to canonical format.
   *
   * @param record The original JSON record (from TSV parsing)
   * @param year The data year for year-aware transformations
   * @return Transformed record with canonical field names and values, or null if invalid
   */
  private ObjectNode transformRecord(JsonNode record, int year) {
    ObjectNode result = MAPPER.createObjectNode();

    // Extract series_id - the key field for parsing
    String seriesId = getTextValue(record, "series_id");
    if (seriesId == null) {
      // Try alternate field name
      seriesId = getTextValue(record, "series");
    }

    if (seriesId == null || seriesId.trim().isEmpty()) {
      LOGGER.debug("JOLTS: Skipping record with no series_id");
      return null;
    }

    seriesId = seriesId.trim();
    result.put("series_id", seriesId);

    // Parse the series ID based on year/format
    SeriesIdComponents components = parseSeriesId(seriesId, year);

    if (components == null) {
      LOGGER.debug("JOLTS: Could not parse series_id: {}", seriesId);
      // Still include the record but with null parsed fields
      result.putNull("state_fips");
      result.putNull("industry_code");
      result.putNull("data_element");
      result.putNull("metric_type");
      result.putNull("seasonal_adj");
    } else {
      result.put("state_fips", components.stateFips);
      result.put("industry_code", components.industryCode);
      result.put("data_element", components.dataElement);
      result.put("metric_type", components.metricType);
      result.put("seasonal_adj", components.seasonalAdj);
    }

    // Year: From record or dimension
    String recordYear = getTextValue(record, "year");
    if (recordYear != null) {
      try {
        result.put("year", Integer.parseInt(recordYear.trim()));
      } catch (NumberFormatException e) {
        result.put("year", year);
      }
    } else {
      result.put("year", year);
    }

    // Period: Direct mapping
    result.put("period", getTextValue(record, "period"));

    // Value: Parse and normalize
    String value = getTextValue(record, "value");
    if (value != null) {
      value = value.trim();
      // Handle special values
      if ("-".equals(value) || "N/A".equalsIgnoreCase(value) || value.isEmpty()) {
        result.putNull("value");
      } else {
        try {
          result.put("value", Double.parseDouble(value.replace(",", "")));
        } catch (NumberFormatException e) {
          result.putNull("value");
        }
      }
    } else {
      result.putNull("value");
    }

    // Footnotes: Direct mapping
    String footnotes = getTextValue(record, "footnote_codes");
    if (footnotes == null) {
      footnotes = getTextValue(record, "footnotes");
    }
    result.put("footnotes", footnotes);

    return result;
  }

  /**
   * Parses a JOLTS series ID into its components.
   *
   * @param seriesId The series ID to parse
   * @param year Data year for format detection
   * @return Parsed components or null if parsing fails
   */
  private SeriesIdComponents parseSeriesId(String seriesId, int year) {
    if (seriesId == null || !seriesId.startsWith("JTS")) {
      return null;
    }

    SeriesIdComponents components = new SeriesIdComponents();

    // Try current format first (more common)
    Matcher currentMatcher = CURRENT_SERIES_PATTERN.matcher(seriesId);
    if (currentMatcher.matches()) {
      components.stateFips = currentMatcher.group(1);
      components.industryCode = currentMatcher.group(2);
      components.dataElement = currentMatcher.group(3);
      components.seasonalAdj = currentMatcher.groupCount() >= 4 ? currentMatcher.group(4) : null;
      components.metricType = DATA_ELEMENT_NAMES.get(components.dataElement);
      if (components.metricType == null) {
        components.metricType = components.dataElement;
      }
      return components;
    }

    // Try legacy format for older data
    Matcher legacyMatcher = LEGACY_SERIES_PATTERN.matcher(seriesId);
    if (legacyMatcher.matches()) {
      components.stateFips = legacyMatcher.group(1);
      String legacyIndustry = legacyMatcher.group(2);
      components.industryCode = normalizeIndustryCode(legacyIndustry);
      components.dataElement = legacyMatcher.group(3);
      components.seasonalAdj = null;  // Legacy format doesn't include this
      components.metricType = DATA_ELEMENT_NAMES.get(components.dataElement);
      if (components.metricType == null) {
        components.metricType = components.dataElement;
      }
      return components;
    }

    // Fallback: Try to extract what we can
    if (seriesId.length() >= 5) {
      components.stateFips = seriesId.substring(3, 5);

      // Try to find the data element at the end
      String suffix = seriesId.substring(seriesId.length() - 3);
      if (DATA_ELEMENT_NAMES.containsKey(suffix)) {
        components.dataElement = suffix;
        components.metricType = DATA_ELEMENT_NAMES.get(suffix);
        components.industryCode = seriesId.substring(5, seriesId.length() - 3);
      } else if (seriesId.length() >= 7) {
        suffix = seriesId.substring(seriesId.length() - 2);
        if (DATA_ELEMENT_NAMES.containsKey(suffix)) {
          components.dataElement = suffix;
          components.metricType = DATA_ELEMENT_NAMES.get(suffix);
          components.industryCode = seriesId.substring(5, seriesId.length() - 2);
        }
      }

      if (components.industryCode != null) {
        components.industryCode = normalizeIndustryCode(components.industryCode);
      }

      return components;
    }

    return null;
  }

  /**
   * Normalizes legacy industry codes to current NAICS-based format.
   *
   * @param code Legacy industry code
   * @return Normalized 7-digit NAICS-based code
   */
  private String normalizeIndustryCode(String code) {
    if (code == null) {
      return null;
    }

    // Check for direct mapping
    String mapped = LEGACY_INDUSTRY_MAP.get(code);
    if (mapped != null) {
      return mapped;
    }

    // Pad to 7 digits if needed
    if (code.length() < 7) {
      StringBuilder padded = new StringBuilder(code);
      while (padded.length() < 7) {
        padded.append("0");
      }
      return padded.toString();
    }

    return code;
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

  /**
   * Container for parsed series ID components.
   */
  private static class SeriesIdComponents {
    String stateFips;
    String industryCode;
    String dataElement;
    String metricType;
    String seasonalAdj;
  }
}
