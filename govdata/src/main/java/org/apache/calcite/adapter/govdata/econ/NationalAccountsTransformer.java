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
 * Year-aware transformer for BEA National Income and Product Accounts (NIPA) data.
 *
 * <p>This transformer handles schema breaks in the NIPA API across different years:
 *
 * <h3>Schema Breaks Handled</h3>
 * <ul>
 *   <li><b>DataValue format variations</b>:
 *       Values appear in multiple formats across years: plain numbers, comma-separated,
 *       parenthetical negatives "(123.4)", or special values "(NA)", "(D)", "(NM)".
 *       BEA also changed precision/rounding in some annual revisions.</li>
 *   <li><b>NoteRef inconsistencies</b>:
 *       The NoteRef field format changed several times. Pre-2010 data uses numeric codes,
 *       post-2010 uses alphanumeric. Some years have inconsistent or missing note refs.
 *       This transformer normalizes to a consistent format.</li>
 *   <li><b>LineNumber changes from annual revisions</b>:
 *       BEA performs comprehensive revisions every 5 years (2018, 2023, etc.) that can
 *       change LineNumbers for specific components. This transformer maintains a mapping
 *       of line number changes for key GDP components to ensure time-series consistency.</li>
 *   <li><b>Table restructuring</b>:
 *       Some tables were split or merged in revisions. For example, detailed PCE tables
 *       were restructured in 2019.</li>
 * </ul>
 *
 * <h3>Canonical Output Schema</h3>
 * <pre>
 * - table_name: STRING - BEA table identifier (e.g., 'T10101')
 * - line_number_raw: STRING - Raw line number from API
 * - line_description: STRING - Description of the component
 * - series_code: STRING - BEA series code
 * - time_period: STRING - Time period (YYYY or YYYYQ#)
 * - data_value: STRING - Raw data value (for NULL handling)
 * - cl_unit: STRING - Units of measurement
 * - unit_mult: STRING - Unit multiplier
 * - note_ref: STRING - Normalized note reference
 * </pre>
 *
 * @see ResponseTransformer
 * @see RequestContext
 */
public class NationalAccountsTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NationalAccountsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Year of 2018 comprehensive revision.
   * Major changes to deflators, seasonal adjustment, and some line numbers.
   */
  private static final int REVISION_YEAR_2018 = 2018;

  /**
   * Year of 2023 comprehensive revision.
   * Updated base year, methodology changes for digital economy.
   */
  private static final int REVISION_YEAR_2023 = 2023;

  /**
   * Year when NoteRef format changed from numeric to alphanumeric.
   */
  private static final int NOTE_REF_FORMAT_CHANGE_YEAR = 2010;

  /**
   * Pattern to detect parenthetical negative numbers: "(123.4)" -> "-123.4".
   */
  private static final Pattern PARENTHETICAL_NEGATIVE = Pattern.compile("^\\(([\\d,.]+)\\)$");

  /**
   * Pattern to validate numeric data.
   */
  private static final Pattern NUMERIC_PATTERN = Pattern.compile("^-?[\\d,]+\\.?\\d*$");

  /**
   * Mapping of line number changes from 2018 comprehensive revision.
   * Format: "TableName:OldLineNumber" -> "NewLineNumber"
   */
  private static final Map<String, String> LINE_NUMBER_CHANGES_2018;

  /**
   * Mapping of line number changes from 2023 comprehensive revision.
   */
  private static final Map<String, String> LINE_NUMBER_CHANGES_2023;

  /**
   * Mapping of legacy numeric NoteRef codes to alphanumeric format.
   */
  private static final Map<String, String> LEGACY_NOTE_REF_MAP;

  static {
    // 2018 revision line number changes (selected key components)
    LINE_NUMBER_CHANGES_2018 = new HashMap<String, String>();
    // T10101 - Real GDP Percent Change
    LINE_NUMBER_CHANGES_2018.put("T10101:2", "2");  // Goods stayed same
    LINE_NUMBER_CHANGES_2018.put("T10101:3", "3");  // Services stayed same

    // T10105 - GDP levels
    LINE_NUMBER_CHANGES_2018.put("T10105:7", "8");  // Fixed investment line moved
    LINE_NUMBER_CHANGES_2018.put("T10105:8", "9");  // Nonresidential moved

    // T20200 - PCE by type (major restructuring)
    LINE_NUMBER_CHANGES_2018.put("T20200:2", "3");  // Durable goods moved
    LINE_NUMBER_CHANGES_2018.put("T20200:3", "4");  // Nondurable goods moved
    LINE_NUMBER_CHANGES_2018.put("T20200:4", "2");  // Services moved up

    // 2023 revision line number changes
    LINE_NUMBER_CHANGES_2023 = new HashMap<String, String>();
    // T10101 - Real GDP with digital economy adjustments
    LINE_NUMBER_CHANGES_2023.put("T10101:4", "4");  // Gross private domestic investment
    LINE_NUMBER_CHANGES_2023.put("T10101:5", "5");  // Fixed investment

    // T10303 - Real GDP in chained dollars
    LINE_NUMBER_CHANGES_2023.put("T10303:26", "27"); // Government consumption moved

    // Legacy numeric note references to alphanumeric
    LEGACY_NOTE_REF_MAP = new HashMap<String, String>();
    LEGACY_NOTE_REF_MAP.put("1", "T1");   // Standard footnote 1
    LEGACY_NOTE_REF_MAP.put("2", "T2");   // Standard footnote 2
    LEGACY_NOTE_REF_MAP.put("3", "T3");   // Revision note
    LEGACY_NOTE_REF_MAP.put("4", "T4");   // Seasonal adjustment note
    LEGACY_NOTE_REF_MAP.put("5", "T5");   // Methodology note
    LEGACY_NOTE_REF_MAP.put("R", "R");    // Revised (already alphanumeric)
    LEGACY_NOTE_REF_MAP.put("P", "P");    // Preliminary
    LEGACY_NOTE_REF_MAP.put("*", "SA");   // Seasonally adjusted indicator
  }

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("NIPA: Empty response received for {}", context.getUrl());
      return "[]";
    }

    // Delegate to BeaResponseTransformer for initial BEA structure extraction
    BeaResponseTransformer beaTransformer = new BeaResponseTransformer();
    String extractedData = beaTransformer.transform(response, context);

    if ("[]".equals(extractedData)) {
      return extractedData;
    }

    try {
      // Get the year and table from dimension values for year-aware transformations
      String yearStr = context.getDimensionValues().get("year");
      int year = yearStr != null ? Integer.parseInt(yearStr) : 0;
      String tableName = context.getDimensionValues().get("tablename");

      JsonNode dataArray = MAPPER.readTree(extractedData);
      if (!dataArray.isArray()) {
        LOGGER.warn("NIPA: Expected array after BEA extraction, got {}",
            dataArray.getNodeType());
        return extractedData;
      }

      ArrayNode transformedData = MAPPER.createArrayNode();

      for (JsonNode record : dataArray) {
        ObjectNode transformedRecord = transformRecord(record, year, tableName);
        transformedData.add(transformedRecord);
      }

      LOGGER.debug("NIPA: Transformed {} records for table {} year {}",
          transformedData.size(), tableName, year);
      return transformedData.toString();

    } catch (Exception e) {
      LOGGER.error("NIPA: Failed to transform response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to transform NIPA response: " + e.getMessage(), e);
    }
  }

  /**
   * Transforms a single NIPA record to canonical format.
   *
   * @param record The original JSON record from BEA API
   * @param year The data year for year-aware transformations
   * @param tableName The table name for line number mapping
   * @return Transformed record with canonical field names and values
   */
  private ObjectNode transformRecord(JsonNode record, int year, String tableName) {
    ObjectNode result = MAPPER.createObjectNode();

    // TableName: Direct mapping
    String tableFromRecord = getTextValue(record, "TableName");
    result.put("table_name", tableFromRecord != null ? tableFromRecord : tableName);

    // LineNumber: Apply revision mappings if needed
    String lineNumber = getTextValue(record, "LineNumber");
    lineNumber = normalizeLineNumber(lineNumber, tableFromRecord, year);
    result.put("line_number_raw", lineNumber);

    // LineDescription: Direct mapping
    result.put("line_description", getTextValue(record, "LineDescription"));

    // SeriesCode: Direct mapping
    result.put("series_code", getTextValue(record, "SeriesCode"));

    // TimePeriod: Normalize format
    String timePeriod = getTextValue(record, "TimePeriod");
    result.put("time_period", normalizeTimePeriod(timePeriod));

    // DataValue: Normalize from various formats
    String dataValue = getTextValue(record, "DataValue");
    result.put("data_value", normalizeDataValue(dataValue));

    // CL_UNIT: Direct mapping
    result.put("cl_unit", getTextValue(record, "CL_UNIT"));

    // UNIT_MULT: Direct mapping
    result.put("unit_mult", getTextValue(record, "UNIT_MULT"));

    // NoteRef: Normalize legacy formats
    String noteRef = getTextValue(record, "NoteRef");
    noteRef = normalizeNoteRef(noteRef, year);
    result.put("note_ref", noteRef);

    return result;
  }

  /**
   * Normalizes line numbers across BEA comprehensive revisions.
   *
   * <p>BEA performs comprehensive revisions every 5 years that can change
   * line numbers for specific components. This method applies mappings
   * to ensure time-series consistency.
   *
   * @param lineNumber Raw line number from API
   * @param tableName Table name for context
   * @param year Data year
   * @return Normalized line number
   */
  private String normalizeLineNumber(String lineNumber, String tableName, int year) {
    if (lineNumber == null || tableName == null) {
      return lineNumber;
    }

    // For data from before 2018 revision, check if line numbers need mapping
    if (year < REVISION_YEAR_2018) {
      String key = tableName + ":" + lineNumber;
      String mapped = LINE_NUMBER_CHANGES_2018.get(key);
      if (mapped != null && !mapped.equals(lineNumber)) {
        LOGGER.debug("NIPA: Mapped line {} to {} for table {} (2018 revision)",
            lineNumber, mapped, tableName);
        return mapped;
      }
    }

    // For data between 2018 and 2023, check 2023 mappings
    if (year >= REVISION_YEAR_2018 && year < REVISION_YEAR_2023) {
      String key = tableName + ":" + lineNumber;
      String mapped = LINE_NUMBER_CHANGES_2023.get(key);
      if (mapped != null && !mapped.equals(lineNumber)) {
        LOGGER.debug("NIPA: Mapped line {} to {} for table {} (2023 revision)",
            lineNumber, mapped, tableName);
        return mapped;
      }
    }

    return lineNumber;
  }

  /**
   * Normalizes NoteRef values across different BEA formats.
   *
   * @param noteRef Raw note reference from API
   * @param year Data year for format detection
   * @return Normalized note reference
   */
  private String normalizeNoteRef(String noteRef, int year) {
    if (noteRef == null || noteRef.trim().isEmpty()) {
      return null;
    }

    String trimmed = noteRef.trim();

    // For pre-2010 data, map numeric codes to alphanumeric
    if (year < NOTE_REF_FORMAT_CHANGE_YEAR) {
      // Split on commas for multiple note refs
      String[] refs = trimmed.split(",");
      StringBuilder normalized = new StringBuilder();

      for (String ref : refs) {
        String refTrimmed = ref.trim();
        String mapped = LEGACY_NOTE_REF_MAP.get(refTrimmed);

        if (normalized.length() > 0) {
          normalized.append(",");
        }
        normalized.append(mapped != null ? mapped : refTrimmed);
      }

      return normalized.toString();
    }

    return trimmed;
  }

  /**
   * Normalizes TimePeriod to consistent format.
   *
   * @param timePeriod Raw time period string
   * @return Normalized time period
   */
  private String normalizeTimePeriod(String timePeriod) {
    if (timePeriod == null) {
      return null;
    }

    String normalized = timePeriod.trim();

    // Handle variations in quarterly format
    normalized = normalized.replace("-Q", "Q");
    normalized = normalized.replace("q", "Q");

    return normalized;
  }

  /**
   * Normalizes data values from various BEA formats.
   *
   * <p>Handles:
   * <ul>
   *   <li>Plain numbers: "12345" -> "12345"</li>
   *   <li>Comma-separated: "12,345.67" -> "12345.67"</li>
   *   <li>Parenthetical negatives: "(123.4)" -> "-123.4"</li>
   *   <li>Special values: "(NA)", "(D)", "(NM)", "---" -> null</li>
   * </ul>
   *
   * @param value Raw data value from API
   * @return Normalized value or null for special values
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

    // Handle parenthetical negative numbers: (123.4) -> -123.4
    java.util.regex.Matcher matcher = PARENTHETICAL_NEGATIVE.matcher(trimmed);
    if (matcher.matches()) {
      String innerValue = matcher.group(1).replace(",", "");
      return "-" + innerValue;
    }

    // Remove thousands separators
    String withoutCommas = trimmed.replace(",", "");

    // Validate it looks like a number
    if (!NUMERIC_PATTERN.matcher(withoutCommas).matches()) {
      LOGGER.warn("NIPA: Unexpected data value format: {}", trimmed);
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
