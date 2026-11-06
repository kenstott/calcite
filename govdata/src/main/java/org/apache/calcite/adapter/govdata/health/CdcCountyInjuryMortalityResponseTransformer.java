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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.PerRecordResponseTransformer;
import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Adds the observation {@code year} to CDC MIOV county injury mortality rows (Socrata psx4-wq38).
 *
 * <p>The source carries no year column. Its {@code period} field is either a four-digit calendar
 * year ({@code "2023"}) or {@code "TTM"} (trailing twelve months), and the window of a TTM row is
 * given only in {@code ttm_date_range} as {@code "June, 2025 to May, 2026"}. The emitted
 * {@code year} is the calendar year for a yearly row and the year in which the trailing window
 * ends for a TTM row. A period or range that matches neither shape is a source-format change and
 * fails the load rather than producing a row with no year.
 */
public class CdcCountyInjuryMortalityResponseTransformer implements PerRecordResponseTransformer {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern YEARLY = Pattern.compile("^\\d{4}$");
  private static final Pattern TTM_RANGE =
      Pattern.compile("^[A-Za-z]+,\\s*\\d{4}\\s+to\\s+[A-Za-z]+,\\s*(\\d{4})$");
  private static final String TTM = "TTM";

  @Override
  public String transform(String response, RequestContext context) {
    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode out = MAPPER.createArrayNode();
      if (root.isArray()) {
        for (JsonNode record : root) {
          ObjectNode row = record.deepCopy();
          row.put("year", yearOf(text(row, "period"), text(row, "ttm_date_range")));
          out.add(row);
        }
      }
      return out.toString();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to parse CDC county injury mortality response", e);
    }
  }

  @Override
  public void transformRecord(Map<String, Object> row, RequestContext context) {
    Object period = row.get("period");
    Object range = row.get("ttm_date_range");
    row.put("year",
        yearOf(period == null ? null : period.toString(), range == null ? null : range.toString()));
  }

  /**
   * Derives the observation year from a source period label.
   *
   * @param period the source {@code period} field
   * @param ttmDateRange the source {@code ttm_date_range} field, required when period is TTM
   * @return the four-digit year
   * @throws IllegalStateException if the label cannot be parsed
   */
  static int yearOf(String period, String ttmDateRange) {
    if (period != null && YEARLY.matcher(period).matches()) {
      return Integer.parseInt(period);
    }
    if (TTM.equals(period)) {
      if (ttmDateRange == null) {
        throw new IllegalStateException(
            "CDC county injury mortality TTM row has no ttm_date_range, so its year is unknown");
      }
      Matcher m = TTM_RANGE.matcher(ttmDateRange.trim());
      if (m.matches()) {
        return Integer.parseInt(m.group(1));
      }
      throw new IllegalStateException("Unparseable CDC ttm_date_range: '" + ttmDateRange + "'");
    }
    throw new IllegalStateException("Unrecognized CDC county injury mortality period: '"
        + period + "' (expected a four-digit year or TTM)");
  }

  private static String text(JsonNode node, String field) {
    JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? null : value.asText();
  }
}
