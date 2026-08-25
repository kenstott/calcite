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
package org.apache.calcite.adapter.govdata.energy;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps EIA STEO Table 10a drilling-activity series ({@code RIGS}/{@code NWD}/{@code NWC}/
 * {@code DUCS} by DPR region) into {@code eia_drilling_activity} rows.
 *
 * <p>The API response carries only {@code seriesId} (e.g. {@code "RIGSPM"}) and
 * {@code seriesDescription} — region and metric are encoded in the ID, not broken out as
 * separate response fields, so this transformer decodes both from the ID prefix/suffix
 * against the fixed, confirmed-live set of 6 regions x 4 metrics (24 series total).
 */
public class EiaDrillingActivityTransformer extends EiaV2Transformer implements ResponseTransformer {

  private static final Map<String, String> REGION_NAMES = new HashMap<>();
  private static final Map<String, String> METRIC_NAMES = new HashMap<>();
  // Longest-suffix-first so "R48" is tried before any shorter code could spuriously match.
  private static final String[] REGION_CODES = {"R48", "PM", "BK", "EF", "AP", "HA"};
  // Longest-prefix-first so "DUCS" is tried before "NWD"/"NWC" could spuriously match its "D"/"C".
  private static final String[] METRIC_CODES = {"DUCS", "RIGS", "NWD", "NWC"};

  static {
    REGION_NAMES.put("PM", "Permian");
    REGION_NAMES.put("BK", "Bakken");
    REGION_NAMES.put("EF", "Eagle Ford");
    REGION_NAMES.put("AP", "Appalachia");
    REGION_NAMES.put("HA", "Haynesville");
    REGION_NAMES.put("R48", "Rest of Lower 48 States, excluding Gulf of America");

    METRIC_NAMES.put("RIGS", "Active Rigs");
    METRIC_NAMES.put("NWD", "New Wells Drilled");
    METRIC_NAMES.put("NWC", "New Wells Completed");
    METRIC_NAMES.put("DUCS", "Drilled but Uncompleted Wells");
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EIA Drilling Activity: empty response for {}", context.getUrl());
      return "[]";
    }
    try {
      JsonNode data = extractDataArray(response);
      ArrayNode result = MAPPER.createArrayNode();
      int rowsSkipped = 0;

      for (JsonNode row : data) {
        String seriesId = getString(row, "seriesId");
        String period = getString(row, "period");
        if (seriesId == null || period == null) {
          rowsSkipped++;
          continue;
        }

        String metricCode = matchPrefix(seriesId, METRIC_CODES);
        String regionCode = metricCode == null ? null : matchSuffix(seriesId.substring(metricCode.length()), REGION_CODES);

        ObjectNode out = MAPPER.createObjectNode();
        out.put("series_id", seriesId);
        putNullable(out, "series_description", getString(row, "seriesDescription"));
        putNullable(out, "region_code", regionCode);
        putNullable(out, "region_name", regionCode == null ? null : REGION_NAMES.get(regionCode));
        putNullable(out, "metric_code", metricCode);
        putNullable(out, "metric_name", metricCode == null ? null : METRIC_NAMES.get(metricCode));
        out.put("period", period);

        try {
          out.put("report_year", parseYear(period));
        } catch (NumberFormatException e) {
          out.putNull("report_year");
        }
        Integer month = parseMonth(period);
        if (month != null) {
          out.put("report_month", month);
        } else {
          out.putNull("report_month");
        }

        Double value = getDouble(row, "value");
        if (value != null) {
          out.put("value", value);
        } else {
          out.putNull("value");
        }

        result.add(out);
      }

      if (rowsSkipped > 0) {
        LOGGER.warn("EIA Drilling Activity: skipped {} row(s) missing seriesId/period", rowsSkipped);
      }
      LOGGER.debug("EIA Drilling Activity: transformed {} records", result.size());
      return result.toString();
    } catch (Exception e) {
      throw new RuntimeException("EIA Drilling Activity: failed to parse response for "
          + context.getUrl(), e);
    }
  }

  private static void putNullable(ObjectNode out, String field, String value) {
    if (value != null) {
      out.put(field, value);
    } else {
      out.putNull(field);
    }
  }

  private static String matchPrefix(String seriesId, String[] candidates) {
    for (String c : candidates) {
      if (seriesId.startsWith(c)) {
        return c;
      }
    }
    return null;
  }

  private static String matchSuffix(String remainder, String[] candidates) {
    for (String c : candidates) {
      if (remainder.equals(c)) {
        return c;
      }
    }
    return null;
  }
}
