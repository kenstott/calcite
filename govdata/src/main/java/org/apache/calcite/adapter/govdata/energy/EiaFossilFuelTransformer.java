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

public class EiaFossilFuelTransformer extends EiaV2Transformer implements ResponseTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EIA Fossil Fuel: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode data = extractDataArray(response);
      ArrayNode result = MAPPER.createArrayNode();

      for (JsonNode row : data) {
        String period = getString(row, "period");
        ObjectNode out = MAPPER.createObjectNode();

        out.put("production_year", parseYear(period));

        Integer month = parseMonth(period);
        if (month != null) {
          out.put("production_month", month);
        } else {
          out.putNull("production_month");
        }

        String duoarea = getString(row, "duoarea");
        if (duoarea != null) {
          out.put("eia_area_code", duoarea);
        } else {
          out.putNull("eia_area_code");
        }

        // State codes in EIA petroleum data are prefixed with 'S' (e.g. "STX" → "TX")
        String stateAbbr = deriveStateAbbr(duoarea);
        if (stateAbbr != null) {
          out.put("state_abbr", stateAbbr);
        } else {
          out.putNull("state_abbr");
        }

        String productName = getString(row, "product-name");
        out.put("fuel_type", deriveFuelType(productName));

        String processCode = getString(row, "process");
        if (processCode != null) {
          out.put("process_code", processCode);
        } else {
          out.putNull("process_code");
        }

        String processName = getString(row, "process-name");
        if (processName != null) {
          out.put("process_name", processName);
        } else {
          out.putNull("process_name");
        }

        Double value = getDouble(row, "value");
        if (value != null) {
          out.put("production_volume", value);
        } else {
          out.putNull("production_volume");
        }

        String units = getString(row, "units");
        if (units != null) {
          out.put("production_unit", units);
        } else {
          out.putNull("production_unit");
        }

        String series = getString(row, "series");
        if (series != null) {
          out.put("series_id", series);
        } else {
          out.putNull("series_id");
        }

        String seriesDesc = getString(row, "series-description");
        if (seriesDesc != null) {
          out.put("series_description", seriesDesc);
        } else {
          out.putNull("series_description");
        }

        result.add(out);
      }

      LOGGER.debug("EIA Fossil Fuel: transformed {} records", result.size());
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("EIA Fossil Fuel: failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private String deriveStateAbbr(String duoarea) {
    if (duoarea == null || duoarea.length() < 2) {
      return null;
    }
    // State codes start with 'S' followed by 2-char state abbr, e.g. "STX" → "TX"
    if (duoarea.startsWith("S") && duoarea.length() == 3) {
      return duoarea.substring(1);
    }
    return null;
  }

  private String deriveFuelType(String productName) {
    if (productName == null) {
      return "Other";
    }
    String lower = productName.toLowerCase();
    if (lower.contains("crude") || lower.contains("petroleum")) {
      return "Crude Oil";
    }
    if (lower.contains("natural gas") || lower.contains("dry")) {
      return "Natural Gas";
    }
    return "Other";
  }
}
