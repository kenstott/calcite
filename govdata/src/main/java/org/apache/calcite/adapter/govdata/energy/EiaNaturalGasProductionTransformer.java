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

public class EiaNaturalGasProductionTransformer extends EiaV2Transformer implements ResponseTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EIA Natural Gas Production: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode data = extractDataArray(response);
      ArrayNode result = MAPPER.createArrayNode();
      int rowsSkipped = 0;

      for (JsonNode row : data) {
        String period = getString(row, "period");
        int productionYear;
        try {
          productionYear = parseYear(period);
        } catch (NumberFormatException e) {
          rowsSkipped++;
          LOGGER.warn("EIA Natural Gas: skipping row with unparseable period '{}'", period);
          continue;
        }
        ObjectNode out = MAPPER.createObjectNode();

        out.put("production_year", productionYear);

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

        String stateAbbr = deriveStateAbbr(duoarea);
        if (stateAbbr != null) {
          out.put("state_abbr", stateAbbr);
        } else {
          out.putNull("state_abbr");
        }

        out.put("fuel_type", "Natural Gas");

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

      if (rowsSkipped > 0) {
        LOGGER.warn("EIA Natural Gas: skipped {} row(s) with unparseable period", rowsSkipped);
      }
      LOGGER.debug("EIA Natural Gas: transformed {} records", result.size());
      return result.toString();

    } catch (Exception e) {
      throw new RuntimeException("EIA Natural Gas: failed to parse response for "
          + context.getUrl(), e);
    }
  }

  private String deriveStateAbbr(String duoarea) {
    if (duoarea == null || duoarea.length() < 2) {
      return null;
    }
    if (duoarea.startsWith("S") && duoarea.length() == 3) {
      return duoarea.substring(1);
    }
    return null;
  }
}
