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

public class EiaElectricityGenerationTransformer extends EiaV2Transformer
    implements ResponseTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EIA Electricity Generation: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode data = extractDataArray(response);
      ArrayNode result = MAPPER.createArrayNode();

      for (JsonNode row : data) {
        String period = getString(row, "period");
        ObjectNode out = MAPPER.createObjectNode();

        out.put("generation_year", parseYear(period));

        Integer month = parseMonth(period);
        if (month != null) {
          out.put("generation_month", month);
        } else {
          out.putNull("generation_month");
        }

        String stateAbbr = getString(row, "location", "stateid");
        if (stateAbbr != null) {
          out.put("state_abbr", stateAbbr);
        } else {
          out.putNull("state_abbr");
        }

        String stateDesc = getString(row, "locationDescription", "stateDescription");
        if (stateDesc != null) {
          out.put("state_description", stateDesc);
        } else {
          out.putNull("state_description");
        }

        String sourceCode = getString(row, "fueltypeid");
        if (sourceCode != null) {
          out.put("energy_source_code", sourceCode);
        } else {
          out.putNull("energy_source_code");
        }

        String sourceDesc = getString(row, "fuelTypeDescription");
        if (sourceDesc != null) {
          out.put("energy_source", sourceDesc);
        } else {
          out.putNull("energy_source");
        }

        String sectorCode = getString(row, "sectorid");
        if (sectorCode != null) {
          out.put("sector_code", sectorCode);
        } else {
          out.putNull("sector_code");
        }

        String sectorDesc = getString(row, "sectorDescription");
        if (sectorDesc != null) {
          out.put("sector", sectorDesc);
        } else {
          out.putNull("sector");
        }

        Double generation = getDouble(row, "generation");
        if (generation != null) {
          out.put("generation_thousand_mwh", generation);
        } else {
          out.putNull("generation_thousand_mwh");
        }

        Double fuelTotal = getDouble(row, "total-consumption-btu");
        if (fuelTotal != null) {
          out.put("fuel_consumed_total_mmbtu", fuelTotal);
        } else {
          out.putNull("fuel_consumed_total_mmbtu");
        }

        Double fuelEg = getDouble(row, "consumption-for-eg-btu");
        if (fuelEg != null) {
          out.put("fuel_consumed_for_eg_mmbtu", fuelEg);
        } else {
          out.putNull("fuel_consumed_for_eg_mmbtu");
        }

        Double fuelCost = getDouble(row, "cost-per-btu");
        if (fuelCost != null) {
          out.put("fuel_cost_per_mmbtu", fuelCost);
        } else {
          out.putNull("fuel_cost_per_mmbtu");
        }

        Double sulfur = getDouble(row, "sulfur-content");
        if (sulfur != null) {
          out.put("sulfur_content", sulfur);
        } else {
          out.putNull("sulfur_content");
        }

        Double ash = getDouble(row, "ash-content");
        if (ash != null) {
          out.put("ash_content", ash);
        } else {
          out.putNull("ash_content");
        }

        result.add(out);
      }

      LOGGER.debug("EIA Electricity Generation: transformed {} records", result.size());
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("EIA Electricity Generation: failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }
}
