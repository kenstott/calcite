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

public class EiaSEDSTransformer extends EiaV2Transformer implements ResponseTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EIA SEDS: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode data = extractDataArray(response);
      ArrayNode result = MAPPER.createArrayNode();

      for (JsonNode row : data) {
        String period = getString(row, "period");
        ObjectNode out = MAPPER.createObjectNode();

        out.put("consumption_year", parseYear(period));

        String stateAbbr = getString(row, "stateid", "location");
        if (stateAbbr != null) {
          out.put("state_abbr", stateAbbr);
        } else {
          out.putNull("state_abbr");
        }

        out.putNull("state_fips");

        String stateName = getString(row, "stateDescription", "locationDescription");
        if (stateName != null) {
          out.put("state_name", stateName);
        } else {
          out.putNull("state_name");
        }

        String msn = getString(row, "msn");
        if (msn != null) {
          out.put("msn", msn);
        } else {
          out.putNull("msn");
        }

        out.put("sector", deriveSector(msn));
        out.put("fuel_type", deriveFuel(msn));

        Double value = getDouble(row, "value");
        if (value != null) {
          out.put("value", value);
        } else {
          out.putNull("value");
        }

        String units = getString(row, "units");
        if (units != null) {
          out.put("units", units);
        } else {
          out.putNull("units");
        }

        // Classify value into the appropriate column based on units
        if (value != null && units != null) {
          String unitsLower = units.toLowerCase();
          if (unitsLower.contains("btu")) {
            out.put("consumption_bbtu", value);
            out.putNull("expenditure_million");
            out.putNull("price_per_mmbtu");
          } else if (unitsLower.contains("per")) {
            out.putNull("consumption_bbtu");
            out.putNull("expenditure_million");
            out.put("price_per_mmbtu", value);
          } else if (unitsLower.contains("dollars")) {
            out.putNull("consumption_bbtu");
            out.put("expenditure_million", value);
            out.putNull("price_per_mmbtu");
          } else {
            out.putNull("consumption_bbtu");
            out.putNull("expenditure_million");
            out.putNull("price_per_mmbtu");
          }
        } else {
          out.putNull("consumption_bbtu");
          out.putNull("expenditure_million");
          out.putNull("price_per_mmbtu");
        }

        String seriesDesc = getString(row, "series-description");
        if (seriesDesc != null) {
          out.put("series_description", seriesDesc);
        } else {
          out.putNull("series_description");
        }

        result.add(out);
      }

      LOGGER.debug("EIA SEDS: transformed {} records", result.size());
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("EIA SEDS: failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private String deriveSector(String msn) {
    if (msn == null || msn.length() < 4) {
      return "Unknown";
    }
    // MSN structure: pos 0-1 = fuel, pos 2-3 = sector
    String sectorCode = msn.substring(2, 4).toUpperCase();
    if ("RC".equals(sectorCode)) {
      return "Residential";
    } else if ("CC".equals(sectorCode)) {
      return "Commercial";
    } else if ("IC".equals(sectorCode)) {
      return "Industrial";
    } else if ("TC".equals(sectorCode)) {
      return "Transportation";
    } else if ("EI".equals(sectorCode)) {
      return "Electric Power";
    } else if ("AC".equals(sectorCode)) {
      return "Total";
    }
    return sectorCode;
  }

  private String deriveFuel(String msn) {
    if (msn == null || msn.length() < 2) {
      return "Unknown";
    }
    // MSN structure: pos 0-1 = fuel code
    String fuelCode = msn.substring(0, 2).toUpperCase();
    if ("CC".equals(fuelCode) || "CL".equals(fuelCode)) {
      return "Coal";
    } else if ("MG".equals(fuelCode) || "PQ".equals(fuelCode)) {
      return "Petroleum";
    } else if ("NG".equals(fuelCode)) {
      return "Natural Gas";
    } else if ("NU".equals(fuelCode)) {
      return "Nuclear";
    } else if ("HY".equals(fuelCode)) {
      return "Hydroelectric";
    } else if ("GE".equals(fuelCode)) {
      return "Geothermal";
    } else if ("WY".equals(fuelCode)) {
      return "Wind";
    } else if ("SO".equals(fuelCode)) {
      return "Solar";
    } else if ("WD".equals(fuelCode) || "LO".equals(fuelCode)) {
      return "Wood/Biomass";
    } else if ("TE".equals(fuelCode)) {
      return "Total";
    }
    return fuelCode;
  }
}
