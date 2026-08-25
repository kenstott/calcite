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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class EiaElectricityGenerationTransformer extends EiaV2Transformer
    implements ResponseTransformer {

  /** Real US state/DC postal codes -- everything else EIA reports under
   * {@code location} (e.g. "US" for the national total, or numeric/named
   * census-division codes such as "90" for Pacific) is a geographic rollup,
   * not a state. */
  private static final Set<String> US_STATE_ABBREVIATIONS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA",
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA",
          "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
          "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX",
          "UT", "VT", "VA", "WA", "WV", "WI", "WY")));

  /** Fuel (energy source) codes that are rollups/aggregations of other fuel
   * codes rather than true leaf-level fuels. */
  private static final Set<String> ROLLUP_CODES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          "ALL", "FOS", "REN", "AOR", "COW", "PET", "TSN")));

  /** Sector codes that are rollups/aggregations of other sector codes rather
   * than true leaf-level sectors. Leaf sectors are numbered 1-8 (Electric
   * Utility, IPP Non-CHP/CHP, Commercial Non-CHP/CHP, Industrial Non-CHP/CHP,
   * Residential); these codes are the distinct, disjoint domain of aggregate
   * sector ids EIA reports alongside them. */
  private static final Set<String> SECTOR_ROLLUP_CODES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          "90", "94", "95", "96", "97", "98", "99")));

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EIA Electricity Generation: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode data = extractDataArray(response);
      ArrayNode result = MAPPER.createArrayNode();
      int rowsSkipped = 0;

      for (JsonNode row : data) {
        String period = getString(row, "period");
        int generationYear;
        try {
          generationYear = parseYear(period);
        } catch (NumberFormatException e) {
          // A malformed period must not fabricate year 0 for this row, nor abort every
          // other row in the batch -- skip and count just this one.
          rowsSkipped++;
          LOGGER.warn("EIA Electricity Generation: skipping row with unparseable period '{}'", period);
          continue;
        }
        ObjectNode out = MAPPER.createObjectNode();

        out.put("generation_year", generationYear);

        Integer month = parseMonth(period);
        if (month != null) {
          out.put("generation_month", month);
        } else {
          out.putNull("generation_month");
        }

        String stateAbbr = getString(row, "location", "stateid");
        if (stateAbbr != null && "US".equals(stateAbbr)) {
          out.putNull("state_abbr");
          out.put("geo_level", "national");
        } else if (stateAbbr != null && US_STATE_ABBREVIATIONS.contains(stateAbbr)) {
          out.put("state_abbr", stateAbbr);
          out.put("geo_level", "state");
        } else if (stateAbbr != null) {
          // Census division codes (e.g. numeric codes like "90" for Pacific) leak through
          // the same location field -- these are rollups, not real states.
          out.putNull("state_abbr");
          out.put("geo_level", "division");
        } else {
          out.putNull("state_abbr");
          out.putNull("geo_level");
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
          out.put("fuel_is_rollup", ROLLUP_CODES.contains(sourceCode));
        } else {
          out.putNull("energy_source_code");
          out.putNull("fuel_is_rollup");
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
          out.put("sector_is_rollup", SECTOR_ROLLUP_CODES.contains(sectorCode));
        } else {
          out.putNull("sector_code");
          out.putNull("sector_is_rollup");
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

      if (rowsSkipped > 0) {
        LOGGER.warn("EIA Electricity Generation: skipped {} row(s) with unparseable period",
            rowsSkipped);
      }
      LOGGER.debug("EIA Electricity Generation: transformed {} records", result.size());
      return result.toString();

    } catch (Exception e) {
      throw new RuntimeException("EIA Electricity Generation: failed to parse response for "
          + context.getUrl(), e);
    }
  }
}
