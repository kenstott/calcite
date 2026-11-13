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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms the BTS NTAD "Hazard Exposure: National Highway System Bridges" ArcGIS
 * FeatureServer JSON into {@code bridge_hazard_exposure} rows.
 *
 * <p>Called once per page by the framework OFFSET paginator (pageSize 2000 =
 * ArcGIS {@code maxRecordCount}); it flattens {@code features[].attributes} to one row
 * per bridge. It must not throw on {@code exceededTransferLimit} (true on every full
 * page; termination is a short final page). The SLOSH flags are Yes/No/No Data in the
 * source and are passed through unchanged; "No Data" means SLOSH did not evaluate the
 * bridge (inland), which is distinct from "No".
 */
public class BridgeHazardExposureTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BridgeHazardExposureTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String[][] TEXT = {
      {"state_code", "STATE_CODE_001"},
      {"structure_number", "STRUCTURE_NUMBER_008"},
      {"county_code", "COUNTY_CODE_003"},
      {"features_intersected", "FEATURES_DESC_006A"},
      {"facility_carried", "FACILITY_CARRIED_007"},
      {"surge_cat1_inundated", "HurricaneCat1_SLOSH_Inundated"},
      {"surge_cat2_inundated", "HurricaneCat2_SLOSH_Inundated"},
      {"surge_cat3_inundated", "HurricaneCat3_SLOSH_Inundated"},
      {"surge_cat4_inundated", "HurricaneCat4_SLOSH_Inundated"},
      {"surge_cat5_inundated", "HurricaneCat5_SLOSH_Inundated"},
  };

  private static final String[][] NUM = {
      {"latitude", "LATDD"},
      {"longitude", "LONGDD"},
      {"riverine_flooding_annual_freq", "RiverineFlooding_NRI_AnnualFreq"},
      {"coastal_flooding_annual_freq", "CoastalFlooding_NRI_AnnualFreq"},
      {"hurricane_annual_freq", "Hurricane_NRI_AnnualFreq"},
      {"strong_wind_annual_freq", "StrongWind_NRI_AnnualFreq"},
      {"tornado_annual_freq", "Tornado_NRI_AnnualFreq"},
      {"hail_annual_freq", "Hail_NRI_AnnualFreq"},
      {"lightning_annual_freq", "Lightning_NRI_AnnualFreq"},
      {"ice_storm_annual_freq", "IceStorm_NRI_AnnualFreq"},
      {"winter_weather_annual_freq", "WinterWeather_NRI_AnnualFreq"},
      {"cold_wave_annual_freq", "ColdWave_NRI_AnnualFreq"},
      {"heat_wave_annual_freq", "HeatWave_NRI_AnnualFreq"},
      {"drought_annual_freq", "Drought_NRI_AnnualFreq"},
      {"wildfire_max_burn_prob", "Wildfire_USDA_MaxBurnProb"},
      {"wildfire_mean_burn_prob", "Wildfire_USDA_MeanBurnProb"},
      {"wildfire_min_burn_prob", "Wildfire_USDA_MinBurnProb"},
      {"sinkhole_max_susceptibility", "Sinkhole_USGS_MaxSusceptibility"},
      {"landslide_max_index", "Landslide_USGS_Max"},
      {"landslide_median_index", "Landslide_USGS_Median"},
  };

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      throw new RuntimeException("bridge_hazard_exposure: empty response from BTS ArcGIS");
    }
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode error = root.path("error");
      if (!error.isMissingNode() && !error.isNull()) {
        throw new RuntimeException("BTS ArcGIS error: " + error.toString());
      }
      JsonNode features = root.path("features");
      if (!features.isArray()) {
        throw new RuntimeException("bridge_hazard_exposure: no 'features' array in response");
      }
      ArrayNode result = MAPPER.createArrayNode();
      for (JsonNode feature : features) {
        JsonNode a = feature.path("attributes");
        if (a.isMissingNode()) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();
        for (String[] m : TEXT) {
          JsonNode v = a.path(m[1]);
          if (v.isNull() || v.isMissingNode() || v.asText().trim().isEmpty()) {
            row.putNull(m[0]);
          } else {
            row.put(m[0], v.asText().trim());
          }
        }
        for (String[] m : NUM) {
          JsonNode v = a.path(m[1]);
          if (v.isNumber()) {
            row.put(m[0], v.asDouble());
          } else {
            row.putNull(m[0]);
          }
        }
        String st = a.path("STATE_CODE_001").asText("").trim();
        String co = a.path("COUNTY_CODE_003").asText("").trim();
        if (!st.isEmpty() && !co.isEmpty()) {
          row.put("county_fips", st + co);
        } else {
          row.putNull("county_fips");
        }
        result.add(row);
      }
      LOGGER.debug("bridge_hazard_exposure: transformed {} features", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("bridge_hazard_exposure transform failed", e);
    }
  }
}
