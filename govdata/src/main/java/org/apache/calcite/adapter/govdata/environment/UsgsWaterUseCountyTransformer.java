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
package org.apache.calcite.adapter.govdata.environment;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps the USGS "Estimated Use of Water in the United States, County-Level Data" CSV
 * (ScienceBase data release, e.g. {@code usco2015v2.0.csv}) into {@code water_use_county}
 * rows — one row per county-year.
 *
 * <p>The file is plain comma-delimited, but the real header is not on line 1: line 1 is a
 * single citation string (with 140 trailing empty fields from unescaped commas), and the
 * column-code header (e.g. {@code STATE,STATEFIPS,COUNTY,...}) is line 2. This transformer
 * locates that header line by its known first field ({@code STATE}) rather than assuming a
 * fixed line number, then reduces the ~140 raw sector columns (public supply/domestic/
 * industrial/irrigation/livestock/aquaculture/mining/thermoelectric, each broken out by
 * groundwater/surface-water and fresh/saline) to the subset carried by {@code water_use_county}.
 * Missing values are encoded upstream as {@code --} or {@code #N/A}; both parse to null via
 * {@link UsgsRdbSupport#putDouble}.
 */
public class UsgsWaterUseCountyTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsgsWaterUseCountyTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      return "[]";
    }
    try {
      ArrayNode out = MAPPER.createArrayNode();
      Map<String, Integer> idx = null;
      try (BufferedReader reader = new BufferedReader(new StringReader(response))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.isEmpty()) {
            continue;
          }
          if (idx == null) {
            // Skip lines until the real header (first field STATE) is found — line 1 is a
            // citation string, not data.
            String[] maybeHeaders = line.split(",", -1);
            if (maybeHeaders.length == 0 || !"STATE".equals(maybeHeaders[0].trim())) {
              continue;
            }
            idx = new HashMap<String, Integer>();
            for (int i = 0; i < maybeHeaders.length; i++) {
              idx.put(maybeHeaders[i].trim(), i);
            }
            continue;
          }
          String[] f = line.split(",", -1);
          String stateFips = UsgsRdbSupport.trimOrNull(get(f, idx, "STATEFIPS"));
          String countyFips = UsgsRdbSupport.trimOrNull(get(f, idx, "COUNTYFIPS"));
          String fips = UsgsRdbSupport.trimOrNull(get(f, idx, "FIPS"));
          if (fips == null) {
            continue;
          }
          ObjectNode row = MAPPER.createObjectNode();
          UsgsRdbSupport.putText(row, "state", get(f, idx, "STATE"));
          UsgsRdbSupport.putText(row, "state_fips", stateFips);
          UsgsRdbSupport.putText(row, "county_name", get(f, idx, "COUNTY"));
          UsgsRdbSupport.putText(row, "county_fips", fips);
          Integer censusYear = parseIntOrNull(get(f, idx, "YEAR"));
          if (censusYear == null) {
            row.putNull("census_year");
          } else {
            row.put("census_year", censusYear.intValue());
          }

          UsgsRdbSupport.putDouble(row, "total_population_thousands", get(f, idx, "TP-TotPop"));

          UsgsRdbSupport.putDouble(row, "public_supply_population_thousands", get(f, idx, "PS-TOPop"));
          UsgsRdbSupport.putDouble(row, "public_supply_withdrawals_mgd", get(f, idx, "PS-Wtotl"));
          UsgsRdbSupport.putDouble(row, "public_supply_fresh_groundwater_mgd", get(f, idx, "PS-WGWFr"));
          UsgsRdbSupport.putDouble(row, "public_supply_fresh_surfacewater_mgd", get(f, idx, "PS-WSWFr"));

          UsgsRdbSupport.putDouble(row, "domestic_selfsupplied_population_thousands", get(f, idx, "DO-SSPop"));
          UsgsRdbSupport.putDouble(row, "domestic_selfsupplied_withdrawals_mgd", get(f, idx, "DO-WFrTo"));
          UsgsRdbSupport.putDouble(row, "domestic_public_supply_deliveries_mgd", get(f, idx, "DO-PSDel"));

          UsgsRdbSupport.putDouble(row, "industrial_withdrawals_mgd", get(f, idx, "IN-Wtotl"));
          UsgsRdbSupport.putDouble(row, "industrial_fresh_groundwater_mgd", get(f, idx, "IN-WGWFr"));
          UsgsRdbSupport.putDouble(row, "industrial_fresh_surfacewater_mgd", get(f, idx, "IN-WSWFr"));
          UsgsRdbSupport.putDouble(row, "industrial_saline_withdrawals_mgd", get(f, idx, "IN-WSaTo"));

          UsgsRdbSupport.putDouble(row, "irrigation_withdrawals_mgd", get(f, idx, "IR-WFrTo"));
          UsgsRdbSupport.putDouble(row, "irrigation_groundwater_mgd", get(f, idx, "IR-WGWFr"));
          UsgsRdbSupport.putDouble(row, "irrigation_surfacewater_mgd", get(f, idx, "IR-WSWFr"));
          UsgsRdbSupport.putDouble(row, "irrigation_consumptive_use_mgd", get(f, idx, "IR-CUsFr"));
          UsgsRdbSupport.putDouble(row, "irrigated_acres_thousands", get(f, idx, "IR-IrTot"));

          UsgsRdbSupport.putDouble(row, "livestock_withdrawals_mgd", get(f, idx, "LI-WFrTo"));

          UsgsRdbSupport.putDouble(row, "aquaculture_withdrawals_mgd", get(f, idx, "AQ-Wtotl"));
          UsgsRdbSupport.putDouble(row, "aquaculture_fresh_withdrawals_mgd", get(f, idx, "AQ-WFrTo"));

          UsgsRdbSupport.putDouble(row, "mining_withdrawals_mgd", get(f, idx, "MI-Wtotl"));
          UsgsRdbSupport.putDouble(row, "mining_fresh_groundwater_mgd", get(f, idx, "MI-WGWFr"));
          UsgsRdbSupport.putDouble(row, "mining_fresh_surfacewater_mgd", get(f, idx, "MI-WSWFr"));

          UsgsRdbSupport.putDouble(row, "thermoelectric_withdrawals_mgd", get(f, idx, "PT-Wtotl"));
          UsgsRdbSupport.putDouble(row, "thermoelectric_fresh_groundwater_mgd", get(f, idx, "PT-WGWFr"));
          UsgsRdbSupport.putDouble(row, "thermoelectric_fresh_surfacewater_mgd", get(f, idx, "PT-WSWFr"));
          UsgsRdbSupport.putDouble(row, "thermoelectric_saline_withdrawals_mgd", get(f, idx, "PT-WSaTo"));
          UsgsRdbSupport.putDouble(row, "thermoelectric_consumptive_use_mgd", get(f, idx, "PT-CUTot"));
          UsgsRdbSupport.putDouble(row, "thermoelectric_power_generated_gwh", get(f, idx, "PT-Power"));
          UsgsRdbSupport.putDouble(row, "thermoelectric_oncethrough_withdrawals_mgd", get(f, idx, "PO-Wtotl"));
          UsgsRdbSupport.putDouble(row, "thermoelectric_recirculating_withdrawals_mgd", get(f, idx, "PC-Wtotl"));

          UsgsRdbSupport.putDouble(row, "total_withdrawals_mgd", get(f, idx, "TO-Wtotl"));
          UsgsRdbSupport.putDouble(row, "total_fresh_groundwater_mgd", get(f, idx, "TO-WGWFr"));
          UsgsRdbSupport.putDouble(row, "total_fresh_surfacewater_mgd", get(f, idx, "TO-WSWFr"));
          UsgsRdbSupport.putDouble(row, "total_saline_withdrawals_mgd", get(f, idx, "TO-WSaTo"));
          UsgsRdbSupport.putDouble(row, "total_consumptive_use_partial_mgd", get(f, idx, "TO-CUTotPartial"));

          out.add(row);
        }
      }
      LOGGER.debug("water_use_county: transformed {} counties", out.size());
      return MAPPER.writeValueAsString(out);
    } catch (IOException e) {
      throw new RuntimeException("water_use_county transform failed: " + e.getMessage(), e);
    }
  }

  private static String get(String[] fields, Map<String, Integer> idx, String col) {
    Integer i = idx.get(col);
    return i != null && i < fields.length ? fields[i] : null;
  }

  private static Integer parseIntOrNull(String value) {
    String v = UsgsRdbSupport.trimOrNull(value);
    if (v == null) {
      return null;
    }
    try {
      return Integer.valueOf((int) Double.parseDouble(v));
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
