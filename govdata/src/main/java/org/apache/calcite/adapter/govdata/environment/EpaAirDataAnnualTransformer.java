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

import org.apache.calcite.adapter.file.etl.RowContext;
import org.apache.calcite.adapter.file.etl.RowTransformer;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps a streamed row from the EPA AirData annual-summary bulk CSV
 * ({@code https://aqs.epa.gov/aqsweb/airdata/annual_conc_by_monitor_<year>.zip}) into the
 * {@code epa_annual_aqi} schema. Replaces the per-state {@code annualData/byState} AQS API
 * fan-out with one national ZIP per year (all pollutants), filtered here to the 5 AQI
 * pollutants and the 50 states + DC.
 *
 * <p>The annual-summary file carries no {@code AQI} column (it is concentration-by-monitor),
 * so {@code aqi} is emitted null — consistent with the prior API behavior, where the annual
 * table's {@code aqi} was already excluded from the DQ null check
 * ({@code weather_dq.sql}: "aqi not populated in annual summary records"). It does carry
 * {@code Valid Day Count}.
 */
public class EpaAirDataAnnualTransformer implements RowTransformer {

  @Override public List<Map<String, Object>> transform(Map<String, Object> row, RowContext context) {
    Map<String, String> n = EpaAirDataSupport.normalize(row);

    String stateFips = EpaAirDataSupport.padFips(n.get("statecode"), 2);
    String paramCode = n.get("parametercode");
    if (stateFips == null || !EpaAirDataSupport.US_STATE_FIPS.contains(stateFips)
        || paramCode == null || !EpaAirDataSupport.AQI_PARAMS.contains(paramCode.trim())) {
      return Collections.emptyList();
    }
    String countyCode = EpaAirDataSupport.padFips(n.get("countycode"), 3);

    Map<String, Object> out = new LinkedHashMap<String, Object>();
    out.put("state_fips", stateFips);
    out.put("county_fips", countyCode == null ? null : stateFips + countyCode);
    out.put("parameter_code", paramCode.trim());
    out.put("parameter_name", n.get("parametername"));
    out.put("year", EpaAirDataSupport.parseInt(n.get("year")));
    out.put("arithmetic_mean", EpaAirDataSupport.parseDouble(n.get("arithmeticmean")));
    out.put("first_max_value", EpaAirDataSupport.parseDouble(n.get("1stmaxvalue")));
    out.put("observation_count", EpaAirDataSupport.parseInt(n.get("observationcount")));
    out.put("valid_day_count", EpaAirDataSupport.parseInt(n.get("validdaycount")));
    out.put("aqi", null);
    return Collections.<Map<String, Object>>singletonList(out);
  }
}
