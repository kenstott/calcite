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
 * Maps a streamed row from an EPA AirData daily-summary bulk CSV
 * ({@code https://aqs.epa.gov/aqsweb/airdata/daily_<param>_<year>.zip}) into the
 * {@code epa_daily_aqi} schema. Replaces the per-state {@code dailyData/byState} AQS API
 * fan-out (51 states &times; 5 pollutants &times; year, rate-limited to 10 req/min) with one
 * national ZIP per pollutant-year — the file is already one pollutant, so no param filter is
 * needed here.
 *
 * <p>AirData daily-summary columns → schema columns: {@code State Code}→state_fips,
 * {@code State Code}+{@code County Code}→county_fips, {@code Site Num}→site_number,
 * {@code Parameter Code}/{@code Parameter Name}→parameter_code/parameter_name,
 * {@code Date Local}→date (+derived year), {@code Sample Duration}/{@code Pollutant Standard},
 * {@code Arithmetic Mean}, {@code 1st Max Value}→first_max_value, {@code AQI},
 * {@code Observation Count}, {@code Latitude}/{@code Longitude}. Non-US (territory) rows are
 * dropped to keep the {@code geo.counties} join valid.
 */
public class EpaAirDataDailyTransformer implements RowTransformer {

  @Override public List<Map<String, Object>> transform(Map<String, Object> row, RowContext context) {
    Map<String, String> n = EpaAirDataSupport.normalize(row);

    String stateFips = EpaAirDataSupport.padFips(n.get("statecode"), 2);
    if (stateFips == null || !EpaAirDataSupport.US_STATE_FIPS.contains(stateFips)) {
      return Collections.emptyList();
    }
    String countyCode = EpaAirDataSupport.padFips(n.get("countycode"), 3);
    String date = n.get("datelocal");

    Map<String, Object> out = new LinkedHashMap<String, Object>();
    out.put("state_fips", stateFips);
    out.put("county_fips", countyCode == null ? null : stateFips + countyCode);
    out.put("site_number", n.get("sitenum"));
    out.put("parameter_code", n.get("parametercode"));
    out.put("parameter_name", n.get("parametername"));
    out.put("date", date);
    out.put("year", EpaAirDataSupport.yearOf(date));
    out.put("sample_duration", n.get("sampleduration"));
    out.put("pollutant_standard", n.get("pollutantstandard"));
    out.put("arithmetic_mean", EpaAirDataSupport.parseDouble(n.get("arithmeticmean")));
    out.put("first_max_value", EpaAirDataSupport.parseDouble(n.get("1stmaxvalue")));
    out.put("aqi", EpaAirDataSupport.parseInt(n.get("aqi")));
    out.put("observation_count", EpaAirDataSupport.parseInt(n.get("observationcount")));
    out.put("latitude", EpaAirDataSupport.parseDouble(n.get("latitude")));
    out.put("longitude", EpaAirDataSupport.parseDouble(n.get("longitude")));
    return Collections.<Map<String, Object>>singletonList(out);
  }
}
