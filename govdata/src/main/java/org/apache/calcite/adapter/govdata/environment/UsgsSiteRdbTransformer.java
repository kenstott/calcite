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
 * Maps a USGS NWIS {@code site} service RDB response ({@code siteOutput=expanded}) into
 * {@code water_sites} rows — one row per monitoring site.
 *
 * <p>Single-block RDB: one header row (column names), a types row, then one data row per
 * site. Columns are addressed by name (order varies with {@code siteOutput}). {@code state_cd}
 * is a 2-digit FIPS; {@code county_cd} is the 3-digit county within state, combined into a
 * 5-digit {@code county_fips}.
 */
public class UsgsSiteRdbTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsgsSiteRdbTransformer.class);
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
          if (line.isEmpty() || line.charAt(0) == '#') {
            continue;
          }
          if (idx == null) {
            // First non-comment line is the header.
            idx = new HashMap<String, Integer>();
            String[] headers = UsgsRdbSupport.splitTabs(line);
            for (int i = 0; i < headers.length; i++) {
              idx.put(headers[i].trim(), i);
            }
            continue;
          }
          if (UsgsRdbSupport.isCommentOrTypesRow(line)) {
            continue;
          }
          String[] f = UsgsRdbSupport.splitTabs(line);
          String siteNo = get(f, idx, "site_no");
          if (siteNo == null || siteNo.trim().isEmpty()) {
            continue;
          }
          ObjectNode row = MAPPER.createObjectNode();
          UsgsRdbSupport.putText(row, "site_no", siteNo);
          UsgsRdbSupport.putText(row, "agency_cd", get(f, idx, "agency_cd"));
          UsgsRdbSupport.putText(row, "station_name", get(f, idx, "station_nm"));
          UsgsRdbSupport.putText(row, "site_type", get(f, idx, "site_tp_cd"));
          UsgsRdbSupport.putDouble(row, "latitude", get(f, idx, "dec_lat_va"));
          UsgsRdbSupport.putDouble(row, "longitude", get(f, idx, "dec_long_va"));
          String stateCd = UsgsRdbSupport.trimOrNull(get(f, idx, "state_cd"));
          String countyCd = UsgsRdbSupport.trimOrNull(get(f, idx, "county_cd"));
          if (stateCd != null) {
            row.put("state_fips", stateCd);
          } else {
            row.putNull("state_fips");
          }
          if (stateCd != null && countyCd != null) {
            row.put("county_fips", stateCd + countyCd);
          } else {
            row.putNull("county_fips");
          }
          UsgsRdbSupport.putText(row, "huc_code", get(f, idx, "huc_cd"));
          UsgsRdbSupport.putDouble(row, "drainage_area_sqmi", get(f, idx, "drain_area_va"));
          out.add(row);
        }
      }
      LOGGER.debug("water_sites: transformed {} sites", out.size());
      return MAPPER.writeValueAsString(out);
    } catch (IOException e) {
      throw new RuntimeException("water_sites transform failed: " + e.getMessage(), e);
    }
  }

  private static String get(String[] fields, Map<String, Integer> idx, String col) {
    Integer i = idx.get(col);
    return i != null && i < fields.length ? fields[i] : null;
  }
}
