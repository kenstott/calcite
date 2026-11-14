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
package org.apache.calcite.adapter.govdata.weather;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Maps the NOAA NCEI Climate at a Glance global land-and-ocean series (NOAAGlobalTemp) CSV
 * ({@code # } comment lines, a {@code Date,Departure from Average} header, then
 * {@code yyyymm,value} rows) into one row per month. Values are departures in degrees Celsius
 * from the 1901-2000 average.
 */
public class NoaaGlobalTempTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    ArrayNode out = MAPPER.createArrayNode();
    boolean sawHeader = false;
    for (String line : response.split("\r?\n")) {
      if (line.startsWith("Date,")) {
        sawHeader = true;
        continue;
      }
      if (!sawHeader || line.trim().isEmpty()) {
        continue;
      }
      String[] f = line.split(",");
      if (f.length != 2 || f[0].trim().length() != 6) {
        throw new RuntimeException("NOAAGlobalTemp: bad row '" + line + "'");
      }
      ObjectNode row = MAPPER.createObjectNode();
      row.put("year", Integer.parseInt(f[0].trim().substring(0, 4)));
      row.put("month", Integer.parseInt(f[0].trim().substring(4, 6)));
      row.put("anomaly_c", Double.parseDouble(f[1].trim()));
      out.add(row);
    }
    if (!sawHeader) {
      throw new RuntimeException("NOAAGlobalTemp: no 'Date,' header in response: "
          + response.substring(0, Math.min(200, response.length())));
    }
    try {
      return MAPPER.writeValueAsString(out);
    } catch (Exception e) {
      throw new RuntimeException("NOAAGlobalTemp transform failed: " + e.getMessage(), e);
    }
  }
}
