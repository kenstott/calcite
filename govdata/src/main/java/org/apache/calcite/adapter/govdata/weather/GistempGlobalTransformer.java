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
 * Maps the NASA GISTEMP v4 global land-ocean table ({@code GLB.Ts+dSST.csv}: a title line, a
 * {@code Year,Jan..Dec,J-D,...} header, then one row per year) into one row per month.
 * Values are anomalies in degrees Celsius against 1951-1980; {@code ***} marks months not yet
 * published and produces no row.
 */
public class GistempGlobalTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    ArrayNode out = MAPPER.createArrayNode();
    boolean sawHeader = false;
    for (String line : response.split("\r?\n")) {
      if (line.startsWith("Year,")) {
        sawHeader = true;
        continue;
      }
      if (!sawHeader || line.trim().isEmpty()) {
        continue;
      }
      String[] f = line.split(",");
      if (f.length < 13) {
        throw new RuntimeException("GISTEMP: short row '" + line + "'");
      }
      int year = Integer.parseInt(f[0].trim());
      for (int m = 1; m <= 12; m++) {
        String v = f[m].trim();
        if (v.startsWith("*")) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();
        row.put("year", year);
        row.put("month", m);
        row.put("anomaly_c", Double.parseDouble(v));
        out.add(row);
      }
    }
    if (!sawHeader) {
      throw new RuntimeException("GISTEMP: no 'Year,' header in response: "
          + response.substring(0, Math.min(200, response.length())));
    }
    try {
      return MAPPER.writeValueAsString(out);
    } catch (Exception e) {
      throw new RuntimeException("GISTEMP transform failed: " + e.getMessage(), e);
    }
  }
}
