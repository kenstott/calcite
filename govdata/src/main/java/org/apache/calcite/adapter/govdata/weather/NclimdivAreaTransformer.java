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
 * Unpivots an NCEI nClimDiv statewide/regional/national file
 * into one row per (area, year, month). Each record is {@code SSS D EE YYYY} followed by twelve
 * 7-character {@code f7.2} monthly values; months not yet published are {@code -99.90}
 * (temperature) or {@code -9.99} (precipitation) and are skipped, so no sentinel reaches the table.
 */
public class NclimdivAreaTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int VALUES_START = 10;
  private static final int WIDTH = 7;

  @Override public String transform(String response, RequestContext context) {
    String element = context.getDimensionValues().get("element");
    final String elementCode;
    final String missing;
    if ("tmpc".equals(element)) {
      elementCode = "02";
      missing = "-99.90";
    } else if ("tmax".equals(element)) {
      elementCode = "27";
      missing = "-99.90";
    } else if ("tmin".equals(element)) {
      elementCode = "28";
      missing = "-99.90";
    } else if ("pcpn".equals(element)) {
      elementCode = "01";
      missing = "-9.99";
    } else {
      throw new IllegalStateException("nClimDiv: unknown element '" + element + "'");
    }
    ArrayNode out = MAPPER.createArrayNode();
    for (String line : response.split("\r?\n")) {
      if (line.trim().isEmpty()) {
        continue;
      }
      if (line.length() < VALUES_START + 12 * WIDTH || line.charAt(3) != '0'
          || !line.substring(4, 6).equals(elementCode)) {
        throw new IllegalStateException("nClimDiv: unexpected record for element " + element
            + ": '" + line + "'");
      }
      String code = line.substring(0, 3);
      String name = NclimdivAreaCodes.name(code);
      String type = NclimdivAreaCodes.type(code);
      int year = Integer.parseInt(line.substring(6, 10));
      for (int m = 0; m < 12; m++) {
        int from = VALUES_START + m * WIDTH;
        String cell = line.substring(from, from + WIDTH).trim();
        if (cell.equals(missing)) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();
        row.put("area_code", code);
        row.put("area_name", name);
        row.put("area_type", type);
        row.put("year", year);
        row.put("month", m + 1);
        row.put("value", Double.parseDouble(cell));
        out.add(row);
      }
    }
    if (out.size() == 0) {
      throw new IllegalStateException("nClimDiv: no data rows parsed for element " + element);
    }
    try {
      return MAPPER.writeValueAsString(out);
    } catch (Exception e) {
      throw new RuntimeException("nClimDiv transform failed: " + e.getMessage(), e);
    }
  }
}
