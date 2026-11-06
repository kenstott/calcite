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

/**
 * Transforms EIA API v2 {@code petroleum/pri/gnd} (Gasoline and Diesel Fuel Update) weekly retail
 * price rows. The geography level is derived from the EIA duoarea code: {@code NUS} national,
 * {@code R10..R50} PADD, {@code R1X/R1Y/R1Z/R5XCA} sub-PADD, {@code S<ST>} state, {@code Y...}
 * city. An unrecognised code is a source-shape change and fails the batch.
 */
public class EiaRetailGasolinePricesTransformer extends EiaV2Transformer
    implements ResponseTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EIA Retail Gasoline Prices: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode data = extractDataArray(response);
      ArrayNode result = MAPPER.createArrayNode();
      int rowsSkipped = 0;

      for (JsonNode row : data) {
        String period = getString(row, "period");
        int priceYear;
        try {
          priceYear = parseYear(period);
        } catch (NumberFormatException e) {
          rowsSkipped++;
          LOGGER.warn("EIA Retail Gasoline Prices: skipping row with unparseable period '{}'",
              period);
          continue;
        }
        String areaCode = getString(row, "duoarea");
        ObjectNode out = MAPPER.createObjectNode();
        putOrNull(out, "report_date", period);
        out.put("price_year", priceYear);
        putOrNull(out, "eia_area_code", areaCode);
        putOrNull(out, "area_name", getString(row, "area-name"));
        putOrNull(out, "geo_level", geoLevel(areaCode));
        putOrNull(out, "state_abbr", stateAbbr(areaCode));
        putOrNull(out, "product_code", getString(row, "product"));
        putOrNull(out, "product", getString(row, "product-name"));
        Double price = getDouble(row, "value");
        if (price != null) {
          out.put("price_dollars_per_gallon", price);
        } else {
          out.putNull("price_dollars_per_gallon");
        }
        putOrNull(out, "series_id", getString(row, "series"));
        putOrNull(out, "series_description", getString(row, "series-description"));
        result.add(out);
      }

      if (rowsSkipped > 0) {
        LOGGER.warn("EIA Retail Gasoline Prices: skipped {} row(s) with unparseable period",
            rowsSkipped);
      }
      return result.toString();
    } catch (Exception e) {
      throw new RuntimeException("EIA Retail Gasoline Prices: failed to parse response for "
          + context.getUrl(), e);
    }
  }

  private static void putOrNull(ObjectNode out, String field, String value) {
    if (value != null) {
      out.put(field, value);
    } else {
      out.putNull(field);
    }
  }

  static String geoLevel(String code) {
    if (code == null) {
      return null;
    }
    if ("NUS".equals(code)) {
      return "national";
    }
    if (code.matches("R[1-5]0")) {
      return "padd";
    }
    if (code.matches("R1[XYZ]|R5XCA")) {
      return "sub_padd";
    }
    if (code.matches("S[A-Z]{2}")) {
      return "state";
    }
    if (code.matches("Y[0-9A-Z]{3,4}")) {
      return "city";
    }
    throw new IllegalStateException("Unrecognised EIA retail price area code '" + code + "'");
  }

  static String stateAbbr(String code) {
    return code != null && code.matches("S[A-Z]{2}") ? code.substring(1) : null;
  }
}
