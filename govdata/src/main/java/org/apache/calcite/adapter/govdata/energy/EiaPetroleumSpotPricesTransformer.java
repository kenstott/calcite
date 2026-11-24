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
/**
 * Transforms EIA API v2 {@code petroleum/pri/spt} (Petroleum Spot Prices) daily rows into tall
 * JSON rows, one per (series, report_date). The price unit differs by series ($/GAL for
 * products, $/BBL for crude), so the value is kept in a single {@code price} column next to
 * the unit EIA publishes.
 */
public class EiaPetroleumSpotPricesTransformer extends EiaV2Transformer
    implements ResponseTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EIA Petroleum Spot Prices: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode data = extractDataArray(response);
      ArrayNode result = MAPPER.createArrayNode();
      for (JsonNode row : data) {
        String period = getString(row, "period");
        ObjectNode out = MAPPER.createObjectNode();
        putOrNull(out, "report_date", period);
        out.put("price_year", parseYear(period));
        putOrNull(out, "series_id", getString(row, "series"));
        putOrNull(out, "series_description", getString(row, "series-description"));
        putOrNull(out, "product_code", getString(row, "product"));
        putOrNull(out, "product", getString(row, "product-name"));
        putOrNull(out, "eia_area_code", getString(row, "duoarea"));
        putOrNull(out, "process_name", getString(row, "process-name"));
        Double price = getDouble(row, "value");
        if (price != null) {
          out.put("price", price);
        } else {
          out.putNull("price");
        }
        putOrNull(out, "units", getString(row, "units"));
        result.add(out);
      }
      return result.toString();
    } catch (Exception e) {
      throw new RuntimeException("EIA Petroleum Spot Prices: failed to parse response for "
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
}
