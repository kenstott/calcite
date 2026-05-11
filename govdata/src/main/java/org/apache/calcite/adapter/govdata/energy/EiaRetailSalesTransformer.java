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

public class EiaRetailSalesTransformer extends EiaV2Transformer implements ResponseTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EIA Retail Sales: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode data = extractDataArray(response);
      ArrayNode result = MAPPER.createArrayNode();

      for (JsonNode row : data) {
        String period = getString(row, "period");
        ObjectNode out = MAPPER.createObjectNode();

        out.put("price_year", parseYear(period));

        Integer month = parseMonth(period);
        if (month != null) {
          out.put("price_month", month);
        } else {
          out.putNull("price_month");
        }

        String stateAbbr = getString(row, "stateid", "location");
        if (stateAbbr != null) {
          out.put("state_abbr", stateAbbr);
        } else {
          out.putNull("state_abbr");
        }

        out.putNull("state_fips");

        String stateDesc = getString(row, "stateDescription", "locationDescription");
        if (stateDesc != null) {
          out.put("state_description", stateDesc);
        } else {
          out.putNull("state_description");
        }

        String sectorCode = getString(row, "sectorid");
        if (sectorCode != null) {
          out.put("sector_code", sectorCode);
        } else {
          out.putNull("sector_code");
        }

        String sectorName = getString(row, "sectorName", "sectorDescription");
        if (sectorName != null) {
          out.put("sector", sectorName);
        } else {
          out.putNull("sector");
        }

        Double price = getDouble(row, "price");
        if (price != null) {
          out.put("avg_price_cents_kwh", price);
        } else {
          out.putNull("avg_price_cents_kwh");
        }

        Double revenue = getDouble(row, "revenue");
        if (revenue != null) {
          out.put("revenue_million_dollars", revenue);
        } else {
          out.putNull("revenue_million_dollars");
        }

        Double sales = getDouble(row, "sales");
        if (sales != null) {
          out.put("sales_million_kwh", sales);
        } else {
          out.putNull("sales_million_kwh");
        }

        Long customers = getLong(row, "customers");
        if (customers != null) {
          out.put("customers", customers);
        } else {
          out.putNull("customers");
        }

        result.add(out);
      }

      LOGGER.debug("EIA Retail Sales: transformed {} records", result.size());
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("EIA Retail Sales: failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }
}
