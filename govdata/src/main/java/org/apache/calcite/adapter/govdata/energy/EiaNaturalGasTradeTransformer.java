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
 * Transforms EIA API v2 {@code natural-gas/move/expc} (exports) and
 * {@code natural-gas/move/impc} (imports) monthly rows into one tall table. The route
 * comes from the {@code trade_route} dimension ({@code expc} or {@code impc}); the
 * {@code process} code splits each row into the trade type (LNG, pipeline, CNG, vessel,
 * truck) and whether the value is a volume (MMcf) or a price ($/Mcf).
 */
public class EiaNaturalGasTradeTransformer extends EiaV2Transformer
    implements ResponseTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    String route = context.getDimensionValues().get("trade_route");
    if (!"expc".equals(route) && !"impc".equals(route)) {
      throw new IllegalStateException("EIA Natural Gas Trade: trade_route dimension must be "
          + "expc or impc, got " + route + " for " + context.getUrl());
    }
    String direction = "expc".equals(route) ? "export" : "import";

    try {
      JsonNode data = extractDataArray(response);
      ArrayNode result = MAPPER.createArrayNode();

      for (JsonNode row : data) {
        String period = getString(row, "period");
        String process = getString(row, "process");
        String units = getString(row, "units");
        Double value = getDouble(row, "value");

        ObjectNode out = MAPPER.createObjectNode();
        out.put("trade_route", route);
        out.put("direction", direction);
        putText(out, "period", period);
        out.put("year", parseYear(period));
        Integer month = parseMonth(period);
        if (month != null) {
          out.put("month", month);
        } else {
          out.putNull("month");
        }
        putText(out, "series_id", getString(row, "series"));
        putText(out, "series_description", getString(row, "series-description"));
        putText(out, "partner_code", getString(row, "duoarea"));
        putText(out, "partner_country", getString(row, "area-name"));
        putText(out, "process_code", process);
        putText(out, "process_name", getString(row, "process-name"));
        putText(out, "trade_type", tradeType(process));
        boolean price = units != null && units.startsWith("$");
        out.put("measure", price ? "price" : "volume");
        if (value != null && !price) {
          out.put("volume_mmcf", value);
        } else {
          out.putNull("volume_mmcf");
        }
        if (value != null && price) {
          out.put("price_dollars_per_mcf", value);
        } else {
          out.putNull("price_dollars_per_mcf");
        }
        putText(out, "units", units);
        result.add(out);
      }

      LOGGER.debug("EIA Natural Gas Trade ({}): transformed {} records", route, result.size());
      return result.toString();

    } catch (Exception e) {
      throw new RuntimeException("EIA Natural Gas Trade: failed to parse response for "
          + context.getUrl(), e);
    }
  }

  private static void putText(ObjectNode out, String key, String value) {
    if (value != null) {
      out.put(key, value);
    } else {
      out.putNull(key);
    }
  }

  /** Maps an EIA process code to a trade type; null when the code is not a known one. */
  static String tradeType(String process) {
    if (process == null) {
      return null;
    }
    switch (process) {
    case "ENG": case "PNG": case "IML": case "PML":
      return "LNG";
    case "ENP": case "PNP": case "IRP": case "PRP":
      return "pipeline";
    case "ENC": case "PRC": case "INC": case "PIC":
      return "CNG";
    case "EVE":
      return "vessel";
    case "ETR":
      return "truck";
    case "EVT":
      return "vessel_and_truck";
    case "ERE":
      return "re-export";
    case "EEX": case "PEX": case "IM0": case "PM0":
      return "total";
    default:
      return null;
    }
  }
}
