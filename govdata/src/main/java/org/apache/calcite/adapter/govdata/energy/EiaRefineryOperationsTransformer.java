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

public class EiaRefineryOperationsTransformer extends EiaV2Transformer
    implements ResponseTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EIA Refinery Operations: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode data = extractDataArray(response);
      ArrayNode result = MAPPER.createArrayNode();

      for (JsonNode row : data) {
        String period = getString(row, "period");
        ObjectNode out = MAPPER.createObjectNode();

        out.put("report_year", parseYear(period));

        Integer month = parseMonth(period);
        if (month != null) {
          out.put("report_month", month);
        } else {
          out.putNull("report_month");
        }

        String areaCode = getString(row, "duoarea", "area");
        if (areaCode != null) {
          out.put("eia_area_code", areaCode);
        } else {
          out.putNull("eia_area_code");
        }

        String padd = getString(row, "area-name");
        if (padd != null) {
          out.put("padd", padd);
        } else {
          out.putNull("padd");
        }

        String series = getString(row, "series");
        if (series != null) {
          out.put("series_id", series);
        } else {
          out.putNull("series_id");
        }

        String processCode = getString(row, "process");
        if (processCode != null) {
          out.put("process_code", processCode);
        } else {
          out.putNull("process_code");
        }

        String metricName = getString(row, "series-description");
        if (metricName != null) {
          out.put("metric_name", metricName);
        } else {
          out.putNull("metric_name");
        }

        String units = getString(row, "units");
        if (units != null) {
          out.put("units", units);
        } else {
          out.putNull("units");
        }

        Double value = getDouble(row, "value");
        if (value != null) {
          out.put("value", value);
        } else {
          out.putNull("value");
        }

        result.add(out);
      }

      LOGGER.debug("EIA Refinery Operations: transformed {} records", result.size());
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("EIA Refinery Operations: failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }
}
