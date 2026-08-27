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
 * Transforms EIA API v2 {@code natural-gas/pri/fut/data} rows (daily Henry Hub spot and
 * futures pricing) into tall JSON rows. One row per (series, report_date); the whole
 * ~7,500-row daily history since 1997 is fetched as a single unpartitioned unit via the
 * source's own OFFSET pagination, the same shape as {@link EiaPetroleumStocksTransformer}
 * minus the year/week derivation (no year partition on this table).
 */
public class EiaNaturalGasPriceTransformer extends EiaV2Transformer implements ResponseTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EIA Natural Gas Price: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode data = extractDataArray(response);
      ArrayNode result = MAPPER.createArrayNode();

      for (JsonNode row : data) {
        ObjectNode out = MAPPER.createObjectNode();

        String period = getString(row, "period");
        if (period != null) {
          out.put("report_date", period);
        } else {
          out.putNull("report_date");
        }

        String series = getString(row, "series");
        if (series != null) {
          out.put("series_id", series);
        } else {
          out.putNull("series_id");
        }

        String seriesDesc = getString(row, "series-description");
        if (seriesDesc != null) {
          out.put("series_description", seriesDesc);
        } else {
          out.putNull("series_description");
        }

        String product = getString(row, "product-name");
        if (product != null) {
          out.put("product", product);
        } else {
          out.putNull("product");
        }

        String processName = getString(row, "process-name");
        if (processName != null) {
          out.put("process_name", processName);
        } else {
          out.putNull("process_name");
        }

        Double value = getDouble(row, "value");
        if (value != null) {
          out.put("value_dollars_per_mmbtu", value);
        } else {
          out.putNull("value_dollars_per_mmbtu");
        }

        String units = getString(row, "units");
        if (units != null) {
          out.put("units", units);
        } else {
          out.putNull("units");
        }

        result.add(out);
      }

      LOGGER.debug("EIA Natural Gas Price: transformed {} records", result.size());
      return result.toString();

    } catch (Exception e) {
      throw new RuntimeException("EIA Natural Gas Price: failed to parse response for "
          + context.getUrl(), e);
    }
  }
}
