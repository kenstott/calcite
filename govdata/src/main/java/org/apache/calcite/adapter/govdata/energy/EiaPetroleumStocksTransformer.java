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

import java.util.Calendar;

public class EiaPetroleumStocksTransformer extends EiaV2Transformer implements ResponseTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EIA Petroleum Stocks: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode data = extractDataArray(response);
      ArrayNode result = MAPPER.createArrayNode();

      for (JsonNode row : data) {
        String period = getString(row, "period");
        ObjectNode out = MAPPER.createObjectNode();

        if (period != null) {
          out.put("report_date", period);
        } else {
          out.putNull("report_date");
        }

        out.put("stock_year", parseYear(period));

        Integer week = computeIsoWeek(period);
        if (week != null) {
          out.put("stock_week", week);
        } else {
          out.putNull("stock_week");
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

        String productCode = getString(row, "product");
        if (productCode != null) {
          out.put("product_code", productCode);
        } else {
          out.putNull("product_code");
        }

        String product = getString(row, "product-name");
        if (product != null) {
          out.put("product", product);
        } else {
          out.putNull("product");
        }

        String processCode = getString(row, "process");
        if (processCode != null) {
          out.put("process_code", processCode);
        } else {
          out.putNull("process_code");
        }

        String processName = getString(row, "process-name");
        if (processName != null) {
          out.put("process_name", processName);
        } else {
          out.putNull("process_name");
        }

        Double stocks = getDouble(row, "value");
        if (stocks != null) {
          out.put("stocks_kbbl", stocks);
        } else {
          out.putNull("stocks_kbbl");
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

        result.add(out);
      }

      LOGGER.debug("EIA Petroleum Stocks: transformed {} records", result.size());
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("EIA Petroleum Stocks: failed to parse response for {}: {}",
          context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private Integer computeIsoWeek(String period) {
    if (period == null || !period.matches("\\d{4}-\\d{2}-\\d{2}")) {
      return null;
    }
    try {
      String[] parts = period.split("-");
      int year = Integer.parseInt(parts[0]);
      int month = Integer.parseInt(parts[1]) - 1;
      int day = Integer.parseInt(parts[2]);

      Calendar cal = Calendar.getInstance();
      cal.setMinimalDaysInFirstWeek(4);
      cal.setFirstDayOfWeek(Calendar.MONDAY);
      cal.set(year, month, day);
      return cal.get(Calendar.WEEK_OF_YEAR);
    } catch (Exception e) {
      return null;
    }
  }
}
