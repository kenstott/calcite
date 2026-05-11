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

public class EiaStorageTransformer extends EiaV2Transformer implements ResponseTransformer {

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("EIA Storage: empty response for {}", context.getUrl());
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

        out.put("storage_year", parseYear(period));

        Integer week = computeIsoWeek(period);
        if (week != null) {
          out.put("storage_week", week);
        } else {
          out.putNull("storage_week");
        }

        String regionCode = getString(row, "duoarea", "area");
        if (regionCode != null) {
          out.put("eia_region_code", regionCode);
        } else {
          out.putNull("eia_region_code");
        }

        String region = getString(row, "area-name", "areaDescription");
        if (region != null) {
          out.put("region", region);
        } else {
          out.putNull("region");
        }

        String storageTypeCode = getString(row, "process");
        if (storageTypeCode != null) {
          out.put("storage_type_code", storageTypeCode);
        } else {
          out.putNull("storage_type_code");
        }

        String storageType = getString(row, "process-name");
        if (storageType != null) {
          out.put("storage_type", storageType);
        } else {
          out.putNull("storage_type");
        }

        Double volume = getDouble(row, "value");
        if (volume != null) {
          out.put("volume_bcf", volume);
        } else {
          out.putNull("volume_bcf");
        }

        String units = getString(row, "units");
        if (units != null) {
          out.put("units", units);
        } else {
          out.putNull("units");
        }

        String series = getString(row, "series");
        if (series != null) {
          out.put("series_id", series);
        } else {
          out.putNull("series_id");
        }

        result.add(out);
      }

      LOGGER.debug("EIA Storage: transformed {} records", result.size());
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("EIA Storage: failed to parse response for {}: {}",
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
      int month = Integer.parseInt(parts[1]) - 1; // Calendar months are 0-based
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
