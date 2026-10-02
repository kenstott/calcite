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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transforms Iowa Environmental Mesonet ASOS CSV responses into flat JSON rows for the
 * {@code asos_observations} table.
 *
 * <p>Response header: {@code station,valid,tmpf,tmpc,metar}. {@code valid} is a UTC
 * {@code YYYY-MM-DD HH:MM} timestamp. {@code tmpf}/{@code tmpc} are empty for the 5-minute
 * (MADIS HFMETAR) feed, so the exact tenths-of-a-degree-Celsius value is also read from the
 * METAR remarks temperature group ({@code T} + sign digit + 3-digit tenths for temperature,
 * then the same for dew point).
 *
 * <p>The request window is the whole UTC year (Jan 1 00:00 to Dec 31 23:59:59), so every row's
 * UTC year equals the {@code year} dimension.
 */
public class AsosObservationTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Remarks temperature group: T, temp sign (0 = +, 1 = -), temp tenths, dew-point group. */
  private static final Pattern T_GROUP = Pattern.compile("\\bT([01])(\\d{3})[01]\\d{3}\\b");

  private static final int CSV_COLUMNS = 5;

  @Override public String transform(String response, RequestContext context) {
    String station = context.getDimensionValues().get("station");
    String reportCode = context.getDimensionValues().get("report_type");
    String reportType = reportTypeName(reportCode);

    ArrayNode result = MAPPER.createArrayNode();
    int pos = 0;
    int length = response.length();
    boolean header = true;
    while (pos < length) {
      int eol = response.indexOf('\n', pos);
      if (eol < 0) {
        eol = length;
      }
      String line = response.substring(pos, eol).trim();
      pos = eol + 1;
      if (line.isEmpty()) {
        continue;
      }
      if (header) {
        header = false;
        if (!line.startsWith("station,")) {
          throw new IllegalStateException("IEM ASOS response for " + station
              + " has unexpected header: " + line);
        }
        continue;
      }
      String[] cols = line.split(",", CSV_COLUMNS);
      if (cols.length < CSV_COLUMNS) {
        throw new IllegalStateException("IEM ASOS row for " + station
            + " has " + cols.length + " columns, expected " + CSV_COLUMNS + ": " + line);
      }
      String valid = cols[1];
      String metar = cols[4];

      ObjectNode row = MAPPER.createObjectNode();
      row.put("station_id", "K" + station);
      row.put("observation_time_utc", valid);
      row.put("observation_date", valid.substring(0, 10));
      row.put("year", Integer.parseInt(valid.substring(0, 4)));
      row.put("report_type", reportType);
      putDoubleOrNull(row, "air_temp_f", cols[2]);
      putDoubleOrNull(row, "air_temp_c", cols[3]);
      Matcher m = T_GROUP.matcher(metar);
      if (m.find()) {
        double tenths = Integer.parseInt(m.group(2)) / 10.0;
        row.put("temp_c_tgroup", "1".equals(m.group(1)) ? -tenths : tenths);
      } else {
        row.putNull("temp_c_tgroup");
      }
      row.put("metar", metar);
      result.add(row);
    }
    return result.toString();
  }

  private static String reportTypeName(String code) {
    if ("1".equals(code)) {
      return "5min";
    }
    if ("3".equals(code)) {
      return "routine";
    }
    if ("4".equals(code)) {
      return "special";
    }
    throw new IllegalArgumentException("Unknown IEM report_type dimension value: " + code);
  }

  private static void putDoubleOrNull(ObjectNode row, String field, String value) {
    if (value.isEmpty()) {
      row.putNull(field);
    } else {
      row.put(field, Double.parseDouble(value));
    }
  }
}
