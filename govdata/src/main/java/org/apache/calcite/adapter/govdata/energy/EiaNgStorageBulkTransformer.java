/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.energy;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Streams the EIA bulk {@code NG.zip} and materializes weekly natural-gas underground-storage
 * rows — the bulk drop-in for the per-period {@code eia_natural_gas_storage} EIA API v2 fan-out.
 *
 * <p>Storage series ({@code NG.NW2_EPG0_S..._R##_BCF.W}) carry the full weekly history in a
 * {@code data:[[YYYYMMDD,value],...]} array, exploded here into one row per region × report_date.
 */
public class EiaNgStorageBulkTransformer extends EiaBulkSeriesTransformer {

  /** Weekly storage-volume series by region, e.g. NG.NW2_EPG0_SWO_R48_BCF.W. */
  private static final Pattern STORAGE_SERIES =
      Pattern.compile("^NG\\.NW2_EPG0_S[A-Z]+_R\\d+_BCF\\.W$");

  private static final DateTimeFormatter YMD = DateTimeFormatter.ofPattern("yyyyMMdd");

  @Override protected List<Map<String, Object>> rowsForSeries(JsonNode series) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    JsonNode sidNode = series.get("series_id");
    if (sidNode == null || !STORAGE_SERIES.matcher(sidNode.asText()).matches()) {
      return rows;
    }
    String seriesId = sidNode.asText();
    String[] seg = seriesIdBody(seriesId); // [NW2, EPG0, SWO, R48, BCF]
    String storageTypeCode = seg.length > 2 ? seg[2] : null;
    String regionCode = seg.length > 3 ? seg[3] : null;
    String units = series.path("unitsshort").asText("Bcf");
    String region = regionFromName(series.path("name").asText(null));

    JsonNode data = series.get("data");
    if (data == null || !data.isArray()) {
      return rows;
    }
    for (JsonNode point : data) {
      if (!point.isArray() || point.size() < 2 || point.get(1).isNull()) {
        continue;
      }
      LocalDate date = LocalDate.parse(point.get(0).asText(), YMD);
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      // 'year' is the partition column (single cumulative fetch → partition by the data year).
      row.put("year", date.getYear());
      row.put("series_id", seriesId);
      row.put("report_date", date.toString());
      row.put("storage_year", date.getYear());
      row.put("storage_week", date.get(WeekFields.ISO.weekOfWeekBasedYear()));
      row.put("eia_region_code", regionCode);
      row.put("region", region);
      row.put("storage_type_code", storageTypeCode);
      row.put("storage_type", "SWO".equals(storageTypeCode) ? "Working Gas" : storageTypeCode);
      row.put("volume_bcf", point.get(1).asDouble());
      row.put("units", units);
      rows.add(row);
    }
    return rows;
  }

  /** Extracts the region label from the series name, e.g. "Weekly East Region ..." -> "East". */
  private static String regionFromName(String name) {
    if (name == null) {
      return null;
    }
    int idx = name.indexOf(" Region");
    if (idx < 0) {
      return name.contains("Lower 48") ? "Lower 48 States" : null;
    }
    String head = name.substring(0, idx);
    int sp = head.lastIndexOf(' ');
    return sp >= 0 ? head.substring(sp + 1) : head;
  }

  /** Package-visible for unit tests (delegates to the instance hook). */
  static List<Map<String, Object>> rowsForSeriesStatic(JsonNode series) {
    return new EiaNgStorageBulkTransformer().rowsForSeries(series);
  }
}
