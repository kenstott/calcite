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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Transforms NOAA CDO NORMAL_MLY API responses into monthly climate normals.
 *
 * <p>Input format (same envelope as {@link CdoDataTransformer}):
 * <pre>{@code
 * {
 *   "results": [
 *     {
 *       "date": "2010-01-01T00:00:00",
 *       "datatype": "MLY-TMAX-NORMAL",
 *       "station": "GHCND:USW00094846",
 *       "attributes": "C",
 *       "value": 3.8
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <p>CDO returns one row per datatype per station per month. This transformer
 * pivots those rows into one output record per (station_id, month) with each
 * datatype mapped to its planned column.
 *
 * <p>The query below deliberately omits {@code units=metric}: NOAA applies the
 * absolute-temperature (F&deg;-32)&times;5/9 conversion formula even to
 * *-STDDEV datatypes (a delta, not an absolute value) when metric units are
 * requested, which corrupts standard deviations into negative numbers &mdash;
 * confirmed live. Fetching everything in CDO's default "standard" units and
 * converting each field here, with the correct formula per field, avoids that
 * bug. Standard-unit raw scales (confirmed live against the CDO API, cross-
 * checked against its own {@code units=metric} output where that is safe to
 * trust): temperature normals are tenths of &deg;F; precipitation normal is
 * hundredths of an inch; snowfall normal is tenths of an inch; stddev fields
 * are tenths of a &deg;F <em>delta</em> (no -32 offset applies).
 *
 * <p>{@code county_fips} is set to null; it requires a post-ETL join against
 * {@code ghcnd_stations_with_county}.
 */
public class ClimateNormalsTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClimateNormalsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Climate Normals: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      if (!root.isObject()) {
        LOGGER.warn("Climate Normals: Unexpected response type {} for {}",
            root.getNodeType(), context.getUrl());
        return "[]";
      }

      JsonNode results = root.get("results");
      if (results == null || !results.isArray()) {
        LOGGER.debug("Climate Normals: No results in response for {}", context.getUrl());
        return "[]";
      }

      String stateFips = context.getDimensionValues().get("state_fips");

      // Group by "station_id|month" preserving insertion order for deterministic output.
      Map<String, StationMonthRecord> grouped = new LinkedHashMap<String, StationMonthRecord>();

      for (JsonNode item : results) {
        String rawStation = getTextOrNull(item, "station");
        if (rawStation == null) {
          continue;
        }
        // Strip the "GHCND:" prefix that CDO prepends to station identifiers.
        String stationId = rawStation.startsWith("GHCND:")
            ? rawStation.substring(6)
            : rawStation;

        String date = getTextOrNull(item, "date");
        if (date == null || date.length() < 7) {
          continue;
        }
        int month;
        try {
          month = Integer.parseInt(date.substring(5, 7));
        } catch (NumberFormatException e) {
          continue;
        }

        String datatype = getTextOrNull(item, "datatype");
        if (datatype == null) {
          continue;
        }

        JsonNode valueNode = item.get("value");
        if (valueNode == null || valueNode.isNull() || !valueNode.isNumber()) {
          continue;
        }
        double rawValue = valueNode.asDouble();

        String key = stationId + "|" + month;
        StationMonthRecord rec = grouped.get(key);
        if (rec == null) {
          rec = new StationMonthRecord(stationId, month);
          grouped.put(key, rec);
        }

        switch (datatype) {
        case "MLY-TMAX-NORMAL":
          rec.normalTmaxC = tenthsFahrenheitToCelsius(rawValue);
          break;
        case "MLY-TMIN-NORMAL":
          rec.normalTminC = tenthsFahrenheitToCelsius(rawValue);
          break;
        case "MLY-TAVG-NORMAL":
          rec.normalTavgC = tenthsFahrenheitToCelsius(rawValue);
          break;
        case "MLY-PRCP-NORMAL":
          // Hundredths of an inch -> mm. -7777 is CDO's "trace" flag, not a measurement:
          // a non-zero amount too small to round up to the reporting resolution. Converting it
          // arithmetically yields -1975.4 mm, a negative precipitation normal. Trace is
          // conventionally carried as zero accumulation.
          rec.normalPrcpMm = isTrace(rawValue) ? 0.0 : (rawValue / 100.0) * MM_PER_INCH;
          break;
        case "MLY-SNOW-NORMAL":
          // Tenths of an inch -> mm; same trace flag as precipitation.
          rec.normalSnowMm = isTrace(rawValue) ? 0.0 : (rawValue / 10.0) * MM_PER_INCH;
          break;
        case "MLY-TMAX-STDDEV":
          rec.tmaxStddev = tenthsFahrenheitDeltaToCelsius(rawValue);
          break;
        case "MLY-TMIN-STDDEV":
          rec.tminStddev = tenthsFahrenheitDeltaToCelsius(rawValue);
          break;
        default:
          break;
        }
      }

      ArrayNode output = MAPPER.createArrayNode();
      for (StationMonthRecord rec : grouped.values()) {
        ObjectNode row = MAPPER.createObjectNode();
        row.putNull("county_fips");
        row.put("station_id", rec.stationId);
        row.put("month", rec.month);
        if (stateFips != null) {
          row.put("state_fips", stateFips);
        } else {
          row.putNull("state_fips");
        }
        putNullableDouble(row, "normal_tmax_c", rec.normalTmaxC);
        putNullableDouble(row, "normal_tmin_c", rec.normalTminC);
        putNullableDouble(row, "normal_tavg_c", rec.normalTavgC);
        putNullableDouble(row, "normal_prcp_mm", rec.normalPrcpMm);
        putNullableDouble(row, "normal_snow_mm", rec.normalSnowMm);
        putNullableDouble(row, "tmax_stddev", rec.tmaxStddev);
        putNullableDouble(row, "tmin_stddev", rec.tminStddev);
        row.putNull("prcp_stddev");
        output.add(row);
      }

      LOGGER.debug("Climate Normals: Transformed {} station-month records for state_fips={}",
          output.size(), stateFips);
      return output.toString();

    } catch (Exception e) {
      throw new RuntimeException("Climate Normals: Failed to parse response for "
          + context.getUrl(), e);
    }
  }

  private static final double MM_PER_INCH = 25.4;

  /** Converts a raw value in tenths of &deg;F to an absolute temperature in &deg;C. */
  /**
   * True for CDO's {@code -7777} trace flag, which occupies the value field of a precipitation or
   * snowfall normal in place of a measurement. Compared with a tolerance because the field arrives
   * as a JSON number.
   */
  private static boolean isTrace(double rawValue) {
    return Math.abs(rawValue - (-7777.0)) < 0.5;
  }

  private static double tenthsFahrenheitToCelsius(double tenthsF) {
    return ((tenthsF / 10.0) - 32.0) * 5.0 / 9.0;
  }

  /** Converts a raw value in tenths of a &deg;F delta (e.g. a stddev) to a &deg;C delta -- no -32 offset. */
  private static double tenthsFahrenheitDeltaToCelsius(double tenthsF) {
    return (tenthsF / 10.0) * 5.0 / 9.0;
  }

  private static void putNullableDouble(ObjectNode row, String field, Double value) {
    if (value != null) {
      row.put(field, value);
    } else {
      row.putNull(field);
    }
  }

  private static String getTextOrNull(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    return v.asText();
  }

  private static final class StationMonthRecord {
    final String stationId;
    final int month;

    Double normalTmaxC;
    Double normalTminC;
    Double normalTavgC;
    Double normalPrcpMm;
    Double normalSnowMm;
    Double tmaxStddev;
    Double tminStddev;

    StationMonthRecord(String stationId, int month) {
      this.stationId = stationId;
      this.month = month;
    }
  }
}
