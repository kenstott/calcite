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
import org.apache.calcite.adapter.file.etl.RetryableHttp;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Fetches NOAA CDO NORMAL_MLY monthly climate normals for one state and pivots them into one
 * record per (station_id, month).
 *
 * <p>CDO returns one result per datatype per station per month:
 * <pre>{@code
 * {
 *   "metadata": {"resultset": {"offset": 1, "count": 13068, "limit": 1000}},
 *   "results": [
 *     {
 *       "date": "2010-01-01T00:00:00",
 *       "datatype": "MLY-TMAX-NORMAL",
 *       "station": "GHCND:USW00094846",
 *       "attributes": "C",
 *       "value": 38
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <p>A station-month's datatypes can land on either side of a page boundary, so the pivot runs
 * over every page of the state's response rather than per page; this transformer therefore does
 * its own paging instead of relying on {@code HttpSource} pagination, which transforms each page
 * separately. The state's whole normals set (a few thousand station-months) is the pivot's
 * working set.
 *
 * <p>The query deliberately omits {@code units=metric}: NOAA applies the
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
public class ClimateNormalsTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClimateNormalsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** CDO's maximum page size. */
  private static final int PAGE_LIMIT = 1000;

  /** CDO result offsets are 1-based. */
  private static final int FIRST_OFFSET = 1;

  private static final double MM_PER_INCH = 25.4;

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String stateFips = context.getDimensionValues().get("state_fips");
    if (stateFips == null) {
      throw new IllegalStateException("Climate Normals: no state_fips dimension in context for "
          + context.getUrl());
    }
    Map<String, String> headers = new LinkedHashMap<String, String>(context.getHeaders());
    headers.put("Accept", "application/json");

    Map<String, StationMonthRecord> grouped = new LinkedHashMap<String, StationMonthRecord>();
    int offset = FIRST_OFFSET;
    int total = 0;
    do {
      String pageUrl = context.getUrl() + "&limit=" + PAGE_LIMIT + "&offset=" + offset;
      JsonNode root = fetchPage(pageUrl, headers, context);
      JsonNode results = root.get("results");
      if (results == null) {
        if (offset == FIRST_OFFSET) {
          LOGGER.debug("Climate Normals: no results for state_fips={}", stateFips);
          return new ArrayList<Map<String, Object>>().iterator();
        }
        throw new IOException("Climate Normals: page at offset " + offset
            + " has no results, but the response reported " + total + " for " + pageUrl);
      }
      if (!results.isArray()) {
        throw new IOException("Climate Normals: unexpected results type " + results.getNodeType()
            + " for " + pageUrl);
      }
      JsonNode count = root.path("metadata").path("resultset").path("count");
      if (!count.isNumber()) {
        throw new IOException("Climate Normals: response carries no metadata.resultset.count for "
            + pageUrl);
      }
      total = count.intValue();
      accumulate(results, grouped);
      offset += PAGE_LIMIT;
    } while (offset <= total);

    List<Map<String, Object>> rows = toRows(grouped, stateFips);
    LOGGER.debug("Climate Normals: {} station-month records from {} results for state_fips={}",
        rows.size(), total, stateFips);
    return rows.iterator();
  }

  private static JsonNode fetchPage(String pageUrl, Map<String, String> headers,
      RequestContext context) throws IOException {
    HttpURLConnection conn =
        RetryableHttp.openWithRetry(pageUrl, headers, context.getRateLimit(), false);
    try (InputStream in = conn.getInputStream()) {
      JsonNode root = MAPPER.readTree(in);
      if (root == null || !root.isObject()) {
        throw new IOException("Climate Normals: unexpected response type for " + pageUrl);
      }
      return root;
    } finally {
      conn.disconnect();
    }
  }

  /** Folds one page of CDO results into the per-(station, month) records. */
  private static void accumulate(JsonNode results, Map<String, StationMonthRecord> grouped) {
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
  }

  private static List<Map<String, Object>> toRows(Map<String, StationMonthRecord> grouped,
      String stateFips) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>(grouped.size());
    for (StationMonthRecord rec : grouped.values()) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("county_fips", null);
      row.put("station_id", rec.stationId);
      row.put("month", Integer.valueOf(rec.month));
      row.put("state_fips", stateFips);
      row.put("normal_tmax_c", rec.normalTmaxC);
      row.put("normal_tmin_c", rec.normalTminC);
      row.put("normal_tavg_c", rec.normalTavgC);
      row.put("normal_prcp_mm", rec.normalPrcpMm);
      row.put("normal_snow_mm", rec.normalSnowMm);
      row.put("tmax_stddev", rec.tmaxStddev);
      row.put("tmin_stddev", rec.tminStddev);
      row.put("prcp_stddev", null);
      rows.add(row);
    }
    return rows;
  }

  /**
   * True for CDO's {@code -7777} trace flag, which occupies the value field of a precipitation or
   * snowfall normal in place of a measurement. Compared with a tolerance because the field arrives
   * as a JSON number.
   */
  private static boolean isTrace(double rawValue) {
    return Math.abs(rawValue - (-7777.0)) < 0.5;
  }

  /** Converts a raw value in tenths of &deg;F to an absolute temperature in &deg;C. */
  private static double tenthsFahrenheitToCelsius(double tenthsF) {
    return ((tenthsF / 10.0) - 32.0) * 5.0 / 9.0;
  }

  /** Converts a raw value in tenths of a &deg;F delta (e.g. a stddev) to a &deg;C delta -- no -32 offset. */
  private static double tenthsFahrenheitDeltaToCelsius(double tenthsF) {
    return (tenthsF / 10.0) * 5.0 / 9.0;
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
