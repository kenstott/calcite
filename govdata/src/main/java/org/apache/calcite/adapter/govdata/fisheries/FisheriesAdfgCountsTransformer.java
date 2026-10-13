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
package org.apache.calcite.adapter.govdata.fisheries;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Rolls the Alaska Dept. of Fish and Game Sport Fish Division daily fish-count export up to
 * one row per count location x species x year.
 *
 * <p>Input: {@code index.cfm?ADFG=export.JSON&countLocationID=..&speciesID=..&year=..}, an object
 * {@code {"COLUMNS":[YEAR,COUNTDATE,FISHCOUNT,SPECIESID,COUNTLOCATIONID,COUNTLOCATION,SPECIES],
 * "DATA":[[...],...]}} with one row per count day. A location/species pair the Division does
 * not count returns an empty DATA array, which is a valid answer and yields no rows.
 */
public class FisheriesAdfgCountsTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter COUNT_DATE =
      DateTimeFormatter.ofPattern("MMMM, dd yyyy", Locale.US);

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      throw new IllegalStateException("ADF&G fish counts: empty response for "
          + context.getUrl());
    }
    JsonNode root;
    try {
      root = MAPPER.readTree(response);
    } catch (IOException e) {
      throw new IllegalStateException("ADF&G fish counts: not JSON for " + context.getUrl(), e);
    }
    JsonNode columns = root.get("COLUMNS");
    JsonNode data = root.get("DATA");
    if (columns == null || data == null || !data.isArray() || columns.size() != 7
        || !"FISHCOUNT".equals(columns.get(2).asText())) {
      throw new IllegalStateException("ADF&G fish counts: unexpected shape for "
          + context.getUrl());
    }

    Map<String, Agg> byKey = new LinkedHashMap<String, Agg>();
    for (JsonNode r : data) {
      long locationId = r.get(4).asLong();
      long speciesId = r.get(3).asLong();
      int year = r.get(0).asInt();
      String key = locationId + "|" + speciesId + "|" + year;
      Agg a = byKey.get(key);
      if (a == null) {
        a = new Agg(locationId, r.get(5).asText().trim(), speciesId, r.get(6).asText().trim(),
            year);
        byKey.put(key, a);
      }
      String raw = r.get(1).asText();
      LocalDate day = LocalDate.parse(raw.substring(0, raw.lastIndexOf(' ')), COUNT_DATE);
      long count = r.get(2).asLong();
      a.total += count;
      a.days++;
      if (count > a.peak) {
        a.peak = count;
      }
      if (a.first == null || day.isBefore(a.first)) {
        a.first = day;
      }
      if (a.last == null || day.isAfter(a.last)) {
        a.last = day;
      }
    }

    ArrayNode result = MAPPER.createArrayNode();
    for (Agg a : byKey.values()) {
      ObjectNode row = MAPPER.createObjectNode();
      row.put("adfg_location_id", a.locationId);
      row.put("location_name", a.locationName);
      row.put("adfg_species_id", a.speciesId);
      row.put("species_name", a.speciesName);
      row.put("state_abbr", "AK");
      row.put("count_year", a.year);
      row.put("total_count", a.total);
      row.put("days_counted", a.days);
      row.put("peak_daily_count", a.peak);
      row.put("first_count_date", a.first.toString());
      row.put("last_count_date", a.last.toString());
      result.add(row);
    }
    try {
      return MAPPER.writeValueAsString(result);
    } catch (IOException e) {
      throw new IllegalStateException("ADF&G fish counts: cannot serialize", e);
    }
  }

  /** Running totals for one location x species x year. */
  private static final class Agg {
    final long locationId;
    final String locationName;
    final long speciesId;
    final String speciesName;
    final int year;
    long total;
    int days;
    long peak = Long.MIN_VALUE;
    LocalDate first;
    LocalDate last;

    Agg(long locationId, String locationName, long speciesId, String speciesName, int year) {
      this.locationId = locationId;
      this.locationName = locationName;
      this.speciesId = speciesId;
      this.speciesName = speciesName;
      this.year = year;
    }
  }
}
