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
package org.apache.calcite.adapter.govdata.disasters;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Maps the NOAA NCEI Billion-Dollar Weather and Climate Disasters event file
 * ({@code events-US-1980-2024.json}: {@code {"description":{...},"data":[...]}}) into
 * {@code billion_dollar_events} rows. Dates arrive as yyyymmdd integers; costs are millions of
 * dollars, {@code adjCost} CPI-adjusted to 2024 dollars and {@code unadjCost} in event-year dollars.
 */
public class NceiBillionDollarEventsTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Dollar year of the file's CPI-adjusted cost (the file covers 1980-2024). */
  static final int ADJUSTED_DOLLAR_YEAR = 2024;

  @Override public String transform(String response, RequestContext context) {
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode data = root.isArray() ? root
          : root.has("results") ? root.path("results") : root.path("data");
      if (!data.isArray()) {
        throw new RuntimeException("NCEI billion-dollar events: no 'data' array in response: "
            + response.substring(0, Math.min(200, response.length())));
      }
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode rec : data) {
        ObjectNode row = MAPPER.createObjectNode();
        String begin = date(rec, "begDate");
        row.put("event_name", rec.path("name").asText());
        row.put("disaster_type", rec.path("disaster").asText());
        row.put("begin_date", begin);
        row.put("end_date", date(rec, "endDate"));
        row.put("begin_year", Integer.parseInt(begin.substring(0, 4)));
        row.put("cost_adjusted_musd", rec.path("adjCost").asDouble());
        row.put("cost_dollar_year", ADJUSTED_DOLLAR_YEAR);
        row.put("cost_unadjusted_musd", rec.path("unadjCost").asDouble());
        row.put("deaths", rec.path("deaths").asInt());
        out.add(row);
      }
      return MAPPER.writeValueAsString(out);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("NCEI billion-dollar events transform failed: " + e.getMessage(), e);
    }
  }

  /** yyyymmdd integer to yyyy-mm-dd. */
  private static String date(JsonNode rec, String field) {
    String s = rec.path(field).asText();
    if (s.length() != 8) {
      throw new RuntimeException("NCEI billion-dollar events: bad " + field + " '" + s + "'");
    }
    return s.substring(0, 4) + "-" + s.substring(4, 6) + "-" + s.substring(6, 8);
  }
}
