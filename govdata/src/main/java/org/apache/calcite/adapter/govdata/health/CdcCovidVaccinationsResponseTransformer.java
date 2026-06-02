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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.PerRecordResponseTransformer;
import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;

/**
 * Transforms CDC COVID-19 vaccination data (Socrata API).
 *
 * <p>Live response is a top-level JSON array. Implements {@link PerRecordResponseTransformer}
 * so HttpSource's streamFromRawCache path handles the paginated {@code {"results":[...]}}
 * cache envelope correctly.
 */
public class CdcCovidVaccinationsResponseTransformer implements PerRecordResponseTransformer {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode out = MAPPER.createArrayNode();

      if (root.isArray()) {
        for (JsonNode record : root) {
          ObjectNode row = MAPPER.createObjectNode();
          mapRecord(record, row);
          out.add(row);
        }
      }

      return out.toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to transform CDC COVID vaccinations response", e);
    }
  }

  @Override
  public void transformRecord(Map<String, Object> row, RequestContext context) {
    Map<String, Object> source = new HashMap<>(row);
    row.clear();
    row.put("date", str(source.get("date")));
    row.put("demographic_category", str(source.get("demographic_category")));
    row.put("dose1_administered", str(source.get("administered_dose1")));
    row.put("dose1_pct_us", str(source.get("administered_dose1_pct_us")));
    row.put("series_complete_count", str(source.get("series_complete_yes")));
    row.put("series_complete_pct", str(source.get("series_complete_pop_pct")));
    row.put("booster_count", str(source.get("booster_doses_yes")));
    row.put("booster_pct", str(source.get("booster_doses_vax_pct_agegroup")));
    row.put("bivalent_booster_count", str(source.get("bivalent_booster")));
    row.put("bivalent_booster_pct", str(source.get("bivalent_booster_pop_pct_agegroup")));
    row.put("type", "cdc_covid_vaccinations");
  }

  private static void mapRecord(JsonNode record, ObjectNode row) {
    put(row, "date", text(record, "date"));
    put(row, "demographic_category", text(record, "demographic_category"));
    put(row, "dose1_administered", text(record, "administered_dose1"));
    put(row, "dose1_pct_us", text(record, "administered_dose1_pct_us"));
    put(row, "series_complete_count", text(record, "series_complete_yes"));
    put(row, "series_complete_pct", text(record, "series_complete_pop_pct"));
    put(row, "booster_count", text(record, "booster_doses_yes"));
    put(row, "booster_pct", text(record, "booster_doses_vax_pct_agegroup"));
    put(row, "bivalent_booster_count", text(record, "bivalent_booster"));
    put(row, "bivalent_booster_pct", text(record, "bivalent_booster_pop_pct_agegroup"));
    put(row, "type", "cdc_covid_vaccinations");
  }

  private static void put(ObjectNode row, String key, Object value) {
    if (value == null) {
      row.putNull(key);
    } else {
      row.put(key, String.valueOf(value));
    }
  }

  private static String text(JsonNode node, String field) {
    JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? null : value.asText(null);
  }

  private static String str(Object value) {
    return value == null ? null : String.valueOf(value);
  }
}
