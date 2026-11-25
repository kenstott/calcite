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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link CollegeScorecardLatestResponseTransformer}. */
@Tag("unit")
class CollegeScorecardLatestTransformerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String[] FIELDS = {
      "id",
      "ope8_id",
      "ope6_id",
      "school.name",
      "school.city",
      "school.state",
      "school.ownership",
      "school.carnegie_basic",
      "school.minority_serving.historically_black",
      "school.minority_serving.tribal",
      "school.online_only",
      "school.locale",
      "latest.student.size",
      "latest.aid.pell_grant_rate",
      "latest.completion.completion_rate_4yr_150nt",
      "latest.completion.completion_rate_less_than_4yr_150nt",
      "latest.student.retention_rate.four_year.full_time",
      "latest.aid.median_debt.completers.overall",
      "latest.aid.median_debt.noncompleters",
      "latest.earnings.10_yrs_after_entry.working_not_enrolled.mean_earnings",
      "latest.earnings.8_yrs_after_entry.median_earnings",
      "latest.student.demographics.median_family_income",
      "latest.repayment.3_yr_default_rate",
      "latest.cost.avg_net_price.public",
      "latest.cost.avg_net_price.private",
      "latest.cost.tuition.in_state",
      "latest.cost.tuition.out_of_state"
  };

  private static ObjectNode record() {
    ObjectNode record = MAPPER.createObjectNode();
    for (String field : FIELDS) {
      record.putNull(field);
    }
    record.put("id", 100663);
    return record;
  }

  private static String page(ObjectNode record) {
    ObjectNode root = MAPPER.createObjectNode();
    ArrayNode results = root.putArray("results");
    results.add(record);
    return root.toString();
  }

  private static String transform(String response) {
    Map<String, String> dims = new HashMap<String, String>();
    dims.put("type", "college_scorecard_latest");
    return new CollegeScorecardLatestResponseTransformer().transform(response,
        RequestContext.builder().url("http://test").dimensionValues(dims).build());
  }

  @Test void mapsLatestFieldsAndAddsNoYear() throws Exception {
    ObjectNode record = record();
    record.put("latest.earnings.10_yrs_after_entry.working_not_enrolled.mean_earnings", 48400);
    record.put("latest.student.demographics.median_family_income", 34489);
    record.put("latest.student.size", "PrivacySuppressed");
    JsonNode rows = MAPPER.readTree(transform(page(record)));
    assertEquals(1, rows.size());
    JsonNode row = rows.get(0);
    assertEquals(100663, row.get("id").asInt());
    assertEquals(48400, row.get("mean_earnings_10yr").asInt());
    assertEquals(34489, row.get("median_family_income").asInt());
    assertTrue(row.get("size").isNull());
    assertFalse(row.has("year"));
    assertFalse(row.has("hispanic_serving"));
    assertEquals(FIELDS.length, row.size());
  }

  @Test void failsWhenARequestedFieldIsMissing() {
    ObjectNode record = record();
    record.remove("latest.student.size");
    assertThrows(IllegalStateException.class, () -> transform(page(record)));
  }

  @Test void failsWithoutAResultsArray() {
    assertThrows(IllegalStateException.class,
        () -> transform("{\"error\":{\"code\":\"OVER_RATE_LIMIT\"}}"));
  }
}
