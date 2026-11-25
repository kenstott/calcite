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
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Transforms College Scorecard API institution responses for
 * {@code college_scorecard_latest}: the current {@code latest.*} values and current
 * directory ({@code school.*}) values, one row per institution, with no year.
 *
 * <p>The API returns flat records keyed by dot-notation field names, and returns every
 * requested key, with JSON null where it has no value. A record that lacks a requested key
 * is an error, because it means the API no longer serves that field under that name.
 * "PrivacySuppressed" string values are mapped to null.
 */
public class CollegeScorecardLatestResponseTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String PRIVACY_SUPPRESSED = "PrivacySuppressed";

  // Scorecard field name to schema column name
  private static final Map<String, String> FIELD_MAP;

  static {
    Map<String, String> m = new LinkedHashMap<String, String>();
    m.put("id",
        "id");
    m.put("ope8_id",
        "ope8_id");
    m.put("ope6_id",
        "ope6_id");
    m.put("school.name",
        "name");
    m.put("school.city",
        "city");
    m.put("school.state",
        "state");
    m.put("school.ownership",
        "ownership");
    m.put("school.carnegie_basic",
        "carnegie_basic");
    m.put("school.minority_serving.historically_black",
        "hbcu");
    m.put("school.minority_serving.tribal",
        "tribal");
    m.put("school.online_only",
        "online_only");
    m.put("school.locale",
        "locale");
    m.put("latest.student.size",
        "size");
    m.put("latest.aid.pell_grant_rate",
        "pell_grant_rate");
    m.put("latest.completion.completion_rate_4yr_150nt",
        "completion_rate_4yr");
    m.put("latest.completion.completion_rate_less_than_4yr_150nt",
        "completion_rate_2yr");
    m.put("latest.student.retention_rate.four_year.full_time",
        "retention_rate_fulltime");
    m.put("latest.aid.median_debt.completers.overall",
        "median_debt_completers");
    m.put("latest.aid.median_debt.noncompleters",
        "median_debt_noncompleters");
    m.put("latest.earnings.10_yrs_after_entry.working_not_enrolled.mean_earnings",
        "mean_earnings_10yr");
    m.put("latest.earnings.8_yrs_after_entry.median_earnings",
        "median_earnings_8yr");
    m.put("latest.student.demographics.median_family_income",
        "median_family_income");
    m.put("latest.repayment.3_yr_default_rate",
        "default_rate_3yr");
    m.put("latest.cost.avg_net_price.public",
        "avg_net_price_public");
    m.put("latest.cost.avg_net_price.private",
        "avg_net_price_private");
    m.put("latest.cost.tuition.in_state",
        "tuition_in_state");
    m.put("latest.cost.tuition.out_of_state",
        "tuition_out_state");
    FIELD_MAP = Collections.unmodifiableMap(m);
  }

  @Override public String transform(String response, RequestContext context) {
    JsonNode root;
    try {
      root = MAPPER.readTree(response);
    } catch (IOException e) {
      throw new IllegalStateException("College Scorecard: failed to parse response for "
          + context.getUrl(), e);
    }

    JsonNode results = root.isArray() ? root : root.path("results");
    if (!results.isArray()) {
      throw new IllegalStateException("College Scorecard: no results array for "
          + context.getUrl());
    }

    ArrayNode out = MAPPER.createArrayNode();
    for (JsonNode record : results) {
      ObjectNode row = MAPPER.createObjectNode();
      for (Map.Entry<String, String> field : FIELD_MAP.entrySet()) {
        JsonNode value = record.get(field.getKey());
        if (value == null) {
          throw new IllegalStateException("College Scorecard: response lacks field "
              + field.getKey());
        }
        if (value.isNull() || PRIVACY_SUPPRESSED.equals(value.asText())) {
          row.putNull(field.getValue());
        } else {
          row.set(field.getValue(), value);
        }
      }
      out.add(row);
    }
    return out.toString();
  }
}
