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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Transforms College Scorecard API (api.data.gov/ed/collegescorecard) institution responses.
 *
 * <p>The Scorecard API returns records with dot-notation field names as flat JSON keys:
 * {@code {"id": 100654, "2022.school.name": "Alabama A&M", "2022.student.size": 5767, ...}}.
 * A field prefixed with a year holds the value in that year's data file; the request is built
 * for the {@code year} dimension, and the same year is injected into each record. Fields
 * without a year prefix are current directory values. An institution with no
 * {@code <year>.school.name} is not in that year's data file and yields no row.
 * "PrivacySuppressed" string values are mapped to null.
 *
 * <p>Note: The correct API host is {@code api.data.gov/ed/collegescorecard/v1/schools.json};
 * {@code api.collegescorecard.ed.gov} is NXDOMAIN.
 */
public class CollegeScorecardResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CollegeScorecardResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String PRIVACY_SUPPRESSED = "PrivacySuppressed";

  // Maps Scorecard dot-notation field names to schema column names; {year} is the data year
  private static final Map<String, String> FIELD_MAP;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("id",                                                                 "id");
    m.put("ope8_id",                                                            "ope8_id");
    m.put("ope6_id",                                                            "ope6_id");
    m.put("{year}.school.name",                                                        "name");
    m.put("{year}.school.city",                                                        "city");
    m.put("{year}.school.state",                                                       "state");
    m.put("{year}.school.ownership",                                                   "ownership");
    m.put("school.carnegie_basic",                                              "carnegie_basic");
    m.put("school.minority_serving.historically_black",                         "hbcu");
    m.put("school.minority_serving.tribal",                                     "tribal");
    m.put("school.minority_serving.hispanic",                                   "hispanic_serving");
    m.put("{year}.school.online_only",                                                 "online_only");
    m.put("school.locale",                                                      "locale");
    m.put("{year}.student.size",                                                "size");
    m.put("{year}.aid.pell_grant_rate",                                         "pell_grant_rate");
    m.put("{year}.completion.completion_rate_4yr_150nt",                        "completion_rate_4yr");
    m.put("{year}.completion.completion_rate_less_than_4yr_150nt",              "completion_rate_2yr");
    m.put("{year}.student.retention_rate.four_year.full_time",                  "retention_rate_fulltime");
    m.put("{year}.aid.median_debt.completers.overall",                          "median_debt_completers");
    m.put("{year}.aid.median_debt.noncompleters",                               "median_debt_noncompleters");
    m.put("{year}.earnings.10_yrs_after_entry.working_not_enrolled.mean_earnings", "mean_earnings_10yr");
    m.put("{year}.earnings.8_yrs_after_entry.median_earnings",                 "median_earnings_8yr");
    m.put("{year}.student.demographics.median_family_income",                   "median_family_income");
    m.put("{year}.repayment.3_yr_default_rate",                                 "default_rate_3yr");
    m.put("{year}.cost.avg_net_price.public",                                   "avg_net_price_public");
    m.put("{year}.cost.avg_net_price.private",                                  "avg_net_price_private");
    m.put("{year}.cost.tuition.in_state",                                       "tuition_in_state");
    m.put("{year}.cost.tuition.out_of_state",                                   "tuition_out_state");
    FIELD_MAP = Collections.unmodifiableMap(m);
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Scorecard: empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      JsonNode results = root.isArray() ? root : root.path("results");
      if (!results.isArray()) {
        LOGGER.debug("Scorecard: no results array for {}", context.getUrl());
        return "[]";
      }

      String year = context.getDimensionValues().get("year");
      if (year == null) {
        throw new IllegalStateException("College Scorecard: year dimension is required");
      }
      Map<String, String> fieldMap = new HashMap<String, String>();
      for (Map.Entry<String, String> e : FIELD_MAP.entrySet()) {
        fieldMap.put(e.getKey().replace("{year}", year), e.getValue());
      }
      String presenceKey = year + ".school.name";

      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode record : results) {
        if (!record.isObject()) {
          continue;
        }
        if (!record.has(presenceKey)) {
          throw new IllegalStateException("College Scorecard: response lacks field " + presenceKey);
        }
        if (record.get(presenceKey).isNull()) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();

        // Inject year dimension
        row.put("year", Integer.parseInt(year));

        // Map flat dot-notation keys to schema column names
        Iterator<Map.Entry<String, JsonNode>> fields = record.properties().iterator();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> entry = fields.next();
          String apiKey = entry.getKey();
          String canonical = fieldMap.get(apiKey);
          if (canonical == null) {
            continue; // unmapped field
          }
          JsonNode value = entry.getValue();
          if (value.isNull() || PRIVACY_SUPPRESSED.equals(value.asText(null))) {
            row.putNull(canonical);
          } else {
            row.set(canonical, value);
          }
        }

        out.add(row);
      }

      LOGGER.debug("Scorecard: {} institution records for year={}", out.size(), year);
      return out.toString();

    } catch (Exception e) {
      throw new RuntimeException("College Scorecard: failed to parse response for "
          + context.getUrl(), e);
    }
  }
}
