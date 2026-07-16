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
package org.apache.calcite.adapter.govdata.housing;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps a CFPB HMDA Data Browser {@code aggregations} response into {@code hmda_loans} rows. The
 * request fans out one fetch per {@code (year, state)}; the response's {@code aggregations[]} array
 * carries one element per {@code (actions_taken, loan_purposes)} cell with a loan {@code count} and
 * loan-amount {@code sum}. The {@code year} partition value is supplied by the dimension; the state
 * abbreviation is taken from the {@code state} dimension. Numeric HMDA codes are decoded to labels.
 */
public class HmdaLoansTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Map<String, String> ACTION_LABELS;
  private static final Map<String, String> PURPOSE_LABELS;

  static {
    Map<String, String> a = new HashMap<String, String>();
    a.put("1", "Loan originated");
    a.put("2", "Application approved but not accepted");
    a.put("3", "Application denied");
    a.put("4", "Application withdrawn by applicant");
    a.put("5", "File closed for incompleteness");
    a.put("6", "Purchased loan");
    a.put("7", "Preapproval request denied");
    a.put("8", "Preapproval request approved but not accepted");
    ACTION_LABELS = Collections.unmodifiableMap(a);

    Map<String, String> p = new HashMap<String, String>();
    p.put("1", "Home purchase");
    p.put("2", "Home improvement");
    p.put("31", "Refinancing");
    p.put("32", "Cash-out refinancing");
    p.put("4", "Other purpose");
    p.put("5", "Not applicable");
    PURPOSE_LABELS = Collections.unmodifiableMap(p);
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      return "[]";
    }
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode aggs = root.path("aggregations");
      if (!aggs.isArray()) {
        JsonNode errorType = root.path("errorType");
        if (!errorType.isMissingNode() && !errorType.isNull()) {
          throw new RuntimeException("HMDA API error: "
              + root.path("message").asText(errorType.asText()));
        }
        return "[]";
      }
      String stateAbbr = context.getDimensionValues() != null
          ? context.getDimensionValues().get("state") : null;
      if (stateAbbr == null) {
        stateAbbr = text(root.path("parameters"), "state");
      }
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode agg : aggs) {
        String action = text(agg, "actions_taken");
        String purpose = text(agg, "loan_purposes");
        if (action == null || purpose == null) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();
        row.put("state_abbr", stateAbbr);
        row.put("action_taken", action);
        row.put("action_label", ACTION_LABELS.get(action));
        row.put("loan_purpose", purpose);
        row.put("loan_purpose_label", PURPOSE_LABELS.get(purpose));
        JsonNode count = agg.path("count");
        if (count.isNumber()) {
          row.put("loan_count", count.asLong());
        } else {
          row.putNull("loan_count");
        }
        JsonNode sum = agg.path("sum");
        if (sum.isNumber()) {
          row.put("loan_amount_total", sum.asDouble());
        } else {
          row.putNull("loan_amount_total");
        }
        out.add(row);
      }
      return MAPPER.writeValueAsString(out);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("HmdaLoansTransformer transform failed: " + e.getMessage(), e);
    }
  }

  private static String text(JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    return v.isMissingNode() || v.isNull() || v.asText().isEmpty() ? null : v.asText();
  }
}
