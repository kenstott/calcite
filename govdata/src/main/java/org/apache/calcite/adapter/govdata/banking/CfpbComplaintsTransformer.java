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
package org.apache.calcite.adapter.govdata.banking;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps CFPB Consumer Complaint Database search-API records into {@code consumer_complaints}
 * rows.
 *
 * <p>Unlike the FDIC BankFind Suite endpoints, this API is OpenSearch-shaped:
 * {@code {"hits": {"total": {"value": N}, "hits": [{"_id": ..., "_source": {...FIELDS...}}]}}}.
 * The record array is at {@code hits.hits} and each element's fields are under {@code _source}.
 * This is not a subclass of {@link AbstractFdicTransformer} — the envelope, date format (ISO-8601
 * here vs. FDIC's {@code MM/DD/YYYY}), and source host are all unrelated to FDIC's.
 */
public class CfpbComplaintsTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Logger LOGGER = LoggerFactory.getLogger(CfpbComplaintsTransformer.class);

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      return "[]";
    }
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode records = root.path("hits").path("hits");
      if (!records.isArray()) {
        JsonNode error = root.path("error");
        if (!error.isMissingNode() && !error.isNull()) {
          throw new RuntimeException("consumer_complaints: CFPB API error: " + error);
        }
        LOGGER.warn("consumer_complaints: no 'hits.hits' array in response (first 200 chars: {})",
            response.substring(0, Math.min(200, response.length())));
        return "[]";
      }
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode element : records) {
        JsonNode source = element.path("_source");
        if (!source.isObject()) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();
        mapRow(source, row);
        out.add(row);
      }
      LOGGER.debug("consumer_complaints: transformed {} records", out.size());
      return MAPPER.writeValueAsString(out);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("consumer_complaints transform failed: " + e.getMessage(), e);
    }
  }

  private static void mapRow(JsonNode rec, ObjectNode row) {
    putLong(row, "complaint_id", rec, "complaint_id");
    putDate(row, "date_received", rec, "date_received");
    putDate(row, "date_sent_to_company", rec, "date_sent_to_company");
    putText(row, "product", rec, "product");
    putText(row, "sub_product", rec, "sub_product");
    putText(row, "issue", rec, "issue");
    putText(row, "sub_issue", rec, "sub_issue");
    putText(row, "company", rec, "company");
    putText(row, "state_abbr", rec, "state");
    putText(row, "zip_code", rec, "zip_code");
    putText(row, "submitted_via", rec, "submitted_via");
    putText(row, "company_response", rec, "company_response");
    putText(row, "timely_response", rec, "timely");
    putBool(row, "has_narrative", rec, "has_narrative");
  }

  // --- shared field helpers -------------------------------------------------

  private static void putText(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
    } else {
      row.put(col, v.asText());
    }
  }

  private static void putLong(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull() || (v.isTextual() && v.asText().isEmpty())) {
      row.putNull(col);
    } else {
      row.put(col, v.asLong());
    }
  }

  private static void putBool(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
    } else if (v.isBoolean()) {
      row.put(col, v.booleanValue());
    } else {
      String s = v.asText().trim();
      row.put(col, "true".equalsIgnoreCase(s) || "1".equals(s));
    }
  }

  /** CFPB dates are ISO-8601 ({@code 2014-08-28T17:01:49.000Z}); take the date part. */
  private static void putDate(ObjectNode row, String col, JsonNode rec, String field) {
    JsonNode v = rec.path(field);
    if (v.isMissingNode() || v.isNull()) {
      row.putNull(col);
      return;
    }
    String s = v.asText();
    if (s.isEmpty()) {
      row.putNull(col);
    } else if (s.length() >= 10) {
      row.put(col, s.substring(0, 10));
    } else {
      row.put(col, s);
    }
  }
}
