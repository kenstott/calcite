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

import java.util.HashMap;
import java.util.Map;

/**
 * Transforms Medicaid Drug Utilization data (CMS DKAN datastore query).
 *
 * <p>The datastore endpoint ignores {@code limit}/{@code offset} and returns the whole
 * annual dataset (~5.3M rows, ~2GB JSON) as a single {@code {"results":[...]}} envelope.
 * Implementing {@link PerRecordResponseTransformer} routes this through HttpSource's
 * {@code streamFromRawCache} path, which drains {@code results[]} one record at a time —
 * the response is never materialised as a String or a JSON tree, so it does not OOM
 * regardless of size, and the raw cache is still used (streamed, not read whole).
 */
public class MedicaidDrugUtilizationResponseTransformer implements PerRecordResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Streaming path: map one source record in-place (called per row by streamFromRawCache). */
  @Override
  public void transformRecord(Map<String, Object> row, RequestContext context) {
    Map<String, Object> src = new HashMap<>(row);
    row.clear();
    row.put("state", str(src.get("state")));
    String ndc = str(src.get("ndc"));
    row.put("ndc", ndc);
    row.put("product_ndc9", NdcNormalizer.fromElevenDigitNdc(ndc));
    row.put("year", str(src.get("year")));
    row.put("quarter", normalizeQuarter(str(src.get("quarter"))));
    row.put("utilization_type", str(src.get("utilization_type")));
    row.put("product_name", str(src.get("product_name")));
    row.put("labeler_code", str(src.get("labeler_code")));
    row.put("units_reimbursed", str(src.get("units_reimbursed")));
    row.put("number_of_prescriptions", str(src.get("number_of_prescriptions")));
    row.put("total_amount_reimbursed", str(src.get("total_amount_reimbursed")));
    row.put("medicaid_amount_reimbursed", str(src.get("medicaid_amount_reimbursed")));
    row.put("non_medicaid_amount_reimbursed", str(src.get("non_medicaid_amount_reimbursed")));
    row.put("suppression_used", str(src.get("suppression_used")));
    row.put("type", "medicaid_drug_utilization");
  }

  /**
   * Non-streaming fallback (live response not routed through streamFromRawCache): parse the
   * {@code results[]} array and delegate each element to {@link #transformRecord} so the field
   * mapping lives in one place.
   */
  @Override
  @SuppressWarnings("unchecked")
  public String transform(String response, RequestContext context) {
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode results = root.has("results") ? root.path("results") : root;
      if (!results.isArray()) {
        return "[]";
      }
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode record : results) {
        Map<String, Object> row = MAPPER.convertValue(record, Map.class);
        transformRecord(row, context);
        out.add(MAPPER.valueToTree(row));
      }
      return out.toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to transform Medicaid drug utilization response", e);
    }
  }

  private static String normalizeQuarter(String raw) {
    if (raw == null) {
      return null;
    }
    switch (raw.trim()) {
    case "1": return "Q1";
    case "2": return "Q2";
    case "3": return "Q3";
    case "4": return "Q4";
    default:  return raw;
    }
  }

  private static String str(Object value) {
    if (value == null) {
      return null;
    }
    String s = String.valueOf(value);
    return s.isEmpty() ? null : s;
  }
}
