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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Projects each page of CMS's POS (Provider of Services) data-api response down to the small
 * field set {@code cms_pos_termination_history} actually needs.
 *
 * <p>The raw response is a bare JSON array of objects carrying all ~473 columns of the POS
 * record layout, most of them legitimately blank for any given facility category (a hospital
 * record populates none of the SNF-only fields, and vice versa). That sparsity is real, not a
 * defect — but it defeats DuckDB's {@code read_json_auto} schema inference in the ETL pipeline's
 * whole-batch expression evaluator ({@code IcebergMaterializationWriter.transformRowsWithDuckDb}):
 * confirmed live 2026-09-11 that feeding it an unprojected batch of these records infers a single
 * {@code MAP(VARCHAR, VARCHAR)} column literally named {@code json} instead of one column per
 * field, so every {@code src."FIELD"} column expression silently resolves to NULL (the batch's
 * "pad missing referenced columns with NULL" safety net — meant for a genuinely absent field —
 * masks this as a normal-looking materialize with no error, no warning, and 100% null rows: the
 * bug this session shipped once and caught only by checking real content, not row counts).
 *
 * <p>Projecting down to ~16 fields per page here, before the batch ever reaches DuckDB's
 * inference, keeps the schema narrow and regular enough to infer correctly as a real struct.
 */
public class CmsPosTerminationFieldProjector implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String[] FIELDS = {
      "PRVDR_NUM", "FAC_NAME", "PRVDR_CTGRY_CD", "STATE_CD", "FIPS_STATE_CD", "FIPS_CNTY_CD",
      "CROSS_REF_PROVIDER_NUMBER", "ORGNL_PRTCPTN_DT", "CRTFCTN_ACTN_TYPE_CD", "PGM_TRMNTN_CD",
      "TRMNTN_EXPRTN_DT", "PSYCH_UNIT_TRMNTN_CD", "PSYCH_UNIT_TRMNTN_DT", "REHAB_UNIT_TRMNTN_CD",
      "REHAB_UNIT_TRMNTN_DT"
  };

  @Override public String transform(String response, RequestContext context) {
    try {
      JsonNode root = MAPPER.readTree(response);
      if (!root.isArray()) {
        return response;
      }
      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode rec : root) {
        ObjectNode projected = MAPPER.createObjectNode();
        for (String field : FIELDS) {
          JsonNode v = rec.path(field);
          if (v.isMissingNode()) {
            projected.putNull(field);
          } else {
            projected.set(field, v);
          }
        }
        out.add(projected);
      }
      return out.toString();
    } catch (Exception e) {
      throw new RuntimeException("CmsPosTerminationFieldProjector: failed to project response", e);
    }
  }
}
