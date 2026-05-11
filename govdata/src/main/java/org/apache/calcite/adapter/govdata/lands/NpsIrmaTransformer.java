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
package org.apache.calcite.adapter.govdata.lands;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms NPS IRMA Statistics API responses into {@code nps_visitation} rows.
 *
 * <p>The IRMA API returns an array of annual or monthly visitation records.
 * This transformer is invoked once per visit_year across all park units.
 *
 * <p>Input: JSON array from
 * {@code https://irma.nps.gov/Stats/api/v1/park/all/visitation/{year}}
 * <pre>
 * [
 *   {
 *     "UnitCode": "YOSE",
 *     "Year": 2023,
 *     "Month": 1,
 *     "RecreationVisitors": 130219,
 *     "RecreationHours": 260438.0,
 *     "TentCampers": 22130,
 *     "RVCampers": 5670,
 *     "BackcountryCampers": 812,
 *     "NonRecreationVisitors": 3402,
 *     "ConcessionLodgingNights": 4820
 *   }
 * ]
 * </pre>
 *
 * <p>Output: JSON array string with columns matching the {@code nps_visitation} schema.
 */
public class NpsIrmaTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NpsIrmaTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("nps_visitation: empty response from NPS IRMA API");
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode result = MAPPER.createArrayNode();

      JsonNode records = root.isArray() ? root : root.path("data");
      if (!records.isArray()) {
        LOGGER.warn("nps_visitation: expected JSON array from IRMA API, got: {}",
            root.getNodeType());
        return "[]";
      }

      for (JsonNode record : records) {
        ObjectNode row = MAPPER.createObjectNode();
        row.put("unit_code", textOrNull(record, "UnitCode"));
        row.put("visit_year", intOrNull(record, "Year"));
        row.put("visit_month", intOrNull(record, "Month"));
        row.put("recreation_visits", intOrNull(record, "RecreationVisitors"));
        row.put("recreation_hours", doubleOrNull(record, "RecreationHours"));
        row.put("tent_campers", intOrNull(record, "TentCampers"));
        row.put("rv_campers", intOrNull(record, "RVCampers"));
        row.put("backcountry_campers", intOrNull(record, "BackcountryCampers"));
        row.put("non_recreation_visits", intOrNull(record, "NonRecreationVisitors"));
        row.put("concession_lodging_nights", intOrNull(record, "ConcessionLodgingNights"));
        result.add(row);
      }

      LOGGER.debug("nps_visitation: transformed {} records", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      LOGGER.error("nps_visitation: failed to transform IRMA response: {}", e.getMessage(), e);
      throw new RuntimeException("nps_visitation transform failed", e);
    }
  }

  private String textOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    return val.isNull() || val.isMissingNode() ? null : val.asText(null);
  }

  private Integer intOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    return val.asInt();
  }

  private Double doubleOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    return val.asDouble();
  }
}
