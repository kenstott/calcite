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
package org.apache.calcite.adapter.govdata.crime;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Transforms TRAC's ICE detention population snapshot response into flat rows.
 *
 * <p>TRAC (Transactional Records Access Clearinghouse, Syracuse University) compiles this
 * from ICE's own internal data, obtained via FOIA/litigation — it is not ICE's own raw
 * publication. The source returns its full point-in-time history in a single JSON array on
 * every fetch (irregular cadence, roughly monthly-ish with gaps, not a fixed schedule), one
 * object per snapshot date:
 * <pre>{@code
 * [{"date": "07/11/2026", "ice_all": 58231, "cbp_all": 7534, "total_all": 65765,
 *   "ice_other": 21675, "cbp_other": 4514, "total_other": 26189,
 *   "ice_pend": 18513, "cbp_pend": 1734, "total_pend": 20247,
 *   "ice_conv": 18043, "cbp_conv": 1286, "total_conv": 19329}, ...]
 * }</pre>
 *
 * <p>Each field crosses one of three arresting agencies ({@code ice}, {@code cbp},
 * {@code total}) with one of four criminal-history categories ({@code conv} = Convicted
 * Criminal, {@code pend} = Pending Criminal Charges, {@code other} = Other Immigration
 * Violator, {@code all} = total across all three). This transformer unpivots each input
 * record into 3 agencies &times; 4 categories = 12 output rows with {@code snapshot_date}
 * (ISO, parsed from the source's {@code MM/dd/yyyy}), {@code arresting_agency},
 * {@code criminality}, and {@code detained_population}.
 */
public class IceDetentionPopulationTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IceDetentionPopulationTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final DateTimeFormatter SOURCE_DATE_FORMAT =
      DateTimeFormatter.ofPattern("MM/dd/yyyy");

  /** Source field prefix -> output arresting_agency label. */
  private static final String[] AGENCY_KEYS = {"ice", "cbp", "total"};
  private static final String[] AGENCY_LABELS = {"ICE", "CBP", "Total"};

  /** Source field suffix -> output criminality label. */
  private static final String[] CRIMINALITY_KEYS = {"conv", "pend", "other", "all"};
  private static final String[] CRIMINALITY_LABELS = {
      "Convicted Criminal", "Pending Criminal Charges", "Other Immigration Violator", "All"
  };

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("ICE Detention: Empty response for {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      if (!root.isArray()) {
        LOGGER.warn("ICE Detention: Expected array response, got {}", root.getNodeType());
        return "[]";
      }

      ArrayNode result = MAPPER.createArrayNode();

      for (JsonNode record : root) {
        String rawDate = getTextOrNull(record, "date");
        if (rawDate == null) {
          continue;
        }

        String snapshotDate;
        try {
          snapshotDate = LocalDate.parse(rawDate, SOURCE_DATE_FORMAT).toString();
        } catch (DateTimeParseException e) {
          LOGGER.warn("ICE Detention: Unparseable date '{}', skipping record", rawDate);
          continue;
        }

        for (int a = 0; a < AGENCY_KEYS.length; a++) {
          for (int c = 0; c < CRIMINALITY_KEYS.length; c++) {
            String field = AGENCY_KEYS[a] + "_" + CRIMINALITY_KEYS[c];
            JsonNode value = record.get(field);

            ObjectNode row = MAPPER.createObjectNode();
            row.put("snapshot_date", snapshotDate);
            row.put("arresting_agency", AGENCY_LABELS[a]);
            row.put("criminality", CRIMINALITY_LABELS[c]);
            if (value != null && value.isNumber()) {
              row.put("detained_population", value.intValue());
            } else {
              row.putNull("detained_population");
            }
            result.add(row);
          }
        }
      }

      LOGGER.debug("ICE Detention: Transformed {} rows from {} snapshot records",
          result.size(), root.size());
      return result.toString();

    } catch (Exception e) {
      throw new RuntimeException("ICE Detention: Failed to parse response for "
          + context.getUrl(), e);
    }
  }

  private static String getTextOrNull(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    return value.asText();
  }
}
