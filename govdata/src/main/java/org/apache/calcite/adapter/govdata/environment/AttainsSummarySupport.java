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
package org.apache.calcite.adapter.govdata.environment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared parsing of the EPA ATTAINS {@code usesStateSummary} response for the
 * {@code attains_state_use_summary} and {@code attains_cause_summary} tables.
 *
 * <p>Response shape: {@code data.reportingCycles[].waterTypes[].useAttainments[]}; each use
 * attainment carries one numeric member per assessment category (e.g. "Fully Supporting",
 * "Not Supporting") plus a matching "&lt;category&gt;-count" member, and a {@code parameters[]} list
 * whose entries carry the same kind of category/count pairs keyed by parameter group.
 */
final class AttainsSummarySupport {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** The API's message for an organization with no public data in the requested cycle. */
  private static final String NO_DATA_MESSAGE = "does not have publicly available data";

  /** ATTAINS organization id to USPS state/territory code, for the state agencies. */
  static final Map<String, String> ORG_STATE;

  static {
    String[][] pairs = {
        {"CA_SWRCB", "CA"}, {"WYDEQ", "WY"}, {"RIDEM", "RI"}, {"PR_LAKES", "PR"},
        {"21SC60WQ", "SC"}, {"DOEE", "DC"}, {"WVDEP", "WV"}, {"MNPCA", "MN"},
        {"OREGONDEQ", "OR"}, {"21NEB001", "NE"}, {"21KY", "KY"}, {"21OHIO", "OH"},
        {"OKDEQ", "OK"}, {"21IND", "IN"}, {"21VASWCB", "VA"}, {"TCEQMAIN", "TX"},
        {"11113300", "NH"}, {"USVIST", "VI"}, {"21ARIZ", "AZ"}, {"UTAHDWQ", "UT"},
        {"IL_EPA", "IL"}, {"1VTDECWQ", "VT"}, {"MTDEQ", "MT"}, {"21MSWQ", "MS"},
        {"SDDENR", "SD"}, {"21NYDECA", "NY"}, {"ARDEQH2O", "AR"}, {"WA_ECOLOGY", "WA"},
        {"21COL001", "CO"}, {"IDEQ", "ID"}, {"21MICH", "MI"}, {"MEDEP", "ME"},
        {"21NEV1", "NV"}, {"21AWIC", "AL"}, {"21DELAWQ", "DE"}, {"21GAEPD", "GA"},
        {"21IOWA", "IA"}, {"WIDNR", "WI"}, {"21NC01WQ", "NC"}, {"TDECWR", "TN"},
        {"21KAN001", "KS"}, {"21AS", "AS"}, {"21NMEX", "NM"}, {"AKDECWQ", "AK"},
        {"LADEQWPD", "LA"}, {"21NJDEP1", "NJ"}, {"CT_DEP01", "CT"}, {"21FL303D", "FL"},
        {"21GUAM", "GU"}, {"21HI", "HI"}, {"MDE_EASP", "MD"}, {"MDNR", "MO"},
        {"MA_DEP", "MA"}, {"21AQ", "MP"}, {"21NDHDWQ", "ND"}, {"21PA", "PA"},
    };
    Map<String, String> m = new LinkedHashMap<String, String>();
    for (String[] p : pairs) {
      m.put(p[0], p[1]);
    }
    ORG_STATE = Collections.unmodifiableMap(m);
  }

  private AttainsSummarySupport() {
  }

  /** Receives one (water type, use) or (water type, use, parameter) category value. */
  interface RowSink {
    void accept(ObjectNode base, String category, double size, long count);
  }

  /**
   * Parses the response and returns the {@code data} node's reporting-cycle array, or null when
   * the API reports the organization has no public data for the cycle (a normal outcome for
   * states that have not yet submitted that cycle). Any other shape is an error.
   */
  static JsonNode cycles(String response, String orgId) throws IOException {
    JsonNode root = MAPPER.readTree(response);
    JsonNode data = root.get("data");
    if (data == null || data.isNull()) {
      JsonNode messages = root.get("messages");
      if (messages != null && messages.size() > 0) {
        String msg = messages.get(0).path("message").asText("");
        if (msg.contains(NO_DATA_MESSAGE)) {
          return null;
        }
        throw new IllegalStateException("ATTAINS " + orgId + ": " + msg);
      }
      throw new IllegalStateException("ATTAINS " + orgId + ": response has no data and no message");
    }
    JsonNode cycles = data.get("reportingCycles");
    if (cycles == null || !cycles.isArray()) {
      throw new IllegalStateException("ATTAINS " + orgId + ": missing data.reportingCycles");
    }
    return cycles;
  }

  static String stateOf(String orgId) {
    String state = ORG_STATE.get(orgId);
    if (state == null) {
      throw new IllegalStateException("ATTAINS organization not in state map: " + orgId);
    }
    return state;
  }

  /** Emits (category, size, count) for each "X" / "X-count" member pair of {@code node}. */
  static void categories(JsonNode node, ObjectNode base, RowSink sink) {
    Iterator<String> it = node.fieldNames();
    while (it.hasNext()) {
      String key = it.next();
      JsonNode value = node.get(key);
      if (!value.isNumber() || key.endsWith("-count")) {
        continue;
      }
      JsonNode count = node.get(key + "-count");
      if (count == null || !count.isNumber()) {
        throw new IllegalStateException("ATTAINS category without count: " + key);
      }
      sink.accept(base, key, value.asDouble(), count.asLong());
    }
  }

  static ArrayNode newArray() {
    return MAPPER.createArrayNode();
  }

  static ObjectNode newObject() {
    return MAPPER.createObjectNode();
  }

  static String write(ArrayNode out) throws IOException {
    return MAPPER.writeValueAsString(out);
  }

  static boolean isBlank(String s) {
    return s == null || s.isEmpty();
  }
}
