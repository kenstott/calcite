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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Flattens the ITA Consolidated Screening List bulk file
 * ({@code https://data.trade.gov/downloadable_consolidated_screening_list/v1/consolidated.json})
 * into one row per listing. Nested address, alias and program arrays become delimited strings.
 * A response without a non-empty {@code results} array fails the run.
 */
public class ConsolidatedScreeningListTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    JsonNode root;
    try {
      root = MAPPER.readTree(response);
    } catch (IOException e) {
      throw new IllegalStateException("consolidated_screening_list: response is not JSON", e);
    }
    JsonNode results = root.path("results");
    if (!results.isArray() || results.size() == 0) {
      throw new IllegalStateException(
          "consolidated_screening_list: response has no results array; the file layout changed");
    }
    ArrayNode out = MAPPER.createArrayNode();
    for (JsonNode r : results) {
      ObjectNode row = out.addObject();
      String source = r.path("source").asText();
      row.put("id", r.path("id").asText());
      row.put("source", source);
      row.put("source_list", listCode(source));
      row.put("name", r.path("name").asText());
      putText(row, "entity_type", r.path("type"));
      putText(row, "federal_register_notice", r.path("federal_register_notice"));
      putText(row, "start_date", r.path("start_date"));
      putText(row, "end_date", r.path("end_date"));
      putText(row, "license_requirement", r.path("license_requirement"));
      putText(row, "license_policy", r.path("license_policy"));
      putText(row, "remarks", r.path("remarks"));
      putText(row, "source_list_url", r.path("source_list_url"));
      Set<String> countries = new LinkedHashSet<String>();
      Set<String> addresses = new LinkedHashSet<String>();
      for (JsonNode a : r.path("addresses")) {
        String c = a.path("country").asText("");
        if (!c.isEmpty()) {
          countries.add(c);
        }
        StringBuilder sb = new StringBuilder();
        for (String f : new String[] {"address", "city", "state", "postal_code", "country"}) {
          String v = a.path(f).asText("").trim();
          if (!v.isEmpty()) {
            sb.append(sb.length() > 0 ? ", " : "").append(v);
          }
        }
        if (sb.length() > 0) {
          addresses.add(sb.toString());
        }
      }
      putText(row, "country", countries.isEmpty() ? null : countries.iterator().next());
      putText(row, "countries", join(countries, ";"));
      putText(row, "addresses", join(addresses, " | "));
      Set<String> aliases = new LinkedHashSet<String>();
      for (JsonNode n : r.path("alt_names")) {
        aliases.add(n.asText());
      }
      putText(row, "alt_names", join(aliases, " | "));
      Set<String> programs = new LinkedHashSet<String>();
      for (JsonNode n : r.path("programs")) {
        programs.add(n.asText());
      }
      putText(row, "programs", join(programs, ";"));
    }
    return out.toString();
  }

  /** Short code in parentheses at the end of the list name, e.g. "EL" for the Entity List. */
  static String listCode(String source) {
    int open = source.indexOf('(');
    int close = source.indexOf(')', open + 1);
    if (open < 0 || close < 0) {
      throw new IllegalStateException(
          "consolidated_screening_list: no list code in source '" + source + "'");
    }
    return source.substring(open + 1, close);
  }

  private static String join(Set<String> values, String sep) {
    if (values.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (String v : values) {
      sb.append(sb.length() > 0 ? sep : "").append(v);
    }
    return sb.toString();
  }

  private static void putText(ObjectNode row, String key, JsonNode v) {
    String s = v.isMissingNode() || v.isNull() ? null : v.asText();
    putText(row, key, s);
  }

  private static void putText(ObjectNode row, String key, String s) {
    if (s == null || s.isEmpty()) {
      row.putNull(key);
    } else {
      row.put(key, s);
    }
  }
}
