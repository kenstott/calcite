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
package org.apache.calcite.adapter.govdata.fisheries;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns the USFWS ECOS species report (ESA listings) into one row per salmon, steelhead and
 * Atlantic salmon listing entity (ESU / DPS).
 *
 * <p>Input: {@code {"meta":..., "data":[[common name, {"value": scientific name, "url": ...},
 * ESA listing status, status category, entity description, listing date MM-dd-yyyy, lead
 * agency], ...]}}. Only the seven salmon and steelhead common names are kept; trout rows in the
 * same genus are out of scope for this table.
 *
 * <p>ECOS carries no state column, so each listing's states are taken from the ESU/DPS name
 * ({@link #STATES_BY_ENTITY}). A listed, experimental or under-review entity with no mapping
 * fails the load, so a new listing forces the table to be extended rather than shipping with no
 * state. Entities that ECOS itself leaves without geography (an empty entity description, or
 * "wherever found") carry null states.
 */
public class FisheriesEsaListingsTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern DATE = Pattern.compile("^(\\d{2})-(\\d{2})-(\\d{4})$");
  private static final Pattern CFR = Pattern.compile("\\s*-?\\s*See (50 CFR [\\d.]+)\\s*$");

  /** ECOS common name to species_group value. */
  private static final Map<String, String> GROUPS = new LinkedHashMap<String, String>();
  /** Lower-case entity-name prefix to the states the ESU/DPS spans. */
  private static final Map<String, String> STATES_BY_ENTITY = new LinkedHashMap<String, String>();

  static {
    GROUPS.put("chinook salmon", "chinook");
    GROUPS.put("coho salmon", "coho");
    GROUPS.put("chum salmon", "chum");
    GROUPS.put("sockeye salmon", "sockeye");
    GROUPS.put("pink salmon", "pink");
    GROUPS.put("steelhead (rainbow trout)", "steelhead");
    GROUPS.put("atlantic salmon", "atlantic");

    STATES_BY_ENTITY.put("gulf of maine", "ME");
    STATES_BY_ENTITY.put("kennebec river", "ME");
    STATES_BY_ENTITY.put("lower columbia river", "OR,WA");
    STATES_BY_ENTITY.put("columbia river esu", "OR,WA");
    STATES_BY_ENTITY.put("snake river", "ID,OR,WA");
    STATES_BY_ENTITY.put("upper willamette river", "OR");
    STATES_BY_ENTITY.put("upper columbia river", "WA");
    STATES_BY_ENTITY.put("middle columbia river", "OR,WA");
    STATES_BY_ENTITY.put("central valley spring-run", "CA");
    STATES_BY_ENTITY.put("sacramento river winter-run", "CA");
    STATES_BY_ENTITY.put("california coastal", "CA");
    STATES_BY_ENTITY.put("california central valley", "CA");
    STATES_BY_ENTITY.put("central california coast", "CA");
    STATES_BY_ENTITY.put("south-central california coast", "CA");
    STATES_BY_ENTITY.put("southern california", "CA");
    STATES_BY_ENTITY.put("northern california", "CA");
    STATES_BY_ENTITY.put("southern oregon - northern california coast", "CA,OR");
    STATES_BY_ENTITY.put("oregon coast", "OR");
    STATES_BY_ENTITY.put("klamath mountains province", "CA,OR");
    STATES_BY_ENTITY.put("puget sound", "WA");
    STATES_BY_ENTITY.put("hood canal", "WA");
    STATES_BY_ENTITY.put("ozette lake", "WA");
    STATES_BY_ENTITY.put("lake sammamish", "WA");
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      throw new IllegalStateException("ESA listings: empty response for " + context.getUrl());
    }
    JsonNode data;
    try {
      data = MAPPER.readTree(response).get("data");
    } catch (IOException e) {
      throw new IllegalStateException("ESA listings: unparseable response for "
          + context.getUrl(), e);
    }
    if (data == null || !data.isArray()) {
      throw new IllegalStateException("ESA listings: no data array in response for "
          + context.getUrl() + " (starts: " + response.substring(0, Math.min(200, response.length())) + ")");
    }

    ArrayNode result = MAPPER.createArrayNode();
    for (JsonNode r : data) {
      String common = r.get(0).asText().trim();
      String group = GROUPS.get(common.toLowerCase(java.util.Locale.ROOT));
      if (group == null) {
        continue;
      }
      String entity = text(r.get(4));
      String status = text(r.get(2));
      String category = text(r.get(3));

      String cfr = null;
      Matcher c = CFR.matcher(entity);
      if (c.find()) {
        cfr = c.group(1);
        entity = entity.substring(0, c.start()).trim();
      }

      ObjectNode row = MAPPER.createObjectNode();
      row.put("common_name", common);
      row.put("scientific_name", r.get(1).get("value").asText());
      row.put("species_group", group);
      putNullable(row, "population_name", entity);
      row.put("esa_status", status);
      putNullable(row, "status_category", category);
      putNullable(row, "cfr_citation", cfr);
      putNullable(row, "listing_date", isoDate(text(r.get(5)), common, entity));
      row.put("lead_agency", text(r.get(6)));

      String states = statesFor(entity);
      boolean inEffect = "Listed".equals(category) || status.startsWith("Experimental")
          || status.startsWith("Under Review");
      if (states == null && inEffect) {
        throw new IllegalStateException("ESA listings: no state mapping for " + common
            + " / '" + entity + "' (" + status + "); add it to STATES_BY_ENTITY");
      }
      putNullable(row, "state_abbrs", states);
      result.add(row);
    }
    if (result.size() == 0) {
      throw new IllegalStateException("ESA listings: no salmon or steelhead rows in "
          + context.getUrl());
    }
    try {
      return MAPPER.writeValueAsString(result);
    } catch (IOException e) {
      throw new IllegalStateException("ESA listings: cannot serialize rows", e);
    }
  }

  private static String statesFor(String entity) {
    String key = entity.toLowerCase(java.util.Locale.ROOT);
    for (Map.Entry<String, String> e : STATES_BY_ENTITY.entrySet()) {
      if (key.startsWith(e.getKey())) {
        return e.getValue();
      }
    }
    return null;
  }

  private static String isoDate(String mdy, String common, String entity) {
    if (mdy.isEmpty()) {
      return null;
    }
    Matcher m = DATE.matcher(mdy);
    if (!m.matches()) {
      throw new IllegalStateException("ESA listings: unexpected date '" + mdy + "' for "
          + common + " / " + entity);
    }
    return m.group(3) + "-" + m.group(1) + "-" + m.group(2);
  }

  private static String text(JsonNode n) {
    return n == null || n.isNull() ? "" : n.asText().trim();
  }

  private static void putNullable(ObjectNode row, String name, String value) {
    if (value == null || value.isEmpty()) {
      row.putNull(name);
    } else {
      row.put(name, value);
    }
  }
}
