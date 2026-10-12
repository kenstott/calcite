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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns the NOAA Fisheries Economics of the United States (FEUS) data-tool files into one row per
 * geography x sector x impact measure (x imports basis, commercial only).
 *
 * <p>Input is a JavaScript file of top-level array assignments (name, equals sign, then a JSON
 * array of header and data rows) served by the FEUS data tool. The {@code feus_file} dimension selects {@code Comm} (commercial
 * seafood industry; arrays {@code commTableFootnotes} and {@code commImpactsState}) or {@code Rec}
 * (marine recreational fishing; arrays {@code recTableFootnotes} and {@code recImpactsStates}).
 *
 * <p>The data year is not a column of the source. It is read from the per-geography title in the
 * footnotes array ("2021 Economic Impacts of the Alabama Seafood Industry"). A geography with no
 * title, or a title without a year, fails the load. The commercial table reports Florida as one
 * state while the footnotes title East and West Florida separately; Florida takes the year
 * shared by both halves and fails the load if they differ.
 *
 * <p>"NA" values are NOAA's confidential-data marker and become null.
 */
public class FisheriesFeusImpactsTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern TITLE_YEAR = Pattern.compile("^(\\d{4}) Economic Impacts");
  private static final Pattern NUMBER = Pattern.compile("^-?\\d+(\\.\\d+)?$");

  private static final Map<String, String> MEASURES = new LinkedHashMap<String, String>();
  private static final Map<String, String> STATE_ABBR = new HashMap<String, String>();

  static {
    MEASURES.put("#Jobs", "jobs");
    MEASURES.put("Sales", "sales");
    MEASURES.put("Income", "income");
    MEASURES.put("Value Added", "value_added");

    String[][] pairs = {
        {"Alabama", "AL"}, {"Alaska", "AK"}, {"California", "CA"}, {"Connecticut", "CT"},
        {"Delaware", "DE"}, {"Florida", "FL"}, {"East Florida", "FL"}, {"West Florida", "FL"},
        {"Georgia", "GA"}, {"Hawai`i", "HI"}, {"Louisiana", "LA"}, {"Maine", "ME"},
        {"Maryland", "MD"}, {"Massachusetts", "MA"}, {"Mississippi", "MS"},
        {"New Hampshire", "NH"}, {"New Jersey", "NJ"}, {"New York", "NY"},
        {"North Carolina", "NC"}, {"Oregon", "OR"}, {"Rhode Island", "RI"},
        {"South Carolina", "SC"}, {"Texas", "TX"}, {"Virginia", "VA"}, {"Washington", "WA"}
    };
    for (String[] p : pairs) {
      STATE_ABBR.put(p[0], p[1]);
    }
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      throw new IllegalStateException("FEUS: empty response for " + context.getUrl());
    }
    String file = context.getDimensionValues().get("feus_file");
    boolean commercial;
    String footnotesVar;
    String impactsVar;
    String footTable;
    if ("Comm".equals(file)) {
      commercial = true;
      footnotesVar = "commTableFootnotes";
      impactsVar = "commImpactsState";
      footTable = "commImpacts";
    } else if ("Rec".equals(file)) {
      commercial = false;
      footnotesVar = "recTableFootnotes";
      impactsVar = "recImpactsStates";
      footTable = "recImpacts";
    } else {
      throw new IllegalStateException("FEUS: unknown feus_file dimension '" + file + "'");
    }

    JsonNode footnotes = extractArray(response, footnotesVar, context);
    JsonNode impacts = extractArray(response, impactsVar, context);

    Map<String, Integer> years = new HashMap<String, Integer>();
    Map<String, String> regions = new HashMap<String, String>();
    for (int i = 1; i < footnotes.size(); i++) {
      JsonNode f = footnotes.get(i);
      if (!footTable.equals(text(f.get(2)))) {
        continue;
      }
      Matcher m = TITLE_YEAR.matcher(text(f.get(3)));
      if (!m.find()) {
        throw new IllegalStateException("FEUS: no year in footnote title '" + text(f.get(3))
            + "' for " + context.getUrl());
      }
      years.put(text(f.get(1)), Integer.valueOf(m.group(1)));
      regions.put(text(f.get(1)), text(f.get(0)));
    }

    ArrayNode result = MAPPER.createArrayNode();
    for (int i = 1; i < impacts.size(); i++) {
      JsonNode r = impacts.get(i);
      String sector;
      String measureRaw;
      String geography;
      String imports;
      String valueRaw;
      if (commercial) {
        sector = text(r.get(0));
        measureRaw = text(r.get(1));
        geography = text(r.get(2));
        imports = text(r.get(3));
        valueRaw = text(r.get(4));
      } else {
        sector = text(r.get(0));
        measureRaw = text(r.get(1));
        geography = text(r.get(2));
        imports = null;
        valueRaw = text(r.get(3));
      }
      String measure = MEASURES.get(measureRaw);
      if (measure == null) {
        throw new IllegalStateException("FEUS: unknown impact type '" + measureRaw + "' in "
            + context.getUrl());
      }

      boolean national = "United States".equals(geography);
      String abbr = null;
      if (!national) {
        abbr = STATE_ABBR.get(geography);
        if (abbr == null) {
          throw new IllegalStateException("FEUS: geography '" + geography
              + "' has no state_abbr mapping; add it to STATE_ABBR");
        }
      }

      ObjectNode row = MAPPER.createObjectNode();
      row.put("industry", commercial ? "commercial" : "recreational");
      row.put("geography", geography);
      row.put("geo_level", national ? "national" : "state");
      putNullable(row, "state_abbr", abbr);
      putNullable(row, "noaa_region", regionFor(geography, regions));
      row.put("data_year", yearFor(geography, years, context));
      row.put("sector", sector);
      row.put("row_level", rowLevel(sector));
      row.put("impact_type", measure);
      row.put("unit", "jobs".equals(measure) ? "jobs" : "thousand_usd");
      if (imports == null) {
        row.putNull("imports_basis");
      } else if ("With Imports".equals(imports)) {
        row.put("imports_basis", "with_imports");
      } else if ("Without Imports".equals(imports)) {
        row.put("imports_basis", "without_imports");
      } else {
        throw new IllegalStateException("FEUS: unknown imports basis '" + imports + "'");
      }
      if ("NA".equals(valueRaw)) {
        row.putNull("impact_value");
      } else if (NUMBER.matcher(valueRaw).matches()) {
        row.put("impact_value", Double.parseDouble(valueRaw));
      } else {
        throw new IllegalStateException("FEUS: non-numeric impact value '" + valueRaw + "' for "
            + geography + " / " + sector + " / " + measureRaw);
      }
      result.add(row);
    }
    if (result.size() == 0) {
      throw new IllegalStateException("FEUS: no impact rows in " + context.getUrl());
    }
    try {
      return MAPPER.writeValueAsString(result);
    } catch (IOException e) {
      throw new IllegalStateException("FEUS: cannot serialize rows", e);
    }
  }

  /** Pulls {@code var <name> = [[...]];} out of the JavaScript and parses it as JSON. */
  private static JsonNode extractArray(String js, String name, RequestContext context) {
    // The arrays end with "]]" followed by the next declaration, a comment or end of file
    // (the source has no semicolons).
    Matcher m = Pattern.compile("var\\s+" + name + "\\s*=\\s*(\\[\\[.*?\\]\\])(?=\\s*(?:;|var\\s|//|\\z))",
        Pattern.DOTALL).matcher(js);
    if (!m.find()) {
      throw new IllegalStateException("FEUS: variable " + name + " not found in "
          + context.getUrl());
    }
    try {
      JsonNode node = MAPPER.readTree(m.group(1));
      if (!node.isArray() || node.size() < 2) {
        throw new IllegalStateException("FEUS: variable " + name + " has no data rows in "
            + context.getUrl());
      }
      return node;
    } catch (IOException e) {
      throw new IllegalStateException("FEUS: variable " + name + " is not valid JSON in "
          + context.getUrl(), e);
    }
  }

  private static int yearFor(String geography, Map<String, Integer> years,
      RequestContext context) {
    Integer y = years.get(geography);
    if (y != null) {
      return y.intValue();
    }
    if ("Florida".equals(geography)) {
      Integer east = years.get("East Florida");
      Integer west = years.get("West Florida");
      if (east != null && east.equals(west)) {
        return east.intValue();
      }
    }
    throw new IllegalStateException("FEUS: no data year in the footnotes for '" + geography
        + "' in " + context.getUrl());
  }

  /** Region as NOAA lists it for the state; Florida spans two regions so carries none. */
  private static String regionFor(String geography, Map<String, String> regions) {
    if ("United States".equals(geography) || "Florida".equals(geography)) {
      return null;
    }
    return regions.get(geography);
  }

  private static String rowLevel(String sector) {
    String s = sector.toLowerCase(Locale.ROOT);
    if (s.startsWith("total durable")) {
      return "durable_expenditures";
    }
    if (s.startsWith("total")) {
      return "total";
    }
    return "component";
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
