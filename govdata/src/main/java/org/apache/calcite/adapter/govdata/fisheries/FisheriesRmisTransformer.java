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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns Regional Mark Information System (RMIS, RMPC/PSMFC) PSC-format v5.0 CSV files into rows.
 *
 * <p>Two file kinds, chosen from the request URL: {@code RL050_<agency>_FULLSET.csv} (release
 * records, one per tag code / untagged release group) and {@code LC050_ALL_FULLSET.csv}
 * (location lookup). Both are quoted CSV with a header line whose first field is
 * {@code record_code}. The state of a location is the first character of its location code
 * (1 AK, 2 BC, 3 WA, 4 ID, 5 OR, 6 CA, 7 high seas), which matches the reporting agencies in the data.
 */
public class FisheriesRmisTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Map<String, String> STATES = new HashMap<String, String>();
  private static final Map<String, String> SPECIES = new HashMap<String, String>();
  private static final Map<String, String> STAGES = new HashMap<String, String>();
  private static final Map<String, String> REARING = new HashMap<String, String>();

  static {
    STATES.put("1", "AK");
    STATES.put("2", "BC");
    STATES.put("3", "WA");
    STATES.put("4", "ID");
    STATES.put("5", "OR");
    STATES.put("6", "CA");
    STATES.put("7", "HS");
    SPECIES.put("1", "Chinook");
    SPECIES.put("2", "Coho");
    SPECIES.put("3", "Steelhead");
    SPECIES.put("4", "Sockeye");
    SPECIES.put("5", "Chum");
    SPECIES.put("6", "Pink");
    SPECIES.put("7", "Masu");
    SPECIES.put("8", "Cutthroat");
    SPECIES.put("9", "Atlantic Salmon");
    STAGES.put("Z", "Zygote");
    STAGES.put("E", "Emergent fry");
    STAGES.put("F", "Fed fry");
    STAGES.put("G", "Fingerling");
    STAGES.put("V", "Advanced fingerling");
    STAGES.put("Y", "Yearling");
    STAGES.put("P", "Pre-smolt");
    STAGES.put("S", "Smolt");
    STAGES.put("A", "Adult");
    STAGES.put("M", "Multiple");
    REARING.put("H", "Hatchery");
    REARING.put("W", "Wild");
    REARING.put("M", "Mixed");
    REARING.put("U", "Unknown");
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      throw new IllegalStateException("RMIS: empty response for " + context.getUrl());
    }
    boolean releases = context.getUrl().contains("/RL050_");
    if (!releases && !context.getUrl().contains("/LC050_")) {
      throw new IllegalStateException("RMIS: unexpected file in " + context.getUrl());
    }
    List<String[]> records = parseCsv(response);
    if (records.isEmpty() || !"record_code".equals(records.get(0)[0].trim())) {
      throw new IllegalStateException("RMIS: no record_code header in " + context.getUrl());
    }
    Map<String, Integer> idx = new HashMap<String, Integer>();
    String[] header = records.get(0);
    for (int i = 0; i < header.length; i++) {
      idx.put(header[i].trim(), i);
    }
    ArrayNode result = MAPPER.createArrayNode();
    for (int r = 1; r < records.size(); r++) {
      String[] f = records.get(r);
      if (f.length != header.length) {
        throw new IllegalStateException("RMIS: row " + r + " has " + f.length + " fields, expected "
            + header.length + " in " + context.getUrl());
      }
      result.add(releases ? releaseRow(f, idx) : locationRow(f, idx));
    }
    if (result.size() == 0) {
      throw new IllegalStateException("RMIS: no data rows for " + context.getUrl());
    }
    try {
      return MAPPER.writeValueAsString(result);
    } catch (java.io.IOException e) {
      throw new IllegalStateException("RMIS: cannot serialize " + context.getUrl(), e);
    }
  }

  private static ObjectNode releaseRow(String[] f, Map<String, Integer> idx) {
    ObjectNode row = MAPPER.createObjectNode();
    row.put("record_type", get(f, idx, "record_code").equals("T") ? "tagged" : "untagged");
    row.put("reporting_agency", get(f, idx, "reporting_agency"));
    row.put("release_agency", get(f, idx, "release_agency"));
    row.put("tag_code_or_release_id", get(f, idx, "tag_code_or_release_id"));
    String species = get(f, idx, "species");
    row.put("species_code", species);
    row.put("species", lookup(SPECIES, species, "species"));
    putText(row, "run_code", get(f, idx, "run"));
    putInt(row, "brood_year", get(f, idx, "brood_year"));
    putDate(row, "first_release_date", get(f, idx, "first_release_date"));
    putDate(row, "last_release_date", get(f, idx, "last_release_date"));
    String last = get(f, idx, "last_release_date");
    if (last.length() >= 4) {
      row.put("release_year", Integer.parseInt(last.substring(0, 4)));
    } else {
      row.putNull("release_year");
    }
    String relLoc = get(f, idx, "release_location_code");
    putText(row, "release_location_code", relLoc);
    putText(row, "hatchery_location_code", get(f, idx, "hatchery_location_code"));
    putText(row, "stock_location_code", get(f, idx, "stock_location_code"));
    if (relLoc.isEmpty()) {
      row.putNull("state_abbr");
    } else {
      row.put("state_abbr", lookup(STATES, relLoc.substring(0, 1), "release_location_code state"));
    }
    String stage = get(f, idx, "release_stage");
    putText(row, "release_stage_code", stage);
    if (stage.isEmpty()) {
      row.putNull("release_stage");
    } else {
      row.put("release_stage", lookup(STAGES, stage, "release_stage"));
    }
    String rearing = get(f, idx, "rearing_type");
    if (rearing.isEmpty()) {
      row.putNull("rearing_type");
    } else {
      row.put("rearing_type", lookup(REARING, rearing, "rearing_type"));
    }
    putText(row, "broodstock_origin_type", get(f, idx, "broodstock_origin_type"));
    putText(row, "release_strategy", get(f, idx, "release_strategy"));
    putDouble(row, "avg_weight_g", get(f, idx, "avg_weight"));
    putDouble(row, "avg_length_mm", get(f, idx, "avg_length"));
    putText(row, "cwt_1st_mark", get(f, idx, "cwt_1st_mark"));
    Long c1 = count(f, idx, "cwt_1st_mark_count");
    Long c2 = count(f, idx, "cwt_2nd_mark_count");
    Long n1 = count(f, idx, "non_cwt_1st_mark_count");
    Long n2 = count(f, idx, "non_cwt_2nd_mark_count");
    putLong(row, "cwt_tagged_count", sum(c1, c2));
    putLong(row, "non_cwt_1st_mark_count", n1);
    putLong(row, "non_cwt_2nd_mark_count", n2);
    putText(row, "non_cwt_1st_mark", get(f, idx, "non_cwt_1st_mark"));
    putLong(row, "total_released", sum(sum(c1, c2), sum(n1, n2)));
    return row;
  }

  private static ObjectNode locationRow(String[] f, Map<String, Integer> idx) {
    ObjectNode row = MAPPER.createObjectNode();
    String code = get(f, idx, "location_code");
    row.put("location_code", code);
    row.put("location_type", get(f, idx, "location_type"));
    row.put("reporting_agency", get(f, idx, "reporting_agency"));
    putText(row, "name", get(f, idx, "name"));
    if (code.isEmpty()) {
      row.putNull("state_abbr");
    } else {
      row.put("state_abbr", lookup(STATES, code.substring(0, 1), "location_code state"));
    }
    putDouble(row, "latitude", get(f, idx, "latitude"));
    putDouble(row, "longitude", get(f, idx, "longitude"));
    putText(row, "psc_basin", get(f, idx, "psc_basin"));
    putText(row, "psc_region", get(f, idx, "psc_region"));
    putText(row, "description", get(f, idx, "description"));
    return row;
  }

  private static String lookup(Map<String, String> map, String code, String what) {
    String v = map.get(code);
    if (v == null) {
      throw new IllegalStateException("RMIS: unknown " + what + " code '" + code + "'");
    }
    return v;
  }

  private static String get(String[] f, Map<String, Integer> idx, String name) {
    Integer i = idx.get(name);
    if (i == null) {
      throw new IllegalStateException("RMIS: column " + name + " missing from header");
    }
    return f[i].trim();
  }

  private static Long count(String[] f, Map<String, Integer> idx, String name) {
    String v = get(f, idx, name);
    return v.isEmpty() ? null : Long.valueOf(v);
  }

  /** Sum of the reported counts; null when none of them was reported. */
  private static Long sum(Long a, Long b) {
    if (a == null) {
      return b;
    }
    return b == null ? a : Long.valueOf(a + b);
  }

  private static void putLong(ObjectNode row, String name, Long v) {
    if (v == null) {
      row.putNull(name);
    } else {
      row.put(name, v.longValue());
    }
  }

  private static void putText(ObjectNode row, String name, String v) {
    if (v.isEmpty()) {
      row.putNull(name);
    } else {
      row.put(name, v);
    }
  }

  private static void putInt(ObjectNode row, String name, String v) {
    if (v.isEmpty()) {
      row.putNull(name);
    } else {
      row.put(name, Integer.parseInt(v));
    }
  }

  private static void putDouble(ObjectNode row, String name, String v) {
    if (v.isEmpty()) {
      row.putNull(name);
    } else {
      row.put(name, Double.parseDouble(v));
    }
  }

  private static void putDate(ObjectNode row, String name, String v) {
    if (v.length() != 8) {
      row.putNull(name);
    } else {
      row.put(name, v.substring(0, 4) + "-" + v.substring(4, 6) + "-" + v.substring(6, 8));
    }
  }

  /** Quote-aware CSV parse (fields may hold commas and newlines inside double quotes). */
  static List<String[]> parseCsv(String text) {
    List<String[]> out = new ArrayList<String[]>();
    List<String> fields = new ArrayList<String>();
    StringBuilder sb = new StringBuilder();
    boolean quoted = false;
    int n = text.length();
    for (int i = 0; i < n; i++) {
      char c = text.charAt(i);
      if (quoted) {
        if (c == '"') {
          if (i + 1 < n && text.charAt(i + 1) == '"') {
            sb.append('"');
            i++;
          } else {
            quoted = false;
          }
        } else {
          sb.append(c);
        }
      } else if (c == '"') {
        quoted = true;
      } else if (c == ',') {
        fields.add(sb.toString());
        sb.setLength(0);
      } else if (c == '\n' || c == '\r') {
        if (c == '\r' && i + 1 < n && text.charAt(i + 1) == '\n') {
          i++;
        }
        fields.add(sb.toString());
        sb.setLength(0);
        out.add(fields.toArray(new String[0]));
        fields = new ArrayList<String>();
      } else {
        sb.append(c);
      }
    }
    if (sb.length() > 0 || !fields.isEmpty()) {
      fields.add(sb.toString());
      out.add(fields.toArray(new String[0]));
    }
    return out;
  }
}
