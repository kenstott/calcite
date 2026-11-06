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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Unpivots a Columbia Basin Research DART "Adult Passage Annual Counts" CSV into rows.
 *
 * <p>Input: {@code adult_annual.php?...&outputFormat=csv&proj=XXX}, one project per call and
 * the project's whole history. A header line ({@code Project,Year,Chinook,Jack Chinook,...}),
 * one line per year with {@code NA} for species not counted, then free-text footnotes and a
 * citation. Only lines whose first field repeats the project name and whose second field is a
 * year are data; the footnotes are skipped. Output is one row per project x year x species
 * with a known count.
 *
 * <p>{@code state_abbrs} and {@code river} come from the project code in the request URL: the
 * dam's location is a fact of the dam, not of the response.
 */
public class FisheriesDartCountsTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern PROJ = Pattern.compile("[?&]proj=([A-Z]+)");
  private static final String NA = "NA";

  /** Project code to {river, comma-separated state abbreviations}. */
  private static final Map<String, String[]> PROJECTS = new HashMap<String, String[]>();

  static {
    PROJECTS.put("BON", new String[] {"Columbia", "OR,WA"});
    PROJECTS.put("TDA", new String[] {"Columbia", "OR,WA"});
    PROJECTS.put("JDA", new String[] {"Columbia", "OR,WA"});
    PROJECTS.put("MCN", new String[] {"Columbia", "OR,WA"});
    PROJECTS.put("PRD", new String[] {"Columbia", "WA"});
    PROJECTS.put("WAN", new String[] {"Columbia", "WA"});
    PROJECTS.put("RIS", new String[] {"Columbia", "WA"});
    PROJECTS.put("RRH", new String[] {"Columbia", "WA"});
    PROJECTS.put("WEL", new String[] {"Columbia", "WA"});
    PROJECTS.put("IHR", new String[] {"Snake", "WA"});
    PROJECTS.put("LMN", new String[] {"Snake", "WA"});
    PROJECTS.put("LGS", new String[] {"Snake", "WA"});
    PROJECTS.put("LWG", new String[] {"Snake", "WA"});
    PROJECTS.put("PRO", new String[] {"Yakima", "WA"});
    PROJECTS.put("ROZ", new String[] {"Yakima", "WA"});
    PROJECTS.put("TUM", new String[] {"Wenatchee", "WA"});
    PROJECTS.put("WFF", new String[] {"Willamette", "OR"});
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      throw new IllegalStateException("DART adult counts: empty response for "
          + context.getUrl());
    }
    Matcher m = PROJ.matcher(context.getUrl());
    if (!m.find()) {
      throw new IllegalStateException("DART adult counts: no proj= in " + context.getUrl());
    }
    String code = m.group(1);
    String[] where = PROJECTS.get(code);
    if (where == null) {
      throw new IllegalStateException("DART adult counts: project " + code
          + " has no river/state mapping in FisheriesDartCountsTransformer");
    }

    String[] lines = response.split("\r?\n");
    String[] header = lines[0].split(",", -1);
    if (header.length < 3 || !"Project".equals(header[0]) || !"Year".equals(header[1].trim())) {
      throw new IllegalStateException("DART adult counts: unexpected header '" + lines[0]
          + "' for " + context.getUrl());
    }

    ArrayNode result = MAPPER.createArrayNode();
    String projectName = null;
    for (int i = 1; i < lines.length; i++) {
      String[] f = lines[i].split(",", -1);
      if (f.length != header.length || !f[1].trim().matches("\\d{4}")) {
        continue;
      }
      if (projectName == null) {
        projectName = f[0].trim();
      }
      if (!projectName.equals(f[0].trim())) {
        continue;
      }
      int year = Integer.parseInt(f[1].trim());
      for (int c = 2; c < f.length; c++) {
        String v = f[c].trim();
        if (v.isEmpty() || NA.equals(v)) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();
        row.put("project_code", code);
        row.put("project_name", projectName);
        row.put("river", where[0]);
        row.put("state_abbrs", where[1]);
        row.put("spans_states", where[1].indexOf(',') >= 0);
        row.put("count_year", year);
        row.put("species", header[c].trim());
        row.put("adult_count", Long.parseLong(v));
        result.add(row);
      }
    }
    if (result.size() == 0) {
      throw new IllegalStateException("DART adult counts: no data rows for " + context.getUrl());
    }
    try {
      return MAPPER.writeValueAsString(result);
    } catch (java.io.IOException e) {
      throw new IllegalStateException("DART adult counts: cannot serialize " + code, e);
    }
  }
}
