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

import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns a SacPAS "CDFW GrandTab Adult Escapement" CSV into rows.
 *
 * <p>Input: {@code grandtab_graph.php?outputFormat=csv&species=Chinook:<run>&type=<origin>...}
 * for the whole Sacramento River System. A header line
 * ({@code End Year of Monitoring Period,Population Estimate,3-Year Rolling Geometric Mean}),
 * one line per year ({@code 2025 *,235769,160808}, where the asterisk marks a preliminary
 * value; the estimate is blank for years with none and the mean is blank when it cannot be
 * computed), then notes and a citation. Only lines whose first field starts with a four-digit
 * year are data. Run and origin come from the request URL.
 */
public class FisheriesGrandTabTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern SPECIES = Pattern.compile("[?&]species=Chinook:([^&]+)");
  private static final Pattern TYPE = Pattern.compile("[?&]type=([^&]+)");
  private static final Pattern YEAR = Pattern.compile("^(\\d{4})( \\*)?$");
  private static final String HEADER_START = "End Year of Monitoring Period";

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      throw new IllegalStateException("GrandTab: empty response for " + context.getUrl());
    }
    String run = urlParam(SPECIES, context.getUrl());
    String origin = urlParam(TYPE, context.getUrl());

    String[] lines = response.split("\r?\n");
    int start = -1;
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].startsWith(HEADER_START)) {
        start = i;
        break;
      }
    }
    if (start < 0) {
      throw new IllegalStateException("GrandTab: no CSV header in response for "
          + context.getUrl() + " (starts: " + lines[0] + ")");
    }

    ArrayNode result = MAPPER.createArrayNode();
    for (int i = start + 1; i < lines.length; i++) {
      String[] f = lines[i].split(",", -1);
      if (f.length != 3) {
        continue;
      }
      Matcher y = YEAR.matcher(f[0].trim());
      if (!y.matches()) {
        continue;
      }
      // A blank estimate means the run/origin had no estimate that year: leave the year out
      // rather than invent a zero.
      if (f[1].trim().isEmpty()) {
        continue;
      }
      ObjectNode row = MAPPER.createObjectNode();
      row.put("run_name", run);
      row.put("origin_type", origin);
      row.put("state_abbr", "CA");
      row.put("count_year", Integer.parseInt(y.group(1)));
      row.put("escapement", Long.parseLong(f[1].trim()));
      row.put("is_preliminary", y.group(2) != null);
      if (f[2].trim().isEmpty()) {
        // The mean needs this and the two previous years' estimates; SacPAS leaves it blank
        // when any is missing.
        row.putNull("rolling_3yr_geomean");
      } else {
        row.put("rolling_3yr_geomean", Long.parseLong(f[2].trim()));
      }
      result.add(row);
    }
    if (result.size() == 0) {
      throw new IllegalStateException("GrandTab: no data rows for " + context.getUrl());
    }
    try {
      return MAPPER.writeValueAsString(result);
    } catch (java.io.IOException e) {
      throw new IllegalStateException("GrandTab: cannot serialize " + run + "/" + origin, e);
    }
  }

  private static String urlParam(Pattern p, String url) {
    Matcher m = p.matcher(url);
    if (!m.find()) {
      throw new IllegalStateException("GrandTab: " + p.pattern() + " not found in " + url);
    }
    try {
      return URLDecoder.decode(m.group(1), "UTF-8");
    } catch (java.io.UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }
}
