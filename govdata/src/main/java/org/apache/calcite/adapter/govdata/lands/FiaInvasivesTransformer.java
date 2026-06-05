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
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Streaming transformer for {@code fia_invasives}.
 *
 * <p>Reads the per-state {@code <ST>_INVASIVE_SUBPLOT_SPP.csv} entry, which
 * records invasive plant species observations on FIA subplots with a percent
 * cover estimate. Aggregates by ({@code STATECD, INVYR, VEG_SPCD}) yielding
 * average cover percent and the number of contributing subplot observations.
 */
public class FiaInvasivesTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiaInvasivesTransformer.class);

  private static final String INVASIVE_ENTRY = "INVASIVE_SUBPLOT_SPP.csv";

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String state = requireState(context);
    String stateFips = FiaLookups.stateFipsForAbbr(state);

    // acc[0] = sum(cover_pct), acc[1] = subplot_count
    Map<String, double[]> groups = new LinkedHashMap<String, double[]>();
    long rowsRead = 0;
    try (FiaStateArchive.EntryHandle entry = FiaStateArchive.openEntry(state, INVASIVE_ENTRY);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(entry.stream, StandardCharsets.UTF_8))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        LOGGER.warn("fia_invasives: empty {}_{}", state, INVASIVE_ENTRY);
        return new ArrayList<Map<String, Object>>().iterator();
      }
      String[] hdr = headerLine.split(",", -1);
      int idxInvyr = indexOf(hdr, "INVYR");
      int idxSpcd = indexOf(hdr, "VEG_SPCD");
      int idxCover = indexOf(hdr, "COVER_PCT");

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        rowsRead++;
        String[] cols = line.split(",", -1);
        int invyr = intAt(cols, idxInvyr);
        String spcd = strAt(cols, idxSpcd);
        if (invyr <= 0 || spcd.isEmpty()) {
          continue;
        }
        double cover = doubleAt(cols, idxCover);
        String key = invyr + "|" + spcd;
        double[] acc = groups.get(key);
        if (acc == null) {
          acc = new double[2];
          groups.put(key, acc);
        }
        acc[0] += cover;
        acc[1] += 1.0;
      }
    }

    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>(groups.size());
    for (Map.Entry<String, double[]> e : groups.entrySet()) {
      String[] parts = e.getKey().split("\\|", -1);
      double[] acc = e.getValue();
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("state_fips", stateFips);
      row.put("inventory_year", Integer.parseInt(parts[0]));
      row.put("species_code", parts[1]);
      Double avg = acc[1] > 0 ? acc[0] / acc[1] : null;
      row.put("cover_pct_avg", avg);
      row.put("subplot_count", (long) acc[1]);
      result.add(row);
    }

    LOGGER.info("fia_invasives[{}]: rows read={} groups={}", state, rowsRead, result.size());
    return result.iterator();
  }

  private static String requireState(RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String state = dims != null ? dims.get("state") : null;
    if (state == null || state.isEmpty()) {
      throw new IllegalStateException(
          "FiaInvasivesTransformer requires 'state' dimension in request context");
    }
    return state;
  }

  private static int indexOf(String[] hdr, String name) {
    for (int i = 0; i < hdr.length; i++) {
      if (name.equals(hdr[i].trim())) {
        return i;
      }
    }
    return -1;
  }

  private static String strAt(String[] cols, int idx) {
    if (idx < 0 || idx >= cols.length) {
      return "";
    }
    return cols[idx].trim();
  }

  private static int intAt(String[] cols, int idx) {
    String v = strAt(cols, idx);
    if (v.isEmpty()) {
      return 0;
    }
    try {
      return (int) Double.parseDouble(v);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static double doubleAt(String[] cols, int idx) {
    String v = strAt(cols, idx);
    if (v.isEmpty()) {
      return 0.0;
    }
    try {
      return Double.parseDouble(v);
    } catch (NumberFormatException e) {
      return 0.0;
    }
  }
}
