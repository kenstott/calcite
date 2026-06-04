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
 * Streaming transformer for {@code fia_seedlings}.
 *
 * <p>Reads the per-state {@code <ST>_SEEDLING.csv} entry and aggregates seedling
 * counts and trees-per-acre estimates by ({@code STATECD, INVYR, SPCD}).
 *
 * <p>FIA records seedling occurrence on a microplot (the central small subplot of the
 * national sampling design); {@code TREECOUNT} is the per-microplot count of seedlings
 * of a species observed at a condition, and {@code TPA_UNADJ} is the unadjusted
 * trees-per-acre conversion that FIA derives from microplot area. Sums of {@code TPA_UNADJ}
 * across plots in a state-year-species group approximate the species' regeneration
 * intensity in seedlings per acre on the sampled forest land.
 */
public class FiaSeedlingsTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiaSeedlingsTransformer.class);

  private static final String SEEDLING_ENTRY = "SEEDLING.csv";

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String state = requireState(context);
    String stateFips = FiaLookups.stateFipsForAbbr(state);

    Map<String, double[]> groups = new LinkedHashMap<String, double[]>();
    long rowsRead = 0;
    try (FiaStateArchive.EntryHandle entry =
             FiaStateArchive.openEntry(state, SEEDLING_ENTRY);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(entry.stream, StandardCharsets.UTF_8))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        LOGGER.warn("fia_seedlings: empty {}_{} in FIA archive", state, SEEDLING_ENTRY);
        return new ArrayList<Map<String, Object>>().iterator();
      }
      String[] hdr = headerLine.split(",", -1);
      int idxInvyr = indexOf(hdr, "INVYR");
      int idxSpcd = indexOf(hdr, "SPCD");
      int idxCount = indexOf(hdr, "TREECOUNT");
      int idxCountCalc = indexOf(hdr, "TREECOUNT_CALC");
      int idxTpa = indexOf(hdr, "TPA_UNADJ");

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        rowsRead++;
        String[] cols = line.split(",", -1);
        int invyr = intAt(cols, idxInvyr);
        int spcd = intAt(cols, idxSpcd);
        if (invyr <= 0 || spcd <= 0) {
          continue;
        }
        int treecount = intAt(cols, idxCount);
        if (treecount <= 0) {
          treecount = intAt(cols, idxCountCalc);
        }
        double tpa = doubleAt(cols, idxTpa);
        String key = invyr + "|" + spcd;
        double[] acc = groups.get(key);
        if (acc == null) {
          acc = new double[3];
          groups.put(key, acc);
        }
        acc[0] += treecount;
        acc[1] += tpa;
        acc[2] += 1.0;
      }
    }

    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>(groups.size());
    for (Map.Entry<String, double[]> e : groups.entrySet()) {
      String[] parts = e.getKey().split("\\|", -1);
      double[] acc = e.getValue();
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("state_fips", stateFips);
      row.put("inventory_year", Integer.parseInt(parts[0]));
      row.put("species_code", Integer.parseInt(parts[1]));
      row.put("seedling_count", (long) acc[0]);
      row.put("seedlings_per_acre", acc[1]);
      row.put("record_count", (long) acc[2]);
      result.add(row);
    }

    LOGGER.info("fia_seedlings[{}]: rows read={} groups={}", state, rowsRead, result.size());
    return result.iterator();
  }

  private static String requireState(RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String state = dims != null ? dims.get("state") : null;
    if (state == null || state.isEmpty()) {
      throw new IllegalStateException(
          "FiaSeedlingsTransformer requires 'state' dimension in request context");
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
