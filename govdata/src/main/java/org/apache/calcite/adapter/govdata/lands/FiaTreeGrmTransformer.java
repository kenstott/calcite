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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Streaming transformer for {@code fia_tree_grm}.
 *
 * <p>Joins the per-state {@code <ST>_TREE_GRM_ESTN.csv} entry against
 * {@code <ST>_TREE.csv} to attach species code ({@code SPCD}), then aggregates the
 * per-tree FIA growth-removal-mortality estimates by
 * ({@code STATECD, INVYR, SPCD, ESTN_TYPE}).
 *
 * <p>Output (one row per group):
 * <ul>
 *   <li>{@code state_fips} — 2-char FIPS</li>
 *   <li>{@code inventory_year}</li>
 *   <li>{@code species_code} — FIA SPCD</li>
 *   <li>{@code estimation_type} — e.g. {@code AL_FOREST}, {@code GS_TIMBER}</li>
 *   <li>{@code ann_net_growth} — sum of per-tree ANN_NET_GROWTH</li>
 *   <li>{@code annual_removals} — sum of per-tree REMOVALS</li>
 *   <li>{@code annual_mortality} — sum of per-tree MORTALITY</li>
 *   <li>{@code tree_count} — number of contributing tree records</li>
 * </ul>
 *
 * <p>FIA's growth/removal/mortality model annualizes per-tree change over the
 * remeasurement period — the sums here represent the state's published annual
 * estimate by species, in the units recorded under {@code ESTN_UNITS} (typically
 * cubic feet or basal area square feet, depending on {@code ESTN_TYPE}).
 *
 * <p>Memory is dominated by the {@code TRE_CN → SPCD} lookup built from
 * {@code <ST>_TREE.csv} — bounded by the state's tree-record count.
 */
public class FiaTreeGrmTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiaTreeGrmTransformer.class);

  private static final String TREE_ENTRY = "TREE.csv";
  private static final String GRM_ENTRY = "TREE_GRM_ESTN.csv";

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String state = requireState(context);
    String stateFips = FiaLookups.stateFipsForAbbr(state);

    Map<String, Integer> treSpcd = readTreeSpeciesMap(state);
    Map<String, double[]> groups = streamGrmAggregates(state, treSpcd);

    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>(groups.size());
    for (Map.Entry<String, double[]> e : groups.entrySet()) {
      String[] parts = e.getKey().split("\\|", -1);
      double[] acc = e.getValue();
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("state_fips", stateFips);
      row.put("inventory_year", Integer.parseInt(parts[0]));
      row.put("species_code", Integer.parseInt(parts[1]));
      row.put("estimation_type", "null".equals(parts[2]) ? null : parts[2]);
      row.put("ann_net_growth", acc[0]);
      row.put("annual_removals", acc[1]);
      row.put("annual_mortality", acc[2]);
      row.put("tree_count", (long) acc[3]);
      result.add(row);
    }

    LOGGER.info("fia_tree_grm[{}]: tree_map_size={} groups={}",
        state, treSpcd.size(), result.size());
    return result.iterator();
  }

  /** Builds a TRE_CN → SPCD lookup by streaming TREE.csv. */
  private Map<String, Integer> readTreeSpeciesMap(String state) throws IOException {
    Map<String, Integer> map = new HashMap<String, Integer>();
    try (FiaStateArchive.EntryHandle entry = FiaStateArchive.openEntry(state, TREE_ENTRY);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(entry.stream, StandardCharsets.UTF_8))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return map;
      }
      String[] hdr = headerLine.split(",", -1);
      int idxCn = indexOf(hdr, "CN");
      int idxSpcd = indexOf(hdr, "SPCD");
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] cols = line.split(",", -1);
        String cn = strAt(cols, idxCn);
        int spcd = intAt(cols, idxSpcd);
        if (!cn.isEmpty() && spcd > 0) {
          map.put(cn, spcd);
        }
      }
    }
    return map;
  }

  private Map<String, double[]> streamGrmAggregates(String state, Map<String, Integer> treSpcd)
      throws IOException {
    Map<String, double[]> groups = new LinkedHashMap<String, double[]>();
    try (FiaStateArchive.EntryHandle entry = FiaStateArchive.openEntry(state, GRM_ENTRY);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(entry.stream, StandardCharsets.UTF_8))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return groups;
      }
      String[] hdr = headerLine.split(",", -1);
      int idxInvyr = indexOf(hdr, "INVYR");
      int idxTreCn = indexOf(hdr, "TRE_CN");
      int idxEstnType = indexOf(hdr, "ESTN_TYPE");
      int idxAnnGrow = indexOf(hdr, "ANN_NET_GROWTH");
      int idxRem = indexOf(hdr, "REMOVALS");
      int idxMort = indexOf(hdr, "MORTALITY");

      long rowsRead = 0;
      long rowsKept = 0;
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        rowsRead++;
        String[] cols = line.split(",", -1);
        String treCn = strAt(cols, idxTreCn);
        Integer spcd = treSpcd.get(treCn);
        if (spcd == null) {
          continue;
        }
        int invyr = intAt(cols, idxInvyr);
        if (invyr <= 0) {
          continue;
        }
        String estnType = strAt(cols, idxEstnType);
        if (estnType.isEmpty()) {
          estnType = "null";
        }
        rowsKept++;
        String key = invyr + "|" + spcd + "|" + estnType;
        double[] acc = groups.get(key);
        if (acc == null) {
          acc = new double[4];
          groups.put(key, acc);
        }
        acc[0] += doubleAt(cols, idxAnnGrow);
        acc[1] += doubleAt(cols, idxRem);
        acc[2] += doubleAt(cols, idxMort);
        acc[3] += 1.0;
      }
      LOGGER.info("fia_tree_grm[{}]: GRM rows read={} kept={}", state, rowsRead, rowsKept);
    }
    return groups;
  }

  private static String requireState(RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String state = dims != null ? dims.get("state") : null;
    if (state == null || state.isEmpty()) {
      throw new IllegalStateException(
          "FiaTreeGrmTransformer requires 'state' dimension in request context");
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
