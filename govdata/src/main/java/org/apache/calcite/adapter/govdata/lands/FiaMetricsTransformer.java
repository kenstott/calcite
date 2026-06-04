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
 * Streaming transformer for the {@code forest_metrics} table.
 *
 * <p>Joins the per-state {@code <ST>_TREE.csv} entry with {@code <ST>_COND.csv} from
 * the FIA datamart archive for the state in the request context (see
 * {@link FiaStateArchive}) to produce {@code CONDPROP_UNADJ}-weighted estimates of
 * trees per acre, live cubic-foot volume per acre, and above-ground carbon stock
 * (tons per acre) by inventory year × forest type group × ownership class. Only live
 * trees ({@code STATUSCD = 1}) in accessible forest conditions
 * ({@code COND_STATUS_CD = 1}) are included.
 *
 * <p>Both ZIP entries are decompressed line-by-line via streaming — per-state TREE
 * data is at most a few hundred MB and the {@code condMap} lookup
 * (keyed by {@code PLT_CN|CONDID}) is bounded by the state's condition count
 * (typically tens of thousands).
 */
public class FiaMetricsTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiaMetricsTransformer.class);

  private static final String COND_ENTRY = "COND.csv";
  private static final String TREE_ENTRY = "TREE.csv";

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String state = requireState(context);
    String stateFips = FiaLookups.stateFipsForAbbr(state);

    Map<String, CondEntry> condMap = readCondMap(state);

    Map<String, double[]> groups = new LinkedHashMap<String, double[]>();
    for (CondEntry cond : condMap.values()) {
      String key = groupKey(cond.invyr, cond.typGrp, cond.ownGrp);
      double[] acc = groups.get(key);
      if (acc == null) {
        acc = new double[4];
        groups.put(key, acc);
      }
      acc[0] += cond.condprop;
    }

    streamTreeMetrics(state, condMap, groups);

    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
    for (Map.Entry<String, double[]> entry : groups.entrySet()) {
      double[] acc = entry.getValue();
      double condpropSum = acc[0];
      if (condpropSum <= 0.0) {
        continue;
      }
      String[] parts = entry.getKey().split("\\|", -1);
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("inventory_year", Integer.parseInt(parts[0]));
      row.put("state_fips", stateFips);
      row.put("forest_type_group", "null".equals(parts[1]) ? null : parts[1]);
      row.put("ownership_class", "null".equals(parts[2]) ? null : parts[2]);
      row.put("trees_per_acre", acc[1] / condpropSum);
      row.put("live_volume_cuft", acc[2] / condpropSum);
      row.put("carbon_stock_tons", acc[3] / condpropSum);
      result.add(row);
    }

    LOGGER.info("forest_metrics[{}]: conditions={} groups={}",
        state, condMap.size(), result.size());
    return result.iterator();
  }

  private Map<String, CondEntry> readCondMap(String state) throws IOException {
    Map<String, CondEntry> map = new HashMap<String, CondEntry>();
    try (FiaStateArchive.EntryHandle entry = FiaStateArchive.openEntry(state, COND_ENTRY);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(entry.stream, StandardCharsets.UTF_8))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        LOGGER.warn("forest_metrics: empty {}_{} in FIA archive", state, COND_ENTRY);
        return map;
      }
      String[] hdr = headerLine.split(",", -1);
      int idxPltCn = indexOf(hdr, "PLT_CN");
      int idxCondId = indexOf(hdr, "CONDID");
      int idxInvyr = indexOf(hdr, "INVYR");
      int idxCondStatus = indexOf(hdr, "COND_STATUS_CD");
      int idxOwngrpcd = indexOf(hdr, "OWNGRPCD");
      int idxFortypcd = indexOf(hdr, "FORTYPCD");
      int idxCondprop = indexOf(hdr, "CONDPROP_UNADJ");
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] cols = line.split(",", -1);
        if (intAt(cols, idxCondStatus) != 1) {
          continue;
        }
        String pltCn = strAt(cols, idxPltCn);
        String condId = strAt(cols, idxCondId);
        int invyr = intAt(cols, idxInvyr);
        String typGrp = FiaLookups.resolveTypGrp(intAt(cols, idxFortypcd));
        String ownGrp = FiaLookups.resolveOwnGrp(intAt(cols, idxOwngrpcd));
        double condprop = doubleAt(cols, idxCondprop);
        map.put(pltCn + "|" + condId,
            new CondEntry(invyr, typGrp, ownGrp, condprop));
      }
    }
    return map;
  }

  private void streamTreeMetrics(String state, Map<String, CondEntry> condMap,
      Map<String, double[]> groups) throws IOException {
    try (FiaStateArchive.EntryHandle entry = FiaStateArchive.openEntry(state, TREE_ENTRY);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(entry.stream, StandardCharsets.UTF_8))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        LOGGER.warn("forest_metrics: empty {}_{} in FIA archive", state, TREE_ENTRY);
        return;
      }
      String[] hdr = headerLine.split(",", -1);
      int idxPltCn = indexOf(hdr, "PLT_CN");
      int idxCondId = indexOf(hdr, "CONDID");
      int idxStatuscd = indexOf(hdr, "STATUSCD");
      int idxTpa = indexOf(hdr, "TPA_UNADJ");
      int idxVol = indexOf(hdr, "VOLCFNET");
      int idxCarbon = indexOf(hdr, "CARBON_AG");

      long rowsRead = 0;
      long rowsKept = 0;
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        rowsRead++;
        String[] cols = line.split(",", -1);
        if (intAt(cols, idxStatuscd) != 1) {
          continue;
        }
        CondEntry cond = condMap.get(strAt(cols, idxPltCn) + "|" + strAt(cols, idxCondId));
        if (cond == null) {
          continue;
        }
        double tpa = doubleAt(cols, idxTpa);
        double vol = doubleAt(cols, idxVol);
        double carbon = doubleAt(cols, idxCarbon);
        double condprop = cond.condprop;
        rowsKept++;
        double[] acc = groups.get(groupKey(cond.invyr, cond.typGrp, cond.ownGrp));
        if (acc == null) {
          continue;
        }
        acc[1] += tpa * condprop;
        acc[2] += tpa * vol * condprop;
        acc[3] += tpa * carbon * condprop / 2000.0; // lbs → tons
      }
      LOGGER.info("forest_metrics[{}]: tree rows read={} kept={}", state, rowsRead, rowsKept);
    }
  }

  private static String requireState(RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String state = dims != null ? dims.get("state") : null;
    if (state == null || state.isEmpty()) {
      throw new IllegalStateException(
          "FiaMetricsTransformer requires 'state' dimension in request context");
    }
    return state;
  }

  private static String groupKey(int invyr, String typGrp, String ownGrp) {
    return invyr + "|" + typGrp + "|" + ownGrp;
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

  private static final class CondEntry {
    final int invyr;
    final String typGrp;
    final String ownGrp;
    final double condprop;

    CondEntry(int invyr, String typGrp, String ownGrp, double condprop) {
      this.invyr = invyr;
      this.typGrp = typGrp;
      this.ownGrp = ownGrp;
      this.condprop = condprop;
    }
  }
}
