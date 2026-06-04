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
 * Streaming transformer for the {@code forest_inventory} table.
 *
 * <p>Reads the per-state {@code <ST>_COND.csv} entry from the FIA datamart archive for
 * the state in the request context (see {@link FiaStateArchive}), filters to
 * {@code COND_STATUS_CD = 1} (accessible forest land), and aggregates by
 * {@code (INVYR, forest_type_group, ownership_class)}. Each ETL batch covers one
 * state — fan-out across states is driven by the {@code state} dimension declared in
 * the YAML.
 *
 * <p>{@code basal_area_sqft} is a {@code CONDPROP_UNADJ}-weighted average of
 * {@code BALIVE}. Land area, live volume, carbon stock, and trees per acre live in
 * {@code forest_metrics} since they require TREE-table data.
 *
 * <p>Memory is bounded by the number of (year × type-group × ownership) groups per
 * state — roughly hundreds — independent of the per-state COND CSV size.
 */
public class FiaDatamartTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiaDatamartTransformer.class);

  private static final String COND_ENTRY = "COND.csv";

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String state = requireState(context);
    String stateFips = FiaLookups.stateFipsForAbbr(state);

    Map<String, double[]> agg = new LinkedHashMap<String, double[]>();
    long rowsRead = 0;
    long rowsKept = 0;
    try (FiaStateArchive.EntryHandle entry = FiaStateArchive.openEntry(state, COND_ENTRY);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(entry.stream, StandardCharsets.UTF_8))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        LOGGER.warn("forest_inventory: empty {}_{} in FIA archive", state, COND_ENTRY);
        return new ArrayList<Map<String, Object>>().iterator();
      }
      String[] hdr = headerLine.split(",", -1);
      int idxInvyr = indexOf(hdr, "INVYR");
      int idxCondStatus = indexOf(hdr, "COND_STATUS_CD");
      int idxOwngrpcd = indexOf(hdr, "OWNGRPCD");
      int idxFortypcd = indexOf(hdr, "FORTYPCD");
      int idxCondprop = indexOf(hdr, "CONDPROP_UNADJ");
      int idxBalive = indexOf(hdr, "BALIVE");

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        rowsRead++;
        String[] cols = line.split(",", -1);
        if (intAt(cols, idxCondStatus) != 1) {
          continue;
        }
        rowsKept++;
        int invyr = intAt(cols, idxInvyr);
        String typGrp = FiaLookups.resolveTypGrp(intAt(cols, idxFortypcd));
        String ownGrp = FiaLookups.resolveOwnGrp(intAt(cols, idxOwngrpcd));
        double condprop = doubleAt(cols, idxCondprop);
        double balive = doubleAt(cols, idxBalive);

        String key = invyr + "|" + typGrp + "|" + ownGrp;
        double[] acc = agg.get(key);
        if (acc == null) {
          acc = new double[2];
          agg.put(key, acc);
        }
        acc[0] += condprop;
        acc[1] += balive * condprop;
      }
    }

    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>(agg.size());
    for (Map.Entry<String, double[]> e : agg.entrySet()) {
      String[] parts = e.getKey().split("\\|", -1);
      double[] acc = e.getValue();
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("inventory_year", Integer.parseInt(parts[0]));
      row.put("state_fips", stateFips);
      row.put("forest_type_group", "null".equals(parts[1]) ? null : parts[1]);
      row.put("ownership_class", "null".equals(parts[2]) ? null : parts[2]);
      row.put("basal_area_sqft", acc[0] > 0.0 ? acc[1] / acc[0] : null);
      result.add(row);
    }

    LOGGER.info("forest_inventory[{}]: rows read={} kept={} groups={}",
        state, rowsRead, rowsKept, result.size());
    return result.iterator();
  }

  private static String requireState(RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String state = dims != null ? dims.get("state") : null;
    if (state == null || state.isEmpty()) {
      throw new IllegalStateException(
          "FiaDatamartTransformer requires 'state' dimension in request context");
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

  private static int intAt(String[] cols, int idx) {
    if (idx < 0 || idx >= cols.length) {
      return 0;
    }
    String v = cols[idx].trim();
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
    if (idx < 0 || idx >= cols.length) {
      return 0.0;
    }
    String v = cols[idx].trim();
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
