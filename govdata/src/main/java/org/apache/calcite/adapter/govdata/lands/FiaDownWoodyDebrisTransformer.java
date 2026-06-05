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
 * Streaming transformer for {@code fia_down_woody_debris}.
 *
 * <p>Reads the per-state {@code <ST>_COND_DWM_CALC.csv} entry, which contains
 * pre-computed condition-level dry biomass tons for coarse woody debris (CWD),
 * fine woody debris (FWD, the sum of small + medium + large), and forest-floor
 * duff + litter biomass. Aggregates by ({@code STATECD, INVYR, debris_category})
 * where {@code debris_category} ∈ {{@code coarse_woody}, {@code fine_woody},
 * {@code duff_litter}}.
 *
 * <p>{@code tons_per_acre} is the area-weighted mean of the FIA-derived
 * per-condition biomass field for that category:
 * <ul>
 *   <li>{@code coarse_woody}: {@code CWD_DRYBIO_ADJ} weighted by {@code CONDPROP_CWD}</li>
 *   <li>{@code fine_woody}: {@code FWD_SM_DRYBIO_ADJ + FWD_MD_DRYBIO_ADJ + FWD_LG_DRYBIO_ADJ}
 *       weighted by the matching condition-proportions</li>
 *   <li>{@code duff_litter}: {@code DUFF_BIOMASS + LITTER_BIOMASS} weighted by
 *       {@code CONDPROP_DUFF}</li>
 * </ul>
 */
public class FiaDownWoodyDebrisTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiaDownWoodyDebrisTransformer.class);

  private static final String DWM_ENTRY = "COND_DWM_CALC.csv";

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String state = requireState(context);
    String stateFips = FiaLookups.stateFipsForAbbr(state);

    // acc[0] = sum(weight*tons), acc[1] = sum(weight), acc[2] = record_count
    Map<String, double[]> groups = new LinkedHashMap<String, double[]>();
    long rowsRead = 0;
    try (FiaStateArchive.EntryHandle entry = FiaStateArchive.openEntry(state, DWM_ENTRY);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(entry.stream, StandardCharsets.UTF_8))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        LOGGER.warn("fia_down_woody_debris: empty {}_{}", state, DWM_ENTRY);
        return new ArrayList<Map<String, Object>>().iterator();
      }
      String[] hdr = headerLine.split(",", -1);
      int idxInvyr = indexOf(hdr, "INVYR");
      int idxCondCwd = indexOf(hdr, "CONDPROP_CWD");
      int idxCondFwdSm = indexOf(hdr, "CONDPROP_FWD_SM");
      int idxCondFwdMd = indexOf(hdr, "CONDPROP_FWD_MD");
      int idxCondFwdLg = indexOf(hdr, "CONDPROP_FWD_LG");
      int idxCondDuff = indexOf(hdr, "CONDPROP_DUFF");
      int idxCwdAdj = indexOf(hdr, "CWD_DRYBIO_ADJ");
      int idxFwdSmAdj = indexOf(hdr, "FWD_SM_DRYBIO_ADJ");
      int idxFwdMdAdj = indexOf(hdr, "FWD_MD_DRYBIO_ADJ");
      int idxFwdLgAdj = indexOf(hdr, "FWD_LG_DRYBIO_ADJ");
      int idxDuffBio = indexOf(hdr, "DUFF_BIOMASS");
      int idxLitterBio = indexOf(hdr, "LITTER_BIOMASS");

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        rowsRead++;
        String[] cols = line.split(",", -1);
        int invyr = intAt(cols, idxInvyr);
        if (invyr <= 0) {
          continue;
        }

        double cwdProp = doubleAt(cols, idxCondCwd);
        double cwdTons = doubleAt(cols, idxCwdAdj);
        if (cwdProp > 0 || cwdTons > 0) {
          addRow(groups, invyr, "coarse_woody", cwdProp, cwdTons);
        }

        double fwdSmProp = doubleAt(cols, idxCondFwdSm);
        double fwdMdProp = doubleAt(cols, idxCondFwdMd);
        double fwdLgProp = doubleAt(cols, idxCondFwdLg);
        double fwdProp = (fwdSmProp + fwdMdProp + fwdLgProp) / 3.0;
        double fwdTons = doubleAt(cols, idxFwdSmAdj)
            + doubleAt(cols, idxFwdMdAdj)
            + doubleAt(cols, idxFwdLgAdj);
        if (fwdProp > 0 || fwdTons > 0) {
          addRow(groups, invyr, "fine_woody", fwdProp, fwdTons);
        }

        double duffProp = doubleAt(cols, idxCondDuff);
        double duffTons = doubleAt(cols, idxDuffBio) + doubleAt(cols, idxLitterBio);
        if (duffProp > 0 || duffTons > 0) {
          addRow(groups, invyr, "duff_litter", duffProp, duffTons);
        }
      }
    }

    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>(groups.size());
    for (Map.Entry<String, double[]> e : groups.entrySet()) {
      String[] parts = e.getKey().split("\\|", -1);
      double[] acc = e.getValue();
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("state_fips", stateFips);
      row.put("inventory_year", Integer.parseInt(parts[0]));
      row.put("debris_category", parts[1]);
      double weight = acc[1];
      Double tons = weight > 0 ? acc[0] / weight : null;
      row.put("tons_per_acre", tons);
      row.put("record_count", (long) acc[2]);
      result.add(row);
    }

    LOGGER.info("fia_down_woody_debris[{}]: rows read={} groups={}",
        state, rowsRead, result.size());
    return result.iterator();
  }

  private static void addRow(Map<String, double[]> groups, int invyr, String cat,
      double weight, double tons) {
    String key = invyr + "|" + cat;
    double[] acc = groups.get(key);
    if (acc == null) {
      acc = new double[3];
      groups.put(key, acc);
    }
    acc[0] += weight * tons;
    acc[1] += weight;
    acc[2] += 1.0;
  }

  private static String requireState(RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String state = dims != null ? dims.get("state") : null;
    if (state == null || state.isEmpty()) {
      throw new IllegalStateException(
          "FiaDownWoodyDebrisTransformer requires 'state' dimension in request context");
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
