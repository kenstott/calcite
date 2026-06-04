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
 * Streaming transformer for {@code fia_plots}.
 *
 * <p>Reads the per-state {@code <ST>_PLOT.csv} entry from the FIA datamart archive.
 * One row per FIA plot, exposing the publicly-released "fuzzed" plot coordinates
 * (lat/lon are perturbed by USDA so exact plot locations cannot be inferred — see
 * FIADB P2 User Guide §2.5.1).
 *
 * <p>Joins downstream: {@code state_fips} to {@code geo.states}, {@code county_fips}
 * to {@code geo.counties}. {@code plot_cn} is the FIA-wide unique plot identifier and
 * is the PK target for plot-grain FIA tables (TREE, COND, SEEDLING, …).
 */
public class FiaPlotsTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiaPlotsTransformer.class);

  private static final String PLOT_ENTRY = "PLOT.csv";

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String state = requireState(context);
    String stateFips = FiaLookups.stateFipsForAbbr(state);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    long rowsRead = 0;
    try (FiaStateArchive.EntryHandle entry = FiaStateArchive.openEntry(state, PLOT_ENTRY);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(entry.stream, StandardCharsets.UTF_8))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        LOGGER.warn("fia_plots: empty {}_{} in FIA archive", state, PLOT_ENTRY);
        return rows.iterator();
      }
      String[] hdr = headerLine.split(",", -1);
      int idxCn = indexOf(hdr, "CN");
      int idxPrevCn = indexOf(hdr, "PREV_PLT_CN");
      int idxInvyr = indexOf(hdr, "INVYR");
      int idxStatecd = indexOf(hdr, "STATECD");
      int idxCountycd = indexOf(hdr, "COUNTYCD");
      int idxPlot = indexOf(hdr, "PLOT");
      int idxStatus = indexOf(hdr, "PLOT_STATUS_CD");
      int idxLat = indexOf(hdr, "LAT");
      int idxLon = indexOf(hdr, "LON");
      int idxElev = indexOf(hdr, "ELEV");
      int idxMeasyear = indexOf(hdr, "MEASYEAR");

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        rowsRead++;
        String[] cols = line.split(",", -1);
        String cn = strAt(cols, idxCn);
        if (cn.isEmpty()) {
          continue;
        }
        int statusCd = intAt(cols, idxStatus);
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("plot_cn", cn);
        row.put("prev_plot_cn", nullIfEmpty(strAt(cols, idxPrevCn)));
        row.put("inventory_year", intOrNull(cols, idxInvyr));
        row.put("state_fips", stateFips);
        String countyFips = countyFips(stateFips, intAt(cols, idxCountycd));
        row.put("county_fips", countyFips);
        row.put("plot_number", intOrNull(cols, idxPlot));
        row.put("plot_status_cd", statusCd > 0 ? statusCd : null);
        row.put("plot_status_name", resolvePlotStatus(statusCd));
        row.put("lat", doubleOrNull(cols, idxLat));
        row.put("lon", doubleOrNull(cols, idxLon));
        row.put("elev_ft", intOrNull(cols, idxElev));
        row.put("measure_year", intOrNull(cols, idxMeasyear));
        rows.add(row);
      }
    }

    LOGGER.info("fia_plots[{}]: rows read={} kept={}", state, rowsRead, rows.size());
    return rows.iterator();
  }

  private static String resolvePlotStatus(int code) {
    switch (code) {
      case 1: return "Sampled - forest";
      case 2: return "Sampled - nonforest";
      case 3: return "Nonsampled";
      default: return null;
    }
  }

  private static String countyFips(String stateFips, int countycd) {
    if (countycd <= 0) {
      return null;
    }
    String c = countycd < 10 ? "00" + countycd : (countycd < 100 ? "0" + countycd : String.valueOf(countycd));
    return stateFips + c;
  }

  private static String requireState(RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String state = dims != null ? dims.get("state") : null;
    if (state == null || state.isEmpty()) {
      throw new IllegalStateException(
          "FiaPlotsTransformer requires 'state' dimension in request context");
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

  private static String nullIfEmpty(String s) {
    return (s == null || s.isEmpty()) ? null : s;
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

  private static Integer intOrNull(String[] cols, int idx) {
    String v = strAt(cols, idx);
    if (v.isEmpty()) {
      return null;
    }
    try {
      return (int) Double.parseDouble(v);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Double doubleOrNull(String[] cols, int idx) {
    String v = strAt(cols, idx);
    if (v.isEmpty()) {
      return null;
    }
    try {
      return Double.parseDouble(v);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
