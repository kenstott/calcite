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
 * Streaming transformer for {@code fia_pop_evaluations}.
 *
 * <p>Joins the per-state {@code <ST>_POP_EVAL.csv} (one row per FIA evaluation —
 * a published, statistically-valid sample of plots producing an estimate) with
 * {@code <ST>_POP_EVAL_TYP.csv} (which assigns one or more evaluation types
 * such as {@code EXPVOL}, {@code EXPCURR}, {@code EXPGROW} to an evaluation
 * via {@code EVAL_CN}). One output row per ({@code eval_cn}, {@code eval_type})
 * pair (a single evaluation may be tagged with multiple types).
 */
public class FiaPopEvaluationsTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiaPopEvaluationsTransformer.class);

  private static final String POP_EVAL_ENTRY = "POP_EVAL.csv";
  private static final String POP_EVAL_TYP_ENTRY = "POP_EVAL_TYP.csv";

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String state = requireState(context);
    String stateFips = FiaLookups.stateFipsForAbbr(state);

    Map<String, List<String>> typesByEvalCn = readEvalTypes(state);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    long rowsRead = 0;
    try (FiaStateArchive.EntryHandle entry = FiaStateArchive.openEntry(state, POP_EVAL_ENTRY);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(entry.stream, StandardCharsets.UTF_8))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        LOGGER.warn("fia_pop_evaluations: empty {}_{}", state, POP_EVAL_ENTRY);
        return rows.iterator();
      }
      String[] hdr = headerLine.split(",", -1);
      int idxCn = indexOf(hdr, "CN");
      int idxEvalGrpCn = indexOf(hdr, "EVAL_GRP_CN");
      int idxDescr = indexOf(hdr, "EVAL_DESCR");
      int idxStart = indexOf(hdr, "START_INVYR");
      int idxEnd = indexOf(hdr, "END_INVYR");

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        rowsRead++;
        String[] cols = splitCsv(line);
        String cn = strAt(cols, idxCn);
        if (cn.isEmpty()) {
          continue;
        }
        String evalGrpCn = nullIfEmpty(strAt(cols, idxEvalGrpCn));
        String descr = nullIfEmpty(strAt(cols, idxDescr));
        Integer start = intOrNull(cols, idxStart);
        Integer end = intOrNull(cols, idxEnd);

        List<String> types = typesByEvalCn.get(cn);
        if (types == null || types.isEmpty()) {
          rows.add(buildRow(stateFips, cn, evalGrpCn, descr, start, end, null));
        } else {
          for (String t : types) {
            rows.add(buildRow(stateFips, cn, evalGrpCn, descr, start, end, t));
          }
        }
      }
    }

    LOGGER.info("fia_pop_evaluations[{}]: rows read={} type_map={} output={}",
        state, rowsRead, typesByEvalCn.size(), rows.size());
    return rows.iterator();
  }

  private Map<String, List<String>> readEvalTypes(String state) throws IOException {
    Map<String, List<String>> map = new HashMap<String, List<String>>();
    try (FiaStateArchive.EntryHandle entry = FiaStateArchive.openEntry(state, POP_EVAL_TYP_ENTRY);
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(entry.stream, StandardCharsets.UTF_8))) {
      String headerLine = reader.readLine();
      if (headerLine == null) {
        return map;
      }
      String[] hdr = headerLine.split(",", -1);
      int idxEvalCn = indexOf(hdr, "EVAL_CN");
      int idxType = indexOf(hdr, "EVAL_TYP");
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] cols = splitCsv(line);
        String evalCn = strAt(cols, idxEvalCn);
        String type = strAt(cols, idxType);
        if (evalCn.isEmpty() || type.isEmpty()) {
          continue;
        }
        List<String> list = map.get(evalCn);
        if (list == null) {
          list = new ArrayList<String>(2);
          map.put(evalCn, list);
        }
        list.add(type);
      }
    }
    return map;
  }

  private static Map<String, Object> buildRow(String stateFips, String cn, String evalGrpCn,
      String descr, Integer start, Integer end, String type) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("state_fips", stateFips);
    row.put("eval_cn", cn);
    row.put("eval_grp_cn", evalGrpCn);
    row.put("eval_descr", descr);
    row.put("start_invyr", start);
    row.put("end_invyr", end);
    row.put("eval_type", type);
    return row;
  }

  private static String requireState(RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String state = dims != null ? dims.get("state") : null;
    if (state == null || state.isEmpty()) {
      throw new IllegalStateException(
          "FiaPopEvaluationsTransformer requires 'state' dimension in request context");
    }
    return state;
  }

  /** Splits a CSV line preserving quoted fields (EVAL_DESCR may contain commas). */
  private static String[] splitCsv(String line) {
    List<String> out = new ArrayList<String>();
    StringBuilder cur = new StringBuilder();
    boolean inQuotes = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (c == '"') {
        if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          cur.append('"');
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (c == ',' && !inQuotes) {
        out.add(cur.toString());
        cur.setLength(0);
      } else {
        cur.append(c);
      }
    }
    out.add(cur.toString());
    return out.toArray(new String[0]);
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
}
