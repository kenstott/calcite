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
import java.net.HttpURLConnection;
import java.net.URI;
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
 * <p>Joins FIA bulk TREE CSV with COND CSV (per state) to produce
 * CONDPROP_UNADJ-weighted estimates of trees per acre, live cubic-foot volume per acre,
 * and above-ground carbon stock (tons per acre) by inventory year × forest type group ×
 * ownership class. Only live trees (STATUSCD=1) in accessible forest conditions
 * (COND_STATUS_CD=1) are included.
 *
 * <p>Implements {@link StreamingResponseTransformer} to avoid OOM: the TREE CSV is read
 * line-by-line via streaming HTTP; only the COND lookup map (O(conditions)) and the
 * aggregation groups map (O(groups)) are held in memory simultaneously.
 *
 * <p>Source URLs:
 * <ul>
 *   <li>TREE: {@code https://apps.fs.usda.gov/fia/datamart/CSV/{stateAbbr}_TREE.csv} (primary)
 *   <li>COND: {@code https://apps.fs.usda.gov/fia/datamart/CSV/{stateAbbr}_COND.csv} (secondary)
 * </ul>
 *
 * <p>Join key: {@code PLT_CN + "|" + CONDID} (matches TREE.PLT_CN/CONDID to COND.PLT_CN/CONDID).
 * Aggregation key: {@code invyr|state_fips|forest_type_group|ownership_class}.
 */
public class FiaMetricsTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiaMetricsTransformer.class);

  private static final int CONNECT_TIMEOUT_MS = 30_000;
  private static final int READ_TIMEOUT_MS = 600_000; // 10 min for large state files

  private static final String COND_BASE_URL =
      "https://apps.fs.usda.gov/fia/datamart/CSV/";

  // ── Lookup tables: FORTYPCD → type-group code → name ─────────────────────

  private static final Map<Integer, Integer> FORTYPCD_TO_TYPGRPCD =
      new HashMap<Integer, Integer>();
  private static final Map<Integer, String> TYPGRP_NAME = new HashMap<Integer, String>();
  private static final Map<Integer, String> OWNGRP_NAME = new HashMap<Integer, String>();

  static {
    FORTYPCD_TO_TYPGRPCD.put(100, 100); FORTYPCD_TO_TYPGRPCD.put(101, 100);
    FORTYPCD_TO_TYPGRPCD.put(102, 100); FORTYPCD_TO_TYPGRPCD.put(103, 100);
    FORTYPCD_TO_TYPGRPCD.put(104, 100); FORTYPCD_TO_TYPGRPCD.put(105, 100);
    FORTYPCD_TO_TYPGRPCD.put(120, 120); FORTYPCD_TO_TYPGRPCD.put(121, 120);
    FORTYPCD_TO_TYPGRPCD.put(122, 120); FORTYPCD_TO_TYPGRPCD.put(123, 120);
    FORTYPCD_TO_TYPGRPCD.put(124, 120); FORTYPCD_TO_TYPGRPCD.put(125, 120);
    FORTYPCD_TO_TYPGRPCD.put(126, 120); FORTYPCD_TO_TYPGRPCD.put(127, 120);
    FORTYPCD_TO_TYPGRPCD.put(128, 120); FORTYPCD_TO_TYPGRPCD.put(129, 120);
    FORTYPCD_TO_TYPGRPCD.put(140, 140); FORTYPCD_TO_TYPGRPCD.put(141, 140);
    FORTYPCD_TO_TYPGRPCD.put(142, 140); FORTYPCD_TO_TYPGRPCD.put(150, 150);
    FORTYPCD_TO_TYPGRPCD.put(151, 150); FORTYPCD_TO_TYPGRPCD.put(160, 160);
    FORTYPCD_TO_TYPGRPCD.put(161, 160); FORTYPCD_TO_TYPGRPCD.put(162, 160);
    FORTYPCD_TO_TYPGRPCD.put(163, 160); FORTYPCD_TO_TYPGRPCD.put(164, 160);
    FORTYPCD_TO_TYPGRPCD.put(165, 160); FORTYPCD_TO_TYPGRPCD.put(166, 160);
    FORTYPCD_TO_TYPGRPCD.put(167, 160); FORTYPCD_TO_TYPGRPCD.put(168, 160);
    FORTYPCD_TO_TYPGRPCD.put(170, 170); FORTYPCD_TO_TYPGRPCD.put(171, 170);
    FORTYPCD_TO_TYPGRPCD.put(172, 170); FORTYPCD_TO_TYPGRPCD.put(180, 180);
    FORTYPCD_TO_TYPGRPCD.put(181, 180); FORTYPCD_TO_TYPGRPCD.put(182, 180);
    FORTYPCD_TO_TYPGRPCD.put(183, 180); FORTYPCD_TO_TYPGRPCD.put(184, 180);
    FORTYPCD_TO_TYPGRPCD.put(185, 180); FORTYPCD_TO_TYPGRPCD.put(200, 200);
    FORTYPCD_TO_TYPGRPCD.put(201, 200); FORTYPCD_TO_TYPGRPCD.put(202, 200);
    FORTYPCD_TO_TYPGRPCD.put(203, 200); FORTYPCD_TO_TYPGRPCD.put(220, 220);
    FORTYPCD_TO_TYPGRPCD.put(221, 220); FORTYPCD_TO_TYPGRPCD.put(222, 220);
    FORTYPCD_TO_TYPGRPCD.put(223, 220); FORTYPCD_TO_TYPGRPCD.put(224, 220);
    FORTYPCD_TO_TYPGRPCD.put(225, 220); FORTYPCD_TO_TYPGRPCD.put(226, 220);
    FORTYPCD_TO_TYPGRPCD.put(240, 240); FORTYPCD_TO_TYPGRPCD.put(241, 240);
    FORTYPCD_TO_TYPGRPCD.put(260, 260); FORTYPCD_TO_TYPGRPCD.put(261, 260);
    FORTYPCD_TO_TYPGRPCD.put(262, 260); FORTYPCD_TO_TYPGRPCD.put(263, 260);
    FORTYPCD_TO_TYPGRPCD.put(264, 260); FORTYPCD_TO_TYPGRPCD.put(265, 260);
    FORTYPCD_TO_TYPGRPCD.put(266, 260); FORTYPCD_TO_TYPGRPCD.put(267, 260);
    FORTYPCD_TO_TYPGRPCD.put(268, 260); FORTYPCD_TO_TYPGRPCD.put(269, 260);
    FORTYPCD_TO_TYPGRPCD.put(270, 260); FORTYPCD_TO_TYPGRPCD.put(271, 260);
    FORTYPCD_TO_TYPGRPCD.put(280, 280); FORTYPCD_TO_TYPGRPCD.put(281, 280);
    FORTYPCD_TO_TYPGRPCD.put(300, 300); FORTYPCD_TO_TYPGRPCD.put(301, 300);
    FORTYPCD_TO_TYPGRPCD.put(304, 300); FORTYPCD_TO_TYPGRPCD.put(305, 300);
    FORTYPCD_TO_TYPGRPCD.put(320, 320); FORTYPCD_TO_TYPGRPCD.put(321, 320);
    FORTYPCD_TO_TYPGRPCD.put(340, 340); FORTYPCD_TO_TYPGRPCD.put(341, 340);
    FORTYPCD_TO_TYPGRPCD.put(342, 340); FORTYPCD_TO_TYPGRPCD.put(360, 360);
    FORTYPCD_TO_TYPGRPCD.put(361, 360); FORTYPCD_TO_TYPGRPCD.put(362, 360);
    FORTYPCD_TO_TYPGRPCD.put(363, 360); FORTYPCD_TO_TYPGRPCD.put(364, 360);
    FORTYPCD_TO_TYPGRPCD.put(365, 360); FORTYPCD_TO_TYPGRPCD.put(366, 360);
    FORTYPCD_TO_TYPGRPCD.put(367, 360); FORTYPCD_TO_TYPGRPCD.put(368, 360);
    FORTYPCD_TO_TYPGRPCD.put(369, 360); FORTYPCD_TO_TYPGRPCD.put(370, 370);
    FORTYPCD_TO_TYPGRPCD.put(371, 370); FORTYPCD_TO_TYPGRPCD.put(380, 380);
    FORTYPCD_TO_TYPGRPCD.put(381, 380); FORTYPCD_TO_TYPGRPCD.put(382, 380);
    FORTYPCD_TO_TYPGRPCD.put(383, 380); FORTYPCD_TO_TYPGRPCD.put(384, 380);
    FORTYPCD_TO_TYPGRPCD.put(385, 380); FORTYPCD_TO_TYPGRPCD.put(390, 390);
    FORTYPCD_TO_TYPGRPCD.put(391, 390); FORTYPCD_TO_TYPGRPCD.put(400, 400);
    FORTYPCD_TO_TYPGRPCD.put(401, 400); FORTYPCD_TO_TYPGRPCD.put(402, 400);
    FORTYPCD_TO_TYPGRPCD.put(403, 400); FORTYPCD_TO_TYPGRPCD.put(404, 400);
    FORTYPCD_TO_TYPGRPCD.put(405, 400); FORTYPCD_TO_TYPGRPCD.put(406, 400);
    FORTYPCD_TO_TYPGRPCD.put(407, 400); FORTYPCD_TO_TYPGRPCD.put(409, 400);
    FORTYPCD_TO_TYPGRPCD.put(500, 500); FORTYPCD_TO_TYPGRPCD.put(501, 500);
    FORTYPCD_TO_TYPGRPCD.put(502, 500); FORTYPCD_TO_TYPGRPCD.put(503, 500);
    FORTYPCD_TO_TYPGRPCD.put(504, 500); FORTYPCD_TO_TYPGRPCD.put(505, 500);
    FORTYPCD_TO_TYPGRPCD.put(506, 500); FORTYPCD_TO_TYPGRPCD.put(507, 500);
    FORTYPCD_TO_TYPGRPCD.put(508, 500); FORTYPCD_TO_TYPGRPCD.put(509, 500);
    FORTYPCD_TO_TYPGRPCD.put(510, 500); FORTYPCD_TO_TYPGRPCD.put(511, 500);
    FORTYPCD_TO_TYPGRPCD.put(512, 500); FORTYPCD_TO_TYPGRPCD.put(513, 500);
    FORTYPCD_TO_TYPGRPCD.put(514, 500); FORTYPCD_TO_TYPGRPCD.put(515, 500);
    FORTYPCD_TO_TYPGRPCD.put(516, 500); FORTYPCD_TO_TYPGRPCD.put(517, 500);
    FORTYPCD_TO_TYPGRPCD.put(519, 500); FORTYPCD_TO_TYPGRPCD.put(520, 500);
    FORTYPCD_TO_TYPGRPCD.put(600, 600); FORTYPCD_TO_TYPGRPCD.put(601, 600);
    FORTYPCD_TO_TYPGRPCD.put(602, 600); FORTYPCD_TO_TYPGRPCD.put(605, 600);
    FORTYPCD_TO_TYPGRPCD.put(606, 600); FORTYPCD_TO_TYPGRPCD.put(607, 600);
    FORTYPCD_TO_TYPGRPCD.put(608, 600); FORTYPCD_TO_TYPGRPCD.put(609, 600);
    FORTYPCD_TO_TYPGRPCD.put(700, 700); FORTYPCD_TO_TYPGRPCD.put(701, 700);
    FORTYPCD_TO_TYPGRPCD.put(702, 700); FORTYPCD_TO_TYPGRPCD.put(703, 700);
    FORTYPCD_TO_TYPGRPCD.put(704, 700); FORTYPCD_TO_TYPGRPCD.put(705, 700);
    FORTYPCD_TO_TYPGRPCD.put(706, 700); FORTYPCD_TO_TYPGRPCD.put(707, 700);
    FORTYPCD_TO_TYPGRPCD.put(708, 700); FORTYPCD_TO_TYPGRPCD.put(709, 700);
    FORTYPCD_TO_TYPGRPCD.put(722, 700); FORTYPCD_TO_TYPGRPCD.put(800, 800);
    FORTYPCD_TO_TYPGRPCD.put(801, 800); FORTYPCD_TO_TYPGRPCD.put(802, 800);
    FORTYPCD_TO_TYPGRPCD.put(803, 800); FORTYPCD_TO_TYPGRPCD.put(805, 800);
    FORTYPCD_TO_TYPGRPCD.put(807, 800); FORTYPCD_TO_TYPGRPCD.put(809, 800);
    FORTYPCD_TO_TYPGRPCD.put(900, 900); FORTYPCD_TO_TYPGRPCD.put(901, 900);
    FORTYPCD_TO_TYPGRPCD.put(902, 900); FORTYPCD_TO_TYPGRPCD.put(903, 900);
    FORTYPCD_TO_TYPGRPCD.put(904, 900); FORTYPCD_TO_TYPGRPCD.put(905, 900);
    FORTYPCD_TO_TYPGRPCD.put(910, 910); FORTYPCD_TO_TYPGRPCD.put(911, 910);
    FORTYPCD_TO_TYPGRPCD.put(912, 910); FORTYPCD_TO_TYPGRPCD.put(920, 920);
    FORTYPCD_TO_TYPGRPCD.put(921, 920); FORTYPCD_TO_TYPGRPCD.put(922, 920);
    FORTYPCD_TO_TYPGRPCD.put(923, 920); FORTYPCD_TO_TYPGRPCD.put(924, 920);
    FORTYPCD_TO_TYPGRPCD.put(925, 920); FORTYPCD_TO_TYPGRPCD.put(926, 920);
    FORTYPCD_TO_TYPGRPCD.put(931, 920); FORTYPCD_TO_TYPGRPCD.put(932, 920);
    FORTYPCD_TO_TYPGRPCD.put(933, 920); FORTYPCD_TO_TYPGRPCD.put(934, 920);
    FORTYPCD_TO_TYPGRPCD.put(935, 920); FORTYPCD_TO_TYPGRPCD.put(940, 940);
    FORTYPCD_TO_TYPGRPCD.put(941, 940); FORTYPCD_TO_TYPGRPCD.put(942, 940);
    FORTYPCD_TO_TYPGRPCD.put(943, 940); FORTYPCD_TO_TYPGRPCD.put(950, 950);
    FORTYPCD_TO_TYPGRPCD.put(951, 950); FORTYPCD_TO_TYPGRPCD.put(952, 950);
    FORTYPCD_TO_TYPGRPCD.put(953, 950); FORTYPCD_TO_TYPGRPCD.put(954, 950);
    FORTYPCD_TO_TYPGRPCD.put(955, 950); FORTYPCD_TO_TYPGRPCD.put(960, 960);
    FORTYPCD_TO_TYPGRPCD.put(961, 960); FORTYPCD_TO_TYPGRPCD.put(962, 960);
    FORTYPCD_TO_TYPGRPCD.put(970, 970); FORTYPCD_TO_TYPGRPCD.put(971, 970);
    FORTYPCD_TO_TYPGRPCD.put(972, 970); FORTYPCD_TO_TYPGRPCD.put(973, 970);
    FORTYPCD_TO_TYPGRPCD.put(974, 970); FORTYPCD_TO_TYPGRPCD.put(975, 970);
    FORTYPCD_TO_TYPGRPCD.put(976, 970); FORTYPCD_TO_TYPGRPCD.put(980, 980);
    FORTYPCD_TO_TYPGRPCD.put(981, 980); FORTYPCD_TO_TYPGRPCD.put(982, 980);
    FORTYPCD_TO_TYPGRPCD.put(983, 980); FORTYPCD_TO_TYPGRPCD.put(984, 980);
    FORTYPCD_TO_TYPGRPCD.put(985, 980); FORTYPCD_TO_TYPGRPCD.put(986, 980);
    FORTYPCD_TO_TYPGRPCD.put(987, 980); FORTYPCD_TO_TYPGRPCD.put(988, 980);
    FORTYPCD_TO_TYPGRPCD.put(989, 980); FORTYPCD_TO_TYPGRPCD.put(990, 990);
    FORTYPCD_TO_TYPGRPCD.put(991, 990); FORTYPCD_TO_TYPGRPCD.put(992, 990);
    FORTYPCD_TO_TYPGRPCD.put(993, 990); FORTYPCD_TO_TYPGRPCD.put(995, 990);
    FORTYPCD_TO_TYPGRPCD.put(999, 999);

    TYPGRP_NAME.put(100, "White / red / jack pine group");
    TYPGRP_NAME.put(120, "Spruce / fir group");
    TYPGRP_NAME.put(140, "Longleaf / slash pine group");
    TYPGRP_NAME.put(150, "Tropical softwoods group");
    TYPGRP_NAME.put(160, "Loblolly / shortleaf pine group");
    TYPGRP_NAME.put(170, "Other eastern softwoods group");
    TYPGRP_NAME.put(180, "Pinyon / juniper group");
    TYPGRP_NAME.put(200, "Douglas-fir group");
    TYPGRP_NAME.put(220, "Ponderosa pine group");
    TYPGRP_NAME.put(240, "Western white pine group");
    TYPGRP_NAME.put(260, "Fir / spruce / mountain hemlock group");
    TYPGRP_NAME.put(280, "Lodgepole pine group");
    TYPGRP_NAME.put(300, "Hemlock / Sitka spruce group");
    TYPGRP_NAME.put(320, "Western larch group");
    TYPGRP_NAME.put(340, "Redwood group");
    TYPGRP_NAME.put(360, "Other western softwoods group");
    TYPGRP_NAME.put(370, "California mixed conifer group");
    TYPGRP_NAME.put(380, "Exotic softwoods group");
    TYPGRP_NAME.put(390, "Other softwoods group");
    TYPGRP_NAME.put(400, "Oak / pine group");
    TYPGRP_NAME.put(500, "Oak / hickory group");
    TYPGRP_NAME.put(600, "Oak / gum / cypress group");
    TYPGRP_NAME.put(700, "Elm / ash / cottonwood group");
    TYPGRP_NAME.put(800, "Maple / beech / birch group");
    TYPGRP_NAME.put(900, "Aspen / birch group");
    TYPGRP_NAME.put(910, "Alder / maple group");
    TYPGRP_NAME.put(920, "Western oak group");
    TYPGRP_NAME.put(940, "Tanoak / laurel group");
    TYPGRP_NAME.put(950, "Other western hardwoods group");
    TYPGRP_NAME.put(960, "Other hardwoods group");
    TYPGRP_NAME.put(970, "Woodland hardwoods group");
    TYPGRP_NAME.put(980, "Tropical hardwoods group");
    TYPGRP_NAME.put(990, "Exotic hardwoods group");
    TYPGRP_NAME.put(999, "Nonstocked");

    OWNGRP_NAME.put(10, "National Forest");
    OWNGRP_NAME.put(20, "Other Federal");
    OWNGRP_NAME.put(30, "State");
    OWNGRP_NAME.put(40, "Local");
    OWNGRP_NAME.put(50, "Private");
  }

  // ── StreamingResponseTransformer ─────────────────────────────────────────

  @Override
  public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String stateAbbr = context.getDimensionValues().get("stateAbbr");
    String treeUrl = context.getUrl();
    String condUrl = COND_BASE_URL + stateAbbr + "_COND.csv";

    // Step 1: Fetch COND CSV → build lookup map (keyed by PLT_CN|CONDID)
    Map<String, CondEntry> condMap = fetchCondMap(condUrl, stateAbbr);

    // Step 2: Pre-populate groups from COND — accumulates condprop_sum once per condition
    // acc layout: [condprop_sum, tpa_w, vol_w, carbon_w]
    Map<String, double[]> groups = new LinkedHashMap<String, double[]>();
    for (CondEntry cond : condMap.values()) {
      String key = groupKey(cond.invyr, cond.stateFips, cond.typGrp, cond.ownGrp);
      double[] acc = groups.get(key);
      if (acc == null) {
        acc = new double[4];
        groups.put(key, acc);
      }
      acc[0] += cond.condprop;
    }

    // Step 3: Stream TREE CSV, accumulate weighted metrics per group
    streamTreeMetrics(treeUrl, stateAbbr, condMap, groups);

    // Step 4: Emit result rows (one per group with condprop_sum > 0)
    List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
    for (Map.Entry<String, double[]> entry : groups.entrySet()) {
      String[] parts = entry.getKey().split("\\|", -1);
      double[] acc = entry.getValue();
      double condpropSum = acc[0];
      if (condpropSum <= 0.0) {
        continue;
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("inventory_year", Integer.parseInt(parts[0]));
      row.put("state_fips", parts[1]);
      row.put("forest_type_group", "null".equals(parts[2]) ? null : parts[2]);
      row.put("ownership_class", "null".equals(parts[3]) ? null : parts[3]);
      row.put("trees_per_acre", acc[1] / condpropSum);
      row.put("live_volume_cuft", acc[2] / condpropSum);
      row.put("carbon_stock_tons", acc[3] / condpropSum);
      result.add(row);
    }

    LOGGER.info("forest_metrics: stateAbbr={} conditions={} groups={}",
        stateAbbr, condMap.size(), result.size());
    return result.iterator();
  }

  // ── COND fetch ────────────────────────────────────────────────────────────

  private Map<String, CondEntry> fetchCondMap(String url, String stateAbbr) throws IOException {
    LOGGER.debug("forest_metrics: fetching COND for stateAbbr={}", stateAbbr);
    HttpURLConnection conn = openConnection(url);
    try {
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
      return parseCond(reader, stateAbbr);
    } finally {
      conn.disconnect();
    }
  }

  private Map<String, CondEntry> parseCond(BufferedReader reader, String stateAbbr)
      throws IOException {
    String headerLine = reader.readLine();
    if (headerLine == null) {
      LOGGER.warn("forest_metrics: empty COND CSV for stateAbbr={}", stateAbbr);
      return new HashMap<String, CondEntry>();
    }
    String[] hdr = headerLine.split(",", -1);
    int idxPltCn = indexOf(hdr, "PLT_CN");
    int idxCondId = indexOf(hdr, "CONDID");
    int idxInvyr = indexOf(hdr, "INVYR");
    int idxStatecd = indexOf(hdr, "STATECD");
    int idxCondStatus = indexOf(hdr, "COND_STATUS_CD");
    int idxOwngrpcd = indexOf(hdr, "OWNGRPCD");
    int idxFortypcd = indexOf(hdr, "FORTYPCD");
    int idxCondprop = indexOf(hdr, "CONDPROP_UNADJ");

    Map<String, CondEntry> map = new HashMap<String, CondEntry>();
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.trim().isEmpty()) {
        continue;
      }
      String[] cols = line.split(",", -1);
      if (intAt(cols, idxCondStatus) != 1) {
        continue; // accessible forest land only
      }
      String pltCn = strAt(cols, idxPltCn);
      String condId = strAt(cols, idxCondId);
      int invyr = intAt(cols, idxInvyr);
      int statecd = intAt(cols, idxStatecd);
      String stateFips = statecd < 10 ? "0" + statecd : String.valueOf(statecd);
      String typGrp = resolveTypGrp(intAt(cols, idxFortypcd));
      String ownGrp = resolveOwnGrp(intAt(cols, idxOwngrpcd));
      double condprop = doubleAt(cols, idxCondprop);
      map.put(pltCn + "|" + condId, new CondEntry(invyr, stateFips, typGrp, ownGrp, condprop));
    }
    return map;
  }

  // ── TREE stream ───────────────────────────────────────────────────────────

  private void streamTreeMetrics(String url, String stateAbbr,
      Map<String, CondEntry> condMap, Map<String, double[]> groups) throws IOException {
    LOGGER.debug("forest_metrics: streaming TREE for stateAbbr={}", stateAbbr);
    HttpURLConnection conn = openConnection(url);
    try {
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
      String headerLine = reader.readLine();
      if (headerLine == null) {
        LOGGER.warn("forest_metrics: empty TREE CSV for stateAbbr={}", stateAbbr);
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
        if (line.trim().isEmpty()) {
          continue;
        }
        rowsRead++;
        String[] cols = line.split(",", -1);
        if (intAt(cols, idxStatuscd) != 1) {
          continue; // live trees only
        }
        String pltCn = strAt(cols, idxPltCn);
        String condId = strAt(cols, idxCondId);
        CondEntry cond = condMap.get(pltCn + "|" + condId);
        if (cond == null) {
          continue; // no matching accessible forest condition
        }
        double tpa = doubleAt(cols, idxTpa);
        double vol = doubleAt(cols, idxVol);
        double carbon = doubleAt(cols, idxCarbon);
        double condprop = cond.condprop;
        rowsKept++;
        String key = groupKey(cond.invyr, cond.stateFips, cond.typGrp, cond.ownGrp);
        double[] acc = groups.get(key);
        if (acc == null) {
          continue; // should not happen; guard for safety
        }
        // Weighted accumulation: metric_w = SUM(TPA_UNADJ * metric * CONDPROP)
        // Divide by condprop_sum at emission to get weighted average per acre
        acc[1] += tpa * condprop;
        acc[2] += tpa * vol * condprop;
        acc[3] += tpa * carbon * condprop / 2000.0; // lbs → tons
      }
      LOGGER.info("forest_metrics: stateAbbr={} tree rows read={} kept={}",
          stateAbbr, rowsRead, rowsKept);
    } finally {
      conn.disconnect();
    }
  }

  // ── HTTP helpers ──────────────────────────────────────────────────────────

  private HttpURLConnection openConnection(String url) throws IOException {
    HttpURLConnection conn =
        (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
    conn.setReadTimeout(READ_TIMEOUT_MS);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    return conn;
  }

  // ── CSV helpers ───────────────────────────────────────────────────────────

  private int indexOf(String[] hdr, String name) {
    for (int i = 0; i < hdr.length; i++) {
      if (name.equals(hdr[i].trim())) {
        return i;
      }
    }
    return -1;
  }

  private String strAt(String[] cols, int idx) {
    if (idx < 0 || idx >= cols.length) {
      return "";
    }
    return cols[idx].trim();
  }

  private int intAt(String[] cols, int idx) {
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

  private double doubleAt(String[] cols, int idx) {
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

  // ── Lookup helpers ────────────────────────────────────────────────────────

  private String resolveTypGrp(int fortypcd) {
    if (fortypcd <= 0) {
      return null;
    }
    Integer typgrpcd = FORTYPCD_TO_TYPGRPCD.get(fortypcd);
    if (typgrpcd == null) {
      return null;
    }
    return TYPGRP_NAME.get(typgrpcd);
  }

  private String resolveOwnGrp(int owngrpcd) {
    if (owngrpcd <= 0) {
      return null;
    }
    return OWNGRP_NAME.get(owngrpcd);
  }

  private String groupKey(int invyr, String stateFips, String typGrp, String ownGrp) {
    return invyr + "|" + stateFips + "|" + typGrp + "|" + ownGrp;
  }

  // ── Inner types ───────────────────────────────────────────────────────────

  private static final class CondEntry {
    final int invyr;
    final String stateFips;
    final String typGrp;
    final String ownGrp;
    final double condprop;

    CondEntry(int invyr, String stateFips, String typGrp, String ownGrp, double condprop) {
      this.invyr = invyr;
      this.stateFips = stateFips;
      this.typGrp = typGrp;
      this.ownGrp = ownGrp;
      this.condprop = condprop;
    }
  }
}
