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
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Transforms USDA FIA bulk COND CSV responses into {@code forest_inventory} rows.
 *
 * <p>Fetches per-state COND CSV files from apps.fs.usda.gov/fia/datamart/CSV/{state}_COND.csv,
 * filters to COND_STATUS_CD=1 (accessible forest land), and aggregates by
 * (INVYR, STATECD, forest_type_group, ownership_class).
 *
 * <p>Basal area is a CONDPROP_UNADJ-weighted average of BALIVE.
 * Land area, live volume, carbon stock, and trees per acre require tree-level
 * data not available in the COND table and are returned as null.
 */
public class FiaDatamartTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FiaDatamartTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Map<Integer, Integer> FORTYPCD_TO_TYPGRPCD = new HashMap<Integer, Integer>();
  private static final Map<Integer, String> TYPGRP_NAME = new HashMap<Integer, String>();
  private static final Map<Integer, String> OWNGRP_NAME = new HashMap<Integer, String>();

  static {
    FORTYPCD_TO_TYPGRPCD.put(100, 100); FORTYPCD_TO_TYPGRPCD.put(101, 100); FORTYPCD_TO_TYPGRPCD.put(102, 100);
    FORTYPCD_TO_TYPGRPCD.put(103, 100); FORTYPCD_TO_TYPGRPCD.put(104, 100); FORTYPCD_TO_TYPGRPCD.put(105, 100);
    FORTYPCD_TO_TYPGRPCD.put(120, 120); FORTYPCD_TO_TYPGRPCD.put(121, 120); FORTYPCD_TO_TYPGRPCD.put(122, 120);
    FORTYPCD_TO_TYPGRPCD.put(123, 120); FORTYPCD_TO_TYPGRPCD.put(124, 120); FORTYPCD_TO_TYPGRPCD.put(125, 120);
    FORTYPCD_TO_TYPGRPCD.put(126, 120); FORTYPCD_TO_TYPGRPCD.put(127, 120); FORTYPCD_TO_TYPGRPCD.put(128, 120);
    FORTYPCD_TO_TYPGRPCD.put(129, 120); FORTYPCD_TO_TYPGRPCD.put(140, 140); FORTYPCD_TO_TYPGRPCD.put(141, 140);
    FORTYPCD_TO_TYPGRPCD.put(142, 140); FORTYPCD_TO_TYPGRPCD.put(150, 150); FORTYPCD_TO_TYPGRPCD.put(151, 150);
    FORTYPCD_TO_TYPGRPCD.put(160, 160); FORTYPCD_TO_TYPGRPCD.put(161, 160); FORTYPCD_TO_TYPGRPCD.put(162, 160);
    FORTYPCD_TO_TYPGRPCD.put(163, 160); FORTYPCD_TO_TYPGRPCD.put(164, 160); FORTYPCD_TO_TYPGRPCD.put(165, 160);
    FORTYPCD_TO_TYPGRPCD.put(166, 160); FORTYPCD_TO_TYPGRPCD.put(167, 160); FORTYPCD_TO_TYPGRPCD.put(168, 160);
    FORTYPCD_TO_TYPGRPCD.put(170, 170); FORTYPCD_TO_TYPGRPCD.put(171, 170); FORTYPCD_TO_TYPGRPCD.put(172, 170);
    FORTYPCD_TO_TYPGRPCD.put(180, 180); FORTYPCD_TO_TYPGRPCD.put(181, 180); FORTYPCD_TO_TYPGRPCD.put(182, 180);
    FORTYPCD_TO_TYPGRPCD.put(183, 180); FORTYPCD_TO_TYPGRPCD.put(184, 180); FORTYPCD_TO_TYPGRPCD.put(185, 180);
    FORTYPCD_TO_TYPGRPCD.put(200, 200); FORTYPCD_TO_TYPGRPCD.put(201, 200); FORTYPCD_TO_TYPGRPCD.put(202, 200);
    FORTYPCD_TO_TYPGRPCD.put(203, 200); FORTYPCD_TO_TYPGRPCD.put(220, 220); FORTYPCD_TO_TYPGRPCD.put(221, 220);
    FORTYPCD_TO_TYPGRPCD.put(222, 220); FORTYPCD_TO_TYPGRPCD.put(223, 220); FORTYPCD_TO_TYPGRPCD.put(224, 220);
    FORTYPCD_TO_TYPGRPCD.put(225, 220); FORTYPCD_TO_TYPGRPCD.put(226, 220); FORTYPCD_TO_TYPGRPCD.put(240, 240);
    FORTYPCD_TO_TYPGRPCD.put(241, 240); FORTYPCD_TO_TYPGRPCD.put(260, 260); FORTYPCD_TO_TYPGRPCD.put(261, 260);
    FORTYPCD_TO_TYPGRPCD.put(262, 260); FORTYPCD_TO_TYPGRPCD.put(263, 260); FORTYPCD_TO_TYPGRPCD.put(264, 260);
    FORTYPCD_TO_TYPGRPCD.put(265, 260); FORTYPCD_TO_TYPGRPCD.put(266, 260); FORTYPCD_TO_TYPGRPCD.put(267, 260);
    FORTYPCD_TO_TYPGRPCD.put(268, 260); FORTYPCD_TO_TYPGRPCD.put(269, 260); FORTYPCD_TO_TYPGRPCD.put(270, 260);
    FORTYPCD_TO_TYPGRPCD.put(271, 260); FORTYPCD_TO_TYPGRPCD.put(280, 280); FORTYPCD_TO_TYPGRPCD.put(281, 280);
    FORTYPCD_TO_TYPGRPCD.put(300, 300); FORTYPCD_TO_TYPGRPCD.put(301, 300); FORTYPCD_TO_TYPGRPCD.put(304, 300);
    FORTYPCD_TO_TYPGRPCD.put(305, 300); FORTYPCD_TO_TYPGRPCD.put(320, 320); FORTYPCD_TO_TYPGRPCD.put(321, 320);
    FORTYPCD_TO_TYPGRPCD.put(340, 340); FORTYPCD_TO_TYPGRPCD.put(341, 340); FORTYPCD_TO_TYPGRPCD.put(342, 340);
    FORTYPCD_TO_TYPGRPCD.put(360, 360); FORTYPCD_TO_TYPGRPCD.put(361, 360); FORTYPCD_TO_TYPGRPCD.put(362, 360);
    FORTYPCD_TO_TYPGRPCD.put(363, 360); FORTYPCD_TO_TYPGRPCD.put(364, 360); FORTYPCD_TO_TYPGRPCD.put(365, 360);
    FORTYPCD_TO_TYPGRPCD.put(366, 360); FORTYPCD_TO_TYPGRPCD.put(367, 360); FORTYPCD_TO_TYPGRPCD.put(368, 360);
    FORTYPCD_TO_TYPGRPCD.put(369, 360); FORTYPCD_TO_TYPGRPCD.put(370, 370); FORTYPCD_TO_TYPGRPCD.put(371, 370);
    FORTYPCD_TO_TYPGRPCD.put(380, 380); FORTYPCD_TO_TYPGRPCD.put(381, 380); FORTYPCD_TO_TYPGRPCD.put(382, 380);
    FORTYPCD_TO_TYPGRPCD.put(383, 380); FORTYPCD_TO_TYPGRPCD.put(384, 380); FORTYPCD_TO_TYPGRPCD.put(385, 380);
    FORTYPCD_TO_TYPGRPCD.put(390, 390); FORTYPCD_TO_TYPGRPCD.put(391, 390);
    FORTYPCD_TO_TYPGRPCD.put(400, 400); FORTYPCD_TO_TYPGRPCD.put(401, 400); FORTYPCD_TO_TYPGRPCD.put(402, 400);
    FORTYPCD_TO_TYPGRPCD.put(403, 400); FORTYPCD_TO_TYPGRPCD.put(404, 400); FORTYPCD_TO_TYPGRPCD.put(405, 400);
    FORTYPCD_TO_TYPGRPCD.put(406, 400); FORTYPCD_TO_TYPGRPCD.put(407, 400); FORTYPCD_TO_TYPGRPCD.put(409, 400);
    FORTYPCD_TO_TYPGRPCD.put(500, 500); FORTYPCD_TO_TYPGRPCD.put(501, 500); FORTYPCD_TO_TYPGRPCD.put(502, 500);
    FORTYPCD_TO_TYPGRPCD.put(503, 500); FORTYPCD_TO_TYPGRPCD.put(504, 500); FORTYPCD_TO_TYPGRPCD.put(505, 500);
    FORTYPCD_TO_TYPGRPCD.put(506, 500); FORTYPCD_TO_TYPGRPCD.put(507, 500); FORTYPCD_TO_TYPGRPCD.put(508, 500);
    FORTYPCD_TO_TYPGRPCD.put(509, 500); FORTYPCD_TO_TYPGRPCD.put(510, 500); FORTYPCD_TO_TYPGRPCD.put(511, 500);
    FORTYPCD_TO_TYPGRPCD.put(512, 500); FORTYPCD_TO_TYPGRPCD.put(513, 500); FORTYPCD_TO_TYPGRPCD.put(514, 500);
    FORTYPCD_TO_TYPGRPCD.put(515, 500); FORTYPCD_TO_TYPGRPCD.put(516, 500); FORTYPCD_TO_TYPGRPCD.put(517, 500);
    FORTYPCD_TO_TYPGRPCD.put(519, 500); FORTYPCD_TO_TYPGRPCD.put(520, 500);
    FORTYPCD_TO_TYPGRPCD.put(600, 600); FORTYPCD_TO_TYPGRPCD.put(601, 600); FORTYPCD_TO_TYPGRPCD.put(602, 600);
    FORTYPCD_TO_TYPGRPCD.put(605, 600); FORTYPCD_TO_TYPGRPCD.put(606, 600); FORTYPCD_TO_TYPGRPCD.put(607, 600);
    FORTYPCD_TO_TYPGRPCD.put(608, 600); FORTYPCD_TO_TYPGRPCD.put(609, 600);
    FORTYPCD_TO_TYPGRPCD.put(700, 700); FORTYPCD_TO_TYPGRPCD.put(701, 700); FORTYPCD_TO_TYPGRPCD.put(702, 700);
    FORTYPCD_TO_TYPGRPCD.put(703, 700); FORTYPCD_TO_TYPGRPCD.put(704, 700); FORTYPCD_TO_TYPGRPCD.put(705, 700);
    FORTYPCD_TO_TYPGRPCD.put(706, 700); FORTYPCD_TO_TYPGRPCD.put(707, 700); FORTYPCD_TO_TYPGRPCD.put(708, 700);
    FORTYPCD_TO_TYPGRPCD.put(709, 700); FORTYPCD_TO_TYPGRPCD.put(722, 700);
    FORTYPCD_TO_TYPGRPCD.put(800, 800); FORTYPCD_TO_TYPGRPCD.put(801, 800); FORTYPCD_TO_TYPGRPCD.put(802, 800);
    FORTYPCD_TO_TYPGRPCD.put(803, 800); FORTYPCD_TO_TYPGRPCD.put(805, 800); FORTYPCD_TO_TYPGRPCD.put(807, 800);
    FORTYPCD_TO_TYPGRPCD.put(809, 800);
    FORTYPCD_TO_TYPGRPCD.put(900, 900); FORTYPCD_TO_TYPGRPCD.put(901, 900); FORTYPCD_TO_TYPGRPCD.put(902, 900);
    FORTYPCD_TO_TYPGRPCD.put(903, 900); FORTYPCD_TO_TYPGRPCD.put(904, 900); FORTYPCD_TO_TYPGRPCD.put(905, 900);
    FORTYPCD_TO_TYPGRPCD.put(910, 910); FORTYPCD_TO_TYPGRPCD.put(911, 910); FORTYPCD_TO_TYPGRPCD.put(912, 910);
    FORTYPCD_TO_TYPGRPCD.put(920, 920); FORTYPCD_TO_TYPGRPCD.put(921, 920); FORTYPCD_TO_TYPGRPCD.put(922, 920);
    FORTYPCD_TO_TYPGRPCD.put(923, 920); FORTYPCD_TO_TYPGRPCD.put(924, 920); FORTYPCD_TO_TYPGRPCD.put(925, 920);
    FORTYPCD_TO_TYPGRPCD.put(926, 920); FORTYPCD_TO_TYPGRPCD.put(931, 920); FORTYPCD_TO_TYPGRPCD.put(932, 920);
    FORTYPCD_TO_TYPGRPCD.put(933, 920); FORTYPCD_TO_TYPGRPCD.put(934, 920); FORTYPCD_TO_TYPGRPCD.put(935, 920);
    FORTYPCD_TO_TYPGRPCD.put(940, 940); FORTYPCD_TO_TYPGRPCD.put(941, 940); FORTYPCD_TO_TYPGRPCD.put(942, 940);
    FORTYPCD_TO_TYPGRPCD.put(943, 940); FORTYPCD_TO_TYPGRPCD.put(950, 950); FORTYPCD_TO_TYPGRPCD.put(951, 950);
    FORTYPCD_TO_TYPGRPCD.put(952, 950); FORTYPCD_TO_TYPGRPCD.put(953, 950); FORTYPCD_TO_TYPGRPCD.put(954, 950);
    FORTYPCD_TO_TYPGRPCD.put(955, 950); FORTYPCD_TO_TYPGRPCD.put(960, 960); FORTYPCD_TO_TYPGRPCD.put(961, 960);
    FORTYPCD_TO_TYPGRPCD.put(962, 960); FORTYPCD_TO_TYPGRPCD.put(970, 970); FORTYPCD_TO_TYPGRPCD.put(971, 970);
    FORTYPCD_TO_TYPGRPCD.put(972, 970); FORTYPCD_TO_TYPGRPCD.put(973, 970); FORTYPCD_TO_TYPGRPCD.put(974, 970);
    FORTYPCD_TO_TYPGRPCD.put(975, 970); FORTYPCD_TO_TYPGRPCD.put(976, 970);
    FORTYPCD_TO_TYPGRPCD.put(980, 980); FORTYPCD_TO_TYPGRPCD.put(981, 980); FORTYPCD_TO_TYPGRPCD.put(982, 980);
    FORTYPCD_TO_TYPGRPCD.put(983, 980); FORTYPCD_TO_TYPGRPCD.put(984, 980); FORTYPCD_TO_TYPGRPCD.put(985, 980);
    FORTYPCD_TO_TYPGRPCD.put(986, 980); FORTYPCD_TO_TYPGRPCD.put(987, 980); FORTYPCD_TO_TYPGRPCD.put(988, 980);
    FORTYPCD_TO_TYPGRPCD.put(989, 980); FORTYPCD_TO_TYPGRPCD.put(990, 990); FORTYPCD_TO_TYPGRPCD.put(991, 990);
    FORTYPCD_TO_TYPGRPCD.put(992, 990); FORTYPCD_TO_TYPGRPCD.put(993, 990); FORTYPCD_TO_TYPGRPCD.put(995, 990);
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

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("forest_inventory: empty response from FIA DataMart for stateAbbr={}",
          context.getDimensionValues().get("stateAbbr"));
      return "[]";
    }

    String stateAbbr = context.getDimensionValues().get("stateAbbr");

    try {
      Map<String, AggKey> header = parseHeader(response);
      if (header == null) {
        LOGGER.warn("forest_inventory: could not parse CSV header for stateAbbr={}", stateAbbr);
        return "[]";
      }

      int idxInvyr = colIdx(header, "INVYR");
      int idxStatecd = colIdx(header, "STATECD");
      int idxCondStatus = colIdx(header, "COND_STATUS_CD");
      int idxOwngrpcd = colIdx(header, "OWNGRPCD");
      int idxFortypcd = colIdx(header, "FORTYPCD");
      int idxCondprop = colIdx(header, "CONDPROP_UNADJ");
      int idxBalive = colIdx(header, "BALIVE");

      Map<String, double[]> agg = new LinkedHashMap<String, double[]>();

      BufferedReader reader = new BufferedReader(new StringReader(response));
      String line = reader.readLine(); // skip header
      int rowsRead = 0;
      int rowsKept = 0;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }
        rowsRead++;
        String[] cols = line.split(",", -1);

        int condStatus = intAt(cols, idxCondStatus);
        if (condStatus != 1) {
          continue;
        }
        rowsKept++;

        int invyr = intAt(cols, idxInvyr);
        int statecd = intAt(cols, idxStatecd);
        int owngrpcd = intAt(cols, idxOwngrpcd);
        int fortypcd = intAt(cols, idxFortypcd);
        double condprop = doubleAt(cols, idxCondprop);
        double balive = doubleAt(cols, idxBalive);

        String stateFips = statecd < 10 ? "0" + statecd : String.valueOf(statecd);
        String typGrp = resolveTypGrp(fortypcd);
        String ownGrp = resolveOwnGrp(owngrpcd);

        String key = invyr + "|" + stateFips + "|" + typGrp + "|" + ownGrp;
        double[] acc = agg.get(key);
        if (acc == null) {
          acc = new double[]{0.0, 0.0};
          agg.put(key, acc);
        }
        acc[0] += condprop;
        acc[1] += balive * condprop;
      }

      ArrayNode result = MAPPER.createArrayNode();
      for (Map.Entry<String, double[]> entry : agg.entrySet()) {
        String[] parts = entry.getKey().split("\\|", -1);
        double[] acc = entry.getValue();

        ObjectNode row = MAPPER.createObjectNode();
        row.put("inventory_year", Integer.parseInt(parts[0]));
        row.put("state_fips", parts[1]);
        row.put("forest_type_group", parts[2].equals("null") ? (String) null : parts[2]);
        row.put("ownership_class", parts[3].equals("null") ? (String) null : parts[3]);
        row.putNull("land_area_acres");
        row.putNull("live_volume_cuft");
        row.putNull("carbon_stock_tons");
        row.putNull("trees_per_acre");
        if (acc[0] > 0.0) {
          row.put("basal_area_sqft", acc[1] / acc[0]);
        } else {
          row.putNull("basal_area_sqft");
        }
        result.add(row);
      }

      LOGGER.info("forest_inventory: stateAbbr={} read={} kept={} groups={}",
          stateAbbr, rowsRead, rowsKept, result.size());
      return MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      LOGGER.error("forest_inventory: failed to transform COND CSV for stateAbbr={}: {}", stateAbbr, e.getMessage(), e);
      throw new RuntimeException("forest_inventory transform failed for " + stateAbbr, e);
    }
  }

  private Map<String, AggKey> parseHeader(String response) throws Exception {
    BufferedReader reader = new BufferedReader(new StringReader(response));
    String headerLine = reader.readLine();
    if (headerLine == null) {
      return null;
    }
    String[] cols = headerLine.split(",", -1);
    Map<String, AggKey> map = new HashMap<String, AggKey>();
    for (int i = 0; i < cols.length; i++) {
      map.put(cols[i].trim(), new AggKey(i));
    }
    return map;
  }

  private int colIdx(Map<String, AggKey> header, String name) {
    AggKey k = header.get(name);
    return k == null ? -1 : k.idx;
  }

  private int intAt(String[] cols, int idx) {
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

  private double doubleAt(String[] cols, int idx) {
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

  private static class AggKey {
    final int idx;
    AggKey(int idx) {
      this.idx = idx;
    }
  }
}
