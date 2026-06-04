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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Shared FIA reference-code lookup tables used by lands transformers.
 *
 * <p>FORTYPCD → forest-type-group code → human-readable group name.
 * OWNGRPCD → ownership-group name.
 * Sourced from the FIADB Phase 2 User Guide reference tables.
 */
final class FiaLookups {

  private FiaLookups() {
  }

  /** Maps individual FORTYPCD codes to their FIA-defined type-group code. */
  static final Map<Integer, Integer> FORTYPCD_TO_TYPGRPCD;

  /** Maps type-group code to its display name. */
  static final Map<Integer, String> TYPGRP_NAME;

  /** Maps ownership-group code (OWNGRPCD) to its display name. */
  static final Map<Integer, String> OWNGRP_NAME;

  static {
    Map<Integer, Integer> fortyp = new HashMap<Integer, Integer>();
    fortyp.put(100, 100); fortyp.put(101, 100); fortyp.put(102, 100);
    fortyp.put(103, 100); fortyp.put(104, 100); fortyp.put(105, 100);
    fortyp.put(120, 120); fortyp.put(121, 120); fortyp.put(122, 120);
    fortyp.put(123, 120); fortyp.put(124, 120); fortyp.put(125, 120);
    fortyp.put(126, 120); fortyp.put(127, 120); fortyp.put(128, 120);
    fortyp.put(129, 120); fortyp.put(140, 140); fortyp.put(141, 140);
    fortyp.put(142, 140); fortyp.put(150, 150); fortyp.put(151, 150);
    fortyp.put(160, 160); fortyp.put(161, 160); fortyp.put(162, 160);
    fortyp.put(163, 160); fortyp.put(164, 160); fortyp.put(165, 160);
    fortyp.put(166, 160); fortyp.put(167, 160); fortyp.put(168, 160);
    fortyp.put(170, 170); fortyp.put(171, 170); fortyp.put(172, 170);
    fortyp.put(180, 180); fortyp.put(181, 180); fortyp.put(182, 180);
    fortyp.put(183, 180); fortyp.put(184, 180); fortyp.put(185, 180);
    fortyp.put(200, 200); fortyp.put(201, 200); fortyp.put(202, 200);
    fortyp.put(203, 200); fortyp.put(220, 220); fortyp.put(221, 220);
    fortyp.put(222, 220); fortyp.put(223, 220); fortyp.put(224, 220);
    fortyp.put(225, 220); fortyp.put(226, 220); fortyp.put(240, 240);
    fortyp.put(241, 240); fortyp.put(260, 260); fortyp.put(261, 260);
    fortyp.put(262, 260); fortyp.put(263, 260); fortyp.put(264, 260);
    fortyp.put(265, 260); fortyp.put(266, 260); fortyp.put(267, 260);
    fortyp.put(268, 260); fortyp.put(269, 260); fortyp.put(270, 260);
    fortyp.put(271, 260); fortyp.put(280, 280); fortyp.put(281, 280);
    fortyp.put(300, 300); fortyp.put(301, 300); fortyp.put(304, 300);
    fortyp.put(305, 300); fortyp.put(320, 320); fortyp.put(321, 320);
    fortyp.put(340, 340); fortyp.put(341, 340); fortyp.put(342, 340);
    fortyp.put(360, 360); fortyp.put(361, 360); fortyp.put(362, 360);
    fortyp.put(363, 360); fortyp.put(364, 360); fortyp.put(365, 360);
    fortyp.put(366, 360); fortyp.put(367, 360); fortyp.put(368, 360);
    fortyp.put(369, 360); fortyp.put(370, 370); fortyp.put(371, 370);
    fortyp.put(380, 380); fortyp.put(381, 380); fortyp.put(382, 380);
    fortyp.put(383, 380); fortyp.put(384, 380); fortyp.put(385, 380);
    fortyp.put(390, 390); fortyp.put(391, 390);
    fortyp.put(400, 400); fortyp.put(401, 400); fortyp.put(402, 400);
    fortyp.put(403, 400); fortyp.put(404, 400); fortyp.put(405, 400);
    fortyp.put(406, 400); fortyp.put(407, 400); fortyp.put(409, 400);
    fortyp.put(500, 500); fortyp.put(501, 500); fortyp.put(502, 500);
    fortyp.put(503, 500); fortyp.put(504, 500); fortyp.put(505, 500);
    fortyp.put(506, 500); fortyp.put(507, 500); fortyp.put(508, 500);
    fortyp.put(509, 500); fortyp.put(510, 500); fortyp.put(511, 500);
    fortyp.put(512, 500); fortyp.put(513, 500); fortyp.put(514, 500);
    fortyp.put(515, 500); fortyp.put(516, 500); fortyp.put(517, 500);
    fortyp.put(519, 500); fortyp.put(520, 500);
    fortyp.put(600, 600); fortyp.put(601, 600); fortyp.put(602, 600);
    fortyp.put(605, 600); fortyp.put(606, 600); fortyp.put(607, 600);
    fortyp.put(608, 600); fortyp.put(609, 600);
    fortyp.put(700, 700); fortyp.put(701, 700); fortyp.put(702, 700);
    fortyp.put(703, 700); fortyp.put(704, 700); fortyp.put(705, 700);
    fortyp.put(706, 700); fortyp.put(707, 700); fortyp.put(708, 700);
    fortyp.put(709, 700); fortyp.put(722, 700);
    fortyp.put(800, 800); fortyp.put(801, 800); fortyp.put(802, 800);
    fortyp.put(803, 800); fortyp.put(805, 800); fortyp.put(807, 800);
    fortyp.put(809, 800);
    fortyp.put(900, 900); fortyp.put(901, 900); fortyp.put(902, 900);
    fortyp.put(903, 900); fortyp.put(904, 900); fortyp.put(905, 900);
    fortyp.put(910, 910); fortyp.put(911, 910); fortyp.put(912, 910);
    fortyp.put(920, 920); fortyp.put(921, 920); fortyp.put(922, 920);
    fortyp.put(923, 920); fortyp.put(924, 920); fortyp.put(925, 920);
    fortyp.put(926, 920); fortyp.put(931, 920); fortyp.put(932, 920);
    fortyp.put(933, 920); fortyp.put(934, 920); fortyp.put(935, 920);
    fortyp.put(940, 940); fortyp.put(941, 940); fortyp.put(942, 940);
    fortyp.put(943, 940); fortyp.put(950, 950); fortyp.put(951, 950);
    fortyp.put(952, 950); fortyp.put(953, 950); fortyp.put(954, 950);
    fortyp.put(955, 950); fortyp.put(960, 960); fortyp.put(961, 960);
    fortyp.put(962, 960); fortyp.put(970, 970); fortyp.put(971, 970);
    fortyp.put(972, 970); fortyp.put(973, 970); fortyp.put(974, 970);
    fortyp.put(975, 970); fortyp.put(976, 970);
    fortyp.put(980, 980); fortyp.put(981, 980); fortyp.put(982, 980);
    fortyp.put(983, 980); fortyp.put(984, 980); fortyp.put(985, 980);
    fortyp.put(986, 980); fortyp.put(987, 980); fortyp.put(988, 980);
    fortyp.put(989, 980); fortyp.put(990, 990); fortyp.put(991, 990);
    fortyp.put(992, 990); fortyp.put(993, 990); fortyp.put(995, 990);
    fortyp.put(999, 999);
    FORTYPCD_TO_TYPGRPCD = Collections.unmodifiableMap(fortyp);

    Map<Integer, String> typgrp = new HashMap<Integer, String>();
    typgrp.put(100, "White / red / jack pine group");
    typgrp.put(120, "Spruce / fir group");
    typgrp.put(140, "Longleaf / slash pine group");
    typgrp.put(150, "Tropical softwoods group");
    typgrp.put(160, "Loblolly / shortleaf pine group");
    typgrp.put(170, "Other eastern softwoods group");
    typgrp.put(180, "Pinyon / juniper group");
    typgrp.put(200, "Douglas-fir group");
    typgrp.put(220, "Ponderosa pine group");
    typgrp.put(240, "Western white pine group");
    typgrp.put(260, "Fir / spruce / mountain hemlock group");
    typgrp.put(280, "Lodgepole pine group");
    typgrp.put(300, "Hemlock / Sitka spruce group");
    typgrp.put(320, "Western larch group");
    typgrp.put(340, "Redwood group");
    typgrp.put(360, "Other western softwoods group");
    typgrp.put(370, "California mixed conifer group");
    typgrp.put(380, "Exotic softwoods group");
    typgrp.put(390, "Other softwoods group");
    typgrp.put(400, "Oak / pine group");
    typgrp.put(500, "Oak / hickory group");
    typgrp.put(600, "Oak / gum / cypress group");
    typgrp.put(700, "Elm / ash / cottonwood group");
    typgrp.put(800, "Maple / beech / birch group");
    typgrp.put(900, "Aspen / birch group");
    typgrp.put(910, "Alder / maple group");
    typgrp.put(920, "Western oak group");
    typgrp.put(940, "Tanoak / laurel group");
    typgrp.put(950, "Other western hardwoods group");
    typgrp.put(960, "Other hardwoods group");
    typgrp.put(970, "Woodland hardwoods group");
    typgrp.put(980, "Tropical hardwoods group");
    typgrp.put(990, "Exotic hardwoods group");
    typgrp.put(999, "Nonstocked");
    TYPGRP_NAME = Collections.unmodifiableMap(typgrp);

    Map<Integer, String> own = new HashMap<Integer, String>();
    own.put(10, "National Forest");
    own.put(20, "Other Federal");
    own.put(30, "State");
    own.put(40, "Local");
    own.put(50, "Private");
    OWNGRP_NAME = Collections.unmodifiableMap(own);
  }

  /** Resolves a FORTYPCD to its display group name, or null if unknown. */
  static String resolveTypGrp(int fortypcd) {
    if (fortypcd <= 0) {
      return null;
    }
    Integer typgrpcd = FORTYPCD_TO_TYPGRPCD.get(fortypcd);
    if (typgrpcd == null) {
      return null;
    }
    return TYPGRP_NAME.get(typgrpcd);
  }

  /** Resolves an OWNGRPCD to its display name, or null if unknown. */
  static String resolveOwnGrp(int owngrpcd) {
    if (owngrpcd <= 0) {
      return null;
    }
    return OWNGRP_NAME.get(owngrpcd);
  }

  /** Converts STATECD to a zero-padded 2-char FIPS string. */
  static String stateFips(int statecd) {
    return statecd < 10 ? "0" + statecd : String.valueOf(statecd);
  }

  /** USPS state code → FIPS code, covering the 50 states + 5 territories that FIA
   *  publishes per-state CSV archives for. DC has no FIA archive (no forest land). */
  private static final Map<String, String> ABBR_TO_FIPS;
  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("AL", "01"); m.put("AK", "02"); m.put("AZ", "04"); m.put("AR", "05");
    m.put("CA", "06"); m.put("CO", "08"); m.put("CT", "09"); m.put("DE", "10");
    m.put("FL", "12"); m.put("GA", "13"); m.put("HI", "15"); m.put("ID", "16");
    m.put("IL", "17"); m.put("IN", "18"); m.put("IA", "19"); m.put("KS", "20");
    m.put("KY", "21"); m.put("LA", "22"); m.put("ME", "23"); m.put("MD", "24");
    m.put("MA", "25"); m.put("MI", "26"); m.put("MN", "27"); m.put("MS", "28");
    m.put("MO", "29"); m.put("MT", "30"); m.put("NE", "31"); m.put("NV", "32");
    m.put("NH", "33"); m.put("NJ", "34"); m.put("NM", "35"); m.put("NY", "36");
    m.put("NC", "37"); m.put("ND", "38"); m.put("OH", "39"); m.put("OK", "40");
    m.put("OR", "41"); m.put("PA", "42"); m.put("RI", "44"); m.put("SC", "45");
    m.put("SD", "46"); m.put("TN", "47"); m.put("TX", "48"); m.put("UT", "49");
    m.put("VT", "50"); m.put("VA", "51"); m.put("WA", "53"); m.put("WV", "54");
    m.put("WI", "55"); m.put("WY", "56");
    m.put("PR", "72"); m.put("GU", "66"); m.put("VI", "78");
    m.put("AS", "60"); m.put("MP", "69");
    ABBR_TO_FIPS = Collections.unmodifiableMap(m);
  }

  /** Returns the 2-char FIPS code for a USPS state abbreviation, or throws if unknown. */
  static String stateFipsForAbbr(String abbr) {
    if (abbr == null) {
      throw new IllegalArgumentException("stateFipsForAbbr: abbr is null");
    }
    String fips = ABBR_TO_FIPS.get(abbr.trim().toUpperCase(java.util.Locale.ROOT));
    if (fips == null) {
      throw new IllegalArgumentException("Unknown USPS state code: " + abbr);
    }
    return fips;
  }
}
