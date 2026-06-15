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
package org.apache.calcite.adapter.govdata.edu;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps NAEP API jurisdiction codes to integer FIPS codes for storage.
 *
 * <p>The NAEP GetAdhocData API uses 2-letter postal abbreviations for states and
 * "NP" for the national-public aggregate. This utility converts those codes to
 * standard FIPS integers so {@code naep_scores} and {@code naep_achievement_levels}
 * rows can join to {@code geo.states} on {@code state_fips}.
 */
final class NaepJurisdictions {

  /** Full comma-separated jurisdiction list for use in NAEP API URLs. */
  static final String ALL_JURISDICTIONS =
      "NP,AL,AK,AZ,AR,CA,CO,CT,DE,DC,FL,GA,HI,ID,IL,IN,IA,KS,KY,LA,"
      + "ME,MD,MA,MI,MN,MS,MO,MT,NE,NV,NH,NJ,NM,NY,NC,ND,OH,OK,OR,PA,"
      + "RI,SC,SD,TN,TX,UT,VT,VA,WA,WV,WI,WY";

  private static final Map<String, Integer> ABBR_TO_FIPS = new HashMap<String, Integer>();

  static {
    ABBR_TO_FIPS.put("AL", 1);
    ABBR_TO_FIPS.put("AK", 2);
    ABBR_TO_FIPS.put("AZ", 4);
    ABBR_TO_FIPS.put("AR", 5);
    ABBR_TO_FIPS.put("CA", 6);
    ABBR_TO_FIPS.put("CO", 8);
    ABBR_TO_FIPS.put("CT", 9);
    ABBR_TO_FIPS.put("DE", 10);
    ABBR_TO_FIPS.put("DC", 11);
    ABBR_TO_FIPS.put("FL", 12);
    ABBR_TO_FIPS.put("GA", 13);
    ABBR_TO_FIPS.put("HI", 15);
    ABBR_TO_FIPS.put("ID", 16);
    ABBR_TO_FIPS.put("IL", 17);
    ABBR_TO_FIPS.put("IN", 18);
    ABBR_TO_FIPS.put("IA", 19);
    ABBR_TO_FIPS.put("KS", 20);
    ABBR_TO_FIPS.put("KY", 21);
    ABBR_TO_FIPS.put("LA", 22);
    ABBR_TO_FIPS.put("ME", 23);
    ABBR_TO_FIPS.put("MD", 24);
    ABBR_TO_FIPS.put("MA", 25);
    ABBR_TO_FIPS.put("MI", 26);
    ABBR_TO_FIPS.put("MN", 27);
    ABBR_TO_FIPS.put("MS", 28);
    ABBR_TO_FIPS.put("MO", 29);
    ABBR_TO_FIPS.put("MT", 30);
    ABBR_TO_FIPS.put("NE", 31);
    ABBR_TO_FIPS.put("NV", 32);
    ABBR_TO_FIPS.put("NH", 33);
    ABBR_TO_FIPS.put("NJ", 34);
    ABBR_TO_FIPS.put("NM", 35);
    ABBR_TO_FIPS.put("NY", 36);
    ABBR_TO_FIPS.put("NC", 37);
    ABBR_TO_FIPS.put("ND", 38);
    ABBR_TO_FIPS.put("OH", 39);
    ABBR_TO_FIPS.put("OK", 40);
    ABBR_TO_FIPS.put("OR", 41);
    ABBR_TO_FIPS.put("PA", 42);
    ABBR_TO_FIPS.put("RI", 44);
    ABBR_TO_FIPS.put("SC", 45);
    ABBR_TO_FIPS.put("SD", 46);
    ABBR_TO_FIPS.put("TN", 47);
    ABBR_TO_FIPS.put("TX", 48);
    ABBR_TO_FIPS.put("UT", 49);
    ABBR_TO_FIPS.put("VT", 50);
    ABBR_TO_FIPS.put("VA", 51);
    ABBR_TO_FIPS.put("WA", 53);
    ABBR_TO_FIPS.put("WV", 54);
    ABBR_TO_FIPS.put("WI", 55);
    ABBR_TO_FIPS.put("WY", 56);
  }

  private NaepJurisdictions() {
  }

  /**
   * Maps a NAEP jurisdiction code to an integer FIPS code.
   *
   * @param jurisdiction NAEP code: "NP" for national-public, 2-letter state abbreviation otherwise
   * @return 0 for the national-public (NP) aggregate, the state FIPS integer for known state codes
   * @throws IllegalArgumentException if the code has no FIPS mapping (unknown jurisdiction)
   */
  static int toFips(String jurisdiction) {
    if (jurisdiction == null || "NP".equals(jurisdiction)) {
      // 0 is reserved for the national-public (NP) aggregate, not a sentinel for "unknown".
      return 0;
    }
    Integer fips = ABBR_TO_FIPS.get(jurisdiction.toUpperCase());
    if (fips == null) {
      // Do NOT fall back to 0 here: that would collide with the NP aggregate and corrupt
      // the geo.states join. An unmapped code is a contract violation, not a data value.
      throw new IllegalArgumentException(
          "Unknown NAEP jurisdiction code (no FIPS mapping): " + jurisdiction);
    }
    return fips;
  }
}
