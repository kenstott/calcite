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
package org.apache.calcite.adapter.govdata.weather;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** nClimDiv statewide / regional / national area code table (state-readme.txt). */
final class NclimdivAreaCodes {

  /** area_type for the 50 states (codes 001-050). */
  static final String STATE = "state";
  /** area_type for the nine NCEI climate regions (101-109). */
  static final String REGION = "region";
  /** area_type for the contiguous-US national series (110). */
  static final String NATIONAL = "national";
  /** area_type for NWS regions, river basins and agricultural belts (111-465). */
  static final String SPECIAL = "special";

  private static final Map<String, String> NAMES;

  static {
    String[] states = {"Alabama", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
        "Delaware", "Florida", "Georgia", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
        "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
        "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
        "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas",
        "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
        "Hawaii", "Alaska"};
    Map<String, String> m = new LinkedHashMap<>();
    for (int i = 0; i < states.length; i++) {
      m.put(String.format("%03d", i + 1), states[i]);
    }
    m.put("101", "Northeast Region");
    m.put("102", "East North Central Region");
    m.put("103", "Central Region");
    m.put("104", "Southeast Region");
    m.put("105", "West North Central Region");
    m.put("106", "South Region");
    m.put("107", "Southwest Region");
    m.put("108", "Northwest Region");
    m.put("109", "West Region");
    m.put("110", "National (contiguous 48 States)");
    m.put("111", "Great Plains");
    m.put("115", "Southern Plains and Gulf Coast");
    m.put("120", "US Rockies and Westward");
    m.put("121", "NWS Eastern Region");
    m.put("122", "NWS Southern Region");
    m.put("123", "NWS Central Region");
    m.put("124", "NWS Western Region");
    m.put("201", "Pacific Northwest Basin");
    m.put("202", "California River Basin");
    m.put("203", "Great Basin");
    m.put("204", "Lower Colorado River Basin");
    m.put("205", "Upper Colorado River Basin");
    m.put("206", "Rio Grande River Basin");
    m.put("207", "Texas Gulf Coast River Basin");
    m.put("208", "Arkansas-White-Red Basin");
    m.put("209", "Lower Mississippi River Basin");
    m.put("210", "Missouri River Basin");
    m.put("211", "Souris-Red-Rainy Basin");
    m.put("212", "Upper Mississippi River Basin");
    m.put("213", "Great Lakes Basin");
    m.put("214", "Tennessee River Basin");
    m.put("215", "Ohio River Basin");
    m.put("216", "South Atlantic-Gulf Basin");
    m.put("217", "Mid-Atlantic Basin");
    m.put("218", "New England Basin");
    m.put("220", "Mississippi River Basin and Tributaries (N. of Memphis, TN)");
    m.put("250", "Spring Wheat Belt (area weighted)");
    m.put("255", "Primary Hard Red Winter Wheat Belt (area weighted)");
    m.put("256", "Winter Wheat Belt (area weighted)");
    m.put("260", "Primary Corn and Soybean Belt (area weighted)");
    m.put("261", "Corn Belt (area weighted)");
    m.put("262", "Soybean Belt (area weighted)");
    m.put("265", "Cotton Belt (area weighted)");
    m.put("350", "Spring Wheat Belt (productivity weighted)");
    m.put("356", "Winter Wheat Belt (productivity weighted)");
    m.put("361", "Corn Belt (productivity weighted)");
    m.put("362", "Soybean Belt (productivity weighted)");
    m.put("365", "Cotton Belt (productivity weighted)");
    NAMES = Collections.unmodifiableMap(m);
  }

  private NclimdivAreaCodes() {
  }

  /** Name for a 3-digit area code; throws for a code the readme does not define. */
  static String name(String code) {
    String n = NAMES.get(code);
    if (n == null) {
      throw new IllegalStateException("nClimDiv: unknown area code '" + code + "'");
    }
    return n;
  }

  static String type(String code) {
    int c = Integer.parseInt(code);
    if (c <= 50) {
      return STATE;
    }
    if (c <= 109) {
      return REGION;
    }
    return c == 110 ? NATIONAL : SPECIAL;
  }
}
