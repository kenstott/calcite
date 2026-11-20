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
package org.apache.calcite.adapter.govdata.health;

import java.util.HashMap;
import java.util.Map;

/** State name to USPS abbreviation for the 50 states. */
final class HealthStateCodes {
  private static final Map<String, String> ABBR = new HashMap<String, String>();

  static {
    String[] p = {
        "Alabama", "AL", "Alaska", "AK", "Arizona", "AZ", "Arkansas", "AR", "California", "CA",
        "Colorado", "CO", "Connecticut", "CT", "Delaware", "DE", "Florida", "FL",
        "Georgia", "GA", "Hawaii", "HI", "Idaho", "ID", "Illinois", "IL",
        "Indiana", "IN", "Iowa", "IA", "Kansas", "KS", "Kentucky", "KY", "Louisiana", "LA",
        "Maine", "ME", "Maryland", "MD", "Massachusetts", "MA", "Michigan", "MI",
        "Minnesota", "MN", "Mississippi", "MS", "Missouri", "MO", "Montana", "MT",
        "Nebraska", "NE", "Nevada", "NV", "New Hampshire", "NH", "New Jersey", "NJ",
        "New Mexico", "NM", "New York", "NY", "North Carolina", "NC", "North Dakota", "ND",
        "Ohio", "OH", "Oklahoma", "OK", "Oregon", "OR", "Pennsylvania", "PA",
        "Rhode Island", "RI", "South Carolina", "SC", "South Dakota", "SD", "Tennessee", "TN",
        "Texas", "TX", "Utah", "UT", "Vermont", "VT", "Virginia", "VA", "Washington", "WA",
        "West Virginia", "WV", "Wisconsin", "WI", "Wyoming", "WY"};
    for (int i = 0; i < p.length; i += 2) {
      ABBR.put(p[i], p[i + 1]);
    }
  }

  private HealthStateCodes() {
  }

  static String abbr(String name) {
    return ABBR.get(name);
  }
}
