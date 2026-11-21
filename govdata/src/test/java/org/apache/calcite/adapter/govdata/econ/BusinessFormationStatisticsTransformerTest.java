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
package org.apache.calcite.adapter.govdata.econ;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class BusinessFormationStatisticsTransformerTest {

  private static List<Map<String, Object>> melt(String line) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    String[] c = line.split(",", -1);
    BusinessFormationStatisticsTransformer.addMonths(c, Integer.parseInt(c[4]), rows);
    return rows;
  }

  @Test void nationalMonthsBecomeRowsAndBlanksAndFlagsAreSkipped() {
    List<Map<String, Object>> rows =
        melt("U,TOTAL,BA_BA,US,2026,548819,506166,S,D,NA,,635310,478271,582422,427667,,");
    assertEquals(6, rows.size());
    Map<String, Object> jan = rows.get(0);
    assertEquals("BA_BA", jan.get("series_code"));
    assertEquals(false, jan.get("seasonally_adjusted"));
    assertEquals("national", jan.get("geo_level"));
    assertEquals("00", jan.get("fips"));
    assertEquals("2026-01", jan.get("period"));
    assertEquals(548819.0, jan.get("value"));
    assertEquals(7, rows.get(2).get("month"));
  }

  @Test void regionHasNoFipsAndStateHasOne() {
    assertNull(melt("A,TOTAL,BA_HBA,MW,2025,1,,,,,,,,,,,").get(0).get("fips"));
    assertEquals("region", melt("A,TOTAL,BA_HBA,MW,2025,1,,,,,,,,,,,").get(0).get("geo_level"));
    Map<String, Object> ca = melt("A,TOTAL,BA_HBA,CA,2025,1,,,,,,,,,,,").get(0);
    assertEquals("state", ca.get("geo_level"));
    assertEquals("06", ca.get("fips"));
    assertEquals(true, ca.get("seasonally_adjusted"));
  }

  @Test void unknownGeoFailsLoudly() {
    assertThrows(IllegalStateException.class, () -> melt("U,TOTAL,BA_BA,ZZ,2025,1,,,,,,,,,,,"));
  }
}
