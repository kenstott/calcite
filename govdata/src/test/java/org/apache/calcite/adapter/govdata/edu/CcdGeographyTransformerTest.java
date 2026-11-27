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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests geography identifier handling in the CCD district and school transformers. */
@Tag("unit")
class CcdGeographyTransformerTest {

  @Test void districtMissingCodesBecomeNull() {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("county_code", "-2");
    row.put("county_name", "N");
    row.put("cbsa", -2);
    row.put("congress_district_id", -2);
    row.put("state_leg_district_lower", "-2");
    row.put("state_leg_district_upper", "-2");
    row.put("guidance_counselors_total_fte", -1);

    new CcdDistrictsResponseTransformer().transformRecord(row, null);

    assertTrue(row.containsKey("county_code"));
    assertNull(row.get("county_code"));
    assertNull(row.get("county_name"));
    assertNull(row.get("cbsa"));
    assertNull(row.get("congress_district_id"));
    assertNull(row.get("state_leg_district_lower"));
    assertNull(row.get("state_leg_district_upper"));
    // Statistics keep their NCES codes in the base table.
    assertEquals(-1, row.get("guidance_counselors_total_fte"));
  }

  @Test void districtIdentifiersArePreservedAndCountyIsPadded() {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("county_code", 1073);
    row.put("county_name", "Jefferson County");
    row.put("cbsa", 13820);
    row.put("congress_district_id", 107);
    row.put("state_leg_district_lower", "01044");

    new CcdDistrictsResponseTransformer().transformRecord(row, null);

    assertEquals("01073", row.get("county_code"));
    assertEquals("Jefferson County", row.get("county_name"));
    assertEquals(13820, row.get("cbsa"));
    assertEquals(107, row.get("congress_district_id"));
    assertEquals("01044", row.get("state_leg_district_lower"));
  }

  @Test void schoolMissingCodesBecomeNullAndGradeCodesRemain() {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("county_code", "-1");
    row.put("cbsa", "-2");
    row.put("state_leg_district_upper", "-2");
    row.put("lowest_grade_offered", -1);

    new CcdSchoolsResponseTransformer().transformRecord(row, null);

    assertNull(row.get("county_code"));
    assertNull(row.get("cbsa"));
    assertNull(row.get("state_leg_district_upper"));
    assertEquals(-1, row.get("lowest_grade_offered"));
  }

  @Test void absentCountyStaysAbsent() {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("county_code", null);

    new CcdSchoolsResponseTransformer().transformRecord(row, null);

    assertNull(row.get("county_code"));
  }
}
