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
package org.apache.calcite.adapter.govdata.housing;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the tract aggregation of {@link HmdaLoanLevelAggregateProvider}. */
@Tag("unit")
class HmdaLoanLevelAggregateProviderTest {

  private static final String HEADER = "state_code,county_code,census_tract,action_taken,"
      + "loan_amount,interest_rate,tract_population,tract_minority_population_percent,"
      + "tract_to_msa_income_percentage";

  @TempDir File dir;

  private Map<String, Map<String, Object>> aggregate(String... lines) throws Exception {
    File csv = new File(dir, "hmda.csv");
    StringBuilder sb = new StringBuilder(HEADER).append('\n');
    for (String line : lines) {
      sb.append(line).append('\n');
    }
    Files.write(csv.toPath(), sb.toString().getBytes(StandardCharsets.UTF_8));
    List<Map<String, Object>> rows = new HmdaLoanLevelAggregateProvider().aggregate(csv, "2025");
    Map<String, Map<String, Object>> byTract = new HashMap<String, Map<String, Object>>();
    for (Map<String, Object> row : rows) {
      assertNull(byTract.put((String) row.get("census_tract"), row),
          "one row per census tract: " + row);
    }
    return byTract;
  }

  @Test void recordsWithoutATractAreNotAttributedToAnyTract() throws Exception {
    Map<String, Map<String, Object>> rows = aggregate(
        "TX,48141,48141001100,1,100000,6.5,4000,20.5,90",
        "TX,NA,NA,1,50000,NA,NA,NA,NA",
        "NA,NA,na,3,70000,NA,NA,NA,NA");
    assertEquals(1, rows.size());
    assertEquals(1L, rows.get("48141001100").get("application_count"));
  }

  @Test void recordWithMissingCountyJoinsItsTractRatherThanSplittingIt() throws Exception {
    Map<String, Map<String, Object>> rows = aggregate(
        "AL,01125,01125010405,1,200000,6.0,5000,10,100",
        "IL,NA,01125010405,3,100000,NA,5000,10,100",
        "AL,01125,01125010405,3,150000,NA,5000,10,100");
    assertEquals(1, rows.size());
    Map<String, Object> row = rows.get("01125010405");
    assertEquals(3L, row.get("application_count"));
    assertEquals(1L, row.get("origination_count"));
    assertEquals(2L, row.get("denial_count"));
    assertEquals("01125", row.get("county_fips"));
    assertEquals("AL", row.get("state_code"));
    assertEquals(2025, row.get("year"));
  }

  @Test void stateIsNullWhenNoRecordReportsTheTractsCounty() throws Exception {
    Map<String, Map<String, Object>> rows = aggregate(
        "TX,NA,48141001100,1,100000,6.5,4000,20.5,90");
    Map<String, Object> row = rows.get("48141001100");
    assertNotNull(row);
    assertEquals("48141", row.get("county_fips"));
    assertNull(row.get("state_code"));
    assertTrue(((Number) row.get("origination_total_amount")).doubleValue() == 100000d);
  }
}
