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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Quarter discovery against the shape data.cms.gov's data.json has today. */
@Tag("unit")
class CmsPbjNurseStaffingProviderTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static String dataset(String title, String start, String... urls) {
    StringBuilder d = new StringBuilder();
    for (String u : urls) {
      if (d.length() > 0) {
        d.append(',');
      }
      d.append("{\"downloadURL\":\"").append(u).append("\",\"temporal\":[{\"@type\":\"PeriodOfTime\","
          + "\"startDate\":\"").append(start).append("\",\"endDate\":\"x\"}]}");
    }
    return "{\"title\":\"" + title + "\",\"distribution\":[" + d + "]}";
  }

  private static List<CmsPbjNurseStaffingProvider.QuarterFile> parse(String... datasets)
      throws Exception {
    JsonNode catalog = MAPPER.readTree("{\"dataset\":[" + String.join(",", datasets) + "]}");
    return CmsPbjNurseStaffingProvider.quartersIn(catalog);
  }

  @Test void eachQuarterIsItsOwnDatasetWithADateSuffixedTitle() throws Exception {
    List<CmsPbjNurseStaffingProvider.QuarterFile> q = parse(
        dataset("Payroll Based Journal Daily Nurse Staffing : 2025-10-01", "2025-10-01", "http://h/q4.csv"),
        dataset("Payroll Based Journal Daily Nurse Staffing : 2026-01-01", "2026-01-01", "", "http://h/q1.csv"),
        dataset("Payroll Based Journal Daily Non-Nurse Staffing : 2026-01-01", "2026-01-01", "http://h/other.csv"),
        dataset("Payroll Based Journal Employee Detail Nursing Home Staffing : 2026-01-01", "2026-01-01", "http://h/emp.csv"));
    assertEquals(2, q.size());
    assertEquals("http://h/q1.csv", q.get(0).csvUrl); // newest first
    assertEquals(2026, q.get(0).year);
    assertEquals(1, q.get(0).quarter);
    assertEquals(4, q.get(1).quarter);
  }

  @Test void stringTemporalAndPlainTitleStillWork() throws Exception {
    List<CmsPbjNurseStaffingProvider.QuarterFile> q = parse(
        "{\"title\":\"Payroll Based Journal Daily Nurse Staffing\",\"distribution\":[{"
            + "\"downloadURL\":\"http://h/a.csv\",\"temporal\":\"2024-04-01/2024-06-30\"}]}");
    assertEquals(1, q.size());
    assertEquals(2, q.get(0).quarter);
  }

  @Test void aSecondDistributionForTheSameQuarterIsNotLoadedTwice() throws Exception {
    List<CmsPbjNurseStaffingProvider.QuarterFile> q = parse(
        dataset("Payroll Based Journal Daily Nurse Staffing : 2025-07-01", "2025-07-01", "http://h/a.csv"),
        dataset("Payroll Based Journal Daily Nurse Staffing : 2025-07-02", "2025-07-01", "http://h/b.csv"));
    assertEquals(1, q.size());
    assertEquals("http://h/a.csv", q.get(0).csvUrl);
  }

  @Test void unrelatedCatalogYieldsNothing() throws Exception {
    assertTrue(parse(dataset("Something else", "2025-01-01", "http://h/x.csv")).isEmpty());
  }
}
