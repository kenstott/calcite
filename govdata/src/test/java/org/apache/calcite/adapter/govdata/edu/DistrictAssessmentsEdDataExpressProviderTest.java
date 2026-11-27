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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the ED Data Express parsing of {@link DistrictAssessmentsEdDataExpressProvider}. */
@Tag("unit")
class DistrictAssessmentsEdDataExpressProviderTest {

  private static final String HEADER = "\"School Year\",State,\"NCES LEA ID\",LEA,School,"
      + "\"NCES SCH ID\",\"Data Group\",\"Data Description\",Value,Numerator,Denominator,"
      + "Population,Subgroup,Characteristics,Age/Grade,\"Academic Subject\",Outcome,"
      + "\"Program Type\"\n";

  private static String line(String value, String subgroup, String grade, String subject) {
    return "2021-2022,ALABAMA,0100005,\"Albertville City\",,,584,\"Performance\"," + value
        + ",,454,\"All Students\",\"" + subgroup + "\",,\"" + grade + "\",\"" + subject
        + "\",\"Percent Proficient\",\n";
  }

  private static Map<Integer, List<Map<String, Object>>> parse(String body) throws IOException {
    return DistrictAssessmentsEdDataExpressProvider.parse(
        new BufferedReader(new StringReader(HEADER + body)), 2021);
  }

  private static void assertValue(String cell, int low, int high, double mid) throws IOException {
    assertArrayEquals(new Number[] {low, high, mid},
        DistrictAssessmentsEdDataExpressProvider.proficiency(cell), cell);
  }

  @Test void convertsEveryPublishedValueForm() throws IOException {
    assertValue("35%", 35, 35, 35.0);
    assertValue("30-34%", 30, 34, 32.0);
    assertValue("<50%", 0, 49, 24.5);
    assertValue("<=10%", 0, 10, 5.0);
    assertValue(">=50%", 50, 100, 75.0);
    assertValue(">95%", 96, 100, 98.0);
    assertValue("S", -3, -3, -3.0);
    assertValue("MISSING", -1, -1, -1.0);
  }

  @Test void rejectsUnrecognisedValue() {
    assertThrows(IOException.class,
        () -> DistrictAssessmentsEdDataExpressProvider.proficiency("n/a"));
    assertThrows(IOException.class,
        () -> DistrictAssessmentsEdDataExpressProvider.proficiency("40-30%"));
  }

  @Test void keepsAllStudentsByGradeWithHighSchoolAsNine() throws IOException {
    Map<Integer, List<Map<String, Object>>> rows = parse(
        line("35%", "All Students in LEA", "All Grades", "Reading/Language Arts")
            + line("42%", "All Students in LEA", "Grade 3", "Reading/Language Arts")
            + line("50%", "Homeless", "Grade 3", "Reading/Language Arts")
            + line("18%", "All Students in LEA", "High School", "Reading/Language Arts"));
    assertEquals(new java.util.HashSet<Integer>(Arrays.asList(3, 9)), rows.keySet());
    assertEquals(1, rows.get(3).size());
    Map<String, Object> row = rows.get(3).get(0);
    Map<String, Object> expected = new HashMap<String, Object>();
    expected.put("leaid", "0100005");
    expected.put("year", 2021);
    expected.put("grade", 3);
    expected.put("fips", 1);
    expected.put("read_test_num_valid", 454);
    expected.put("read_test_pct_prof_midpt", 42.0);
    for (Map.Entry<String, Object> e : expected.entrySet()) {
      assertEquals(e.getValue(), row.get(e.getKey()), e.getKey());
    }
    assertNull(row.get("math_test_pct_prof_midpt"));
  }

  @Test void rejectsAnotherSubject() {
    assertThrows(IOException.class,
        () -> parse(line("35%", "All Students in LEA", "Grade 3", "Mathematics")));
  }
}
