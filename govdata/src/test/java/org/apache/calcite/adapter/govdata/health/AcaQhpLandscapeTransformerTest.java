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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Benchmark and participation aggregation for the CMS QHP landscape rows. */
@Tag("unit")
class AcaQhpLandscapeTransformerTest {

  private static final List<String> HEADER = Arrays.asList("State Code", "FIPS County Code",
      "County Name", "Metal Level", "Issuer Name", "HIOS Issuer ID",
      "Plan ID (Standard Component)", "Rating Area", "Premium Adult Individual Age 21",
      "Premium Adult Individual Age 40");

  private static List<String> plan(String fips, String metal, String issuer, String planId,
      String p21, String p40) {
    return Arrays.asList("AK", fips, "County " + fips, metal, "Name", issuer, planId,
        "Rating Area 2", p21, p40);
  }

  private static AcaQhpLandscapeTransformer.Aggregator feed(List<String>... rows) {
    AcaQhpLandscapeTransformer.Aggregator agg = new AcaQhpLandscapeTransformer.Aggregator(2027);
    agg.accept(Arrays.asList("90939", "displayed records"));
    agg.accept(HEADER);
    for (List<String> r : rows) {
      agg.accept(r);
    }
    return agg;
  }

  @SuppressWarnings("unchecked")
  @Test void benchmarkIsSecondLowestSilver() {
    List<Map<String, Object>> out = feed(
        plan("02013", "Silver", "1", "P1", "$900.00", "$1,300.00"),
        plan("02013", "Silver", "2", "P2", "$800.00", "$1,100.00"),
        plan("02013", "Silver", "2", "P3", "$1,000.00", "$1,500.00"),
        plan("02013", "Gold", "1", "P4", "$1,100.00", "$1,600.00"),
        plan("02013", "Expanded Bronze", "3", "P5", "$600.00", "$850.00"),
        plan("02013", "Bronze", "3", "P6", "$650.00", "$900.00")).finish("t");
    assertEquals(1, out.size());
    Map<String, Object> r = out.get(0);
    assertEquals(2027, r.get("plan_year"));
    assertEquals(900.0, r.get("benchmark_silver_premium_age21"));
    assertEquals(1300.0, r.get("benchmark_silver_premium_age40"));
    assertEquals(800.0, r.get("lowest_silver_premium_age21"));
    assertEquals(600.0, r.get("lowest_bronze_premium_age21"));
    assertEquals(1100.0, r.get("lowest_gold_premium_age21"));
    assertEquals(6, r.get("plan_count"));
    assertEquals(3, r.get("silver_plan_count"));
    assertEquals(3, r.get("issuer_count"));
    assertEquals(2, r.get("silver_issuer_count"));
  }

  @Test void tiedLowestSilverPlansMakeBenchmarkEqualLowest() {
    List<Map<String, Object>> out = feed(
        plan("02013", "Silver", "1", "P1", "$800.00", "$1,100.00"),
        plan("02013", "Silver", "2", "P2", "$800.00", "$1,100.00"),
        plan("02013", "Silver", "3", "P3", "$950.00", "$1,300.00")).finish("t");
    assertEquals(800.0, out.get(0).get("benchmark_silver_premium_age21"));
  }

  @Test void oneSilverPlanHasNoBenchmark() {
    List<Map<String, Object>> out = feed(
        plan("02013", "Silver", "1", "P1", "$800.00", "$1,100.00"),
        plan("02013", "Gold", "1", "P2", "$900.00", "$1,200.00")).finish("t");
    assertNull(out.get(0).get("benchmark_silver_premium_age21"));
    assertEquals(800.0, out.get(0).get("lowest_silver_premium_age21"));
  }

  @Test void childOnlyListingWithNullPremiumIsNotABenchmarkCandidate() {
    List<Map<String, Object>> out = feed(
        plan("02013", "Silver", "1", "P1", "$800.00", "$1,100.00"),
        plan("02013", "Silver", "2", "P2", "NULL", "NULL"),
        plan("02013", "Silver", "3", "P3", "$950.00", "$1,300.00")).finish("t");
    assertEquals(950.0, out.get(0).get("benchmark_silver_premium_age21"));
    assertEquals(3, out.get(0).get("silver_plan_count"));
  }

  @Test void samePlanListedTwiceCountsOnce() {
    List<Map<String, Object>> out = feed(
        plan("02013", "Silver", "1", "P1", "$800.00", "$1,100.00"),
        plan("02013", "Silver", "1", "P1", "$800.00", "$1,100.00")).finish("t");
    assertEquals(1, out.get(0).get("plan_count"));
  }

  @Test void countiesAreSeparateRows() {
    List<Map<String, Object>> out = feed(
        plan("02013", "Silver", "1", "P1", "$800.00", "$1,100.00"),
        plan("02016", "Silver", "1", "P1", "$800.00", "$1,100.00")).finish("t");
    assertEquals(2, out.size());
  }

  @Test void missingRequiredHeaderFailsTheBatch() {
    AcaQhpLandscapeTransformer.Aggregator agg = new AcaQhpLandscapeTransformer.Aggregator(2027);
    assertThrows(IllegalStateException.class,
        () -> agg.accept(Arrays.asList("State Code", "FIPS County Code")));
  }

  @Test void emptySheetFailsTheBatch() {
    assertThrows(IllegalStateException.class, () -> feed().finish("t"));
  }

  @Test void moneyParsesFormattedAndNullCells() {
    assertEquals(2541.12, AcaQhpLandscapeTransformer.money("$2,541.12"));
    assertNull(AcaQhpLandscapeTransformer.money("NULL"));
    assertNull(AcaQhpLandscapeTransformer.money(null));
  }
}
