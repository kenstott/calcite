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

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class NpsDeferredMaintenanceTransformerTest {

  private static final String ACADIA =
      "Infrastructure Fact Sheet\nThe figures below show the park’s real property inventory\n"
      + "by asset category as of the end of Fiscal Year (FY) 2025.\n"
      + "208 Buildings\n153 Miles of Trails\n82 Miles of Unpaved Roads4\n17 Water Systems\n"
      + "7 Campgrounds\n77 Housing Units5\n18 Waste Water Systems\n56 Miles of Paved Roads6\n"
      + "The park also has 50 other assets. This includes Electrical\n"
      + "There is an estimated $138 M in Deferred Maintenance\nand Repairs (DM&R)7 associated. The\n"
      + "park has an estimated $9.6 M in Annual Routine\nMaintenance8 requirements.\n"
      + "Park Infrastructure Condition\nAcadia National Park has a total portfolio DM&R of $138 M. The\n"
      + "Paved Roads asset category currently accounts for the largest\nportion of DM&R at $62 M. The "
      + "Buildings asset category has $45 M\nin DM&R. Unpaved Roads has $9.5 M. All Other Assets has $5 M.\n"
      + "Trails has $4.5 M. Water Systems has $4.2 M. Housing has $3.7 M.\n"
      + "Waste Water Systems has $3.1 M. Campgrounds has $1.9 M.\n";

  @Test void parsesCategoriesWithoutConfusingWasteWaterAndWater() {
    ObjectNode r = NpsDeferredMaintenanceTransformer.parseFactSheet(
        "Acadia National Park", "Maine", "http://x/a.pdf", ACADIA);
    assertEquals(2025, r.get("fiscal_year").asInt());
    assertEquals(208, r.get("buildings").asLong());
    assertEquals(153.0, r.get("trail_miles").asDouble(), 0.0);
    assertEquals(138000000L, r.get("dmr_total_usd").asLong());
    assertEquals(9600000L, r.get("annual_routine_maintenance_usd").asLong());
    assertEquals(62000000L, r.get("dmr_paved_roads").asLong());
    assertEquals(45000000L, r.get("dmr_buildings").asLong());
    assertEquals(4200000L, r.get("dmr_water_systems").asLong());
    assertEquals(3100000L, r.get("dmr_waste_water_systems").asLong());
    assertEquals(5000000L, r.get("dmr_other_assets").asLong());
  }

  @Test void zeroCategoriesAreZeroWhenTotalReconciles() {
    String t = ACADIA.replace("138 M", "1.4 M").replaceAll("(?s)Paved Roads asset category.*$",
        "Trails asset category currently accounts for the largest portion of DM&R at \\$1.4 M. "
        + "The Buildings, Housing, Campgrounds, Unpaved Roads, Water Systems, Waste Water Systems, Paved Roads, and All Other Assets asset categories have \\$0 in current deferred maintenance.");
    ObjectNode r = NpsDeferredMaintenanceTransformer.parseFactSheet("X", "Hawaii", "u", t);
    assertEquals(1400000L, r.get("dmr_trails").asLong());
    assertEquals(0L, r.get("dmr_buildings").asLong());
  }

  @Test void unnamedCategoryTakesTheRemainder() {
    String t = ACADIA.replace("138 M", "308 K").replaceAll("(?s)Paved Roads asset category.*$",
        "The Buildings asset category currently accounts for the largest portion of DM&R at \\$171 K. "
        + "The Trails, Housing, Campgrounds, Unpaved Roads, Water Systems, Waste Water Systems, and "
        + "Paved Roads asset categories have \\$0 in current deferred maintenance and repair needs.");
    ObjectNode r = NpsDeferredMaintenanceTransformer.parseFactSheet("Adams", "Massachusetts", "u", t);
    assertEquals(171000L, r.get("dmr_buildings").asLong());
    assertEquals(137000L, r.get("dmr_other_assets").asLong());
    assertEquals(0L, r.get("dmr_waste_water_systems").asLong());
    assertEquals(0L, r.get("dmr_paved_roads").asLong());
  }

  @Test void categorySumMismatchFails() {
    String t = ACADIA.replace("Campgrounds has $1.9 M", "Campgrounds has $91 M");
    assertThrows(IllegalStateException.class, () ->
        NpsDeferredMaintenanceTransformer.parseFactSheet("Acadia", "Maine", "u", t));
  }

  @Test void totalPrintedAsWholeMillionAllowsHalfMillionRounding() {
    String t = ACADIA.replace("138 M", "1 M").replaceAll("(?s)Paved Roads asset category.*$",
        "The Buildings asset category currently accounts for the largest portion of DM&R at \\$732 K. "
        + "The Paved Roads asset category has \\$181 K in DM&R. All Other Assets has \\$8 K. "
        + "The Trails, Housing, Campgrounds, Unpaved Roads, Water Systems, and Waste Water Systems "
        + "asset categories have \\$0 in current deferred maintenance.");
    ObjectNode r = NpsDeferredMaintenanceTransformer.parseFactSheet("LR", "Arkansas", "u", t);
    assertEquals(1000000L, r.get("dmr_total_usd").asLong());
    assertEquals(732000L, r.get("dmr_buildings").asLong());
  }

  @Test void dollarsParsesUnits() {
    assertEquals(1300000000L, NpsDeferredMaintenanceTransformer.dollars("$1.3 B"));
    assertEquals(149000L, NpsDeferredMaintenanceTransformer.dollars("$149 K"));
    assertEquals(0L, NpsDeferredMaintenanceTransformer.dollars("$0"));
  }
}
