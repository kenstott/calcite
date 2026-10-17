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
package org.apache.calcite.adapter.govdata.disasters;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the disaster x county aggregation of {@link FemaIhpRegistrationsProvider}. */
@Tag("unit")
class FemaIhpRegistrationsProviderTest {

  @TempDir File dir;

  /** Registration columns the aggregate reads; all others of the source are not needed. */
  private static final String TABLE = "CREATE TABLE reg ("
      + "id BIGINT, disasterNumber SMALLINT, county VARCHAR, fips VARCHAR, "
      + "damagedStateAbbreviation VARCHAR, householdComposition VARCHAR, grossIncome VARCHAR, "
      + "ownRent VARCHAR, primaryResidence BOOLEAN DEFAULT false, "
      + "homeOwnersInsurance BOOLEAN DEFAULT false, floodInsurance BOOLEAN DEFAULT false, "
      + "ihpEligible BOOLEAN DEFAULT false, ihpAmount DECIMAL(12,2) DEFAULT 0, "
      + "fipAmount DECIMAL(12,2) DEFAULT 0, haEligible BOOLEAN DEFAULT false, "
      + "haAmount DECIMAL(12,2) DEFAULT 0, onaEligible BOOLEAN DEFAULT false, "
      + "onaAmount DECIMAL(12,2) DEFAULT 0, transientAccommodAmount DECIMAL(12,2) DEFAULT 0, "
      + "homeDamage BOOLEAN DEFAULT false, destroyed BOOLEAN DEFAULT false, "
      + "sbaApproved BOOLEAN DEFAULT false, inspnIssued BOOLEAN DEFAULT false, "
      + "inspnReturned BOOLEAN DEFAULT false, rpfvl DECIMAL(12,2) DEFAULT 0, "
      + "ppfvl DECIMAL(12,2) DEFAULT 0, floodDamage BOOLEAN DEFAULT false, "
      + "floodDamageAmount DECIMAL(12,2) DEFAULT 0, tsaEligible BOOLEAN DEFAULT false, "
      + "tsaCheckedIn BOOLEAN DEFAULT false, rentalAssistanceAmount DECIMAL(12,2) DEFAULT 0, "
      + "repairAmount DECIMAL(12,2) DEFAULT 0, replacementAmount DECIMAL(12,2) DEFAULT 0, "
      + "personalPropertyAmount DECIMAL(12,2) DEFAULT 0, unmetNeedRp DECIMAL(12,2) DEFAULT 0, "
      + "unmetNeedPp DECIMAL(12,2) DEFAULT 0)";

  private File parquet(String... inserts) throws Exception {
    File out = new File(dir, "reg.parquet");
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = conn.createStatement()) {
      st.execute(TABLE);
      for (String insert : inserts) {
        st.execute("INSERT INTO reg (" + insert + ")");
      }
      st.execute("COPY reg TO '" + out.getAbsolutePath() + "' (FORMAT PARQUET)");
    }
    return out;
  }

  private static String row(int id, int disaster, String county, String fips, String state,
      String extra) {
    return "id, disasterNumber, county, fips, damagedStateAbbreviation"
        + (extra.isEmpty() ? "" : ", " + extra.substring(0, extra.indexOf('|')))
        + ") VALUES (" + id + ", " + disaster + ", " + q(county) + ", " + q(fips) + ", "
        + q(state) + (extra.isEmpty() ? "" : ", " + extra.substring(extra.indexOf('|') + 1));
  }

  private static String q(String s) {
    return s == null ? "NULL" : "'" + s.replace("'", "''") + "'";
  }

  /** Key of a result row: the county code, else the area name. */
  private static Map<String, Map<String, Object>> byKey(List<Map<String, Object>> rows) {
    Map<String, Map<String, Object>> m = new HashMap<String, Map<String, Object>>();
    for (Map<String, Object> r : rows) {
      String key = r.get("disaster_number") + "/"
          + (r.get("county_fips") != null ? r.get("county_fips") : "name:" + r.get("county_name"));
      assertNull(m.put(key, r), "one row per disaster x county: " + key);
    }
    return m;
  }

  private List<Map<String, Object>> aggregate(File parquet,
      FemaIhpRegistrationsProvider.CountyCodeLookup lookup) throws Exception {
    return new FemaIhpRegistrationsProvider().aggregate(parquet, lookup);
  }

  @Test void registrationsOfOneCountyBecomeOneRowWithCountsAndSums() throws Exception {
    File f = parquet(
        row(1, 4332, "Harris (County)", "48201", "TX",
            "ownRent, grossIncome, householdComposition, ihpEligible, ihpAmount, haAmount, "
            + "onaAmount, destroyed, homeDamage, homeOwnersInsurance|'O', '<$15,000', '1', true, "
            + "1000.50, 800.25, 200.25, true, true, true"),
        row(2, 4332, "Harris (County)", "48201", "TX",
            "ownRent, grossIncome, householdComposition, floodInsurance, rpfvl|'R', "
            + "'$15,000-$30,000', '>5', true, 300.00"),
        row(3, 4332, "Harris (County)", "48201", "TX", "grossIncome|NULL"),
        row(4, 4332, "Fort Bend (County)", "48157", "TX", "ownRent|'O'"));
    Map<String, Map<String, Object>> rows = byKey(aggregate(f, (d, s, a) -> {
      throw new AssertionError("no registration lacks a fips, nothing to look up");
    }));
    assertEquals(2, rows.size());
    Map<String, Object> harris = rows.get("4332/48201");
    assertNotNull(harris);
    assertEquals(4332, ((Number) harris.get("disaster_number")).intValue());
    assertEquals("48", harris.get("state_fips"));
    assertEquals("TX", harris.get("state_abbr"));
    assertEquals("Harris (County)", harris.get("county_name"));
    assertEquals(3L, harris.get("registrations"));
    assertEquals(0L, harris.get("fips_from_declaration_registrations"));
    assertEquals(1L, harris.get("owner_registrations"));
    assertEquals(1L, harris.get("renter_registrations"));
    assertEquals(1L, harris.get("tenure_unknown_registrations"));
    assertEquals(1L, harris.get("income_under_15000_registrations"));
    assertEquals(1L, harris.get("income_15000_to_30000_registrations"));
    assertEquals(1L, harris.get("income_unreported_registrations"));
    assertEquals(1L, harris.get("household_1_person_registrations"));
    assertEquals(1L, harris.get("household_over_5_person_registrations"));
    assertEquals(1L, harris.get("ihp_eligible_registrations"));
    assertEquals(1L, harris.get("ihp_awarded_registrations"));
    assertEquals(1L, harris.get("destroyed_registrations"));
    assertEquals(1L, harris.get("homeowners_insurance_registrations"));
    assertEquals(1L, harris.get("flood_insurance_registrations"));
    assertEquals(1000.50, (Double) harris.get("ihp_amount"), 1e-9);
    assertEquals(800.25, (Double) harris.get("ha_amount"), 1e-9);
    assertEquals(300.00, (Double) harris.get("real_property_verified_loss"), 1e-9);
    assertEquals(0.0, (Double) harris.get("flood_damage_amount"), 1e-9);
    assertEquals(1L, rows.get("4332/48157").get("registrations"));
  }

  @Test void aTribalAreasRegistrationsGoToTheCountiesTheyAreIn() throws Exception {
    File f = parquet(
        row(1, 4827, "Eastern Band of Cherokee Indians", "37173", "NC", ""),
        row(2, 4827, "Eastern Band of Cherokee Indians", "37173", "NC", ""),
        row(3, 4827, "Eastern Band of Cherokee Indians", "37099", "NC", ""),
        row(4, 4827, "Swain (County)", "37173", "NC", ""));
    Map<String, Map<String, Object>> rows = byKey(aggregate(f, (d, s, a) -> null));
    assertEquals(2, rows.size());
    assertEquals(3L, rows.get("4827/37173").get("registrations"));
    assertEquals(1L, rows.get("4827/37099").get("registrations"));
  }

  @Test void aRegistrationWithoutFipsTakesTheDeclarationsCountyAndIsCounted() throws Exception {
    File f = parquet(
        row(1, 6502, "Camden (County)", null, "GA", ""),
        row(2, 6502, "Camden (County)", null, "GA", ""),
        row(3, 4933, "Lake (County)", "18089", "IN", ""),
        row(4, 4933, "Lake (County)", null, "IN", ""));
    List<String> asked = new ArrayList<String>();
    Map<String, Map<String, Object>> rows = byKey(aggregate(f, (d, s, a) -> {
      asked.add(d + "|" + s + "|" + a);
      return d == 6502 ? "13039" : "18089";
    }));
    assertEquals(2, rows.size());
    assertEquals(2, asked.size(), "one lookup per distinct disaster/state/area: " + asked);
    Map<String, Object> camden = rows.get("6502/13039");
    assertEquals(2L, camden.get("registrations"));
    assertEquals(2L, camden.get("fips_from_declaration_registrations"));
    assertEquals("13", camden.get("state_fips"));
    Map<String, Object> lake = rows.get("4933/18089");
    assertEquals(2L, lake.get("registrations"));
    assertEquals(1L, lake.get("fips_from_declaration_registrations"));
  }

  @Test void anUnresolvedAreaKeepsANullCountyAndOneRowPerAreaName() throws Exception {
    File f = parquet(
        row(1, 4900, "Navajo Nation", null, "AZ", ""),
        row(2, 4900, "Navajo Nation", null, "AZ", ""),
        row(3, 4900, "Hopi Tribe", null, "AZ", ""),
        row(4, 4900, "Nowhere (County)", null, "AZ", ""));
    Map<String, Map<String, Object>> rows = byKey(aggregate(f,
        (d, s, a) -> a.equals("Nowhere (County)") ? null : "04000"));
    assertEquals(3, rows.size());
    Map<String, Object> navajo = rows.get("4900/name:Navajo Nation");
    assertNull(navajo.get("county_fips"));
    assertEquals(2L, navajo.get("registrations"));
    assertEquals(0L, navajo.get("fips_from_declaration_registrations"));
    assertEquals("04", navajo.get("state_fips"), "a statewide/tribal code still gives the state");
    assertNull(rows.get("4900/name:Nowhere (County)").get("state_fips"));
  }

  @Test void aFailedLookupFailsTheAggregationInsteadOfLeavingCountiesNull() throws Exception {
    File f = parquet(row(1, 6502, "Camden (County)", null, "GA", ""));
    assertThrows(IOException.class, () -> aggregate(f, (d, s, a) -> {
      throw new IOException("DisasterDeclarationsSummaries load failed");
    }));
  }
}
