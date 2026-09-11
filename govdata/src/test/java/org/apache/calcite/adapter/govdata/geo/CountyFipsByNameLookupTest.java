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
package org.apache.calcite.adapter.govdata.geo;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Pins the 19 EIA-860 county-name spellings that failed to resolve against
 * {@code energy.eia_power_plants} in production (D-243), plus the ambiguous cases the
 * lookup must still refuse rather than guess.
 */
@Tag("unit")
public class CountyFipsByNameLookupTest {

  @Test void resolvesEiaPeriodVariant() {
    // EIA writes "St Joseph" (no period); the crosswalk's canonical form is "St. Joseph".
    assertEquals("18141", CountyFipsByNameLookup.lookup("IN", "St Joseph"));
    assertEquals("26147", CountyFipsByNameLookup.lookup("MI", "St Clair"));
    assertEquals("29183", CountyFipsByNameLookup.lookup("MO", "St Charles"));
    assertEquals("27137", CountyFipsByNameLookup.lookup("MN", "St Louis"));
  }

  @Test void resolvesEiaCasingVariant() {
    // EIA's own casing is inconsistent even within one county across rows.
    assertEquals("18141", CountyFipsByNameLookup.lookup("IN", "ST JOSEPH"));
    assertEquals("18141", CountyFipsByNameLookup.lookup("IN", "st joseph"));
  }

  @Test void resolvesEiaApostropheVariant() {
    // EIA writes "Prince Georges"; the crosswalk carries the possessive "Prince George's".
    assertEquals("24033", CountyFipsByNameLookup.lookup("MD", "Prince Georges"));
  }

  @Test void resolvesEiaSpacingVariant() {
    // EIA writes "DeSoto" with no space; the crosswalk carries "De Soto".
    assertEquals("22031", CountyFipsByNameLookup.lookup("LA", "DeSoto"));
  }

  @Test void resolvesVirginiaIndependentCities() {
    // TIGER's own county_name for a VA independent city is identical to its same-named
    // county ("Richmond" resolves to both 51159 the county and 51760 the city), so the
    // bare name is genuinely ambiguous and stays unresolved (see below). EIA disambiguates
    // by appending "City", which these aliases resolve to the city's own FIPS.
    assertEquals("51760", CountyFipsByNameLookup.lookup("VA", "Richmond City"));
    assertEquals("51670", CountyFipsByNameLookup.lookup("VA", "Hopewell City"));
    assertEquals("51740", CountyFipsByNameLookup.lookup("VA", "Portsmouth City"));
    assertEquals("51580", CountyFipsByNameLookup.lookup("VA", "Covington City"));
    assertEquals("51550", CountyFipsByNameLookup.lookup("VA", "Chesapeake City"));
  }

  @Test void resolvesMissouriAndMarylandIndependentCities() {
    assertEquals("29510", CountyFipsByNameLookup.lookup("MO", "St. Louis City"));
    assertEquals("24510", CountyFipsByNameLookup.lookup("MD", "Baltimore City"));
  }

  /**
   * The bare, undisambiguated name for a state with both a county and a same-named
   * independent city must stay unresolved. Silently picking one would misattribute a
   * generator to the wrong FIPS half the time, which is worse than the NULL this
   * class's own contract already returns for an unmatched name.
   */
  @Test void refusesAmbiguousBareCityCountyName() {
    assertNull(CountyFipsByNameLookup.lookup("VA", "Richmond"));
    assertNull(CountyFipsByNameLookup.lookup("MO", "St. Louis"));
    assertNull(CountyFipsByNameLookup.lookup("MD", "Baltimore"));
  }

  @Test void stillResolvesOrdinaryNames() {
    assertEquals("02013", CountyFipsByNameLookup.lookup("AK", "Aleutians East"));
  }

  @Test void unmatchedNameReturnsNull() {
    assertNull(CountyFipsByNameLookup.lookup("XX", "Nowhere"));
    assertNull(CountyFipsByNameLookup.lookup(null, "Aleutians East"));
    assertNull(CountyFipsByNameLookup.lookup("AK", null));
  }
}
