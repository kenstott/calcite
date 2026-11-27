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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Parsing tests for {@link UsgsSeismicHazardByCountyProvider}. */
@Tag("unit")
class UsgsSeismicHazardByCountyProviderTest {

  private static final String CSV = "﻿STATEFP,COUNTYFP,COUNAME,STNAME,POPULATION,"
      + "LATITUDE,LONGITUDE\r\n"
      + "06,075,San Francisco,California,873965,+37.760000,-122.440000\r\n";

  /** Trimmed from live service responses for a San Francisco point, 2026-11-27. */
  private static final String REFERENCE = "{\"request\":{\"status\":\"success\"},"
      + "\"response\":{\"data\":{\"pgam\":0.54,\"ss\":1.57,\"s1\":0.6,\"sds\":0.94,"
      + "\"sd1\":0.5,\"sdc\":\"D\",\"underlyingData\":{\"pgauh\":0.75}},"
      + "\"metadata\":{\"modelVersion\":\"v5.0.x\"}}}";

  private static final String DEFAULT_CLASS = "{\"request\":{\"status\":\"success\"},"
      + "\"response\":{\"data\":{\"pgam\":0.63,\"ss\":1.57,\"s1\":0.6,\"sds\":1.21,"
      + "\"sd1\":1.17,\"sdc\":\"D\",\"underlyingData\":{\"pgauh\":\"See underlying data for "
      + "Site Classes C, CD, and D\"}},\"metadata\":{\"modelVersion\":\"v5.0.x\"}}}";

  private static UsgsSeismicHazardByCountyProvider.County county() throws IOException {
    return UsgsSeismicHazardByCountyProvider.parseCentroids(CSV).get(0);
  }

  @Test void parsesCentroidsWithByteOrderMark() throws IOException {
    List<UsgsSeismicHazardByCountyProvider.County> counties =
        UsgsSeismicHazardByCountyProvider.parseCentroids(CSV);
    assertEquals(1, counties.size());
    assertEquals("06075", counties.get(0).countyFips);
    assertEquals(37.76, counties.get(0).latitude);
    assertEquals(-122.44, counties.get(0).longitude);
  }

  @Test void rejectsUnexpectedHeader() {
    assertThrows(IOException.class,
        () -> UsgsSeismicHazardByCountyProvider.parseCentroids("<html>\n"));
  }

  @Test void takesUniformHazardFromReferenceAndDesignValuesFromDefault() throws IOException {
    Map<String, Object> row =
        UsgsSeismicHazardByCountyProvider.buildRow(county(), REFERENCE, DEFAULT_CLASS);
    assertEquals("06", row.get("state_fips"));
    assertEquals(0.75, row.get("pga_2pct50yr_g"));
    assertEquals(1.21, row.get("sds_g"));
    assertEquals(0.63, row.get("pgam_g"));
    assertEquals(0.94, row.get("sds_site_class_bc_g"));
    assertEquals("D", row.get("seismic_design_category"));
    assertEquals("v5.0.x", row.get("model_version"));
  }

  @Test void failsWhenAValueIsMissing() throws IOException {
    UsgsSeismicHazardByCountyProvider.County county = county();
    String noPga = REFERENCE.replace("\"pgauh\":0.75", "\"pgauh\":null");
    assertThrows(IOException.class,
        () -> UsgsSeismicHazardByCountyProvider.buildRow(county, noPga, DEFAULT_CLASS));
  }

  @Test void failsOnUnsuccessfulStatus() throws IOException {
    UsgsSeismicHazardByCountyProvider.County county = county();
    String failed = DEFAULT_CLASS.replace("\"success\"", "\"error\"");
    assertThrows(IOException.class,
        () -> UsgsSeismicHazardByCountyProvider.buildRow(county, REFERENCE, failed));
  }
}
