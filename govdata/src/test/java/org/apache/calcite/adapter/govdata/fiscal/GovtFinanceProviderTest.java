/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.fiscal;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link GovtFinanceProvider}'s zip-link discovery and fixed-width record parsing.
 */
@Tag("unit")
class GovtFinanceProviderTest {

  private String findZipUrl(String html) throws Exception {
    Method m = GovtFinanceProvider.class.getDeclaredMethod("findZipUrl", String.class);
    m.setAccessible(true);
    return (String) m.invoke(null, html);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> toRow(String line, String year) throws Exception {
    return toRow(line, year, Collections.<String, String>emptyMap());
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> toRow(String line, String year, Map<String, String> unitCounties)
      throws Exception {
    Method m = GovtFinanceProvider.class.getDeclaredMethod("toRow", String.class, String.class,
        Map.class);
    m.setAccessible(true);
    return (Map<String, Object>) m.invoke(null, line, year, unitCounties);
  }

  @Test void testMatchesUnderscoreTitleCase2012And2017Plus() throws Exception {
    String html = "<html><body><a href=\"https://www2.census.gov/programs-surveys/"
        + "gov-finances/tables/2023/2023_Individual_Unit_Files.zip\">Individual Unit File"
        + "</a></body></html>";
    assertEquals(
        "https://www2.census.gov/programs-surveys/gov-finances/tables/2023/"
            + "2023_Individual_Unit_Files.zip",
        findZipUrl(html),
        "absolute hrefs should pass through unchanged");
  }

  @Test void testMatchesLowercaseUnderscore2013And2016() throws Exception {
    // Real 2013/2016 Census filenames use lowercase "file".
    String html = "<html><body><a href=\"https://www2.census.gov/programs-surveys/"
        + "gov-finances/tables/2013/2013_Individual_Unit_file.zip\">Data</a></body></html>";
    assertEquals(
        "https://www2.census.gov/programs-surveys/gov-finances/tables/2013/"
            + "2013_Individual_Unit_file.zip",
        findZipUrl(html));
  }

  @Test void testMatchesHyphenatedLowercase2014And2015() throws Exception {
    // Real 2014/2015 Census filenames are all-lowercase and hyphenated, not underscored.
    String html = "<html><body><a href=\"https://www2.census.gov/programs-surveys/"
        + "gov-finances/tables/2014/2014-individual-unit-file.zip\">Data</a></body></html>";
    assertEquals(
        "https://www2.census.gov/programs-surveys/gov-finances/tables/2014/"
            + "2014-individual-unit-file.zip",
        findZipUrl(html));
  }

  @Test void testResolvesProtocolRelativeHrefToWww2Host() throws Exception {
    // Real live 2013 landing page href: a protocol-relative URL pointing at www2.census.gov, a
    // different host than the www.census.gov landing page. Treating this like a site-root-relative
    // path produces the malformed https://www.census.gov//www2.census.gov/... .
    String html = "<html><body><a href=\"//www2.census.gov/programs-surveys/gov-finances/"
        + "tables/2013/summary-tables/2013_Individual_Unit_file.zip\">Data</a></body></html>";
    assertEquals(
        "https://www2.census.gov/programs-surveys/gov-finances/tables/2013/summary-tables/"
            + "2013_Individual_Unit_file.zip",
        findZipUrl(html));
  }

  @Test void testResolvesRelativeHrefAgainstCensusHost() throws Exception {
    String html = "<html><body><a href=\"/programs-surveys/gov-finances/tables/2016/"
        + "2016_Individual_Unit_file.zip\">Data</a></body></html>";
    assertEquals(
        "https://www.census.gov/programs-surveys/gov-finances/tables/2016/"
            + "2016_Individual_Unit_file.zip",
        findZipUrl(html));
  }

  @Test void testReturnsNullWhenNoMatchingLinkPresent() throws Exception {
    String html = "<html><body><a href=\"/some/other/file.zip\">Unrelated</a></body></html>";
    assertNull(findZipUrl(html));
  }

  /** Real 2019 record: 32 characters, 12-character ID. */
  @Test void testParsesNewLayout32Char() throws Exception {
    Map<String, Object> row = toRow("06000022634919T    331689552019R", "2019");
    assertEquals("06", row.get("state_fips"));
    assertEquals("0", row.get("gov_type_code"));
    assertEquals("State", row.get("gov_type_name"));
    assertEquals("000", row.get("county_fips"));
    assertEquals("226349", row.get("unit_id"));
    assertEquals("19T", row.get("item_code"));
    assertEquals(33168955L, row.get("amount_thousands"));
    assertEquals(2019, row.get("year"));
    assertEquals("R", row.get("imputation_flag"));
  }

  /**
   * Real 2013 record: 34 characters, 14-character ID (the 12-character ID plus a literal "00"
   * pad). Reading it with the 32-character offsets yields item code "001" and an unparseable
   * amount, which is what this case pins down. {@code county_fips} comes from the Fin_GID lookup,
   * not the ID's own county field (a Census sequence number for this layout, not FIPS).
   */
  @Test void testParsesOldLayout34Char() throws Exception {
    Map<String, Object> row = toRow("0610010010000019T      4110002013I", "2013",
        Collections.singletonMap("061001001000", "08001"));
    assertEquals("08", row.get("state_fips"), "Census state code 06 (Colorado) is FIPS 08");
    assertEquals("1", row.get("gov_type_code"));
    assertEquals("County", row.get("gov_type_name"));
    assertEquals("001", row.get("county_fips"), "from the Fin_GID entry, not the ID's own county field");
    assertEquals("001000", row.get("unit_id"));
    assertEquals("19T", row.get("item_code"));
    assertEquals(411000L, row.get("amount_thousands"));
    assertEquals(2013, row.get("year"));
    assertEquals("I", row.get("imputation_flag"));
  }

  /** A 2012-2016 unit ID with no matching Fin_GID entry is a loud failure, not a silent skip. */
  @Test void testOldLayoutThrowsWhenUnitMissingFromGid() throws Exception {
    InvocationTargetException e = assertThrows(InvocationTargetException.class,
        () -> toRow("0610010010000019T      4110002013I", "2013",
            Collections.<String, String>emptyMap()));
    assertTrue(e.getCause() instanceof IllegalStateException);
  }

  /** A Fin_GID entry whose own state disagrees with the ID's translated state is a data anomaly. */
  @Test void testOldLayoutThrowsWhenGidStateDisagreesWithId() throws Exception {
    InvocationTargetException e = assertThrows(InvocationTargetException.class,
        () -> toRow("0610010010000019T      4110002013I", "2013",
            Collections.singletonMap("061001001000", "09001")));
    assertTrue(e.getCause() instanceof IllegalStateException);
  }

  /** Census alphabetical state code to FIPS for the 2012-2016 layout, spot-checked at the edges. */
  @Test void testOldLayoutTranslatesCensusStateCodeToFips() throws Exception {
    String[][] censusToFips = {
        {"01", "01"}, {"03", "04"}, {"08", "10"}, {"09", "11"}, {"29", "32"}, {"30", "33"},
        {"33", "36"}, {"40", "44"}, {"44", "48"}, {"48", "53"}, {"49", "54"}, {"50", "55"},
        {"51", "56"}};
    for (String[] pair : censusToFips) {
      String unitId = pair[0] + "0000000000";
      Map<String, Object> row =
          toRow(pair[0] + "000000000000" + "19T      4110002013I", "2013",
              Collections.singletonMap(unitId, pair[1] + "000"));
      assertEquals(pair[1], row.get("state_fips"), "Census state code " + pair[0]);
    }
  }

  @Test void testNewLayoutStateCodeIsAlreadyFips() throws Exception {
    assertEquals("48", toRow("48000022634919T    331689552019R", "2019").get("state_fips"));
    assertEquals("56", toRow("56000022634919T    331689552019R", "2019").get("state_fips"));
  }

  @Test void testOldLayoutRejectsCodeOutsideCensusRange() throws Exception {
    InvocationTargetException e = assertThrows(InvocationTargetException.class,
        () -> toRow("52" + "000000000000" + "19T      4110002013I", "2013"));
    assertTrue(e.getCause() instanceof IllegalStateException);
  }

  @Test void testSkipsRecordOfUnexpectedLength() throws Exception {
    assertNull(toRow("0610010010000019T      41100020", "2013"),
        "a 31-character record matches neither layout and must be skipped, not shifted");
  }
}
