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

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link GovtFinanceTransformer}'s zip-link discovery — regression coverage for
 * the 2013-2016 interior gap in {@code fiscal.govt_finance_by_unit}, caused by a case-sensitive
 * selector missing real Census filename variants for those years.
 */
@Tag("unit")
class GovtFinanceTransformerTest {

  private String findZipUrl(String html) throws Exception {
    GovtFinanceTransformer transformer = new GovtFinanceTransformer();
    Method m = GovtFinanceTransformer.class.getDeclaredMethod("findZipUrl", String.class);
    m.setAccessible(true);
    return (String) m.invoke(transformer, html);
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
    // Real 2013/2016 Census filenames use lowercase "file" — this is exactly what the old
    // case-sensitive "Individual_Unit_Fil" selector missed, causing the 2013-2016 gap.
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
}
