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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * D-178 - {@code getPartitionYear} staged a 10-K/10-Q document under a {@code year=} directory
 * derived from its extracted fiscal period-end date with no check on how far that date was from
 * the filing's actual SEC submission date. {@code extractPeriodEndDate}'s XBRL-context-scan
 * fallback only caps candidate dates at the current year, so an unrelated context (a forward-
 * looking maturity date, a mis-tagged comparative period) could be picked up as the period end
 * and stage the document years away from where its own {@code filing_date} column said it
 * belonged. Because the materializer reads staged files with DuckDB's
 * {@code hive_partitioning=true}, the wrong directory then silently overrode the correct
 * per-row {@code year} value at query time -- confirmed live on accession
 * 0000814184-19-000076 (filed 2019-07-31, staged under {@code year=2024} in
 * {@code sec.mda_sections}).
 *
 * <p>These tests pin {@link XbrlToParquetConverter#plausiblePeriodEndYear}, the proximity check
 * that now gates whether a period-end date is trusted for partitioning.
 */
@Tag("unit")
class PeriodEndPartitionYearTest {

  @Test @DisplayName("a period-end date the same year as filing is trusted")
  void testSameYearIsPlausible() {
    assertEquals("2019",
        XbrlToParquetConverter.plausiblePeriodEndYear("2019-06-30", "2019-07-31"));
  }

  @Test @DisplayName("a period-end date one year before filing (normal fiscal lag) is trusted")
  void testOneYearLagIsPlausible() {
    assertEquals("2019",
        XbrlToParquetConverter.plausiblePeriodEndYear("2019-12-31", "2020-02-15"));
  }

  @Test @DisplayName("a period-end date exactly at the 2-year boundary is still trusted")
  void testTwoYearGapIsPlausible() {
    assertEquals("2017",
        XbrlToParquetConverter.plausiblePeriodEndYear("2017-12-31", "2019-07-31"));
  }

  @Test @DisplayName("the confirmed live defect: a period-end year 5 years ahead of filing is rejected")
  void testImplausibleFutureYearIsRejected() {
    // accession 0000814184-19-000076: filed 2019-07-31, wrongly staged under year=2024
    assertNull(XbrlToParquetConverter.plausiblePeriodEndYear("2024-06-30", "2019-07-31"));
  }

  @Test @DisplayName("a period-end year far in the past relative to filing is also rejected")
  void testImplausiblePastYearIsRejected() {
    assertNull(XbrlToParquetConverter.plausiblePeriodEndYear("2010-01-01", "2019-07-31"));
  }

  @Test @DisplayName("a null candidate date yields no plausible year")
  void testNullCandidateIsRejected() {
    assertNull(XbrlToParquetConverter.plausiblePeriodEndYear(null, "2019-07-31"));
  }

  @Test @DisplayName("a malformed candidate date yields no plausible year")
  void testMalformedCandidateIsRejected() {
    assertNull(XbrlToParquetConverter.plausiblePeriodEndYear("not-a-date", "2019-07-31"));
  }

  @Test @DisplayName("an unparseable filing date cannot disprove plausibility, so the candidate is kept")
  void testUnparseableFilingDateKeepsCandidate() {
    assertEquals("2024",
        XbrlToParquetConverter.plausiblePeriodEndYear("2024-06-30", null));
  }
}
