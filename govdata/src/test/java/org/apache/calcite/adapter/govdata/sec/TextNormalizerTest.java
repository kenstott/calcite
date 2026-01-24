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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link TextNormalizer}.
 */
@Tag("unit")
class TextNormalizerTest {

  private TextNormalizer normalizer;

  @BeforeEach
  void setUp() {
    // Context: Q1 2024 filing
    normalizer = TextNormalizer.forPeriod(2024, 1);
  }

  // ========== Quarter Normalization Tests ==========

  @Test
  void testQuarterPatternQ1_2024() {
    assertEquals("Revenue increased in 2024-Q1",
        normalizer.normalize("Revenue increased in Q1 2024"));
  }

  @Test
  void testQuarterPatternQ1_24() {
    assertEquals("Revenue increased in 2024-Q1",
        normalizer.normalize("Revenue increased in Q1 '24"));
  }

  @Test
  void testQuarterPattern2024_Q1() {
    assertEquals("Revenue increased in 2024-Q1",
        normalizer.normalize("Revenue increased in 2024 Q1"));
  }

  @Test
  void testFirstQuarterWithYear() {
    assertEquals("During 2024-Q1, revenue grew",
        normalizer.normalize("During first quarter 2024, revenue grew"));
  }

  @Test
  void testSecondQuarterWithYear() {
    assertEquals("During 2023-Q2, expenses decreased",
        normalizer.normalize("During second quarter 2023, expenses decreased"));
  }

  @Test
  void testThirdQuarterOfYear() {
    assertEquals("In 2024-Q3, we launched",
        normalizer.normalize("In third quarter of 2024, we launched"));
  }

  @Test
  void testFourthQuarterShortYear() {
    assertEquals("Results for 2023-Q4",
        normalizer.normalize("Results for fourth quarter '23"));
  }

  @Test
  void testOrdinalQuarterNoYear() {
    // Uses filing context (2024-Q1)
    assertEquals("The 2024-Q1 showed improvement",
        normalizer.normalize("The first quarter showed improvement"));
  }

  @Test
  void testCurrentQuarter() {
    assertEquals("In the 2024-Q1, we achieved",
        normalizer.normalize("In the current quarter, we achieved"));
  }

  @Test
  void testCurrentQuarterNoThe() {
    assertEquals("In 2024-Q1, we achieved",
        normalizer.normalize("In current quarter, we achieved"));
  }

  // ========== Period Ended Normalization Tests ==========

  @Test
  void testThreeMonthsEndedMarch() {
    assertEquals("For the 2024-Q1, revenue was",
        normalizer.normalize("For the three months ended March 31, 2024, revenue was"));
  }

  @Test
  void testThreeMonthsEndedJune() {
    assertEquals("For the 2024-Q2, revenue was",
        normalizer.normalize("For the three months ended June 30, 2024, revenue was"));
  }

  @Test
  void testSixMonthsEnded() {
    assertEquals("For the 2024-H1 (Q1-Q2), revenue was",
        normalizer.normalize("For the six months ended June 30, 2024, revenue was"));
  }

  @Test
  void testNineMonthsEnded() {
    assertEquals("For the 2024-9M (Q1-Q3), revenue was",
        normalizer.normalize("For the nine months ended September 30, 2024, revenue was"));
  }

  @Test
  void testTwelveMonthsEnded() {
    assertEquals("For the 2024-FY, revenue was",
        normalizer.normalize("For the twelve months ended December 31, 2024, revenue was"));
  }

  // ========== Fiscal Year Normalization Tests ==========

  @Test
  void testFiscalYear2024() {
    assertEquals("In FY2024, we expanded",
        normalizer.normalize("In fiscal year 2024, we expanded"));
  }

  @Test
  void testFiscal2024() {
    assertEquals("During FY2024",
        normalizer.normalize("During fiscal 2024"));
  }

  @Test
  void testFYAbbreviation() {
    assertEquals("FY2024 results",
        normalizer.normalize("FY'24 results"));
  }

  @Test
  void testFYWithSpace() {
    assertEquals("FY2024 guidance",
        normalizer.normalize("FY 24 guidance"));
  }

  @Test
  void testCurrentYear() {
    assertEquals("FY2024 outlook",
        normalizer.normalize("current year outlook"));
  }

  // ========== Relative Date Normalization Tests ==========

  @Test
  void testPriorYear() {
    assertEquals("compared to FY2023",
        normalizer.normalize("compared to prior year"));
  }

  @Test
  void testPreviousYear() {
    assertEquals("versus FY2023",
        normalizer.normalize("versus previous year"));
  }

  @Test
  void testLastFiscalYear() {
    assertEquals("in FY2023, we",
        normalizer.normalize("in last fiscal year, we"));
  }

  @Test
  void testPriorYearPeriod() {
    // Q1 2024 filing → prior year period is Q1 2023
    assertEquals("compared to 2023-Q1",
        normalizer.normalize("compared to prior year period"));
  }

  @Test
  void testYearAgo() {
    assertEquals("FY2023 level",
        normalizer.normalize("a year ago level"));
  }

  @Test
  void testYearOverYear() {
    assertEquals("YoY (FY2024 vs FY2023) growth was 5%",
        normalizer.normalize("year-over-year growth was 5%"));
  }

  @Test
  void testYoYAbbreviation() {
    assertEquals("YoY (FY2024 vs FY2023) change",
        normalizer.normalize("YoY change"));
  }

  @Test
  void testQoQAbbreviation() {
    // Q1 2024 → previous is Q4 2023
    assertEquals("QoQ (2024-Q1 vs 2023-Q4) improvement",
        normalizer.normalize("QoQ improvement"));
  }

  @Test
  void testQoQFromQ2() {
    // Q2 2024 → previous is Q1 2024
    TextNormalizer q2Normalizer = TextNormalizer.forPeriod(2024, 2);
    assertEquals("QoQ (2024-Q2 vs 2024-Q1) growth",
        q2Normalizer.normalize("QoQ growth"));
  }

  // ========== Monetary Value Normalization Tests ==========

  @Test
  void testDollarMillions() {
    assertEquals("Revenue was $5,200,000",
        normalizer.normalize("Revenue was $5.2 million"));
  }

  @Test
  void testDollarBillions() {
    assertEquals("Total assets of $12,500,000,000",
        normalizer.normalize("Total assets of $12.5 billion"));
  }

  @Test
  void testDollarMSuffix() {
    assertEquals("Expenses of $3,400,000",
        normalizer.normalize("Expenses of $3.4M"));
  }

  @Test
  void testDollarBSuffix() {
    assertEquals("Market cap $150,000,000,000",
        normalizer.normalize("Market cap $150B"));
  }

  @Test
  void testNumberMillionsNoDollar() {
    assertEquals("headcount of 45,000,000",
        normalizer.normalize("headcount of 45 million"));
  }

  @Test
  void testNumberBillionsNoDollar() {
    assertEquals("users reached 2,000,000,000",
        normalizer.normalize("users reached 2 billion"));
  }

  // ========== Combined Normalization Tests ==========

  @Test
  void testComplexSentence() {
    String input = "Revenue for Q1 2024 was $5.2 million, up 15% year-over-year "
        + "compared to the prior year period.";
    String expected = "Revenue for 2024-Q1 was $5,200,000, up 15% YoY (FY2024 vs FY2023) "
        + "compared to the 2023-Q1.";
    assertEquals(expected, normalizer.normalize(input));
  }

  @Test
  void testMDASentence() {
    String input = "For the three months ended March 31, 2024, net income increased "
        + "to $125 million from $98 million in the first quarter of 2023.";
    String expected = "For the 2024-Q1, net income increased "
        + "to $125,000,000 from $98,000,000 in the 2023-Q1.";
    assertEquals(expected, normalizer.normalize(input));
  }

  @Test
  void testEarningsCallSentence() {
    String input = "Looking at fiscal 2024, we expect revenue growth of 10% YoY "
        + "with current quarter tracking above expectations.";
    String expected = "Looking at FY2024, we expect revenue growth of 10% YoY (FY2024 vs FY2023) "
        + "with 2024-Q1 tracking above expectations.";
    assertEquals(expected, normalizer.normalize(input));
  }

  // ========== Edge Cases ==========

  @Test
  void testNullInput() {
    assertNull(normalizer.normalize(null));
  }

  @Test
  void testEmptyInput() {
    assertEquals("", normalizer.normalize(""));
  }

  @Test
  void testNoNormalizationNeeded() {
    String input = "The company reported strong results.";
    assertEquals(input, normalizer.normalize(input));
  }

  @Test
  void testPreservesCaseElsewhere() {
    // Normalization should not affect other text casing
    String input = "Apple Inc. reported Q1 2024 results.";
    String expected = "Apple Inc. reported 2024-Q1 results.";
    assertEquals(expected, normalizer.normalize(input));
  }

  // ========== Factory Method Tests ==========

  @Test
  void testForFilingFactory() {
    TextNormalizer filingNormalizer = TextNormalizer.forFiling("2023-06-15", "2023-06-30");
    // Filing in June → Q2
    assertEquals("In the 2023-Q2, we",
        filingNormalizer.normalize("In the current quarter, we"));
  }

  @Test
  void testForPeriodFactory() {
    TextNormalizer periodNormalizer = TextNormalizer.forPeriod(2025, 3);
    assertEquals("Results for the 2025-Q3",
        periodNormalizer.normalize("Results for the current quarter"));
  }
}
