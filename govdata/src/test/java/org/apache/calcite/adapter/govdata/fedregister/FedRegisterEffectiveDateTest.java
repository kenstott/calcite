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
package org.apache.calcite.adapter.govdata.fedregister;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests effective-date extraction from Federal Register EFFDATE / DATES prose. */
@Tag("unit")
class FedRegisterEffectiveDateTest {

  @Test void testEffectiveSentence() {
    assertEquals("2024-04-01",
        FedRegisterBulkXmlDataProvider.extractEffectiveDate(
            "DATES: This rule is effective April 1, 2024."));
  }

  @Test void testEffectiveWithTimeOfDay() {
    assertEquals("2024-05-16",
        FedRegisterBulkXmlDataProvider.extractEffectiveDate(
            "DATES: Effective date 0901 UTC, May 16, 2024. The Director of the Federal Register"
            + " approves this incorporation by reference action under 1 CFR part 51."));
    assertEquals("2020-05-01",
        FedRegisterBulkXmlDataProvider.extractEffectiveDate(
            "DATES: This rule is effective at 12:01 a.m. on May 1, 2020."));
  }

  @Test void testTakesEffectAndGoesIntoEffect() {
    assertEquals("2015-09-24",
        FedRegisterBulkXmlDataProvider.extractEffectiveDate(
            "DATES: This rule takes effect on September 24, 2015."));
    assertEquals("2014-01-08",
        FedRegisterBulkXmlDataProvider.extractEffectiveDate(
            "DATES: This rule goes into effect on January 8, 2014."));
  }

  @Test void testAirworthinessDirectiveNumberIsNotADate() {
    assertEquals("2023-03-29",
        FedRegisterBulkXmlDataProvider.extractEffectiveDate(
            "DATES: This correction is effective March 29, 2023. The effective date of AD"
            + " 2024-05-05 remains March 29, 2023."));
    assertEquals("2019-06-17",
        FedRegisterBulkXmlDataProvider.extractEffectiveDate(
            "DATES: This AD is effective June 17, 2019 to all persons except those persons to"
            + " whom it was made immediately effective by Emergency AD 2019-08-51, issued on"
            + " April 18, 2019."));
  }

  @Test void testDateWithoutEffectiveAnchorIsNull() {
    assertNull(FedRegisterBulkXmlDataProvider.extractEffectiveDate(
        "DATES: The regulation in 33 CFR 165.160 will be enforced from 9 p.m. to 9:20 p.m. on"
        + " December 31, 2013."));
    assertNull(FedRegisterBulkXmlDataProvider.extractEffectiveDate(
        "DATES: Comments must be received by May 9, 2011. This rule is effective upon"
        + " publication."));
  }

  @Test void testRelativeEffectiveDateIsNull() {
    assertNull(FedRegisterBulkXmlDataProvider.extractEffectiveDate(
        "DATES: Effective Date: This regulation will become effective 30 days after"
        + " publication in the Federal Register."));
  }

  @Test void testBareDate() {
    assertEquals("2015-09-03",
        FedRegisterBulkXmlDataProvider.extractEffectiveDate("DATES: September 3, 2015."));
  }

  @Test void testImpossibleCalendarDateIsNull() {
    assertNull(FedRegisterBulkXmlDataProvider.extractEffectiveDate(
        "DATES: This rule is effective February 31, 2024."));
  }

  @Test void testNullText() {
    assertNull(FedRegisterBulkXmlDataProvider.extractEffectiveDate(null));
  }
}
