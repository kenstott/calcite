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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Reproduces the D-063 defect: an 8-K prepared-remarks exhibit names its section only in the
 * heading paragraph (e.g. "Exhibit 99.3 ... Prepared Management Remarks"); the narrative
 * paragraphs that follow never repeat that phrase, so classifying each paragraph independently
 * (the pre-fix behavior) misfiles the whole section as "other". Fixed CIK 0000049071, accession
 * 0000049071-25-000004 (Humana) — the exact heading text below is that filing's own wording.
 */
@Tag("unit")
class EarningsTranscriptSectionStateTest {

  @Test void headingNamesTheSectionForItselfAndForFollowingParagraphs() {
    XbrlToParquetConverter converter = new XbrlToParquetConverter(null);
    String heading = "Exhibit 99.3 Humana Inc. Prepared Management Remarks";
    String narrative1 = "Thank you, operator. Good morning, everyone, and thank you for joining "
        + "us today.";
    String narrative2 = "Our results this quarter reflect continued momentum across the "
        + "business.";

    String currentSection = converter.detectSectionTypeWithState(heading, null);
    assertEquals("prepared_remarks", currentSection);

    currentSection = converter.detectSectionTypeWithState(narrative1, currentSection);
    assertEquals("prepared_remarks", currentSection);

    currentSection = converter.detectSectionTypeWithState(narrative2, currentSection);
    assertEquals("prepared_remarks", currentSection);
  }

  /** Pins the defect being routed around: with no state carried forward, a narrative paragraph
   *  that never repeats the heading's phrase falls through to "other". */
  @Test void withoutInheritedStateNarrativeParagraphMisfilesAsOther() {
    XbrlToParquetConverter converter = new XbrlToParquetConverter(null);
    String narrative = "Thank you, operator. Good morning, everyone, and thank you for joining "
        + "us today.";

    assertEquals("other", converter.detectSectionTypeWithState(narrative, null));
  }

  @Test void newHeadingSwitchesTheInheritedSection() {
    XbrlToParquetConverter converter = new XbrlToParquetConverter(null);
    String preparedRemarksHeading = "Prepared Remarks";
    String qaHeading = "Exhibit 99.4 Question and Answer Session";
    String qaNarrative = "Analyst: Can you comment on margin trends?";

    String currentSection = converter.detectSectionTypeWithState(preparedRemarksHeading, null);
    assertEquals("prepared_remarks", currentSection);

    currentSection = converter.detectSectionTypeWithState(qaHeading, currentSection);
    assertEquals("q_and_a", currentSection);

    currentSection = converter.detectSectionTypeWithState(qaNarrative, currentSection);
    assertEquals("q_and_a", currentSection);
  }
}
