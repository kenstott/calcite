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

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for narrowing an 8-K's expected outputs by the items it reports.
 *
 * <p>An 8-K reports whatever event its items name, and only Item 2.02 — Results of Operations and
 * Financial Condition — is an earnings release. Expecting a transcript from every 8-K leaves
 * roughly four in five permanently incomplete: measured across 2019-2022, between 18% and 22%
 * produced one, every year. Those filings are re-tested and reprocessed on every restart and can
 * never pass, so the cost recurs indefinitely.
 */
@Tag("unit")
class FormTypeItemsTest {

  private static boolean expectsEarnings(FormType form, String items) {
    Set<FormType.OutputType> outputs = form.getExpectedOutputs(false, items);
    return outputs.contains(FormType.OutputType.EARNINGS);
  }

  @Test void testEarningsItemIsExpectedToProduceATranscript() {
    assertTrue(expectsEarnings(FormType.FORM_8K, "2.02,9.01"),
        "an Item 2.02 filing is an earnings release and owes a transcript");
    assertTrue(expectsEarnings(FormType.FORM_8K, "2.02"),
        "2.02 alone is still an earnings release");
    assertTrue(expectsEarnings(FormType.FORM_8K_A, "9.01,2.02"),
        "order within the item list is not significant");
  }

  @Test void testOtherItemsOweNoTranscript() {
    assertFalse(expectsEarnings(FormType.FORM_8K, "5.02"),
        "a director departure produces no earnings transcript");
    assertFalse(expectsEarnings(FormType.FORM_8K, "1.01,9.01"),
        "a material agreement produces no earnings transcript");
    assertFalse(expectsEarnings(FormType.FORM_8K_A, "5.07,9.01"),
        "a shareholder vote produces no earnings transcript");
  }

  @Test void testFilingReportingNoItemsOwesNoTranscript() {
    assertFalse(expectsEarnings(FormType.FORM_8K, ""),
        "empty means EDGAR reported no items, which is an answer, not an absence of one");
  }

  /**
   * Unknown items must not retire a filing.
   *
   * <p>EDGAR's submissions payload covers every filing, so null means the lookup failed rather
   * than that the filing reports nothing. Reprocessing an 8-K that turns out to owe no transcript
   * costs time; declaring one complete when it owed a transcript loses the data quietly, and
   * nothing downstream would ever ask again.
   */
  @Test void testUnknownItemsStayConservative() {
    assertTrue(expectsEarnings(FormType.FORM_8K, null),
        "a failed lookup must leave the expectation in place, not silently drop it");
  }

  /**
   * The list is comma separated, so an item is matched whole.
   *
   * <p>A substring test would read 2.02 out of a hypothetical 12.02 and expect a transcript from a
   * filing that owes none — reintroducing the false positives this narrowing exists to remove.
   */
  @Test void testItemsMatchWholeNotSubstring() {
    assertFalse(expectsEarnings(FormType.FORM_8K, "12.02"),
        "12.02 contains the text 2.02 but is a different item");
    assertFalse(expectsEarnings(FormType.FORM_8K, "2.0"),
        "2.0 is not 2.02");
    assertTrue(expectsEarnings(FormType.FORM_8K, "1.01, 2.02 ,9.01"),
        "surrounding whitespace must not defeat the match");
  }

  @Test void testFormsThatNeverProduceTranscriptsAreUnaffected() {
    assertFalse(expectsEarnings(FormType.FORM_10K, "2.02"),
        "items narrow the 8-K expectation only; a 10-K never owed a transcript");
    assertFalse(expectsEarnings(FormType.FORM_4, null),
        "a Form 4 is unaffected by items being unknown");
  }

  /**
   * Narrowing removes the earnings expectation and nothing else.
   *
   * <p>An 8-K still owes its metadata and, when vectorization is on, its chunks. Dropping those
   * alongside earnings would retire filings that are genuinely missing data.
   */
  @Test void testNarrowingLeavesTheOtherExpectationsIntact() {
    Set<FormType.OutputType> narrowed = FormType.FORM_8K.getExpectedOutputs(true, "5.02");

    assertTrue(narrowed.contains(FormType.OutputType.METADATA),
        "every 8-K still owes its filing metadata");
    assertTrue(narrowed.contains(FormType.OutputType.CHUNKS),
        "an 8-K still owes its chunks when vectorization is enabled");
    assertFalse(narrowed.contains(FormType.OutputType.EARNINGS),
        "only the earnings expectation is narrowed away");
  }

  @Test void testUnnarrowedCallIsUnchanged() {
    assertTrue(FormType.FORM_8K.getExpectedOutputs(false)
            .contains(FormType.OutputType.EARNINGS),
        "the single-argument form keeps its original meaning for callers that have no items");
  }
}
