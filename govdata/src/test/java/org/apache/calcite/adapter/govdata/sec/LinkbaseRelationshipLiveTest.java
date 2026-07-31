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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reads a real filing's linkbases from EDGAR and checks what comes back.
 *
 * <p>The unit tests exercise this parser on linkbases written to demonstrate a rule. Real
 * linkbases are produced by dozens of filing agents and are where the assumptions actually get
 * tested — that locators resolve, that an href fragment separates prefix from local name on its
 * first underscore, that arcs the parser declines to emit really are label and reference arcs.
 *
 * <p>Fixed to one filing whose contents will not change: EDGAR filings are immutable once
 * accepted, so the expected counts below are stable. They were established independently before
 * this parser existed.
 */
@Tag("integration")
class LinkbaseRelationshipLiveTest {

  private static final String CIK = "0001325676";
  private static final String ACCESSION = "0001193125-21-093815";

  /** Established from this filing's linkbases before the Java parser was written. */
  private static final int EXPECTED_TOTAL = 948;
  private static final int EXPECTED_CALCULATION = 47;
  private static final int EXPECTED_DEFINITION = 443;
  private static final int EXPECTED_PRESENTATION = 458;

  private static List<Map<String, Object>> extract() {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    // Reading linkbases touches no storage; the provider is only used when writing parquet.
    new XbrlToParquetConverter(null)
        .extractLinkbaseRelationships(CIK, ACCESSION, "2021-03-26", rows);
    return rows;
  }

  private static int countOf(List<Map<String, Object>> rows, String linkbaseType) {
    int n = 0;
    for (Map<String, Object> row : rows) {
      if (linkbaseType.equals(row.get("linkbase_type"))) {
        n++;
      }
    }
    return n;
  }

  @Test void testRealFilingYieldsTheExpectedRelationships() {
    List<Map<String, Object>> rows = extract();

    assertEquals(EXPECTED_TOTAL, rows.size(), "total relationships from all three linkbases");
    assertEquals(EXPECTED_CALCULATION, countOf(rows, "calculation"));
    assertEquals(EXPECTED_DEFINITION, countOf(rows, "definition"));
    assertEquals(EXPECTED_PRESENTATION, countOf(rows, "presentation"));
  }

  /**
   * Every locator an arc references must resolve to a concept.
   *
   * <p>An unresolved endpoint is dropped rather than written, so this failing would show up as a
   * quietly short relationship set rather than an error — the shape of loss this whole change
   * exists to remove.
   */
  @Test void testEveryRowHasResolvedEndpoints() {
    for (Map<String, Object> row : extract()) {
      String from = (String) row.get("from_concept");
      String to = (String) row.get("to_concept");
      assertTrue(from != null && from.contains(":"),
          "from_concept should be a prefixed name, got " + from);
      assertTrue(to != null && to.contains(":"),
          "to_concept should be a prefixed name, got " + to);
    }
  }

  /**
   * Concepts must be spelled the way financial_line_items spells them.
   *
   * <p>The two tables are meant to join — that is what reconstructing a statement hierarchy or
   * checking a rollup requires. The stored data does not currently permit it, because concepts
   * were written as {@code us-gaapAssets} or {@code loc_us-gaap_Assets}.
   */
  @Test void testConceptsUseThePrefixedFormThatJoins() {
    Set<String> concepts = new HashSet<String>();
    for (Map<String, Object> row : extract()) {
      concepts.add((String) row.get("from_concept"));
      concepts.add((String) row.get("to_concept"));
    }

    assertTrue(concepts.contains("us-gaap:Liabilities"),
        "expected a standard us-gaap concept in prefixed form; got e.g. " + firstOf(concepts));
    for (String concept : concepts) {
      assertTrue(concept.matches("^[A-Za-z][A-Za-z0-9-]*:[A-Za-z0-9_.-]+$"),
          "not a prefixed concept name: " + concept);
    }
  }

  @Test void testCalculationArcsCarryWeights() {
    int weighted = 0;
    for (Map<String, Object> row : extract()) {
      if ("calculation".equals(row.get("linkbase_type")) && row.get("weight") != null) {
        weighted++;
      }
    }
    assertEquals(EXPECTED_CALCULATION, weighted,
        "every summation-item arc states the weight its child contributes");
  }

  private static String firstOf(Set<String> values) {
    return values.isEmpty() ? "<none>" : values.iterator().next();
  }
}
