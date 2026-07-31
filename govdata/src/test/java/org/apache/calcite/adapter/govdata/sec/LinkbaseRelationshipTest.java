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
import org.w3c.dom.Document;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for reading relationships out of an XBRL linkbase.
 *
 * <p>Relationships are published in linkbase files beside a filing, not inside the filing
 * document, so a converter reading only the document finds none and writes no rows — silently,
 * since the filing is still recorded as processed. Measured on 2021, 4,846 of 22,567 annual and
 * quarterly filings had no relationships for this reason, and reprocessing recovered 5% of them
 * because re-reading the same document cannot produce what was never in it.
 */
@Tag("unit")
class LinkbaseRelationshipTest {

  private static final String PRESENTATION =
      "<?xml version='1.0'?>"
      + "<linkbase xmlns='http://www.xbrl.org/2003/linkbase'"
      + "          xmlns:xlink='http://www.w3.org/1999/xlink'>"
      + "  <presentationLink xlink:role='http://acme.com/role/BalanceSheet'>"
      + "    <loc xlink:href='acme-20211231.xsd#us-gaap_AssetsAbstract' xlink:label='a'/>"
      + "    <loc xlink:href='acme-20211231.xsd#us-gaap_Assets' xlink:label='b'/>"
      + "    <presentationArc xlink:arcrole='http://www.xbrl.org/2003/arcrole/parent-child'"
      + "        xlink:from='a' xlink:to='b' order='1'"
      + "        preferredLabel='http://www.xbrl.org/2003/role/totalLabel'/>"
      + "  </presentationLink>"
      + "</linkbase>";

  private static Document parse(String xml) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    return factory.newDocumentBuilder().parse(
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }

  private static List<Map<String, Object>> read(String xml, String linkbaseType) throws Exception {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    // Reading a linkbase touches no storage; the provider is only used when writing parquet.
    new XbrlToParquetConverter(null).readExtendedLinks(
        parse(xml), linkbaseType, "0000123456", "0000123456-21-000001", "2021-03-01", 2021, rows);
    return rows;
  }

  @Test void testPresentationArcBecomesARelationship() throws Exception {
    List<Map<String, Object>> rows = read(PRESENTATION, "presentation");

    assertEquals(1, rows.size());
    Map<String, Object> row = rows.get(0);
    assertEquals("presentation", row.get("linkbase_type"));
    assertEquals("http://www.xbrl.org/2003/arcrole/parent-child", row.get("arc_role"));
    assertEquals("http://acme.com/role/BalanceSheet", row.get("link_role"));
    assertEquals("us-gaap:AssetsAbstract", row.get("from_concept"));
    assertEquals("us-gaap:Assets", row.get("to_concept"));
    assertEquals(Double.valueOf(1), row.get("order"));
    assertEquals("http://www.xbrl.org/2003/role/totalLabel", row.get("preferred_label"));
  }

  /**
   * Every column the schema declares NOT NULL must be filled on every row.
   *
   * <p>A row failing this cannot be written, so the filing's whole relationship set is lost at the
   * point of writing rather than at the point of reading — the failure surfaces far from its cause.
   */
  @Test void testRequiredColumnsAreAlwaysPopulated() throws Exception {
    for (Map<String, Object> row : read(PRESENTATION, "presentation")) {
      for (String required
          : new String[] {"cik", "accession_number", "filing_date", "year", "linkbase_type",
              "arc_role", "from_concept", "to_concept"}) {
        assertTrue(row.get(required) != null, required + " must not be null");
      }
    }
  }

  /**
   * Locator labels are scoped to the extended link that declares them.
   *
   * <p>The same label names different concepts in two links of one file — filing agents reuse
   * short labels freely. Building the map per file instead of per link silently attaches arcs to
   * whichever concept was loaded last, producing relationships that were never filed.
   */
  @Test void testLocatorLabelsDoNotLeakBetweenLinks() throws Exception {
    String xml =
        "<?xml version='1.0'?>"
        + "<linkbase xmlns='http://www.xbrl.org/2003/linkbase'"
        + "          xmlns:xlink='http://www.w3.org/1999/xlink'>"
        + "  <calculationLink xlink:role='http://acme.com/role/A'>"
        + "    <loc xlink:href='a.xsd#us-gaap_Assets' xlink:label='x'/>"
        + "    <loc xlink:href='a.xsd#us-gaap_Cash' xlink:label='y'/>"
        + "    <calculationArc xlink:arcrole='http://www.xbrl.org/2003/arcrole/summation-item'"
        + "        xlink:from='x' xlink:to='y' weight='1' order='1'/>"
        + "  </calculationLink>"
        + "  <calculationLink xlink:role='http://acme.com/role/B'>"
        + "    <loc xlink:href='a.xsd#us-gaap_Liabilities' xlink:label='x'/>"
        + "    <loc xlink:href='a.xsd#us-gaap_AccountsPayable' xlink:label='y'/>"
        + "    <calculationArc xlink:arcrole='http://www.xbrl.org/2003/arcrole/summation-item'"
        + "        xlink:from='x' xlink:to='y' weight='-1' order='2'/>"
        + "  </calculationLink>"
        + "</linkbase>";

    List<Map<String, Object>> rows = read(xml, "calculation");
    Map<String, String> byRole = new HashMap<String, String>();
    for (Map<String, Object> row : rows) {
      byRole.put((String) row.get("link_role"),
          row.get("from_concept") + "->" + row.get("to_concept"));
    }

    assertEquals(2, rows.size());
    assertEquals("us-gaap:Assets->us-gaap:Cash", byRole.get("http://acme.com/role/A"));
    assertEquals("us-gaap:Liabilities->us-gaap:AccountsPayable",
        byRole.get("http://acme.com/role/B"));
  }

  @Test void testCalculationWeightIsCaptured() throws Exception {
    String xml = PRESENTATION
        .replace("presentationLink", "calculationLink")
        .replace("presentationArc", "calculationArc")
        .replace("order='1'", "order='1' weight='-1'");

    assertEquals(Double.valueOf(-1), read(xml, "calculation").get(0).get("weight"));
  }

  /**
   * A prohibited arc states the relationship does not hold.
   *
   * <p>It removes one inherited from a base taxonomy. Dropping the flag would store the arc as an
   * assertion, so the graph would claim a relationship the filer explicitly deleted.
   */
  @Test void testProhibitionIsPreserved() throws Exception {
    String xml = PRESENTATION.replace("order='1'", "order='1' use='prohibited' priority='2'");
    Map<String, Object> row = read(xml, "presentation").get(0);

    assertEquals("prohibited", row.get("arc_use"));
    assertEquals(Integer.valueOf(2), row.get("arc_priority"));
  }

  @Test void testDimensionalAttributesAreCaptured() throws Exception {
    String xml =
        "<?xml version='1.0'?>"
        + "<linkbase xmlns='http://www.xbrl.org/2003/linkbase'"
        + "          xmlns:xlink='http://www.w3.org/1999/xlink'"
        + "          xmlns:xbrldt='http://xbrl.org/2005/xbrldt'>"
        + "  <definitionLink xlink:role='http://acme.com/role/D'>"
        + "    <loc xlink:href='a.xsd#us-gaap_StatementTable' xlink:label='t'/>"
        + "    <loc xlink:href='a.xsd#us-gaap_StatementClassOfStockAxis' xlink:label='d'/>"
        + "    <definitionArc xlink:arcrole='http://xbrl.org/int/dim/arcrole/hypercube-dimension'"
        + "        xlink:from='t' xlink:to='d' xbrldt:closed='true'"
        + "        xbrldt:contextElement='segment' order='1'/>"
        + "  </definitionLink>"
        + "</linkbase>";

    Map<String, Object> row = read(xml, "definition").get(0);
    assertEquals(Boolean.TRUE, row.get("closed"));
    assertEquals("segment", row.get("context_element"));
  }

  /**
   * Label and reference arcs point at documentation, not at a second concept.
   *
   * <p>Their target is a resource declared inline rather than a locator, so there is no concept to
   * put in to_concept. Emitting them anyway is how a relationships table fills with rows whose
   * endpoints are label stubs — 10.7% of the existing table points at a {@code _lbl} target.
   */
  @Test void testArcsToResourcesAreSkipped() throws Exception {
    String xml =
        "<?xml version='1.0'?>"
        + "<linkbase xmlns='http://www.xbrl.org/2003/linkbase'"
        + "          xmlns:xlink='http://www.w3.org/1999/xlink'>"
        + "  <labelLink xlink:role='http://www.xbrl.org/2003/role/link'>"
        + "    <loc xlink:href='a.xsd#us-gaap_Assets' xlink:label='c'/>"
        + "    <label xlink:label='lbl' xlink:role='http://www.xbrl.org/2003/role/label'>Assets</label>"
        + "    <labelArc xlink:arcrole='http://www.xbrl.org/2003/arcrole/concept-label'"
        + "        xlink:from='c' xlink:to='lbl'/>"
        + "  </labelLink>"
        + "</linkbase>";

    assertEquals(0, read(xml, "label").size(),
        "an arc whose target is a label resource is not a relationship between concepts");
  }

  @Test void testHrefFragmentBecomesAPrefixedConcept() {
    assertEquals("us-gaap:Assets",
        XbrlToParquetConverter.conceptFromHref("us-gaap-2021.xsd#us-gaap_Assets"));
    assertEquals("dei:EntityRegistrantName",
        XbrlToParquetConverter.conceptFromHref("dei-2021.xsd#dei_EntityRegistrantName"));
  }

  /**
   * Only the first underscore separates prefix from local name.
   *
   * <p>A company namespace is often the CIK, and local names do contain underscores. Splitting on
   * the last one, or on all of them, mangles the concept so it no longer joins to
   * financial_line_items.
   */
  @Test void testOnlyTheFirstUnderscoreSeparates() {
    assertEquals("ck0001325676:SomeMember_Detail",
        XbrlToParquetConverter.conceptFromHref("x.xsd#ck0001325676_SomeMember_Detail"));
  }

  @Test void testHrefWithoutFragmentOrSeparator() {
    assertEquals("Assets", XbrlToParquetConverter.conceptFromHref("Assets"));
    assertNull(XbrlToParquetConverter.conceptFromHref(null));
    assertNull(XbrlToParquetConverter.conceptFromHref(""));
  }
}
