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

import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for extracting a 13F filer's name from its {@code <filingManager>} cover-page element.
 *
 * <p>{@code getElementText(doc, "filingManager")} — a plain, un-narrowed lookup — does not return
 * the filer's name. {@code Node.getTextContent} concatenates every descendant text node, and
 * {@code <filingManager>} wraps both {@code <name>} and {@code <address>}, so the call returns the
 * company name and its street address run together with no separator. The fixture below reproduces
 * that shape — filingManager containing name and an address whose fields use the {@code ns1:}
 * prefix — confirmed against a real cover page fetched live (CIK 0001707921, accession
 * 0001104659-22-117816); the enclosing element nesting is reconstructed, not a byte-exact copy.
 *
 * <p>This bug never surfaced in stored data because a separate bug (fixed in aabff1033) meant the
 * document parsed here was, until now, always EDGAR's HTML viewer rendering instead of this real
 * XML — which has no filingManager element at all, so extraction returned null rather than
 * garbled text. Fixing that one exposes this one: the currently-running SEC repair jobs carry
 * aabff1033 and, without this fix, would begin writing the garbled form for every 13F-HR
 * filing_metadata row and manager_name they touch from here on.
 */
@Tag("unit")
class FilingManagerNameTest {

  /** CIK 0001707921, accession 0001104659-22-117816 — the exact structure, trimmed to the parts
   *  relevant here. Fetched live 2026-07-31. */
  private static final String COVER_PAGE =
      "<?xml version='1.0' encoding='UTF-8'?>"
      + "<edgarSubmission xmlns='http://www.sec.gov/edgar/thirteenffiler'"
      + "                 xmlns:ns1='http://www.sec.gov/edgar/common'>"
      + "  <formData>"
      + "    <coverPage>"
      + "      <filingManager>"
      + "        <name>ECP ControlCo, LLC</name>"
      + "        <address>"
      + "          <ns1:street1>40 Beechwood Road</ns1:street1>"
      + "          <ns1:city>Summit</ns1:city>"
      + "          <ns1:stateOrCountry>NJ</ns1:stateOrCountry>"
      + "          <ns1:zipCode>07901</ns1:zipCode>"
      + "        </address>"
      + "      </filingManager>"
      + "    </coverPage>"
      + "  </formData>"
      + "</edgarSubmission>";

  private static Document parse(String xml) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    return factory.newDocumentBuilder().parse(
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }

  @Test void testNarrowedLookupReturnsOnlyTheName() throws Exception {
    String name = new XbrlToParquetConverter(null)
        .getElementText(parse(COVER_PAGE), "filingManager", "name");

    assertEquals("ECP ControlCo, LLC", name);
  }

  /**
   * Documents the bug this fix removes: the un-narrowed 2-argument lookup on the same fixture
   * returns the name and address concatenated, not the name alone. If this assertion ever starts
   * failing, getElementText(Document, String) changed and this test's premise needs revisiting —
   * it is not asserting desired behavior, it is pinning the defect being routed around.
   */
  @Test void testUnnarrowedLookupWouldHaveReturnedTheWholeElement() throws Exception {
    String wholeElementText = new XbrlToParquetConverter(null)
        .getElementText(parse(COVER_PAGE), "filingManager");

    assertEquals("ECP ControlCo, LLC                  40 Beechwood Road"
        + "          Summit          NJ          07901", wholeElementText);
  }

  @Test void testMissingFilingManagerReturnsNull() throws Exception {
    String xml = "<?xml version='1.0'?><edgarSubmission><formData/></edgarSubmission>";

    assertNull(new XbrlToParquetConverter(null).getElementText(parse(xml), "filingManager", "name"));
  }

  /**
   * A filingManager with no name child — malformed, but must not throw or fall back to the
   * address text.
   */
  @Test void testFilingManagerWithoutNameReturnsNull() throws Exception {
    String xml = "<?xml version='1.0'?><edgarSubmission><formData><coverPage>"
        + "<filingManager><address><street1>1 Main St</street1></address></filingManager>"
        + "</coverPage></formData></edgarSubmission>";

    assertNull(new XbrlToParquetConverter(null).getElementText(parse(xml), "filingManager", "name"));
  }
}
