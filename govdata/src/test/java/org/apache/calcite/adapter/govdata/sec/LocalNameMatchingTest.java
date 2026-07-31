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
import org.w3c.dom.Element;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for finding XBRL elements whatever prefix the filer used.
 *
 * <p>A traditional XBRL instance writes {@code <xbrli:context>}; once inline XBRL has been
 * flattened the same element is {@code <context>}. To getElementsByTagName, which matches the
 * qualified name, those are two different tags — so asking for the bare name skips every prefixed
 * filing and writes no contexts for it, while still recording the filing as processed.
 *
 * <p>That is what happened: filing_contexts coverage of annual and quarterly filings ran from
 * 41.3% in 2019 to 95.2% in 2023, tracking the retreat of traditional instances rather than
 * anything about the filings. Reprocessing recovered 10% of the gap, because running the same
 * lookup over the same document finds the same nothing.
 */
@Tag("unit")
class LocalNameMatchingTest {

  private static final String PREFIXED =
      "<?xml version='1.0'?>"
      + "<xbrl xmlns:xbrli='http://www.xbrl.org/2003/instance'>"
      + "  <xbrli:context id='c1'>"
      + "    <xbrli:period><xbrli:startDate>2021-01-01</xbrli:startDate>"
      + "      <xbrli:endDate>2021-12-31</xbrli:endDate></xbrli:period>"
      + "  </xbrli:context>"
      + "  <xbrli:context id='c2'>"
      + "    <xbrli:period><xbrli:instant>2021-12-31</xbrli:instant></xbrli:period>"
      + "  </xbrli:context>"
      + "</xbrl>";

  private static final String BARE =
      "<?xml version='1.0'?>"
      + "<xbrl>"
      + "  <context id='c1'>"
      + "    <period><startDate>2021-01-01</startDate><endDate>2021-12-31</endDate></period>"
      + "  </context>"
      + "</xbrl>";

  private static Document parse(String xml, boolean namespaceAware) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(namespaceAware);
    return factory.newDocumentBuilder().parse(
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }

  @Test void testPrefixedElementsAreFound() throws Exception {
    List<Element> contexts =
        XbrlToParquetConverter.elementsByLocalName(parse(PREFIXED, true), "context");

    assertEquals(2, contexts.size(), "xbrli:context must be found by its local name");
    assertEquals("c1", contexts.get(0).getAttribute("id"));
  }

  @Test void testUnprefixedElementsAreStillFound() throws Exception {
    assertEquals(1,
        XbrlToParquetConverter.elementsByLocalName(parse(BARE, true), "context").size(),
        "flattened inline XBRL keeps working");
  }

  /**
   * The fallback parser is not namespace aware, so getLocalName returns null there.
   *
   * <p>Deriving the local name from the node name instead keeps the same lookup working for a
   * document that had to be recovered by the lenient parser.
   */
  @Test void testWorksWhenTheParserIsNotNamespaceAware() throws Exception {
    assertEquals(2,
        XbrlToParquetConverter.elementsByLocalName(parse(PREFIXED, false), "context").size(),
        "a document parsed without namespace awareness must still match");
  }

  @Test void testNestedElementsAreFoundWithinAContext() throws Exception {
    Element context =
        XbrlToParquetConverter.elementsByLocalName(parse(PREFIXED, true), "context").get(0);

    assertEquals("2021-01-01",
        XbrlToParquetConverter.elementsByLocalName(context, "startDate").get(0).getTextContent());
    assertEquals(0, XbrlToParquetConverter.elementsByLocalName(context, "instant").size(),
        "the first context is a duration, so it has no instant");
  }

  /**
   * A local name must not match a different element that merely ends with it.
   *
   * <p>{@code endDate} and {@code startDate} both end in "Date"; matching loosely would put a
   * start date in the period_end column.
   */
  @Test void testMatchIsOnTheWholeLocalName() throws Exception {
    Document doc = parse(PREFIXED, true);

    assertEquals(1, XbrlToParquetConverter.elementsByLocalName(doc, "endDate").size());
    assertEquals(0, XbrlToParquetConverter.elementsByLocalName(doc, "Date").size());
    assertEquals(0, XbrlToParquetConverter.elementsByLocalName(doc, "ntext").size());
  }
}
