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
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for reading holdings out of a 13F information table.
 *
 * <p>Large filers (JPMorgan Chase, accession 0000019617-23-000542, among them) publish the table
 * with a namespace prefix on every element, {@code <ns1:infoTable><ns1:nameOfIssuer>}; others use a
 * default namespace. Both shapes must yield the same fields.
 */
@Tag("unit")
class ThirteenFHoldingsExtractionTest {

  private static final String PREFIXED =
      "<?xml version='1.0' encoding='UTF-8'?>"
      + "<ns1:informationTable xmlns:ns1='http://www.sec.gov/edgar/document/thirteenf/informationtable'>"
      + row("ns1:", "ISHARES CORE TOTAL BOND ETF", "46434V613", "549804159", "23")
      + row("ns1:", "ISHARES CORE TOTAL BOND ETF", "46434V613", "549804159", "24")
      + "</ns1:informationTable>";

  private static final String DEFAULT_NS =
      "<?xml version='1.0' encoding='UTF-8'?>"
      + "<informationTable xmlns='http://www.sec.gov/edgar/document/thirteenf/informationtable'>"
      + row("", "APPLE INC", "037833100", "1515058", null)
      + "</informationTable>";

  private static String row(String p, String issuer, String cusip, String value, String other) {
    return "<" + p + "infoTable>"
        + "<" + p + "nameOfIssuer>" + issuer + "</" + p + "nameOfIssuer>"
        + "<" + p + "titleOfClass>COM</" + p + "titleOfClass>"
        + "<" + p + "cusip>" + cusip + "</" + p + "cusip>"
        + "<" + p + "value>" + value + "</" + p + "value>"
        + "<" + p + "shrsOrPrnAmt><" + p + "sshPrnamt>15643</" + p + "sshPrnamt>"
        + "<" + p + "sshPrnamtType>SH</" + p + "sshPrnamtType></" + p + "shrsOrPrnAmt>"
        + "<" + p + "investmentDiscretion>SOLE</" + p + "investmentDiscretion>"
        + (other == null ? "" : "<" + p + "otherManager>" + other + "</" + p + "otherManager>")
        + "<" + p + "votingAuthority><" + p + "Sole>1</" + p + "Sole><" + p + "Shared>2</" + p
        + "Shared><" + p + "None>3</" + p + "None></" + p + "votingAuthority>"
        + "</" + p + "infoTable>";
  }

  private static Document parse(String xml) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    return factory.newDocumentBuilder().parse(
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }

  private static List<Map<String, Object>> extract(String xml, String filingDate) throws Exception {
    return new XbrlToParquetConverter(null).extract13FHoldings(parse(xml), "0000019617",
        "13F-HR", filingDate, "0000019617-23-000542", 2023, "2023-09-30");
  }

  @Test void testPrefixedElementsYieldEveryField() throws Exception {
    List<Map<String, Object>> rows = extract(PREFIXED, "2023-11-14");

    assertEquals(2, rows.size());
    Map<String, Object> r = rows.get(0);
    assertEquals("ISHARES CORE TOTAL BOND ETF", r.get("issuer_name"));
    assertEquals("COM", r.get("title_of_class"));
    assertEquals("46434V613", r.get("cusip"));
    assertEquals(15643d, r.get("shares_or_principal"));
    assertEquals("SH", r.get("shares_or_principal_type"));
    assertEquals("SOLE", r.get("investment_discretion"));
    assertEquals(1d, r.get("voting_authority_sole"));
    assertEquals(2d, r.get("voting_authority_shared"));
    assertEquals(3d, r.get("voting_authority_none"));
  }

  @Test void testDefaultNamespaceElementsYieldEveryField() throws Exception {
    Map<String, Object> r = extract(DEFAULT_NS, "2023-11-14").get(0);

    assertEquals("APPLE INC", r.get("issuer_name"));
    assertEquals("037833100", r.get("cusip"));
    assertNull(r.get("other_manager"));
  }

  @Test void testRowsDifferingOnlyByOtherManagerStayDistinct() throws Exception {
    List<Map<String, Object>> rows = extract(PREFIXED, "2023-11-14");

    assertEquals("23", rows.get(0).get("other_manager"));
    assertEquals("24", rows.get(1).get("other_manager"));
  }

  @Test void testValueFromJanuary3rd2023IsDollarsAndStoredAsThousands() throws Exception {
    assertEquals(1515.058d, extract(DEFAULT_NS, "2023-01-03").get(0).get("value_thousands"));
    assertEquals(1515.058d, extract(DEFAULT_NS, "2024-05-15").get(0).get("value_thousands"));
  }

  @Test void testValueBeforeJanuary3rd2023IsAlreadyThousands() throws Exception {
    assertEquals(1515058d, extract(DEFAULT_NS, "2023-01-02").get(0).get("value_thousands"));
    assertEquals(1515058d, extract(DEFAULT_NS, "2019-08-14").get(0).get("value_thousands"));
  }
}
