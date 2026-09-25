/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.govdata.law;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ScotusModsParser}; the XML mirrors GovInfo's real MODS records for
 * 532 U.S. 23 (curated outcome fields present) and 583 U.S. 17 (only citation and title).
 */
@Tag("unit")
class ScotusModsParserTest {

  private static final String NS = "xmlns=\"http://www.loc.gov/mods/v3\"";

  private static final String HOST =
      "<relatedItem type=\"host\" ID=\"P0b\"><titleInfo><title>United States Reports, "
          + "HOST-VOLUME</title></titleInfo><extension><docClass>USREPORTS</docClass>"
          + "<volume>HOST</volume><courtTerm>9999</courtTerm></extension></relatedItem>";

  private static final String PUBLISHER =
      "<name type=\"corporate\"><namePart>United States Government Publishing Office"
          + "</namePart></name>";

  private static final String RICH =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?><mods " + NS + " version=\"3.3\">" + PUBLISHER
          + "<extension><collectionCode>USREPORTS</collectionCode></extension>"
          + "<titleInfo><title>Traffix Devices, Inc. v. Marketing Displays, Inc., 532 U.S. 23 "
          + "(2001)</title><partNumber>532 U.S. 23</partNumber></titleInfo>"
          + "<extension><accessId>USREPORTS-532-23</accessId><reportNumber>23</reportNumber>"
          + "<sequenceNumber>8</sequenceNumber><decisionDate>2001-03-20</decisionDate>"
          + "<courtTerm>2000</courtTerm><usCitation>532 U.S. 23</usCitation>"
          + "<fullCitation>532 U.S. 23 (2001)</fullCitation>"
          + "<subject authority=\"llc\" uri=\"u\"><topic>Trade Dress</topic>"
          + "<topic>Trademarks</topic></subject>"
          + "<subject authority=\"scdb\" uri=\"u\"><topic>Patents and Copyrights: Copyright"
          + "</topic><topic>Economic Activity</topic></subject>"
          + "<docketId>2000-031-01</docketId><petitioner>Manufacturer</petitioner>"
          + "<respondent>Manufacturer</respondent><jurisdiction>Cert</jurisdiction>"
          + "<caseOrigin>Michigan Eastern U.S. District Court</caseOrigin>"
          + "<caseSource>U.S. Court of Appeals, Sixth Circuit</caseSource>"
          + "<certReason>Federal court conflict</certReason>"
          + "<chiefJustice>William H. Rehnquist</chiefJustice>"
          + "<dateArgument>2000-11-29</dateArgument>"
          + "<authorityDecision>Statutory construction</authorityDecision>"
          + "<law><type>Infrequently litigated statutes</type>"
          + "<legalProvisions>Infrequently litigated statutes</legalProvisions></law>"
          + "<decisionType>Opinion of the court (orally argued)</decisionType>"
          + "<unconstitutionality>No declaration of unconstitutionality</unconstitutionality>"
          + "<disposition>Reversed and remanded</disposition>"
          + "<partyWinning>Petitioning party received a favorable disposition</partyWinning>"
          + "<majorityOpinion><writer>Anthony M. Kennedy</writer>"
          + "<assigner>William H. Rehnquist</assigner></majorityOpinion>"
          + "<votes><majority>9</majority><minority>0</minority></votes></extension>"
          + HOST + "</mods>";

  private static final String SPARSE =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?><mods " + NS + " version=\"3.3\">" + PUBLISHER
          + "<titleInfo><title>Hamer v. Neighborhood Housing Services of Chicago, 583 U.S. 17 "
          + "(2017)</title></titleInfo>"
          + "<extension><accessId>USREPORTS-583-17</accessId><reportNumber>17</reportNumber>"
          + "<sequenceNumber>9</sequenceNumber>"
          + "<decisionDate notSpecified=\"day-month\">2017-01-01</decisionDate>"
          + "<usCitation>583 U.S. 17</usCitation><fullCitation>583 U.S. 17 (2017)</fullCitation>"
          + "</extension>" + HOST + "</mods>";

  private static Map<String, Object> parse(String xml) throws XMLStreamException {
    return ScotusModsParser.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }

  @Test void richRecordCarriesTheCuratedOutcomeFields() throws Exception {
    Map<String, Object> row = parse(RICH);
    assertEquals("USREPORTS-532-23", row.get("granule_id"));
    assertEquals("532 U.S. 23", row.get("us_citation"));
    assertEquals("Reversed and remanded", row.get("disposition"));
    assertEquals("Petitioning party received a favorable disposition", row.get("party_winning"));
    assertEquals("2000-031-01", row.get("govinfo_docket_id"));
    assertEquals("Cert", row.get("jurisdiction"));
    assertEquals("Michigan Eastern U.S. District Court", row.get("case_origin"));
    assertEquals("U.S. Court of Appeals, Sixth Circuit", row.get("case_source"));
    assertEquals("Opinion of the court (orally argued)", row.get("decision_type"));
    assertEquals("Anthony M. Kennedy", row.get("opinion_writer"));
    assertEquals("2000-11-29", row.get("argument_date"));
    assertEquals("2000", row.get("court_term"));
  }

  @Test void numericFieldsAreIntegers() throws Exception {
    Map<String, Object> row = parse(RICH);
    assertEquals(Integer.valueOf(9), row.get("votes_majority"));
    assertEquals(Integer.valueOf(0), row.get("votes_minority"));
    assertEquals(Integer.valueOf(23), row.get("first_page"));
    assertEquals(Integer.valueOf(8), row.get("sequence_number"));
  }

  @Test void fullDecisionDateYieldsDateAndYear() throws Exception {
    Map<String, Object> row = parse(RICH);
    assertEquals("2001-03-20", row.get("decision_date"));
    assertEquals(Integer.valueOf(2001), row.get("decision_year"));
  }

  @Test void subjectTopicsAreSplitByAuthority() throws Exception {
    Map<String, Object> row = parse(RICH);
    assertEquals(Arrays.asList("Trade Dress", "Trademarks"), row.get("lc_topics"));
    assertEquals(Arrays.asList("Patents and Copyrights: Copyright", "Economic Activity"),
        row.get("scdb_issue_areas"));
    assertEquals(Arrays.asList("Infrequently litigated statutes"), row.get("law_types"));
    assertEquals(Arrays.asList("Infrequently litigated statutes"), row.get("legal_provisions"));
  }

  @Test void theEnclosingVolumeBlockIsNotReadAsTheCase() throws Exception {
    Map<String, Object> row = parse(RICH);
    assertEquals("Traffix Devices, Inc. v. Marketing Displays, Inc., 532 U.S. 23 (2001)",
        row.get("case_title"));
    assertEquals("2000", row.get("court_term"));
    assertFalse(row.toString().contains("HOST"));
    assertFalse(row.toString().contains("9999"));
  }

  @Test void yearOnlyDecisionDateHasNoDate() throws Exception {
    Map<String, Object> row = parse(SPARSE);
    assertFalse(row.containsKey("decision_date"));
    assertEquals(Integer.valueOf(2017), row.get("decision_year"));
  }

  @Test void fieldsTheRecordLacksAreAbsentNotDefaulted() throws Exception {
    Map<String, Object> row = parse(SPARSE);
    for (String key : new String[] {"disposition", "party_winning", "govinfo_docket_id",
        "votes_majority", "lc_topics", "scdb_issue_areas", "court_term", "case_origin"}) {
      assertFalse(row.containsKey(key), key);
    }
    assertTrue(row.containsKey("us_citation"));
    assertEquals("Hamer v. Neighborhood Housing Services of Chicago, 583 U.S. 17 (2017)",
        row.get("case_title"));
  }

  @Test void aNonNumericPageIsAnErrorNotASilentNull() {
    String bad = SPARSE.replace("<reportNumber>17</reportNumber>",
        "<reportNumber>seventeen</reportNumber>");
    assertThrows(IllegalArgumentException.class, () -> parse(bad));
  }
}
