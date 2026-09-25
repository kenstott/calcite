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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the streaming section parser in {@link UsCodeXmlDataProvider}.
 */
@Tag("unit")
class UsCodeXmlDataProviderTest {

  private static final String NS =
      " xmlns=\"http://xml.house.gov/schemas/uslm/1.0\""
          + " xmlns:dc=\"http://purl.org/dc/elements/1.1/\"";

  private static final String TITLE_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<uscDoc" + NS + " identifier=\"/us/usc/t5\">\n"
          + "<meta><dc:title>Title 5</dc:title><docNumber>5</docNumber>"
          + "<property role=\"is-positive-law\">yes</property></meta>\n"
          + "<main><title identifier=\"/us/usc/t5\"><num value=\"5\">Title 5—</num>"
          + "<heading>GOVERNMENT ORGANIZATION</heading>\n"
          + "<note><p>Current through 119-111</p></note>\n"
          + "<toc><tocItem>Ignored table of contents</tocItem></toc>\n"
          + "<chapter identifier=\"/us/usc/t5/ch1\"><num value=\"1\">CHAPTER 1—</num>"
          + "<heading>ORGANIZATION</heading>\n"
          + "<section identifier=\"/us/usc/t5/s101\"><num value=\"101\">§ 101.</num>"
          + "<heading> Executive departments</heading><content>\n"
          + "<p class=\"indent0\">The Executive departments are:</p>\n"
          + "<p class=\"indent3\">The Department   of State.</p>\n"
          + "</content><sourceCredit>(<ref href=\"/us/pl/89/554\">Pub. L. 89–554</ref>, "
          + "<date date=\"1966-09-06\">Sept. 6, 1966</date>)</sourceCredit>"
          + "<notes><note><p>Editorial note that must not appear</p></note></notes></section>\n"
          + "<section identifier=\"/us/usc/t5/s102\" status=\"repealed\">"
          + "<num value=\"102\">§ 102.</num><heading> Repealed</heading>"
          + "<notes><note><p>Repeal history</p></note></notes></section>\n"
          + "<section identifier=\"/us/usc/t5/s103\"><num value=\"103\">§ 103.</num>"
          + "<heading> Structured</heading>"
          + "<subsection><num value=\"a\">(a)</num><heading> In general.</heading>"
          + "<chapeau>Text A</chapeau>"
          + "<paragraph><num value=\"1\">(1)</num><content>one</content></paragraph>"
          + "</subsection></section>\n"
          + "<section identifier=\"/us/usc/t5/s103\"><num value=\"103\">§ 103.</num>"
          + "<heading> Second 103</heading><content><p>Duplicate number.</p></content></section>\n"
          + "</chapter></title></main></uscDoc>\n";

  private static final String APPENDIX_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<uscDoc" + NS + " identifier=\"/us/usc/t5a\">\n"
          + "<meta><dc:title>Title 5 Appendix</dc:title><docNumber>5a</docNumber>"
          + "<property role=\"is-positive-law\">no</property></meta>\n"
          + "<appendix identifier=\"/us/usc/t5a\"><num value=\"5a\">Title 5—APPENDIX</num>"
          + "<heading>FEDERAL ADVISORY COMMITTEE ACT</heading>\n"
          + "<section status=\"transferred\"><num value=\"1\">[§ 1.</num>"
          + "<heading> Transferred]</heading><notes><note><p>x</p></note></notes></section>\n"
          + "</appendix></uscDoc>\n";

  private static List<Map<String, Object>> parse(String xml, File dir) throws IOException {
    File file = new File(dir, "usc.xml");
    Files.write(file.toPath(), xml.getBytes(StandardCharsets.UTF_8));
    Iterator<Map<String, Object>> it = UsCodeXmlDataProvider.streamSections(file, dir, "119-111");
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    while (it.hasNext()) {
      rows.add(it.next());
    }
    return rows;
  }

  private static File tempDir() throws IOException {
    return Files.createTempDirectory("us-code-test-").toFile();
  }

  @Test void testTitleSectionsAreStreamedWithTextAndMetadata() throws Exception {
    File dir = tempDir();
    List<Map<String, Object>> rows = parse(TITLE_XML, dir);
    assertEquals(4, rows.size());

    Map<String, Object> s101 = rows.get(0);
    assertEquals("5", s101.get("title_number"));
    assertEquals("GOVERNMENT ORGANIZATION", s101.get("title_name"));
    assertEquals(Boolean.TRUE, s101.get("is_positive_law"));
    assertEquals("119-111", s101.get("release_point"));
    assertEquals("101", s101.get("section_number"));
    assertEquals(1, s101.get("section_seq"));
    assertEquals("/us/usc/t5/s101", s101.get("usc_identifier"));
    assertEquals("5 U.S.C. § 101", s101.get("citation"));
    assertEquals("Executive departments", s101.get("heading"));
    assertNull(s101.get("status"));
    assertEquals("Chapter 1 — ORGANIZATION", s101.get("hierarchy"));
    assertEquals("1", s101.get("chapter_number"));
    assertEquals("ORGANIZATION", s101.get("chapter_heading"));
    assertEquals("5 U.S.C. § 101 — Executive departments\n"
        + "The Executive departments are:\nThe Department of State.", s101.get("section_text"));
    assertEquals("(Pub. L. 89–554, Sept. 6, 1966)", s101.get("source_credit"));
    assertFalse(((String) s101.get("section_text")).contains("Editorial"));
    assertFalse(((String) s101.get("section_text")).contains("Ignored table"));
  }

  @Test void testRepealedSectionKeepsRowButHasNoText() throws Exception {
    List<Map<String, Object>> rows = parse(TITLE_XML, tempDir());
    Map<String, Object> s102 = rows.get(1);
    assertEquals("repealed", s102.get("status"));
    assertEquals("Repealed", s102.get("heading"));
    assertNull(s102.get("section_text"));
    assertNull(s102.get("source_credit"));
  }

  @Test void testNestedSubsectionsGetOneLineEach() throws Exception {
    List<Map<String, Object>> rows = parse(TITLE_XML, tempDir());
    assertEquals("5 U.S.C. § 103 — Structured\n(a) In general.\nText A\n(1) one",
        rows.get(2).get("section_text"));
  }

  @Test void testDuplicateSectionNumberGetsNextSeq() throws Exception {
    List<Map<String, Object>> rows = parse(TITLE_XML, tempDir());
    assertEquals(1, rows.get(2).get("section_seq"));
    assertEquals(2, rows.get(3).get("section_seq"));
    assertEquals("103", rows.get(3).get("section_number"));
  }

  @Test void testAppendixTitleHasNoIdentifierOrHierarchy() throws Exception {
    List<Map<String, Object>> rows = parse(APPENDIX_XML, tempDir());
    assertEquals(1, rows.size());
    Map<String, Object> row = rows.get(0);
    assertEquals("5a", row.get("title_number"));
    assertEquals("FEDERAL ADVISORY COMMITTEE ACT", row.get("title_name"));
    assertEquals(Boolean.FALSE, row.get("is_positive_law"));
    assertEquals("5A U.S.C. App. § 1", row.get("citation"));
    assertNull(row.get("usc_identifier"));
    assertNull(row.get("hierarchy"));
    assertNull(row.get("chapter_number"));
    assertEquals("transferred", row.get("status"));
    assertNull(row.get("section_text"));
  }

  @Test void testTempDirIsDeletedOnceStreamIsExhausted() throws Exception {
    File dir = tempDir();
    parse(TITLE_XML, dir);
    assertFalse(dir.exists());
  }

  @Test void testSectionWithoutNumValueFails() throws Exception {
    String xml = APPENDIX_XML.replace("<num value=\"1\">", "<num>");
    File dir = tempDir();
    File file = new File(dir, "usc.xml");
    Files.write(file.toPath(), xml.getBytes(StandardCharsets.UTF_8));
    Iterator<Map<String, Object>> it = UsCodeXmlDataProvider.streamSections(file, dir, "119-111");
    assertNotNull(it);
    assertThrows(java.io.UncheckedIOException.class, it::hasNext);
    assertTrue(!dir.exists());
  }
}
