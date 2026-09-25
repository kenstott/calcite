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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the {@code usc_subsections} split in {@link UsCodeXmlDataProvider}: a section
 * is cut only at its own structural boundaries into units of at most
 * {@link UsCodeXmlDataProvider#UNIT_MAX_CHARS} characters, and the units always add back up to the
 * whole section text.
 */
@Tag("unit")
class UsCodeSubsectionsTest {

  private static final String NS =
      " xmlns=\"http://xml.house.gov/schemas/uslm/1.0\""
          + " xmlns:dc=\"http://purl.org/dc/elements/1.1/\"";

  private static String title(String sections) {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<uscDoc" + NS + " identifier=\"/us/usc/t5\">\n"
        + "<meta><dc:title>Title 5</dc:title><docNumber>5</docNumber>"
        + "<property role=\"is-positive-law\">yes</property></meta>\n"
        + "<main><title identifier=\"/us/usc/t5\"><num value=\"5\">Title 5—</num>"
        + "<heading>GOVERNMENT ORGANIZATION</heading>\n"
        + sections
        + "</title></main></uscDoc>\n";
  }

  private static String section(String number, String body) {
    return "<section identifier=\"/us/usc/t5/s" + number + "\"><num value=\"" + number
        + "\">§ " + number + ".</num><heading> Heading " + number + "</heading>"
        + body + "</section>\n";
  }

  private static String sub(String label, String text) {
    return "<subsection><num value=\"" + label + "\">(" + label + ")</num><content><p>" + text
        + "</p></content></subsection>";
  }

  private static String para(String label, String text) {
    return "<paragraph><num value=\"" + label + "\">(" + label + ")</num><content><p>" + text
        + "</p></content></paragraph>";
  }

  /** Exactly {@code n} characters of word-like filler, no leading or trailing space. */
  private static String text(int n) {
    StringBuilder sb = new StringBuilder();
    while (sb.length() < n) {
      sb.append("alpha beta gamma ");
    }
    return sb.substring(0, n - 1).trim() + "x";
  }

  private static List<Map<String, Object>> drain(Iterator<Map<String, Object>> it) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    while (it.hasNext()) {
      rows.add(it.next());
    }
    return rows;
  }

  private static File write(String xml) throws IOException {
    File dir = Files.createTempDirectory("us-code-units-").toFile();
    Files.write(new File(dir, "usc.xml").toPath(), xml.getBytes(StandardCharsets.UTF_8));
    return dir;
  }

  private static List<Map<String, Object>> units(String xml) throws IOException {
    File dir = write(xml);
    return drain(UsCodeXmlDataProvider.streamUnits(new File(dir, "usc.xml"), dir, "119-111"));
  }

  private static List<Map<String, Object>> sections(String xml) throws IOException {
    File dir = write(xml);
    return drain(UsCodeXmlDataProvider.streamSections(new File(dir, "usc.xml"), dir, "119-111"));
  }

  /** The units of one section, joined by newlines, must equal that section's text minus the
   *  citation-and-heading line the sections table adds. */
  private static void assertLossless(String xml, String sectionNumber) throws IOException {
    String sectionText = null;
    for (Map<String, Object> s : sections(xml)) {
      if (sectionNumber.equals(s.get("section_number"))) {
        sectionText = (String) s.get("section_text");
      }
    }
    String body = sectionText.substring(sectionText.indexOf('\n') + 1);
    StringBuilder joined = new StringBuilder();
    for (Map<String, Object> u : units(xml)) {
      if (sectionNumber.equals(u.get("section_number"))) {
        if (joined.length() > 0) {
          joined.append('\n');
        }
        joined.append((String) u.get("unit_text"));
      }
    }
    assertEquals(body, joined.toString());
  }

  private static List<String> column(List<Map<String, Object>> rows, String name) {
    List<String> out = new ArrayList<String>();
    for (Map<String, Object> r : rows) {
      out.add((String) r.get(name));
    }
    return out;
  }

  @Test void testSmallSectionIsOneUnitWithNoPath() throws Exception {
    String xml = title(section("101", "<content><p>The Executive departments are:</p>"
        + "<p>The Department of State.</p></content>"));
    List<Map<String, Object>> rows = units(xml);
    assertEquals(1, rows.size());
    Map<String, Object> u = rows.get(0);
    assertEquals("5", u.get("title_number"));
    assertEquals("101", u.get("section_number"));
    assertEquals(1, u.get("section_seq"));
    assertEquals(1, u.get("unit_seq"));
    assertNull(u.get("unit_path"));
    assertEquals("5 U.S.C. § 101", u.get("citation"));
    assertEquals("The Executive departments are:\nThe Department of State.", u.get("unit_text"));
  }

  @Test void testSmallSectionWithSubsectionsStaysWhole() throws Exception {
    String xml = title(section("102", sub("a", "one") + sub("b", "two")));
    List<Map<String, Object>> rows = units(xml);
    assertEquals(1, rows.size());
    assertNull(rows.get(0).get("unit_path"));
    assertEquals("(a)\none\n(b)\ntwo", rows.get(0).get("unit_text"));
    assertLossless(xml, "102");
  }

  @Test void testSectionWithoutOperativeTextHasNoUnits() throws Exception {
    String xml = title("<section identifier=\"/us/usc/t5/s103\" status=\"repealed\">"
        + "<num value=\"103\">§ 103.</num><heading> Repealed</heading></section>\n"
        + section("104", "<content><p>Text.</p></content>"));
    List<Map<String, Object>> rows = units(xml);
    assertEquals(Arrays.asList("104"), column(rows, "section_number"));
  }

  @Test void testLargeSectionSplitsAtSubsectionsAndPacksNeighbours() throws Exception {
    String xml = title(section("300", sub("a", text(900)) + sub("b", text(900))
        + sub("c", text(900))));
    List<Map<String, Object>> rows = units(xml);
    assertEquals(Arrays.asList("(a) to (b)", "(c)"), column(rows, "unit_path"));
    assertEquals(Arrays.asList("5 U.S.C. § 300(a) to (b)", "5 U.S.C. § 300(c)"),
        column(rows, "citation"));
    assertEquals(Arrays.asList(1, 2), Arrays.asList(rows.get(0).get("unit_seq"),
        rows.get(1).get("unit_seq")));
    for (Map<String, Object> r : rows) {
      assertTrue(((String) r.get("unit_text")).length() <= UsCodeXmlDataProvider.UNIT_MAX_CHARS);
    }
    assertLossless(xml, "300");
  }

  @Test void testOversizedSubsectionSplitsIntoItsParagraphs() throws Exception {
    StringBuilder paragraphs = new StringBuilder();
    for (int i = 1; i <= 5; i++) {
      paragraphs.append(para(String.valueOf(i), text(700)));
    }
    String xml = title(section("301",
        "<subsection><num value=\"a\">(a)</num>" + paragraphs + "</subsection>"
            + sub("b", text(100))));
    List<Map<String, Object>> rows = units(xml);
    assertEquals(Arrays.asList("(a) to (a)(2)", "(a)(3) to (a)(4)", "(a)(5) to (b)"),
        column(rows, "unit_path"));
    for (Map<String, Object> r : rows) {
      assertTrue(((String) r.get("unit_text")).length() <= UsCodeXmlDataProvider.UNIT_MAX_CHARS);
    }
    assertLossless(xml, "301");
  }

  @Test void testAnUnbrokenParagraphOverTheLimitStaysOneUnit() throws Exception {
    String xml = title(section("302", sub("a", text(3000))));
    List<Map<String, Object>> rows = units(xml);
    assertEquals(1, rows.size());
    assertEquals("(a)", rows.get(0).get("unit_path"));
    assertTrue(((String) rows.get(0).get("unit_text")).length()
        > UsCodeXmlDataProvider.UNIT_MAX_CHARS);
    assertLossless(xml, "302");
  }

  @Test void testDuplicateSectionNumbersKeepTheirOwnSequence() throws Exception {
    String xml = title(section("400", "<content><p>First.</p></content>")
        + section("400", "<content><p>Second.</p></content>"));
    List<Map<String, Object>> rows = units(xml);
    assertEquals(Arrays.asList(1, 2), Arrays.asList(rows.get(0).get("section_seq"),
        rows.get(1).get("section_seq")));
    assertEquals("Second.", rows.get(1).get("unit_text"));
  }

  @Test void testTempDirIsDeletedOnceTheUnitStreamIsExhausted() throws Exception {
    File dir = write(title(section("500", "<content><p>Text.</p></content>")));
    drain(UsCodeXmlDataProvider.streamUnits(new File(dir, "usc.xml"), dir, "119-111"));
    assertFalse(dir.exists());
  }
}
