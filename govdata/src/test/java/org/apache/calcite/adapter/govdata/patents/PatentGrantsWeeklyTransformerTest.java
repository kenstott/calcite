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
package org.apache.calcite.adapter.govdata.patents;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the PTBLXML catalog selection and document parsing of patent_grants_weekly. */
@Tag("unit")
class PatentGrantsWeeklyTransformerTest {

  private static String document(String number, String kind, String type, String title) {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<!DOCTYPE us-patent-grant SYSTEM \"us-patent-grant-v47-2022-02-17.dtd\" [ ]>\n"
        + "<us-patent-grant lang=\"EN\">\n"
        + "<us-bibliographic-data-grant>\n"
        + "<publication-reference><document-id><country>US</country>\n"
        + "<doc-number>" + number + "</doc-number><kind>" + kind + "</kind>"
        + "<date>20261124</date></document-id></publication-reference>\n"
        + "<application-reference appl-type=\"" + type + "\"><document-id>\n"
        + "<country>US</country><doc-number>30018576</doc-number><date>20250818</date>\n"
        + "</document-id></application-reference>\n"
        + "<invention-title id=\"a\">" + title + "</invention-title>\n"
        + "<us-references-cited><us-citation><patcit><document-id>\n"
        + "<doc-number>1589850</doc-number><date>19260600</date>\n"
        + "</document-id></patcit></us-citation></us-references-cited>\n"
        + "<number-of-claims>17</number-of-claims>\n"
        + "</us-bibliographic-data-grant>\n"
        + "<abstract><p>Body &#x26; text</p></abstract>\n"
        + "</us-patent-grant>\n";
  }

  private static Iterator<Map<String, Object>> rows(String xml, String issueDate) {
    return PatentGrantsWeeklyTransformer.rows(new BufferedReader(new StringReader(xml)),
        issueDate, "ipgb20261124_wk47");
  }

  @Test void readsEveryDocumentOfAConcatenatedFile() {
    Iterator<Map<String, Object>> rows = rows(
        document("12805418", "B2", "utility", "Widget")
            + document("PP037850", "P2", "plant", "Rose named &#x2018;A&#x2019;")
            + document("RE051130", "E1", "reissue", "CO<sub>2</sub> trap"), "2026-11-24");

    Map<String, Object> utility = rows.next();
    assertEquals("12805418", utility.get("patent_id"));
    assertEquals(2026, utility.get("grant_year"));
    assertEquals("2026-11-24", utility.get("patent_date"));
    assertEquals("utility", utility.get("patent_type"));
    assertEquals("Widget", utility.get("patent_title"));
    assertEquals(17, utility.get("num_claims"));
    assertEquals("B2", utility.get("wipo_kind"));
    assertEquals("30018576", utility.get("application_number"));
    assertEquals("2025-08-18", utility.get("application_date"));
    assertEquals("ipgb20261124_wk47.zip", utility.get("source_file"));

    Map<String, Object> plant = rows.next();
    assertEquals("PP37850", plant.get("patent_id"));
    assertEquals("Rose named ‘A’", plant.get("patent_title"));

    Map<String, Object> reissue = rows.next();
    assertEquals("RE51130", reissue.get("patent_id"));
    assertEquals("CO2 trap", reissue.get("patent_title"));
    assertFalse(rows.hasNext());
  }

  @Test void rejectsADocumentFromAnotherIssue() {
    assertThrows(UncheckedIOException.class,
        () -> rows(document("12805418", "B2", "utility", "Widget"), "2026-11-17"));
  }

  @Test void rejectsAnEmptyFile() {
    assertThrows(UncheckedIOException.class, () -> rows("", "2026-11-24"));
  }

  @Test void selectsTheHighestRevisionOfEachIssueInTheYear() throws IOException {
    String catalog = "{\"files\":[{\"fileName\":\"ipgb20260106_wk01_r2.zip\"},"
        + "{\"fileName\":\"ipgb20260106_wk01.zip\"},"
        + "{\"fileName\":\"ipgb20260106_wk01_r1.zip\"},"
        + "{\"fileName\":\"ipgb20260113_wk02.zip\"},"
        + "{\"fileName\":\"ipgb20251230_wk52.zip\"},"
        + "{\"fileName\":\"2014_xml.zip\"},"
        + "{\"fileName\":\"us-patent-grant-v47-2022-02-17.dtd\"}]}";

    SortedMap<String, String> issues = PatentGrantsWeeklyTransformer.latestRevisions(
        new ObjectMapper().readTree(catalog), "2026");

    assertEquals(2, issues.size());
    assertEquals("ipgb20260106_wk01_r2", issues.get("2026-01-06"));
    assertEquals("ipgb20260113_wk02", issues.get("2026-01-13"));
  }
}
