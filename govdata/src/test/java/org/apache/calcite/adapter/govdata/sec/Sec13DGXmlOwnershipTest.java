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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

import java.io.InputStream;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Structured Schedule 13D/G {@code primary_doc.xml} extraction. The fixtures are the real
 * documents of accessions 0000929638-26-003961 (SCHEDULE 13D/A) and 0001172661-26-004681
 * (SCHEDULE 13G, four reporting persons), with the narrative items removed.
 */
@Tag("unit")
public class Sec13DGXmlOwnershipTest {

  private static Document load(String name) throws Exception {
    DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
    f.setNamespaceAware(true);
    try (InputStream is = Sec13DGXmlOwnershipTest.class.getResourceAsStream("/sec13dg/" + name)) {
      return f.newDocumentBuilder().parse(is);
    }
  }

  @Test void schedule13dFieldsComeFromTheStructuredDocument() throws Exception {
    Map<String, Object> row = XbrlToParquetConverter.extract13DGOwnershipFromXml(
        load("sc13d-primary_doc.xml"), "0001690996", "SCHEDULE 13D/A", "2026-09-02",
        "0000929638-26-003961", 2026);
    assertEquals("City National Rochdale Select Strategies Fund", row.get("subject_company"));
    assertEquals("0001690996", row.get("subject_cik"));
    assertEquals("RBC Rochdale, LLC", row.get("filer_name"));
    assertEquals("0001009289", row.get("filer_cik"));
    assertEquals("17801L108", row.get("cusip"));
    assertEquals("Shares of beneficial interest", row.get("title_of_class"));
    assertEquals("08/31/2026", row.get("date_of_event"));
    assertEquals(97.3, (Double) row.get("percent_of_class"), 0.0);
    assertEquals(13506519.0, (Double) row.get("shares_beneficially_owned"), 0.0);
    assertEquals(0.0, (Double) row.get("sole_voting_power"), 0.0);
    assertEquals(13506519.0, (Double) row.get("sole_dispositive_power"), 0.0);
    assertEquals("IA", row.get("type_of_reporting_person"));
    assertEquals("OO", row.get("source_of_funds"));
  }

  @Test void schedule13gTakesTheLeadReportingPerson() throws Exception {
    Map<String, Object> row = XbrlToParquetConverter.extract13DGOwnershipFromXml(
        load("sc13g-primary_doc.xml"), "0002149916", "SCHEDULE 13G", "2026-10-20",
        "0001172661-26-004681", 2026);
    assertEquals("TCGX Acquisition Corp. II", row.get("subject_company"));
    assertEquals("0002149916", row.get("subject_cik"));
    assertEquals("Deep Track Capital, LP", row.get("filer_name"));
    assertEquals("0001856083", row.get("filer_cik"));
    assertEquals("G87082106", row.get("cusip"));
    assertEquals("Class A Ordinary Shares", row.get("title_of_class"));
    assertEquals("10/16/2026", row.get("date_of_event"));
    assertEquals(9.9, (Double) row.get("percent_of_class"), 0.0);
    assertEquals(990000.0, (Double) row.get("shares_beneficially_owned"), 0.0);
    assertEquals(990000.0, (Double) row.get("shared_voting_power"), 0.0);
    assertEquals(990000.0, (Double) row.get("shared_dispositive_power"), 0.0);
    assertEquals("IA", row.get("type_of_reporting_person"));
    assertNull(row.get("source_of_funds"));
  }
}
