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
package org.apache.calcite.adapter.govdata.health;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class HhsOcrBreachProviderTest {

  private static final String HEADER = "\"javax.faces.component.UIPanel@24606030\",\"State\","
      + "\"Covered Entity Type\",\"Individuals Affected\",\"Breach Submission Date\","
      + "\"Type of Breach\",\"Location of Breached Information\","
      + "\"javax.faces.component.UIPanel@3ce3b3c0\",\"Web Description\"\n";

  @Test void parsesRowsIncludingMultilineQuotedDescription() throws Exception {
    String csv = HEADER
        + "\"Central Ohio Primary Care Physicians, Inc. (COPC)\",\"OH\",\"Healthcare Provider\","
        + "\"1351650\",\"10/16/2026\",\"Hacking/IT Incident\",\"Network Server\",\"No\",\"\"\n"
        + "\"Brooke Army Medical Center\",\"TX\",\"Healthcare Provider\",\"1000\",\"10/21/2009\","
        + "\"Theft\",\"Paper/Films\",\"Yes\",\"A binder was stolen.\nIt held \"\"PHI\"\".\"\n";
    List<Map<String, Object>> rows = new ArrayList<>();
    int n = HhsOcrBreachProvider.parseCsv(csv, "Archive", rows);
    assertEquals(2, n);
    Map<String, Object> a = rows.get(0);
    assertEquals("Central Ohio Primary Care Physicians, Inc. (COPC)", a.get("entity_name"));
    assertEquals(Long.valueOf(1351650L), a.get("individuals_affected"));
    assertEquals("2026-10-16", a.get("breach_submission_date"));
    assertEquals(Integer.valueOf(2026), a.get("breach_year"));
    assertEquals(Boolean.FALSE, a.get("business_associate_present"));
    assertEquals(null, a.get("web_description"));
    assertEquals("Archive", a.get("report_status"));
    Map<String, Object> b = rows.get(1);
    assertEquals(Boolean.TRUE, b.get("business_associate_present"));
    assertEquals("A binder was stolen.\nIt held \"PHI\".", b.get("web_description"));
  }

  @Test void rejectsShiftedHeader() {
    String csv = HEADER.replace("\"State\"", "\"Province\"");
    assertThrows(IOException.class,
        () -> HhsOcrBreachProvider.parseCsv(csv, "Archive", new ArrayList<>()));
  }

  @Test void rejectsNonYesNoBusinessAssociateFlag() {
    String csv = HEADER + "\"X\",\"OH\",\"Health Plan\",\"600\",\"01/02/2020\",\"Loss\",\"Laptop\","
        + "\"Maybe\",\"\"\n";
    assertThrows(IOException.class,
        () -> HhsOcrBreachProvider.parseCsv(csv, "Archive", new ArrayList<>()));
  }

  @Test void locatesGeneratedComponentIdsByVisibleText() throws Exception {
    String html = "<a href=\"#\" onclick=\"mojarra.jsfcljs(document.getElementById('ocrForm'),"
        + "{'ocrForm:j_idt39':'ocrForm:j_idt39'},'');return false\">View HIPAA Breach Reports</a>"
        + "<a href=\"#\" onclick=\"mojarra.jsfcljs(document.getElementById('ocrForm'),"
        + "{'ocrForm:j_idt385':'ocrForm:j_idt385'},'');return false\"><img id=\"ocrForm:j_idt386\" "
        + "src=\"/x/csv.png\" alt=\"CSV\" width=\"24\" title=\"Export as CSV\" /></a>";
    assertEquals("ocrForm:j_idt39",
        HhsOcrBreachProvider.componentIdForLink(html, "View HIPAA Breach Reports"));
    assertEquals("ocrForm:j_idt385",
        HhsOcrBreachProvider.componentIdForImgTitle(html, "Export as CSV"));
    assertThrows(IOException.class,
        () -> HhsOcrBreachProvider.componentIdForLink(html, "Missing"));
  }
}
