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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Parsing of the SEC enforcement listing pages (rows copied from the live pages, 2026-11-22). */
@Tag("unit")
class SecEnforcementTransformerTest {

  private static final String LR = "<table><tbody>"
      + "<tr class=\"pr-list-page-row\"><td><time datetime=\"2026-11-20T20:05:57Z\">Nov. 20, 2026</time></td>"
      + "<td><div class='release-view__respondents'><a href='/enforcement-litigation/litigation-releases/lr-26692'>Jose A. Gamez</a></div>"
      + "<div class=\"view-table_subfield view-table_subfield_release_number\"><span class=\"view-table_subfield_value\">LR-26692</span></div>"
      + "<div class=\"view-table_subfield view-table_subfield_see_also\"><span class=\"view-table_subfield_value\">"
      + "<a href=\"/files/litigation/complaints/2026/comp26692.pdf\">SEC Complaint</a></span></div></td></tr>"
      + "</tbody></table><ul><li><a href=\"?page=1\">2</a></li><li><a href=\"?page=120\">last</a></li></ul>";

  private static final String AP = "<table><tbody>"
      + "<tr class=\"pr-list-page-row\"><td><time datetime=\"2026-09-10T21:12:02Z\">Sept. 10, 2026</time></td>"
      + "<td><div class='release-view__respondents'><a href='https://www.sec.gov/files/litigation/admin/2026/34-106331.pdf'>Evergreen Sustainable Enterprises, Inc.</a></div>"
      + "<div class=\"view-table_subfield view-table_subfield_release_number\"><span class=\"view-table_subfield_value\">34-106331</span></div>"
      + "<div class=\"view-table_subfield view-table_subfield_file_number\"><span class=\"view-table_subfield_value\">3-22713</span></div></td></tr>"
      + "</tbody></table>";

  @Test void litigationRowAndPager() throws Exception {
    assertEquals(120, SecEnforcementTransformer.lastPage(LR));
    ArrayNode out = new ObjectMapper().createArrayNode();
    SecEnforcementTransformer.parse(LR, "litigation-releases", "civil", out);
    assertEquals(1, out.size());
    JsonNode r = out.get(0);
    assertEquals("LR-26692", r.get("release_number").asText());
    assertEquals("2026-11-20", r.get("filed_date").asText());
    assertEquals(2027, r.get("fiscal_year").asInt());
    assertEquals("https://www.sec.gov/enforcement-litigation/litigation-releases/lr-26692",
        r.get("release_url").asText());
    assertEquals("https://www.sec.gov/files/litigation/complaints/2026/comp26692.pdf",
        r.get("complaint_url").asText());
    assertTrue(r.get("file_number").isNull());
  }

  @Test void administrativeRowHasFileNumber() {
    ArrayNode out = new ObjectMapper().createArrayNode();
    SecEnforcementTransformer.parse(AP, "administrative-proceedings", "administrative", out);
    JsonNode r = out.get(0);
    assertEquals("3-22713", r.get("file_number").asText());
    assertEquals(2026, r.get("fiscal_year").asInt());
    assertTrue(r.get("complaint_url").isNull());
  }
}
