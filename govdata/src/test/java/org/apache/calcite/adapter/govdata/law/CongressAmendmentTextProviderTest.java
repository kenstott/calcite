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

import org.apache.calcite.adapter.govdata.GovDataException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link CongressAmendmentTextProvider#rowsOf} against the response the Congress.gov API
 * returned for S.Amdt. 7012 of the 119th Congress on 2026-11-20.
 */
@Tag("unit")
class CongressAmendmentTextProviderTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String S7012 = "{\"pagination\":{\"count\":1},"
      + "\"request\":{\"amendmentNumber\":\"7012\",\"amendmentType\":\"samdt\"},"
      + "\"textVersions\":[{\"date\":\"2026-11-16T05:00:00Z\",\"formats\":["
      + "{\"type\":\"HTML\",\"url\":\"https://www.congress.gov/119/crec/2026/11/16/172/173/"
      + "modified/CREC-2026-11-16-pt1-PgS5689.htm\"},"
      + "{\"type\":\"PDF\",\"url\":\"https://www.congress.gov/119/crec/2026/11/16/172/173/"
      + "CREC-2026-11-16-pt1-PgS5689.pdf\"}],\"type\":\"Submitted\"}]}";

  @Test void oneRowPerVersionAndFormat() throws Exception {
    List<Map<String, Object>> rows =
        CongressAmendmentTextProvider.rowsOf("119", "samdt", "7012", MAPPER.readTree(S7012));
    assertEquals(2, rows.size());
    Map<String, Object> html = rows.get(0);
    assertEquals(119, html.get("congress"));
    assertEquals("SAMDT", html.get("amendment_type"));
    assertEquals(7012, html.get("amendment_number"));
    assertEquals("Submitted", html.get("version_type"));
    assertEquals("2026-11-16T05:00:00Z", html.get("version_date"));
    assertEquals("HTML", html.get("format_type"));
    assertTrue(((String) html.get("url")).endsWith("CREC-2026-11-16-pt1-PgS5689.htm"));
    assertEquals("PDF", rows.get(1).get("format_type"));
  }

  @Test void noTextVersionsYieldsNoRows() throws Exception {
    JsonNode none = MAPPER.readTree("{\"textVersions\":[]}");
    assertTrue(CongressAmendmentTextProvider.rowsOf("119", "hamdt", "1", none).isEmpty());
  }

  @Test void responseWithoutTextVersionsFails() throws Exception {
    JsonNode bad = MAPPER.readTree("{\"error\":\"x\"}");
    assertThrows(GovDataException.class,
        () -> CongressAmendmentTextProvider.rowsOf("119", "hamdt", "1", bad));
  }

  @Test void versionWithoutFormatsFails() throws Exception {
    JsonNode bad = MAPPER.readTree("{\"textVersions\":[{\"type\":\"Offered\"}]}");
    assertThrows(GovDataException.class,
        () -> CongressAmendmentTextProvider.rowsOf("119", "hamdt", "1", bad));
  }
}
