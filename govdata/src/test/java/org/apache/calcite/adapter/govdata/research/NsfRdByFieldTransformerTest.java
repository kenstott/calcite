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
package org.apache.calcite.adapter.govdata.research;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers govdata-ops#257: NCSES's pre-2016 long-run historical tables (e.g. table 134,
 * FYs 2004-14) encode the field-of-R&D hierarchy via leading non-breaking-space characters
 * in the field-name string rather than a real cell-style indent, unlike the current-edition
 * table this transformer was originally written against. Hits the live Wayback Machine
 * mirror directly (the original nsf.gov URLs are dead), same live-fetch pattern as the
 * sibling {@code NsfHerdTransformerTest}.
 */
class NsfRdByFieldTransformerTest {

  @Test @Tag("integration") void parsesHistoricalNbspEncodedFieldLevels() throws Exception {
    String url = "https://web.archive.org/web/20150629045504if_/"
        + "http://www.nsf.gov/statistics/nsf14316/tables/tab134.xlsx";
    RequestContext context = RequestContext.builder().url(url).build();

    String json = new NsfRdByFieldTransformer().transform(null, context);
    JsonNode rows = new ObjectMapper().readTree(json);
    assertTrue(rows.isArray() && rows.size() > 0, "expected parsed rows from table 134");

    boolean sawGrandTotal = false;
    boolean sawLevel1 = false;
    boolean sawLevel2 = false;
    for (JsonNode row : rows) {
      String field = row.get("rd_field").asText();
      int level = row.get("field_level").asInt();
      int year = row.get("year").asInt();
      assertTrue(year >= 2004 && year <= 2014, "year out of expected FY2004-14 range: " + row);
      if ("All fields".equals(field)) {
        assertEquals(0, level, "'All fields' must be level 0: " + row);
        sawGrandTotal = true;
      } else if ("Computer sciences and mathematics".equals(field)) {
        assertEquals(1, level, "broad field must be level 1: " + row);
        sawLevel1 = true;
      } else if ("Computer sciences".equals(field)) {
        assertEquals(2, level, "sub-field must be level 2: " + row);
        sawLevel2 = true;
      }
    }
    assertTrue(sawGrandTotal, "expected an 'All fields' (level 0) row");
    assertTrue(sawLevel1, "expected a level-1 broad-field row");
    assertTrue(sawLevel2, "expected a level-2 sub-field row");
  }

  /**
   * Regression for a header row hardcoded at a fixed index: table 133 (FYs 1993-2003,
   * same publication as table 134 above) has its "Field" header row at index 2, not 3
   * (a 2-row title block instead of 3) — confirmed live 2026-09-13 this silently parsed
   * zero records under a fixed-index assumption. {@code findHeaderRow} locates the
   * header by content instead.
   */
  @Test @Tag("integration") void parsesHistoricalTableWithDifferentHeaderRowOffset()
      throws Exception {
    String url = "https://web.archive.org/web/20150629045708if_/"
        + "http://www.nsf.gov/statistics/nsf14316/tables/tab133.xlsx";
    RequestContext context = RequestContext.builder().url(url).build();

    String json = new NsfRdByFieldTransformer().transform(null, context);
    JsonNode rows = new ObjectMapper().readTree(json);
    assertTrue(rows.isArray() && rows.size() > 0, "expected parsed rows from table 133");

    boolean sawGrandTotal = false;
    for (JsonNode row : rows) {
      int year = row.get("year").asInt();
      assertTrue(year >= 1993 && year <= 2003, "year out of expected FY1993-2003 range: " + row);
      if ("All fields".equals(row.get("rd_field").asText())) {
        assertEquals(0, row.get("field_level").asInt());
        sawGrandTotal = true;
      }
    }
    assertTrue(sawGrandTotal, "expected an 'All fields' (level 0) row");
  }

  /**
   * Covers govdata-ops#325: the FY2015 gap is sourced from NCSES's Data Explorer-era
   * table 123, which spans FYs 2009-18 in one file — far more years than the single-year
   * gap it exists to close. Confirms {@code vintage=hist_2015} filters output down to
   * FY2015 only, and that this file's style-indent step (2/4 for level 1/2, double the
   * current edition's 1/2) is correctly normalized to field_level 0/1/2.
   */
  @Test @Tag("integration") void parsesDataExplorerEraTableFilteredToFy2015() throws Exception {
    String url = "https://web.archive.org/web/20190617181329if_/"
        + "https://ncsesdata.nsf.gov/fedfunds/2017/excel/ffs17-dt-tab123.xlsx";
    RequestContext context = RequestContext.builder().url(url)
        .dimensionValues(java.util.Collections.singletonMap("vintage", "hist_2015"))
        .build();

    String json = new NsfRdByFieldTransformer().transform(null, context);
    JsonNode rows = new ObjectMapper().readTree(json);
    assertTrue(rows.isArray() && rows.size() > 0, "expected parsed rows from table 123");

    boolean sawGrandTotal = false;
    boolean sawLevel1 = false;
    boolean sawLevel2 = false;
    for (JsonNode row : rows) {
      String field = row.get("rd_field").asText();
      int level = row.get("field_level").asInt();
      int year = row.get("year").asInt();
      assertEquals(2015, year, "hist_2015 must filter out every other year in the file: " + row);
      if ("All fields".equals(field)) {
        assertEquals(0, level, "'All fields' must be level 0: " + row);
        sawGrandTotal = true;
      } else if ("Computer sciences and mathematics".equals(field)) {
        assertEquals(1, level, "broad field must be level 1: " + row);
        sawLevel1 = true;
      } else if ("Computer sciences".equals(field)) {
        assertEquals(2, level, "sub-field must be level 2: " + row);
        sawLevel2 = true;
      }
    }
    assertTrue(sawGrandTotal, "expected an 'All fields' (level 0) row");
    assertTrue(sawLevel1, "expected a level-1 broad-field row");
    assertTrue(sawLevel2, "expected a level-2 sub-field row");
  }
}
