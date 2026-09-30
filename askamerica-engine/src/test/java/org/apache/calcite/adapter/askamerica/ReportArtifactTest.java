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
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportArtifactTest {

  private static DashboardLayout.Panel bar(List<String> cats, Double... values) {
    DashboardLayout.Panel p = new DashboardLayout.Panel();
    p.chartType = "bar";
    p.title = "T";
    p.categories = cats;
    p.series = Collections.singletonList(
        new ChartRenderer.SeriesSpec("S", Arrays.asList(values)));
    return p;
  }

  private static JsonNode firstPanel(DashboardLayout.Panel p) {
    ObjectNode out = ReportArtifact.build("Title", null, null, null,
        Collections.<ReportPage.Section>emptyList(), Collections.<ReportPage.Source>emptyList(),
        Collections.singletonList(p), 2);
    return out.path("panels").get(0);
  }

  @Test void missingValueIsSuppressedNotZero() {
    JsonNode panel = firstPanel(bar(Arrays.asList("DE", "WY"), null, 3.0));
    assertTrue(panel.path("series").get(0).path("values").get(0).isNull());
    JsonNode cell = panel.path("hints").path("suppressed_cells").get(0);
    assertEquals("DE", cell.path("category").asText());
    assertEquals("S", cell.path("series").asText());
    assertEquals(1, panel.path("hints").path("suppressed_cells").size());
  }

  @Test void manyCategoriesPreferHorizontalBars() {
    List<String> cats = new ArrayList<>();
    Double[] vals = new Double[9];
    for (int i = 0; i < 9; i++) {
      cats.add("c" + i);
      vals[i] = 1.0;
    }
    assertEquals("horizontal",
        firstPanel(bar(cats, vals)).path("hints").path("orientation_hint").asText());
  }

  @Test void fewShortCategoriesStayVertical() {
    assertEquals("vertical", firstPanel(bar(Arrays.asList("A", "B"), 1.0, 2.0))
        .path("hints").path("orientation_hint").asText());
  }

  @Test void longLabelPrefersHorizontalBars() {
    assertEquals("horizontal", firstPanel(bar(Arrays.asList("A very long label", "B"), 1.0, 2.0))
        .path("hints").path("orientation_hint").asText());
  }

  @Test void sectionsAndTitlePassThrough() {
    ObjectNode out = ReportArtifact.build("Head", "Sub", "Foot", null,
        Collections.singletonList(new ReportPage.Section("Summary", "<p>x</p>")),
        Collections.<ReportPage.Source>emptyList(),
        Collections.<DashboardLayout.Panel>emptyList(), 2);
    assertEquals("Head", out.path("title").asText());
    assertEquals("Sub", out.path("subtitle").asText());
    assertEquals("<p>x</p>", out.path("sections").get(0).path("html").asText());
    assertTrue(out.path("byline").isMissingNode());
  }

  @Test void sourcesPassThroughAsCitations() {
    ObjectNode out = ReportArtifact.build("Head", null, null, null,
        Collections.<ReportPage.Section>emptyList(),
        Collections.singletonList(new ReportPage.Source("BLS CPI", "https://bls.gov/cpi",
            null, "SELECT 1", null, null)),
        Collections.<DashboardLayout.Panel>emptyList(), 2);
    JsonNode src = out.path("sources").get(0);
    assertEquals("BLS CPI", src.path("label").asText());
    assertEquals("https://bls.gov/cpi", src.path("url").asText());
    assertEquals("SELECT 1", src.path("sql").asText());
    assertTrue(src.path("note").isMissingNode());
  }
}
