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
package org.apache.calcite.adapter.govdata.fiscal;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Link selection and row-label resolution for the ACF TANF tables. */
@Tag("unit")
class AcfTanfLinksTest {

  private static final String DOCS = "https://acf.gov/sites/default/files/documents/ofa/";

  @Test void picksTheTanfOnlyCaseloadWorkbookInEveryNamingEra() {
    String[][] pages = {
        {"fy2024-ssp-caseload.xlsx", "fy2024-tanf-caseload.xlsx", "fy2024-tanssp-caseload.xlsx"},
        {"fy2020__ssp_caseload.xlsx", "fy2020_tanf_caseload_0.xlsx", "fy2020_tanfssp_caseload_0.xlsx"},
        {"2018_15months_ssp_web.xlsx", "2018_15months_tan_web_03252019.xlsx",
            "2018_15months_tanssp_web.xlsx"},
    };
    String[] wanted = {"fy2024-tanf-caseload.xlsx", "fy2020_tanf_caseload_0.xlsx",
        "2018_15months_tan_web_03252019.xlsx"};
    for (int i = 0; i < pages.length; i++) {
      String html = "";
      for (String f : pages[i]) {
        html += "<a href=\"/sites/default/files/documents/ofa/" + f + "\">x</a>";
      }
      assertEquals(DOCS + wanted[i],
          TanfCaseloadTransformer.tanfWorkbook(AcfWorkbooks.workbookLinks(html), "page"));
    }
  }

  @Test void caseloadPageWithoutATanfWorkbookThrows() {
    List<String> links = Arrays.asList(DOCS + "fy2024-ssp-caseload.xlsx");
    assertThrows(IllegalStateException.class, () -> TanfCaseloadTransformer.tanfWorkbook(links, "p"));
  }

  @Test void picksTheWorkParticipationWorkbook() {
    List<String> links = Arrays.asList(DOCS + "other.xlsx", DOCS + "wpr2018_web.xlsx");
    assertEquals(DOCS + "wpr2018_web.xlsx", TanfWorkParticipationTransformer.workbook(links, "p"));
  }

  @Test void resolvesNationalStateAndTerritoryLabels() {
    assertEquals("US", AcfWorkbooks.stateCode("U.S. Totals"));
    assertEquals("US", AcfWorkbooks.stateCode("United States"));
    assertEquals("DC", AcfWorkbooks.stateCode("District of Col."));
    assertEquals("GU", AcfWorkbooks.stateCode("Guam"));
    assertEquals("VI", AcfWorkbooks.stateCode("Virgin Islands "));
    assertEquals("PR", AcfWorkbooks.stateCode("Puerto Rico"));
    assertThrows(IllegalStateException.class, () -> AcfWorkbooks.stateCode("Notes:"));
  }
}
