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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class FhwaApportionmentsTransformerTest {

  private static String tr(String... cells) {
    StringBuilder b = new StringBuilder("<tr>");
    for (String c : cells) {
      b.append("<td>").append(c).append("</td>");
    }
    return b.append("</tr>\n").toString();
  }

  private static String mainTable(String paSum) {
    return "<table>" + tr("State", "National Highway Perfo<br>rmance", "Surface Transportation",
        "Highway Safety Improve", "Railway- Highway Crossings", "Mitigation &amp; Air Quality",
        "Metropolitan Planning", "National Highway Freight", "Carbon Reduction", "PROTECT Formula",
        "Apportioned Total")
        + tr("Pennsylvania", "1,245,038,090", "605,694,206", "134,015,891", "7,168,370",
            "120,831,982", "18,158,683", "60,384,438", "54,007,733", "61,410,663", paSum)
        + tr("State", "National Highway Perfo")
        + tr("Apportioned Total", "1", "2", "3", "4", "5", "6", "7", "8", "9", "45")
        + "</table>";
  }

  @Test void parsesMainTableIntoOneRowPerProgram() {
    List<Map<String, Object>> rows =
        FhwaApportionmentsTransformer.parseMain(2025, 892, mainTable("2,306,710,056"));
    assertEquals(9, rows.size());
    assertEquals("NHPP", rows.get(0).get("program"));
    assertEquals("PA", rows.get(0).get("state_abbr"));
    assertEquals(1245038090d, rows.get(0).get("amount"));
    assertEquals("CMAQ", rows.get(4).get("program"));
    assertEquals("N4510.892", rows.get(8).get("notice_number"));
  }

  @Test void failsWhenProgramsDoNotAddToNoticeTotal() {
    assertThrows(IllegalStateException.class, () ->
        FhwaApportionmentsTransformer.parseMain(2025, 892, mainTable("2,306,710,057")));
  }

  @Test void failsOnUnknownProgramColumn() {
    String html = "<table>" + tr("State", "National Highway Performance", "Mystery Program",
        "Apportioned Total") + tr("Pennsylvania", "1", "2", "3") + "</table>";
    assertThrows(IllegalStateException.class, () ->
        FhwaApportionmentsTransformer.parseMain(2025, 892, html));
  }

  @Test void parsesBridgeFormulaTable() {
    StringBuilder b = new StringBuilder("<table>" + tr("State", "Bridge Formula Program")
        + tr("Bridge (Main)", "Off-System Bridges", "Total")
        + tr("Pennsylvania", "300,371,235", "53,006,688", "353,377,923")
        + tr("Puerto Rico", "10", "5", "15"));
    for (int i = 0; i < 20; i++) {
      b.append(tr("Filler", "1", "1", "2"));
    }
    b.append(tr("Total", "4,511,374,999", "796,125,001", "5,307,500,000")).append("</table>");
    List<Map<String, Object>> rows = FhwaApportionmentsTransformer.parseBfp(2022, 867, b.toString());
    assertEquals(4, rows.size());
    assertEquals("BFP_MAIN", rows.get(0).get("program"));
    assertEquals(53006688d, rows.get(1).get("amount"));
    assertEquals("PR", rows.get(2).get("state_abbr"));
  }

  @Test void discoversLatestNoticePerFiscalYearAndKind() {
    String index = "<table>"
        + tr("N4510.858", "12/14/21", "Apportionment of Federal-Aid Highway Program Funds For Fiscal Year (FY) 2022 ")
        + tr("N4510.865", "3/1/22", "Apportionment of Fiscal Year (FY) 2022 Highway Infrastructure Program Funds for the Bridge Formula Program Pursuant to the Infrastructure Investment and Jobs Act")
        + tr("N4510.867", "4/8/22", "Revised Apportionment of Fiscal Year (FY) 2022 Highway Infrastructure Program Funds for the Bridge Formula Program Pursuant to the Infrastructure Investment and Jobs Act")
        + tr("N4510.880", "10/2/23", "Apportionment of Federal-Aid Highway Program Funds for Fiscal Year 2024")
        + tr("N4510.853", "3/4/21", "Revised Apportionment of Federal-Aid Highway Program Funds For Fiscal Year (FY) 2021")
        + tr("N4510.907", "10/1/25", "Apportionment of Fiscal Year 2026 Highway Infrastructure Program Funds for the Bridge Formula Program Pursuant to the Infrastructure Investment and Jobs Act")
        + tr("N4510.881", "10/2/23", "Sequestration of Highway Funds for the Fiscal Year (FY) 2024")
        + tr("N4510.916", "10/1/26", "Apportionment of Federal-Aid Highway Program Funds for the Period Beginning on October 1, 2026, and Ending on December 11, 2026")
        + "</table>";
    Map<Integer, Integer> main = new TreeMap<Integer, Integer>();
    Map<Integer, Integer> bfp = new TreeMap<Integer, Integer>();
    FhwaApportionmentsTransformer.discoverNotices(index, main, bfp);
    assertEquals(2, main.size());
    assertEquals(907, bfp.get(2026).intValue());
    assertEquals(858, main.get(2022).intValue());
    assertEquals(880, main.get(2024).intValue());
    assertEquals(2, bfp.size());
    assertEquals(867, bfp.get(2022).intValue());
  }
}
