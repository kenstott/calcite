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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Parsing of the CMS OEP county public use file. */
@Tag("unit")
class AcaOepCountyTransformerTest {

  private static final String[] COLS = {"State_Abrvtn", "County_FIPS_Cd", "Cnsmr", "New_Cnsmr",
      "Tot_Renrl", "Actv_Renrl", "Auto_Renrl", "Avg_Prm", "Avg_Prm_Aftr_APTC", "APTC_Cnsmr",
      "APTC_Cnsmr_Avg_APTC", "CSR_Cnsmr", "Age_0_17", "Age_18_25", "Age_26_34", "Age_35_44",
      "Age_45_54", "Age_55_64", "Age_GE65", "Male", "Female", "Ctstrphc", "Brnz", "Slvr", "Gld",
      "Pltnm", "FPL_LT100", "FPL_100_138", "FPL_100_150", "FPL_150_200", "FPL_200_250",
      "FPL_250_300", "FPL_300_400", "FPL_GT400", "FPL_OTHR", "Dntl_Cnsmr"};

  /** A row with every column set to {@code fill} except the first three. */
  private static String row(String state, String fips, String cnsmr, String fill) {
    StringBuilder sb = new StringBuilder(state + "," + fips + ",\"" + cnsmr + "\"");
    for (int i = 3; i < COLS.length; i++) {
      sb.append(',').append(fill);
    }
    return sb.toString();
  }

  private static List<Map<String, Object>> parse(String... rows) throws IOException {
    StringBuilder sb = new StringBuilder(String.join(",", COLS)).append('\n');
    for (String r : rows) {
      sb.append(r).append('\n');
    }
    return AcaOepCountyTransformer.parse(new BufferedReader(new StringReader(sb.toString())),
        2026, "t");
  }

  @Test void parsesCountsMoneyAndSuppression() throws IOException {
    List<Map<String, Object>> out = parse(
        row("AL", "01001", "3,613", "5"),
        row("AL", "01003", "*", "+"),
        row("Total", "Total", "15,771,397", "1"));
    assertEquals(2, out.size());
    Map<String, Object> r = out.get(0);
    assertEquals(2026, r.get("plan_year"));
    assertEquals("01001", r.get("county_fips"));
    assertEquals(3613, r.get("plan_selections"));
    assertEquals(5.0, r.get("avg_premium"));
    assertNull(out.get(1).get("plan_selections"));
    assertNull(out.get(1).get("avg_aptc"));
  }

  @Test void moneyWithDollarAndTrailingSpace() {
    assertEquals(738.0, AcaOepCountyTransformer.money("$738 ", "c"));
    assertEquals(1234.0, AcaOepCountyTransformer.money("$1,234", "c"));
  }

  @Test void quotedCommasAndEscapedQuotes() {
    assertEquals(Arrays.asList("a", "3,613", "b\"c", ""),
        AcaOepCountyTransformer.splitCsv("a,\"3,613\",\"b\"\"c\","));
  }

  @Test void nonNumericCellFails() {
    assertThrows(IllegalStateException.class, () -> parse(row("AL", "01001", "3,613", "n/a")));
  }

  @Test void missingHeaderFails() {
    assertThrows(IllegalStateException.class, () -> AcaOepCountyTransformer.parse(
        new BufferedReader(new StringReader("State_Abrvtn,County_FIPS_Cd,Cnsmr\nAL,01001,1\n")),
        2026, "t"));
  }

  @Test void badFipsFails() {
    assertThrows(IllegalStateException.class, () -> parse(row("AL", "1001", "3", "5")));
  }
}
