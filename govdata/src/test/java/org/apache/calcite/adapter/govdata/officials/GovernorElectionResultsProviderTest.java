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
package org.apache.calcite.adapter.govdata.officials;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the strict parsing of the governor rows of the Cha and Kuriwaki candidates file. */
@Tag("unit")
class GovernorElectionResultsProviderTest {

  private static final String HEADER = "year\tstate\toffice\tdist\ttype\tnextup\tparty\t"
      + "party_formal\tname_snyder\tinc\tcandidatevotes\ttotalvotes\twon\trunoff\n";

  private static List<Map<String, Object>> parse(String body) throws IOException {
    return GovernorElectionResultsProvider.parse(
        new BufferedReader(new StringReader(HEADER + body)), "rel");
  }

  @Test void keepsOnlyGovernorRowsAndMapsFields() throws Exception {
    List<Map<String, Object>> rows = parse(
        "2006.0\t\"AK\"\t\"G\"\t\t\"G\"\t2010.0\t\"R\"\t\"R\"\t\"PALIN, SARAH H.\"\t0.0\t"
        + "114697.0\t236938.0\t1.0\t\n"
        + "2006.0\t\"AK\"\t\"S\"\t\t\"G\"\t2008.0\t\"R\"\t\"R\"\t\"STEVENS, TED\"\t1.0\t"
        + "1.0\t2.0\t1.0\t\n"
        + "2006.0\t\"AK\"\t\"G\"\t\t\"G\"\t2010.0\t\"D\"\t\"DFL\"\t\"KNOWLES, TONY\"\t0.0\t"
        + "97238.0\t236938.0\t0.0\t\n"
        + "2018.0\t\"GA\"\t\"G\"\t\t\"G\"\t2022.0\t\"Other\"\t\t\"X, Y\"\t\t5.0\t10.0\t0.0\t0.0\n");
    assertEquals(3, rows.size());
    Map<String, Object> palin = rows.get(0);
    assertEquals(2006, palin.get("year"));
    assertEquals("02", palin.get("state_fips"));
    assertEquals("Alaska", palin.get("state_name"));
    assertEquals("PALIN, SARAH H.", palin.get("candidate"));
    assertEquals("R", palin.get("party_major"));
    assertEquals(Boolean.TRUE, palin.get("won"));
    assertEquals(114697L, palin.get("candidate_votes"));
    assertEquals(236938L, palin.get("total_votes"));
    assertEquals(2010, palin.get("nextup_year"));
    assertEquals(Boolean.FALSE, palin.get("special"));
    assertNull(palin.get("runoff_flag"));
    assertEquals(1, palin.get("line_no"));
    assertEquals("D", rows.get(1).get("party_major"));
    assertEquals("DFL", rows.get(1).get("party_formal"));
    assertEquals(2, rows.get(1).get("line_no"));
    Map<String, Object> x = rows.get(2);
    assertEquals("OTHER", x.get("party_major"));
    assertNull(x.get("incumbent_code"));
    assertNull(x.get("party_formal"));
    assertEquals(Boolean.FALSE, x.get("runoff_flag"));
  }

  @Test void readsScientificNotationAndRejectsFractions() throws Exception {
    List<Map<String, Object>> rows = parse(
        "2020.0\t\"CA\"\t\"G\"\t\t\"G\"\t2024.0\t\"R\"\t\"R\"\t\"A, B\"\t0.0\t"
        + "1.0094822E7\t1.0094822E7\t1.0\t\n");
    assertEquals(10094822L, rows.get(0).get("candidate_votes"));
    assertThrows(IOException.class, () -> parse(
        "2020.0\t\"CA\"\t\"G\"\t\t\"G\"\t2024.0\t\"R\"\t\"R\"\t\"A, B\"\t0.0\t"
        + "1.5\t2.0\t1.0\t\n"));
  }

  @Test void rejectsUnknownPartyOfficeTypeStateAndShortRecord() {
    assertThrows(IOException.class, () -> parse(
        "2006.0\t\"AK\"\t\"G\"\t\t\"G\"\t2010.0\t\"Whig\"\t\"Whig\"\t\"A, B\"\t0.0\t1.0\t2.0\t1.0\t\n"));
    assertThrows(IOException.class, () -> parse(
        "2006.0\t\"AK\"\t\"Q\"\t\t\"G\"\t2010.0\t\"R\"\t\"R\"\t\"A, B\"\t0.0\t1.0\t2.0\t1.0\t\n"));
    assertThrows(IOException.class, () -> parse(
        "2006.0\t\"AK\"\t\"G\"\t\t\"X\"\t2010.0\t\"R\"\t\"R\"\t\"A, B\"\t0.0\t1.0\t2.0\t1.0\t\n"));
    assertThrows(IOException.class, () -> parse(
        "2006.0\t\"PR\"\t\"G\"\t\t\"G\"\t2010.0\t\"R\"\t\"R\"\t\"A, B\"\t0.0\t1.0\t2.0\t1.0\t\n"));
    assertThrows(IOException.class, () -> parse("2006.0\t\"AK\"\t\"G\"\n"));
    assertThrows(IOException.class, () -> GovernorElectionResultsProvider.parse(
        new BufferedReader(new StringReader("year\tstate\n")), "rel"));
  }
}
