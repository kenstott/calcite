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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Rules of {@link LobbyingBillReferences}. */
@Tag("unit")
class LobbyingBillReferencesTest {
  private static final LocalDate POSTED_2023 = LocalDate.of(2023, 4, 20);

  private static List<LobbyingBillReferences.Ref> parse(String text) {
    return LobbyingBillReferences.extract(text, POSTED_2023);
  }

  @Test void congressOnDate() {
    assertEquals(118, LobbyingBillReferences.congressOn(LocalDate.of(2023, 4, 20)));
    assertEquals(118, LobbyingBillReferences.congressOn(LocalDate.of(2024, 12, 31)));
    assertEquals(119, LobbyingBillReferences.congressOn(LocalDate.of(2025, 1, 3)));
    assertEquals(118, LobbyingBillReferences.congressOn(LocalDate.of(2025, 1, 2)));
    assertEquals(115, LobbyingBillReferences.congressOn(LocalDate.of(2017, 6, 1)));
  }

  @Test void formsMapToBillTypes() {
    List<LobbyingBillReferences.Ref> r = parse("H.R. 1; HR2; S. 3; S4; H.J.Res. 5; S.J.Res. 6;"
        + " H.Con.Res. 7; S.Con.Res. 8; H.Res. 9; S.Res. 10; H.R. No. 11");
    String[] types = {"hr", "hr", "s", "s", "hjres", "sjres", "hconres", "sconres", "hres",
        "sres", "hr"};
    assertEquals(types.length, r.size());
    for (int i = 0; i < types.length; i++) {
      assertEquals(types[i], r.get(i).billType);
      assertEquals(i + 1, r.get(i).billNumber);
      assertEquals(Integer.valueOf(118), r.get(i).congress);
      assertEquals(LobbyingBillReferences.INFERRED, r.get(i).matchBasis);
    }
  }

  @Test void explicitTagAfterAndBefore() {
    assertEquals(Integer.valueOf(115), parse("H.R. 1 (115th)").get(0).congress);
    assertEquals(Integer.valueOf(116), parse("S. 2, 116th Congress").get(0).congress);
    assertEquals(Integer.valueOf(117), parse("H.R. 3 of the 117th Congress").get(0).congress);
    LobbyingBillReferences.Ref b = parse("117th Congress: H.R. 4").get(0);
    assertEquals(Integer.valueOf(117), b.congress);
    assertEquals(LobbyingBillReferences.EXPLICIT, b.matchBasis);
  }

  @Test void ordinalThatIsNotACongressIsNotATag() {
    LobbyingBillReferences.Ref r = parse("HR 6644 21st Century Road to Housing Act").get(0);
    assertEquals(Integer.valueOf(118), r.congress);
    assertEquals(LobbyingBillReferences.INFERRED, r.matchBasis);
  }

  @Test void markdownEmphasisBetweenReferenceAndTag() {
    LobbyingBillReferences.Ref r = parse("**HR 1118** (117th Congress), DISCLOSE Act").get(0);
    assertEquals(Integer.valueOf(117), r.congress);
    assertEquals(LobbyingBillReferences.EXPLICIT, r.matchBasis);
  }

  @Test void tagLaterThanPostingCongressIsUnresolved() {
    LobbyingBillReferences.Ref r = parse("H.R. 5 (119th)").get(0);
    assertNull(r.congress);
    assertEquals(LobbyingBillReferences.UNRESOLVED, r.matchBasis);
  }

  @Test void explicitWinsOverInferredAndDuplicatesCollapse() {
    List<LobbyingBillReferences.Ref> r = parse("H.R. 1 and H.R. 1 (118th) and H.R. 1");
    assertEquals(1, r.size());
    assertEquals(LobbyingBillReferences.EXPLICIT, r.get(0).matchBasis);
  }

  @Test void sameNumberDifferentCongressStaysTwoRows() {
    assertEquals(2, parse("H.R. 1 (117th); H.R. 1 (118th)").size());
  }

  @Test void nonReferencesAreIgnored() {
    assertTrue(parse("Section 5 of the farm bill; HR 5 years; Congress's. wording; NASS 12")
        .size() <= 1);
    assertTrue(parse(null).isEmpty());
    assertTrue(parse("H.R. 1, 2 and 3").size() == 1);
    assertTrue(parse("House Bill 1").isEmpty());
  }
}
