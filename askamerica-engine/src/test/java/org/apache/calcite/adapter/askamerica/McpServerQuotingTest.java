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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Reserved words must be quoted when they name a schema, table or column, and left alone
 * when they are keywords doing their grammatical job.
 */
@Tag("unit")
class McpServerQuotingTest {

  private static String rewrite(String sql) {
    return McpServer.quoteReservedIdentifiers(sql);
  }

  @Test @DisplayName("reserved schema qualifier is quoted")
  void quotesReservedSchemaQualifier() {
    assertEquals("SELECT * FROM \"ref\".naics",
        rewrite("SELECT * FROM ref.naics"));
  }

  @Test @DisplayName("reserved column after a dot is quoted")
  void quotesReservedColumnAfterDot() {
    assertEquals("SELECT t.\"year\" FROM fiscal.usaspending_by_state t",
        rewrite("SELECT t.year FROM fiscal.usaspending_by_state t"));
  }

  @Test @DisplayName("EXTRACT(YEAR FROM ...) is left alone")
  void leavesExtractKeywordAlone() {
    String sql = "SELECT EXTRACT(YEAR FROM filed_date) FROM sec.filing_metadata";
    assertEquals(sql, rewrite(sql));
  }

  @Test @DisplayName("ORDER BY and COUNT(*) are left alone")
  void leavesKeywordsAlone() {
    String sql = "SELECT COUNT(*) AS n FROM geo.counties ORDER BY county_name";
    assertEquals(sql, rewrite(sql));
  }

  @Test @DisplayName("a bare reserved word is not rewritten")
  void leavesBareReservedWordAlone() {
    // Ambiguous without catalog knowledge: could be a column or the start of YEAR(...).
    String sql = "SELECT year FROM fiscal.usaspending_by_state";
    assertEquals(sql, rewrite(sql));
  }

  @Test @DisplayName("string literals are never rewritten")
  void skipsStringLiterals() {
    String sql = "SELECT * FROM geo.counties WHERE note = 'see ref.table'";
    assertEquals(sql, rewrite(sql));
  }

  @Test @DisplayName("doubled quotes inside a literal do not end it")
  void handlesEscapedQuoteInLiteral() {
    String sql = "SELECT * FROM geo.counties WHERE n = 'O''Brien ref.x'";
    assertEquals(sql, rewrite(sql));
  }

  @Test @DisplayName("an already-quoted identifier is not double-quoted")
  void doesNotDoubleQuote() {
    String sql = "SELECT * FROM \"ref\".naics";
    assertEquals(sql, rewrite(sql));
  }

  @Test @DisplayName("line comments are not rewritten")
  void skipsLineComments() {
    String sql = "SELECT 1 -- from ref.naics\nFROM geo.counties";
    assertEquals(sql, rewrite(sql));
  }

  @Test @DisplayName("decimal literals are not treated as qualified names")
  void ignoresDecimalLiterals() {
    String sql = "SELECT * FROM fiscal.usaspending_by_state WHERE per_capita > 1.5";
    assertEquals(sql, rewrite(sql));
  }

  @Test @DisplayName("qualified star is untouched")
  void handlesQualifiedStar() {
    String sql = "SELECT t.* FROM \"ref\".naics t";
    assertEquals(sql, rewrite(sql));
  }

  @Test @DisplayName("both sides of a dot are handled together")
  void quotesBothSides() {
    assertEquals("SELECT \"ref\".\"type\" FROM \"ref\".naics",
        rewrite("SELECT ref.type FROM ref.naics"));
  }
}
