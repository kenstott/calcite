/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.duckdb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link DuckDBSqlRewriter#rewrite(String)} — the transparent pre-parse
 * rewrite of the reserved-keyword statistical aggregates (corr, regr_*) to their
 * non-reserved aliases. Verifies function calls are rewritten while string literals,
 * quoted identifiers, non-call occurrences, and substrings are left untouched.
 */
@Tag("unit")
public class DuckDBSqlRewriterTest {

  @Test void rewritesReservedAggregateCalls() {
    assertEquals("SELECT agg_corr(CAST(a AS DOUBLE), CAST(b AS DOUBLE)) FROM t",
        DuckDBSqlRewriter.rewrite("SELECT corr(a, b) FROM t"));
    assertEquals("SELECT agg_regr_slope(CAST(y AS DOUBLE), CAST(x AS DOUBLE)), "
        + "agg_regr_intercept(CAST(y AS DOUBLE), CAST(x AS DOUBLE)) FROM t",
        DuckDBSqlRewriter.rewrite("SELECT regr_slope(y, x), regr_intercept(y, x) FROM t"));
    assertEquals("agg_regr_r2 agg_regr_avgx agg_regr_avgy agg_regr_sxy",
        DuckDBSqlRewriter.rewrite("regr_r2( regr_avgx( regr_avgy( regr_sxy(")
            .replaceAll("\\(", ""));
  }

  @Test void isCaseInsensitive() {
    assertEquals("SELECT agg_corr(CAST(a AS DOUBLE), CAST(b AS DOUBLE)) FROM t",
        DuckDBSqlRewriter.rewrite("SELECT CORR(a, b) FROM t"));
    assertEquals("SELECT agg_corr(CAST(a AS DOUBLE), CAST(b AS DOUBLE)) FROM t",
        DuckDBSqlRewriter.rewrite("SELECT Corr(a, b) FROM t"));
  }

  @Test void allowsWhitespaceBeforeParen() {
    assertEquals("SELECT agg_corr (CAST(a AS DOUBLE), CAST(b AS DOUBLE)) FROM t",
        DuckDBSqlRewriter.rewrite("SELECT corr (a, b) FROM t"));
  }

  @Test void leavesStringLiteralsUntouched() {
    assertEquals("SELECT 'corr(x)' FROM t",
        DuckDBSqlRewriter.rewrite("SELECT 'corr(x)' FROM t"));
    assertEquals("SELECT agg_corr(CAST(a AS DOUBLE), CAST(b AS DOUBLE)), 'regr_slope(q)' FROM t",
        DuckDBSqlRewriter.rewrite("SELECT corr(a, b), 'regr_slope(q)' FROM t"));
  }

  @Test void leavesQuotedIdentifiersUntouched() {
    assertEquals("SELECT \"corr\" FROM t",
        DuckDBSqlRewriter.rewrite("SELECT \"corr\" FROM t"));
  }

  @Test void doesNotRewriteNonCallOrSubstringMatches() {
    // Not followed by '(' — a bare identifier, left alone.
    assertEquals("SELECT corr FROM t",
        DuckDBSqlRewriter.rewrite("SELECT corr FROM t"));
    // Longer identifier that merely starts with a reserved name — must not match.
    assertEquals("SELECT correlation(a) FROM t",
        DuckDBSqlRewriter.rewrite("SELECT correlation(a) FROM t"));
    // Reserved name as a suffix of another identifier — must not match.
    assertEquals("SELECT my_corr(a) FROM t",
        DuckDBSqlRewriter.rewrite("SELECT my_corr(a) FROM t"));
  }

  @Test void nullAndEmptyAreReturnedUnchanged() {
    assertEquals(null, DuckDBSqlRewriter.rewrite(null));
    assertEquals("", DuckDBSqlRewriter.rewrite(""));
    assertEquals("SELECT 1", DuckDBSqlRewriter.rewrite("SELECT 1"));
  }

  // ---- operand forcing: DECIMAL literals must reach the UDAF as DOUBLE ----------------

  @Test @DisplayName("corr operands are cast to DOUBLE so a DECIMAL literal binds")
  void castsCorrOperands() {
    String out = DuckDBSqlRewriter.rewrite(
        "SELECT corr(y, x) FROM (VALUES (1.0, 5.0)) AS t(x, y)");
    assertTrue(out.contains("agg_corr(CAST(y AS DOUBLE), CAST(x AS DOUBLE))"),
        "expected both operands cast, got: " + out);
  }

  @Test @DisplayName("a non-aliased stats aggregate is cast too")
  void castsMedianOperand() {
    String out = DuckDBSqlRewriter.rewrite("SELECT median(x) FROM t");
    assertTrue(out.contains("median(CAST(x AS DOUBLE))"), "got: " + out);
  }

  @Test @DisplayName("a nested call's own arguments are not split or double-wrapped")
  void nestedCallArgumentsSurvive() {
    String out = DuckDBSqlRewriter.rewrite("SELECT corr(coalesce(y, 0), x) FROM t");
    assertTrue(out.contains("CAST(coalesce(y, 0) AS DOUBLE)"),
        "the nested comma must not split the argument list, got: " + out);
  }

  @Test @DisplayName("quantile_cont keeps both operands, fraction included")
  void quantileTakesTwoOperands() {
    String out = DuckDBSqlRewriter.rewrite("SELECT quantile_cont(x, 0.25) FROM t");
    assertTrue(out.contains("quantile_cont(CAST(x AS DOUBLE), CAST(0.25 AS DOUBLE))"),
        "got: " + out);
  }

  @Test @DisplayName("a string literal containing a comma or paren is left intact")
  void stringLiteralsAreNotParsedAsSyntax() {
    String out = DuckDBSqlRewriter.rewrite("SELECT corr(y, CASE WHEN s = 'a,b(' THEN 1 ELSE 0 END) FROM t");
    assertTrue(out.contains("'a,b('"), "the literal must survive verbatim, got: " + out);
    assertTrue(out.contains("CAST(y AS DOUBLE)"), "got: " + out);
  }

  @Test @DisplayName("a column merely NAMED corr is untouched")
  void nonCallOccurrenceUntouched() {
    String out = DuckDBSqlRewriter.rewrite("SELECT corr FROM t");
    assertEquals("SELECT corr FROM t", out);
  }
}
