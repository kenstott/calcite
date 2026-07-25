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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link DuckDBSqlRewriter#rewrite(String)} — the transparent pre-parse
 * rewrite of the reserved-keyword statistical aggregates (corr, regr_*) to their
 * non-reserved aliases. Verifies function calls are rewritten while string literals,
 * quoted identifiers, non-call occurrences, and substrings are left untouched.
 */
@Tag("unit")
public class DuckDBSqlRewriterTest {

  @Test void rewritesReservedAggregateCalls() {
    assertEquals("SELECT agg_corr(a, b) FROM t",
        DuckDBSqlRewriter.rewrite("SELECT corr(a, b) FROM t"));
    assertEquals("SELECT agg_regr_slope(y, x), agg_regr_intercept(y, x) FROM t",
        DuckDBSqlRewriter.rewrite("SELECT regr_slope(y, x), regr_intercept(y, x) FROM t"));
    assertEquals("agg_regr_r2 agg_regr_avgx agg_regr_avgy agg_regr_sxy",
        DuckDBSqlRewriter.rewrite("regr_r2( regr_avgx( regr_avgy( regr_sxy(")
            .replaceAll("\\(", ""));
  }

  @Test void isCaseInsensitive() {
    assertEquals("SELECT agg_corr(a, b) FROM t",
        DuckDBSqlRewriter.rewrite("SELECT CORR(a, b) FROM t"));
    assertEquals("SELECT agg_corr(a, b) FROM t",
        DuckDBSqlRewriter.rewrite("SELECT Corr(a, b) FROM t"));
  }

  @Test void allowsWhitespaceBeforeParen() {
    assertEquals("SELECT agg_corr (a, b) FROM t",
        DuckDBSqlRewriter.rewrite("SELECT corr (a, b) FROM t"));
  }

  @Test void leavesStringLiteralsUntouched() {
    assertEquals("SELECT 'corr(x)' FROM t",
        DuckDBSqlRewriter.rewrite("SELECT 'corr(x)' FROM t"));
    assertEquals("SELECT agg_corr(a, b), 'regr_slope(q)' FROM t",
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
}
