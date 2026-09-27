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
package org.apache.calcite.test;

import org.junit.jupiter.api.Test;

/**
 * End-to-end regression tests for
 * <a href="https://github.com/kenstott/govdata-ops/issues/439">[GOVDATA-OPS-439]</a>:
 * {@code COALESCE(SUM(x), 0)} over a nullable (LEFT JOIN) column, grouped by
 * more than one key, used to throw {@code AssertionError: type mismatch}
 * because {@link org.apache.calcite.rel.rules.ProjectAggregateMergeRule}
 * built the replacement {@code SUM0} reference without offsetting it by the
 * number of group keys (see plan-level coverage in
 * {@link RelOptRulesTest#testProjectAggregateMergeSum0WithMultipleGroupKeys()}
 * and
 * {@link RelOptRulesTest#testProjectAggregateMergeSum0WithMultipleGroupKeysAndOtherAgg()}).
 *
 * <p>These tests exercise the full JDBC path (parse, validate, plan, execute)
 * to additionally confirm that the computed values are correct, not just that
 * planning does not throw.
 */
class Defect439ReproTest {
  @Test void coalesceSumOverLeftJoinGroupByTwoColumns() {
    final String sql = "SELECT s.state_abbr, s.state_name, COALESCE(SUM(c.cap), 0) AS x\n"
        + "FROM (VALUES ('CA','California'), ('TX','Texas'), ('NY','New York'))\n"
        + "       AS s(state_abbr, state_name)\n"
        + "LEFT JOIN (VALUES ('CA', CAST(5.0 AS DOUBLE), 'Addition', 2022),\n"
        + "                  ('CA', CAST(3.0 AS DOUBLE), 'Addition', 2023))\n"
        + "       AS c(state_abbr, cap, change_type, change_year)\n"
        + "  ON c.state_abbr = s.state_abbr\n"
        + "GROUP BY s.state_abbr, s.state_name";
    CalciteAssert.that()
        .query(sql)
        .returnsUnordered(
            "STATE_ABBR=CA; STATE_NAME=California; X=8.0",
            "STATE_ABBR=NY; STATE_NAME=New York  ; X=0.0",
            "STATE_ABBR=TX; STATE_NAME=Texas     ; X=0.0");
  }

  @Test void coalesceSumOverLeftJoinGroupByTwoColumnsWithSecondAggregate() {
    final String sql = "SELECT s.state_abbr, s.state_name, COUNT(*) AS cnt,\n"
        + "    COALESCE(SUM(c.cap), 0) AS x\n"
        + "FROM (VALUES ('CA','California'), ('TX','Texas'), ('NY','New York'))\n"
        + "       AS s(state_abbr, state_name)\n"
        + "LEFT JOIN (VALUES ('CA', CAST(5.0 AS DOUBLE), 'Addition', 2022),\n"
        + "                  ('CA', CAST(3.0 AS DOUBLE), 'Addition', 2023))\n"
        + "       AS c(state_abbr, cap, change_type, change_year)\n"
        + "  ON c.state_abbr = s.state_abbr\n"
        + "GROUP BY s.state_abbr, s.state_name";
    CalciteAssert.that()
        .query(sql)
        .returnsUnordered(
            "STATE_ABBR=CA; STATE_NAME=California; CNT=2; X=8.0",
            "STATE_ABBR=NY; STATE_NAME=New York  ; CNT=1; X=0.0",
            "STATE_ABBR=TX; STATE_NAME=Texas     ; CNT=1; X=0.0");
  }
}
