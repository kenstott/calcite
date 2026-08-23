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
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * End-to-end regression test for a real MCP-captured query that used to blow up the planner
 * with {@code AssertionError: mismatched type $4 INTEGER} (a
 * {@code RexUtil.FixNullabilityShuttle}/{@code RexChecker} failure surfacing at different
 * points depending on planning phase).
 *
 * <p>Root cause: {@code RelFieldTrimmer.trimFields(Join, ...)} silently dropped a column that
 * was only referenced through a correlated {@code RexSubQuery} embedded in a join condition
 * (here, {@code fec.candidates.state}, read only via the correlated scalar sub-query
 * {@code m.state_name = (SELECT sr.state_name FROM geo.state_ref sr WHERE sr.state_abbr =
 * c.state)}), while leaving the sub-query's buried correlate-variable reference pointing at the
 * now-stale ordinal. Once {@code SubQueryRemoveRule}/{@code RelDecorrelator} later expanded that
 * sub-query into a real join, the stale ordinal resolved to a different (wrongly-typed) column
 * — see {@code RelFieldTrimmer.trimFields(Join, ImmutableBitSet, Set)} for the fix, and
 * {@code RelFieldTrimmerTest#testTrimCorrelatedSubqueryInJoinCondition()} in core for a
 * fast, non-integration unit-test reproduction of the same defect via {@code RelBuilder}
 * directly (no live connection required).
 *
 * <p>Requires R2 connectivity across the {@code fec}, {@code officials}, and {@code geo}
 * schemas (see {@code govdata/.env.prod.sample}); skips if unreachable.
 */
@Tag("integration")
public class HouseWinnersCrashReproTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(HouseWinnersCrashReproTest.class);

  private static final String SQL =
      "WITH house_winners AS (\n"
      + "  SELECT c.candidate_id, c.candidate_name, c.party, c.state, c.district\n"
      + "  FROM fec.candidates c\n"
      + "  JOIN officials.members m\n"
      + "    ON m.congress = 119\n"
      + "   AND m.district IS NOT NULL\n"
      + "   AND m.state_name = (SELECT sr.state_name FROM geo.state_ref sr WHERE sr.state_abbr = c.state)\n"
      + "   AND CAST(m.district AS VARCHAR) = CAST(CAST(c.district AS INTEGER) AS VARCHAR)\n"
      + "  WHERE c.election_year = 2024\n"
      + "    AND c.office = 'H'\n"
      + "    AND c.candidate_status = 'C'\n"
      + ")\n"
      + "SELECT COUNT(*) AS n_winners, COUNT(DISTINCT candidate_id) AS n_distinct\n"
      + "FROM house_winners";

  private static volatile boolean schemasAvailable = false;

  @BeforeAll
  static void checkEnvironment() throws Exception {
    Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    try (Connection c =
             DriverManager.getConnection("jdbc:govdata:source=fec,officials,geo", props)) {
      schemasAvailable = true;
      LOGGER.info("fec, officials, geo schemas reachable — running house-winners regression test");
    } catch (Exception e) {
      LOGGER.warn("fec/officials/geo schemas unavailable ({}); skipping", e.getMessage());
    }
  }

  private static Connection openConnection() throws Exception {
    Assumptions.assumeTrue(schemasAvailable,
        "fec, officials, geo schemas unavailable — skipping house-winners regression test");
    Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    return DriverManager.getConnection("jdbc:govdata:source=fec,officials,geo", props);
  }

  /**
   * A correlated scalar sub-query inside a JOIN...ON condition, joining {@code fec.candidates}
   * to {@code officials.members} with a cross-schema correlation into {@code geo.state_ref},
   * must plan without throwing.
   */
  @Test
  void houseWinnersQueryPlansWithoutAssertionError() throws Exception {
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery("EXPLAIN PLAN FOR " + SQL)) {
      StringBuilder plan = new StringBuilder();
      while (rs.next()) {
        plan.append(rs.getString(1)).append('\n');
      }
      LOGGER.info("Plan for house-winners query:\n{}", plan);
      assertFalse(plan.toString().isEmpty(), "EXPLAIN PLAN FOR must return a non-empty plan");
    }
  }
}
