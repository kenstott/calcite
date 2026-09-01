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
 * D-084 repro attempt: pre/post COVID death-rate CTE (health.cdc_mortality) joined against
 * officials.state_political_index, the shape reported to throw a RexShiftShuttle error and,
 * via ols_regression, a singular-matrix error.
 */
@Tag("integration")
public class D084OlsRegressionReproTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(D084OlsRegressionReproTest.class);

  private static final String SQL =
      "WITH pre AS (\n"
      + "  SELECT state, AVG(CAST(deaths AS DOUBLE)) AS pre_deaths\n"
      + "  FROM health.cdc_mortality\n"
      + "  WHERE source_type = 'weekly' AND cause_name = 'COVID-19'\n"
      + "    AND week_ending_date < '2021-01-01'\n"
      + "  GROUP BY state\n"
      + "),\n"
      + "post AS (\n"
      + "  SELECT state, AVG(CAST(deaths AS DOUBLE)) AS post_deaths\n"
      + "  FROM health.cdc_mortality\n"
      + "  WHERE source_type = 'weekly' AND cause_name = 'COVID-19'\n"
      + "    AND week_ending_date >= '2021-01-01'\n"
      + "  GROUP BY state\n"
      + "),\n"
      + "rates AS (\n"
      + "  SELECT pre.state AS state, pre.pre_deaths AS pre_deaths, post.post_deaths AS post_deaths,\n"
      + "         (post.post_deaths - pre.pre_deaths) AS delta_deaths\n"
      + "  FROM pre JOIN post ON pre.state = post.state\n"
      + ")\n"
      + "SELECT r.state, r.pre_deaths, r.post_deaths, r.delta_deaths, p.cpi\n"
      + "FROM rates r\n"
      + "JOIN officials.state_political_index p\n"
      + "  ON p.state_name = r.state AND p.congress = 117";

  private static volatile boolean schemasAvailable = false;

  @BeforeAll
  static void checkEnvironment() throws Exception {
    Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    try (Connection c =
             DriverManager.getConnection("jdbc:govdata:source=health,officials", props)) {
      schemasAvailable = true;
      LOGGER.info("health, officials schemas reachable — running D-084 regression test");
    } catch (Exception e) {
      LOGGER.warn("health/officials schemas unavailable ({}); skipping", e.getMessage());
    }
  }

  private static Connection openConnection() throws Exception {
    Assumptions.assumeTrue(schemasAvailable,
        "health, officials schemas unavailable — skipping D-084 regression test");
    Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    return DriverManager.getConnection("jdbc:govdata:source=health,officials", props);
  }

  @Test
  void planExplain() throws Exception {
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery("EXPLAIN PLAN FOR " + SQL)) {
      StringBuilder plan = new StringBuilder();
      while (rs.next()) {
        plan.append(rs.getString(1)).append('\n');
      }
      LOGGER.info("Plan for D-084 query:\n{}", plan);
      assertFalse(plan.toString().isEmpty(), "EXPLAIN PLAN FOR must return a non-empty plan");
    }
  }

  @Test
  void executeQuery() throws Exception {
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(SQL)) {
      int n = 0;
      StringBuilder sb = new StringBuilder();
      while (rs.next()) {
        n++;
        sb.append(rs.getString("state")).append('=')
          .append(rs.getString("pre_deaths")).append('/')
          .append(rs.getString("post_deaths")).append('/')
          .append(rs.getString("delta_deaths")).append('/')
          .append(rs.getString("cpi")).append('\n');
      }
      LOGGER.info("D-084 query returned {} rows:\n{}", n, sb);
    }
  }

  private static final String SQL_CORRELATED =
      "WITH pre AS (\n"
      + "  SELECT state, AVG(CAST(deaths AS DOUBLE)) AS pre_deaths\n"
      + "  FROM health.cdc_mortality\n"
      + "  WHERE source_type = 'weekly' AND cause_name = 'COVID-19'\n"
      + "    AND week_ending_date < '2021-01-01'\n"
      + "  GROUP BY state\n"
      + "),\n"
      + "post AS (\n"
      + "  SELECT state, AVG(CAST(deaths AS DOUBLE)) AS post_deaths\n"
      + "  FROM health.cdc_mortality\n"
      + "  WHERE source_type = 'weekly' AND cause_name = 'COVID-19'\n"
      + "    AND week_ending_date >= '2021-01-01'\n"
      + "  GROUP BY state\n"
      + "),\n"
      + "rates AS (\n"
      + "  SELECT pre.state AS state, pre.pre_deaths AS pre_deaths, post.post_deaths AS post_deaths,\n"
      + "         (post.post_deaths - pre.pre_deaths) AS delta_deaths\n"
      + "  FROM pre JOIN post ON pre.state = post.state\n"
      + ")\n"
      + "SELECT r.state, r.pre_deaths, r.post_deaths, r.delta_deaths, p.cpi\n"
      + "FROM rates r\n"
      + "JOIN officials.state_political_index p\n"
      + "  ON p.state_name = r.state\n"
      + " AND p.congress = (SELECT MAX(x.congress) FROM officials.state_political_index x WHERE x.state_name = p.state_name)";

  @Test
  void planExplainCorrelated() throws Exception {
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery("EXPLAIN PLAN FOR " + SQL_CORRELATED)) {
      StringBuilder plan = new StringBuilder();
      while (rs.next()) {
        plan.append(rs.getString(1)).append('\n');
      }
      LOGGER.info("Plan for D-084 correlated query:\n{}", plan);
      assertFalse(plan.toString().isEmpty(), "EXPLAIN PLAN FOR must return a non-empty plan");
    }
  }

  @Test
  void executeQueryCorrelated() throws Exception {
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(SQL_CORRELATED)) {
      int n = 0;
      StringBuilder sb = new StringBuilder();
      while (rs.next()) {
        n++;
        sb.append(rs.getString("state")).append('=')
          .append(rs.getString("pre_deaths")).append('/')
          .append(rs.getString("post_deaths")).append('/')
          .append(rs.getString("delta_deaths")).append('/')
          .append(rs.getString("cpi")).append('\n');
      }
      LOGGER.info("D-084 correlated query returned {} rows:\n{}", n, sb);
    }
  }


  @Test
  void diagnoseStateNames() throws Exception {
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT DISTINCT state FROM health.cdc_mortality "
             + "WHERE source_type = 'weekly' AND cause_name = 'COVID-19' ORDER BY 1")) {
      StringBuilder sb = new StringBuilder();
      int n = 0;
      while (rs.next()) {
        n++;
        sb.append(rs.getString(1)).append('|');
      }
      LOGGER.info("cdc_mortality weekly COVID-19 distinct states ({}): {}", n, sb);
    }
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT DISTINCT state_name FROM officials.state_political_index "
             + "WHERE congress = 117 ORDER BY 1")) {
      StringBuilder sb = new StringBuilder();
      int n = 0;
      while (rs.next()) {
        n++;
        sb.append(rs.getString(1)).append('|');
      }
      LOGGER.info("state_political_index congress=117 distinct state_name ({}): {}", n, sb);
    }
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT source_type, cause_name, COUNT(*) FROM health.cdc_mortality "
             + "WHERE cause_name = 'COVID-19' GROUP BY source_type, cause_name")) {
      StringBuilder sb = new StringBuilder();
      while (rs.next()) {
        sb.append(rs.getString(1)).append('/').append(rs.getString(2)).append('=')
          .append(rs.getInt(3)).append(' ');
      }
      LOGGER.info("cdc_mortality COVID-19 rows by source_type: {}", sb);
    }
  }


  @Test
  void diagnoseStateGroupBy() throws Exception {
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT state, COUNT(*) FROM health.cdc_mortality "
             + "WHERE source_type = 'weekly' AND cause_name = 'COVID-19' GROUP BY state ORDER BY 1")) {
      StringBuilder sb = new StringBuilder();
      int n = 0;
      while (rs.next()) {
        n++;
        sb.append('[').append(rs.getString(1)).append('=').append(rs.getInt(2)).append(']');
      }
      LOGGER.info("cdc_mortality weekly COVID-19 GROUP BY state ({} groups): {}", n, sb);
    }
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT state, week_ending_date, cause_name, source_type, deaths FROM health.cdc_mortality "
             + "WHERE source_type = 'weekly' AND cause_name = 'COVID-19' LIMIT 10")) {
      StringBuilder sb = new StringBuilder();
      int n = 0;
      while (rs.next()) {
        n++;
        sb.append('[').append(rs.getString(1)).append('|').append(rs.getString(2)).append('|')
          .append(rs.getString(3)).append('|').append(rs.getString(4)).append('|')
          .append(rs.getString(5)).append(']');
      }
      LOGGER.info("cdc_mortality weekly COVID-19 sample rows ({}): {}", n, sb);
    }
  }


  @Test
  void diagnoseSourceTypeValues() throws Exception {
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT source_type, LENGTH(source_type), COUNT(*) FROM health.cdc_mortality "
             + "GROUP BY source_type, LENGTH(source_type) ORDER BY 1")) {
      StringBuilder sb = new StringBuilder();
      while (rs.next()) {
        sb.append('[').append(rs.getString(1)).append("(len=").append(rs.getInt(2)).append(")=")
          .append(rs.getInt(3)).append(']');
      }
      LOGGER.info("cdc_mortality ALL source_type values (no filter): {}", sb);
    }
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT COUNT(*) FROM health.cdc_mortality WHERE source_type = \'weekly\'")) {
      rs.next();
      LOGGER.info("cdc_mortality count WHERE source_type = 'weekly' (literal): {}", rs.getInt(1));
    }
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT COUNT(*) FROM health.cdc_mortality WHERE source_type LIKE \'weekly%\'")) {
      rs.next();
      LOGGER.info("cdc_mortality count WHERE source_type LIKE 'weekly%': {}", rs.getInt(1));
    }
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT COUNT(*) FROM health.cdc_mortality WHERE cause_name = \'COVID-19\'")) {
      rs.next();
      LOGGER.info("cdc_mortality count WHERE cause_name = 'COVID-19' (no source_type filter): {}", rs.getInt(1));
    }
  }


  @Test
  void diagnoseAnnualEquality() throws Exception {
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT COUNT(*) FROM health.cdc_mortality WHERE source_type = \'annual\'")) {
      rs.next();
      LOGGER.info("cdc_mortality count WHERE source_type = 'annual' (literal): {}", rs.getInt(1));
    }
    try (Connection c = openConnection();
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "EXPLAIN PLAN FOR SELECT COUNT(*) FROM health.cdc_mortality WHERE source_type = \'weekly\'")) {
      StringBuilder sb = new StringBuilder();
      while (rs.next()) {
        sb.append(rs.getString(1)).append('\n');
      }
      LOGGER.info("Plan for source_type='weekly' count:\n{}", sb);
    }
  }

}
