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
package org.apache.calcite.adapter.jira;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

/**
 * Read-only checks against a live Jira Cloud site: the credentials are accepted, the site's
 * forms list, and the first form's fields list. Run with {@code -PincludeTags=integration};
 * needs {@code JIRA_BASE_URL}, {@code JIRA_EMAIL} and {@code JIRA_API_TOKEN}.
 */
@Tag("integration")
class JiraLiveTest {

  private static String env(String name) {
    final String value = System.getenv(name);
    if (value == null || value.isEmpty()) {
      throw new IllegalStateException(name + " is not set; the live Jira tests need it");
    }
    return value;
  }

  @Test void theSitesFormsAndTheFirstFormsFieldsList() throws Exception {
    final Map<String, Object> operand = new HashMap<>();
    operand.put("baseUrl", env("JIRA_BASE_URL"));
    operand.put("email", env("JIRA_EMAIL"));
    operand.put("apiToken", env("JIRA_API_TOKEN"));
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA")) {
      final SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();
      root.add("jira", new JiraSchemaFactory().create(root, "jira", operand));
      final String project;
      final String issueType;
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT project_key, issue_type "
               + "FROM jira._jira_forms ORDER BY project_key, issue_type")) {
        assertThat("the site lists a form", rs.next(), is(true));
        project = rs.getString(1);
        issueType = rs.getString(2);
      }
      int fields = 0;
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT field_id, data_type FROM jira._jira_fields "
               + "WHERE project_key = '" + project + "' AND issue_type = '"
               + issueType.replace("'", "''") + "'")) {
        while (rs.next()) {
          fields++;
        }
      }
      assertThat(fields, greaterThan(0));
    }
  }
}
