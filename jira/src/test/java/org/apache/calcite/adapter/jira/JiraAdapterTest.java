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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The Jira adapter against {@link FakeJira}: forms resolved into tables, values read as a person
 * sees them, what of a WHERE clause reaches Jira, and INSERT, UPDATE and DELETE of issues and
 * links.
 */
class JiraAdapterTest {

  private static final String FORMS = "[{\"name\":\"tasks\",\"project\":\"ABC\","
      + "\"issueType\":\"Task\",\"fields\":[{\"field\":\"description\"},"
      + "{\"field\":\"priority\"},{\"field\":\"labels\"},{\"field\":\"Story Points\"},"
      + "{\"field\":\"customfield_10020\"},{\"field\":\"assignee\"},"
      + "{\"field\":\"assignee.displayName\"},{\"field\":\"duedate\"},"
      + "{\"field\":\"customfield_10030\"}]}]";

  private FakeJira jira;
  private final List<Long> waits = new ArrayList<>();
  private Connection connection;

  @BeforeEach void start() throws Exception {
    jira = new FakeJira();
  }

  @AfterEach void stop() throws SQLException {
    if (connection != null) {
      connection.close();
    }
    jira.close();
  }

  private Map<String, Object> operand(String forms) {
    final Map<String, Object> operand = new HashMap<>();
    operand.put("baseUrl", jira.baseUrl());
    operand.put("email", FakeJira.EMAIL);
    operand.put("apiToken", FakeJira.TOKEN);
    operand.put("forms", forms);
    operand.put("pageSize", "2");
    operand.put("retrySleeper", (JiraConnection.Sleeper) waits::add);
    return operand;
  }

  private Connection connect(Map<String, Object> operand) throws SQLException {
    connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
    final SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();
    root.add("jira", new JiraSchemaFactory().create(root, "jira", operand));
    return connection;
  }

  private List<String> rows(String sql) throws SQLException {
    if (connection == null) {
      connect(operand(FORMS));
    }
    final List<String> rows = new ArrayList<>();
    try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      final int columns = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        final List<String> values = new ArrayList<>();
        for (int i = 1; i <= columns; i++) {
          values.add(rs.getString(i));
        }
        rows.add(String.join("|", values));
      }
    }
    return rows;
  }

  private int update(String sql) throws SQLException {
    if (connection == null) {
      connect(operand(FORMS));
    }
    try (Statement stmt = connection.createStatement()) {
      return stmt.executeUpdate(sql);
    }
  }

  /** The messages of an exception and its causes, joined. */
  private static String messages(Throwable e) {
    final StringBuilder text = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause()) {
      text.append(t.getMessage()).append(" | ");
    }
    return text.toString();
  }

  private ObjectNode task(String summary) {
    final ObjectNode fields = FakeJira.MAPPER.createObjectNode().put("summary", summary);
    return jira.add("ABC", "10001", fields);
  }

  private JiraFormTable tasksTable() throws SQLException {
    return (JiraFormTable) connection.unwrap(CalciteConnection.class).getRootSchema()
        .subSchemas().get("jira")
        .tables().get("tasks");
  }

  @Test void aRejectedTokenFailsTheSchemaNamingTheAccount() {
    final Map<String, Object> operand = operand(FORMS);
    operand.put("apiToken", "wrong");
    final JiraException e = assertThrows(JiraException.class,
        () -> new JiraSchemaFactory().create(null, "jira", operand));
    assertThat(messages(e), containsString("HTTP 401"));
    assertThat(messages(e), containsString(FakeJira.EMAIL));
  }

  @Test void valuesReadAsAPersonSeesThem() throws Exception {
    final ObjectNode fields = (ObjectNode) FakeJira.MAPPER.readTree("{"
        + "\"summary\":\"Fix login\","
        + "\"description\":{\"type\":\"doc\",\"version\":1,\"content\":["
        + "{\"type\":\"paragraph\",\"content\":[{\"type\":\"text\",\"text\":\"Line one\"}]},"
        + "{\"type\":\"paragraph\",\"content\":[{\"type\":\"text\",\"text\":\"Line two\"}]}]},"
        + "\"priority\":{\"id\":\"2\",\"name\":\"High\"},"
        + "\"labels\":[\"ui\",\"auth\"],"
        + "\"customfield_10016\":5,"
        + "\"customfield_10020\":{\"id\":\"7\",\"value\":\"Major\"},"
        + "\"assignee\":{\"accountId\":\"acc-1\",\"displayName\":\"Ada Lovelace\"},"
        + "\"duedate\":\"2026-11-01\","
        + "\"customfield_10030\":{\"teamId\":\"t1\"}}");
    jira.add("ABC", "10001", fields);
    assertThat(rows("SELECT * FROM jira.tasks"),
        contains("ABC-1|10000|Fix login|To Do|2026-10-01 09:00:00|2026-10-02 10:30:00"
            + "|Line one\nLine two|High|[\"ui\",\"auth\"]|5.0|Major|acc-1|Ada Lovelace"
            + "|2026-11-01|{\"teamId\":\"t1\"}"));
  }

  @Test void columnsAreTheFixedOnesThenTheFieldsSnakeCased() throws Exception {
    connect(operand(FORMS));
    final List<String> names = new ArrayList<>();
    try (ResultSet rs = connection.getMetaData().getColumns(null, "jira", "tasks", null)) {
      while (rs.next()) {
        names.add(rs.getString("COLUMN_NAME"));
      }
    }
    assertThat(names, contains("key", "id", "summary", "status", "created", "updated",
        "description", "priority", "labels", "story_points", "severity", "assignee",
        "assignee_name", "due_date", "team"));
  }

  @Test void twoFieldsGivingOneColumnAreRefusedNamingBoth() {
    final Exception e = assertThrows(Exception.class, () -> connect(operand(
        "[{\"name\":\"t\",\"project\":\"ABC\",\"issueType\":\"Task\",\"fields\":["
            + "{\"field\":\"customfield_10020\"},{\"field\":\"customfield_10050\"}]}]"))
        .createStatement().executeQuery("SELECT * FROM jira.t"));
    assertThat(messages(e), containsString("field customfield_10050 and field "
        + "customfield_10020 both give the column 'severity'"));
  }

  @Test void aFieldWhoseColumnIsAFixedOneIsRefused() {
    final Exception e = assertThrows(Exception.class, () -> connect(operand(
        "[{\"name\":\"t\",\"project\":\"ABC\",\"issueType\":\"Task\",\"fields\":["
            + "{\"field\":\"status\"}]}]")).createStatement().executeQuery("SELECT * FROM jira.t"));
    assertThat(messages(e), containsString("field status and the fixed column status"));
  }

  @Test void aFieldNameSharedByTwoFieldsIsRefusedNamingTheirIds() {
    final Exception e = assertThrows(Exception.class, () -> connect(operand(
        "[{\"name\":\"t\",\"project\":\"ABC\",\"issueType\":\"Task\",\"fields\":["
            + "{\"field\":\"Owner\"}]}]")).createStatement().executeQuery("SELECT * FROM jira.t"));
    assertThat(messages(e),
        containsString("is the name of [customfield_10040, customfield_10041]"));
  }

  @Test void aColumnGivenInTheOperandIsUsed() throws Exception {
    connect(operand("[{\"name\":\"t\",\"project\":\"ABC\",\"issueType\":\"Task\",\"fields\":["
        + "{\"field\":\"customfield_10016\",\"column\":\"points\"}]}]"));
    task("A");
    assertThat(rows("SELECT key, points FROM jira.t"), contains("ABC-1|null"));
  }

  @Test void aSearchIsPagedAndALimitStopsThePages() throws Exception {
    task("A");
    task("B");
    task("C");
    assertThat(rows("SELECT key FROM jira.tasks"), contains("ABC-1", "ABC-2", "ABC-3"));
    assertThat(jira.requests("POST", "/rest/api/3/search/jql"), hasSize(2));
    jira.requests.clear();
    assertThat(rows("SELECT key FROM jira.tasks LIMIT 1"), hasSize(1));
    assertThat(jira.requests("POST", "/rest/api/3/search/jql"), hasSize(1));
  }

  @Test void aSearchAsksOnlyForTheSelectedFields() throws Exception {
    task("A");
    rows("SELECT key, summary, story_points FROM jira.tasks");
    final JsonNode body = jira.requests("POST", "/rest/api/3/search/jql").get(0).body;
    assertThat(body.path("fields").toString(), is("[\"summary\",\"customfield_10016\"]"));
    assertThat(body.path("jql").asText(), is("(project = \"ABC\" AND issuetype = 10001)"));
  }

  @Test void keysAreFetchedByKeyAndAMissingOrForeignKeyIsNoRow() throws Exception {
    task("A");
    task("B");
    jira.add("XYZ", "10001", FakeJira.MAPPER.createObjectNode().put("summary", "other"));
    assertThat(rows("SELECT key, summary FROM jira.tasks "
            + "WHERE key IN ('ABC-2', 'ABC-9', 'XYZ-1')"),
        contains("ABC-2|B"));
    assertThat(jira.requests("POST", "/rest/api/3/search/jql"), empty());
    assertThat(jira.requests("POST", "/rest/api/3/issue/bulkfetch").get(0).body
        .path("issueIdsOrKeys").toString(), is("[\"ABC-2\",\"ABC-9\",\"XYZ-1\"]"));
  }

  @Test void aDatetimeBoundIsSentWidenedAndStillAppliedExactly() throws Exception {
    task("A");
    assertThat(rows("SELECT key FROM jira.tasks "
        + "WHERE created >= TIMESTAMP '2026-10-01 09:00:01'"), empty());
    assertThat(jira.requests("POST", "/rest/api/3/search/jql").get(0).body.path("jql").asText(),
        is("(project = \"ABC\" AND issuetype = 10001) AND created >= \"2026-09-30\""));
  }

  @Test void equalityOnAnOptionIsNotSent() throws Exception {
    task("A");
    assertThat(rows("SELECT key FROM jira.tasks WHERE severity = 'Nope'"), empty());
    assertThat(jira.requests("POST", "/rest/api/3/search/jql").get(0).body.path("jql").asText(),
        is("(project = \"ABC\" AND issuetype = 10001)"));
  }

  @Test void insertCreatesIssuesFiftyARequestAndReportsTheirKeys() throws Exception {
    final StringBuilder values = new StringBuilder();
    for (int i = 1; i <= 51; i++) {
      values.append(i == 1 ? "" : ", ").append("('Task ").append(i).append("')");
    }
    assertThat(update("INSERT INTO jira.tasks (summary) VALUES " + values), is(51));
    assertThat(jira.requests("POST", "/rest/api/3/issue/bulk"), hasSize(2));
    final List<String> keys = tasksTable().takeInsertedKeys();
    assertThat(keys, hasSize(51));
    assertThat(keys.get(0), is("ABC-1"));
    assertThat(keys.get(50), is("ABC-51"));
  }

  @Test void insertWritesEachFieldInItsJiraShape() throws Exception {
    update("INSERT INTO jira.tasks (summary, description, priority, labels, story_points, "
        + "severity, assignee, due_date) VALUES ('S', 'one\ntwo', 'High', '[\"a\",\"b\"]', 3, "
        + "'Major', 'acc-1', DATE '2026-11-01')");
    final JsonNode fields = jira.requests("POST", "/rest/api/3/issue/bulk").get(0).body
        .path("issueUpdates").get(0).path("fields");
    assertThat(fields.toString(), is("{\"project\":{\"key\":\"ABC\"},"
        + "\"issuetype\":{\"id\":\"10001\"},\"summary\":\"S\","
        + "\"description\":{\"type\":\"doc\",\"version\":1,\"content\":["
        + "{\"type\":\"paragraph\",\"content\":[{\"type\":\"text\",\"text\":\"one\"}]},"
        + "{\"type\":\"paragraph\",\"content\":[{\"type\":\"text\",\"text\":\"two\"}]}]},"
        + "\"priority\":{\"name\":\"High\"},\"labels\":[\"a\",\"b\"],"
        + "\"customfield_10016\":3.0,\"customfield_10020\":{\"value\":\"Major\"},"
        + "\"assignee\":{\"accountId\":\"acc-1\"},\"duedate\":\"2026-11-01\"}"));
  }

  @Test void insertWithAStatusIsRefused() {
    final SQLException e = assertThrows(SQLException.class,
        () -> update("INSERT INTO jira.tasks (summary, status) VALUES ('S', 'Done')"));
    assertThat(messages(e), containsString("a new issue starts in its workflow's first "
        + "status. Change it with UPDATE"));
  }

  @Test void aBulkCreateThatFailsNamesTheRowAndTheIssuesCreated() throws Exception {
    jira.override(r -> r.path.endsWith("/issue/bulk")
        ? new FakeJira.Reply(201, "{\"issues\":[{\"id\":\"1\",\"key\":\"ABC-1\"}],"
            + "\"errors\":[{\"failedElementNumber\":1,\"status\":400,\"elementErrors\":"
            + "{\"errors\":{\"summary\":\"You must specify a summary\"}}}]}")
        : null);
    final SQLException e = assertThrows(SQLException.class,
        () -> update("INSERT INTO jira.tasks (summary) VALUES ('A'), ('B')"));
    assertThat(messages(e), containsString("failed after 1 of 2 issues were created "
        + "([ABC-1]): row 2: summary: You must specify a summary"));
    assertThat(tasksTable().takeInsertedKeys(), contains("ABC-1"));
  }

  @Test void updateWritesFieldsAndTakesTheTransitionToANewStatus() throws Exception {
    task("A");
    assertThat(update("UPDATE jira.tasks SET summary = 'A2', status = 'Done' "
        + "WHERE key = 'ABC-1'"), is(1));
    assertThat(jira.requests("PUT", "/rest/api/3/issue/ABC-1").get(0).body.toString(),
        is("{\"fields\":{\"summary\":\"A2\"}}"));
    assertThat(jira.requests("POST", "/rest/api/3/issue/ABC-1/transitions").get(0).body
        .toString(), is("{\"transition\":{\"id\":\"31\"}}"));
    assertThat(rows("SELECT summary, status FROM jira.tasks"), contains("A2|Done"));
  }

  @Test void aDisplayNameIsReadOnly() {
    task("A");
    final SQLException e = assertThrows(SQLException.class,
        () -> update("UPDATE jira.tasks SET assignee_name = 'Ada'"));
    assertThat(messages(e), containsString("sets assignee_name, which Jira assigns or which "
        + "cannot be written"));
  }

  @Test void aStatusNoTransitionReachesIsRefusedNamingTheReachableOnes() {
    task("A");
    final SQLException e = assertThrows(SQLException.class,
        () -> update("UPDATE jira.tasks SET status = 'Closed' WHERE key = 'ABC-1'"));
    assertThat(messages(e), containsString("No workflow transition takes ABC-1 from "
        + "'To Do' to 'Closed'; the statuses reachable from 'To Do' are [In Progress, Done]"));
  }

  @Test void updateOfAColumnJiraAssignsIsRefused() {
    task("A");
    final SQLException e = assertThrows(SQLException.class,
        () -> update("UPDATE jira.tasks SET created = TIMESTAMP '2026-01-01 00:00:00'"));
    assertThat(messages(e), containsString("sets created, which Jira assigns"));
  }

  @Test void deleteOfAnIssueWithSubtasksIsRefusedNamingThemAndDeletesNothing() throws Exception {
    task("A");
    final ObjectNode parent = task("B");
    ((ArrayNode) parent.path("fields").path("subtasks"))
        .addObject().put("key", "ABC-7");
    final SQLException e = assertThrows(SQLException.class,
        () -> update("DELETE FROM jira.tasks"));
    assertThat(messages(e), containsString("Issues with subtasks: {ABC-2=[ABC-7]}. "
        + "Nothing was deleted"));
    assertThat(jira.requests("DELETE", "/rest/api/3/issue/ABC-1"), empty());
  }

  @Test void deleteRemovesEachIssue() throws Exception {
    task("A");
    task("B");
    assertThat(update("DELETE FROM jira.tasks WHERE key = 'ABC-2'"), is(1));
    assertThat(rows("SELECT key FROM jira.tasks"), contains("ABC-1"));
  }

  @Test void linksAreServedOnceWithTheirEnds() throws Exception {
    task("A");
    task("B");
    task("C");
    jira.link("Blocks", "ABC-1", "ABC-2");
    jira.link("Relates", "ABC-3", "ABC-1");
    assertThat(rows("SELECT * FROM jira.issue_links ORDER BY link_id"),
        contains("20000|Blocks|ABC-1|ABC-2", "20001|Relates|ABC-3|ABC-1"));
    jira.requests.clear();
    assertThat(rows("SELECT link_id FROM jira.issue_links WHERE inward_key = 'ABC-2'"),
        contains("20000"));
    assertThat(jira.requests("POST", "/rest/api/3/search/jql"), empty());
  }

  @Test void aLinkIsCreatedWithItsIdReportedAndDeleted() throws Exception {
    task("A");
    task("B");
    assertThat(update("INSERT INTO jira.issue_links (link_type, outward_key, inward_key) "
        + "VALUES ('Blocks', 'ABC-1', 'ABC-2')"), is(1));
    assertThat(jira.requests("POST", "/rest/api/3/issueLink").get(0).body.toString(),
        is("{\"type\":{\"name\":\"Blocks\"},\"outwardIssue\":{\"key\":\"ABC-1\"},"
            + "\"inwardIssue\":{\"key\":\"ABC-2\"}}"));
    final IssueLinksTable links = (IssueLinksTable) connection.unwrap(CalciteConnection.class).getRootSchema()
        .subSchemas().get("jira").tables().get("issue_links");
    assertThat(links.takeInsertedKeys(), contains("20000"));
    assertThat(update("DELETE FROM jira.issue_links WHERE link_id = '20000'"), is(1));
    assertThat(jira.requests("DELETE", "/rest/api/3/issueLink/20000"), hasSize(1));
  }

  @Test void aLinkCannotBeUpdated() {
    task("A");
    task("B");
    jira.link("Blocks", "ABC-1", "ABC-2");
    final SQLException e = assertThrows(SQLException.class,
        () -> update("UPDATE jira.issue_links SET link_type = 'Relates'"));
    assertThat(messages(e), containsString("Jira cannot change a link in place"));
  }

  @Test void formsListsEveryProjectAndIssueType() throws Exception {
    assertThat(rows("SELECT project_key, issue_type, issue_type_id, subtask, name "
            + "FROM jira._jira_forms ORDER BY project_key, issue_type"),
        contains("ABC|Sub-task|10002|true|abc_sub_task", "ABC|Task|10001|false|abc_task",
            "XYZ|Sub-task|10002|true|xyz_sub_task", "XYZ|Task|10001|false|xyz_task"));
  }

  @Test void fieldsListsOneIssueTypesCreateFieldsWithTheirColumnTypes() throws Exception {
    assertThat(rows("SELECT field_id, column_name, data_type, required, read_only "
            + "FROM jira._jira_fields "
            + "WHERE project_key = 'ABC' AND issue_type = 'Task' ORDER BY ordinal_position"),
        contains("summary|summary|character varying|true|false",
            "description|description|character varying|false|false",
            "priority|priority|character varying|false|false",
            "labels|labels|character varying|false|false",
            "customfield_10016|story_points|double precision|false|false",
            "customfield_10020|severity|character varying|false|false",
            "assignee|assignee|character varying|false|false",
            "assignee.displayName|assignee_name|character varying|false|true",
            "duedate|due_date|date|false|false",
            "customfield_10030|team|character varying|false|true"));
  }

  @Test void fieldsWithoutAnIssueTypeIsRefused() {
    final SQLException e = assertThrows(SQLException.class,
        () -> rows("SELECT field_id FROM jira._jira_fields WHERE project_key = 'ABC'"));
    assertThat(messages(e), containsString("must be queried with project_key = '<key>' "
        + "AND issue_type = '<issue type>'"));
  }

  @Test void aRateLimitedRequestIsRetriedAfterItsRetryAfter() throws Exception {
    final boolean[] limited = {false};
    jira.override(r -> {
      if (r.path.endsWith("/search/jql") && !limited[0]) {
        limited[0] = true;
        final FakeJira.Reply reply = new FakeJira.Reply(429, "{}");
        reply.headers.put("Retry-After", "2");
        return reply;
      }
      return null;
    });
    task("A");
    assertThat(rows("SELECT key FROM jira.tasks"), contains("ABC-1"));
    assertThat(waits, contains(2000L));
  }
}
