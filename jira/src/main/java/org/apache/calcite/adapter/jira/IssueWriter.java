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

import org.apache.calcite.adapter.jira.JiraCatalog.Form;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes the issues of one form.
 *
 * <ul>
 *   <li>INSERT: {@code POST /issue/bulk}, 50 issues a request (Jira's maximum), in the form's
 *   project and issue type. A new issue starts in its workflow's first status, so a status is
 *   refused; so are key, id, created and updated, which Jira assigns.
 *   <li>UPDATE: one {@code PUT /issue/<key>} per issue for the changed fields, then, if the
 *   status changed, the workflow transition that reaches the new status. No transition reaching
 *   it is a refusal naming the statuses that are reachable.
 *   <li>DELETE: one {@code DELETE /issue/<key>} per issue. An issue with subtasks is refused,
 *   naming them, before any issue is deleted; subtasks are never deleted along with it.
 * </ul>
 *
 * <p>There are no transactions: a statement that fails part way leaves what was written before
 * the failure written, and the error says how many rows were.
 */
final class IssueWriter {

  /** Issues per bulk create, Jira's maximum. */
  static final int CREATE_BATCH = 50;

  private final JiraConnection connection;
  private final Form form;
  private final boolean documentFormat;

  IssueWriter(JiraConnection connection, Form form) {
    this.connection = connection;
    this.form = form;
    this.documentFormat = connection.flavor().documentFormat;
  }

  /**
   * Creates one issue per row, each row holding one value per column (NULL for a column the
   * statement leaves out, which Jira then defaults).
   *
   * @param keys receives the key of each issue created, in order
   */
  long insert(List<Object[]> rows, List<String> keys) {
    final List<ObjectNode> updates = new ArrayList<>(rows.size());
    for (int r = 0; r < rows.size(); r++) {
      final Object[] row = rows.get(r);
      final ObjectNode fields = JiraConnection.MAPPER.createObjectNode();
      fields.putObject("project").put("key", form.projectKey);
      fields.putObject("issuetype").put("id", form.issueTypeId);
      for (int c = 0; c < form.columns.size(); c++) {
        final JiraColumn column = form.columns.get(c);
        if (row[c] == null) {
          continue;
        }
        if (column.name.equals("status")) {
          throw new JiraException("INSERT into " + form.name + " sets status (row " + (r + 1)
              + "); a new issue starts in its workflow's first status. Change it with UPDATE");
        }
        if (!column.writable) {
          throw new JiraException("INSERT into " + form.name + " sets " + column.name
              + " (row " + (r + 1) + "), which Jira assigns or which cannot be written");
        }
        fields.set(column.fieldId, column.write(row[c], documentFormat));
      }
      final ObjectNode update = JiraConnection.MAPPER.createObjectNode();
      update.set("fields", fields);
      updates.add(update);
    }
    for (int start = 0; start < updates.size(); start += CREATE_BATCH) {
      final List<ObjectNode> batch =
          updates.subList(start, Math.min(updates.size(), start + CREATE_BATCH));
      final ObjectNode body = JiraConnection.MAPPER.createObjectNode();
      body.putArray("issueUpdates").addAll(batch);
      final JiraConnection.Reply reply =
          connection.exchange("POST", "/issue/bulk", body, 201, 400);
      final JsonNode answer = reply.body == null
          ? JiraConnection.MAPPER.createObjectNode() : reply.body;
      for (JsonNode issue : answer.path("issues")) {
        keys.add(issue.path("key").asText());
      }
      final JsonNode errors = answer.path("errors");
      if (reply.status != 201 || errors.size() > 0) {
        final StringBuilder message = new StringBuilder();
        for (JsonNode error : errors) {
          message.append(message.length() == 0 ? "" : "; ").append("row ")
              .append(start + error.path("failedElementNumber").asInt() + 1).append(": ")
              .append(JiraConnection.errorText(error.path("elementErrors")));
        }
        throw new JiraException(reply.status, "INSERT into " + form.name + " failed after "
            + keys.size() + " of " + rows.size() + " issues were created (" + keys + "): "
            + (message.length() == 0 ? JiraConnection.errorText(answer) : message));
      }
    }
    return keys.size();
  }

  /** Updates one issue per row: the table row followed by one new value per update column. */
  long update(List<Object[]> rows, List<String> updateColumns) {
    final int keyIndex = form.columnIndex("key");
    final int statusIndex = form.columnIndex("status");
    long written = 0;
    for (Object[] row : rows) {
      final String key = (String) row[keyIndex];
      try {
        final ObjectNode fields = JiraConnection.MAPPER.createObjectNode();
        String status = null;
        for (int u = 0; u < updateColumns.size(); u++) {
          final String name = updateColumns.get(u);
          final Object value = row[form.columns.size() + u];
          if (name.equals("status")) {
            if (value == null) {
              throw new JiraException("UPDATE of " + form.name + " clears the status of " + key
                  + "; an issue always has one");
            }
            if (!value.equals(row[statusIndex])) {
              status = value.toString();
            }
            continue;
          }
          final JiraColumn column = form.columns.get(form.columnIndex(name));
          if (!column.writable) {
            throw new JiraException("UPDATE of " + form.name + " sets " + name
                + ", which Jira assigns or which cannot be written");
          }
          fields.set(column.fieldId, column.write(value, documentFormat));
        }
        if (fields.size() > 0) {
          final ObjectNode body = JiraConnection.MAPPER.createObjectNode();
          body.set("fields", fields);
          connection.put("/issue/" + JiraConnection.encode(key), body);
        }
        if (status != null) {
          transition(key, (String) row[statusIndex], status);
        }
        written++;
      } catch (JiraException e) {
        throw new JiraException(e.status, "UPDATE of " + form.name + " failed at issue " + key
            + " after " + written + " issues were updated: " + e.getMessage());
      }
    }
    return written;
  }

  /** Moves an issue to a status through the workflow transition that reaches it. */
  private void transition(String key, String from, String to) {
    final JsonNode transitions =
        connection.get("/issue/" + JiraConnection.encode(key) + "/transitions", Map.of())
            .path("transitions");
    String id = null;
    final List<String> reachable = new ArrayList<>();
    for (JsonNode transition : transitions) {
      final String target = transition.path("to").path("name").asText();
      reachable.add(target);
      if (id == null && target.equals(to)) {
        id = transition.path("id").asText();
      }
    }
    if (id == null) {
      throw new JiraException("No workflow transition takes " + key + " from '" + from
          + "' to '" + to + "'; the statuses reachable from '" + from + "' are " + reachable);
    }
    final ObjectNode body = JiraConnection.MAPPER.createObjectNode();
    body.putObject("transition").put("id", id);
    connection.exchange("POST", "/issue/" + JiraConnection.encode(key) + "/transitions", body,
        204);
  }

  /** Deletes one issue per row, after checking that none has subtasks. */
  long delete(List<Object[]> rows) {
    final int keyIndex = form.columnIndex("key");
    final Map<String, List<String>> withSubtasks = new LinkedHashMap<>();
    for (Object[] row : rows) {
      final String key = (String) row[keyIndex];
      final JsonNode subtasks = connection.get("/issue/" + JiraConnection.encode(key),
          Map.of("fields", "subtasks")).path("fields").path("subtasks");
      if (subtasks.size() > 0) {
        final List<String> keys = new ArrayList<>();
        for (JsonNode subtask : subtasks) {
          keys.add(subtask.path("key").asText());
        }
        withSubtasks.put(key, keys);
      }
    }
    if (!withSubtasks.isEmpty()) {
      throw new JiraException("DELETE from " + form.name + " would leave subtasks without "
          + "their parent; delete the subtasks first. Issues with subtasks: " + withSubtasks
          + ". Nothing was deleted");
    }
    long written = 0;
    for (Object[] row : rows) {
      final String key = (String) row[keyIndex];
      try {
        connection.delete("/issue/" + JiraConnection.encode(key));
      } catch (JiraException e) {
        throw new JiraException(e.status, "DELETE from " + form.name + " failed at issue " + key
            + " after " + written + " issues were deleted: " + e.getMessage());
      }
      written++;
    }
    return written;
  }
}
