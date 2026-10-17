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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Reads issues one page at a time, a page only when the one before is used up, so a LIMIT
 * above the scan stops the requests once it has its rows.
 *
 * <p>A search is {@code POST /search/jql}, paged by {@code nextPageToken}. Issues named by key
 * are {@code POST /issue/bulkfetch}, 100 keys a request; a key with no issue (or one the account
 * cannot see) is simply not returned, and an issue of another project or issue type than the
 * forms read is dropped, as a search would not have found it.
 */
final class IssueReader implements Iterator<JsonNode> {

  /** Keys per bulk fetch, Jira's maximum. */
  static final int FETCH_CHUNK = 100;

  private final JiraConnection connection;
  private final List<Form> forms;
  private final List<String> fields;
  private final int pageSize;
  private final @Nullable String jql;
  private final @Nullable Deque<String> keys;
  private final Deque<JsonNode> page = new ArrayDeque<>();
  private @Nullable String nextPageToken;
  private boolean done;

  private IssueReader(JiraConnection connection, List<Form> forms, Collection<String> fields,
      int pageSize, @Nullable String jql, @Nullable Collection<String> keys) {
    this.connection = connection;
    this.forms = forms;
    this.fields = new ArrayList<>(fields);
    if (keys != null) {
      this.fields.add("project");
      this.fields.add("issuetype");
    }
    if (this.fields.isEmpty()) {
      // A search with no field returns ids only; the key is wanted, which any field brings
      this.fields.add("summary");
    }
    this.pageSize = pageSize;
    this.jql = jql;
    this.keys = keys == null ? null : new ArrayDeque<>(keys);
  }

  /** The issues of the forms that a JQL search over them, narrowed by {@code clauses}, finds. */
  static IssueReader search(JiraConnection connection, List<Form> forms,
      Collection<String> fields, int pageSize, List<String> clauses) {
    final StringBuilder jql = new StringBuilder();
    for (Form form : forms) {
      jql.append(jql.length() == 0 ? "(" : " OR (").append(form.jql()).append(')');
    }
    if (forms.size() > 1) {
      jql.insert(0, '(').append(')');
    }
    for (String clause : clauses) {
      jql.append(" AND ").append(clause);
    }
    return new IssueReader(connection, forms, fields, pageSize, jql.toString(), null);
  }

  /** The issues with these keys that belong to one of the forms. */
  static IssueReader fetch(JiraConnection connection, List<Form> forms,
      Collection<String> fields, Collection<String> keys) {
    return new IssueReader(connection, forms, fields, FETCH_CHUNK, null, keys);
  }

  @Override public boolean hasNext() {
    while (page.isEmpty() && !done) {
      if (keys != null) {
        fetchNext();
      } else {
        searchNext();
      }
    }
    return !page.isEmpty();
  }

  @Override public JsonNode next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return page.removeFirst();
  }

  private void searchNext() {
    final ObjectNode body = JiraConnection.MAPPER.createObjectNode();
    body.put("jql", jql);
    body.put("maxResults", pageSize);
    final ArrayNode requested = body.putArray("fields");
    fields.forEach(requested::add);
    if (nextPageToken != null) {
      body.put("nextPageToken", nextPageToken);
    }
    final JsonNode answer = connection.post("/search/jql", body);
    final JsonNode issues = answer.path("issues");
    if (!issues.isArray()) {
      throw new JiraException("POST /search/jql answered without an 'issues' list");
    }
    issues.forEach(page::add);
    final JsonNode token = answer.path("nextPageToken");
    if (token.isTextual() && !answer.path("isLast").asBoolean(false)) {
      nextPageToken = token.asText();
    } else {
      done = true;
    }
  }

  private void fetchNext() {
    if (keys.isEmpty()) {
      done = true;
      return;
    }
    final ObjectNode body = JiraConnection.MAPPER.createObjectNode();
    final ArrayNode requested = body.putArray("fields");
    fields.forEach(requested::add);
    final ArrayNode chunk = body.putArray("issueIdsOrKeys");
    while (!keys.isEmpty() && chunk.size() < FETCH_CHUNK) {
      chunk.add(keys.removeFirst());
    }
    final JsonNode issues = connection.post("/issue/bulkfetch", body).path("issues");
    for (JsonNode issue : issues) {
      if (inForms(issue)) {
        page.add(issue);
      }
    }
  }

  private boolean inForms(JsonNode issue) {
    final String project = issue.path("fields").path("project").path("key").asText();
    final String type = issue.path("fields").path("issuetype").path("id").asText();
    for (Form form : forms) {
      if (form.projectKey.equals(project) && form.issueTypeId.equals(type)) {
        return true;
      }
    }
    return false;
  }
}
