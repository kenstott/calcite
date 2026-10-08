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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.adapter.sharepoint.auth.SharePointAuth;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * SQL UPDATE and DELETE of a SharePoint list, against a client that keeps its items in memory.
 */
@Tag("unit")
class SharePointSqlModifyTest {

  /** A list of three columns whose items are held in memory. */
  private static class InMemoryClient extends MicrosoftGraphListClient {
    final List<Map<String, Object>> items = new ArrayList<>();
    final List<String> updatedIds = new ArrayList<>();
    final List<Map<String, Object>> updates = new ArrayList<>();
    final List<String> deletedIds = new ArrayList<>();

    InMemoryClient() {
      super("https://update-test.example", new SharePointAuth() {
        @Override public String getAccessToken() {
          return "token";
        }
      });
    }

    void item(String id, String title, double priority, boolean done) {
      Map<String, Object> item = new LinkedHashMap<>();
      item.put("id", id);
      item.put("Title", title);
      item.put("Priority", priority);
      item.put("Done", done);
      items.add(item);
    }

    @Override public List<Map<String, Object>> getListItems(String listId) {
      return items;
    }

    @Override public void deleteListItem(String listId, String itemId) {
      deletedIds.add(itemId);
    }

    @Override public void updateListItem(String listId, String itemId,
        Map<String, Object> fields) {
      updatedIds.add(itemId);
      updates.add(new LinkedHashMap<>(fields));
      for (Map<String, Object> item : items) {
        if (itemId.equals(item.get("id"))) {
          item.putAll(fields);
        }
      }
    }
  }

  private final InMemoryClient client = new InMemoryClient();

  private Connection connect() throws SQLException {
    client.item("1", "first", 1, false);
    client.item("2", "second", 2, false);
    client.item("3", "third", 3, false);
    SharePointListMetadata metadata =
        new SharePointListMetadata("list-id", "Tasks", "TasksList",
            Arrays.asList(new SharePointColumn("Title", "Title", "text", false),
                new SharePointColumn("Priority", "Priority", "number", false),
                new SharePointColumn("Done", "Done", "boolean", false)));
    final Table table = new SharePointListTable(metadata, client);
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    connection.unwrap(CalciteConnection.class).getRootSchema().add("sharepoint",
        new AbstractSchema() {
          @Override protected Map<String, Table> getTableMap() {
            return Collections.singletonMap("tasks", table);
          }
        });
    return connection;
  }

  @Test void updateWritesOnlyTheColumnsSetToTheRowsMatched() throws SQLException {
    try (Connection connection = connect();
         PreparedStatement update =
             connection.prepareStatement("UPDATE sharepoint.tasks SET title = ?, done = ?"
                 + " WHERE id = ?")) {
      update.setString(1, "changed");
      update.setBoolean(2, true);
      update.setString(3, "2");
      assertEquals(1, update.executeUpdate());
    }
    assertEquals(Collections.singletonList("2"), client.updatedIds);
    Map<String, Object> expected = new LinkedHashMap<>();
    expected.put("Title", "changed");
    expected.put("Done", true);
    assertEquals(Collections.singletonList(expected), client.updates);
  }

  @Test void updateOfSeveralRowsReportsTheirCount() throws SQLException {
    try (Connection connection = connect();
         Statement statement = connection.createStatement()) {
      assertEquals(2,
          statement.executeUpdate("UPDATE sharepoint.tasks SET priority = 9"
              + " WHERE priority >= 2"));
    }
    assertEquals(Arrays.asList("2", "3"), client.updatedIds);
    assertEquals(9.0d, ((Number) client.updates.get(0).get("Priority")).doubleValue());
  }

  @Test void theIdCannotBeUpdated() throws SQLException {
    try (Connection connection = connect();
         Statement statement = connection.createStatement()) {
      assertThrows(SQLException.class,
          () -> statement.executeUpdate("UPDATE sharepoint.tasks SET id = 'x' WHERE id = '1'"));
    }
    assertEquals(Collections.emptyList(), client.updatedIds);
  }

  @Test void deleteRemovesTheRowsMatchedAndReportsTheirCount() throws SQLException {
    try (Connection connection = connect();
         PreparedStatement delete =
             connection.prepareStatement("DELETE FROM sharepoint.tasks WHERE id = ?")) {
      delete.setString(1, "2");
      assertEquals(1, delete.executeUpdate());
    }
    assertEquals(Collections.singletonList("2"), client.deletedIds);
  }

  @Test void deleteOfSeveralRows() throws SQLException {
    try (Connection connection = connect();
         Statement statement = connection.createStatement()) {
      assertEquals(2,
          statement.executeUpdate("DELETE FROM sharepoint.tasks WHERE priority >= 2"));
      assertEquals(0,
          statement.executeUpdate("DELETE FROM sharepoint.tasks WHERE priority > 100"));
    }
    assertEquals(Arrays.asList("2", "3"), client.deletedIds);
  }
}
