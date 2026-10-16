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

import org.apache.calcite.adapter.jira.JiraCatalog.FieldSpec;
import org.apache.calcite.adapter.jira.JiraCatalog.FormSpec;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.InformationSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Factory for Jira Cloud schemas.
 *
 * <p>Operands ({@code baseUrl}, {@code email} and {@code apiToken} are required):
 * <ul>
 *   <li>{@code baseUrl}: the site, {@code https://<site>.atlassian.net}. Plain http is accepted
 *   only for a loopback host.
 *   <li>{@code email}, {@code apiToken}: the account and its API token (basic auth). They are
 *   checked with {@code GET /rest/api/3/myself} when the schema is created.
 *   <li>{@code forms}: the forms served, a list (or its JSON text) of
 *   {@code {"name", "project", "issueType", "fields": [{"field", "column"}]}}. {@code project}
 *   is a project key, {@code issueType} an issue type's name or id, {@code field} a field's id
 *   or name, {@code column} the column it is served as (default: its name snake-cased). With no
 *   fields a form has the fixed columns only. See {@link JiraCatalog}.
 *   <li>{@code pageSize}: issues per search request; default 100.
 *   <li>{@code maxConcurrentRequests}: default 4.
 *   <li>{@code maxRetries}: retries of a 429 response; default 3.
 *   <li>{@code maxRetryWaitSeconds}: total wait across retries; default 60.
 *   <li>{@code requestTimeoutSeconds}: default 60.
 *   <li>{@code retrySleeper}: what pauses between retries, a {@code JiraConnection.Sleeper};
 *   Java callers only (tests). Default: {@link Thread#sleep(long)}.
 * </ul>
 * Numbers may be given as numbers or as text, because the JDBC driver passes URL text.
 */
public class JiraSchemaFactory implements SchemaFactory {

  public static final JiraSchemaFactory INSTANCE = new JiraSchemaFactory();

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    final URI baseUrl = baseUrl(required(operand, "baseUrl"));
    final JiraFlavor flavor =
        JiraFlavor.cloud(required(operand, "email"), required(operand, "apiToken"));
    final JiraConnection connection = new JiraConnection(baseUrl, flavor,
        integer(operand, "maxConcurrentRequests", 4, 1),
        integer(operand, "maxRetries", 3, 0),
        Duration.ofSeconds(integer(operand, "maxRetryWaitSeconds", 60, 0)),
        Duration.ofSeconds(integer(operand, "requestTimeoutSeconds", 60, 1)),
        sleeper(operand.get("retrySleeper")));
    final List<FormSpec> forms = forms(operand.get("forms"));
    connection.myself();
    registerInformationSchema(parentSchema);
    return new JiraSchema(connection, integer(operand, "pageSize", 100, 1), forms);
  }

  /** Gives the connection's root an {@code information_schema} unless it has one (#518). */
  private static void registerInformationSchema(SchemaPlus parentSchema) {
    SchemaPlus root = parentSchema;
    while (root.getParentSchema() != null) {
      root = root.getParentSchema();
    }
    if (root.subSchemas().get("information_schema") == null) {
      root.add("information_schema", new InformationSchema(root, "CALCITE"));
    }
  }

  private static JiraConnection.Sleeper sleeper(@Nullable Object value) {
    if (value == null) {
      return Thread::sleep;
    }
    if (!(value instanceof JiraConnection.Sleeper)) {
      throw new IllegalArgumentException("'retrySleeper' must be a JiraConnection.Sleeper: "
          + value.getClass());
    }
    return (JiraConnection.Sleeper) value;
  }

  static List<FormSpec> forms(@Nullable Object value) {
    final List<FormSpec> forms = new ArrayList<>();
    if (value == null) {
      return forms;
    }
    final Object parsed;
    if (value instanceof String) {
      try {
        parsed = JiraConnection.MAPPER.readValue((String) value,
            new TypeReference<List<Object>>() { });
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("'forms' is not a JSON list: "
            + e.getOriginalMessage(), e);
      }
    } else {
      parsed = value;
    }
    if (!(parsed instanceof List)) {
      throw new IllegalArgumentException("'forms' must be a list of forms");
    }
    for (Object item : (List<?>) parsed) {
      if (!(item instanceof Map)) {
        throw new IllegalArgumentException("Each entry of 'forms' must be an object: " + item);
      }
      final Map<?, ?> form = (Map<?, ?>) item;
      final String formName = text(form, "name", "a form");
      final List<FieldSpec> fields = new ArrayList<>();
      final Object fieldList = form.get("fields");
      if (fieldList != null) {
        if (!(fieldList instanceof List)) {
          throw new IllegalArgumentException("'fields' of form '" + formName + "' must be a "
              + "list");
        }
        for (Object field : (List<?>) fieldList) {
          if (!(field instanceof Map)) {
            throw new IllegalArgumentException("Each field of form '" + formName
                + "' must be an object with 'field': " + field);
          }
          final Map<?, ?> entry = (Map<?, ?>) field;
          final Object column = entry.get("column");
          fields.add(new FieldSpec(text(entry, "field", "a field of form '" + formName + "'"),
              column == null ? null : column.toString()));
        }
      }
      forms.add(new FormSpec(formName, text(form, "project", "form '" + formName + "'"),
          text(form, "issueType", "form '" + formName + "'"), fields));
    }
    return forms;
  }

  private static String text(Map<?, ?> map, String key, String what) {
    final Object value = map.get(key);
    if (value == null || value.toString().trim().isEmpty()) {
      throw new IllegalArgumentException("'" + key + "' is required in " + what);
    }
    return value.toString().trim();
  }

  private static URI baseUrl(String text) {
    final URI uri;
    try {
      uri = URI.create(text);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("'baseUrl' is not a valid URL: " + text, e);
    }
    final String host = uri.getHost();
    if (host == null) {
      throw new IllegalArgumentException("'baseUrl' has no host: " + text);
    }
    final boolean loopback = host.equals("localhost") || host.equals("127.0.0.1");
    if (!"https".equals(uri.getScheme()) && !("http".equals(uri.getScheme()) && loopback)) {
      throw new IllegalArgumentException(
          "'baseUrl' must be https (http is accepted only for localhost): " + text);
    }
    return uri;
  }

  private static String required(Map<String, Object> operand, String key) {
    final Object value = operand.get(key);
    if (value == null || value.toString().isEmpty()) {
      throw new IllegalArgumentException("Jira operand '" + key + "' is required");
    }
    return value.toString();
  }

  private static int integer(Map<String, Object> operand, String key, int defaultValue,
      int minimum) {
    final Object value = operand.get(key);
    if (value == null) {
      return defaultValue;
    }
    final int number;
    try {
      number = value instanceof Number ? ((Number) value).intValue()
          : Integer.parseInt(value.toString().trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Jira operand '" + key + "' is not a whole number: " + value, e);
    }
    if (number < minimum) {
      throw new IllegalArgumentException(
          "Jira operand '" + key + "' must be at least " + minimum + ": " + number);
    }
    return number;
  }
}
