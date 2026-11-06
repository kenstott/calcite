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

import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

/**
 * A column of a form table and how its Jira field reads and writes.
 *
 * <p>Values read as Jira shows them to a person: an option as its value, a priority, status,
 * issue type, resolution, component or version as its name, a project as its key, a cascading
 * select as {@code "parent / child"}, rich text (Atlassian Document Format) as plain text. A user
 * reads as its account id; its display name is a read-only field of its own
 * ({@code <id>.displayName}), never written, because a display name is not unique. A multi-value field reads as a JSON array of
 * those values (pgwire has no array type), and is written the same way. A field of a type with no
 * known shape reads as its JSON text and cannot be written.
 */
final class JiraColumn {

  /** How a field's value is shaped. */
  enum Kind {
    /** The issue's key, from the issue itself. */
    KEY(SqlTypeName.VARCHAR),
    /** The issue's numeric id, as text, from the issue itself. */
    ID(SqlTypeName.VARCHAR),
    STRING(SqlTypeName.VARCHAR),
    RICH_TEXT(SqlTypeName.VARCHAR),
    NUMBER(SqlTypeName.DOUBLE),
    DATE(SqlTypeName.DATE),
    DATETIME(SqlTypeName.TIMESTAMP),
    OPTION(SqlTypeName.VARCHAR),
    CASCADING(SqlTypeName.VARCHAR),
    /** An object read and written by its {@code name}: priority, status, resolution, ... */
    NAMED(SqlTypeName.VARCHAR),
    PROJECT(SqlTypeName.VARCHAR),
    USER(SqlTypeName.VARCHAR),
    /** The display name of the user field this column accompanies; read-only. */
    USER_NAME(SqlTypeName.VARCHAR),
    STRING_ARRAY(SqlTypeName.VARCHAR),
    OPTION_ARRAY(SqlTypeName.VARCHAR),
    NAMED_ARRAY(SqlTypeName.VARCHAR),
    USER_ARRAY(SqlTypeName.VARCHAR),
    /** A field whose shape is not known: its JSON text; read-only. */
    JSON(SqlTypeName.VARCHAR);

    final SqlTypeName sqlType;

    Kind(SqlTypeName sqlType) {
      this.sqlType = sqlType;
    }
  }

  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

  /** Jira's datetime text, e.g. {@code 2026-10-16T09:30:12.345+0000}. */
  private static final DateTimeFormatter DATETIME =
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSZ", Locale.ROOT);

  /** The SQL column name. */
  final String name;
  /** The Jira field id ({@code summary}, {@code customfield_10016}); null for key and id. */
  final @Nullable String fieldId;
  final Kind kind;
  /** Whether an INSERT or UPDATE may set it. Status is written by a transition. */
  final boolean writable;

  JiraColumn(String name, @Nullable String fieldId, Kind kind, boolean writable) {
    this.name = name;
    this.fieldId = fieldId;
    this.kind = kind;
    this.writable = writable;
  }

  /**
   * The kind of a field, from its {@code schema} object in Jira's field metadata.
   *
   * @param fieldId the field's id, which names the rich-text system fields
   */
  static Kind kindOf(String fieldId, JsonNode schema) {
    final String type = schema.path("type").asText("");
    final String custom = schema.path("custom").asText("");
    switch (type) {
    case "string":
      return fieldId.equals("description") || fieldId.equals("environment")
          || custom.endsWith(":textarea") ? Kind.RICH_TEXT : Kind.STRING;
    case "number":
      return Kind.NUMBER;
    case "date":
      return Kind.DATE;
    case "datetime":
      return Kind.DATETIME;
    case "option":
      return Kind.OPTION;
    case "option-with-child":
      return Kind.CASCADING;
    case "priority":
    case "status":
    case "issuetype":
    case "resolution":
    case "securitylevel":
    case "component":
    case "version":
      return Kind.NAMED;
    case "project":
      return Kind.PROJECT;
    case "user":
      return Kind.USER;
    case "array":
      switch (schema.path("items").asText("")) {
      case "string":
        return Kind.STRING_ARRAY;
      case "option":
        return Kind.OPTION_ARRAY;
      case "component":
      case "version":
        return Kind.NAMED_ARRAY;
      case "user":
        return Kind.USER_ARRAY;
      default:
        return Kind.JSON;
      }
    default:
      return Kind.JSON;
    }
  }

  /** Whether a column of this kind can be set by an INSERT or UPDATE. */
  static boolean writable(Kind kind) {
    switch (kind) {
    case KEY:
    case ID:
    case PROJECT:
    case USER_NAME:
    case JSON:
      return false;
    default:
      return true;
    }
  }

  /**
   * The SQL column name a Jira field name gets by default: lower case, every run of characters
   * other than letters and digits one underscore, none at either end ({@code Story Points} is
   * {@code story_points}).
   */
  static String columnName(String fieldName) {
    final String snake = fieldName.toLowerCase(Locale.ROOT)
        .replaceAll("[^\\p{L}\\p{N}]+", "_")
        .replaceAll("^_+|_+$", "");
    if (snake.isEmpty()) {
      throw new JiraException("Field name '" + fieldName + "' has no letter or digit to make a "
          + "column name of; give the field a column");
    }
    return snake;
  }

  /**
   * The SQL value of this column in an issue.
   *
   * @param issue          the issue as Jira returns it ({@code key}, {@code id}, {@code fields})
   * @param documentFormat whether rich text is Atlassian Document Format (Cloud) or plain text
   */
  @Nullable Object read(JsonNode issue, boolean documentFormat) {
    if (kind == Kind.KEY) {
      return issue.path("key").asText();
    }
    if (kind == Kind.ID) {
      return issue.path("id").asText();
    }
    final JsonNode value = issue.path("fields").path(fieldId);
    if (value.isMissingNode() || value.isNull()) {
      return null;
    }
    try {
      switch (kind) {
      case STRING:
        return value.asText();
      case RICH_TEXT:
        return documentFormat ? Documents.toText(value) : value.asText();
      case NUMBER:
        if (!value.isNumber()) {
          throw new IllegalArgumentException("expected a number");
        }
        return value.doubleValue();
      case DATE:
        return (int) LocalDate.parse(value.asText()).toEpochDay();
      case DATETIME:
        return OffsetDateTime.parse(value.asText(), DATETIME).toInstant().toEpochMilli();
      case OPTION:
        return required(value, "value");
      case CASCADING:
        final String parent = required(value, "value");
        final JsonNode child = value.path("child");
        return child.isObject() ? parent + " / " + required(child, "value") : parent;
      case NAMED:
        return required(value, "name");
      case PROJECT:
        return required(value, "key");
      case USER:
        return required(value, "accountId");
      case USER_NAME:
        return required(value, "displayName");
      case STRING_ARRAY:
        return value.toString();
      case OPTION_ARRAY:
        return members(value, "value");
      case NAMED_ARRAY:
        return members(value, "name");
      case USER_ARRAY:
        return members(value, "accountId");
      case JSON:
        return value.toString();
      default:
        throw new IllegalStateException("Unhandled column kind " + kind);
      }
    } catch (IllegalArgumentException | DateTimeParseException e) {
      throw new JiraException("Cannot read field " + fieldId + " of issue "
          + issue.path("key").asText() + " as " + kind + ": value " + value + " is not valid: "
          + e.getMessage(), e);
    }
  }

  /**
   * The JSON Jira takes for this column's SQL value, in an issue's {@code fields}. A null is
   * JSON null, which clears the field.
   */
  JsonNode write(@Nullable Object value, boolean documentFormat) {
    if (!writable) {
      throw new JiraException("Column " + name + " cannot be written");
    }
    if (value == null) {
      return NODES.nullNode();
    }
    switch (kind) {
    case STRING:
      return NODES.textNode(value.toString());
    case RICH_TEXT:
      return documentFormat ? Documents.fromText(value.toString())
          : NODES.textNode(value.toString());
    case NUMBER:
      return NODES.numberNode(new BigDecimal(value.toString()));
    case DATE:
      return NODES.textNode(LocalDate.ofEpochDay(((Number) value).longValue()).toString());
    case DATETIME:
      return NODES.textNode(DATETIME.format(
          Instant.ofEpochMilli(((Number) value).longValue()).atOffset(ZoneOffset.UTC)));
    case OPTION:
      return NODES.objectNode().put("value", value.toString());
    case CASCADING:
      final String text = value.toString();
      final int slash = text.indexOf(" / ");
      if (slash < 0) {
        return NODES.objectNode().put("value", text);
      }
      final ObjectNode cascade = NODES.objectNode().put("value", text.substring(0, slash));
      cascade.putObject("child").put("value", text.substring(slash + 3));
      return cascade;
    case NAMED:
      return NODES.objectNode().put("name", value.toString());
    case USER:
      return NODES.objectNode().put("accountId", value.toString());
    case STRING_ARRAY:
      return array(value);
    case OPTION_ARRAY:
      return wrapped(value, "value");
    case NAMED_ARRAY:
      return wrapped(value, "name");
    case USER_ARRAY:
      return wrapped(value, "accountId");
    default:
      throw new JiraException("Column " + name + " cannot be written");
    }
  }

  private static String required(JsonNode value, String member) {
    final JsonNode text = value.path(member);
    if (!text.isTextual()) {
      throw new IllegalArgumentException("expected an object with '" + member + "'");
    }
    return text.asText();
  }

  /** A JSON array of one member of each element. */
  private static String members(JsonNode value, String member) {
    if (!value.isArray()) {
      throw new IllegalArgumentException("expected an array");
    }
    final ArrayNode out = NODES.arrayNode();
    for (JsonNode element : value) {
      out.add(required(element, member));
    }
    return out.toString();
  }

  /** The JSON array text an SQL value of an array column holds, each element text. */
  private ArrayNode array(Object value) {
    final JsonNode parsed;
    try {
      parsed = JiraConnection.MAPPER.readTree(value.toString());
    } catch (JsonProcessingException e) {
      throw new JiraException("Column " + name + " takes a JSON array of text, e.g. "
          + "'[\"a\",\"b\"]': " + value);
    }
    if (!parsed.isArray()) {
      throw new JiraException("Column " + name + " takes a JSON array of text, e.g. "
          + "'[\"a\",\"b\"]': " + value);
    }
    final ArrayNode out = NODES.arrayNode();
    for (JsonNode element : parsed) {
      if (!element.isTextual()) {
        throw new JiraException("Column " + name + " takes a JSON array of text; element "
            + element + " is not text");
      }
      out.add(element.asText());
    }
    return out;
  }

  private ArrayNode wrapped(Object value, String member) {
    final ArrayNode out = NODES.arrayNode();
    for (JsonNode element : array(value)) {
      out.addObject().put(member, element.asText());
    }
    return out;
  }
}
