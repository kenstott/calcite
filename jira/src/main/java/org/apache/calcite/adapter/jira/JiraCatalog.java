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

import org.apache.calcite.adapter.jira.JiraColumn.Kind;

import com.fasterxml.jackson.databind.JsonNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The site's projects, issue types and fields, read from its REST metadata, and the forms
 * resolved against them.
 *
 * <p>A form is one project and one issue type, with the fields chosen for it. Its table has the
 * fixed columns {@code key}, {@code id}, {@code summary}, {@code status}, {@code created} and
 * {@code updated}, then one column per chosen field. A user field's display name is a field of its
 * own, {@code <user field id>.displayName}, read-only
 * field). A field is named by its id or by its name; it is looked up among the issue type's
 * create-screen fields first, then among all the site's fields (status, resolution and the like
 * are on no create screen). Two fields that give the same column, or a field whose column is a
 * fixed one, are refused naming both, never renamed.
 */
final class JiraCatalog {

  /** A form as the {@code forms} operand names it. */
  static final class FormSpec {
    final String name;
    final String project;
    final String issueType;
    final List<FieldSpec> fields;

    FormSpec(String name, String project, String issueType, List<FieldSpec> fields) {
      this.name = name;
      this.project = project;
      this.issueType = issueType;
      this.fields = fields;
    }
  }

  /** One chosen field: its id or name, and the column it is served as (null: the default). */
  static final class FieldSpec {
    final String field;
    final @Nullable String column;

    FieldSpec(String field, @Nullable String column) {
      this.field = field;
      this.column = column;
    }
  }

  /** A form resolved against the site. */
  static final class Form {
    final String name;
    final String projectKey;
    final String issueTypeId;
    final String issueTypeName;
    final List<JiraColumn> columns;

    Form(String name, String projectKey, String issueTypeId, String issueTypeName,
        List<JiraColumn> columns) {
      this.name = name;
      this.projectKey = projectKey;
      this.issueTypeId = issueTypeId;
      this.issueTypeName = issueTypeName;
      this.columns = columns;
    }

    int columnIndex(String column) {
      for (int i = 0; i < columns.size(); i++) {
        if (columns.get(i).name.equals(column)) {
          return i;
        }
      }
      throw new IllegalStateException("Form " + name + " has no column " + column);
    }

    /** The JQL that selects this form's issues. */
    String jql() {
      return "project = " + Jql.quote(projectKey) + " AND issuetype = " + issueTypeId;
    }
  }

  /** A project's issue type, from its create metadata. */
  static final class IssueType {
    final String id;
    final String name;
    final boolean subtask;

    IssueType(String id, String name, boolean subtask) {
      this.id = id;
      this.name = name;
      this.subtask = subtask;
    }
  }

  /** A field as metadata describes it. */
  static final class FieldMeta {
    final String id;
    final String name;
    final JsonNode schema;
    final boolean required;

    FieldMeta(String id, String name, JsonNode schema, boolean required) {
      this.id = id;
      this.name = name;
      this.schema = schema;
      this.required = required;
    }

    Kind kind() {
      return JiraColumn.kindOf(id, schema);
    }
  }

  /** The fixed columns every form table starts with. */
  static List<JiraColumn> fixedColumns() {
    final List<JiraColumn> columns = new ArrayList<>();
    columns.add(new JiraColumn("key", null, Kind.KEY, false));
    columns.add(new JiraColumn("id", null, Kind.ID, false));
    columns.add(new JiraColumn("summary", "summary", Kind.STRING, true));
    columns.add(new JiraColumn("status", "status", Kind.NAMED, false));
    columns.add(new JiraColumn("created", "created", Kind.DATETIME, false));
    columns.add(new JiraColumn("updated", "updated", Kind.DATETIME, false));
    return columns;
  }

  private static final int METADATA_PAGE = 50;

  /** Suffix of the field id that names a user field's display name. */
  static final String DISPLAY_NAME = ".displayName";

  private final JiraConnection connection;
  private @Nullable List<FieldMeta> siteFields;

  JiraCatalog(JiraConnection connection) {
    this.connection = connection;
  }

  /** Every project the account can see: key to name, in the site's order. */
  Map<String, String> projects() {
    final Map<String, String> projects = new LinkedHashMap<>();
    for (int startAt = 0;;) {
      final JsonNode page = connection.get("/project/search",
          Map.of("startAt", Integer.toString(startAt), "maxResults",
              Integer.toString(METADATA_PAGE)));
      final JsonNode values = page.path("values");
      for (JsonNode project : values) {
        projects.put(project.path("key").asText(), project.path("name").asText());
      }
      startAt += values.size();
      if (values.size() == 0 || lastPage(page, startAt, "/project/search")) {
        return projects;
      }
    }
  }

  /** The issue types the account can create in a project. */
  List<IssueType> issueTypes(String projectKey) {
    final List<IssueType> types = new ArrayList<>();
    for (JsonNode type
        : pages("/issue/createmeta/" + JiraConnection.encode(projectKey) + "/issuetypes",
            "issueTypes")) {
      types.add(new IssueType(type.path("id").asText(), type.path("name").asText(),
          type.path("subtask").asBoolean(false)));
    }
    return types;
  }

  /** The issue type of a project named by id or by name (ignoring case). */
  IssueType issueType(String projectKey, String idOrName) {
    final List<IssueType> types = issueTypes(projectKey);
    for (IssueType type : types) {
      if (type.id.equals(idOrName)) {
        return type;
      }
    }
    for (IssueType type : types) {
      if (type.name.equalsIgnoreCase(idOrName)) {
        return type;
      }
    }
    final List<String> names = new ArrayList<>();
    for (IssueType type : types) {
      names.add(type.name);
    }
    throw new JiraException("Project " + projectKey + " has no issue type '" + idOrName
        + "' the account can create; it has " + names);
  }

  /** The fields of an issue type's create screen, in the site's order. */
  List<FieldMeta> createFields(String projectKey, String issueTypeId) {
    final List<FieldMeta> fields = new ArrayList<>();
    for (JsonNode field : pages("/issue/createmeta/" + JiraConnection.encode(projectKey)
        + "/issuetypes/" + JiraConnection.encode(issueTypeId), "fields")) {
      fields.add(new FieldMeta(field.path("fieldId").asText(), field.path("name").asText(),
          field.path("schema"), field.path("required").asBoolean(false)));
    }
    return fields;
  }

  /** Every field of the site, read once. */
  synchronized List<FieldMeta> siteFields() {
    if (siteFields == null) {
      final List<FieldMeta> fields = new ArrayList<>();
      for (JsonNode field : connection.get("/field", Map.of())) {
        fields.add(new FieldMeta(field.path("id").asText(), field.path("name").asText(),
            field.path("schema"), false));
      }
      siteFields = fields;
    }
    return siteFields;
  }

  /** Resolves a form: its project, its issue type and the columns of its fields. */
  Form resolve(FormSpec spec) {
    final String projectKey = spec.project.toUpperCase(Locale.ROOT);
    final IssueType type = issueType(projectKey, spec.issueType);
    final List<JiraColumn> columns = fixedColumns();
    // Column name to the field id that gave it, for naming both sides of a collision
    final Map<String, String> owners = new LinkedHashMap<>();
    for (JiraColumn column : columns) {
      owners.put(column.name, "the fixed column " + column.name);
    }
    final List<FieldMeta> createFields = spec.fields.isEmpty() ? new ArrayList<>()
        : createFields(projectKey, type.id);
    for (FieldSpec fieldSpec : spec.fields) {
      final boolean displayName = fieldSpec.field.endsWith(DISPLAY_NAME);
      final FieldMeta field = field(spec, createFields, displayName
          ? fieldSpec.field.substring(0, fieldSpec.field.length() - DISPLAY_NAME.length())
          : fieldSpec.field);
      final Kind kind;
      final String column;
      if (displayName) {
        if (field.kind() != Kind.USER) {
          throw new JiraException("Form '" + spec.name + "': '" + fieldSpec.field + "' names "
              + "the display name of " + field.id + ", which is not a user field");
        }
        kind = Kind.USER_NAME;
        column = fieldSpec.column != null ? fieldSpec.column
            : JiraColumn.columnName(field.name) + "_name";
      } else {
        kind = field.kind();
        column = fieldSpec.column != null ? fieldSpec.column
            : JiraColumn.columnName(field.name);
      }
      claim(spec, owners, column, fieldSpec.field);
      columns.add(new JiraColumn(column, field.id, kind, JiraColumn.writable(kind)));
    }
    return new Form(spec.name, projectKey, type.id, type.name, columns);
  }

  private static void claim(FormSpec spec, Map<String, String> owners, String column,
      String fieldId) {
    final String owner = owners.putIfAbsent(column, "field " + fieldId);
    if (owner != null) {
      throw new JiraException("Form '" + spec.name + "': field " + fieldId + " and " + owner
          + " both give the column '" + column + "'; give one of them another column");
    }
  }

  /** The field a form names, by id, then by name among the create fields, then the site's. */
  private FieldMeta field(FormSpec spec, List<FieldMeta> createFields, String idOrName) {
    for (List<FieldMeta> fields : List.of(createFields, siteFields())) {
      for (FieldMeta field : fields) {
        if (field.id.equals(idOrName)) {
          return field;
        }
      }
    }
    for (List<FieldMeta> fields : List.of(createFields, siteFields())) {
      final List<FieldMeta> named = new ArrayList<>();
      for (FieldMeta field : fields) {
        if (field.name.equalsIgnoreCase(idOrName)) {
          named.add(field);
        }
      }
      if (named.size() == 1) {
        return named.get(0);
      }
      if (named.size() > 1) {
        final List<String> ids = new ArrayList<>();
        for (FieldMeta field : named) {
          ids.add(field.id);
        }
        throw new JiraException("Form '" + spec.name + "': field name '" + idOrName
            + "' is the name of " + ids + "; name the field by its id");
      }
    }
    throw new JiraException("Form '" + spec.name + "': '" + idOrName + "' is not a field of "
        + "the site (by id or name)");
  }

  /** Whether a {@code startAt}-paged answer is the last page: by its total, or isLast. */
  static boolean lastPage(JsonNode page, int next, String path) {
    if (page.path("total").isNumber()) {
      return next >= page.path("total").asInt();
    }
    if (page.path("isLast").isBoolean()) {
      return page.path("isLast").asBoolean();
    }
    throw new JiraException("GET " + path + " answered a page with neither 'total' nor "
        + "'isLast', so the adapter cannot tell whether more follow");
  }

  /** Every element of a {@code startAt}-paged metadata list held under {@code member}. */
  private List<JsonNode> pages(String path, String member) {
    final List<JsonNode> all = new ArrayList<>();
    for (int startAt = 0;;) {
      final JsonNode page = connection.get(path,
          Map.of("startAt", Integer.toString(startAt), "maxResults",
              Integer.toString(METADATA_PAGE)));
      final JsonNode values = page.path(member);
      if (!values.isArray()) {
        final List<String> members = new ArrayList<>();
        page.fieldNames().forEachRemaining(members::add);
        throw new JiraException("GET " + path + " answered without a '" + member
            + "' list; it has " + members);
      }
      values.forEach(all::add);
      startAt += values.size();
      if (values.size() == 0 || lastPage(page, startAt, path)) {
        return all;
      }
    }
  }
}
