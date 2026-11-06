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
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@value #NAME}{@code (link_id, link_type, outward_key, inward_key)}: the links between issues,
 * each once, where either end is an issue of a served form.
 *
 * <p>{@code outward_key} and {@code inward_key} are the link's {@code outwardIssue} and
 * {@code inwardIssue}, as {@code GET /issueLink/<id>} and {@code POST /issueLink} name them;
 * {@code link_type} is the link type's name ({@code Blocks}, {@code Relates}).
 *
 * <p>Read from the {@code issuelinks} field of the forms' issues. A condition
 * {@code outward_key = '<key>'} or {@code inward_key = '<key>'} (or several ORed, or IN) fetches
 * just those issues. INSERT creates a link ({@code link_id} is assigned, so it must be NULL);
 * DELETE removes one; UPDATE is refused, as Jira cannot change a link in place.
 */
public class IssueLinksTable extends AbstractJiraTable {

  static final String NAME = "issue_links";

  private static final int LINK_ID = 0;
  private static final int LINK_TYPE = 1;
  private static final int OUTWARD_KEY = 2;
  private static final int INWARD_KEY = 3;

  private final JiraSchema schema;

  IssueLinksTable(JiraSchema schema) {
    this.schema = schema;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("link_id", SqlTypeName.VARCHAR).nullable(true)
        .add("link_type", SqlTypeName.VARCHAR).nullable(true)
        .add("outward_key", SqlTypeName.VARCHAR).nullable(true)
        .add("inward_key", SqlTypeName.VARCHAR).nullable(true)
        .build();
  }

  @Override Enumerable<Object[]> read(List<RexNode> filters, int @Nullable [] projects,
      @Nullable AtomicBoolean cancel) {
    final int[] selected = projects != null ? projects : allColumns(4);
    final Set<String> keys = endKeys(filters);
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        final List<Form> forms = schema.forms();
        final List<Object[]> rows = new ArrayList<>();
        if (!forms.isEmpty()) {
          final Iterator<JsonNode> issues = keys != null
              ? IssueReader.fetch(schema.connection(), forms, List.of("issuelinks"), keys)
              : IssueReader.search(schema.connection(), forms, List.of("issuelinks"),
                  schema.pageSize(), List.of());
          final Map<String, Object[]> links = new LinkedHashMap<>();
          while (issues.hasNext()) {
            checkCancel(cancel, NAME);
            final JsonNode issue = issues.next();
            for (JsonNode link : issue.path("fields").path("issuelinks")) {
              links.computeIfAbsent(link.path("id").asText(), id -> row(issue, link));
            }
          }
          for (Object[] link : links.values()) {
            final Object[] row = new Object[selected.length];
            for (int i = 0; i < selected.length; i++) {
              row[i] = link[selected[i]];
            }
            rows.add(row);
          }
        }
        return Linq4j.enumerator(rows);
      }
    };
  }

  /**
   * A link as one of its issues lists it: the entry names the other end, as
   * {@code outwardIssue} when that end is the outward one, so the listing issue is the other.
   */
  private static Object[] row(JsonNode issue, JsonNode link) {
    final String self = issue.path("key").asText();
    final String type = link.path("type").path("name").asText();
    if (link.has("outwardIssue")) {
      return new Object[] {link.path("id").asText(), type,
          link.path("outwardIssue").path("key").asText(), self};
    }
    return new Object[] {link.path("id").asText(), type, self,
        link.path("inwardIssue").path("key").asText()};
  }

  /** The issue keys conditions on outward_key or inward_key pin a scan to, or null. */
  private static @Nullable Set<String> endKeys(List<RexNode> filters) {
    for (RexNode filter : filters) {
      for (RexNode condition : RelOptUtil.conjunctions(filter)) {
        final Set<String> keys = new LinkedHashSet<>();
        boolean pinned = true;
        for (RexNode disjunct : RelOptUtil.disjunctions(condition)) {
          final String key = endKey(disjunct);
          if (key == null) {
            pinned = false;
            break;
          }
          keys.add(key);
        }
        if (pinned && !keys.isEmpty()) {
          return keys;
        }
      }
    }
    return null;
  }

  private static @Nullable String endKey(RexNode condition) {
    if (condition.getKind() != SqlKind.EQUALS) {
      return null;
    }
    final List<RexNode> operands = ((RexCall) condition).getOperands();
    for (int side = 0; side < 2; side++) {
      final RexNode column = operands.get(side);
      final RexNode value = operands.get(1 - side);
      if (column instanceof RexInputRef && value instanceof RexLiteral) {
        final int index = ((RexInputRef) column).getIndex();
        if (index == OUTWARD_KEY || index == INWARD_KEY) {
          return ((RexLiteral) value).getValueAs(String.class);
        }
      }
    }
    return null;
  }

  /** The link id. */
  @Override public String getKeyColumn() {
    return "link_id";
  }

  @Override public Enumerable<Long> modify(String operation, Enumerable<Object[]> rows,
      String updateColumns) {
    final List<Object[]> rowList = rows.toList();
    long written = 0;
    switch (TableModify.Operation.valueOf(operation)) {
    case INSERT:
      final List<String> ids = new ArrayList<>();
      try {
        for (Object[] row : rowList) {
          ids.add(create(row));
          written++;
        }
      } finally {
        recordInserted(ids);
      }
      break;
    case DELETE:
      for (Object[] row : rowList) {
        try {
          schema.connection().delete(
              "/issueLink/" + JiraConnection.encode((String) row[LINK_ID]));
        } catch (JiraException e) {
          throw new JiraException(e.status, "DELETE from " + NAME + " failed at link "
              + row[LINK_ID] + " after " + written + " links were deleted: " + e.getMessage());
        }
        written++;
      }
      break;
    case UPDATE:
      throw new JiraException("UPDATE of " + NAME + " is not supported: Jira cannot change a "
          + "link in place. DELETE it and INSERT the new one (columns "
          + Arrays.asList(updateColumns.split(",")) + ")");
    default:
      throw new UnsupportedOperationException(operation + " is not supported on " + NAME);
    }
    return Linq4j.singletonEnumerable(written);
  }

  /** Creates a link and returns its id, read back from its outward issue's links. */
  private String create(Object[] row) {
    if (row[LINK_ID] != null) {
      throw new JiraException("INSERT into " + NAME + " sets link_id, which Jira assigns");
    }
    for (int column : new int[] {LINK_TYPE, OUTWARD_KEY, INWARD_KEY}) {
      if (row[column] == null) {
        throw new JiraException("INSERT into " + NAME + " needs link_type, outward_key and "
            + "inward_key");
      }
    }
    final String type = (String) row[LINK_TYPE];
    final String outward = (String) row[OUTWARD_KEY];
    final String inward = (String) row[INWARD_KEY];
    final ObjectNode body = JiraConnection.MAPPER.createObjectNode();
    body.putObject("type").put("name", type);
    body.putObject("outwardIssue").put("key", outward);
    body.putObject("inwardIssue").put("key", inward);
    schema.connection().exchange("POST", "/issueLink", body, 201);
    // The answer carries no id; the outward issue lists the link with its inward end
    final JsonNode links = schema.connection().get("/issue/" + JiraConnection.encode(outward),
        Map.of("fields", "issuelinks")).path("fields").path("issuelinks");
    for (JsonNode link : links) {
      if (link.path("type").path("name").asText().equals(type)
          && link.path("inwardIssue").path("key").asText().equals(inward)) {
        return link.path("id").asText();
      }
    }
    throw new JiraException("Jira accepted the " + type + " link from " + outward + " to "
        + inward + " but " + outward + " does not list it");
  }
}
