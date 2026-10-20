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
package org.apache.calcite.adapter.servicenow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes rows of one ServiceNow table, one record per request.
 *
 * <ul>
 *   <li>INSERT on a table bound to an import set: one POST per row to the Import Set API, its
 *   columns sent under the staging fields of the bound transform map (see {@link ImportBinding}).
 *   The instance transforms the row at once; a result of {@code inserted} or {@code updated} (a
 *   coalesce matched an existing record) is a written row, {@code ignored} or {@code skipped}
 *   writes nothing and is logged, and {@code error} is a refusal naming the row.
 *   <li>INSERT on any other table: one POST per row to the Table API.
 *   <li>UPDATE and DELETE: one PATCH or DELETE per row, by the sys_id of the row the read path
 *   found.
 * </ul>
 *
 * <p>Columns whose name starts with {@code sys_} are the instance's own and are never written; a
 * reference field is written through its sys_id column, and its {@code __display} column is never
 * written. There are no transactions: a statement that fails part way leaves the rows written
 * before the failing one written, and the error names the failing row and how many were written.
 * A refusal by the instance (HTTP 403) names the table.
 */
final class ServiceNowWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceNowWriter.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final ServiceNowConnection connection;
  private final String table;
  private final List<ServiceNowColumn> columns;
  private final ImportBinding binding;

  /**
   * Creates a writer.
   *
   * @param columns the table's columns, in row order
   * @param binding the import set INSERTs go through, or null to insert directly
   */
  ServiceNowWriter(ServiceNowConnection connection, String table, List<ServiceNowColumn> columns,
      ImportBinding binding) {
    this.connection = connection;
    this.table = table;
    this.columns = columns;
    this.binding = binding;
  }

  /**
   * Inserts the rows, each holding one value per column (NULL for a column the statement leaves
   * out, which ServiceNow then defaults).
   *
   * @param keys receives the sys_id of each record written, in order
   * @return the number of records written
   */
  long insert(List<Object[]> rows, List<String> keys) {
    long written = 0;
    for (int i = 0; i < rows.size(); i++) {
      final Object[] row = rows.get(i);
      final ObjectNode fields = MAPPER.createObjectNode();
      try {
        for (int c = 0; c < columns.size(); c++) {
          if (row[c] == null) {
            continue;
          }
          final ServiceNowColumn column = writable(columns.get(c));
          final String field = binding == null ? column.field : binding.sourceField(column.field);
          fields.put(field, ValueConverter.toText(column, row[c], table));
        }
        if (binding == null) {
          keys.add(sysId(connection.insertRecord(table, fields).body.path("result"), i));
          written++;
        } else {
          final String key = imported(connection.importRow(binding.stagingTable, fields), i);
          if (key != null) {
            keys.add(key);
            written++;
          }
        }
      } catch (ServiceNowException e) {
        throw failed("INSERT", i, null, written, e);
      }
    }
    return written;
  }

  /**
   * Updates the rows, each holding its sys_id followed by one new value per update column.
   *
   * @return the number of records written
   */
  long update(List<Object[]> rows, List<String> updateColumns) {
    final Map<String, ServiceNowColumn> byName = new HashMap<>();
    for (ServiceNowColumn column : columns) {
      byName.put(column.name, column);
    }
    long written = 0;
    for (int i = 0; i < rows.size(); i++) {
      final Object[] row = rows.get(i);
      final String sysId = (String) row[0];
      try {
        final ObjectNode fields = MAPPER.createObjectNode();
        for (int u = 0; u < updateColumns.size(); u++) {
          final ServiceNowColumn column = writable(byName.get(updateColumns.get(u)));
          fields.put(column.field,
              ValueConverter.toText(column, row[1 + u], table));
        }
        connection.updateRecord(table, sysId, fields);
        written++;
      } catch (ServiceNowException e) {
        throw failed("UPDATE", i, sysId, written, e);
      }
    }
    return written;
  }

  /**
   * Deletes the rows, each holding its sys_id.
   *
   * @return the number of records deleted
   */
  long delete(List<Object[]> rows) {
    long written = 0;
    for (int i = 0; i < rows.size(); i++) {
      final String sysId = (String) rows.get(i)[0];
      try {
        connection.deleteRecord(table, sysId);
        written++;
      } catch (ServiceNowException e) {
        throw failed("DELETE", i, sysId, written, e);
      }
    }
    return written;
  }

  private ServiceNowColumn writable(ServiceNowColumn column) {
    if (column.display) {
      throw new ServiceNowException(table + "." + column.name + " is the display value of the "
          + "reference field " + column.field + " and cannot be written; write the referenced "
          + "record's sys_id to " + column.field);
    }
    if (column.field.startsWith("sys_")) {
      throw new ServiceNowException(table + "." + column.name + " is a system field and cannot "
          + "be written");
    }
    return column;
  }

  private String sysId(JsonNode record, int row) {
    final JsonNode sysId = record.path("sys_id");
    if (!sysId.isTextual() || sysId.asText().isEmpty()) {
      throw new ServiceNowException("The response to row " + (row + 1) + " has no sys_id: "
          + abbreviate(record));
    }
    return sysId.asText();
  }

  /**
   * The sys_id of the record an imported row reached, or null if the map ignored or skipped it.
   */
  private String imported(ServiceNowConnection.Response response, int row) {
    JsonNode mine = null;
    for (JsonNode result : response.body.path("result")) {
      if (table.equals(result.path("table").asText())
          && binding.mapName.equals(result.path("transform_map").asText())) {
        if (mine != null) {
          throw new ServiceNowException("The import of row " + (row + 1) + " reports two "
              + "results of transform map '" + binding.mapName + "': "
              + abbreviate(response.body));
        }
        mine = result;
      }
    }
    if (mine == null) {
      throw new ServiceNowException("The import of row " + (row + 1) + " through "
          + binding.stagingTable + " reports no result of transform map '" + binding.mapName
          + "' on " + table + ": " + abbreviate(response.body));
    }
    final String status = mine.path("status").asText();
    switch (status) {
    case "inserted":
    case "updated":
      return sysId(mine, row);
    case "ignored":
    case "skipped":
      LOGGER.warn("Row {} imported through {} was {} by transform map '{}'; {} was not written",
          row + 1, binding.stagingTable, status, binding.mapName, table);
      return null;
    case "error":
      throw new ServiceNowException("Transform map '" + binding.mapName + "' refused row "
          + (row + 1) + ": " + abbreviate(mine));
    default:
      throw new ServiceNowException("The import of row " + (row + 1) + " has the status '"
          + status + "', which the adapter does not know: " + abbreviate(mine));
    }
  }

  private ServiceNowException failed(String operation, int row, String sysId, long written,
      ServiceNowException e) {
    return new ServiceNowException(e.getStatus(), operation + " on " + table + " failed at row "
        + (row + 1) + (sysId == null ? "" : " (sys_id " + sysId + ")") + "; " + written
        + " row(s) before it were written and stay written (ServiceNow has no transactions). "
        + e.getMessage(), e);
  }

  private static String abbreviate(JsonNode node) {
    final String text = node.toString();
    return text.length() <= 400 ? text : text.substring(0, 400) + "...";
  }
}
