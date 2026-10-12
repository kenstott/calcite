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

import org.apache.calcite.adapter.servicenow.ServiceNowColumn.Kind;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * What an instance says about its own tables and columns, read from the metadata tables with the
 * Table API.
 *
 * <ul>
 *   <li>{@code sys_db_object}: one row per table, with its label, its parent ({@code super_class})
 *   and the package that delivers it ({@code sys_package}), whose name is the table's theme.
 *   <li>{@code sys_dictionary}: one row per column, on the table that declares it. A table's own
 *   rows list only the columns it declares; the rest are on its ancestors' rows, so columns are
 *   resolved by walking {@code super_class} up to the root ({@code incident} extends {@code task}).
 *   <li>{@code sys_glide_object}: the scalar type behind each field type, used for types the
 *   adapter does not know by name.
 * </ul>
 *
 * <p>The table list is one read of {@code sys_db_object} ({@link Snapshot}, what the on-disk cache
 * stores). An instance has thousands of tables and its dictionary over a hundred thousand rows, so
 * a table's columns are read only when that table is resolved: its own and its ancestors'
 * {@code sys_dictionary} rows, nothing else. Columns are never inferred from sample rows: if the
 * user cannot read the metadata tables, the read fails and says which table and what access is
 * missing. A column of an unmapped type fails only the table it is on.
 */
final class ServiceNowCatalog {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Table and field names that may appear in a URL path or a {@code sysparm_fields} list. */
  static final Pattern NAME = Pattern.compile("[A-Za-z0-9_$]+");

  private static final int SNAPSHOT_VERSION = 2;

  /** Theme of a table that no package delivers. */
  static final String NO_PACKAGE = "(no package)";

  /** A row of {@code sys_db_object}. */
  static final class TableRow {
    final String sysId;
    final String name;
    final String label;
    /** sys_id of the parent table's row, or empty for a root table. */
    final String superClassId;
    /** The name of the package that delivers the table; see {@link #theme(String, String)}. */
    final String theme;

    TableRow(String sysId, String name, String label, String superClassId, String theme) {
      this.sysId = sysId;
      this.name = name;
      this.label = label;
      this.superClassId = superClassId;
      this.theme = theme;
    }
  }

  /**
   * A table's theme: the name of the package that delivers it. A table no package delivers is
   * {@link #NO_PACKAGE}. A package whose name is empty is named by its sys_id, one group per
   * package, so that tables of different unnamed packages are not merged (the owner's decision of
   * 2026-10-12).
   */
  static String theme(String packageId, String packageName) {
    if (packageId.isEmpty()) {
      return NO_PACKAGE;
    }
    return packageName.isEmpty() ? "(unnamed package " + packageId + ")" : packageName;
  }

  /** Where a table's column metadata is read from, one table at a time. */
  interface Metadata {
    /** The {@code sys_dictionary} rows of the named tables and of no other. */
    List<DictionaryRow> dictionary(List<String> tables);

    /** Field type name to scalar type name, from {@code sys_glide_object}. */
    Map<String, String> glideTypes();
  }

  /** A row of {@code sys_dictionary}. */
  static final class DictionaryRow {
    final String table;
    /** Column name; empty on the row that describes the table itself. */
    final String element;
    final String internalType;
    final int maxLength;
    final boolean active;

    DictionaryRow(String table, String element, String internalType, int maxLength,
        boolean active) {
      this.table = table;
      this.element = element;
      this.internalType = internalType;
      this.maxLength = maxLength;
      this.active = active;
    }
  }

  /** The instance's table list, as stored in the cache. */
  static final class Snapshot {
    final List<TableRow> tables;

    Snapshot(List<TableRow> tables) {
      this.tables = tables;
    }

    String toJson() {
      final ObjectNode root = MAPPER.createObjectNode();
      root.put("version", SNAPSHOT_VERSION);
      final ArrayNode tableNodes = root.putArray("tables");
      for (TableRow t : tables) {
        tableNodes.addObject().put("sysId", t.sysId).put("name", t.name).put("label", t.label)
            .put("superClassId", t.superClassId).put("theme", t.theme);
      }
      return root.toString();
    }

    static Snapshot fromJson(String json) {
      try {
        final JsonNode root = MAPPER.readTree(json);
        if (root.path("version").asInt(-1) != SNAPSHOT_VERSION) {
          throw new ServiceNowException("Catalog cache has version " + root.path("version")
              + ", expected " + SNAPSHOT_VERSION);
        }
        final List<TableRow> tables = new ArrayList<>();
        for (JsonNode t : array(root, "tables")) {
          tables.add(
              new TableRow(string(t, "sysId"), string(t, "name"), string(t, "label"),
                  string(t, "superClassId"), string(t, "theme")));
        }
        return new Snapshot(tables);
      } catch (IOException e) {
        throw new ServiceNowException("Catalog cache is not valid JSON", e);
      }
    }

    private static JsonNode array(JsonNode node, String field) {
      final JsonNode child = node.path(field);
      if (!child.isArray()) {
        throw new ServiceNowException("Catalog cache has no '" + field + "' array");
      }
      return child;
    }

    private static String string(JsonNode node, String field) {
      final JsonNode child = node.path(field);
      if (!child.isTextual()) {
        throw new ServiceNowException("Catalog cache entry has no string '" + field + "': "
            + node);
      }
      return child.asText();
    }
  }

  /** A table with its resolved columns. */
  static final class TableDef {
    final String name;
    final String label;
    final List<ServiceNowColumn> columns;

    TableDef(String name, String label, List<ServiceNowColumn> columns) {
      this.name = name;
      this.label = label;
      this.columns = Collections.unmodifiableList(columns);
    }
  }

  private final Snapshot snapshot;
  private final Metadata metadata;
  /** Field types whose columns are left out, by explicit configuration. */
  private final Set<String> excludedTypes;
  private final Map<String, TableRow> tablesByName = new TreeMap<>();
  private final Map<String, TableRow> tablesById = new HashMap<>();
  private final Map<String, TableDef> resolved = new ConcurrentHashMap<>();
  private Map<String, String> glideTypes;

  ServiceNowCatalog(Snapshot snapshot, Set<String> excludedTypes, Metadata metadata) {
    this.snapshot = snapshot;
    this.metadata = metadata;
    this.excludedTypes = new HashSet<>();
    for (String type : excludedTypes) {
      this.excludedTypes.add(type.toLowerCase(Locale.ROOT));
    }
    for (TableRow t : snapshot.tables) {
      if (tablesByName.put(t.name, t) != null) {
        throw new ServiceNowException("sys_db_object has two rows named '" + t.name + "'");
      }
      tablesById.put(t.sysId, t);
    }
  }

  Snapshot snapshot() {
    return snapshot;
  }

  /** Names of all tables the instance lists, sorted. */
  List<String> tableNames() {
    return new ArrayList<>(tablesByName.keySet());
  }

  /** Every table the instance lists, sorted by name. Reads no column metadata. */
  List<TableRow> tables() {
    return new ArrayList<>(tablesByName.values());
  }

  boolean hasTable(String name) {
    return tablesByName.containsKey(name);
  }

  /** Returns the table with its columns, resolving it on first use. */
  TableDef table(String name) {
    final TableDef cached = resolved.get(name);
    if (cached != null) {
      return cached;
    }
    final TableDef table = resolve(name);
    resolved.put(name, table);
    return table;
  }

  private TableDef resolve(String name) {
    final TableRow row = tablesByName.get(name);
    if (row == null) {
      throw new ServiceNowException("Table '" + name + "' is not in sys_db_object");
    }
    // The table and its ancestors, nearest first
    final Set<String> seen = new LinkedHashSet<>();
    TableRow current = row;
    while (current != null) {
      if (!seen.add(current.name)) {
        throw new ServiceNowException("Table inheritance of '" + name + "' is circular: " + seen);
      }
      if (current.superClassId.isEmpty()) {
        current = null;
      } else {
        final TableRow parent = tablesById.get(current.superClassId);
        if (parent == null) {
          throw new ServiceNowException("Table '" + current.name + "' extends the sys_db_object "
              + "row " + current.superClassId + ", which is not in the catalog");
        }
        current = parent;
      }
    }
    final Map<String, List<DictionaryRow>> dictionaryByTable = new HashMap<>();
    for (DictionaryRow d : metadata.dictionary(new ArrayList<>(seen))) {
      if (!seen.contains(d.table)) {
        // The request named only these tables; a row of another means the instance ignored the
        // filter, and the columns cannot be told apart
        throw new ServiceNowException("sys_dictionary returned a row of table '" + d.table
            + "' when asked for the rows of " + seen + " only");
      }
      dictionaryByTable.computeIfAbsent(d.table, k -> new ArrayList<>()).add(d);
    }
    // The table's own columns win over an ancestor's of the same name
    final Map<String, DictionaryRow> elements = new TreeMap<>();
    for (String table : seen) {
      for (DictionaryRow d : dictionaryByTable.getOrDefault(table,
          Collections.<DictionaryRow>emptyList())) {
        // The row with an empty element describes the table, not a column
        if (!d.element.isEmpty() && d.active) {
          elements.putIfAbsent(d.element, d);
        }
      }
    }
    if (!elements.containsKey("sys_id")) {
      throw new ServiceNowException("Table '" + name + "' has no sys_id column in sys_dictionary "
          + "(including its ancestors " + seen + "). The adapter pages by sys_id and cannot read "
          + "a table without it.");
    }

    final List<ServiceNowColumn> columns = new ArrayList<>();
    final Set<String> columnNames = new HashSet<>(elements.keySet());
    final DictionaryRow sysId = elements.remove("sys_id");
    columns.add(column(name, sysId));
    for (DictionaryRow d : elements.values()) {
      if (!ServiceNowCatalog.NAME.matcher(d.element).matches()) {
        throw new ServiceNowException("Column name '" + d.element + "' of table '" + name
            + "' cannot be used in a request");
      }
      if (excludedTypes.contains(d.internalType.toLowerCase(Locale.ROOT))) {
        continue;
      }
      final ServiceNowColumn column = column(name, d);
      columns.add(column);
      if (column.kind == Kind.REFERENCE) {
        final ServiceNowColumn display = ServiceNowColumn.displayOf(column);
        if (columnNames.contains(display.name)) {
          throw new ServiceNowException("Table '" + name + "' already has a column named '"
              + display.name + "', which the display value of reference field '" + column.field
              + "' would need");
        }
        columns.add(display);
      }
    }
    return new TableDef(name, row.label, columns);
  }

  /** The field types of {@code sys_glide_object}, read once, when a type is not documented. */
  private synchronized Map<String, String> glideTypes() {
    if (glideTypes == null) {
      glideTypes = metadata.glideTypes();
    }
    return glideTypes;
  }

  private ServiceNowColumn column(String table, DictionaryRow d) {
    Kind kind = GlideTypes.known(d.internalType);
    if (kind == null) {
      final String scalar = glideTypes().get(d.internalType);
      if (scalar != null) {
        kind = GlideTypes.scalar(scalar);
      }
    }
    if (kind == null) {
      throw new ServiceNowException("Column " + table + "." + d.element + " has field type '"
          + d.internalType + "', which the adapter cannot map: it is not a documented type and "
          + (glideTypes().containsKey(d.internalType)
              ? "its sys_glide_object scalar type '" + glideTypes().get(d.internalType)
                  + "' is not one the adapter maps"
              : "sys_glide_object has no row for it")
          + ". Leave the type out explicitly with the excludeColumnTypes operand, or restrict "
          + "the schema to other tables with the tables operand.");
    }
    return ServiceNowColumn.field(d.element, d.internalType, kind, d.maxLength);
  }

  // ---- loading from the instance --------------------------------------------------------

  /**
   * Reads the table list: {@code sys_db_object} only. The package's name comes from the
   * reference's display value, because the integration user need not be able to read
   * {@code sys_package} itself.
   */
  static Snapshot load(ServiceNowConnection connection, int pageSize) {
    final List<TableRow> tables = new ArrayList<>();
    for (JsonNode row : readAll(connection, "sys_db_object", pageSize, true, "",
        "sys_id", "name", "label", "super_class", "sys_package")) {
      final String name = Rows.text(row, "name", "sys_db_object", true);
      if (!NAME.matcher(name).matches()) {
        throw new ServiceNowException("sys_db_object has a table name that cannot be used in "
            + "a request: '" + name + "'");
      }
      tables.add(
          new TableRow(Rows.text(row, "sys_id", "sys_db_object", true), name,
              Rows.text(row, "label", "sys_db_object", false),
              Rows.text(row, "super_class", "sys_db_object", false),
              theme(Rows.text(row, "sys_package", "sys_db_object", false),
                  Rows.displayText(row, "sys_package", "sys_db_object"))));
    }
    return new Snapshot(tables);
  }

  /** Column metadata read from the instance, one table and its ancestors per request. */
  static Metadata metadata(ServiceNowConnection connection, int pageSize) {
    return new Metadata() {
      @Override public List<DictionaryRow> dictionary(List<String> tables) {
        final List<DictionaryRow> dictionary = new ArrayList<>();
        // Table names match NAME, so they cannot break out of the IN list
        for (JsonNode row : readAll(connection, "sys_dictionary", pageSize, false,
            "nameIN" + String.join(",", tables),
            "sys_id", "name", "element", "internal_type", "max_length", "active")) {
          final String element = Rows.text(row, "element", "sys_dictionary", false);
          final String internalType = Rows.text(row, "internal_type", "sys_dictionary", false);
          if (element.isEmpty()) {
            // The row that describes the table itself; it declares no column
            continue;
          }
          if (internalType.isEmpty()) {
            throw new ServiceNowException("sys_dictionary row for "
                + Rows.text(row, "name", "sys_dictionary", true) + "." + element
                + " has an empty internal_type");
          }
          dictionary.add(
              new DictionaryRow(Rows.text(row, "name", "sys_dictionary", true), element,
                  internalType, maxLength(row), activeFlag(row)));
        }
        return dictionary;
      }

      @Override public Map<String, String> glideTypes() {
        final Map<String, String> glideTypes = new LinkedHashMap<>();
        for (JsonNode row : readAll(connection, "sys_glide_object", pageSize, false, "",
            "sys_id", "name", "scalar_type")) {
          glideTypes.put(Rows.text(row, "name", "sys_glide_object", true),
              Rows.text(row, "scalar_type", "sys_glide_object", false));
        }
        return glideTypes;
      }
    };
  }

  private static int maxLength(JsonNode row) {
    final String text = Rows.text(row, "max_length", "sys_dictionary", false);
    if (text.isEmpty()) {
      return 0;
    }
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException e) {
      throw new ServiceNowException("sys_dictionary max_length is not a number: '" + text + "'",
          e);
    }
  }

  private static boolean activeFlag(JsonNode row) {
    final String text = Rows.text(row, "active", "sys_dictionary", false);
    if ("true".equals(text)) {
      return true;
    }
    if ("false".equals(text)) {
      return false;
    }
    throw new ServiceNowException("sys_dictionary active is neither true nor false: '" + text
        + "'");
  }

  private static List<JsonNode> readAll(ServiceNowConnection connection, String table,
      int pageSize, boolean displayValues, String query, String... fields) {
    final List<JsonNode> rows = new ArrayList<>();
    try {
      final TableReader reader =
          new TableReader(connection, table, Arrays.asList(fields), displayValues, pageSize, query);
      while (reader.hasNext()) {
        rows.add(reader.next());
      }
    } catch (ServiceNowException e) {
      if (e.getStatus() == 401 || e.getStatus() == 403) {
        throw new ServiceNowException(e.getStatus(), "The integration user cannot read the "
            + "metadata table " + table + ", which the adapter needs to discover tables and "
            + "columns. Grant it read access to sys_db_object, sys_dictionary and "
            + "sys_glide_object (for example the personalize_dictionary role, or read ACLs on "
            + "those tables and their fields). The adapter does not guess columns from sample "
            + "rows. " + e.getMessage(), e);
      }
      throw e;
    }
    return rows;
  }
}
