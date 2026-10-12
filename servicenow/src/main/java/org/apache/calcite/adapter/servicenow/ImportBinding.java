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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The import set a table's INSERTs go through: its staging table, the transform map from that
 * staging table to the table, and the map's field entries.
 *
 * <p>The staging table has its own field names, so an inserted column is sent under the source
 * field of the map entry whose target is that column. A column the map has no entry for, or
 * whose entry is computed by a script, cannot be written through the map and is refused by name
 * before anything is sent (the owner's decision of 2026-10-12).
 */
final class ImportBinding {

  final String table;
  final String stagingTable;
  /** The transform map's name, as the Import Set API reports it in each result. */
  final String mapName;
  /** Target field (a column of the table) to the staging table's source field. */
  private final Map<String, String> sourceOf;
  /** Target fields whose entry is computed by a script. */
  private final Set<String> scripted;

  private ImportBinding(String table, String stagingTable, String mapName,
      Map<String, String> sourceOf, Set<String> scripted) {
    this.table = table;
    this.stagingTable = stagingTable;
    this.mapName = mapName;
    this.sourceOf = sourceOf;
    this.scripted = scripted;
  }

  /**
   * The staging field a column is sent under.
   *
   * @throws ServiceNowException if the map has no plain entry for the column
   */
  String sourceField(String column) {
    if (scripted.contains(column)) {
      throw new ServiceNowException("Cannot insert " + table + "." + column + " through the "
          + "import set " + stagingTable + ": transform map '" + mapName + "' computes it with "
          + "a script, so no staging field carries it");
    }
    final String source = sourceOf.get(column);
    if (source == null) {
      throw new ServiceNowException("Cannot insert " + table + "." + column + " through the "
          + "import set " + stagingTable + ": transform map '" + mapName + "' has no field "
          + "entry for it. The map writes " + new ArrayList<>(sourceOf.keySet()));
    }
    return source;
  }

  /** Reads the transform map from {@code stagingTable} to {@code table} and its entries. */
  static ImportBinding load(ServiceNowConnection connection, int pageSize, String table,
      String stagingTable) {
    // Both names match ServiceNowCatalog.NAME (checked on the operand), so they cannot break
    // out of the query
    final List<JsonNode> maps = new ArrayList<>();
    for (JsonNode row : ServiceNowCatalog.readAll(connection, "sys_transform_map", pageSize,
        false, "source_table=" + stagingTable + "^target_table=" + table,
        "sys_id", "name", "source_table", "target_table", "active")) {
      if (!stagingTable.equals(Rows.text(row, "source_table", "sys_transform_map", true))
          || !table.equals(Rows.text(row, "target_table", "sys_transform_map", true))) {
        throw new ServiceNowException("sys_transform_map returned a map of "
            + Rows.text(row, "source_table", "sys_transform_map", false) + " -> "
            + Rows.text(row, "target_table", "sys_transform_map", false) + " when asked for "
            + stagingTable + " -> " + table + " only");
      }
      maps.add(row);
    }
    if (maps.isEmpty()) {
      throw new ServiceNowException("No transform map from the import set " + stagingTable
          + " to " + table + " is readable on the instance; the 'importSet' binding of " + table
          + " cannot be used");
    }
    if (maps.size() > 1) {
      final List<String> names = new ArrayList<>();
      for (JsonNode map : maps) {
        names.add(Rows.text(map, "name", "sys_transform_map", false) + " ("
            + Rows.text(map, "sys_id", "sys_transform_map", true) + ")");
      }
      throw new ServiceNowException("The import set " + stagingTable + " has " + maps.size()
          + " transform maps to " + table + ": " + names + ". The binding names a staging "
          + "table, so it cannot choose between them");
    }
    final JsonNode map = maps.get(0);
    final String mapId = Rows.text(map, "sys_id", "sys_transform_map", true);
    final String mapName = Rows.text(map, "name", "sys_transform_map", false);
    if (!"true".equals(Rows.text(map, "active", "sys_transform_map", true))) {
      throw new ServiceNowException("The transform map '" + mapName + "' from " + stagingTable
          + " to " + table + " is not active, so a row imported through it would not reach "
          + table);
    }
    final Map<String, String> sourceOf = new HashMap<>();
    final Set<String> scripted = new HashSet<>();
    for (JsonNode entry : ServiceNowCatalog.readAll(connection, "sys_transform_entry", pageSize,
        false, "map=" + mapId,
        "sys_id", "map", "source_field", "target_field", "use_source_script")) {
      if (!mapId.equals(Rows.text(entry, "map", "sys_transform_entry", true))) {
        throw new ServiceNowException("sys_transform_entry returned an entry of map "
            + Rows.text(entry, "map", "sys_transform_entry", true) + " when asked for map "
            + mapId + " only");
      }
      final String target = Rows.text(entry, "target_field", "sys_transform_entry", false);
      if (target.isEmpty()) {
        continue;
      }
      if ("true".equals(Rows.text(entry, "use_source_script", "sys_transform_entry", true))) {
        scripted.add(target);
      } else {
        sourceOf.put(target, Rows.text(entry, "source_field", "sys_transform_entry", true));
      }
    }
    return new ImportBinding(table, stagingTable, mapName, sourceOf, scripted);
  }
}
