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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * Operator-triggered entry point for rebuilding a DuckDB-backed catalog against current data,
 * without a new JAR — e.g. an "update schema" tool exposed by a long-lived server that holds a
 * govdata connection open across many calls.
 *
 * <p>Deliberately separate from the lazy per-view path in {@link DuckDBPendingViews}: that one
 * resolves a single view the first time a query actually asks for it. This one is the explicit,
 * whole-catalog counterpart — call it after fixing whatever made some views fail (a sync gap, a
 * bad row) to retry them all against a live connection, rather than waiting for a query to
 * stumble onto each one individually.
 */
public final class DuckDBCatalogMaintenance {

  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBCatalogMaintenance.class);

  private DuckDBCatalogMaintenance() {}

  /**
   * Retries every still-pending deferred view across every DuckDB-backed schema mounted on this
   * connection. Mounted schemas normally share one DuckDB database file, so this is normally one
   * rebuild pass, not one per schema, even on a many-schema catalog connection.
   */
  public static void rebuildPendingViews(CalciteConnection connection) throws SQLException {
    SchemaPlus root = connection.getRootSchema();
    Set<String> doneCatalogPaths = new HashSet<>();
    for (String name : root.getSubSchemaNames()) {
      SchemaPlus sub = root.getSubSchema(name);
      DuckDBJdbcSchema duckSchema;
      try {
        // Unwrap throws ClassCastException, not null, for a subschema that isn't wrappable to
        // this type (e.g. the metadata/information_schema subschema) — every non-DuckDB
        // subschema hits this, so it's the expected way most iterations end, not a real error.
        duckSchema = sub == null ? null : sub.unwrap(DuckDBJdbcSchema.class);
      } catch (ClassCastException notDuckDb) {
        continue;
      }
      if (duckSchema == null) {
        continue;
      }
      String catalogPath = duckSchema.getCatalogPath();
      if (catalogPath == null || !doneCatalogPaths.add(catalogPath)) {
        continue;
      }
      LOGGER.info("Rebuilding pending deferred views for catalog '{}' (schema '{}')",
          catalogPath, name);
      DuckDBPendingViews.buildAll(catalogPath, duckSchema.getPersistentConnection());
    }
  }
}
