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
import org.apache.calcite.schema.lookup.LikePattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

/**
 * Operator-triggered adjustment of a DuckDB-backed catalog's resources without a new JAR —
 * e.g. a "set memory limit" tool exposed by a long-lived server that holds a govdata connection
 * open across many calls.
 */
public final class DuckDBCatalogMaintenance {

  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBCatalogMaintenance.class);

  private DuckDBCatalogMaintenance() {}

  /**
   * Sets DuckDB's {@code memory_limit}/{@code max_memory} on every DuckDB-backed catalog mounted
   * on this connection, immediately — unlike the {@code -Dcalcite.duckdb.memoryLimit} system
   * property (which only takes effect for connections opened AFTER it changes), this reaches the
   * database instance(s) already backing this live connection.
   *
   * @param limit a DuckDB size literal, e.g. {@code "8GB"}
   */
  public static void setMemoryLimit(CalciteConnection connection, String limit)
      throws SQLException {
    forEachDuckDbCatalog(connection, (catalogPath, duckSchema) -> {
      LOGGER.info("Setting memory_limit={} for catalog '{}'", limit, catalogPath);
      try (Statement stmt = duckSchema.getPersistentConnection().createStatement()) {
        stmt.execute("SET memory_limit = '" + limit + "'");
        stmt.execute("SET max_memory = '" + limit + "'");
      }
    });
  }

  @FunctionalInterface
  private interface CatalogAction {
    void apply(String catalogPath, DuckDBJdbcSchema duckSchema) throws SQLException;
  }

  /**
   * Walks every subschema mounted on this connection, applying {@code action} once per distinct
   * underlying DuckDB database file (mounted schemas normally share one file, so this is
   * normally one call, not one per schema, even on a many-schema catalog connection).
   */
  private static void forEachDuckDbCatalog(CalciteConnection connection, CatalogAction action)
      throws SQLException {
    SchemaPlus root = connection.getRootSchema();
    Set<String> doneCatalogPaths = new HashSet<>();
    for (String name : root.subSchemas().getNames(LikePattern.any())) {
      SchemaPlus sub = root.subSchemas().get(name);
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
      action.apply(catalogPath, duckSchema);
    }
  }
}
