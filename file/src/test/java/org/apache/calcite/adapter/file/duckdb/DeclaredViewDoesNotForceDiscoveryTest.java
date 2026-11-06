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

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A declared SQL view ({@code tables: [{type: view}]}) is registered from its SQL, not resolved as
 * a table. Counting it among the tables "not deterministically resolvable from config" made
 * econ, census and weather (the govdata schemas that declare views) send the whole schema through
 * {@code registerFilesAsViews -> getAllTableRecords -> getTableMap()} at mount: per-table
 * object-store discovery for every Iceberg table, 45s for econ on a real start (govdata-ops#985).
 */
@Tag("unit")
class DeclaredViewDoesNotForceDiscoveryTest {

  @TempDir Path tempDir;

  private static Map<String, Object> icebergTable(String name, Path warehouse) {
    Map<String, Object> iceberg = new LinkedHashMap<>();
    iceberg.put("warehousePath", warehouse.toString());
    iceberg.put("tableName", name);
    Map<String, Object> materialize = new LinkedHashMap<>();
    materialize.put("format", "iceberg");
    materialize.put("iceberg", iceberg);
    Map<String, Object> def = new LinkedHashMap<>();
    def.put("name", name);
    def.put("pattern", "type=" + name + "/*.parquet");
    def.put("materialize", materialize);
    return def;
  }

  private static Map<String, Object> view(String name) {
    Map<String, Object> def = new LinkedHashMap<>();
    def.put("name", name);
    def.put("type", "view");
    def.put("sql", "SELECT 1 AS x");
    return def;
  }

  private FileSchema schema(List<Map<String, Object>> tables,
      List<Map<String, Object>> partitioned) {
    SchemaPlus parent = mock(SchemaPlus.class);
    when(parent.getName()).thenReturn("root");
    return new FileSchema(parent, "viewschema", tempDir.toFile(), tempDir.toFile(), null, tables,
        new ExecutionEngineConfig(), false, null, null, partitioned, null, "SMART_CASING",
        "SMART_CASING", null, null, null, null, false);
  }

  private void register(FileSchema fileSchema, String catalogPath) throws Exception {
    Method m = DuckDBJdbcSchemaFactory.class.getDeclaredMethod("registerDeclaredTablesForDuckDB",
        Connection.class, String.class, boolean.class, String.class, String.class,
        FileSchema.class, Set.class, String.class);
    m.setAccessible(true);
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS viewschema");
      m.invoke(null, conn, tempDir.toString(), false, "viewschema", "viewschema", fileSchema,
          Collections.<String>emptySet(), catalogPath);
    }
  }

  @Test void viewDeclaredBesideIcebergTablesDoesNotResolveAnyTable() throws Exception {
    List<Map<String, Object>> tables = new ArrayList<>();
    tables.add(view("some_view"));
    List<Map<String, Object>> partitioned = new ArrayList<>();
    partitioned.add(icebergTable("t1", tempDir.resolve("wh")));
    partitioned.add(icebergTable("t2", tempDir.resolve("wh")));
    FileSchema fileSchema = schema(tables, partitioned);
    String catalog = tempDir.resolve("cat1.duckdb").toString();

    register(fileSchema, catalog);

    // Both Iceberg tables were deferred, and nothing was resolved against the (empty) warehouse:
    // resolving would have recorded t1 and t2 as omitted (not materialized) or discovered them.
    assertEquals(new java.util.HashSet<>(java.util.Arrays.asList("t1", "t2")),
        DuckDBPendingViews.pendingViewNames(catalog, "viewschema"));
    assertTrue(fileSchema.getOmittedTableNames().isEmpty(),
        "no table may be resolved at mount, but these were: " + fileSchema.getOmittedTableNames());
    assertTrue(fileSchema.getConversionMetadata().getAllConversions().isEmpty());
  }

  @Test void isDeclaredSqlViewSeparatesViewsFromTables() {
    List<Map<String, Object>> tables = new ArrayList<>();
    tables.add(view("some_view"));
    List<Map<String, Object>> partitioned = new ArrayList<>();
    partitioned.add(icebergTable("t1", tempDir.resolve("wh")));
    FileSchema fileSchema = schema(tables, partitioned);

    assertTrue(fileSchema.isDeclaredSqlView("some_view"));
    assertEquals(false, fileSchema.isDeclaredSqlView("t1"));
    assertEquals(false, fileSchema.isDeclaredSqlView("absent"));
  }
}
