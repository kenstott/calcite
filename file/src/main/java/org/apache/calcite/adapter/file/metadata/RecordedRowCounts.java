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
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.adapter.file.ConstraintAwareJdbcSchema;
import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.duckdb.DuckDBJdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.lookup.LikePattern;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The row counts a connection's file-backed schemas already hold, for a catalog that must report
 * cardinalities (PostgreSQL's {@code pg_class.reltuples}) without resolving, loading or counting
 * any table.
 *
 * <p>See {@link FileSchema#getRecordedRowCounts()} for where each count comes from. A schema that
 * is not file-backed has no entry.
 */
public final class RecordedRowCounts {

  private RecordedRowCounts() {
  }

  /**
   * Recorded counts for every file-backed schema mounted on a connection.
   *
   * @param connection an open Calcite connection
   * @return schema name to (relation name to recorded row count)
   */
  public static Map<String, Map<String, Long>> forConnection(Connection connection)
      throws SQLException {
    SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();
    Map<String, Map<String, Long>> bySchema = new LinkedHashMap<>();
    for (String schemaName : root.subSchemas().getNames(LikePattern.any())) {
      SchemaPlus subSchema = root.subSchemas().get(schemaName);
      if (subSchema == null) {
        continue;
      }
      FileSchema fileSchema = fileSchemaOf(subSchema.unwrap(CalciteSchema.class).schema);
      if (fileSchema != null) {
        bySchema.put(schemaName, fileSchema.getRecordedRowCounts());
      }
    }
    return bySchema;
  }

  /** The FileSchema behind a mounted schema, or null when the schema is not file-backed. */
  private static @Nullable FileSchema fileSchemaOf(Schema schema) {
    if (schema instanceof FileSchema) {
      return (FileSchema) schema;
    }
    if (schema instanceof ConstraintAwareJdbcSchema) {
      return ((ConstraintAwareJdbcSchema) schema).getOwner();
    }
    if (schema instanceof DuckDBJdbcSchema) {
      return ((DuckDBJdbcSchema) schema).getFileSchema();
    }
    return null;
  }
}
