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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Relational expression that can be implemented in DuckDB.
 */
public interface DuckDBRel extends RelNode {

  /** Calling convention for DuckDB operators */
  Convention CONVENTION = new Convention.Impl("DUCKDB", DuckDBRel.class);

  /**
   * Generates SQL for this operation.
   */
  void generateSql(SqlBuilder builder);

  /**
   * Helper class for building SQL queries.
   */
  class SqlBuilder {
    private final StringBuilder sql = new StringBuilder();
    private final List<String> tableAliases = new ArrayList<>();
    private int aliasCounter = 0;

    public String nextAlias() {
      return "t" + (aliasCounter++);
    }

    public SqlBuilder append(String s) {
      sql.append(s);
      return this;
    }

    public SqlBuilder append(char c) {
      sql.append(c);
      return this;
    }

    public String getSql() {
      return sql.toString();
    }

    @Override public String toString() {
      return sql.toString();
    }
  }
}
