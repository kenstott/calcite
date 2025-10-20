/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.mcp.metadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Detects vector column relationship patterns.
 *
 * <p>Analyzes JDBC metadata and [VECTOR ...] annotations to determine
 * which pattern a vector column follows:
 * <ol>
 *   <li>Pattern 1 (CO_LOCATED): Vector column in source table</li>
 *   <li>Pattern 2a (FK_FORMAL): Formal FK constraint to source table</li>
 *   <li>Pattern 2b (FK_LOGICAL): Logical reference via metadata</li>
 *   <li>Pattern 3 (MULTI_SOURCE): Polymorphic source tracking</li>
 * </ol>
 */
public class PatternDetector {

  /**
   * Detect the vector pattern for a table.
   *
   * @param conn JDBC connection
   * @param schema Schema name (null for default)
   * @param table Table name
   * @param vm VectorMetadata from column comment
   * @return Detected pattern
   * @throws SQLException if metadata access fails
   */
  public static VectorPattern detectPattern(
      Connection conn,
      String schema,
      String table,
      VectorMetadata vm) throws SQLException {

    // Pattern 3: Multi-source (metadata has source_table_col)
    if (vm.getSourceTableColumn() != null) {
      vm.setPattern(VectorPattern.MULTI_SOURCE);
      return VectorPattern.MULTI_SOURCE;
    }

    // Pattern 2b: Logical FK (metadata has source_table)
    if (vm.getSourceTable() != null) {
      vm.setPattern(VectorPattern.FK_LOGICAL);
      return VectorPattern.FK_LOGICAL;
    }

    // Pattern 2a: Check for formal FK
    DatabaseMetaData meta = conn.getMetaData();
    try (ResultSet fkRs = meta.getImportedKeys(null, schema, table)) {
      while (fkRs.next()) {
        String fkColumn = fkRs.getString("FKCOLUMN_NAME");
        String pkTable = fkRs.getString("PKTABLE_NAME");
        String pkColumn = fkRs.getString("PKCOLUMN_NAME");

        // Store discovered FK for use in joins
        vm.setDiscoveredFkColumn(fkColumn);
        vm.setDiscoveredTargetTable(pkTable);
        vm.setDiscoveredTargetColumn(pkColumn);
        vm.setPattern(VectorPattern.FK_FORMAL);
        return VectorPattern.FK_FORMAL;
      }
    }

    // Pattern 1: Co-located (no FK, no metadata)
    vm.setPattern(VectorPattern.CO_LOCATED);
    return VectorPattern.CO_LOCATED;
  }
}
