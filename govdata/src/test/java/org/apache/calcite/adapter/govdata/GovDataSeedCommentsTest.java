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
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The seed catalog carries the schema YAMLs' descriptions as DuckDB comments. */
@Tag("unit")
class GovDataSeedCommentsTest {

  @Test void yamlDescriptionsBecomeCatalogComments() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = conn.createStatement()) {
      st.execute("CREATE SCHEMA fec");
      st.execute("CREATE VIEW fec.candidates AS SELECT 'x' AS candidate_id, 'y' AS zip");

      GovDataSeedComments.Result r = GovDataSeedComments.apply(conn);

      assertEquals(1, r.relationsCommented, r.toString());
      assertEquals(2, r.columnsCommented, r.toString());
      try (ResultSet rs = st.executeQuery("SELECT comment FROM duckdb_columns() "
          + "WHERE schema_name = 'fec' AND table_name = 'candidates' AND column_name = 'zip'")) {
        assertTrue(rs.next());
        assertEquals("Mailing address ZIP code", rs.getString(1));
      }
      try (ResultSet rs = st.executeQuery("SELECT comment FROM duckdb_views() "
          + "WHERE schema_name = 'fec' AND view_name = 'candidates'")) {
        assertTrue(rs.next());
        assertTrue(rs.getString(1).contains("federal candidates"), rs.getString(1));
      }
    }
  }
}
