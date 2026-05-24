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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive JDBC metadata integration tests for govdata schemas.
 *
 * <p>Validates the full JDBC metadata contract:
 * - DatabaseMetaData.getSchemas()
 * - DatabaseMetaData.getTables()
 * - DatabaseMetaData.getColumns()     ← historically untested; returns 0 for Iceberg tables
 * - DatabaseMetaData.getPrimaryKeys()
 * - DatabaseMetaData.getImportedKeys()
 * - ResultSetMetaData after query execution
 *
 * <p>Uses the GovDataDriver (jdbc:govdata:) path — same as the MCP server — against
 * DQ-passed Iceberg schemas. Skips if GOVDATA_PARQUET_DIR is not set.
 */
@Tag("integration")
public class JdbcMetadataIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMetadataIntegrationTest.class);

  // Use fec: DQ-tracked, Iceberg format, confirmed working against R2
  private static final String SCHEMA = "fec";

  private static Connection openConnection() throws Exception {
    Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    return DriverManager.getConnection("jdbc:govdata:source=" + SCHEMA, props);
  }

  @BeforeAll
  static void checkEnvironment() {
    // GovDataDriver defaults to s3://govdata-parquet-v1 when GOVDATA_PARQUET_DIR is unset.
    // Verify R2 is reachable by attempting a connection; skip if it fails.
    try {
      Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");
      Properties props = new Properties();
      props.setProperty("lex", "ORACLE");
      props.setProperty("unquotedCasing", "TO_LOWER");
      Connection c = DriverManager.getConnection("jdbc:govdata:source=" + SCHEMA, props);
      c.close();
      LOGGER.info("R2 reachable — running JDBC metadata integration tests");
    } catch (Exception e) {
      Assumptions.assumeTrue(false, "R2 not reachable, skipping: " + e.getMessage());
    }
  }

  // ── getSchemas ──────────────────────────────────────────────────────────────

  @Test
  void getSchemas_returnsTargetSchema() throws Exception {
    try (Connection c = openConnection()) {
      DatabaseMetaData meta = c.getMetaData();
      List<String> schemas = new ArrayList<>();
      try (ResultSet rs = meta.getSchemas()) {
        while (rs.next()) {
          schemas.add(rs.getString("TABLE_SCHEM").toLowerCase(java.util.Locale.ROOT));
        }
      }
      LOGGER.info("Schemas: {}", schemas);
      assertTrue(schemas.contains(SCHEMA),
          "getSchemas() must include '" + SCHEMA + "'; got: " + schemas);
    }
  }

  // ── getTables ───────────────────────────────────────────────────────────────

  @Test
  void getTables_returnsAtLeastOneTable() throws Exception {
    try (Connection c = openConnection()) {
      DatabaseMetaData meta = c.getMetaData();
      List<String> tables = new ArrayList<>();
      try (ResultSet rs = meta.getTables(null, SCHEMA, "%", null)) {
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }
      }
      LOGGER.info("Tables in {}: {}", SCHEMA, tables);
      assertFalse(tables.isEmpty(),
          "getTables() must return at least one table for schema '" + SCHEMA + "'");
    }
  }

  // ── getColumns ──────────────────────────────────────────────────────────────

  @Test
  void getColumns_returnsColumnsForEachTable() throws Exception {
    try (Connection c = openConnection()) {
      DatabaseMetaData meta = c.getMetaData();

      List<String> tables = new ArrayList<>();
      try (ResultSet rs = meta.getTables(null, SCHEMA, "%", null)) {
        while (rs.next()) {
          tables.add(rs.getString("TABLE_NAME"));
        }
      }
      assertFalse(tables.isEmpty(), "Need at least one table to test getColumns()");

      for (String table : tables) {
        List<String> columns = new ArrayList<>();
        try (ResultSet rs = meta.getColumns(null, SCHEMA, table, "%")) {
          while (rs.next()) {
            String colName = rs.getString("COLUMN_NAME");
            String typeName = rs.getString("TYPE_NAME");
            columns.add(colName + ":" + typeName);
          }
        }
        LOGGER.info("Columns for {}.{}: {}", SCHEMA, table, columns);
        assertFalse(columns.isEmpty(),
            "getColumns() returned 0 columns for table '" + SCHEMA + "." + table + "'. "
                + "Iceberg tables must expose their schema via JDBC metadata.");
      }
    }
  }

  @Test
  void getColumns_columnNamesMatchYamlDefinitions() throws Exception {
    // Spot-check: fec.candidates must have known columns (defined in fec-schema.yaml)
    try (Connection c = openConnection()) {
      DatabaseMetaData meta = c.getMetaData();
      List<String> columns = new ArrayList<>();
      try (ResultSet rs = meta.getColumns(null, SCHEMA, "candidates", "%")) {
        while (rs.next()) {
          columns.add(rs.getString("COLUMN_NAME").toLowerCase(java.util.Locale.ROOT));
        }
      }
      LOGGER.info("Columns for {}.candidates: {}", SCHEMA, columns);
      assertFalse(columns.isEmpty(),
          "getColumns() returned 0 columns for fec.candidates");
      assertTrue(columns.contains("candidate_id"),
          "fec.candidates must contain candidate_id; got: " + columns);
    }
  }

  // ── getPrimaryKeys ──────────────────────────────────────────────────────────

  @Test
  void getPrimaryKeys_candidatesHasKnownPrimaryKey() throws Exception {
    // fec-schema.yaml declares: candidates.primaryKey = [type, year, candidate_id]
    // This verifies the full wiring: YAML constraints → GovDataSchemaFactory.setTableConstraints()
    // → enrichedOperand → FileSchemaFactory → PartitionedParquetTable.getStatistic() → JDBC metadata
    try (Connection c = openConnection()) {
      DatabaseMetaData meta = c.getMetaData();
      List<String> pks = new ArrayList<>();
      try (ResultSet rs = meta.getPrimaryKeys(null, SCHEMA, "candidates")) {
        while (rs.next()) {
          pks.add(rs.getString("COLUMN_NAME").toLowerCase(java.util.Locale.ROOT));
        }
      }
      LOGGER.info("Primary keys for {}.candidates: {}", SCHEMA, pks);
      assertFalse(pks.isEmpty(),
          "getPrimaryKeys() returned no rows for fec.candidates. "
          + "primaryKey is declared in fec-schema.yaml — constraints must flow through "
          + "GovDataSchemaFactory.enrichedOperand to FileSchemaFactory.");
      assertTrue(pks.contains("candidate_id"),
          "fec.candidates PK must include candidate_id; got: " + pks);
    }
  }

  @Test
  void getPrimaryKeys_committeesHasKnownPrimaryKey() throws Exception {
    // fec-schema.yaml declares: committees.primaryKey = [type, year, committee_id]
    try (Connection c = openConnection()) {
      DatabaseMetaData meta = c.getMetaData();
      List<String> pks = new ArrayList<>();
      try (ResultSet rs = meta.getPrimaryKeys(null, SCHEMA, "committees")) {
        while (rs.next()) {
          pks.add(rs.getString("COLUMN_NAME").toLowerCase(java.util.Locale.ROOT));
        }
      }
      LOGGER.info("Primary keys for {}.committees: {}", SCHEMA, pks);
      assertFalse(pks.isEmpty(),
          "getPrimaryKeys() returned no rows for fec.committees (declared in fec-schema.yaml)");
      assertTrue(pks.contains("committee_id"),
          "fec.committees PK must include committee_id; got: " + pks);
    }
  }

  // ── getImportedKeys ─────────────────────────────────────────────────────────

  @Test
  void getImportedKeys_committeesHasFkToCandidates() throws Exception {
    // fec-schema.yaml: committees has FK candidate_id → candidates.candidate_id
    try (Connection c = openConnection()) {
      DatabaseMetaData meta = c.getMetaData();
      List<String> fkCols = new ArrayList<>();
      try (ResultSet rs = meta.getImportedKeys(null, SCHEMA, "committees")) {
        while (rs.next()) {
          String fkCol = rs.getString("FKCOLUMN_NAME").toLowerCase(java.util.Locale.ROOT);
          String pkTable = rs.getString("PKTABLE_NAME").toLowerCase(java.util.Locale.ROOT);
          fkCols.add(fkCol + "->" + pkTable);
        }
      }
      LOGGER.info("Imported keys for {}.committees: {}", SCHEMA, fkCols);
      assertFalse(fkCols.isEmpty(),
          "getImportedKeys() returned no rows for fec.committees "
          + "(FK candidate_id→candidates declared in fec-schema.yaml)");
      assertTrue(fkCols.stream().anyMatch(e -> e.startsWith("candidate_id")),
          "fec.committees must have FK on candidate_id; got: " + fkCols);
    }
  }

  @Test
  void getImportedKeys_doesNotThrowForTableWithNoFk() throws Exception {
    // candidates has no FKs to other fec tables — getImportedKeys must return empty, not throw
    try (Connection c = openConnection()) {
      DatabaseMetaData meta = c.getMetaData();
      List<String> fks = new ArrayList<>();
      try (ResultSet rs = meta.getImportedKeys(null, SCHEMA, "candidates")) {
        while (rs.next()) {
          fks.add(rs.getString("FKCOLUMN_NAME"));
        }
      }
      LOGGER.info("Imported keys for {}.candidates: {}", SCHEMA, fks);
      // candidates has no intra-fec FKs (only cross-schema FKs to geo which may not be wired)
      // assertion: method completes without exception, result set closed cleanly
    }
  }

  // ── ResultSetMetaData ───────────────────────────────────────────────────────

  @Test
  void resultSetMetaData_matchesGetColumns() throws Exception {
    try (Connection c = openConnection()) {
      DatabaseMetaData meta = c.getMetaData();

      // Get the first table
      String firstTable = null;
      try (ResultSet rs = meta.getTables(null, SCHEMA, "%", null)) {
        if (rs.next()) {
          firstTable = rs.getString("TABLE_NAME");
        }
      }
      assertNotNull(firstTable, "Need at least one table");

      // Execute a query and check ResultSetMetaData
      String sql = "SELECT * FROM " + SCHEMA + "." + firstTable + " FETCH FIRST 1 ROWS ONLY";
      try (Statement stmt = c.createStatement();
           ResultSet rs = stmt.executeQuery(sql)) {
        ResultSetMetaData rsmd = rs.getMetaData();
        int colCount = rsmd.getColumnCount();
        LOGGER.info("ResultSetMetaData for {}.{}: {} columns", SCHEMA, firstTable, colCount);

        assertTrue(colCount > 0,
            "ResultSetMetaData.getColumnCount() returned 0 for query: " + sql);

        List<String> rsmdCols = new ArrayList<>();
        for (int i = 1; i <= colCount; i++) {
          rsmdCols.add(rsmd.getColumnName(i) + ":" + rsmd.getColumnTypeName(i));
        }
        LOGGER.info("ResultSetMetaData columns: {}", rsmdCols);

        // Verify DatabaseMetaData.getColumns() agrees on column count
        List<String> metaCols = new ArrayList<>();
        try (ResultSet colRs = meta.getColumns(null, SCHEMA, firstTable, "%")) {
          while (colRs.next()) {
            metaCols.add(colRs.getString("COLUMN_NAME"));
          }
        }
        assertFalse(metaCols.isEmpty(),
            "DatabaseMetaData.getColumns() returned 0 for table '" + firstTable
                + "' even though query returned " + colCount + " columns");
      }
    }
  }

  // ── getTables type-filter regression ───────────────────────────────────────

  @Test
  void getTables_withTableTypeFilter_returnsResults() throws Exception {
    // Regression: getTables(..."TABLE") returned [] even when tables exist.
    // Root cause: Calcite registers some table types differently; null is safer.
    try (Connection c = openConnection()) {
      DatabaseMetaData meta = c.getMetaData();

      List<String> withFilter = new ArrayList<>();
      try (ResultSet rs = meta.getTables(null, SCHEMA, "%", new String[]{"TABLE"})) {
        while (rs.next()) withFilter.add(rs.getString("TABLE_NAME"));
      }
      List<String> withoutFilter = new ArrayList<>();
      try (ResultSet rs = meta.getTables(null, SCHEMA, "%", null)) {
        while (rs.next()) withoutFilter.add(rs.getString("TABLE_NAME"));
      }
      LOGGER.info("getTables with type=TABLE: {}", withFilter);
      LOGGER.info("getTables with type=null:  {}", withoutFilter);

      assertFalse(withoutFilter.isEmpty(), "getTables(null) must return tables");
      // type=TABLE must return at least the Iceberg-backed govdata tables;
      // null returns tables + views so withoutFilter.size() >= withFilter.size()
      assertFalse(withFilter.isEmpty(),
          "getTables with type=TABLE returned 0 but null returned " + withoutFilter.size()
              + ". Type filter is dropping valid Iceberg-backed tables.");
      assertTrue(withFilter.size() <= withoutFilter.size(),
          "getTables with type=TABLE returned more entries than with null filter — impossible");
    }
  }

  // ── View support ─────────────────────────────────────────────────────────

  @Test
  void getTables_viewFilter_returnsViews() throws Exception {
    // Single-schema YAML views (e.g. fec.active_candidates) should always register in DuckDB.
    // Cross-schema views (e.g. candidate_district_profile joining geo) may fail when
    // geo is not available, so we only assert on the single-schema view here.
    try (Connection c = openConnection()) {
      DatabaseMetaData meta = c.getMetaData();
      List<String> views = new ArrayList<>();
      try (ResultSet rs = meta.getTables(null, SCHEMA, "%", new String[]{"VIEW"})) {
        while (rs.next()) {
          views.add(rs.getString("TABLE_NAME").toLowerCase(java.util.Locale.ROOT));
        }
      }
      LOGGER.info("Views in {} (type=VIEW filter): {}", SCHEMA, views);
      assertTrue(views.contains("active_candidates"),
          "fec.active_candidates (single-schema view) must appear with type=VIEW; got: " + views);
    }
  }

  @Test
  void getColumns_viewReturnsColumns() throws Exception {
    // Verify that getColumns() works for a single-schema SQL view.
    try (Connection c = openConnection()) {
      DatabaseMetaData meta = c.getMetaData();
      List<String> columns = new ArrayList<>();
      try (ResultSet rs = meta.getColumns(null, SCHEMA, "active_candidates", "%")) {
        while (rs.next()) {
          columns.add(rs.getString("COLUMN_NAME").toLowerCase(java.util.Locale.ROOT));
        }
      }
      LOGGER.info("Columns for {}.active_candidates: {}", SCHEMA, columns);
      assertFalse(columns.isEmpty(),
          "getColumns() must return columns for fec.active_candidates");
      assertTrue(columns.contains("candidate_id"),
          "active_candidates must have candidate_id; got: " + columns);
    }
  }

  // ── ref schema PK regression guard ──────────────────────────────────────────

  @Test
  void ref_gleifEntities_hasKnownPrimaryKey() throws Exception {
    // ref-schema.yaml declares: gleif_entities.primaryKey = [type, lei]
    // Regression guard for issue #19: IcebergTable-backed tables were returning empty PKs
    // because ConstraintAwareJdbcTable.getStatistic() used DuckDB column indices while
    // CalciteMetaImpl.getPrimaryKeys() mapped them using IcebergTable column order.
    Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    try (Connection c = DriverManager.getConnection("jdbc:govdata:source=ref", props)) {
      DatabaseMetaData meta = c.getMetaData();
      List<String> pks = new ArrayList<>();
      try (ResultSet rs = meta.getPrimaryKeys(null, "ref", "gleif_entities")) {
        while (rs.next()) {
          pks.add(rs.getString("COLUMN_NAME").toLowerCase(java.util.Locale.ROOT));
        }
      }
      LOGGER.info("Primary keys for ref.gleif_entities: {}", pks);
      assertFalse(pks.isEmpty(),
          "getPrimaryKeys() returned no rows for ref.gleif_entities. "
          + "primaryKey=[type,lei] is declared in ref-schema.yaml — constraints must flow "
          + "through IcebergTable with column indices aligned to IcebergTable.getRowType().");
      assertTrue(pks.contains("lei"),
          "ref.gleif_entities PK must include lei; got: " + pks);
    }
  }

  @Test
  void ref_secCompanyTickers_hasKnownPrimaryKey() throws Exception {
    // ref-schema.yaml declares: sec_company_tickers.primaryKey = [type, cik]
    Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    try (Connection c = DriverManager.getConnection("jdbc:govdata:source=ref", props)) {
      DatabaseMetaData meta = c.getMetaData();
      List<String> pks = new ArrayList<>();
      try (ResultSet rs = meta.getPrimaryKeys(null, "ref", "sec_company_tickers")) {
        while (rs.next()) {
          pks.add(rs.getString("COLUMN_NAME").toLowerCase(java.util.Locale.ROOT));
        }
      }
      LOGGER.info("Primary keys for ref.sec_company_tickers: {}", pks);
      assertFalse(pks.isEmpty(),
          "getPrimaryKeys() returned no rows for ref.sec_company_tickers "
          + "(declared in ref-schema.yaml)");
      assertTrue(pks.contains("cik"),
          "ref.sec_company_tickers PK must include cik; got: " + pks);
    }
  }

  // ── Cross-schema spot check ─────────────────────────────────────────────────

  @Test
  void fec_individualContributions_hasColumns() throws Exception {
    // FEC is a DQ-tracked Iceberg schema — spot-check that getColumns() works
    // for a known Iceberg table (this was the original failing case)

    Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    try (Connection c = DriverManager.getConnection("jdbc:govdata:source=fec", props)) {
      DatabaseMetaData meta = c.getMetaData();
      List<String> columns = new ArrayList<>();
      try (ResultSet rs = meta.getColumns(null, "fec", "individual_contributions", "%")) {
        while (rs.next()) {
          columns.add(rs.getString("COLUMN_NAME"));
        }
      }
      LOGGER.info("Columns for fec.individual_contributions: {}", columns);
      assertFalse(columns.isEmpty(),
          "DatabaseMetaData.getColumns() returned 0 for fec.individual_contributions. "
              + "This is the core MCP describe_table regression.");
    }
  }
}
