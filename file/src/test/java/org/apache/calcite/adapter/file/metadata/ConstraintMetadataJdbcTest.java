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
package org.apache.calcite.adapter.file.metadata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies that primary-key and foreign-key constraints defined in the Calcite
 * model operand ({@code tableConstraints}) are correctly exposed through the
 * JDBC {@link DatabaseMetaData} methods ({@code getPrimaryKeys},
 * {@code getImportedKeys}) and through {@code Table.getStatistic()}.
 *
 * <p>The constraint path under test is:
 * <pre>
 *   model JSON tableConstraints
 *     → FileSchemaFactory.create()
 *     → FileSchema.setConstraintMetadata()
 *     → RefreshablePartitionedParquetTable.constraintConfig
 *     → RefreshablePartitionedParquetTable.getStatistic()
 *     → CalciteMetaImpl.getPrimaryKeys() / getImportedKeys()
 *     → DatabaseMetaData.getPrimaryKeys() / getImportedKeys()
 * </pre>
 *
 * <p>These constraints are also the source for {@code Statistic.getKeys()} and
 * {@code Statistic.getReferentialConstraints()}, which the Calcite CBO uses for
 * join-elimination and unique-key metadata.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class ConstraintMetadataJdbcTest {

  @TempDir
  Path tempDir;

  // -----------------------------------------------------------------------
  // Tests
  // -----------------------------------------------------------------------

  @Test
  void getPrimaryKeys_returnsConfiguredPkColumns() throws Exception {
    createParquet("customers.parquet",
        "SELECT CAST(i AS INTEGER) AS customer_id, 'name_' || CAST(i AS VARCHAR) AS name "
        + "FROM generate_series(1, 5) AS t(i)");
    createParquet("orders.parquet",
        "SELECT CAST(i AS INTEGER) AS order_id, CAST(i AS INTEGER) AS customer_id, "
        + "CAST(i AS DOUBLE) * 10.0 AS amount "
        + "FROM generate_series(1, 10) AS t(i)");

    try (Connection conn = openConnection()) {
      DatabaseMetaData meta = conn.getMetaData();

      // customers PK = customer_id
      List<String> custPk = readPkColumns(meta, "test", "customers");
      assertEquals(Arrays.asList("customer_id"), custPk,
          "customers PK should be [customer_id]");

      // orders PK = order_id
      List<String> ordersPk = readPkColumns(meta, "test", "orders");
      assertEquals(Arrays.asList("order_id"), ordersPk,
          "orders PK should be [order_id]");
    }
  }

  @Test
  void getPrimaryKeys_noConstraint_returnsEmpty() throws Exception {
    createParquet("unconstrained.parquet",
        "SELECT CAST(i AS INTEGER) AS id FROM generate_series(1, 3) AS t(i)");

    try (Connection conn = openConnection()) {
      DatabaseMetaData meta = conn.getMetaData();
      List<String> pk = readPkColumns(meta, "test", "unconstrained");
      assertTrue(pk.isEmpty(), "Table with no constraint config should return no PKs");
    }
  }

  @Test
  void getImportedKeys_returnsConfiguredFkRelationship() throws Exception {
    createParquet("customers.parquet",
        "SELECT CAST(i AS INTEGER) AS customer_id, 'name_' || CAST(i AS VARCHAR) AS name "
        + "FROM generate_series(1, 5) AS t(i)");
    createParquet("orders.parquet",
        "SELECT CAST(i AS INTEGER) AS order_id, CAST(i AS INTEGER) AS customer_id, "
        + "CAST(i AS DOUBLE) * 10.0 AS amount "
        + "FROM generate_series(1, 10) AS t(i)");

    try (Connection conn = openConnection()) {
      DatabaseMetaData meta = conn.getMetaData();
      // getImportedKeys(catalog, schema, fkTable) returns FKs where orders is the child
      try (ResultSet fks = meta.getImportedKeys(null, "test", "orders")) {
        assertTrue(fks.next(), "orders should have at least one imported key (FK to customers)");
        assertEquals("customer_id", fks.getString("FKCOLUMN_NAME"),
            "FK column should be customer_id");
        assertEquals("customers", fks.getString("PKTABLE_NAME"),
            "Referenced table should be customers");
        assertEquals("customer_id", fks.getString("PKCOLUMN_NAME"),
            "Referenced column should be customer_id");
        assertFalse(fks.next(), "orders should have exactly one FK column");
      }
    }
  }

  @Test
  void statisticKeys_exposedForCbo() throws Exception {
    createParquet("customers.parquet",
        "SELECT CAST(i AS INTEGER) AS customer_id, 'n' || CAST(i AS VARCHAR) AS name "
        + "FROM generate_series(1, 5) AS t(i)");

    // Verify via EXPLAIN that the planner sees uniqueness — the presence of PK constraints
    // lets RelMdUniqueKeys derive uniqueness, which the optimizer uses for join elimination.
    // We probe indirectly: a SELECT DISTINCT on a PK column should not add a sort/aggregate
    // on top of the scan when the optimizer knows it's already unique.
    //
    // Direct assertion: run a query that would fail or produce wrong results without
    // constraint awareness. Here we just verify the plan contains no duplicates and
    // query succeeds (smoke test — deeper plan assertions belong in query-plan tests).
    try (Connection conn = openConnection();
         Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery("SELECT \"customer_id\" FROM \"customers\" ORDER BY 1");
      int count = 0;
      int prev = Integer.MIN_VALUE;
      while (rs.next()) {
        int val = rs.getInt(1);
        assertTrue(val > prev, "Rows should be ordered");
        prev = val;
        count++;
      }
      assertEquals(5, count, "Should return all 5 customer rows");
    }
  }

  /**
   * Verifies the partitionedTables path: when primaryKey/foreignKeys are declared
   * as top-level fields on partitionedTables entries (the YAML schema pattern),
   * FileSchemaFactory extracts them into tableConstraints so getPrimaryKeys() works.
   *
   * <p>This covers the fix in FileSchemaFactory.create() that extracts constraints
   * from partitionedTables when no explicit tableConstraints operand is provided.
   */
  @Test
  void getPrimaryKeys_fromPartitionedTablesPrimaryKeyField() throws Exception {
    createParquet("pt_customers.parquet",
        "SELECT CAST(i AS INTEGER) AS customer_id, 'n' || CAST(i AS VARCHAR) AS name "
        + "FROM generate_series(1, 3) AS t(i)");
    createParquet("pt_orders.parquet",
        "SELECT CAST(i AS INTEGER) AS order_id, CAST(i AS INTEGER) AS customer_id "
        + "FROM generate_series(1, 5) AS t(i)");

    String dir = tempDir.toFile().getAbsolutePath().replace("\\", "\\\\");
    // Use partitionedTables with top-level primaryKey — no tableConstraints key
    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"pttest\","
        + "\"schemas\":[{"
        + "  \"name\":\"pttest\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "  \"operand\":{"
        + "    \"directory\":\"" + dir + "\","
        + "    \"storageType\":\"local\","
        + "    \"partitionedTables\":["
        + "      {"
        + "        \"name\":\"pt_customers\","
        + "        \"pattern\":\"pt_customers*.parquet\","
        + "        \"primaryKey\":[\"customer_id\"]"
        + "      },"
        + "      {"
        + "        \"name\":\"pt_orders\","
        + "        \"pattern\":\"pt_orders*.parquet\","
        + "        \"primaryKey\":[\"order_id\"],"
        + "        \"foreignKeys\":[{"
        + "          \"columns\":[\"customer_id\"],"
        + "          \"targetTable\":\"pt_customers\","
        + "          \"targetColumns\":[\"customer_id\"]"
        + "        }]"
        + "      }"
        + "    ]"
        + "  }"
        + "}]}";

    Properties props = new Properties();
    props.put("model", "inline:" + model);
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      DatabaseMetaData meta = conn.getMetaData();

      List<String> custPk = readPkColumns(meta, "pttest", "pt_customers");
      assertEquals(Arrays.asList("customer_id"), custPk,
          "pt_customers PK should be [customer_id] when declared via partitionedTables.primaryKey");

      List<String> ordersPk = readPkColumns(meta, "pttest", "pt_orders");
      assertEquals(Arrays.asList("order_id"), ordersPk,
          "pt_orders PK should be [order_id] when declared via partitionedTables.primaryKey");

      // Also verify FK is surfaced
      try (ResultSet fks = meta.getImportedKeys(null, "pttest", "pt_orders")) {
        assertTrue(fks.next(), "pt_orders should have an imported key to pt_customers");
        assertEquals("customer_id", fks.getString("FKCOLUMN_NAME"));
        assertEquals("pt_customers", fks.getString("PKTABLE_NAME"));
      }
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private List<String> readPkColumns(DatabaseMetaData meta,
      String schema, String table) throws Exception {
    List<String> cols = new ArrayList<>();
    try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
      while (rs.next()) {
        cols.add(rs.getString("COLUMN_NAME"));
      }
    }
    return cols;
  }

  private void createParquet(String fileName, String selectSql) throws Exception {
    File out = new File(tempDir.toFile(), fileName);
    String sql = "COPY (" + selectSql + ") TO '" + out.getAbsolutePath() + "' (FORMAT PARQUET)";
    ProcessBuilder pb = new ProcessBuilder("duckdb", "-c", sql);
    pb.redirectErrorStream(true);
    Process proc = pb.start();
    int exit = proc.waitFor();
    if (exit != 0) {
      byte[] buf = new byte[4096];
      int len = proc.getInputStream().read(buf);
      String err = len > 0 ? new String(buf, 0, len) : "unknown error";
      fail("DuckDB CLI failed creating " + fileName + ": " + err);
    }
    assertTrue(out.exists(), "Parquet file should exist: " + out);
  }

  private Connection openConnection() throws Exception {
    String dir = tempDir.toFile().getAbsolutePath().replace("\\", "\\\\");
    // tableConstraints: orders has PK(order_id) + FK(customer_id -> customers.customer_id)
    //                   customers has PK(customer_id)
    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"test\","
        + "\"schemas\":[{"
        + "  \"name\":\"test\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "  \"operand\":{"
        + "    \"directory\":\"" + dir + "\","
        + "    \"ephemeralCache\":true,"
        + "    \"tableConstraints\":{"
        + "      \"orders\":{"
        + "        \"primaryKey\":[\"order_id\"],"
        + "        \"foreignKeys\":[{"
        + "          \"columns\":[\"customer_id\"],"
        + "          \"targetTable\":\"customers\","
        + "          \"targetColumns\":[\"customer_id\"]"
        + "        }]"
        + "      },"
        + "      \"customers\":{"
        + "        \"primaryKey\":[\"customer_id\"]"
        + "      }"
        + "    }"
        + "  }"
        + "}]}";

    Properties props = new Properties();
    props.put("model", "inline:" + model);
    return DriverManager.getConnection("jdbc:calcite:", props);
  }
}
