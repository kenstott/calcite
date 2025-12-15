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
package org.apache.calcite.adapter.file.jdbc;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for JDBC dialect implementations.
 *
 * <p>These tests verify that each dialect correctly generates SQL statements
 * and JDBC URLs for their respective engines.
 */
@Tag("unit")
public class JdbcDialectTest {

  // =========================================================================
  // DuckDB Dialect Tests
  // =========================================================================

  @Test
  public void testDuckDBDriverClassName() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;
    assertEquals("org.duckdb.DuckDBDriver", dialect.getDriverClassName());
  }

  @Test
  public void testDuckDBBuildJdbcUrlDefault() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    // Empty config should return in-memory URL
    String url = dialect.buildJdbcUrl(Collections.emptyMap());
    assertEquals("jdbc:duckdb:", url);
  }

  @Test
  public void testDuckDBBuildJdbcUrlWithPath() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    Map<String, String> config = new HashMap<String, String>();
    config.put("path", "/tmp/test.duckdb");

    String url = dialect.buildJdbcUrl(config);
    assertEquals("jdbc:duckdb:/tmp/test.duckdb", url);
  }

  @Test
  public void testDuckDBBuildJdbcUrlNullConfig() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    String url = dialect.buildJdbcUrl(null);
    assertEquals("jdbc:duckdb:", url);
  }

  @Test
  public void testDuckDBReadParquetSqlAllColumns() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", Collections.emptyList());
    assertEquals("SELECT * FROM read_parquet('s3://bucket/data/*.parquet')", sql);
  }

  @Test
  public void testDuckDBReadParquetSqlWithColumns() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    List<String> columns = Arrays.asList("id", "name", "amount");
    String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", columns);
    assertEquals("SELECT id, name, amount FROM read_parquet('s3://bucket/data/*.parquet')", sql);
  }

  @Test
  public void testDuckDBReadIcebergSql() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    String sql = dialect.readIcebergSql("s3://bucket/warehouse/table", Collections.emptyList());
    assertEquals("SELECT * FROM iceberg_scan('s3://bucket/warehouse/table')", sql);
  }

  @Test
  public void testDuckDBReadIcebergSqlWithColumns() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    List<String> columns = Arrays.asList("id", "timestamp");
    String sql = dialect.readIcebergSql("s3://bucket/warehouse/table", columns);
    assertEquals("SELECT id, timestamp FROM iceberg_scan('s3://bucket/warehouse/table')", sql);
  }

  @Test
  public void testDuckDBSupportsDirectGlob() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;
    assertTrue(dialect.supportsDirectGlob());
  }

  @Test
  public void testDuckDBSupportsIceberg() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;
    assertTrue(dialect.supportsIceberg());
  }

  @Test
  public void testDuckDBName() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;
    assertEquals("DuckDB", dialect.getName());
  }

  // =========================================================================
  // Trino Dialect Tests
  // =========================================================================

  @Test
  public void testTrinoDriverClassName() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;
    assertEquals("io.trino.jdbc.TrinoDriver", dialect.getDriverClassName());
  }

  @Test
  public void testTrinoBuildJdbcUrlDefault() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;

    String url = dialect.buildJdbcUrl(Collections.emptyMap());
    assertEquals("jdbc:trino://localhost:8080/hive/default", url);
  }

  @Test
  public void testTrinoBuildJdbcUrlWithConfig() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;

    Map<String, String> config = new HashMap<String, String>();
    config.put("host", "trino.example.com");
    config.put("port", "443");
    config.put("catalog", "iceberg");
    config.put("schema", "datalake");

    String url = dialect.buildJdbcUrl(config);
    assertEquals("jdbc:trino://trino.example.com:443/iceberg/datalake", url);
  }

  @Test
  public void testTrinoReadParquetSql() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;

    // Trino derives table name from path
    String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", Collections.emptyList());
    assertEquals("SELECT * FROM data", sql);
  }

  @Test
  public void testTrinoReadParquetSqlWithColumns() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;

    List<String> columns = Arrays.asList("id", "name");
    String sql = dialect.readParquetSql("s3://bucket/warehouse/customers/*.parquet", columns);
    assertEquals("SELECT id, name FROM customers", sql);
  }

  @Test
  public void testTrinoDoesNotSupportDirectGlob() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;
    assertFalse(dialect.supportsDirectGlob());
  }

  @Test
  public void testTrinoSupportsIcebergViaCatalog() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;
    assertTrue(dialect.supportsIceberg());
  }

  @Test
  public void testTrinoReadIcebergReturnsNull() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;

    // readIcebergSql returns null because Trino requires catalog registration
    String sql = dialect.readIcebergSql("s3://bucket/warehouse/table", Collections.emptyList());
    assertNull(sql);
  }

  @Test
  public void testTrinoRegisterTableSql() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;

    String sql = dialect.registerTableSql("my_table", "s3://bucket/data/", "parquet");
    assertEquals("CREATE TABLE IF NOT EXISTS my_table WITH (external_location = 's3://bucket/data/', format = 'PARQUET')", sql);
  }

  @Test
  public void testTrinoName() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;
    assertEquals("Trino", dialect.getName());
  }

  // =========================================================================
  // Spark SQL Dialect Tests
  // =========================================================================

  @Test
  public void testSparkSqlDriverClassName() {
    JdbcDialect dialect = SparkSqlDialect.INSTANCE;
    assertEquals("org.apache.hive.jdbc.HiveDriver", dialect.getDriverClassName());
  }

  @Test
  public void testSparkSqlBuildJdbcUrlDefault() {
    JdbcDialect dialect = SparkSqlDialect.INSTANCE;

    String url = dialect.buildJdbcUrl(Collections.emptyMap());
    assertEquals("jdbc:hive2://localhost:10000/default", url);
  }

  @Test
  public void testSparkSqlBuildJdbcUrlWithConfig() {
    JdbcDialect dialect = SparkSqlDialect.INSTANCE;

    Map<String, String> config = new HashMap<String, String>();
    config.put("host", "spark.example.com");
    config.put("port", "10001");
    config.put("database", "analytics");

    String url = dialect.buildJdbcUrl(config);
    assertEquals("jdbc:hive2://spark.example.com:10001/analytics", url);
  }

  @Test
  public void testSparkSqlReadParquetSql() {
    JdbcDialect dialect = SparkSqlDialect.INSTANCE;

    String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", Collections.emptyList());
    assertEquals("SELECT * FROM parquet.`s3://bucket/data/*.parquet`", sql);
  }

  @Test
  public void testSparkSqlReadParquetSqlWithColumns() {
    JdbcDialect dialect = SparkSqlDialect.INSTANCE;

    List<String> columns = Arrays.asList("id", "name", "created_at");
    String sql = dialect.readParquetSql("hdfs:///data/events/*.parquet", columns);
    assertEquals("SELECT id, name, created_at FROM parquet.`hdfs:///data/events/*.parquet`", sql);
  }

  @Test
  public void testSparkSqlSupportsDirectGlob() {
    JdbcDialect dialect = SparkSqlDialect.INSTANCE;
    assertTrue(dialect.supportsDirectGlob());
  }

  @Test
  public void testSparkSqlSupportsIcebergViaCatalog() {
    JdbcDialect dialect = SparkSqlDialect.INSTANCE;
    assertTrue(dialect.supportsIceberg());
  }

  @Test
  public void testSparkSqlName() {
    JdbcDialect dialect = SparkSqlDialect.INSTANCE;
    assertEquals("Spark SQL", dialect.getName());
  }

  // =========================================================================
  // ClickHouse Dialect Tests
  // =========================================================================

  @Test
  public void testClickHouseDriverClassName() {
    JdbcDialect dialect = ClickHouseDialect.INSTANCE;
    assertEquals("com.clickhouse.jdbc.ClickHouseDriver", dialect.getDriverClassName());
  }

  @Test
  public void testClickHouseBuildJdbcUrlDefault() {
    JdbcDialect dialect = ClickHouseDialect.INSTANCE;

    String url = dialect.buildJdbcUrl(Collections.emptyMap());
    assertEquals("jdbc:clickhouse://localhost:8123/default", url);
  }

  @Test
  public void testClickHouseBuildJdbcUrlWithConfig() {
    JdbcDialect dialect = ClickHouseDialect.INSTANCE;

    Map<String, String> config = new HashMap<String, String>();
    config.put("host", "clickhouse.example.com");
    config.put("port", "8443");
    config.put("database", "analytics");

    String url = dialect.buildJdbcUrl(config);
    assertEquals("jdbc:clickhouse://clickhouse.example.com:8443/analytics", url);
  }

  @Test
  public void testClickHouseReadParquetSql() {
    JdbcDialect dialect = ClickHouseDialect.INSTANCE;

    String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", Collections.emptyList());
    assertEquals("SELECT * FROM s3('s3://bucket/data/*.parquet', 'Parquet')", sql);
  }

  @Test
  public void testClickHouseReadParquetSqlWithColumns() {
    JdbcDialect dialect = ClickHouseDialect.INSTANCE;

    List<String> columns = Arrays.asList("event_id", "user_id", "timestamp");
    String sql = dialect.readParquetSql("s3://bucket/events/*.parquet", columns);
    assertEquals("SELECT event_id, user_id, timestamp FROM s3('s3://bucket/events/*.parquet', 'Parquet')", sql);
  }

  @Test
  public void testClickHouseReadIcebergSql() {
    JdbcDialect dialect = ClickHouseDialect.INSTANCE;

    String sql = dialect.readIcebergSql("s3://bucket/warehouse/table", Collections.emptyList());
    assertEquals("SELECT * FROM iceberg('s3://bucket/warehouse/table')", sql);
  }

  @Test
  public void testClickHouseReadIcebergSqlWithColumns() {
    JdbcDialect dialect = ClickHouseDialect.INSTANCE;

    List<String> columns = Arrays.asList("id", "data");
    String sql = dialect.readIcebergSql("s3://bucket/warehouse/events", columns);
    assertEquals("SELECT id, data FROM iceberg('s3://bucket/warehouse/events')", sql);
  }

  @Test
  public void testClickHouseSupportsDirectGlob() {
    JdbcDialect dialect = ClickHouseDialect.INSTANCE;
    assertTrue(dialect.supportsDirectGlob());
  }

  @Test
  public void testClickHouseSupportsIceberg() {
    JdbcDialect dialect = ClickHouseDialect.INSTANCE;
    assertTrue(dialect.supportsIceberg());
  }

  @Test
  public void testClickHouseName() {
    JdbcDialect dialect = ClickHouseDialect.INSTANCE;
    assertEquals("ClickHouse", dialect.getName());
  }

  // =========================================================================
  // Factory Tests
  // =========================================================================

  @Test
  public void testFactoryCreatesDuckDB() {
    JdbcDialect dialect = JdbcDialectFactory.createDialect("duckdb");
    assertNotNull(dialect);
    assertEquals("DuckDB", dialect.getName());
  }

  @Test
  public void testFactoryCreatesTrino() {
    JdbcDialect dialect = JdbcDialectFactory.createDialect("trino");
    assertNotNull(dialect);
    assertEquals("Trino", dialect.getName());
  }

  @Test
  public void testFactoryCreatesSpark() {
    JdbcDialect dialect = JdbcDialectFactory.createDialect("spark");
    assertNotNull(dialect);
    assertEquals("Spark SQL", dialect.getName());
  }

  @Test
  public void testFactoryCreatesClickHouse() {
    JdbcDialect dialect = JdbcDialectFactory.createDialect("clickhouse");
    assertNotNull(dialect);
    assertEquals("ClickHouse", dialect.getName());
  }

  @Test
  public void testFactoryCaseInsensitive() {
    JdbcDialect d1 = JdbcDialectFactory.createDialect("DUCKDB");
    JdbcDialect d2 = JdbcDialectFactory.createDialect("DuckDB");
    JdbcDialect d3 = JdbcDialectFactory.createDialect("duckdb");

    assertEquals(d1.getName(), d2.getName());
    assertEquals(d2.getName(), d3.getName());
  }

  @Test
  public void testFactoryThrowsOnUnknownEngine() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() throws Throwable {
        JdbcDialectFactory.createDialect("unknown_engine");
      }
    });
  }

  @Test
  public void testFactoryThrowsOnNullEngine() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() throws Throwable {
        JdbcDialectFactory.createDialect(null);
      }
    });
  }

  @Test
  public void testFactoryThrowsOnEmptyEngine() {
    assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
      @Override
      public void execute() throws Throwable {
        JdbcDialectFactory.createDialect("");
      }
    });
  }

  @Test
  public void testFactoryIsSupported() {
    assertTrue(JdbcDialectFactory.isSupported("duckdb"));
    assertTrue(JdbcDialectFactory.isSupported("TRINO"));
    assertTrue(JdbcDialectFactory.isSupported("Spark"));
    assertTrue(JdbcDialectFactory.isSupported("ClickHouse"));
    assertFalse(JdbcDialectFactory.isSupported("mysql"));
    assertFalse(JdbcDialectFactory.isSupported(null));
    assertFalse(JdbcDialectFactory.isSupported(""));
  }

  @Test
  public void testFactoryGetSupportedEngineTypes() {
    String[] types = JdbcDialectFactory.getSupportedEngineTypes();
    assertEquals(4, types.length);

    List<String> typesList = Arrays.asList(types);
    assertTrue(typesList.contains("duckdb"));
    assertTrue(typesList.contains("trino"));
    assertTrue(typesList.contains("spark"));
    assertTrue(typesList.contains("clickhouse"));
  }

  // =========================================================================
  // Edge Case Tests
  // =========================================================================

  @Test
  public void testReadParquetWithNullColumns() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    // null columns should be treated as all columns
    String sql = dialect.readParquetSql("test.parquet", null);
    assertEquals("SELECT * FROM read_parquet('test.parquet')", sql);
  }

  @Test
  public void testReadParquetWithSingleColumn() {
    JdbcDialect dialect = SparkSqlDialect.INSTANCE;

    List<String> columns = Collections.singletonList("id");
    String sql = dialect.readParquetSql("data.parquet", columns);
    assertEquals("SELECT id FROM parquet.`data.parquet`", sql);
  }

  @Test
  public void testTrinoTableNameFromDeepPath() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;

    // Should extract 'events' from the path
    String sql = dialect.readParquetSql("s3://bucket/warehouse/db/schema/events/*.parquet",
                                         Collections.emptyList());
    assertEquals("SELECT * FROM events", sql);
  }

  @Test
  public void testTrinoTableNameFromPathWithSpecialChars() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;

    // Special characters should be replaced with underscores
    String sql = dialect.readParquetSql("s3://bucket/my-table-2024/*.parquet",
                                         Collections.emptyList());
    assertEquals("SELECT * FROM my_table_2024", sql);
  }

  // =========================================================================
  // View Creation Tests
  // =========================================================================

  @Test
  public void testDuckDBCreateParquetViewSql() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    String sql = dialect.createParquetViewSql("myschema", "mytable",
        "s3://bucket/data/*.parquet", false);
    assertEquals(
        "CREATE VIEW IF NOT EXISTS \"myschema\".\"mytable\" AS SELECT * FROM read_parquet('s3://bucket/data/*.parquet')",
        sql);
  }

  @Test
  public void testDuckDBCreateParquetViewSqlWithHivePartitioning() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    String sql = dialect.createParquetViewSql("myschema", "mytable",
        "s3://bucket/data/**/*.parquet", true);
    assertEquals(
        "CREATE VIEW IF NOT EXISTS \"myschema\".\"mytable\" AS SELECT * FROM read_parquet('s3://bucket/data/**/*.parquet', hive_partitioning = true)",
        sql);
  }

  @Test
  public void testDuckDBCreateParquetViewSqlNoSchema() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    String sql = dialect.createParquetViewSql(null, "mytable",
        "s3://bucket/data/*.parquet", false);
    assertEquals(
        "CREATE VIEW IF NOT EXISTS \"mytable\" AS SELECT * FROM read_parquet('s3://bucket/data/*.parquet')",
        sql);
  }

  @Test
  public void testDuckDBCreateIcebergViewSql() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    String sql = dialect.createIcebergViewSql("myschema", "mytable",
        "s3://bucket/warehouse/table");
    assertEquals(
        "CREATE VIEW IF NOT EXISTS \"myschema\".\"mytable\" AS SELECT * FROM iceberg_scan('s3://bucket/warehouse/table')",
        sql);
  }

  @Test
  public void testDuckDBCreateOrReplaceParquetViewSql() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    String sql = dialect.createOrReplaceParquetViewSql("myschema", "mytable",
        "s3://bucket/data/*.parquet", false);
    assertEquals(
        "CREATE OR REPLACE VIEW \"myschema\".\"mytable\" AS SELECT * FROM read_parquet('s3://bucket/data/*.parquet')",
        sql);
  }

  @Test
  public void testTrinoCreateParquetViewSql() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;

    String sql = dialect.createParquetViewSql("myschema", "mytable",
        "s3://bucket/data/", false);
    assertEquals(
        "CREATE TABLE IF NOT EXISTS \"myschema\".\"mytable\" WITH (external_location = 's3://bucket/data/', format = 'PARQUET')",
        sql);
  }

  @Test
  public void testTrinoCreateIcebergViewSql() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;

    String sql = dialect.createIcebergViewSql("myschema", "mytable",
        "s3://bucket/warehouse/table");
    assertEquals(
        "CALL iceberg.system.register_table('myschema', 'mytable', 's3://bucket/warehouse/table')",
        sql);
  }

  @Test
  public void testTrinoDropViewSql() {
    JdbcDialect dialect = TrinoDialect.INSTANCE;

    String sql = dialect.dropViewSql("myschema", "mytable");
    assertEquals("DROP TABLE IF EXISTS \"myschema\".\"mytable\"", sql);
  }

  @Test
  public void testSparkSqlCreateParquetViewSql() {
    JdbcDialect dialect = SparkSqlDialect.INSTANCE;

    String sql = dialect.createParquetViewSql("myschema", "mytable",
        "s3://bucket/data/*.parquet", false);
    assertEquals(
        "CREATE OR REPLACE VIEW \"myschema\".\"mytable\" AS SELECT * FROM parquet.`s3://bucket/data/*.parquet`",
        sql);
  }

  @Test
  public void testSparkSqlCreateIcebergViewSql() {
    JdbcDialect dialect = SparkSqlDialect.INSTANCE;

    String sql = dialect.createIcebergViewSql("myschema", "mytable",
        "s3://bucket/warehouse/table");
    assertEquals(
        "CREATE OR REPLACE VIEW \"myschema\".\"mytable\" AS SELECT * FROM iceberg.`s3://bucket/warehouse/table`",
        sql);
  }

  @Test
  public void testClickHouseCreateParquetViewSqlS3() {
    JdbcDialect dialect = ClickHouseDialect.INSTANCE;

    String sql = dialect.createParquetViewSql("myschema", "mytable",
        "s3://bucket/data/*.parquet", false);
    assertEquals(
        "CREATE OR REPLACE VIEW \"myschema\".\"mytable\" AS SELECT * FROM s3('s3://bucket/data/*.parquet', 'Parquet')",
        sql);
  }

  @Test
  public void testClickHouseCreateParquetViewSqlLocal() {
    JdbcDialect dialect = ClickHouseDialect.INSTANCE;

    String sql = dialect.createParquetViewSql("myschema", "mytable",
        "/data/files/*.parquet", false);
    assertEquals(
        "CREATE OR REPLACE VIEW \"myschema\".\"mytable\" AS SELECT * FROM file('/data/files/*.parquet', 'Parquet')",
        sql);
  }

  @Test
  public void testClickHouseCreateIcebergViewSql() {
    JdbcDialect dialect = ClickHouseDialect.INSTANCE;

    String sql = dialect.createIcebergViewSql("myschema", "mytable",
        "s3://bucket/warehouse/table");
    assertEquals(
        "CREATE OR REPLACE VIEW \"myschema\".\"mytable\" AS SELECT * FROM iceberg('s3://bucket/warehouse/table')",
        sql);
  }

  @Test
  public void testDefaultDropViewSql() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    String sql = dialect.dropViewSql("myschema", "mytable");
    assertEquals("DROP VIEW IF EXISTS \"myschema\".\"mytable\"", sql);
  }

  @Test
  public void testDefaultCreateSchemaSql() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    String sql = dialect.createSchemaSql("myschema");
    assertEquals("CREATE SCHEMA IF NOT EXISTS \"myschema\"", sql);
  }

  @Test
  public void testQualifyNameWithSchema() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    String name = dialect.qualifyName("myschema", "mytable");
    assertEquals("\"myschema\".\"mytable\"", name);
  }

  @Test
  public void testQualifyNameWithoutSchema() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    String name = dialect.qualifyName(null, "mytable");
    assertEquals("\"mytable\"", name);
  }

  @Test
  public void testQualifyNameWithEmptySchema() {
    JdbcDialect dialect = DuckDBDialect.INSTANCE;

    String name = dialect.qualifyName("", "mytable");
    assertEquals("\"mytable\"", name);
  }
}
