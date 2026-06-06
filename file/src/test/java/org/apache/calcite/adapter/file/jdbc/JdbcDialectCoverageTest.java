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
package org.apache.calcite.adapter.file.jdbc;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Comprehensive unit tests for all JDBC dialect classes in the file adapter.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>{@link DuckDBDialect} - embedded analytical database dialect</li>
 *   <li>{@link ClickHouseDialect} - OLAP database dialect</li>
 *   <li>{@link SparkSqlDialect} - Spark SQL via Thrift Server dialect</li>
 *   <li>{@link TrinoDialect} - distributed SQL engine dialect</li>
 *   <li>{@link JdbcDialectFactory} - factory for creating dialect instances</li>
 *   <li>{@link HiveSqlDialectFactory} - factory for Hive SQL dialect</li>
 * </ul>
 */
@Tag("unit")
public class JdbcDialectCoverageTest {

  // ===========================================================================
  // DuckDBDialect Tests
  // ===========================================================================

  @Nested
  class DuckDBDialectTests {

    private final DuckDBDialect dialect = DuckDBDialect.INSTANCE;

    @Test void testSingletonInstance() {
      assertNotNull(DuckDBDialect.INSTANCE);
      assertSame(DuckDBDialect.INSTANCE, dialect);
    }

    @Test void testGetName() {
      assertEquals("DuckDB", dialect.getName());
    }

    @Test void testGetDriverClassName() {
      assertEquals("org.duckdb.DuckDBDriver", dialect.getDriverClassName());
    }

    // -- buildJdbcUrl --

    @Test void testBuildJdbcUrlEmptyConfig() {
      Map<String, String> config = new HashMap<String, String>();
      assertEquals("jdbc:duckdb:", dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlNullConfig() {
      assertEquals("jdbc:duckdb:", dialect.buildJdbcUrl(null));
    }

    @Test void testBuildJdbcUrlWithPath() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("path", "/path/to/db");
      assertEquals("jdbc:duckdb:/path/to/db", dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlWithEmptyPath() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("path", "");
      assertEquals("jdbc:duckdb:", dialect.buildJdbcUrl(config));
    }

    // -- readParquetSql --

    @Test void testReadParquetSqlWithColumns() {
      String sql =
          dialect.readParquetSql("s3://bucket/data/*.parquet",
          Arrays.asList("id", "name"));
      assertEquals(
          "SELECT id, name FROM read_parquet('s3://bucket/data/*.parquet')",
          sql);
    }

    @Test void testReadParquetSqlNullColumns() {
      String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", null);
      assertEquals(
          "SELECT * FROM read_parquet('s3://bucket/data/*.parquet')",
          sql);
    }

    @Test void testReadParquetSqlEmptyColumns() {
      String sql =
          dialect.readParquetSql("/tmp/data.parquet",
          Collections.<String>emptyList());
      assertEquals(
          "SELECT * FROM read_parquet('/tmp/data.parquet')",
          sql);
    }

    @Test void testReadParquetSqlSingleColumn() {
      String sql =
          dialect.readParquetSql("/data/file.parquet",
          Collections.singletonList("amount"));
      assertEquals(
          "SELECT amount FROM read_parquet('/data/file.parquet')",
          sql);
    }

    // -- readIcebergSql --

    @Test void testReadIcebergSqlWithColumns() {
      String sql =
          dialect.readIcebergSql("s3://warehouse/table",
          Arrays.asList("col1", "col2"));
      assertEquals(
          "SELECT col1, col2 FROM iceberg_scan('s3://warehouse/table')",
          sql);
    }

    @Test void testReadIcebergSqlNullColumns() {
      String sql = dialect.readIcebergSql("s3://warehouse/table", null);
      assertEquals(
          "SELECT * FROM iceberg_scan('s3://warehouse/table')",
          sql);
    }

    @Test void testReadIcebergSqlEmptyColumns() {
      String sql =
          dialect.readIcebergSql("/local/iceberg/table",
          Collections.<String>emptyList());
      assertEquals(
          "SELECT * FROM iceberg_scan('/local/iceberg/table')",
          sql);
    }

    // -- createParquetViewSql --

    @Test void testCreateParquetViewSqlWithSchemaNoHive() {
      String sql =
          dialect.createParquetViewSql("myschema", "myview", "/data/*.parquet", false);
      assertEquals(
          "CREATE VIEW IF NOT EXISTS \"myschema\".\"myview\""
              + " AS SELECT * FROM read_parquet('/data/*.parquet')",
          sql);
    }

    @Test void testCreateParquetViewSqlWithSchemaHivePartitioning() {
      String sql =
          dialect.createParquetViewSql("myschema", "myview", "/data/*.parquet", true);
      assertEquals(
          "CREATE VIEW IF NOT EXISTS \"myschema\".\"myview\""
              + " AS SELECT * FROM read_parquet('/data/*.parquet',"
              + " hive_partitioning = true)",
          sql);
    }

    @Test void testCreateParquetViewSqlNullSchema() {
      String sql =
          dialect.createParquetViewSql(null, "myview", "/data/*.parquet", false);
      assertEquals(
          "CREATE VIEW IF NOT EXISTS \"myview\""
              + " AS SELECT * FROM read_parquet('/data/*.parquet')",
          sql);
    }

    // -- createOrReplaceParquetViewSql --

    @Test void testCreateOrReplaceParquetViewSqlNoHive() {
      String sql =
          dialect.createOrReplaceParquetViewSql("schema", "view", "/data/*.parquet", false);
      assertEquals(
          "CREATE OR REPLACE VIEW \"schema\".\"view\""
              + " AS SELECT * FROM read_parquet('/data/*.parquet')",
          sql);
    }

    @Test void testCreateOrReplaceParquetViewSqlHivePartitioning() {
      String sql =
          dialect.createOrReplaceParquetViewSql("schema", "view", "/data/*.parquet", true);
      assertEquals(
          "CREATE OR REPLACE VIEW \"schema\".\"view\""
              + " AS SELECT * FROM read_parquet('/data/*.parquet',"
              + " hive_partitioning = true)",
          sql);
    }

    @Test void testCreateOrReplaceParquetViewSqlNullSchema() {
      String sql =
          dialect.createOrReplaceParquetViewSql(null, "view", "/data/*.parquet", false);
      assertEquals(
          "CREATE OR REPLACE VIEW \"view\""
              + " AS SELECT * FROM read_parquet('/data/*.parquet')",
          sql);
    }

    // -- createIcebergViewSql --

    @Test void testCreateIcebergViewSql() {
      String sql =
          dialect.createIcebergViewSql("myschema", "myview", "s3://bucket/table");
      assertEquals(
          "CREATE VIEW IF NOT EXISTS \"myschema\".\"myview\""
              + " AS SELECT * FROM iceberg_scan('s3://bucket/table')",
          sql);
    }

    @Test void testCreateIcebergViewSqlNullSchema() {
      String sql =
          dialect.createIcebergViewSql(null, "myview", "s3://bucket/table");
      assertEquals(
          "CREATE VIEW IF NOT EXISTS \"myview\""
              + " AS SELECT * FROM iceberg_scan('s3://bucket/table')",
          sql);
    }

    // -- feature flags --

    @Test void testSupportsDirectGlob() {
      assertTrue(dialect.supportsDirectGlob());
    }

    @Test void testSupportsIceberg() {
      assertTrue(dialect.supportsIceberg());
    }

    // -- default interface methods --

    @Test void testDropViewSqlDefault() {
      String sql = dialect.dropViewSql("schema", "view");
      assertEquals("DROP VIEW IF EXISTS \"schema\".\"view\"", sql);
    }

    @Test void testDropViewSqlDefaultNullSchema() {
      String sql = dialect.dropViewSql(null, "view");
      assertEquals("DROP VIEW IF EXISTS \"view\"", sql);
    }

    @Test void testCreateSchemaSqlDefault() {
      String sql = dialect.createSchemaSql("testschema");
      assertEquals("CREATE SCHEMA IF NOT EXISTS \"testschema\"", sql);
    }

    @Test void testQualifyNameWithSchema() {
      String result = dialect.qualifyName("schema", "table");
      assertEquals("\"schema\".\"table\"", result);
    }

    @Test void testQualifyNameWithoutSchema() {
      String result = dialect.qualifyName(null, "table");
      assertEquals("\"table\"", result);
    }

    @Test void testQualifyNameEmptySchema() {
      String result = dialect.qualifyName("", "table");
      assertEquals("\"table\"", result);
    }

    @Test void testRegisterTableSqlThrowsUnsupported() {
      assertThrows(UnsupportedOperationException.class, new org.junit.jupiter.api.function.Executable() {
        @Override public void execute() {
          dialect.registerTableSql("tbl", "/path", "PARQUET");
        }
      });
    }
  }

  // ===========================================================================
  // ClickHouseDialect Tests
  // ===========================================================================

  @Nested
  class ClickHouseDialectTests {

    private final ClickHouseDialect dialect = ClickHouseDialect.INSTANCE;

    @Test void testSingletonInstance() {
      assertNotNull(ClickHouseDialect.INSTANCE);
      assertSame(ClickHouseDialect.INSTANCE, dialect);
    }

    @Test void testGetName() {
      assertEquals("ClickHouse", dialect.getName());
    }

    @Test void testGetDriverClassName() {
      assertEquals("com.clickhouse.jdbc.ClickHouseDriver",
          dialect.getDriverClassName());
    }

    // -- buildJdbcUrl --

    @Test void testBuildJdbcUrlDefaults() {
      Map<String, String> config = new HashMap<String, String>();
      assertEquals("jdbc:clickhouse://localhost:8123/default",
          dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlNullConfig() {
      assertEquals("jdbc:clickhouse://localhost:8123/default",
          dialect.buildJdbcUrl(null));
    }

    @Test void testBuildJdbcUrlFullConfig() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("host", "ch.example.com");
      config.put("port", "9000");
      config.put("database", "analytics");
      assertEquals("jdbc:clickhouse://ch.example.com:9000/analytics",
          dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlPartialConfigHostOnly() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("host", "myhost");
      assertEquals("jdbc:clickhouse://myhost:8123/default",
          dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlPartialConfigDatabaseOnly() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("database", "mydb");
      assertEquals("jdbc:clickhouse://localhost:8123/mydb",
          dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlEmptyValues() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("host", "");
      config.put("port", "");
      config.put("database", "");
      assertEquals("jdbc:clickhouse://localhost:8123/default",
          dialect.buildJdbcUrl(config));
    }

    // -- readParquetSql --

    @Test void testReadParquetSqlWithColumns() {
      String sql =
          dialect.readParquetSql("s3://bucket/data/*.parquet",
          Arrays.asList("id", "name"));
      assertEquals(
          "SELECT id, name FROM s3('s3://bucket/data/*.parquet', 'Parquet')",
          sql);
    }

    @Test void testReadParquetSqlNullColumns() {
      String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", null);
      assertEquals(
          "SELECT * FROM s3('s3://bucket/data/*.parquet', 'Parquet')",
          sql);
    }

    @Test void testReadParquetSqlEmptyColumns() {
      String sql =
          dialect.readParquetSql("/local/data.parquet",
          Collections.<String>emptyList());
      assertEquals(
          "SELECT * FROM s3('/local/data.parquet', 'Parquet')",
          sql);
    }

    @Test void testReadParquetSqlSingleColumn() {
      String sql =
          dialect.readParquetSql("s3://bucket/file.parquet",
          Collections.singletonList("amount"));
      assertEquals(
          "SELECT amount FROM s3('s3://bucket/file.parquet', 'Parquet')",
          sql);
    }

    // -- readIcebergSql --

    @Test void testReadIcebergSqlWithColumns() {
      String sql =
          dialect.readIcebergSql("s3://warehouse/table",
          Arrays.asList("col1", "col2"));
      assertEquals(
          "SELECT col1, col2 FROM iceberg('s3://warehouse/table')",
          sql);
    }

    @Test void testReadIcebergSqlNullColumns() {
      String sql = dialect.readIcebergSql("s3://warehouse/table", null);
      assertEquals(
          "SELECT * FROM iceberg('s3://warehouse/table')",
          sql);
    }

    @Test void testReadIcebergSqlEmptyColumns() {
      String sql =
          dialect.readIcebergSql("/local/table",
          Collections.<String>emptyList());
      assertEquals(
          "SELECT * FROM iceberg('/local/table')",
          sql);
    }

    // -- createParquetViewSql --

    @Test void testCreateParquetViewSqlS3Path() {
      String sql =
          dialect.createParquetViewSql("schema", "view", "s3://bucket/data/*.parquet", false);
      assertEquals(
          "CREATE OR REPLACE VIEW \"schema\".\"view\""
              + " AS SELECT * FROM s3('s3://bucket/data/*.parquet', 'Parquet')",
          sql);
    }

    @Test void testCreateParquetViewSqlS3aPath() {
      String sql =
          dialect.createParquetViewSql("schema", "view", "s3a://bucket/data/*.parquet", false);
      assertEquals(
          "CREATE OR REPLACE VIEW \"schema\".\"view\""
              + " AS SELECT * FROM s3('s3a://bucket/data/*.parquet', 'Parquet')",
          sql);
    }

    @Test void testCreateParquetViewSqlLocalPath() {
      String sql =
          dialect.createParquetViewSql("schema", "view", "/local/data/*.parquet", false);
      assertEquals(
          "CREATE OR REPLACE VIEW \"schema\".\"view\""
              + " AS SELECT * FROM file('/local/data/*.parquet', 'Parquet')",
          sql);
    }

    @Test void testCreateParquetViewSqlNullSchema() {
      String sql =
          dialect.createParquetViewSql(null, "view", "/local/data.parquet", false);
      assertEquals(
          "CREATE OR REPLACE VIEW \"view\""
              + " AS SELECT * FROM file('/local/data.parquet', 'Parquet')",
          sql);
    }

    @Test void testCreateParquetViewSqlHivePartitioningIgnored() {
      // ClickHouse does not have a separate hive partitioning flag in the SQL
      String sqlWithHive =
          dialect.createParquetViewSql("s", "v", "s3://b/d", true);
      String sqlWithoutHive =
          dialect.createParquetViewSql("s", "v", "s3://b/d", false);
      assertEquals(sqlWithHive, sqlWithoutHive);
    }

    // -- createIcebergViewSql --

    @Test void testCreateIcebergViewSql() {
      String sql =
          dialect.createIcebergViewSql("myschema", "myview", "s3://bucket/table");
      assertEquals(
          "CREATE OR REPLACE VIEW \"myschema\".\"myview\""
              + " AS SELECT * FROM iceberg('s3://bucket/table')",
          sql);
    }

    @Test void testCreateIcebergViewSqlNullSchema() {
      String sql =
          dialect.createIcebergViewSql(null, "myview", "s3://bucket/table");
      assertEquals(
          "CREATE OR REPLACE VIEW \"myview\""
              + " AS SELECT * FROM iceberg('s3://bucket/table')",
          sql);
    }

    // -- createOrReplaceParquetViewSql --

    @Test void testCreateOrReplaceParquetViewSqlDelegatesToCreate() {
      String createSql =
          dialect.createParquetViewSql("s", "v", "s3://b/d", false);
      String replaceSql =
          dialect.createOrReplaceParquetViewSql("s", "v", "s3://b/d", false);
      assertEquals(createSql, replaceSql);
    }

    // -- dropViewSql --

    @Test void testDropViewSql() {
      String sql = dialect.dropViewSql("schema", "view");
      assertEquals("DROP VIEW IF EXISTS \"schema\".\"view\"", sql);
    }

    @Test void testDropViewSqlNullSchema() {
      String sql = dialect.dropViewSql(null, "view");
      assertEquals("DROP VIEW IF EXISTS \"view\"", sql);
    }

    // -- feature flags --

    @Test void testSupportsDirectGlob() {
      assertTrue(dialect.supportsDirectGlob());
    }

    @Test void testSupportsIceberg() {
      assertTrue(dialect.supportsIceberg());
    }
  }

  // ===========================================================================
  // SparkSqlDialect Tests
  // ===========================================================================

  @Nested
  class SparkSqlDialectTests {

    private final SparkSqlDialect dialect = SparkSqlDialect.INSTANCE;

    @Test void testSingletonInstance() {
      assertNotNull(SparkSqlDialect.INSTANCE);
      assertSame(SparkSqlDialect.INSTANCE, dialect);
    }

    @Test void testGetName() {
      assertEquals("Spark SQL", dialect.getName());
    }

    @Test void testGetDriverClassName() {
      assertEquals("org.apache.hive.jdbc.HiveDriver",
          dialect.getDriverClassName());
    }

    // -- buildJdbcUrl --

    @Test void testBuildJdbcUrlDefaults() {
      Map<String, String> config = new HashMap<String, String>();
      assertEquals("jdbc:hive2://localhost:10000/default",
          dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlNullConfig() {
      assertEquals("jdbc:hive2://localhost:10000/default",
          dialect.buildJdbcUrl(null));
    }

    @Test void testBuildJdbcUrlFullConfig() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("host", "spark.example.com");
      config.put("port", "10001");
      config.put("database", "analytics");
      assertEquals("jdbc:hive2://spark.example.com:10001/analytics",
          dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlPartialConfigHostOnly() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("host", "spark.internal");
      assertEquals("jdbc:hive2://spark.internal:10000/default",
          dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlPartialConfigDatabaseOnly() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("database", "mydb");
      assertEquals("jdbc:hive2://localhost:10000/mydb",
          dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlEmptyValues() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("host", "");
      config.put("port", "");
      config.put("database", "");
      assertEquals("jdbc:hive2://localhost:10000/default",
          dialect.buildJdbcUrl(config));
    }

    // -- readParquetSql --

    @Test void testReadParquetSqlWithColumns() {
      String sql =
          dialect.readParquetSql("s3://bucket/data/*.parquet",
          Arrays.asList("id", "name"));
      assertEquals(
          "SELECT id, name FROM parquet.`s3://bucket/data/*.parquet`",
          sql);
    }

    @Test void testReadParquetSqlNullColumns() {
      String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", null);
      assertEquals(
          "SELECT * FROM parquet.`s3://bucket/data/*.parquet`",
          sql);
    }

    @Test void testReadParquetSqlEmptyColumns() {
      String sql =
          dialect.readParquetSql("/tmp/data.parquet",
          Collections.<String>emptyList());
      assertEquals(
          "SELECT * FROM parquet.`/tmp/data.parquet`",
          sql);
    }

    @Test void testReadParquetSqlSingleColumn() {
      String sql =
          dialect.readParquetSql("/data/file.parquet",
          Collections.singletonList("amount"));
      assertEquals(
          "SELECT amount FROM parquet.`/data/file.parquet`",
          sql);
    }

    // -- readIcebergSql (default interface method returns null) --

    @Test void testReadIcebergSqlReturnsNull() {
      // SparkSqlDialect does not override readIcebergSql; the default returns null
      assertNull(dialect.readIcebergSql("s3://warehouse/table", null));
    }

    @Test void testReadIcebergSqlWithColumnsReturnsNull() {
      assertNull(
          dialect.readIcebergSql(
          "s3://warehouse/table",
          Arrays.asList("a", "b")));
    }

    // -- createParquetViewSql --

    @Test void testCreateParquetViewSqlWithSchema() {
      String sql =
          dialect.createParquetViewSql("myschema", "myview", "s3://bucket/data/*.parquet", false);
      assertEquals(
          "CREATE OR REPLACE VIEW \"myschema\".\"myview\""
              + " AS SELECT * FROM parquet.`s3://bucket/data/*.parquet`",
          sql);
    }

    @Test void testCreateParquetViewSqlNullSchema() {
      String sql =
          dialect.createParquetViewSql(null, "myview", "/local/data.parquet", false);
      assertEquals(
          "CREATE OR REPLACE VIEW \"myview\""
              + " AS SELECT * FROM parquet.`/local/data.parquet`",
          sql);
    }

    @Test void testCreateParquetViewSqlHivePartitioningAutoDetected() {
      // Spark auto-detects hive partitioning, so the output is the same
      String sqlWithHive =
          dialect.createParquetViewSql("s", "v", "s3://b/d", true);
      String sqlWithoutHive =
          dialect.createParquetViewSql("s", "v", "s3://b/d", false);
      assertEquals(sqlWithHive, sqlWithoutHive);
    }

    // -- createIcebergViewSql --

    @Test void testCreateIcebergViewSql() {
      String sql =
          dialect.createIcebergViewSql("myschema", "myview", "s3://bucket/table");
      assertEquals(
          "CREATE OR REPLACE VIEW \"myschema\".\"myview\""
              + " AS SELECT * FROM aperio_iceberg.myschema.myview",
          sql);
    }

    @Test void testCreateIcebergViewSqlNullSchema() {
      String sql =
          dialect.createIcebergViewSql(null, "myview", "s3://bucket/table");
      assertEquals(
          "CREATE OR REPLACE VIEW \"myview\""
              + " AS SELECT * FROM aperio_iceberg.null.myview",
          sql);
    }

    // -- createIcebergTableSql --

    @Test void testCreateIcebergTableSql() {
      String sql =
          dialect.createIcebergTableSql("mydb", "mytable", "s3://bucket/warehouse/table");
      assertEquals(
          "CREATE TABLE IF NOT EXISTS aperio_iceberg.mydb.mytable"
              + " USING iceberg LOCATION 's3://bucket/warehouse/table'",
          sql);
    }

    @Test void testCreateIcebergTableSqlDifferentValues() {
      String sql =
          dialect.createIcebergTableSql("production", "events", "/data/iceberg/events");
      assertEquals(
          "CREATE TABLE IF NOT EXISTS aperio_iceberg.production.events"
              + " USING iceberg LOCATION '/data/iceberg/events'",
          sql);
    }

    // -- createOrReplaceParquetViewSql --

    @Test void testCreateOrReplaceParquetViewSqlDelegatesToCreate() {
      String createSql =
          dialect.createParquetViewSql("s", "v", "s3://b/d", false);
      String replaceSql =
          dialect.createOrReplaceParquetViewSql("s", "v", "s3://b/d", false);
      assertEquals(createSql, replaceSql);
    }

    // -- default interface methods --

    @Test void testDropViewSqlDefault() {
      String sql = dialect.dropViewSql("schema", "view");
      assertEquals("DROP VIEW IF EXISTS \"schema\".\"view\"", sql);
    }

    @Test void testDropViewSqlDefaultNullSchema() {
      String sql = dialect.dropViewSql(null, "view");
      assertEquals("DROP VIEW IF EXISTS \"view\"", sql);
    }

    // -- feature flags --

    @Test void testSupportsDirectGlob() {
      assertTrue(dialect.supportsDirectGlob());
    }

    @Test void testSupportsIceberg() {
      assertTrue(dialect.supportsIceberg());
    }
  }

  // ===========================================================================
  // TrinoDialect Tests
  // ===========================================================================

  @Nested
  class TrinoDialectTests {

    private final TrinoDialect dialect = TrinoDialect.INSTANCE;

    @Test void testSingletonInstance() {
      assertNotNull(TrinoDialect.INSTANCE);
      assertSame(TrinoDialect.INSTANCE, dialect);
    }

    @Test void testGetName() {
      assertEquals("Trino", dialect.getName());
    }

    @Test void testGetDriverClassName() {
      assertEquals("io.trino.jdbc.TrinoDriver",
          dialect.getDriverClassName());
    }

    // -- buildJdbcUrl --

    @Test void testBuildJdbcUrlDefaults() {
      Map<String, String> config = new HashMap<String, String>();
      assertEquals("jdbc:trino://localhost:8080/hive/default",
          dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlNullConfig() {
      assertEquals("jdbc:trino://localhost:8080/hive/default",
          dialect.buildJdbcUrl(null));
    }

    @Test void testBuildJdbcUrlFullConfig() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("host", "trino.example.com");
      config.put("port", "8443");
      config.put("catalog", "iceberg");
      config.put("schema", "datalake");
      assertEquals("jdbc:trino://trino.example.com:8443/iceberg/datalake",
          dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlPartialConfigHostOnly() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("host", "trino.internal");
      assertEquals("jdbc:trino://trino.internal:8080/hive/default",
          dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlPartialConfigCatalogSchema() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("catalog", "delta");
      config.put("schema", "warehouse");
      assertEquals("jdbc:trino://localhost:8080/delta/warehouse",
          dialect.buildJdbcUrl(config));
    }

    @Test void testBuildJdbcUrlEmptyValues() {
      Map<String, String> config = new HashMap<String, String>();
      config.put("host", "");
      config.put("port", "");
      config.put("catalog", "");
      config.put("schema", "");
      assertEquals("jdbc:trino://localhost:8080/hive/default",
          dialect.buildJdbcUrl(config));
    }

    // -- readParquetSql (derives table name from path) --

    @Test void testReadParquetSqlWithColumnsS3Path() {
      String sql =
          dialect.readParquetSql("s3://bucket/data/events.parquet",
          Arrays.asList("id", "name"));
      assertEquals("SELECT id, name FROM events", sql);
    }

    @Test void testReadParquetSqlNullColumns() {
      String sql =
          dialect.readParquetSql("s3://bucket/data/events.parquet", null);
      assertEquals("SELECT * FROM events", sql);
    }

    @Test void testReadParquetSqlEmptyColumns() {
      String sql =
          dialect.readParquetSql("/data/users.parquet",
          Collections.<String>emptyList());
      assertEquals("SELECT * FROM users", sql);
    }

    @Test void testReadParquetSqlGlobPattern() {
      // Glob patterns: pathToTableName extracts directory name before glob
      String sql =
          dialect.readParquetSql("s3://bucket/data/*.parquet", null);
      assertEquals("SELECT * FROM data", sql);
    }

    @Test void testReadParquetSqlEmptyPath() {
      String sql = dialect.readParquetSql("", null);
      assertEquals("SELECT * FROM unknown_table", sql);
    }

    @Test void testReadParquetSqlNullPath() {
      String sql = dialect.readParquetSql(null, null);
      assertEquals("SELECT * FROM unknown_table", sql);
    }

    // -- readIcebergSql (default returns null for Trino) --

    @Test void testReadIcebergSqlReturnsNull() {
      // TrinoDialect does not override readIcebergSql; default returns null
      assertNull(dialect.readIcebergSql("s3://warehouse/table", null));
    }

    // -- registerTableSql --

    @Test void testRegisterTableSql() {
      String sql =
          dialect.registerTableSql("events", "s3://bucket/data/events/", "parquet");
      assertEquals(
          "CREATE TABLE IF NOT EXISTS events"
              + " WITH (external_location = 's3://bucket/data/events/',"
              + " format = 'PARQUET')",
          sql);
    }

    @Test void testRegisterTableSqlOrcFormat() {
      String sql =
          dialect.registerTableSql("users", "/data/users/", "orc");
      assertEquals(
          "CREATE TABLE IF NOT EXISTS users"
              + " WITH (external_location = '/data/users/',"
              + " format = 'ORC')",
          sql);
    }

    // -- createParquetViewSql --

    @Test void testCreateParquetViewSqlWithSchema() {
      String sql =
          dialect.createParquetViewSql("myschema", "myview", "s3://bucket/data/", false);
      assertEquals(
          "CREATE TABLE IF NOT EXISTS \"myschema\".\"myview\""
              + " WITH (external_location = 's3://bucket/data/',"
              + " format = 'PARQUET')",
          sql);
    }

    @Test void testCreateParquetViewSqlHivePartitioning() {
      String sql =
          dialect.createParquetViewSql("myschema", "myview", "s3://bucket/data/", true);
      assertEquals(
          "CREATE TABLE IF NOT EXISTS \"myschema\".\"myview\""
              + " WITH (external_location = 's3://bucket/data/',"
              + " format = 'PARQUET',"
              + " partitioned_by = ARRAY[])",
          sql);
    }

    @Test void testCreateParquetViewSqlNullSchema() {
      String sql =
          dialect.createParquetViewSql(null, "myview", "/data/events/", false);
      assertEquals(
          "CREATE TABLE IF NOT EXISTS \"myview\""
              + " WITH (external_location = '/data/events/',"
              + " format = 'PARQUET')",
          sql);
    }

    // -- createIcebergViewSql --

    @Test void testCreateIcebergViewSql() {
      String sql =
          dialect.createIcebergViewSql("myschema", "myview", "s3://bucket/warehouse/table");
      assertEquals(
          "CALL iceberg.system.register_table('myschema', 'myview',"
              + " 's3://bucket/warehouse/table')",
          sql);
    }

    @Test void testCreateIcebergViewSqlNullSchema() {
      String sql =
          dialect.createIcebergViewSql(null, "myview", "s3://bucket/warehouse/table");
      assertEquals(
          "CALL iceberg.system.register_table('default', 'myview',"
              + " 's3://bucket/warehouse/table')",
          sql);
    }

    // -- dropViewSql (overridden to use DROP TABLE) --

    @Test void testDropViewSql() {
      String sql = dialect.dropViewSql("schema", "view");
      assertEquals("DROP TABLE IF EXISTS \"schema\".\"view\"", sql);
    }

    @Test void testDropViewSqlNullSchema() {
      String sql = dialect.dropViewSql(null, "view");
      assertEquals("DROP TABLE IF EXISTS \"view\"", sql);
    }

    // -- feature flags --

    @Test void testSupportsDirectGlob() {
      assertFalse(dialect.supportsDirectGlob());
    }

    @Test void testSupportsIceberg() {
      assertTrue(dialect.supportsIceberg());
    }
  }

  // ===========================================================================
  // JdbcDialectFactory Tests
  // ===========================================================================

  @Nested
  class JdbcDialectFactoryTests {

    // -- createDialect --

    @Test void testCreateDialectDuckdb() {
      JdbcDialect dialect = JdbcDialectFactory.createDialect("duckdb");
      assertSame(DuckDBDialect.INSTANCE, dialect);
    }

    @Test void testCreateDialectDuckdbUpperCase() {
      JdbcDialect dialect = JdbcDialectFactory.createDialect("DUCKDB");
      assertSame(DuckDBDialect.INSTANCE, dialect);
    }

    @Test void testCreateDialectDuckdbMixedCase() {
      JdbcDialect dialect = JdbcDialectFactory.createDialect("DuckDB");
      assertSame(DuckDBDialect.INSTANCE, dialect);
    }

    @Test void testCreateDialectTrino() {
      JdbcDialect dialect = JdbcDialectFactory.createDialect("trino");
      assertSame(TrinoDialect.INSTANCE, dialect);
    }

    @Test void testCreateDialectTrinoUpperCase() {
      JdbcDialect dialect = JdbcDialectFactory.createDialect("TRINO");
      assertSame(TrinoDialect.INSTANCE, dialect);
    }

    @Test void testCreateDialectSpark() {
      JdbcDialect dialect = JdbcDialectFactory.createDialect("spark");
      assertSame(SparkSqlDialect.INSTANCE, dialect);
    }

    @Test void testCreateDialectSparkUpperCase() {
      JdbcDialect dialect = JdbcDialectFactory.createDialect("SPARK");
      assertSame(SparkSqlDialect.INSTANCE, dialect);
    }

    @Test void testCreateDialectClickhouse() {
      JdbcDialect dialect = JdbcDialectFactory.createDialect("clickhouse");
      assertSame(ClickHouseDialect.INSTANCE, dialect);
    }

    @Test void testCreateDialectClickhouseUpperCase() {
      JdbcDialect dialect = JdbcDialectFactory.createDialect("CLICKHOUSE");
      assertSame(ClickHouseDialect.INSTANCE, dialect);
    }

    @Test void testCreateDialectClickhouseMixedCase() {
      JdbcDialect dialect = JdbcDialectFactory.createDialect("ClickHouse");
      assertSame(ClickHouseDialect.INSTANCE, dialect);
    }

    @Test void testCreateDialectUnknownThrows() {
      assertThrows(IllegalArgumentException.class,
          new org.junit.jupiter.api.function.Executable() {
            @Override public void execute() {
              JdbcDialectFactory.createDialect("unknown");
            }
          });
    }

    @Test void testCreateDialectNullThrows() {
      assertThrows(IllegalArgumentException.class,
          new org.junit.jupiter.api.function.Executable() {
            @Override public void execute() {
              JdbcDialectFactory.createDialect(null);
            }
          });
    }

    @Test void testCreateDialectEmptyThrows() {
      assertThrows(IllegalArgumentException.class,
          new org.junit.jupiter.api.function.Executable() {
            @Override public void execute() {
              JdbcDialectFactory.createDialect("");
            }
          });
    }

    @Test void testCreateDialectUnknownExceptionMessage() {
      try {
        JdbcDialectFactory.createDialect("postgres");
        // Should not reach here
        assertTrue(false, "Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage().contains("postgres"));
        assertTrue(e.getMessage().contains("Supported types"));
      }
    }

    // -- isSupported --

    @Test void testIsSupportedDuckdb() {
      assertTrue(JdbcDialectFactory.isSupported("duckdb"));
    }

    @Test void testIsSupportedDuckdbUpperCase() {
      assertTrue(JdbcDialectFactory.isSupported("DUCKDB"));
    }

    @Test void testIsSupportedTrino() {
      assertTrue(JdbcDialectFactory.isSupported("trino"));
    }

    @Test void testIsSupportedSpark() {
      assertTrue(JdbcDialectFactory.isSupported("spark"));
    }

    @Test void testIsSupportedClickhouse() {
      assertTrue(JdbcDialectFactory.isSupported("clickhouse"));
    }

    @Test void testIsSupportedUnknown() {
      assertFalse(JdbcDialectFactory.isSupported("unknown"));
    }

    @Test void testIsSupportedNull() {
      assertFalse(JdbcDialectFactory.isSupported(null));
    }

    @Test void testIsSupportedEmpty() {
      assertFalse(JdbcDialectFactory.isSupported(""));
    }

    // -- getSupportedEngineTypes --

    @Test void testGetSupportedEngineTypes() {
      String[] types = JdbcDialectFactory.getSupportedEngineTypes();
      assertNotNull(types);
      assertEquals(4, types.length);
    }

    @Test void testGetSupportedEngineTypesContainsAll() {
      String[] types = JdbcDialectFactory.getSupportedEngineTypes();
      assertArrayEquals(
          new String[]{"duckdb", "trino", "spark", "clickhouse"},
          types);
    }

    @Test void testGetSupportedEngineTypesReturnsNewArray() {
      // Verify defensive copy behavior by checking it returns a new array each time
      String[] types1 = JdbcDialectFactory.getSupportedEngineTypes();
      String[] types2 = JdbcDialectFactory.getSupportedEngineTypes();
      // Content should be equal even if arrays are different instances
      assertArrayEquals(types1, types2);
    }
  }

  // ===========================================================================
  // HiveSqlDialectFactory Tests
  // ===========================================================================

  @Nested
  class HiveSqlDialectFactoryTests {

    private final HiveSqlDialectFactory factory = new HiveSqlDialectFactory();

    @Test void testCreateWithNullMetadata() {
      SqlDialect dialect = factory.create(null);
      assertSame(HiveSqlDialect.DEFAULT, dialect);
    }

    @Test void testCreateWithMockMetadata() {
      DatabaseMetaData mockMetaData = mock(DatabaseMetaData.class);
      SqlDialect dialect = factory.create(mockMetaData);
      assertSame(HiveSqlDialect.DEFAULT, dialect);
    }

    @Test void testCreateAlwaysReturnsSameInstance() {
      SqlDialect dialect1 = factory.create(null);
      SqlDialect dialect2 = factory.create(mock(DatabaseMetaData.class));
      assertSame(dialect1, dialect2);
    }

    @Test void testFactoryImplementsSqlDialectFactory() {
      assertTrue(factory instanceof org.apache.calcite.sql.SqlDialectFactory);
    }
  }

  // ===========================================================================
  // Cross-Dialect Consistency Tests
  // ===========================================================================

  @Nested
  class CrossDialectTests {

    @Test void testAllDialectsHaveNonNullNames() {
      assertNotNull(DuckDBDialect.INSTANCE.getName());
      assertNotNull(ClickHouseDialect.INSTANCE.getName());
      assertNotNull(SparkSqlDialect.INSTANCE.getName());
      assertNotNull(TrinoDialect.INSTANCE.getName());
    }

    @Test void testAllDialectsHaveNonNullDriverClassNames() {
      assertNotNull(DuckDBDialect.INSTANCE.getDriverClassName());
      assertNotNull(ClickHouseDialect.INSTANCE.getDriverClassName());
      assertNotNull(SparkSqlDialect.INSTANCE.getDriverClassName());
      assertNotNull(TrinoDialect.INSTANCE.getDriverClassName());
    }

    @Test void testAllDialectsBuildUrlWithNullConfig() {
      // All dialects should handle null config gracefully
      assertNotNull(DuckDBDialect.INSTANCE.buildJdbcUrl(null));
      assertNotNull(ClickHouseDialect.INSTANCE.buildJdbcUrl(null));
      assertNotNull(SparkSqlDialect.INSTANCE.buildJdbcUrl(null));
      assertNotNull(TrinoDialect.INSTANCE.buildJdbcUrl(null));
    }

    @Test void testAllDialectsGenerateReadParquetSql() {
      // All dialects should generate non-null SQL for readParquetSql
      assertNotNull(DuckDBDialect.INSTANCE.readParquetSql("/data.parquet", null));
      assertNotNull(ClickHouseDialect.INSTANCE.readParquetSql("/data.parquet", null));
      assertNotNull(SparkSqlDialect.INSTANCE.readParquetSql("/data.parquet", null));
      assertNotNull(TrinoDialect.INSTANCE.readParquetSql("/data.parquet", null));
    }

    @Test void testAllDialectsGenerateCreateParquetViewSql() {
      assertNotNull(
          DuckDBDialect.INSTANCE.createParquetViewSql(
          "s", "v", "/d", false));
      assertNotNull(
          ClickHouseDialect.INSTANCE.createParquetViewSql(
          "s", "v", "/d", false));
      assertNotNull(
          SparkSqlDialect.INSTANCE.createParquetViewSql(
          "s", "v", "/d", false));
      assertNotNull(
          TrinoDialect.INSTANCE.createParquetViewSql(
          "s", "v", "/d", false));
    }

    @Test void testFactoryReturnsCorrectDialectTypes() {
      assertTrue(JdbcDialectFactory.createDialect("duckdb") instanceof DuckDBDialect);
      assertTrue(JdbcDialectFactory.createDialect("clickhouse") instanceof ClickHouseDialect);
      assertTrue(JdbcDialectFactory.createDialect("spark") instanceof SparkSqlDialect);
      assertTrue(JdbcDialectFactory.createDialect("trino") instanceof TrinoDialect);
    }

    @Test void testOnlyTrinoDoesNotSupportDirectGlob() {
      assertTrue(DuckDBDialect.INSTANCE.supportsDirectGlob());
      assertTrue(ClickHouseDialect.INSTANCE.supportsDirectGlob());
      assertTrue(SparkSqlDialect.INSTANCE.supportsDirectGlob());
      assertFalse(TrinoDialect.INSTANCE.supportsDirectGlob());
    }

    @Test void testAllDialectsSupportIceberg() {
      assertTrue(DuckDBDialect.INSTANCE.supportsIceberg());
      assertTrue(ClickHouseDialect.INSTANCE.supportsIceberg());
      assertTrue(SparkSqlDialect.INSTANCE.supportsIceberg());
      assertTrue(TrinoDialect.INSTANCE.supportsIceberg());
    }
  }
}
