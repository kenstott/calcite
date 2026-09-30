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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.BaseFileTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for Iceberg tables with actual data to ensure DuckDB's iceberg_scan works properly.
 */
@Tag("integration")
public class IcebergNonEmptyTableTest extends BaseFileTest {

  @TempDir
  Path tempDir;

  private String warehousePath;
  private String ordersTablePath;

  @BeforeEach
  public void setUp() throws Exception {
    warehousePath = tempDir.resolve("warehouse").toString();

    // Create Iceberg catalog
    Configuration conf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    // Create schema for orders table
    Schema ordersSchema =
        new Schema(Types.NestedField.required(1, "order_id", Types.IntegerType.get()),
        Types.NestedField.required(2, "customer_id", Types.StringType.get()),
        Types.NestedField.required(3, "product_id", Types.StringType.get()),
        Types.NestedField.required(4, "amount", Types.DoubleType.get()),
        Types.NestedField.required(5, "order_date", Types.TimestampType.withZone()));

    // Create orders table
    Table ordersTable =
        catalog.createTable(TableIdentifier.of("orders"),
        ordersSchema,
        PartitionSpec.unpartitioned());
    ordersTablePath = ordersTable.location();

    // Add some data to the table
    addDataToTable(ordersTable, ordersSchema);
  }

  private void addDataToTable(Table table, Schema schema) throws Exception {
    // Create a data file writer
    OutputFile outputFile =
        table.io().newOutputFile(table.location() + "/data/orders-" + UUID.randomUUID() + ".parquet");

    DataWriter<Record> dataWriter = Parquet.writeData(outputFile)
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .build();

    // Add some sample records
    OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

    GenericRecord record1 = GenericRecord.create(schema);
    record1.setField("order_id", 1);
    record1.setField("customer_id", "CUST001");
    record1.setField("product_id", "PROD001");
    record1.setField("amount", 100.50);
    record1.setField("order_date", now);
    dataWriter.write(record1);

    GenericRecord record2 = GenericRecord.create(schema);
    record2.setField("order_id", 2);
    record2.setField("customer_id", "CUST002");
    record2.setField("product_id", "PROD002");
    record2.setField("amount", 250.75);
    record2.setField("order_date", now.plusDays(1));
    dataWriter.write(record2);

    GenericRecord record3 = GenericRecord.create(schema);
    record3.setField("order_id", 3);
    record3.setField("customer_id", "CUST001");
    record3.setField("product_id", "PROD003");
    record3.setField("amount", 75.00);
    record3.setField("order_date", now.plusDays(2));
    dataWriter.write(record3);

    // Close the writer and commit the data file
    dataWriter.close();

    // Commit the new data file to the table
    table.newAppend()
        .appendFile(dataWriter.toDataFile())
        .commit();
  }

  @Test public void testNonEmptyIcebergTableWithDuckDB() throws Exception {
    // This test verifies that DuckDB can properly query non-empty Iceberg tables
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"ephemeralCache\": true,\n"
        + "        \"tables\": [\n"
        + "          {\n"
        + "            \"name\": \"orders\",\n"
        + "            \"url\": \"" + ordersTablePath + "\",\n"
        + "            \"format\": \"iceberg\"\n"
        + "          }\n"
        + "        ]\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    info.setProperty("quotedCasing", "UNCHANGED");
    info.setProperty("caseSensitive", "false");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Test 1: Basic count query
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders");
      assertTrue(rs.next(), "Should have a result row");
      int count = rs.getInt(1);
      assertEquals(3, count, "Should have 3 rows in the table");
      rs.close();

      // Test 2: Select all columns
      rs = statement.executeQuery("SELECT * FROM orders ORDER BY order_id");

      // First row
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("order_id"));
      assertEquals("CUST001", rs.getString("customer_id"));
      assertEquals("PROD001", rs.getString("product_id"));
      assertEquals(100.50, rs.getDouble("amount"), 0.01);

      // Second row
      assertTrue(rs.next());
      assertEquals(2, rs.getInt("order_id"));
      assertEquals("CUST002", rs.getString("customer_id"));
      assertEquals("PROD002", rs.getString("product_id"));
      assertEquals(250.75, rs.getDouble("amount"), 0.01);

      // Third row
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("order_id"));
      assertEquals("CUST001", rs.getString("customer_id"));
      assertEquals("PROD003", rs.getString("product_id"));
      assertEquals(75.00, rs.getDouble("amount"), 0.01);

      rs.close();

      // Test 3: Aggregation query
      rs = statement.executeQuery("SELECT customer_id, SUM(amount) as total_amount " +
                                  "FROM orders GROUP BY customer_id ORDER BY customer_id");

      assertTrue(rs.next());
      assertEquals("CUST001", rs.getString("customer_id"));
      assertEquals(175.50, rs.getDouble("total_amount"), 0.01); // 100.50 + 75.00

      assertTrue(rs.next());
      assertEquals("CUST002", rs.getString("customer_id"));
      assertEquals(250.75, rs.getDouble("total_amount"), 0.01);

      rs.close();

      // Test 4: Filter query
      rs = statement.executeQuery("SELECT * FROM orders WHERE amount > 100");
      int largeOrderCount = 0;
      while (rs.next()) {
        assertTrue(rs.getDouble("amount") > 100);
        largeOrderCount++;
      }
      assertEquals(2, largeOrderCount, "Should have 2 orders with amount > 100");
      rs.close();
    }
  }

  /**
   * A declared {@code partitionedTables} entry backed by an Iceberg table that ETL has not
   * materialized yet (no metadata directory at all under the warehouse) must be OMITTED from the
   * mounted schema — not crash the whole mount — while every other declared table, including a
   * real Iceberg table under the same warehouse, still mounts and is queryable. Reproduces the
   * production failure where one not-yet-materialized declared table (e.g.
   * {@code environment.water_withdrawals}) took an entire schema mount down.
   */
  @Test public void testMissingBackingIcebergTableOmittedNotCrashed() throws Exception {
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"ephemeralCache\": true,\n"
        + "        \"baseDirectory\": \"" + tempDir.resolve("base") + "\",\n"
        + "        \"partitionedTables\": [\n"
        + "          {\n"
        + "            \"name\": \"orders\",\n"
        + "            \"materialize\": {\n"
        + "              \"format\": \"iceberg\",\n"
        + "              \"iceberg\": {\n"
        + "                \"warehousePath\": \"" + warehousePath + "\",\n"
        + "                \"tableName\": \"orders\"\n"
        + "              }\n"
        + "            }\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"never_materialized\",\n"
        + "            \"materialize\": {\n"
        + "              \"format\": \"iceberg\",\n"
        + "              \"iceberg\": {\n"
        + "                \"warehousePath\": \"" + warehousePath + "\",\n"
        + "                \"tableName\": \"does_not_exist\"\n"
        + "              }\n"
        + "            }\n"
        + "          }\n"
        + "        ]\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    info.setProperty("quotedCasing", "UNCHANGED");
    info.setProperty("caseSensitive", "false");

    // Connecting (which mounts the schema) must NOT throw, even though one declared table has
    // no backing Iceberg data at all.
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // The other declared table, backed by real data, still mounts and is queryable.
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders");
      assertTrue(rs.next(), "Should have a result row");
      assertEquals(3, rs.getInt(1), "Real table's rows are unaffected by the missing sibling");
      rs.close();

      // The not-yet-materialized table is omitted, not exposed as a broken/empty entry.
      boolean found = false;
      try (ResultSet tables =
               connection.getMetaData().getTables(null, "TEST", "%", null)) {
        while (tables.next()) {
          if ("never_materialized".equalsIgnoreCase(tables.getString("TABLE_NAME"))) {
            found = true;
          }
        }
      }
      assertFalse(found,
          "Not-yet-materialized table must be omitted from the mounted schema's table listing");
    }
  }

  /**
   * A declared {@code partitionedTables} entry whose Iceberg location EXISTS with a {@code
   * metadata/} directory and a {@code version-hint.text} naming a version, but no readable {@code
   * v{N}.metadata.json} behind it (an interrupted first write, or a hint left dangling by a
   * since-reverted commit), must also be OMITTED — existence of {@code version-hint.text} alone is
   * not enough to call a table materialized. Reproduces the production failure reported for {@code
   * ag.ers_commodity_costs_returns} against R2 (govdata/src/main/resources/ag/ag-schema.yaml):
   * existence-only checking let the table register, and it only failed later, with DuckDB's
   * "Could not guess Iceberg table version".
   */
  @Test public void testDanglingVersionHintOmittedNotCrashed() throws Exception {
    java.nio.file.Path brokenMetadataDir =
        java.nio.file.Paths.get(warehousePath, "broken_table", "metadata");
    java.nio.file.Files.createDirectories(brokenMetadataDir);
    java.nio.file.Files.write(brokenMetadataDir.resolve("version-hint.text"),
        "1".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    // Deliberately no v1.metadata.json written: the location and its metadata directory exist,
    // but there is no valid metadata file for the version the hint names.

    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"ephemeralCache\": true,\n"
        + "        \"baseDirectory\": \"" + tempDir.resolve("base2") + "\",\n"
        + "        \"partitionedTables\": [\n"
        + "          {\n"
        + "            \"name\": \"orders\",\n"
        + "            \"materialize\": {\n"
        + "              \"format\": \"iceberg\",\n"
        + "              \"iceberg\": {\n"
        + "                \"warehousePath\": \"" + warehousePath + "\",\n"
        + "                \"tableName\": \"orders\"\n"
        + "              }\n"
        + "            }\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"broken_table\",\n"
        + "            \"materialize\": {\n"
        + "              \"format\": \"iceberg\",\n"
        + "              \"iceberg\": {\n"
        + "                \"warehousePath\": \"" + warehousePath + "\",\n"
        + "                \"tableName\": \"broken_table\"\n"
        + "              }\n"
        + "            }\n"
        + "          }\n"
        + "        ]\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    info.setProperty("quotedCasing", "UNCHANGED");
    info.setProperty("caseSensitive", "false");

    // Connecting (which mounts the schema) must NOT throw, even though one declared table's
    // location has a metadata directory with no readable metadata file behind its version hint.
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // The other declared table, backed by real data, still mounts and is queryable.
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders");
      assertTrue(rs.next(), "Should have a result row");
      assertEquals(3, rs.getInt(1), "Real table's rows are unaffected by the broken sibling");
      rs.close();

      // The table with a dangling version hint is omitted, not exposed as a broken entry.
      boolean found = false;
      try (ResultSet tables =
               connection.getMetaData().getTables(null, "TEST", "%", null)) {
        while (tables.next()) {
          if ("broken_table".equalsIgnoreCase(tables.getString("TABLE_NAME"))) {
            found = true;
          }
        }
      }
      assertFalse(found,
          "A table with a dangling version-hint.text (no matching metadata.json) must be "
              + "omitted from the mounted schema's table listing");
    }
  }

  /**
   * Under the DuckDB engine, a not-yet-materialized declared table must not appear in the
   * schema's table names either. DuckDBJdbcSchema lists declared and pending-view names, but
   * getTable() returns null for an omitted table, and JDBC getTables() rejects a listed name that
   * does not resolve — so one omitted table made the whole schema's metadata listing throw, and
   * pgwire-govdata dropped the schema from its catalog.
   */
  @Test public void testOmittedTableDoesNotBreakDuckDbTableListing() throws Exception {
    String model = "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"TEST\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"TEST\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"ephemeralCache\": true,\n"
        + "        \"executionEngine\": \"duckdb\",\n"
        + "        \"baseDirectory\": \"" + tempDir.resolve("base3") + "\",\n"
        + "        \"partitionedTables\": [\n"
        + "          {\n"
        + "            \"name\": \"orders\",\n"
        + "            \"materialize\": {\n"
        + "              \"enabled\": true,\n"
        + "              \"format\": \"iceberg\",\n"
        + "              \"iceberg\": {\n"
        + "                \"warehousePath\": \"" + warehousePath + "\",\n"
        + "                \"tableName\": \"orders\"\n"
        + "              }\n"
        + "            }\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"never_materialized\",\n"
        + "            \"materialize\": {\n"
        + "              \"enabled\": true,\n"
        + "              \"format\": \"iceberg\",\n"
        + "              \"iceberg\": {\n"
        + "                \"warehousePath\": \"" + warehousePath + "\",\n"
        + "                \"tableName\": \"does_not_exist\"\n"
        + "              }\n"
        + "            }\n"
        + "          }\n"
        + "        ]\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    info.setProperty("quotedCasing", "UNCHANGED");
    info.setProperty("caseSensitive", "false");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders");
      assertTrue(rs.next(), "Should have a result row");
      assertEquals(3, rs.getInt(1), "Real table's rows are unaffected by the missing sibling");
      rs.close();

      boolean foundOrders = false;
      boolean foundOmitted = false;
      try (ResultSet tables =
               connection.getMetaData().getTables(null, "TEST", "%", null)) {
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          if ("orders".equalsIgnoreCase(tableName)) {
            foundOrders = true;
          }
          if ("never_materialized".equalsIgnoreCase(tableName)) {
            foundOmitted = true;
          }
        }
      }
      assertTrue(foundOrders, "The materialized sibling must still be listed");
      assertFalse(foundOmitted, "The not-yet-materialized table must not be listed");
    }
  }
}
