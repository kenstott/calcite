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
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Confirms, end-to-end through the full Calcite -> DuckDB stack, that {@code SELECT *}
 * over an Iceberg table delivers the <em>live Iceberg</em> column set — not a narrower
 * set derived from the model's table declaration.
 *
 * <p>The model declares the table with only {@code name}/{@code url}/{@code format:iceberg}
 * and no column list, so the "declared" column set is empty (narrower than Iceberg). The
 * test materializes a 2-column Iceberg table, then evolves it to 3 columns, and asserts a
 * fresh connection's {@code SELECT *} returns all three. This verifies the row type is
 * sourced from {@code IcebergTable.getRowType} (live Iceberg schema), as traced in the
 * table-updater design doc, rather than from any static/declared definition.
 *
 * <p>Note: this does not exercise the stale-persisted-view failure (the
 * "Contents of view were altered" binder error), which was confirmed at the DuckDB process
 * level separately. Reproducing that faithfully needs a persisted catalog read by a fresh
 * process; in-JVM catalog/row-type caching makes it unreliable to assert here.
 */
@Tag("integration")
public class IcebergSelectStarWideningTest extends BaseFileTest {

  @TempDir
  Path tempDir;

  private String warehousePath;
  private String ordersTablePath;

  @BeforeEach
  public void setUp() throws Exception {
    warehousePath = tempDir.resolve("warehouse").toString();

    Configuration conf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    // Start narrow: two columns only.
    Schema ordersSchema =
        new Schema(Types.NestedField.required(1, "order_id", Types.IntegerType.get()),
        Types.NestedField.required(2, "customer_id", Types.StringType.get()));

    Table ordersTable =
        catalog.createTable(TableIdentifier.of("orders"),
        ordersSchema,
        PartitionSpec.unpartitioned());
    ordersTablePath = ordersTable.location();

    appendRows(ordersTable, ordersTable.schema(), 2, false);
  }

  /**
   * Appends {@code count} rows. When {@code withAmount} is true, sets the evolved
   * {@code amount} column (id*100.0); otherwise writes only the two base columns.
   */
  private void appendRows(Table table, Schema schema, int count, boolean withAmount)
      throws Exception {
    OutputFile outputFile =
        table.io().newOutputFile(
            table.location() + "/data/orders-" + UUID.randomUUID() + ".parquet");

    DataWriter<Record> dataWriter = Parquet.writeData(outputFile)
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .build();

    int start = withAmount ? 100 : 0;
    for (int i = 0; i < count; i++) {
      int id = start + i + 1;
      GenericRecord record = GenericRecord.create(schema);
      record.setField("order_id", id);
      record.setField("customer_id", "CUST" + id);
      if (withAmount) {
        record.setField("amount", id * 100.0);
      }
      dataWriter.write(record);
    }
    dataWriter.close();
    table.newAppend().appendFile(dataWriter.toDataFile()).commit();
  }

  private String model() {
    return "{\n"
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
  }

  private Connection connect() throws Exception {
    Properties info = new Properties();
    info.setProperty("model", "inline:" + model());
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    info.setProperty("quotedCasing", "UNCHANGED");
    info.setProperty("caseSensitive", "false");
    return DriverManager.getConnection("jdbc:calcite:", info);
  }

  private static List<String> columnNames(ResultSet rs) throws Exception {
    ResultSetMetaData md = rs.getMetaData();
    List<String> names = new ArrayList<String>();
    for (int i = 1; i <= md.getColumnCount(); i++) {
      names.add(md.getColumnName(i).toLowerCase(Locale.ROOT));
    }
    return names;
  }

  @Test public void testSelectStarDeliversLiveIcebergColumns() throws Exception {
    // Baseline: model declares no columns; SELECT * should still expose the 2 Iceberg columns.
    try (Connection connection = connect();
         Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery("SELECT * FROM orders ORDER BY order_id")) {
      List<String> names = columnNames(rs);
      assertEquals(2, names.size(),
          "Baseline SELECT * should expose exactly the 2 Iceberg columns, got: " + names);
      assertTrue(names.contains("order_id") && names.contains("customer_id"),
          "Expected order_id, customer_id; got: " + names);
    }

    // Evolve the Iceberg table: add a third column and append a row that populates it.
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehousePath);
    Table table = catalog.loadTable(TableIdentifier.of("orders"));
    table.updateSchema().addColumn("amount", Types.DoubleType.get()).commit();
    Table evolved = catalog.loadTable(TableIdentifier.of("orders"));
    appendRows(evolved, evolved.schema(), 1, true);

    // Fresh connection: SELECT * must now deliver all THREE live Iceberg columns,
    // even though the model still declares none. This is the crux: the column set
    // tracks the live Iceberg schema, not a static/declared (narrower) definition.
    try (Connection connection = connect();
         Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery("SELECT * FROM orders ORDER BY order_id")) {
      List<String> names = columnNames(rs);
      assertEquals(3, names.size(),
          "After widening, SELECT * should expose all 3 live Iceberg columns, got: " + names);
      assertTrue(names.contains("amount"),
          "Newly added Iceberg column 'amount' must appear in SELECT *; got: " + names);

      // The new row carries amount; the two original rows are null for amount.
      int rowsWithAmount = 0;
      while (rs.next()) {
        double amount = rs.getDouble("amount");
        if (!rs.wasNull()) {
          rowsWithAmount++;
          assertTrue(amount > 0, "Populated amount should be > 0, got: " + amount);
        }
      }
      assertEquals(1, rowsWithAmount,
          "Exactly the appended row should have a non-null amount");
    }
  }
}
