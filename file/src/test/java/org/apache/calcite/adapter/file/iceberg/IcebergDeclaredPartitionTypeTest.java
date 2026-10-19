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
import org.apache.calcite.runtime.Hook;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
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
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A partition column an Iceberg table stores as text and its schema declares as an integer is
 * served as the integer, and a filter on it still prunes partitions in DuckDB.
 *
 * <p>govdata's year partitions are stored as text ({@code year=2023} from the hive path) and
 * declared INTEGER; DuckDB cannot prune through {@code CAST(year AS INTEGER) = 2023}, so the
 * filter must reach DuckDB as {@code year = '2023'}.
 */
@Tag("integration")
public class IcebergDeclaredPartitionTypeTest extends BaseFileTest {

  private static final Pattern FILES_READ = Pattern.compile("Total Files Read: (\\d+)");

  /** A view over spending that passes its stored-text year through. */
  private static final String SPENDING_BY_YEAR =
      "SELECT year, SUM(amount) AS total FROM spending GROUP BY year";

  @TempDir Path tempDir;

  private String warehousePath;

  @BeforeEach public void setUp() throws Exception {
    warehousePath = tempDir.resolve("warehouse").toString();
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehousePath);
    // Five years, two rows each, one data file a year.
    List<String> years = new ArrayList<>();
    for (int year = 2010; year <= 2014; year++) {
      years.add(String.valueOf(year));
    }
    createTable(catalog, "spending", "year", years, 2);
    // Twelve unpadded months, one row each, one data file a month.
    List<String> months = new ArrayList<>();
    for (int month = 1; month <= 12; month++) {
      months.add(String.valueOf(month));
    }
    createTable(catalog, "monthly", "month", months, 1);
  }

  private static void createTable(HadoopCatalog catalog, String name, String partition,
      List<String> values, int rowsEach) throws Exception {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "county", Types.StringType.get()),
            Types.NestedField.optional(2, "amount", Types.DoubleType.get()),
            Types.NestedField.optional(3, partition, Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity(partition).build();
    Table table = catalog.createTable(TableIdentifier.of(name), schema, spec);
    AppendFiles append = table.newAppend();
    for (String value : values) {
      append.appendFile(writePartition(table, schema, spec, partition, value, rowsEach));
    }
    append.commit();
  }

  private static DataFile writePartition(Table table, Schema schema, PartitionSpec spec,
      String partition, String value, int rows) throws Exception {
    GenericRecord partitionRow = GenericRecord.create(schema);
    partitionRow.setField(partition, value);
    PartitionKey key = new PartitionKey(spec, schema);
    key.partition(partitionRow);
    DataWriter<Record> writer =
        Parquet.writeData(table.io().newOutputFile(table.location() + "/data/" + partition + "="
            + value + "/" + UUID.randomUUID() + ".parquet"))
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .withSpec(spec)
            .withPartition(key)
            .build();
    try {
      for (int i = 0; i < rows; i++) {
        GenericRecord row = GenericRecord.create(schema);
        row.setField("county", "c" + i);
        row.setField("amount", Double.parseDouble(value) + i);
        row.setField(partition, value);
        writer.write(row);
      }
    } finally {
      writer.close();
    }
    return writer.toDataFile();
  }

  /**
   * A DuckDB-engine schema of both tables. {@code year} is declared with four stored digits,
   * {@code month} without (stored unpadded); {@code constrained} declares primary keys, which
   * govdata always does and which wraps each table once more.
   */
  private Connection connect(boolean constrained) throws Exception {
    String model = "{\"version\": \"1.0\", \"defaultSchema\": \"TEST\", \"schemas\": [{"
        + "\"name\": \"TEST\", \"type\": \"custom\","
        + "\"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\": {\"ephemeralCache\": true, \"executionEngine\": \"duckdb\","
        + "\"baseDirectory\": \"" + json(tempDir.resolve(constrained ? "basec" : "base")) + "\","
        + (constrained
            ? "\"tableConstraints\": {"
                + "\"spending\": {\"primaryKey\": [\"county\", \"year\"]},"
                + "\"monthly\": {\"primaryKey\": [\"county\", \"month\"]}},"
            : "")
        + "\"partitionedTables\": ["
        + table("spending", "{\"name\": \"year\", \"type\": \"INTEGER\", \"storedDigits\": 4}")
        + ", "
        + table("monthly", "{\"name\": \"month\", \"type\": \"INTEGER\"}")
        + "],"
        // A view passing the stored-text year through, as govdata's
        // disasters_storm_impact_by_county does from storm_events.
        + "\"views\": [{\"name\": \"spending_by_year\", \"sql\": \"" + SPENDING_BY_YEAR + "\","
        + "\"columnDefinitions\": [{\"name\": \"year\", \"type\": \"INTEGER\","
        + " \"storedDigits\": 4}]}]"
        + "}}]}";
    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    info.setProperty("quotedCasing", "UNCHANGED");
    info.setProperty("caseSensitive", "false");
    return DriverManager.getConnection("jdbc:calcite:", info);
  }

  private String table(String name, String partitionColumn) {
    return "{\"name\": \"" + name + "\","
        + "\"partitions\": {\"style\": \"hive\", \"columnDefinitions\": [" + partitionColumn
        + "]},"
        + "\"materialize\": {\"enabled\": true, \"format\": \"iceberg\", \"iceberg\": {"
        + "\"warehousePath\": \"" + json(Path.of(warehousePath)) + "\", \"tableName\": \""
        + name + "\"}}}";
  }

  private static String json(Path path) {
    return path.toString().replace("\\", "\\\\");
  }

  /** What one query answered, and the SQL it sent DuckDB. */
  private static final class Answer {
    final List<Object> partitions = new ArrayList<>();
    final List<String> sql = new ArrayList<>();
  }

  /** The partition value of each row of {@code table} matching {@code where}, in order. */
  private Answer query(String table, String column, String where) throws Exception {
    Answer answer = new Answer();
    try (Connection connection = connect(true);
         Statement statement = connection.createStatement();
         Hook.Closeable hook =
             Hook.QUERY_PLAN.addThread((Consumer<Object>) o -> answer.sql.add(String.valueOf(o)));
         ResultSet rs = statement.executeQuery("SELECT \"" + column + "\" FROM \"" + table
             + "\" WHERE " + where + " ORDER BY \"" + column + "\", \"county\"")) {
      while (rs.next()) {
        answer.partitions.add(rs.getObject(1));
      }
    }
    assertEquals(1, answer.sql.size(), "one statement reaches DuckDB: " + answer.sql);
    return answer;
  }

  /** The data files DuckDB reads to answer {@code sql} over this test's tables. */
  private int filesRead(String sql) throws Exception {
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehousePath);
    try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
         Statement s = duck.createStatement()) {
      s.execute("INSTALL iceberg");
      s.execute("LOAD iceberg");
      s.execute("CREATE SCHEMA \"TEST\"");
      for (String table : new String[] {"spending", "monthly"}) {
        s.execute("CREATE VIEW \"TEST\".\"" + table + "\" AS SELECT * FROM iceberg_scan('"
            + catalog.loadTable(TableIdentifier.of(table)).location()
            + "', allow_moved_paths=true)");
      }
      s.execute("CREATE VIEW \"TEST\".\"spending_by_year\" AS "
          + SPENDING_BY_YEAR.replace("FROM spending", "FROM \"TEST\".\"spending\""));
      StringBuilder plan = new StringBuilder();
      try (ResultSet rs = s.executeQuery("EXPLAIN ANALYZE " + sql)) {
        while (rs.next()) {
          plan.append(rs.getString(2));
        }
      }
      Matcher m = FILES_READ.matcher(plan);
      assertTrue(m.find(), "DuckDB reports the files it read: " + plan);
      return Integer.parseInt(m.group(1));
    }
  }

  private static List<Object> each(int first, int last, int times) {
    List<Object> values = new ArrayList<>();
    for (int v = first; v <= last; v++) {
      for (int i = 0; i < times; i++) {
        values.add(v);
      }
    }
    return values;
  }

  @Test public void theDeclaredTypeIsServedWithOrWithoutConstraints() throws Exception {
    for (boolean constrained : new boolean[] {false, true}) {
      try (Connection connection = connect(constrained);
           Statement statement = connection.createStatement();
           ResultSet rs = statement.executeQuery(
               "SELECT \"year\", \"amount\" FROM \"spending\" WHERE \"county\" = 'c0' "
                   + "ORDER BY \"year\"")) {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(java.sql.Types.INTEGER, md.getColumnType(1), md.getColumnTypeName(1));
        List<Object> years = new ArrayList<>();
        while (rs.next()) {
          years.add(rs.getObject(1));
        }
        assertEquals(each(2010, 2014, 1), years, "constrained=" + constrained);
      }
    }
  }

  @Test public void aFilterOnAFixedWidthYearPrunesToItsPartitions() throws Exception {
    Object[][] cases = {
        {"\"year\" = 2012", each(2012, 2012, 2), 1},
        {"\"year\" IN (2011, 2013)", List.of(2011, 2011, 2013, 2013), 2},
        {"\"year\" BETWEEN 2011 AND 2012", each(2011, 2012, 2), 2},
        {"\"year\" >= 2013", each(2013, 2014, 2), 2},
        {"\"year\" < 2011", each(2010, 2010, 2), 1},
        {"2014 <= \"year\"", each(2014, 2014, 2), 1},
        // DuckDB prunes no partition by a not-equal; it still compares the stored text.
        {"\"year\" <> 2012", List.of(2010, 2010, 2011, 2011, 2013, 2013, 2014, 2014), 5},
    };
    for (Object[] c : cases) {
      String where = (String) c[0];
      Answer answer = query("spending", "year", where);
      assertEquals(c[1], answer.partitions, where);
      String sql = answer.sql.get(0);
      assertFalse(sql.contains("WHERE CAST"), where + " reaches DuckDB on the stored text: " + sql);
      assertEquals(c[2], filesRead(sql), where + ": " + sql);
    }
  }

  @Test public void aBoundedRangeOnAnUnpaddedMonthPrunesAsTheListOfItsTexts() throws Exception {
    Answer bounded = query("monthly", "month", "\"month\" BETWEEN 3 AND 5");
    assertEquals(each(3, 5, 1), bounded.partitions);
    assertFalse(bounded.sql.get(0).contains("WHERE CAST"), bounded.sql.get(0));
    assertEquals(3, filesRead(bounded.sql.get(0)), bounded.sql.get(0));

    Answer listed = query("monthly", "month", "\"month\" IN (9, 10)");
    assertEquals(each(9, 10, 1), listed.partitions);
    assertEquals(2, filesRead(listed.sql.get(0)), listed.sql.get(0));
  }

  @Test public void anUnboundedRangeOnAnUnpaddedMonthKeepsTheCast() throws Exception {
    // '10' < '9' as text, so the range cannot be compared as text; the cast answers it.
    Answer answer = query("monthly", "month", "\"month\" >= 9");
    assertEquals(each(9, 12, 1), answer.partitions);
    assertTrue(answer.sql.get(0).contains("WHERE CAST"), answer.sql.get(0));
  }

  @Test public void aLiteralNoStoredYearCanHoldKeepsTheCast() throws Exception {
    for (String where : new String[] {"\"year\" = 12345", "\"year\" > 99999", "\"year\" = -1"}) {
      Answer answer = query("spending", "year", where);
      assertEquals(List.of(), answer.partitions, where);
      assertTrue(answer.sql.get(0).contains("WHERE CAST"),
          where + " keeps the cast: " + answer.sql.get(0));
    }
  }

  @Test public void aViewDeclaringAStoredTextYearServesItAndPrunes() throws Exception {
    try (Connection connection = connect(true); Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery(
             "SELECT \"year\", \"total\" FROM \"spending_by_year\" ORDER BY \"year\"")) {
      assertEquals(java.sql.Types.INTEGER, rs.getMetaData().getColumnType(1),
          rs.getMetaData().getColumnTypeName(1));
      List<Object> years = new ArrayList<>();
      while (rs.next()) {
        years.add(rs.getObject(1));
      }
      assertEquals(each(2010, 2014, 1), years);
    }
    for (Object[] c : new Object[][] {
        {"\"year\" = 2012", List.of(2012), 1},
        {"\"year\" >= 2013", List.of(2013, 2014), 2}}) {
      String where = (String) c[0];
      Answer answer = new Answer();
      try (Connection connection = connect(true);
           Statement statement = connection.createStatement();
           Hook.Closeable hook = Hook.QUERY_PLAN.addThread(
               (Consumer<Object>) o -> answer.sql.add(String.valueOf(o)));
           ResultSet rs = statement.executeQuery("SELECT \"year\" FROM \"spending_by_year\" WHERE "
               + where + " ORDER BY \"year\"")) {
        while (rs.next()) {
          answer.partitions.add(rs.getObject(1));
        }
      }
      assertEquals(c[1], answer.partitions, where);
      String sql = answer.sql.get(0);
      assertFalse(sql.contains("WHERE CAST"), where + " reaches DuckDB on the stored text: " + sql);
      assertEquals(c[2], filesRead(sql), where + ": " + sql);
    }
  }
}
