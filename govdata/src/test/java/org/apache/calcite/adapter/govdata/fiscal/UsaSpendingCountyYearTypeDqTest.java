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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.IcebergMaterializationWriter;
import org.apache.calcite.adapter.file.etl.MaterializeConfig;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Data check of the partition-type fix on real data: one fiscal year of
 * {@code fiscal.usaspending_by_county}, fetched from USAspending by the table's own provider and
 * written by the table's own materialize config into a local warehouse, has an INTEGER year.
 *
 * <p>Before the fix the writer typed the partition from the materialize block alone, which names
 * no column definitions, and wrote the year as text. Nothing here leaves the local temp directory.
 */
@Tag("integration")
public class UsaSpendingCountyYearTypeDqTest {

  private static final String TABLE = "usaspending_by_county";

  @TempDir Path tempDir;

  @SuppressWarnings("unchecked")
  private static Map<String, Object> tableDefinition() throws Exception {
    try (InputStream in =
             UsaSpendingCountyYearTypeDqTest.class.getResourceAsStream("/fiscal/fiscal-schema.yaml")) {
      LoaderOptions options = new LoaderOptions();
      options.setMaxAliasesForCollections(100000);
      Map<String, Object> schema = new Yaml(new SafeConstructor(options)).load(in);
      for (Object t : (List<Object>) schema.get("partitionedTables")) {
        Map<String, Object> table = (Map<String, Object>) t;
        if (TABLE.equals(table.get("name"))) {
          return table;
        }
      }
    }
    throw new AssertionError(TABLE + " is not in fiscal-schema.yaml");
  }

  /** {@code table} with its Iceberg warehouse moved to {@code warehouse}; a deep copy. */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> inWarehouse(Map<String, Object> table, String warehouse)
      throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> copy = mapper.readValue(mapper.writeValueAsString(table), Map.class);
    Map<String, Object> materialize = (Map<String, Object>) copy.get("materialize");
    Map<String, Object> iceberg = new LinkedHashMap<>((Map<String, Object>) materialize.get("iceberg"));
    iceberg.put("warehousePath", warehouse);
    materialize.put("iceberg", iceberg);
    return copy;
  }

  @Test void oneFiscalYearIsWrittenWithAnIntegerYearAndReadsBackAsIt() throws Exception {
    String warehouse = tempDir.resolve("fiscal").toString();
    Map<String, Object> table = inWarehouse(tableDefinition(), warehouse);
    MaterializeConfig materialize = EtlPipelineConfig.materializeFromTableMap(table);

    Map<String, String> variables = new LinkedHashMap<>();
    variables.put("type", TABLE);
    variables.put("year", "2023");
    Iterator<Map<String, Object>> fetched =
        new UsaSpendingCountyProvider().fetch(null, variables, RawCache.passthrough());
    List<Map<String, Object>> rows = new ArrayList<>();
    fetched.forEachRemaining(rows::add);
    assertTrue(rows.size() > 2500, "FY2023 has a row for each county: " + rows.size());

    try (IcebergMaterializationWriter writer =
             new IcebergMaterializationWriter(new LocalFileStorageProvider(), warehouse, null,
                 tempDir.resolve("work").toFile())) {
      writer.initialize(materialize);
      writer.writeBatch(rows.iterator(), variables);
      writer.commit();
    }

    Table written = new HadoopTables(new Configuration()).load(warehouse + "/" + TABLE);
    assertEquals(Types.IntegerType.get(), written.schema().findField("year").type(),
        "the stored year is the declared INTEGER");

    String model = "{\"version\": \"1.0\", \"defaultSchema\": \"fiscal\", \"schemas\": [{"
        + "\"name\": \"fiscal\", \"type\": \"custom\","
        + "\"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\"operand\": {\"ephemeralCache\": true, \"executionEngine\": \"duckdb\","
        + "\"baseDirectory\": \"" + tempDir.resolve("base") + "\","
        + "\"partitionedTables\": [{\"name\": \"" + TABLE + "\","
        + "\"partitions\": " + new ObjectMapper().writeValueAsString(table.get("partitions")) + ","
        + "\"materialize\": {\"enabled\": true, \"format\": \"iceberg\", \"iceberg\": {"
        + "\"warehousePath\": \"" + warehouse + "\", \"tableName\": \"" + TABLE + "\"}}}]}}]}";
    Properties info = new Properties();
    info.setProperty("model", "inline:" + model);
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    info.setProperty("quotedCasing", "UNCHANGED");
    info.setProperty("caseSensitive", "false");
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery("SELECT \"year\", COUNT(*) AS n, "
             + "SUM(\"obligated_amount_excl_loans\") AS total FROM \"" + TABLE + "\" "
             + "WHERE \"year\" = 2023 AND \"county_fips\" LIKE '39%' GROUP BY \"year\"")) {
      assertEquals(java.sql.Types.INTEGER, rs.getMetaData().getColumnType(1),
          rs.getMetaData().getColumnTypeName(1));
      assertTrue(rs.next(), "Ohio's FY2023 counties are read back");
      assertEquals(2023, rs.getInt(1));
      assertEquals(88, rs.getInt(2), "a row for each of Ohio's 88 counties");
      assertTrue(rs.getDouble(3) > 1.0e9, "Ohio's federal money in FY2023: " + rs.getDouble(3));
    }
  }
}
