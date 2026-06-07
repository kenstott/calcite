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

import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Sources;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link IcebergTable#setConstraintConfig} causes
 * {@link IcebergTable#getStatistic} to expose PK keys via
 * {@code Statistic.getKeys()}.
 *
 * <p>Regression coverage for Bug 3 in GitHub issue #19: IcebergTable previously
 * returned an empty statistic regardless of constraints declared in the schema
 * YAML, so {@code CalciteMetaImpl.getPrimaryKeys()} returned no rows for
 * Iceberg-backed tables.
 */
@Tag("integration")
@Execution(ExecutionMode.SAME_THREAD)
public class IcebergTableConstraintTest {

  @TempDir
  Path tempDir;

  private Table createIcebergTable(String tablePath) {
    Schema schema = new Schema(
        Types.NestedField.required(1, "order_id", Types.IntegerType.get()),
        Types.NestedField.required(2, "customer_id", Types.IntegerType.get()),
        Types.NestedField.required(3, "amount", Types.DoubleType.get()));
    Configuration conf = new Configuration();
    HadoopTables tables = new HadoopTables(conf);
    return tables.create(schema, PartitionSpec.unpartitioned(), tablePath);
  }

  @Test
  void getStatistic_withConstraintConfig_exposesPkKeys() {
    String tablePath = tempDir.resolve("orders").toString();
    Table icebergTable = createIcebergTable(tablePath);

    IcebergTable table = new IcebergTable(icebergTable, Sources.of(new File(tablePath)));

    Map<String, Object> constraintConfig = new LinkedHashMap<String, Object>();
    constraintConfig.put("primaryKey", Collections.singletonList("order_id"));
    table.setConstraintConfig(constraintConfig);

    Statistic stat = table.getStatistic();
    List<ImmutableBitSet> keys = stat.getKeys();
    assertNotNull(keys, "getKeys() must not return null after setConstraintConfig");
    assertFalse(keys.isEmpty(),
        "getStatistic() must return PK key after setConstraintConfig — "
        + "CalciteMetaImpl.getPrimaryKeys() depends on this");

    // order_id is the first column (index 0)
    assertTrue(keys.get(0).get(0),
        "PK key bitset must include column index 0 (order_id)");
  }

  @Test
  void getStatistic_withoutConstraintConfig_returnsNoKeys() {
    String tablePath = tempDir.resolve("orders_no_pk").toString();
    Table icebergTable = createIcebergTable(tablePath);

    IcebergTable table = new IcebergTable(icebergTable, Sources.of(new File(tablePath)));

    // No setConstraintConfig() call — must return empty keys
    Statistic stat = table.getStatistic();
    List<ImmutableBitSet> keys = stat.getKeys();
    assertTrue(keys == null || keys.isEmpty(),
        "Tables without constraint config must return no PK keys");
  }

  @Test
  void getStatistic_compositePk_exposesAllKeyColumns() {
    String tablePath = tempDir.resolve("composite").toString();
    Table icebergTable = createIcebergTable(tablePath);

    IcebergTable table = new IcebergTable(icebergTable, Sources.of(new File(tablePath)));

    // Composite PK on (order_id, customer_id)
    Map<String, Object> constraintConfig = new LinkedHashMap<String, Object>();
    List<String> pkCols = new java.util.ArrayList<String>();
    pkCols.add("order_id");
    pkCols.add("customer_id");
    constraintConfig.put("primaryKey", pkCols);
    table.setConstraintConfig(constraintConfig);

    Statistic stat = table.getStatistic();
    List<ImmutableBitSet> keys = stat.getKeys();
    assertNotNull(keys);
    assertFalse(keys.isEmpty(), "Composite PK must produce at least one key entry");

    // The composite key bitset must include both column 0 and column 1
    ImmutableBitSet keyBits = keys.get(0);
    assertTrue(keyBits.get(0), "Composite PK must include column index 0 (order_id)");
    assertTrue(keyBits.get(1), "Composite PK must include column index 1 (customer_id)");
  }

  @Test
  void getStatistic_emptyConstraintConfig_returnsNoKeys() {
    String tablePath = tempDir.resolve("empty_constraints").toString();
    Table icebergTable = createIcebergTable(tablePath);

    IcebergTable table = new IcebergTable(icebergTable, Sources.of(new File(tablePath)));
    table.setConstraintConfig(Collections.<String, Object>emptyMap());

    Statistic stat = table.getStatistic();
    List<ImmutableBitSet> keys = stat.getKeys();
    assertTrue(keys == null || keys.isEmpty(),
        "Empty constraint config must not produce spurious PK keys");
  }

  @Test
  void getStatistic_pkColumnNameCaseMustMatchSchema() {
    String tablePath = tempDir.resolve("case_check").toString();
    Table icebergTable = createIcebergTable(tablePath);

    IcebergTable table = new IcebergTable(icebergTable, Sources.of(new File(tablePath)));

    // Schema has "order_id" (lowercase) — constraint must use matching case
    Map<String, Object> constraintConfig = new LinkedHashMap<String, Object>();
    constraintConfig.put("primaryKey", Collections.singletonList("order_id"));
    table.setConstraintConfig(constraintConfig);

    Statistic stat = table.getStatistic();
    List<ImmutableBitSet> keys = stat.getKeys();
    assertNotNull(keys);
    assertFalse(keys.isEmpty(), "PK key with matching column name must be found");
    assertEquals(ImmutableBitSet.of(0), keys.get(0), "order_id is column 0");
  }
}
