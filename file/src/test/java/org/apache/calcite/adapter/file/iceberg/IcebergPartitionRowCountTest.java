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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests that {@link IcebergPartitionRowCount} answers a filtered count from manifests only when the
 * partition tuple settles the predicate, and declines otherwise.
 *
 * <p>The fixture mirrors the shape that motivated the rule: {@code cftc_trades}, identity
 * partitioned on {@code asset_class} and {@code year}, with a differently sized partition per asset
 * class so a wrong partition selection cannot coincidentally produce the right number.
 */
@Tag("unit")
public class IcebergPartitionRowCountTest {

  /** Rows written per (asset_class, year) partition — deliberately all different. */
  private static final int RATES_2023 = 7;
  private static final int RATES_2024 = 11;
  private static final int CREDITS_2024 = 13;
  private static final int FOREX_2024 = 17;

  @TempDir
  Path tempDir;

  private Map<String, Object> catalogConfig;
  private StorageProvider storageProvider;

  @BeforeEach void setUp() {
    storageProvider = new LocalFileStorageProvider();
    catalogConfig = new HashMap<>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", tempDir.resolve("warehouse").toString());
  }

  @AfterEach void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  // -----------------------------------------------------------------------------------------
  // Fast path: the predicate is settled by the partition tuple
  // -----------------------------------------------------------------------------------------

  @Test void countsOnePartitionColumnFromManifests() throws IOException {
    Table table = tradesTable("one_partition_column");
    Long count = IcebergPartitionRowCount.countMatching(table,
        Expressions.equal("asset_class", "RATES"));
    assertEquals(Long.valueOf(RATES_2023 + RATES_2024), count);
  }

  @Test void countsIntersectionOfTwoPartitionColumns() throws IOException {
    Table table = tradesTable("two_partition_columns");
    Long count = IcebergPartitionRowCount.countMatching(table,
        Expressions.and(Expressions.equal("asset_class", "RATES"),
            Expressions.equal("year", 2024L)));
    assertEquals(Long.valueOf(RATES_2024), count);
  }

  @Test void countsInListOverPartitionValues() throws IOException {
    Table table = tradesTable("in_list");
    Long count = IcebergPartitionRowCount.countMatching(table,
        Expressions.in("asset_class", Arrays.asList("CREDITS", "FOREX")));
    assertEquals(Long.valueOf(CREDITS_2024 + FOREX_2024), count);
  }

  @Test void countsZeroForAPartitionValueThatHasNoFiles() throws IOException {
    Table table = tradesTable("absent_partition_value");
    // COMMODITIES is a legitimate asset class with no rows written — exactly the state
    // cftc.commodity_derivatives is in. Zero is the right answer, not a decline.
    Long count = IcebergPartitionRowCount.countMatching(table,
        Expressions.equal("asset_class", "COMMODITIES"));
    assertEquals(Long.valueOf(0L), count);
  }

  @Test void countsWholeTableWithNoPredicate() throws IOException {
    Table table = tradesTable("no_predicate");
    Long count = IcebergPartitionRowCount.countMatching(table, Expressions.alwaysTrue());
    assertEquals(Long.valueOf(RATES_2023 + RATES_2024 + CREDITS_2024 + FOREX_2024), count);
  }

  @Test void countsZeroForATableWithNoSnapshot() {
    Table table = IcebergCatalogManager.createTable(catalogConfig, "never_written",
        tradesSchema(), tradesSpec());
    Long count = IcebergPartitionRowCount.countMatching(table,
        Expressions.equal("asset_class", "RATES"));
    assertEquals(Long.valueOf(0L), count);
  }

  // -----------------------------------------------------------------------------------------
  // Declines: the manifests cannot settle the predicate
  // -----------------------------------------------------------------------------------------

  @Test void declinesFilterOnNonPartitionColumn() throws IOException {
    Table table = tradesTable("non_partition_column");
    // cleared is a real column but not a partition field, so every surviving file keeps a
    // row-level residual and the manifest row counts would overstate the answer.
    assertNull(IcebergPartitionRowCount.countMatching(table,
        Expressions.equal("cleared", "Y")));
  }

  @Test void declinesMixedPartitionAndNonPartitionFilter() throws IOException {
    Table table = tradesTable("mixed_columns");
    assertNull(IcebergPartitionRowCount.countMatching(table,
        Expressions.and(Expressions.equal("asset_class", "RATES"),
            Expressions.equal("cleared", "Y"))));
  }

  @Test void declinesFilterOnUnpartitionedTable() throws IOException {
    Table table = IcebergCatalogManager.createTable(catalogConfig, "unpartitioned",
        tradesSchema(), PartitionSpec.unpartitioned());
    append(table, partition(null, null), 5, "RATES", 2024);
    assertNull(IcebergPartitionRowCount.countMatching(table,
        Expressions.equal("asset_class", "RATES")));
  }

  @Test void declinesPredicateNamingAColumnTheTableDoesNotHave() throws IOException {
    Table table = tradesTable("unknown_column");
    assertNull(IcebergPartitionRowCount.countMatching(table,
        Expressions.equal("no_such_column", "x")));
  }

  // -----------------------------------------------------------------------------------------
  // Predicate construction
  // -----------------------------------------------------------------------------------------

  @Test void buildsEqualityForOneValueAndInForSeveral() {
    Map<String, List<Object>> accepted = new LinkedHashMap<>();
    accepted.put("asset_class", Collections.singletonList("RATES"));
    Expression single = IcebergPartitionRowCount.toPredicate(accepted);
    assertNotNull(single);
    assertEquals(Expression.Operation.EQ, single.op());

    accepted.clear();
    accepted.put("asset_class", Arrays.asList("RATES", "CREDITS"));
    Expression many = IcebergPartitionRowCount.toPredicate(accepted);
    assertNotNull(many);
    assertEquals(Expression.Operation.IN, many.op());
  }

  @Test void buildsAlwaysTrueForAnEmptyMap() {
    Expression expression = IcebergPartitionRowCount.toPredicate(new LinkedHashMap<>());
    assertNotNull(expression);
    assertEquals(Expression.Operation.TRUE, expression.op());
  }

  @Test void refusesAValueWithNoIcebergLiteralForm() {
    Map<String, List<Object>> accepted = new LinkedHashMap<>();
    accepted.put("trade_date", Collections.<Object>singletonList(new java.util.Date()));
    assertNull(IcebergPartitionRowCount.toPredicate(accepted));
  }

  @Test void refusesAColumnWithNoAcceptedValues() {
    Map<String, List<Object>> accepted = new LinkedHashMap<>();
    accepted.put("asset_class", Collections.emptyList());
    assertNull(IcebergPartitionRowCount.toPredicate(accepted));
  }

  // -----------------------------------------------------------------------------------------
  // Fixture
  // -----------------------------------------------------------------------------------------

  private static Schema tradesSchema() {
    return new Schema(
        Types.NestedField.optional(1, "dissemination_id", Types.LongType.get()),
        Types.NestedField.optional(2, "cleared", Types.StringType.get()),
        Types.NestedField.optional(3, "asset_class", Types.StringType.get()),
        Types.NestedField.optional(4, "year", Types.IntegerType.get()));
  }

  private static PartitionSpec tradesSpec() {
    return PartitionSpec.builderFor(tradesSchema())
        .identity("asset_class")
        .identity("year")
        .build();
  }

  private Table tradesTable(String name) throws IOException {
    Table table =
        IcebergCatalogManager.createTable(catalogConfig, name, tradesSchema(), tradesSpec());
    append(table, partition("RATES", 2023), RATES_2023, "RATES", 2023);
    append(table, partition("RATES", 2024), RATES_2024, "RATES", 2024);
    append(table, partition("CREDITS", 2024), CREDITS_2024, "CREDITS", 2024);
    append(table, partition("FOREX", 2024), FOREX_2024, "FOREX", 2024);
    table.refresh();
    return table;
  }

  private static Map<String, String> partition(String assetClass, Integer year) {
    if (assetClass == null) {
      return null;
    }
    Map<String, String> values = new LinkedHashMap<>();
    values.put("asset_class", assetClass);
    values.put("year", String.valueOf(year));
    return values;
  }

  /** Writes {@code rowCount} rows into one partition and commits them. */
  private void append(Table table, Map<String, String> partitionValues, int rowCount,
      String assetClass, int year) throws IOException {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("dissemination_id", Long.valueOf(i));
      // Half cleared, half not: a count filtered on this column must differ from the
      // partition count, so a wrongly-fired fast path would show up as a wrong number.
      row.put("cleared", i % 2 == 0 ? "Y" : "N");
      row.put("asset_class", assetClass);
      row.put("year", Integer.valueOf(year));
      rows.add(row);
    }
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    DataFile dataFile = writer.writeRecords(rows, partitionValues);
    assertNotNull(dataFile, "fixture must produce a data file");
    writer.commitDataFiles(Collections.singletonList(dataFile), null);
  }
}
