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
package org.apache.calcite.adapter.govdata.etl;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.iceberg.IcebergTableWriter;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the verifier can tell a sorted table from an unsorted one.
 *
 * <p>This matters more than it looks. verify-healed-tables.sh is the last gate before
 * sync-healed-tables-to-r2.sh deletes the pre-heal files from R2 — the only remaining copy. A
 * verifier that always passes would wave through a broken heal and destroy the ability to
 * recover from it, so the property under test is not "it passes on good data" but "it FAILS on
 * bad data".
 */
@Tag("unit")
public class IcebergSortVerifierTest {

  @TempDir
  Path tempDir;

  private Table table;
  private IcebergTableWriter writer;

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "name", Types.StringType.get()),
      Types.NestedField.optional(2, "id", Types.IntegerType.get()),
      Types.NestedField.optional(3, "year", Types.IntegerType.get()));

  @BeforeEach void setUp() {
    Map<String, Object> catalogConfig = new HashMap<>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", tempDir.resolve("warehouse").toString());
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("year").build();
    table = IcebergCatalogManager.createTable(catalogConfig, "verify_test", SCHEMA, spec);
    writer = new IcebergTableWriter(table, new LocalFileStorageProvider());
  }

  @AfterEach void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  /**
   * Writes files that each span the whole key range, by striding through the keys.
   *
   * <p>This is the shape ingest produces and the shape in which bounds cannot prune: every
   * file's [min,max] covers nearly the entire domain, so a point lookup overlaps them all.
   * Files written in natural order would already be clustered and would make the test vacuous.
   */
  private void writeInterleaved(int files, int rowsPerFile) throws Exception {
    int total = files * rowsPerFile;
    int width = String.valueOf(total).length();
    for (int f = 0; f < files; f++) {
      List<Map<String, Object>> rows = new ArrayList<>();
      for (int r = 0; r < rowsPerFile; r++) {
        int keyIndex = r * files + f;
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("name", "name-" + String.format("%0" + width + "d", keyIndex));
        row.put("id", keyIndex);
        row.put("year", 2024);
        rows.add(row);
      }
      Map<String, String> partition = new HashMap<>();
      partition.put("year", "2024");
      DataFile df = writer.writeRecords(rows, partition);
      assertNotNull(df);
      table.newAppend().appendFile(df).commit();
    }
  }

  @Test void failsOnAnUnsortedTable() throws Exception {
    writeInterleaved(6, 100);

    // No heal has run: no aperio.sorted-by, and every file's bounds span the key space.
    assertFalse(IcebergSortVerifier.verify(table, "name"),
        "an unsorted table must FAIL verification — this is the gate before R2 deletion");
  }

  @Test void passesAfterHeal() throws Exception {
    writeInterleaved(6, 100);
    assertFalse(IcebergSortVerifier.verify(table, "name"), "precondition: starts unsorted");

    writer.healSortOrder(Arrays.asList("name"), 128L * 1024 * 1024, 0);
    table.refresh();

    assertTrue(IcebergSortVerifier.verify(table, "name"),
        "after heal the table must verify: rows preserved, order recorded, bounds narrowed");
  }

  /**
   * Writes one file per year partition, each spanning the whole key range.
   *
   * <p>This is the shape the SEC tables actually have: partitioned by year, and every year holds
   * rows for nearly every key, so each partition's single file covers the entire domain.
   */
  private void writeOneFilePerYear(int years, int rowsPerFile) throws Exception {
    int width = String.valueOf(rowsPerFile).length();
    for (int y = 0; y < years; y++) {
      int year = 2020 + y;
      List<Map<String, Object>> rows = new ArrayList<>();
      for (int r = 0; r < rowsPerFile; r++) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("name", "name-" + String.format("%0" + width + "d", r));
        row.put("id", r);
        row.put("year", year);
        rows.add(row);
      }
      Map<String, String> partition = new HashMap<>();
      partition.put("year", String.valueOf(year));
      DataFile df = writer.writeRecords(rows, partition);
      assertNotNull(df);
      table.newAppend().appendFile(df).commit();
    }
  }

  @Test void passesWhenEveryPartitionHoldsOneFileSpanningTheKeyRange() throws Exception {
    writeOneFilePerYear(8, 100);
    writer.healSortOrder(Arrays.asList("name"), 128L * 1024 * 1024, 0);
    table.refresh();

    // Pooling every file's bounds across partitions reports 8 of 8 files (100%) here and fails
    // the table — which is what happened to filing_metadata, insider_transactions and
    // earnings_transcripts on 2026-08-17. Nothing is wrong with them: a heal sorts within a
    // partition and cannot move a row across one, so a year-partitioned table whose every year
    // spans the key range CANNOT have narrow cross-partition bounds however well it is sorted.
    // Overlap has to be measured inside a partition, and a one-file partition has none.
    assertTrue(IcebergSortVerifier.verify(table, "name"),
        "one file per partition means there is nothing to prune within a partition; measuring"
            + " overlap across partitions fails correctly-healed data purely for being small");
  }

  @Test void failsWhenTheSortPropertyIsMissing() throws Exception {
    writeInterleaved(4, 50);
    writer.healSortOrder(Arrays.asList("name"), 128L * 1024 * 1024, 0);
    table.refresh();
    assertTrue(IcebergSortVerifier.verify(table, "name"), "precondition: healed table passes");

    // Data is still sorted, but the record of it is gone. That means a re-heal would not be
    // skipped and the table's claimed state no longer matches, so verification must not pass.
    table.updateProperties().remove(IcebergTableWriter.SORTED_BY_PROPERTY).commit();
    table.refresh();

    assertFalse(IcebergSortVerifier.verify(table, "name"),
        "a missing aperio.sorted-by must fail even when the data happens to be sorted");
  }

  /**
   * Pins the catalog config key names against IcebergMaintenanceRunner's, which is the pair that
   * actually works against MinIO.
   *
   * <p>This is a structural assertion because the failure mode is silent: wrong key names are
   * ignored by the loader rather than rejected, so the endpoint never reaches the SDK and the
   * call tries to resolve a real AWS host. Every other test here uses a local warehouse and
   * therefore exercises no S3 path, which is exactly how the bug shipped.
   */
  @Test void buildsTheCatalogConfigKeysTheLoaderActuallyReads() {
    Map<String, Object> cfg = IcebergSortVerifier.buildCatalogConfig(
        "s3a://bucket/sec", "AK", "SK", "http://minio:9000");

    assertEquals("hadoop", cfg.get("catalog"), "loader reads 'catalog', not 'catalogType'");
    assertEquals("s3a://bucket/sec", cfg.get("warehouse"),
        "loader reads 'warehouse', not 'warehousePath'");

    @SuppressWarnings("unchecked")
    Map<String, String> hadoop = (Map<String, String>) cfg.get("hadoopConfig");
    assertNotNull(hadoop, "credentials travel in a hadoopConfig map, not as top-level keys");
    assertEquals("AK", hadoop.get("fs.s3a.access.key"));
    assertEquals("SK", hadoop.get("fs.s3a.secret.key"));
    assertEquals("http://minio:9000", hadoop.get("fs.s3a.endpoint"),
        "the endpoint must reach the SDK, or it resolves a real AWS host");
    assertEquals("true", hadoop.get("fs.s3a.path.style.access"),
        "MinIO needs path-style access");
  }

  @Test void omitsCredentialsWhenNoneAreGiven() {
    Map<String, Object> cfg = IcebergSortVerifier.buildCatalogConfig("/tmp/wh", null, null, null);
    @SuppressWarnings("unchecked")
    Map<String, String> hadoop = (Map<String, String>) cfg.get("hadoopConfig");
    assertNotNull(hadoop);
    assertTrue(hadoop.isEmpty(), "a local warehouse must not get empty S3 settings");
  }

  @Test void failsOnAColumnThatIsNotInTheSchema() throws Exception {
    writeInterleaved(3, 50);
    assertFalse(IcebergSortVerifier.verify(table, "no_such_column"),
        "an unknown sort column must fail rather than being reported as verified");
  }
}
