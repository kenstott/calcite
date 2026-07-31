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

import org.apache.calcite.adapter.file.partition.IncrementalTracker;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for adding columns to an existing Iceberg table without dropping it.
 *
 * <p>The materializer's response to a YAML schema gaining a column used to be unconditional:
 * drop the table, recreate it with the new schema, lose every row. xbrl_relationships held 195M
 * rows — the correct majority alongside the polluted rows a bug produced — and all of them would
 * be discarded the moment four columns were added to describe attributes the old schema had no
 * room for, whether or not the addition was the point of the change.
 *
 * <p>An addition is safe exactly when every existing column survives unchanged: same name, same
 * type. Iceberg's schema evolution can express that directly — old data files still satisfy the
 * new schema, reading the missing columns as null, which is correct because those rows genuinely
 * predate whatever populates the new column. Anything else (a column renamed, retyped, or
 * removed) cannot be told apart from data loss from the schema alone, and keeps the original
 * drop-and-recreate behavior.
 */
@Tag("integration")
class IcebergSchemaEvolutionTest {

  @TempDir
  Path tempDir;

  private IcebergMaterializer newMaterializer() {
    IcebergCatalogManager.clearCache();
    return new IcebergMaterializer(
        tempDir.resolve("warehouse").toString(), null, IncrementalTracker.NOOP);
  }

  private static IcebergCatalogManager.ColumnDef col(String name, String type) {
    return new IcebergCatalogManager.ColumnDef(name, type);
  }

  private static IcebergMaterializer.MaterializationConfig configWith(String tableId,
      List<IcebergCatalogManager.ColumnDef> columns) {
    return IcebergMaterializer.MaterializationConfig.builder()
        .sourcePattern("unused")
        .targetTableId(tableId)
        .tableColumns(columns)
        .build();
  }

  // ===== pureColumnAdditions =====

  @Test void testPureAdditionIsRecognized() {
    Schema existing = new Schema(
        Types.NestedField.optional(1, "cik", Types.StringType.get()),
        Types.NestedField.optional(2, "accession", Types.StringType.get()));

    String added = IcebergMaterializer.pureColumnAdditions(existing,
        Arrays.asList(col("cik", "VARCHAR"), col("accession", "VARCHAR"),
            col("arc_use", "VARCHAR")));

    assertEquals("arc_use", added);
  }

  @Test void testRemovedColumnIsNotAPureAddition() {
    Schema existing = new Schema(
        Types.NestedField.optional(1, "cik", Types.StringType.get()),
        Types.NestedField.optional(2, "accession", Types.StringType.get()));

    // "accession" is gone from the expected list, even though two new columns arrived —
    // a net column-count increase must not mask a column that disappeared.
    String added = IcebergMaterializer.pureColumnAdditions(existing,
        Arrays.asList(col("cik", "VARCHAR"), col("new_a", "VARCHAR"), col("new_b", "VARCHAR")));

    assertNull(added, "a schema missing an existing column is not a pure addition");
  }

  @Test void testRetypedColumnIsNotAPureAddition() {
    Schema existing = new Schema(
        Types.NestedField.optional(1, "cik", Types.StringType.get()),
        Types.NestedField.optional(2, "accession", Types.StringType.get()));

    String added = IcebergMaterializer.pureColumnAdditions(existing,
        Arrays.asList(col("cik", "VARCHAR"), col("accession", "BIGINT"), col("new_col", "VARCHAR")));

    assertNull(added, "a column whose type changed is not a pure addition");
  }

  @Test void testMultipleAdditionsAreAllListed() {
    Schema existing = new Schema(Types.NestedField.optional(1, "cik", Types.StringType.get()));

    String added = IcebergMaterializer.pureColumnAdditions(existing,
        Arrays.asList(col("cik", "VARCHAR"), col("a", "VARCHAR"), col("b", "INTEGER")));

    assertEquals("a, b", added);
  }

  @Test void testColumnOrderDoesNotMatter() {
    Schema existing = new Schema(
        Types.NestedField.optional(1, "cik", Types.StringType.get()),
        Types.NestedField.optional(2, "accession", Types.StringType.get()));

    // The YAML declares columns in a different order than Iceberg assigned field IDs in — the
    // config has no notion of field ID at all, so this must not be read as every column changing.
    String added = IcebergMaterializer.pureColumnAdditions(existing,
        Arrays.asList(col("new_col", "VARCHAR"), col("accession", "VARCHAR"), col("cik", "VARCHAR")));

    assertEquals("new_col", added);
  }

  // ===== ensureTableExists, against a real Hadoop catalog =====

  @Test void testAddingAColumnPreservesExistingRows() throws Exception {
    IcebergMaterializer materializer = newMaterializer();
    String tableId = "evo_add_" + UUID.randomUUID().toString().substring(0, 8);

    IcebergMaterializer.TableSetupResult created = materializer.ensureTableExists(
        configWith(tableId, Arrays.asList(col("cik", "VARCHAR"), col("accession", "VARCHAR"))));
    assertFalse(created.wasRecreated, "a table that did not exist yet is created, not recreated");

    appendRow(created.table, "0000320193", "0000320193-21-000001");
    long snapshotBefore = created.table.currentSnapshot().snapshotId();
    assertEquals(1, countRows(created.table));

    IcebergMaterializer.TableSetupResult evolved = materializer.ensureTableExists(
        configWith(tableId, Arrays.asList(
            col("cik", "VARCHAR"), col("accession", "VARCHAR"), col("arc_use", "VARCHAR"))));

    assertFalse(evolved.wasRecreated, "a pure column addition must not be reported as a recreate");
    assertEquals(3, evolved.table.schema().columns().size());
    assertEquals(snapshotBefore, evolved.table.currentSnapshot().snapshotId(),
        "adding a column is a metadata-only change; it must not touch the data snapshot");
    assertEquals(1, countRows(evolved.table),
        "the row written before the column existed must still be there");
  }

  @Test void testNonAdditiveChangeStillDropsAndRecreates() throws Exception {
    IcebergMaterializer materializer = newMaterializer();
    String tableId = "evo_drop_" + UUID.randomUUID().toString().substring(0, 8);

    IcebergMaterializer.TableSetupResult created = materializer.ensureTableExists(
        configWith(tableId, Arrays.asList(col("cik", "VARCHAR"), col("accession", "VARCHAR"))));
    appendRow(created.table, "0000320193", "0000320193-21-000001");
    assertEquals(1, countRows(created.table));

    // "accession" retyped to BIGINT alongside a genuinely new column: more columns than before,
    // but not a pure addition, so the legacy behavior — the only thing that can be relied on when
    // the schema-level compatibility check cannot rule out data loss — must still apply.
    IcebergMaterializer.TableSetupResult recreated = materializer.ensureTableExists(
        configWith(tableId, Arrays.asList(
            col("cik", "VARCHAR"), col("accession", "BIGINT"), col("new_col", "VARCHAR"))));

    assertTrue(recreated.wasRecreated,
        "a retyped column cannot be proven safe, so the table is still dropped and recreated");
    // A freshly created table always carries one empty snapshot (IcebergCatalogManager commits it
    // so the table is queryable before any data arrives) — currentSnapshot() is never null, so row
    // count, not snapshot presence, is what proves the row from before the drop is really gone.
    assertEquals(0, countRows(recreated.table),
        "the recreated table is empty — this is the pre-existing behavior, unchanged");
  }

  /** Appends one row through a real Parquet DataWriter, matching how production data lands. */
  private static void appendRow(Table table, String cik, String accession) throws Exception {
    OutputFile outputFile = table.io().newOutputFile(
        table.location() + "/data/file-" + UUID.randomUUID() + ".parquet");
    DataWriter<Record> writer = Parquet.writeData(outputFile)
        .schema(table.schema())
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .build();
    GenericRecord record = GenericRecord.create(table.schema());
    record.setField("cik", cik);
    record.setField("accession", accession);
    writer.write(record);
    writer.close();
    table.newAppend().appendFile(writer.toDataFile()).commit();
  }

  private static long countRows(Table table) throws Exception {
    long total = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        total += task.file().recordCount();
      }
    }
    return total;
  }
}
