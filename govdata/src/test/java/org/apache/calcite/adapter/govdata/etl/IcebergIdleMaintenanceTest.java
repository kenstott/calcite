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

import org.apache.calcite.adapter.file.iceberg.CrossProcessCommitLock;
import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.iceberg.IcebergTableWriter;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.YamlUtils;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A table that compacts on its last write and then idles must still shed the files the
 * compaction replaced once its snapshots age past retention, with no further write.
 */
@Tag("unit")
public class IcebergIdleMaintenanceTest {

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "name", Types.StringType.get()),
      Types.NestedField.optional(2, "id", Types.IntegerType.get()),
      Types.NestedField.optional(3, "year", Types.IntegerType.get()));

  private static final Pattern TIMESTAMP_MS = Pattern.compile("\"timestamp-ms\"\\s*:\\s*\\d+");

  @TempDir
  Path tempDir;

  private Table table;
  private IcebergTableWriter writer;
  private final StorageProvider storage = new LocalFileStorageProvider();

  @BeforeEach void setUp() throws Exception {
    Map<String, Object> catalogConfig = new HashMap<>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", tempDir.resolve("warehouse").toString());
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("year").build();
    table = IcebergCatalogManager.createTable(catalogConfig, "idle_test", SCHEMA, spec);
    writer = new IcebergTableWriter(table, storage);

    for (int f = 0; f < 6; f++) {
      List<Map<String, Object>> rows = new ArrayList<>();
      for (int r = 0; r < 20; r++) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("name", "name-" + f + "-" + r);
        row.put("id", f * 20 + r);
        row.put("year", 2024);
        rows.add(row);
      }
      Map<String, String> partition = new HashMap<>();
      partition.put("year", "2024");
      DataFile df = writer.writeRecords(rows, partition);
      assertNotNull(df);
      table.newAppend().appendFile(df).commit();
    }
    int compacted = writer.compactSmallFiles(128L * 1024 * 1024, 2, 64L * 1024 * 1024, 7,
        new ArrayList<String>());
    assertTrue(compacted > 0, "precondition: the compaction rewrote the partition");
    table.refresh();
  }

  @AfterEach void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  /** Rewrites every snapshot timestamp in the metadata files to {@code ageDays} ago. */
  private void backdateSnapshots(int ageDays) throws IOException {
    long old = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(ageDays);
    Path metadata = java.nio.file.Paths.get(table.location().replace("file:", ""), "metadata");
    try (DirectoryStream<Path> files = Files.newDirectoryStream(metadata, "*.metadata.json")) {
      for (Path file : files) {
        String json = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        Matcher m = TIMESTAMP_MS.matcher(json);
        String rewritten = m.replaceAll(Matcher.quoteReplacement("\"timestamp-ms\" : " + old));
        Files.write(file, rewritten.getBytes(StandardCharsets.UTF_8));
      }
    }
    // The local filesystem keeps a checksum beside each file; the rewrite invalidates it.
    try (DirectoryStream<Path> sums = Files.newDirectoryStream(metadata, ".*.crc")) {
      for (Path sum : sums) {
        Files.delete(sum);
      }
    }
    table = new org.apache.iceberg.hadoop.HadoopTables(new org.apache.hadoop.conf.Configuration())
        .load(table.location());
  }

  private Set<String> liveRowNames() throws IOException {
    Set<String> paths = new HashSet<>();
    for (FileScanTask task : table.newScan().planFiles()) {
      paths.add(task.file().path().toString());
    }
    return paths;
  }

  private long liveRowCount() {
    long rows = 0;
    for (FileScanTask task : table.newScan().planFiles()) {
      rows += task.file().recordCount();
    }
    return rows;
  }

  @Test void expiresReplacedFilesOfAnIdleCompactedTable() throws Exception {
    backdateSnapshots(26);
    Set<String> liveBefore = liveRowNames();
    long rowsBefore = liveRowCount();
    assertEquals(120, rowsBefore);

    IcebergIdleMaintenance.Outcome outcome =
        IcebergIdleMaintenance.expireIdleTable("idle_test", table, storage, 7);

    assertFalse(outcome.skippedBusy);
    assertTrue(outcome.filesDeleted() > 0,
        "superseded pre-compaction files must be deleted: " + outcome);
    assertEquals(1, outcome.snapshotsAfter, "only the current snapshot remains: " + outcome);
    assertEquals(liveBefore.size(), outcome.liveFiles);
    assertEquals(outcome.liveFiles, outcome.filesOnDiskAfter,
        "nothing but the live files is left on disk: " + outcome);
    table.refresh();
    assertEquals(liveBefore, liveRowNames(), "the live file set is unchanged");
    assertEquals(rowsBefore, liveRowCount(), "the live rows are unchanged");
  }

  @Test void keepsFilesWhileASnapshotIsInsideRetention() throws Exception {
    backdateSnapshots(2);
    int onDiskBefore = IcebergIdleMaintenance.expireIdleTable("idle_test", table, storage, 7)
        .filesOnDiskBefore;

    IcebergIdleMaintenance.Outcome outcome =
        IcebergIdleMaintenance.expireIdleTable("idle_test", table, storage, 7);

    assertEquals(0, outcome.filesDeleted(), "no file may go while its snapshot is retained");
    assertEquals(onDiskBefore, outcome.filesOnDiskAfter);
    assertEquals(outcome.snapshotsBefore, outcome.snapshotsAfter);
  }

  @Test void skipsATableWhoseCommitLockIsHeld() throws Exception {
    backdateSnapshots(26);
    CrossProcessCommitLock.Handle held = CrossProcessCommitLock.tryAcquire(table.location());
    assertNotNull(held);
    IcebergIdleMaintenance.Outcome outcome;
    try {
      // The lock is held by this thread, so tryAcquire inside the pass sees it as taken only when
      // taken by another holder; run the pass from another thread to stand in for that holder.
      final IcebergIdleMaintenance.Outcome[] holder = new IcebergIdleMaintenance.Outcome[1];
      Thread other = new Thread(new Runnable() {
        @Override public void run() {
          try {
            holder[0] = IcebergIdleMaintenance.expireIdleTable("idle_test", table, storage, 7);
          } catch (IOException e) {
            throw new IllegalStateException(e);
          }
        }
      });
      other.start();
      other.join();
      outcome = holder[0];
    } finally {
      held.close();
    }
    assertTrue(outcome.skippedBusy, "a table mid-commit is skipped: " + outcome);
  }

  @Test void readsTargetsFromTheSchemasOwnMaintenanceConfig() throws Exception {
    String yaml = "partitionedTables:\n"
        + "  - name: kept\n"
        + "    materialize:\n"
        + "      iceberg:\n"
        + "        warehousePath: s3://b/econ\n"
        + "        runMaintenance: true\n"
        + "        snapshotRetentionDays: 14\n"
        + "  - name: not_maintained\n"
        + "    materialize:\n"
        + "      iceberg:\n"
        + "        warehousePath: s3://b/econ\n";
    JsonNode root = YamlUtils.parseYamlOrJson(
        new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), "t.yaml");

    List<IcebergIdleMaintenance.Target> targets = IcebergIdleMaintenance.targetsOf(root, "t.yaml");

    assertEquals(1, targets.size());
    assertEquals("kept", targets.get(0).name);
    assertEquals(14, targets.get(0).retentionDays);
    assertEquals("s3://b/econ", targets.get(0).warehouse);
  }

  @Test void tableWithoutWarehousePathResolvesToTheSchemasMaterializeDirectory() throws Exception {
    String yaml = "schemaName: \"ag\"\n"
        + "materializeDirectory: \"s3://b\"\n"
        + "partitionedTables:\n"
        + "  - name: plain\n"
        + "    materialize:\n"
        + "      iceberg:\n"
        + "        runMaintenance: true\n"
        + "  - name: explicit\n"
        + "    materialize:\n"
        + "      iceberg:\n"
        + "        warehousePath: s3://b/other\n"
        + "        runMaintenance: true\n";
    JsonNode root = YamlUtils.parseYamlOrJson(
        new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), "t.yaml");

    List<IcebergIdleMaintenance.Target> targets = IcebergIdleMaintenance.targetsOf(root, "t.yaml");

    assertEquals("s3://b/ag", targets.get(0).warehouse);
    assertEquals("s3://b/other", targets.get(1).warehouse);
  }

  @Test void tableWithNoWarehouseAnywhereIsRejected() throws Exception {
    String yaml = "partitionedTables:\n"
        + "  - name: orphan\n"
        + "    materialize:\n"
        + "      iceberg:\n"
        + "        runMaintenance: true\n";
    JsonNode root = YamlUtils.parseYamlOrJson(
        new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), "t.yaml");

    org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
        () -> IcebergIdleMaintenance.targetsOf(root, "t.yaml"));
  }

  @Test void everyShippedSchemaResolvesItsMaintenanceTargets() throws Exception {
    Path resources = Path.of(IcebergIdleMaintenance.class.getResource("/ag/ag-schema.yaml").toURI())
        .getParent().getParent();
    int tables = 0;
    try (DirectoryStream<Path> dirs = Files.newDirectoryStream(resources)) {
      for (Path dir : dirs) {
        if (!Files.isDirectory(dir)) {
          continue;
        }
        try (DirectoryStream<Path> yamls = Files.newDirectoryStream(dir, "*-schema.yaml")) {
          for (Path yaml : yamls) {
            JsonNode root = YamlUtils.parseYamlOrJson(Files.newInputStream(yaml), yaml.toString());
            try {
              tables += IcebergIdleMaintenance.targetsOf(root, yaml.toString()).size();
            } catch (IllegalStateException e) {
              // GOVDATA_PARQUET_DIR is the one variable the script requires the caller to supply
              assertTrue(e.getMessage().contains("'${GOVDATA_PARQUET_DIR}"), e.getMessage());
              tables++;
            }
          }
        }
      }
    }
    assertTrue(tables > 0, "no maintenance tables found");
  }
}
