/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.iceberg.IcebergTableWriter;
import org.apache.calcite.adapter.file.storage.S3StorageProvider;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * One-time production migration: adds {@code ingest_ts} to {@code ref.vectorized_chunks} and
 * simultaneously deduplicates existing rows (some sources' writers append rather than
 * delete-before-insert on reprocess — see that column's own comment in ref-schema.yaml).
 *
 * <p>The actual read-and-dedup happens entirely outside the JVM: {@code migrate-via-pg.sh}
 * bulk-loads the source Iceberg table into a Postgres staging table (stamping {@code ingest_ts}
 * at insert time) and dedups it there with a plain SQL self-join {@code DELETE} keyed on
 * {@code chunk_id}; {@code export-to-s3-staging.sh} then has DuckDB write that already-deduped
 * table straight to an S3 staging path, Hive-partitioned by {@code source_schema}, in the exact
 * physical Parquet layout Iceberg expects.
 *
 * <p>This class never reads a single row: it registers those externally-written files as Iceberg
 * {@link DataFile}s via {@link IcebergTableWriter#stageFiles}, which computes record count and
 * column statistics from each file's Parquet footer alone (the same footer-only mechanism Spark's
 * {@code add_files} and Trino's {@code register_table} use), then commits the replacement as an
 * explicit atomic {@link RewriteFiles}: every data file currently live in the table (enumerated
 * before staging) is deleted and every newly-staged file is added, in one commit validated
 * against the starting snapshot. {@code IcebergTableWriter#replacePartitionsDataFiles} (Iceberg's
 * dynamic partition-overwrite operation) looked like the natural fit but, tested against a
 * scratch table, left a stale file behind in a partition it should have replaced — an explicit
 * file-by-file rewrite has no such ambiguity: it names precisely what it deletes and precisely
 * what it adds. An earlier version of this migration read every row through DuckDB's JDBC driver
 * embedded in this same JVM to hand rows to {@link IcebergTableWriter#writeRecords}; that path is
 * gone because embedding DuckDB that way let its native memory grow unbounded regardless of
 * {@code memory_limit}, and a separate LIMIT/OFFSET pagination attempt at working around it
 * silently duplicated and dropped rows (LIMIT/OFFSET across separate query executions is not
 * guaranteed deterministic without ORDER BY). Since DuckDB has no Iceberg write support, the
 * S3-staging + footer-registration split keeps every byte of row data inside DuckDB's own address
 * space and lets the JVM deal only in file paths and footer metadata.
 *
 * <p>Requires {@code MIGRATE_VECTORIZED_CHUNKS_CONFIRM=yes} — a full-table rewrite of production
 * data must never fire from a stray test run.
 */
@Tag("integration")
public class VectorizedChunksIngestTsMigrationIT {

  // Overridable so this exact class can be dry-run against a small scratch table before ever
  // pointing it at the real 25M-row production table -- see the class comment.
  private static final String WAREHOUSE_PATH =
      System.getenv().getOrDefault("MIGRATE_WAREHOUSE_PATH", "s3a://govdata-parquet-v1/ref");
  private static final String TABLE_ID = "vectorized_chunks";
  // Populated by export-to-s3-staging.sh: source_schema=<value>/*.parquet, already deduped and
  // already carrying ingest_ts.
  private static final String STAGING_PATH = System.getenv("MIGRATE_S3_STAGING_PATH");

  @Test
  public void migrate() throws Exception {
    String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
    assumeTrue(accessKey != null && !accessKey.isEmpty(), "AWS_ACCESS_KEY_ID not set");
    assumeTrue(secretKey != null && !secretKey.isEmpty(), "AWS_SECRET_ACCESS_KEY not set");
    assumeTrue(endpoint != null && !endpoint.isEmpty(), "AWS_ENDPOINT_OVERRIDE not set");
    assumeTrue(STAGING_PATH != null && !STAGING_PATH.isEmpty(),
        "MIGRATE_S3_STAGING_PATH not set — run export-to-s3-staging.sh first");
    assumeTrue("yes".equals(System.getenv("MIGRATE_VECTORIZED_CHUNKS_CONFIRM")),
        "MIGRATE_VECTORIZED_CHUNKS_CONFIRM=yes not set — refusing to rewrite production data");

    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("accessKeyId", accessKey);
    storageConfig.put("secretAccessKey", secretKey);
    storageConfig.put("endpoint", endpoint);
    storageConfig.put("region", "auto");
    S3StorageProvider storageProvider = new S3StorageProvider(storageConfig);

    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.s3a.access.key", accessKey);
    hadoopConfig.put("fs.s3a.secret.key", secretKey);
    hadoopConfig.put("fs.s3a.endpoint", endpoint);
    hadoopConfig.put("fs.s3a.path.style.access", "true");

    Map<String, Object> catalogConfig = new HashMap<>();
    catalogConfig.put("warehousePath", WAREHOUSE_PATH);
    catalogConfig.put("hadoopConfig", hadoopConfig);

    Table table = IcebergCatalogManager.loadTable(catalogConfig, TABLE_ID);
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    if (table.schema().findField("ingest_ts") == null) {
      System.err.println("[migration] adding ingest_ts column (schema evolution, metadata-only)");
      table.updateSchema().addColumn("ingest_ts", Types.TimestampType.withoutZone()).commit();
      table.refresh();
    } else {
      System.err.println("[migration] ingest_ts column already present — proceeding to dedup pass");
    }

    // DuckDB-written Parquet carries no Iceberg field IDs, so footer-metrics registration (via
    // stageFiles below) needs a name mapping to bind footer column stats to the right field IDs,
    // and readers need the same mapping as a table property to resolve columns by name -- see
    // IcebergMaterializer's identical setup for the same file-passthrough shape. Must be (re)done
    // after the schema evolution above so ingest_ts is included.
    String mappingJson = NameMappingParser.toJson(MappingUtil.create(table.schema()));
    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();
    table.refresh();

    // Captured before staging, for optimistic-concurrency validation and as the exact set of
    // files this commit deletes.
    Snapshot startSnapshot = table.currentSnapshot();
    Long startSnapshotId = startSnapshot == null ? null : startSnapshot.snapshotId();
    List<DataFile> oldFiles = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask t : tasks) {
        oldFiles.add(t.file());
      }
    }
    System.err.println("[migration] " + oldFiles.size() + " existing data file(s) to replace");

    System.err.println("[migration] staging files from " + STAGING_PATH);
    List<DataFile> newFiles = writer.stageFiles(STAGING_PATH);
    assumeTrue(!newFiles.isEmpty(), "stageFiles found no Parquet files under " + STAGING_PATH);

    long total = 0;
    for (DataFile f : newFiles) {
      total += f.recordCount();
    }
    System.err.println("[migration] " + newFiles.size() + " data file(s) staged, " + total
        + " row(s) total — committing atomically");

    RewriteFiles rewrite = table.newRewrite();
    if (startSnapshotId != null) {
      rewrite.validateFromSnapshot(startSnapshotId);
    }
    for (DataFile f : oldFiles) {
      rewrite.deleteFile(f);
    }
    for (DataFile f : newFiles) {
      rewrite.addFile(f);
    }
    rewrite.commit();
    System.err.println("[migration] committed: " + oldFiles.size() + " old file(s) replaced by "
        + newFiles.size() + " new file(s), " + total + " total row(s)");
  }
}
