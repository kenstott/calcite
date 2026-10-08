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

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetUtil;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * One-off repair tool: registers data files that already sit in a table's own storage location —
 * recovered from a mirror after a schema-drift or concurrent-writer drop-and-recreate physically
 * deleted them from the live lineage (kenstott/govdata-ops#610) — as one new append snapshot on
 * the live table.
 *
 * <p>Commits through {@link Table#newAppend()}, the same path every ordinary materialization
 * write uses, so the version-hint, manifest lists and snapshot history stay exactly as consistent
 * as a normal write leaves them — never a hand-edited {@code metadata.json} (see
 * {@code IcebergMaterializationWriter}'s own schema-drift drop+recreate for why that distinction
 * matters: a table-level drop is a supported operation precisely because every write, repair
 * included, goes through the catalog API instead of the raw files).
 *
 * <p>Row count and column-level metrics are recomputed from the actual recovered file at commit
 * time rather than trusted from whatever manifest identified the file as recoverable, so a restore
 * is verified against the real recovered bytes, not stale bookkeeping. Files must already be
 * present at the path given in the manifest — copy them back from the mirror (e.g. {@code mc cp})
 * before running this; it registers files, it does not fetch them.
 *
 * <p>Usage:
 * <pre>{@code
 * java -cp sih-govdata.jar org.apache.calcite.adapter.file.iceberg.IcebergOrphanFileRestoreRunner \
 *   --table-path s3://govdata-parquet-v1/research/nih_award_projects \
 *   --manifest /path/to/restore-manifest.tsv \
 *   --endpoint localhost:9002 \
 *   --access-key-id "$AWS_ACCESS_KEY_ID" \
 *   --secret-access-key "$AWS_SECRET_ACCESS_KEY" \
 *   --region auto \
 *   [--dry-run]
 * }</pre>
 *
 * <p>Manifest format: one line per file, tab-separated {@code path\tpartitionPath\trecordCount}.
 */
public final class IcebergOrphanFileRestoreRunner {

  private IcebergOrphanFileRestoreRunner() {
  }

  public static void main(String[] args) throws Exception {
    String tablePath = null;
    String manifestPath = null;
    String endpoint = null;
    String accessKeyId = null;
    String secretAccessKey = null;
    String region = null;
    boolean dryRun = false;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
      case "--table-path":
        tablePath = args[++i];
        break;
      case "--manifest":
        manifestPath = args[++i];
        break;
      case "--endpoint":
        endpoint = args[++i];
        break;
      case "--access-key-id":
        accessKeyId = args[++i];
        break;
      case "--secret-access-key":
        secretAccessKey = args[++i];
        break;
      case "--region":
        region = args[++i];
        break;
      case "--dry-run":
        dryRun = true;
        break;
      default:
        System.err.println("Unknown argument: " + args[i]);
        System.exit(1);
      }
    }
    if (tablePath == null || manifestPath == null || endpoint == null
        || accessKeyId == null || secretAccessKey == null || region == null) {
      System.err.println("Usage: IcebergOrphanFileRestoreRunner --table-path <s3://...> "
          + "--manifest <path.tsv> --endpoint <host:port> --access-key-id <id> "
          + "--secret-access-key <key> --region <region> [--dry-run]");
      System.exit(1);
    }

    List<String[]> entries = readManifest(manifestPath);
    System.out.println("Loaded " + entries.size() + " file(s) to restore from " + manifestPath);

    Map<String, String> s3Config = new HashMap<>();
    s3Config.put("endpoint", endpoint);
    s3Config.put("accessKeyId", accessKeyId);
    s3Config.put("secretAccessKey", secretAccessKey);
    s3Config.put("region", region);
    s3Config.put("pathStyleAccess", "true");

    Table table = S3FileIOTables.loadWritable(tablePath, s3Config);
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);

    List<DataFile> staged = new ArrayList<>();
    long totalRecords = 0;
    for (String[] entry : entries) {
      String path = entry[0];
      String partitionPath = entry[1];
      long recordCount = Long.parseLong(entry[2]);

      InputFile inputFile = table.io().newInputFile(path);
      if (!inputFile.exists()) {
        throw new IllegalStateException(
            "File not found at expected path (copy it back from the mirror before running "
                + "this): " + path);
      }
      Metrics metrics = ParquetUtil.fileMetrics(inputFile, metricsConfig);
      if (metrics.recordCount() != recordCount) {
        throw new IllegalStateException(
            "Row count mismatch for " + path + ": manifest says " + recordCount
                + ", file actually has " + metrics.recordCount());
      }

      DataFile dataFile = DataFiles.builder(table.spec())
          .withPath(path)
          .withFormat(FileFormat.PARQUET)
          .withFileSizeInBytes(inputFile.getLength())
          .withPartitionPath(partitionPath)
          .withRecordCount(recordCount)
          .withMetrics(metrics)
          .build();
      staged.add(dataFile);
      totalRecords += recordCount;
      System.out.println("Verified " + path + " (" + recordCount + " rows, partition "
          + partitionPath + ")");
    }

    if (dryRun) {
      System.out.println("DRY RUN — verified " + staged.size() + " file(s), " + totalRecords
          + " total rows. No snapshot committed.");
      return;
    }

    AppendFiles append = table.newAppend();
    for (DataFile dataFile : staged) {
      append.appendFile(dataFile);
    }
    append.commit();

    Snapshot snapshot = table.currentSnapshot();
    System.out.println("Committed snapshot " + snapshot.snapshotId() + " with " + staged.size()
        + " file(s), " + totalRecords + " rows appended to " + tablePath);
  }

  private static List<String[]> readManifest(String manifestPath) throws Exception {
    List<String[]> entries = new ArrayList<>();
    // storage-provider-guard: allow — local CLI input file for this standalone repair tool,
    // not schema-managed data.
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(manifestPath), Charset.defaultCharset()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }
        String[] parts = line.split("\t");
        if (parts.length != 3) {
          throw new IllegalArgumentException("Malformed manifest line: " + line);
        }
        entries.add(parts);
      }
    }
    if (entries.isEmpty()) {
      throw new IllegalArgumentException("Manifest is empty: " + manifestPath);
    }
    return entries;
  }
}
