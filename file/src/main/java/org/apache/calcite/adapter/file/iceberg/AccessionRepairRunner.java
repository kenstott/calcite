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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One-off repair tool: replaces the committed rows for a specific, already-materialized set of
 * accession numbers with freshly regenerated data for the same accessions, already sitting in
 * source parquet under the given glob (e.g. after a corrected re-run of the ETL for those
 * accessions).
 *
 * <p>The normal incremental materialization path in {@link IcebergMaterializer} intentionally
 * never re-appends an accession already committed to Iceberg (accession-level dedup, to keep an
 * append-only pipeline from duplicating rows on every run). That is correct for ordinary
 * incremental ingestion but has no path for correcting bad data already committed under an
 * accession that pipeline logic still considers "done" — this tool is that path, used
 * explicitly and only for a scoped, known-affected accession list.
 *
 * <p>Usage:
 * <pre>{@code
 * java -cp sih-govdata.jar org.apache.calcite.adapter.file.iceberg.AccessionRepairRunner \
 *   --warehouse s3://bucket/sec \
 *   --table filing_metadata \
 *   --year 2026 \
 *   --source-glob 's3://bucket/sec/year=2026/*_metadata.parquet' \
 *   --accessions-file /path/to/accession_numbers.txt \
 *   [--dry-run]
 * }</pre>
 */
public class AccessionRepairRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(AccessionRepairRunner.class);

  public static void main(String[] args) throws Exception {
    String warehouse = null;
    String tableName = null;
    Integer year = null;
    String sourceGlob = null;
    String accessionsFile = null;
    boolean dryRun = false;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
      case "--warehouse":
        warehouse = args[++i];
        break;
      case "--table":
        tableName = args[++i];
        break;
      case "--year":
        year = Integer.parseInt(args[++i]);
        break;
      case "--source-glob":
        sourceGlob = args[++i];
        break;
      case "--accessions-file":
        accessionsFile = args[++i];
        break;
      case "--dry-run":
        dryRun = true;
        break;
      default:
        System.err.println("Unknown argument: " + args[i]);
        System.exit(1);
      }
    }

    if (warehouse == null || tableName == null || year == null
        || sourceGlob == null || accessionsFile == null) {
      System.err.println("Usage: AccessionRepairRunner --warehouse <path> --table <name> "
          + "--year <int> --source-glob <glob> --accessions-file <path> [--dry-run]");
      System.exit(1);
    }

    Set<String> accessions = readAccessions(accessionsFile);
    System.out.println("Loaded " + accessions.size() + " target accession numbers");

    Configuration conf = buildHadoopConf();
    String hadoopWarehouse = warehouse.replace("s3://", "s3a://");
    String tablePath = hadoopWarehouse + "/" + tableName;
    // A writable table load (not the read-only StaticTableOperations pattern CompactionRunner
    // uses to dodge R2's 403-for-missing-key on version-hint.text): this tool commits new
    // snapshots, and the target here is local MinIO, which returns a real 404 for a missing key,
    // so HadoopTables' normal version-hint discovery works.
    Table table = new HadoopTables(conf).load(tablePath);

    Connection conn = openDuckDb();
    List<Map<String, Object>> correctedRows;
    try {
      correctedRows = fetchCorrectedRows(conn, sourceGlob, accessionsFile);
    } finally {
      conn.close();
    }
    System.out.println("Read " + correctedRows.size() + " corrected rows from source glob");

    Set<String> correctedAccessions = new HashSet<>();
    for (Map<String, Object> row : correctedRows) {
      Object acc = row.get("accession_number");
      if (acc != null) {
        correctedAccessions.add(acc.toString());
      }
    }
    Set<String> missing = new HashSet<>(accessions);
    missing.removeAll(correctedAccessions);
    if (!missing.isEmpty()) {
      System.out.println("WARNING: " + missing.size()
          + " target accessions have no corrected source row (source file missing/not yet written) "
          + "— they will be left as-is (NOT deleted): sample="
          + missing.stream().limit(5).toArray().length);
    }

    // Only touch accessions we actually have a corrected replacement row for.
    Set<String> toReplace = new HashSet<>(correctedAccessions);
    toReplace.retainAll(accessions);
    System.out.println("Accessions to delete+replace: " + toReplace.size());

    if (dryRun) {
      System.out.println("DRY RUN — no changes committed.");
      return;
    }

    if (toReplace.isEmpty()) {
      System.out.println("Nothing to do.");
      return;
    }

    // Iceberg's deleteFromRowFilter() only supports whole-data-file deletes: it throws
    // ValidationException when a candidate file has a mix of matching/non-matching rows, which an
    // accession_number IN (...) filter over a handful of large per-year files always produces
    // (verified: deleting the affected accessions this way failed on the very first batch,
    // atomically, before touching the table). A full partition replace sidesteps row-level
    // delete semantics entirely: read every row currently in the year partition, drop the
    // to-be-replaced accessions, union in their corrected rows, and atomically swap the whole
    // partition via the already-used ReplacePartitions path.
    Map<String, Map<String, Object>> correctedByAccession = new LinkedHashMap<>();
    for (Map<String, Object> row : correctedRows) {
      Object acc = row.get("accession_number");
      if (acc != null && toReplace.contains(acc.toString())) {
        correctedByAccession.put(acc.toString(), row);
      }
    }

    Connection scanConn = openDuckDb();
    List<Map<String, Object>> rowsToWrite;
    try {
      rowsToWrite = fetchUnaffectedPartitionRows(scanConn, warehouse, tableName, year, toReplace);
    } finally {
      scanConn.close();
    }
    System.out.println("Read " + rowsToWrite.size() + " unaffected rows already in year=" + year);
    rowsToWrite.addAll(correctedByAccession.values());
    System.out.println("Rebuilding year=" + year + " partition with " + rowsToWrite.size()
        + " total rows (" + correctedByAccession.size() + " corrected)");

    IcebergTableWriter writer = new IcebergTableWriter(table, null);
    Map<String, String> partitionValues = Collections.singletonMap("year", String.valueOf(year));
    DataFile dataFile = writer.writeRecords(rowsToWrite, partitionValues);
    if (dataFile != null) {
      writer.replacePartitionsDataFiles(Collections.singletonList(dataFile));
      System.out.println("Replaced year=" + year + " partition with " + rowsToWrite.size()
          + " rows (" + correctedByAccession.size() + " corrected) in " + tableName);
    } else {
      System.out.println("writeRecords returned no data file — nothing committed");
    }
  }

  private static List<Map<String, Object>> fetchUnaffectedPartitionRows(Connection conn,
      String warehouse, String tableName, int year, Set<String> excludeAccessions)
      throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSTALL iceberg");
      stmt.execute("LOAD iceberg");
      stmt.execute("SET unsafe_enable_version_guessing = true");
    }
    List<Map<String, Object>> rows = new ArrayList<>();
    String icebergLocation = warehouse + "/" + tableName;
    String sql = "SELECT * FROM iceberg_scan('" + icebergLocation + "', allow_moved_paths=true) "
        + "WHERE year = " + year;
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      ResultSetMetaData meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();
      int accessionCol = -1;
      for (int i = 1; i <= columnCount; i++) {
        if ("accession_number".equalsIgnoreCase(meta.getColumnName(i))) {
          accessionCol = i;
          break;
        }
      }
      if (accessionCol < 0) {
        throw new IllegalStateException("No accession_number column in " + icebergLocation);
      }
      while (rs.next()) {
        String accession = rs.getString(accessionCol);
        if (accession != null && excludeAccessions.contains(accession)) {
          continue;
        }
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= columnCount; i++) {
          row.put(meta.getColumnName(i), rs.getObject(i));
        }
        rows.add(row);
      }
    }
    return rows;
  }

  private static Set<String> readAccessions(String path) throws Exception {
    Set<String> accessions = new HashSet<>();
    // storage-provider-guard: allow — local CLI input file for this standalone repair tool,
    // same as CompactionRunner's local --warehouse/--table args; not a data-pipeline path.
    try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
      String line;
      boolean first = true;
      while ((line = reader.readLine()) != null) {
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
          continue;
        }
        String accession = trimmed.split(",")[0].trim();
        if (first && accession.equalsIgnoreCase("accession_number")) {
          first = false;
          continue;
        }
        first = false;
        accessions.add(accession);
      }
    }
    return accessions;
  }

  private static Connection openDuckDb() throws Exception {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSTALL httpfs");
      stmt.execute("LOAD httpfs");
      stmt.execute("INSTALL parquet");
      stmt.execute("LOAD parquet");
      String endpoint = System.getenv("AWS_ENDPOINT_URL");
      if (endpoint == null) {
        endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
      }
      if (endpoint != null) {
        endpoint = endpoint.replaceFirst("^https?://", "");
        stmt.execute("SET s3_endpoint='" + endpoint + "'");
      }
      String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
      String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
      if (accessKey != null) {
        stmt.execute("SET s3_access_key_id='" + accessKey + "'");
      }
      if (secretKey != null) {
        stmt.execute("SET s3_secret_access_key='" + secretKey + "'");
      }
      stmt.execute("SET s3_region='auto'");
      stmt.execute("SET s3_url_style='path'");
    }
    return conn;
  }

  private static List<Map<String, Object>> fetchCorrectedRows(Connection conn, String sourceGlob,
      String accessionsFile) throws Exception {
    List<Map<String, Object>> rows = new ArrayList<>();
    // DISTINCT-per-accession: the source glob can contain more than one write for the same
    // accession (retry-safe batch flush), which would otherwise create duplicate rows on append.
    String sql = "SELECT p.* FROM ("
        + "  SELECT p.*, ROW_NUMBER() OVER (PARTITION BY p.accession_number) AS rn"
        + "  FROM read_parquet('" + sourceGlob + "', union_by_name=true) p"
        + "  JOIN read_csv('" + accessionsFile + "', header=false, "
        + "    columns={'accession_number':'VARCHAR','cik':'VARCHAR'}) a"
        + "  ON p.accession_number = a.accession_number"
        + ") p WHERE rn = 1";
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      ResultSetMetaData meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();
      while (rs.next()) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= columnCount; i++) {
          row.put(meta.getColumnName(i), rs.getObject(i));
        }
        rows.add(row);
      }
    }
    return rows;
  }

  private static Configuration buildHadoopConf() {
    Configuration conf = new Configuration();
    String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
    if (accessKey != null) {
      conf.set("fs.s3a.access.key", accessKey);
    }
    if (secretKey != null) {
      conf.set("fs.s3a.secret.key", secretKey);
    }
    if (endpoint != null) {
      conf.set("fs.s3a.endpoint", endpoint);
      conf.set("fs.s3a.path.style.access", "true");
      conf.set("fs.s3a.change.detection.mode", "none");
      conf.set("fs.s3a.change.detection.version.required", "false");
    }
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    return conf;
  }

}
