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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.iceberg.IcebergMaterializer;
import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.adapter.file.storage.S3StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Builds, on purpose, the race between a forced pass and another worker that absorbs part of the
 * pass's staged files, against the DQ bucket only.
 *
 * <p>Sixty accessions (thirty in each of two year partitions) are staged as twelve metadata
 * files. Another worker absorbs four of them (twenty accessions) and then the forced pass claims
 * the other eight (forty accessions). The pass's operand forces all sixty. The forced delete scope
 * is the only variable: the previous scope (the whole operand) against
 * {@link SecSchemaFactory#forcedWithClaimedReplacement}. Five accessions also carry an older row
 * from an earlier attempt, to show that a claimed replacement still replaces exactly once.
 *
 * <p>Environment (the DQ launch environment, {@code govdata/.env.dq} plus the MinIO credentials):
 * {@code GOVDATA_DQ_BUCKET}, {@code AWS_ACCESS_KEY_ID}, {@code AWS_SECRET_ACCESS_KEY},
 * {@code AWS_ENDPOINT_OVERRIDE}. The test refuses to run unless the bucket name ends in
 * {@code -dq}.
 */
@Tag("integration")
public class ForcedPassConcurrentAbsorbDqTest {

  private static final int PER_FILE = 5;
  private static final int[] YEARS = {2025, 2026};
  private static final int FILES_PER_YEAR = 6;

  private static String accession(int year, int n) {
    return String.format("0000000001-%02d-%06d", year % 100, n);
  }

  @Test public void previousScopeLosesRowsAndClaimedScopeKeepsThem() throws Exception {
    String bucket = System.getenv("GOVDATA_DQ_BUCKET");
    String key = System.getenv("AWS_ACCESS_KEY_ID");
    String secret = System.getenv("AWS_SECRET_ACCESS_KEY");
    String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
    assumeTrue(bucket != null && key != null && secret != null && endpoint != null,
        "DQ environment not sourced");
    assertTrue(bucket.endsWith("-dq"), "refusing to run against bucket " + bucket);
    assertTrue(endpoint.contains("localhost"), "refusing to run against endpoint " + endpoint);

    String run = Long.toString(System.currentTimeMillis());
    Result previous = scenario(bucket, key, secret, endpoint, run + "_previous", false);
    Result fixed = scenario(bucket, key, secret, endpoint, run + "_fixed", true);

    StringBuilder table = new StringBuilder();
    table.append("bucket=").append(bucket).append(" endpoint=").append(endpoint).append('\n');
    table.append("scope | rows | distinct | absent | old_rows_left | survived_other_worker\n");
    for (Result r : Arrays.asList(previous, fixed)) {
      table.append(r.label).append(" | ").append(r.rows).append(" | ").append(r.distinct)
          .append(" | ").append(r.absent).append(" | ").append(r.oldRowsLeft).append(" | ")
          .append(r.otherWorkerSurvived).append("/20\n");
    }
    File out = new File("build/race-1063-results.txt");
    Files.createDirectories(out.getAbsoluteFile().getParentFile().toPath());
    Files.write(out.toPath(),
        table.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));

    assertTrue(previous.absent > 0, "previous scope should lose the other worker's rows");
    assertEquals(0, fixed.absent, "claimed scope must leave no accession absent");
    assertEquals(fixed.rows, fixed.distinct, "no accession duplicated");
    assertEquals(60, fixed.rows);
    assertEquals(0, fixed.oldRowsLeft, "claimed replacements replace the older row exactly once");
    assertEquals(20, fixed.otherWorkerSurvived, "the other worker's rows survive");
  }

  private static final class Result {
    String label;
    long rows;
    long distinct;
    long absent;
    long oldRowsLeft;
    long otherWorkerSurvived;
  }

  private Result scenario(String bucket, String key, String secret, String endpoint, String id,
      boolean claimedScope) throws Exception {
    Map<String, Object> cfg = new HashMap<String, Object>();
    cfg.put("accessKeyId", key);
    cfg.put("secretAccessKey", secret);
    cfg.put("endpoint", endpoint);
    cfg.put("region", "us-east-1");
    S3StorageProvider storage = new S3StorageProvider(cfg);

    String base = "s3://" + bucket + "/ci-test/race-1063/" + id;
    String warehouse = "s3a://" + bucket + "/ci-test/race-1063/" + id + "/warehouse";
    String tableId = "filing_metadata_race";
    String pattern = base + "/sec/year=*/*metadata*.parquet";

    // Twelve staged files, six per year, five accessions each: file f of year y holds accessions
    // y-numbered f*5 .. f*5+4.
    List<String> all = new ArrayList<String>();
    Set<String> operand = new HashSet<String>();
    Map<String, List<String>> fileAccessions = new HashMap<String, List<String>>();
    for (int year : YEARS) {
      for (int f = 0; f < FILES_PER_YEAR; f++) {
        List<String> accs = new ArrayList<String>();
        for (int i = 0; i < PER_FILE; i++) {
          accs.add(accession(year, f * PER_FILE + i));
        }
        String path = base + "/sec/year=" + year + "/metadata_batch_" + String.format("%04d", f)
            + ".parquet";
        stage(storage, path, accs, year, "new");
        all.add(path);
        fileAccessions.put(path, accs);
        operand.addAll(accs);
      }
    }
    // The other worker absorbs files 0 and 1 of each year (20 accessions).
    List<String> other = new ArrayList<String>();
    Set<String> otherAccs = new HashSet<String>();
    for (int year : YEARS) {
      for (int f = 0; f < 2; f++) {
        String p = base + "/sec/year=" + year + "/metadata_batch_" + String.format("%04d", f)
            + ".parquet";
        other.add(p);
        otherAccs.addAll(fileAccessions.get(p));
      }
    }
    List<String> claimed = new ArrayList<String>(all);
    claimed.removeAll(other);

    // An older row from an earlier attempt for five accessions the pass will claim (year 2026,
    // file 2), absorbed first so the table exists and holds them.
    String oldPath = base + "/sec/year=2026/metadata_batch_old.parquet";
    List<String> oldAccs = fileAccessions.get(base + "/sec/year=2026/metadata_batch_0002.parquet");
    stage(storage, oldPath, oldAccs, 2026, "old");

    IcebergMaterializer materializer =
        new IcebergMaterializer(warehouse, storage, IncrementalTracker.NOOP);
    assertEquals(0, materializer.materialize(config(pattern, warehouse, tableId,
        Collections.singletonList(oldPath), Collections.<String>emptySet())).getFailedCount());
    assertEquals(0, materializer.materialize(config(pattern, warehouse, tableId, other,
        Collections.<String>emptySet())).getFailedCount());

    // The forced pass: its operand forces every accession it staged.
    Set<String> forced;
    if (claimedScope) {
      Set<String> restaged = materializer.distinctValuesInFiles(claimed, "accession_number");
      forced = SecSchemaFactory.forcedWithClaimedReplacement(operand, restaged, false);
    } else {
      forced = operand;
    }
    assertEquals(0, materializer.materialize(config(pattern, warehouse, tableId, claimed,
        forced)).getFailedCount());

    Result r = new Result();
    r.label = claimedScope ? "claimed " : "previous";
    String location = warehouse + "/" + tableId;
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
        Statement st = c.createStatement()) {
      st.execute("SET memory_limit='2GB'");
      st.execute("INSTALL httpfs");
      st.execute("LOAD httpfs");
      st.execute("INSTALL iceberg");
      st.execute("LOAD iceberg");
      st.execute("CREATE SECRET dq (TYPE S3, KEY_ID '" + key + "', SECRET '" + secret
          + "', ENDPOINT '" + endpoint.replace("http://", "") + "', URL_STYLE 'path', "
          + "USE_SSL false)");
      String scan = "iceberg_scan('" + location.replace("s3a://", "s3://")
          + "', allow_moved_paths => true)";
      r.rows = one(st, "SELECT count(*) FROM " + scan);
      r.distinct = one(st, "SELECT count(DISTINCT accession_number) FROM " + scan);
      r.absent = operand.size() - r.distinct;
      r.oldRowsLeft = one(st, "SELECT count(*) FROM " + scan + " WHERE version = 'old'");
      r.otherWorkerSurvived = one(st, "SELECT count(DISTINCT accession_number) FROM " + scan
          + " WHERE accession_number IN (" + quoted(otherAccs) + ")");
    }
    return r;
  }

  private static String quoted(Set<String> values) {
    StringBuilder sb = new StringBuilder();
    for (String v : values) {
      if (sb.length() > 0) {
        sb.append(',');
      }
      sb.append('\'').append(v).append('\'');
    }
    return sb.toString();
  }

  private static long one(Statement st, String sql) throws Exception {
    try (ResultSet rs = st.executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private static IcebergMaterializer.MaterializationConfig config(String pattern,
      String warehouse, String tableId, List<String> staged, Set<String> forced) {
    List<IcebergCatalogManager.ColumnDef> columns = Arrays.asList(
        new IcebergCatalogManager.ColumnDef("cik", "string"),
        new IcebergCatalogManager.ColumnDef("accession_number", "string"),
        new IcebergCatalogManager.ColumnDef("version", "string"),
        new IcebergCatalogManager.ColumnDef("year", "int"));
    List<PartitionedTableConfig.ColumnDefinition> partitions =
        Collections.singletonList(new PartitionedTableConfig.ColumnDefinition("year", "int"));
    return IcebergMaterializer.MaterializationConfig.builder()
        .sourcePattern(pattern)
        .sourceFormat(IcebergMaterializer.SourceFormat.PARQUET)
        .targetTableId(tableId)
        .sourceTableName("filing_metadata")
        .tableColumns(columns)
        .partitionColumns(partitions)
        .batchPartitionColumns(Collections.singletonList("year"))
        .incrementalKeys(Collections.singletonList("year"))
        .yearRange(2025, 2026)
        .icebergTableLocation(warehouse + "/" + tableId)
        .accessionColumn("accession_number")
        .stagedSourceFiles(staged)
        .forceAccessions(forced)
        .description("forced pass race, DQ only")
        .build();
  }

  private static void stage(S3StorageProvider storage, String s3Path, List<String> accessions,
      int year, String version) throws Exception {
    File local = File.createTempFile("race1063", ".parquet");
    local.delete();
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
        Statement st = c.createStatement()) {
      st.execute("COPY (SELECT '0000000001' AS cik, a AS accession_number, '" + version
          + "' AS version, " + year + " AS year FROM (VALUES "
          + valuesList(accessions) + ") t(a)) TO '" + local.getAbsolutePath()
          + "' (FORMAT PARQUET)");
    }
    storage.writeFile(s3Path, Files.readAllBytes(local.toPath()));
    local.delete();
  }

  private static String valuesList(List<String> accessions) {
    StringBuilder sb = new StringBuilder();
    for (String a : accessions) {
      if (sb.length() > 0) {
        sb.append(',');
      }
      sb.append("('").append(a).append("')");
    }
    return sb.toString();
  }
}
