/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holders.
 */
package org.apache.calcite.adapter.file.partition;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for the per-batch anti-bloat compaction fix in
 * {@link S3HivePipelineTracker}.
 *
 * <h2>The bug being guarded</h2>
 * <p>When the active year (e.g. year=2026) has many straggler individual marker
 * files, the fast-path straggler loop processed them in batches but only deleted
 * them AFTER the full scan completed (in the caller's {@code deleteSpecificFiles}
 * call, gated on {@code fullyScannedYears}).  A SIGKILL or timeout between batches
 * left all stragglers undeleted; each run added new ones — unbounded snowball.
 *
 * <h2>The fix</h2>
 * <p>After each batch is successfully merged into stageCache, immediately write
 * a new {@code _compacted/} file (write-before-delete), then delete that batch's
 * individual files.  Each batch is then durable + cleaned regardless of whether
 * the full scan completes.
 *
 * <h2>Test strategy</h2>
 * <ul>
 *   <li>Uses a local filesystem path as {@code bucketPath} so that DuckDB can
 *       read and write parquet files without httpfs / S3 connectivity.</li>
 *   <li>Mocks the AWS S3 client only for {@code listTrackerFiles} (the S3 listing)
 *       and {@code deleteSpecificFiles} (the S3 delete); downloads are served from
 *       real local files via the mock {@code getObject}.</li>
 *   <li>The straggler S3 URIs returned by the mock list are mapped back to local
 *       files by the mock {@code getObject}, so {@code downloadTrackerFilesParallel}
 *       works without network access.</li>
 *   <li>The {@code compactFromCache} COPY goes to the local filesystem, which
 *       succeeds without any S3 credentials.</li>
 * </ul>
 */
@Tag("unit")
public class S3HivePipelineTrackerAntiBloatTest {

  @TempDir
  Path tempDir;

  private S3HivePipelineTracker tracker;
  private Connection duckConn;

  /** All S3 keys that were submitted to deleteObjects during the test. */
  private final List<String> deletedKeys = new CopyOnWriteArrayList<String>();

  @BeforeEach
  void setUp() throws Exception {
    // Use a local filesystem path so DuckDB reads/writes go to local disk (no httpfs).
    // The tracker is created with an empty config — S3 ops use the mock client only.
    tracker = new S3HivePipelineTracker(
        tempDir.toAbsolutePath().toString(), null,
        Collections.<String, String>emptyMap());

    // Shared real DuckDB connection for creating test parquet files
    duckConn = DriverManager.getConnection("jdbc:duckdb:");
  }

  @AfterEach
  void tearDown() throws Exception {
    if (duckConn != null && !duckConn.isClosed()) {
      duckConn.close();
    }
    if (tracker != null) {
      tracker.close();
    }
    deletedKeys.clear();
  }

  // ===== Core regression tests =====

  /**
   * Invariant (a): marker completeness — reading a year after full straggler
   * compaction must reflect every completed table that was in the individual files.
   *
   * Invariant (b): forward progress — the anti-bloat loop must delete the stragglers
   * that it successfully folded, so the recorded deletedKeys set covers all
   * individual straggler files that were successfully merged in this run.
   */
  @Test
  void testStragglersAreCompactedAndDeletedPerBatch() throws Exception {
    // Create 3 individual straggler parquet files on local disk
    File stragg1 = createTrackerParquet("year=2026__key=a", "tableA", "staging");
    File stragg2 = createTrackerParquet("year=2026__key=b", "tableB", "staging");
    File stragg3 = createTrackerParquet("year=2026__key=c", "tableC", "staging");

    // Create a base compacted file on disk so the fast path is taken.
    // Place it in the _compacted/ dir under the local tracker root.
    File compactedDir = new File(tempDir.toFile(), "year=2026/_compacted");
    compactedDir.mkdirs();
    File compactedBase = new File(compactedDir, "base.parquet");
    createTrackerParquetAt("year=2026__key=base", "tableBase", "staging", compactedBase);

    // Wire mock S3: list returns stragglers; getObject serves local bytes; deleteObjects records.
    AmazonS3 mockS3 = buildMockS3(
        new Straggler("s3stragg1", stragg1, "year=2026__key=a"),
        new Straggler("s3stragg2", stragg2, "year=2026__key=b"),
        new Straggler("s3stragg3", stragg3, "year=2026__key=c"));
    injectS3Client(mockS3);

    // Inject a real DuckDB connection with no httpfs configuration needed
    injectDuckdbConnection(tracker);

    Method scanMethod = S3HivePipelineTracker.class
        .getDeclaredMethod("scanAndCacheYear", String.class);
    scanMethod.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<String> returned = (List<String>) scanMethod.invoke(tracker, "2026");

    // (a) All 4 tables (3 stragglers + base) must be in stageCache
    Map<String, Set<String>> stageCache = getStageCache(tracker);
    Set<String> allCached = new HashSet<String>();
    for (Set<String> tables : stageCache.values()) {
      allCached.addAll(tables);
    }
    assertTrue(allCached.contains("tableA"),
        "tableA must be in stageCache after straggler merge");
    assertTrue(allCached.contains("tableB"),
        "tableB must be in stageCache after straggler merge");
    assertTrue(allCached.contains("tableC"),
        "tableC must be in stageCache after straggler merge");
    assertTrue(allCached.contains("tableBase"),
        "tableBase (from compacted base) must still be in stageCache");

    // (b) All 3 straggler S3 keys must have been submitted for deletion.
    // The exact key value depends on how deleteSpecificFiles strips the bucket prefix,
    // so we use contains() checks which are stable regardless of the local tempDir path.
    assertTrue(!deletedKeys.isEmpty(),
        "At least some straggler keys must have been submitted for deletion");
    assertTrue(anyDeletedKeyContains("s3stragg1"),
        "s3stragg1 key must be in deletedKeys. Got: " + deletedKeys);
    assertTrue(anyDeletedKeyContains("s3stragg2"),
        "s3stragg2 key must be in deletedKeys. Got: " + deletedKeys);
    assertTrue(anyDeletedKeyContains("s3stragg3"),
        "s3stragg3 key must be in deletedKeys. Got: " + deletedKeys);

    // At least one _compacted/ file must have been written on disk (additive batch compaction)
    File[] compactedFiles = compactedDir.listFiles();
    assertTrue(compactedFiles != null && compactedFiles.length >= 2,
        "At least 2 _compacted/ files must exist (base + 1 per-batch write). Found: "
            + (compactedFiles == null ? 0 : compactedFiles.length));
  }

  /**
   * Invariant (b) forward progress under partial scan: even a single-batch scan
   * must delete the files it processed, so straggler count decreases every run.
   *
   * <p>Since READ_BATCH_SIZE=10000 and we only seed 2 stragglers, the whole
   * set fits in one batch.  After the scan, both must be deleted.
   */
  @Test
  void testSingleBatchForwardProgress() throws Exception {
    File stragg1 = createTrackerParquet("year=2026__key=p1", "tableProg1", "staging");
    File stragg2 = createTrackerParquet("year=2026__key=p2", "tableProg2", "staging");

    File compactedDir = new File(tempDir.toFile(), "year=2026/_compacted");
    compactedDir.mkdirs();
    File compactedBase = new File(compactedDir, "base2.parquet");
    createTrackerParquetAt("year=2026__key=base2", "tableBase2", "staging", compactedBase);

    AmazonS3 mockS3 = buildMockS3(
        new Straggler("fp1", stragg1, "year=2026__key=p1"),
        new Straggler("fp2", stragg2, "year=2026__key=p2"));
    injectS3Client(mockS3);
    injectDuckdbConnection(tracker);

    Method scanMethod = S3HivePipelineTracker.class
        .getDeclaredMethod("scanAndCacheYear", String.class);
    scanMethod.setAccessible(true);
    scanMethod.invoke(tracker, "2026");

    // Both in one batch; both must have been deleted
    assertTrue(anyDeletedKeyContains("fp1"),
        "fp1 must be deleted (forward progress). Got: " + deletedKeys);
    assertTrue(anyDeletedKeyContains("fp2"),
        "fp2 must be deleted (forward progress). Got: " + deletedKeys);
  }

  /**
   * Invariant (c): idempotent reads — a marker present in BOTH a _compacted/ file
   * and an individual straggler (the interrupted window between compacted-write and
   * straggler-delete) must not produce two entries in the stageCache set.
   *
   * <p>The {@code readTrackerGlobAllPhases} query uses ROW_NUMBER+latest-wins
   * dedup, so inserting the same (source_key, table_name, phase) twice yields
   * exactly one cache entry.
   */
  @Test
  void testDuplicateMarkerDeduplicatedInStageCache() throws Exception {
    // tableA already in stageCache (simulating it was in the compacted file)
    Map<String, Set<String>> stageCache = getStageCache(tracker);
    Set<String> existing = new LinkedHashSet<String>();
    existing.add("tableA");
    stageCache.put("year=2026__key=a\0staging", existing);

    // Create a straggler that also claims tableA (duplicate during interrupted window)
    File duplicate = createTrackerParquet("year=2026__key=a", "tableA", "staging");

    File compactedDir = new File(tempDir.toFile(), "year=2026/_compacted");
    compactedDir.mkdirs();
    File compactedBase = new File(compactedDir, "base3.parquet");
    createTrackerParquetAt("year=2026__key=base3", "tableBase3", "staging", compactedBase);

    AmazonS3 mockS3 = buildMockS3(
        new Straggler("dup1", duplicate, "year=2026__key=a"));
    injectS3Client(mockS3);
    injectDuckdbConnection(tracker);

    Method scanMethod = S3HivePipelineTracker.class
        .getDeclaredMethod("scanAndCacheYear", String.class);
    scanMethod.setAccessible(true);
    scanMethod.invoke(tracker, "2026");

    // tableA must appear exactly once — the Set semantics ensure this
    Set<String> tables = stageCache.get("year=2026__key=a\0staging");
    assertTrue(tables != null && tables.contains("tableA"),
        "tableA must remain in stageCache");
    assertEquals(1, tables.size(),
        "tableA must appear exactly once (no duplicate); stageCache entry: " + tables);
  }

  /**
   * noCompact mode: when {@code calcite.tracker.noCompact=true} is set, the
   * per-batch compaction must be skipped entirely — no _compacted/ files written,
   * no straggler deletes.
   */
  @Test
  void testNoCompactModeSkipsPerBatchCompaction() throws Exception {
    System.setProperty("calcite.tracker.noCompact", "true");
    try {
      S3HivePipelineTracker noCompactTracker = new S3HivePipelineTracker(
          tempDir.toAbsolutePath().toString(), null,
          Collections.<String, String>emptyMap());
      try {
        File stragg = createTrackerParquet("year=2026__key=nc", "tableNC", "staging");
        File compactedDir = new File(tempDir.toFile(), "year=2026/_compacted");
        compactedDir.mkdirs();
        File compactedBase = new File(compactedDir, "base_nc.parquet");
        createTrackerParquetAt("year=2026__key=baseNC", "tableBaseNC", "staging", compactedBase);
        int initialCompactedCount = compactedDir.list() == null ? 0 : compactedDir.list().length;

        AmazonS3 mockS3 = buildMockS3(
            new Straggler("nc1", stragg, "year=2026__key=nc"));
        injectS3Client(noCompactTracker, mockS3);
        injectDuckdbConnection(noCompactTracker);

        Method scanMethod = S3HivePipelineTracker.class
            .getDeclaredMethod("scanAndCacheYear", String.class);
        scanMethod.setAccessible(true);
        scanMethod.invoke(noCompactTracker, "2026");

        // noCompact mode: no straggler deletes from the per-batch loop
        assertTrue(deletedKeys.isEmpty(),
            "noCompact mode must not delete any files. Got: " + deletedKeys);

        // noCompact mode: no new _compacted/ files written
        int finalCompactedCount = compactedDir.list() == null ? 0 : compactedDir.list().length;
        assertEquals(initialCompactedCount, finalCompactedCount,
            "noCompact mode must not write any _compacted/ files");
      } finally {
        noCompactTracker.close();
      }
    } finally {
      System.clearProperty("calcite.tracker.noCompact");
    }
  }

  // ===== Helpers =====

  /** Simple record for wiring straggler files to mock S3 keys. */
  private static final class Straggler {
    final String s3Key;   // used in S3 key path: source_key={s3Key}/{s3Key}.parquet
    final File localFile; // real local parquet file to serve from getObject
    final String sourceKey; // tracker sourceKey to verify stageCache

    Straggler(String s3Key, File localFile, String sourceKey) {
      this.s3Key = s3Key;
      this.localFile = localFile;
      this.sourceKey = sourceKey;
    }
  }

  /**
   * Create a tracker parquet file at the given destination path.
   */
  private void createTrackerParquetAt(String sourceKey, String tableName,
      String phase, File dest) throws Exception {
    long asOf = System.currentTimeMillis();
    try (Statement stmt = duckConn.createStatement()) {
      stmt.execute("COPY (SELECT "
          + "'" + escape(sourceKey) + "' AS source_key, "
          + "'" + escape(tableName) + "' AS table_name, "
          + "'" + escape(phase) + "' AS phase, "
          + "'complete' AS state, "
          + "100 AS row_count, "
          + "NULL::VARCHAR AS config_hash, "
          + "NULL::VARCHAR AS signature, "
          + "NULL::VARCHAR AS error_message, "
          + asOf + " AS as_of"
          + ") TO '" + dest.getAbsolutePath().replace("'", "''") + "' (FORMAT PARQUET)");
    }
  }

  /**
   * Create a tracker parquet file in {@link #tempDir} with a name derived
   * from the sourceKey.
   */
  private File createTrackerParquet(String sourceKey, String tableName, String phase)
      throws Exception {
    String safeName = sourceKey.replaceAll("[^A-Za-z0-9_-]", "_");
    File f = new File(tempDir.toFile(), safeName + ".parquet");
    createTrackerParquetAt(sourceKey, tableName, phase, f);
    return f;
  }

  private static String escape(String s) {
    return s.replace("'", "''");
  }

  /**
   * Build a mock S3 client that:
   * <ul>
   *   <li>Returns a straggler listing for the {@code source_key=} prefix query.</li>
   *   <li>Returns an empty listing for the {@code _compacted/} prefix query (used by
   *       {@code deleteCompactedFiles}; the actual compacted file is read by DuckDB
   *       directly from the local filesystem, not via the S3 client).</li>
   *   <li>Serves {@code getObject} from the real local files.</li>
   *   <li>Records {@code deleteObjects} calls in {@link #deletedKeys}.</li>
   *   <li>Accepts {@code putObject} calls silently (tracker may upload temp
   *       files; we verify side effects via the filesystem instead).</li>
   * </ul>
   */
  private AmazonS3 buildMockS3(Straggler... stragglers) throws Exception {
    AmazonS3 mock = mock(AmazonS3.class);

    // Straggler listing
    ListObjectsV2Result stragglerResult = new ListObjectsV2Result();
    stragglerResult.setTruncated(false);
    for (Straggler s : stragglers) {
      S3ObjectSummary summary = new S3ObjectSummary();
      summary.setKey(
          "tracker/year=2026/source_key=" + s.s3Key + "/" + s.s3Key + ".parquet");
      stragglerResult.getObjectSummaries().add(summary);
    }

    // Empty listing for _compacted/ prefix (deleteCompactedFiles)
    ListObjectsV2Result emptyResult = new ListObjectsV2Result();
    emptyResult.setTruncated(false);

    when(mock.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(
        invocation -> {
          ListObjectsV2Request req = invocation.getArgument(0);
          String prefix = req.getPrefix() != null ? req.getPrefix() : "";
          if (prefix.contains("_compacted/")) {
            return emptyResult;
          }
          return stragglerResult;
        });

    // getObject: serve from real local file bytes
    when(mock.getObject(anyString(), anyString())).thenAnswer(invocation -> {
      String key = invocation.getArgument(1);
      for (Straggler s : stragglers) {
        if (key.contains(s.s3Key)) {
          return buildS3Object(s.localFile);
        }
      }
      // Unknown key — return empty object (prevents NPE, signals "not found")
      S3Object obj = new S3Object();
      obj.setObjectContent(new S3ObjectInputStream(
          new ByteArrayInputStream(new byte[0]), null));
      return obj;
    });

    // deleteObjects: record keys
    when(mock.deleteObjects(any(DeleteObjectsRequest.class))).thenAnswer(invocation -> {
      DeleteObjectsRequest req = invocation.getArgument(0);
      for (DeleteObjectsRequest.KeyVersion kv : req.getKeys()) {
        deletedKeys.add(kv.getKey());
      }
      return new DeleteObjectsResult(
          Collections.<DeleteObjectsResult.DeletedObject>emptyList());
    });

    // putObject: no-op (we inspect the filesystem for compacted writes)
    when(mock.putObject(anyString(), anyString(), any(InputStream.class),
        any(ObjectMetadata.class))).thenReturn(null);

    return mock;
  }

  private S3Object buildS3Object(File localFile) throws Exception {
    byte[] bytes = Files.readAllBytes(localFile.toPath());
    S3Object obj = new S3Object();
    obj.setObjectContent(new S3ObjectInputStream(
        new ByteArrayInputStream(bytes), null));
    return obj;
  }

  private void injectS3Client(AmazonS3 client) throws Exception {
    injectS3Client(tracker, client);
  }

  private void injectS3Client(S3HivePipelineTracker target, AmazonS3 client) throws Exception {
    Field f = S3HivePipelineTracker.class.getDeclaredField("s3Client");
    f.setAccessible(true);
    f.set(target, client);
  }

  private void injectDuckdbConnection(S3HivePipelineTracker target) throws Exception {
    // Give the tracker a fresh in-memory DuckDB connection.
    // No httpfs configuration is needed because bucketPath is a local path.
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    Field connField = S3HivePipelineTracker.class.getDeclaredField("connection");
    connField.setAccessible(true);
    connField.set(target, conn);

    Field initField = S3HivePipelineTracker.class.getDeclaredField("initialized");
    initField.setAccessible(true);
    initField.set(target, true);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Set<String>> getStageCache(S3HivePipelineTracker t) throws Exception {
    Field f = S3HivePipelineTracker.class.getDeclaredField("stageCache");
    f.setAccessible(true);
    return (Map<String, Set<String>>) f.get(t);
  }

  /** Returns true if any recorded deleted key contains the given substring. */
  private boolean anyDeletedKeyContains(String substring) {
    for (String key : deletedKeys) {
      if (key.contains(substring)) {
        return true;
      }
    }
    return false;
  }
}
