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

import org.apache.calcite.adapter.file.partition.PipelineTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the R2 Class A LIST op elimination in {@link SecFilingCache}.
 *
 * <p>Verifies the three-tier strategy for {@code fileExists()} calls:
 * <ol>
 *   <li>In-memory cache (populated by {@link SecFilingCache#preloadFileInventory(int, int)}) —
 *       zero Class A ops after initial scan.</li>
 *   <li>Exact year path (derived from {@code filingDate}) — 1 Class B HEAD op, not 11
 *       Class A LIST ops, when cache is unavailable.</li>
 *   <li>Glob fallback ({@code year=*}) — used only when both cache and year are absent.</li>
 * </ol>
 *
 * <p>All tests are pure unit tests (no S3, no DuckDB, no network).
 */
@Tag("unit")
public class SecFilingCacheFileInventoryTest {

  // Production: govdataParquetDir already includes /sec (set by materializeDirectory in
  // sec-schema.yaml). SecFilingCache receives this value directly as parquetBaseDir.
  private static final String PARQUET_BASE = "s3://govdata-parquet-v1/sec";

  // -------------------------------------------------------------------------
  // 1. preloadFileInventory populates the cache
  // -------------------------------------------------------------------------

  @Test
  void preloadFileInventory_populatesCacheFromListFiles() throws IOException {
    String cik = "0000320193";
    String accession = "0000320193-24-000006";

    // Build the exact paths that XbrlToParquetConverter would write
    List<String> knownPaths = buildExpectedPaths(cik, accession, "2024");

    TrackingStorageProvider provider = new TrackingStorageProvider(knownPaths);
    SecFilingCache cache = new SecFilingCache(
        new InMemoryPipelineTracker(), provider, PARQUET_BASE);

    cache.preloadFileInventory(2024, 2024);

    // preloadFileInventory(2024, 2024) internally scans year=2023 (fiscal buffer) + year=2024.
    // Each year gets 2 listFiles calls: primary path + legacy path (migration fallback).
    assertEquals(4, provider.listFilesCallCount(),
        "preloadFileInventory must call listFiles 4 times: 2 years x (primary + legacy)");
    // exists() must NOT have been called — everything from the cache
    assertEquals(0, provider.existsCallCount(),
        "preloadFileInventory must not call exists()");

    // checkS3Files with known filing date — all files present in cache
    FileInventory inventory = cache.checkS3Files(cik, accession, "2024-03-15");
    assertTrue(inventory.hasMetadata(), "metadata should be found in cache");
    assertTrue(inventory.hasFacts(), "facts should be found in cache");
    assertTrue(inventory.hasContexts(), "contexts should be found in cache");
    assertTrue(inventory.hasRelationships(), "relationships should be found in cache");
    assertTrue(inventory.hasMda(), "mda should be found in cache");
    assertTrue(inventory.hasInsider(), "insider should be found in cache");
    assertTrue(inventory.hasEarnings(), "earnings should be found in cache");
    assertTrue(inventory.hasChunks(), "chunks should be found in cache");

    // Still no exists() calls — everything answered from in-memory cache
    assertEquals(0, provider.existsCallCount(),
        "checkS3Files must not call exists() when cache is populated");
  }

  // -------------------------------------------------------------------------
  // 2. Cache hit for unknown year (no filingDate) — O(1) byName map lookup, no exists()
  // -------------------------------------------------------------------------

  @Test
  void preloadedCache_answersCorrectlyWhenNoFilingDateProvided() throws IOException {
    String cik = "0000320193";
    String accession = "0000320193-24-000006";
    List<String> knownPaths = buildExpectedPaths(cik, accession, "2024");

    TrackingStorageProvider provider = new TrackingStorageProvider(knownPaths);
    SecFilingCache cache = new SecFilingCache(
        new InMemoryPipelineTracker(), provider, PARQUET_BASE);
    cache.preloadFileInventory(2024, 2024);
    provider.resetCounters();

    // null filingDate — cache must still answer via byName map (O(1))
    FileInventory inventory = cache.checkS3Files(cik, accession, null);
    assertTrue(inventory.hasMetadata(), "metadata found via cache byName map");
    assertEquals(0, provider.existsCallCount(),
        "no exists() calls when cache is available, even without filingDate");
  }

  // -------------------------------------------------------------------------
  // 3. File absent from cache returns false, no exists() call
  // -------------------------------------------------------------------------

  @Test
  void preloadedCache_returnsFalseForUnknownFile() throws IOException {
    // Cache contains paths for a DIFFERENT accession
    String knownCik = "0000320193";
    String knownAccession = "0000320193-24-000006";
    List<String> knownPaths = buildExpectedPaths(knownCik, knownAccession, "2024");

    TrackingStorageProvider provider = new TrackingStorageProvider(knownPaths);
    SecFilingCache cache = new SecFilingCache(
        new InMemoryPipelineTracker(), provider, PARQUET_BASE);
    cache.preloadFileInventory(2024, 2024);
    provider.resetCounters();

    // Check a filing NOT in the cache
    FileInventory inventory = cache.checkS3Files("0000789019", "0000789019-24-000001", "2024-06-01");
    assertFalse(inventory.hasMetadata(), "unknown filing should not be found in cache");
    assertFalse(inventory.hasFacts(), "unknown filing should not be found in cache");
    assertEquals(0, provider.existsCallCount(),
        "no exists() calls even for unknown filing when cache is populated");
  }

  // -------------------------------------------------------------------------
  // 4. No cache + filingDate → exact year= path, NOT year=* glob
  // -------------------------------------------------------------------------

  @Test
  void noCacheWithFilingDate_usesExactYearPath() throws IOException {
    String cik = "0000320193";
    String accession = "0000320193-24-000006";

    // Provider returns false for every exists() — we only care about the paths queried
    TrackingStorageProvider provider = new TrackingStorageProvider(Collections.<String>emptyList());
    SecFilingCache cache = new SecFilingCache(
        new InMemoryPipelineTracker(), provider, PARQUET_BASE);
    // Do NOT call preloadFileInventory — cache stays null

    cache.checkS3Files(cik, accession, "2024-03-15");

    assertTrue(provider.existsCallCount() > 0, "exists() must be called when no cache");
    for (String queried : provider.queriedExistsPaths()) {
      assertFalse(queried.contains("year=*"),
          "With a known filingDate, must NOT use year=* glob: " + queried);
      assertTrue(queried.contains("year=2024"),
          "With filingDate=2024-03-15, must use year=2024: " + queried);
    }
  }

  // -------------------------------------------------------------------------
  // 5. No cache + no filingDate → glob (year=*) fallback
  // -------------------------------------------------------------------------

  @Test
  void noCacheNoFilingDate_fallsBackToYearGlob() throws IOException {
    String cik = "0000320193";
    String accession = "0000320193-24-000006";

    TrackingStorageProvider provider = new TrackingStorageProvider(Collections.<String>emptyList());
    SecFilingCache cache = new SecFilingCache(
        new InMemoryPipelineTracker(), provider, PARQUET_BASE);

    cache.checkS3Files(cik, accession, null);  // null filingDate

    assertTrue(provider.existsCallCount() > 0, "exists() must be called when no cache");
    for (String queried : provider.queriedExistsPaths()) {
      assertTrue(queried.contains("year=*"),
          "With no filingDate and no cache, must use year=* glob: " + queried);
    }
  }

  // -------------------------------------------------------------------------
  // 6. checkFiling passes filingDate into checkS3Files (integration of flow)
  // -------------------------------------------------------------------------

  @Test
  void checkFiling_passesFilingDateThroughToFileExistsWhenHealingPath() throws IOException {
    String cik = "0000320193";
    String accession = "0000320193-24-000006";
    String formType = "10-K";
    String filingDate = "2024-03-15";

    // Provider has all 8 files present for the exact year=2024 path
    List<String> presentPaths = buildExpectedPaths(cik, accession, "2024");
    TrackingStorageProvider provider = new TrackingStorageProvider(presentPaths);

    // Empty tracker — forces the "not in tracker → checkS3Files" branch
    InMemoryPipelineTracker tracker = new InMemoryPipelineTracker();
    SecFilingCache cache = new SecFilingCache(tracker, provider, PARQUET_BASE);

    // Self-healing path: tracker empty → checkS3Files called → all files found → SKIP
    ProcessingDecision decision = cache.checkFiling(cik, accession, formType, filingDate, false);
    assertEquals(ProcessingDecision.Action.SKIP, decision.getAction(),
        "Self-healing: all files present → decision must be SKIP");

    // All exists() queries must have used year=2024, not year=*
    for (String queried : provider.queriedExistsPaths()) {
      assertTrue(queried.contains("year=2024"),
          "checkFiling must pass filingDate to fileExists, got: " + queried);
      assertFalse(queried.contains("year=*"),
          "checkFiling must not use year=* glob when filingDate is known: " + queried);
    }
  }

  // -------------------------------------------------------------------------
  // 7. preloadFileInventory + checkFiling → zero exists() calls end-to-end
  // -------------------------------------------------------------------------

  @Test
  void preloadedCache_checkFiling_zeroExistsCallsOnSelfHealingPath() throws IOException {
    String cik = "0000320193";
    String accession = "0000320193-24-000006";
    String formType = "10-K";
    String filingDate = "2024-03-15";

    List<String> presentPaths = buildExpectedPaths(cik, accession, "2024");
    TrackingStorageProvider provider = new TrackingStorageProvider(presentPaths);
    InMemoryPipelineTracker tracker = new InMemoryPipelineTracker();
    SecFilingCache cache = new SecFilingCache(tracker, provider, PARQUET_BASE);

    cache.preloadFileInventory(2024, 2024);
    provider.resetCounters();  // reset after the single listFiles scan

    ProcessingDecision decision = cache.checkFiling(cik, accession, formType, filingDate, false);
    assertEquals(ProcessingDecision.Action.SKIP, decision.getAction(),
        "Self-healing with cache: all files present → SKIP");
    assertEquals(0, provider.existsCallCount(),
        "With preloaded cache, checkFiling must not issue any exists() calls");
  }

  // -------------------------------------------------------------------------
  // 8. no_xbrl sentinel detected via preload
  // -------------------------------------------------------------------------

  @Test
  void preloadedCache_detectsNoXbrlSentinel() throws IOException {
    String cik = "0001234567";
    String accession = "0001234567-25-000042";
    String sentinelPath = PARQUET_BASE + "/year=2025/" + cik + "_" + accession + "_no_xbrl.parquet";

    TrackingStorageProvider provider = new TrackingStorageProvider(
        Collections.singletonList(sentinelPath));
    SecFilingCache cache = new SecFilingCache(
        new InMemoryPipelineTracker(), provider, PARQUET_BASE);
    cache.preloadFileInventory(2025, 2025);
    provider.resetCounters();

    FileInventory inventory = cache.checkS3Files(cik, accession, "2025-03-10");
    assertTrue(inventory.hasNoXbrl(), "no_xbrl sentinel must be detected after preload");
    assertFalse(inventory.hasAnyFiles(), "sentinel must not count as a regular data file");
    assertEquals(0, provider.existsCallCount(), "no S3 calls when cache is populated");
  }

  // -------------------------------------------------------------------------
  // 9. Fiscal-year buffer: Jan-Apr filing output at year-1 found via byName
  // -------------------------------------------------------------------------

  @Test
  void preloadedCache_findsFiscalYearShiftedFilingViaPriorYearScan() throws IOException {
    String cik = "0000320193";
    // accession with 25 = filed in 2025 (Jan-Apr), output written to year=2024 by getPartitionYear
    String accession = "0000320193-25-000010";
    List<String> knownPaths = buildExpectedPaths(cik, accession, "2024");

    TrackingStorageProvider provider = new TrackingStorageProvider(knownPaths);
    SecFilingCache cache = new SecFilingCache(
        new InMemoryPipelineTracker(), provider, PARQUET_BASE);
    // preloadFileInventory(2025, 2025) also scans year=2024 (fiscal buffer) internally
    cache.preloadFileInventory(2025, 2025);
    provider.resetCounters();

    // filingDate "2025-02-15" → exact path check uses year=2025 (not in cache),
    // but byName fallback finds the year=2024 entry populated from the fiscal buffer scan
    FileInventory inventory = cache.checkS3Files(cik, accession, "2025-02-15");
    assertTrue(inventory.hasMetadata(),
        "Jan-Apr filing with output at year=2024 must be found via byName fiscal buffer");
    assertTrue(inventory.hasFacts(),
        "Jan-Apr filing with output at year=2024 must be found via byName fiscal buffer");
    assertEquals(0, provider.existsCallCount(), "no S3 calls when cache is populated");
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Builds the 8 expected parquet paths (one per table type) for a filing.
   */
  private static List<String> buildExpectedPaths(String cik, String accession, String year) {
    String[] tableTypes = {"metadata", "facts", "contexts", "relationships",
        "mda", "insider", "earnings", "chunks"};
    List<String> paths = new ArrayList<String>(tableTypes.length);
    for (String tableType : tableTypes) {
      paths.add(PARQUET_BASE + "/year=" + year + "/" + cik + "_" + accession + "_" + tableType + ".parquet");
    }
    return paths;
  }

  // -------------------------------------------------------------------------
  // Tracking StorageProvider
  // -------------------------------------------------------------------------

  /**
   * {@link StorageProvider} that:
   * <ul>
   *   <li>Returns a pre-configured set of file paths from {@link #listFiles}.</li>
   *   <li>Returns {@code true} from {@link #exists} only for those same paths.</li>
   *   <li>Counts all {@code listFiles} and {@code exists} calls for assertion.</li>
   *   <li>Records every path passed to {@code exists} for pattern assertions.</li>
   * </ul>
   */
  private static final class TrackingStorageProvider implements StorageProvider {

    private final List<String> knownPaths;
    private final AtomicInteger listFilesCount = new AtomicInteger();
    private final AtomicInteger existsCount = new AtomicInteger();
    private final List<String> existsPathsQueried =
        Collections.synchronizedList(new ArrayList<String>());

    TrackingStorageProvider(List<String> knownPaths) {
      this.knownPaths = new ArrayList<String>(knownPaths);
    }

    int listFilesCallCount() {
      return listFilesCount.get();
    }

    int existsCallCount() {
      return existsCount.get();
    }

    List<String> queriedExistsPaths() {
      return Collections.unmodifiableList(existsPathsQueried);
    }

    void resetCounters() {
      listFilesCount.set(0);
      existsCount.set(0);
      existsPathsQueried.clear();
    }

    @Override
    public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
      listFilesCount.incrementAndGet();
      List<FileEntry> entries = new ArrayList<FileEntry>();
      for (final String knownPath : knownPaths) {
        if (knownPath.startsWith(path)) {
          int lastSlash = knownPath.lastIndexOf('/');
          String name = lastSlash >= 0 ? knownPath.substring(lastSlash + 1) : knownPath;
          entries.add(new FileEntry(knownPath, name, false, 1024L,
              System.currentTimeMillis()));
        }
      }
      return entries;
    }

    @Override
    public boolean exists(String path) throws IOException {
      existsCount.incrementAndGet();
      existsPathsQueried.add(path);
      // Exact match first
      if (knownPaths.contains(path)) {
        return true;
      }
      // glob: year=* — check if any known path matches the non-wildcard part
      if (path.contains("year=*")) {
        String suffix = path.substring(path.indexOf("year=*") + "year=*".length());
        for (String known : knownPaths) {
          if (known.endsWith(suffix)) {
            return true;
          }
        }
      }
      return false;
    }

    @Override
    public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }

    @Override
    public String getStorageType() { return "tracking"; }

    @Override public FileMetadata getMetadata(String path) throws IOException {
      throw new IOException("not implemented");
    }
    @Override public InputStream openInputStream(String path) throws IOException {
      throw new IOException("not implemented");
    }
    @Override public Reader openReader(String path) throws IOException {
      throw new IOException("not implemented");
    }
    @Override public byte[] readRange(String path, long offset, long length) throws IOException {
      throw new IOException("not implemented");
    }
    @Override public boolean isDirectory(String path) { return false; }
    @Override public void writeFile(String path, byte[] content) { }
    @Override public void writeFile(String path, InputStream content) { }
    @Override public void createDirectories(String path) { }
    @Override public boolean delete(String path) { return false; }
    @Override public int deleteBatch(List<String> paths) { return 0; }
    @Override public void ensureLifecycleRule(String prefix, int expirationDays) { }
    @Override public String getStagingDirectory(String purpose) { return "/tmp/staging"; }
    @Override public void copyFile(String source, String destination) { }
    @Override public boolean hasChanged(String path, FileMetadata cachedMetadata) { return false; }
    @Override public void cleanupMacosMetadata(String directoryPath) { }
    @Override public Map<String, String> getS3Config() {
      return Collections.<String, String>emptyMap();
    }
    @Override public void writeAvroParquet(String path,
        org.apache.avro.Schema schema,
        List<org.apache.avro.generic.GenericRecord> records,
        String recordType) { }
  }

  // -------------------------------------------------------------------------
  // In-memory PipelineTracker (same pattern as Worker23NoReprocessTest)
  // -------------------------------------------------------------------------

  private static final class InMemoryPipelineTracker implements PipelineTracker {

    private final Map<String, Set<Map<String, String>>> processed =
        new HashMap<String, Set<Map<String, String>>>();
    private final Map<String, String> tableCompletions =
        new HashMap<String, String>();

    @Override
    public boolean isProcessed(String alternateName, String sourceTable,
        Map<String, String> keyValues) {
      Set<Map<String, String>> entries = processed.get(alternateName);
      return entries != null && entries.contains(keyValues);
    }

    @Override
    public boolean isProcessedWithTtl(String alternateName, String sourceTable,
        Map<String, String> keyValues, long ttlMillis) {
      return isProcessed(alternateName, sourceTable, keyValues);
    }

    @Override
    public void markProcessed(String alternateName, String sourceTable,
        Map<String, String> keyValues, String targetPattern) {
      Set<Map<String, String>> set = processed.get(alternateName);
      if (set == null) {
        set = new HashSet<Map<String, String>>();
        processed.put(alternateName, set);
      }
      set.add(new HashMap<String, String>(keyValues));
    }

    @Override
    public Set<Map<String, String>> getProcessedKeyValues(String alternateName) {
      Set<Map<String, String>> entries = processed.get(alternateName);
      return entries != null ? entries : Collections.<Map<String, String>>emptySet();
    }

    @Override
    public void invalidate(String alternateName, Map<String, String> keyValues) {
      Set<Map<String, String>> entries = processed.get(alternateName);
      if (entries != null) {
        entries.remove(keyValues);
      }
    }

    @Override
    public void invalidateAll(String alternateName) {
      processed.remove(alternateName);
    }

    @Override
    public Set<Integer> filterUnprocessed(String alternateName, String sourceTable,
        List<Map<String, String>> allCombinations) {
      Set<Integer> result = new HashSet<Integer>();
      for (int i = 0; i < allCombinations.size(); i++) {
        if (!isProcessed(alternateName, sourceTable, allCombinations.get(i))) {
          result.add(i);
        }
      }
      return result;
    }

    @Override
    public boolean isTableComplete(String pipelineName, String dimensionSignature) {
      return dimensionSignature.equals(tableCompletions.get(pipelineName));
    }

    @Override
    public void markTableComplete(String pipelineName, String dimensionSignature) {
      tableCompletions.put(pipelineName, dimensionSignature);
    }

    @Override
    public void invalidateTableCompletion(String pipelineName) {
      tableCompletions.remove(pipelineName);
    }

    @Override
    public void clearAllCompletions() {
      processed.clear();
      tableCompletions.clear();
    }

    @Override
    public Set<String> getCompletedTables(String sourceKey, String phase) {
      Set<String> tables = new HashSet<String>();
      String suffix = ":" + phase;
      Map<String, String> sourceKeyMap = Collections.singletonMap("source_key", sourceKey);
      for (Map.Entry<String, Set<Map<String, String>>> entry : processed.entrySet()) {
        String name = entry.getKey();
        if (name.endsWith(suffix) && entry.getValue().contains(sourceKeyMap)) {
          tables.add(name.substring(0, name.length() - suffix.length()));
        }
      }
      return tables;
    }
  }
}