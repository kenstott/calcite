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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link SecFilingCache#preloadFileInventory(int, int)} is scoped to the
 * worker's year range and that — after preloading — all file-existence queries are
 * answered entirely from in-memory maps with zero live S3 calls.
 */
@Tag("unit")
public class SecFilingCacheYearScopedPreloadTest {

  // Production: govdataParquetDir already includes /sec (set by materializeDirectory in
  // sec-schema.yaml). SecFilingCache receives this value directly as parquetBaseDir.
  private static final String PARQUET_BASE = "s3://govdata-parquet-v1/sec";
  private static final String SEC_DIR = PARQUET_BASE;

  // ---------------------------------------------------------------------------
  // 1. Only the requested year partitions are listed — not the sec/ root
  // ---------------------------------------------------------------------------

  @Test
  void preloadFileInventory_callsListFilesOnlyForRequestedYears() throws IOException {
    // Files exist for years 2023, 2024, 2025, 2026
    List<String> allPaths = new ArrayList<String>();
    allPaths.addAll(buildPaths("0000320193", "0000320193-23-000001", "2023"));
    allPaths.addAll(buildPaths("0000320193", "0000320193-24-000001", "2024"));
    allPaths.addAll(buildPaths("0000789019", "0000789019-25-000001", "2025"));
    allPaths.addAll(buildPaths("0000789019", "0000789019-26-000001", "2026"));

    YearTrackingProvider provider = new YearTrackingProvider(allPaths);
    SecFilingCache cache = new SecFilingCache(
        PipelineTracker.NOOP_PIPELINE, provider, PARQUET_BASE);

    cache.preloadFileInventory(2024, 2025);

    List<String> listed = provider.listFilesPathsQueried();
    // preloadFileInventory scans startYear-1 as a fiscal-year buffer (Jan-Apr 10-K/Q filings
    // are attributed to the prior year by getPartitionYear). So requesting 2024-2025 scans
    // years 2023, 2024, 2025. 2 dirs each (primary + legacy migration fallback) = 6 total.
    assertEquals(6, listed.size(), "listFiles must be called twice per year: (startYear-1, startYear, endYear) × (primary + legacy)");
    assertTrue(listed.contains(SEC_DIR + "/year=2023"), "must list year=2023 partition (fiscal-year buffer)");
    assertTrue(listed.contains(SEC_DIR + "/year=2024"), "must list year=2024 partition");
    assertTrue(listed.contains(SEC_DIR + "/year=2025"), "must list year=2025 partition");
    assertFalse(listed.contains(SEC_DIR),
        "must NOT list the sec/ root — that scans millions of files across all years");
    assertFalse(listed.stream().anyMatch(p -> p.contains("year=2026")),
        "must NOT list out-of-range year=2026");
  }

  // ---------------------------------------------------------------------------
  // 2. After preload, zero S3 calls for any checkS3Files variant
  // ---------------------------------------------------------------------------

  @Test
  void afterPreload_zeroS3CallsRegardlessOfFilingDatePresence() throws IOException {
    String cik = "0000320193";
    String accession = "0000320193-26-000001";
    List<String> paths = buildPaths(cik, accession, "2026");

    YearTrackingProvider provider = new YearTrackingProvider(paths);
    SecFilingCache cache = new SecFilingCache(
        PipelineTracker.NOOP_PIPELINE, provider, PARQUET_BASE);
    cache.preloadFileInventory(2026, 2026);
    provider.resetCounters();

    // With known date — uses exact path in s3FileCache (O(1))
    FileInventory withDate = cache.checkS3Files(cik, accession, "2026-03-15");
    assertTrue(withDate.hasMetadata(), "known filing with date must be found in cache");

    // Without date — uses s3FileCacheByName (O(1) byName map)
    FileInventory noDate = cache.checkS3Files(cik, accession, null);
    assertTrue(noDate.hasMetadata(), "known filing without date must be found via byName map");

    // Unknown filing with date — must answer false, still no S3 call
    FileInventory unknownWithDate = cache.checkS3Files(
        "9999999999", "9999999999-26-000001", "2026-01-01");
    assertFalse(unknownWithDate.hasMetadata(), "unknown filing must not be found");

    // Unknown filing without date — must answer false via byName miss, still no S3 call
    FileInventory unknownNoDate = cache.checkS3Files(
        "9999999999", "9999999999-26-000001", null);
    assertFalse(unknownNoDate.hasMetadata(), "unknown filing (no date) must not be found");

    assertEquals(0, provider.listFilesCallCount(),
        "listFiles must not be called after preload is complete");
    assertEquals(0, provider.existsCallCount(),
        "exists() must never be called — all queries answered from in-memory maps");
  }

  // ---------------------------------------------------------------------------
  // 3. Multi-year preload — files from all listed years are resolved correctly
  // ---------------------------------------------------------------------------

  @Test
  void preloadFileInventory_multiYear_findsFilesInBothYearsViaDateAndByName() throws IOException {
    String cik2024 = "0000320193";
    String acc2024 = "0000320193-24-000001";
    String cik2025 = "0000789019";
    String acc2025 = "0000789019-25-000001";

    List<String> allPaths = new ArrayList<String>();
    allPaths.addAll(buildPaths(cik2024, acc2024, "2024"));
    allPaths.addAll(buildPaths(cik2025, acc2025, "2025"));

    YearTrackingProvider provider = new YearTrackingProvider(allPaths);
    SecFilingCache cache = new SecFilingCache(
        PipelineTracker.NOOP_PIPELINE, provider, PARQUET_BASE);
    cache.preloadFileInventory(2024, 2025);
    provider.resetCounters();

    // Year 2024 filing via exact date path
    FileInventory inv2024 = cache.checkS3Files(cik2024, acc2024, "2024-06-01");
    assertTrue(inv2024.hasMetadata(), "2024 filing metadata found via exact year path");
    assertTrue(inv2024.hasFacts(), "2024 filing facts found via exact year path");

    // Year 2025 filing via exact date path
    FileInventory inv2025 = cache.checkS3Files(cik2025, acc2025, "2025-09-30");
    assertTrue(inv2025.hasMetadata(), "2025 filing metadata found via exact year path");

    // Year 2024 filing via byName map (no filing date known)
    FileInventory inv2024NoDate = cache.checkS3Files(cik2024, acc2024, null);
    assertTrue(inv2024NoDate.hasMetadata(), "2024 filing found via byName map (no date)");
    assertTrue(inv2024NoDate.hasChunks(), "2024 filing chunks found via byName map (no date)");

    // Year 2025 filing via byName map
    FileInventory inv2025NoDate = cache.checkS3Files(cik2025, acc2025, null);
    assertTrue(inv2025NoDate.hasMetadata(), "2025 filing found via byName map (no date)");

    assertEquals(0, provider.existsCallCount(),
        "zero exists() calls — all resolved from in-memory maps");
  }

  // ---------------------------------------------------------------------------
  // Helper: build the 8 expected parquet paths for one filing
  // ---------------------------------------------------------------------------

  private static List<String> buildPaths(String cik, String accession, String year) {
    String[] types = {
        "metadata", "facts", "contexts", "relationships",
        "mda", "insider", "earnings", "chunks"
    };
    List<String> paths = new ArrayList<String>(types.length);
    for (String t : types) {
      paths.add(SEC_DIR + "/year=" + year + "/" + cik + "_" + accession + "_" + t + ".parquet");
    }
    return paths;
  }

  // ---------------------------------------------------------------------------
  // StorageProvider that records which paths were passed to listFiles
  // ---------------------------------------------------------------------------

  private static final class YearTrackingProvider implements StorageProvider {

    private final List<String> knownPaths;
    private final CopyOnWriteArrayList<String> listedPaths = new CopyOnWriteArrayList<String>();
    private final AtomicInteger listFilesCount = new AtomicInteger();
    private final AtomicInteger existsCount = new AtomicInteger();

    YearTrackingProvider(List<String> knownPaths) {
      this.knownPaths = new ArrayList<String>(knownPaths);
    }

    List<String> listFilesPathsQueried() {
      return Collections.unmodifiableList(listedPaths);
    }

    int listFilesCallCount() {
      return listFilesCount.get();
    }

    int existsCallCount() {
      return existsCount.get();
    }

    void resetCounters() {
      listedPaths.clear();
      listFilesCount.set(0);
      existsCount.set(0);
    }

    @Override
    public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
      listFilesCount.incrementAndGet();
      listedPaths.add(path);
      List<FileEntry> entries = new ArrayList<FileEntry>();
      for (final String known : knownPaths) {
        if (known.startsWith(path)) {
          int lastSlash = known.lastIndexOf('/');
          String name = lastSlash >= 0 ? known.substring(lastSlash + 1) : known;
          entries.add(new FileEntry(known, name, false, 1024L, System.currentTimeMillis()));
        }
      }
      return entries;
    }

    @Override
    public boolean exists(String path) throws IOException {
      existsCount.incrementAndGet();
      return knownPaths.contains(path);
    }

    @Override
    public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }

    @Override public String getStorageType() { return "year-tracking"; }

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
}
