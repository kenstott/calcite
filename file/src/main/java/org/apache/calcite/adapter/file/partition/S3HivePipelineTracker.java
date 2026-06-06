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
package org.apache.calcite.adapter.file.partition;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * S3 hive-partitioned append-only PipelineTracker.
 *
 * <p>Uses an in-memory DuckDB instance with the {@code httpfs} extension to read/write
 * hive-partitioned parquet files on S3. Since the DuckDB instance is in-memory,
 * there are no file locks, enabling safe concurrent access from multiple workers.
 *
 * <p>State model: append-only parquet files in hive layout:
 * <pre>
 *   s3://bucket/tracker/year={YYYY}/source_key={key}/{uuid}.parquet
 * </pre>
 *
 * <p>Read: {@code GROUP BY (source_key, table_name, phase), take MAX(as_of)} gives
 * the latest state for each combination.
 *
 * <p>Write: Each state change appends a new parquet file. No deletes, no updates.
 */
public class S3HivePipelineTracker implements PipelineTracker, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3HivePipelineTracker.class);

  /** Fixed year partition for _table_complete markers. Avoids year=* wildcard scans. */
  private static final String COMPLETION_YEAR = "0";

  /** Max files per batch when reading tracker files via explicit list.
   *  Tracker files are tiny (~1-5KB), so large batches are fine for data volume.
   *  DuckDB parallelizes reads across cores within each batch. */
  private static final int READ_BATCH_SIZE = 10000;

  private final String bucketPath;
  private final String endpoint;
  private final Map<String, String> config;
  private Connection connection;
  private final Object connectionLock = new Object();
  private boolean initialized;
  /** Lazy-initialized S3 client for ListObjectsV2 file listing. */
  private AmazonS3 s3Client;
  /** Cached result of probing for any tracker data; null = not yet checked. */
  @SuppressWarnings("UnusedVariable")
  private Boolean hasAnyTrackerData;
  /** Processed keys per table, loaded on demand (absent key = not yet loaded for that table). */
  private final ConcurrentHashMap<String, Set<String>> processedKeysCache =
      new ConcurrentHashMap<String, Set<String>>();
  /** In-memory cache of table completions for the duration of this tracker instance. */
  private final Map<String, CachedCompletion> completionCache =
      new ConcurrentHashMap<String, CachedCompletion>();
  /** Tables whose latest completion state is "cleared" (explicitly reset, not just absent). */
  private final Set<String> clearedTables =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  /** True after preloadAllCompletions has run (even if no markers found). */
  private volatile boolean completionsPreloaded;
  /**
   * In-memory cache of per-period completion state keyed by the 5-tuple period key.
   * Value is the latest marker state ("complete"/"invalidate") or "" for known-absent.
   */
  private final Map<String, String> periodCompletionCache =
      new ConcurrentHashMap<String, String>();
  /**
   * In-memory cache of completed tables per (sourceKey, phase).
   * Key format: "sourceKey\0phase" → Set of completed table names.
   * Populated by {@link #bulkGetCompletedTables} and {@link #getCompletedTables},
   * so subsequent {@link #isComplete} calls are O(1) memory lookups.
   */
  private final Map<String, Set<String>> stageCache =
      new ConcurrentHashMap<String, Set<String>>();
  /**
   * Tracks which years have been scanned (prevents retries).
   * Key is just the year string (e.g. "2026", "0").
   * Scans always load ALL phases — no phase-level tracking needed.
   */
  private final Set<String> scannedYears =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  /**
   * Tracks which years were fully scanned with complete data.
   * Only when a year is fully scanned is it safe to cache empty sets for missing
   * source keys (meaning they genuinely have no tracker data).
   */
  private final Set<String> fullyScannedYears =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  /** When true, skip compaction and file deletion during reads (for parallel worker mode). */
  private final boolean noCompact;

  /** Pending state writes awaiting flush to S3. */
  private final List<PendingState> pendingStates = new ArrayList<PendingState>();
  /** Flush threshold for pending states. */
  private static final int PENDING_FLUSH_THRESHOLD = parsePendingFlushThreshold();
  /** Number of parallel S3 PUT threads during flush. Overridable via ETL_TRACKER_FLUSH_PARALLELISM. */
  private static final int FLUSH_PARALLELISM = parseFlushParallelism();
  /** Whether flush-on-shutdown hook has been registered. */
  private volatile boolean shutdownHookRegistered = false;
  /** S3 paths of individual tracker files written during this run, for shutdown compaction. */
  private final List<String> flushedIndividualPaths =
      Collections.synchronizedList(new ArrayList<String>());

  /**
   * Create an S3-backed pipeline tracker.
   *
   * @param bucketPath S3 path for tracker data (e.g. "s3://bucket/tracker")
   * @param endpoint   Optional S3 endpoint override (for MinIO, R2, etc.)
   */
  public S3HivePipelineTracker(String bucketPath, String endpoint) {
    this(bucketPath, endpoint, Collections.<String, String>emptyMap());
  }

  /**
   * Create an S3-backed pipeline tracker with full configuration.
   *
   * @param bucketPath S3 path for tracker data (e.g. "s3://bucket/tracker")
   * @param endpoint   Optional S3 endpoint override (for MinIO, R2, etc.)
   * @param config     Configuration map with accessKeyId, secretAccessKey, region
   */
  public S3HivePipelineTracker(String bucketPath, String endpoint,
      Map<String, String> config) {
    this.bucketPath = bucketPath.endsWith("/") ? bucketPath.substring(0, bucketPath.length() - 1)
        : bucketPath;
    this.endpoint = endpoint;
    this.config = config != null ? config : Collections.<String, String>emptyMap();
    this.noCompact = "true".equals(System.getProperty("calcite.tracker.noCompact"));
    if (this.noCompact) {
      LOGGER.info("No-compact mode: tracker will read but not compact or delete files");
    }
  }

  /**
   * Get or create the AWS S3 client for file listing (ListObjectsV2).
   * Uses the same credentials as DuckDB httpfs.
   */
  private AmazonS3 getS3Client() {
    if (s3Client != null) {
      return s3Client;
    }
    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.setSocketTimeout(60 * 1000);
    clientConfig.setConnectionTimeout(30 * 1000);

    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
        .withClientConfiguration(clientConfig);

    String accessKey = config.get("accessKeyId");
    String secretKey = config.get("secretAccessKey");
    if (accessKey != null && secretKey != null) {
      builder.withCredentials(
          new com.amazonaws.auth.AWSStaticCredentialsProvider(
              new com.amazonaws.auth.BasicAWSCredentials(accessKey, secretKey)));
    }

    String region = config.get("region");
    if (region == null) {
      region = "us-east-1";
    }

    if (endpoint != null && !endpoint.isEmpty()) {
      String cleanEndpoint = endpoint;
      builder.withEndpointConfiguration(
          new EndpointConfiguration(cleanEndpoint, region));
      builder.withPathStyleAccessEnabled(true);
    } else {
      builder.withRegion(region);
    }

    s3Client = builder.build();
    return s3Client;
  }

  /**
   * List tracker parquet files under a prefix using paginated ListObjectsV2.
   * Excludes files in {@code _compacted/} directories.
   *
   * @param prefix S3 key prefix (e.g. "year=2026/source_key=")
   * @return list of full S3 URIs for matching parquet files
   */
  private List<String> listTrackerFiles(String prefix) {
    // Parse bucket and key from bucketPath (e.g. "s3://bucket/tracker")
    String path = bucketPath.startsWith("s3://") ? bucketPath.substring(5) : bucketPath;
    int slash = path.indexOf('/');
    String bucket = slash > 0 ? path.substring(0, slash) : path;
    String keyPrefix = slash > 0 ? path.substring(slash + 1) + "/" + prefix : prefix;

    List<String> files = new ArrayList<String>();
    ListObjectsV2Request request = new ListObjectsV2Request()
        .withBucketName(bucket)
        .withPrefix(keyPrefix);

    int pages = 0;
    ListObjectsV2Result result;
    do {
      result = getS3Client().listObjectsV2(request);
      pages++;

      for (S3ObjectSummary summary : result.getObjectSummaries()) {
        String key = summary.getKey();
        if (key.endsWith(".parquet") && !key.contains("_compacted/")) {
          files.add("s3://" + bucket + "/" + key);
        }
      }

      if (pages % 50 == 0) {
        LOGGER.info("Listed {} files so far ({} pages)...", files.size(), pages);
      }

      request.setContinuationToken(result.getNextContinuationToken());
    } while (result.isTruncated());

    LOGGER.info("Listed {} tracker files under {} ({} pages)",
        files.size(), prefix, pages);
    return files;
  }

  private Connection getConnection() throws SQLException {
    synchronized (connectionLock) {
      if (connection == null || connection.isClosed()) {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        LOGGER.debug("Opened in-memory DuckDB connection for S3 tracker");
      }
      if (!initialized) {
        initializeExtensions();
        initialized = true;
      }
      return connection;
    }
  }

  private void initializeExtensions() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSTALL httpfs");
      stmt.execute("LOAD httpfs");
      stmt.execute("SET memory_limit='1GB'");

      String accessKey = config.get("accessKeyId");
      String secretKey = config.get("secretAccessKey");
      if (accessKey == null || accessKey.isEmpty()
          || secretKey == null || secretKey.isEmpty()) {
        LOGGER.warn("S3 tracker missing accessKeyId/secretAccessKey in config. "
            + "Provide credentials via model.json operand. Available keys: {}",
            config.keySet());
        return;
      }

      // Default region to 'auto' when a custom endpoint is configured (e.g. R2).
      // Without this, DuckDB httpfs defaults to 'us-east-1', causing HTTP 400 on R2 reads.
      String region = config.get("region");
      if ((region == null || region.isEmpty()) && endpoint != null && !endpoint.isEmpty()) {
        region = "auto";
      }

      String sessionToken = config.get("sessionToken");

      StringBuilder sql =
          new StringBuilder("CREATE OR REPLACE SECRET s3_tracker_secret (TYPE S3");
      sql.append(", KEY_ID '").append(accessKey).append("'");
      sql.append(", SECRET '").append(secretKey).append("'");
      if (region != null && !region.isEmpty()) {
        sql.append(", REGION '").append(region).append("'");
      }
      if (sessionToken != null && !sessionToken.isEmpty()) {
        sql.append(", SESSION_TOKEN '").append(sessionToken).append("'");
      }
      if (endpoint != null && !endpoint.isEmpty()) {
        String host = endpoint.replaceAll("https?://", "");
        sql.append(", ENDPOINT '").append(host).append("'");
        sql.append(", URL_STYLE 'path'");
        if (endpoint.startsWith("http://")) {
          sql.append(", USE_SSL false");
        }
      }
      sql.append(")");
      stmt.execute(sql.toString());

      LOGGER.info("Initialized S3 httpfs extension for tracker at {} (endpoint={}, region={})",
          bucketPath, endpoint, region);
    }
  }

  /**
   * Bulk-retrieve completed tables for multiple source keys.
   *
   * <p>Groups source keys by their extracted year and performs a full year-level
   * scan for each year. This avoids the DuckDB limitation where
   * {@code read_parquet([glob1, glob2, ...])} fails with "No files found" if
   * even one glob pattern matches no files — which silently discards valid data
   * for all other patterns in the list.
   *
   * <p>Year-level scanning reads all tracker files for a year partition in one
   * query, caches every source key found, then compacts the files into a single
   * parquet file so future startups read one file instead of thousands.
   */
  @Override public Map<String, Set<String>> bulkGetCompletedTables(
      java.util.Collection<String> sourceKeys, String phase) {
    if (sourceKeys.isEmpty()) {
      return new HashMap<String, Set<String>>();
    }

    // Return cached results for source keys already in stageCache, only query uncached ones
    Map<String, Set<String>> cachedResults = new HashMap<String, Set<String>>();
    List<String> uncachedKeys = new ArrayList<String>();
    for (String sk : sourceKeys) {
      String cacheKey = sk + "\0" + phase;
      Set<String> cached = stageCache.get(cacheKey);
      if (cached != null) {
        if (!cached.isEmpty()) {
          cachedResults.put(sk, cached);
        }
      } else {
        uncachedKeys.add(sk);
      }
    }
    if (uncachedKeys.isEmpty()) {
      return cachedResults;
    }

    long bulkStart = System.currentTimeMillis();
    LOGGER.info("Bulk loading completed tables for {} source keys (phase={}, {} already cached)",
        uncachedKeys.size(), phase, cachedResults.size());

    // Group uncached keys by extracted year for year-level scanning
    Map<String, List<String>> keysByYear = new HashMap<String, List<String>>();
    for (String sk : uncachedKeys) {
      String year = "_table_complete".equals(sk)
          ? COMPLETION_YEAR : extractYear(sk, System.currentTimeMillis());
      List<String> list = keysByYear.get(year);
      if (list == null) {
        list = new ArrayList<String>();
        keysByYear.put(year, list);
      }
      list.add(sk);
    }

    // Scan each year's tracker data (once per year, ALL phases, cached for subsequent calls)
    Map<String, Set<String>> result = new HashMap<String, Set<String>>();
    for (Map.Entry<String, List<String>> yearEntry : keysByYear.entrySet()) {
      String year = yearEntry.getKey();
      List<String> yearKeys = yearEntry.getValue();

      if (!scannedYears.contains(year)) {
        List<String> scannedFiles = scanAndCacheYear(year);
        scannedYears.add(year);
        if (scannedFiles != null) {
          fullyScannedYears.add(year);
          if (!noCompact && !scannedFiles.isEmpty()) {
            deleteSpecificFiles(scannedFiles, year);
          }
        } else {
          LOGGER.info("Year {} scan incomplete — individual queries will occur on demand", year);
        }
      }

      // Collect results from cache for the requested keys
      for (String sk : yearKeys) {
        String cacheKey = sk + "\0" + phase;
        Set<String> cached = stageCache.get(cacheKey);
        if (cached != null && !cached.isEmpty()) {
          result.put(sk, cached);
        }
      }
    }

    // Cache empty sets for uncached keys that weren't found in the scan.
    // Only safe when the year was fully scanned (compacted or full scan succeeded).
    for (String sk : uncachedKeys) {
      String cacheKey = sk + "\0" + phase;
      if (!stageCache.containsKey(cacheKey)) {
        String year = "_table_complete".equals(sk)
            ? COMPLETION_YEAR : extractYear(sk, System.currentTimeMillis());
        // Only cache empty if year was fully scanned (not just attempted)
        if (fullyScannedYears.contains(year)) {
          stageCache.put(cacheKey, new LinkedHashSet<String>());
        }
      }
    }

    long bulkElapsed = System.currentTimeMillis() - bulkStart;
    LOGGER.info("Bulk loaded completed tables: {} keys queried, {} with data, {}ms",
        uncachedKeys.size(), result.size(), bulkElapsed);
    cachedResults.putAll(result);
    return cachedResults;
  }

  /**
   * Scan and compact tracker data for a range of years.
   *
   * <p>For each year in the range, reads all tracker files (or the existing
   * compacted file), caches the data, and writes a compacted file. This is
   * intended for standalone compaction runs ({@code --compact-only}) to prepare
   * tracker data for fast reads on subsequent ETL runs.
   *
   * <p>Also compacts year=0 (table completion markers).
   */
  public void compactYearRange(int startYear, int endYear) {
    long start = System.currentTimeMillis();
    LOGGER.info("Compacting tracker years {}-{} (plus completion markers)...",
        startYear, endYear);

    // Delete existing compacted files so the slow path runs and captures
    // any new tracker entries written since the last compaction.
    deleteCompactedFiles(COMPLETION_YEAR);
    for (int year = startYear; year <= endYear; year++) {
      deleteCompactedFiles(String.valueOf(year));
    }

    // Compact table completion markers (year=0)
    // scanAndCacheYear returns the list of individual files it read.
    // After compaction succeeds, we delete exactly those files (not a fresh listing).
    List<String> scannedFiles = scanAndCacheYear(COMPLETION_YEAR);
    if (scannedFiles != null && !scannedFiles.isEmpty()) {
      deleteSpecificFiles(scannedFiles, COMPLETION_YEAR);
    }

    // Compact each data year (all phases in one pass)
    for (int year = startYear; year <= endYear; year++) {
      scannedFiles = scanAndCacheYear(String.valueOf(year));
      if (scannedFiles != null && !scannedFiles.isEmpty()) {
        deleteSpecificFiles(scannedFiles, String.valueOf(year));
      }
    }

    long elapsed = System.currentTimeMillis() - start;
    LOGGER.info("Tracker compaction complete: years {}-{} in {}ms",
        startYear, endYear, elapsed);
  }

  /**
   * Scan tracker data for a year partition and cache results.
   *
   * <p>Two-phase approach:
   * <ol>
   * <li><b>Fast path</b>: Read {@code _compacted/*.parquet} (single file, instant).
   *     Available after a previous run compacted the tracker data.</li>
   * <li><b>Slow path</b>: Read source_key=*&#47;*.parquet (all individual files).
   *     Only needed on the first run for each year. After scanning, writes a
   *     compacted file from the in-memory cache so future runs use the fast path.</li>
   * </ol>
   *
   * @return list of individual S3 files that were scanned (empty if fast-path hit),
   *         or null if the scan failed
   */
  private List<String> scanAndCacheYear(String year) {
    long start = System.currentTimeMillis();

    // Fast path: try compacted file first (O(1) file read, all phases)
    String compactedGlob = bucketPath + "/year=" + year + "/_compacted/*.parquet";
    LOGGER.info("Scanning tracker year={} — checking for compacted file...", year);
    int[] counts = readTrackerGlobAllPhases(compactedGlob);
    if (counts != null) {
      long elapsed = System.currentTimeMillis() - start;
      LOGGER.info("Scanned tracker year={} from compacted file: "
          + "{} source keys, {} completed tables, {}ms",
          year, counts[0], counts[1], elapsed);

      // Merge any individual files written after compaction by other workers.
      // Without this, concurrent workers' tracker writes would be invisible.
      // Process in batches to avoid OOM when hundreds of thousands of files exist.
      String prefix = "year=" + year + "/source_key=";
      List<String> stragglers = listTrackerFiles(prefix);
      if (!stragglers.isEmpty()) {
        LOGGER.info("Found {} individual tracker files alongside compacted file for year={}, "
            + "merging in batches of {}", stragglers.size(), year, READ_BATCH_SIZE);
        int totalExtra = 0;
        for (int i = 0; i < stragglers.size(); i += READ_BATCH_SIZE) {
          int end = Math.min(i + READ_BATCH_SIZE, stragglers.size());
          List<String> batch = stragglers.subList(i, end);
          java.io.File tempDir = null;
          try {
            tempDir = downloadTrackerFilesParallel(batch, year);
            String localGlob = tempDir.getAbsolutePath() + "/*.parquet";
            int[] extra = readTrackerGlobAllPhases(localGlob);
            if (extra != null) {
              totalExtra += extra[1];
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to merge straggler batch {}/{} for year={}: {}",
                (i / READ_BATCH_SIZE) + 1,
                (stragglers.size() + READ_BATCH_SIZE - 1) / READ_BATCH_SIZE,
                year, e.getMessage());
          } finally {
            if (tempDir != null) {
              deleteDir(tempDir);
            }
          }
        }
        if (totalExtra > 0) {
          LOGGER.info("Merged {} extra tables from individual files for year={}", totalExtra, year);
        }
      }

      return stragglers; // return individual files so caller can delete them
    }

    // Slow path: list files via S3 API (paginated), then batch-read with DuckDB.
    // This avoids DuckDB's glob expansion which tries to list+open all files at once.
    LOGGER.info("Scanning full tracker year={} — listing files via S3 API...", year);
    String prefix = "year=" + year + "/source_key=";
    List<String> files = listTrackerFiles(prefix);

    if (files.isEmpty()) {
      long elapsed = System.currentTimeMillis() - start;
      LOGGER.info("No tracker data found for year={} ({}ms)", year, elapsed);
      return Collections.emptyList();
    }

    // Download files in batches to a local temp dir, then read from disk.
    // Batching avoids OOM when hundreds of thousands of files exist.
    int totalSourceKeys = 0;
    int totalTables = 0;
    try {
      for (int i = 0; i < files.size(); i += READ_BATCH_SIZE) {
        int end = Math.min(i + READ_BATCH_SIZE, files.size());
        List<String> batch = files.subList(i, end);
        java.io.File tempDir = null;
        try {
          tempDir = downloadTrackerFilesParallel(batch, year);
          String localGlob = tempDir.getAbsolutePath() + "/*.parquet";
          int[] batchCounts = readTrackerGlobAllPhases(localGlob);
          if (batchCounts != null) {
            totalSourceKeys += batchCounts[0];
            totalTables += batchCounts[1];
          }
        } finally {
          if (tempDir != null) {
            deleteDir(tempDir);
          }
        }
      }

      long elapsed = System.currentTimeMillis() - start;
      LOGGER.info("Scanned tracker year={}: {} source keys, {} completed tables, {}ms "
          + "({} files downloaded+read)", year, totalSourceKeys, totalTables, elapsed,
          files.size());
      if (!noCompact) {
        compactFromCache(year);
      }
      return files;
    } catch (Exception e) {
      LOGGER.warn("Batched download failed for year={}, falling back to direct S3 reads: {}",
          year, e.getMessage());
      if (readBatchedFromS3(files, year, start)) {
        return files;
      }
      return null;
    }
  }

  /**
   * Download tracker files from S3 in parallel to a local temp directory.
   *
   * <p>Uses a thread pool to download many small files concurrently, avoiding
   * the per-file TLS overhead that makes sequential reads slow on R2/S3.
   *
   * @param s3Files list of full S3 URIs
   * @param year    year label for logging
   * @return temp directory containing the downloaded parquet files
   */
  private java.io.File downloadTrackerFilesParallel(List<String> s3Files, String year)
      throws Exception {
    java.io.File tempDir = java.io.File.createTempFile("tracker-" + year + "-", "");
    tempDir.delete();
    tempDir.mkdirs();

    AmazonS3 client = getS3Client();
    int threads = Math.min(50, s3Files.size());
    java.util.concurrent.ExecutorService pool =
        java.util.concurrent.Executors.newFixedThreadPool(threads);
    java.util.concurrent.atomic.AtomicInteger completed =
        new java.util.concurrent.atomic.AtomicInteger(0);
    java.util.concurrent.atomic.AtomicInteger errors =
        new java.util.concurrent.atomic.AtomicInteger(0);
    int total = s3Files.size();
    long downloadStart = System.currentTimeMillis();

    LOGGER.info("Downloading {} tracker files to local temp dir ({} threads)...",
        total, threads);

    List<java.util.concurrent.Future<?>> futures =
        new ArrayList<java.util.concurrent.Future<?>>();
    for (int i = 0; i < s3Files.size(); i++) {
      final String s3Uri = s3Files.get(i);
      final int idx = i;
      final java.io.File localFile = new java.io.File(tempDir, idx + ".parquet");
      futures.add(
          pool.submit(new Runnable() {
        @Override public void run() {
          try {
            // Parse s3://bucket/key
            String path = s3Uri.substring(5); // remove "s3://"
            int slash = path.indexOf('/');
            String bucket = path.substring(0, slash);
            String key = path.substring(slash + 1);

            com.amazonaws.services.s3.model.S3Object obj =
                client.getObject(bucket, key);
            try {
              java.io.InputStream in = obj.getObjectContent();
              try {
                java.io.FileOutputStream out = new java.io.FileOutputStream(localFile);
                try {
                  byte[] buf = new byte[8192];
                  int n;
                  while ((n = in.read(buf)) > 0) {
                    out.write(buf, 0, n);
                  }
                } finally {
                  out.close();
                }
              } finally {
                in.close();
              }
            } finally {
              obj.close();
            }

            int done = completed.incrementAndGet();
            if (done % 10000 == 0 || done == total) {
              LOGGER.info("Downloaded {}/{} tracker files ({} errors)...",
                  done, total, errors.get());
            }
          } catch (Exception e) {
            errors.incrementAndGet();
            if (errors.get() <= 3) {
              LOGGER.warn("Failed to download {}: {}", s3Uri, e.getMessage());
            }
          }
        }
      }));
    }

    // Wait for all downloads
    for (java.util.concurrent.Future<?> f : futures) {
      f.get();
    }
    pool.shutdown();

    long downloadElapsed = System.currentTimeMillis() - downloadStart;
    LOGGER.info("Downloaded {} tracker files in {}ms ({} errors, {} threads)",
        completed.get(), downloadElapsed, errors.get(), threads);

    // Remove corrupt/empty files — DuckDB read_parquet fails the entire glob if any file
    // is below the 8-byte PAR1 magic minimum (happens after JVM-killed mid-upload).
    int corrupt = 0;
    java.io.File[] downloaded = tempDir.listFiles();
    if (downloaded != null) {
      for (java.io.File f : downloaded) {
        if (f.length() < 8) {
          f.delete();
          corrupt++;
        }
      }
    }
    if (corrupt > 0) {
      LOGGER.warn("Removed {} corrupt/empty tracker files from tempDir (size < 8 bytes)",
          corrupt);
    }

    if (errors.get() > total / 2) {
      throw new RuntimeException("Too many download errors: " + errors.get() + "/" + total);
    }
    return tempDir;
  }

  /** Fallback: batched S3 reads (all phases) when parallel download fails. */
  private boolean readBatchedFromS3(List<String> files, String year, long start) {
    int totalSourceKeys = 0;
    int totalTables = 0;
    for (int i = 0; i < files.size(); i += READ_BATCH_SIZE) {
      int end = Math.min(i + READ_BATCH_SIZE, files.size());
      List<String> batch = files.subList(i, end);

      StringBuilder fileList = new StringBuilder();
      for (int j = 0; j < batch.size(); j++) {
        if (j > 0) {
          fileList.append(", ");
        }
        fileList.append("'").append(batch.get(j)).append("'");
      }

      int[] batchCounts = readTrackerGlobAllPhases("[" + fileList.toString() + "]");
      if (batchCounts != null) {
        totalSourceKeys += batchCounts[0];
        totalTables += batchCounts[1];
      }

      LOGGER.info("Scanned tracker year={} batch {}/{}: {} files, {} source keys so far",
          year, (i / READ_BATCH_SIZE) + 1,
          (files.size() + READ_BATCH_SIZE - 1) / READ_BATCH_SIZE,
          batch.size(), totalSourceKeys);
    }

    long elapsed = System.currentTimeMillis() - start;
    LOGGER.info("Scanned tracker year={}: {} source keys, {} completed tables, {}ms ({} files)",
        year, totalSourceKeys, totalTables, elapsed, files.size());
    if (!noCompact) {
      compactFromCache(year);
    }
    return true;
  }

  /** Recursively delete a directory. */
  private static void deleteDir(java.io.File dir) {
    java.io.File[] files = dir.listFiles();
    if (files != null) {
      for (java.io.File f : files) {
        if (f.isDirectory()) {
          deleteDir(f);
        } else {
          f.delete();
        }
      }
    }
    dir.delete();
  }

  /**
   * Read tracker data from ALL phases and populate the stageCache.
   * Unlike {@code readTrackerGlob}, this does not filter by phase,
   * so the cache (and subsequent compacted file) contains complete data.
   *
   * @param globOrList glob pattern or explicit file list
   * @return int[]{sourceKeyCount, tableCount} on success, null on "no files found"
   */
  private int[] readTrackerGlobAllPhases(String globOrList) {
    String parquetArg = globOrList.startsWith("[")
        ? globOrList
        : "'" + globOrList + "'";
    String sql = "SELECT source_key, table_name, phase FROM ("
        + "  SELECT source_key, table_name, phase, state, "
        + "    ROW_NUMBER() OVER (PARTITION BY source_key, table_name, phase"
        + "      ORDER BY as_of DESC) AS rn"
        + "  FROM read_parquet(" + parquetArg + ", "
        + "hive_partitioning=false, union_by_name=true)"
        + ") WHERE rn = 1 AND state = 'complete'";

    int sourceKeyCount = 0;
    int tableCount = 0;
    try (Statement stmt = getConnection().createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        String sourceKey = rs.getString("source_key");
        String tableName = rs.getString("table_name");
        String phase = rs.getString("phase");
        String cacheKey = sourceKey + "\0" + phase;
        Set<String> tables = stageCache.get(cacheKey);
        if (tables == null) {
          tables = new LinkedHashSet<String>();
          stageCache.put(cacheKey, tables);
          sourceKeyCount++;
        }
        tables.add(tableName);
        tableCount++;
      }
    } catch (SQLException e) {
      String msg = e.getMessage();
      if (msg != null && (msg.contains("No files found")
          || msg.contains("Could not find")
          || msg.contains("HTTP 404"))) {
        return null;
      }
      LOGGER.warn("Failed to read tracker from {}: {}", globOrList, msg);
      return null;
    }
    return new int[]{sourceKeyCount, tableCount};
  }

  /**
   * Write a compacted tracker file from the in-memory stageCache.
   *
   * <p>Collects all cached (source_key, table_name) pairs for the given year,
   * writes them to a single parquet file in {@code _compacted/}. This avoids
   * re-reading thousands of small files from S3 — the data is already in memory.
   *
   * <p>Future {@link #scanAndCacheYear} calls read this single file instead of
   * scanning all individual tracker files.
   */
  private void compactFromCache(String year) {
    long start = System.currentTimeMillis();
    long asOf = System.currentTimeMillis();
    String compactedPath = bucketPath + "/year=" + year
        + "/_compacted/" + UUID.randomUUID().toString() + ".parquet";

    // Write directly from stageCache to DuckDB temp table, avoiding
    // an intermediate ArrayList that duplicates all the data in memory.
    String tableName = "_compact_" + year.replace("-", "_");
    int rowCount = 0;
    try {
      Connection conn = getConnection();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS " + tableName);
        stmt.execute("CREATE TABLE " + tableName + " ("
            + "source_key VARCHAR, table_name VARCHAR, phase VARCHAR, "
            + "state VARCHAR, row_count BIGINT, config_hash VARCHAR, "
            + "signature VARCHAR, error_message VARCHAR, as_of BIGINT)");
      }

      try (PreparedStatement ps =
          conn.prepareStatement("INSERT INTO " + tableName
              + " VALUES (?, ?, ?, 'complete', 0, NULL, NULL, NULL, ?)")) {
        int batchSize = 0;
        for (Map.Entry<String, Set<String>> entry : stageCache.entrySet()) {
          String key = entry.getKey();
          int sep = key.indexOf('\0');
          if (sep < 0) {
            continue;
          }
          String sourceKey = key.substring(0, sep);
          String phase = key.substring(sep + 1);
          String skYear = "_table_complete".equals(sourceKey)
              ? COMPLETION_YEAR : extractYear(sourceKey, asOf);
          if (!year.equals(skYear)) {
            continue;
          }
          for (String tbl : entry.getValue()) {
            ps.setString(1, sourceKey);
            ps.setString(2, tbl);
            ps.setString(3, phase);
            ps.setLong(4, asOf);
            ps.addBatch();
            batchSize++;
            rowCount++;
            if (batchSize >= 10000) {
              ps.executeBatch();
              batchSize = 0;
            }
          }
        }
        if (batchSize > 0) {
          ps.executeBatch();
        }
      }

      if (rowCount == 0) {
        LOGGER.info("No cached tracker data to compact for year={}", year);
        try (Statement stmt = conn.createStatement()) {
          stmt.execute("DROP TABLE IF EXISTS " + tableName);
        }
        return;
      }

      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("COPY " + tableName
            + " TO '" + compactedPath + "' (FORMAT PARQUET)");
        stmt.execute("DROP TABLE " + tableName);
      }

      long elapsed = System.currentTimeMillis() - start;
      LOGGER.info("Compacted tracker year={}: {} rows written to S3 in {}ms",
          year, rowCount, elapsed);
    } catch (SQLException e) {
      LOGGER.warn("Failed to compact tracker year={}: {}", year, e.getMessage());
      try (Statement stmt = getConnection().createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS " + tableName);
      } catch (SQLException e2) {
        // ignore cleanup failure
      }
    }
  }

  /**
   * Delete existing compacted files for a year partition.
   * Called before re-compaction to ensure the slow path runs and captures
   * any new tracker entries written since the last compaction.
   */
  private void deleteCompactedFiles(String year) {
    String prefix = "year=" + year + "/_compacted/";
    try {
      List<String> files = listTrackerFilesIncludeCompacted(prefix);
      if (files.isEmpty()) {
        return;
      }
      AmazonS3 client = getS3Client();
      String path = bucketPath.startsWith("s3://") ? bucketPath.substring(5) : bucketPath;
      int slash = path.indexOf('/');
      String bucket = slash > 0 ? path.substring(0, slash) : path;

      List<DeleteObjectsRequest.KeyVersion> batch =
          new ArrayList<DeleteObjectsRequest.KeyVersion>();
      for (String file : files) {
        String filePath = file.startsWith("s3://") ? file.substring(5) : file;
        int fileSlash = filePath.indexOf('/');
        String key = filePath.substring(fileSlash + 1);
        batch.add(new DeleteObjectsRequest.KeyVersion(key));
        if (batch.size() >= 1000) {
          client.deleteObjects(
              new DeleteObjectsRequest(bucket).withKeys(batch).withQuiet(true));
          batch.clear();
        }
      }
      if (!batch.isEmpty()) {
        client.deleteObjects(
            new DeleteObjectsRequest(bucket).withKeys(batch).withQuiet(true));
      }
      LOGGER.info("Deleted {} compacted files for year={}", files.size(), year);
    } catch (Exception e) {
      LOGGER.warn("Failed to delete compacted files for year={}: {}", year, e.getMessage());
    }
  }

  /**
   * Delete specific S3 files that were previously listed and compacted.
   * Only deletes the exact files passed in — never does a fresh listing.
   * This ensures we never delete files written after the scan started.
   */
  private void deleteSpecificFiles(List<String> s3Files, String year) {
    try {
      AmazonS3 client = getS3Client();
      String path = bucketPath.startsWith("s3://") ? bucketPath.substring(5) : bucketPath;
      int slash = path.indexOf('/');
      String bucket = slash > 0 ? path.substring(0, slash) : path;

      // Batch delete: up to 1000 keys per S3 DeleteObjects request
      List<DeleteObjectsRequest.KeyVersion> batch =
          new ArrayList<DeleteObjectsRequest.KeyVersion>();
      int deleted = 0;
      for (String file : s3Files) {
        String filePath = file.startsWith("s3://") ? file.substring(5) : file;
        int fileSlash = filePath.indexOf('/');
        String key = filePath.substring(fileSlash + 1);
        batch.add(new DeleteObjectsRequest.KeyVersion(key));
        if (batch.size() >= 1000) {
          client.deleteObjects(
              new DeleteObjectsRequest(bucket).withKeys(batch).withQuiet(true));
          deleted += batch.size();
          batch.clear();
          if (deleted % 10000 == 0) {
            LOGGER.info("Deleted {}/{} compacted tracker files for year={}...",
                deleted, s3Files.size(), year);
          }
        }
      }
      if (!batch.isEmpty()) {
        client.deleteObjects(
            new DeleteObjectsRequest(bucket).withKeys(batch).withQuiet(true));
        deleted += batch.size();
      }
      LOGGER.info("Deleted {} individual tracker files for year={} (compacted)",
          deleted, year);
    } catch (Exception e) {
      LOGGER.warn("Failed to delete tracker files for year={}: {}",
          year, e.getMessage());
    }
  }

  private void compactOnClose() {
    if (noCompact || fullyScannedYears.isEmpty()) {
      return;
    }
    for (String year : new HashSet<String>(fullyScannedYears)) {
      try {
        deleteCompactedFiles(year);
        compactFromCache(year);
        List<String> toDelete = new ArrayList<String>();
        for (String path : flushedIndividualPaths) {
          if (path.contains("/year=" + year + "/")) {
            toDelete.add(path);
          }
        }
        if (!toDelete.isEmpty()) {
          deleteSpecificFiles(toDelete, year);
        }
        LOGGER.info("Shutdown compaction complete for year={}: {} individual files removed",
            year, toDelete.size());
      } catch (Exception e) {
        LOGGER.warn("Shutdown compaction failed for year={}: {}", year, e.getMessage());
      }
    }
    flushedIndividualPaths.clear();
  }

  /**
   * List files under a prefix, including files in _compacted/ directories.
   */
  private List<String> listTrackerFilesIncludeCompacted(String prefix) {
    String path = bucketPath.startsWith("s3://") ? bucketPath.substring(5) : bucketPath;
    int slash = path.indexOf('/');
    String bucket = slash > 0 ? path.substring(0, slash) : path;
    String keyPrefix = slash > 0 ? path.substring(slash + 1) + "/" + prefix : prefix;

    List<String> files = new ArrayList<String>();
    ListObjectsV2Request request = new ListObjectsV2Request()
        .withBucketName(bucket)
        .withPrefix(keyPrefix);

    ListObjectsV2Result result;
    do {
      result = getS3Client().listObjectsV2(request);
      for (S3ObjectSummary summary : result.getObjectSummaries()) {
        String key = summary.getKey();
        if (key.endsWith(".parquet")) {
          files.add("s3://" + bucket + "/" + key);
        }
      }
      request.setContinuationToken(result.getNextContinuationToken());
    } while (result.isTruncated());

    return files;
  }

  /**
   * Buffer a state row for later batch-flush to S3.
   *
   * <p>Instead of writing one parquet file per state change (which causes
   * millions of S3 PUT calls for large ETL jobs), state changes are buffered
   * in memory and flushed when the buffer reaches {@link #PENDING_FLUSH_THRESHOLD},
   * when {@link #close()} is called, or when {@link #flushPendingStates()} is
   * called explicitly.
   *
   * <p>The in-memory {@link #stageCache} is still updated immediately by callers
   * (e.g. {@link #markComplete}), so reads remain correct without flushing.
   */
  private void writeState(String sourceKey, String tableName, String phase,
      String state, long rowCount, String configHash, String signature,
      String errorMessage) {
    long asOf = System.currentTimeMillis();
    ensureShutdownHook();

    boolean shouldFlush = false;
    synchronized (connectionLock) {
      pendingStates.add(
          new PendingState(sourceKey, tableName, phase,
          state, rowCount, configHash, signature, errorMessage, asOf));
      if (pendingStates.size() >= PENDING_FLUSH_THRESHOLD) {
        shouldFlush = true;
      }
    }
    if (shouldFlush) {
      flushPendingStates();
    }
  }

  /**
   * Read the latest state for a (source_key, table_name, phase) combination.
   */
  private String readLatestState(String sourceKey, String tableName, String phase) {
    String year = completionYearFor(sourceKey, System.currentTimeMillis());
    String glob = bucketPath + "/year=" + year + "/source_key=" + sanitizeHiveValue(sourceKey)
        + "/*.parquet";
    String sql = "SELECT state FROM read_parquet('" + glob + "', "
        + "hive_partitioning=true, union_by_name=true) "
        + "WHERE source_key = ? AND table_name = ? AND phase = ? "
        + "ORDER BY as_of DESC LIMIT 1";

    synchronized (connectionLock) {
      try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
        stmt.setString(1, sourceKey);
        stmt.setString(2, tableName);
        stmt.setString(3, phase);
        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            return rs.getString("state");
          }
        }
      } catch (SQLException e) {
        // Glob may match no files, treat as not found
        LOGGER.debug("No tracker state found for {}/{}/{}: {}",
            sourceKey, tableName, phase, e.getMessage());
      }
    }
    return null;
  }

  // ===== PipelineTracker Implementation =====

  @Override public boolean isComplete(String sourceKey, String tableName, String phase) {
    // Check stage cache first (populated by bulkGetCompletedTables / getCompletedTables)
    String cacheKey = sourceKey + "\0" + phase;
    Set<String> cached = stageCache.get(cacheKey);
    if (cached != null) {
      return cached.contains(tableName);
    }
    // Cache miss — use getCompletedTables to populate full set (avoids separate S3 query)
    Set<String> tables = getCompletedTables(sourceKey, phase);
    return tables.contains(tableName);
  }

  @Override public void markComplete(String sourceKey, String tableName, String phase,
      long rowCount) {
    writeState(sourceKey, tableName, phase, "complete", rowCount, null, null, null);
    // Update stage cache
    String cacheKey = sourceKey + "\0" + phase;
    Set<String> tables = stageCache.get(cacheKey);
    if (tables != null) {
      tables.add(tableName);
    } else {
      Set<String> newSet =
          Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
      newSet.add(tableName);
      stageCache.put(cacheKey, newSet);
    }
  }

  @Override public void markError(String sourceKey, String tableName, String phase,
      String error) {
    writeState(sourceKey, tableName, phase, "error", 0, null, null, error);
  }

  @Override public void markCleared(String sourceKey, String tableName, String phase) {
    writeState(sourceKey, tableName, phase, "cleared", 0, null, null, null);
    // Update stage cache — remove the cleared table
    String cacheKey = sourceKey + "\0" + phase;
    Set<String> tables = stageCache.get(cacheKey);
    if (tables != null) {
      tables.remove(tableName);
    }
  }

  @Override public Set<String> getCompletedTables(String sourceKey, String phase) {
    // Check stage cache first
    String cacheKey = sourceKey + "\0" + phase;
    Set<String> cached = stageCache.get(cacheKey);
    if (cached != null) {
      return cached;
    }

    Set<String> tables = new LinkedHashSet<String>();
    String year = extractYear(sourceKey, System.currentTimeMillis());
    String prefix = "year=" + year + "/source_key=" + sanitizeHiveValue(sourceKey) + "/";
    List<String> files;
    try {
      files = listTrackerFiles(prefix);
    } catch (Exception e) {
      LOGGER.warn("Error listing tracker files for {}/{}: {}", sourceKey, phase, e.getMessage());
      stageCache.put(cacheKey, tables);
      return tables;
    }

    if (!files.isEmpty()) {
      java.io.File tempDir = null;
      try {
        tempDir = downloadTrackerFilesParallel(files, year);
        String localGlob = tempDir.getAbsolutePath() + "/*.parquet";
        String sql = "SELECT table_name FROM ("
            + "  SELECT table_name, state, ROW_NUMBER() OVER "
            + "    (PARTITION BY table_name ORDER BY as_of DESC) AS rn "
            + "  FROM read_parquet('" + localGlob + "', union_by_name=true) "
            + "  WHERE phase = ?"
            + ") WHERE rn = 1 AND state = 'complete'";
        synchronized (connectionLock) {
          try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
            stmt.setString(1, phase);
            try (ResultSet rs = stmt.executeQuery()) {
              while (rs.next()) {
                tables.add(rs.getString("table_name"));
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Error reading tracker for {}/{}: {}", sourceKey, phase, e.getMessage());
      } finally {
        if (tempDir != null) {
          deleteDir(tempDir);
        }
      }
    }
    // Cache the result (even if empty — prevents repeated S3 queries for missing data)
    stageCache.put(cacheKey, tables);
    return tables;
  }

  // ===== IncrementalTracker Bridge Implementation =====

  @Override public boolean isProcessed(String alternateName, String sourceTable,
      Map<String, String> keyValues) {
    String sourceKey = flattenKeyValues(keyValues);
    // Check whether a schema-scoped processed-cleared sentinel exists (written by freshStart).
    // If so, a "complete" marker is only honoured when its as_of is AFTER the sentinel.
    long clearedAt = getProcessedClearedSentinelAsOf(alternateName);
    if (clearedAt <= 0) {
      // No sentinel: use the fast string-only read.
      String state = readLatestState(sourceKey, alternateName, "incremental");
      return "complete".equals(state);
    }
    // Sentinel exists: must compare timestamps.
    String[] result = readLatestStateAndAsOf(sourceKey, alternateName, "incremental");
    String stateStr = result[0];
    if (!"complete".equals(stateStr)) {
      return false;
    }
    long processedAt = 0;
    if (result[1] != null) {
      try {
        processedAt = Long.parseLong(result[1]);
      } catch (NumberFormatException e) {
        // treat as 0
      }
    }
    // Only "complete" if the marker was written AFTER the sentinel.
    return processedAt > clearedAt;
  }

  @Override public boolean isProcessedWithTtl(String alternateName, String sourceTable,
      Map<String, String> keyValues, long ttlMillis) {
    String sourceKey = flattenKeyValues(keyValues);
    String year = extractYear(sourceKey, System.currentTimeMillis());
    String glob = bucketPath + "/year=" + year + "/source_key=" + sanitizeHiveValue(sourceKey)
        + "/*.parquet";
    String sql = "SELECT as_of FROM read_parquet('" + glob + "', "
        + "hive_partitioning=true, union_by_name=true) "
        + "WHERE source_key = ? AND table_name = ? AND phase = 'incremental' "
        + "AND state = 'complete' "
        + "ORDER BY as_of DESC LIMIT 1";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, sourceKey);
      stmt.setString(2, alternateName);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          long processedAt = rs.getLong("as_of");
          return (System.currentTimeMillis() - processedAt) < ttlMillis;
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("TTL check failed for {}: {}", alternateName, e.getMessage());
    }
    return false;
  }

  @Override public void markProcessed(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern) {
    markProcessedWithRowCount(alternateName, sourceTable, keyValues, targetPattern, -1);
  }

  @Override public void markProcessedWithRowCount(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern, long rowCount) {
    String sourceKey = flattenKeyValues(keyValues);
    writeState(sourceKey, alternateName, "incremental", "complete", rowCount,
        null, null, null);
  }

  @Override public void markProcessedWithError(String alternateName, String sourceTable,
      Map<String, String> keyValues, String targetPattern, String errorMessage) {
    String sourceKey = flattenKeyValues(keyValues);
    writeState(sourceKey, alternateName, "incremental", "error", 0,
        null, null, errorMessage);
  }

  @Override public Set<Map<String, String>> getProcessedKeyValues(String alternateName) {
    return getProcessedKeyValues(alternateName, null);
  }

  @Override public Set<Map<String, String>> getProcessedKeyValues(String alternateName,
      String year) {
    Set<Map<String, String>> result = new HashSet<>();
    String glob = year != null
        ? bucketPath + "/year=" + year + "/source_key=*/*.parquet"
        : bucketPath + "/year=*/source_key=*/*.parquet";

    String sql = "SELECT source_key FROM ("
        + "  SELECT source_key, state, ROW_NUMBER() OVER "
        + "    (PARTITION BY source_key ORDER BY as_of DESC) AS rn "
        + "  FROM read_parquet('" + glob + "', hive_partitioning=true, union_by_name=true) "
        + "  WHERE table_name = ? AND phase = 'incremental'"
        + ") WHERE rn = 1 AND state = 'complete'";

    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String sourceKey = rs.getString("source_key");
          result.add(unflattenKeyValues(sourceKey));
        }
      }
    } catch (SQLException e) {
      LOGGER.debug("Error getting processed keys for {}: {}", alternateName, e.getMessage());
    }
    return result;
  }

  @Override public void invalidate(String alternateName, Map<String, String> keyValues) {
    String sourceKey = flattenKeyValues(keyValues);
    writeState(sourceKey, alternateName, "incremental", "cleared", 0,
        null, null, null);
  }

  @Override public void invalidateAll(String alternateName) {
    // Append-only: read all completed source_keys and write "cleared" markers
    // Use hive_partitioning=false to avoid DuckDB Hive partition mismatch errors
    // when source_key values have different formats across schemas (e.g. SEC vs ETL).
    // The source_key column is stored inside each parquet file, so we read it from there.
    String glob = bucketPath + "/year=*/source_key=*/*.parquet";
    String sql = "SELECT source_key FROM ("
        + "  SELECT source_key, state, ROW_NUMBER() OVER "
        + "    (PARTITION BY source_key ORDER BY as_of DESC) AS rn "
        + "  FROM read_parquet('" + glob + "', "
        + "hive_partitioning=false, union_by_name=true) "
        + "  WHERE table_name = ? AND phase = 'incremental'"
        + ") WHERE rn = 1 AND state = 'complete'";

    Set<String> completedKeys = new LinkedHashSet<String>();
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, alternateName);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          completedKeys.add(rs.getString("source_key"));
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Failed to read completed keys for invalidateAll({}): {}",
          alternateName, e.getMessage());
      return;
    }

    if (completedKeys.isEmpty()) {
      LOGGER.info("invalidateAll({}): no completed partition keys found", alternateName);
      return;
    }

    LOGGER.info("invalidateAll({}): clearing {} completed partition keys",
        alternateName, completedKeys.size());
    for (String sourceKey : completedKeys) {
      writeState(sourceKey, alternateName, "incremental", "cleared", 0,
          null, null, null);
    }
  }

  /** Maximum number of glob paths per DuckDB query to avoid OOM with large dimension spaces. */
  @SuppressWarnings("UnusedVariable")
  private static final int FILTER_CHUNK_SIZE = 50_000;

  @Override public Set<Integer> filterUnprocessed(String alternateName, String sourceTable,
      List<Map<String, String>> allCombinations) {
    if (allCombinations == null || allCombinations.isEmpty()) {
      return Collections.emptySet();
    }

    // Load tracker data for this table on demand — avoids full-bucket scan across all schemas
    if (!processedKeysCache.containsKey(alternateName)) {
      processedKeysCache.put(alternateName, loadProcessedKeysForTable(alternateName));
    }

    Set<String> processedKeys = processedKeysCache.get(alternateName);
    if (processedKeys == null || processedKeys.isEmpty()) {
      LOGGER.info("No tracker data found for {} — all {} combinations unprocessed",
          alternateName, allCombinations.size());
      return allIndices(allCombinations.size());
    }

    LOGGER.info("Tracker found {} processed keys for {} (out of {} total)",
        processedKeys.size(), alternateName, allCombinations.size());

    // Pre-compute flattened keys and match against cached processed set
    Set<Integer> unprocessed = new HashSet<Integer>();
    for (int i = 0; i < allCombinations.size(); i++) {
      String flat = flattenKeyValues(allCombinations.get(i));
      if (!processedKeys.contains(flat)) {
        unprocessed.add(i);
      }
    }
    return unprocessed;
  }

  /** Scan tracker batch files for a single table and return its processed source keys. */
  private Set<String> loadProcessedKeysForTable(String tableName) {
    Set<String> result = new HashSet<String>();
    String globPath = bucketPath + "/year=*/source_key=_batch_*/*.parquet";

    // WHERE table_name = ? scopes the scan to this table — no cross-schema full-bucket reads
    String sql = "SELECT source_key FROM ("
        + "  SELECT source_key, state, ROW_NUMBER() OVER "
        + "    (PARTITION BY source_key ORDER BY as_of DESC) AS rn "
        + "  FROM read_parquet('" + globPath + "', "
        + "hive_partitioning=false, union_by_name=true) "
        + "  WHERE phase = 'incremental' AND table_name = ?"
        + ") WHERE rn = 1 AND state = 'complete'";

    long start = System.currentTimeMillis();
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, tableName);
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          result.add(rs.getString("source_key"));
        }
      }
      long elapsed = System.currentTimeMillis() - start;
      LOGGER.info("Tracker scan for {}: {} processed keys in {}ms",
          tableName, result.size(), elapsed);
    } catch (SQLException e) {
      String msg = e.getMessage();
      if (msg != null && (msg.contains("No files found")
          || msg.contains("Could not find")
          || msg.contains("HTTP 404"))) {
        LOGGER.info("No tracker batch files found for {} ({}ms)",
            tableName, System.currentTimeMillis() - start);
        return result;
      }
      LOGGER.debug("Error loading tracker data for {}: {}", tableName, msg);
    }
    return result;
  }

  private Set<Integer> allIndices(int size) {
    Set<Integer> all = new HashSet<>();
    for (int i = 0; i < size; i++) {
      all.add(i);
    }
    return all;
  }

  // ===== Table Completion =====

  @Override public boolean isTableComplete(String pipelineName, String dimensionSignature) {
    // Use in-memory cache first, then fall back to getCachedCompletion
    CachedCompletion cached = completionCache.get(pipelineName);
    if (cached == null) {
      cached = getCachedCompletion(pipelineName);
    }
    if (cached == null) {
      return false;
    }
    return dimensionSignature.equals(cached.signature);
  }

  @Override public void markTableComplete(String pipelineName, String dimensionSignature) {
    writeState("_table_complete", pipelineName, "table_completion", "complete",
        0, null, dimensionSignature, null);
    completionCache.put(pipelineName,
        new CachedCompletion(null, dimensionSignature, 0, System.currentTimeMillis(), 0));
  }

  @Override public void markTableCompleteWithConfig(String pipelineName, String configHash,
      String dimensionSignature, long rowCount) {
    writeState("_table_complete", pipelineName, "table_completion", "complete",
        rowCount, configHash, dimensionSignature, null);
    completionCache.put(pipelineName,
        new CachedCompletion(configHash, dimensionSignature, rowCount,
            System.currentTimeMillis(), 0));
  }

  @Override public void markTableCompleteWithSourceWatermark(String pipelineName,
      String configHash, String dimensionSignature, long rowCount,
      long sourceFileWatermark) {
    // Store watermark in config_hash field for simplicity
    String configWithWatermark = configHash + ":wm=" + sourceFileWatermark;
    writeState("_table_complete", pipelineName, "table_completion", "complete",
        rowCount, configWithWatermark, dimensionSignature, null);
    completionCache.put(pipelineName,
        new CachedCompletion(configHash, dimensionSignature, rowCount,
            System.currentTimeMillis(), sourceFileWatermark));
  }

  @Override public CachedCompletion getCachedCompletion(String pipelineName) {
    // Table was explicitly invalidated — treat as not complete regardless of cache state.
    // invalidateTableCompletion writes "cleared" asynchronously; this guards against
    // preloadAllCompletions reading stale "complete" state from S3 before the flush arrives.
    // "_all" is written by clearAllCompletions() and acts as a wildcard for the whole schema.
    if (clearedTables.contains(pipelineName) || clearedTables.contains("_all")) {
      return null;
    }

    // Check in-memory cache first
    CachedCompletion memoryCached = completionCache.get(pipelineName);
    if (memoryCached != null) {
      return memoryCached;
    }

    // If preloadAllCompletions already ran, the cache is authoritative — no need for per-table S3 queries
    if (completionsPreloaded) {
      return null;
    }

    long queryStart = System.currentTimeMillis();
    String glob = bucketPath + "/year=" + COMPLETION_YEAR
        + "/source_key=_table_complete/*.parquet";
    String sql = "SELECT config_hash, signature, row_count, as_of, state "
        + "FROM read_parquet('" + glob + "', hive_partitioning=true, union_by_name=true) "
        + "WHERE table_name = ? AND phase = 'table_completion' "
        + "ORDER BY as_of DESC LIMIT 1";
    try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
      stmt.setString(1, pipelineName);
      try (ResultSet rs = stmt.executeQuery()) {
        long queryElapsed = System.currentTimeMillis() - queryStart;
        if (rs.next()) {
          String state = rs.getString("state");
          if (!"complete".equals(state)) {
            LOGGER.info("getCachedCompletion({}) hit S3 in {}ms — latest state is '{}', not complete",
                pipelineName, queryElapsed, state);
            if ("cleared".equals(state)) {
              clearedTables.add(pipelineName);
            }
            return null;
          }
          String configHash = rs.getString("config_hash");
          String signature = rs.getString("signature");
          long rowCount = rs.getLong("row_count");
          long completedAt = rs.getLong("as_of");

          // Parse watermark from config hash if present
          long watermark = 0;
          if (configHash != null && configHash.contains(":wm=")) {
            int wmIdx = configHash.indexOf(":wm=");
            try {
              watermark = Long.parseLong(configHash.substring(wmIdx + 4));
              configHash = configHash.substring(0, wmIdx);
            } catch (NumberFormatException e) {
              // ignore
            }
          }
          CachedCompletion result =
              new CachedCompletion(configHash, signature, rowCount, completedAt, watermark);
          completionCache.put(pipelineName, result);
          LOGGER.info("getCachedCompletion({}) hit S3 in {}ms — found ({} rows)",
              pipelineName, queryElapsed, rowCount);
          return result;
        } else {
          LOGGER.info("getCachedCompletion({}) hit S3 in {}ms — not found",
              pipelineName, queryElapsed);
        }
      }
    } catch (SQLException e) {
      long queryElapsed = System.currentTimeMillis() - queryStart;
      LOGGER.info("getCachedCompletion({}) S3 query failed after {}ms: {}",
          pipelineName, queryElapsed, e.getMessage());
    }
    return null;
  }

  @Override public void preloadAllCompletions() {
    long start = System.currentTimeMillis();
    LOGGER.info("Preloading all table completion markers from S3 (year={})...", COMPLETION_YEAR);
    String glob = bucketPath + "/year=" + COMPLETION_YEAR
        + "/source_key=_table_complete/*.parquet";
    String sql = "SELECT table_name, config_hash, signature, row_count, as_of, state "
        + "FROM ("
        + "  SELECT table_name, config_hash, signature, row_count, as_of, state,"
        + "    ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY as_of DESC) AS rn"
        + "  FROM read_parquet('" + glob + "', hive_partitioning=true, union_by_name=true)"
        + "  WHERE phase = 'table_completion'"
        + ") WHERE rn = 1 AND state IN ('complete', 'cleared')";
    int count = 0;
    try (Statement stmt = getConnection().createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        String tableName = rs.getString("table_name");
        String state = rs.getString("state");
        if ("cleared".equals(state)) {
          clearedTables.add(tableName);
          continue;
        }
        String configHash = rs.getString("config_hash");
        String signature = rs.getString("signature");
        long rowCount = rs.getLong("row_count");
        long completedAt = rs.getLong("as_of");

        long watermark = 0;
        if (configHash != null && configHash.contains(":wm=")) {
          int wmIdx = configHash.indexOf(":wm=");
          try {
            watermark = Long.parseLong(configHash.substring(wmIdx + 4));
            configHash = configHash.substring(0, wmIdx);
          } catch (NumberFormatException e) {
            // ignore
          }
        }
        completionCache.put(tableName,
            new CachedCompletion(configHash, signature, rowCount, completedAt, watermark));
        count++;
      }
    } catch (SQLException e) {
      String msg = e.getMessage();
      if (msg != null && (msg.contains("No files found")
          || msg.contains("Could not find")
          || msg.contains("HTTP 404"))) {
        LOGGER.info("No table completion markers found ({}ms)", System.currentTimeMillis() - start);
        completionsPreloaded = true;
        return;
      }
      LOGGER.warn("Failed to preload table completions: {}", msg);
      return;
    }
    completionsPreloaded = true;
    long elapsed = System.currentTimeMillis() - start;
    LOGGER.info("Preloaded {} table completion markers in {}ms", count, elapsed);
  }

  @Override public void invalidateTableCompletion(String pipelineName) {
    completionCache.remove(pipelineName);
    clearedTables.add(pipelineName);
    writeState("_table_complete", pipelineName, "table_completion", "cleared",
        0, null, null, null);
  }

  @Override public boolean wasTableCleared(String pipelineName) {
    return clearedTables.contains(pipelineName);
  }

  // ===== Per-Period Completion =====

  /**
   * Fixed source_key partition for per-period completion markers, mirroring
   * {@code _table_complete}. Lands all period markers under year={@link #COMPLETION_YEAR}
   * so reads avoid year=* wildcard scans. The period key (year/quarter/month/week/day/
   * day_of_week + pipeline) lives in table_name.
   */
  private static final String PERIOD_SOURCE_KEY = "_period_complete";
  private static final String PERIOD_PHASE = "period_completion";

  /**
   * Fixed source_key for schema-scoped period-cleared sentinels written by
   * {@link #clearPeriodCompletions}. The schema name lives in {@code table_name}.
   * Also lives at year=0 (COMPLETION_YEAR) via the same {@code completionYearFor} logic.
   */
  private static final String PERIOD_CLEARED_SOURCE_KEY = "_period_cleared";

  /**
   * In-memory cache of schema-name → cleared-sentinel as_of timestamp.
   * Populated when the sentinel is written (this process) or read from S3.
   * A value of 0 means "not cleared" (sentinel not found).
   */
  private final Map<String, Long> periodClearedAsOfCache =
      new ConcurrentHashMap<String, Long>();

  /**
   * Fixed source_key for schema-scoped processed-cleared sentinels written by
   * {@link #clearProcessedKeys}. The schema name lives in {@code table_name}.
   * Lives at year=0 (COMPLETION_YEAR) via the {@code completionYearFor} routing.
   */
  private static final String PROCESSED_CLEARED_SOURCE_KEY = "_processed_cleared";

  /** Phase label for processed-key cleared sentinels. */
  private static final String PROCESSED_CLEARED_PHASE = "incremental_cleared";

  /**
   * In-memory cache of schema-name → processed-cleared-sentinel as_of timestamp.
   * Populated when the sentinel is written (this process) or read from S3.
   * A value of 0 means "not cleared" (sentinel not found).
   */
  private final Map<String, Long> processedClearedAsOfCache =
      new ConcurrentHashMap<String, Long>();

  @Override public void flushPending() {
    flushPendingStates();
  }

  @Override public boolean isPeriodComplete(String pipelineName,
      Map<String, String> periodValues) {
    String periodKey = IncrementalTracker.periodCompletionKey(pipelineName, periodValues);

    // Look up the cleared-sentinel asOf for this pipeline's schema (0 = no clear recorded).
    // Uses longest-prefix matching so "econ_reference_*" doesn't collide with "econ_*".
    long clearedAt = getClearedSentinelAsOf(pipelineName);

    // Cache shortcut: if no clear ever recorded, the simple state check is sufficient.
    if (clearedAt <= 0) {
      String cached = periodCompletionCache.get(periodKey);
      if (cached != null) {
        return "complete".equals(cached);
      }
      String state = readLatestState(PERIOD_SOURCE_KEY, periodKey, PERIOD_PHASE);
      periodCompletionCache.put(periodKey, state == null ? "" : state);
      return "complete".equals(state);
    }

    // A clear sentinel exists: must compare as_of timestamps.
    // Bypass the string-only cache and read state+asOf from S3.
    String[] stateResult = readLatestStateAndAsOf(PERIOD_SOURCE_KEY, periodKey, PERIOD_PHASE);
    String stateStr = stateResult[0];
    long completedAt = 0;
    if (stateResult[1] != null) {
      try {
        completedAt = Long.parseLong(stateResult[1]);
      } catch (NumberFormatException e) {
        // treat as 0
      }
    }

    if (!"complete".equals(stateStr)) {
      periodCompletionCache.put(periodKey, stateStr == null ? "" : stateStr);
      return false;
    }

    // The period's "complete" marker was written before or at the clear: treat as not complete.
    if (completedAt <= clearedAt) {
      periodCompletionCache.remove(periodKey);
      return false;
    }

    // The completion is newer than the clear: it is genuinely complete.
    periodCompletionCache.put(periodKey, "complete");
    return true;
  }

  @Override public void markPeriodComplete(String pipelineName,
      Map<String, String> periodValues) {
    String periodKey = IncrementalTracker.periodCompletionKey(pipelineName, periodValues);
    if ("complete".equals(periodCompletionCache.get(periodKey))) {
      return; // already marked complete this run — avoid duplicate appends per batch
    }
    writeState(PERIOD_SOURCE_KEY, periodKey, PERIOD_PHASE, "complete", 0, null, null, null);
    periodCompletionCache.put(periodKey, "complete");
  }

  @Override public void invalidatePeriod(String pipelineName,
      Map<String, String> periodValues) {
    String periodKey = IncrementalTracker.periodCompletionKey(pipelineName, periodValues);
    writeState(PERIOD_SOURCE_KEY, periodKey, PERIOD_PHASE, "invalidate", 0, null, null, null);
    periodCompletionCache.put(periodKey, "invalidate");
  }

  @Override public void clearPeriodCompletions(String schemaName) {
    LOGGER.info("clearPeriodCompletions({}): writing cleared sentinel at year=0", schemaName);
    long asOf = System.currentTimeMillis();
    // Write the sentinel immediately (not buffered) so subsequent isPeriodComplete calls in
    // this same process see it without requiring an explicit flushPending.
    writeStateWithAsOf(PERIOD_CLEARED_SOURCE_KEY, schemaName, PERIOD_PHASE, "cleared",
        0, null, null, null, asOf);
    periodClearedAsOfCache.put(schemaName, asOf);
    // Invalidate any period-complete cache entries for this schema's pipelines.
    // pipelineName format is "<schema>_<table>", so entries prefixed with schemaName+"_" belong here.
    String prefix = schemaName + "_";
    for (String key : new ArrayList<String>(periodCompletionCache.keySet())) {
      // periodKey format: "year_quarter_month_week_day_dow_pipelineName"
      // The pipeline name is the last '_'-delimited segment — but easier: check if the
      // periodKey contains the pipeline prefix (schemaName + "_") as a substring after the
      // last period-slot separator. We detect this by checking that the periodKey ends with
      // "_" + schemaName + "_" + something, i.e. contains "_" + prefix.
      if (key.contains("_" + prefix)) {
        periodCompletionCache.remove(key);
      }
    }
    LOGGER.info("clearPeriodCompletions({}): sentinel asOf={}, cache entries invalidated",
        schemaName, asOf);
  }

  @Override public void clearProcessedKeys(String schemaName) {
    LOGGER.info("clearProcessedKeys({}): writing processed-cleared sentinel at year=0", schemaName);
    long asOf = System.currentTimeMillis();
    // Write the sentinel immediately (not buffered) so subsequent isProcessed calls in
    // this same process see it without requiring an explicit flushPending.
    writeStateWithAsOf(PROCESSED_CLEARED_SOURCE_KEY, schemaName, PROCESSED_CLEARED_PHASE,
        "cleared", 0, null, null, null, asOf);
    processedClearedAsOfCache.put(schemaName, asOf);
    // Evict processedKeysCache entries belonging to this schema's pipelines.
    // Pipeline names start with "<schemaName>_"; the cache key is the pipeline name (alternateName).
    String prefix = schemaName + "_";
    for (String key : new ArrayList<String>(processedKeysCache.keySet())) {
      if (key.startsWith(prefix) || key.equals(schemaName)) {
        processedKeysCache.remove(key);
      }
    }
    LOGGER.info("clearProcessedKeys({}): sentinel asOf={}, processedKeysCache entries evicted",
        schemaName, asOf);
  }

  /**
   * Returns the as_of timestamp of the latest "_processed_cleared" sentinel for the schema
   * that owns the given pipeline name, using longest-prefix matching.
   *
   * <p>Mirrors {@link #getClearedSentinelAsOf}: the sentinel's {@code table_name} is the schema
   * name and the pipeline prefix match uses the same longest-prefix algorithm.
   *
   * @param pipelineName fully-qualified pipeline name (e.g. "energy_eia_coal_mines")
   * @return as_of millis of the processed-cleared sentinel, or 0 if no sentinel found
   */
  private long getProcessedClearedSentinelAsOf(String pipelineName) {
    // First pass: check in-memory cache for a schema that matches this pipeline.
    String bestMatch = null;
    int bestLen = -1;
    for (String schemaName : processedClearedAsOfCache.keySet()) {
      String prefix = schemaName + "_";
      if (pipelineName.startsWith(prefix) && schemaName.length() > bestLen) {
        bestMatch = schemaName;
        bestLen = schemaName.length();
      }
    }
    if (bestMatch != null) {
      return processedClearedAsOfCache.get(bestMatch);
    }

    // Second pass: not in cache. Derive candidate schema names from the pipeline name
    // (longest prefix first) and query S3 for each, caching misses as 0L.
    List<String> candidates = new ArrayList<String>();
    String[] parts = pipelineName.split("_", -1);
    for (int len = parts.length - 1; len >= 1; len--) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < len; i++) {
        if (i > 0) {
          sb.append('_');
        }
        sb.append(parts[i]);
      }
      candidates.add(sb.toString());
    }

    for (String candidate : candidates) {
      Long cached = processedClearedAsOfCache.get(candidate);
      if (cached != null) {
        if (cached > 0) {
          return cached;
        }
        continue; // cached miss
      }
      // Not cached: query S3 for this candidate schema sentinel
      String[] result = readLatestStateAndAsOf(PROCESSED_CLEARED_SOURCE_KEY, candidate,
          PROCESSED_CLEARED_PHASE);
      String stateStr = result[0];
      String asOfStr = result[1];
      if ("cleared".equals(stateStr) && asOfStr != null) {
        long asOf = 0;
        try {
          asOf = Long.parseLong(asOfStr);
        } catch (NumberFormatException e) {
          // ignore
        }
        if (asOf > 0) {
          processedClearedAsOfCache.put(candidate, asOf);
          return asOf;
        }
      }
      // Not found: cache as miss
      processedClearedAsOfCache.put(candidate, 0L);
    }

    return 0L;
  }

  /**
   * Read the latest (state, as_of) for a (source_key, table_name, phase) combination.
   *
   * @return String[2] where [0]=state (may be null if not found), [1]=as_of as decimal string
   *         (may be null if not found)
   */
  private String[] readLatestStateAndAsOf(String sourceKey, String tableName, String phase) {
    String year = completionYearFor(sourceKey, System.currentTimeMillis());
    String glob = bucketPath + "/year=" + year + "/source_key=" + sanitizeHiveValue(sourceKey)
        + "/*.parquet";
    String sql = "SELECT state, as_of FROM read_parquet('" + glob + "', "
        + "hive_partitioning=true, union_by_name=true) "
        + "WHERE source_key = ? AND table_name = ? AND phase = ? "
        + "ORDER BY as_of DESC LIMIT 1";

    synchronized (connectionLock) {
      try (PreparedStatement stmt = getConnection().prepareStatement(sql)) {
        stmt.setString(1, sourceKey);
        stmt.setString(2, tableName);
        stmt.setString(3, phase);
        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            return new String[]{rs.getString("state"), String.valueOf(rs.getLong("as_of"))};
          }
        }
      } catch (SQLException e) {
        LOGGER.debug("No tracker state found for {}/{}/{}: {}",
            sourceKey, tableName, phase, e.getMessage());
      }
    }
    return new String[]{null, null};
  }

  /**
   * Write a state row with an explicitly supplied {@code asOf} timestamp.
   *
   * <p>Used by {@link #clearPeriodCompletions} to ensure the cleared sentinel is
   * written with a known timestamp that can be compared to subsequent period completes.
   */
  private void writeStateWithAsOf(String sourceKey, String tableName, String phase,
      String state, long rowCount, String configHash, String signature,
      String errorMessage, long asOf) {
    ensureShutdownHook();
    boolean shouldFlush = false;
    synchronized (connectionLock) {
      pendingStates.add(
          new PendingState(sourceKey, tableName, phase,
          state, rowCount, configHash, signature, errorMessage, asOf));
      if (pendingStates.size() >= PENDING_FLUSH_THRESHOLD) {
        shouldFlush = true;
      }
    }
    if (shouldFlush) {
      flushPendingStates();
    }
    // Flush immediately so the sentinel is durable before the caller proceeds.
    flushPendingStates();
  }

  /**
   * Returns the as_of timestamp of the latest "_period_cleared" sentinel for the schema
   * that owns the given pipeline name, using longest-prefix matching.
   *
   * <p>Schema derivation: the sentinel's {@code table_name} is the schema name.  A pipeline
   * "energy_eia_electricity_generation" belongs to schema "energy" because
   * {@code "energy_" + "eia_electricity_generation"} matches — and not to any longer prefix
   * like "energy_eia" (which would also need to be a registered sentinel). The longest
   * registered schema name whose name followed by "_" is a prefix of pipelineName wins.
   *
   * <p>If no sentinel is known in memory, S3 is queried once per schema and the result
   * is cached (including "not found" as 0L).
   *
   * @param pipelineName fully-qualified pipeline name (e.g. "energy_eia_electricity_generation")
   * @return as_of millis of the cleared sentinel, or 0 if no sentinel found
   */
  private long getClearedSentinelAsOf(String pipelineName) {
    // First pass: find all schema names in cache that match this pipeline as a prefix.
    String bestMatch = null;
    int bestLen = -1;
    for (String schemaName : periodClearedAsOfCache.keySet()) {
      String prefix = schemaName + "_";
      if (pipelineName.startsWith(prefix) && schemaName.length() > bestLen) {
        bestMatch = schemaName;
        bestLen = schemaName.length();
      }
    }
    if (bestMatch != null) {
      return periodClearedAsOfCache.get(bestMatch);
    }

    // Second pass: not in cache at all. Attempt an S3 read for the candidate schema derived
    // from the pipeline name. We read the sentinel for the longest prefix of pipelineName
    // that a schema name could be. Since we don't know all schema names a priori, we check
    // by reading the _period_cleared source key for the portion of pipelineName up to each
    // underscore, longest first. Stop at the first match (or after exhausting candidates).
    // This is O(#underscores) S3 reads at most, and results are cached so it happens once
    // per pipeline per JVM.
    List<String> candidates = new ArrayList<String>();
    String[] parts = pipelineName.split("_", -1);
    // Build candidates from longest schema prefix to shortest.
    // e.g. "energy_eia_electricity_generation" → "energy_eia_electricity", "energy_eia", "energy"
    for (int len = parts.length - 1; len >= 1; len--) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < len; i++) {
        if (i > 0) {
          sb.append('_');
        }
        sb.append(parts[i]);
      }
      candidates.add(sb.toString());
    }

    for (String candidate : candidates) {
      // Check if already cached as a miss (0L means "checked, not found")
      Long cached = periodClearedAsOfCache.get(candidate);
      if (cached != null) {
        if (cached > 0) {
          return cached;
        }
        continue; // cached miss, skip to next
      }
      // Not cached: query S3 for this candidate schema sentinel
      String[] result = readLatestStateAndAsOf(PERIOD_CLEARED_SOURCE_KEY, candidate, PERIOD_PHASE);
      String stateStr = result[0];
      String asOfStr = result[1];
      if ("cleared".equals(stateStr) && asOfStr != null) {
        long asOf = 0;
        try {
          asOf = Long.parseLong(asOfStr);
        } catch (NumberFormatException e) {
          // ignore
        }
        if (asOf > 0) {
          periodClearedAsOfCache.put(candidate, asOf);
          return asOf;
        }
      }
      // Not found for this candidate: cache as miss
      periodClearedAsOfCache.put(candidate, 0L);
    }

    return 0L;
  }

  @Override public void clearAllCompletions() {
    LOGGER.warn("clearAllCompletions on S3 tracker writes 'cleared' markers. "
        + "Old parquet files remain but are superseded by the cleared state.");
    completionCache.clear();
    // Write cleared markers for table completion
    writeState("_table_complete", "_all", "table_completion", "cleared",
        0, null, null, null);
  }

  // ===== Utility Methods =====

  private String flattenKeyValues(Map<String, String> keyValues) {
    if (keyValues == null || keyValues.isEmpty()) {
      return "_empty";
    }
    if (keyValues.size() == 1) {
      return keyValues.values().iterator().next();
    }
    // Multi-key: sort and join
    StringBuilder sb = new StringBuilder();
    java.util.List<String> keys = new java.util.ArrayList<>(keyValues.keySet());
    java.util.Collections.sort(keys);
    for (String key : keys) {
      if (sb.length() > 0) {
        sb.append("__");
      }
      sb.append(key).append("=").append(keyValues.get(key));
    }
    return sb.toString();
  }

  private Map<String, String> unflattenKeyValues(String sourceKey) {
    if ("_empty".equals(sourceKey)) {
      return new LinkedHashMap<String, String>();
    }
    // Try to parse key=value pairs
    Map<String, String> result = new LinkedHashMap<>();
    if (sourceKey.contains("=") && sourceKey.contains("__")) {
      for (String part : sourceKey.split("__")) {
        int eq = part.indexOf('=');
        if (eq > 0) {
          result.put(part.substring(0, eq), part.substring(eq + 1));
        }
      }
    }
    if (result.isEmpty()) {
      // Single value, use generic key
      result.put("source_key", sourceKey);
    }
    return result;
  }

  private String sanitizeHiveValue(String value) {
    // Replace characters that are problematic in hive partition paths
    return value.replace("/", "_").replace(" ", "_").replace(":", "_");
  }

  /**
   * Resolves the year partition for a marker source key. The fixed completion
   * source keys ({@code _table_complete}, {@code _period_complete}) land under
   * {@link #COMPLETION_YEAR}; all others derive the year from the key itself.
   */
  private String completionYearFor(String sourceKey, long asOf) {
    if (PERIOD_SOURCE_KEY.equals(sourceKey)) {
      return COMPLETION_YEAR;
    }
    if (PERIOD_CLEARED_SOURCE_KEY.equals(sourceKey)) {
      return COMPLETION_YEAR;
    }
    if (PROCESSED_CLEARED_SOURCE_KEY.equals(sourceKey)) {
      return COMPLETION_YEAR;
    }
    return "_table_complete".equals(sourceKey) ? COMPLETION_YEAR : extractYear(sourceKey, asOf);
  }

  private String extractYear(String sourceKey, long asOf) {
    if (sourceKey != null) {
      // 1. Flattened dimension key: "geography=state__type=acs__year=2023"
      int yearIdx = sourceKey.indexOf("year=");
      if (yearIdx >= 0) {
        int start = yearIdx + 5; // length of "year="
        int end = start;
        while (end < sourceKey.length() && Character.isDigit(sourceKey.charAt(end))) {
          end++;
        }
        if (end - start == 4) {
          return sourceKey.substring(start, end);
        }
      }

      // 2. SEC accession format: 0000123456-YY-012345
      if (sourceKey.length() >= 15 && sourceKey.charAt(10) == '-') {
        try {
          int yy = Integer.parseInt(sourceKey.substring(11, 13));
          return String.valueOf(yy >= 90 ? 1900 + yy : 2000 + yy);
        } catch (NumberFormatException e) {
          // Fall through
        }
      }

      // 3. Bare 4-digit year (single-dimension key like "2023")
      if (sourceKey.length() == 4) {
        try {
          int y = Integer.parseInt(sourceKey);
          if (y >= 1900 && y <= 2100) {
            return sourceKey;
          }
        } catch (NumberFormatException e) {
          // Fall through
        }
      }
    }
    // Fall back to current year from timestamp
    java.util.Calendar cal = java.util.Calendar.getInstance();
    cal.setTimeInMillis(asOf);
    return String.valueOf(cal.get(java.util.Calendar.YEAR));
  }

  // ===== Write-Behind Buffering =====

  private static int parsePendingFlushThreshold() {
    String env = System.getenv("ETL_TRACKER_FLUSH_THRESHOLD");
    if (env != null && !env.isEmpty()) {
      try {
        return Integer.parseInt(env);
      } catch (NumberFormatException e) {
        // fall through
      }
    }
    return 500;
  }

  private static int parseFlushParallelism() {
    String env = System.getenv("ETL_TRACKER_FLUSH_PARALLELISM");
    if (env != null && !env.isEmpty()) {
      try {
        return Math.max(1, Integer.parseInt(env));
      } catch (NumberFormatException e) {
        // fall through
      }
    }
    return 50;
  }

  /**
   * Flush all pending tracker state writes to S3 as batched parquet files.
   *
   * <p>Groups pending states by (year, sourceKey) and writes one parquet file per group.
   * Files are written to the correct hive-partitioned path:
   * {@code year={Y}/source_key={key}/{uuid}.parquet}, which is where
   * {@link #getCompletedTables} reads from.
   *
   * <p>Writes use the AWS SDK directly (not DuckDB COPY TO) to avoid DuckDB httpfs
   * URL-encoding the {@code =} signs in hive-partition directory names, which causes
   * R2/S3 to return HTTP 400.
   */
  public void flushPendingStates() {
    List<PendingState> toFlush;
    synchronized (connectionLock) {
      if (pendingStates.isEmpty()) {
        return;
      }
      toFlush = new ArrayList<PendingState>(pendingStates);
      pendingStates.clear();
    }

    // Group by (year, sourceKey) — preserves the hive path layout that readers expect.
    // Key format: year + NUL + sourceKey
    Map<String, List<PendingState>> bySourceKey =
        new LinkedHashMap<String, List<PendingState>>();
    for (PendingState ps : toFlush) {
      String year = completionYearFor(ps.sourceKey, ps.asOf);
      String groupKey = year + "\0" + ps.sourceKey;
      List<PendingState> list = bySourceKey.get(groupKey);
      if (list == null) {
        list = new ArrayList<PendingState>();
        bySourceKey.put(groupKey, list);
      }
      list.add(ps);
    }

    // Phase 1 (sequential, DuckDB lock): write each group to a local temp parquet file.
    // Phase 2 (parallel): upload all temp files to S3 concurrently.
    final List<PendingState> failed =
        Collections.synchronizedList(new ArrayList<PendingState>());
    final AtomicInteger flushedCount = new AtomicInteger(0);

    // Build upload tasks: (tempFile, s3Path, states) for each group.
    List<Object[]> uploads = new ArrayList<Object[]>(bySourceKey.size());
    for (Map.Entry<String, List<PendingState>> entry : bySourceKey.entrySet()) {
      String groupKey = entry.getKey();
      int sep = groupKey.indexOf('\0');
      String year = groupKey.substring(0, sep);
      String sourceKey = groupKey.substring(sep + 1);
      List<PendingState> states = entry.getValue();
      String uuid = UUID.randomUUID().toString();
      String s3Path = bucketPath + "/year=" + year + "/source_key="
          + sanitizeHiveValue(sourceKey) + "/" + uuid + ".parquet";
      try {
        java.io.File tempFile = writeParquetToTemp(states);
        uploads.add(new Object[]{tempFile, s3Path, states, sourceKey});
      } catch (Exception e) {
        LOGGER.error("Failed to write local parquet for source_key={}: {}",
            sourceKey, e.getMessage());
        failed.addAll(states);
      }
    }

    // Phase 2: parallel S3 uploads.
    ExecutorService pool =
        Executors.newFixedThreadPool(Math.min(FLUSH_PARALLELISM, uploads.size()));
    List<Future<?>> futures = new ArrayList<Future<?>>(uploads.size());
    for (final Object[] upload : uploads) {
      futures.add(
          pool.submit(new Runnable() {
        @Override public void run() {
          java.io.File tempFile = (java.io.File) upload[0];
          String s3Path = (String) upload[1];
          @SuppressWarnings("unchecked")
          List<PendingState> states = (List<PendingState>) upload[2];
          String sourceKey = (String) upload[3];
          try {
            uploadTempToS3(tempFile, s3Path);
            flushedIndividualPaths.add(s3Path);
            flushedCount.addAndGet(states.size());
            LOGGER.info("Flushed {} tracker states to {}", states.size(), s3Path);
          } catch (Exception e) {
            LOGGER.error("Failed to upload tracker states for source_key={}: {}",
                sourceKey, e.getMessage());
            failed.addAll(states);
          } finally {
            if (tempFile.exists() && !tempFile.delete()) {
              LOGGER.warn("Could not delete temp tracker file: {}",
                  tempFile.getAbsolutePath());
            }
          }
        }
      }));
    }
    pool.shutdown();
    for (Future<?> f : futures) {
      try {
        f.get();
      } catch (Exception e) {
        LOGGER.error("Unexpected error waiting for flush upload: {}", e.getMessage());
      }
    }

    if (!failed.isEmpty()) {
      synchronized (connectionLock) {
        pendingStates.addAll(0, failed);
      }
    }

    int total = flushedCount.get();
    if (total > 0) {
      LOGGER.info("Flushed {} total tracker states across {} source keys ({} threads)",
          total, uploads.size(), Math.min(FLUSH_PARALLELISM, uploads.size()));
    }
  }

  /**
   * Write pending tracker states to a local temp parquet file (DuckDB, under connectionLock).
   * The caller is responsible for deleting the returned file after upload.
   *
   * @param states  pending state rows to write
   * @return local temp parquet file ready for upload
   * @throws Exception if the local DuckDB write fails
   */
  private java.io.File writeParquetToTemp(List<PendingState> states) throws Exception {
    java.io.File tempFile = java.io.File.createTempFile("tracker-flush-", ".parquet");
    synchronized (connectionLock) {
      Connection conn = getConnection();
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("CREATE TEMP TABLE IF NOT EXISTS pending_flush_aws ("
            + "source_key VARCHAR, table_name VARCHAR, phase VARCHAR, "
            + "state VARCHAR, row_count BIGINT, config_hash VARCHAR, "
            + "signature VARCHAR, error_message VARCHAR, as_of BIGINT)");
        stmt.executeUpdate("DELETE FROM pending_flush_aws");
      }
      try (PreparedStatement pstmt =
          conn.prepareStatement("INSERT INTO pending_flush_aws VALUES (?,?,?,?,?,?,?,?,?)")) {
        for (PendingState ps : states) {
          pstmt.setString(1, ps.sourceKey);
          pstmt.setString(2, ps.tableName);
          pstmt.setString(3, ps.phase);
          pstmt.setString(4, ps.state);
          pstmt.setLong(5, ps.rowCount);
          pstmt.setString(6, ps.configHash);
          pstmt.setString(7, ps.signature);
          pstmt.setString(8, ps.errorMessage);
          pstmt.setLong(9, ps.asOf);
          pstmt.addBatch();
        }
        pstmt.executeBatch();
      }
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("COPY pending_flush_aws TO '"
            + tempFile.getAbsolutePath().replace("'", "''")
            + "' (FORMAT PARQUET)");
      }
    }
    return tempFile;
  }

  /**
   * Upload a local parquet file to S3 via the AWS SDK (no connectionLock needed).
   *
   * <p>Using the SDK avoids DuckDB httpfs URL-encoding {@code =} signs in hive-partition
   * path components (e.g. {@code year=2026}), which causes R2/S3 to return HTTP 400.
   *
   * @param tempFile local parquet file to upload (caller deletes after this returns)
   * @param s3Path   full S3 URI including hive partition directories
   * @throws Exception if the S3 upload fails
   */
  private void uploadTempToS3(java.io.File tempFile, String s3Path) throws Exception {
    String pathWithoutScheme = s3Path.startsWith("s3://") ? s3Path.substring(5) : s3Path;
    int firstSlash = pathWithoutScheme.indexOf('/');
    String bucket = pathWithoutScheme.substring(0, firstSlash);
    String key = pathWithoutScheme.substring(firstSlash + 1);
    com.amazonaws.services.s3.model.ObjectMetadata metadata =
        new com.amazonaws.services.s3.model.ObjectMetadata();
    metadata.setContentLength(tempFile.length());
    try (java.io.FileInputStream fis = new java.io.FileInputStream(tempFile)) {
      getS3Client().putObject(bucket, key, fis, metadata);
    }
  }

  /**
   * Ensure a JVM shutdown hook is registered to flush pending states.
   * Called lazily on first writeState to avoid registering hooks for
   * tracker instances that only read.
   */
  private void ensureShutdownHook() {
    if (!shutdownHookRegistered) {
      shutdownHookRegistered = true;
      final S3HivePipelineTracker tracker = this;
      Runtime.getRuntime().addShutdownHook(
          new Thread(new Runnable() {
        @Override public void run() {
          try {
            tracker.flushPendingStates();
          } catch (Exception e) {
            // best effort on shutdown
          }
          try {
            tracker.compactOnClose();
          } catch (Exception e) {
            // best effort on shutdown
          }
        }
      }, "tracker-flush-shutdown"));
    }
  }

  /** Buffered state entry awaiting flush to S3. */
  private static class PendingState {
    final String sourceKey;
    final String tableName;
    final String phase;
    final String state;
    final long rowCount;
    final String configHash;
    final String signature;
    final String errorMessage;
    final long asOf;

    PendingState(String sourceKey, String tableName, String phase,
        String state, long rowCount, String configHash, String signature,
        String errorMessage, long asOf) {
      this.sourceKey = sourceKey;
      this.tableName = tableName;
      this.phase = phase;
      this.state = state;
      this.rowCount = rowCount;
      this.configHash = configHash;
      this.signature = signature;
      this.errorMessage = errorMessage;
      this.asOf = asOf;
    }
  }

  /**
   * Reads the ETL high-water mark date for the given run key.
   *
   * @param runKey identifies the specific ETL job (e.g. "2026_2026_8-K,4,13F-HR")
   * @return the stored date, or null if none has been written yet
   */
  public LocalDate readEtlHighWaterMark(String runKey) {
    String path = bucketPath.startsWith("s3://") ? bucketPath.substring(5) : bucketPath;
    int slash = path.indexOf('/');
    String bucket = slash > 0 ? path.substring(0, slash) : path;
    String keyPrefix = slash > 0 ? path.substring(slash + 1) : "";
    String s3Key = (keyPrefix.isEmpty() ? "" : keyPrefix + "/")
        + "_meta/etl_hwm/" + runKey + ".txt";
    try {
      S3Object obj = getS3Client().getObject(bucket, s3Key);
      try (InputStream is = obj.getObjectContent()) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[64];
        int n;
        while ((n = is.read(buf)) != -1) {
          baos.write(buf, 0, n);
        }
        return LocalDate.parse(baos.toString(StandardCharsets.UTF_8.name()).trim());
      }
    } catch (AmazonS3Exception e) {
      if ("NoSuchKey".equals(e.getErrorCode())) {
        return null;
      }
      LOGGER.warn("Failed to read ETL high-water mark ({}): {}", runKey, e.getMessage());
      return null;
    } catch (Exception e) {
      LOGGER.warn("Failed to read ETL high-water mark ({}): {}", runKey, e.getMessage());
      return null;
    }
  }

  /**
   * Writes the ETL high-water mark date for the given run key.
   *
   * @param runKey identifies the specific ETL job (e.g. "2026_2026_8-K,4,13F-HR")
   * @param date   the high-water mark to store
   */
  public void writeEtlHighWaterMark(String runKey, LocalDate date) {
    String path = bucketPath.startsWith("s3://") ? bucketPath.substring(5) : bucketPath;
    int slash = path.indexOf('/');
    String bucket = slash > 0 ? path.substring(0, slash) : path;
    String keyPrefix = slash > 0 ? path.substring(slash + 1) : "";
    String s3Key = (keyPrefix.isEmpty() ? "" : keyPrefix + "/")
        + "_meta/etl_hwm/" + runKey + ".txt";
    try {
      byte[] bytes = date.toString().getBytes(StandardCharsets.UTF_8);
      ObjectMetadata meta = new ObjectMetadata();
      meta.setContentLength(bytes.length);
      meta.setContentType("text/plain");
      getS3Client().putObject(bucket, s3Key, new ByteArrayInputStream(bytes), meta);
      LOGGER.info("Wrote ETL high-water mark ({}): {}", runKey, date);
    } catch (Exception e) {
      LOGGER.warn("Failed to write ETL high-water mark ({}): {}", runKey, e.getMessage());
    }
  }

  @Override public void close() {
    // Flush any pending states before closing the connection
    try {
      flushPendingStates();
    } catch (Exception e) {
      LOGGER.warn("Error flushing pending tracker states on close: {}", e.getMessage());
    }
    try {
      compactOnClose();
    } catch (Exception e) {
      LOGGER.warn("Error compacting tracker on close: {}", e.getMessage());
    }
    synchronized (connectionLock) {
      if (connection != null) {
        try {
          connection.close();
          LOGGER.debug("Closed S3 tracker in-memory DuckDB connection");
        } catch (SQLException e) {
          LOGGER.warn("Error closing S3 tracker connection: {}", e.getMessage());
        }
        connection = null;
        initialized = false;
        hasAnyTrackerData = null;
        completionCache.clear();
        stageCache.clear();
        processedKeysCache.clear();
        scannedYears.clear();
        fullyScannedYears.clear();
      }
      if (s3Client != null) {
        try {
          s3Client.shutdown();
        } catch (Exception e) {
          // ignore
        }
        s3Client = null;
      }
    }
  }
}
